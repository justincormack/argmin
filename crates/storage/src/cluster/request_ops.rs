use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::Arc;
use std::sync::MutexGuard;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Mutex, OnceLock};

use placement::NodeId;

#[cfg(any(test, feature = "test-hooks"))]
use super::{
    MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
    MetadataCommandApplyTestContext, MetadataCommandApplyTestKind,
};
use crate::metadata_command::{
    AbortMultipartUploadCommand, AdvanceCompletedMultipartUploadSequenceCommand,
    BucketPropertyMutation, BucketRecord, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteCompletedMultipartUploadCommand, DeleteObjectPayloadReclaimCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MarkBucketDeletingCommand, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandPayload, ObjectPayloadReclaimCommand, PutBucketAclCommand,
    PutBucketPropertyCommand, PutBucketSubresourceCommand, PutBucketVersioningCommand,
    PutObjectMetadataCommand, PutObjectMetadataMutation,
};
use crate::traits::PgMetadataStore;
use crate::*;

const INTERNAL_LIST_PAGE_SIZE: u32 = 1_000;

struct InsertDeleteMarkerDraft<'a> {
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    version_id: VersionId,
    owner: &'a OwnerIdentity,
    stale_payload: Option<ObjectPayloadReclaimCommand>,
}

struct BucketLifecycleContext<'a> {
    bucket_node: &'a SharedStorageNode,
    _bucket_guard: crate::node::BucketLockGuard<'a>,
    bucket_info: BucketInfo,
    raw_lifecycle: Option<String>,
}

#[cfg(test)]
type MetadataCommandApplyTestHook =
    Arc<dyn Fn(NodeId, &MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
type AbortMultipartPendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamPutCreatePendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamPutCreateCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamPutFinalizeCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type BucketDeleteCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type CompletedMultipartOrderCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
static BEFORE_METADATA_COMMAND_APPLY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS: OnceLock<
    Mutex<HashMap<usize, AbortMultipartPendingInstallTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutCreatePendingInstallTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutCreateCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutFinalizeCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_COMPLETED_MULTIPART_ORDER_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, CompletedMultipartOrderCommandIdTestHook>>,
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

#[cfg(test)]
pub(crate) struct StreamPutCreatePendingInstallTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct StreamPutCreateCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct StreamPutFinalizeCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct CompletedMultipartOrderCommandIdTestHookGuard {
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
impl Drop for CompletedMultipartOrderCommandIdTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_COMPLETED_MULTIPART_ORDER_COMMAND_ID_HOOKS
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn maybe_run_before_completed_multipart_order_command_id_hook(_scope_id: usize) {
    let hook = BEFORE_COMPLETED_MULTIPART_ORDER_COMMAND_ID_HOOKS
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
        MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(command) => (
            MetadataCommandApplyTestKind::AdvanceCompletedMultipartUploadSequence,
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
        MetadataCommandPayload::DeleteCompletedMultipartUpload(command) => (
            MetadataCommandApplyTestKind::DeleteCompletedMultipartUpload,
            Some(command.record.bucket.clone()),
            Some(command.record.key.clone()),
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

fn merge_bucket_snapshot_pair_request(
    source: BucketSnapshotRequest,
    destination: BucketSnapshotRequest,
) -> BucketSnapshotRequest {
    BucketSnapshotRequest {
        policy: source.policy || destination.policy,
        tags: match (source.tags, destination.tags) {
            (BucketSnapshotTagsRequest::Always, _) | (_, BucketSnapshotTagsRequest::Always) => {
                BucketSnapshotTagsRequest::Always
            }
            (BucketSnapshotTagsRequest::IfBucketAbacEnabled, _)
            | (_, BucketSnapshotTagsRequest::IfBucketAbacEnabled) => {
                BucketSnapshotTagsRequest::IfBucketAbacEnabled
            }
            (BucketSnapshotTagsRequest::NotRequested, BucketSnapshotTagsRequest::NotRequested) => {
                BucketSnapshotTagsRequest::NotRequested
            }
        },
        lifecycle: source.lifecycle || destination.lifecycle,
        cors: source.cors || destination.cors,
    }
}

fn conflicting_pending_metadata_command(context: &'static str) -> BucketSnapshotLoadError {
    StoreError::Io {
        context,
        source: std::io::Error::other("conflicting pending metadata command"),
    }
    .into()
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

    #[cfg(test)]
    pub(crate) fn test_install_before_stream_put_create_pending_install_hook(
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

    #[cfg(test)]
    pub(crate) fn test_install_before_stream_put_finalize_command_id_hook(
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

    #[cfg(test)]
    pub(crate) fn test_install_before_completed_multipart_order_command_id_hook(
        &self,
        hook: CompletedMultipartOrderCommandIdTestHook,
    ) -> CompletedMultipartOrderCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_COMPLETED_MULTIPART_ORDER_COMMAND_ID_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        CompletedMultipartOrderCommandIdTestHookGuard { scope_id }
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
        self.metadata_primary_topology_node().pg_ids()
    }

    fn metadata_pg_ids(&self) -> Vec<u32> {
        self.metadata_primary_topology_node().pg_ids().to_vec()
    }

    fn metadata_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        self.metadata_pg_primary_node(pg_id)?.get_pg(pg_id)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_bucket_pg_available(
        &self,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .try_probe_bucket_pg_available(bucket)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .try_probe_object_pg_available(bucket, key)
    }

    pub fn create_bucket_with_config_and_load_info(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        let pg_id = self.bucket_metadata_pg_id(&bucket);
        let primary_node = self.bucket_metadata_primary_node(&bucket)?;
        let _bucket_guard = primary_node.lock_bucket(&bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id)?;
            match PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket) {
                Ok(info) => {
                    return Ok(BucketCreateAttemptOutcome::Exists(info));
                }
                Err(MetadataError::BucketNotFound { .. }) => {}
                Err(other) => return Err(other.into()),
            }
        }

        let pg_id = PgId::new(pg_id);
        loop {
            let (command, clear_pending_on_zero_apply) = match self
                .pending_metadata_command_for_bucket(pg_id, &bucket)?
            {
                Some(command) => {
                    if self.drain_unrelated_pending_metadata_command_for_bucket(
                        pg_id, &bucket, &command,
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
                            self.drain_pending_metadata_command_pg_slot(pg_id, &bucket, &command)?;
                            continue;
                        }
                    }
                }
                None => {
                    let Some(command_id) =
                        self.next_bucket_metadata_command_id_or_drain(pg_id, &bucket)?
                    else {
                        continue;
                    };
                    let bucket_pg = primary_node.get_pg(pg_id.get())?;
                    let bucket_execution_generation =
                        bucket_pg.next_bucket_execution_generation_candidate()?;
                    drop(bucket_pg);
                    let command = CreateBucketCommand::from_config(
                        config,
                        crate::clock::current_time_millis(),
                        bucket_execution_generation,
                    )
                    .map_err(|reason| MetadataError::InvalidBucketName {
                        reason: reason.to_string(),
                    })?;
                    let command = MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::CreateBucket(command),
                    );
                    if self
                        .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, &command)?
                        .is_none()
                    {
                        continue;
                    }
                    (command, true)
                }
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                &bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket(&*bucket_pg, &bucket)?;
            return Ok(BucketCreateAttemptOutcome::Created(info));
        }
    }

    pub(super) fn apply_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let primary_node_id = self
            .local_map
            .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?
            .node_id();
        self.apply_metadata_command_to_acting_set_from_origin(primary_node_id, command)
    }

    fn apply_metadata_command_to_acting_set_from_origin(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut nodes = self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let primary_node_id = self
            .local_map
            .pg_route(pg_id)
            .expect("validated metadata PG command route must exist")
            .primary_node_id();
        nodes.sort_by_key(|node| node.node_id() == primary_node_id);
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            let acceptance = self
                .local_map
                .validate_metadata_command_for_replica(
                    origin_node_id,
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
            self.validate_metadata_command_bucket_write_reservation(command)
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
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                }
            })?;
            pg.apply_metadata_command_and_record(node.node_id().as_u32(), command)
                .map_err(|source| MetadataCommandApplyFailure {
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
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let primary_node_id = self
            .local_map
            .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?
            .node_id();
        let mut nodes = self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        nodes.sort_by_key(|node| node.node_id() == primary_node_id);
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
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
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                }
            })?;
            pg.record_metadata_command_abandoned(node.node_id().as_u32(), command)
                .map_err(|source| MetadataCommandApplyFailure {
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
        let pg_id = command.id().pg_id();
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                }
            })?;
            if pg
                .metadata_command_abandoned(node.node_id().as_u32(), command)
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
        self.apply_metadata_command_to_acting_set_from_origin(origin_node_id, command)
            .map_err(|error| error.source)
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set(
        &self,
        pg_id: PgId,
        _bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        match self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            false,
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

    pub(super) fn finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            true,
        )
    }

    fn finish_pending_metadata_command_to_acting_set_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        retry_partial_exact_conflict: bool,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        let mut command = command.clone();
        loop {
            let command_bucket = command.bucket_name();
            if self
                .metadata_command_has_abandoned_log_on_acting_set(&command)
                .map_err(|error| error.source)?
            {
                self.record_abandoned_metadata_command_to_acting_set(&command)
                    .map_err(|error| error.source)?;
                self.release_metadata_command_bucket_write_reservation(&command)?;
                self.remove_pending_metadata_command_for_bucket(pg_id, command_bucket, &command)?;
                return Ok(FinishPendingMetadataCommandResult::Abandoned);
            }
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_metadata_command_bucket_write_reservation(&command)?;
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command_bucket,
                        &command,
                    )?;
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
                        && self.partial_exact_metadata_command_conflict_is_retryable(
                            pg_id,
                            &command,
                            applied_nodes,
                        )?
                    {
                        return Ok(FinishPendingMetadataCommandResult::RetryPartialExactConflict);
                    }
                    if applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let Some(reissued) =
                            self.reissue_pending_metadata_command(pg_id, &command)?
                        else {
                            return Ok(FinishPendingMetadataCommandResult::Abandoned);
                        };
                        command = reissued;
                        continue;
                    }
                    if clear_pending_on_zero_apply && applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| error.source)?;
                        self.remove_pending_metadata_command_for_bucket(
                            pg_id,
                            command_bucket,
                            &command,
                        )?;
                    }
                    return Err(source);
                }
            }
        }
    }

    fn metadata_command_bucket_name(command: &MetadataCommandEnvelope) -> &BucketName {
        command.bucket_name()
    }

    fn drain_unrelated_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pending_bucket = Self::metadata_command_bucket_name(command).clone();
        if pending_bucket == *bucket {
            return Ok(false);
        }
        self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, command)?;
        Ok(true)
    }

    fn drain_pending_metadata_command_pg_slot(
        &self,
        pg_id: PgId,
        pending_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry(
                    pg_id, command, false,
                )?;
            if let FinishPendingMetadataCommandResult::RetryPartialExactConflict = outcome {
                return Ok(());
            }
        } else {
            self.finish_pending_object_metadata_command_for_bucket(pg_id, pending_bucket, command)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        }
        Ok(())
    }

    fn drain_pending_completed_multipart_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set(pg_id, bucket, command, false)?;
        Ok(())
    }

    fn next_bucket_metadata_command_id_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        match self.next_bucket_metadata_command_id(pg_id) {
            Ok(command_id) => Ok(Some(command_id)),
            Err(
                error @ BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                }),
            ) => {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&command).clone();
                    self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, &command)?;
                    Ok(None)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    fn finish_pending_command_for_completed_multipart_order(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
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
                self.finish_pending_object_metadata_command_for_bucket(pg_id, bucket, command)
            }
            MetadataCommandPayload::CreateBucket(_)
            | MetadataCommandPayload::PutBucketVersioning(_)
            | MetadataCommandPayload::PutBucketAcl(_)
            | MetadataCommandPayload::PutBucketProperty(_)
            | MetadataCommandPayload::PutBucketSubresource(_)
            | MetadataCommandPayload::MarkBucketDeleting(_)
            | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
            | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => self
                .finish_pending_metadata_command_to_acting_set(pg_id, bucket, command, false)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error),
        }
    }

    fn delete_bucket_from_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_start",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        let mut nodes = self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?;
        let primary_node_id = self
            .local_map
            .pg_route(pg_id)
            .expect("validated metadata PG delete route must exist")
            .primary_node_id();
        nodes.sort_by_key(|node| node.node_id() == primary_node_id);

        for node in nodes {
            let node_id = node.node_id();
            let pg = node.storage_node().get_pg(pg_id.get())?;
            match PgMetadataStore::delete_finalized_bucket(&*pg, bucket) {
                Ok(()) => {
                    pg.refresh_metadata_command_state_digest()?;
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_node_ok",
                        Some(format_args!(
                            "bucket={:?} pg_id={} node_id={:?}",
                            bucket,
                            pg_id.get(),
                            node_id
                        )),
                    );
                }
                Err(crate::error::MetadataError::BucketNotFound { .. })
                    if node_id == primary_node_id =>
                {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_primary_missing",
                        Some(format_args!(
                            "bucket={:?} pg_id={} node_id={:?}",
                            bucket,
                            pg_id.get(),
                            node_id
                        )),
                    );
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_replica_missing",
                        Some(format_args!(
                            "bucket={:?} pg_id={} node_id={:?}",
                            bucket,
                            pg_id.get(),
                            node_id
                        )),
                    );
                }
                Err(other) => {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_node_error",
                        Some(format_args!(
                            "bucket={:?} pg_id={} node_id={:?} error={:?}",
                            bucket,
                            pg_id.get(),
                            node_id,
                            other
                        )),
                    );
                    return Err(other.into());
                }
            }
        }
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_done",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        Ok(BucketDeleteFinalizeOutcome::Finalized)
    }

    pub fn load_bucket_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .load_bucket_snapshot(bucket, request)
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
            let Ok(node) = self.metadata_pg_primary_node(pg_id) else {
                continue;
            };
            let Ok(generations) = node.load_bucket_execution_generations_for_pg(pg_id, &buckets)
            else {
                continue;
            };
            batches.push((buckets, generations));
        }
        batches
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
            Ok(action(snapshot))
        })
    }

    pub(crate) fn with_bucket_write_reservation_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<Result<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        loop {
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

            let result = (|| {
                let bucket_pg = reservation.node.get_pg(reservation.pg_id)?;
                let snapshot = crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket_pg, bucket, request,
                )?;
                drop(bucket_pg);
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
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self.bucket_metadata_primary_node_arc(bucket)?;
        let bucket_pg = node.get_pg(pg_id)?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*bucket_pg,
            bucket,
            &reservation_id,
            &owner_token,
            self.operation_epoch(),
            operation_kind,
            crate::clock::current_time_millis(),
            None,
            target_context,
        )?;
        if let Err(error) = PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
            let _ = PgMetadataStore::release_durable_bucket_write_reservation(
                &*bucket_pg,
                bucket,
                &record.reservation_id,
                &record.owner_token,
                record.cluster_epoch,
                record.bucket_execution_generation,
            );
            return Err(error.into());
        }
        drop(bucket_pg);
        Ok(super::DurableBucketWriteReservation {
            node,
            pg_id,
            record,
            legacy_counter_acquired: true,
        })
    }

    pub(super) fn release_durable_bucket_write_reservation(
        &self,
        reservation: super::DurableBucketWriteReservation,
    ) -> Result<(), BucketSnapshotLoadError> {
        let bucket_pg = reservation.node.get_pg(reservation.pg_id)?;
        let durable_result = PgMetadataStore::release_durable_bucket_write_reservation(
            &*bucket_pg,
            &reservation.record.bucket,
            &reservation.record.reservation_id,
            &reservation.record.owner_token,
            reservation.record.cluster_epoch,
            reservation.record.bucket_execution_generation,
        );
        let legacy_result = if reservation.legacy_counter_acquired {
            PgMetadataStore::release_bucket_write_reservation(
                &*bucket_pg,
                &reservation.record.bucket,
            )
        } else {
            Ok(())
        };
        if legacy_result.is_ok() && reservation.legacy_counter_acquired {
            reservation
                .node
                .notify_bucket_coordination_change(&reservation.record.bucket);
        }
        durable_result?;
        legacy_result?;
        Ok(())
    }

    pub(super) fn wait_for_durable_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        let node = self.bucket_metadata_primary_node_arc(bucket)?;
        let bucket_pg = node.get_pg(self.bucket_metadata_pg_id(bucket))?;
        match PgMetadataStore::head_bucket(&*bucket_pg, bucket) {
            Ok(_) => {}
            Err(MetadataError::BucketNotFound { .. }) => {
                return Err(MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into());
            }
            Err(other) => return Err(other.into()),
        }
        drop(bucket_pg);
        std::thread::sleep(std::time::Duration::from_millis(1));
        Ok(())
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
            let merged_request = merge_bucket_snapshot_pair_request(source.1, destination.1);
            let bucket = self.load_bucket_snapshot(source.0, merged_request)?;
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let source_pg_id = self.bucket_metadata_pg_id(source.0);
        let destination_pg_id = self.bucket_metadata_pg_id(destination.0);
        if source_pg_id == destination_pg_id {
            let bucket_pg = self.metadata_pg(source_pg_id)?;
            return Ok(BucketSnapshotPair::Distinct {
                source: Box::new(crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket_pg, source.0, source.1,
                )?),
                destination: Box::new(crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket_pg,
                    destination.0,
                    destination.1,
                )?),
            });
        }

        let (source_snapshot, destination_snapshot) = if source_pg_id < destination_pg_id {
            let source_pg = self.metadata_pg(source_pg_id)?;
            let destination_pg = self.metadata_pg(destination_pg_id)?;
            (
                crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &source_pg, source.0, source.1,
                )?,
                crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &destination_pg,
                    destination.0,
                    destination.1,
                )?,
            )
        } else {
            let destination_pg = self.metadata_pg(destination_pg_id)?;
            let source_pg = self.metadata_pg(source_pg_id)?;
            (
                crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &source_pg, source.0, source.1,
                )?,
                crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &destination_pg,
                    destination.0,
                    destination.1,
                )?,
            )
        };

        Ok(BucketSnapshotPair::Distinct {
            source: Box::new(source_snapshot),
            destination: Box::new(destination_snapshot),
        })
    }

    pub fn begin_bucket_delete(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self.bucket_metadata_primary_node(bucket)?;
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_delete_begin_start",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        let drain = node.begin_bucket_write_drain(bucket)?;
        crate::node::maybe_run_after_begin_bucket_delete_drain_hook(bucket);

        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::MarkBucketDeleting(mark)
                        if mark.bucket_name() == bucket =>
                    {
                        let bucket_pg = node.get_pg(pg_id.get())?;
                        let current = bucket_pg.head_bucket_record_raw(bucket)?;
                        if !Self::pending_bucket_command_matches_current(
                            current,
                            &mark.bucket,
                            |record| {
                                Ok(MarkBucketDeletingCommand::from_bucket(
                                    record.with_execution_generation(
                                        mark.bucket.bucket_execution_generation,
                                    ),
                                )
                                .bucket)
                            },
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                        {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                conflicting_pending_metadata_command(
                                    "conflicting pending mark bucket deleting command",
                                ),
                            ));
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::MarkBucketDeleting(_) => {
                        self.finish_pending_metadata_command_to_acting_set(
                            pg_id, bucket, &command, false,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        continue;
                    }
                    MetadataCommandPayload::CreateBucket(_)
                    | MetadataCommandPayload::PutBucketVersioning(_)
                    | MetadataCommandPayload::PutBucketAcl(_)
                    | MetadataCommandPayload::PutBucketProperty(_)
                    | MetadataCommandPayload::PutBucketSubresource(_)
                    | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
                    | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.finish_pending_metadata_command_to_acting_set(
                            pg_id, bucket, &command, false,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
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
                        for raw_pg_id in self.metadata_pg_ids() {
                            self.drain_pending_object_metadata_commands_for_exact_bucket(
                                PgId::new(raw_pg_id),
                                bucket,
                            )
                            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        }
                        continue;
                    }
                }
            } else {
                for raw_pg_id in self.metadata_pg_ids() {
                    self.drain_pending_object_metadata_commands_for_exact_bucket(
                        PgId::new(raw_pg_id),
                        bucket,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                }
                if self
                    .pending_metadata_command_for_bucket(pg_id, bucket)?
                    .is_some()
                {
                    continue;
                }
                if self.bucket_has_visible_data(bucket, true)? {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_not_empty",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
                    );
                    return Err(crate::error::MetadataError::BucketNotEmpty.into());
                }
                #[cfg(test)]
                maybe_run_before_bucket_delete_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                let command_id = match self.next_metadata_command_id(pg_id) {
                    Ok(command_id) => command_id,
                    Err(StoreError::MetadataCommandLogConflict { .. }) => continue,
                    Err(error) => return Err(BucketWriteDrainError::from(error)),
                };
                let bucket_pg = node.get_pg(pg_id.get())?;
                let current = bucket_pg.head_bucket_record_raw(bucket)?;
                if current.state == BucketState::Deleting {
                    node.notify_bucket_coordination_change(bucket);
                    drain.persist();
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_done",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
                    );
                    return Ok(());
                }
                let bucket_execution_generation =
                    bucket_pg.next_bucket_execution_generation_candidate()?;
                drop(bucket_pg);
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::MarkBucketDeleting(
                        MarkBucketDeletingCommand::from_bucket(
                            current.with_execution_generation(bucket_execution_generation),
                        ),
                    ),
                );
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                    .map_err(BucketWriteDrainError::from)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            match outcome {
                FinishPendingMetadataCommandResult::Applied => {}
                FinishPendingMetadataCommandResult::Abandoned
                | FinishPendingMetadataCommandResult::RetryPartialExactConflict => continue,
            }

            node.notify_bucket_coordination_change(bucket);
            drain.persist();
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_begin_done",
                Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
            );
            return Ok(());
        }
    }

    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket_node = self.bucket_metadata_primary_node(bucket)?;
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_start",
            Some(format_args!(
                "bucket={:?} bucket_pg_id={}",
                bucket,
                self.bucket_metadata_pg_id(bucket)
            )),
        );
        let _bucket_guard = bucket_node.lock_bucket(bucket);
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        {
            let bucket_pg = self.metadata_pg(bucket_pg_id)?;
            let info = match PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket) {
                Ok(info) => info,
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_not_found",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
                    );
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(other) => return Err(other.into()),
            };
            if info.state != BucketState::Deleting {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_not_deleting",
                    Some(format_args!(
                        "bucket={:?} pg_id={} state={:?}",
                        bucket, bucket_pg_id, info.state
                    )),
                );
                return Ok(BucketDeleteFinalizeOutcome::NotDeleting);
            }
        }

        if self.bucket_has_visible_data(bucket, false)? {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_finalize_pending_visible_data",
                Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
            );
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        let reclaim_roots = self.bucket_payload_reclaim_roots(bucket)?;
        for root in &reclaim_roots {
            if self.local_map.runtime_state().object_payload_lease_count(
                &root.bucket,
                &root.key,
                root.generation_id,
            ) == 0
            {
                self.enqueue_object_payload_reclaim(&root.bucket, &root.key, root.generation_id);
            }
        }

        if !reclaim_roots.is_empty()
            || self
                .local_map
                .runtime_state()
                .bucket_object_payload_lease_count(bucket)
                != 0
        {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_finalize_pending_reclaim",
                Some(format_args!(
                    "bucket={:?} pg_id={} reclaim_roots={} lease_count={}",
                    bucket,
                    bucket_pg_id,
                    reclaim_roots.len(),
                    self.local_map
                        .runtime_state()
                        .bucket_object_payload_lease_count(bucket)
                )),
            );
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        self.delete_completed_multipart_uploads_for_bucket(bucket)?;

        self.delete_bucket_from_acting_set(PgId::new(bucket_pg_id), bucket)
    }

    fn bucket_has_visible_data(
        &self,
        bucket: &BucketName,
        include_stream_uploads: bool,
    ) -> Result<bool, BucketWriteDrainError> {
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            let versions = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                start_at: None,
                max_keys: 1,
            })?;
            if !versions.versions.is_empty() {
                return Ok(true);
            }

            let uploads = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !uploads.uploads.is_empty() {
                return Ok(true);
            }

            if include_stream_uploads {
                let sessions = pg.list_all_stream_uploads()?;
                if sessions
                    .iter()
                    .any(|session| session.bucket == bucket.as_str())
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn bucket_payload_reclaim_roots(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<PayloadReclaimRoot>, BucketWriteDrainError> {
        let mut roots = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            if let Some(root) = PgMetadataStore::get_bucket_payload_reclaim_root(&*pg, bucket)? {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    fn delete_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let records =
            self.completed_multipart_upload_records_for_bucket::<BucketWriteDrainError>(bucket)?;
        for (pg_id, record) in records {
            self.delete_completed_multipart_upload_record_with_command(pg_id, record)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        }
        Ok(())
    }

    fn completed_multipart_upload_records_for_bucket<E>(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<(PgId, CompletedMultipartUploadRecord)>, E>
    where
        E: From<StoreError> + From<MetadataError>,
    {
        let mut records = HashMap::<(u32, UploadId), CompletedMultipartUploadRecord>::new();
        for raw_pg_id in self.metadata_pg_ids() {
            let pg_id = PgId::new(raw_pg_id);
            let nodes = self
                .local_map
                .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?;
            for node in nodes {
                let pg = node.storage_node().get_pg(pg_id.get())?;
                for record in
                    pg.list_completed_multipart_upload_records_for_bucket(bucket.as_str())?
                {
                    let key = (pg_id.get(), record.upload_id.clone());
                    match records.entry(key) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(record);
                        }
                        std::collections::hash_map::Entry::Occupied(entry)
                            if entry.get() == &record => {}
                        std::collections::hash_map::Entry::Occupied(_) => {
                            return Err(MetadataError::Db {
                                context:
                                    "conflicting completed multipart upload tombstone replicas",
                                source: rusqlite::Error::InvalidQuery,
                            }
                            .into());
                        }
                    }
                }
            }
        }
        Ok(records
            .into_iter()
            .map(|((pg_id, _), record)| (PgId::new(pg_id), record))
            .collect())
    }

    fn delete_completed_multipart_upload_record_with_command(
        &self,
        pg_id: PgId,
        record: CompletedMultipartUploadRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, &record.bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket(
                    pg_id,
                    &record.bucket,
                    &command,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::DeleteCompletedMultipartUpload(delete)
                        if delete.record == record =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::DeleteCompletedMultipartUpload(_) => {
                        self.finish_pending_metadata_command_to_acting_set(
                            pg_id,
                            &record.bucket,
                            &command,
                            false,
                        )?;
                        continue;
                    }
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.drain_pending_completed_multipart_sequence_command(
                            pg_id,
                            &record.bucket,
                            &command,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot(
                            pg_id,
                            &record.bucket,
                            &command,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) =
                    self.next_bucket_metadata_command_id_or_drain(pg_id, &record.bucket)?
                else {
                    continue;
                };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::DeleteCompletedMultipartUpload(Box::new(
                        DeleteCompletedMultipartUploadCommand {
                            record: record.clone(),
                        },
                    )),
                );
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, &record.bucket, &command)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                &record.bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }
            return Ok(());
        }
    }

    pub fn head_bucket_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .head_bucket_info(bucket)
    }

    pub fn get_bucket_subresource(
        &self,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .get_bucket_subresource(bucket, kind)
    }

    fn pending_bucket_command_matches_current(
        current: BucketRecord,
        target: &BucketRecord,
        build_expected: impl FnOnce(BucketRecord) -> Result<BucketRecord, BucketSnapshotLoadError>,
    ) -> Result<bool, BucketSnapshotLoadError> {
        if current.bucket_execution_generation == target.bucket_execution_generation {
            return Ok(current.command_metadata_eq(target));
        }
        if current.bucket_execution_generation > target.bucket_execution_generation {
            return Ok(false);
        }
        Ok(build_expected(current)?.command_metadata_eq(target))
    }

    pub fn put_bucket_versioning_and_load_info(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
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

        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.bucket_name() == bucket =>
                    {
                        let bucket_pg = primary_node.get_pg(pg_id.get())?;
                        let current = bucket_pg.head_bucket_record_raw(bucket)?;
                        let same_request = versioning.bucket.versioning == state;
                        if !Self::pending_bucket_command_matches_current(
                            current,
                            &versioning.bucket,
                            |record| {
                                if state == BucketVersioningState::Disabled
                                    && record.versioning != BucketVersioningState::Disabled
                                {
                                    return Err(MetadataError::InvalidVersioningTransition {
                                        from: record.versioning,
                                        to: state,
                                    }
                                    .into());
                                }
                                Ok(PutBucketVersioningCommand::from_bucket(
                                    record.with_execution_generation(
                                        versioning.bucket.bucket_execution_generation,
                                    ),
                                    state,
                                )
                                .bucket)
                            },
                        )? {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket versioning command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.drain_pending_completed_multipart_sequence_command(
                            pg_id, bucket, &command,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) =
                    self.next_bucket_metadata_command_id_or_drain(pg_id, bucket)?
                else {
                    continue;
                };
                let bucket_pg = primary_node.get_pg(pg_id.get())?;
                let current = bucket_pg.head_bucket_record_raw(bucket)?;
                let bucket_execution_generation =
                    bucket_pg.next_bucket_execution_generation_candidate()?;
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::PutBucketVersioning(
                        PutBucketVersioningCommand::from_bucket(
                            current.with_execution_generation(bucket_execution_generation),
                            state,
                        ),
                    ),
                );
                drop(bucket_pg);
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
            return Ok(info);
        }
    }

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

    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::PublicAccessBlock(None),
        )
    }

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

    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::OwnershipControls(None),
        )
    }

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

    pub fn put_bucket_acl_and_load_info(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        }

        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketAcl(acl) if acl.bucket_name() == bucket => {
                        let bucket_pg = primary_node.get_pg(pg_id.get())?;
                        let current = bucket_pg.head_bucket_record_raw(bucket)?;
                        let same_request = acl.bucket.acl_grants == *acl_grants
                            && acl.bucket.public_read == public_read
                            && acl.bucket.public_write == public_write;
                        if !Self::pending_bucket_command_matches_current(
                            current,
                            &acl.bucket,
                            |record| {
                                Ok(PutBucketAclCommand::from_bucket(
                                    record.with_execution_generation(
                                        acl.bucket.bucket_execution_generation,
                                    ),
                                    acl_grants.clone(),
                                    public_read,
                                    public_write,
                                )
                                .bucket)
                            },
                        )? {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket acl command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.drain_pending_completed_multipart_sequence_command(
                            pg_id, bucket, &command,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) =
                    self.next_bucket_metadata_command_id_or_drain(pg_id, bucket)?
                else {
                    continue;
                };
                let bucket_pg = primary_node.get_pg(pg_id.get())?;
                let current = bucket_pg.head_bucket_record_raw(bucket)?;
                let bucket_execution_generation =
                    bucket_pg.next_bucket_execution_generation_candidate()?;
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                        current.with_execution_generation(bucket_execution_generation),
                        acl_grants.clone(),
                        public_read,
                        public_write,
                    )),
                );
                drop(bucket_pg);
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
            return Ok(info);
        }
    }

    fn put_bucket_property_command_and_load_info(
        &self,
        bucket: &BucketName,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        }

        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketProperty(property)
                        if property.bucket_name() == bucket
                            && property.effect == mutation.effect() =>
                    {
                        let bucket_pg = primary_node.get_pg(pg_id.get())?;
                        let current = bucket_pg.head_bucket_record_raw(bucket)?;
                        if !Self::pending_bucket_command_matches_current(
                            current,
                            &property.bucket,
                            |record| {
                                Ok(PutBucketPropertyCommand::from_bucket_and_mutation(
                                    record.with_execution_generation(
                                        property.bucket.bucket_execution_generation,
                                    ),
                                    mutation.clone(),
                                )
                                .bucket)
                            },
                        )? {
                            self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.drain_pending_completed_multipart_sequence_command(
                            pg_id, bucket, &command,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) =
                    self.next_bucket_metadata_command_id_or_drain(pg_id, bucket)?
                else {
                    continue;
                };
                let bucket_pg = primary_node.get_pg(pg_id.get())?;
                let current = bucket_pg.head_bucket_record_raw(bucket)?;
                let bucket_execution_generation =
                    bucket_pg.next_bucket_execution_generation_candidate()?;
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::PutBucketProperty(
                        PutBucketPropertyCommand::from_bucket_and_mutation(
                            current.with_execution_generation(bucket_execution_generation),
                            mutation.clone(),
                        ),
                    ),
                );
                drop(bucket_pg);
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
            return Ok(info);
        }
    }

    pub fn put_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_subresource_command_and_load_info(
            bucket,
            BucketSubresourceMutation::Put {
                kind: req.kind,
                body: req.body.to_owned(),
                aux: req.aux,
            },
        )
    }

    pub fn delete_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_subresource_command_and_load_info(
            bucket,
            BucketSubresourceMutation::Delete { kind },
        )
    }

    fn put_bucket_subresource_command_and_load_info(
        &self,
        bucket: &BucketName,
        mutation: BucketSubresourceMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        if let BucketSubresourceMutation::Put { kind, aux, .. } = &mutation {
            if !kind.supports_aux(*aux) {
                return Err(MetadataError::Db {
                    context: "put bucket subresource",
                    source: rusqlite::Error::InvalidParameterName(format!(
                        "{kind:?} does not support aux {aux:?}"
                    )),
                }
                .into());
            }
        }

        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        }
        loop {
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketSubresource(subresource)
                        if subresource.matches_mutation(bucket, &mutation) =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {
                        self.drain_pending_completed_multipart_sequence_command(
                            pg_id, bucket, &command,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot(pg_id, bucket, &command)?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) =
                    self.next_bucket_metadata_command_id_or_drain(pg_id, bucket)?
                else {
                    continue;
                };
                let bucket_pg = primary_node.get_pg(pg_id.get())?;
                let bucket_execution_generation =
                    bucket_pg.next_bucket_execution_generation_candidate()?;
                drop(bucket_pg);
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                        bucket.clone(),
                        mutation.clone(),
                        bucket_execution_generation,
                    )),
                );
                if self
                    .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                    .is_none()
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set(
                pg_id,
                bucket,
                &command,
                clear_pending_on_zero_apply,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
            return Ok(info);
        }
    }

    pub fn list_buckets_for_owner(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        let mut buckets = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            let mut page = pg.list_buckets(owner_canonical_id)?;
            buckets.append(&mut page);
        }
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    pub fn prune_completed_multipart_uploads_for_bucket_with_limit(
        &self,
        bucket: &BucketName,
        keep: usize,
    ) -> Result<(), ObjectPgActionError> {
        crate::node::maybe_run_before_completed_multipart_prune_hook(bucket)?;

        let mut uploads =
            self.completed_multipart_upload_records_for_bucket::<ObjectPgActionError>(bucket)?;
        uploads.sort_by(|(left_pg, left), (right_pg, right)| {
            right
                .completion_order
                .cmp(&left.completion_order)
                .then_with(|| left_pg.get().cmp(&right_pg.get()))
                .then_with(|| left.upload_id.as_str().cmp(right.upload_id.as_str()))
        });
        for (pg_id, record) in uploads.into_iter().skip(keep) {
            self.delete_completed_multipart_upload_record_with_command(pg_id, record)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        }
        Ok(())
    }

    pub fn list_lifecycle_sweep_buckets(
        &self,
    ) -> Result<LifecycleSweepBuckets, ObjectPgActionError> {
        let mut lifecycle_buckets = Vec::new();
        let mut aborting_buckets = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            lifecycle_buckets.extend(pg.list_buckets_with_lifecycle()?);
            aborting_buckets.extend(pg.list_buckets_with_aborting_multipart_uploads()?);
        }
        lifecycle_buckets.sort_by(|a, b| a.name.cmp(&b.name));
        aborting_buckets.sort();
        aborting_buckets.dedup();
        Ok(LifecycleSweepBuckets {
            lifecycle_buckets,
            aborting_buckets,
        })
    }

    pub fn list_all_objects_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let mut all_objects = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut start_after = None;
            loop {
                let pg = self.metadata_pg(pg_id)?;
                let resp = pg.list_objects(&ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    start_after: start_after.clone(),
                    start_at: None,
                    max_keys: INTERNAL_LIST_PAGE_SIZE,
                })?;
                drop(pg);
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
                let pg = self.metadata_pg(pg_id)?;
                let resp = pg.list_object_versions(&ListObjectVersionsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    key_marker: key_marker.clone(),
                    version_id_marker,
                    start_at: None,
                    max_keys: INTERNAL_LIST_PAGE_SIZE,
                })?;
                drop(pg);
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
                let pg = self.metadata_pg(pg_id)?;
                let resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    key_marker: key_marker.clone(),
                    upload_id_marker: upload_id_marker.clone(),
                    max_uploads: INTERNAL_LIST_PAGE_SIZE,
                })?;
                drop(pg);
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

    pub fn list_objects_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        record_cap: usize,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, ObjectPgActionError> {
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
            let mut all_objects: Vec<StoredObject> = Vec::new();
            let mut hit_record_cap = false;
            for pg_id in self.metadata_pg_ids() {
                if hit_record_cap {
                    break;
                }
                let pg = self.metadata_pg(pg_id)?;
                let resp = pg.list_objects(&ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    start_after: continuation_token.clone(),
                    start_at: None,
                    max_keys: fetch_limit,
                })?;
                all_objects.extend(resp.objects);
                if all_objects.len() >= record_cap {
                    all_objects.truncate(record_cap);
                    hit_record_cap = true;
                }
            }

            all_objects.sort_by(|a, b| a.key().cmp(b.key()));
            all_objects.dedup_by(|a, b| a.key() == b.key());

            let max = max_keys as usize;
            let mut objects = Vec::new();
            let mut next_continuation_token = None;
            for object in &all_objects {
                if objects.len() >= max {
                    break;
                }
                let object_key = object.key();
                objects.push(object.clone());
                next_continuation_token = Some(object_key.clone());
            }

            let is_truncated = hit_record_cap || all_objects.len() > max;
            return Ok(ListedBucketObjects {
                objects,
                common_prefixes: Vec::new(),
                is_truncated,
                next_continuation_token: if is_truncated {
                    next_continuation_token
                } else {
                    None
                },
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
            let pg = self.metadata_pg(cursor.pg_id)?;
            let resp = pg.list_objects(&ListObjectsReq {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                start_after,
                start_at,
                max_keys: fetch_limit,
            })?;
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

    pub fn list_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
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
            let pg = self.metadata_pg(cursor.pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                key_marker,
                version_id_marker,
                start_at,
                max_keys: fetch_limit,
            })?;
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

    pub fn load_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_object_if(bucket, key, version_id, action)
    }

    pub fn load_existing_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_existing_live_object(bucket, key)
    }

    pub fn load_object_read_snapshot_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_object_read_snapshot_if(bucket, key, version_id, snapshot_mode, action)
    }

    pub fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .payload_reclaim_exists(bucket, key, generation_id)
    }

    pub fn get_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<Option<String>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .get_object_tags_if(bucket, key, version_id, action)
    }

    fn new_put_object_metadata_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        object: LiveObjectRecord,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        Ok(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?,
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object,
            })),
        ))
    }

    fn put_object_metadata_command_from_stored(
        stored: &StoredObject,
        version_id: VersionId,
        mutation: PutObjectMetadataMutation,
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
        ))
    }

    fn put_object_metadata_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        requested_version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() {
                    if update.object.bucket == *bucket && update.object.key == *key {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored = match requested_version_id {
                            Some(version_id) => {
                                if version_id != update.object.version_id {
                                    drop(object_pg);
                                    self.apply_pending_object_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )?;
                                    self.remove_pending_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )
                                    .map_err(ObjectPgActionError::from)?;
                                    continue;
                                }
                                PgMetadataStore::get_object_version(
                                    &*object_pg,
                                    bucket,
                                    key,
                                    version_id,
                                )?
                            }
                            None => {
                                let stored =
                                    PgMetadataStore::get_object_meta(&*object_pg, bucket, key)?;
                                if stored.version_id() != update.object.version_id {
                                    drop(object_pg);
                                    self.apply_pending_object_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )?;
                                    continue;
                                }
                                stored
                            }
                        };
                        let (value, version_id, mutation) = match action(&stored) {
                            Ok(command) => command,
                            Err(error) => return Ok(Err(error)),
                        };
                        let expected = Self::put_object_metadata_command_from_stored(
                            &stored, version_id, mutation,
                        )?;
                        if update.as_ref() != &expected {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "conflicting pending command for object metadata update",
                            ));
                        }
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(value));
                    }
                }

                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let stored = match requested_version_id {
                Some(version_id) => {
                    PgMetadataStore::get_object_version(&*object_pg, bucket, key, version_id)?
                }
                None => PgMetadataStore::get_object_meta(&*object_pg, bucket, key)?,
            };
            let (value, version_id, mutation) = match action(&stored) {
                Ok(command) => command,
                Err(error) => return Ok(Err(error)),
            };
            let update =
                Self::put_object_metadata_command_from_stored(&stored, version_id, mutation)?;
            let command =
                match self.new_put_object_metadata_command(pg_id, &object_pg, update.object) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        drop(object_pg);
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
            drop(object_pg);
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(value));
        }
    }

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
                PutObjectMetadataMutation::PutTags(tags.to_string()),
            ))
        })
    }

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

    pub fn put_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        retention: ObjectRetention,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                (),
                version_id,
                PutObjectMetadataMutation::PutRetention(retention),
            ))
        })
    }

    pub fn put_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        legal_hold: StoredLegalHoldStatus,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                (),
                version_id,
                PutObjectMetadataMutation::PutLegalHold(legal_hold),
            ))
        })
    }

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
        self.object_metadata_primary_node(bucket, key)?
            .get_object_legal_hold_if(bucket, key, version_id, action)
    }

    pub fn get_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<Option<ObjectRetention>, E>,
    ) -> Result<Result<Option<ObjectRetention>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .get_object_retention_if(bucket, key, version_id, action)
    }

    fn load_bucket_lifecycle_context(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<BucketLifecycleContext<'_>>, ObjectPgActionError> {
        let bucket_node = self.bucket_metadata_primary_node(bucket)?;
        let bucket_guard = bucket_node.lock_bucket(bucket);
        let bucket_pg = bucket_node.get_pg(self.bucket_metadata_pg_id(bucket))?;
        let bucket_info = match PgMetadataStore::head_bucket(&*bucket_pg, bucket) {
            Ok(info) => info,
            Err(MetadataError::BucketNotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let raw_lifecycle = if bucket_info.bucket_lifecycle_present {
            PgMetadataStore::get_bucket_subresource(
                &*bucket_pg,
                bucket,
                BucketSubresourceKind::Lifecycle,
            )?
            .map(|stored| stored.body)
        } else {
            None
        };
        Ok(Some(BucketLifecycleContext {
            bucket_node,
            _bucket_guard: bucket_guard,
            bucket_info,
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
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_metadata_command_bucket_write_reservation(&command)
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
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        source,
                    } = error;
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
                    if applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                super::bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
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

    fn delete_command_target_from_stored(
        &self,
        object_pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        stored: Option<&StoredObject>,
    ) -> Result<Option<DeleteObjectVersionTarget>, ObjectPgActionError> {
        match stored {
            None => Ok(None),
            Some(StoredObject::DeleteMarker(_)) => {
                Ok(Some(DeleteObjectVersionTarget::DeleteMarker))
            }
            Some(StoredObject::Live(record)) => Ok(Some(
                self.live_delete_command_target(object_pg, bucket, key, record)?,
            )),
        }
    }

    fn live_delete_command_target(
        &self,
        object_pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &LiveObjectRecord,
    ) -> Result<DeleteObjectVersionTarget, ObjectPgActionError> {
        let payload = Self::snapshot_live_object_payload_reclaim_command(
            object_pg,
            bucket,
            key,
            record,
            crate::clock::current_time_millis(),
        )?;
        Ok(DeleteObjectVersionTarget::Live {
            generation_id: record.generation_id,
            layout: record.layout,
            payload,
        })
    }

    fn new_delete_object_version_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        target: DeleteObjectVersionTarget,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        Ok(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                target,
            })),
        ))
    }

    fn new_insert_delete_marker_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        draft: InsertDeleteMarkerDraft<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        Ok(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?,
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: draft.bucket.clone(),
                key: draft.key.clone(),
                version_id: draft.version_id,
                owner: draft.owner.clone(),
                write_sequence: object_pg
                    .next_object_write_sequence(draft.bucket.as_str(), draft.key.as_str())?,
                last_modified_millis: crate::clock::current_time_millis(),
                stale_payload: draft.stale_payload,
            }),
        ))
    }

    fn deleted_specific_from_command_target(
        target: &DeleteObjectVersionTarget,
    ) -> DeletedSpecificObjectVersion {
        match target {
            DeleteObjectVersionTarget::DeleteMarker => DeletedSpecificObjectVersion::DeleteMarker,
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
            DeleteObjectVersionTarget::DeleteMarker => DeletedCurrentObject::DeleteMarker,
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

    pub fn delete_specific_object_version_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, version_id) {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored = match PgMetadataStore::get_object_version(
                            &*object_pg,
                            bucket,
                            key,
                            version_id,
                        ) {
                            Ok(stored) => Some(stored),
                            Err(MetadataError::ObjectNotFound) => None,
                            Err(error) => return Err(error.into()),
                        };
                        let value = match action(stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                            value,
                            deleted: Self::deleted_specific_from_command_target(&delete.target),
                        }));
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let stored =
                match PgMetadataStore::get_object_version(&*object_pg, bucket, key, version_id) {
                    Ok(stored) => Some(stored),
                    Err(MetadataError::ObjectNotFound) => None,
                    Err(error) => return Err(error.into()),
                };
            let value = match action(stored.as_ref()) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            let Some(target) =
                self.delete_command_target_from_stored(&object_pg, bucket, key, stored.as_ref())?
            else {
                return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                    value,
                    deleted: DeletedSpecificObjectVersion::Missing,
                }));
            };
            let command = self.new_delete_object_version_command(
                pg_id, &object_pg, bucket, key, version_id, target,
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    drop(object_pg);
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            drop(object_pg);
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
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

    pub fn delete_current_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored =
                            match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                                Ok(stored) => Some(stored),
                                Err(MetadataError::ObjectNotFound) => None,
                                Err(error) => return Err(error.into()),
                            };
                        if stored
                            .as_ref()
                            .is_some_and(|stored| stored.version_id() == delete.version_id)
                        {
                            let value = match action(stored.as_ref()) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            drop(object_pg);
                            self.apply_pending_object_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )?;
                            return Ok(Ok(DeleteCurrentObjectOutcome {
                                value,
                                deleted: Self::deleted_current_from_command_target(&delete.target),
                            }));
                        }
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let stored = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(stored) => Some(stored),
                Err(MetadataError::ObjectNotFound) => None,
                Err(error) => return Err(error.into()),
            };
            let value = match action(stored.as_ref()) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            let Some(stored) = stored.as_ref() else {
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::Missing,
                }));
            };
            let StoredObject::Live(record) = stored else {
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::DeleteMarker,
                }));
            };
            let target = self.live_delete_command_target(&object_pg, bucket, key, record)?;
            let command = self.new_delete_object_version_command(
                pg_id,
                &object_pg,
                bucket,
                key,
                record.version_id,
                target,
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    drop(object_pg);
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            drop(object_pg);
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
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

    pub fn insert_current_delete_marker_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        owner: OwnerIdentity,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() {
                    if marker.matches_request(bucket, key) {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored =
                            match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                                Ok(stored) => Some(stored),
                                Err(MetadataError::ObjectNotFound) => None,
                                Err(error) => return Err(error.into()),
                            };
                        let value = match action(stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                            value,
                            version_id: marker.version_id,
                        }));
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let stored = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(stored) => Some(stored),
                Err(MetadataError::ObjectNotFound) => None,
                Err(error) => return Err(error.into()),
            };
            let value = match action(stored.as_ref()) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            drop(object_pg);
            let marker_vid = self.reserve_next_object_version(pg_id, bucket, key, primary_node)?;
            let object_pg = primary_node.get_pg(pg_id.get())?;
            let command = self.new_insert_delete_marker_command(
                pg_id,
                &object_pg,
                InsertDeleteMarkerDraft {
                    bucket,
                    key,
                    version_id: marker_vid,
                    owner: &owner,
                    stale_payload: None,
                },
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    drop(object_pg);
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            drop(object_pg);
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                value,
                version_id: marker_vid,
            }));
        }
    }

    pub fn expire_current_object_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        mut should_expire: impl FnMut(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        let Some(lifecycle_context) = self.load_bucket_lifecycle_context(bucket)? else {
            return Ok(Ok(None));
        };
        let BucketLifecycleContext {
            bucket_node: lifecycle_bucket_node,
            _bucket_guard: _lifecycle_bucket_guard,
            bucket_info,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(None));
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _object_bucket_guard = (!std::ptr::eq(lifecycle_bucket_node, primary_node))
            .then(|| primary_node.lock_bucket(bucket));
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
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored = match PgMetadataStore::get_object_version(
                            &*object_pg,
                            bucket,
                            key,
                            expected_version_id,
                        ) {
                            Ok(stored) => stored,
                            Err(MetadataError::ObjectNotFound) => {
                                drop(object_pg);
                                self.apply_pending_object_metadata_command_for_bucket(
                                    pg_id, bucket, &command,
                                )?;
                                return Ok(Ok(None));
                            }
                            Err(error) => return Err(error.into()),
                        };
                        let StoredObject::Live(record) = stored else {
                            drop(object_pg);
                            self.apply_pending_object_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )?;
                            return Ok(Ok(None));
                        };
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
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
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored =
                            match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                                Ok(stored) => stored,
                                Err(MetadataError::ObjectNotFound) => {
                                    drop(object_pg);
                                    self.apply_pending_object_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )?;
                                    return Ok(Ok(None));
                                }
                                Err(error) => return Err(error.into()),
                            };
                        let StoredObject::Live(record) = stored else {
                            drop(object_pg);
                            self.apply_pending_object_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )?;
                            return Ok(Ok(None));
                        };
                        if record.version_id != expected_version_id {
                            drop(object_pg);
                            self.apply_pending_object_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )?;
                            return Ok(Ok(None));
                        }
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        let reclaim_generation_id =
                            super::object_payload_reclaim_generation(&marker.stale_payload);
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(due.then_some(ExpireCurrentObjectOutcome {
                            reclaim_generation_id,
                        })));
                    }
                    _ => {
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        continue;
                    }
                }
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let stored = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(stored) => stored,
                Err(MetadataError::ObjectNotFound) => return Ok(Ok(None)),
                Err(error) => return Err(error.into()),
            };
            let StoredObject::Live(record) = stored else {
                return Ok(Ok(None));
            };
            if record.version_id != expected_version_id {
                return Ok(Ok(None));
            }
            let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                Ok(due) => due,
                Err(error) => return Ok(Err(error)),
            };
            if !due {
                return Ok(Ok(None));
            }

            let (command, reclaim_generation_id) = match bucket_info.versioning {
                BucketVersioningState::Disabled => {
                    let target =
                        self.live_delete_command_target(&object_pg, bucket, key, &record)?;
                    let reclaim_generation_id =
                        super::delete_object_version_reclaim_generation(&target);
                    let command = self.new_delete_object_version_command(
                        pg_id,
                        &object_pg,
                        bucket,
                        key,
                        record.version_id,
                        target,
                    );
                    let command = match command {
                        Ok(command) => command,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    drop(object_pg);
                    (command, reclaim_generation_id)
                }
                BucketVersioningState::Enabled => {
                    drop(object_pg);
                    let marker_vid =
                        self.reserve_next_object_version(pg_id, bucket, key, primary_node)?;
                    let object_pg = primary_node.get_pg(pg_id.get())?;
                    let command = self.new_insert_delete_marker_command(
                        pg_id,
                        &object_pg,
                        InsertDeleteMarkerDraft {
                            bucket,
                            key,
                            version_id: marker_vid,
                            owner: &owner,
                            stale_payload: None,
                        },
                    );
                    let command = match command {
                        Ok(command) => command,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    drop(object_pg);
                    (command, None)
                }
                BucketVersioningState::Suspended => {
                    let stale_payload = self.snapshot_direct_put_stale_payload_command(
                        &object_pg,
                        bucket,
                        key,
                        crate::clock::current_time_millis(),
                    )?;
                    let reclaim_generation_id =
                        super::object_payload_reclaim_generation(&stale_payload);
                    let command = self.new_insert_delete_marker_command(
                        pg_id,
                        &object_pg,
                        InsertDeleteMarkerDraft {
                            bucket,
                            key,
                            version_id: VersionId::Null,
                            owner: &owner,
                            stale_payload,
                        },
                    );
                    let command = match command {
                        Ok(command) => command,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    drop(object_pg);
                    (command, reclaim_generation_id)
                }
            };
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
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
        mut select_versions: impl FnMut(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        let Some(lifecycle_context) = self.load_bucket_lifecycle_context(bucket)? else {
            return Ok(Ok(Vec::new()));
        };
        let BucketLifecycleContext {
            bucket_node: lifecycle_bucket_node,
            _bucket_guard: _lifecycle_bucket_guard,
            bucket_info: _bucket_info,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(Vec::new()));
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _object_bucket_guard = (!std::ptr::eq(lifecycle_bucket_node, primary_node))
            .then(|| primary_node.lock_bucket(bucket));
        let mut completed_reclaimed_generation_ids = Vec::new();

        'retry: loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let versions = match PgMetadataStore::list_object_versions_for_key(
                            &*object_pg,
                            bucket,
                            key,
                        ) {
                            Ok(versions) => versions,
                            Err(MetadataError::ObjectNotFound) => {
                                drop(object_pg);
                                self.apply_pending_object_metadata_command_for_bucket(
                                    pg_id, bucket, &command,
                                )?;
                                return Ok(Ok(Vec::new()));
                            }
                            Err(error) => return Err(error.into()),
                        };
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
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(reclaim_generation_id.into_iter().collect()));
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let versions =
                match PgMetadataStore::list_object_versions_for_key(&*object_pg, bucket, key) {
                    Ok(versions) => versions,
                    Err(MetadataError::ObjectNotFound) => return Ok(Ok(Vec::new())),
                    Err(error) => return Err(error.into()),
                };
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
                let target = self.live_delete_command_target(&object_pg, bucket, key, record)?;
                let reclaim_generation_id =
                    super::delete_object_version_reclaim_generation(&target);
                delete_targets.push((record.version_id, target, reclaim_generation_id));
            }
            drop(object_pg);

            for (version_id, target, reclaim_generation_id) in delete_targets {
                let command_id = match self.next_object_metadata_command_id(pg_id) {
                    Ok(command_id) => command_id,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                        continue 'retry;
                    }
                    Err(error) => return Err(error),
                };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::DeleteObjectVersion(Box::new(
                        DeleteObjectVersionCommand {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            version_id,
                            target,
                        },
                    )),
                );
                match self
                    .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
                {
                    super::SnapshotSensitiveCommandInstall::Installed => {}
                    super::SnapshotSensitiveCommandInstall::ContenderDrained => continue 'retry,
                }
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                if let Some(generation_id) = reclaim_generation_id {
                    completed_reclaimed_generation_ids.push(generation_id);
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
        mut should_delete: impl FnMut(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let Some(lifecycle_context) = self.load_bucket_lifecycle_context(bucket)? else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_node: lifecycle_bucket_node,
            _bucket_guard: _lifecycle_bucket_guard,
            bucket_info: _bucket_info,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _object_bucket_guard = (!std::ptr::eq(lifecycle_bucket_node, primary_node))
            .then(|| primary_node.lock_bucket(bucket));

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, expected_version_id)
                        && matches!(delete.target, DeleteObjectVersionTarget::DeleteMarker)
                    {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let versions = match PgMetadataStore::list_object_versions_for_key(
                            &*object_pg,
                            bucket,
                            key,
                        ) {
                            Ok(versions) => versions,
                            Err(MetadataError::ObjectNotFound) => {
                                drop(object_pg);
                                self.apply_pending_object_metadata_command_for_bucket(
                                    pg_id, bucket, &command,
                                )?;
                                return Ok(Ok(false));
                            }
                            Err(error) => return Err(error.into()),
                        };
                        let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        drop(object_pg);
                        self.apply_pending_object_metadata_command_for_bucket(
                            pg_id, bucket, &command,
                        )?;
                        return Ok(Ok(due));
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let versions =
                match PgMetadataStore::list_object_versions_for_key(&*object_pg, bucket, key) {
                    Ok(versions) => versions,
                    Err(MetadataError::ObjectNotFound) => return Ok(Ok(false)),
                    Err(error) => return Err(error.into()),
                };
            let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                Ok(due) => due,
                Err(error) => return Ok(Err(error)),
            };
            if !due {
                return Ok(Ok(false));
            }
            let command = self.new_delete_object_version_command(
                pg_id,
                &object_pg,
                bucket,
                key,
                expected_version_id,
                DeleteObjectVersionTarget::DeleteMarker,
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    drop(object_pg);
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            drop(object_pg);
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
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
        self.object_metadata_primary_node(bucket, key)?;
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
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
        if !runtime_state.acquire_object_payload_lease(bucket, key, generation_id) {
            return Err(StoreError::NotFound);
        }
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
        ))
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
            .runtime_state()
            .object_payload_lease_count(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map
            .runtime_state()
            .bucket_object_payload_lease_count(bucket)
    }

    pub fn enqueue_object_payload_reclaim(
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
            .enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    pub fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map
            .runtime_state()
            .enqueue_bucket_delete_finalize(bucket);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map.runtime_state().try_take_reclaim_work()
    }

    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map.runtime_state().wait_for_reclaim_work(stop)
    }

    pub fn wake_reclaim_workers(&self) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map.runtime_state().wake_reclaim_workers();
    }

    pub fn reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let node = self.object_metadata_primary_node(bucket, key)?;
        let runtime_state = self.local_map.runtime_state();
        if runtime_state.object_payload_lease_count(bucket, key, generation_id) != 0 {
            return Ok(false);
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_reclaim_delete = matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                    if delete.matches_request(bucket, key, generation_id)
            );
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            if matching_reclaim_delete {
                runtime_state.clear_object_payload_reclaim_fence(bucket, key, generation_id);
                return Ok(true);
            }
        }

        let reclaim = {
            let meta_pg = node.get_pg(pg_id.get())?;
            if runtime_state.object_payload_lease_count(bucket, key, generation_id) != 0 {
                return Ok(false);
            }

            if let Some(reclaim) =
                PgMetadataStore::get_object_segments_reclaim(&*meta_pg, bucket, key, generation_id)?
            {
                Some(ObjectPayloadReclaimCommand::Segments(reclaim))
            } else {
                PgMetadataStore::get_multipart_reclaim(&*meta_pg, bucket, key, generation_id)?
                    .map(ObjectPayloadReclaimCommand::Multipart)
            }
        };

        let Some(reclaim) = reclaim else {
            return Ok(false);
        };

        if !runtime_state.try_begin_object_payload_reclaim(bucket, key, generation_id) {
            return Ok(false);
        }

        let mut payload_delete_started = false;
        let result = (|| -> Result<bool, ObjectPgActionError> {
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
                        match part {
                            MultipartReclaimPartRecord::ShardSet {
                                part_okh,
                                part_vid,
                                data_pg_id,
                                ec,
                                ..
                            } => {
                                payload_delete_started = true;
                                self.delete_payload_shard_set(
                                    *data_pg_id,
                                    *ec,
                                    part_okh,
                                    *part_vid,
                                )?;
                            }
                            MultipartReclaimPartRecord::Segments { segments, .. } => {
                                for segment in segments {
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
                }
            }

            let _bucket_guard = node.lock_bucket(bucket);
            loop {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let matching_reclaim_delete = matches!(
                        command.payload(),
                        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                            if delete.matches_request(bucket, key, generation_id)
                    );
                    self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                    if matching_reclaim_delete {
                        return Ok(true);
                    }
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
                        ),
                    )),
                );
                self.set_pending_metadata_command_for_bucket(
                    pg_id,
                    bucket,
                    &command,
                    "conflicting pending command for payload reclaim metadata delete",
                )?;
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                return Ok(true);
            }
        })();
        let keep_reclaim_fence = result.is_err() && payload_delete_started;
        runtime_state.finish_object_payload_reclaim(bucket, key, generation_id, keep_reclaim_fence);
        result
    }

    pub(super) fn delete_complete_multipart_cleanup_best_effort(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        cleanup: &CompleteMultipartCommitCleanup,
    ) {
        for part in &cleanup.omitted_parts {
            if part.part_okh == [0u8; 16] {
                continue;
            }
            let data_pg_id = self
                .metadata_primary_topology_node()
                .pg_topology()
                .object_generation_multipart_part_data_pg(
                    bucket,
                    key,
                    generation_id,
                    part.part_number,
                )
                .get();
            self.delete_multipart_shard_set_best_effort(
                data_pg_id,
                &part.part_okh,
                part.part_vid,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
        self.delete_multipart_part_segments_best_effort(&cleanup.omitted_streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    pub(super) fn delete_finalize_upload_part_cleanup_best_effort(
        &self,
        cleanup: &FinalizeStreamPartCleanup,
    ) {
        if let Some(part) = cleanup
            .existing_part
            .as_ref()
            .filter(|part| part.part_okh != [0u8; 16])
        {
            let data_pg_id = self
                .metadata_primary_topology_node()
                .pg_topology()
                .object_generation_multipart_part_data_pg(
                    &cleanup.upload.bucket,
                    &cleanup.upload.key,
                    cleanup.upload.object_generation_id,
                    part.part_number,
                )
                .get();
            self.delete_multipart_shard_set_best_effort(
                data_pg_id,
                &part.part_okh,
                part.part_vid,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
        self.delete_multipart_part_segments_best_effort(&cleanup.displaced_segments);
    }

    pub(super) fn delete_abort_multipart_cleanup_best_effort(
        &self,
        cleanup: &AbortMultipartUploadCleanup,
    ) {
        for part in &cleanup.parts {
            if part.part_okh == [0u8; 16] {
                continue;
            }
            let data_pg_id = self
                .metadata_primary_topology_node()
                .pg_topology()
                .object_generation_multipart_part_data_pg(
                    &cleanup.upload.bucket,
                    &cleanup.upload.key,
                    cleanup.upload.object_generation_id,
                    part.part_number,
                )
                .get();
            self.delete_multipart_shard_set_best_effort(
                data_pg_id,
                &part.part_okh,
                part.part_vid,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
        self.delete_multipart_part_segments_best_effort(&cleanup.streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    fn delete_multipart_part_segments_best_effort(&self, segments: &[MultipartPartSegmentRecord]) {
        for segment in segments {
            self.delete_multipart_shard_set_best_effort(
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
        data_pg_id: u32,
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        self.delete_payload_shard_set_best_effort(data_pg_id, ec, okh, generation_id);
    }

    pub fn create_put_object_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        enum Attempt<T> {
            Complete(T),
            Retry,
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        loop {
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            let reservation = match self.acquire_durable_bucket_write_reservation(
                bucket,
                "put-object-stream-create",
                Some(key.as_str()),
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
                let bucket_pg = reservation.node.get_pg(reservation.pg_id)?;
                let snapshot = crate::SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket_pg, bucket, request,
                )?;
                drop(bucket_pg);

                let primary_node = self.object_metadata_primary_node(bucket, key)?;
                let object_pg = primary_node.get_pg(pg_id.get())?;
                let existing_object =
                    match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                        Ok(StoredObject::Live(record)) => Some(StoredObject::Live(record)),
                        Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                            None
                        }
                        Err(error) => return Err(error.into()),
                    };
                drop(object_pg);

                let (value, create) = match action(snapshot, existing_object) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                if self
                    .matching_stream_upload_exists(
                        pg_id,
                        &create,
                        super::applied_stream_create_command(&applied_commands, &create),
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    return Ok(Ok(Attempt::Complete(value)));
                }
                self.reserve_put_object_generation(bucket, key, &create.session_id)
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;

                #[cfg(test)]
                maybe_run_before_stream_put_create_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                let command_id = match self.next_object_metadata_command_id(pg_id) {
                    Ok(command_id) => command_id,
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
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                            create.clone(),
                            crate::clock::current_time_millis(),
                            Some(proof.clone()),
                        ),
                    )),
                );
                #[cfg(test)]
                maybe_run_before_stream_put_create_pending_install_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if !self
                    .try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
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
        mut action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        let (command, new_pending_command, prepared) = loop {
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let is_matching_stream_commit = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.matches_request(
                            bucket,
                            key,
                            session_id,
                            commit.object.generation_id,
                        )
                );
                if is_matching_stream_commit {
                    break;
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }

            let pending_command = self
                .pending_metadata_command_for_bucket(pg_id, bucket)?
                .filter(|command| {
                    matches!(
                        command.payload(),
                        MetadataCommandPayload::CommitDirectPutObject(commit)
                            if commit.matches_request(
                                bucket,
                                key,
                                session_id,
                                commit.object.generation_id,
                            )
                    )
                });

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let session = match object_pg.get_stream_upload(session_id) {
                Ok(session) => session,
                Err(MetadataError::StreamSessionNotFound { .. })
                    if let Some(command) = pending_command.clone() =>
                {
                    drop(object_pg);
                    self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if session.state != StreamUploadState::InProgress {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "stream session is not in progress".to_string(),
                });
            }
            if session.bucket != bucket.as_str() || session.key != key.as_str() {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "session bucket/key mismatch".to_string(),
                });
            }
            if !matches!(session.target, StreamUploadTarget::PutObject) {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "session is not a PutObject session".to_string(),
                });
            }
            let existing_etag = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                Err(MetadataError::ObjectNotFound) => None,
                Err(other) => return Err(other.into()),
            };
            let staging_segments = object_pg.list_stream_segments(session_id)?;
            let prepared = match action(StreamPutFinalizeSnapshot {
                session,
                existing_etag,
            }) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };
            let segments_total: u64 = staging_segments.iter().map(|segment| segment.size).sum();
            if segments_total != total_size {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: format!(
                        "total_size mismatch: caller passed {total_size} but staged segments sum to {segments_total}"
                    ),
                });
            }

            let (command, new_pending_command) = match pending_command {
                Some(command) => {
                    drop(object_pg);
                    (command, false)
                }
                None => {
                    let (version_id, object_pg) =
                        if prepared.versioning == BucketVersioningState::Enabled {
                            drop(object_pg);
                            let version_id =
                                self.reserve_next_object_version(pg_id, bucket, key, primary_node)?;
                            (version_id, primary_node.get_pg(pg_id.get())?)
                        } else {
                            (VersionId::Null, object_pg)
                        };
                    let generation_id =
                        object_pg.get_object_generation_reservation(bucket, key, session_id)?;
                    let last_modified_millis = crate::clock::current_time_millis();
                    let write_sequence =
                        object_pg.next_object_write_sequence(bucket.as_str(), key.as_str())?;
                    let stale_payload = if version_id.is_null() {
                        self.snapshot_direct_put_stale_payload_command(
                            &object_pg,
                            bucket,
                            key,
                            last_modified_millis,
                        )?
                    } else {
                        None
                    };
                    let committed_segments: Vec<ObjectSegmentRecord> = staging_segments
                        .iter()
                        .map(|segment| ObjectSegmentRecord {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            version_id,
                            segment_index: segment.segment_index,
                            size: segment.size,
                            segment_crc64: segment.segment_crc64,
                            segment_okh: segment.segment_okh,
                            segment_vid: segment.segment_vid,
                            data_pg_id: segment.data_pg_id,
                            ec_k: segment.ec_k,
                            ec_m: segment.ec_m,
                        })
                        .collect();
                    let object = PutLiveObjectReq {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id,
                        owner: prepared.owner.clone(),
                        acl_grants: prepared.acl_grants.clone(),
                        public_read: prepared.public_read,
                        generation_id,
                        size: prepared.size,
                        etag: ObjectEtag::single_part(prepared.etag_crc64),
                        ec: staging_segments.first().map_or(
                            self.default_payload_ec_shape(),
                            |segment| EcShape {
                                k: segment.ec_k,
                                m: segment.ec_m,
                            },
                        ),
                        layout: ObjectLayout::Standard,
                        tags: prepared.tags.clone(),
                        metadata_blob: Some(prepared.metadata_blob.clone()),
                        system_metadata_blob: Some(prepared.system_metadata_blob.clone()),
                        object_lock: prepared.object_lock,
                        encryption: prepared.encryption.clone(),
                    };
                    #[cfg(test)]
                    maybe_run_before_stream_put_finalize_command_id_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    );
                    let command_id = match self
                        .next_object_metadata_command_id_from_locked_pg(pg_id, &object_pg)
                    {
                        Ok(command_id) => command_id,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let command = MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::CommitDirectPutObject(Box::new(
                            CommitDirectPutObjectCommand {
                                object,
                                segments: committed_segments,
                                generation_reservation_id: session_id.clone(),
                                write_sequence,
                                last_modified_millis,
                                stale_payload,
                            },
                        )),
                    );
                    drop(object_pg);
                    if !self
                        .try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                    {
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
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        }

        let object_pg = primary_node.get_pg(pg_id.get())?;
        let stored = PgMetadataStore::get_object_meta(&*object_pg, bucket, key)?;
        let live_record = stored.as_live().ok_or_else(|| MetadataError::Db {
            context: "stored object missing live record after stream put",
            source: rusqlite::Error::QueryReturnedNoRows,
        })?;
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            unreachable!("stream put commit pending command kind changed");
        };
        Ok(Ok(FinalizeStreamPutOutcome {
            value: prepared.value,
            version_id: commit.object.version_id,
            encryption: commit.object.encryption.clone(),
            live_tags: live_record.tags.clone(),
            live_size: live_record.size,
            live_last_modified: live_record.last_modified,
            stale_generation_id: super::object_payload_reclaim_generation(&commit.stale_payload),
        }))
    }

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
        enum Attempt<T> {
            Complete(CreateMultipartUploadOutcome<T>),
            Retry,
        }

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        loop {
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            let attempt =
                self.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
                    let primary_node = self.object_metadata_primary_node(bucket, key)?;
                    let object_pg = primary_node.get_pg(pg_id.get())?;
                    let existing_object =
                        match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                            Ok(StoredObject::Live(record)) => Some(StoredObject::Live(record)),
                            Ok(StoredObject::DeleteMarker(_))
                            | Err(MetadataError::ObjectNotFound) => None,
                            Err(error) => return Err(error.into()),
                        };
                    drop(object_pg);

                    let (value, create) = match action(snapshot, existing_object) {
                        Ok(prepared) => prepared,
                        Err(error) => return Ok(Err(error)),
                    };
                    if let Some(initiated_at) = self
                        .matching_multipart_upload_initiated_at(
                            pg_id,
                            &create,
                            super::applied_multipart_create_command(&applied_commands, &create),
                        )
                        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                    {
                        return Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                            value,
                            initiated_at,
                        })));
                    }

                    let object_generation_id = {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        PgMetadataStore::next_generation_id(&*object_pg, bucket, key)?
                    };
                    let command_id = match self.next_object_metadata_command_id(pg_id) {
                        Ok(command_id) => command_id,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
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
                    let command = MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::CreateMultipartUpload(Box::new(
                            CreateMultipartUploadCommand::from_request(
                                create.clone(),
                                object_generation_id,
                                crate::clock::current_time_millis(),
                            ),
                        )),
                    );
                    match self
                        .install_snapshot_sensitive_metadata_command_or_drain(
                            pg_id, bucket, &command,
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
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }

                    let initiated_at = {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        PgMetadataStore::get_multipart_upload(&*object_pg, &create.upload_id)?
                            .initiated_at
                    };
                    Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                        value,
                        initiated_at,
                    })))
                })?;
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
        self.object_metadata_primary_node(bucket, key)?
            .load_multipart_upload(bucket, key, upload_id)
    }

    pub fn begin_upload_part_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
        session_id: &SessionId,
        mut action: impl FnMut(
            &MultipartUploadRecord,
        ) -> Result<(AuthorizedMultipartUploadRecord, T), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        loop {
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            let object_pg = primary_node.get_pg(pg_id.get())?;
            let upload = PgMetadataStore::get_multipart_upload(&*object_pg, upload_id)?;
            if upload.bucket != *bucket
                || upload.key != *key
                || upload.state != UploadState::InProgress
            {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    },
                ));
            }
            let (authorized_upload, result) = match action(&upload) {
                Ok((authorized_upload, result)) => (authorized_upload, result),
                Err(error) => return Ok(Err(error)),
            };
            if authorized_upload.record() != &upload {
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
            drop(object_pg);
            if self
                .matching_stream_upload_exists(
                    pg_id,
                    &create,
                    super::applied_stream_create_command(&applied_commands, &create),
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            {
                return Ok(Ok(result));
            }
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::CreateStreamUpload(Box::new(
                    CreateStreamUploadCommand::from_request(
                        create,
                        crate::clock::current_time_millis(),
                    ),
                )),
            );
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            return Ok(Ok(result));
        }
    }

    pub fn create_upload_part_stream_session(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
    ) -> Result<SessionId, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        loop {
            let applied_commands =
                self.drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)?;
            let object_pg = primary_node.get_pg(pg_id.get())?;
            let upload = PgMetadataStore::get_multipart_upload(&*object_pg, upload_id)?;
            if upload != *authorized_upload.record() || upload.state != UploadState::InProgress {
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
                encryption: upload.encryption,
            };
            drop(object_pg);
            if self.matching_stream_upload_exists(
                pg_id,
                &create,
                super::applied_stream_create_command(&applied_commands, &create),
            )? {
                return Ok(session_id.clone());
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
                MetadataCommandPayload::CreateStreamUpload(Box::new(
                    CreateStreamUploadCommand::from_request(
                        create,
                        crate::clock::current_time_millis(),
                    ),
                )),
            );
            match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command)?
            {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => continue,
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
        self.object_metadata_primary_node(bucket, key)?
            .load_in_progress_multipart_upload(bucket, key, upload_id)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartUploadRecord>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .try_load_in_progress_multipart_upload(bucket, key, upload_id)
    }

    pub fn load_in_progress_multipart_upload_for_listing(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_in_progress_multipart_upload_for_listing(bucket, key, upload_id)
    }

    pub fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        self.object_metadata_primary_node(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?
        .load_multipart_completion_snapshot(authorized_upload, requested_part_numbers)
    }

    pub fn load_multipart_completion_preflight(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        self.object_metadata_primary_node(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?
        .load_multipart_completion_preflight(authorized_upload)
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

    fn snapshot_completed_multipart_stale_payload(
        object_pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<CompletedMultipartStalePayload>, ObjectPgActionError> {
        let stored =
            match PgMetadataStore::get_object_version(object_pg, bucket, key, VersionId::Null) {
                Ok(stored) => stored,
                Err(MetadataError::ObjectNotFound) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
        let StoredObject::Live(record) = stored else {
            return Ok(None);
        };

        match record.layout {
            ObjectLayout::Standard => {
                let segments =
                    PgMetadataStore::get_object_segments(object_pg, bucket, key, VersionId::Null)?;
                Ok(Some(CompletedMultipartStalePayload::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
            ObjectLayout::MultipartManifest { .. } => {
                let parts =
                    PgMetadataStore::get_object_parts(object_pg, bucket, key, VersionId::Null)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                            object_pg,
                            bucket,
                            key,
                            VersionId::Null,
                            part.part_number,
                        )?);
                    }
                }
                Ok(Some(CompletedMultipartStalePayload::Multipart {
                    generation_id: record.generation_id,
                    parts,
                    streaming_segments,
                }))
            }
        }
    }

    fn completed_multipart_stale_payload_to_reclaim_command(
        bucket: &BucketName,
        key: &ObjectKey,
        created_at: u64,
        payload: &CompletedMultipartStalePayload,
    ) -> ObjectPayloadReclaimCommand {
        match payload {
            CompletedMultipartStalePayload::Segments {
                generation_id,
                segments,
            } => ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id: *generation_id,
                created_at,
                segments: segments
                    .iter()
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
            }),
            CompletedMultipartStalePayload::Multipart {
                generation_id,
                parts,
                streaming_segments,
            } => ObjectPayloadReclaimCommand::Multipart(Self::multipart_reclaim_from_parts(
                bucket,
                key,
                *generation_id,
                created_at,
                parts,
                streaming_segments,
            )),
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

    fn snapshot_upload_part_stream_cleanup(
        object_pg: &crate::PgStore,
        upload_id: &UploadId,
    ) -> Result<(Vec<StreamUploadRecord>, Vec<StreamUploadSegmentRecord>), ObjectPgActionError>
    {
        let mut stream_uploads = PgMetadataStore::list_all_stream_uploads(object_pg)?
            .into_iter()
            .filter(|session| {
                matches!(
                    &session.target,
                    StreamUploadTarget::UploadPart {
                        upload_id: session_upload_id,
                        ..
                    } if session_upload_id == upload_id
                )
            })
            .collect::<Vec<_>>();
        stream_uploads.sort_by(|a, b| a.session_id.as_str().cmp(b.session_id.as_str()));

        let mut stream_upload_segments = Vec::new();
        for session in &stream_uploads {
            stream_upload_segments.extend(PgMetadataStore::list_stream_segments(
                object_pg,
                &session.session_id,
            )?);
        }
        Ok((stream_uploads, stream_upload_segments))
    }

    fn reserve_completed_multipart_upload_order(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ObjectPgActionError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                {
                    continue;
                }
                if let MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance) =
                    command.payload()
                {
                    let completion_order = advance.completion_order;
                    match self
                        .finish_pending_metadata_command_to_acting_set(
                            pg_id, bucket, &command, false,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        super::PendingMetadataCommandOutcome::Applied => {
                            return Ok(completion_order);
                        }
                        super::PendingMetadataCommandOutcome::Abandoned
                        | super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            continue;
                        }
                    }
                }
                match self
                    .finish_pending_command_for_completed_multipart_order(pg_id, bucket, &command)?
                {
                    super::PendingMetadataCommandOutcome::Applied => continue,
                    super::PendingMetadataCommandOutcome::Abandoned
                    | super::PendingMetadataCommandOutcome::RetryPartialExactConflict => continue,
                }
            }

            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let current_order = bucket_pg.completed_multipart_upload_sequence_for_bucket(bucket)?;
            let completion_order =
                current_order
                    .checked_add(1)
                    .ok_or_else(|| MetadataError::Db {
                        context: "reserve completed multipart upload order overflow",
                        source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                            "completed multipart upload sequence overflow",
                        )),
                    })?;
            i64::try_from(completion_order).map_err(|_| MetadataError::Db {
                context: "reserve completed multipart upload order overflow",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                    "completed multipart upload sequence exceeds SQLite integer range",
                )),
            })?;
            drop(bucket_pg);

            #[cfg(test)]
            maybe_run_before_completed_multipart_order_command_id_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            let Some(command_id) = self
                .next_bucket_metadata_command_id_or_drain(pg_id, bucket)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            else {
                continue;
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                    AdvanceCompletedMultipartUploadSequenceCommand {
                        bucket: bucket.clone(),
                        completion_order,
                    },
                ),
            );
            if self
                .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                .map_err(ObjectPgActionError::from)?
                .is_none()
            {
                continue;
            }
            match self.finish_pending_metadata_command_to_acting_set(pg_id, bucket, &command, true)
            {
                Ok(super::PendingMetadataCommandOutcome::Applied) => return Ok(completion_order),
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
    pub(crate) fn test_reserve_completed_multipart_upload_order(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ObjectPgActionError> {
        self.reserve_completed_multipart_upload_order(bucket)
    }

    fn apply_multipart_completion_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        match self.apply_metadata_command_to_acting_set(command) {
            Ok(()) => {
                self.release_metadata_command_bucket_write_reservation(command)
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                self.remove_pending_metadata_command_for_bucket(pg_id, bucket, command)
                    .map_err(ObjectPgActionError::from)?;
                self.after_object_metadata_command_applied(command);
                Ok(())
            }
            Err(error) => Err(super::bucket_snapshot_error_to_object_pg_action_error(
                error.source,
            )),
        }
    }

    pub fn complete_multipart_upload_commit_serialized(
        &self,
        req: CompleteMultipartCommitRequest,
        keep_completed_uploads: usize,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let bucket = req.bucket.clone();
        let key = req.key.clone();
        let upload_id = req.upload_id.clone();
        let generation_id = req.generation_id;
        let pg_id = PgId::new(self.object_metadata_pg_id(&bucket, &key));
        let bucket_primary_node = self.bucket_metadata_primary_node(&bucket)?;
        let _completion_guard = bucket_primary_node.lock_multipart_completion_bucket(&bucket);
        let primary_node = self.object_metadata_primary_node(&bucket, &key)?;
        let _bucket_guard = primary_node.lock_bucket(&bucket);

        'retry_after_pending_conflict: loop {
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, &bucket)? {
                if let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() {
                    if commit.matches_request(
                        &bucket,
                        &key,
                        &upload_id,
                        generation_id,
                        &req.part_records,
                    ) {
                        let outcome = Self::complete_multipart_outcome_from_command(commit);
                        self.apply_multipart_completion_command(pg_id, &bucket, &command)?;
                        self.prune_completed_multipart_uploads_for_bucket_with_limit(
                            &bucket,
                            keep_completed_uploads,
                        )?;
                        return Ok(outcome);
                    }
                }
                self.apply_pending_object_metadata_command_for_bucket(pg_id, &bucket, &command)?;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let upload = PgMetadataStore::get_multipart_upload(&*object_pg, &upload_id)?;
            if upload.bucket != bucket
                || upload.key != key
                || upload.state != UploadState::InProgress
            {
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into());
            }
            if upload.object_generation_id != generation_id {
                return Err(MetadataError::Db {
                    context: "complete multipart command generation mismatch",
                    source: rusqlite::Error::InvalidQuery,
                }
                .into());
            }
            if req.part_records.is_empty() {
                return Err(MetadataError::Db {
                    context: "complete multipart command empty parts",
                    source: rusqlite::Error::InvalidQuery,
                }
                .into());
            }

            let (version_id, object_pg) = if req.versioning == BucketVersioningState::Enabled {
                drop(object_pg);
                let version_id =
                    self.reserve_next_object_version(pg_id, &bucket, &key, primary_node)?;
                (version_id, primary_node.get_pg(pg_id.get())?)
            } else {
                (VersionId::Null, object_pg)
            };
            let stale_payload = if version_id.is_null() {
                Self::snapshot_completed_multipart_stale_payload(&object_pg, &bucket, &key)?
            } else {
                None
            };
            let parts_count =
                std::num::NonZeroU32::new(u32::try_from(req.part_records.len()).map_err(|_| {
                    MetadataError::Db {
                        context: "complete multipart command too many parts",
                        source: rusqlite::Error::InvalidQuery,
                    }
                })?)
                .ok_or_else(|| MetadataError::Db {
                    context: "complete multipart command empty parts",
                    source: rusqlite::Error::InvalidQuery,
                })?;
            let object_parts: Vec<ObjectPartRecord> = req
                .part_records
                .iter()
                .map(|part| {
                    let data_pg_id = self
                        .metadata_primary_topology_node()
                        .pg_topology()
                        .object_generation_multipart_part_data_pg(
                            &bucket,
                            &key,
                            generation_id,
                            part.part_number,
                        )
                        .get();
                    ObjectPartRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id,
                        part_number: part.part_number,
                        size: part.size,
                        etag: part.etag.clone(),
                        etag_kind: part.etag_kind,
                        part_okh: part.part_okh,
                        part_vid: part.part_vid,
                        ec_k: part.ec_k,
                        ec_m: part.ec_m,
                        data_pg_id,
                        checksum: part.checksum.clone(),
                    }
                })
                .collect();
            let selected_part_numbers: std::collections::BTreeSet<u32> = req
                .part_records
                .iter()
                .map(|part| part.part_number)
                .collect();
            let all_parts = PgMetadataStore::list_multipart_parts(
                &*object_pg,
                &ListPartsReq {
                    upload_id: upload_id.clone(),
                    part_number_marker: None,
                    max_parts: u32::MAX,
                },
            )?
            .parts;
            let omitted_parts = all_parts
                .into_iter()
                .filter(|part| !selected_part_numbers.contains(&part.part_number))
                .collect::<Vec<_>>();
            let all_streaming_segments =
                PgMetadataStore::get_all_multipart_part_segments_for_upload(
                    &*object_pg,
                    &upload_id,
                )?;
            let mut selected_streaming_segments = Vec::new();
            let mut omitted_streaming_segments = Vec::new();
            for mut segment in all_streaming_segments {
                if selected_part_numbers.contains(&segment.part_number) {
                    segment.version_id = version_id.to_u64();
                    selected_streaming_segments.push(segment);
                } else {
                    omitted_streaming_segments.push(segment);
                }
            }
            let (stream_uploads, stream_upload_segments) =
                Self::snapshot_upload_part_stream_cleanup(&object_pg, &upload_id)?;
            let stream_uploads = stream_uploads
                .iter()
                .map(TerminalStreamCleanupRecord::from)
                .collect();

            let last_modified_millis = crate::clock::current_time_millis();
            let completed_at_millis = last_modified_millis;
            let write_sequence =
                object_pg.next_object_write_sequence(bucket.as_str(), key.as_str())?;
            let stale_payload_command = stale_payload.as_ref().map(|payload| {
                Self::completed_multipart_stale_payload_to_reclaim_command(
                    &bucket,
                    &key,
                    last_modified_millis,
                    payload,
                )
            });
            drop(object_pg);
            let completion_order = self.reserve_completed_multipart_upload_order(&bucket)?;
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => return Err(error),
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::CommitMultipartObject(Box::new(
                    CommitMultipartObjectCommand {
                        upload_id: upload_id.clone(),
                        object: PutLiveObjectReq {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            version_id,
                            owner: req.owner.clone(),
                            acl_grants: req.acl_grants.clone(),
                            public_read: req.public_read,
                            generation_id,
                            size: req.size,
                            etag: ObjectEtag::MultipartComposite {
                                crc64: req.etag_crc64,
                                parts: parts_count,
                            },
                            ec: EcShape { k: 0, m: 0 },
                            layout: ObjectLayout::MultipartManifest { parts_count },
                            tags: req.tags.clone(),
                            metadata_blob: req.metadata_blob.clone(),
                            system_metadata_blob: req.system_metadata_blob.clone(),
                            object_lock: req.object_lock,
                            encryption: req.encryption.clone(),
                        },
                        parts: object_parts,
                        selected_streaming_segments,
                        omitted_parts,
                        omitted_streaming_segments,
                        stream_uploads,
                        stream_upload_segments,
                        write_sequence,
                        completion_order,
                        completed_at_millis,
                        initiator: upload.initiator.clone(),
                        last_modified_millis,
                        stale_payload: stale_payload_command,
                    },
                )),
            );
            // A matching completion contender carries the exact outcome this caller must return.
            // Let the retry loop observe it instead of draining it generically and losing that
            // request-shaped result.
            if !self.try_install_pending_metadata_command_for_bucket(pg_id, &bucket, &command)? {
                continue 'retry_after_pending_conflict;
            }
            self.apply_multipart_completion_command(pg_id, &bucket, &command)?;

            let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
                unreachable!("new complete multipart command changed payload kind");
            };
            self.prune_completed_multipart_uploads_for_bucket_with_limit(
                &bucket,
                keep_completed_uploads,
            )?;
            return Ok(Self::complete_multipart_outcome_from_command(commit));
        }
    }

    fn validate_upload_part_stream_session(
        session: &StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        if session.state != StreamUploadState::InProgress {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != *bucket || session.key != *key {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id: sess_upload_id,
                part_number: sess_part_number,
            } if sess_upload_id == upload_id && *sess_part_number == part_number => Ok(()),
            StreamUploadTarget::UploadPart { .. } => Err(ObjectPgActionError::InvalidRequest {
                reason: "session upload_id/part_number mismatch".to_string(),
            }),
            StreamUploadTarget::PutObject => Err(ObjectPgActionError::InvalidRequest {
                reason: "session is not an UploadPart session".to_string(),
            }),
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

    pub fn finalize_upload_part_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        mut action: impl FnMut(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);

        loop {
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
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            let session = match object_pg.get_stream_upload(session_id) {
                Ok(session) => session,
                Err(MetadataError::StreamSessionNotFound { .. })
                    if let Some(command) = pending_command.clone() =>
                {
                    drop(object_pg);
                    self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                    self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                        .map_err(ObjectPgActionError::from)?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            Self::validate_upload_part_stream_session(
                &session,
                bucket,
                key,
                upload_id,
                part_number,
            )?;
            let upload = PgMetadataStore::get_multipart_upload(&*object_pg, upload_id)?;
            if upload.bucket != *bucket
                || upload.key != *key
                || upload.state != UploadState::InProgress
            {
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into());
            }
            let existing_part =
                match PgMetadataStore::get_multipart_part(&*object_pg, upload_id, part_number) {
                    Ok(existing) => Some(existing),
                    Err(MetadataError::PartNotFound { .. }) => None,
                    Err(other) => return Err(other.into()),
                };
            let existing_part_generation = existing_part.as_ref().map(|part| part.generation);
            let staging_segments = object_pg.list_stream_segments(session_id)?;
            let displaced_segments = PgMetadataStore::get_all_multipart_part_segments_for_upload(
                &*object_pg,
                upload_id,
            )?
            .into_iter()
            .filter(|segment| segment.part_number == part_number)
            .collect::<Vec<_>>();

            let prepared = match action(StreamUploadPartSnapshot {
                session,
                upload: upload.clone(),
                existing_part_generation,
                staging_segments,
            }) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };

            let command_payload = CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                upload,
                part: prepared.part.clone(),
                segments: prepared.segments.clone(),
                existing_part,
                displaced_segments,
            };
            let command_is_pending = pending_command.is_some();
            let command = if let Some(command) = pending_command {
                let MetadataCommandPayload::CommitStreamPart(pending) = command.payload() else {
                    unreachable!("filtered pending command changed kind");
                };
                if !Self::commit_stream_part_commands_match_retry(pending, &command_payload) {
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: "pending stream part commit does not match retry".to_string(),
                    });
                }
                drop(object_pg);
                command
            } else {
                let command_id =
                    match self.next_object_metadata_command_id_from_locked_pg(pg_id, &object_pg) {
                        Ok(command_id) => command_id,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::CommitStreamPart(Box::new(command_payload)),
                );
                drop(object_pg);
                if !self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)? {
                    continue;
                }
                command
            };

            let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
                unreachable!("stream part pending command kind changed");
            };
            let last_modified = commit.part.last_modified;
            if command_is_pending {
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            } else {
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }
            return Ok(Ok(FinalizeStreamPartOutcome {
                value: prepared.value,
                last_modified,
            }));
        }
    }

    pub fn list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&UploadId>,
        record_cap: usize,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, ObjectPgActionError> {
        let mut uploads = Vec::new();
        let mut hit_record_cap = false;
        for pg_id in self.metadata_pg_ids() {
            if hit_record_cap {
                break;
            }
            let pg = self.metadata_pg(pg_id)?;
            let resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: prefix.cloned(),
                key_marker: key_marker.cloned(),
                upload_id_marker: upload_id_marker.cloned(),
                max_uploads: max_uploads.saturating_add(1),
            })?;
            uploads.extend(resp.uploads);
            if uploads.len() >= record_cap {
                uploads.truncate(record_cap);
                hit_record_cap = true;
            }
        }
        Ok(ListedBucketMultipartUploads {
            uploads,
            hit_record_cap,
        })
    }

    pub fn list_multipart_parts_for_authorized_upload(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        self.object_metadata_primary_node(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?
        .list_multipart_parts_for_authorized_upload(
            authorized_upload,
            part_number_marker,
            max_parts,
        )
    }

    pub fn lookup_multipart_upload_management(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .lookup_multipart_upload_management(bucket, key, upload_id)
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        self.abort_multipart_upload_locked(pg_id, bucket, key, upload_id)
    }

    fn abort_multipart_upload_locked(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        'retry_after_pending_conflict: loop {
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let matching_abort = matches!(
                    command.payload(),
                    MetadataCommandPayload::AbortMultipartUpload(abort)
                        if abort.bucket == *bucket
                            && abort.key == *key
                            && abort.upload_id == *upload_id
                );
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                if matching_abort {
                    return Ok(true);
                }
            }

            let command =
                match self.prepare_abort_multipart_upload_command(pg_id, bucket, key, upload_id) {
                    Ok(Some(command)) => command,
                    Ok(None) => return Ok(false),
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                        continue 'retry_after_pending_conflict;
                    }
                    Err(error) => return Err(error),
                };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            if !self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)? {
                continue 'retry_after_pending_conflict;
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(true);
        }
    }

    pub fn abort_authorized_multipart_upload(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<bool, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        self.abort_authorized_multipart_upload_locked(pg_id, authorized_upload)
    }

    fn abort_authorized_multipart_upload_locked(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<bool, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        'retry_after_pending_conflict: loop {
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let matching_abort = matches!(
                    command.payload(),
                    MetadataCommandPayload::AbortMultipartUpload(abort)
                        if abort.bucket == *bucket
                            && abort.key == *key
                            && abort.upload_id == *upload_id
                );
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                if matching_abort {
                    return Ok(true);
                }
            }

            let command = match self
                .prepare_authorized_abort_multipart_upload_command(pg_id, authorized_upload)
            {
                Ok(Some(command)) => command,
                Ok(None) => return Ok(false),
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => return Err(error),
            };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            if !self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)? {
                continue 'retry_after_pending_conflict;
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(true);
        }
    }

    fn prepare_abort_multipart_upload_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let cleanup = {
            let object_pg = primary_node.get_pg(pg_id.get())?;
            object_pg.prepare_abort_multipart_upload_cleanup(bucket, key, upload_id)?
        };
        let cleanup = match cleanup {
            Some(cleanup) => cleanup,
            None => return Ok(None),
        };

        Ok(Some(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id(pg_id)?,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                cleanup,
            })),
        )))
    }

    fn prepare_authorized_abort_multipart_upload_command(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let primary_node = self.object_metadata_primary_node(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?;
        let cleanup = {
            let object_pg = primary_node.get_pg(pg_id.get())?;
            object_pg.prepare_authorized_abort_multipart_upload_cleanup(authorized_upload)?
        };
        let cleanup = match cleanup {
            Some(cleanup) => cleanup,
            None => return Ok(None),
        };

        Ok(Some(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id(pg_id)?,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: authorized_upload.record().bucket.clone(),
                key: authorized_upload.record().key.clone(),
                upload_id: authorized_upload.record().upload_id.clone(),
                cleanup,
            })),
        )))
    }

    pub fn abort_multipart_upload_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        should_abort: impl FnOnce(Option<&str>, &MultipartUploadRecord) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let Some(lifecycle_context) = self.load_bucket_lifecycle_context(bucket)? else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_node: lifecycle_bucket_node,
            _bucket_guard: _lifecycle_bucket_guard,
            raw_lifecycle,
            ..
        } = lifecycle_context;

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _object_bucket_guard = (!std::ptr::eq(lifecycle_bucket_node, primary_node))
            .then(|| primary_node.lock_bucket(bucket));
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_abort = matches!(
                command.payload(),
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.bucket == *bucket
                        && abort.key == *key
                        && abort.upload_id == *upload_id
            );
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            if matching_abort {
                return Ok(Ok(true));
            }
        }

        let upload = {
            let object_pg = primary_node.get_pg(pg_id.get())?;
            match PgMetadataStore::get_multipart_upload(&*object_pg, upload_id) {
                Ok(upload) => {
                    if upload.bucket != *bucket || upload.key != *key {
                        return Ok(Ok(false));
                    }
                    upload
                }
                Err(MetadataError::NoSuchUpload { .. }) => return Ok(Ok(false)),
                Err(error) => return Err(error.into()),
            }
        };

        if upload.state == UploadState::Aborting {
            return self
                .abort_multipart_upload_locked(pg_id, bucket, key, upload_id)
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

        self.abort_multipart_upload_locked(pg_id, bucket, key, upload_id)
            .map(Ok)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_ec_scratch_allocation_count(&self, shape: EcShape) -> usize {
        self.metadata_primary_topology_node()
            .test_ec_scratch_allocation_count(shape)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.metadata_primary_topology_node()
            .test_bucket_pg_id_for(bucket)
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
    pub fn test_object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.metadata_primary_topology_node()
            .test_object_pg_id_for(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> u32 {
        self.metadata_primary_topology_node()
            .test_data_pg_id_for(bucket, key, generation_id)
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
        self.metadata_primary_topology_node()
            .test_multipart_part_data_pg_id_for(bucket, key, object_generation_id, part_number)
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
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_segments_reclaim(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            pg.put_object_segments_reclaim(reclaim)?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            pg.put_multipart_reclaim(reclaim)?;
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
    pub fn test_list_bucket_payload_reclaim_roots(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<PayloadReclaimRoot>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_bucket_payload_reclaim_roots(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_became_noncurrent_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
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
        };
        let _ = self
            .create_bucket_with_config_and_load_info(&create)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        self.begin_bucket_delete(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_delete_bucket_metadata(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        match self.delete_bucket_from_acting_set(pg_id, bucket)? {
            BucketDeleteFinalizeOutcome::Finalized => Ok(()),
            BucketDeleteFinalizeOutcome::NotFound => {
                Err(crate::error::MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into())
            }
            BucketDeleteFinalizeOutcome::NotDeleting | BucketDeleteFinalizeOutcome::Pending => {
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
            node.storage_node()
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
            node.storage_node()
                .test_force_stream_upload_created_at(bucket, key, session_id, created_at)?;
            let pg = node.storage_node().get_pg(pg_id.get())?;
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

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_lock_bucket(&self, bucket: &BucketName) -> crate::node::BucketLockGuard<'_> {
        self.metadata_primary_test_hook_node().lock_bucket(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_lock_multipart_completion_bucket(
        &self,
        bucket: &BucketName,
    ) -> crate::node::BucketLockGuard<'_> {
        self.metadata_primary_test_hook_node()
            .lock_multipart_completion_bucket(bucket)
    }
}
