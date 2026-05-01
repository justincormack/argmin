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
    AbortMultipartUploadCommand, BucketPropertyMutation, BucketSubresourceMutation,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload,
    ObjectPayloadReclaimCommand, PutBucketAclCommand, PutBucketPropertyCommand,
    PutBucketSubresourceCommand, PutBucketVersioningCommand, PutObjectMetadataCommand,
    PutObjectMetadataMutation,
};
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
static BEFORE_METADATA_COMMAND_APPLY_HOOK: OnceLock<Mutex<Option<MetadataCommandApplyTestHook>>> =
    OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOK: OnceLock<
    Mutex<Option<MetadataCommandApplyContextTestHook>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) struct MetadataCommandApplyTestHookGuard;

#[cfg(test)]
impl Drop for MetadataCommandApplyTestHookGuard {
    fn drop(&mut self) {
        let hook = BEFORE_METADATA_COMMAND_APPLY_HOOK.get_or_init(|| Mutex::new(None));
        *hook.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for MetadataCommandApplyContextTestHookGuard {
    fn drop(&mut self) {
        let hook = BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOK.get_or_init(|| Mutex::new(None));
        *hook.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

fn maybe_run_before_metadata_command_apply_hook(
    _node_id: NodeId,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let hook = BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook(metadata_command_apply_test_context(_node_id, _command))?;
        }
    }
    #[cfg(test)]
    {
        let hook = BEFORE_METADATA_COMMAND_APPLY_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook(_node_id, _command)?;
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn metadata_command_apply_test_context(
    node_id: NodeId,
    command: &MetadataCommandEnvelope,
) -> MetadataCommandApplyTestContext {
    let (kind, bucket, key) = match command.payload() {
        MetadataCommandPayload::CreateBucket(command) => (
            MetadataCommandApplyTestKind::CreateBucket,
            Some(command.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketVersioning(command) => (
            MetadataCommandApplyTestKind::PutBucketVersioning,
            Some(command.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketAcl(command) => (
            MetadataCommandApplyTestKind::PutBucketAcl,
            Some(command.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketProperty(command) => (
            MetadataCommandApplyTestKind::PutBucketProperty,
            Some(command.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketSubresource(command) => (
            MetadataCommandApplyTestKind::PutBucketSubresource,
            Some(command.name.clone()),
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
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::CreateStreamUpload(command) => (
            MetadataCommandApplyTestKind::CreateStreamUpload,
            Some(command.request.bucket.clone()),
            Some(command.request.key.clone()),
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
            Some(command.request.bucket.clone()),
            Some(command.request.key.clone()),
        ),
        MetadataCommandPayload::AbortMultipartUpload(command) => (
            MetadataCommandApplyTestKind::AbortMultipartUpload,
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
    versions: Vec<StoredObject>,
    next_index: usize,
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

pub(super) struct MetadataCommandApplyFailure {
    pub(super) applied_nodes: usize,
    pub(super) source: BucketSnapshotLoadError,
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
        let slot = BEFORE_METADATA_COMMAND_APPLY_HOOK.get_or_init(|| Mutex::new(None));
        *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(hook);
        MetadataCommandApplyTestHookGuard
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_metadata_command_apply_context_hook(
        &self,
        hook: MetadataCommandApplyContextTestHook,
    ) -> MetadataCommandApplyContextTestHookGuard {
        let slot = BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOK.get_or_init(|| Mutex::new(None));
        *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(hook);
        MetadataCommandApplyContextTestHookGuard { _private: () }
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
        let runtime_state = self.local_map.runtime_state();
        let (command, clear_pending_on_zero_apply) = if let Some(command) =
            runtime_state.pending_metadata_command_for_bucket(pg_id, &bucket)
        {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create) if create.matches_config(config) => {
                    (command, false)
                }
                MetadataCommandPayload::CreateBucket(_) => {
                    return Err(MetadataError::BucketAlreadyExists.into());
                }
                MetadataCommandPayload::PutBucketVersioning(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket versioning command for create bucket",
                    ));
                }
                MetadataCommandPayload::PutBucketAcl(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket acl command for create bucket",
                    ));
                }
                MetadataCommandPayload::PutBucketProperty(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket property command for create bucket",
                    ));
                }
                MetadataCommandPayload::PutBucketSubresource(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket subresource command for create bucket",
                    ));
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::ReleaseObjectGeneration(_)
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
                | MetadataCommandPayload::AbortMultipartUpload(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending object command for create bucket",
                    ));
                }
            }
        } else {
            let command_id = MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let bucket_execution_generation = bucket_pg.reserve_bucket_execution_generation()?;
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
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                &bucket,
                &command,
                "conflicting pending command for create bucket",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            (command, true)
        };
        self.apply_pending_metadata_command_to_acting_set(
            pg_id,
            &bucket,
            &command,
            clear_pending_on_zero_apply,
        )?;

        let bucket_pg = primary_node.get_pg(pg_id.get())?;
        let info = PgMetadataStore::head_bucket(&*bucket_pg, &bucket)?;
        self.local_map
            .runtime_state()
            .remove_pending_metadata_command_for_bucket(pg_id, &bucket);
        Ok(BucketCreateAttemptOutcome::Created(info))
    }

    pub(super) fn apply_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
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
            maybe_run_before_metadata_command_apply_hook(node.node_id(), command).map_err(
                |source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                },
            )?;
            if let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() {
                let bucket_pg_id = node
                    .storage_node()
                    .pg_topology()
                    .bucket_pg_for(&commit.object.bucket);
                let bucket_pg = node.storage_node().get_pg(bucket_pg_id).map_err(|source| {
                    MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    }
                })?;
                bucket_pg
                    .advance_completed_multipart_upload_sequence_for_bucket(
                        &commit.object.bucket,
                        commit.completion_order,
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    })?;
            }
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                }
            })?;
            pg.apply_metadata_command(command)
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                })?;
        }
        Ok(())
    }

    fn apply_pending_metadata_command_to_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<(), BucketSnapshotLoadError> {
        match self.apply_metadata_command_to_acting_set(command) {
            Ok(()) => Ok(()),
            Err(error) => {
                if clear_pending_on_zero_apply && error.applied_nodes == 0 {
                    self.local_map
                        .runtime_state()
                        .remove_pending_metadata_command_for_bucket(pg_id, bucket);
                }
                Err(error.source)
            }
        }
    }

    fn delete_bucket_from_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
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
            let pg = node.storage_node().get_pg(pg_id.get())?;
            match PgMetadataStore::delete_bucket(&*pg, bucket) {
                Ok(()) => {}
                Err(crate::error::MetadataError::BucketNotFound { .. })
                    if node.node_id() == primary_node_id =>
                {
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {}
                Err(other) => return Err(other.into()),
            }
        }
        self.local_map
            .runtime_state()
            .clear_pending_metadata_command_for_bucket(pg_id, bucket);
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
        self.bucket_metadata_primary_node(bucket)?
            .with_bucket_write_snapshot(bucket, request, action)
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
        let node = self.bucket_metadata_primary_node(bucket)?;
        let drain = node.begin_bucket_write_drain(bucket)?;
        crate::node::maybe_run_after_begin_bucket_delete_drain_hook(bucket);

        if self.bucket_has_visible_data(bucket, true)? {
            return Err(crate::error::MetadataError::BucketNotEmpty.into());
        }

        node.mark_bucket_deleting(bucket)?;
        drain.persist();
        Ok(())
    }

    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = bucket_node.lock_bucket(bucket);
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        {
            let bucket_pg = self.metadata_pg(bucket_pg_id)?;
            let info = match PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket) {
                Ok(info) => info,
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(other) => return Err(other.into()),
            };
            if info.state != BucketState::Deleting {
                return Ok(BucketDeleteFinalizeOutcome::NotDeleting);
            }
        }

        if self.bucket_has_visible_data(bucket, false)? {
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
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            PgMetadataStore::delete_completed_multipart_uploads_for_bucket(&*pg, bucket)?;
        }
        Ok(())
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

        let runtime_state = self.local_map.runtime_state();
        let (command, clear_pending_on_zero_apply) = if let Some(command) =
            runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
        {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.matches_request(bucket, state) =>
                {
                    (command, false)
                }
                MetadataCommandPayload::PutBucketVersioning(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "conflicting pending put bucket versioning command",
                    ));
                }
                MetadataCommandPayload::CreateBucket(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending create bucket command for versioning",
                    ));
                }
                MetadataCommandPayload::PutBucketAcl(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket acl command for versioning",
                    ));
                }
                MetadataCommandPayload::PutBucketProperty(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket property command for versioning",
                    ));
                }
                MetadataCommandPayload::PutBucketSubresource(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket subresource command for versioning",
                    ));
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::ReleaseObjectGeneration(_)
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
                | MetadataCommandPayload::AbortMultipartUpload(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending object command for versioning",
                    ));
                }
            }
        } else {
            let command_id = MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let bucket_execution_generation = bucket_pg.reserve_bucket_execution_generation()?;
            drop(bucket_pg);
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::new(
                    bucket.clone(),
                    state,
                    bucket_execution_generation,
                )),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for bucket versioning",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            (command, true)
        };
        self.apply_pending_metadata_command_to_acting_set(
            pg_id,
            bucket,
            &command,
            clear_pending_on_zero_apply,
        )?;

        let bucket_pg = primary_node.get_pg(pg_id.get())?;
        let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        self.local_map
            .runtime_state()
            .remove_pending_metadata_command_for_bucket(pg_id, bucket);
        Ok(info)
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

        let runtime_state = self.local_map.runtime_state();
        let (command, clear_pending_on_zero_apply) = if let Some(command) =
            runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
        {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.matches_request(bucket, acl_grants, public_read, public_write) =>
                {
                    (command, false)
                }
                MetadataCommandPayload::PutBucketAcl(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "conflicting pending put bucket acl command",
                    ));
                }
                MetadataCommandPayload::CreateBucket(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending create bucket command for bucket acl",
                    ));
                }
                MetadataCommandPayload::PutBucketVersioning(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket versioning command for bucket acl",
                    ));
                }
                MetadataCommandPayload::PutBucketProperty(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket property command for bucket acl",
                    ));
                }
                MetadataCommandPayload::PutBucketSubresource(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket subresource command for bucket acl",
                    ));
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::ReleaseObjectGeneration(_)
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
                | MetadataCommandPayload::AbortMultipartUpload(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending object command for bucket acl",
                    ));
                }
            }
        } else {
            let command_id = MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let bucket_execution_generation = bucket_pg.reserve_bucket_execution_generation()?;
            drop(bucket_pg);
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::new(
                    bucket.clone(),
                    acl_grants.clone(),
                    public_read,
                    public_write,
                    bucket_execution_generation,
                )),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for bucket ACL",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            (command, true)
        };
        self.apply_pending_metadata_command_to_acting_set(
            pg_id,
            bucket,
            &command,
            clear_pending_on_zero_apply,
        )?;

        let bucket_pg = primary_node.get_pg(pg_id.get())?;
        let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        self.local_map
            .runtime_state()
            .remove_pending_metadata_command_for_bucket(pg_id, bucket);
        Ok(info)
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

        let runtime_state = self.local_map.runtime_state();
        let (command, clear_pending_on_zero_apply) = if let Some(command) =
            runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
        {
            match command.payload() {
                MetadataCommandPayload::PutBucketProperty(property)
                    if property.matches_request(bucket, &mutation) =>
                {
                    (command, false)
                }
                MetadataCommandPayload::PutBucketProperty(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "conflicting pending put bucket property command",
                    ));
                }
                MetadataCommandPayload::CreateBucket(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending create bucket command for bucket property",
                    ));
                }
                MetadataCommandPayload::PutBucketVersioning(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket versioning command for bucket property",
                    ));
                }
                MetadataCommandPayload::PutBucketAcl(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket acl command for bucket property",
                    ));
                }
                MetadataCommandPayload::PutBucketSubresource(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket subresource command for bucket property",
                    ));
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::ReleaseObjectGeneration(_)
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
                | MetadataCommandPayload::AbortMultipartUpload(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending object command for bucket property",
                    ));
                }
            }
        } else {
            let command_id = MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let bucket_execution_generation = bucket_pg.reserve_bucket_execution_generation()?;
            drop(bucket_pg);
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::PutBucketProperty(PutBucketPropertyCommand::new(
                    bucket.clone(),
                    mutation,
                    bucket_execution_generation,
                )),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for bucket property",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            (command, true)
        };
        self.apply_pending_metadata_command_to_acting_set(
            pg_id,
            bucket,
            &command,
            clear_pending_on_zero_apply,
        )?;

        let bucket_pg = primary_node.get_pg(pg_id.get())?;
        let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        self.local_map
            .runtime_state()
            .remove_pending_metadata_command_for_bucket(pg_id, bucket);
        Ok(info)
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
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let primary_node = self.bucket_metadata_primary_node(bucket)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        {
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        }
        let runtime_state = self.local_map.runtime_state();
        let (command, clear_pending_on_zero_apply) = if let Some(command) =
            runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
        {
            match command.payload() {
                MetadataCommandPayload::PutBucketSubresource(subresource)
                    if subresource.matches_request(bucket, &mutation) =>
                {
                    (command, false)
                }
                MetadataCommandPayload::PutBucketSubresource(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "conflicting pending put bucket subresource command",
                    ));
                }
                MetadataCommandPayload::CreateBucket(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending create bucket command for bucket subresource",
                    ));
                }
                MetadataCommandPayload::PutBucketVersioning(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket versioning command for bucket subresource",
                    ));
                }
                MetadataCommandPayload::PutBucketAcl(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket acl command for bucket subresource",
                    ));
                }
                MetadataCommandPayload::PutBucketProperty(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending put bucket property command for bucket subresource",
                    ));
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::ReleaseObjectGeneration(_)
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
                | MetadataCommandPayload::AbortMultipartUpload(_) => {
                    return Err(conflicting_pending_metadata_command(
                        "unexpected pending object command for bucket subresource",
                    ));
                }
            }
        } else {
            let command_id = MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            let bucket_pg = primary_node.get_pg(pg_id.get())?;
            let bucket_execution_generation = bucket_pg.reserve_bucket_execution_generation()?;
            drop(bucket_pg);
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                    bucket.clone(),
                    mutation,
                    bucket_execution_generation,
                )),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for bucket subresource",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            (command, true)
        };
        self.apply_pending_metadata_command_to_acting_set(
            pg_id,
            bucket,
            &command,
            clear_pending_on_zero_apply,
        )?;

        let bucket_pg = primary_node.get_pg(pg_id.get())?;
        let info = PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket)?;
        self.local_map
            .runtime_state()
            .remove_pending_metadata_command_for_bucket(pg_id, bucket);
        Ok(info)
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

        let mut uploads: Vec<(u32, UploadId, u64)> = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            let local = pg.list_completed_multipart_uploads_for_bucket(bucket.as_str())?;
            uploads.extend(
                local
                    .into_iter()
                    .map(|(upload_id, completion_order)| (pg_id, upload_id, completion_order)),
            );
        }

        uploads.sort_by_key(|entry| std::cmp::Reverse(entry.2));
        for (pg_id, upload_id, _) in uploads.into_iter().skip(keep) {
            let pg = self.metadata_pg(pg_id)?;
            pg.delete_completed_multipart_upload(&upload_id)?;
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
                versions,
                next_index: 0,
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
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        if max_keys == 0 {
            return Ok(ListedBucketObjectVersions {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        }

        let fetch_limit = max_keys.saturating_add(1);
        let prefix = prefix.cloned();
        let key_marker = key_marker.cloned();
        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let pg = self.metadata_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                key_marker: key_marker.clone(),
                version_id_marker,
                max_keys: fetch_limit,
            })?;
            cursors.push(VersionCursor {
                versions: resp.versions,
                next_index: 0,
            });
        }

        let max = max_keys as usize;
        let mut merged_versions = Vec::with_capacity(max.saturating_add(1));
        while merged_versions.len() <= max {
            let Some((cursor_index, _)) = cursors
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
            else {
                break;
            };
            merged_versions.push(cursors[cursor_index].pop_current());
        }

        let is_truncated = merged_versions.len() > max;
        let versions: Vec<StoredObject> = merged_versions.into_iter().take(max).collect();
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            if let Some(last) = versions.last() {
                (Some(last.key().clone()), Some(last.version_id()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(ListedBucketObjectVersions {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
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
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        mutation: PutObjectMetadataMutation,
    ) -> MetadataCommandEnvelope {
        let command_id = MetadataCommandId::new(
            self.operation_epoch(),
            pg_id,
            self.local_map
                .runtime_state()
                .next_metadata_command_log_index(pg_id),
        );
        MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                mutation,
            })),
        )
    }

    fn put_object_metadata_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        requested_version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();
        let mut action = Some(action);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
                if let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() {
                    if update.bucket == *bucket && update.key == *key {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored = match requested_version_id {
                            Some(version_id) => {
                                if version_id != update.version_id {
                                    drop(object_pg);
                                    self.apply_pending_object_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )?;
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
                                if stored.version_id() != update.version_id {
                                    drop(object_pg);
                                    self.apply_pending_object_metadata_command_for_bucket(
                                        pg_id, bucket, &command,
                                    )?;
                                    continue;
                                }
                                stored
                            }
                        };
                        let (value, version_id, mutation) =
                            match action.take().expect("object metadata action used once")(&stored)
                            {
                                Ok(command) => command,
                                Err(error) => return Ok(Err(error)),
                            };
                        if !update.matches_request(bucket, key, version_id, &mutation) {
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
            let (value, version_id, mutation) =
                match action.take().expect("object metadata action used once")(&stored) {
                    Ok(command) => command,
                    Err(error) => return Ok(Err(error)),
                };
            let command =
                self.new_put_object_metadata_command(pg_id, bucket, key, version_id, mutation);
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for object metadata mutation",
            )?;
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
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
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
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
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
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
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
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
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
        action: impl FnOnce(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
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
        match self.apply_metadata_command_to_acting_set(command) {
            Ok(()) => {
                self.local_map
                    .runtime_state()
                    .remove_pending_metadata_command_for_bucket(pg_id, bucket);
                self.after_object_metadata_command_applied(command);
                Ok(())
            }
            Err(error) => {
                if error.applied_nodes == 0 {
                    self.local_map
                        .runtime_state()
                        .remove_pending_metadata_command_for_bucket(pg_id, bucket);
                }
                Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error.source,
                ))
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
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        target: DeleteObjectVersionTarget,
    ) -> MetadataCommandEnvelope {
        let command_id = MetadataCommandId::new(
            self.operation_epoch(),
            pg_id,
            self.local_map
                .runtime_state()
                .next_metadata_command_log_index(pg_id),
        );
        MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                target,
            })),
        )
    }

    fn new_insert_delete_marker_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        draft: InsertDeleteMarkerDraft<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let command_id = MetadataCommandId::new(
            self.operation_epoch(),
            pg_id,
            self.local_map
                .runtime_state()
                .next_metadata_command_log_index(pg_id),
        );
        Ok(MetadataCommandEnvelope::new(
            command_id,
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
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();
        let mut action = Some(action);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
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
                        let value = match action.take().expect("delete action used once")(
                            stored.as_ref(),
                        ) {
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
            let value = match action.take().expect("delete action used once")(stored.as_ref()) {
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
            let command =
                self.new_delete_object_version_command(pg_id, bucket, key, version_id, target);
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for object version delete",
            )?;
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
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();
        let mut action = Some(action);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
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
                            let value = match action.take().expect("delete action used once")(
                                stored.as_ref(),
                            ) {
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
            let value = match action.take().expect("delete action used once")(stored.as_ref()) {
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
                bucket,
                key,
                record.version_id,
                target,
            );
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for current object delete",
            )?;
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
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();
        let mut action = Some(action);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
                if let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() {
                    if marker.matches_request(bucket, key) {
                        let object_pg = primary_node.get_pg(pg_id.get())?;
                        let stored =
                            match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                                Ok(stored) => Some(stored),
                                Err(MetadataError::ObjectNotFound) => None,
                                Err(error) => return Err(error.into()),
                            };
                        let value = match action.take().expect("delete marker action used once")(
                            stored.as_ref(),
                        ) {
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
            let value =
                match action.take().expect("delete marker action used once")(stored.as_ref()) {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                };
            let marker_vid = PgMetadataStore::next_version_id(&*object_pg, bucket, key)?;
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
            )?;
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for delete marker insertion",
            )?;
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
        should_expire: impl FnOnce(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
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
        let runtime_state = self.local_map.runtime_state();
        let owner = OwnerIdentity::new(
            bucket_info.owner_principal.clone(),
            bucket_info.owner_canonical_id.clone(),
        );
        let mut should_expire = Some(should_expire);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
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
                        let due = match should_expire
                            .take()
                            .expect("current expiration predicate used once")(
                            raw_lifecycle.as_deref(),
                            &record,
                        ) {
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
                        let due = match should_expire
                            .take()
                            .expect("current expiration predicate used once")(
                            raw_lifecycle.as_deref(),
                            &record,
                        ) {
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
            let due = match should_expire
                .take()
                .expect("current expiration predicate used once")(
                raw_lifecycle.as_deref(), &record
            ) {
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
                    (
                        self.new_delete_object_version_command(
                            pg_id,
                            bucket,
                            key,
                            record.version_id,
                            target,
                        ),
                        reclaim_generation_id,
                    )
                }
                BucketVersioningState::Enabled => {
                    let marker_vid = PgMetadataStore::next_version_id(&*object_pg, bucket, key)?;
                    (
                        self.new_insert_delete_marker_command(
                            pg_id,
                            &object_pg,
                            InsertDeleteMarkerDraft {
                                bucket,
                                key,
                                version_id: marker_vid,
                                owner: &owner,
                                stale_payload: None,
                            },
                        )?,
                        None,
                    )
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
                    (
                        self.new_insert_delete_marker_command(
                            pg_id,
                            &object_pg,
                            InsertDeleteMarkerDraft {
                                bucket,
                                key,
                                version_id: VersionId::Null,
                                owner: &owner,
                                stale_payload,
                            },
                        )?,
                        reclaim_generation_id,
                    )
                }
            };
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for lifecycle current object expiry",
            )?;
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
        select_versions: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
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
        let runtime_state = self.local_map.runtime_state();
        let mut select_versions = Some(select_versions);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
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
                        let due_version_ids = match select_versions
                            .take()
                            .expect("noncurrent expiration selector used once")(
                            raw_lifecycle.as_deref(),
                            &versions,
                        ) {
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
            let due_version_ids = match select_versions
                .take()
                .expect("noncurrent expiration selector used once")(
                raw_lifecycle.as_deref(),
                &versions,
            ) {
                Ok(version_ids) => version_ids,
                Err(error) => return Ok(Err(error)),
            };
            if due_version_ids.is_empty() {
                return Ok(Ok(Vec::new()));
            }

            let mut commands = Vec::new();
            let mut reclaimed_generation_ids = Vec::new();
            for stored in &versions {
                let Some(record) = stored.as_live() else {
                    continue;
                };
                if !due_version_ids.contains(&record.version_id) {
                    continue;
                }
                let target = self.live_delete_command_target(&object_pg, bucket, key, record)?;
                if let Some(generation_id) =
                    super::delete_object_version_reclaim_generation(&target)
                {
                    reclaimed_generation_ids.push(generation_id);
                }
                commands.push(self.new_delete_object_version_command(
                    pg_id,
                    bucket,
                    key,
                    record.version_id,
                    target,
                ));
            }
            drop(object_pg);

            for command in commands {
                self.set_pending_metadata_command_for_bucket(
                    pg_id,
                    bucket,
                    &command,
                    "conflicting pending command for lifecycle noncurrent object expiry",
                )?;
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }
            return Ok(Ok(reclaimed_generation_ids));
        }
    }

    pub fn delete_expired_delete_marker_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_delete: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<bool, E>,
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
        let runtime_state = self.local_map.runtime_state();
        let mut should_delete = Some(should_delete);

        loop {
            if let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket)
            {
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
                        let due = match should_delete
                            .take()
                            .expect("delete-marker expiration predicate used once")(
                            raw_lifecycle.as_deref(),
                            &versions,
                        ) {
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
            let due = match should_delete
                .take()
                .expect("delete-marker expiration predicate used once")(
                raw_lifecycle.as_deref(),
                &versions,
            ) {
                Ok(due) => due,
                Err(error) => return Ok(Err(error)),
            };
            if !due {
                return Ok(Ok(false));
            }
            let command = self.new_delete_object_version_command(
                pg_id,
                bucket,
                key,
                expected_version_id,
                DeleteObjectVersionTarget::DeleteMarker,
            );
            drop(object_pg);
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for expired delete marker removal",
            )?;
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
        let runtime_state = self.local_map.runtime_state();
        runtime_state.acquire_object_payload_lease(bucket, key, generation_id);
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

        enum ReclaimPayload {
            Segments(ObjectSegmentsReclaimRecord),
            Multipart(MultipartReclaimRecord),
        }

        if self
            .local_map
            .runtime_state()
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(false);
        }

        let meta_pg_id = self.object_metadata_pg_id(bucket, key);
        let reclaim = {
            let meta_pg = node.get_pg(meta_pg_id)?;
            if self
                .local_map
                .runtime_state()
                .object_payload_lease_count(bucket, key, generation_id)
                != 0
            {
                return Ok(false);
            }

            if let Some(reclaim) =
                PgMetadataStore::get_object_segments_reclaim(&*meta_pg, bucket, key, generation_id)?
            {
                Some(ReclaimPayload::Segments(reclaim))
            } else {
                PgMetadataStore::get_multipart_reclaim(&*meta_pg, bucket, key, generation_id)?
                    .map(ReclaimPayload::Multipart)
            }
        };

        let Some(reclaim) = reclaim else {
            return Ok(false);
        };

        if self
            .local_map
            .runtime_state()
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(false);
        }

        match &reclaim {
            ReclaimPayload::Segments(reclaim) => {
                for segment in &reclaim.segments {
                    self.delete_payload_shard_set(
                        segment.data_pg_id,
                        segment.ec,
                        &segment.segment_okh,
                        segment.segment_vid,
                    )?;
                }
            }
            ReclaimPayload::Multipart(reclaim) => {
                for part in &reclaim.parts {
                    match part {
                        MultipartReclaimPartRecord::ShardSet {
                            part_okh,
                            part_vid,
                            data_pg_id,
                            ec,
                            ..
                        } => {
                            self.delete_payload_shard_set(*data_pg_id, *ec, part_okh, *part_vid)?;
                        }
                        MultipartReclaimPartRecord::Segments { segments, .. } => {
                            for segment in segments {
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

        let meta_pg = node.get_pg(meta_pg_id)?;
        if self
            .local_map
            .runtime_state()
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(false);
        }

        match reclaim {
            ReclaimPayload::Segments(_) => PgMetadataStore::delete_object_segments_reclaim(
                &*meta_pg,
                bucket,
                key,
                generation_id,
            )?,
            ReclaimPayload::Multipart(_) => {
                PgMetadataStore::delete_multipart_reclaim(&*meta_pg, bucket, key, generation_id)?
            }
        }
        self.enqueue_bucket_delete_finalize(bucket);
        Ok(true)
    }

    fn delete_complete_multipart_cleanup_best_effort(
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
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        primary_node.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
            let object_pg = primary_node.get_pg(pg_id.get())?;
            let existing_object = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(StoredObject::Live(record)) => Some(StoredObject::Live(record)),
                Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => None,
                Err(error) => return Err(error.into()),
            };
            drop(object_pg);

            let (value, create) = match action(snapshot, existing_object) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };
            if self
                .matching_stream_upload_exists(pg_id, &create)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            {
                return Ok(Ok(value));
            }
            self.reserve_put_object_generation(bucket, key, &create.session_id)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;

            let command = MetadataCommandEnvelope::new(
                self.next_object_metadata_command_id(pg_id),
                MetadataCommandPayload::CreateStreamUpload(Box::new(CreateStreamUploadCommand {
                    request: create.clone(),
                    created_at_millis: crate::clock::current_time_millis(),
                })),
            );
            if let Err(error) = self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for stream upload creation",
            ) {
                let cleanup = self
                    .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                    .and_then(|_| {
                        self.release_object_generation_reservation(bucket, key, &create.session_id)
                    });
                if let Err(cleanup_error) = cleanup {
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        cleanup_error,
                    ));
                }
                return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                    error,
                ));
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                if self
                    .local_map
                    .runtime_state()
                    .pending_metadata_command_for_bucket(pg_id, bucket)
                    .is_none()
                {
                    let _ =
                        self.release_object_generation_reservation(bucket, key, &create.session_id);
                }
                return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                    error,
                ));
            }

            Ok(Ok(value))
        })
    }

    pub fn finalize_put_object_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnOnce(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();

        while let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket) {
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

        let pending_command = runtime_state
            .pending_metadata_command_for_bucket(pg_id, bucket)
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
        let session = object_pg.get_stream_upload(session_id)?;
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

        let (command, new_pending_command) = if let Some(command) = pending_command {
            (command, false)
        } else {
            let version_id = if prepared.versioning == BucketVersioningState::Enabled {
                PgMetadataStore::next_version_id(&*object_pg, bucket, key)?
            } else {
                VersionId::Null
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
                ec: staging_segments
                    .first()
                    .map_or(self.default_payload_ec_shape(), |segment| EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    }),
                layout: ObjectLayout::Standard,
                tags: prepared.tags.clone(),
                metadata_blob: Some(prepared.metadata_blob.clone()),
                system_metadata_blob: Some(prepared.system_metadata_blob.clone()),
                object_lock: prepared.object_lock,
                encryption: prepared.encryption.clone(),
            };
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    self.operation_epoch(),
                    pg_id,
                    runtime_state.next_metadata_command_log_index(pg_id),
                ),
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
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for stream PUT finalization",
            )?;
            (command, true)
        };
        drop(object_pg);

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
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        primary_node.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
            let object_pg = primary_node.get_pg(pg_id.get())?;
            let existing_object = match PgMetadataStore::get_object_meta(&*object_pg, bucket, key) {
                Ok(StoredObject::Live(record)) => Some(StoredObject::Live(record)),
                Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => None,
                Err(error) => return Err(error.into()),
            };
            drop(object_pg);

            let (value, create) = match action(snapshot, existing_object) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };
            if let Some(initiated_at) = self
                .matching_multipart_upload_initiated_at(pg_id, &create)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            {
                return Ok(Ok(CreateMultipartUploadOutcome {
                    value,
                    initiated_at,
                }));
            }

            let object_generation_id = {
                let object_pg = primary_node.get_pg(pg_id.get())?;
                PgMetadataStore::next_generation_id(&*object_pg, bucket, key)?
            };
            let command = MetadataCommandEnvelope::new(
                self.next_object_metadata_command_id(pg_id),
                MetadataCommandPayload::CreateMultipartUpload(Box::new(
                    CreateMultipartUploadCommand {
                        request: create.clone(),
                        object_generation_id,
                        initiated_at_millis: crate::clock::current_time_millis(),
                    },
                )),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for multipart upload creation",
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                    error,
                ));
            }

            let initiated_at = {
                let object_pg = primary_node.get_pg(pg_id.get())?;
                PgMetadataStore::get_multipart_upload(&*object_pg, &create.upload_id)?.initiated_at
            };
            Ok(Ok(CreateMultipartUploadOutcome {
                value,
                initiated_at,
            }))
        })
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
        action: impl FnOnce(&MultipartUploadRecord) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let object_pg = primary_node.get_pg(pg_id.get())?;
        let upload = PgMetadataStore::get_multipart_upload(&*object_pg, upload_id)?;
        if upload.bucket != *bucket || upload.key != *key || upload.state != UploadState::InProgress
        {
            return Err(BucketSnapshotLoadError::Metadata(
                MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                },
            ));
        }
        let result = action(&upload);
        if result.is_err() {
            return Ok(result);
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
            .matching_stream_upload_exists(pg_id, &create)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
        {
            return Ok(result);
        }
        let command = MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id(pg_id),
            MetadataCommandPayload::CreateStreamUpload(Box::new(CreateStreamUploadCommand {
                request: create,
                created_at_millis: crate::clock::current_time_millis(),
            })),
        );
        self.set_pending_metadata_command_for_bucket(
            pg_id,
            bucket,
            &command,
            "conflicting pending command for upload part stream creation",
        )
        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        Ok(result)
    }

    pub fn create_upload_part_stream_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
        session_id: &SessionId,
    ) -> Result<SessionId, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        loop {
            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
            let primary_node = self.object_metadata_primary_node(bucket, key)?;
            let object_pg = primary_node.get_pg(pg_id.get())?;
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
            if self.matching_stream_upload_exists(pg_id, &create)? {
                return Ok(session_id.clone());
            }
            let command = MetadataCommandEnvelope::new(
                self.next_object_metadata_command_id(pg_id),
                MetadataCommandPayload::CreateStreamUpload(Box::new(CreateStreamUploadCommand {
                    request: create,
                    created_at_millis: crate::clock::current_time_millis(),
                })),
            );
            if self
                .set_pending_metadata_command_for_bucket(
                    pg_id,
                    bucket,
                    &command,
                    "conflicting pending command for upload part stream creation",
                )
                .is_err()
            {
                self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                continue;
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
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_multipart_completion_snapshot(bucket, key, upload_id, requested_part_numbers)
    }

    pub fn load_multipart_completion_preflight(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .load_multipart_completion_preflight(bucket, key, upload_id)
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

    fn complete_multipart_command_cleanup(
        command: &CommitMultipartObjectCommand,
    ) -> CompleteMultipartCommitCleanup {
        CompleteMultipartCommitCleanup {
            omitted_parts: command.omitted_parts.clone(),
            omitted_streaming_segments: command.omitted_streaming_segments.clone(),
        }
    }

    fn apply_multipart_completion_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        match self.apply_metadata_command_to_acting_set(command) {
            Ok(()) => {
                self.local_map
                    .runtime_state()
                    .remove_pending_metadata_command_for_bucket(pg_id, bucket);
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
        let runtime_state = self.local_map.runtime_state();

        while let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, &bucket)
        {
            if let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() {
                if commit.matches_request(
                    &bucket,
                    &key,
                    &upload_id,
                    generation_id,
                    &req.part_records,
                ) {
                    let outcome = Self::complete_multipart_outcome_from_command(commit);
                    let cleanup = Self::complete_multipart_command_cleanup(commit);
                    self.apply_multipart_completion_command(pg_id, &bucket, &command)?;
                    self.delete_complete_multipart_cleanup_best_effort(
                        &bucket,
                        &key,
                        generation_id,
                        &cleanup,
                    );
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
        if upload.bucket != bucket || upload.key != key || upload.state != UploadState::InProgress {
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

        let version_id = if req.versioning == BucketVersioningState::Enabled {
            PgMetadataStore::next_version_id(&*object_pg, &bucket, &key)?
        } else {
            VersionId::Null
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
            PgMetadataStore::get_all_multipart_part_segments_for_upload(&*object_pg, &upload_id)?;
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

        let last_modified_millis = crate::clock::current_time_millis();
        let completed_at_millis = last_modified_millis;
        let write_sequence = object_pg.next_object_write_sequence(bucket.as_str(), key.as_str())?;
        let stale_payload_command = stale_payload.as_ref().map(|payload| {
            Self::completed_multipart_stale_payload_to_reclaim_command(
                &bucket,
                &key,
                last_modified_millis,
                payload,
            )
        });
        drop(object_pg);
        let completion_order = bucket_primary_node
            .next_completed_multipart_upload_order_for_bucket(&bucket)
            .map_err(|error| match error {
                BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
                BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
            })?;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.operation_epoch(),
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: upload_id.clone(),
                object: PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id,
                    owner: req.owner,
                    acl_grants: req.acl_grants,
                    public_read: req.public_read,
                    generation_id,
                    size: req.size,
                    etag: ObjectEtag::MultipartComposite {
                        crc64: req.etag_crc64,
                        parts: parts_count,
                    },
                    ec: EcShape { k: 0, m: 0 },
                    layout: ObjectLayout::MultipartManifest { parts_count },
                    tags: req.tags,
                    metadata_blob: req.metadata_blob,
                    system_metadata_blob: req.system_metadata_blob,
                    object_lock: req.object_lock,
                    encryption: req.encryption,
                },
                parts: object_parts,
                selected_streaming_segments,
                omitted_parts,
                omitted_streaming_segments,
                write_sequence,
                completion_order,
                completed_at_millis,
                initiator: upload.initiator.clone(),
                last_modified_millis,
                stale_payload: stale_payload_command,
            })),
        );
        self.set_pending_metadata_command_for_bucket(
            pg_id,
            &bucket,
            &command,
            "conflicting pending command for multipart completion",
        )?;
        self.apply_multipart_completion_command(pg_id, &bucket, &command)?;

        let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
            unreachable!("new complete multipart command changed payload kind");
        };
        let cleanup = Self::complete_multipart_command_cleanup(commit);
        self.delete_complete_multipart_cleanup_best_effort(&bucket, &key, generation_id, &cleanup);
        self.prune_completed_multipart_uploads_for_bucket_with_limit(
            &bucket,
            keep_completed_uploads,
        )?;
        Ok(Self::complete_multipart_outcome_from_command(commit))
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
        action: impl FnOnce(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        let runtime_state = self.local_map.runtime_state();

        while let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket) {
            let is_matching_stream_part_commit = matches!(
                command.payload(),
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.matches_request(bucket, key, upload_id, session_id, part_number)
            );
            if is_matching_stream_part_commit {
                break;
            }
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        }

        let pending_command = runtime_state
            .pending_metadata_command_for_bucket(pg_id, bucket)
            .filter(|command| {
                matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitStreamPart(commit)
                        if commit.matches_request(bucket, key, upload_id, session_id, part_number)
                )
            });

        let object_pg = primary_node.get_pg(pg_id.get())?;
        let session = object_pg.get_stream_upload(session_id)?;
        Self::validate_upload_part_stream_session(&session, bucket, key, upload_id, part_number)?;
        let upload = PgMetadataStore::get_multipart_upload(&*object_pg, upload_id)?;
        if upload.bucket != *bucket || upload.key != *key || upload.state != UploadState::InProgress
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
        let displaced_segments =
            PgMetadataStore::get_all_multipart_part_segments_for_upload(&*object_pg, upload_id)?
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
            command
        } else {
            let command = MetadataCommandEnvelope::new(
                self.next_object_metadata_command_id(pg_id),
                MetadataCommandPayload::CommitStreamPart(Box::new(command_payload)),
            );
            self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for stream part finalization",
            )?;
            command
        };
        drop(object_pg);

        let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
            unreachable!("stream part pending command kind changed");
        };
        let last_modified = commit.part.last_modified;
        if command_is_pending {
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        } else {
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        }
        Ok(Ok(FinalizeStreamPartOutcome {
            value: prepared.value,
            last_modified,
        }))
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

    pub fn list_multipart_parts_for_upload<E, F>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number_marker: Option<u32>,
        max_parts: u32,
        authorize: F,
    ) -> Result<Result<ListedMultipartParts, E>, ObjectPgActionError>
    where
        F: FnOnce(&MultipartUploadRecord) -> Result<(), E>,
    {
        self.object_metadata_primary_node(bucket, key)?
            .list_multipart_parts_for_upload(
                bucket,
                key,
                upload_id,
                part_number_marker,
                max_parts,
                authorize,
            )
    }

    pub fn lookup_abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartUploadLookup, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .lookup_abort_multipart_upload(bucket, key, upload_id)
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let runtime_state = self.local_map.runtime_state();
        while let Some(command) = runtime_state.pending_metadata_command_for_bucket(pg_id, bucket) {
            let matching_abort = matches!(
                command.payload(),
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.bucket == *bucket
                        && abort.key == *key
                        && abort.upload_id == *upload_id
            );
            if matching_abort {
                self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                return Ok(true);
            }
            self.apply_pending_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        }

        let Some(command) =
            self.prepare_abort_multipart_upload_command(pg_id, bucket, key, upload_id)?
        else {
            return Ok(false);
        };
        let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
            unreachable!("prepared abort multipart command changed payload kind");
        };
        self.set_pending_metadata_command_for_bucket(
            pg_id,
            bucket,
            &command,
            "conflicting pending command for multipart abort",
        )?;
        self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        Ok(true)
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
            self.next_object_metadata_command_id(pg_id),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
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
            _bucket_guard: _lifecycle_bucket_guard,
            raw_lifecycle,
            ..
        } = lifecycle_context;

        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        while let Some(command) = self
            .local_map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, bucket)
        {
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

        let primary_node = self.object_metadata_primary_node(bucket, key)?;
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
            return self.abort_multipart_upload(bucket, key, upload_id).map(Ok);
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

        self.abort_multipart_upload(bucket, key, upload_id).map(Ok)
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
        self.metadata_primary_bridge_node()?
            .test_put_object_segments_reclaim(bucket, key, reclaim)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_put_multipart_reclaim(bucket, key, reclaim)
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
        self.metadata_primary_bridge_node()?
            .test_force_became_noncurrent_at(bucket, key, version_id, became_noncurrent_at)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_create_deleting_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        self.metadata_primary_bridge_node()?
            .test_create_deleting_bucket(bucket)
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
        self.metadata_primary_bridge_node()?
            .test_set_upload_state(bucket, key, upload_id, state)
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
        self.metadata_primary_bridge_node()?
            .test_force_stream_upload_created_at(bucket, key, session_id, created_at)
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
