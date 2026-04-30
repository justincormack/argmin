use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::MutexGuard;
#[cfg(test)]
use std::sync::{Arc, Mutex, OnceLock};

#[cfg(test)]
use placement::NodeId;

use crate::metadata_command::{
    BucketPropertyMutation, BucketSubresourceMutation, CreateBucketCommand,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload, PutBucketAclCommand,
    PutBucketPropertyCommand, PutBucketSubresourceCommand, PutBucketVersioningCommand,
};
use crate::*;

const INTERNAL_LIST_PAGE_SIZE: u32 = 1_000;

#[cfg(test)]
type MetadataCommandApplyTestHook =
    Arc<dyn Fn(NodeId, &MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
static BEFORE_METADATA_COMMAND_APPLY_HOOK: OnceLock<Mutex<Option<MetadataCommandApplyTestHook>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) struct MetadataCommandApplyTestHookGuard;

#[cfg(test)]
impl Drop for MetadataCommandApplyTestHookGuard {
    fn drop(&mut self) {
        let hook = BEFORE_METADATA_COMMAND_APPLY_HOOK.get_or_init(|| Mutex::new(None));
        *hook.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

#[cfg(test)]
fn maybe_run_before_metadata_command_apply_hook(
    node_id: NodeId,
    command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    let hook = BEFORE_METADATA_COMMAND_APPLY_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(node_id, command)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_run_before_metadata_command_apply_hook(
    _node_id: placement::NodeId,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    Ok(())
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

struct MetadataCommandApplyFailure {
    applied_nodes: usize,
    source: BucketSnapshotLoadError,
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
                    self.local_map
                        .runtime_state()
                        .remove_pending_metadata_command_for_bucket(PgId::new(pg_id), &bucket);
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
            runtime_state.set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone());
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

    fn apply_metadata_command_to_acting_set(
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
            runtime_state.set_pending_metadata_command_for_bucket(pg_id, bucket, command.clone());
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
            runtime_state.set_pending_metadata_command_for_bucket(pg_id, bucket, command.clone());
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
            runtime_state.set_pending_metadata_command_for_bucket(pg_id, bucket, command.clone());
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
            runtime_state.set_pending_metadata_command_for_bucket(pg_id, bucket, command.clone());
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

    pub fn put_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        tags: &str,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .put_object_tags_if(bucket, key, version_id, tags, action)
    }

    pub fn delete_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .delete_object_tags_if(bucket, key, version_id, action)
    }

    pub fn put_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        retention: ObjectRetention,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .put_object_retention_if(bucket, key, version_id, retention, action)
    }

    pub fn put_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        legal_hold: StoredLegalHoldStatus,
        action: impl FnOnce(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .put_object_legal_hold_if(bucket, key, version_id, legal_hold, action)
    }

    pub fn put_object_acl_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .put_object_acl_if(bucket, key, version_id, action)
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

    pub fn delete_specific_object_version_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .delete_specific_object_version_if(bucket, key, version_id, action)
    }

    pub fn delete_current_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .delete_current_object_if(bucket, key, action)
    }

    pub fn insert_current_delete_marker_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        owner: OwnerIdentity,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .insert_current_delete_marker_if(bucket, key, owner, action)
    }

    pub fn expire_current_object_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_expire: impl FnOnce(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .expire_current_object_if_due(bucket, key, expected_version_id, should_expire)
    }

    pub fn delete_noncurrent_live_versions_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        select_versions: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .delete_noncurrent_live_versions_if_due(bucket, key, select_versions)
    }

    pub fn delete_expired_delete_marker_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_delete: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .delete_expired_delete_marker_if_due(bucket, key, expected_version_id, should_delete)
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

    fn delete_finalize_upload_part_cleanup_best_effort(&self, cleanup: &FinalizeStreamPartCleanup) {
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

    fn delete_abort_multipart_cleanup_best_effort(&self, cleanup: &AbortMultipartUploadCleanup) {
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
        self.object_metadata_primary_node(bucket, key)?
            .create_put_object_stream_session(bucket, key, request, action)
    }

    pub fn finalize_put_object_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnOnce(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .finalize_put_object_stream(bucket, key, session_id, total_size, action)
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
        self.object_metadata_primary_node(bucket, key)?
            .create_multipart_upload(bucket, key, request, action)
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
        self.object_metadata_primary_node(bucket, key)?
            .begin_upload_part_stream_session(
                bucket,
                key,
                upload_id,
                part_number,
                session_id,
                action,
            )
    }

    pub fn create_upload_part_stream_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
        session_id: &SessionId,
    ) -> Result<SessionId, ObjectPgActionError> {
        self.object_metadata_primary_node(bucket, key)?
            .create_upload_part_stream_session(bucket, key, upload_id, part_number, session_id)
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

    pub fn complete_multipart_upload_commit_serialized(
        &self,
        req: CompleteMultipartCommitRequest,
        keep_completed_uploads: usize,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let cleanup_bucket = req.bucket.clone();
        let cleanup_key = req.key.clone();
        let cleanup_generation_id = req.generation_id;
        let node = self.object_metadata_primary_node(&cleanup_bucket, &cleanup_key)?;
        let (outcome, cleanup) = node.complete_multipart_upload_commit_serialized(req)?;
        self.delete_complete_multipart_cleanup_best_effort(
            &cleanup_bucket,
            &cleanup_key,
            cleanup_generation_id,
            &cleanup,
        );
        self.prune_completed_multipart_uploads_for_bucket_with_limit(
            &cleanup_bucket,
            keep_completed_uploads,
        )?;
        Ok(outcome)
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
        let outcome = self
            .object_metadata_primary_node(bucket, key)?
            .finalize_upload_part_stream(bucket, key, upload_id, session_id, part_number, action)?;
        if let Some(cleanup) = outcome.cleanup.as_ref() {
            self.delete_finalize_upload_part_cleanup_best_effort(cleanup);
        }
        Ok(outcome.result)
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
        let cleanup = self
            .object_metadata_primary_node(bucket, key)?
            .abort_multipart_upload(bucket, key, upload_id)?;
        if let Some(cleanup) = cleanup.as_ref() {
            self.delete_abort_multipart_cleanup_best_effort(cleanup);
        }
        Ok(cleanup.is_some())
    }

    pub fn abort_multipart_upload_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        should_abort: impl FnOnce(Option<&str>, &MultipartUploadRecord) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        match self
            .object_metadata_primary_node(bucket, key)?
            .abort_multipart_upload_if_due(bucket, key, upload_id, should_abort)?
        {
            Ok(Some(cleanup)) => {
                self.delete_abort_multipart_cleanup_best_effort(&cleanup);
                Ok(Ok(true))
            }
            Ok(None) => Ok(Ok(false)),
            Err(error) => Ok(Err(error)),
        }
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
