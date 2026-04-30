use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;

use crate::*;

// Metadata routing moves incrementally in Phase 6. Single-PG bucket/object
// operations route through the local metadata PG primary; composite scans still
// use the temporary metadata-primary bridge until they are split by PG.
impl super::StorageCluster {
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_pg_ids(&self) -> &[u32] {
        self.metadata_primary_topology_node().pg_ids()
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
        self.bucket_metadata_primary_node(&bucket)?
            .create_bucket_with_config_and_load_info(config)
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
        self.metadata_primary_bridge_node()?
            .load_bucket_snapshot_pair(source, destination)
    }

    pub fn begin_bucket_delete(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        self.metadata_primary_bridge_node()?
            .begin_bucket_delete(bucket)
    }

    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        self.metadata_primary_bridge_node()?
            .try_finalize_bucket_delete(bucket)
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
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_versioning_and_load_info(bucket, state)
    }

    pub fn put_bucket_object_lock_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_object_lock_and_load_info(bucket, config)
    }

    pub fn put_bucket_encryption_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_encryption_and_load_info(bucket, config)
    }

    pub fn put_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_public_access_block_and_load_info(bucket, config)
    }

    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .delete_bucket_public_access_block_and_load_info(bucket)
    }

    pub fn put_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_ownership_controls_and_load_info(bucket, config)
    }

    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .delete_bucket_ownership_controls_and_load_info(bucket)
    }

    pub fn put_bucket_abac_enabled_and_load_info(
        &self,
        bucket: &BucketName,
        enabled: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_abac_enabled_and_load_info(bucket, enabled)
    }

    pub fn put_bucket_acl_and_load_info(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_acl_and_load_info(bucket, acl_grants, public_read, public_write)
    }

    pub fn put_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .put_bucket_subresource_and_load_info(bucket, req)
    }

    pub fn delete_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.bucket_metadata_primary_node(bucket)?
            .delete_bucket_subresource_and_load_info(bucket, kind)
    }

    pub fn list_buckets_for_owner(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_buckets_for_owner(owner_canonical_id)
    }

    pub fn prune_completed_multipart_uploads_for_bucket_with_limit(
        &self,
        bucket: &BucketName,
        keep: usize,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .prune_completed_multipart_uploads_for_bucket_with_limit(bucket, keep)
    }

    pub fn list_lifecycle_sweep_buckets(
        &self,
    ) -> Result<LifecycleSweepBuckets, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_lifecycle_sweep_buckets()
    }

    pub fn list_all_objects_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_all_objects_for_bucket(bucket)
    }

    pub fn list_all_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_all_object_versions_for_bucket(bucket)
    }

    pub fn list_all_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_all_multipart_uploads_for_bucket(bucket)
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
        self.metadata_primary_bridge_node()?
            .list_objects_for_bucket(
                bucket,
                prefix,
                delimiter,
                continuation_token,
                record_cap,
                max_keys,
            )
    }

    pub fn list_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .list_object_versions_for_bucket(
                bucket,
                prefix,
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
        let node = self.metadata_primary_bridge_node_arc()?;
        node.acquire_object_payload_lease(bucket, key, generation_id);
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            node,
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
        let Ok(node) = self.metadata_primary_bridge_node() else {
            return 0;
        };
        node.object_payload_lease_count(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        let Ok(node) = self.metadata_primary_bridge_node() else {
            return 0;
        };
        node.bucket_object_payload_lease_count(bucket)
    }

    pub fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let Ok(bridge_node) = self.metadata_primary_bridge_node() else {
            return;
        };
        bridge_node.enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    pub fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) {
        let Ok(bridge_node) = self.metadata_primary_bridge_node() else {
            return;
        };
        bridge_node.enqueue_bucket_delete_finalize(bucket);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        self.metadata_primary_bridge_node()
            .ok()?
            .try_take_reclaim_work()
    }

    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        self.metadata_primary_bridge_node()
            .ok()?
            .wait_for_reclaim_work(stop)
    }

    pub fn wake_reclaim_workers(&self) {
        let Ok(bridge_node) = self.metadata_primary_bridge_node() else {
            return;
        };
        bridge_node.wake_reclaim_workers();
    }

    pub fn reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let lease_node = self.metadata_primary_bridge_node()?;
        let node = self.object_metadata_primary_node(bucket, key)?;

        enum ReclaimPayload {
            Segments(ObjectSegmentsReclaimRecord),
            Multipart(MultipartReclaimRecord),
        }

        if lease_node.object_payload_lease_count(bucket, key, generation_id) != 0 {
            return Ok(false);
        }

        let meta_pg_id = self.object_metadata_pg_id(bucket, key);
        let reclaim = {
            let meta_pg = node.get_pg(meta_pg_id)?;
            if lease_node.object_payload_lease_count(bucket, key, generation_id) != 0 {
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

        if lease_node.object_payload_lease_count(bucket, key, generation_id) != 0 {
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
        if lease_node.object_payload_lease_count(bucket, key, generation_id) != 0 {
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
        self.metadata_primary_bridge_node()?
            .list_multipart_uploads_for_bucket(
                bucket,
                prefix,
                key_marker,
                upload_id_marker,
                record_cap,
                max_uploads,
            )
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
        self.metadata_primary_bridge_node()?
            .delete_bucket_metadata(bucket)
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
