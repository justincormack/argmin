use super::*;

impl LocalStorageNodeClient {
    pub(crate) fn new(node_id: NodeId, storage_node: Arc<SharedStorageNode>) -> Self {
        Self {
            node_id,
            storage_node,
        }
    }

    fn next_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        pg: &crate::PgStore,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_from_locked_pg_at_least(
            pg_id,
            cluster_epoch,
            pg,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    fn next_metadata_command_id_from_locked_pg_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        pg: &crate::PgStore,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let max_log_index = pg.max_metadata_command_log_index(cluster_epoch)?;
        if let Some(slot) =
            pg.pending_metadata_command_slot(self.node_id.as_u32(), cluster_epoch)?
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id: self.node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch,
                log_index: slot.id.log_index().get(),
            });
        }
        let next_log_index = max_log_index
            .checked_add(1)
            .map(|next| next.max(min_log_index.get()))
            .and_then(MetadataCommandLogIndex::new)
            .ok_or(StoreError::MetadataCommandLogConflict {
                node_id: self.node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch,
                log_index: u64::MAX,
            })?;
        Ok(MetadataCommandId::new(cluster_epoch, pg_id, next_log_index))
    }
}

impl PlacedShardNodeClient for LocalStorageNodeClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.storage_node
            .write_shard_file(data_pg_id.get(), key, data)
    }

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        _expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        self.storage_node.read_shard_file(data_pg_id.get(), key)
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        _expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.storage_node
            .read_shard_file_into(data_pg_id.get(), key, dst)
    }

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
        self.storage_node.delete_shard_file(data_pg_id.get(), key)
    }
}

impl ShardReadHandleNodeClient for LocalStorageNodeClient {
    fn acquire_read_handles(
        &self,
        _read_operation_id: &str,
        _entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        Ok(Box::new(LocalStorageNodeReadHandleLease))
    }
}

impl ShardReadHandleLease for LocalStorageNodeReadHandleLease {
    fn release(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl ShardAckNodeClient for LocalStorageNodeClient {
    fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.register_written_shards_batch_exact(shard_batch)
    }

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.validate_written_shard_ack(key, ack)
    }

    fn load_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<WriteAck, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let stat = pg.stat_shard(key)?;
        Ok(WriteAck {
            crc64: stat.crc64,
            stored_size: stat.size,
        })
    }

    fn delete_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.delete_shard_record(key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(pg_id, work_item)?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_placed_segment_shard_repair(work_item, last_error)
    }

    fn list_placed_segment_shard_repairs(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.list_placed_segment_shard_repairs()
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.acquire_placed_segment_shard_repair_claim(request)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_epoch(pg_id, cluster_epoch, claim)?;
        validate_placed_segment_shard_repair_route(pg_id, &claim.work_item)?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.complete_placed_segment_shard_repair_claim(claim)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_epoch(pg_id, cluster_epoch, claim)?;
        validate_placed_segment_shard_repair_route(pg_id, &claim.work_item)?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_placed_segment_shard_repair_claim_error(claim, last_error, next_attempt_after)
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(pg_id, work_item)?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.resolve_placed_segment_shard_repair(work_item)
            .map(|_| ())
    }
}

fn validate_placed_segment_shard_repair_route(
    pg_id: PgId,
    work_item: &PlacedSegmentShardRepairWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.data_pg_id != pg_id.get() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair work item data PG {} does not match routed PG {}",
                work_item.request.data_pg_id,
                pg_id.get()
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_epoch(
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StoreError> {
    if claim.cluster_epoch != cluster_epoch {
        return Err(StoreError::StalePayloadOperation {
            pg_id: pg_id.get(),
            operation_epoch: claim.cluster_epoch,
            current_epoch: cluster_epoch,
        });
    }
    Ok(())
}

impl ShardScavengerNodeClient for LocalStorageNodeClient {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        self.storage_node
            .list_scavenger_shard_files(data_pg_id.get())
    }

    fn list_scavenger_shard_rows(&self, pg_id: PgId) -> Result<Vec<ScavengerShardRow>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.list_scavenger_shard_rows()
    }

    fn list_shard_scavenger_payload_references(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.list_shard_scavenger_payload_references()
    }

    fn record_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_shard_scavenger_observation(observation)
    }

    fn list_shard_scavenger_observations(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.list_shard_scavenger_observations()
    }

    fn resolve_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.resolve_shard_scavenger_observation(key).map(|_| ())
    }
}

impl BucketMetadataNodeClient for LocalStorageNodeClient {
    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::head_bucket_raw(self, pg_id, bucket)
    }

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::head_bucket_info(self, pg_id, bucket)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::load_bucket_snapshot(self, pg_id, bucket, request)
    }

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::load_bucket_snapshot_pair(
            self,
            source_pg_id,
            source,
            destination_pg_id,
            destination,
        )
    }

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_create_bucket_command(
            self, pg_id, bucket, command_id, config,
        )
    }

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_advance_completed_multipart_upload_sequence_command(
            self, pg_id, bucket, command_id,
        )
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(MarkBucketDeletingCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
            )
            .bucket)
        })
    }

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        if current.state == BucketState::Deleting {
            return Ok(MarkBucketDeletingCommandBuild::AlreadyDeleting);
        }
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MarkBucketDeletingCommandBuild::Command(Box::new(
            MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                    current.with_execution_generation(bucket_execution_generation),
                )),
            ),
        )))
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::pending_put_bucket_versioning_command_matches_current(
            self, pg_id, bucket, command, state,
        )
    }

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_put_bucket_versioning_command(
            self, pg_id, bucket, command_id, state,
        )
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::pending_put_bucket_acl_command_matches_current(
            self,
            pg_id,
            bucket,
            command,
            acl_grants,
            public_read,
            public_write,
        )
    }

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_put_bucket_acl_command(
            self,
            pg_id,
            bucket,
            command_id,
            acl_grants,
            public_read,
            public_write,
        )
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::pending_put_bucket_property_command_matches_current(
            self, pg_id, bucket, command, mutation,
        )
    }

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_put_bucket_property_command(
            self, pg_id, bucket, command_id, mutation,
        )
    }

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::build_put_bucket_subresource_command(
            self, pg_id, bucket, command_id, mutation,
        )
    }

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_bucket_subresource(self, pg_id, bucket, kind)
    }

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::list_buckets(&*pg, owner_canonical_id)?)
    }

    fn load_bucket_execution_generations(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.load_bucket_execution_generations(buckets)?)
    }

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.load_bucket_fast_path_identities(buckets)?)
    }
}

impl BucketWriteReservationNodeClient for LocalStorageNodeClient {
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::durable_bucket_write_drain_exists(self, pg_id, bucket)
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::acquire_durable_bucket_write_reservation(
            self,
            pg_id,
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        )
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::validate_bucket_write_reservation_proof(self, pg_id, proof)
    }

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::release_durable_bucket_write_reservation(self, pg_id, record)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::release_metadata_command_bucket_write_reservation(
            self, pg_id, proof,
        )
    }

    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::begin_durable_bucket_write_drain(
            self,
            pg_id,
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            created_at,
            lease_deadline,
        )
    }

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::clear_durable_bucket_write_drain(self, pg_id, record)
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::clear_expired_durable_bucket_write_drain(
            self, pg_id, bucket, now,
        )
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::heartbeat_durable_bucket_write_drain(
            self,
            pg_id,
            record,
            lease_deadline,
        )
    }

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::durable_bucket_write_reservations(self, pg_id, bucket)
    }

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::heartbeat_durable_bucket_write_reservation(
            self,
            pg_id,
            proof,
            lease_deadline,
        )
    }

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        <Self as StorageNodeClient>::delete_finalized_bucket(self, pg_id, bucket)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_bucket_delete_finalize_roots(self, pg_id, now, limit)
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::acquire_bucket_delete_finalize_claim(
            self,
            pg_id,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::release_bucket_delete_finalize_claim(self, pg_id, claim)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_lifecycle_sweep_roots(self, pg_id, now, limit)
    }

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::list_lifecycle_sweep_buckets(self, pg_id)
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::acquire_lifecycle_sweep_claim(
            self,
            pg_id,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::heartbeat_lifecycle_sweep_claim(
            self,
            pg_id,
            claim,
            heartbeat_at,
            lease_deadline,
        )
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::record_lifecycle_sweep_claim_error(
            self, pg_id, claim, last_error,
        )
    }

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::release_lifecycle_sweep_claim(self, pg_id, claim)
    }
}

impl ObjectGenerationMetadataNodeClient for LocalStorageNodeClient {
    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        <Self as StorageNodeClient>::object_generation_reservation(
            self,
            pg_id,
            bucket,
            key,
            reservation_id,
        )
    }

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        <Self as StorageNodeClient>::next_object_generation_id(self, pg_id, bucket, key)
    }
}

impl ObjectVersionMetadataNodeClient for LocalStorageNodeClient {
    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        <Self as StorageNodeClient>::next_object_version_id(self, pg_id, bucket, key)
    }
}

impl DirectPutMetadataNodeClient for LocalStorageNodeClient {
    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_direct_put_commit_snapshot(
            self,
            pg_id,
            bucket,
            key,
            reservation_id,
            generation_id,
        )
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_direct_put_commit_command(self, request)
    }
}

impl ObjectMutationMetadataNodeClient for LocalStorageNodeClient {
    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_put_object_metadata_snapshot(
            self, pg_id, bucket, key, version_id,
        )
    }

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_put_object_metadata_command(self, request)
    }

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_current_object_delete_snapshot(self, pg_id, bucket, key)
    }

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_specific_object_delete_snapshot(
            self, pg_id, bucket, key, version_id,
        )
    }

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        <Self as StorageNodeClient>::list_object_versions_for_lifecycle(self, pg_id, bucket, key)
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_delete_specific_object_version_command(self, request)
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_delete_current_object_command(self, request)
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_insert_delete_marker_command(self, request)
    }

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        <Self as StorageNodeClient>::matching_stream_upload_exists(
            self,
            pg_id,
            create,
            expected_command,
        )
    }

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        <Self as StorageNodeClient>::matching_multipart_upload_initiated_at(
            self,
            pg_id,
            create,
            expected_command,
        )
    }

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_stream_upload_session(
            self, pg_id, bucket, key, session_id,
        )
    }

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::load_multipart_upload(self, pg_id, bucket, key, upload_id)
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_in_progress_multipart_upload(
            self, pg_id, bucket, key, upload_id,
        )
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_in_progress_multipart_upload_for_listing(
            self, pg_id, bucket, key, upload_id,
        )
    }

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_multipart_completion_snapshot(
            self,
            pg_id,
            authorized_upload,
            requested_part_numbers,
        )
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_multipart_completion_preflight(
            self,
            pg_id,
            authorized_upload,
        )
    }

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        <Self as StorageNodeClient>::list_multipart_parts_for_authorized_upload(
            self,
            pg_id,
            authorized_upload,
            part_number_marker,
            max_parts,
        )
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        <Self as StorageNodeClient>::lookup_multipart_upload_management(
            self, pg_id, bucket, key, upload_id,
        )
    }

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_create_stream_upload_command(self, request)
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_create_multipart_upload_command(self, request)
    }

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_stream_upload_segments(
            self, pg_id, bucket, key, session_id,
        )
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        <Self as StorageNodeClient>::list_stream_uploads_for_bucket_page(
            self,
            pg_id,
            bucket,
            session_id_marker,
            limit,
        )
    }

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        <Self as StorageNodeClient>::list_all_stream_uploads_page(
            self,
            pg_id,
            session_id_marker,
            limit,
        )
    }

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::list_completed_multipart_upload_records_for_bucket_page(
            self,
            pg_id,
            bucket,
            upload_id_marker,
            limit,
        )
    }

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        <Self as StorageNodeClient>::payload_reclaim_exists(self, pg_id, bucket, key, generation_id)
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_bucket_payload_reclaim_root(self, pg_id, bucket)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_payload_reclaim_root(self, pg_id)
    }

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::get_object_payload_reclaim(
            self,
            pg_id,
            bucket,
            key,
            generation_id,
        )
    }

    fn acquire_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::acquire_object_payload_reclaim_claim(
            self,
            pg_id,
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        <Self as StorageNodeClient>::release_object_payload_reclaim_claim(self, pg_id, claim)
    }

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        <Self as StorageNodeClient>::prepare_stream_segment_append(
            self, pg_id, bucket, key, request,
        )
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_stream_put_finalize_snapshot(
            self, pg_id, bucket, key, session_id,
        )
    }

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_stream_put_commit_command(self, request)
    }

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_stream_part_finalize_snapshot(
            self,
            pg_id,
            bucket,
            key,
            upload_id,
            session_id,
            part_number,
        )
    }

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_stream_part_commit_command(self, request)
    }

    fn load_multipart_completion_stale_payload_source(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(load_null_live_stale_payload_source_from_pg(
            &pg, bucket, key,
        )?)
    }

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_complete_multipart_object_command(self, request)
    }

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_abort_multipart_upload_cleanup(
            self, pg_id, bucket, key, upload_id,
        )
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_abort_multipart_upload_command(self, request)
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        <Self as StorageNodeClient>::build_authorized_abort_multipart_upload_command(self, request)
    }
}

impl ObjectReadMetadataNodeClient for LocalStorageNodeClient {
    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_object_read_auth_subject(
            self, pg_id, bucket, key, version_id,
        )
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_object_read_snapshot_for_subject(
            self,
            pg_id,
            bucket,
            key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: VersionId,
    ) -> Result<Option<String>, ObjectPgActionError> {
        <Self as StorageNodeClient>::get_object_tags_for_subject(
            self,
            pg_id,
            bucket,
            key,
            version_id,
            expected_identity,
            authorized_version_id,
        )
    }
}

impl ObjectListingMetadataNodeClient for LocalStorageNodeClient {
    fn list_objects_page(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_objects(req)?)
    }

    fn list_object_versions_page(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_object_versions(req)?)
    }

    fn list_multipart_uploads_page(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_multipart_uploads(req)?)
    }
}

impl StorageNodeClient for LocalStorageNodeClient {
    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_completed_multipart_upload_records_for_bucket_page(
            bucket,
            upload_id_marker,
            limit,
        )?)
    }

    fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.storage_node
            .try_acquire_object_payload_lease(bucket, key, generation_id)
    }

    fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.storage_node
            .release_object_payload_lease(bucket, key, generation_id)
    }

    fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.storage_node
            .try_begin_object_payload_reclaim(bucket, key, generation_id)
    }

    fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    ) {
        self.storage_node
            .finish_object_payload_reclaim(bucket, key, generation_id, keep_fence);
    }

    fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.storage_node
            .clear_object_payload_reclaim_fence(bucket, key, generation_id);
    }

    fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.storage_node
            .object_payload_lease_count(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        self.storage_node.bucket_object_payload_lease_count(bucket)
    }
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_drain(&*pg, bucket)?.is_some())
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        )?)
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &proof.bucket,
            &proof.reservation_id,
        )?
        else {
            return Err(MetadataError::BucketWriteReservationNotFound {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        };
        if !proof.matches_record(&record) {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        if record
            .lease_deadline
            .is_some_and(|deadline| deadline <= crate::clock::current_time_millis())
        {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        let current_bucket = PgMetadataStore::head_bucket_raw(&*pg, &proof.bucket)?;
        if current_bucket.state == BucketState::Active
            && current_bucket.bucket_incarnation_generation == proof.bucket_incarnation_generation
        {
            Ok(())
        } else {
            Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into())
        }
    }

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_durable_bucket_write_reservation(
            &*pg,
            &record.bucket,
            &record.reservation_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
            record.bucket_incarnation_generation,
        )?)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        if let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
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
        Ok(
            PgMetadataStore::release_metadata_command_bucket_write_reservation(
                &*pg,
                &proof.bucket,
                &proof.reservation_id,
                &proof.owner_token,
                proof.cluster_epoch,
                proof.bucket_execution_generation,
                proof.bucket_incarnation_generation,
            )?,
        )
    }

    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            created_at,
            lease_deadline,
        )?)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::clear_durable_bucket_write_drain(
            &*pg,
            &record.bucket,
            &record.drain_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
        )?)
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::clear_expired_durable_bucket_write_drain(
            &*pg, bucket, now,
        )?)
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::heartbeat_durable_bucket_write_drain(
            &*pg,
            &record.bucket,
            &record.drain_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
            lease_deadline,
            crate::clock::current_time_millis(),
        )?)
    }

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_reservations(
            &*pg, bucket,
        )?)
    }

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::heartbeat_durable_bucket_write_reservation(
            &*pg,
            DurableBucketWriteReservationHeartbeat {
                name: &proof.bucket,
                reservation_id: &proof.reservation_id,
                owner_token: &proof.owner_token,
                cluster_epoch: proof.cluster_epoch,
                bucket_execution_generation: proof.bucket_execution_generation,
                bucket_incarnation_generation: proof.bucket_incarnation_generation,
                lease_deadline,
            },
        )?)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let snapshot = SharedStorageNode::load_bucket_snapshot_from_pg(&pg, bucket, request)?;
        drop(pg);
        Ok(snapshot)
    }

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        if source.0 == destination.0 {
            let merged_request = source.1.union(destination.1);
            let bucket = <Self as StorageNodeClient>::load_bucket_snapshot(
                self,
                source_pg_id,
                source.0,
                merged_request,
            )?;
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let source_snapshot = <Self as StorageNodeClient>::load_bucket_snapshot(
            self,
            source_pg_id,
            source.0,
            source.1,
        )?;
        let destination_snapshot = <Self as StorageNodeClient>::load_bucket_snapshot(
            self,
            destination_pg_id,
            destination.0,
            destination.1,
        )?;
        Ok(BucketSnapshotPair::Distinct {
            source: Box::new(source_snapshot),
            destination: Box::new(destination_snapshot),
        })
    }

    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket_raw(&*pg, bucket)?)
    }

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket(&*pg, bucket)?)
    }

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        PgMetadataStore::delete_finalized_bucket(&*pg, bucket)?;
        pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match PgMetadataStore::head_bucket_raw(&*pg, bucket) {
            Ok(info) => return Ok(CreateBucketCommandBuild::Exists(info)),
            Err(MetadataError::BucketNotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        let command = CreateBucketCommand::from_config(
            config,
            crate::clock::current_time_millis(),
            bucket_execution_generation,
        )
        .map_err(|reason| MetadataError::InvalidBucketName { reason })?;
        Ok(CreateBucketCommandBuild::Command(Box::new(
            MetadataCommandEnvelope::new(command_id, MetadataCommandPayload::CreateBucket(command)),
        )))
    }

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current_order = pg.completed_multipart_upload_sequence_for_bucket(bucket)?;
        let completion_order = current_order
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
        Ok((
            completion_order,
            MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                    AdvanceCompletedMultipartUploadSequenceCommand {
                        bucket: bucket.clone(),
                        completion_order,
                    },
                ),
            ),
        ))
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
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
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                state,
            )
            .bucket)
        })
    }

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        if state == BucketVersioningState::Disabled
            && current.versioning != BucketVersioningState::Disabled
        {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current.versioning,
                to: state,
            }
            .into());
        }
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current.with_execution_generation(bucket_execution_generation),
                state,
            )),
        ))
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(PutBucketAclCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                acl_grants.clone(),
                public_read,
                public_write,
            )
            .bucket)
        })
    }

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                current.with_execution_generation(bucket_execution_generation),
                acl_grants.clone(),
                public_read,
                public_write,
            )),
        ))
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(PutBucketPropertyCommand::from_bucket_and_mutation(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                mutation.clone(),
            )
            .bucket)
        })
    }

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketProperty(
                PutBucketPropertyCommand::from_bucket_and_mutation(
                    current.with_execution_generation(bucket_execution_generation),
                    mutation.clone(),
                ),
            ),
        ))
    }

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                mutation.clone(),
                bucket_execution_generation,
            )),
        ))
    }

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_subresource(&*pg, bucket, kind)?.map(|stored| stored.body))
    }

    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match version_id {
            Some(version_id) => Ok(PgMetadataStore::get_object_version(
                &*pg, bucket, key, version_id,
            )?),
            None => Ok(PgMetadataStore::get_object_meta(&*pg, bucket, key)?),
        }
    }

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = match request.requested_version_id {
            Some(version_id) => {
                PgMetadataStore::get_object_version(&*pg, request.bucket, request.key, version_id)
            }
            None => PgMetadataStore::get_object_meta(&*pg, request.bucket, request.key),
        };
        let current = match current {
            Ok(current) => current,
            Err(MetadataError::ObjectNotFound) => {
                return Err(ObjectPgActionError::StaleObjectReadSubject);
            }
            Err(other) => return Err(other.into()),
        };
        if &current != request.expected_stored {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        if current.version_id() != request.version_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "object metadata action returned version {:?} for stored version {:?}",
                    request.version_id,
                    current.version_id()
                ),
            });
        }
        let live = current
            .as_live()
            .ok_or(MetadataError::MethodNotAllowedOnDeleteMarker)?;
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutObjectMetadata(Box::new(
                PutObjectMetadataCommand::from_live_object_and_mutation(
                    live.clone(),
                    request.mutation,
                    request.bucket_write_reservation.clone(),
                ),
            )),
        ))
    }

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let stored = load_current_object_optional_from_pg(&pg, bucket, key)?;
        Ok(load_object_delete_snapshot_from_stored(
            &pg, bucket, key, stored,
        )?)
    }

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let stored = load_object_version_optional_from_pg(&pg, bucket, key, version_id)?;
        Ok(load_object_delete_snapshot_from_stored(
            &pg, bucket, key, stored,
        )?)
    }

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match PgMetadataStore::list_object_versions_for_key(&*pg, bucket, key) {
            Ok(versions) => Ok(versions),
            Err(MetadataError::ObjectNotFound) => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = if let Some(expected_version_list) = request.expected_version_list {
            let versions = match PgMetadataStore::list_object_versions_for_key(
                &*pg,
                request.bucket,
                request.key,
            ) {
                Ok(versions) => versions,
                Err(MetadataError::ObjectNotFound) => Vec::new(),
                Err(error) => return Err(error.into()),
            };
            if versions.as_slice() != expected_version_list {
                return Err(ObjectPgActionError::StaleObjectReadSubject);
            }
            versions
                .into_iter()
                .find(|stored| stored.version_id() == request.version_id)
        } else {
            load_object_version_optional_from_pg(
                &pg,
                request.bucket,
                request.key,
                request.version_id,
            )?
        };
        if current.as_ref() != request.expected_stored {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(target) =
            delete_command_target_from_stored(&pg, request.bucket, request.key, current.as_ref())?
        else {
            return Ok(None);
        };
        if !delete_target_matches_expected(Some(&target), request.expected_target) {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                version_id: request.version_id,
                target,
            })),
        )))
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_current_object_optional_from_pg(&pg, request.bucket, request.key)?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(StoredObject::Live(record)) = current.as_ref() else {
            return Ok(None);
        };
        let target = live_delete_command_target(&pg, request.bucket, request.key, record)?;
        if !delete_target_matches_expected(Some(&target), request.expected_target) {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                version_id: record.version_id,
                target,
            })),
        )))
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_current_object_optional_from_pg(&pg, request.bucket, request.key)?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let stale_payload = match request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(stale_payload) => stale_payload,
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
                let (source, stale_payload) = snapshot_direct_put_stale_payload_for_snapshot(
                    &pg,
                    request.bucket,
                    request.key,
                    created_at,
                )?;
                if source.as_ref() != request.expected_stale_payload_source {
                    return Err(ObjectPgActionError::StaleObjectReadSubject);
                }
                stale_payload
            }
        };
        let write_sequence =
            pg.next_object_write_sequence(request.bucket.as_str(), request.key.as_str())?;
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                version_id: request.version_id,
                owner: request.owner.clone(),
                write_sequence,
                last_modified_millis: crate::clock::current_time_millis(),
                stale_payload,
            }),
        ))
    }

    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_read_auth_subject_from_object_pg(
            &pg, bucket, key, version_id,
        )
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_read_snapshot_for_subject_from_object_pg(
            &pg,
            bucket,
            key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: s3_types::VersionId,
    ) -> Result<Option<String>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::get_object_tags_for_subject_from_object_pg(
            &pg,
            bucket,
            key,
            version_id,
            expected_identity,
            authorized_version_id,
        )
    }

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(load_multipart_upload_from_pg(&pg, bucket, key, upload_id)?)
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(load_in_progress_multipart_upload_from_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        <Self as StorageNodeClient>::load_in_progress_multipart_upload(
            self, pg_id, bucket, key, upload_id,
        )
    }

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let mut part_records = Vec::with_capacity(requested_part_numbers.len());
        for &part_number in requested_part_numbers {
            part_records.push(pg.get_multipart_part(upload_id, part_number)?);
        }
        let selected_part_numbers = part_records
            .iter()
            .map(|part| part.part_number)
            .collect::<BTreeSet<_>>();
        let (selected_streaming_segments, cleanup) =
            snapshot_complete_multipart_cleanup_from_pg(&pg, upload_id, &selected_part_numbers)?;
        let (stale_payload_source, _) =
            snapshot_direct_put_stale_payload_for_snapshot(&pg, bucket, key, 0)?;
        Ok(MultipartCompletionSnapshot {
            existing_etag,
            stale_payload_source,
            part_records,
            selected_streaming_segments,
            cleanup,
        })
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        Ok(MultipartCompletionPreflight { existing_etag })
    }

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let response = pg.list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })?;
        Ok(ListedMultipartParts { upload, response })
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match load_multipart_upload_from_pg(&pg, bucket, key, upload_id) {
            Ok(upload) if upload.state == UploadState::InProgress => {
                return Ok(MultipartUploadManagementLookup::InProgress(Box::new(
                    upload,
                )));
            }
            Ok(upload) => {
                return Ok(MultipartUploadManagementLookup::NonInProgress(Box::new(
                    upload,
                )));
            }
            Err(MetadataError::NoSuchUpload { .. }) => {}
            Err(error) => return Err(error.into()),
        }

        if let Some(completed) = pg.get_completed_multipart_upload(upload_id)? {
            if completed.bucket == *bucket && completed.key == *key {
                return Ok(MultipartUploadManagementLookup::Completed(completed));
            }
        }
        Ok(MultipartUploadManagementLookup::Missing)
    }

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.prepare_abort_multipart_upload_cleanup(bucket, key, upload_id)?)
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let cleanup = pg.prepare_abort_multipart_upload_cleanup(
            request.bucket,
            request.key,
            request.upload_id,
        )?;
        if cleanup.as_ref() != request.expected_cleanup {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(cleanup) = cleanup else {
            return Ok(None);
        };
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                upload_id: request.upload_id.clone(),
                cleanup,
                bucket_write_reservation: request.bucket_write_reservation,
            })),
        )))
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let cleanup =
            pg.prepare_authorized_abort_multipart_upload_cleanup(request.authorized_upload)?;
        if cleanup.as_ref() != request.expected_cleanup {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(cleanup) = cleanup else {
            return Ok(None);
        };
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: request.authorized_upload.record().bucket.clone(),
                key: request.authorized_upload.record().key.clone(),
                upload_id: request.authorized_upload.record().upload_id.clone(),
                cleanup,
                bucket_write_reservation: request.bucket_write_reservation,
            })),
        )))
    }

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::payload_reclaim_exists(
            &*pg,
            bucket,
            key,
            generation_id,
        )?)
    }

    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<s3_types::VersionId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::next_version_id(&*pg, bucket, key)?)
    }

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::next_generation_id(&*pg, bucket, key)?)
    }

    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_object_generation_reservation(
            &*pg,
            bucket,
            key,
            reservation_id,
        )?)
    }

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(session_id)?;
        validate_stream_upload_session_binding(&session, bucket, key)?;
        Ok(session)
    }

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match pg.get_stream_upload(&create.session_id) {
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

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match pg.get_multipart_upload(&create.upload_id) {
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

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        match (&request.request.target, request.precondition) {
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObject {
                    expected_current,
                    require_generation_reservation,
                },
            ) => {
                let current = load_current_object_optional_from_pg(
                    &pg,
                    &request.request.bucket,
                    &request.request.key,
                )?;
                if current.as_ref() != expected_current {
                    return Err(ObjectPgActionError::StaleObjectReadSubject);
                }
                if require_generation_reservation {
                    PgMetadataStore::get_object_generation_reservation(
                        &*pg,
                        &request.request.bucket,
                        &request.request.key,
                        &request.request.session_id,
                    )?;
                }
            }
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation,
                },
            ) => {
                if require_generation_reservation {
                    PgMetadataStore::get_object_generation_reservation(
                        &*pg,
                        &request.request.bucket,
                        &request.request.key,
                        &request.request.session_id,
                    )?;
                }
            }
            (
                StreamUploadTarget::UploadPart { upload_id, .. },
                CreateStreamUploadPrecondition::UploadPart { expected_upload },
            ) => {
                let current = load_in_progress_multipart_upload_from_pg(
                    &pg,
                    &request.request.bucket,
                    &request.request.key,
                    upload_id,
                )?;
                if &current != expected_upload {
                    return Err(MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    }
                    .into());
                }
            }
            _ => {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "create stream upload precondition does not match target".to_string(),
                });
            }
        }
        match pg.get_stream_upload(&request.request.session_id) {
            Ok(_) => {
                return Err(MetadataError::Db {
                    context: "create stream upload existing session mismatch",
                    source: rusqlite::Error::InvalidQuery,
                }
                .into());
            }
            Err(MetadataError::StreamSessionNotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    request.request.clone(),
                    crate::clock::current_time_millis(),
                    request.bucket_write_reservation.clone(),
                ),
            )),
        ))
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_current_object_optional_from_pg(
            &pg,
            &request.request.bucket,
            &request.request.key,
        )?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        match pg.get_multipart_upload(&request.request.upload_id) {
            Ok(_) => {
                return Err(MetadataError::Db {
                    context: "create multipart upload existing upload mismatch",
                    source: rusqlite::Error::InvalidQuery,
                }
                .into());
            }
            Err(MetadataError::NoSuchUpload { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let object_generation_id = PgMetadataStore::next_generation_id(
            &*pg,
            &request.request.bucket,
            &request.request.key,
        )?;
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateMultipartUpload(Box::new(
                CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                    request.request.clone(),
                    object_generation_id,
                    crate::clock::current_time_millis(),
                    request.bucket_write_reservation.clone(),
                ),
            )),
        ))
    }

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(session_id)?;
        validate_stream_upload_session_bucket_key(&session, bucket, key)?;
        Ok(pg.list_stream_segments(session_id)?)
    }

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_all_stream_uploads_page(session_id_marker, limit)?)
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_stream_uploads_for_bucket_page(bucket, session_id_marker, limit)?)
    }

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(&request.session_id)?;
        validate_stream_upload_session_binding(&session, bucket, key)?;
        reject_duplicate_stream_segment_index(&pg, &request.session_id, request.segment_index)?;
        let pg_topology = self.storage_node.pg_topology();
        let (segment_okh, segment_vid, data_pg_id) = match session.target {
            StreamUploadTarget::PutObject => {
                let generation_id =
                    pg.get_object_generation_reservation(bucket, key, &request.session_id)?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    crate::segment_key_hash(
                        bucket.as_str(),
                        key.as_str(),
                        generation_id,
                        request.segment_index,
                    ),
                    segment_vid,
                    pg_topology
                        .object_generation_segment_data_pg(
                            bucket,
                            key,
                            generation_id,
                            request.segment_index,
                        )
                        .get(),
                )
            }
            StreamUploadTarget::UploadPart {
                ref upload_id,
                part_number,
            } => {
                let upload =
                    load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    request.segment_okh,
                    segment_vid,
                    pg_topology
                        .object_generation_multipart_part_segment_data_pg(
                            bucket,
                            key,
                            upload.object_generation_id,
                            part_number,
                            request.segment_index,
                        )
                        .get(),
                )
            }
        };
        let ec = self.storage_node.default_ec_shape();
        let segment_record = StreamUploadSegmentRecord {
            session_id: request.session_id.clone(),
            segment_index: request.segment_index,
            size: request.size,
            segment_crc64: request.segment_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            ec_k: ec.k,
            ec_m: ec.m,
        };
        Ok((session.target, segment_record))
    }

    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        load_direct_put_commit_snapshot_from_pg(&pg, bucket, key, reservation_id, generation_id)
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_direct_put_commit_snapshot_from_pg(
            &pg,
            &request.request.bucket,
            &request.request.key,
            &request.request.generation_reservation_id,
            request.request.generation_id,
        )?;
        if &current != request.expected_snapshot {
            return Err(ObjectPgActionError::StaleDirectPutCommitSnapshot);
        }
        if request.request.versioning == BucketVersioningState::Enabled
            && request.version_id.is_null()
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "versioned direct PUT commit requires reserved version id".to_string(),
            });
        }
        if request.request.versioning != BucketVersioningState::Enabled
            && !request.version_id.is_null()
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "unversioned direct PUT commit must use null version id".to_string(),
            });
        }

        let last_modified_millis = crate::clock::current_time_millis();
        let write_sequence = pg.next_object_write_sequence(
            request.request.bucket.as_str(),
            request.request.key.as_str(),
        )?;
        let stale_payload = if request.version_id.is_null() {
            snapshot_direct_put_stale_payload_command(
                &pg,
                &request.request.bucket,
                &request.request.key,
                last_modified_millis,
            )?
        } else {
            None
        };

        let segment_record = ObjectSegmentRecord {
            bucket: request.request.bucket.clone(),
            key: request.request.key.clone(),
            version_id: request.version_id,
            segment_index: request.request.segment_index,
            size: request.request.size,
            segment_crc64: request.request.segment_crc64,
            segment_okh: request.request.segment_okh,
            segment_vid: request.request.segment_vid,
            data_pg_id: request.request.data_pg_id,
            ec_k: request.request.ec.k,
            ec_m: request.request.ec.m,
        };
        let object = PutLiveObjectReq {
            bucket: request.request.bucket.clone(),
            key: request.request.key.clone(),
            version_id: request.version_id,
            owner: request.request.owner.clone(),
            acl_grants: request.request.acl_grants.clone(),
            public_read: request.request.public_read,
            generation_id: request.request.generation_id,
            size: request.request.size,
            etag: ObjectEtag::single_part(request.request.etag_crc64),
            ec: request.request.ec,
            layout: ObjectLayout::Standard,
            tags: request.request.tags.clone(),
            metadata_blob: Some(request.request.metadata_blob.clone()),
            system_metadata_blob: Some(request.request.system_metadata_blob.clone()),
            object_lock: request.request.object_lock,
            encryption: request.request.encryption.clone(),
        };
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object,
                segments: vec![segment_record],
                generation_reservation_id: request.request.generation_reservation_id.clone(),
                write_sequence,
                last_modified_millis,
                stale_payload,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                stream_create_bucket_write_reservation: None,
            })),
        ))
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        load_stream_put_finalize_snapshot_from_pg(&pg, bucket, key, session_id)
    }

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_stream_put_finalize_snapshot_from_pg(
            &pg,
            request.bucket,
            request.key,
            request.session_id,
        )?;
        if &current != request.expected_snapshot {
            return Err(ObjectPgActionError::StaleStreamFinalizeSnapshot);
        }
        let segments_total: u64 = current
            .staging_segments
            .iter()
            .map(|segment| segment.size)
            .sum();
        if segments_total != request.total_size {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "total_size mismatch: caller passed {} but staged segments sum to {segments_total}",
                    request.total_size
                ),
            });
        }

        let version_id = request.commit.version_id;
        if request.commit.versioning == BucketVersioningState::Enabled && version_id.is_null() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "versioned stream PUT commit requires reserved version id".to_string(),
            });
        }
        if request.commit.versioning != BucketVersioningState::Enabled && !version_id.is_null() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "unversioned stream PUT commit must use null version id".to_string(),
            });
        }
        let generation_id = current.generation_id;
        let last_modified_millis = crate::clock::current_time_millis();
        let write_sequence =
            pg.next_object_write_sequence(request.bucket.as_str(), request.key.as_str())?;
        let stale_payload = if version_id.is_null() {
            snapshot_direct_put_stale_payload_command(
                &pg,
                request.bucket,
                request.key,
                last_modified_millis,
            )?
        } else {
            None
        };
        let committed_segments: Vec<ObjectSegmentRecord> = current
            .staging_segments
            .iter()
            .map(|segment| ObjectSegmentRecord {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
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
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            version_id,
            owner: request.commit.owner.clone(),
            acl_grants: request.commit.acl_grants.clone(),
            public_read: request.commit.public_read,
            generation_id,
            size: request.commit.size,
            etag: ObjectEtag::single_part(request.commit.etag_crc64),
            ec: current.staging_segments.first().map_or(
                self.storage_node.default_ec_shape(),
                |segment| EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            ),
            layout: ObjectLayout::Standard,
            tags: request.commit.tags.clone(),
            metadata_blob: Some(request.commit.metadata_blob.clone()),
            system_metadata_blob: Some(request.commit.system_metadata_blob.clone()),
            object_lock: request.commit.object_lock,
            encryption: request.commit.encryption.clone(),
        };
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object,
                segments: committed_segments,
                generation_reservation_id: request.session_id.clone(),
                write_sequence,
                last_modified_millis,
                stale_payload,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                stream_create_bucket_write_reservation: current
                    .session
                    .bucket_write_reservation
                    .clone(),
            })),
        ))
    }

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        load_stream_part_finalize_snapshot_from_pg(
            &pg,
            bucket,
            key,
            upload_id,
            session_id,
            part_number,
        )
    }

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let current = load_stream_part_finalize_snapshot_from_pg(
            &pg,
            request.bucket,
            request.key,
            request.upload_id,
            request.session_id,
            request.part_number,
        )?;
        if &current != request.expected_snapshot {
            return Err(ObjectPgActionError::StaleStreamFinalizeSnapshot);
        }
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                session_id: request.session_id.clone(),
                upload: current.auth_snapshot.upload,
                part: request.part.clone(),
                segments: request.segments.to_vec(),
                existing_part: current.existing_part,
                displaced_segments: current.displaced_segments,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
            })),
        ))
    }

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(request.pg_id.get())?;
        let complete = request.request;
        let upload = PgMetadataStore::get_multipart_upload(&*pg, &complete.upload_id)?;
        if upload.bucket != complete.bucket
            || upload.key != complete.key
            || upload.state != UploadState::InProgress
        {
            return Err(MetadataError::NoSuchUpload {
                upload_id: complete.upload_id.to_string(),
            }
            .into());
        }
        if upload.object_generation_id != complete.generation_id {
            return Err(MetadataError::Db {
                context: "complete multipart command generation mismatch",
                source: rusqlite::Error::InvalidQuery,
            }
            .into());
        }
        if complete.part_records.is_empty() {
            return Err(MetadataError::Db {
                context: "complete multipart command empty parts",
                source: rusqlite::Error::InvalidQuery,
            }
            .into());
        }
        if complete.versioning == BucketVersioningState::Enabled && request.version_id.is_null() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "versioned multipart completion requires reserved version id".to_string(),
            });
        }
        if complete.versioning != BucketVersioningState::Enabled && !request.version_id.is_null() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "unversioned multipart completion must use null version id".to_string(),
            });
        }
        if request.version_id.is_null() {
            let (stale_payload_source, _) = snapshot_direct_put_stale_payload_for_snapshot(
                &pg,
                &complete.bucket,
                &complete.key,
                0,
            )?;
            if stale_payload_source != complete.expected_stale_payload_source {
                return Err(ObjectPgActionError::StaleMultipartCompletionSnapshot);
            }
        }

        let parts_len =
            u32::try_from(complete.part_records.len()).map_err(|_| MetadataError::Db {
                context: "complete multipart command too many parts",
                source: rusqlite::Error::InvalidQuery,
            })?;
        let parts_count = std::num::NonZeroU32::new(parts_len).ok_or(MetadataError::Db {
            context: "complete multipart command empty parts",
            source: rusqlite::Error::InvalidQuery,
        })?;
        let selected_part_numbers: std::collections::BTreeSet<u32> = complete
            .part_records
            .iter()
            .map(|part| part.part_number)
            .collect();
        for expected_part in &complete.part_records {
            let current_part = PgMetadataStore::get_multipart_part(
                &*pg,
                &complete.upload_id,
                expected_part.part_number,
            )
            .map_err(|error| match error {
                MetadataError::PartNotFound { .. } => {
                    ObjectPgActionError::StaleMultipartCompletionSnapshot
                }
                other => other.into(),
            })?;
            if current_part != *expected_part {
                return Err(ObjectPgActionError::StaleMultipartCompletionSnapshot);
            }
        }
        let object_parts = complete
            .part_records
            .iter()
            .map(|part| {
                let data_pg_id = self
                    .storage_node
                    .pg_topology()
                    .object_generation_multipart_part_data_pg(
                        &complete.bucket,
                        &complete.key,
                        complete.generation_id,
                        part.part_number,
                    )
                    .get();
                ObjectPartRecord {
                    bucket: complete.bucket.clone(),
                    key: complete.key.clone(),
                    version_id: request.version_id,
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
        if object_parts != request.expected_object_parts {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "complete multipart expected object parts do not match topology"
                    .to_string(),
            });
        }

        let (mut selected_streaming_segments, cleanup) =
            snapshot_complete_multipart_cleanup_from_pg(
                &pg,
                &complete.upload_id,
                &selected_part_numbers,
            )?;
        if selected_streaming_segments != complete.selected_streaming_segments
            || cleanup != complete.expected_cleanup
        {
            return Err(ObjectPgActionError::StaleMultipartCompletionSnapshot);
        }
        for segment in &mut selected_streaming_segments {
            segment.version_id = request.version_id.to_u64();
        }

        let last_modified_millis = crate::clock::current_time_millis();
        let completed_at_millis = last_modified_millis;
        let write_sequence =
            pg.next_object_write_sequence(complete.bucket.as_str(), complete.key.as_str())?;
        let stale_payload = if request.version_id.is_null() {
            snapshot_direct_put_stale_payload_command(
                &pg,
                &complete.bucket,
                &complete.key,
                last_modified_millis,
            )?
        } else {
            None
        };
        let command_id = self.next_metadata_command_id_from_locked_pg(
            request.pg_id,
            request.cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: complete.upload_id.clone(),
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                object: PutLiveObjectReq {
                    bucket: complete.bucket.clone(),
                    key: complete.key.clone(),
                    version_id: request.version_id,
                    owner: complete.owner.clone(),
                    acl_grants: complete.acl_grants.clone(),
                    public_read: complete.public_read,
                    generation_id: complete.generation_id,
                    size: complete.size,
                    etag: ObjectEtag::MultipartComposite {
                        crc64: complete.etag_crc64,
                        parts: parts_count,
                    },
                    ec: EcShape { k: 0, m: 0 },
                    layout: ObjectLayout::MultipartManifest { parts_count },
                    tags: complete.tags.clone(),
                    metadata_blob: complete.metadata_blob.clone(),
                    system_metadata_blob: complete.system_metadata_blob.clone(),
                    object_lock: complete.object_lock,
                    encryption: complete.encryption.clone(),
                },
                parts: object_parts,
                selected_streaming_segments,
                omitted_parts: cleanup.omitted_parts,
                omitted_streaming_segments: cleanup.omitted_streaming_segments,
                stream_uploads: cleanup.stream_uploads,
                stream_upload_segments: cleanup.stream_upload_segments,
                write_sequence,
                completion_order: request.completion_order,
                completed_at_millis,
                initiator: upload.initiator.clone(),
                last_modified_millis,
                stale_payload,
            })),
        ))
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_payload_reclaim_root(
            &*pg, bucket,
        )?)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_payload_reclaim_root(&*pg)?)
    }

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        if let Some(reclaim) =
            PgMetadataStore::get_object_segments_reclaim(&*pg, bucket, key, generation_id)?
        {
            Ok(Some(ObjectPayloadReclaimCommand::Segments(reclaim)))
        } else {
            Ok(
                PgMetadataStore::get_multipart_reclaim(&*pg, bucket, key, generation_id)?
                    .map(ObjectPayloadReclaimCommand::Multipart),
            )
        }
    }

    fn acquire_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_object_payload_reclaim_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_object_payload_reclaim_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.key,
            claim.generation_id,
            claim.reclaim_kind,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_delete_finalize_roots(
            &*pg, now, limit,
        )?)
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_bucket_delete_finalize_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_bucket_delete_finalize_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_lifecycle_sweep_roots(
            &*pg, now, limit,
        )?)
    }

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(LifecycleSweepBuckets {
            lifecycle_buckets: PgMetadataStore::list_buckets_with_lifecycle(&*pg)?,
            aborting_buckets: PgMetadataStore::list_buckets_with_aborting_multipart_uploads(&*pg)?,
        })
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_lifecycle_sweep_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::heartbeat_lifecycle_sweep_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
            heartbeat_at,
            lease_deadline,
        )?)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::record_lifecycle_sweep_claim_error(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
            last_error,
        )?)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_lifecycle_sweep_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }
}

impl MetadataCommandNodeClient for LocalStorageNodeClient {
    fn open_metadata_command_critical_section(
        &self,
        _pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandNodeClient>, StoreError> {
        Ok(Box::new(self.clone()))
    }

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.max_metadata_command_log_index(cluster_epoch)
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        self.next_metadata_command_id_from_locked_pg_at_least(
            pg_id,
            cluster_epoch,
            &pg,
            min_log_index,
        )
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.pending_metadata_command_envelope(self.node_id.as_u32(), cluster_epoch)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.try_insert_pending_metadata_command_slot(self.node_id.as_u32(), command, bucket)
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.try_insert_bucket_control_pending_metadata_command_slot(
            self.node_id.as_u32(),
            command,
            bucket,
        )
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.remove_pending_metadata_command_slot(self.node_id.as_u32(), command)
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.replace_pending_metadata_command_slot_for_reissue(
            self.node_id.as_u32(),
            previous,
            replacement,
            bucket,
        )
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_replica_state()
    }

    fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.validate_metadata_command_replay_state(self.node_id.as_u32(), cluster_epoch)
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.validate_metadata_command_replay_state_preserving_pending_slot(
            self.node_id.as_u32(),
            cluster_epoch,
        )
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_acceptance(self.node_id.as_u32(), command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_abandon_acceptance(self.node_id.as_u32(), command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.applied_metadata_command_log_entry_hashes(self.node_id.as_u32(), command)
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.retained_metadata_command_log_hashes(
            self.node_id.as_u32(),
            cluster_epoch,
            first_log_index,
            last_log_index,
        )
    }

    fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.retained_metadata_command_log_entries(
            self.node_id.as_u32(),
            cluster_epoch,
            first_log_index,
            last_log_index,
        )
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.has_matching_applied_metadata_command_log_entry(
            self.node_id.as_u32(),
            command,
            expected_previous_log_hash,
        )
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.apply_metadata_command_and_record(self.node_id.as_u32(), command)
    }

    fn replay_metadata_command_for_peering(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.apply_metadata_command_and_record(self.node_id.as_u32(), command)
    }

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_metadata_command_abandoned(self.node_id.as_u32(), command)
    }

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_abandoned(self.node_id.as_u32(), command)
    }
}
