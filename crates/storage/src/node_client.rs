use std::sync::Arc;

use placement::NodeId;

use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError};
use crate::metadata_command::{
    MetadataCommandAcceptance, MetadataCommandEnvelope, MetadataCommandReplicaState,
};
use crate::node::SharedStorageNode;
use crate::pg_store::ScavengerShardFileScan;
use crate::traits::PgMetadataStore;
use crate::types::{
    BucketName, BucketState, BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch,
    DataPgId, GenerationId, ObjectKey, PgId, ShardKey, WriteAck,
};

pub(crate) trait StorageNodeClient: Send + Sync {
    fn node_id(&self) -> NodeId;

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError>;

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Vec<u8>, StoreError>;

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError>;

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError>;

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError>;

    fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool;

    fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize;

    fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool;

    fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    );

    fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    );

    fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize;

    #[cfg(any(test, feature = "test-hooks"))]
    fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize;

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError>;

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError>;

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError>;

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError>;

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError>;

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError>;

    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError>;

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError>;

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError>;
}

pub(crate) struct LocalStorageNodeClient {
    node_id: NodeId,
    storage_node: Arc<SharedStorageNode>,
}

impl LocalStorageNodeClient {
    pub(crate) fn new(node_id: NodeId, storage_node: Arc<SharedStorageNode>) -> Self {
        Self {
            node_id,
            storage_node,
        }
    }
}

impl StorageNodeClient for LocalStorageNodeClient {
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
    ) -> Result<Vec<u8>, StoreError> {
        self.storage_node.read_shard_file(data_pg_id.get(), key)
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.storage_node
            .read_shard_file_into(data_pg_id.get(), key, dst)
    }

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
        self.storage_node.delete_shard_file(data_pg_id.get(), key)
    }

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        self.storage_node
            .list_scavenger_shard_files(data_pg_id.get())
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

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.max_metadata_command_log_index(cluster_epoch)
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

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_replica_state()
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
