// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    validate_metadata_command_recovery_certificate, AbortStreamUploadCommand,
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionMode,
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::BucketAclSummary;

struct LocalObjectPayloadLease {
    storage_node: Arc<SharedStorageNode>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    released: bool,
}

struct LocalObjectPayloadLeaseRoute {
    storage_node: Arc<SharedStorageNode>,
    route_cluster_epoch: ClusterEpoch,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

struct LocalRetainedObjectPayloadReclaimRoute {
    storage_node: Arc<SharedStorageNode>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    authority: ObjectPayloadReclaimClaimProof,
}

struct LocalRetainedPlacedShardRoute {
    storage_node: Arc<SharedStorageNode>,
    location: crate::cluster::ShardLocation,
    key: ShardKey,
}

struct LocalPlacedShardRoute {
    storage_node: Arc<SharedStorageNode>,
    location: crate::cluster::ShardLocation,
    key: ShardKey,
}

struct LocalShardReadHandleRoute {
    _route_cluster_epoch: ClusterEpoch,
    _read_operation_id: String,
    _entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
}

struct LocalRetainedShardAckRoute {
    storage_node: Arc<SharedStorageNode>,
    data_pg_id: DataPgId,
    key: ShardKey,
}

struct LocalShardAckRoute {
    storage_node: Arc<SharedStorageNode>,
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
}

struct LocalShardScavengerDataRoute {
    storage_node: Arc<SharedStorageNode>,
    _route_cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
}

struct LocalShardScavengerObjectScanRoute {
    storage_node: Arc<SharedStorageNode>,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataScanPgId,
}

struct LocalObjectReadMetadataRoute {
    storage_node: Arc<SharedStorageNode>,
    node_id: NodeId,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    authorization: MetadataReadAuthorization,
}

struct LocalObjectGenerationMetadataRoute {
    storage_node: Arc<SharedStorageNode>,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalObjectVersionMetadataRoute {
    storage_node: Arc<SharedStorageNode>,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalDirectPutMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalPutObjectMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalObjectDeleteMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalMultipartUploadCreationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalMultipartUploadLookupMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    authorization: MetadataReadAuthorization,
}

struct LocalAuthorizedMultipartUploadMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    authorized_upload: AuthorizedMultipartUploadRecord,
    authorization: MetadataReadAuthorization,
}

struct LocalMultipartCompletionMutationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalMultipartAbortMutationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
}

struct LocalObjectPayloadReclaimMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

struct LocalStreamUploadCreationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct LocalStreamUploadSessionMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    session_id: SessionId,
}

struct LocalStreamPutFinalizationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    session_id: SessionId,
}

struct LocalStreamPartFinalizationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
    session_id: SessionId,
    part_number: u32,
}

struct LocalObjectListingMetadataRoute {
    storage_node: Arc<SharedStorageNode>,
    node_id: NodeId,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataScanPgId,
    authorization: MetadataReadAuthorization,
}

struct LocalObjectMutationScanMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataScanPgId,
}

struct LocalBucketMetadataScanRoute {
    storage_node: Arc<SharedStorageNode>,
    node_id: NodeId,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    authorization: MetadataReadAuthorization,
}

struct LocalBucketWriteReservationScanRoute<'a> {
    client: &'a LocalStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
}

struct LocalBucketMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
    authorization: MetadataReadAuthorization,
}

struct LocalBucketDeleteReplicaMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
}

impl ObjectPayloadLeaseNodeLease for LocalObjectPayloadLease {
    fn release(&mut self) -> Result<usize, StoreError> {
        if self.released {
            return Ok(0);
        }
        let remaining = self.storage_node.release_object_payload_lease(
            &self.bucket,
            &self.key,
            self.generation_id,
        );
        self.released = true;
        Ok(remaining)
    }
}

impl Drop for LocalObjectPayloadLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl LocalStorageNodeClient {
    pub(crate) fn new(node_id: NodeId, storage_node: Arc<SharedStorageNode>) -> Self {
        Self {
            node_id,
            storage_node,
        }
    }

    fn prepare_stream_segment_append_inner(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_fence: Option<AdmittedRouteEffectFence>,
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
                if let Some(effect_fence) = effect_fence {
                    effect_fence.require_valid_for(effect_fence.cluster_epoch())?;
                }
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
                if let Some(effect_fence) = effect_fence {
                    effect_fence.require_valid_for(effect_fence.cluster_epoch())?;
                }
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
            payload_crc64: request.payload_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            placement_cluster_epoch: ClusterEpoch::INITIAL,
            ec_k: ec.k,
            ec_m: ec.m,
        };
        Ok((session.target, segment_record))
    }

    fn next_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        pg: &PgStore,
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
        pg: &PgStore,
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

fn combined_stream_segment_crc64(segments: &[StreamUploadSegmentRecord]) -> u64 {
    segments
        .iter()
        .fold(checksum::crc64::checksum(&[]), |crc64, segment| {
            checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
        })
}

impl PlacedShardNodeClient for LocalStorageNodeClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn open_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn PlacedShardRoute + '_>, StoreError> {
        if location.node_id() != self.node_id {
            return Err(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            });
        }
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open active placed shard route",
            });
        }
        drop(self.storage_node.get_pg(location.data_pg_id().get())?);
        Ok(Box::new(LocalPlacedShardRoute {
            storage_node: Arc::clone(&self.storage_node),
            location,
            key: key.clone(),
        }))
    }
}

impl PlacedShardRoute for LocalPlacedShardRoute {
    fn write_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError> {
        self.storage_node
            .write_shard_file(self.location.data_pg_id().get(), &self.key, data)
    }

    fn write_placed_shard_with_effect_fence(
        &self,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StoreError> {
        effect_fence.require_valid_for(self.location.cluster_epoch())?;
        self.storage_node
            .write_shard_file(self.location.data_pg_id().get(), &self.key, data)
    }

    fn repair_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError> {
        self.storage_node
            .write_shard_file(self.location.data_pg_id().get(), &self.key, data)
    }

    fn read_placed_shard(&self, _expected_ack: WriteAck) -> Result<Vec<u8>, StoreError> {
        self.storage_node
            .read_shard_file(self.location.data_pg_id().get(), &self.key)
    }

    fn read_placed_shard_into(
        &self,
        _expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.storage_node
            .read_shard_file_into(self.location.data_pg_id().get(), &self.key, dst)
    }

    fn delete_placed_shard(&self) -> Result<(), StoreError> {
        self.storage_node
            .delete_shard_file(self.location.data_pg_id().get(), &self.key)
    }
}

impl RetainedPlacedShardNodeClient for LocalStorageNodeClient {
    fn open_retained_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedPlacedShardRoute + '_>, StoreError> {
        if location.node_id() != self.node_id {
            return Err(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            });
        }
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained placed shard route",
            });
        }
        drop(self.storage_node.get_pg(location.data_pg_id().get())?);
        Ok(Box::new(LocalRetainedPlacedShardRoute {
            storage_node: Arc::clone(&self.storage_node),
            location,
            key: key.clone(),
        }))
    }
}

impl RetainedPlacedShardRoute for LocalRetainedPlacedShardRoute {
    fn read_placed_shard_for_historical_inspection(
        &self,
    ) -> Result<(Vec<u8>, WriteAck), StoreError> {
        let payload = self
            .storage_node
            .read_shard_file(self.location.data_pg_id().get(), &self.key)?;
        let ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(&payload),
        };
        Ok((payload, ack))
    }

    fn delete_placed_shard_for_historical_cleanup(&self) -> Result<(), StoreError> {
        self.storage_node
            .delete_shard_file(self.location.data_pg_id().get(), &self.key)
    }
}

impl ShardReadHandleNodeClient for LocalStorageNodeClient {
    fn open_shard_read_handle_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleRoute + '_>, StoreError> {
        if entries.is_empty() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open shard read-handle route",
            });
        }
        for (location, key) in &entries {
            if location.node_id() != self.node_id
                || location.cluster_epoch() != route_cluster_epoch
                || location.shard_index() != key.shard_index()
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "open shard read-handle route",
                });
            }
            drop(self.storage_node.get_pg(location.data_pg_id().get())?);
        }
        Ok(Box::new(LocalShardReadHandleRoute {
            _route_cluster_epoch: route_cluster_epoch,
            _read_operation_id: read_operation_id.to_string(),
            _entries: entries,
        }))
    }
}

impl ShardReadHandleRoute for LocalShardReadHandleRoute {
    fn acquire(self: Box<Self>) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        Ok(Box::new(LocalStorageNodeReadHandleLease))
    }
}

impl ObjectPayloadLeaseNodeClient for LocalStorageNodeClient {
    fn open_object_payload_lease_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadLeaseRoute + '_>, StoreError> {
        Ok(Box::new(LocalObjectPayloadLeaseRoute {
            storage_node: Arc::clone(&self.storage_node),
            route_cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }))
    }
}

impl ObjectPayloadLeaseRoute for LocalObjectPayloadLeaseRoute {
    fn acquire_object_payload_lease(
        &self,
        _kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        if !self.storage_node.try_acquire_object_payload_lease(
            &self.bucket,
            &self.key,
            self.generation_id,
        ) {
            return Ok(None);
        }
        Ok(Some(Box::new(LocalObjectPayloadLease {
            storage_node: Arc::clone(&self.storage_node),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation_id: self.generation_id,
            released: false,
        })))
    }

    fn try_begin_object_payload_reclaim(
        &self,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError> {
        if authority.cluster_epoch != self.route_cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "begin object payload reclaim",
            });
        }
        Ok(self.storage_node.try_begin_object_payload_reclaim(
            &self.bucket,
            &self.key,
            self.generation_id,
            authority,
        ))
    }

    fn object_payload_lease_count(&self) -> Result<usize, StoreError> {
        Ok(self.storage_node.object_payload_lease_count(
            &self.bucket,
            &self.key,
            self.generation_id,
        ))
    }
}

impl RetainedObjectPayloadReclaimNodeClient for LocalStorageNodeClient {
    fn open_retained_object_payload_reclaim_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<Box<dyn RetainedObjectPayloadReclaimRoute + '_>, StoreError> {
        if authority.cluster_epoch != route_cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained object payload reclaim route",
            });
        }
        Ok(Box::new(LocalRetainedObjectPayloadReclaimRoute {
            storage_node: Arc::clone(&self.storage_node),
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            authority: authority.clone(),
        }))
    }
}

impl RetainedObjectPayloadReclaimRoute for LocalRetainedObjectPayloadReclaimRoute {
    fn finish_object_payload_reclaim(&self, keep_fence: bool) -> Result<(), StoreError> {
        self.storage_node
            .finish_object_payload_reclaim(
                &self.bucket,
                &self.key,
                self.generation_id,
                &self.authority,
                keep_fence,
            )
            .then_some(())
            .ok_or(StoreError::ObjectPayloadReclaimFenceAuthorityMismatch)
    }

    fn clear_object_payload_reclaim_fence(&self) -> Result<(), StoreError> {
        self.storage_node
            .clear_object_payload_reclaim_fence(
                &self.bucket,
                &self.key,
                self.generation_id,
                &self.authority,
            )
            .then_some(())
            .ok_or(StoreError::ObjectPayloadReclaimFenceAuthorityMismatch)
    }
}

impl ShardReadHandleLease for LocalStorageNodeReadHandleLease {
    fn release(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl ShardAckNodeClient for LocalStorageNodeClient {
    fn open_shard_ack_route(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardAckRoute + '_>, StoreError> {
        drop(self.storage_node.get_pg(data_pg_id.get())?);
        Ok(Box::new(LocalShardAckRoute {
            storage_node: Arc::clone(&self.storage_node),
            cluster_epoch,
            data_pg_id,
        }))
    }
}

impl ShardAckRoute for LocalShardAckRoute {
    fn register_shard_acks(&self, shard_batch: &[(&ShardKey, WriteAck)]) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.register_written_shards_batch_exact(shard_batch)
    }

    fn validate_shard_ack(&self, key: &ShardKey, ack: WriteAck) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.validate_written_shard_ack(key, ack)
    }

    fn load_shard_ack(&self, key: &ShardKey) -> Result<WriteAck, StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let stat = pg.stat_shard(key)?;
        Ok(WriteAck {
            crc64: stat.crc64,
            stored_size: stat.size,
        })
    }

    fn delete_shard_ack(&self, key: &ShardKey) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.delete_shard_record(key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.record_placed_segment_shard_repair(work_item, last_error)
    }

    fn list_placed_segment_shard_repairs(
        &self,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let repairs = pg.list_placed_segment_shard_repairs()?;
        for repair in &repairs {
            validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &repair.work_item)?;
        }
        Ok(repairs)
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let claim = pg.acquire_placed_segment_shard_repair_claim(request)?;
        if let Some(claim) = &claim {
            validate_placed_segment_shard_repair_claim_epoch(
                self.data_pg_id.pg_id(),
                self.cluster_epoch,
                claim,
            )?;
            validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        }
        Ok(claim)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_epoch(
            self.data_pg_id.pg_id(),
            self.cluster_epoch,
            claim,
        )?;
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.complete_placed_segment_shard_repair_claim(claim)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_epoch(
            self.data_pg_id.pg_id(),
            self.cluster_epoch,
            claim,
        )?;
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.record_placed_segment_shard_repair_claim_error(claim, last_error, next_attempt_after)
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.resolve_placed_segment_shard_repair(work_item)
            .map(|_| ())
    }

    fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.record_placed_segment_shard_backfill(work_item, remaining_tolerance, last_error)
    }

    fn list_placed_segment_shard_backfills(
        &self,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let backfills = pg.list_placed_segment_shard_backfills()?;
        for backfill in &backfills {
            validate_placed_segment_shard_backfill_route(
                self.data_pg_id.pg_id(),
                &backfill.work_item,
            )?;
        }
        Ok(backfills)
    }

    fn count_placed_segment_shard_backfills(&self) -> Result<usize, StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.placed_segment_shard_backfill_count()
    }

    fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.placed_segment_shard_backfill_exists(work_item)
    }

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let claim = pg.acquire_placed_segment_shard_backfill_claim(request)?;
        if let Some(claim) = &claim {
            validate_placed_segment_shard_backfill_claim_epoch(
                self.data_pg_id.pg_id(),
                self.cluster_epoch,
                claim,
            )?;
            validate_placed_segment_shard_backfill_route(
                self.data_pg_id.pg_id(),
                &claim.work_item,
            )?;
        }
        Ok(claim)
    }

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_claim_epoch(
            self.data_pg_id.pg_id(),
            self.cluster_epoch,
            claim,
        )?;
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.complete_placed_segment_shard_backfill_claim(claim)
    }

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_claim_epoch(
            self.data_pg_id.pg_id(),
            self.cluster_epoch,
            claim,
        )?;
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.record_placed_segment_shard_backfill_claim_error(claim, last_error, next_attempt_after)
    }

    fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.resolve_placed_segment_shard_backfill(work_item)
            .map(|_| ())
    }
}

impl RetainedShardAckNodeClient for LocalStorageNodeClient {
    fn open_retained_shard_ack_route(
        &self,
        _route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedShardAckRoute + '_>, StoreError> {
        drop(self.storage_node.get_pg(data_pg_id.get())?);
        Ok(Box::new(LocalRetainedShardAckRoute {
            storage_node: Arc::clone(&self.storage_node),
            data_pg_id,
            key: key.clone(),
        }))
    }
}

impl RetainedShardAckRoute for LocalRetainedShardAckRoute {
    fn load_written_shard_ack_for_historical_inspection(&self) -> Result<WriteAck, StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        let stat = pg.stat_shard(&self.key)?;
        Ok(WriteAck {
            crc64: stat.crc64,
            stored_size: stat.size,
        })
    }

    fn delete_retained_shard_ack(&self) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(self.data_pg_id.get())?;
        pg.delete_shard_record(&self.key)
    }
}

impl ShardScavengerNodeClient for LocalStorageNodeClient {
    #[cfg(test)]
    fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError> {
        self.storage_node.cluster_map_history_route_references()
    }

    fn open_shard_scavenger_data_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerDataRoute + '_>, StoreError> {
        self.storage_node.require_open_pg(data_pg_id.get())?;
        Ok(Box::new(LocalShardScavengerDataRoute {
            storage_node: Arc::clone(&self.storage_node),
            _route_cluster_epoch: route_cluster_epoch,
            data_pg_id,
        }))
    }

    fn open_shard_scavenger_object_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ShardScavengerObjectScanRoute + '_>, StoreError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalShardScavengerObjectScanRoute {
            storage_node: Arc::clone(&self.storage_node),
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
        }))
    }
}

impl ShardScavengerDataRoute for LocalShardScavengerDataRoute {
    fn list_scavenger_shard_files(&self) -> Result<ScavengerShardFileScan, StoreError> {
        self.storage_node
            .list_scavenger_shard_files(self.data_pg_id.get())
    }

    fn list_scavenger_shard_rows(&self) -> Result<Vec<ScavengerShardRow>, StoreError> {
        self.storage_node
            .get_pg(self.data_pg_id.get())?
            .list_scavenger_shard_rows()
    }
}

impl ShardScavengerObjectScanRoute for LocalShardScavengerObjectScanRoute {
    fn list_placed_segment_backfill_reference_page(
        &self,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<PlacedSegmentBackfillReferencePage, StoreError> {
        self.storage_node
            .get_pg(self.pg_id.get())?
            .list_placed_segment_backfill_reference_page(after, limit)
    }

    fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let pg = self.storage_node.get_pg(self.pg_id.get())?;
        pg.list_shard_scavenger_payload_references()
    }
}

struct LocalShardScavengerObservationRoute<'a> {
    client: &'a LocalStorageNodeClient,
    data_pg_id: DataPgId,
}

impl LocalShardScavengerObservationRoute<'_> {
    fn validate_key(&self, key: &ShardScavengerObservationKey) -> Result<(), StoreError> {
        if key.data_pg_id != self.data_pg_id.get() {
            return Err(StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: self.data_pg_id.get(),
                observation_pg_id: key.data_pg_id,
            });
        }
        Ok(())
    }
}

impl ShardScavengerObservationNodeClient for LocalStorageNodeClient {
    fn open_shard_scavenger_observation_route(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerObservationRoute + '_>, StoreError> {
        drop(self.storage_node.get_pg(data_pg_id.get())?);
        Ok(Box::new(LocalShardScavengerObservationRoute {
            client: self,
            data_pg_id,
        }))
    }
}

impl ShardScavengerObservationRoute for LocalShardScavengerObservationRoute<'_> {
    fn record_shard_scavenger_observation(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_key(&observation.key)?;
        let pg = self.client.storage_node.get_pg(self.data_pg_id.get())?;
        pg.record_shard_scavenger_observation(observation)
    }

    fn list_shard_scavenger_observations(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let pg = self.client.storage_node.get_pg(self.data_pg_id.get())?;
        let observations = pg.list_shard_scavenger_observations()?;
        for observation in &observations {
            self.validate_key(&observation.key)?;
        }
        Ok(observations)
    }

    fn resolve_shard_scavenger_observation(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        self.validate_key(key)?;
        let pg = self.client.storage_node.get_pg(self.data_pg_id.get())?;
        pg.resolve_shard_scavenger_observation(key).map(|_| ())
    }
}

impl LocalStorageNodeClient {
    fn head_bucket_replica_for_delete(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_raw(pg_id, bucket)
    }

    fn head_bucket_raw(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket_raw(&*pg, bucket)?)
    }

    fn build_create_bucket_command(
        &self,
        pg_id: BucketPgId,
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
            CreateBucketCommandBuildAuthority::new(),
            config,
            crate::clock::current_time_millis(),
            bucket_execution_generation,
        )
        .map_err(|reason| MetadataError::InvalidBucketName { reason })?;
        Ok(CreateBucketCommandBuild::Command(Box::new(
            MetadataCommandEnvelope::new(command_id, MetadataCommandPayload::CreateBucket(command)),
        )))
    }

    fn build_advance_multipart_completion_barrier_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        if &bucket_write_reservation.bucket != bucket {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: bucket_write_reservation.reservation_id.clone(),
            }
            .into());
        }
        if bucket_write_reservation.operation_kind
            != COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
            || bucket_write_reservation.target_context.as_deref() != Some(completion_target_context)
        {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: bucket_write_reservation.reservation_id.clone(),
            }
            .into());
        }
        self.validate_bucket_write_reservation_proof(pg_id, bucket_write_reservation)?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current_sequence = pg.multipart_completion_barrier_sequence_for_bucket(bucket)?;
        let barrier_sequence =
            current_sequence
                .checked_add(1)
                .ok_or_else(|| MetadataError::InvariantViolation {
                    context: "reserve multipart completion barrier overflow",
                    reason: "multipart completion barrier sequence overflow".into(),
                })?;
        i64::try_from(barrier_sequence).map_err(|_| MetadataError::InvariantViolation {
            context: "reserve multipart completion barrier overflow",
            reason: "multipart completion barrier sequence exceeds the durable integer range"
                .into(),
        })?;
        Ok((
            barrier_sequence,
            MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
                    AdvanceMultipartCompletionBarrierCommand {
                        bucket: bucket.clone(),
                        barrier_sequence,
                    },
                ),
            ),
        ))
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(PutBucketAclCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                acl_grants.clone(),
                summary,
            )
            .bucket)
        })
    }

    fn build_put_bucket_acl_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                current.with_execution_generation(bucket_execution_generation),
                acl_grants.clone(),
                summary,
            )),
        ))
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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

    fn open_bucket_metadata_scan_route_impl(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalBucketMetadataScanRoute {
            storage_node: Arc::clone(&self.storage_node),
            node_id: self.node_id,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorization,
        }))
    }
}

impl BucketMetadataNodeClient for LocalStorageNodeClient {
    fn open_bucket_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open bucket metadata route",
            }
            .into());
        }
        Ok(Box::new(LocalBucketMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            authorization: MetadataReadAuthorization::active(pg_id.pg_id()),
        }))
    }

    fn open_bucket_metadata_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn BucketMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open bucket metadata read route",
            }
            .into());
        }
        Ok(Box::new(LocalBucketMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            authorization,
        }))
    }

    fn open_bucket_delete_replica_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketDeleteReplicaMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open bucket delete replica metadata route",
            }
            .into());
        }
        Ok(Box::new(LocalBucketDeleteReplicaMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_bucket_metadata_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        self.open_bucket_metadata_scan_route_impl(
            route_cluster_epoch,
            pg_id,
            MetadataReadAuthorization::active(pg_id.pg_id()),
        )
    }

    fn open_bucket_metadata_read_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        self.open_bucket_metadata_scan_route_impl(route_cluster_epoch, pg_id, authorization)
    }
}

impl LocalBucketMetadataRoute<'_> {
    fn require_mutation_authority(
        &self,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if self.authorization.is_active() {
            return Ok(());
        }
        Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into())
    }

    fn require_command_id(
        &self,
        command_id: MetadataCommandId,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_mutation_authority(operation)?;
        if command_id.cluster_epoch() != self.route_cluster_epoch
            || command_id.pg_id() != self.pg_id.pg_id()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_mutation_authority(operation)?;
        if bucket != &self.bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl BucketMetadataRoute for LocalBucketMetadataRoute<'_> {
    fn head_bucket_raw(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        let pg = self.client.storage_node.get_pg_for_metadata_read(
            self.client.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        Ok(PgMetadataStore::head_bucket_raw(&*pg, &self.bucket)?)
    }

    fn head_bucket_info(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        let pg = self.client.storage_node.get_pg_for_metadata_read(
            self.client.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        Ok(PgMetadataStore::head_bucket(&*pg, &self.bucket)?)
    }

    fn load_bucket_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        let pg = self.client.storage_node.get_pg_for_metadata_read(
            self.client.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        SharedStorageNode::load_bucket_snapshot_from_pg(&pg, &self.bucket, request)
    }

    fn build_create_bucket_command(
        &self,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build create bucket command")?;
        if config.name != self.bucket.as_str() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create bucket command",
            }
            .into());
        }
        self.client
            .build_create_bucket_command(self.pg_id, &self.bucket, command_id, config)
    }

    fn build_advance_multipart_completion_barrier_command(
        &self,
        command_id: MetadataCommandId,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build multipart completion barrier command")?;
        self.client
            .build_advance_multipart_completion_barrier_command(
                self.pg_id,
                &self.bucket,
                command_id,
                completion_target_context,
                bucket_write_reservation,
            )
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending mark bucket deleting command",
        )?;
        self.client
            .pending_mark_bucket_deleting_command_matches_current(self.pg_id, &self.bucket, command)
    }

    fn build_mark_bucket_deleting_command(
        &self,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build mark bucket deleting command")?;
        self.client
            .build_mark_bucket_deleting_command(self.pg_id, &self.bucket, command_id)
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket versioning command",
        )?;
        self.client
            .pending_put_bucket_versioning_command_matches_current(
                self.pg_id,
                &self.bucket,
                command,
                state,
            )
    }

    fn build_put_bucket_versioning_command(
        &self,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket versioning command")?;
        self.client
            .build_put_bucket_versioning_command(self.pg_id, &self.bucket, command_id, state)
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket ACL command",
        )?;
        self.client.pending_put_bucket_acl_command_matches_current(
            self.pg_id,
            &self.bucket,
            command,
            acl_grants,
            summary,
        )
    }

    fn build_put_bucket_acl_command(
        &self,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket ACL command")?;
        self.client.build_put_bucket_acl_command(
            self.pg_id,
            &self.bucket,
            command_id,
            acl_grants,
            summary,
        )
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket property command",
        )?;
        self.client
            .pending_put_bucket_property_command_matches_current(
                self.pg_id,
                &self.bucket,
                command,
                mutation,
            )
    }

    fn build_put_bucket_property_command(
        &self,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket property command")?;
        self.client.build_put_bucket_property_command(
            self.pg_id,
            &self.bucket,
            command_id,
            mutation,
        )
    }

    fn build_put_bucket_subresource_command(
        &self,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket subresource command")?;
        self.client.build_put_bucket_subresource_command(
            self.pg_id,
            &self.bucket,
            command_id,
            mutation,
        )
    }

    fn get_bucket_subresource(
        &self,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        let pg = self.client.storage_node.get_pg_for_metadata_read(
            self.client.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        Ok(
            PgMetadataStore::get_bucket_subresource(&*pg, &self.bucket, kind)?
                .map(|stored| stored.body),
        )
    }
}

impl BucketDeleteReplicaMetadataRoute for LocalBucketDeleteReplicaMetadataRoute<'_> {
    fn head_bucket_replica_for_delete(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.client
            .head_bucket_replica_for_delete(self.pg_id, &self.bucket)
    }
}

impl LocalBucketMetadataScanRoute {
    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), StoreError> {
        if self.storage_node.bucket_metadata_pg_for(bucket) != self.pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation });
        }
        Ok(())
    }
}

impl BucketMetadataScanRoute for LocalBucketMetadataScanRoute {
    fn list_buckets(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let buckets = PgMetadataStore::list_buckets(&*pg, owner_canonical_id)?;
        for bucket in &buckets {
            if bucket.owner_canonical_id.as_str() != owner_canonical_id {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "list buckets for owner",
                }
                .into());
            }
            self.require_bucket(&bucket.name, "list buckets for owner")?;
        }
        Ok(buckets)
    }

    fn load_bucket_execution_generations(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        for bucket in buckets {
            self.require_bucket(bucket, "load bucket execution generations")?;
        }
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let generations = pg.load_bucket_execution_generations(buckets)?;
        for bucket in generations.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "load bucket execution generations",
                }
                .into());
            }
            self.require_bucket(bucket, "load bucket execution generations")?;
        }
        Ok(generations)
    }

    fn load_bucket_fast_path_identities(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        for bucket in buckets {
            self.require_bucket(bucket, "load bucket fast-path identities")?;
        }
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let identities = pg.load_bucket_fast_path_identities(buckets)?;
        for bucket in identities.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "load bucket fast-path identities",
                }
                .into());
            }
            self.require_bucket(bucket, "load bucket fast-path identities")?;
        }
        Ok(identities)
    }
}

impl LocalStorageNodeClient {
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_drain(&*pg, bucket)?.is_some())
    }

    fn durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_drain(&*pg, bucket)?)
    }

    fn record_bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::record_bucket_delete_attempt_outcome(
            &*pg, record,
        )?)
    }

    fn bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::bucket_delete_attempt_outcome(
            &*pg, bucket,
        )?)
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg, acquire,
        )?)
    }

    fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        effect_fence.require_valid_for(acquire.cluster_epoch)?;
        Ok(PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg, acquire,
        )?)
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
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
        if record.lease_deadline <= crate::clock::current_time_millis() {
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

    #[allow(clippy::too_many_arguments)] // Private adapter mirrors the durable drain record fields.
    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
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

    #[allow(clippy::too_many_arguments)] // Private adapter adds the effect authority to that record.
    fn begin_durable_bucket_write_drain_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(cluster_epoch)?;
        self.begin_durable_bucket_write_drain(
            pg_id,
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            created_at,
            lease_deadline,
        )
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_reservations(
            &*pg, bucket,
        )?)
    }

    fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        effect_fence.require_valid_for(effect_fence.cluster_epoch())?;
        Ok(PgMetadataStore::heartbeat_durable_bucket_write_reservation(
            &*pg,
            DurableBucketWriteReservationHeartbeat {
                name: &proof.bucket,
                reservation_id: &proof.reservation_id,
                owner_token: &proof.owner_token,
                cluster_epoch: proof.cluster_epoch,
                bucket_execution_generation: proof.bucket_execution_generation,
                bucket_incarnation_generation: proof.bucket_incarnation_generation,
                current_lease_deadline: proof.lease_deadline,
                lease_deadline,
                now: crate::clock::current_time_millis(),
            },
        )?)
    }
}

struct LocalBucketWriteReservationRoute<'a> {
    client: &'a LocalStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
}

impl LocalBucketWriteReservationRoute<'_> {
    fn require_bucket_subject(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if bucket != &self.bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_current_subject(
        &self,
        bucket: &BucketName,
        cluster_epoch: ClusterEpoch,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(bucket, operation)?;
        if cluster_epoch != self.route_cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_claim_subject(
        &self,
        bucket: &BucketName,
        cluster_epoch: ClusterEpoch,
        pg_id: u32,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_current_subject(bucket, cluster_epoch, operation)?;
        if pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl BucketWriteReservationNodeClient for LocalStorageNodeClient {
    fn open_bucket_write_reservation_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open bucket write reservation route",
            }
            .into());
        }
        Ok(Box::new(LocalBucketWriteReservationRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_bucket_write_reservation_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketWriteReservationScanRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalBucketWriteReservationScanRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
        }))
    }
}

impl LocalBucketWriteReservationScanRoute<'_> {
    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if self.client.storage_node.bucket_metadata_pg_for(bucket) != self.pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl BucketWriteReservationScanRoute for LocalBucketWriteReservationScanRoute<'_> {
    fn get_bucket_delete_finalize_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let roots = self
            .client
            .get_bucket_delete_finalize_roots(self.pg_id, now, limit)?;
        for root in &roots {
            self.require_bucket(&root.bucket, "get bucket delete finalize roots")?;
        }
        Ok(roots)
    }

    fn get_bucket_delete_begin_roots(
        &self,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, BucketSnapshotLoadError> {
        if let Some(bucket) = start_after_bucket {
            self.require_bucket(bucket, "get bucket delete begin roots")?;
        }
        let roots = self.client.get_bucket_delete_begin_roots(
            self.pg_id,
            now,
            start_after_bucket,
            limit,
        )?;
        for root in &roots {
            self.require_bucket(&root.bucket, "get bucket delete begin roots")?;
        }
        Ok(roots)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let roots = self
            .client
            .get_lifecycle_sweep_roots(self.pg_id, now, limit)?;
        for root in &roots {
            self.require_bucket(&root.bucket, "get lifecycle sweep roots")?;
        }
        Ok(roots)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let buckets = self.client.list_buckets_with_lifecycle(self.pg_id)?;
        for bucket in &buckets {
            self.require_bucket(&bucket.name, "list lifecycle sweep buckets")?;
        }
        Ok(buckets)
    }
}

impl BucketWriteReservationRoute for LocalBucketWriteReservationRoute<'_> {
    fn durable_bucket_write_drain_exists(&self) -> Result<bool, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_drain_exists(self.pg_id, &self.bucket)
    }

    fn durable_bucket_write_drain(
        &self,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_drain(self.pg_id, &self.bucket)
    }

    fn record_bucket_delete_attempt_outcome(
        &self,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(&record.bucket, "record bucket delete attempt outcome")?;
        self.client
            .record_bucket_delete_attempt_outcome(self.pg_id, record)
    }

    fn bucket_delete_attempt_outcome(
        &self,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketSnapshotLoadError> {
        self.client
            .bucket_delete_attempt_outcome(self.pg_id, &self.bucket)
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire durable bucket write reservation",
        )?;
        self.client
            .acquire_durable_bucket_write_reservation(self.pg_id, acquire)
    }

    fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire durable bucket write reservation",
        )?;
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .acquire_durable_bucket_write_reservation_with_effect_fence(
                self.pg_id,
                acquire,
                effect_fence,
            )
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(&proof.bucket, "validate bucket write reservation proof")?;
        self.client
            .validate_bucket_write_reservation_proof(self.pg_id, proof)
    }

    fn validate_bucket_write_reservation_proof_until(
        &self,
        proof: &BucketWriteReservationProof,
        deadline: Instant,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(&proof.bucket, "validate bucket write reservation proof")?;
        require_metadata_command_operation_deadline(deadline)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
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
        if !proof.matches_record(&record)
            || record.lease_deadline <= crate::clock::current_time_millis()
        {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        let current_bucket = PgMetadataStore::head_bucket_raw(&*pg, &proof.bucket)?;
        require_metadata_command_operation_deadline(deadline)?;
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

    fn begin_durable_bucket_write_drain(
        &self,
        drain_id: &str,
        owner_token: &str,
        created_at: u64,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        self.client.begin_durable_bucket_write_drain(
            self.pg_id,
            &self.bucket,
            drain_id,
            owner_token,
            self.route_cluster_epoch,
            created_at,
            lease_deadline,
        )
    }

    fn begin_durable_bucket_write_drain_with_effect_fence(
        &self,
        drain_id: &str,
        owner_token: &str,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .begin_durable_bucket_write_drain_with_effect_fence(
                self.pg_id,
                &self.bucket,
                drain_id,
                owner_token,
                self.route_cluster_epoch,
                created_at,
                lease_deadline,
                effect_fence,
            )
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        self.client
            .clear_expired_durable_bucket_write_drain(self.pg_id, &self.bucket, now)
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        self.require_bucket_subject(&record.bucket, "heartbeat durable bucket write drain")?;
        self.client
            .heartbeat_durable_bucket_write_drain(self.pg_id, record, lease_deadline)
    }

    fn durable_bucket_write_reservations(
        &self,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_reservations(self.pg_id, &self.bucket)
    }

    fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_bucket_subject(&proof.bucket, "heartbeat durable bucket write reservation")?;
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                self.pg_id,
                proof,
                lease_deadline,
                effect_fence,
            )
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        self.client.acquire_bucket_delete_finalize_claim(
            self.pg_id,
            &self.bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            self.route_cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn bucket_delete_finalize_claim(
        &self,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        self.client
            .bucket_delete_finalize_claim(self.pg_id, &self.bucket)
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        self.client.acquire_lifecycle_sweep_claim(
            self.pg_id,
            &self.bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            self.route_cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.cluster_epoch,
            claim.pg_id,
            "heartbeat lifecycle sweep claim",
        )?;
        self.client
            .heartbeat_lifecycle_sweep_claim(self.pg_id, claim, heartbeat_at, lease_deadline)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.cluster_epoch,
            claim.pg_id,
            "record lifecycle sweep claim error",
        )?;
        self.client
            .record_lifecycle_sweep_claim_error(self.pg_id, claim, last_error)
    }
}

struct LocalRetainedBucketWriteReservationRoute<'a> {
    client: &'a LocalStorageNodeClient,
    pg_id: BucketPgId,
    bucket: BucketName,
}

impl LocalRetainedBucketWriteReservationRoute<'_> {
    fn require_subject(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if bucket != &self.bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_claim_subject(
        &self,
        bucket: &BucketName,
        pg_id: u32,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(bucket, operation)?;
        if pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl RetainedBucketWriteReservationNodeClient for LocalStorageNodeClient {
    fn open_retained_bucket_write_reservation_route(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn RetainedBucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained bucket write reservation route",
            }
            .into());
        }
        drop(self.storage_node.get_pg(pg_id.get())?);
        Ok(Box::new(LocalRetainedBucketWriteReservationRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_retained_bucket_write_reservation_route_until(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        deadline: Instant,
    ) -> Result<Box<dyn RetainedBucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        if self.storage_node.bucket_metadata_pg_for(bucket) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained bucket write reservation route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(Box::new(LocalRetainedBucketWriteReservationRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
        }))
    }
}

impl RetainedBucketWriteReservationRoute for LocalRetainedBucketWriteReservationRoute<'_> {
    fn release_durable_bucket_write_reservation(
        &self,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(&record.bucket, "release durable bucket write reservation")?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::release_durable_bucket_write_reservation(
            &*pg, record,
        )?)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(
            &proof.bucket,
            "release metadata command bucket write reservation",
        )?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
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
        Ok(PgMetadataStore::release_metadata_command_bucket_write_reservation(&*pg, proof)?)
    }

    fn release_metadata_command_bucket_write_reservation_until(
        &self,
        proof: &BucketWriteReservationProof,
        deadline: Instant,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(
            &proof.bucket,
            "release metadata command bucket write reservation",
        )?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
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
        require_metadata_command_operation_deadline(deadline)?;
        Ok(PgMetadataStore::release_metadata_command_bucket_write_reservation(&*pg, proof)?)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(&record.bucket, "clear durable bucket write drain")?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::clear_durable_bucket_write_drain(
            &*pg,
            &record.bucket,
            &record.drain_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
            record.lease_deadline,
        )?)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.pg_id,
            "release bucket delete finalize claim",
        )?;
        LocalStorageNodeClient::release_bucket_delete_finalize_claim(self.client, self.pg_id, claim)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(&claim.bucket, claim.pg_id, "release lifecycle sweep claim")?;
        LocalStorageNodeClient::release_lifecycle_sweep_claim(self.client, self.pg_id, claim)
    }
}

impl ObjectGenerationMetadataNodeClient for LocalStorageNodeClient {
    fn open_object_generation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectGenerationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open object generation metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectGenerationMetadataRoute {
            storage_node: Arc::clone(&self.storage_node),
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl ObjectGenerationMetadataRoute for LocalObjectGenerationMetadataRoute {
    fn object_generation_reservation(
        &self,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::get_object_generation_reservation(
            &*pg,
            &self.bucket,
            &self.key,
            reservation_id,
        )?)
    }

    fn next_object_generation_id(&self) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::next_generation_id(
            &*pg,
            &self.bucket,
            &self.key,
        )?)
    }
}

impl ObjectVersionMetadataNodeClient for LocalStorageNodeClient {
    fn open_object_version_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectVersionMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open object version metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectVersionMetadataRoute {
            storage_node: Arc::clone(&self.storage_node),
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl ObjectVersionMetadataRoute for LocalObjectVersionMetadataRoute {
    fn next_object_version_id(&self) -> Result<VersionId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::next_version_id(
            &*pg,
            &self.bucket,
            &self.key,
        )?)
    }
}

impl DirectPutMetadataNodeClient for LocalStorageNodeClient {
    fn open_direct_put_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn DirectPutMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open direct PUT metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalDirectPutMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl LocalDirectPutMetadataRoute<'_> {
    fn require_request_subject(
        &self,
        request: &BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if request.request.bucket != self.bucket
            || request.request.key != self.key
            || !request
                .request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
            || request.request.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build direct PUT commit command",
            }
            .into());
        }
        Ok(())
    }
}

impl DirectPutMetadataRoute for LocalDirectPutMetadataRoute<'_> {
    fn load_direct_put_commit_snapshot(
        &self,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        load_direct_put_commit_snapshot_from_pg(
            &pg,
            self.client.node_id,
            &self.bucket,
            &self.key,
            reservation_id,
            generation_id,
        )
    }

    fn load_direct_put_commit_snapshot_until(
        &self,
        reservation_id: &SessionId,
        generation_id: GenerationId,
        deadline: Instant,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        load_direct_put_commit_snapshot_from_pg(
            &pg,
            self.client.node_id,
            &self.bucket,
            &self.key,
            reservation_id,
            generation_id,
        )
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_request_subject(&request)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        self.build_direct_put_commit_command_from_pg(request, &pg)
    }

    fn build_direct_put_commit_command_until(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
        deadline: Instant,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_request_subject(&request)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let command = self.build_direct_put_commit_command_from_pg(request, &pg)?;
        require_metadata_command_operation_deadline(deadline)
            .map_err(ObjectPgActionError::Store)?;
        Ok(command)
    }
}

impl LocalDirectPutMetadataRoute<'_> {
    fn build_direct_put_commit_command_from_pg(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
        pg: &crate::node_runtime::pg_store::PgStore,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let current = load_direct_put_commit_snapshot_from_pg(
            pg,
            self.client.node_id,
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
                pg,
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
            placement_cluster_epoch: self.route_cluster_epoch,
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
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            pg,
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
            })),
        ))
    }
}

struct LocalRetainedObjectMutationMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    pg_id: ObjectMetadataPgId,
    cluster_epoch: ClusterEpoch,
    bucket: BucketName,
    key: ObjectKey,
}

impl LocalRetainedObjectMutationMetadataRoute<'_> {
    fn require_claim_subject(
        &self,
        claim: &ObjectPayloadReclaimClaimRecord,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if claim.bucket != self.bucket
            || claim.key != self.key
            || claim.pg_id != self.pg_id.get()
            || claim.cluster_epoch != self.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl RetainedObjectMutationMetadataNodeClient for LocalStorageNodeClient {
    fn open_retained_object_mutation_route(
        &self,
        pg_id: ObjectMetadataPgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn RetainedObjectMutationMetadataRoute + '_>, BucketSnapshotLoadError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained object mutation route",
            }
            .into());
        }
        drop(self.storage_node.get_pg(pg_id.get())?);
        Ok(Box::new(LocalRetainedObjectMutationMetadataRoute {
            client: self,
            pg_id,
            cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl RetainedObjectMutationMetadataRoute for LocalRetainedObjectMutationMetadataRoute<'_> {
    fn prepare_retained_stream_upload_abort(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<PreparedRetainedStreamUploadAbort>, ObjectPgActionError> {
        let raw_pg_id = self.pg_id.pg_id();
        if let Some(pending) =
            <LocalStorageNodeClient as MetadataCommandNodeClient>::pending_metadata_command_envelope(
                self.client,
                raw_pg_id,
                self.cluster_epoch,
            )?
        {
            return PreparedRetainedStreamUploadAbort::new_if_matches(
                self.pg_id,
                self.cluster_epoch,
                &self.bucket,
                &self.key,
                session_id,
                pending,
            )
            .map(Some)
            .ok_or(ObjectPgActionError::Store(
                StoreError::MetadataCommandContention {
                    context: "retained stream abort found an unrelated pending command",
                },
            ));
        }

        let stream_session = match LocalStorageNodeClient::load_stream_upload_session(
            self.client,
            self.pg_id,
            &self.bucket,
            &self.key,
            session_id,
        ) {
            Ok(session) => session,
            Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound { .. })) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let staged_segments = LocalStorageNodeClient::load_stream_upload_segments(
            self.client,
            self.pg_id,
            &self.bucket,
            &self.key,
            session_id,
        )?;
        let command_id = <LocalStorageNodeClient as MetadataCommandNodeClient>::next_metadata_command_id_at_least(
            self.client,
            raw_pg_id,
            self.cluster_epoch,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )?;
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                session_id: session_id.clone(),
                staged_segments,
                stream_create_bucket_write_reservation: stream_session
                    .bucket_write_reservation
                    .clone(),
            })),
        );
        let prepared = PreparedRetainedStreamUploadAbort::new_if_matches(
            self.pg_id,
            self.cluster_epoch,
            &self.bucket,
            &self.key,
            session_id,
            command,
        )
        .ok_or(ObjectPgActionError::Store(
            StoreError::MetadataCommandContention {
                context: "retained stream abort preparation produced an invalid command",
            },
        ))?;
        <LocalStorageNodeClient as MetadataCommandNodeClient>::try_insert_pending_metadata_command_slot(
            self.client,
            raw_pg_id,
            prepared.command(),
            Some(&self.bucket),
        )?;
        Ok(Some(prepared))
    }

    fn release_object_payload_reclaim_claim(
        &self,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(claim, "release object payload reclaim claim")?;
        LocalStorageNodeClient::release_object_payload_reclaim_claim(self.client, self.pg_id, claim)
    }
}

impl LocalPutObjectMetadataRoute<'_> {
    fn require_request_subject(
        &self,
        request: &BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if request.expected_stored.bucket() != &self.bucket
            || request.expected_stored.key() != &self.key
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build PUT object metadata command",
            }
            .into());
        }
        Ok(())
    }
}

impl PutObjectMetadataRoute for LocalPutObjectMetadataRoute<'_> {
    fn load_put_object_metadata_snapshot(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        match version_id {
            Some(version_id) => Ok(PgMetadataStore::get_object_version(
                &*pg,
                &self.bucket,
                &self.key,
                version_id,
            )?),
            None => Ok(PgMetadataStore::get_object_meta(
                &*pg,
                &self.bucket,
                &self.key,
            )?),
        }
    }

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_request_subject(&request)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current = match request.requested_version_id {
            Some(version_id) => {
                PgMetadataStore::get_object_version(&*pg, &self.bucket, &self.key, version_id)
            }
            None => PgMetadataStore::get_object_meta(&*pg, &self.bucket, &self.key),
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
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
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
}

impl LocalObjectDeleteMetadataRoute<'_> {
    fn require_proof(
        &self,
        proof: &BucketWriteReservationProof,
        operation_kind: &str,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if !proof.matches_exact_mutation_subject(
            self.route_cluster_epoch,
            &self.bucket,
            operation_kind,
            Some(self.key.as_str()),
        ) {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_stored_subject(
        &self,
        stored: Option<&StoredObject>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if stored.is_some_and(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_reclaim_subject(
        &self,
        reclaim: Option<&ObjectPayloadReclaimCommand>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let crossed = match reclaim {
            Some(ObjectPayloadReclaimCommand::Segments(record)) => {
                record.bucket != self.bucket || record.key != self.key
            }
            Some(ObjectPayloadReclaimCommand::Multipart(record)) => {
                record.bucket != self.bucket || record.key != self.key
            }
            None => false,
        };
        if crossed {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_delete_target_subject(
        &self,
        target: Option<&DeleteObjectVersionTarget>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if let Some(DeleteObjectVersionTarget::Live { payload, .. }) = target {
            self.require_reclaim_subject(Some(payload), operation)?;
        }
        Ok(())
    }
}

impl ObjectDeleteMetadataRoute for LocalObjectDeleteMetadataRoute<'_> {
    fn load_current_object_delete_snapshot(
        &self,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let stored = load_current_object_optional_from_pg(&pg, &self.bucket, &self.key)?;
        Ok(load_object_delete_snapshot_from_stored(
            &pg,
            &self.bucket,
            &self.key,
            stored,
        )?)
    }

    fn load_specific_object_delete_snapshot(
        &self,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let stored =
            load_object_version_optional_from_pg(&pg, &self.bucket, &self.key, version_id)?;
        Ok(load_object_delete_snapshot_from_stored(
            &pg,
            &self.bucket,
            &self.key,
            stored,
        )?)
    }

    fn list_object_versions_for_lifecycle(&self) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        match PgMetadataStore::list_object_versions_for_key(&*pg, &self.bucket, &self.key) {
            Ok(versions) => Ok(versions),
            Err(MetadataError::ObjectNotFound) => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
            "build delete-specific object command",
        )?;
        self.require_stored_subject(
            request.expected_stored,
            "build delete-specific object command",
        )?;
        self.require_delete_target_subject(
            request.expected_target,
            "build delete-specific object command",
        )?;
        if request.expected_version_list.is_some_and(|versions| {
            versions
                .iter()
                .any(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
        }) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build delete-specific object command",
            }
            .into());
        }
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current = if let Some(expected_version_list) = request.expected_version_list {
            let versions = match PgMetadataStore::list_object_versions_for_key(
                &*pg,
                &self.bucket,
                &self.key,
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
            load_object_version_optional_from_pg(&pg, &self.bucket, &self.key, request.version_id)?
        };
        if current.as_ref() != request.expected_stored {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(target) =
            delete_command_target_from_stored(&pg, &self.bucket, &self.key, current.as_ref())?
        else {
            return Ok(None);
        };
        if !delete_target_matches_expected(Some(&target), request.expected_target) {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                version_id: request.version_id,
                mode: DeleteObjectVersionMode::Specific,
                target,
            })),
        )))
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
            "build delete-current object command",
        )?;
        self.require_stored_subject(
            request.expected_current,
            "build delete-current object command",
        )?;
        self.require_delete_target_subject(
            request.expected_target,
            "build delete-current object command",
        )?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current = load_current_object_optional_from_pg(&pg, &self.bucket, &self.key)?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(StoredObject::Live(record)) = current.as_ref() else {
            return Ok(None);
        };
        let target = live_delete_command_target(&pg, &self.bucket, &self.key, record)?;
        if !delete_target_matches_expected(Some(&target), request.expected_target) {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                version_id: record.version_id,
                mode: DeleteObjectVersionMode::Current,
                target,
            })),
        )))
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
            "build insert-delete-marker command",
        )?;
        self.require_stored_subject(
            request.expected_current,
            "build insert-delete-marker command",
        )?;
        self.require_stored_subject(
            request.expected_stale_payload_source,
            "build insert-delete-marker command",
        )?;
        if let InsertDeleteMarkerStalePayload::Explicit(reclaim) = &request.stale_payload {
            self.require_reclaim_subject(reclaim.as_ref(), "build insert-delete-marker command")?;
        }
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current = load_current_object_optional_from_pg(&pg, &self.bucket, &self.key)?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let stale_payload = match request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(stale_payload) => stale_payload,
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
                let (source, stale_payload) = snapshot_direct_put_stale_payload_for_snapshot(
                    &pg,
                    &self.bucket,
                    &self.key,
                    created_at,
                )?;
                if source.as_ref() != request.expected_stale_payload_source {
                    return Err(ObjectPgActionError::StaleObjectReadSubject);
                }
                stale_payload
            }
        };
        let write_sequence =
            pg.next_object_write_sequence(self.bucket.as_str(), self.key.as_str())?;
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: request.bucket_write_reservation.clone(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                version_id: request.version_id,
                owner: request.owner.clone(),
                write_sequence,
                last_modified_millis: crate::clock::current_time_millis(),
                stale_payload,
            }),
        ))
    }
}

impl LocalMultipartUploadCreationMetadataRoute<'_> {
    fn require_create_subject(
        &self,
        create: &CreateMultipartUploadReq,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if create.bucket != self.bucket || create.key != self.key {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_expected_command_subject(
        &self,
        create: &CreateMultipartUploadReq,
        command: &CreateMultipartUploadCommand,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if command.upload().bucket != self.bucket
            || command.upload().key != self.key
            || !command.matches_request(create)
            || !command
                .bucket_write_reservation()
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl MultipartUploadCreationMetadataRoute for LocalMultipartUploadCreationMetadataRoute<'_> {
    fn matching_multipart_upload_initiated_at(
        &self,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        self.require_create_subject(create, "match multipart upload creation")?;
        if let Some(command) = expected_command {
            self.require_expected_command_subject(
                create,
                command,
                "match multipart upload creation",
            )?;
        }
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let upload_id =
            expected_command.map_or(&create.upload_id, |command| &command.upload().upload_id);
        match pg.get_multipart_upload(upload_id) {
            Ok(existing)
                if expected_command.is_some_and(|command| {
                    multipart_upload_matches_command(&existing, command)
                }) =>
            {
                Ok(Some(existing.initiated_at))
            }
            Ok(_) => Err(MetadataError::InvariantViolation {
                context: "create multipart upload existing upload mismatch",
                reason: "existing multipart upload does not match the command".into(),
            }
            .into()),
            Err(MetadataError::NoSuchUpload { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_create_subject(request.request, "build create multipart upload command")?;
        if request
            .expected_current
            .is_some_and(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create multipart upload command",
            }
            .into());
        }
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current = load_current_object_optional_from_pg(&pg, &self.bucket, &self.key)?;
        if current.as_ref() != request.expected_current {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        match pg.get_multipart_upload(&request.request.upload_id) {
            Ok(_) => {
                return Err(MetadataError::InvariantViolation {
                    context: "create multipart upload existing upload mismatch",
                    reason: "multipart upload already exists".into(),
                }
                .into());
            }
            Err(MetadataError::NoSuchUpload { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let object_generation_id =
            PgMetadataStore::next_generation_id(&*pg, &self.bucket, &self.key)?;
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateMultipartUpload(Box::new(
                CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                    crate::node_runtime::CreateMultipartUploadCommandBuildAuthority::new(),
                    request.request.clone(),
                    object_generation_id,
                    current
                        .as_ref()
                        .map(|stored| pg.multipart_object_identity(stored))
                        .transpose()?,
                    crate::clock::current_time_millis(),
                    request.bucket_write_reservation.clone(),
                ),
            )),
        ))
    }
}

impl LocalStreamUploadCreationMetadataRoute<'_> {
    fn require_create_subject(
        &self,
        create: &CreateStreamUploadReq,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if create.bucket != self.bucket || create.key != self.key {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_expected_command_subject(
        &self,
        create: &CreateStreamUploadReq,
        command: &CreateStreamUploadCommand,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let expected_operation_kind = match create.target {
            StreamUploadTarget::PutObject => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            StreamUploadTarget::UploadPart { .. } => {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
        };
        if !command.matches_request(create)
            || !command
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    expected_operation_kind,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_build_subject(
        &self,
        request: &BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        self.require_create_subject(request.request, "build create stream upload command")?;
        let expected_operation_kind = match (&request.request.target, &request.precondition) {
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck { .. },
            ) => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObject {
                    expected_current, ..
                },
            ) if expected_current.is_none_or(|stored| {
                stored.bucket() == &self.bucket && stored.key() == &self.key
            }) =>
            {
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            (
                StreamUploadTarget::UploadPart { upload_id, .. },
                CreateStreamUploadPrecondition::UploadPart { expected_upload },
            ) if expected_upload.bucket == self.bucket
                && expected_upload.key == self.key
                && expected_upload.upload_id == *upload_id =>
            {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            _ => {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "build create stream upload command",
                }
                .into());
            }
        };
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                expected_operation_kind,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create stream upload command",
            }
            .into());
        }
        Ok(())
    }
}

impl StreamUploadCreationMetadataRoute for LocalStreamUploadCreationMetadataRoute<'_> {
    fn matching_stream_upload_exists(
        &self,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        self.require_create_subject(create, "match stream upload creation")?;
        if let Some(command) = expected_command {
            self.require_expected_command_subject(create, command, "match stream upload creation")?;
        }
        self.client
            .matching_stream_upload_exists_for_route(self.pg_id, create, expected_command)
    }

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_build_subject(&request)?;
        self.client.build_create_stream_upload_command_for_route(
            self.pg_id,
            self.route_cluster_epoch,
            request,
        )
    }
}

impl StreamUploadSessionMetadataRoute for LocalStreamUploadSessionMetadataRoute<'_> {
    fn load_session(&self) -> Result<StreamUploadRecord, ObjectPgActionError> {
        self.client.load_stream_upload_session(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.session_id,
        )
    }

    fn load_segments(&self) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        self.client.load_stream_upload_segments(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.session_id,
        )
    }

    fn prepare_segment_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if request.session_id != self.session_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "prepare stream segment append",
            }
            .into());
        }
        self.client.prepare_stream_segment_append_inner(
            self.pg_id,
            &self.bucket,
            &self.key,
            request,
            Some(effect_fence),
        )
    }

    fn update_put_bucket_write_reservation(
        &self,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        // The active route authorizes this update at the current epoch. The
        // durable reservation may have originated in an older retained epoch,
        // so bind its stable subject without requiring that historical proof
        // epoch to equal the active route epoch.
        if current.bucket != self.bucket
            || current.operation_kind != PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            || current.target_context.as_deref() != Some(self.key.as_str())
            || renewed.bucket != self.bucket
            || renewed.operation_kind != PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            || renewed.target_context.as_deref() != Some(self.key.as_str())
            || !current.has_same_stable_identity(renewed)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "update stream upload bucket write reservation",
            }
            .into());
        }
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let session = PgMetadataStore::get_stream_upload(&*pg, &self.session_id)?;
        validate_stream_upload_session_binding(&session, &self.bucket, &self.key)?;
        if session.target != StreamUploadTarget::PutObject
            || session.bucket_write_reservation.as_ref() != Some(current)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "update stream upload bucket write reservation",
            }
            .into());
        }
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        Ok(
            PgMetadataStore::update_stream_upload_bucket_write_reservation(
                &*pg,
                &self.session_id,
                current,
                renewed,
            )?,
        )
    }
}

impl StreamPutFinalizationMetadataRoute for LocalStreamPutFinalizationMetadataRoute<'_> {
    fn load_snapshot(&self) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        self.client.load_stream_put_finalize_snapshot(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.session_id,
        )
    }

    fn load_snapshot_until(
        &self,
        deadline: Instant,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        load_stream_put_finalize_snapshot_from_pg(&pg, &self.bucket, &self.key, &self.session_id)
    }

    fn build_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build stream PUT commit command",
            }
            .into());
        }
        self.client
            .build_stream_put_commit_command(self, request, effect_fence)
    }
}

impl StreamPartFinalizationMetadataRoute for LocalStreamPartFinalizationMetadataRoute<'_> {
    fn load_snapshot(&self) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
        self.client.load_stream_part_finalize_snapshot(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.upload_id,
            &self.session_id,
            self.part_number,
        )
    }

    fn build_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if request.part.upload_id != self.upload_id || request.part.part_number != self.part_number
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build stream part commit command",
            }
            .into());
        }
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build stream part commit command",
            }
            .into());
        }
        self.client
            .build_stream_part_commit_command(self, request, effect_fence)
    }
}

impl MultipartUploadLookupMetadataRoute for LocalMultipartUploadLookupMetadataRoute<'_> {
    fn load_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        self.client.load_multipart_upload(
            self.pg_id,
            &self.bucket,
            &self.key,
            upload_id,
            self.authorization,
        )
    }

    fn load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.client.load_in_progress_multipart_upload(
            self.pg_id,
            &self.bucket,
            &self.key,
            upload_id,
            self.authorization,
        )
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.client.load_in_progress_multipart_upload_for_listing(
            self.pg_id,
            &self.bucket,
            &self.key,
            upload_id,
            self.authorization,
        )
    }

    fn lookup_multipart_upload_management(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        self.client.lookup_multipart_upload_management(
            self.pg_id,
            &self.bucket,
            &self.key,
            upload_id,
            self.authorization,
        )
    }
}

impl AuthorizedMultipartUploadMetadataRoute for LocalAuthorizedMultipartUploadMetadataRoute<'_> {
    fn load_multipart_completion_snapshot(
        &self,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        self.client.load_multipart_completion_snapshot(
            self.pg_id,
            &self.authorized_upload,
            requested_part_numbers,
            self.authorization,
        )
    }

    fn load_multipart_completion_preflight(
        &self,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        self.client.load_multipart_completion_preflight(
            self.pg_id,
            &self.authorized_upload,
            self.authorization,
        )
    }

    fn list_multipart_parts(
        &self,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        self.client.list_multipart_parts_for_authorized_upload(
            self.pg_id,
            &self.authorized_upload,
            part_number_marker,
            max_parts,
            self.authorization,
        )
    }
}

impl MultipartCompletionMutationMetadataRoute
    for LocalMultipartCompletionMutationMetadataRoute<'_>
{
    fn load_stale_payload_source(&self) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        Ok(load_null_live_stale_payload_source_from_pg(
            &pg,
            &self.bucket,
            &self.key,
        )?)
    }

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        require_multipart_completion_mutation_subject(
            self.route_cluster_epoch,
            &self.bucket,
            &self.key,
            &request,
        )?;
        self.client.build_complete_multipart_object_command(
            self.route_cluster_epoch,
            self.pg_id,
            request,
        )
    }
}

impl MultipartAbortMutationMetadataRoute for LocalMultipartAbortMutationMetadataRoute<'_> {
    fn load_cleanup(&self) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        Ok(pg.prepare_abort_multipart_upload_cleanup(&self.bucket, &self.key, &self.upload_id)?)
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_multipart_abort_mutation_subject(
            MultipartAbortMutationSubject {
                route_cluster_epoch: self.route_cluster_epoch,
                bucket: &self.bucket,
                key: &self.key,
                upload_id: &self.upload_id,
            },
            None,
            request.expected_cleanup,
            request.bucket_write_reservation,
            "build abort multipart upload command",
        )?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let cleanup =
            pg.prepare_abort_multipart_upload_cleanup(&self.bucket, &self.key, &self.upload_id)?;
        if cleanup.as_ref() != request.expected_cleanup {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(cleanup) = cleanup else {
            return Ok(None);
        };
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.upload_id.clone(),
                cleanup,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
            })),
        )))
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_multipart_abort_mutation_subject(
            MultipartAbortMutationSubject {
                route_cluster_epoch: self.route_cluster_epoch,
                bucket: &self.bucket,
                key: &self.key,
                upload_id: &self.upload_id,
            },
            Some(request.authorized_upload.record()),
            request.expected_cleanup,
            request.bucket_write_reservation,
            "build authorized abort multipart upload command",
        )?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let cleanup =
            pg.prepare_authorized_abort_multipart_upload_cleanup(request.authorized_upload)?;
        if cleanup.as_ref() != request.expected_cleanup {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        let Some(cleanup) = cleanup else {
            return Ok(None);
        };
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(Some(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.upload_id.clone(),
                cleanup,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
            })),
        )))
    }
}

impl ObjectPayloadReclaimMetadataRoute for LocalObjectPayloadReclaimMetadataRoute<'_> {
    fn exists(&self) -> Result<bool, ObjectPgActionError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        Ok(PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &self.bucket,
            &self.key,
            self.generation_id,
        )?)
    }

    fn load_payload(&self) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        if let Some(reclaim) =
            pg.get_object_segments_reclaim(&self.bucket, &self.key, self.generation_id)?
        {
            Ok(Some(ObjectPayloadReclaimCommand::Segments(reclaim)))
        } else {
            Ok(pg
                .get_multipart_reclaim(&self.bucket, &self.key, self.generation_id)?
                .map(ObjectPayloadReclaimCommand::Multipart))
        }
    }

    fn acquire_claim(
        &self,
        request: AcquireObjectPayloadReclaimClaimReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let routed_kind = if pg
            .get_object_segments_reclaim(&self.bucket, &self.key, self.generation_id)?
            .is_some()
        {
            Some(ObjectPayloadReclaimKind::ObjectSegments)
        } else if pg
            .get_multipart_reclaim(&self.bucket, &self.key, self.generation_id)?
            .is_some()
        {
            Some(ObjectPayloadReclaimKind::Multipart)
        } else {
            None
        };
        let Some(routed_kind) = routed_kind else {
            return Ok(None);
        };
        if routed_kind != request.reclaim_kind {
            return Err(BucketSnapshotLoadError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "acquire object payload reclaim claim",
                },
            ));
        }
        match pg.acquire_object_payload_reclaim_claim(
            &self.bucket,
            request.bucket_incarnation_generation,
            &self.key,
            self.generation_id,
            request.reclaim_kind,
            request.claim_id,
            request.owner_token,
            self.route_cluster_epoch,
            effect_fence,
            request.claimed_at,
            request.lease_deadline,
            request.now,
        ) {
            Ok(claim) => Ok(claim),
            Err(MetadataError::RouteEffectRejected { source }) => {
                Err(BucketSnapshotLoadError::Store(source))
            }
            Err(error) => Err(BucketSnapshotLoadError::Metadata(error)),
        }
    }

    fn build_delete_object_payload_reclaim_command(
        &self,
        request: BuildDeleteObjectPayloadReclaimCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_object_payload_reclaim_command_subject(
            self.route_cluster_epoch,
            self.pg_id,
            &self.bucket,
            &self.key,
            self.generation_id,
            &request,
        )?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        let current_payload = match request.payload {
            ObjectPayloadReclaimCommand::Segments(_) => pg
                .get_object_segments_reclaim(&self.bucket, &self.key, self.generation_id)?
                .map(ObjectPayloadReclaimCommand::Segments),
            ObjectPayloadReclaimCommand::Multipart(_) => pg
                .get_multipart_reclaim(&self.bucket, &self.key, self.generation_id)?
                .map(ObjectPayloadReclaimCommand::Multipart),
        };
        if current_payload.as_ref() != Some(request.payload)
            || pg.object_payload_reclaim_claim()?.as_ref() != Some(request.claim)
        {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        let command_id = self.client.next_metadata_command_id_from_locked_pg(
            self.pg_id.pg_id(),
            self.route_cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                DeleteObjectPayloadReclaimCommand::new(
                    self.bucket.clone(),
                    self.key.clone(),
                    self.generation_id,
                    request.payload.clone(),
                    ObjectPayloadReclaimClaimProof::from(request.claim),
                ),
            )),
        ))
    }
}

impl ObjectMutationMetadataNodeClient for LocalStorageNodeClient {
    fn open_put_object_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn PutObjectMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open PUT object metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalPutObjectMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_object_delete_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectDeleteMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open object delete metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectDeleteMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadCreationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open multipart upload creation metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalMultipartUploadCreationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_upload_lookup_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadLookupMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open multipart upload lookup metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalMultipartUploadLookupMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            authorization: MetadataReadAuthorization::active(pg_id.pg_id()),
        }))
    }

    fn open_authorized_multipart_upload_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Box<dyn AuthorizedMultipartUploadMetadataRoute + '_>, ObjectPgActionError> {
        let upload = authorized_upload.record();
        if self
            .storage_node
            .object_metadata_pg_for(&upload.bucket, &upload.key)
            != pg_id
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open authorized multipart upload metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalAuthorizedMultipartUploadMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorized_upload: authorized_upload.clone(),
            authorization: MetadataReadAuthorization::active(pg_id.pg_id()),
        }))
    }

    fn open_multipart_completion_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartCompletionMutationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open multipart completion mutation metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalMultipartCompletionMutationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_abort_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Box<dyn MultipartAbortMutationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open multipart abort mutation metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalMultipartAbortMutationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
        }))
    }

    fn open_object_payload_reclaim_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadReclaimMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open object payload reclaim metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectPayloadReclaimMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }))
    }

    fn open_stream_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn StreamUploadCreationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open stream upload creation metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalStreamUploadCreationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_stream_upload_session_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamUploadSessionMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open stream upload session metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalStreamUploadSessionMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
        }))
    }

    fn open_stream_put_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamPutFinalizationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open stream PUT finalization metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalStreamPutFinalizationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
        }))
    }

    fn open_stream_part_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<Box<dyn StreamPartFinalizationMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open stream part finalization metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalStreamPartFinalizationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            session_id: session_id.clone(),
            part_number,
        }))
    }

    fn open_object_mutation_scan_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ObjectMutationScanMetadataRoute + '_>, ObjectPgActionError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectMutationScanMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
        }))
    }
}

impl ObjectReadMetadataNodeClient for LocalStorageNodeClient {
    fn open_object_read_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn ObjectReadMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open object read metadata route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectReadMetadataRoute {
            storage_node: Arc::clone(&self.storage_node),
            node_id: self.node_id,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            authorization,
        }))
    }

    fn open_multipart_upload_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn MultipartUploadLookupMetadataRoute + '_>, ObjectPgActionError> {
        if self.storage_node.object_metadata_pg_for(bucket, key) != pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open multipart upload read route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalMultipartUploadLookupMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            authorization,
        }))
    }

    fn open_authorized_multipart_upload_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn AuthorizedMultipartUploadMetadataRoute + '_>, ObjectPgActionError> {
        let upload = authorized_upload.record();
        if self
            .storage_node
            .object_metadata_pg_for(&upload.bucket, &upload.key)
            != pg_id
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open authorized multipart upload read route",
            }
            .into());
        }
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalAuthorizedMultipartUploadMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorized_upload: authorized_upload.clone(),
            authorization,
        }))
    }
}

impl ObjectReadMetadataRoute for LocalObjectReadMetadataRoute {
    fn load_object_read_auth_subject(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        SharedStorageNode::load_object_read_auth_subject_from_object_pg(
            &pg,
            &self.bucket,
            &self.key,
            version_id,
        )
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        SharedStorageNode::load_object_read_snapshot_for_subject_from_object_pg(
            &pg,
            &self.bucket,
            &self.key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }
}

impl ObjectListingMetadataNodeClient for LocalStorageNodeClient {
    fn open_object_listing_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
        authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn ObjectListingMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.storage_node.require_open_pg(pg_id.get())?;
        Ok(Box::new(LocalObjectListingMetadataRoute {
            storage_node: Arc::clone(&self.storage_node),
            node_id: self.node_id,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorization,
        }))
    }
}

impl ObjectListingMetadataRoute for LocalObjectListingMetadataRoute {
    fn list_objects_page(
        &self,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let response = pg.list_objects(req)?;
        for object in &response.objects {
            self.validate_listing_subject(
                object.bucket(),
                object.key(),
                "validate object listing response scan PG",
            )?;
        }
        Ok(response)
    }

    fn list_object_versions_page(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let response = pg.list_object_versions(req)?;
        for object in &response.versions {
            self.validate_listing_subject(
                object.bucket(),
                object.key(),
                "validate object version listing response scan PG",
            )?;
        }
        Ok(response)
    }

    fn list_multipart_uploads_page(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            self.pg_id.pg_id(),
            self.authorization,
        )?;
        let response = pg.list_multipart_uploads(req)?;
        for upload in &response.uploads {
            self.validate_listing_subject(
                &upload.bucket,
                &upload.key,
                "validate multipart upload listing response scan PG",
            )?;
        }
        Ok(response)
    }
}

impl LocalObjectListingMetadataRoute {
    fn validate_listing_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if self
            .storage_node
            .object_metadata_pg_for(bucket, key)
            .pg_id()
            != self.pg_id.pg_id()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl LocalObjectMutationScanMetadataRoute<'_> {
    fn require_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation: &'static str,
    ) -> Result<(), StoreError> {
        if self
            .client
            .storage_node
            .object_metadata_pg_for(bucket, key)
            .pg_id()
            != self.pg_id.pg_id()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation });
        }
        Ok(())
    }

    fn validate_stream_page(
        &self,
        page: &StreamUploadRecordPage,
        expected_bucket: Option<&BucketName>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        for upload in &page.uploads {
            if expected_bucket.is_some_and(|bucket| bucket != &upload.bucket) {
                return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
            }
            self.require_subject(&upload.bucket, &upload.key, operation)?;
        }
        Ok(())
    }

    fn validate_root(
        &self,
        root: &PayloadReclaimRoot,
        expected_bucket: Option<&BucketName>,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if expected_bucket.is_some_and(|bucket| bucket != &root.bucket) {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        self.require_subject(&root.bucket, &root.key, operation)?;
        Ok(())
    }
}

impl ObjectMutationScanMetadataRoute for LocalObjectMutationScanMetadataRoute<'_> {
    fn list_aborting_multipart_upload_bucket_witnesses(
        &self,
    ) -> Result<Vec<AbortingMultipartUploadBucketWitness>, ObjectPgActionError> {
        let witnesses = self
            .client
            .list_aborting_multipart_upload_bucket_witnesses(self.pg_id)?;
        for witness in &witnesses {
            self.require_subject(
                &witness.bucket,
                &witness.key,
                "list aborting multipart upload bucket witnesses",
            )?;
        }
        Ok(witnesses)
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let page = self.client.list_stream_uploads_for_bucket_page(
            self.pg_id,
            bucket,
            session_id_marker,
            limit,
        )?;
        self.validate_stream_page(&page, Some(bucket), "list bucket stream uploads")?;
        Ok(page)
    }

    fn list_all_stream_uploads_page(
        &self,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let page =
            self.client
                .list_all_stream_uploads_page(self.pg_id, session_id_marker, limit)?;
        self.validate_stream_page(&page, None, "list PG stream uploads")?;
        Ok(page)
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let root = self
            .client
            .get_bucket_payload_reclaim_root(self.pg_id, bucket)?;
        if let Some(root) = &root {
            self.validate_root(root, Some(bucket), "get bucket payload reclaim root")?;
        }
        Ok(root)
    }

    fn get_payload_reclaim_root(
        &self,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let root = self.client.get_payload_reclaim_root(self.pg_id)?;
        if let Some(root) = &root {
            self.validate_root(root, None, "get PG payload reclaim root")?;
        }
        Ok(root)
    }

    fn object_payload_reclaim_claim(
        &self,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let claim = self.client.object_payload_reclaim_claim(self.pg_id)?;
        if let Some(claim) = &claim {
            if claim.pg_id != self.pg_id.get() {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "get PG payload reclaim claim",
                }
                .into());
            }
            self.require_subject(&claim.bucket, &claim.key, "get PG payload reclaim claim")?;
        }
        Ok(claim)
    }
}

impl LocalStorageNodeClient {
    fn load_multipart_upload(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
        Ok(load_multipart_upload_from_pg(&pg, bucket, key, upload_id)?)
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
        Ok(load_in_progress_multipart_upload_from_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        Self::load_in_progress_multipart_upload(self, pg_id, bucket, key, upload_id, authorization)
    }

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let current_object = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => Some(stored),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let existing_etag = current_object
            .as_ref()
            .and_then(|stored| stored.as_live().map(|record| record.etag.format()));
        let current_object_identity = current_object
            .as_ref()
            .map(|stored| pg.multipart_object_identity(stored))
            .transpose()?;
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
        Ok(MultipartCompletionSnapshot::from_storage(
            crate::types::MultipartCompletionSubject::from_upload(&upload),
            existing_etag,
            current_object_identity,
            stale_payload_source,
            part_records,
            selected_streaming_segments,
            cleanup,
        ))
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
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
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
        authorization: MetadataReadAuthorization,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
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
        ListedMultipartParts::from_storage(upload, response).map_err(|reason| {
            MetadataError::InvariantViolation {
                context: "project multipart parts listing",
                reason: reason.to_string(),
            }
            .into()
        })
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        authorization: MetadataReadAuthorization,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let pg = self.storage_node.get_pg_for_metadata_read(
            self.node_id,
            pg_id.pg_id(),
            authorization,
        )?;
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

        if let Some(completed) = pg.get_multipart_completion_replay(bucket, key, upload_id)? {
            return Ok(MultipartUploadManagementLookup::Replay(Box::new(completed)));
        }
        Ok(MultipartUploadManagementLookup::Missing)
    }

    fn load_stream_upload_session(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(session_id)?;
        validate_stream_upload_session_binding(&session, bucket, key)?;
        Ok(session)
    }

    fn matching_stream_upload_exists_for_route(
        &self,
        pg_id: ObjectMetadataPgId,
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
            Ok(_) => Err(MetadataError::InvariantViolation {
                context: "create stream upload existing session mismatch",
                reason: "existing stream session does not match the command".into(),
            }
            .into()),
            Err(MetadataError::StreamSessionNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn build_create_stream_upload_command_for_route(
        &self,
        pg_id: ObjectMetadataPgId,
        route_cluster_epoch: ClusterEpoch,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
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
                return Err(MetadataError::InvariantViolation {
                    context: "create stream upload existing session mismatch",
                    reason: "stream session already exists".into(),
                }
                .into());
            }
            Err(MetadataError::StreamSessionNotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let command_id =
            self.next_metadata_command_id_from_locked_pg(pg_id.pg_id(), route_cluster_epoch, &pg)?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                    request.request.clone(),
                    crate::clock::current_time_millis(),
                    request.cleanup_after,
                    request.bucket_write_reservation.clone(),
                ),
            )),
        ))
    }

    fn load_stream_upload_segments(
        &self,
        pg_id: ObjectMetadataPgId,
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
        pg_id: ObjectMetadataScanPgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_all_stream_uploads_page(session_id_marker, limit)?)
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: ObjectMetadataScanPgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_stream_uploads_for_bucket_page(bucket, session_id_marker, limit)?)
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        load_stream_put_finalize_snapshot_from_pg(&pg, bucket, key, session_id)
    }

    fn build_stream_put_commit_command(
        &self,
        route: &LocalStreamPutFinalizationMetadataRoute<'_>,
        request: BuildStreamPutCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(route.pg_id.get())?;
        let current = load_stream_put_finalize_snapshot_from_pg(
            &pg,
            &route.bucket,
            &route.key,
            &route.session_id,
        )?;
        if &current != request.expected_snapshot {
            return Err(ObjectPgActionError::StaleStreamFinalizeSnapshot);
        }
        if current.session.bucket_write_reservation.as_ref()
            != Some(request.bucket_write_reservation)
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason:
                    "stream PUT commit reservation proof does not match the durable stream session"
                        .to_string(),
            });
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
        let staged_crc64 = combined_stream_segment_crc64(&current.staging_segments);
        if staged_crc64 != request.commit.etag_crc64 {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "stream PUT etag CRC64 mismatch: caller passed {} but staged payload segments combine to {staged_crc64}",
                    request.commit.etag_crc64
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
        effect_fence.require_valid_for(route.route_cluster_epoch)?;
        let write_sequence =
            pg.next_object_write_sequence(route.bucket.as_str(), route.key.as_str())?;
        let stale_payload = if version_id.is_null() {
            snapshot_direct_put_stale_payload_command(
                &pg,
                &route.bucket,
                &route.key,
                last_modified_millis,
            )?
        } else {
            None
        };
        let committed_segments: Vec<ObjectSegmentRecord> = current
            .staging_segments
            .iter()
            .map(|segment| ObjectSegmentRecord {
                bucket: route.bucket.clone(),
                key: route.key.clone(),
                version_id,
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                placement_cluster_epoch: segment.placement_cluster_epoch,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
            })
            .collect();
        let object = PutLiveObjectReq {
            bucket: route.bucket.clone(),
            key: route.key.clone(),
            version_id,
            owner: request.commit.owner.clone(),
            acl_grants: request.commit.acl_grants.clone(),
            public_read: request.commit.public_read,
            generation_id,
            size: request.total_size,
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
        effect_fence.require_valid_for(route.route_cluster_epoch)?;
        let command_id = self.next_metadata_command_id_from_locked_pg(
            route.pg_id.pg_id(),
            route.route_cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object,
                segments: committed_segments,
                generation_reservation_id: route.session_id.clone(),
                write_sequence,
                last_modified_millis,
                stale_payload,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
            })),
        ))
    }

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: ObjectMetadataPgId,
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
        route: &LocalStreamPartFinalizationMetadataRoute<'_>,
        request: BuildStreamPartCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(route.pg_id.get())?;
        let current = load_stream_part_finalize_snapshot_from_pg(
            &pg,
            &route.bucket,
            &route.key,
            &route.upload_id,
            &route.session_id,
            route.part_number,
        )?;
        if &current != request.expected_snapshot {
            return Err(ObjectPgActionError::StaleStreamFinalizeSnapshot);
        }
        let staged_segments = &current.auth_snapshot.staging_segments;
        let segments_total: u64 = staged_segments.iter().map(|segment| segment.size).sum();
        if segments_total != request.part.size {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "stream UploadPart size mismatch: caller passed {} but staged segments sum to {segments_total}",
                    request.part.size
                ),
            });
        }
        let staged_crc64 = combined_stream_segment_crc64(staged_segments);
        if staged_crc64 != request.part.payload_crc64 {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "stream UploadPart etag CRC64 mismatch: caller passed {} but staged payload segments combine to {staged_crc64}",
                    request.part.payload_crc64
                ),
            });
        }
        let committed_segments: Vec<MultipartPartSegmentRecord> = staged_segments
            .iter()
            .map(|segment| MultipartPartSegmentRecord {
                bucket: route.bucket.clone(),
                key: route.key.clone(),
                upload_id: route.upload_id.clone(),
                version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                part_number: route.part_number,
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                placement_cluster_epoch: segment.placement_cluster_epoch,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
            })
            .collect();
        if committed_segments != request.segments {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream UploadPart segments do not match staged stream segments"
                    .to_string(),
            });
        }
        effect_fence.require_valid_for(route.route_cluster_epoch)?;
        let command_id = self.next_metadata_command_id_from_locked_pg(
            route.pg_id.pg_id(),
            route.route_cluster_epoch,
            &pg,
        )?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                bucket: route.bucket.clone(),
                key: route.key.clone(),
                session_id: route.session_id.clone(),
                upload: current.auth_snapshot.upload,
                part: request.part.clone(),
                segments: committed_segments,
                existing_part: current.existing_part,
                displaced_segments: current.displaced_segments,
                bucket_write_reservation: request.bucket_write_reservation.clone(),
            })),
        ))
    }

    fn build_complete_multipart_object_command(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
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
            return Err(MetadataError::InvariantViolation {
                context: "complete multipart command generation mismatch",
                reason: "multipart upload generation does not match the command".into(),
            }
            .into());
        }
        if complete.part_records.is_empty() {
            return Err(MetadataError::InvariantViolation {
                context: "complete multipart command empty parts",
                reason: "multipart completion requires at least one part".into(),
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
        let current_object =
            load_current_object_optional_from_pg(&pg, &complete.bucket, &complete.key)?;
        let current_object_identity = current_object
            .as_ref()
            .map(|stored| pg.multipart_object_identity(stored))
            .transpose()?;
        if complete.conditional_completion
            && current_object_identity != upload.initiated_object_identity
        {
            return Err(ObjectPgActionError::MultipartConditionalRequestConflict);
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

        let parts_len = u32::try_from(complete.part_records.len()).map_err(|_| {
            MetadataError::InvariantViolation {
                context: "complete multipart command too many parts",
                reason: "multipart part count exceeds the durable range".into(),
            }
        })?;
        let parts_count =
            std::num::NonZeroU32::new(parts_len).ok_or(MetadataError::InvariantViolation {
                context: "complete multipart command empty parts",
                reason: "multipart completion requires at least one part".into(),
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
        let object_parts = complete_multipart_expected_object_parts(
            complete,
            request.version_id,
            self.storage_node.pg_topology(),
        );
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
        let command_id =
            self.next_metadata_command_id_from_locked_pg(pg_id.pg_id(), route_cluster_epoch, &pg)?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: complete.upload_id.clone(),
                completion_fingerprint: complete.completion_fingerprint,
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
                last_modified_millis,
                stale_payload,
            })),
        ))
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: ObjectMetadataScanPgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_payload_reclaim_root(
            &*pg, bucket,
        )?)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_payload_reclaim_root(&*pg)?)
    }

    fn object_payload_reclaim_claim(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::object_payload_reclaim_claim(&*pg)?)
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: ObjectMetadataPgId,
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
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_delete_finalize_roots(
            &*pg, now, limit,
        )?)
    }

    fn get_bucket_delete_begin_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_delete_begin_roots(
            &*pg,
            now,
            start_after_bucket,
            limit,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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

    fn bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
        _bucket: &BucketName,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::bucket_delete_finalize_claim(&*pg)?)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_lifecycle_sweep_roots(
            &*pg, now, limit,
        )?)
    }

    fn list_buckets_with_lifecycle(
        &self,
        pg_id: BucketPgId,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::list_buckets_with_lifecycle(&*pg)?)
    }

    fn list_aborting_multipart_upload_bucket_witnesses(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Vec<AbortingMultipartUploadBucketWitness>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        PgMetadataStore::list_aborting_multipart_upload_bucket_witnesses(&*pg)
            .map_err(ObjectPgActionError::Metadata)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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
        pg_id: BucketPgId,
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

impl LocalStorageNodeClient {
    fn apply_metadata_command_and_record_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.apply_metadata_command_and_record(self.node_id.as_u32(), command)
    }

    fn remove_pending_metadata_command_slot_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.remove_pending_metadata_command_slot(self.node_id.as_u32(), command)
    }

    fn remove_pending_metadata_command_slot_inner_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        require_metadata_command_operation_deadline(deadline)?;
        pg.remove_pending_metadata_command_slot(self.node_id.as_u32(), command)
    }
}

impl MetadataCommandInspectionNodeClient for LocalStorageNodeClient {
    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        MetadataCommandNodeClient::max_metadata_command_log_index(self, pg_id, cluster_epoch)
    }

    fn max_metadata_command_log_index_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<u64, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let max = pg.max_metadata_command_log_index(cluster_epoch)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(max)
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandNodeClient::pending_metadata_command_envelope(self, pg_id, cluster_epoch)
    }

    fn pending_metadata_command_envelope_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let pending = pg.pending_metadata_command_envelope(self.node_id.as_u32(), cluster_epoch)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(pending)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        MetadataCommandNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn metadata_command_replica_state_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        require_metadata_command_operation_deadline(deadline)?;
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let state = pg.metadata_command_replica_state()?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(state)
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        MetadataCommandNodeClient::metadata_command_checkpoint(self, pg_id, cluster_epoch)
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        MetadataCommandNodeClient::metadata_command_checkpoint_candidates(
            self,
            pg_id,
            cluster_epoch,
            max_applied_log_index,
            limit,
        )
    }

    fn metadata_command_replica_state_can_initialize(
        &self,
        pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_replica_state_can_initialize()
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(self, pg_id, command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_abandon_acceptance(self, pg_id, command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        MetadataCommandNodeClient::applied_metadata_command_log_entry_hashes(self, pg_id, command)
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        require_metadata_command_operation_deadline(deadline)?;
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let hashes =
            pg.applied_metadata_command_log_entry_hashes(self.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(hashes)
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        MetadataCommandNodeClient::retained_metadata_command_log_hashes(
            self,
            pg_id,
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
        MetadataCommandNodeClient::retained_metadata_command_log_entries(
            self,
            pg_id,
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
        MetadataCommandNodeClient::has_matching_applied_metadata_command_log_entry(
            self,
            pg_id,
            command,
            expected_previous_log_hash,
        )
    }

    #[cfg(test)]
    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_abandoned(self.node_id.as_u32(), command)
    }

    fn metadata_command_abandoned_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let abandoned = pg.metadata_command_abandoned(self.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(abandoned)
    }
}

struct LocalMetadataCommandPeeringRoute<'a> {
    client: &'a LocalStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
}

impl LocalMetadataCommandPeeringRoute<'_> {
    fn validate_command_route(&self, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        if command.id().pg_id() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: self.client.node_id.as_u32(),
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id.get(),
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(())
    }

    fn validate_transfer_commands(
        &self,
        commands: &[MetadataTransferCommand],
    ) -> Result<(), StoreError> {
        commands
            .iter()
            .try_for_each(|command| self.validate_command_route(&command.command))
    }

    fn validate_checkpoint_pg(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<(), StoreError> {
        if checkpoint.pg_id != self.pg_id {
            return Err(StoreError::MetadataCheckpointInvalid {
                node_id: self.client.node_id.as_u32(),
                pg_id: self.pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                reason: format!(
                    "checkpoint PG {} does not match captured peering PG {}",
                    checkpoint.pg_id.get(),
                    self.pg_id.get()
                ),
            });
        }
        Ok(())
    }
}

impl MetadataCommandPeeringNodeClient for LocalStorageNodeClient {
    fn open_metadata_command_peering_route(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandPeeringRoute + '_>, StoreError> {
        drop(self.storage_node.get_pg(pg_id.get())?);
        Ok(Box::new(LocalMetadataCommandPeeringRoute {
            client: self,
            pg_id,
            cluster_epoch,
        }))
    }
}

impl MetadataCommandPeeringRoute for LocalMetadataCommandPeeringRoute<'_> {
    fn validate_metadata_command_replay_state(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.validate_metadata_command_replay_state(self.client.node_id.as_u32(), self.cluster_epoch)
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.validate_metadata_command_replay_state_preserving_pending_slot(
            self.client.node_id.as_u32(),
            self.cluster_epoch,
        )
    }

    fn initialize_metadata_transfer_empty_state(
        &self,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.initialize_metadata_transfer_empty_state(
            self.client.node_id.as_u32(),
            self.cluster_epoch,
            expected_state_digest,
        )
    }

    fn initialize_metadata_transfer_matching_state(
        &self,
        applied_log_index: u64,
        applied_log_hash: MetadataCommandLogHash,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.initialize_metadata_transfer_matching_state(
            self.client.node_id.as_u32(),
            self.cluster_epoch,
            applied_log_index,
            applied_log_hash,
            expected_state_digest,
        )
    }

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        commands: &[MetadataTransferCommand],
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_transfer_commands(commands)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.adopt_metadata_transfer_state_from_rebased_commands(
            self.client.node_id.as_u32(),
            self.cluster_epoch,
            commands,
            expected_state_digest,
        )
    }

    fn install_metadata_transfer_checkpoint_base(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_checkpoint_pg(checkpoint)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.install_metadata_transfer_checkpoint_base(
            self.client.node_id.as_u32(),
            self.cluster_epoch,
            checkpoint,
        )
    }

    fn replay_metadata_command_for_peering(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.validate_command_route(command)
            .map_err(BucketSnapshotLoadError::Store)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.apply_metadata_command_and_record(self.client.node_id.as_u32(), command)
    }
}

struct LocalMetadataCommandRecoveryCriticalSection {
    client: LocalStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
}

impl LocalMetadataCommandRecoveryCriticalSection {
    fn validate_command_route(&self, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        if command.id().pg_id() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: self.client.node_id.as_u32(),
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id.get(),
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(())
    }

    fn validate_optional_command_route(
        &self,
        command: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), StoreError> {
        command.map_or(Ok(()), |command| self.validate_command_route(command))
    }
}

struct LocalMetadataCommandRecoveryReplicaRoute<'a> {
    client: &'a LocalStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    authorized_source: &'a MetadataCommandEnvelope,
    abandoned_source: Option<&'a MetadataCommandEnvelope>,
    command: &'a MetadataCommandEnvelope,
}

impl LocalStorageNodeClient {
    fn metadata_command_recovery_replica_route<'a>(
        &'a self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<LocalMetadataCommandRecoveryReplicaRoute<'a>, StoreError> {
        let validator = LocalMetadataCommandRecoveryCriticalSection {
            client: self.clone(),
            pg_id,
            cluster_epoch,
        };
        validator.validate_command_route(authorized_source)?;
        validator.validate_optional_command_route(abandoned_source)?;
        validator.validate_command_route(command)?;
        validate_metadata_command_recovery_certificate(
            authorized_source,
            abandoned_source,
            command,
        )
        .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
            operation: "open metadata command recovery replica route",
        })?;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        if command.payload() != authorized_source.payload() {
            let abandoned_source = abandoned_source
                .expect("validated recovery follow-up must carry its abandoned source");
            if !pg.metadata_command_abandoned(self.node_id.as_u32(), abandoned_source)? {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "open metadata command recovery replica route",
                });
            }
        }
        drop(pg);
        Ok(LocalMetadataCommandRecoveryReplicaRoute {
            client: self,
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        })
    }
}

impl MetadataCommandRecoveryNodeClient for LocalStorageNodeClient {
    fn open_metadata_command_recovery_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandRecoveryCriticalSection>, StoreError> {
        drop(self.storage_node.get_pg(pg_id.get())?);
        Ok(Box::new(LocalMetadataCommandRecoveryCriticalSection {
            client: self.clone(),
            pg_id,
            cluster_epoch,
        }))
    }

    fn open_metadata_command_recovery_replica_apply_route<'a>(
        &'a self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<Box<dyn MetadataCommandRecoveryReplicaApplyRoute + 'a>, StoreError> {
        Ok(Box::new(self.metadata_command_recovery_replica_route(
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        )?))
    }

    fn open_metadata_command_recovery_replica_abandon_route<'a>(
        &'a self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<Box<dyn MetadataCommandRecoveryReplicaAbandonRoute + 'a>, StoreError> {
        Ok(Box::new(self.metadata_command_recovery_replica_route(
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        )?))
    }
}

impl MetadataCommandRecoveryReplicaApplyRoute for LocalMetadataCommandRecoveryReplicaRoute<'_> {
    fn apply(self: Box<Self>) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let route = LocalMetadataCommandRecoveryCriticalSection {
            client: self.client.clone(),
            pg_id: self.pg_id,
            cluster_epoch: self.cluster_epoch,
        };
        route.apply_metadata_command_and_record_for_recovery(
            self.authorized_source,
            self.abandoned_source,
            self.command,
        )
    }

    fn apply_until(
        self: Box<Self>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        require_metadata_command_operation_deadline(deadline)
            .map_err(MetadataCommandApplyError::not_sent)?;
        self.apply().map_err(MetadataCommandApplyError::definitive)
    }
}

impl MetadataCommandRecoveryReplicaAbandonRoute for LocalMetadataCommandRecoveryReplicaRoute<'_> {
    fn record_abandoned(self: Box<Self>) -> Result<MetadataCommandReplicaState, StoreError> {
        let route = LocalMetadataCommandRecoveryCriticalSection {
            client: self.client.clone(),
            pg_id: self.pg_id,
            cluster_epoch: self.cluster_epoch,
        };
        route.record_metadata_command_abandoned(self.command)
    }

    fn record_abandoned_until(
        self: Box<Self>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        require_metadata_command_operation_deadline(deadline)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let state =
            pg.record_metadata_command_abandoned(self.client.node_id.as_u32(), self.command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(state)
    }
}

impl MetadataCommandRecoveryCriticalSection for LocalMetadataCommandRecoveryCriticalSection {
    fn max_metadata_command_log_index(&self) -> Result<u64, StoreError> {
        MetadataCommandInspectionNodeClient::max_metadata_command_log_index(
            &self.client,
            self.pg_id,
            self.cluster_epoch,
        )
    }

    fn max_metadata_command_log_index_until(&self, deadline: Instant) -> Result<u64, StoreError> {
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let max = pg.max_metadata_command_log_index(self.cluster_epoch)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(max)
    }

    fn pending_metadata_command_envelope(
        &self,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandInspectionNodeClient::pending_metadata_command_envelope(
            &self.client,
            self.pg_id,
            self.cluster_epoch,
        )
    }

    fn pending_metadata_command_envelope_until(
        &self,
        deadline: Instant,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let pending =
            pg.pending_metadata_command_envelope(self.client.node_id.as_u32(), self.cluster_epoch)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(pending)
    }

    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_command_route(command)?;
        MetadataCommandInspectionNodeClient::metadata_command_acceptance(
            &self.client,
            self.pg_id,
            command,
        )
    }

    fn metadata_command_abandon_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_command_route(command)?;
        MetadataCommandInspectionNodeClient::metadata_command_abandon_acceptance(
            &self.client,
            self.pg_id,
            command,
        )
    }

    fn metadata_command_abandon_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let acceptance =
            pg.metadata_command_abandon_acceptance(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(acceptance)
    }

    fn pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let started =
            pg.pending_metadata_command_publication_started(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(started)
    }

    fn mark_pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyError> {
        self.validate_command_route(command)
            .map_err(MetadataCommandApplyError::not_sent)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)
            .map_err(MetadataCommandApplyError::not_sent)?;
        pg.mark_pending_metadata_command_publication_started(self.client.node_id.as_u32(), command)
            .map_err(MetadataCommandApplyError::definitive)?;
        require_metadata_command_operation_deadline(deadline)
            .map_err(MetadataCommandApplyError::may_have_applied)
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let hashes =
            pg.applied_metadata_command_log_entry_hashes(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(hashes)
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        let deadline = Instant::now()
            .checked_add(STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT)
            .ok_or_else(|| {
                MetadataCommandPendingSlotReplaceError::not_sent(
                    crate::node_client::storage_rpc_deadline_expired(
                        "set local metadata command pending slot replace deadline",
                    ),
                )
            })?;
        self.replace_pending_metadata_command_slot_for_reissue_until(
            previous,
            replacement,
            bucket,
            deadline,
        )
    }

    fn replace_pending_metadata_command_slot_for_reissue_until(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        self.validate_command_route(previous)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        self.validate_command_route(replacement)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        let result = pg.replace_pending_metadata_command_slot_for_reissue(
            self.client.node_id.as_u32(),
            previous,
            replacement,
            bucket,
        );
        if Instant::now() >= deadline {
            return Err(MetadataCommandPendingSlotReplaceError::may_have_applied(
                crate::node_client::storage_rpc_deadline_expired(
                    "local metadata command pending slot replace deadline expired",
                ),
            ));
        }
        result.map_err(|error| match error {
            crate::pg_store::PendingMetadataCommandSlotReplaceError::Definitive(source) => {
                MetadataCommandPendingSlotReplaceError::definitive(source)
            }
            crate::pg_store::PendingMetadataCommandSlotReplaceError::MayHaveApplied(source) => {
                MetadataCommandPendingSlotReplaceError::may_have_applied(source)
            }
        })
    }

    fn replace_pending_metadata_command_slot_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        self.validate_command_route(authorized_source)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        self.validate_optional_command_route(abandoned_source)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        MetadataCommandRecoveryCriticalSection::replace_pending_metadata_command_slot_for_reissue(
            self,
            previous,
            replacement,
            bucket,
        )
    }

    fn replace_pending_metadata_command_slot_for_recovery_until(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        self.validate_command_route(authorized_source)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        self.validate_optional_command_route(abandoned_source)
            .map_err(MetadataCommandPendingSlotReplaceError::not_sent)?;
        MetadataCommandRecoveryCriticalSection::replace_pending_metadata_command_slot_for_reissue_until(
            self,
            previous,
            replacement,
            bucket,
            deadline,
        )
    }

    fn apply_metadata_command_and_record_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.validate_command_route(authorized_source)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_optional_command_route(abandoned_source)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_command_route(command)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.client
            .apply_metadata_command_and_record_inner(self.pg_id, command)
    }

    fn record_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_command_route(command)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.record_metadata_command_abandoned(self.client.node_id.as_u32(), command)
    }

    fn record_metadata_command_abandoned_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let state = pg.record_metadata_command_abandoned(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(state)
    }
}

struct LocalRetainedStreamUploadAbortMetadataRoute<'a> {
    client: &'a LocalStorageNodeClient,
    prepared: &'a PreparedRetainedStreamUploadAbort,
}

impl RetainedMetadataCommandNodeClient for LocalStorageNodeClient {
    fn open_retained_stream_upload_abort_route<'a>(
        &'a self,
        prepared: &'a PreparedRetainedStreamUploadAbort,
    ) -> Result<Box<dyn RetainedStreamUploadAbortMetadataRoute + 'a>, StoreError> {
        self.storage_node.require_open_pg(prepared.pg_id().get())?;
        Ok(Box::new(LocalRetainedStreamUploadAbortMetadataRoute {
            client: self,
            prepared,
        }))
    }
}

impl RetainedStreamUploadAbortMetadataRoute for LocalRetainedStreamUploadAbortMetadataRoute<'_> {
    fn apply(&self) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.client.apply_metadata_command_and_record_inner(
            self.prepared.pg_id().pg_id(),
            self.prepared.command(),
        )
    }

    fn finish(&self) -> Result<bool, StoreError> {
        self.client.remove_pending_metadata_command_slot_inner(
            self.prepared.pg_id().pg_id(),
            self.prepared.command(),
        )
    }
}

struct LocalMetadataCommandCriticalSection {
    client: LocalStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
}

impl LocalMetadataCommandCriticalSection {
    fn validate_command_route(&self, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        if command.id().pg_id() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: self.client.node_id.as_u32(),
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id.get(),
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(())
    }
}

impl MetadataCommandCriticalSection for LocalMetadataCommandCriticalSection {
    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_command_route(command)?;
        let pg = self.client.storage_node.get_pg(self.pg_id.get())?;
        pg.metadata_command_acceptance(self.client.node_id.as_u32(), command)
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let hashes =
            pg.applied_metadata_command_log_entry_hashes(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(hashes)
    }

    fn metadata_command_abandon_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let acceptance =
            pg.metadata_command_abandon_acceptance(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(acceptance)
    }

    fn pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.validate_command_route(command)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)?;
        let started =
            pg.pending_metadata_command_publication_started(self.client.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(started)
    }

    fn mark_pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyError> {
        self.validate_command_route(command)
            .map_err(MetadataCommandApplyError::not_sent)?;
        let pg = self
            .client
            .storage_node
            .get_pg_until(self.pg_id.get(), deadline)
            .map_err(MetadataCommandApplyError::not_sent)?;
        pg.mark_pending_metadata_command_publication_started(self.client.node_id.as_u32(), command)
            .map_err(MetadataCommandApplyError::definitive)?;
        require_metadata_command_operation_deadline(deadline)
            .map_err(MetadataCommandApplyError::may_have_applied)
    }

    fn apply_metadata_command_and_record(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.validate_command_route(command)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.client
            .apply_metadata_command_and_record_inner(self.pg_id, command)
    }
}

impl MetadataCommandNodeClient for LocalStorageNodeClient {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandCriticalSection>, StoreError> {
        drop(self.storage_node.get_pg(pg_id.get())?);
        Ok(Box::new(LocalMetadataCommandCriticalSection {
            client: self.clone(),
            pg_id,
            cluster_epoch,
        }))
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

    fn try_insert_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        pg.try_insert_pending_metadata_command_slot(self.node_id.as_u32(), command, bucket)
    }

    fn try_insert_pending_metadata_command_slot_classified_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<(), MetadataCommandPendingSlotInsertError> {
        let pg = self
            .storage_node
            .get_pg_until(pg_id.get(), deadline)
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        require_metadata_command_operation_deadline(deadline)
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        pg.try_insert_pending_metadata_command_slot_classified(
            self.node_id.as_u32(),
            command,
            bucket,
        )
        .map_err(|error| match error {
            crate::pg_store::PendingMetadataCommandSlotInsertError::Definitive(source) => {
                MetadataCommandPendingSlotInsertError::definitive(source)
            }
            crate::pg_store::PendingMetadataCommandSlotInsertError::MayHaveApplied(source) => {
                MetadataCommandPendingSlotInsertError::may_have_applied(source)
            }
        })
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        pg.try_insert_pending_metadata_command_slot(self.node_id.as_u32(), command, bucket)
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        require_metadata_command_operation_deadline(deadline)?;
        pg.try_insert_pending_metadata_command_slot(self.node_id.as_u32(), command, bucket)
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence_classified_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), MetadataCommandPendingSlotInsertError> {
        let pg = self
            .storage_node
            .get_pg_until(pg_id.get(), deadline)
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        effect_fence
            .require_valid_for(command.id().cluster_epoch())
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        require_metadata_command_operation_deadline(deadline)
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        pg.try_insert_pending_metadata_command_slot_classified(
            self.node_id.as_u32(),
            command,
            bucket,
        )
        .map_err(|error| match error {
            crate::pg_store::PendingMetadataCommandSlotInsertError::Definitive(source) => {
                MetadataCommandPendingSlotInsertError::definitive(source)
            }
            crate::pg_store::PendingMetadataCommandSlotInsertError::MayHaveApplied(source) => {
                MetadataCommandPendingSlotInsertError::may_have_applied(source)
            }
        })
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

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        pg.try_insert_bucket_control_pending_metadata_command_slot(
            self.node_id.as_u32(),
            command,
            bucket,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        require_metadata_command_operation_deadline(deadline)?;
        pg.try_insert_bucket_control_pending_metadata_command_slot(
            self.node_id.as_u32(),
            command,
            bucket,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        require_metadata_command_operation_deadline(deadline)?;
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
        self.remove_pending_metadata_command_slot_inner(pg_id, command)
    }

    fn remove_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.remove_pending_metadata_command_slot_inner_until(pg_id, command, deadline)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_replica_state()
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_checkpoint(self.node_id.as_u32(), cluster_epoch)
    }

    fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let checkpoint =
            pg.record_current_metadata_command_checkpoint(self.node_id.as_u32(), cluster_epoch)?;
        Ok(MetadataCommandReplicaState {
            cluster_epoch: checkpoint.cluster_epoch,
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash,
            state_digest: checkpoint.state_digest,
        })
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_checkpoint_candidates(cluster_epoch, max_applied_log_index, limit)
    }

    fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.compact_metadata_command_log(cluster_epoch)
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
        self.apply_metadata_command_and_record_inner(pg_id, command)
    }

    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_metadata_command_abandoned(self.node_id.as_u32(), command)
    }

    fn record_metadata_command_abandoned_on_replica_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        let state = pg.record_metadata_command_abandoned(self.node_id.as_u32(), command)?;
        require_metadata_command_operation_deadline(deadline)?;
        Ok(state)
    }
}
