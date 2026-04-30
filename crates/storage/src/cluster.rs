use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Mutex, OnceLock};

use ec::{EcConfig, ErasureCodec};
use placement::NodeId;

pub use local::{LocalClusterMap, LocalNodeStore, LocalNodeStoreConfig, LocalPgRoute};

use crate::error::{ClusterBuildError, ShardIoError, StoreError};
use crate::node::{DirectPutCommitError, SharedStorageNode};
use crate::traits::ShardStore;
use crate::types::{
    BucketName, ClusterEpoch, CommitDirectPutObjectReq, DataPgId, DirectPutCommitSnapshot,
    DirectPutWrittenSegment, EcShape, FinalizeDirectPutObjectOutcome, GenerationId,
    ObjectEncryption, ObjectKey, PgId, PrepareStreamUploadSegmentAppendReq,
    SegmentStoredBytesRequest, SessionId, ShardIndex, ShardKey, StreamUploadRecord,
    StreamUploadSegmentRecord, StreamUploadTarget, WriteAck, WrittenShardAck,
};
use crate::ObjectPgActionError;

mod local;
mod request_ops;

const TRACE_TARGET: &str = "storage";

#[cfg(any(test, feature = "test-hooks"))]
type StreamAbortHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_STREAM_ABORT_STORAGE_HOOK: OnceLock<Mutex<Option<StreamAbortHook>>> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
pub struct StreamAbortTestHookGuard;

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamAbortTestHookGuard {
    fn drop(&mut self) {
        let hook = BEFORE_STREAM_ABORT_STORAGE_HOOK.get_or_init(|| Mutex::new(None));
        *hook.lock().unwrap() = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_before_stream_abort_storage_hook() {
    let hook = BEFORE_STREAM_ABORT_STORAGE_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .clone();
    if let Some(hook) = hook {
        hook();
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
    node: Arc<SharedStorageNode>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    released: bool,
}

impl ObjectPayloadLease {
    fn new(
        node: Arc<SharedStorageNode>,
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
    ) -> Self {
        Self {
            node,
            bucket,
            key,
            generation_id,
            released: false,
        }
    }

    pub fn release(mut self) -> ReleasedObjectPayloadLease {
        let remaining =
            self.node
                .release_object_payload_lease(&self.bucket, &self.key, self.generation_id);
        self.released = true;
        ReleasedObjectPayloadLease {
            node: Arc::clone(&self.node),
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
            let _ =
                self.node
                    .release_object_payload_lease(&self.bucket, &self.key, self.generation_id);
        }
    }
}

pub struct ReleasedObjectPayloadLease {
    node: Arc<SharedStorageNode>,
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
        self.node
            .payload_reclaim_exists(&self.bucket, &self.key, self.generation_id)
    }

    pub fn enqueue_object_payload_reclaim(&self) {
        self.node
            .enqueue_object_payload_reclaim(&self.bucket, &self.key, self.generation_id);
    }
}

/// Cluster-shaped storage handle.
///
/// The initial local multihost implementation keeps metadata operations on the
/// static metadata primary while payload placement and IO move behind
/// cluster-owned APIs.
#[derive(Clone)]
pub struct StorageCluster {
    single_node: Arc<SharedStorageNode>,
    local_map: Arc<LocalClusterMap>,
    operation_epoch: ClusterEpoch,
}

impl StorageCluster {
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

    // Transitional metadata-primary bridge. Production metadata paths must pass
    // through this helper until Phase 6 replaces the bridge with routed PG
    // commands.
    fn metadata_primary_bridge_node(&self) -> Result<&SharedStorageNode, StoreError> {
        self.require_current_metadata_primary_bridge_epoch()?;
        Ok(self.single_node.as_ref())
    }

    fn metadata_primary_bridge_node_arc(&self) -> Result<Arc<SharedStorageNode>, StoreError> {
        self.require_current_metadata_primary_bridge_epoch()?;
        Ok(Arc::clone(&self.single_node))
    }

    // Read-only topology/config helpers still use the metadata-primary node
    // while PgTopology lives on SharedStorageNode.
    fn metadata_primary_topology_node(&self) -> &SharedStorageNode {
        self.single_node.as_ref()
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

    pub fn default_payload_ec_shape(&self) -> EcShape {
        self.metadata_primary_topology_node().default_ec_shape()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_stream_abort_storage_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> StreamAbortTestHookGuard {
        let slot = BEFORE_STREAM_ABORT_STORAGE_HOOK.get_or_init(|| Mutex::new(None));
        *slot.lock().unwrap() = Some(hook);
        StreamAbortTestHookGuard
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
        self.metadata_primary_bridge_node()?
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
        self.metadata_primary_bridge_node()?
            .reserve_put_object_generation(bucket, key, reservation_id)
    }

    pub fn release_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .release_object_generation_reservation(bucket, key, reservation_id)
    }

    pub fn commit_direct_put_object_from_payload_shards<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        action: impl FnOnce(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        let result = self
            .metadata_primary_bridge_node()?
            .commit_direct_put_object(req, written_shards, action);
        match result {
            Ok(Ok(outcome)) => Ok(Ok(outcome)),
            Ok(Err(error)) => {
                self.delete_direct_put_segment_payload_shards(
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                    written_shards,
                );
                Ok(Err(error))
            }
            Err(DirectPutCommitError::PrePublish(error)) => {
                self.delete_direct_put_segment_payload_shards(
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                    written_shards,
                );
                Err(error)
            }
            Err(error @ DirectPutCommitError::PostPublish(_)) => Err(error.into_action_error()),
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
        self.metadata_primary_bridge_node()?
            .create_put_object_stream_session_record(bucket, key, session_id, encryption)
    }

    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .load_stream_upload_session(bucket, key, session_id)
    }

    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .prepare_stream_segment_append(bucket, key, request)
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
        let result = self
            .metadata_primary_bridge_node()?
            .commit_stream_segment_append(
                bucket,
                key,
                session_id,
                segment_index,
                segment_record,
                shard_batch,
            );
        if result.is_err() {
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
        result
    }

    pub fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        self.require_current_metadata_primary_bridge_epoch()?;
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_before_stream_abort_storage_hook();
        let staged_segments = self
            .metadata_primary_bridge_node()?
            .abort_stream_upload_session(bucket, key, session_id)?;
        self.delete_staged_stream_segment_payload_shards_best_effort(&staged_segments);
        Ok(())
    }

    pub fn list_stream_upload_sessions_best_effort(&self) -> Vec<StreamUploadRecord> {
        let Ok(bridge_node) = self.metadata_primary_bridge_node() else {
            return Vec::new();
        };
        bridge_node.list_all_stream_uploads_best_effort()
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
        // Phase 4 ack bridge: placed payload bytes are routed by LocalClusterMap,
        // but per-shard CRC/size acks still live in metadata-primary PG stores.
        let pg = self.metadata_primary_bridge_node()?.get_pg(data_pg_id)?;
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
                emit_best_effort_payload_cleanup_error("place payload shards", &error);
                return;
            }
        };

        for shard_key in shard_keys {
            match Self::placed_payload_shard_location(&locations, shard_key) {
                Ok(location) => {
                    if let Err(error) = self.delete_payload_shard(location, shard_key) {
                        let error = shard_io_error_to_store(error);
                        emit_best_effort_payload_cleanup_error(
                            "delete placed payload shard",
                            &error,
                        );
                    }
                }
                Err(error) => {
                    emit_best_effort_payload_cleanup_error("resolve placed payload shard", &error);
                }
            }
        }
    }

    fn delete_metadata_primary_payload_shard_keys(
        &self,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) -> Result<(), ObjectPgActionError> {
        // Phase 4 ack bridge cleanup. The placed shard files are deleted
        // separately by the caller before these metadata-primary ack rows.
        let data_pg = self.metadata_primary_bridge_node()?.get_pg(data_pg_id)?;
        for shard_key in shard_keys {
            data_pg.delete_shard(shard_key)?;
        }
        Ok(())
    }

    fn delete_metadata_primary_payload_shard_keys_best_effort(
        &self,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) {
        let bridge_node = match self.metadata_primary_bridge_node() {
            Ok(bridge_node) => bridge_node,
            Err(error) => {
                emit_best_effort_payload_cleanup_error(
                    "resolve metadata-primary payload ack bridge",
                    &error,
                );
                return;
            }
        };
        let data_pg = match bridge_node.get_pg(data_pg_id) {
            Ok(data_pg) => data_pg,
            Err(error) => {
                emit_best_effort_payload_cleanup_error(
                    "resolve metadata-primary payload ack PG",
                    &error,
                );
                return;
            }
        };
        for shard_key in shard_keys {
            if let Err(error) = data_pg.delete_shard(shard_key) {
                emit_best_effort_payload_cleanup_error(
                    "delete metadata-primary payload ack",
                    &error,
                );
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

fn emit_best_effort_payload_cleanup_error(operation: &'static str, error: &StoreError) {
    let Some(trace) = observability::current_context() else {
        return;
    };
    let _ = observability::event_in_context(
        &trace,
        TRACE_TARGET,
        "payload_cleanup_best_effort_error",
        Some(format_args!("operation={operation:?} error={error}")),
    );
}
