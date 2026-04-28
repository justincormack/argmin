use std::sync::Arc;

use crate::error::StoreError;
use crate::node::SharedStorageNode;
use crate::types::{
    BucketName, CommitDirectPutObjectReq, DirectPutCommitSnapshot, DirectPutWrittenSegment,
    EcShape, FinalizeDirectPutObjectOutcome, GenerationId, ObjectEncryption, ObjectKey,
    PrepareStreamUploadSegmentAppendReq, SegmentStoredBytesRequest, SessionId, ShardKey,
    StreamUploadRecord, StreamUploadSegmentRecord, StreamUploadTarget, WriteAck, WrittenShardAck,
};
use crate::ObjectPgActionError;

mod request_ops;

/// Cluster-shaped storage handle.
///
/// Phase 1 keeps this backed by one local shared node so existing storage
/// behavior remains unchanged while coordinator code stops owning the local
/// implementation type directly. Process-local identity remains based on the
/// underlying local node until cluster-owned identity exists.
#[derive(Clone)]
pub struct StorageCluster {
    single_node: Arc<SharedStorageNode>,
}

impl StorageCluster {
    pub fn single_node(single_node: Arc<SharedStorageNode>) -> Self {
        Self { single_node }
    }

    pub fn shared_single_node(single_node: Arc<SharedStorageNode>) -> Arc<Self> {
        Arc::new(Self::single_node(single_node))
    }

    /// Temporary process-local registry key for shared coordinator workers.
    ///
    /// Multiple `StorageCluster` handles backed by the same local node keep
    /// sharing process-local workers until a real cluster identity exists.
    pub fn process_local_registry_key(&self) -> usize {
        Arc::as_ptr(&self.single_node) as usize
    }

    pub fn default_payload_ec_shape(&self) -> EcShape {
        self.single_node.default_ec_shape()
    }

    pub fn write_direct_put_segment_payload_shards(
        &self,
        transient_segment_id: &SessionId,
        segment_index: u32,
        segment_vid: GenerationId,
        segment_okh: &[u8; 16],
        data: &[u8],
    ) -> Result<DirectPutWrittenSegment, StoreError> {
        self.single_node.write_direct_put_segment_shards(
            transient_segment_id,
            segment_index,
            segment_vid,
            segment_okh,
            data,
        )
    }

    pub fn commit_direct_put_object_from_payload_shards<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        action: impl FnOnce(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        self.single_node
            .commit_direct_put_object(req, written_shards, action)
    }

    pub fn create_put_object_stream_session_record(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        self.single_node
            .create_put_object_stream_session_record(bucket, key, session_id, encryption)
    }

    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        self.single_node
            .load_stream_upload_session(bucket, key, session_id)
    }

    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        self.single_node
            .prepare_stream_segment_append(bucket, key, request)
    }

    pub fn write_stream_segment_payload_shards(
        &self,
        shard_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        self.single_node
            .write_stream_segment_shards(shard_pg_id, segment_okh, segment_vid, data)
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
        self.single_node.commit_stream_segment_append(
            bucket,
            key,
            session_id,
            segment_index,
            segment_record,
            shard_batch,
        )
    }

    pub fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        self.single_node
            .abort_stream_upload_session(bucket, key, session_id)
    }

    pub fn list_stream_upload_sessions_best_effort(&self) -> Vec<StreamUploadRecord> {
        self.single_node.list_all_stream_uploads_best_effort()
    }

    pub fn read_segment_payload_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.single_node.read_segment_stored_bytes_into(req, dst)
    }
}

impl From<Arc<SharedStorageNode>> for StorageCluster {
    fn from(single_node: Arc<SharedStorageNode>) -> Self {
        Self::single_node(single_node)
    }
}
