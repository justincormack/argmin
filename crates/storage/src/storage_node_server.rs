use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::control_plane::{
    ClusterRuntimeMapSnapshot, ControlPlaneError, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneHeartbeatSink, HeartbeatLease, NodeHeartbeat, PgRouteSnapshot,
};
use crate::data_dir::prepare_private_data_dir;
use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError};
use crate::metadata_command::{MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload};
use crate::node::SharedStorageNode;
use crate::node_client::{
    complete_multipart_expected_object_parts, BucketMetadataNodeClient,
    BucketWriteReservationNodeClient, BuildAbortMultipartUploadCommandReq,
    BuildAuthorizedAbortMultipartUploadCommandReq, BuildCompleteMultipartObjectCommandReq,
    BuildCreateMultipartUploadCommandReq, BuildCreateStreamUploadCommandReq,
    BuildDeleteCurrentObjectCommandReq, BuildDeleteSpecificObjectVersionCommandReq,
    BuildDirectPutCommitCommandReq, BuildInsertDeleteMarkerCommandReq,
    BuildPutObjectMetadataCommandReq, BuildStreamPartCommitCommandReq,
    BuildStreamPutCommitCommandReq, CreateBucketCommandBuild, CreateStreamUploadPrecondition,
    DirectPutMetadataNodeClient, InsertDeleteMarkerStalePayload, LocalStorageNodeClient,
    MarkBucketDeletingCommandBuild, ObjectGenerationMetadataNodeClient,
    ObjectListingMetadataNodeClient, ObjectMutationMetadataNodeClient,
    ObjectReadMetadataNodeClient, ObjectVersionMetadataNodeClient, ShardAckNodeClient,
    ShardScavengerNodeClient,
};
use crate::pg_store::{MetadataCommandCheckpoint, PgStore};
use crate::storage_rpc::{
    decode_abort_multipart_cleanup_request, decode_abort_multipart_command_build_request,
    decode_authorized_abort_multipart_command_build_request, decode_bucket_batch_request,
    decode_bucket_delete_finalize_claim_acquire_request,
    decode_bucket_delete_finalize_claim_record_request,
    decode_bucket_delete_finalize_roots_request, decode_bucket_list_request,
    decode_bucket_mark_deleting_command_build_request,
    decode_bucket_metadata_control_command_build_request,
    decode_bucket_metadata_control_pending_match_request, decode_bucket_pg_request,
    decode_bucket_request, decode_bucket_snapshot_pair_request, decode_bucket_snapshot_request,
    decode_bucket_subresource_get_request, decode_bucket_write_drain_begin_request,
    decode_bucket_write_drain_clear_expired_request, decode_bucket_write_drain_heartbeat_request,
    decode_bucket_write_drain_record_request, decode_bucket_write_reservation_acquire_request,
    decode_bucket_write_reservation_heartbeat_request,
    decode_bucket_write_reservation_proof_request, decode_bucket_write_reservation_record_request,
    decode_cluster_map_history_reference_summary_request,
    decode_complete_multipart_command_build_request,
    decode_completed_multipart_order_command_build_request,
    decode_completed_multipart_uploads_list_request, decode_create_bucket_command_build_request,
    decode_create_multipart_upload_command_build_request,
    decode_create_stream_upload_command_build_request,
    decode_delete_current_object_command_build_request,
    decode_delete_specific_object_command_build_request, decode_direct_put_command_build_request,
    decode_direct_put_commit_snapshot_request, decode_insert_delete_marker_command_build_request,
    decode_lifecycle_sweep_claim_acquire_request, decode_lifecycle_sweep_claim_error_request,
    decode_lifecycle_sweep_claim_heartbeat_request, decode_lifecycle_sweep_claim_record_request,
    decode_lifecycle_sweep_roots_request, decode_list_multipart_uploads_request,
    decode_list_object_versions_request, decode_list_objects_request,
    decode_metadata_command_checkpoint_candidates_request,
    decode_metadata_command_log_entry_range_request,
    decode_metadata_command_log_hash_range_request,
    decode_metadata_command_matching_applied_request, decode_metadata_command_next_id_request,
    decode_metadata_command_pending_slot_replace_request,
    decode_metadata_command_pending_slot_request, decode_metadata_command_request,
    decode_metadata_command_state_request, decode_metadata_command_transfer_adopt_request,
    decode_metadata_command_transfer_checkpoint_base_request,
    decode_metadata_command_transfer_empty_state_request,
    decode_metadata_command_transfer_matching_state_request,
    decode_multipart_completion_preflight_request, decode_multipart_completion_snapshot_request,
    decode_multipart_parts_list_request, decode_multipart_upload_load_request,
    decode_multipart_upload_match_request, decode_object_delete_snapshot_request,
    decode_object_generation_reservation_request,
    decode_object_payload_reclaim_claim_acquire_request,
    decode_object_payload_reclaim_claim_record_request,
    decode_object_payload_reclaim_exists_request, decode_object_read_auth_subject_request,
    decode_object_read_snapshot_request, decode_object_request,
    decode_object_tags_for_subject_request,
    decode_placed_segment_shard_backfill_claim_acquire_request,
    decode_placed_segment_shard_backfill_claim_error_request,
    decode_placed_segment_shard_backfill_claim_record_request,
    decode_placed_segment_shard_backfill_item_request,
    decode_placed_segment_shard_backfill_record_request,
    decode_placed_segment_shard_repair_claim_acquire_request,
    decode_placed_segment_shard_repair_claim_error_request,
    decode_placed_segment_shard_repair_claim_record_request,
    decode_placed_segment_shard_repair_item_request,
    decode_placed_segment_shard_repair_record_request, decode_proof_release_request,
    decode_put_object_metadata_command_build_request, decode_put_object_metadata_snapshot_request,
    decode_read_handle_acquire_request, decode_read_handle_release_request,
    decode_scavenger_list_files_request, decode_scavenger_observation_key_request,
    decode_scavenger_observation_record_request, decode_shard_ack_batch_request,
    decode_shard_ack_item_request, decode_shard_delete_request, decode_shard_read_range_request,
    decode_shard_read_request, decode_shard_write_request,
    decode_stream_part_commit_command_build_request, decode_stream_part_finalize_snapshot_request,
    decode_stream_put_commit_command_build_request, decode_stream_put_finalize_snapshot_request,
    decode_stream_segment_append_prepare_request, decode_stream_upload_match_request,
    decode_stream_upload_session_request, decode_stream_uploads_list_request,
    decode_stream_uploads_pg_list_request, encode_abort_multipart_cleanup_response,
    encode_bucket_delete_finalize_claim_optional_record_response,
    encode_bucket_delete_finalize_roots_response, encode_bucket_delete_finalized_response,
    encode_bucket_execution_generations_response, encode_bucket_fast_path_identities_response,
    encode_bucket_info_outcome_response, encode_bucket_list_response,
    encode_bucket_mark_deleting_command_build_response,
    encode_bucket_metadata_control_command_build_response, encode_bucket_snapshot_pair_response,
    encode_bucket_snapshot_response, encode_bucket_subresource_get_response,
    encode_bucket_write_drain_begin_response, encode_bucket_write_drain_optional_record_response,
    encode_bucket_write_reservation_record_response,
    encode_bucket_write_reservations_list_response,
    encode_cluster_map_history_reference_summary_response,
    encode_completed_multipart_order_command_build_response,
    encode_completed_multipart_uploads_list_response, encode_create_bucket_command_build_response,
    encode_direct_put_command_build_response, encode_direct_put_commit_snapshot_response,
    encode_health_response, encode_lifecycle_sweep_buckets_response,
    encode_lifecycle_sweep_claim_optional_record_response,
    encode_lifecycle_sweep_claim_record_response, encode_lifecycle_sweep_roots_response,
    encode_list_multipart_uploads_response, encode_list_object_versions_response,
    encode_list_objects_response, encode_metadata_command_acceptance_response,
    encode_metadata_command_applied_hashes_response, encode_metadata_command_bool_outcome_response,
    encode_metadata_command_bool_response, encode_metadata_command_checkpoint_candidates_response,
    encode_metadata_command_checkpoint_response, encode_metadata_command_log_compact_response,
    encode_metadata_command_log_entry_range_response,
    encode_metadata_command_log_hash_range_response,
    encode_metadata_command_max_log_index_response, encode_metadata_command_next_id_response,
    encode_metadata_command_pending_envelope_response,
    encode_metadata_command_pending_slot_insert_response,
    encode_metadata_command_pending_slot_remove_response,
    encode_metadata_command_state_outcome_response, encode_metadata_command_state_response,
    encode_multipart_completion_preflight_response, encode_multipart_completion_snapshot_response,
    encode_multipart_completion_stale_source_response, encode_multipart_management_lookup_response,
    encode_multipart_parts_list_response, encode_multipart_upload_load_response,
    encode_multipart_upload_match_response, encode_object_delete_snapshot_response,
    encode_object_generation_reservation_response, encode_object_generation_response,
    encode_object_lifecycle_version_list_response, encode_object_metadata_command_build_response,
    encode_object_payload_reclaim_claim_optional_record_response,
    encode_object_payload_reclaim_response, encode_object_read_auth_subject_response,
    encode_object_read_snapshot_response, encode_object_tags_for_subject_response,
    encode_object_version_response, encode_payload_reclaim_root_response,
    encode_placed_segment_shard_backfill_claim_optional_record_response,
    encode_placed_segment_shard_backfill_count_response,
    encode_placed_segment_shard_backfills_response,
    encode_placed_segment_shard_repair_claim_optional_record_response,
    encode_placed_segment_shard_repairs_response, encode_put_object_metadata_snapshot_response,
    encode_read_handle_acquire_response, encode_read_handle_release_response,
    encode_scavenger_list_files_response, encode_scavenger_observations_response,
    encode_scavenger_payload_references_response, encode_scavenger_shard_rows_response,
    encode_shard_ack_item_response, encode_shard_read_range_response, encode_shard_read_response,
    encode_shard_write_ack, encode_storage_rpc_error_response, encode_storage_rpc_success_response,
    encode_stream_part_finalize_snapshot_response, encode_stream_put_finalize_snapshot_response,
    encode_stream_segment_append_prepare_response, encode_stream_upload_match_response,
    encode_stream_upload_segments_response, encode_stream_upload_session_response,
    encode_stream_uploads_list_response, read_storage_rpc_request_frame_from,
    write_storage_rpc_frame_to, StorageRpcAbortMultipartCleanupResponse,
    StorageRpcAbortMultipartCommandBuildRequest,
    StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcBucketBatchRequest,
    StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse,
    StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcBucketDeleteFinalizeRootsRequest,
    StorageRpcBucketDeleteFinalizeRootsResponse, StorageRpcBucketDeleteFinalizedOutcome,
    StorageRpcBucketDeleteFinalizedResponse, StorageRpcBucketExecutionGenerationsResponse,
    StorageRpcBucketFastPathIdentitiesResponse, StorageRpcBucketInfoOutcome,
    StorageRpcBucketInfoOutcomeResponse, StorageRpcBucketListRequest, StorageRpcBucketListResponse,
    StorageRpcBucketMarkDeletingCommandBuildOutcome,
    StorageRpcBucketMarkDeletingCommandBuildRequest,
    StorageRpcBucketMarkDeletingCommandBuildResponse,
    StorageRpcBucketMetadataControlCommandBuildRequest,
    StorageRpcBucketMetadataControlCommandBuildResponse, StorageRpcBucketMetadataControlMutation,
    StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcBucketPgRequest,
    StorageRpcBucketRequest, StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotPairOutcome,
    StorageRpcBucketSnapshotPairRequest, StorageRpcBucketSnapshotPairResponse,
    StorageRpcBucketSnapshotRequest, StorageRpcBucketSnapshotResponse,
    StorageRpcBucketSubresourceGetRequest, StorageRpcBucketSubresourceGetResponse,
    StorageRpcBucketWriteDrainBeginOutcome, StorageRpcBucketWriteDrainBeginRequest,
    StorageRpcBucketWriteDrainBeginResponse, StorageRpcBucketWriteDrainClearExpiredRequest,
    StorageRpcBucketWriteDrainOptionalRecordResponse, StorageRpcBucketWriteDrainRecordRequest,
    StorageRpcBucketWriteReservationAcquireOutcome, StorageRpcBucketWriteReservationAcquireRequest,
    StorageRpcBucketWriteReservationHeartbeatRequest, StorageRpcBucketWriteReservationProofRequest,
    StorageRpcBucketWriteReservationRecordRequest, StorageRpcBucketWriteReservationRecordResponse,
    StorageRpcBucketWriteReservationsListResponse,
    StorageRpcClusterMapHistoryReferenceSummaryRequest,
    StorageRpcClusterMapHistoryReferenceSummaryResponse,
    StorageRpcCompleteMultipartCommandBuildRequest,
    StorageRpcCompletedMultipartOrderCommandBuildRequest,
    StorageRpcCompletedMultipartOrderCommandBuildResponse,
    StorageRpcCompletedMultipartUploadsListRequest,
    StorageRpcCompletedMultipartUploadsListResponse, StorageRpcCreateBucketCommandBuildOutcome,
    StorageRpcCreateBucketCommandBuildRequest, StorageRpcCreateBucketCommandBuildResponse,
    StorageRpcCreateMultipartUploadCommandBuildRequest,
    StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcCreateStreamUploadPrecondition,
    StorageRpcDeleteCurrentObjectCommandBuildRequest,
    StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcDirectPutCommandBuildOutcome,
    StorageRpcDirectPutCommandBuildRequest, StorageRpcDirectPutCommandBuildResponse,
    StorageRpcDirectPutCommitSnapshotRequest, StorageRpcDirectPutCommitSnapshotResponse,
    StorageRpcErrorCode, StorageRpcErrorResponse, StorageRpcFrame, StorageRpcHealthResponse,
    StorageRpcInsertDeleteMarkerCommandBuildRequest, StorageRpcLifecycleSweepBucketsResponse,
    StorageRpcLifecycleSweepClaimAcquireRequest, StorageRpcLifecycleSweepClaimErrorRequest,
    StorageRpcLifecycleSweepClaimHeartbeatRequest,
    StorageRpcLifecycleSweepClaimOptionalRecordResponse,
    StorageRpcLifecycleSweepClaimRecordRequest, StorageRpcLifecycleSweepClaimRecordResponse,
    StorageRpcLifecycleSweepRootsRequest, StorageRpcLifecycleSweepRootsResponse,
    StorageRpcListMultipartUploadsRequest, StorageRpcListMultipartUploadsResponse,
    StorageRpcListObjectVersionsRequest, StorageRpcListObjectVersionsResponse,
    StorageRpcListObjectsRequest, StorageRpcListObjectsResponse, StorageRpcMessageKind,
    StorageRpcMetadataCommandAcceptanceOutcome, StorageRpcMetadataCommandAcceptanceResponse,
    StorageRpcMetadataCommandAppliedHashesOutcome, StorageRpcMetadataCommandAppliedHashesResponse,
    StorageRpcMetadataCommandBoolOutcome, StorageRpcMetadataCommandBoolOutcomeResponse,
    StorageRpcMetadataCommandBoolResponse, StorageRpcMetadataCommandCheckpointCandidatesRequest,
    StorageRpcMetadataCommandCheckpointCandidatesResponse,
    StorageRpcMetadataCommandCheckpointResponse, StorageRpcMetadataCommandLogCompactResponse,
    StorageRpcMetadataCommandLogEntryRangeResponse, StorageRpcMetadataCommandLogHashRangeRequest,
    StorageRpcMetadataCommandLogHashRangeResponse, StorageRpcMetadataCommandMatchingAppliedRequest,
    StorageRpcMetadataCommandMaxLogIndexResponse, StorageRpcMetadataCommandNextIdOutcome,
    StorageRpcMetadataCommandNextIdRequest, StorageRpcMetadataCommandNextIdResponse,
    StorageRpcMetadataCommandPendingEnvelopeResponse,
    StorageRpcMetadataCommandPendingSlotInsertOutcome,
    StorageRpcMetadataCommandPendingSlotInsertResponse,
    StorageRpcMetadataCommandPendingSlotRemoveResponse,
    StorageRpcMetadataCommandPendingSlotReplaceRequest,
    StorageRpcMetadataCommandPendingSlotRequest, StorageRpcMetadataCommandRequest,
    StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateOutcomeResponse,
    StorageRpcMetadataCommandStateRequest, StorageRpcMetadataCommandStateResponse,
    StorageRpcMetadataCommandTransferAdoptRequest,
    StorageRpcMetadataCommandTransferCheckpointBaseRequest,
    StorageRpcMetadataCommandTransferEmptyStateRequest,
    StorageRpcMetadataCommandTransferMatchingStateRequest,
    StorageRpcMultipartCompletionPreflightOutcome, StorageRpcMultipartCompletionPreflightRequest,
    StorageRpcMultipartCompletionPreflightResponse, StorageRpcMultipartCompletionSnapshotOutcome,
    StorageRpcMultipartCompletionSnapshotRequest, StorageRpcMultipartCompletionSnapshotResponse,
    StorageRpcMultipartCompletionStaleSourceResponse, StorageRpcMultipartManagementLookupResponse,
    StorageRpcMultipartPartsListOutcome, StorageRpcMultipartPartsListRequest,
    StorageRpcMultipartPartsListResponse, StorageRpcMultipartUploadLoadOutcome,
    StorageRpcMultipartUploadLoadRequest, StorageRpcMultipartUploadLoadResponse,
    StorageRpcMultipartUploadMatchRequest, StorageRpcMultipartUploadMatchResponse,
    StorageRpcObjectDeleteSnapshotRequest, StorageRpcObjectDeleteSnapshotResponse,
    StorageRpcObjectGenerationReservationOutcome, StorageRpcObjectGenerationReservationRequest,
    StorageRpcObjectGenerationReservationResponse, StorageRpcObjectGenerationResponse,
    StorageRpcObjectLifecycleVersionListResponse, StorageRpcObjectMetadataCommandBuildOutcome,
    StorageRpcObjectMetadataCommandBuildResponse,
    StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse,
    StorageRpcObjectPayloadReclaimClaimRecordRequest, StorageRpcObjectPayloadReclaimExistsRequest,
    StorageRpcObjectPayloadReclaimResponse, StorageRpcObjectReadAuthSubjectOutcome,
    StorageRpcObjectReadAuthSubjectRequest, StorageRpcObjectReadAuthSubjectResponse,
    StorageRpcObjectReadSnapshotOutcome, StorageRpcObjectReadSnapshotRequest,
    StorageRpcObjectReadSnapshotResponse, StorageRpcObjectRequest,
    StorageRpcObjectTagsForSubjectOutcome, StorageRpcObjectTagsForSubjectRequest,
    StorageRpcObjectTagsForSubjectResponse, StorageRpcObjectVersionResponse,
    StorageRpcPayloadReclaimRootResponse, StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
    StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
    StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse,
    StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
    StorageRpcPlacedSegmentShardBackfillItemRequest,
    StorageRpcPlacedSegmentShardBackfillRecordRequest,
    StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
    StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
    StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse,
    StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
    StorageRpcPlacedSegmentShardRepairItemRequest, StorageRpcPlacedSegmentShardRepairRecordRequest,
    StorageRpcProofReleaseRequest, StorageRpcPutObjectMetadataCommandBuildRequest,
    StorageRpcPutObjectMetadataSnapshotOutcome, StorageRpcPutObjectMetadataSnapshotRequest,
    StorageRpcPutObjectMetadataSnapshotResponse, StorageRpcReadHandleAcquireRequest,
    StorageRpcReadHandleAcquireResponse, StorageRpcReadHandleReleaseRequest,
    StorageRpcReadHandleReleaseResponse, StorageRpcScavengerListFilesRequest,
    StorageRpcScavengerObservationKeyRequest, StorageRpcScavengerObservationRecordRequest,
    StorageRpcShardAckBatchRequest, StorageRpcShardAckItem, StorageRpcShardAckItemRequest,
    StorageRpcShardDeleteRequest, StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest,
    StorageRpcShardWriteRequest, StorageRpcStreamError,
    StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcStreamPartFinalizeSnapshotRequest,
    StorageRpcStreamPartFinalizeSnapshotResponse, StorageRpcStreamPutCommitCommandBuildRequest,
    StorageRpcStreamPutFinalizeSnapshotRequest, StorageRpcStreamPutFinalizeSnapshotResponse,
    StorageRpcStreamSegmentAppendPrepareOutcome, StorageRpcStreamSegmentAppendPrepareRequest,
    StorageRpcStreamSegmentAppendPrepareResponse, StorageRpcStreamUploadMatchRequest,
    StorageRpcStreamUploadMatchResponse, StorageRpcStreamUploadSegmentsOutcome,
    StorageRpcStreamUploadSegmentsResponse, StorageRpcStreamUploadSessionOutcome,
    StorageRpcStreamUploadSessionRequest, StorageRpcStreamUploadSessionResponse,
    StorageRpcStreamUploadsListRequest, StorageRpcStreamUploadsListResponse,
    StorageRpcStreamUploadsPgListRequest, STORAGE_RPC_FRAME_ENCODING_VERSION,
    STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES, STORAGE_RPC_MAX_PAYLOAD_LEN,
};
use crate::traits::ShardStore;
use crate::types::{BucketState, ClusterEpoch, GenerationId, PgId, PgState, SessionId, WriteAck};
use crate::types::{
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardRepairClaimAcquire, PlacedSegmentShardRepairClaimRecord,
};
use crate::{
    BucketName, BucketWriteDrainError, EcShape, NodeId, ObjectPgActionError, ShardKey,
    ShardLocation,
};

#[cfg(test)]
type MetadataCommandBeforeWaitHook = Arc<dyn Fn(PgId) + Send + Sync>;

const DATA_DIR_LOCK_FILE: &str = ".argmin-storage-node.lock";
const STORAGE_NODE_INCARNATION_FILE: &str = "control-plane-node-incarnation";
const STORAGE_NODE_INCARNATION_TMP_FILE: &str = ".control-plane-node-incarnation.tmp";
const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const STORAGE_NODE_MAX_ACTIVE_SESSIONS: usize = 1024;
const STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION: usize = 4096;
const STORAGE_NODE_MAX_LIVE_READ_OPERATIONS: usize = 16 * 1024;
const STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS: usize = 64 * 1024;

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[derive(Debug, Clone)]
pub struct StorageNodeProcessConfig {
    pub node_id: NodeId,
    pub cluster_epoch: ClusterEpoch,
    pub route_map_valid_until_ms: Option<u64>,
    pub data_dir: PathBuf,
    pub default_ec_shape: EcShape,
    pub pg_ids: Vec<u32>,
    pub socket_path: PathBuf,
    pub pg_routes: Vec<StorageNodePgRoute>,
    pub historical_pg_routes: Vec<StorageNodePgRoute>,
}

#[derive(Debug, Clone)]
pub struct StorageNodeControlPlaneRefresh {
    lease: HeartbeatLease,
    runtime_map: ClusterRuntimeMapSnapshot,
    next_config: StorageNodeProcessConfig,
}

impl StorageNodeControlPlaneRefresh {
    #[must_use]
    pub fn lease(&self) -> &HeartbeatLease {
        &self.lease
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn next_config(&self) -> &StorageNodeProcessConfig {
        &self.next_config
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        HeartbeatLease,
        ClusterRuntimeMapSnapshot,
        StorageNodeProcessConfig,
    ) {
        (self.lease, self.runtime_map, self.next_config)
    }
}

impl StorageNodeProcessConfig {
    pub fn from_runtime_map(
        node_id: NodeId,
        data_dir: impl Into<PathBuf>,
        default_ec_shape: EcShape,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Self, StorageNodeServerError> {
        let node = runtime_map
            .nodes()
            .iter()
            .find(|node| node.node_id() == node_id)
            .ok_or(StorageNodeServerError::RuntimeMapNodeNotFound {
                node_id: node_id.as_u32(),
                cluster_epoch: runtime_map.cluster_epoch(),
            })?;
        let pg_routes: Vec<StorageNodePgRoute> = runtime_map
            .pg_routes()
            .iter()
            .map(StorageNodePgRoute::from)
            .collect();
        let historical_pg_routes: Vec<StorageNodePgRoute> = runtime_map
            .historical_pg_routes()
            .iter()
            .map(StorageNodePgRoute::from)
            .collect();
        let pg_ids: Vec<u32> = pg_routes.iter().map(|route| route.pg_id).collect();
        validate_pg_ids(&pg_ids)?;
        validate_pg_routes(&pg_ids, &pg_routes)?;

        Ok(Self {
            node_id,
            cluster_epoch: runtime_map.cluster_epoch(),
            route_map_valid_until_ms: runtime_map.valid_until_ms(),
            data_dir: data_dir.into(),
            default_ec_shape,
            pg_ids,
            socket_path: PathBuf::from(node.endpoint()),
            pg_routes,
            historical_pg_routes,
        })
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.route_map_valid_until_ms
    }

    pub fn is_route_map_valid_at(&self, now_ms: u64) -> bool {
        self.route_map_valid_until_ms
            .is_none_or(|valid_until_ms| valid_until_ms > now_ms)
    }

    pub fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StorageNodeServerError> {
        match self.route_map_valid_until_ms {
            Some(valid_until_ms) if valid_until_ms <= now_ms => {
                Err(StorageNodeServerError::RouteMapExpired {
                    cluster_epoch: self.cluster_epoch,
                    valid_until_ms,
                    now_ms,
                })
            }
            _ => Ok(()),
        }
    }

    pub fn validate_runtime_refresh_from(
        &self,
        current: &StorageNodeProcessConfig,
    ) -> Result<(), StorageNodeServerError> {
        if self.node_id != current.node_id {
            return Err(StorageNodeServerError::RuntimeRefreshNodeChanged {
                current: current.node_id.as_u32(),
                candidate: self.node_id.as_u32(),
            });
        }
        if self.data_dir != current.data_dir {
            return Err(StorageNodeServerError::RuntimeRefreshDataDirChanged {
                current: current.data_dir.clone(),
                candidate: self.data_dir.clone(),
            });
        }
        if self.default_ec_shape != current.default_ec_shape {
            return Err(StorageNodeServerError::RuntimeRefreshEcShapeChanged {
                current: current.default_ec_shape,
                candidate: self.default_ec_shape,
            });
        }
        if self.socket_path != current.socket_path {
            return Err(StorageNodeServerError::RuntimeRefreshSocketPathChanged {
                current: current.socket_path.clone(),
                candidate: self.socket_path.clone(),
            });
        }
        Ok(())
    }

    pub fn control_plane_heartbeat(
        &self,
        node: &SharedStorageNode,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
    ) -> Result<NodeHeartbeat, StorageNodeServerError> {
        for route in &self.pg_routes {
            if route.cluster_epoch != self.cluster_epoch {
                return Err(StorageNodeServerError::RouteEpochMismatch {
                    pg_id: route.pg_id,
                    route_epoch: route.cluster_epoch,
                    config_epoch: self.cluster_epoch,
                });
            }
        }
        let endpoint =
            self.socket_path
                .to_str()
                .ok_or_else(|| StorageNodeServerError::SocketPathNotUtf8 {
                    path: self.socket_path.clone(),
                })?;
        node.control_plane_heartbeat(
            self.node_id,
            node_incarnation,
            endpoint,
            self.cluster_epoch,
            requested_lease_duration_ms,
            self.pg_routes
                .iter()
                .filter(|route| route.acting_set.contains(&self.node_id))
                .map(|route| (PgId::new(route.pg_id), route.state)),
        )
        .map_err(StorageNodeServerError::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodePgRoute {
    pub pg_id: u32,
    pub cluster_epoch: ClusterEpoch,
    pub state: PgState,
    pub primary_node_id: NodeId,
    pub acting_set: Vec<NodeId>,
}

impl From<&PgRouteSnapshot> for StorageNodePgRoute {
    fn from(route: &PgRouteSnapshot) -> Self {
        Self {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        }
    }
}

fn storage_node_rpc_trace_context(node_id: NodeId, request_id: u64) -> observability::TraceContext {
    observability::TraceContext::from_ids(
        format!("storage-node-{}-rpc-{}", node_id.as_u32(), request_id),
        format!("storage-node-{}-rpc-{}", node_id.as_u32(), request_id),
    )
}

fn emit_storage_node_metadata_command_log_conflict(
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    maybe_print_storage_node_metadata_command_conflict_diagnostic(
        "log_conflict",
        node_id,
        pg_id,
        cluster_epoch,
        log_index,
        command_kind,
    );
    let _ = observability::emit_metadata_command_conflict(
        "storage",
        observability::MetadataCommandConflictSummary {
            node_id: Some(node_id),
            pg_id,
            cluster_epoch: cluster_epoch.get(),
            log_index: Some(log_index),
            kind: "log_conflict",
            command_kind,
        },
    );
}

fn metadata_command_log_conflict_kind_from_pg(
    pg: &crate::PgStore,
    node_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
) -> Option<&'static str> {
    pg.metadata_command_log_entry_command_kind_name(cluster_epoch, log_index)
        .ok()
        .flatten()
        .or_else(|| {
            pg.pending_metadata_command_envelope(node_id, cluster_epoch)
                .ok()
                .flatten()
                .and_then(|command| {
                    (command.id().log_index().get() == log_index)
                        .then(|| command.payload().kind_name())
                })
        })
}

fn emit_storage_node_metadata_command_log_conflict_for_pg(
    pg: &crate::PgStore,
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    emit_storage_node_metadata_command_log_conflict(
        node_id,
        pg_id,
        cluster_epoch,
        log_index,
        command_kind.or_else(|| {
            metadata_command_log_conflict_kind_from_pg(pg, node_id, cluster_epoch, log_index)
        }),
    );
}

fn emit_storage_node_metadata_command_pending_conflict(
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    candidate_log_index: u64,
    command_kind: Option<&'static str>,
) {
    maybe_print_storage_node_metadata_command_conflict_diagnostic(
        "pending_slot_conflict",
        node_id,
        pg_id,
        cluster_epoch,
        candidate_log_index,
        command_kind,
    );
    let _ = observability::emit_metadata_command_conflict(
        "storage",
        observability::MetadataCommandConflictSummary {
            node_id: Some(node_id),
            pg_id,
            cluster_epoch: cluster_epoch.get(),
            log_index: Some(candidate_log_index),
            kind: "pending_slot_conflict",
            command_kind,
        },
    );
}

fn maybe_print_storage_node_metadata_command_conflict_diagnostic(
    kind: &'static str,
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    if std::env::var_os("ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS").is_none() {
        return;
    }
    eprintln!(
        "metadata command conflict source=storage-node kind={kind} node_id={node_id} pg_id={pg_id} cluster_epoch={} log_index={log_index} command_kind={}",
        cluster_epoch.get(),
        command_kind.unwrap_or("unknown")
    );
}

fn maybe_emit_storage_rpc_error(node_id: NodeId, kind: StorageRpcMessageKind, payload: &[u8]) {
    if !matches!(payload.first(), Some(1)) {
        return;
    }
    let Ok(Err(error)) = crate::storage_rpc::decode_storage_rpc_response_payload(payload) else {
        return;
    };
    let rpc_kind = format!("{kind:?}");
    let error_code = format!("{:?}", error.code);
    let _ = observability::emit_storage_rpc_error(
        "storage",
        observability::StorageRpcErrorSummary {
            node_id: node_id.as_u32(),
            rpc_kind: &rpc_kind,
            error_code: &error_code,
            message: &error.message,
        },
    );
}

#[derive(Debug, thiserror::Error)]
pub enum StorageNodeServerError {
    #[error("storage-node PG set must not be empty")]
    EmptyPgSet,
    #[error("duplicate storage-node PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },
    #[error("duplicate storage-node PG route {pg_id}")]
    DuplicatePgRoute { pg_id: u32 },
    #[error("storage-node PG {pg_id} is missing a route")]
    MissingPgRoute { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is not configured for this node")]
    RoutePgNotConfigured { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is inconsistent across static config")]
    InconsistentPgRoute { pg_id: u32 },
    #[error(
        "storage-node PG route {pg_id} has cluster epoch {route_epoch}, expected config epoch {config_epoch}"
    )]
    RouteEpochMismatch {
        pg_id: u32,
        route_epoch: ClusterEpoch,
        config_epoch: ClusterEpoch,
    },
    #[error("storage-node PG route {pg_id} primary node {primary_node_id} is not in acting set")]
    RoutePrimaryNotInActingSet { pg_id: u32, primary_node_id: u32 },
    #[error("storage node {node_id} is absent from runtime map for cluster epoch {cluster_epoch}")]
    RuntimeMapNodeNotFound {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },
    #[error(
        "storage-node route map for cluster epoch {cluster_epoch} expired at {valid_until_ms}ms, now {now_ms}ms"
    )]
    RouteMapExpired {
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },
    #[error("storage-node runtime refresh changed node id from {current} to {candidate}")]
    RuntimeRefreshNodeChanged { current: u32, candidate: u32 },
    #[error(
        "storage-node runtime refresh changed data directory from {current:?} to {candidate:?}"
    )]
    RuntimeRefreshDataDirChanged {
        current: PathBuf,
        candidate: PathBuf,
    },
    #[error("storage-node runtime refresh changed EC shape from {current:?} to {candidate:?}")]
    RuntimeRefreshEcShapeChanged {
        current: EcShape,
        candidate: EcShape,
    },
    #[error("storage-node runtime refresh changed socket path from {current:?} to {candidate:?}")]
    RuntimeRefreshSocketPathChanged {
        current: PathBuf,
        candidate: PathBuf,
    },
    #[error(
        "storage-node runtime refresh changed opened PG set from {current:?} to {candidate:?}"
    )]
    RuntimeRefreshPgSetChanged {
        current: Vec<u32>,
        candidate: Vec<u32>,
    },
    #[error(
        "storage-node runtime refresh attempted epoch downgrade from {current} to {candidate}"
    )]
    RuntimeRefreshEpochDowngrade {
        current: ClusterEpoch,
        candidate: ClusterEpoch,
    },
    #[error(
        "storage-node runtime refresh reduced same-epoch route-map validity from {current:?} to {candidate:?}"
    )]
    RuntimeRefreshValidityRegression {
        current: Option<u64>,
        candidate: Option<u64>,
    },
    #[error("storage-node control-plane refresh loop interval must be non-zero")]
    ControlPlaneRefreshLoopZeroInterval,
    #[error("spawn storage-node control-plane refresh loop")]
    ControlPlaneRefreshLoopSpawn {
        #[source]
        source: io::Error,
    },
    #[error("duplicate storage-node id {id}")]
    DuplicateNodeId { id: u32 },
    #[error(
        "storage node {duplicate_node_id} shares data directory {data_dir:?} with storage node {first_node_id}"
    )]
    DuplicateDataDir {
        first_node_id: u32,
        duplicate_node_id: u32,
        data_dir: PathBuf,
    },
    #[error(
        "storage node {duplicate_node_id} shares Unix socket path {socket_path:?} with storage node {first_node_id}"
    )]
    DuplicateSocketPath {
        first_node_id: u32,
        duplicate_node_id: u32,
        socket_path: PathBuf,
    },
    #[error("storage-node socket path {path:?} has no parent directory")]
    SocketPathMissingParent { path: PathBuf },
    #[error("storage-node socket path {path:?} must be absolute")]
    SocketPathNotAbsolute { path: PathBuf },
    #[error("storage-node socket path {path:?} is not valid UTF-8")]
    SocketPathNotUtf8 { path: PathBuf },
    #[error("storage-node socket path {path:?} has no file name")]
    SocketPathMissingFileName { path: PathBuf },
    #[error("storage-node socket directory {path:?} must be private; mode is {mode:#o}")]
    SocketDirectoryNotPrivate { path: PathBuf, mode: u32 },
    #[error("storage-node socket path {path:?} already exists")]
    SocketPathExists { path: PathBuf },
    #[error("storage-node data directory {path:?} is already locked")]
    DataDirAlreadyLocked { path: PathBuf },
    #[error("storage-node I/O error during {context} for {path:?}: {source}")]
    Io {
        context: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid storage-node incarnation {value:?} in {path:?}")]
    InvalidNodeIncarnation { path: PathBuf, value: String },
    #[error("storage-node incarnation counter overflowed in {path:?}")]
    NodeIncarnationOverflow { path: PathBuf },
    #[error("failed to open storage node: {0}")]
    Store(#[from] StoreError),
    #[error("storage RPC stream error: {message}")]
    RpcStream { message: String },
    #[error("storage RPC response payload error: {message}")]
    ResponsePayload { message: String },
    #[error("storage-node active session limit {limit} is exhausted")]
    TooManyActiveSessions { limit: usize },
    #[error("control-plane heartbeat failed: {0}")]
    ControlPlane(#[from] ControlPlaneError),
}

pub fn validate_storage_node_process_configs(
    configs: &[StorageNodeProcessConfig],
) -> Result<(), StorageNodeServerError> {
    let mut node_ids = BTreeMap::<u32, ()>::new();
    let mut data_dirs = BTreeMap::<PathBuf, NodeId>::new();
    let mut socket_paths = BTreeMap::<PathBuf, NodeId>::new();
    let mut pg_routes = BTreeMap::<u32, StorageNodePgRoute>::new();
    for config in configs {
        if node_ids.insert(config.node_id.as_u32(), ()).is_some() {
            return Err(StorageNodeServerError::DuplicateNodeId {
                id: config.node_id.as_u32(),
            });
        }
        validate_pg_ids(&config.pg_ids)?;
        validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
        let data_dir = canonicalize_existing_or_parent(&config.data_dir, "data directory")?;
        if let Some(first_node_id) = data_dirs.insert(data_dir.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateDataDir {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                data_dir,
            });
        }
        let socket_path = canonical_socket_path(&config.socket_path)?;
        if let Some(first_node_id) = socket_paths.insert(socket_path.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateSocketPath {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                socket_path,
            });
        }
        for route in &config.pg_routes {
            match pg_routes.get(&route.pg_id) {
                Some(existing) if existing != route => {
                    return Err(StorageNodeServerError::InconsistentPgRoute { pg_id: route.pg_id })
                }
                Some(_) => {}
                None => {
                    pg_routes.insert(route.pg_id, route.clone());
                }
            }
        }
    }
    Ok(())
}

pub struct StorageNodeServer {
    config: RwLock<StorageNodeProcessConfig>,
    _data_dir_lock: StorageNodeDataDirLock,
    control_plane_incarnation_lock: Mutex<()>,
    _node: Arc<SharedStorageNode>,
    listener: UnixListener,
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    active_sessions: Arc<StorageNodeActiveSessions>,
    metadata_command_locks: StorageNodeMetadataCommandLocks,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageNodeControlPlaneRefreshLoopStatus {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_lease: Option<HeartbeatLease>,
    pub last_error: Option<String>,
}

pub struct StorageNodeControlPlaneRefreshLoop {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<StorageNodeControlPlaneRefreshLoopStatus>>,
    handle: Option<JoinHandle<()>>,
}

impl StorageNodeControlPlaneRefreshLoop {
    pub fn status(&self) -> StorageNodeControlPlaneRefreshLoopStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn stop(&mut self) {
        {
            let (lock, cvar) = &*self.stop;
            let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageNodeControlPlaneRefreshLoop {
    fn drop(&mut self) {
        self.stop();
    }
}

fn encode_metadata_command_checkpoint_success_response(
    operation: &'static str,
    payload: &[u8],
    max_payload_len: usize,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let response = encode_storage_rpc_success_response(payload);
    if response.len() <= max_payload_len {
        return Ok(response);
    }
    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ResourceExhausted,
        message: format!(
            "{operation} response is too large: {} bytes exceeds storage RPC payload limit {} bytes",
            response.len(),
            max_payload_len
        ),
    })
}

fn metadata_command_checkpoint_candidates_for_frame(
    pg: &PgStore,
    cluster_epoch: ClusterEpoch,
    mut max_applied_log_index: u64,
    limit: usize,
    max_payload_len: usize,
) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
    let mut checkpoints = Vec::new();
    while checkpoints.len() < limit {
        let candidates = pg.metadata_command_checkpoint_candidates(
            cluster_epoch,
            max_applied_log_index,
            STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES,
        )?;
        let Some(last_candidate) = candidates.last() else {
            break;
        };
        let next_max_applied_log_index = last_candidate.applied_log_index.checked_sub(1);
        for candidate in candidates {
            let mut next_checkpoints = checkpoints.clone();
            next_checkpoints.push(candidate.clone());
            let payload = match encode_metadata_command_checkpoint_candidates_response(
                &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                    checkpoints: next_checkpoints,
                },
            ) {
                Ok(payload) => payload,
                Err(_) => continue,
            };
            if encode_storage_rpc_success_response(&payload).len() <= max_payload_len {
                checkpoints.push(candidate);
                if checkpoints.len() == limit {
                    return Ok(checkpoints);
                }
            } else if checkpoints.is_empty() {
                continue;
            } else {
                return Ok(checkpoints);
            }
        }
        let Some(next_max_applied_log_index) = next_max_applied_log_index else {
            break;
        };
        max_applied_log_index = next_max_applied_log_index;
    }
    Ok(checkpoints)
}

impl StorageNodeServer {
    pub fn bind(config: StorageNodeProcessConfig) -> Result<Self, StorageNodeServerError> {
        validate_process_config_route_table(&config)?;
        validate_socket_directory(&config.socket_path)?;
        let data_dir_lock = StorageNodeDataDirLock::acquire(&config.data_dir)?;
        cleanup_stale_socket_path(&config.socket_path)?;
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )?;
        let listener = UnixListener::bind(&config.socket_path).map_err(|source| {
            StorageNodeServerError::Io {
                context: "bind storage-node socket",
                path: config.socket_path.clone(),
                source,
            }
        })?;
        Ok(Self {
            config: RwLock::new(config),
            _data_dir_lock: data_dir_lock,
            control_plane_incarnation_lock: Mutex::new(()),
            _node: Arc::new(node),
            listener,
            read_handles: Arc::new(Mutex::new(StorageNodeReadHandleState::default())),
            active_sessions: Arc::new(StorageNodeActiveSessions::default()),
            metadata_command_locks: StorageNodeMetadataCommandLocks::default(),
        })
    }

    pub fn advance_control_plane_node_incarnation(&self) -> Result<u64, StorageNodeServerError> {
        let _guard = self
            .control_plane_incarnation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        advance_storage_node_incarnation(&self.config_snapshot().data_dir)
    }

    pub fn accept_one(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (mut stream, _) =
            self.listener
                .accept()
                .map_err(|source| StorageNodeServerError::Io {
                    context: "accept storage-node connection",
                    path: self.config_snapshot().socket_path,
                    source,
                })?;
        self.connection_handler()
            .handle_session(&mut stream, session_guard)
    }

    pub fn serve_forever(&self) -> Result<(), StorageNodeServerError> {
        loop {
            self.accept_and_spawn()?;
        }
    }

    pub fn control_plane_heartbeat(
        &self,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
    ) -> Result<NodeHeartbeat, StorageNodeServerError> {
        self.config_snapshot().control_plane_heartbeat(
            &self._node,
            node_incarnation,
            requested_lease_duration_ms,
        )
    }

    pub fn heartbeat_control_plane(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatSink,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let heartbeat =
            self.control_plane_heartbeat(node_incarnation, requested_lease_duration_ms)?;
        control_plane
            .submit_node_heartbeat(heartbeat, authority_now_ms)
            .map_err(StorageNodeServerError::from)
    }

    pub fn refresh_control_plane_runtime_map(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatRuntimeMapSource,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<StorageNodeControlPlaneRefresh, StorageNodeServerError> {
        let heartbeat =
            self.control_plane_heartbeat(node_incarnation, requested_lease_duration_ms)?;
        let refresh = control_plane
            .refresh_node_heartbeat(heartbeat, authority_now_ms)
            .map_err(StorageNodeServerError::from)?;
        let (lease, runtime_map) = refresh.into_parts();
        let current_config = self.config_snapshot();
        let next_config = StorageNodeProcessConfig::from_runtime_map(
            current_config.node_id,
            current_config.data_dir.clone(),
            current_config.default_ec_shape,
            &runtime_map,
        )?;
        next_config.validate_runtime_refresh_from(&current_config)?;
        Ok(StorageNodeControlPlaneRefresh {
            lease,
            runtime_map,
            next_config,
        })
    }

    pub fn refresh_and_install_control_plane_runtime_map(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatRuntimeMapSource,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let refresh = self.refresh_control_plane_runtime_map(
            control_plane,
            node_incarnation,
            requested_lease_duration_ms,
            authority_now_ms,
        )?;
        self.install_control_plane_refresh(refresh)
    }

    pub fn spawn_control_plane_refresh_loop<S, F>(
        self: Arc<Self>,
        mut control_plane: S,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        refresh_interval: Duration,
        authority_now_ms: F,
    ) -> Result<StorageNodeControlPlaneRefreshLoop, StorageNodeServerError>
    where
        S: ControlPlaneHeartbeatRuntimeMapSource + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
    {
        if refresh_interval.is_zero() {
            return Err(StorageNodeServerError::ControlPlaneRefreshLoopZeroInterval);
        }

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let status = Arc::new(Mutex::new(
            StorageNodeControlPlaneRefreshLoopStatus::default(),
        ));
        let worker_stop = Arc::clone(&stop);
        let worker_status = Arc::clone(&status);
        let handle = thread::Builder::new()
            .name(format!(
                "argmin-storage-node-{}-control-plane-refresh",
                self.config_snapshot().node_id.as_u32()
            ))
            .spawn(move || loop {
                let result = self.refresh_and_install_control_plane_runtime_map(
                    &mut control_plane,
                    node_incarnation,
                    requested_lease_duration_ms,
                    authority_now_ms(),
                );
                {
                    let mut status = worker_status
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    status.attempts += 1;
                    match result {
                        Ok(lease) => {
                            status.successes += 1;
                            status.last_lease = Some(lease);
                            status.last_error = None;
                        }
                        Err(error) => {
                            let error = error.to_string();
                            if status.last_error.as_deref() != Some(error.as_str()) {
                                eprintln!(
                                    "storage-node {} control-plane refresh failed: {error}",
                                    self.config_snapshot().node_id.as_u32()
                                );
                            }
                            status.failures += 1;
                            status.last_error = Some(error);
                        }
                    }
                }

                let (lock, cvar) = &*worker_stop;
                let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if *stopped {
                    break;
                }
                let (stopped, _) = cvar
                    .wait_timeout(stopped, refresh_interval)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *stopped {
                    break;
                }
            })
            .map_err(|source| StorageNodeServerError::ControlPlaneRefreshLoopSpawn { source })?;

        Ok(StorageNodeControlPlaneRefreshLoop {
            stop,
            status,
            handle: Some(handle),
        })
    }

    pub fn install_control_plane_refresh(
        &self,
        refresh: StorageNodeControlPlaneRefresh,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let (lease, _runtime_map, next_config) = refresh.into_parts();
        self.install_control_plane_runtime_config(next_config)?;
        Ok(lease)
    }

    pub fn install_control_plane_runtime_config(
        &self,
        next_config: StorageNodeProcessConfig,
    ) -> Result<(), StorageNodeServerError> {
        validate_process_config_route_table(&next_config)?;
        let mut current_config = self.config.write().unwrap_or_else(|e| e.into_inner());
        next_config.validate_runtime_refresh_from(&current_config)?;
        if next_config.pg_ids != current_config.pg_ids {
            return Err(StorageNodeServerError::RuntimeRefreshPgSetChanged {
                current: current_config.pg_ids.clone(),
                candidate: next_config.pg_ids.clone(),
            });
        }
        if next_config.cluster_epoch < current_config.cluster_epoch {
            return Err(StorageNodeServerError::RuntimeRefreshEpochDowngrade {
                current: current_config.cluster_epoch,
                candidate: next_config.cluster_epoch,
            });
        }
        if next_config.cluster_epoch == current_config.cluster_epoch
            && route_map_validity_regressed(
                current_config.route_map_valid_until_ms,
                next_config.route_map_valid_until_ms,
            )
        {
            return Err(StorageNodeServerError::RuntimeRefreshValidityRegression {
                current: current_config.route_map_valid_until_ms,
                candidate: next_config.route_map_valid_until_ms,
            });
        }
        *current_config = next_config;
        Ok(())
    }

    fn accept_and_spawn(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (mut stream, _) =
            self.listener
                .accept()
                .map_err(|source| StorageNodeServerError::Io {
                    context: "accept storage-node connection",
                    path: self.config_snapshot().socket_path,
                    source,
                })?;
        let handler = self.connection_handler();
        thread::spawn(move || {
            if let Err(error) = handler.handle_session(&mut stream, session_guard) {
                eprintln!("storage-node connection failed: {error}");
            }
        });
        Ok(())
    }

    fn connection_handler(&self) -> StorageNodeConnectionHandler {
        StorageNodeConnectionHandler {
            config: self.config_snapshot(),
            node: Arc::clone(&self._node),
            read_handles: Arc::clone(&self.read_handles),
            metadata_command_locks: self.metadata_command_locks.clone(),
        }
    }

    fn config_snapshot(&self) -> StorageNodeProcessConfig {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn acquire_session(&self) -> StorageNodeActiveSessionGuard {
        self.active_sessions
            .acquire(STORAGE_NODE_MAX_ACTIVE_SESSIONS)
    }

    #[cfg(test)]
    pub(crate) fn read_handle_count(&self, location: ShardLocation) -> usize {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .count(location)
    }
}

#[derive(Clone, Default)]
struct StorageNodeMetadataCommandLocks {
    state: Arc<StorageNodeMetadataCommandLockState>,
}

const METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_AFTER: Duration = Duration::from_secs(1);
const METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_INTERVAL: Duration = Duration::from_secs(5);
const METADATA_COMMAND_LOCK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug)]
struct StorageNodeMetadataCommandLockContext {
    request_id: u64,
    kind: StorageRpcMessageKind,
}

#[derive(Clone, Copy, Debug)]
struct StorageNodeMetadataCommandLockHolder {
    acquired_context: Option<StorageNodeMetadataCommandLockContext>,
    current_context: Option<StorageNodeMetadataCommandLockContext>,
    acquired_at: Instant,
    current_started_at: Option<Instant>,
}

impl StorageNodeMetadataCommandLocks {
    #[cfg(test)]
    fn set_before_wait_hook(&self, hook: MetadataCommandBeforeWaitHook) {
        *self
            .state
            .before_wait_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    fn acquire(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) -> StorageNodeMetadataCommandGuard {
        let started_at = Instant::now();
        let mut next_diagnostic_at = started_at + METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_AFTER;
        let mut waited = false;
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(holder) = held.get(&pg_id).copied() {
            waited = true;
            #[cfg(test)]
            let before_wait_hook = self
                .state
                .before_wait_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let now = Instant::now();
            if now >= next_diagnostic_at {
                next_diagnostic_at = now + METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_INTERVAL;
                let waited = started_at.elapsed();
                drop(held);
                emit_metadata_command_lock_wait_diagnostic(node_id, pg_id, context, holder, waited);
                held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
                continue;
            }
            drop(held);
            #[cfg(test)]
            if let Some(hook) = before_wait_hook {
                hook(pg_id);
            }
            held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
            if !held.contains_key(&pg_id) {
                continue;
            }
            let (next_held, _) = self
                .state
                .available
                .wait_timeout(held, METADATA_COMMAND_LOCK_WAIT_POLL_INTERVAL)
                .unwrap_or_else(|e| e.into_inner());
            held = next_held;
        }
        if waited {
            let _ = observability::emit_metadata_command_session_wait(
                "storage",
                observability::MetadataCommandSessionWaitSummary {
                    node_id: node_id.as_u32(),
                    pg_id: pg_id.get(),
                    wait_us: started_at.elapsed().as_micros(),
                },
            );
        }
        held.insert(
            pg_id,
            StorageNodeMetadataCommandLockHolder {
                acquired_context: context,
                current_context: context,
                acquired_at: Instant::now(),
                current_started_at: Some(Instant::now()),
            },
        );
        StorageNodeMetadataCommandGuard {
            locks: self.clone(),
            pg_id,
            released: false,
        }
    }

    fn update_context(&self, pg_id: PgId, context: Option<StorageNodeMetadataCommandLockContext>) {
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(holder) = held.get_mut(&pg_id) {
            holder.current_context = context;
            holder.current_started_at = context.map(|_| Instant::now());
        }
    }

    fn release(&self, pg_id: PgId) {
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        if held.remove(&pg_id).is_some() {
            self.state.available.notify_all();
        }
    }
}

fn emit_metadata_command_lock_wait_diagnostic(
    node_id: NodeId,
    pg_id: PgId,
    waiter: Option<StorageNodeMetadataCommandLockContext>,
    holder: StorageNodeMetadataCommandLockHolder,
    waited: Duration,
) {
    let waiter_request_id = waiter
        .map(|context| context.request_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let waiter_kind = waiter
        .map(|context| context.kind.operation_name())
        .unwrap_or("unknown");
    let holder_request_id = holder
        .acquired_context
        .map(|context| context.request_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let holder_kind = holder
        .acquired_context
        .map(|context| context.kind.operation_name())
        .unwrap_or("unknown");
    let held_us = holder.acquired_at.elapsed().as_micros();
    let (holder_current_request_id, holder_current_kind, holder_current_elapsed_us) =
        match (holder.current_context, holder.current_started_at) {
            (Some(context), Some(started_at)) => (
                context.request_id.to_string(),
                context.kind.operation_name(),
                started_at.elapsed().as_micros().to_string(),
            ),
            _ => ("none".to_string(), "none", "none".to_string()),
        };
    let waited_us = waited.as_micros();
    let detail = format!(
        "node_id={} pg_id={} waiter_request_id={} waiter_kind=\"{}\" waited_us={} holder_request_id={} holder_kind=\"{}\" holder_held_us={} holder_current_request_id={} holder_current_kind=\"{}\" holder_current_elapsed_us={}",
        node_id.as_u32(),
        pg_id.get(),
        waiter_request_id,
        waiter_kind,
        waited_us,
        holder_request_id,
        holder_kind,
        held_us,
        holder_current_request_id,
        holder_current_kind,
        holder_current_elapsed_us
    );
    let _ = observability::emit_flight_event(
        "storage",
        "metadata_command_lock_wait_blocked",
        detail.clone(),
    );
    eprintln!("metadata_command_lock_wait_blocked {detail}");
}

#[derive(Default)]
struct StorageNodeMetadataCommandLockState {
    held: Mutex<BTreeMap<PgId, StorageNodeMetadataCommandLockHolder>>,
    available: Condvar,
    #[cfg(test)]
    before_wait_hook: Mutex<Option<MetadataCommandBeforeWaitHook>>,
}

struct StorageNodeMetadataCommandGuard {
    locks: StorageNodeMetadataCommandLocks,
    pg_id: PgId,
    released: bool,
}

impl StorageNodeMetadataCommandGuard {
    fn release(&mut self) {
        if !self.released {
            self.locks.release(self.pg_id);
            self.released = true;
        }
    }
}

impl Drop for StorageNodeMetadataCommandGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
struct StorageNodeConnectionHandler {
    config: StorageNodeProcessConfig,
    node: Arc<SharedStorageNode>,
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    metadata_command_locks: StorageNodeMetadataCommandLocks,
}

impl StorageNodeConnectionHandler {
    fn metadata_command_pg_guard(
        &self,
        session: &StorageNodeSession<'_>,
        pg_id: PgId,
    ) -> Option<StorageNodeMetadataCommandGuard> {
        if session.holds_metadata_command_pg_lock(pg_id) {
            None
        } else {
            Some(self.metadata_command_locks.acquire(
                self.config.node_id,
                pg_id,
                session.current_rpc_context(),
            ))
        }
    }

    fn handle_session(
        &self,
        stream: &mut UnixStream,
        _session_guard: StorageNodeActiveSessionGuard,
    ) -> Result<(), StorageNodeServerError> {
        let mut session = StorageNodeSession::new(&self.read_handles);
        loop {
            let frame = match read_storage_rpc_request_frame_from(stream) {
                Ok(frame) => frame,
                Err(StorageRpcStreamError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                    ) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(rpc_stream_error(error)),
            };
            let _rpc_trace = observability::AttachedTrace::new(storage_node_rpc_trace_context(
                self.config.node_id,
                frame.request_id,
            ));
            let started = std::time::Instant::now();
            let trace_rpc_lifecycle = trace_storage_rpc_lifecycle(frame.kind);
            if trace_rpc_lifecycle {
                let _ = observability::emit_flight_event(
                    "storage_rpc_server",
                    "storage_rpc_server_frame_received",
                    format!(
                        "node_id={} rpc_request_id={} kind={}",
                        self.config.node_id.as_u32(),
                        frame.request_id,
                        frame.kind.operation_name()
                    ),
                );
            }
            session.set_current_rpc_context(frame.request_id, frame.kind);
            session.update_metadata_command_lock_context(
                &self.metadata_command_locks,
                session.current_rpc_context(),
            );
            let response = match self.dispatch_frame(&mut session, &frame) {
                Ok(response) => response,
                Err(error) => {
                    session.clear_metadata_command_lock_context(&self.metadata_command_locks);
                    return Err(error);
                }
            };
            if trace_rpc_lifecycle {
                let _ = observability::emit_flight_event(
                    "storage_rpc_server",
                    "storage_rpc_server_dispatch_done",
                    format!(
                        "node_id={} rpc_request_id={} kind={} elapsed_us={}",
                        self.config.node_id.as_u32(),
                        frame.request_id,
                        frame.kind.operation_name(),
                        started.elapsed().as_micros()
                    ),
                );
            }
            if let Err(error) = write_storage_rpc_frame_to(stream, &response) {
                session.clear_metadata_command_lock_context(&self.metadata_command_locks);
                return Err(rpc_stream_error(error));
            }
            session.clear_metadata_command_lock_context(&self.metadata_command_locks);
            if trace_rpc_lifecycle {
                let _ = observability::emit_flight_event(
                    "storage_rpc_server",
                    "storage_rpc_server_response_written",
                    format!(
                        "node_id={} rpc_request_id={} kind={} elapsed_us={}",
                        self.config.node_id.as_u32(),
                        frame.request_id,
                        frame.kind.operation_name(),
                        started.elapsed().as_micros()
                    ),
                );
            }
        }
    }

    fn dispatch_frame(
        &self,
        session: &mut StorageNodeSession<'_>,
        frame: &StorageRpcFrame,
    ) -> Result<StorageRpcFrame, StorageNodeServerError> {
        let payload = match frame.kind {
            StorageRpcMessageKind::Health => {
                if frame.payload.is_empty() {
                    let health = StorageRpcHealthResponse {
                        protocol_version: STORAGE_RPC_FRAME_ENCODING_VERSION,
                        node_id: self.config.node_id,
                        cluster_epoch: self.config.cluster_epoch,
                    };
                    let health_payload = encode_health_response(&health);
                    Ok(encode_storage_rpc_success_response(&health_payload))
                } else {
                    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "health request payload must be empty".to_string(),
                    })
                }
            }
            StorageRpcMessageKind::ClusterMapHistoryReferenceSummary => {
                match decode_cluster_map_history_reference_summary_request(&frame.payload) {
                    Ok(request) => self.cluster_map_history_reference_summary_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ReadHandlesAcquire => {
                match decode_read_handle_acquire_request(&frame.payload) {
                    Ok(request) => self.read_handles_acquire_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ReadHandlesRelease => {
                match decode_read_handle_release_request(&frame.payload) {
                    Ok(request) => self.read_handles_release_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ProofRelease => {
                match decode_proof_release_request(&frame.payload) {
                    Ok(request) => self.proof_release_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationAcquire => {
                match decode_bucket_write_reservation_acquire_request(&frame.payload) {
                    Ok(request) => self.bucket_write_reservation_acquire_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationValidate => {
                match decode_bucket_write_reservation_proof_request(&frame.payload) {
                    Ok(request) => self.bucket_write_reservation_validate_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationHeartbeat => {
                match decode_bucket_write_reservation_heartbeat_request(&frame.payload) {
                    Ok(request) => self.bucket_write_reservation_heartbeat_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationRelease => {
                match decode_bucket_write_reservation_record_request(&frame.payload) {
                    Ok(request) => self.bucket_write_reservation_release_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainBegin => {
                match decode_bucket_write_drain_begin_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_begin_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainClear => {
                match decode_bucket_write_drain_record_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_clear_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainClearExpired => {
                match decode_bucket_write_drain_clear_expired_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_clear_expired_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainHeartbeat => {
                match decode_bucket_write_drain_heartbeat_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_heartbeat_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainExists => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_exists_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationsList => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_write_reservations_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalized => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_finalized_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeRoots => {
                match decode_bucket_delete_finalize_roots_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_finalize_roots_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire => {
                match decode_bucket_delete_finalize_claim_acquire_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_finalize_claim_acquire_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease => {
                match decode_bucket_delete_finalize_claim_record_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_finalize_claim_release_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepBucketsList => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_buckets_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepRoots => {
                match decode_lifecycle_sweep_roots_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_roots_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimAcquire => {
                match decode_lifecycle_sweep_claim_acquire_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_claim_acquire_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimHeartbeat => {
                match decode_lifecycle_sweep_claim_heartbeat_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_claim_heartbeat_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimError => {
                match decode_lifecycle_sweep_claim_error_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_claim_error_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimRelease => {
                match decode_lifecycle_sweep_claim_record_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_claim_release_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectListPage => {
                match decode_list_objects_request(&frame.payload) {
                    Ok(request) => self.object_list_page_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectVersionListPage => {
                match decode_list_object_versions_request(&frame.payload) {
                    Ok(request) => self.object_version_list_page_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadListPage => {
                match decode_list_multipart_uploads_request(&frame.payload) {
                    Ok(request) => self.object_multipart_upload_list_page_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectGenerationNext => {
                match decode_object_request(&frame.payload) {
                    Ok(request) => self.object_generation_next_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectGenerationReservation => {
                match decode_object_generation_reservation_request(&frame.payload) {
                    Ok(request) => self.object_generation_reservation_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::DirectPutCommitSnapshotLoad => {
                match decode_direct_put_commit_snapshot_request(&frame.payload) {
                    Ok(request) => self.direct_put_commit_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::DirectPutCommitCommandBuild => {
                match decode_direct_put_command_build_request(&frame.payload) {
                    Ok(request) => self.direct_put_commit_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectReadAuthSubjectLoad => {
                match decode_object_read_auth_subject_request(&frame.payload) {
                    Ok(request) => self.object_read_auth_subject_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectReadSnapshotLoad => {
                match decode_object_read_snapshot_request(&frame.payload) {
                    Ok(request) => self.object_read_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectTagsForSubjectLoad => {
                match decode_object_tags_for_subject_request(&frame.payload) {
                    Ok(request) => self.object_tags_for_subject_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad => {
                match decode_put_object_metadata_snapshot_request(&frame.payload) {
                    Ok(request) => self.put_object_metadata_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMetadataPutCommandBuild => {
                match decode_put_object_metadata_command_build_request(&frame.payload) {
                    Ok(request) => self.put_object_metadata_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad
            | StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad
            | StorageRpcMessageKind::ObjectLifecycleVersionListLoad => {
                match decode_object_delete_snapshot_request(&frame.payload) {
                    Ok(request) => match frame.kind {
                        StorageRpcMessageKind::ObjectLifecycleVersionListLoad => {
                            self.object_lifecycle_version_list_response(request)
                        }
                        _ => self.object_delete_snapshot_response(frame.kind, request),
                    },
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild => {
                match decode_delete_specific_object_command_build_request(&frame.payload) {
                    Ok(request) => self.delete_specific_object_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild => {
                match decode_delete_current_object_command_build_request(&frame.payload) {
                    Ok(request) => self.delete_current_object_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild => {
                match decode_insert_delete_marker_command_build_request(&frame.payload) {
                    Ok(request) => self.insert_delete_marker_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadMatch => {
                match decode_stream_upload_match_request(&frame.payload) {
                    Ok(request) => self.stream_upload_match_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadSessionLoad => {
                match decode_stream_upload_session_request(&frame.payload) {
                    Ok(request) => self.stream_upload_session_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad => {
                match decode_stream_upload_session_request(&frame.payload) {
                    Ok(request) => self.stream_upload_segments_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadsList => {
                match decode_stream_uploads_list_request(&frame.payload) {
                    Ok(request) => self.stream_uploads_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadsPgList => {
                match decode_stream_uploads_pg_list_request(&frame.payload) {
                    Ok(request) => self.stream_uploads_pg_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectCompletedMultipartUploadsList => {
                match decode_completed_multipart_uploads_list_request(&frame.payload) {
                    Ok(request) => self.completed_multipart_uploads_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_payload_reclaim_root_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimExists => {
                match decode_object_payload_reclaim_exists_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_exists_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimRoot => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_root_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimLoad => {
                match decode_object_payload_reclaim_exists_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_load_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire => {
                match decode_object_payload_reclaim_claim_acquire_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_claim_acquire_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease => {
                match decode_object_payload_reclaim_claim_record_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_claim_release_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare => {
                match decode_stream_segment_append_prepare_request(&frame.payload) {
                    Ok(request) => self.stream_segment_append_prepare_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadMatch => {
                match decode_multipart_upload_match_request(&frame.payload) {
                    Ok(request) => self.multipart_upload_match_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadLoad
            | StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad
            | StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad => {
                match decode_multipart_upload_load_request(&frame.payload) {
                    Ok(request) => self.multipart_upload_load_response(frame.kind, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad => {
                match decode_multipart_completion_snapshot_request(&frame.payload) {
                    Ok(request) => self.multipart_completion_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad => {
                match decode_multipart_completion_preflight_request(&frame.payload) {
                    Ok(request) => self.multipart_completion_preflight_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartPartsList => {
                match decode_multipart_parts_list_request(&frame.payload) {
                    Ok(request) => self.multipart_parts_list_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartManagementLookup => {
                match decode_multipart_upload_load_request(&frame.payload) {
                    Ok(request) => self.multipart_management_lookup_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild => {
                match decode_create_stream_upload_command_build_request(&frame.payload) {
                    Ok(request) => self.create_stream_upload_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadCommandBuild => {
                match decode_create_multipart_upload_command_build_request(&frame.payload) {
                    Ok(request) => self.create_multipart_upload_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad => {
                match decode_stream_put_finalize_snapshot_request(&frame.payload) {
                    Ok(request) => self.stream_put_finalize_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild => {
                match decode_stream_put_commit_command_build_request(&frame.payload) {
                    Ok(request) => self.stream_put_commit_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad => {
                match decode_stream_part_finalize_snapshot_request(&frame.payload) {
                    Ok(request) => self.stream_part_finalize_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild => {
                match decode_stream_part_commit_command_build_request(&frame.payload) {
                    Ok(request) => self.stream_part_commit_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild => {
                match decode_complete_multipart_command_build_request(&frame.payload) {
                    Ok(request) => self.complete_multipart_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAbortCommandBuild => {
                match decode_abort_multipart_command_build_request(&frame.payload) {
                    Ok(request) => self.abort_multipart_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad => {
                match decode_abort_multipart_cleanup_request(&frame.payload) {
                    Ok(request) => self.abort_multipart_cleanup_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild => {
                match decode_authorized_abort_multipart_command_build_request(&frame.payload) {
                    Ok(request) => self.authorized_abort_multipart_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad => {
                match decode_object_request(&frame.payload) {
                    Ok(request) => self.multipart_completion_stale_source_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::CompletedMultipartOrderCommandBuild => {
                match decode_completed_multipart_order_command_build_request(&frame.payload) {
                    Ok(request) => self.completed_multipart_order_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectVersionNext => match decode_object_request(&frame.payload)
            {
                Ok(request) => self.object_version_next_response(request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardWrite => match decode_shard_write_request(&frame.payload) {
                Ok(request) => self.shard_write_response(request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardRepairWrite => {
                match decode_shard_write_request(&frame.payload) {
                    Ok(request) => self.shard_repair_write_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardRead => match decode_shard_read_request(&frame.payload) {
                Ok(request) => self.shard_read_response(request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardHistoricalRead => {
                match decode_shard_read_request(&frame.payload) {
                    Ok(request) => self.shard_historical_read_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardReadRange => {
                match decode_shard_read_range_request(&frame.payload) {
                    Ok(request) => self.shard_read_range_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardDelete => {
                match decode_shard_delete_request(&frame.payload) {
                    Ok(request) => self.shard_delete_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckRecord => {
                match decode_shard_ack_batch_request(&frame.payload) {
                    Ok(request) => self.shard_ack_record_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckValidate => {
                match decode_shard_ack_batch_request(&frame.payload) {
                    Ok(request) => self.shard_ack_validate_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckLoad => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_load_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckHistoricalLoad => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_historical_load_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckDelete => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_delete_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerListFiles => {
                match decode_scavenger_list_files_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_list_files_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerShardRows => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_shard_rows_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerPayloadReferences => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_payload_references_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerObservationRecord => {
                match decode_scavenger_observation_record_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_observation_record_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerObservations => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_observations_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerObservationResolve => {
                match decode_scavenger_observation_key_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_observation_resolve_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairRecord => {
                match decode_placed_segment_shard_repair_record_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_repair_record_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairs => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_repairs_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairResolve => {
                match decode_placed_segment_shard_repair_item_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_repair_resolve_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire => {
                match decode_placed_segment_shard_repair_claim_acquire_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_repair_claim_acquire_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete => {
                match decode_placed_segment_shard_repair_claim_record_request(&frame.payload) {
                    Ok(request) => {
                        self.placed_segment_shard_repair_claim_complete_response(request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimError => {
                match decode_placed_segment_shard_repair_claim_error_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_repair_claim_error_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillRecord => {
                match decode_placed_segment_shard_backfill_record_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfill_record_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfills => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfills_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillCount => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfill_count_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillExists => {
                match decode_placed_segment_shard_backfill_item_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfill_exists_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillResolve => {
                match decode_placed_segment_shard_backfill_item_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfill_resolve_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire => {
                match decode_placed_segment_shard_backfill_claim_acquire_request(&frame.payload) {
                    Ok(request) => {
                        self.placed_segment_shard_backfill_claim_acquire_response(request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete => {
                match decode_placed_segment_shard_backfill_claim_record_request(&frame.payload) {
                    Ok(request) => {
                        self.placed_segment_shard_backfill_claim_complete_response(request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError => {
                match decode_placed_segment_shard_backfill_claim_error_request(&frame.payload) {
                    Ok(request) => self.placed_segment_shard_backfill_claim_error_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandReplicaState => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_replica_state_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandAcceptance => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => self.metadata_command_acceptance_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_abandon_acceptance_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert => {
                match decode_metadata_command_pending_slot_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_pending_slot_insert_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert => {
                match decode_metadata_command_pending_slot_request(&frame.payload) {
                    Ok(request) => self
                        .metadata_command_bucket_control_pending_slot_insert_response(
                            session, request,
                        ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_pending_slot_remove_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPendingSlotReplace => {
                match decode_metadata_command_pending_slot_replace_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_pending_slot_replace_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandMaxLogIndex => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_max_log_index_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes => {
                match decode_metadata_command_log_hash_range_request(&frame.payload) {
                    Ok(request) => self.metadata_command_log_hash_range_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries => {
                match decode_metadata_command_log_entry_range_request(&frame.payload) {
                    Ok(request) => self.metadata_command_log_entry_range_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandNextId => {
                match decode_metadata_command_next_id_request(&frame.payload) {
                    Ok(request) => self.metadata_command_next_id_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPendingEnvelope => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_pending_envelope_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandValidateReplayState => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self
                        .metadata_command_validate_replay_state_response(session, request, false),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_validate_replay_state_response(session, request, true)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self
                        .metadata_command_replica_state_can_initialize_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt => {
                match decode_metadata_command_transfer_adopt_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_transfer_state_adopt_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize => {
                match decode_metadata_command_transfer_empty_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_transfer_empty_state_initialize_response(
                        session, request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize => {
                match decode_metadata_command_transfer_matching_state_request(&frame.payload) {
                    Ok(request) => self
                        .metadata_command_transfer_matching_state_initialize_response(
                            session, request,
                        ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall => {
                match decode_metadata_command_transfer_checkpoint_base_request(&frame.payload) {
                    Ok(request) => self.metadata_command_transfer_checkpoint_base_install_response(
                        session, request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandCheckpointExport => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_checkpoint_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_checkpoint_record_current_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandLogCompact => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_log_compact_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandCheckpointCandidates => {
                match decode_metadata_command_checkpoint_candidates_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_checkpoint_candidates_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => self.metadata_command_applied_hashes_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog => {
                match decode_metadata_command_matching_applied_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_matching_applied_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandAbandoned => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => self.metadata_command_abandoned_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRecordAbandoned => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_record_abandoned_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandApplyAndRecord => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => {
                        self.metadata_command_apply_and_record_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord => {
                match decode_metadata_command_request(&frame.payload) {
                    Ok(request) => self.metadata_command_peering_replay_apply_and_record_response(
                        session, request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPgLockAcquire => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_pg_lock_acquire_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPgLockRelease => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.metadata_command_pg_lock_release_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketHeadRaw => match decode_bucket_request(&frame.payload) {
                Ok(request) => self.bucket_head_response(request, false),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketHeadInfo => match decode_bucket_request(&frame.payload) {
                Ok(request) => self.bucket_head_response(request, true),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketSnapshotLoad => {
                match decode_bucket_snapshot_request(&frame.payload) {
                    Ok(request) => self.bucket_snapshot_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketSnapshotPairLoad => {
                match decode_bucket_snapshot_pair_request(&frame.payload) {
                    Ok(request) => self.bucket_snapshot_pair_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketCreateCommandBuild => {
                match decode_create_bucket_command_build_request(&frame.payload) {
                    Ok(request) => self.bucket_create_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketMetadataControlPendingMatch => {
                match decode_bucket_metadata_control_pending_match_request(&frame.payload) {
                    Ok(request) => self.bucket_metadata_control_pending_match_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketMetadataControlCommandBuild => {
                match decode_bucket_metadata_control_command_build_request(&frame.payload) {
                    Ok(request) => self.bucket_metadata_control_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketMarkDeletingCommandBuild => {
                match decode_bucket_mark_deleting_command_build_request(&frame.payload) {
                    Ok(request) => self.bucket_mark_deleting_command_build_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketSubresourceGet => {
                match decode_bucket_subresource_get_request(&frame.payload) {
                    Ok(request) => self.bucket_subresource_get_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketList => match decode_bucket_list_request(&frame.payload) {
                Ok(request) => self.bucket_list_response(request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketExecutionGenerations => {
                match decode_bucket_batch_request(&frame.payload) {
                    Ok(request) => self.bucket_execution_generations_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketFastPathIdentities => {
                match decode_bucket_batch_request(&frame.payload) {
                    Ok(request) => self.bucket_fast_path_identities_response(request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            kind => self.unsupported_operation_response(kind),
        }
        .map_err(|error| StorageNodeServerError::ResponsePayload {
            message: error.to_string(),
        })?;
        maybe_emit_storage_rpc_error(self.config.node_id, frame.kind, &payload);
        Ok(StorageRpcFrame {
            request_id: frame.request_id,
            kind: frame.kind,
            payload,
        })
    }

    fn read_handles_acquire_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcReadHandleAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_locations(&request.locations) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match session.acquire_read_handles(request) {
            Ok(locations) => {
                let payload =
                    encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                        locations,
                    })?;
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&error)?,
        };
        Ok(response)
    }

    fn read_handles_release_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcReadHandleReleaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        session.release_read_handles(&request.read_operation_id);
        let payload = encode_read_handle_release_response(&StorageRpcReadHandleReleaseResponse);
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn proof_release_response(
        &self,
        request: StorageRpcProofReleaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route =
            match self.cleanup_pg_route(request.node_id, request.cluster_epoch, request.pg_id) {
                Ok(route) => route,
                Err(error) => return encode_storage_rpc_error_response(&error),
            };
        if route.primary_node_id != self.config.node_id {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for proof release on PG {}",
                    self.config.node_id.as_u32(),
                    request.pg_id.get()
                ),
            });
        }
        let expected_pg_id =
            PgId::new(self.node.pg_topology().bucket_pg_for(&request.proof.bucket));
        if request.pg_id != expected_pg_id {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "proof release PG {} does not match bucket {} PG {}",
                    request.pg_id.get(),
                    request.proof.bucket.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::release_metadata_command_bucket_write_reservation(
            &local_client,
            request.pg_id,
            &request.proof,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_generation_next_response(
        &self,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.pg_id,
            &request.bucket,
            &request.key,
            "object generation allocation",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectGenerationMetadataNodeClient::next_object_generation_id(
            &local_client,
            request.pg_id,
            &request.bucket,
            &request.key,
        ) {
            Ok(generation_id) => {
                let payload =
                    encode_object_generation_response(&StorageRpcObjectGenerationResponse {
                        generation_id,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn bucket_write_reservation_acquire_response(
        &self,
        request: StorageRpcBucketWriteReservationAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "bucket write reservation acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &local_client,
            request.pg_id,
            &request.bucket,
            &request.reservation_id,
            &request.owner_token,
            request.cluster_epoch,
            &request.operation_kind,
            request.created_at,
            request.lease_deadline,
            request.target_context.as_deref(),
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Draining,
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
                            name,
                        },
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_reservation_validate_response(
        &self,
        request: StorageRpcBucketWriteReservationProofRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.proof.bucket,
            "bucket write reservation validate",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::validate_bucket_write_reservation_proof(
            &local_client,
            request.pg_id,
            &request.proof,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_reservation_heartbeat_response(
        &self,
        request: StorageRpcBucketWriteReservationHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.proof.bucket,
            "bucket write reservation heartbeat",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_reservation(
            &local_client,
            request.pg_id,
            &request.proof,
            request.lease_deadline,
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_reservation_release_response(
        &self,
        request: StorageRpcBucketWriteReservationRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.record.bucket,
            "bucket write reservation release",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
            &local_client,
            request.pg_id,
            &request.record,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_drain_begin_response(
        &self,
        request: StorageRpcBucketWriteDrainBeginRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.bucket.pg_id,
            &request.bucket.bucket,
            "bucket write drain begin",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            &request.drain_id,
            &request.owner_token,
            request.bucket.cluster_epoch,
            request.created_at,
            request.lease_deadline,
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_begin_response(
                    &StorageRpcBucketWriteDrainBeginResponse {
                        outcome: StorageRpcBucketWriteDrainBeginOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDrainConflict {
                ..
            })) => {
                let payload = encode_bucket_write_drain_begin_response(
                    &StorageRpcBucketWriteDrainBeginResponse {
                        outcome: StorageRpcBucketWriteDrainBeginOutcome::Conflict,
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_drain_clear_response(
        &self,
        request: StorageRpcBucketWriteDrainRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.record.bucket,
            "bucket write drain clear",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
            &local_client,
            request.pg_id,
            &request.record,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_drain_clear_expired_response(
        &self,
        request: StorageRpcBucketWriteDrainClearExpiredRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.bucket.pg_id,
            &request.bucket.bucket,
            "bucket write drain clear expired",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::clear_expired_durable_bucket_write_drain(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.now,
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_optional_record_response(
                    &StorageRpcBucketWriteDrainOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_drain_heartbeat_response(
        &self,
        request: crate::storage_rpc::StorageRpcBucketWriteDrainHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.record.bucket,
            "bucket write drain heartbeat",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_drain(
            &local_client,
            request.pg_id,
            &request.record,
            request.lease_deadline,
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_optional_record_response(
                    &StorageRpcBucketWriteDrainOptionalRecordResponse {
                        record: Some(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(
                &bucket_write_drain_heartbeat_error_response(error),
            ),
        }
    }

    fn bucket_write_drain_exists_response(
        &self,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "bucket write drain exists",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
            &local_client,
            request.pg_id,
            &request.bucket,
        ) {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_write_reservations_list_response(
        &self,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "bucket write reservations list",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::durable_bucket_write_reservations(
            &local_client,
            request.pg_id,
            &request.bucket,
        ) {
            Ok(records) => {
                let payload = encode_bucket_write_reservations_list_response(
                    &StorageRpcBucketWriteReservationsListResponse { records },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_delete_finalized_response(
        &self,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_pg_for_bucket(request.pg_id, &request.bucket, "bucket delete finalized")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::delete_finalized_bucket(
            &local_client,
            request.pg_id,
            &request.bucket,
        ) {
            Ok(()) => {
                let payload = encode_bucket_delete_finalized_response(
                    &StorageRpcBucketDeleteFinalizedResponse {
                        outcome: StorageRpcBucketDeleteFinalizedOutcome::Deleted,
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { name })) => {
                let payload = encode_bucket_delete_finalized_response(
                    &StorageRpcBucketDeleteFinalizedResponse {
                        outcome: StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { name },
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => {
                encode_storage_rpc_error_response(&bucket_write_drain_error_response(error))
            }
        }
    }

    fn bucket_delete_finalize_roots_response(
        &self,
        request: StorageRpcBucketDeleteFinalizeRootsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.route.pg_id, "bucket delete roots") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::get_bucket_delete_finalize_roots(
            &local_client,
            request.route.pg_id,
            request.now,
            request.limit,
        ) {
            Ok(roots) => {
                let payload = encode_bucket_delete_finalize_roots_response(
                    &StorageRpcBucketDeleteFinalizeRootsResponse { roots },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_delete_finalize_claim_acquire_response(
        &self,
        request: StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.bucket.pg_id,
            &request.bucket.bucket,
            "bucket delete finalize claim acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.bucket_incarnation_generation,
            &request.claim_id,
            &request.owner_token,
            request.bucket.cluster_epoch,
            request.claimed_at,
            request.lease_deadline,
            request.now,
        ) {
            Ok(record) => {
                let payload = encode_bucket_delete_finalize_claim_optional_record_response(
                    &StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_delete_finalize_claim_release_response(
        &self,
        request: StorageRpcBucketDeleteFinalizeClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.record.bucket,
            "bucket delete finalize claim release",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
            &local_client,
            request.pg_id,
            &request.record,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_buckets_list_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "lifecycle sweep bucket list") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::list_lifecycle_sweep_buckets(
            &local_client,
            request.pg_id,
        ) {
            Ok(buckets) => {
                let payload = encode_lifecycle_sweep_buckets_response(
                    &StorageRpcLifecycleSweepBucketsResponse { buckets },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_roots_response(
        &self,
        request: StorageRpcLifecycleSweepRootsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "lifecycle sweep roots") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::get_lifecycle_sweep_roots(
            &local_client,
            request.pg_id,
            request.now,
            request.limit,
        ) {
            Ok(roots) => {
                let payload = encode_lifecycle_sweep_roots_response(
                    &StorageRpcLifecycleSweepRootsResponse { roots },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_claim_acquire_response(
        &self,
        request: StorageRpcLifecycleSweepClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.bucket.pg_id,
            &request.bucket.bucket,
            "lifecycle sweep claim acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::acquire_lifecycle_sweep_claim(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.bucket_incarnation_generation,
            &request.claim_id,
            &request.owner_token,
            request.bucket.cluster_epoch,
            request.claimed_at,
            request.lease_deadline,
            request.now,
        ) {
            Ok(record) => {
                let payload = encode_lifecycle_sweep_claim_optional_record_response(
                    &StorageRpcLifecycleSweepClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_claim_heartbeat_response(
        &self,
        request: StorageRpcLifecycleSweepClaimHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_lifecycle_sweep_claim_route(
            &request.record,
            "lifecycle sweep claim heartbeat",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::heartbeat_lifecycle_sweep_claim(
            &local_client,
            request.record.pg_id,
            &request.record.claim,
            request.heartbeat_at,
            request.lease_deadline,
        ) {
            Ok(record) => {
                let payload = encode_lifecycle_sweep_claim_record_response(
                    &StorageRpcLifecycleSweepClaimRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_claim_error_response(
        &self,
        request: StorageRpcLifecycleSweepClaimErrorRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self
            .validate_lifecycle_sweep_claim_route(&request.record, "lifecycle sweep claim error")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::record_lifecycle_sweep_claim_error(
            &local_client,
            request.record.pg_id,
            &request.record.claim,
            &request.last_error,
        ) {
            Ok(record) => {
                let payload = encode_lifecycle_sweep_claim_record_response(
                    &StorageRpcLifecycleSweepClaimRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn lifecycle_sweep_claim_release_response(
        &self,
        request: StorageRpcLifecycleSweepClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self
            .validate_lifecycle_sweep_claim_cleanup_route(&request, "lifecycle sweep claim release")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketWriteReservationNodeClient::release_lifecycle_sweep_claim(
            &local_client,
            request.pg_id,
            &request.claim,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_list_page_response(
        &self,
        request: StorageRpcListObjectsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "object list page") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectListingMetadataNodeClient::list_objects_page(
            &local_client,
            request.pg_id,
            &request.request,
        ) {
            Ok(response) => {
                let payload =
                    encode_list_objects_response(&StorageRpcListObjectsResponse { response })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_version_list_page_response(
        &self,
        request: StorageRpcListObjectVersionsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "object version list page") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectListingMetadataNodeClient::list_object_versions_page(
            &local_client,
            request.pg_id,
            &request.request,
        ) {
            Ok(response) => {
                let payload =
                    encode_list_object_versions_response(&StorageRpcListObjectVersionsResponse {
                        response,
                    })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_multipart_upload_list_page_response(
        &self,
        request: StorageRpcListMultipartUploadsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "object multipart upload list page")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectListingMetadataNodeClient::list_multipart_uploads_page(
            &local_client,
            request.pg_id,
            &request.request,
        ) {
            Ok(response) => {
                let payload = encode_list_multipart_uploads_response(
                    &StorageRpcListMultipartUploadsResponse { response },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_version_next_response(
        &self,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_pg_for_object(
            request.pg_id,
            &request.bucket,
            &request.key,
            "object version allocation",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectVersionMetadataNodeClient::next_object_version_id(
            &local_client,
            request.pg_id,
            &request.bucket,
            &request.key,
        ) {
            Ok(version_id) => {
                let payload =
                    encode_object_version_response(&StorageRpcObjectVersionResponse { version_id });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn object_generation_reservation_response(
        &self,
        request: StorageRpcObjectGenerationReservationRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object generation reservation lookup",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match ObjectGenerationMetadataNodeClient::object_generation_reservation(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.reservation_id,
        ) {
            Ok(generation_id) => StorageRpcObjectGenerationReservationOutcome::Found(generation_id),
            Err(ObjectPgActionError::Metadata(
                crate::MetadataError::ObjectGenerationReservationNotFound { reservation_id },
            )) => StorageRpcObjectGenerationReservationOutcome::NotFound {
                reservation_id: SessionId::try_from(reservation_id).map_err(|_| {
                    crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "stored reservation id is invalid",
                    )
                })?,
            },
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_object_generation_reservation_response(
            &StorageRpcObjectGenerationReservationResponse { outcome },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn direct_put_commit_snapshot_response(
        &self,
        request: StorageRpcDirectPutCommitSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "direct PUT commit snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.reservation_id,
            request.generation_id,
        ) {
            Ok(snapshot) => {
                let payload = encode_direct_put_commit_snapshot_response(
                    &StorageRpcDirectPutCommitSnapshotResponse { snapshot },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn direct_put_commit_command_build_response(
        &self,
        request: StorageRpcDirectPutCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if request.object.bucket != request.request.bucket
            || request.object.bucket != request.request.bucket_write_reservation.bucket
            || request.object.bucket != request.bucket_write_reservation.bucket
            || request.request.bucket_write_reservation != request.bucket_write_reservation
        {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "direct PUT bucket write reservation proof does not match object bucket"
                    .to_string(),
            });
        }
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "direct PUT commit command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match DirectPutMetadataNodeClient::build_direct_put_commit_command(
            &local_client,
            BuildDirectPutCommitCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                request: &request.request,
                version_id: request.version_id,
                expected_snapshot: &request.expected_snapshot,
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => {
                let payload = encode_direct_put_command_build_response(
                    &StorageRpcDirectPutCommandBuildResponse {
                        outcome: StorageRpcDirectPutCommandBuildOutcome::Command(Box::new(command)),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(ObjectPgActionError::StaleDirectPutCommitSnapshot) => {
                let payload = encode_direct_put_command_build_response(
                    &StorageRpcDirectPutCommandBuildResponse {
                        outcome: StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            })) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some("CommitDirectPutObject"),
                );
                let payload = encode_direct_put_command_build_response(
                    &StorageRpcDirectPutCommandBuildResponse {
                        outcome: StorageRpcDirectPutCommandBuildOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn put_object_metadata_snapshot_response(
        &self,
        request: StorageRpcPutObjectMetadataSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object metadata PUT snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.version_id,
        ) {
            Ok(stored) => StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(Box::new(stored)),
            Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)) => {
                StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound
            }
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_put_object_metadata_snapshot_response(
            &StorageRpcPutObjectMetadataSnapshotResponse { outcome },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn put_object_metadata_command_build_response(
        &self,
        request: StorageRpcPutObjectMetadataCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "object metadata PUT command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_put_object_metadata_command(
            &local_client,
            BuildPutObjectMetadataCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                requested_version_id: request.requested_version_id,
                expected_stored: &request.expected_stored,
                version_id: request.version_id,
                mutation: request.mutation,
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => {
                match object_metadata_command_build_error_outcome(error, Some("PutObjectMetadata"))
                {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_delete_snapshot_response(
        &self,
        kind: StorageRpcMessageKind,
        request: StorageRpcObjectDeleteSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object delete snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let stored = match kind {
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad => {
                ObjectMutationMetadataNodeClient::load_current_object_delete_snapshot(
                    &local_client,
                    request.object.pg_id,
                    &request.object.bucket,
                    &request.object.key,
                )
            }
            StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad => {
                let Some(version_id) = request.version_id else {
                    return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "specific delete snapshot requires version id".to_string(),
                    });
                };
                ObjectMutationMetadataNodeClient::load_specific_object_delete_snapshot(
                    &local_client,
                    request.object.pg_id,
                    &request.object.bucket,
                    &request.object.key,
                    version_id,
                )
            }
            _ => unreachable!("object delete snapshot response called with non-delete kind"),
        };
        let snapshot = match stored {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_object_delete_snapshot_response(&StorageRpcObjectDeleteSnapshotResponse {
                stored: snapshot.stored,
                target: snapshot.target,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_lifecycle_version_list_response(
        &self,
        request: StorageRpcObjectDeleteSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if request.version_id.is_some() {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "lifecycle version list request must not include version id".to_string(),
            });
        }
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object lifecycle version list load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let versions = match ObjectMutationMetadataNodeClient::list_object_versions_for_lifecycle(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
        ) {
            Ok(versions) => versions,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_object_lifecycle_version_list_response(
            &StorageRpcObjectLifecycleVersionListResponse { versions },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn delete_specific_object_command_build_response(
        &self,
        request: StorageRpcDeleteSpecificObjectCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "delete-specific object command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response =
            match ObjectMutationMetadataNodeClient::build_delete_specific_object_version_command(
                &local_client,
                BuildDeleteSpecificObjectVersionCommandReq {
                    pg_id: request.object.pg_id,
                    cluster_epoch: request.object.cluster_epoch,
                    bucket: &request.object.bucket,
                    key: &request.object.key,
                    version_id: request.version_id,
                    expected_stored: request.expected_stored.as_ref(),
                    expected_target: request.expected_target.as_ref(),
                    expected_version_list: request.expected_version_list.as_deref(),
                    bucket_write_reservation: &request.bucket_write_reservation,
                },
            ) {
                Ok(Some(command)) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
                }
                Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
                }
                Err(error) => match object_metadata_command_build_error_outcome(
                    error,
                    Some("DeleteObjectVersion"),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                },
            };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn delete_current_object_command_build_response(
        &self,
        request: StorageRpcDeleteCurrentObjectCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "delete-current object command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_delete_current_object_command(
            &local_client,
            BuildDeleteCurrentObjectCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                expected_current: request.expected_current.as_ref(),
                expected_target: request.expected_target.as_ref(),
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => match object_metadata_command_build_error_outcome(
                error,
                Some("DeleteObjectVersion"),
            ) {
                Ok(outcome) => outcome,
                Err(error) => return encode_storage_rpc_error_response(&error),
            },
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn insert_delete_marker_command_build_response(
        &self,
        request: StorageRpcInsertDeleteMarkerCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "insert-delete-marker command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let stale_payload = match request.stale_payload {
            crate::storage_rpc::StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim) => {
                InsertDeleteMarkerStalePayload::Explicit(reclaim)
            }
            crate::storage_rpc::StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                created_at,
            } => InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at },
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &local_client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                expected_current: request.expected_current.as_ref(),
                version_id: request.version_id,
                owner: &request.owner,
                stale_payload,
                expected_stale_payload_source: request.expected_stale_payload_source.as_ref(),
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => {
                match object_metadata_command_build_error_outcome(error, Some("InsertDeleteMarker"))
                {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_upload_match_response(
        &self,
        request: StorageRpcStreamUploadMatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream upload match",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let exists = match ObjectMutationMetadataNodeClient::matching_stream_upload_exists(
            &local_client,
            request.object.pg_id,
            &request.request,
            request.expected_command.as_ref(),
        ) {
            Ok(exists) => exists,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_stream_upload_match_response(&StorageRpcStreamUploadMatchResponse { exists });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_upload_session_response(
        &self,
        request: StorageRpcStreamUploadSessionRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream upload session load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match ObjectMutationMetadataNodeClient::load_stream_upload_session(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.session_id,
        ) {
            Ok(session) => StorageRpcStreamUploadSessionOutcome::Loaded(Box::new(session)),
            Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound { .. })) => {
                StorageRpcStreamUploadSessionOutcome::NotFound {
                    session_id: request.session_id,
                }
            }
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_stream_upload_session_response(&StorageRpcStreamUploadSessionResponse {
                outcome,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_upload_segments_response(
        &self,
        request: StorageRpcStreamUploadSessionRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream upload segments load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match ObjectMutationMetadataNodeClient::load_stream_upload_segments(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.session_id,
        ) {
            Ok(segments) => StorageRpcStreamUploadSegmentsOutcome::Loaded(segments),
            Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound { .. })) => {
                StorageRpcStreamUploadSegmentsOutcome::NotFound {
                    session_id: request.session_id,
                }
            }
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_stream_upload_segments_response(&StorageRpcStreamUploadSegmentsResponse {
                outcome,
            })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_uploads_list_response(
        &self,
        request: StorageRpcStreamUploadsListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.bucket.pg_id, "object stream uploads list")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let page = match ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.session_id_marker.as_ref(),
            request.limit,
        ) {
            Ok(page) => page,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        for upload in &page.uploads {
            if let Err(error) = self.validate_pg_for_object(
                request.bucket.pg_id,
                &upload.bucket,
                &upload.key,
                "object stream uploads list",
            ) {
                return encode_storage_rpc_error_response(&error);
            }
        }
        let payload = encode_stream_uploads_list_response(&StorageRpcStreamUploadsListResponse {
            uploads: page.uploads,
            next_session_id_marker: page.next_session_id_marker,
        })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_uploads_pg_list_response(
        &self,
        request: StorageRpcStreamUploadsPgListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "object stream uploads PG list")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let page = match ObjectMutationMetadataNodeClient::list_all_stream_uploads_page(
            &local_client,
            request.pg_id,
            request.session_id_marker.as_ref(),
            request.limit,
        ) {
            Ok(page) => page,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_stream_uploads_list_response(&StorageRpcStreamUploadsListResponse {
            uploads: page.uploads,
            next_session_id_marker: page.next_session_id_marker,
        })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn completed_multipart_uploads_list_response(
        &self,
        request: StorageRpcCompletedMultipartUploadsListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.bucket.pg_id,
            "object completed multipart uploads list",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let records =
            match ObjectMutationMetadataNodeClient::list_completed_multipart_upload_records_for_bucket_page(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                request.upload_id_marker.as_ref(),
                request.limit,
            ) {
                Ok(records) => records,
                Err(error) => return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
            };
        let payload = encode_completed_multipart_uploads_list_response(
            &StorageRpcCompletedMultipartUploadsListResponse {
                records: records.records,
                next_upload_id_marker: records.next_upload_id_marker,
            },
        )?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn bucket_payload_reclaim_root_response(
        &self,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "object bucket payload reclaim root")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let root = match ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &local_client,
            request.pg_id,
            &request.bucket,
        ) {
            Ok(root) => root,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let payload =
            encode_payload_reclaim_root_response(&StorageRpcPayloadReclaimRootResponse { root });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_exists_response(
        &self,
        request: StorageRpcObjectPayloadReclaimExistsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object payload reclaim exists",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let exists = match ObjectMutationMetadataNodeClient::payload_reclaim_exists(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.generation_id,
        ) {
            Ok(exists) => exists,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload =
            encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                value: exists,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_root_response(
        &self,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "object payload reclaim root") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let root = match ObjectMutationMetadataNodeClient::get_payload_reclaim_root(
            &local_client,
            request.pg_id,
        ) {
            Ok(root) => root,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let payload =
            encode_payload_reclaim_root_response(&StorageRpcPayloadReclaimRootResponse { root });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_load_response(
        &self,
        request: StorageRpcObjectPayloadReclaimExistsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object payload reclaim load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let reclaim = match ObjectMutationMetadataNodeClient::get_object_payload_reclaim(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.generation_id,
        ) {
            Ok(reclaim) => reclaim,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let payload =
            encode_object_payload_reclaim_response(&StorageRpcObjectPayloadReclaimResponse {
                reclaim,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_claim_acquire_response(
        &self,
        request: StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object payload reclaim claim acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectMutationMetadataNodeClient::acquire_object_payload_reclaim_claim(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            request.bucket_incarnation_generation,
            &request.object.key,
            request.generation_id,
            request.reclaim_kind,
            &request.claim_id,
            &request.owner_token,
            request.object.cluster_epoch,
            request.claimed_at,
            request.lease_deadline,
            request.now,
        ) {
            Ok(record) => {
                let payload = encode_object_payload_reclaim_claim_optional_record_response(
                    &StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn object_payload_reclaim_claim_release_response(
        &self,
        request: StorageRpcObjectPayloadReclaimClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.pg_id,
            &request.record.bucket,
            &request.record.key,
            "object payload reclaim claim release",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
            &local_client,
            request.pg_id,
            &request.record,
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn stream_segment_append_prepare_response(
        &self,
        request: StorageRpcStreamSegmentAppendPrepareRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream segment append prepare",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match ObjectMutationMetadataNodeClient::prepare_stream_segment_append(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.request,
        ) {
            Ok((target, mut segment)) => {
                segment.placement_cluster_epoch = request.object.cluster_epoch;
                StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
                    target,
                    segment: Box::new(segment),
                }
            }
            Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound { .. })) => {
                StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
                    session_id: request.request.session_id,
                }
            }
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_stream_segment_append_prepare_response(
            &StorageRpcStreamSegmentAppendPrepareResponse { outcome },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_upload_match_response(
        &self,
        request: StorageRpcMultipartUploadMatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart upload match",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let initiated_at =
            match ObjectMutationMetadataNodeClient::matching_multipart_upload_initiated_at(
                &local_client,
                request.object.pg_id,
                &request.request,
                request.expected_command.as_ref(),
            ) {
                Ok(initiated_at) => initiated_at,
                Err(error) => {
                    return encode_storage_rpc_error_response(&object_pg_error_response(error));
                }
            };
        let payload =
            encode_multipart_upload_match_response(&StorageRpcMultipartUploadMatchResponse {
                initiated_at,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_upload_load_response(
        &self,
        kind: StorageRpcMessageKind,
        request: StorageRpcMultipartUploadLoadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart upload load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let outcome = match kind {
            StorageRpcMessageKind::ObjectMultipartUploadLoad => {
                match ObjectMutationMetadataNodeClient::load_multipart_upload(
                    &local_client,
                    request.object.pg_id,
                    &request.object.bucket,
                    &request.object.key,
                    &request.upload_id,
                ) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload {
                        ..
                    })) => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                        upload_id: request.upload_id,
                    },
                    Err(error) => {
                        return encode_storage_rpc_error_response(
                            &bucket_snapshot_error_response(error),
                        );
                    }
                }
            }
            StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad => {
                match ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload(
                    &local_client,
                    request.object.pg_id,
                    &request.object.bucket,
                    &request.object.key,
                    &request.upload_id,
                ) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                        StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                            upload_id: request.upload_id,
                        }
                    }
                    Err(error) => {
                        return encode_storage_rpc_error_response(
                            &object_pg_error_response(error),
                        );
                    }
                }
            }
            StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad => {
                match ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload_for_listing(
                    &local_client,
                    request.object.pg_id,
                    &request.object.bucket,
                    &request.object.key,
                    &request.upload_id,
                ) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                        StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                            upload_id: request.upload_id,
                        }
                    }
                    Err(error) => {
                        return encode_storage_rpc_error_response(
                            &object_pg_error_response(error),
                        );
                    }
                }
            }
            _ => unreachable!("multipart upload load handler called for wrong kind"),
        };
        let payload =
            encode_multipart_upload_load_response(&StorageRpcMultipartUploadLoadResponse {
                outcome,
            })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_completion_snapshot_response(
        &self,
        request: StorageRpcMultipartCompletionSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart completion snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome = match ObjectMutationMetadataNodeClient::load_multipart_completion_snapshot(
            &local_client,
            request.object.pg_id,
            &authorized_upload,
            &request.requested_part_numbers,
        ) {
            Ok(snapshot) => {
                StorageRpcMultipartCompletionSnapshotOutcome::Loaded(Box::new(snapshot))
            }
            Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
                    upload_id: authorized_upload.upload_id.clone(),
                }
            }
            Err(ObjectPgActionError::Metadata(MetadataError::PartNotFound {
                part_number, ..
            })) => StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: authorized_upload.upload_id.clone(),
                part_number,
            },
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_multipart_completion_snapshot_response(
            &StorageRpcMultipartCompletionSnapshotResponse { outcome },
        )?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_completion_preflight_response(
        &self,
        request: StorageRpcMultipartCompletionPreflightRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart completion preflight load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome = match ObjectMutationMetadataNodeClient::load_multipart_completion_preflight(
            &local_client,
            request.object.pg_id,
            &authorized_upload,
        ) {
            Ok(preflight) => StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight),
            Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
                    upload_id: authorized_upload.upload_id.clone(),
                }
            }
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_multipart_completion_preflight_response(
            &StorageRpcMultipartCompletionPreflightResponse { outcome },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_parts_list_response(
        &self,
        request: StorageRpcMultipartPartsListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart parts list",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome =
            match ObjectMutationMetadataNodeClient::list_multipart_parts_for_authorized_upload(
                &local_client,
                request.object.pg_id,
                &authorized_upload,
                request.part_number_marker,
                request.max_parts,
            ) {
                Ok(listed) => StorageRpcMultipartPartsListOutcome::Loaded(Box::new(listed)),
                Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                    StorageRpcMultipartPartsListOutcome::NoSuchUpload {
                        upload_id: authorized_upload.upload_id.clone(),
                    }
                }
                Err(error) => {
                    return encode_storage_rpc_error_response(&object_pg_error_response(error));
                }
            };
        let payload =
            encode_multipart_parts_list_response(&StorageRpcMultipartPartsListResponse {
                outcome,
            })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_management_lookup_response(
        &self,
        request: StorageRpcMultipartUploadLoadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "multipart management lookup",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let lookup = match ObjectMutationMetadataNodeClient::lookup_multipart_upload_management(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.upload_id,
        ) {
            Ok(lookup) => lookup,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_multipart_management_lookup_response(
            &StorageRpcMultipartManagementLookupResponse { lookup },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn create_stream_upload_command_build_response(
        &self,
        request: StorageRpcCreateStreamUploadCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "stream upload command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let precondition = match &request.precondition {
            StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation,
            } => CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation: *require_generation_reservation,
            },
            StorageRpcCreateStreamUploadPrecondition::PutObject {
                expected_current,
                require_generation_reservation,
            } => CreateStreamUploadPrecondition::PutObject {
                expected_current: expected_current.as_ref(),
                require_generation_reservation: *require_generation_reservation,
            },
            StorageRpcCreateStreamUploadPrecondition::UploadPart { expected_upload } => {
                CreateStreamUploadPrecondition::UploadPart { expected_upload }
            }
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &local_client,
            BuildCreateStreamUploadCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                request: &request.request,
                precondition,
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Missing
            }
            Err(error) => {
                match object_metadata_command_build_error_outcome(error, Some("CreateStreamUpload"))
                {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn create_multipart_upload_command_build_response(
        &self,
        request: StorageRpcCreateMultipartUploadCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "multipart upload command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_create_multipart_upload_command(
            &local_client,
            BuildCreateMultipartUploadCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                request: &request.request,
                expected_current: request.expected_current.as_ref(),
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => match object_metadata_command_build_error_outcome(
                error,
                Some("CreateMultipartUpload"),
            ) {
                Ok(outcome) => outcome,
                Err(error) => return encode_storage_rpc_error_response(&error),
            },
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_put_finalize_snapshot_response(
        &self,
        request: StorageRpcStreamPutFinalizeSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream PUT finalize snapshot",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let snapshot = match ObjectMutationMetadataNodeClient::load_stream_put_finalize_snapshot(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.session_id,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_stream_put_finalize_snapshot_response(
            &StorageRpcStreamPutFinalizeSnapshotResponse { snapshot },
        )?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_put_commit_command_build_response(
        &self,
        request: StorageRpcStreamPutCommitCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "stream PUT commit command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_stream_put_commit_command(
            &local_client,
            BuildStreamPutCommitCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                session_id: &request.session_id,
                total_size: request.total_size,
                expected_snapshot: &request.expected_snapshot,
                commit: &request.commit,
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => match object_metadata_command_build_error_outcome(
                error,
                Some("CommitDirectPutObject"),
            ) {
                Ok(outcome) => outcome,
                Err(error) => return encode_storage_rpc_error_response(&error),
            },
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_part_finalize_snapshot_response(
        &self,
        request: StorageRpcStreamPartFinalizeSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "stream part finalize snapshot",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let snapshot = match ObjectMutationMetadataNodeClient::load_stream_part_finalize_snapshot(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.upload_id,
            &request.session_id,
            request.part_number,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_stream_part_finalize_snapshot_response(
            &StorageRpcStreamPartFinalizeSnapshotResponse { snapshot },
        )?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_part_commit_command_build_response(
        &self,
        request: StorageRpcStreamPartCommitCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "stream part commit command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_stream_part_commit_command(
            &local_client,
            BuildStreamPartCommitCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                upload_id: &request.upload_id,
                session_id: &request.session_id,
                part_number: request.part_number,
                expected_snapshot: &request.expected_snapshot,
                part: &request.part,
                segments: &request.segments,
                bucket_write_reservation: &request.bucket_write_reservation,
            },
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => {
                match object_metadata_command_build_error_outcome(error, Some("CommitStreamPart")) {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn complete_multipart_command_build_response(
        &self,
        request: StorageRpcCompleteMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "complete multipart command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let expected_object_parts = complete_multipart_expected_object_parts(
            &request.request,
            request.version_id,
            self.node.pg_topology(),
        );
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response =
            match ObjectMutationMetadataNodeClient::build_complete_multipart_object_command(
                &local_client,
                BuildCompleteMultipartObjectCommandReq {
                    pg_id: request.object.pg_id,
                    cluster_epoch: request.object.cluster_epoch,
                    request: &request.request,
                    version_id: request.version_id,
                    expected_object_parts: &expected_object_parts,
                    completion_order: request.completion_order,
                    bucket_write_reservation: &request.bucket_write_reservation,
                },
            ) {
                Ok(command) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
                }
                Err(ObjectPgActionError::StaleMultipartCompletionSnapshot) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
                }
                Err(error) => match object_metadata_command_build_error_outcome(
                    error,
                    Some("CommitMultipartObject"),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                },
            };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn multipart_completion_stale_source_response(
        &self,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.pg_id,
            &request.bucket,
            &request.key,
            "multipart completion stale source load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let source =
            match ObjectMutationMetadataNodeClient::load_multipart_completion_stale_payload_source(
                &local_client,
                request.pg_id,
                &request.bucket,
                &request.key,
            ) {
                Ok(source) => source,
                Err(error) => {
                    return encode_storage_rpc_error_response(&object_pg_error_response(error))
                }
            };
        let payload = encode_multipart_completion_stale_source_response(
            &StorageRpcMultipartCompletionStaleSourceResponse { source },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn abort_multipart_command_build_response(
        &self,
        request: StorageRpcAbortMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "abort multipart command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response = match ObjectMutationMetadataNodeClient::build_abort_multipart_upload_command(
            &local_client,
            BuildAbortMultipartUploadCommandReq {
                pg_id: request.object.pg_id,
                cluster_epoch: request.object.cluster_epoch,
                bucket: &request.object.bucket,
                key: &request.object.key,
                upload_id: &request.upload_id,
                expected_cleanup: request.expected_cleanup.as_ref(),
                bucket_write_reservation: request.bucket_write_reservation,
            },
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
            }
            Err(error) => match object_metadata_command_build_error_outcome(
                error,
                Some("AbortMultipartUpload"),
            ) {
                Ok(outcome) => outcome,
                Err(error) => return encode_storage_rpc_error_response(&error),
            },
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn abort_multipart_cleanup_response(
        &self,
        request: crate::storage_rpc::StorageRpcAbortMultipartCleanupRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "abort multipart cleanup load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let cleanup = match ObjectMutationMetadataNodeClient::load_abort_multipart_upload_cleanup(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            &request.upload_id,
        ) {
            Ok(cleanup) => cleanup,
            Err(error) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload =
            encode_abort_multipart_cleanup_response(&StorageRpcAbortMultipartCleanupResponse {
                cleanup,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn authorized_abort_multipart_command_build_response(
        &self,
        request: StorageRpcAuthorizedAbortMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_object_mutation_command_request(
            &request.object,
            &request.bucket_write_reservation,
            "authorized abort multipart command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let response =
            match ObjectMutationMetadataNodeClient::build_authorized_abort_multipart_upload_command(
                &local_client,
                BuildAuthorizedAbortMultipartUploadCommandReq {
                    pg_id: request.object.pg_id,
                    cluster_epoch: request.object.cluster_epoch,
                    authorized_upload: &authorized_upload,
                    expected_cleanup: request.expected_cleanup.as_ref(),
                    bucket_write_reservation: request.bucket_write_reservation,
                },
            ) {
                Ok(Some(command)) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
                }
                Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot
                }
                Err(error) => match object_metadata_command_build_error_outcome(
                    error,
                    Some("AbortMultipartUpload"),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                },
            };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome: response },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_read_auth_subject_response(
        &self,
        request: StorageRpcObjectReadAuthSubjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object read auth subject load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectReadMetadataNodeClient::load_object_read_auth_subject(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.version_id,
        ) {
            Ok(subject) => {
                let payload = encode_object_read_auth_subject_response(
                    &StorageRpcObjectReadAuthSubjectResponse {
                        outcome: StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(subject)),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)) => {
                let payload = encode_object_read_auth_subject_response(
                    &StorageRpcObjectReadAuthSubjectResponse {
                        outcome: StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn object_read_snapshot_response(
        &self,
        request: StorageRpcObjectReadSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object read snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectReadMetadataNodeClient::load_object_read_snapshot_for_subject(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.version_id,
            &request.expected_identity,
            request.snapshot_mode,
        ) {
            Ok(snapshot) => {
                let payload =
                    encode_object_read_snapshot_response(&StorageRpcObjectReadSnapshotResponse {
                        outcome: StorageRpcObjectReadSnapshotOutcome::Loaded(Box::new(snapshot)),
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                let payload =
                    encode_object_read_snapshot_response(&StorageRpcObjectReadSnapshotResponse {
                        outcome: StorageRpcObjectReadSnapshotOutcome::StaleSubject,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn object_tags_for_subject_response(
        &self,
        request: StorageRpcObjectTagsForSubjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.object.node_id,
            request.object.cluster_epoch,
            request.object.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_object(
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            "object tags for subject load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match ObjectReadMetadataNodeClient::get_object_tags_for_subject(
            &local_client,
            request.object.pg_id,
            &request.object.bucket,
            &request.object.key,
            request.version_id,
            &request.expected_identity,
            request.authorized_version_id,
        ) {
            Ok(tags) => {
                let payload = encode_object_tags_for_subject_response(
                    &StorageRpcObjectTagsForSubjectResponse {
                        outcome: StorageRpcObjectTagsForSubjectOutcome::Loaded(tags),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(ObjectPgActionError::StaleObjectReadSubject) => {
                let payload = encode_object_tags_for_subject_response(
                    &StorageRpcObjectTagsForSubjectResponse {
                        outcome: StorageRpcObjectTagsForSubjectOutcome::StaleSubject,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&object_pg_error_response(error)),
        }
    }

    fn bucket_head_response(
        &self,
        request: StorageRpcBucketRequest,
        filtered: bool,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = if filtered {
            BucketMetadataNodeClient::head_bucket_info(
                &local_client,
                request.pg_id,
                &request.bucket,
            )
        } else {
            BucketMetadataNodeClient::head_bucket_raw(&local_client, request.pg_id, &request.bucket)
        };
        match result {
            Ok(info) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::Info(info),
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::BucketNotFound { name },
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_snapshot_response(
        &self,
        request: StorageRpcBucketSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.bucket.pg_id,
            &request.bucket.bucket,
            "bucket snapshot load",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::load_bucket_snapshot(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.request,
        ) {
            Ok(snapshot) => {
                let payload = encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::Loaded(Box::new(snapshot)),
                });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })) => {
                let payload = encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound { name },
                });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_snapshot_pair_response(
        &self,
        request: StorageRpcBucketSnapshotPairRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        for bucket in [&request.source.bucket, &request.destination.bucket] {
            if let Err(error) =
                self.validate_pg_route(bucket.node_id, bucket.cluster_epoch, bucket.pg_id)
            {
                return encode_storage_rpc_error_response(&error);
            }
            if let Err(error) = self.validate_primary_pg_for_bucket(
                bucket.pg_id,
                &bucket.bucket,
                "bucket snapshot pair load",
            ) {
                return encode_storage_rpc_error_response(&error);
            }
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::load_bucket_snapshot_pair(
            &local_client,
            request.source.bucket.pg_id,
            (&request.source.bucket.bucket, request.source.request),
            request.destination.bucket.pg_id,
            (
                &request.destination.bucket.bucket,
                request.destination.request,
            ),
        ) {
            Ok(pair) => {
                let payload =
                    encode_bucket_snapshot_pair_response(&StorageRpcBucketSnapshotPairResponse {
                        outcome: StorageRpcBucketSnapshotPairOutcome::Loaded(Box::new(pair)),
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })) => {
                let payload =
                    encode_bucket_snapshot_pair_response(&StorageRpcBucketSnapshotPairResponse {
                        outcome: StorageRpcBucketSnapshotPairOutcome::BucketNotFound { name },
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_create_command_build_response(
        &self,
        request: StorageRpcCreateBucketCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if request.bucket != request.config.name {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "request bucket must match create-bucket config name".to_string(),
            });
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let config = request.config.as_create_bucket_config();
        match BucketMetadataNodeClient::build_create_bucket_command(
            &local_client,
            request.pg_id,
            &request.bucket,
            request.command_id,
            &config,
        ) {
            Ok(CreateBucketCommandBuild::Exists(info)) => {
                let payload = encode_create_bucket_command_build_response(
                    &StorageRpcCreateBucketCommandBuildResponse {
                        outcome: StorageRpcCreateBucketCommandBuildOutcome::Exists(info),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Ok(CreateBucketCommandBuild::Command(command)) => {
                let payload = encode_create_bucket_command_build_response(
                    &StorageRpcCreateBucketCommandBuildResponse {
                        outcome: StorageRpcCreateBucketCommandBuildOutcome::Command(command),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn completed_multipart_order_command_build_response(
        &self,
        request: StorageRpcCompletedMultipartOrderCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "completed multipart order command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::build_advance_completed_multipart_upload_sequence_command(
            &local_client,
            request.pg_id,
            &request.bucket,
            request.command_id,
        ) {
            Ok((completion_order, command)) => {
                let payload = encode_completed_multipart_order_command_build_response(
                    &StorageRpcCompletedMultipartOrderCommandBuildResponse {
                        completion_order,
                        command,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_metadata_control_pending_match_response(
        &self,
        request: StorageRpcBucketMetadataControlPendingMatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_bucket_metadata_control_route(
            &request.bucket,
            "bucket metadata control pending match",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = match (&request.mutation, request.command.payload()) {
            (
                StorageRpcBucketMetadataControlMutation::MarkDeleting,
                MetadataCommandPayload::MarkBucketDeleting(command),
            ) => BucketMetadataNodeClient::pending_mark_bucket_deleting_command_matches_current(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                command,
            ),
            (
                StorageRpcBucketMetadataControlMutation::Versioning(state),
                MetadataCommandPayload::PutBucketVersioning(command),
            ) => BucketMetadataNodeClient::pending_put_bucket_versioning_command_matches_current(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                command,
                *state,
            ),
            (
                StorageRpcBucketMetadataControlMutation::Acl {
                    acl_grants,
                    public_read,
                    public_write,
                },
                MetadataCommandPayload::PutBucketAcl(command),
            ) => BucketMetadataNodeClient::pending_put_bucket_acl_command_matches_current(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                command,
                acl_grants,
                *public_read,
                *public_write,
            ),
            (
                StorageRpcBucketMetadataControlMutation::Property(mutation),
                MetadataCommandPayload::PutBucketProperty(command),
            ) => BucketMetadataNodeClient::pending_put_bucket_property_command_matches_current(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                command,
                mutation,
            ),
            _ => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "pending command payload does not match bucket control mutation"
                        .to_string(),
                });
            }
        };
        match result {
            Ok(value) => {
                let payload = encode_metadata_command_bool_response(
                    &crate::storage_rpc::StorageRpcMetadataCommandBoolResponse { value },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_metadata_control_command_build_response(
        &self,
        request: StorageRpcBucketMetadataControlCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_bucket_metadata_control_route(
            &request.bucket,
            "bucket metadata control command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = match &request.mutation {
            StorageRpcBucketMetadataControlMutation::MarkDeleting => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "mark-deleting command build uses a dedicated RPC".to_string(),
                });
            }
            StorageRpcBucketMetadataControlMutation::Versioning(state) => {
                BucketMetadataNodeClient::build_put_bucket_versioning_command(
                    &local_client,
                    request.bucket.pg_id,
                    &request.bucket.bucket,
                    request.command_id,
                    *state,
                )
            }
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants,
                public_read,
                public_write,
            } => BucketMetadataNodeClient::build_put_bucket_acl_command(
                &local_client,
                request.bucket.pg_id,
                &request.bucket.bucket,
                request.command_id,
                acl_grants,
                *public_read,
                *public_write,
            ),
            StorageRpcBucketMetadataControlMutation::Property(mutation) => {
                BucketMetadataNodeClient::build_put_bucket_property_command(
                    &local_client,
                    request.bucket.pg_id,
                    &request.bucket.bucket,
                    request.command_id,
                    mutation,
                )
            }
            StorageRpcBucketMetadataControlMutation::Subresource(mutation) => {
                BucketMetadataNodeClient::build_put_bucket_subresource_command(
                    &local_client,
                    request.bucket.pg_id,
                    &request.bucket.bucket,
                    request.command_id,
                    mutation,
                )
            }
        };
        match result {
            Ok(command) => {
                let payload = encode_bucket_metadata_control_command_build_response(
                    &StorageRpcBucketMetadataControlCommandBuildResponse { command },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_mark_deleting_command_build_response(
        &self,
        request: StorageRpcBucketMarkDeletingCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_bucket_metadata_control_route(
            &request.bucket,
            "bucket mark-deleting command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::build_mark_bucket_deleting_command(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.command_id,
        ) {
            Ok(MarkBucketDeletingCommandBuild::AlreadyDeleting) => {
                let info = match BucketMetadataNodeClient::head_bucket_raw(
                    &local_client,
                    request.bucket.pg_id,
                    &request.bucket.bucket,
                ) {
                    Ok(info) if info.state == BucketState::Deleting => info,
                    Ok(_) => {
                        return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                            code: StorageRpcErrorCode::Internal,
                            message: "mark-deleting builder reported non-deleting bucket as already deleting"
                                .to_string(),
                        });
                    }
                    Err(error) => {
                        return encode_storage_rpc_error_response(&bucket_snapshot_error_response(
                            error,
                        ));
                    }
                };
                let payload = encode_bucket_mark_deleting_command_build_response(
                    &StorageRpcBucketMarkDeletingCommandBuildResponse {
                        outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(
                            info,
                        ),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Ok(MarkBucketDeletingCommandBuild::Command(command)) => {
                let payload = encode_bucket_mark_deleting_command_build_response(
                    &StorageRpcBucketMarkDeletingCommandBuildResponse {
                        outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_subresource_get_response(
        &self,
        request: StorageRpcBucketSubresourceGetRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_bucket_metadata_control_route(&request.bucket, "bucket subresource get")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::get_bucket_subresource(
            &local_client,
            request.bucket.pg_id,
            &request.bucket.bucket,
            request.kind,
        ) {
            Ok(body) => {
                let payload = encode_bucket_subresource_get_response(
                    &StorageRpcBucketSubresourceGetResponse { body },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_list_response(
        &self,
        request: StorageRpcBucketListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "bucket list") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::list_buckets(
            &local_client,
            request.pg_id,
            &request.owner_canonical_id,
        ) {
            Ok(buckets) => {
                let payload =
                    encode_bucket_list_response(&StorageRpcBucketListResponse { buckets })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_execution_generations_response(
        &self,
        request: StorageRpcBucketBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_bucket_batch_route(&request, "bucket execution generations")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::load_bucket_execution_generations(
            &local_client,
            request.pg_id,
            &request.buckets,
        ) {
            Ok(generations) => {
                let payload = encode_bucket_execution_generations_response(
                    &StorageRpcBucketExecutionGenerationsResponse { generations },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn bucket_fast_path_identities_response(
        &self,
        request: StorageRpcBucketBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_bucket_batch_route(&request, "bucket fast-path identities")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match BucketMetadataNodeClient::load_bucket_fast_path_identities(
            &local_client,
            request.pg_id,
            &request.buckets,
        ) {
            Ok(identities) => {
                let payload = encode_bucket_fast_path_identities_response(
                    &StorageRpcBucketFastPathIdentitiesResponse { identities },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&bucket_snapshot_error_response(error)),
        }
    }

    fn shard_write_response(
        &self,
        request: StorageRpcShardWriteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location(request.location) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.node.write_shard_file_if_absent(
            request.location.data_pg_id().get(),
            &request.shard_key,
            &request.payload,
        ) {
            Ok(ack) => {
                let payload = encode_shard_write_ack(ack);
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_repair_write_response(
        &self,
        request: StorageRpcShardWriteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location(request.location) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.node.write_shard_file(
            request.location.data_pg_id().get(),
            &request.shard_key,
            &request.payload,
        ) {
            Ok(ack) => {
                let payload = encode_shard_write_ack(ack);
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_read_response(
        &self,
        request: StorageRpcShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location(request.location) {
            return encode_storage_rpc_error_response(&error);
        }
        self.shard_read_file_response(request)
    }

    fn shard_historical_read_response(
        &self,
        request: StorageRpcShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location_for_historical_inspection(request.location)
        {
            return encode_storage_rpc_error_response(&error);
        }
        self.shard_read_file_response(request)
    }

    fn shard_read_file_response(
        &self,
        request: StorageRpcShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let response = match self
            .node
            .read_shard_file(request.location.data_pg_id().get(), &request.shard_key)
        {
            Ok(payload) => {
                let actual_size = payload.len() as u64;
                let actual_crc = checksum::crc64::checksum(&payload);
                if actual_size != request.expected_ack.stored_size
                    || actual_crc != request.expected_ack.crc64
                {
                    encode_storage_rpc_error_response(&store_error_response(
                        StoreError::ShardAckMismatch {
                            shard: request.shard_key,
                            expected_size: request.expected_ack.stored_size,
                            expected_crc: request.expected_ack.crc64,
                            actual_size,
                            actual_crc,
                        },
                    ))?
                } else {
                    let payload = encode_shard_read_response(&payload, request.expected_ack)?;
                    encode_storage_rpc_success_response(&payload)
                }
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_read_range_response(
        &self,
        request: StorageRpcShardReadRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location(request.location) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self
            .node
            .read_shard_file(request.location.data_pg_id().get(), &request.shard_key)
        {
            Ok(payload) => {
                let actual_size = payload.len() as u64;
                let actual_crc = checksum::crc64::checksum(&payload);
                if actual_size != request.expected_ack.stored_size
                    || actual_crc != request.expected_ack.crc64
                {
                    encode_storage_rpc_error_response(&store_error_response(
                        StoreError::ShardAckMismatch {
                            shard: request.shard_key,
                            expected_size: request.expected_ack.stored_size,
                            expected_crc: request.expected_ack.crc64,
                            actual_size,
                            actual_crc,
                        },
                    ))?
                } else {
                    let start = request.offset as usize;
                    let end = start + request.length as usize;
                    let payload = encode_shard_read_range_response(&payload[start..end]);
                    encode_storage_rpc_success_response(&payload)
                }
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_delete_response(
        &self,
        request: StorageRpcShardDeleteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_location_for_cleanup(request.location) {
            return encode_storage_rpc_error_response(&error);
        }
        let _delete_fence = match self.try_begin_shard_delete(request.location, &request.shard_key)
        {
            Ok(delete_fence) => delete_fence,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match self
            .node
            .delete_shard_file(request.location.data_pg_id().get(), &request.shard_key)
        {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_ack_record_response(
        &self,
        request: StorageRpcShardAckBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard ack record") {
            return encode_storage_rpc_error_response(&error);
        }
        let shard_batch: Vec<(&crate::types::ShardKey, WriteAck)> = request
            .items
            .iter()
            .map(|item| (&item.shard_key, item.ack))
            .collect();
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.register_written_shards_batch_exact(&shard_batch))
        {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_ack_validate_response(
        &self,
        request: StorageRpcShardAckBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard ack validate") {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.validate_shard_ack_batch(request.pg_id, &request.items) {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_ack_load_response(
        &self,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard ack load") {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            let stat = pg.stat_shard(&request.shard_key)?;
            Ok(WriteAck {
                crc64: stat.crc64,
                stored_size: stat.size,
            })
        }) {
            Ok(ack) => encode_storage_rpc_success_response(&encode_shard_ack_item_response(
                &StorageRpcShardAckItem {
                    shard_key: request.shard_key,
                    ack,
                },
            )),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_ack_historical_load_response(
        &self,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if request.node_id != self.config.node_id {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    request.node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if !self.config.pg_ids.contains(&request.pg_id.get()) {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!(
                    "PG {} is not configured on this storage node",
                    request.pg_id.get()
                ),
            });
        }
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            let stat = pg.stat_shard(&request.shard_key)?;
            Ok(WriteAck {
                crc64: stat.crc64,
                stored_size: stat.size,
            })
        }) {
            Ok(ack) => encode_storage_rpc_success_response(&encode_shard_ack_item_response(
                &StorageRpcShardAckItem {
                    shard_key: request.shard_key,
                    ack,
                },
            )),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_ack_delete_response(
        &self,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route =
            match self.cleanup_pg_route(request.node_id, request.cluster_epoch, request.pg_id) {
                Ok(route) => route,
                Err(error) => return encode_storage_rpc_error_response(&error),
            };
        if route.primary_node_id != self.config.node_id {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for shard ack delete on PG {}",
                    self.config.node_id.as_u32(),
                    request.pg_id.get()
                ),
            });
        }
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.delete_shard_record(&request.shard_key))
        {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn validate_shard_ack_batch(
        &self,
        pg_id: PgId,
        items: &[crate::storage_rpc::StorageRpcShardAckItem],
    ) -> Result<(), StoreError> {
        let pg = self.node.get_pg(pg_id.get())?;
        for item in items {
            pg.validate_written_shard_ack(&item.shard_key, item.ack)?;
        }
        Ok(())
    }

    fn shard_scavenger_list_files_response(
        &self,
        request: StorageRpcScavengerListFilesRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.node_id,
            request.cluster_epoch,
            PgId::new(request.data_pg_id.get()),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self
            .node
            .list_scavenger_shard_files(request.data_pg_id.get())
        {
            Ok(scan) => {
                let payload = encode_scavenger_list_files_response(&scan);
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn shard_scavenger_shard_rows_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard scavenger shard rows") {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.list_scavenger_shard_rows(request.pg_id) {
            Ok(rows) => Ok(encode_storage_rpc_success_response(
                &encode_scavenger_shard_rows_response(&rows),
            )),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn shard_scavenger_payload_references_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "shard scavenger payload references")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.list_shard_scavenger_payload_references(request.pg_id) {
            Ok(references) => Ok(encode_storage_rpc_success_response(
                &encode_scavenger_payload_references_response(&references),
            )),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn shard_scavenger_observation_record_response(
        &self,
        request: StorageRpcScavengerObservationRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "shard scavenger observation record")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client
            .record_shard_scavenger_observation(request.route.pg_id, &request.observation)
        {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn shard_scavenger_observations_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard scavenger observations")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.list_shard_scavenger_observations(request.pg_id) {
            Ok(observations) => Ok(encode_storage_rpc_success_response(
                &encode_scavenger_observations_response(&observations),
            )),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn shard_scavenger_observation_resolve_response(
        &self,
        request: StorageRpcScavengerObservationKeyRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "shard scavenger observation resolve")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.resolve_shard_scavenger_observation(request.route.pg_id, &request.key) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repair_record_response(
        &self,
        request: StorageRpcPlacedSegmentShardRepairRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "placed segment shard repair record")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.record_placed_segment_shard_repair(
            request.route.pg_id,
            &request.work_item,
            request.last_error.as_deref(),
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repairs_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "placed segment shard repairs")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.list_placed_segment_shard_repairs(request.pg_id) {
            Ok(repairs) => {
                let payload = encode_placed_segment_shard_repairs_response(&repairs)?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repair_resolve_response(
        &self,
        request: StorageRpcPlacedSegmentShardRepairItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "placed segment shard repair resolve")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client
            .resolve_placed_segment_shard_repair(request.route.pg_id, &request.work_item)
        {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repair_claim_acquire_response(
        &self,
        request: StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard repair claim acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let acquire = PlacedSegmentShardRepairClaimAcquire {
            claim_id: request.claim_id,
            owner_token: request.owner_token,
            cluster_epoch: request.route.cluster_epoch,
            claimed_at: request.claimed_at,
            lease_deadline: request.lease_deadline,
            now: request.now,
        };
        match local_client.acquire_placed_segment_shard_repair_claim(request.route.pg_id, &acquire)
        {
            Ok(record) => {
                let payload = encode_placed_segment_shard_repair_claim_optional_record_response(
                    &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repair_claim_complete_response(
        &self,
        request: StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard repair claim complete",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = validate_placed_segment_shard_repair_claim_route_epoch(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.complete_placed_segment_shard_repair_claim(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_repair_claim_error_response(
        &self,
        request: StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard repair claim error",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = validate_placed_segment_shard_repair_claim_route_epoch(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.record_placed_segment_shard_repair_claim_error(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
            &request.last_error,
            request.next_attempt_after,
        ) {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_record_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "placed segment shard backfill record")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.record_placed_segment_shard_backfill(
            request.route.pg_id,
            &request.work_item,
            request.remaining_tolerance,
            request.last_error.as_deref(),
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfills_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "placed segment shard backfills")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.list_placed_segment_shard_backfills(request.pg_id) {
            Ok(backfills) => {
                let payload = encode_placed_segment_shard_backfills_response(&backfills)?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_count_response(
        &self,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "placed segment shard backfill count")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.count_placed_segment_shard_backfills(request.pg_id) {
            Ok(count) => {
                let payload = encode_placed_segment_shard_backfill_count_response(count)?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_exists_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "placed segment shard backfill exists")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client
            .placed_segment_shard_backfill_exists(request.route.pg_id, &request.work_item)
        {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_resolve_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.route.pg_id, "placed segment shard backfill resolve")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client
            .resolve_placed_segment_shard_backfill(request.route.pg_id, &request.work_item)
        {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_claim_acquire_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard backfill claim acquire",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let acquire = PlacedSegmentShardBackfillClaimAcquire {
            claim_id: request.claim_id,
            owner_token: request.owner_token,
            cluster_epoch: request.route.cluster_epoch,
            claimed_at: request.claimed_at,
            lease_deadline: request.lease_deadline,
            now: request.now,
        };
        match local_client
            .acquire_placed_segment_shard_backfill_claim(request.route.pg_id, &acquire)
        {
            Ok(record) => {
                let payload = encode_placed_segment_shard_backfill_claim_optional_record_response(
                    &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_claim_complete_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard backfill claim complete",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = validate_placed_segment_shard_backfill_claim_route_epoch(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.complete_placed_segment_shard_backfill_claim(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn placed_segment_shard_backfill_claim_error_response(
        &self,
        request: StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route(
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(
            request.route.pg_id,
            "placed segment shard backfill claim error",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = validate_placed_segment_shard_backfill_claim_route_epoch(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
        ) {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        match local_client.record_placed_segment_shard_backfill_claim_error(
            request.route.pg_id,
            request.route.cluster_epoch,
            &request.claim,
            &request.last_error,
            request.next_attempt_after,
        ) {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error)),
        }
    }

    fn metadata_command_replica_state_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.metadata_command_replica_state())
        {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_max_log_index_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.max_metadata_command_log_index(request.cluster_epoch))
        {
            Ok(max_log_index) => {
                let payload = encode_metadata_command_max_log_index_response(
                    &StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_log_hash_range_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.retained_metadata_command_log_hashes(
                self.config.node_id.as_u32(),
                request.cluster_epoch,
                request.first_log_index,
                request.last_log_index,
            )
        }) {
            Ok(entries) => {
                let payload = encode_metadata_command_log_hash_range_response(
                    &StorageRpcMetadataCommandLogHashRangeResponse { entries },
                )?;
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    None,
                );
                encode_storage_rpc_error_response(&store_error_response(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ))?
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_log_entry_range_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.retained_metadata_command_log_entries(
                self.config.node_id.as_u32(),
                request.cluster_epoch,
                request.first_log_index,
                request.last_log_index,
            )
        }) {
            Ok(entries) => {
                let payload = encode_metadata_command_log_entry_range_response(
                    &StorageRpcMetadataCommandLogEntryRangeResponse { entries },
                )?;
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    None,
                );
                encode_storage_rpc_error_response(&store_error_response(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ))?
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_next_id_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandNextIdRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let Some(min_log_index) = MetadataCommandLogIndex::new(request.min_log_index) else {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command min log index must not be zero".to_string(),
            });
        };
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => {
                match self.next_metadata_command_id_from_pg(request.pg_id, &pg, min_log_index) {
                    Ok(id) => {
                        let payload = encode_metadata_command_next_id_response(
                            &StorageRpcMetadataCommandNextIdResponse {
                                outcome: StorageRpcMetadataCommandNextIdOutcome::Allocated {
                                    cluster_epoch: id.cluster_epoch(),
                                    pg_id: id.pg_id(),
                                    log_index: id.log_index().get(),
                                },
                            },
                        );
                        encode_storage_rpc_success_response(&payload)
                    }
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                    }) => {
                        emit_storage_node_metadata_command_log_conflict_for_pg(
                            &pg,
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                            None,
                        );
                        let payload = encode_metadata_command_next_id_response(
                            &StorageRpcMetadataCommandNextIdResponse {
                                outcome: StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                                    node_id,
                                    pg_id,
                                    cluster_epoch,
                                    log_index,
                                },
                            },
                        );
                        encode_storage_rpc_success_response(&payload)
                    }
                    Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
                }
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn next_metadata_command_id_from_pg(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let cluster_epoch = self.config.cluster_epoch;
        let max_log_index = pg.max_metadata_command_log_index(cluster_epoch)?;
        if let Some(slot) =
            pg.pending_metadata_command_slot(self.config.node_id.as_u32(), cluster_epoch)?
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id: self.config.node_id.as_u32(),
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
                node_id: self.config.node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch,
                log_index: u64::MAX,
            })?;
        Ok(MetadataCommandId::new(cluster_epoch, pg_id, next_log_index))
    }

    fn metadata_command_pending_envelope_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.pending_metadata_command_envelope(
                self.config.node_id.as_u32(),
                request.cluster_epoch,
            )
        }) {
            Ok(command) => {
                let payload = encode_metadata_command_pending_envelope_response(
                    &StorageRpcMetadataCommandPendingEnvelopeResponse { command },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_validate_replay_state_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
        preserve_pending_slot: bool,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            if preserve_pending_slot {
                pg.validate_metadata_command_replay_state_preserving_pending_slot(
                    self.config.node_id.as_u32(),
                    request.cluster_epoch,
                )
            } else {
                pg.validate_metadata_command_replay_state(
                    self.config.node_id.as_u32(),
                    request.cluster_epoch,
                )
            }
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_replica_state_can_initialize_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.metadata_command_replica_state_can_initialize())
        {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_checkpoint_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.metadata_command_checkpoint(request.node_id.as_u32(), request.cluster_epoch)
        }) {
            Ok(checkpoint) => {
                let payload = encode_metadata_command_checkpoint_response(
                    &StorageRpcMetadataCommandCheckpointResponse { checkpoint },
                )?;
                encode_metadata_command_checkpoint_success_response(
                    "metadata command checkpoint export",
                    &payload,
                    STORAGE_RPC_MAX_PAYLOAD_LEN,
                )?
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_checkpoint_record_current_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "metadata command checkpoint record current")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.record_current_metadata_command_checkpoint(
                request.node_id.as_u32(),
                request.cluster_epoch,
            )
        }) {
            Ok(checkpoint) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse {
                        state: crate::metadata_command::MetadataCommandReplicaState {
                            cluster_epoch: checkpoint.cluster_epoch,
                            applied_log_index: checkpoint.applied_log_index,
                            applied_log_hash: checkpoint.applied_log_hash,
                            state_digest: checkpoint.state_digest,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_log_compact_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "metadata command log compact")
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self
            .node
            .get_pg(request.pg_id.get())
            .and_then(|pg| pg.compact_metadata_command_log(request.cluster_epoch))
        {
            Ok(status) => {
                let payload = encode_metadata_command_log_compact_response(
                    &StorageRpcMetadataCommandLogCompactResponse { status },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_checkpoint_candidates_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandCheckpointCandidatesRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            let checkpoints = metadata_command_checkpoint_candidates_for_frame(
                &pg,
                request.cluster_epoch,
                request.max_applied_log_index,
                request.limit as usize,
                STORAGE_RPC_MAX_PAYLOAD_LEN,
            )?;
            Ok(checkpoints)
        }) {
            Ok(checkpoints) => {
                let payload = encode_metadata_command_checkpoint_candidates_response(
                    &StorageRpcMetadataCommandCheckpointCandidatesResponse { checkpoints },
                )?;
                encode_metadata_command_checkpoint_success_response(
                    "metadata command checkpoint candidates",
                    &payload,
                    STORAGE_RPC_MAX_PAYLOAD_LEN,
                )?
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_transfer_state_adopt_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandTransferAdoptRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.adopt_metadata_transfer_state_from_rebased_commands(
                request.node_id.as_u32(),
                request.cluster_epoch,
                &request.commands,
                request.expected_state_digest,
            )
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_transfer_empty_state_initialize_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandTransferEmptyStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.initialize_metadata_transfer_empty_state(
                request.node_id.as_u32(),
                request.cluster_epoch,
                request.expected_state_digest,
            )
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_transfer_matching_state_initialize_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandTransferMatchingStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.initialize_metadata_transfer_matching_state(
                request.node_id.as_u32(),
                request.cluster_epoch,
                request.applied_log_index,
                request.applied_log_hash,
                request.expected_state_digest,
            )
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_transfer_checkpoint_base_install_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandTransferCheckpointBaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_with_allowed_states(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &[PgState::Peering],
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.install_metadata_transfer_checkpoint_base(
                request.node_id.as_u32(),
                request.cluster_epoch,
                &request.checkpoint,
            )
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_response(
                    &StorageRpcMetadataCommandStateResponse { state },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_applied_hashes_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.applied_metadata_command_log_entry_hashes(
                self.config.node_id.as_u32(),
                &request.command,
            )
        }) {
            Ok(hashes) => {
                let payload = encode_metadata_command_applied_hashes_response(
                    &StorageRpcMetadataCommandAppliedHashesResponse {
                        outcome: StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_applied_hashes_response(
                    &StorageRpcMetadataCommandAppliedHashesResponse {
                        outcome: StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_matching_applied_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandMatchingAppliedRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.has_matching_applied_metadata_command_log_entry(
                self.config.node_id.as_u32(),
                &request.command,
                request.expected_previous_log_hash,
            )
        }) {
            Ok(value) => {
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::Value(value),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_abandoned_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.metadata_command_abandoned(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(value) => {
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::Value(value),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_record_abandoned_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.record_metadata_command_abandoned(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(state) => {
                let payload = encode_metadata_command_state_outcome_response(
                    &StorageRpcMetadataCommandStateOutcomeResponse {
                        outcome: StorageRpcMetadataCommandStateOutcome::State(state),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_state_outcome_response(
                    &StorageRpcMetadataCommandStateOutcomeResponse {
                        outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_apply_and_record_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.metadata_command_apply_and_record_response_with_allowed_states(
            session,
            request,
            &[PgState::Active],
        )
    }

    fn metadata_command_peering_replay_apply_and_record_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.metadata_command_apply_and_record_response_with_allowed_states(
            session,
            request,
            &[PgState::Peering],
        )
    }

    fn metadata_command_apply_and_record_response_with_allowed_states(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
        allowed_states: &[PgState],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_with_allowed_states(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            allowed_states,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => match pg
                .apply_metadata_command_and_record(self.config.node_id.as_u32(), &request.command)
            {
                Ok(state) => {
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome: StorageRpcMetadataCommandStateOutcome::State(state),
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                })) => {
                    emit_storage_node_metadata_command_log_conflict(
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                        Some(request.command.payload().kind_name()),
                    );
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                                node_id,
                                pg_id,
                                cluster_epoch,
                                log_index,
                            },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Store(error)) => {
                    encode_storage_rpc_error_response(&store_error_response(error))?
                }
                Err(BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::ObjectGenerationReservationConflict {
                        reservation_id,
                        generation_id,
                    },
                )) => {
                    let reservation_id = SessionId::try_from(reservation_id).map_err(|_| {
                        crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "stored reservation id is invalid",
                        )
                    })?;
                    let generation_id = GenerationId::new(generation_id).ok_or(
                        crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "stored generation id is invalid",
                        ),
                    )?;
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome: StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                                reservation_id,
                                generation_id,
                            },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::ObjectVersionReservationConflict { version_id },
                )) => {
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome:
                                StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                                    version_id,
                                },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::StaleBucketMetadataCommand {
                        name,
                        bucket_execution_generation,
                    },
                )) => {
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome:
                                StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                                    name,
                                    bucket_execution_generation,
                                },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::StaleObjectWriteCommand {
                        bucket,
                        key,
                        write_sequence,
                        generation_id,
                    },
                )) => {
                    let generation_id = generation_id
                        .map(|generation_id| {
                            GenerationId::new(generation_id).ok_or(
                                crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                                    "stored stale object generation id is invalid",
                                ),
                            )
                        })
                        .transpose()?;
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome:
                                StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                                    bucket,
                                    key,
                                    write_sequence,
                                    generation_id,
                                },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::StreamSegmentConflict { segment_index },
                )) => {
                    let payload = encode_metadata_command_state_outcome_response(
                        &StorageRpcMetadataCommandStateOutcomeResponse {
                            outcome: StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict {
                                segment_index,
                            },
                        },
                    );
                    encode_storage_rpc_success_response(&payload)
                }
                Err(BucketSnapshotLoadError::Metadata(error)) => {
                    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::Internal,
                        message: error.to_string(),
                    })?
                }
            },
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_acceptance_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.metadata_command_acceptance(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(acceptance) => {
                let payload = encode_metadata_command_acceptance_response(
                    &StorageRpcMetadataCommandAcceptanceResponse {
                        outcome: StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_acceptance_response(
                    &StorageRpcMetadataCommandAcceptanceResponse {
                        outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_abandon_acceptance_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.metadata_command_abandon_acceptance(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(acceptance) => {
                let payload = encode_metadata_command_acceptance_response(
                    &StorageRpcMetadataCommandAcceptanceResponse {
                        outcome: StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_acceptance_response(
                    &StorageRpcMetadataCommandAcceptanceResponse {
                        outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_pending_slot_insert_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandPendingSlotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Some(scope_bucket) = request.scope_bucket.as_ref() {
            if scope_bucket != request.command.bucket_name() {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message:
                        "metadata command pending slot scope bucket does not match command bucket"
                            .to_string(),
                });
            }
        }
        let canonical_scope_bucket = request.command.bucket_name().clone();
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.try_insert_pending_metadata_command_slot(
                self.config.node_id.as_u32(),
                &request.command,
                Some(&canonical_scope_bucket),
            )
        }) {
            Ok(()) => {
                let payload = encode_metadata_command_pending_slot_insert_response(
                    &StorageRpcMetadataCommandPendingSlotInsertResponse {
                        outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted,
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandPendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            }) => {
                emit_storage_node_metadata_command_pending_conflict(
                    self.config.node_id.as_u32(),
                    pg_id,
                    cluster_epoch,
                    candidate_log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_pending_slot_insert_response(
                    &StorageRpcMetadataCommandPendingSlotInsertResponse {
                        outcome:
                            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                                pg_id,
                                cluster_epoch,
                                existing_log_index,
                                candidate_log_index,
                            },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_pending_slot_insert_response(
                    &StorageRpcMetadataCommandPendingSlotInsertResponse {
                        outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_bucket_control_pending_slot_insert_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandPendingSlotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let Some(scope_bucket) = request.scope_bucket.as_ref() else {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command bucket-control pending slot requires a scope bucket"
                    .to_string(),
            });
        };
        if scope_bucket != request.command.bucket_name() {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message:
                    "metadata command bucket-control scope bucket does not match command bucket"
                        .to_string(),
            });
        }
        let canonical_scope_bucket = request.command.bucket_name().clone();
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            let inserted = pg.try_insert_bucket_control_pending_metadata_command_slot(
                self.config.node_id.as_u32(),
                &request.command,
                &canonical_scope_bucket,
            )?;
            if inserted {
                return Ok(true);
            }
            let exact_pending = pg
                .pending_metadata_command_slot(
                    self.config.node_id.as_u32(),
                    request.command.id().cluster_epoch(),
                )?
                .is_some_and(|slot| {
                    slot.id == request.command.id()
                        && slot.command_checksum == request.command.checksum_crc64()
                        && slot.command_bytes == request.command.command_bytes()
                        && slot.scope_bucket.as_ref() == Some(&canonical_scope_bucket)
                });
            Ok(exact_pending)
        }) {
            Ok(value) => {
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::Value(value),
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }) => {
                emit_storage_node_metadata_command_log_conflict(
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                    Some(request.command.payload().kind_name()),
                );
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse {
                        outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_pending_slot_remove_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.remove_pending_metadata_command_slot(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(removed) => {
                let payload = encode_metadata_command_pending_slot_remove_response(
                    &StorageRpcMetadataCommandPendingSlotRemoveResponse { removed },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn cluster_map_history_reference_summary_response(
        &self,
        request: StorageRpcClusterMapHistoryReferenceSummaryRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_node_epoch(request.node_id, request.cluster_epoch) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.node.cluster_map_history_reference_summary() {
            Ok(summary) => {
                let payload = encode_cluster_map_history_reference_summary_response(
                    &StorageRpcClusterMapHistoryReferenceSummaryResponse { summary },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn validate_node_epoch(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "request cluster epoch {cluster_epoch} does not match storage node cluster epoch {}",
                    self.config.cluster_epoch
                ),
            });
        }
        Ok(())
    }

    fn metadata_command_pending_slot_replace_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandPendingSlotReplaceRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Some(scope_bucket) = request.scope_bucket.as_ref() {
            if scope_bucket != request.replacement.bucket_name() {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message:
                        "metadata command replacement scope bucket does not match command bucket"
                            .to_string(),
                });
            }
        }
        let canonical_scope_bucket = request
            .scope_bucket
            .as_ref()
            .map(|_| request.replacement.bucket_name().clone());
        let _pg_guard = self.metadata_command_pg_guard(session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.replace_pending_metadata_command_slot_for_reissue(
                self.config.node_id.as_u32(),
                &request.previous,
                &request.replacement,
                canonical_scope_bucket.as_ref(),
            )
        }) {
            Ok(removed) => {
                let payload = encode_metadata_command_pending_slot_remove_response(
                    &StorageRpcMetadataCommandPendingSlotRemoveResponse { removed },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_pg_lock_acquire_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) =
            self.validate_primary_pg(request.pg_id, "metadata command critical section")
        {
            return encode_storage_rpc_error_response(&error);
        }
        session.acquire_metadata_command_pg_lock(
            &self.metadata_command_locks,
            self.config.node_id,
            request.pg_id,
            session.current_rpc_context(),
        );
        Ok(encode_storage_rpc_success_response(&[]))
    }

    fn metadata_command_pg_lock_release_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        session.release_metadata_command_pg_lock(request.pg_id);
        Ok(encode_storage_rpc_success_response(&[]))
    }

    fn validate_shard_locations(
        &self,
        locations: &[ShardLocation],
    ) -> Result<(), StorageRpcErrorResponse> {
        for &location in locations {
            self.validate_shard_location(location)?;
        }
        Ok(())
    }

    fn try_begin_shard_delete(
        &self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<StorageNodeShardDeleteFence, StorageRpcErrorResponse> {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_begin_delete(location, shard_key)?;
        Ok(StorageNodeShardDeleteFence {
            read_handles: Arc::clone(&self.read_handles),
            location,
            shard_key: shard_key.clone(),
        })
    }

    fn validate_shard_location(
        &self,
        location: ShardLocation,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route(
            location.node_id(),
            location.cluster_epoch(),
            PgId::new(location.data_pg_id().get()),
        )
    }

    fn validate_shard_location_for_cleanup(
        &self,
        location: ShardLocation,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_for_cleanup(
            location.node_id(),
            location.cluster_epoch(),
            PgId::new(location.data_pg_id().get()),
        )
    }

    fn validate_shard_location_for_historical_inspection(
        &self,
        location: ShardLocation,
    ) -> Result<(), StorageRpcErrorResponse> {
        if location.node_id() != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    location.node_id().as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let pg_id = location.data_pg_id().get();
        if !self.config.pg_ids.contains(&pg_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {pg_id} is not configured on this storage node"),
            });
        }
        Ok(())
    }

    fn validate_pg_route(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_with_allowed_states(
            node_id,
            cluster_epoch,
            pg_id,
            &[PgState::Active],
        )
    }

    fn validate_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_current_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id)
    }

    fn validate_pg_route_for_metadata_transfer_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if cluster_epoch < self.config.cluster_epoch {
            return self.validate_historical_pg_route_for_peering_inspection(
                node_id,
                cluster_epoch,
                pg_id,
            );
        }
        self.validate_current_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id)
    }

    fn validate_current_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_with_allowed_states(
            node_id,
            cluster_epoch,
            pg_id,
            &[PgState::Active, PgState::Peering],
        )
    }

    fn validate_historical_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let raw_pg_id = pg_id.get();
        let Some(route) = self
            .config
            .historical_pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == cluster_epoch)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} route for historical epoch {} is not retained",
                    cluster_epoch.get()
                ),
            });
        };
        if route.state != PgState::Peering {
            let code = if route.state == PgState::Active {
                StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
            } else {
                StorageRpcErrorCode::InactivePgRoute
            };
            return Err(StorageRpcErrorResponse {
                code,
                message: format!(
                    "historical peering inspection for PG {raw_pg_id} at epoch {} requires Peering route, got {}",
                    cluster_epoch.get(),
                    route.state
                ),
            });
        }
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_route_for_metadata_log_read(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if cluster_epoch == self.config.cluster_epoch {
            return self.validate_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id);
        }
        if cluster_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} is newer than storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let now_ms = crate::clock::current_time_millis();
        if let Some(valid_until_ms) = self.config.route_map_valid_until_ms {
            if valid_until_ms <= now_ms {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: format!(
                        "storage-node route map for cluster epoch {} expired at {valid_until_ms}ms, now {now_ms}ms",
                        self.config.cluster_epoch.get()
                    ),
                });
            }
        }
        let raw_pg_id = pg_id.get();
        if let Some(route) = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id)
        {
            if route.cluster_epoch != self.config.cluster_epoch {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::WrongClusterEpoch,
                    message: format!(
                        "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                        route.cluster_epoch.get(),
                        self.config.cluster_epoch.get()
                    ),
                });
            }
            if route.state == PgState::Peering && route.acting_set.contains(&self.config.node_id) {
                return Ok(());
            }
        }
        if self.config.historical_pg_routes.iter().any(|route| {
            route.pg_id == raw_pg_id
                && route.cluster_epoch >= cluster_epoch
                && route.state == PgState::Peering
                && route.acting_set.contains(&self.config.node_id)
        }) {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "PG {raw_pg_id} has no retained Peering route covering metadata log epoch {}",
                cluster_epoch.get()
            ),
        })
    }

    fn validate_pg_route_with_allowed_states(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        allowed_states: &[PgState],
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} does not match storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        let now_ms = crate::clock::current_time_millis();
        if let Some(valid_until_ms) = self.config.route_map_valid_until_ms {
            if valid_until_ms <= now_ms {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: format!(
                        "storage-node route map for cluster epoch {} expired at {valid_until_ms}ms, now {now_ms}ms",
                        self.config.cluster_epoch.get()
                    ),
                });
            }
        }
        let raw_pg_id = pg_id.get();
        let Some(route) = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {raw_pg_id} is not configured on this storage node"),
            });
        };
        if route.cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::WrongClusterEpoch,
                message: format!(
                    "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                    route.cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if !allowed_states.contains(&route.state) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!("PG {raw_pg_id} route is {}", route.state),
            });
        }
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_route_for_cleanup(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.cleanup_pg_route(node_id, cluster_epoch, pg_id)
            .map(|_| ())
    }

    fn cleanup_pg_route(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&StorageNodePgRoute, StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let raw_pg_id = pg_id.get();
        if !self.config.pg_ids.contains(&raw_pg_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {raw_pg_id} is not configured on this storage node"),
            });
        }
        if cluster_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} is newer than storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        let route = if cluster_epoch == self.config.cluster_epoch {
            let Some(route) = self
                .config
                .pg_routes
                .iter()
                .find(|route| route.pg_id == raw_pg_id)
            else {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::UnknownPg,
                    message: format!("PG {raw_pg_id} is not configured on this storage node"),
                });
            };
            if route.cluster_epoch != self.config.cluster_epoch {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::WrongClusterEpoch,
                    message: format!(
                        "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                        route.cluster_epoch.get(),
                        self.config.cluster_epoch.get()
                    ),
                });
            }
            route
        } else {
            let Some(route) = self
                .config
                .historical_pg_routes
                .iter()
                .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == cluster_epoch)
            else {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: format!(
                        "PG {raw_pg_id} route for cleanup epoch {} is not retained",
                        cluster_epoch.get()
                    ),
                });
            };
            route
        };
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(route)
    }

    fn validate_primary_pg_for_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &crate::ObjectKey,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated object metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        self.validate_pg_for_object(pg_id, bucket, key, operation)
    }

    fn validate_object_mutation_command_request(
        &self,
        object: &StorageRpcObjectRequest,
        bucket_write_reservation: &crate::metadata_command::BucketWriteReservationProof,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        if object.bucket != bucket_write_reservation.bucket {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} bucket write reservation proof does not match object bucket"
                ),
            });
        }
        self.validate_pg_route(object.node_id, object.cluster_epoch, object.pg_id)?;
        self.validate_primary_pg_for_object(object.pg_id, &object.bucket, &object.key, operation)
    }

    fn validate_pg_for_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &crate::ObjectKey,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let expected_pg_id = PgId::new(self.node.pg_topology().object_pg_for(bucket, key));
        if pg_id != expected_pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match object {}/{} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    key.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let expected_pg_id = PgId::new(self.node.pg_topology().bucket_pg_for(bucket));
        if pg_id != expected_pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match bucket {} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_primary_pg(
        &self,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_primary_pg_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated bucket metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        let expected_pg_id = PgId::new(self.node.pg_topology().bucket_pg_for(bucket));
        if pg_id != expected_pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match bucket {} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_lifecycle_sweep_claim_route(
        &self,
        request: &StorageRpcLifecycleSweepClaimRecordRequest,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        if request.claim.pg_id != request.pg_id.get() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} request PG {} does not match claim PG {}",
                    request.pg_id.get(),
                    request.claim.pg_id
                ),
            });
        }
        self.validate_primary_pg_for_bucket(request.pg_id, &request.claim.bucket, operation)
    }

    fn validate_lifecycle_sweep_claim_cleanup_route(
        &self,
        request: &StorageRpcLifecycleSweepClaimRecordRequest,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_for_cleanup(request.node_id, request.cluster_epoch, request.pg_id)?;
        if request.claim.pg_id != request.pg_id.get() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} request PG {} does not match claim PG {}",
                    request.pg_id.get(),
                    request.claim.pg_id
                ),
            });
        }
        self.validate_primary_pg_for_bucket(request.pg_id, &request.claim.bucket, operation)
    }

    fn validate_bucket_metadata_control_route(
        &self,
        request: &StorageRpcBucketRequest,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        self.validate_primary_pg_for_bucket(request.pg_id, &request.bucket, operation)
    }

    fn validate_bucket_batch_route(
        &self,
        request: &StorageRpcBucketBatchRequest,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        self.validate_primary_pg(request.pg_id, operation)?;
        for bucket in &request.buckets {
            self.validate_pg_for_bucket(request.pg_id, bucket, operation)?;
        }
        Ok(())
    }

    fn unsupported_operation_response(
        &self,
        kind: StorageRpcMessageKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: format!("{kind:?} is not implemented by this storage-node server slice"),
        })
    }
}

fn trace_storage_rpc_lifecycle(kind: StorageRpcMessageKind) -> bool {
    matches!(
        kind,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope
            | StorageRpcMessageKind::MetadataCommandPendingSlotInsert
            | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
            | StorageRpcMessageKind::MetadataCommandPendingSlotReplace
            | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
    )
}

#[derive(Debug, Default)]
struct StorageNodeActiveSessionState {
    active: usize,
}

#[derive(Debug, Default)]
struct StorageNodeActiveSessions {
    state: Mutex<StorageNodeActiveSessionState>,
    available: Condvar,
}

impl StorageNodeActiveSessions {
    fn acquire(self: &Arc<Self>, limit: usize) -> StorageNodeActiveSessionGuard {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while !state.try_acquire(limit) {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        StorageNodeActiveSessionGuard {
            active_sessions: Arc::clone(self),
        }
    }

    #[cfg(test)]
    fn try_acquire(self: &Arc<Self>, limit: usize) -> Option<StorageNodeActiveSessionGuard> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.try_acquire(limit) {
            Some(StorageNodeActiveSessionGuard {
                active_sessions: Arc::clone(self),
            })
        } else {
            None
        }
    }

    fn release(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release();
        self.available.notify_one();
    }
}

impl StorageNodeActiveSessionState {
    fn try_acquire(&mut self, limit: usize) -> bool {
        if self.active >= limit {
            return false;
        }
        self.active += 1;
        true
    }

    fn release(&mut self) {
        self.active = self
            .active
            .checked_sub(1)
            .expect("storage-node active session release without acquire");
    }
}

struct StorageNodeActiveSessionGuard {
    active_sessions: Arc<StorageNodeActiveSessions>,
}

impl Drop for StorageNodeActiveSessionGuard {
    fn drop(&mut self) {
        self.active_sessions.release();
    }
}

#[derive(Debug, Default)]
struct StorageNodeReadHandleState {
    handle_counts: BTreeMap<ReadHandleShardKey, usize>,
    delete_fences: BTreeSet<ReadHandleShardKey>,
    live_read_operations: usize,
    live_read_handle_locations: usize,
}

impl StorageNodeReadHandleState {
    fn try_acquire(
        &mut self,
        entries: &[(ShardLocation, ShardKey)],
    ) -> Result<(), StorageRpcErrorResponse> {
        if self.live_read_operations >= STORAGE_NODE_MAX_LIVE_READ_OPERATIONS {
            return Err(resource_exhausted_response(format!(
                "storage-node live read operation limit {} is exhausted",
                STORAGE_NODE_MAX_LIVE_READ_OPERATIONS
            )));
        }
        let live_read_handle_locations = self
            .live_read_handle_locations
            .checked_add(entries.len())
            .ok_or_else(|| {
                resource_exhausted_response(
                    "storage-node live read handle location counter overflowed".to_string(),
                )
            })?;
        if live_read_handle_locations > STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS {
            return Err(resource_exhausted_response(format!(
                "storage-node live read handle location limit {} is exhausted",
                STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS
            )));
        }
        for (location, shard_key) in entries {
            let key = ReadHandleShardKey::new(*location, shard_key);
            if self.delete_fences.contains(&key) {
                return Err(shard_delete_in_progress_response(format!(
                    "shard {:?} at {:?} is being deleted",
                    shard_key, location
                )));
            }
        }
        for (location, shard_key) in entries {
            *self
                .handle_counts
                .entry(ReadHandleShardKey::new(*location, shard_key))
                .or_insert(0) += 1;
        }
        self.live_read_operations += 1;
        self.live_read_handle_locations = live_read_handle_locations;
        Ok(())
    }

    fn release(&mut self, entries: &[(ShardLocation, ShardKey)]) {
        self.live_read_operations = self
            .live_read_operations
            .checked_sub(1)
            .expect("read handle operation release without acquire");
        self.live_read_handle_locations = self
            .live_read_handle_locations
            .checked_sub(entries.len())
            .expect("read handle location release without acquire");
        for (location, shard_key) in entries {
            let key = ReadHandleShardKey::new(*location, shard_key);
            let entry = self
                .handle_counts
                .get_mut(&key)
                .expect("read handle release without acquire");
            *entry -= 1;
            if *entry == 0 {
                self.handle_counts.remove(&key);
            }
        }
    }

    #[cfg(test)]
    fn count(&self, location: ShardLocation) -> usize {
        let location_key = ShardLocationKey::from(location);
        self.handle_counts
            .iter()
            .filter(|(key, _)| key.location == location_key)
            .map(|(_, count)| *count)
            .sum()
    }

    fn try_begin_delete(
        &mut self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<(), StorageRpcErrorResponse> {
        let key = ReadHandleShardKey::new(location, shard_key);
        if self.handle_counts.get(&key).copied().unwrap_or(0) > 0 {
            return Err(resource_exhausted_response(format!(
                "shard {:?} at {:?} has active read handles",
                shard_key, location
            )));
        }
        if !self.delete_fences.insert(key) {
            return Err(resource_exhausted_response(format!(
                "shard {:?} at {:?} is already being deleted",
                shard_key, location
            )));
        }
        Ok(())
    }

    fn finish_delete(&mut self, location: ShardLocation, shard_key: &ShardKey) {
        self.delete_fences
            .remove(&ReadHandleShardKey::new(location, shard_key));
    }
}

struct StorageNodeShardDeleteFence {
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    location: ShardLocation,
    shard_key: ShardKey,
}

impl Drop for StorageNodeShardDeleteFence {
    fn drop(&mut self) {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish_delete(self.location, &self.shard_key);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShardLocationKey {
    cluster_epoch: u64,
    data_pg_id: u32,
    shard_index: u8,
    node_id: u32,
}

impl From<ShardLocation> for ShardLocationKey {
    fn from(location: ShardLocation) -> Self {
        Self {
            cluster_epoch: location.cluster_epoch().get(),
            data_pg_id: location.data_pg_id().get(),
            shard_index: location.shard_index().get(),
            node_id: location.node_id().as_u32(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadHandleShardKey {
    location: ShardLocationKey,
    shard_key: ShardKey,
}

impl ReadHandleShardKey {
    fn new(location: ShardLocation, shard_key: &ShardKey) -> Self {
        Self {
            location: ShardLocationKey::from(location),
            shard_key: shard_key.clone(),
        }
    }
}

impl PartialOrd for ReadHandleShardKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadHandleShardKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.location
            .cmp(&other.location)
            .then_with(|| self.shard_key.as_bytes().cmp(other.shard_key.as_bytes()))
    }
}

struct StorageNodeSession<'a> {
    shared_handles: &'a Mutex<StorageNodeReadHandleState>,
    read_operations: BTreeMap<String, SessionReadHandle>,
    metadata_command_guards: BTreeMap<PgId, StorageNodeMetadataCommandGuard>,
    current_rpc_context: Option<StorageNodeMetadataCommandLockContext>,
}

impl<'a> StorageNodeSession<'a> {
    fn new(shared_handles: &'a Mutex<StorageNodeReadHandleState>) -> Self {
        Self {
            shared_handles,
            read_operations: BTreeMap::new(),
            metadata_command_guards: BTreeMap::new(),
            current_rpc_context: None,
        }
    }

    fn set_current_rpc_context(&mut self, request_id: u64, kind: StorageRpcMessageKind) {
        self.current_rpc_context = Some(StorageNodeMetadataCommandLockContext { request_id, kind });
    }

    fn current_rpc_context(&self) -> Option<StorageNodeMetadataCommandLockContext> {
        self.current_rpc_context
    }

    fn holds_metadata_command_pg_lock(&self, pg_id: PgId) -> bool {
        self.metadata_command_guards.contains_key(&pg_id)
    }

    fn update_metadata_command_lock_context(
        &self,
        locks: &StorageNodeMetadataCommandLocks,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) {
        for &pg_id in self.metadata_command_guards.keys() {
            locks.update_context(pg_id, context);
        }
    }

    fn clear_metadata_command_lock_context(&self, locks: &StorageNodeMetadataCommandLocks) {
        self.update_metadata_command_lock_context(locks, None);
    }

    fn acquire_metadata_command_pg_lock(
        &mut self,
        locks: &StorageNodeMetadataCommandLocks,
        node_id: NodeId,
        pg_id: PgId,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) {
        if self.metadata_command_guards.contains_key(&pg_id) {
            return;
        }
        let guard = locks.acquire(node_id, pg_id, context);
        self.metadata_command_guards.insert(pg_id, guard);
    }

    fn release_metadata_command_pg_lock(&mut self, pg_id: PgId) {
        self.metadata_command_guards.remove(&pg_id);
    }

    fn acquire_read_handles(
        &mut self,
        request: StorageRpcReadHandleAcquireRequest,
    ) -> Result<Vec<ShardLocation>, StorageRpcErrorResponse> {
        let entries: Vec<(ShardLocation, ShardKey)> = request
            .locations
            .iter()
            .copied()
            .zip(request.shard_keys.iter().cloned())
            .collect();
        match self.read_operations.get(&request.read_operation_id) {
            Some(existing) if existing.entries == entries && existing.is_acquired => {
                return Ok(existing
                    .entries
                    .iter()
                    .map(|(location, _)| *location)
                    .collect());
            }
            Some(_) => {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::Internal,
                    message: format!(
                        "read operation {} was already acquired with different shard locations",
                        request.read_operation_id
                    ),
                });
            }
            None => {}
        }
        if self.read_operations.len() >= STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION {
            return Err(resource_exhausted_response(format!(
                "storage-node session read operation limit {} is exhausted",
                STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION
            )));
        }

        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_acquire(&entries)?;
        self.read_operations.insert(
            request.read_operation_id,
            SessionReadHandle {
                entries,
                is_acquired: true,
            },
        );
        Ok(request.locations)
    }

    fn release_read_handles(&mut self, read_operation_id: &str) {
        let Some(existing) = self.read_operations.remove(read_operation_id) else {
            return;
        };
        if !existing.is_acquired {
            return;
        }
        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release(&existing.entries);
    }
}

impl Drop for StorageNodeSession<'_> {
    fn drop(&mut self) {
        let mut shared_handles = self
            .shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for existing in self.read_operations.values_mut() {
            if existing.is_acquired {
                shared_handles.release(&existing.entries);
                existing.is_acquired = false;
            }
        }
    }
}

#[derive(Debug)]
struct SessionReadHandle {
    entries: Vec<(ShardLocation, ShardKey)>,
    is_acquired: bool,
}

fn rpc_stream_error(error: StorageRpcStreamError) -> StorageNodeServerError {
    StorageNodeServerError::RpcStream {
        message: error.to_string(),
    }
}

fn resource_exhausted_response(message: String) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ResourceExhausted,
        message,
    }
}

fn shard_delete_in_progress_response(message: String) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ShardDeleteInProgress,
        message,
    }
}

fn validate_placed_segment_shard_repair_claim_route_epoch(
    pg_id: PgId,
    route_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StoreError> {
    if claim.cluster_epoch != route_epoch {
        return Err(StoreError::StalePayloadOperation {
            pg_id: pg_id.get(),
            operation_epoch: claim.cluster_epoch,
            current_epoch: route_epoch,
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_route_epoch(
    pg_id: PgId,
    route_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) -> Result<(), StoreError> {
    if claim.cluster_epoch != route_epoch {
        return Err(StoreError::StalePayloadOperation {
            pg_id: pg_id.get(),
            operation_epoch: claim.cluster_epoch,
            current_epoch: route_epoch,
        });
    }
    Ok(())
}

fn store_error_response(error: StoreError) -> StorageRpcErrorResponse {
    match error {
        StoreError::NotFound => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::NotFound,
            message: "not found".to_string(),
        },
        StoreError::MetadataCommandContention { context } => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: format!("metadata command contention during {context}"),
        },
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn bucket_snapshot_error_response(error: BucketSnapshotLoadError) -> StorageRpcErrorResponse {
    match error {
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimNotFound { claim_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ReclaimClaimNotFound,
                message: claim_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict { claim_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ReclaimClaimConflict,
                message: claim_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id,
        }) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::BucketWriteReservationConflict,
            message: reservation_id,
        },
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationNotFound {
            reservation_id,
        }) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::BucketWriteReservationNotFound,
            message: reservation_id,
        },
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention { context }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: format!("metadata command contention during {context}"),
            }
        }
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn bucket_write_drain_heartbeat_error_response(
    error: BucketSnapshotLoadError,
) -> StorageRpcErrorResponse {
    match error {
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDrainConflict { drain_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::BucketWriteDrainConflict,
                message: drain_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDrainNotFound { drain_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::BucketWriteDrainNotFound,
                message: drain_id,
            }
        }
        error => bucket_snapshot_error_response(error),
    }
}

fn bucket_write_drain_error_response(error: BucketWriteDrainError) -> StorageRpcErrorResponse {
    match error {
        BucketWriteDrainError::Store(StoreError::MetadataCommandContention { context }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: format!("metadata command contention during {context}"),
            }
        }
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn object_pg_error_response(error: ObjectPgActionError) -> StorageRpcErrorResponse {
    match error {
        ObjectPgActionError::Store(StoreError::MetadataCommandContention { context }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: format!("metadata command contention during {context}"),
            }
        }
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn object_metadata_command_build_error_outcome(
    error: ObjectPgActionError,
    command_kind: Option<&'static str>,
) -> Result<StorageRpcObjectMetadataCommandBuildOutcome, StorageRpcErrorResponse> {
    match error {
        ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        }) => {
            emit_storage_node_metadata_command_log_conflict(
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
                command_kind,
            );
            Ok(StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            })
        }
        error => Err(object_pg_error_response(error)),
    }
}

impl Drop for StorageNodeServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.config_snapshot().socket_path);
    }
}

pub fn advance_storage_node_incarnation(data_dir: &Path) -> Result<u64, StorageNodeServerError> {
    fs::create_dir_all(data_dir).map_err(|source| StorageNodeServerError::Io {
        context: "create storage-node data directory for incarnation",
        path: data_dir.to_path_buf(),
        source,
    })?;
    let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
    let current = match fs::read_to_string(&path) {
        Ok(contents) => parse_storage_node_incarnation(&path, &contents)?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(StorageNodeServerError::Io {
                context: "read storage-node incarnation",
                path,
                source,
            });
        }
    };
    let next = current
        .checked_add(1)
        .filter(|value| *value != 0)
        .ok_or_else(|| StorageNodeServerError::NodeIncarnationOverflow { path: path.clone() })?;
    persist_storage_node_incarnation(data_dir, &path, next)?;
    Ok(next)
}

fn parse_storage_node_incarnation(
    path: &Path,
    contents: &str,
) -> Result<u64, StorageNodeServerError> {
    let trimmed = contents.trim();
    let incarnation =
        trimmed
            .parse::<u64>()
            .map_err(|_| StorageNodeServerError::InvalidNodeIncarnation {
                path: path.to_path_buf(),
                value: contents.to_owned(),
            })?;
    if incarnation == 0 {
        return Err(StorageNodeServerError::InvalidNodeIncarnation {
            path: path.to_path_buf(),
            value: contents.to_owned(),
        });
    }
    Ok(incarnation)
}

fn persist_storage_node_incarnation(
    data_dir: &Path,
    path: &Path,
    incarnation: u64,
) -> Result<(), StorageNodeServerError> {
    let tmp_path = data_dir.join(STORAGE_NODE_INCARNATION_TMP_FILE);
    {
        let mut tmp_file =
            File::create(&tmp_path).map_err(|source| StorageNodeServerError::Io {
                context: "create storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
        tmp_file
            .write_all(format!("{incarnation}\n").as_bytes())
            .map_err(|source| StorageNodeServerError::Io {
                context: "write storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
        tmp_file
            .sync_all()
            .map_err(|source| StorageNodeServerError::Io {
                context: "sync storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
    }
    fs::rename(&tmp_path, path).map_err(|source| StorageNodeServerError::Io {
        context: "commit storage-node incarnation",
        path: path.to_path_buf(),
        source,
    })?;
    File::open(data_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StorageNodeServerError::Io {
            context: "sync storage-node incarnation directory",
            path: data_dir.to_path_buf(),
            source,
        })?;
    Ok(())
}

struct StorageNodeDataDirLock {
    _file: File,
}

impl StorageNodeDataDirLock {
    fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        prepare_private_data_dir(data_dir).map_err(|source| StorageNodeServerError::Io {
            context: "prepare private storage-node data directory",
            path: data_dir.to_path_buf(),
            source,
        })?;
        let path = data_dir.join(DATA_DIR_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| StorageNodeServerError::Io {
                context: "open storage-node data-dir lock",
                path: path.clone(),
                source,
            })?;
        // SAFETY: flock operates on a valid file descriptor owned by `file`.
        // The descriptor remains open for the lifetime of StorageNodeDataDirLock.
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if rc != 0 {
            let source = io::Error::last_os_error();
            return if source.kind() == io::ErrorKind::WouldBlock {
                Err(StorageNodeServerError::DataDirAlreadyLocked {
                    path: data_dir.to_path_buf(),
                })
            } else {
                Err(StorageNodeServerError::Io {
                    context: "lock storage-node data directory",
                    path,
                    source,
                })
            };
        }
        Ok(Self { _file: file })
    }
}

impl std::fmt::Debug for StorageNodeDataDirLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNodeDataDirLock")
            .finish_non_exhaustive()
    }
}

impl Drop for StorageNodeDataDirLock {
    fn drop(&mut self) {}
}

fn validate_pg_ids(pg_ids: &[u32]) -> Result<(), StorageNodeServerError> {
    if pg_ids.is_empty() {
        return Err(StorageNodeServerError::EmptyPgSet);
    }
    let mut seen = BTreeMap::<u32, ()>::new();
    for &pg_id in pg_ids {
        if seen.insert(pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgId { pg_id });
        }
    }
    Ok(())
}

fn validate_pg_routes(
    pg_ids: &[u32],
    routes: &[StorageNodePgRoute],
) -> Result<(), StorageNodeServerError> {
    let configured: BTreeMap<u32, ()> = pg_ids.iter().map(|&pg_id| (pg_id, ())).collect();
    let mut seen = BTreeMap::<u32, ()>::new();
    for route in routes {
        if seen.insert(route.pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgRoute { pg_id: route.pg_id });
        }
        if !configured.contains_key(&route.pg_id) {
            return Err(StorageNodeServerError::RoutePgNotConfigured { pg_id: route.pg_id });
        }
        if !route.acting_set.contains(&route.primary_node_id) {
            return Err(StorageNodeServerError::RoutePrimaryNotInActingSet {
                pg_id: route.pg_id,
                primary_node_id: route.primary_node_id.as_u32(),
            });
        }
    }
    for &pg_id in pg_ids {
        if !seen.contains_key(&pg_id) {
            return Err(StorageNodeServerError::MissingPgRoute { pg_id });
        }
    }
    Ok(())
}

fn validate_process_config_route_table(
    config: &StorageNodeProcessConfig,
) -> Result<(), StorageNodeServerError> {
    validate_pg_ids(&config.pg_ids)?;
    validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
    for route in &config.pg_routes {
        if route.cluster_epoch != config.cluster_epoch {
            return Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: route.pg_id,
                route_epoch: route.cluster_epoch,
                config_epoch: config.cluster_epoch,
            });
        }
    }
    Ok(())
}

fn route_map_validity_regressed(current: Option<u64>, candidate: Option<u64>) -> bool {
    match (current, candidate) {
        (Some(current), Some(candidate)) => candidate < current,
        _ => false,
    }
}

fn validate_socket_directory(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    validate_absolute_socket_path(socket_path)?;
    let parent =
        socket_path
            .parent()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
                path: socket_path.to_path_buf(),
            })?;
    socket_path
        .file_name()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
            path: socket_path.to_path_buf(),
        })?;
    let metadata = fs::metadata(parent).map_err(|source| StorageNodeServerError::Io {
        context: "stat storage-node socket directory",
        path: parent.to_path_buf(),
        source,
    })?;
    let mode = metadata.permissions().mode() & 0o7777;
    if !metadata.is_dir() || mode != 0o700 {
        return Err(StorageNodeServerError::SocketDirectoryNotPrivate {
            path: parent.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

fn canonical_socket_path(path: &Path) -> Result<PathBuf, StorageNodeServerError> {
    validate_absolute_socket_path(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context: "canonicalize storage-node socket directory",
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}

fn cleanup_stale_socket_path(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    let metadata =
        match fs::symlink_metadata(socket_path).map_err(|source| StorageNodeServerError::Io {
            context: "stat storage-node socket path",
            path: socket_path.to_path_buf(),
            source,
        }) {
            Ok(metadata) => metadata,
            Err(StorageNodeServerError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(())
            }
            Err(error) => return Err(error),
        };
    if !metadata.file_type().is_socket() {
        return Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        });
    }
    match UnixStream::connect(socket_path) {
        Ok(_) => Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        }),
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(socket_path).map_err(|source| StorageNodeServerError::Io {
                context: "remove stale storage-node socket",
                path: socket_path.to_path_buf(),
                source,
            })
        }
        Err(source) => Err(StorageNodeServerError::Io {
            context: "connect existing storage-node socket",
            path: socket_path.to_path_buf(),
            source,
        }),
    }
}

fn validate_absolute_socket_path(path: &Path) -> Result<(), StorageNodeServerError> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(StorageNodeServerError::SocketPathNotAbsolute {
            path: path.to_path_buf(),
        })
    }
}

fn canonicalize_existing_or_parent(
    path: &Path,
    context: &'static str,
) -> Result<PathBuf, StorageNodeServerError> {
    if path.exists() {
        return path
            .canonicalize()
            .map_err(|source| StorageNodeServerError::Io {
                context,
                path: path.to_path_buf(),
                source,
            });
    }
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context,
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use s3_types::{AclGrants, BucketObjectLockConfig, BucketVersioningState};

    use crate::control_plane::{
        FileControlPlaneStore, NodeHeartbeat, NodeMembershipState, SingleAuthorityControlPlane,
    };
    use crate::metadata_command::{
        BucketWriteReservationProof, CommitDirectPutObjectCommand, CreateBucketCommand,
        DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
        MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
        MetadataCommandPayload, MetadataTransferCommand, PutBucketAclCommand,
        ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
    };
    use crate::storage_rpc::{
        decode_bucket_mark_deleting_command_build_response, decode_health_response,
        decode_metadata_command_acceptance_response,
        decode_metadata_command_applied_hashes_response,
        decode_metadata_command_bool_outcome_response,
        decode_metadata_command_log_hash_range_response,
        decode_metadata_command_max_log_index_response, decode_metadata_command_next_id_response,
        decode_metadata_command_pending_envelope_response,
        decode_metadata_command_pending_slot_insert_response,
        decode_metadata_command_pending_slot_remove_response,
        decode_metadata_command_state_outcome_response, decode_metadata_command_state_response,
        decode_read_handle_acquire_response, decode_read_handle_release_response,
        decode_scavenger_list_files_response, decode_shard_ack_item_response,
        decode_shard_read_range_response, decode_shard_read_response, decode_shard_write_ack,
        decode_storage_rpc_response_payload, encode_bucket_mark_deleting_command_build_request,
        encode_bucket_pg_request, encode_metadata_command_log_hash_range_request,
        encode_metadata_command_matching_applied_request, encode_metadata_command_next_id_request,
        encode_metadata_command_pending_slot_request, encode_metadata_command_request,
        encode_metadata_command_state_request, encode_metadata_command_transfer_adopt_request,
        encode_metadata_command_transfer_checkpoint_base_request,
        encode_metadata_command_transfer_empty_state_request,
        encode_metadata_command_transfer_matching_state_request,
        encode_read_handle_acquire_request, encode_read_handle_release_request,
        encode_scavenger_list_files_request, encode_scavenger_observation_key_request,
        encode_scavenger_observation_record_request, encode_shard_ack_batch_request,
        encode_shard_ack_item_request, encode_shard_delete_request,
        encode_shard_read_range_request, encode_shard_read_request, encode_shard_write_request,
        encode_storage_rpc_frame, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
        StorageRpcBucketMarkDeletingCommandBuildOutcome,
        StorageRpcBucketMarkDeletingCommandBuildRequest, StorageRpcBucketPgRequest,
        StorageRpcBucketRequest, StorageRpcMetadataCommandAcceptanceOutcome,
        StorageRpcMetadataCommandLogHashRangeRequest,
        StorageRpcMetadataCommandMatchingAppliedRequest, StorageRpcMetadataCommandNextIdRequest,
        StorageRpcMetadataCommandPendingSlotInsertOutcome,
        StorageRpcMetadataCommandPendingSlotRequest, StorageRpcMetadataCommandRequest,
        StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateRequest,
        StorageRpcMetadataCommandTransferAdoptRequest,
        StorageRpcMetadataCommandTransferCheckpointBaseRequest,
        StorageRpcMetadataCommandTransferEmptyStateRequest,
        StorageRpcMetadataCommandTransferMatchingStateRequest, StorageRpcReadHandleAcquireRequest,
        StorageRpcReadHandleReleaseRequest, StorageRpcScavengerListFilesRequest,
        StorageRpcScavengerObservationKeyRequest, StorageRpcScavengerObservationRecordRequest,
        StorageRpcShardAckBatchRequest, StorageRpcShardAckItem, StorageRpcShardAckItemRequest,
        StorageRpcShardDeleteRequest, StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest,
        StorageRpcShardWriteRequest,
    };
    use crate::traits::{PgMetadataStore, ShardStore};
    use crate::types::{
        BucketName, BucketSubresourceAux, BucketSubresourceKind, CreateBucketConfig, DataPgId,
        GenerationId, PgId, PlacedSegmentShardBackfillClaimRecord,
        PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimRecord,
        PlacedSegmentShardRepairWorkItem, PutBucketSubresource, SegmentStoredBytesRequest,
        ShardIndex, ShardKey, ShardScavengerObservationKey, ShardScavengerObservationReason,
        ShardScavengerObservationRecord, VersionId,
    };

    #[test]
    fn placed_segment_shard_repair_claim_route_epoch_must_match_claim_epoch() {
        let route_epoch = ClusterEpoch::new(2).unwrap();
        let claim_epoch = ClusterEpoch::new(1).unwrap();
        let claim = PlacedSegmentShardRepairClaimRecord {
            work_item: PlacedSegmentShardRepairWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: [0xAC; 16],
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: 0x1234,
                    ec: EcShape { k: 4, m: 2 },
                },
                shard_index: ShardIndex::new(5),
            },
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: claim_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        };

        assert!(matches!(
            validate_placed_segment_shard_repair_claim_route_epoch(
                PgId::new(7),
                route_epoch,
                &claim
            ),
            Err(StoreError::StalePayloadOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == claim_epoch && current_epoch == route_epoch
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_claim_route_epoch_must_match_claim_epoch() {
        let route_epoch = ClusterEpoch::new(2).unwrap();
        let claim_epoch = ClusterEpoch::new(1).unwrap();
        let claim = PlacedSegmentShardBackfillClaimRecord {
            work_item: PlacedSegmentShardBackfillWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: [0xAC; 16],
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: 0x1234,
                    ec: EcShape { k: 4, m: 2 },
                },
                source_cluster_epoch: ClusterEpoch::new(1).unwrap(),
                desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            },
            remaining_tolerance: 2,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: claim_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        };

        assert!(matches!(
            validate_placed_segment_shard_backfill_claim_route_epoch(
                PgId::new(7),
                route_epoch,
                &claim
            ),
            Err(StoreError::StalePayloadOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == claim_epoch && current_epoch == route_epoch
        ));
    }

    fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
        StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            route_map_valid_until_ms: None,
            data_dir: tmp.path().join("node"),
            default_ec_shape: EcShape { k: 4, m: 2 },
            pg_ids: vec![0],
            socket_path: tmp.path().join("sock").join("storage.sock"),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            }],
            historical_pg_routes: Vec::new(),
        }
    }

    fn test_route(pg_id: u32) -> StorageNodePgRoute {
        StorageNodePgRoute {
            pg_id,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        }
    }

    #[test]
    fn storage_node_incarnation_advances_and_persists() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");

        assert_eq!(advance_storage_node_incarnation(&data_dir).unwrap(), 1);
        assert_eq!(advance_storage_node_incarnation(&data_dir).unwrap(), 2);
        assert_eq!(
            std::fs::read_to_string(data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "2\n"
        );
        assert!(!data_dir.join(STORAGE_NODE_INCARNATION_TMP_FILE).exists());
    }

    #[test]
    fn storage_node_server_advances_incarnation_while_bound() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        assert_eq!(server.advance_control_plane_node_incarnation().unwrap(), 1);
        assert_eq!(
            std::fs::read_to_string(config.data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "1\n"
        );
    }

    #[test]
    fn storage_node_server_serializes_concurrent_incarnation_advances() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let caller_count = 8;
        let barrier = Arc::new(Barrier::new(caller_count));
        let mut joins = Vec::new();

        for _ in 0..caller_count {
            let server = Arc::clone(&server);
            let barrier = Arc::clone(&barrier);
            joins.push(thread::spawn(move || {
                barrier.wait();
                server.advance_control_plane_node_incarnation().unwrap()
            }));
        }

        let mut incarnations = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        incarnations.sort_unstable();

        assert_eq!(incarnations, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            std::fs::read_to_string(config.data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "8\n"
        );
        assert!(!config
            .data_dir
            .join(STORAGE_NODE_INCARNATION_TMP_FILE)
            .exists());
    }

    #[test]
    fn storage_node_incarnation_rejects_invalid_persisted_value() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&data_dir).unwrap();
        let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
        std::fs::write(&path, "0\n").unwrap();

        assert!(matches!(
            advance_storage_node_incarnation(&data_dir),
            Err(StorageNodeServerError::InvalidNodeIncarnation {
                path: error_path,
                value,
            }) if error_path == path && value == "0\n"
        ));
    }

    #[test]
    fn storage_node_incarnation_rejects_overflow() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&data_dir).unwrap();
        let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
        std::fs::write(&path, format!("{}\n", u64::MAX)).unwrap();

        assert!(matches!(
            advance_storage_node_incarnation(&data_dir),
            Err(StorageNodeServerError::NodeIncarnationOverflow { path: error_path })
                if error_path == path
        ));
    }

    #[test]
    fn storage_node_process_config_preserves_route_map_validity() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = Some(1_500);

        assert_eq!(config.route_map_valid_until_ms(), Some(1_500));
        assert!(config.is_route_map_valid_at(1_499));
        assert!(matches!(
            config.require_route_map_valid_at(1_500),
            Err(StorageNodeServerError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 1_500,
                now_ms: 1_500,
            }) if cluster_epoch == config.cluster_epoch
        ));
    }

    #[test]
    fn storage_node_server_rejects_expired_route_map_for_serving_rpc() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = Some(1);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let error = server
            .connection_handler()
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(error.message.contains("route map"));
        assert!(error.message.contains("expired"));
    }

    #[test]
    fn metadata_command_pg_lock_release_allows_expired_route_map_cleanup() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = Some(1);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let mut session = StorageNodeSession::new(&server.read_handles);
        session.acquire_metadata_command_pg_lock(
            &server.metadata_command_locks,
            config.node_id,
            PgId::new(0),
            None,
        );
        assert!(session.holds_metadata_command_pg_lock(PgId::new(0)));

        let request = StorageRpcMetadataCommandStateRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };
        let response = handler
            .metadata_command_pg_lock_release_response(&mut session, request)
            .unwrap();
        decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();

        assert!(!session.holds_metadata_command_pg_lock(PgId::new(0)));
    }

    #[test]
    fn bucket_write_reservation_release_allows_expired_route_map_cleanup() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("expired-route-reservation-cleanup");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let record = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                &bucket,
                "reservation-expired-route-cleanup",
                "owner-token-expired-route-cleanup",
                config.cluster_epoch,
                "put-object",
                10,
                Some(20),
                Some("key=a"),
            )
            .unwrap()
        };

        config.route_map_valid_until_ms = Some(1);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let serving_error = handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(serving_error.code, StorageRpcErrorCode::StaleShardLocation);

        let request = StorageRpcBucketWriteReservationRecordRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            record: record.clone(),
        };
        let response = handler
            .bucket_write_reservation_release_response(request)
            .unwrap();
        decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();

        let pg = server._node.get_pg(0).unwrap();
        assert!(PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &bucket,
            &record.reservation_id,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn storage_node_runtime_refresh_allows_route_table_changes() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);
        let mut candidate = current.clone();
        candidate.cluster_epoch = ClusterEpoch::new(2).unwrap();
        candidate.route_map_valid_until_ms = Some(3_000);
        candidate.pg_ids = vec![0, 1];
        candidate.pg_routes.push(StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: candidate.cluster_epoch,
            state: PgState::Peering,
            primary_node_id: candidate.node_id,
            acting_set: vec![candidate.node_id],
        });
        candidate.pg_routes[0].cluster_epoch = candidate.cluster_epoch;

        candidate.validate_runtime_refresh_from(&current).unwrap();
    }

    #[test]
    fn storage_node_runtime_refresh_rejects_process_identity_changes() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);

        let mut changed_node = current.clone();
        changed_node.node_id = NodeId::new(8);
        assert!(matches!(
            changed_node.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshNodeChanged {
                current: 7,
                candidate: 8,
            })
        ));

        let mut changed_data = current.clone();
        changed_data.data_dir = tmp.path().join("other-node");
        assert!(matches!(
            changed_data.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshDataDirChanged { .. })
        ));

        let mut changed_ec = current.clone();
        changed_ec.default_ec_shape = EcShape { k: 2, m: 1 };
        assert!(matches!(
            changed_ec.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshEcShapeChanged { .. })
        ));

        let mut changed_socket = current.clone();
        changed_socket.socket_path = tmp.path().join("sock").join("other.sock");
        assert!(matches!(
            changed_socket.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshSocketPathChanged { .. })
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_pg_set_changes() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut candidate = config.clone();
        candidate.pg_ids = vec![0, 1];
        candidate.pg_routes.push(test_route(1));

        assert!(matches!(
            server.install_control_plane_runtime_config(candidate),
            Err(StorageNodeServerError::RuntimeRefreshPgSetChanged {
                current,
                candidate,
            }) if current == vec![0] && candidate == vec![0, 1]
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_validates_route_table() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut candidate = config.clone();
        candidate.cluster_epoch = ClusterEpoch::new(2).unwrap();

        assert!(matches!(
            server.install_control_plane_runtime_config(candidate),
            Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: 0,
                route_epoch,
                config_epoch,
            }) if route_epoch == ClusterEpoch::new(1).unwrap()
                && config_epoch == ClusterEpoch::new(2).unwrap()
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_epoch_downgrade() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut stale = config.clone();
        stale.cluster_epoch = ClusterEpoch::new(1).unwrap();
        stale.pg_routes[0].cluster_epoch = stale.cluster_epoch;

        assert!(matches!(
            server.install_control_plane_runtime_config(stale),
            Err(StorageNodeServerError::RuntimeRefreshEpochDowngrade {
                current,
                candidate,
            }) if current == ClusterEpoch::new(2).unwrap()
                && candidate == ClusterEpoch::new(1).unwrap()
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_same_epoch_validity_regression() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = Some(5_000);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut stale = config.clone();
        stale.route_map_valid_until_ms = Some(4_000);

        assert!(matches!(
            server.install_control_plane_runtime_config(stale),
            Err(StorageNodeServerError::RuntimeRefreshValidityRegression {
                current: Some(5_000),
                candidate: Some(4_000),
            })
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_accepts_bounded_authoritative_refresh() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = None;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut authoritative = config.clone();
        authoritative.route_map_valid_until_ms = Some(5_000);

        server
            .install_control_plane_runtime_config(authoritative)
            .unwrap();
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(5_000)
        );
    }

    #[test]
    fn expired_route_map_rejects_new_work_but_allows_cleanup_route_validation() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_valid_until_ms = Some(1);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();

        let new_work_error = handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(new_work_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(new_work_error
            .message
            .contains("storage-node route map for cluster epoch"));

        handler
            .validate_pg_route_for_cleanup(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap();

        let stale_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
        let stale_epoch_error = handler
            .validate_pg_route_for_cleanup(config.node_id, stale_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(
            stale_epoch_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
    }

    #[test]
    fn storage_node_process_config_builds_control_plane_heartbeat() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: config.cluster_epoch,
                state: PgState::Peering,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
            StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: config.cluster_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
        ];
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        let heartbeat = config.control_plane_heartbeat(&node, 12, 2_000).unwrap();

        assert_eq!(heartbeat.node_id, config.node_id);
        assert_eq!(heartbeat.node_incarnation, 12);
        assert_eq!(heartbeat.endpoint, config.socket_path.to_str().unwrap());
        assert_eq!(heartbeat.observed_epoch, config.cluster_epoch);
        assert_eq!(heartbeat.requested_lease_duration_ms, 2_000);
        assert_eq!(
            heartbeat.cluster_map_history_reference_summary,
            node.cluster_map_history_reference_summary().unwrap()
        );
        assert_eq!(heartbeat.pg_observations.len(), 2);
        assert_eq!(heartbeat.pg_observations[0].pg_id, PgId::new(0));
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Peering);
        assert_eq!(heartbeat.pg_observations[1].pg_id, PgId::new(1));
        assert_eq!(heartbeat.pg_observations[1].state, PgState::Active);
        for observation in &heartbeat.pg_observations {
            let metadata_state = {
                let pg = node.get_pg(observation.pg_id.get()).unwrap();
                pg.metadata_command_replica_state().unwrap()
            };
            assert_eq!(
                observation.metadata_proof.applied_log_index,
                metadata_state.applied_log_index
            );
            assert_eq!(
                observation.metadata_proof.applied_log_hash,
                metadata_state.applied_log_hash
            );
            assert_eq!(
                observation.metadata_proof.state_digest,
                metadata_state.state_digest
            );
        }
    }

    #[test]
    fn storage_node_process_config_rejects_route_epoch_mismatch_for_heartbeat() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(1).unwrap();
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        assert!(matches!(
            config.control_plane_heartbeat(&node, 12, 2_000),
            Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: 0,
                route_epoch,
                config_epoch,
            }) if route_epoch == ClusterEpoch::new(1).unwrap()
                && config_epoch == ClusterEpoch::new(2).unwrap()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn storage_node_process_config_rejects_non_utf8_heartbeat_endpoint() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from(OsString::from_vec(vec![0xff]));
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        assert!(matches!(
            config.control_plane_heartbeat(&node, 12, 2_000),
            Err(StorageNodeServerError::SocketPathNotUtf8 { path }) if path == config.socket_path
        ));
    }

    #[test]
    fn storage_node_server_builds_control_plane_heartbeat() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let heartbeat = server.control_plane_heartbeat(12, 2_000).unwrap();

        assert_eq!(heartbeat.node_id, config.node_id);
        assert_eq!(heartbeat.node_incarnation, 12);
        assert_eq!(heartbeat.endpoint, config.socket_path.to_str().unwrap());
        assert_eq!(heartbeat.observed_epoch, config.cluster_epoch);
        assert_eq!(heartbeat.requested_lease_duration_ms, 2_000);
        assert_eq!(
            heartbeat.cluster_map_history_reference_summary,
            server
                ._node
                .cluster_map_history_reference_summary()
                .unwrap()
        );
        assert_eq!(heartbeat.pg_observations.len(), 1);
        assert_eq!(heartbeat.pg_observations[0].pg_id, PgId::new(0));
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Active);
        let metadata_state = {
            let pg = server._node.get_pg(0).unwrap();
            pg.metadata_command_replica_state().unwrap()
        };
        assert_eq!(
            heartbeat.pg_observations[0]
                .metadata_proof
                .applied_log_index,
            metadata_state.applied_log_index
        );
        assert_eq!(
            heartbeat.pg_observations[0].metadata_proof.applied_log_hash,
            metadata_state.applied_log_hash
        );
        assert_eq!(
            heartbeat.pg_observations[0].metadata_proof.state_digest,
            metadata_state.state_digest
        );
    }

    #[test]
    fn runtime_map_config_opens_non_acting_pgs_without_heartbeat_observation() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let acting_node_id = NodeId::new(8);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage-7.sock");
        let acting_socket_path = tmp.path().join("sock").join("storage-8.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        for (heartbeat_node_id, heartbeat_socket_path) in [
            (node_id, socket_path.clone()),
            (acting_node_id, acting_socket_path),
        ] {
            authority
                .set_node_membership(heartbeat_node_id, NodeMembershipState::Active)
                .unwrap();
            let first = authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_reference_summary:
                            crate::PgClusterMapHistoryReferenceSummary::default(),
                        pg_observations: Vec::new(),
                    },
                    1_000,
                )
                .unwrap();
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: first.cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_reference_summary:
                            crate::PgClusterMapHistoryReferenceSummary::default(),
                        pg_observations: Vec::new(),
                    },
                    1_001,
                )
                .unwrap();
        }
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        authority
            .set_pg_acting_set(pg_id, vec![acting_node_id])
            .unwrap();
        let observed_epoch = authority.snapshot().cluster_epoch();
        for (heartbeat_node_id, heartbeat_socket_path) in [
            (node_id, socket_path.clone()),
            (
                acting_node_id,
                tmp.path().join("sock").join("storage-8.sock"),
            ),
        ] {
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch,
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_reference_summary:
                            crate::PgClusterMapHistoryReferenceSummary::default(),
                        pg_observations: Vec::new(),
                    },
                    1_002,
                )
                .unwrap();
        }

        let runtime_map = authority.snapshot().runtime_map(1_003).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();

        assert_eq!(config.pg_ids, vec![pg_id.get()]);
        assert_eq!(config.pg_routes.len(), 1);
        assert_eq!(config.pg_routes[0].acting_set, vec![acting_node_id]);

        let server = StorageNodeServer::bind(config).unwrap();
        let heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert!(heartbeat.pg_observations.is_empty());

        let live_error = server
            .connection_handler()
            .validate_pg_route(node_id, runtime_map.cluster_epoch(), pg_id)
            .unwrap_err();
        assert!(matches!(
            live_error.code,
            StorageRpcErrorCode::InactivePgRoute | StorageRpcErrorCode::NonActingSetAccess
        ));
        server
            .connection_handler()
            .validate_shard_location_for_historical_inspection(ShardLocation::new(
                runtime_map.cluster_epoch(),
                DataPgId::new(pg_id),
                ShardIndex::new(0),
                node_id,
            ))
            .unwrap();
    }

    #[test]
    fn runtime_map_storage_node_server_heartbeat_updates_authority_pg_observation() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let server = StorageNodeServer::bind(config).unwrap();
        let heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert_eq!(heartbeat.observed_epoch, runtime_map.cluster_epoch());
        assert_eq!(heartbeat.pg_observations.len(), 1);
        assert_eq!(heartbeat.pg_observations[0].pg_id, pg_id);
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Peering);
        let proof = heartbeat.pg_observations[0].metadata_proof;

        let lease = server
            .heartbeat_control_plane(&mut authority, 12, 1_000, 1_003)
            .unwrap();
        assert_eq!(lease.node_id(), node_id);
        assert_eq!(lease.cluster_epoch(), runtime_map.cluster_epoch());

        let observation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .pg_observation(pg_id)
            .unwrap();
        assert_eq!(observation.state(), PgState::Peering);
        assert_eq!(observation.observed_epoch(), runtime_map.cluster_epoch());
        assert_eq!(observation.observed_at_ms(), 1_003);
        assert_eq!(observation.metadata_proof(), proof);
    }

    #[test]
    fn storage_node_refreshes_control_plane_runtime_map_candidate() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let mut config = config;
        config.route_map_valid_until_ms = Some(1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let stale_error = server
            .connection_handler()
            .validate_pg_route(node_id, runtime_map.cluster_epoch(), pg_id)
            .unwrap_err();
        assert_eq!(stale_error.code, StorageRpcErrorCode::StaleShardLocation);

        let refresh = server
            .refresh_control_plane_runtime_map(&mut authority, 12, 1_000, 1_003)
            .unwrap();
        assert_eq!(refresh.lease().node_id(), node_id);
        assert_eq!(
            refresh.lease().cluster_epoch(),
            refresh.runtime_map().cluster_epoch()
        );
        assert_eq!(refresh.next_config().node_id, node_id);
        assert_eq!(
            refresh.next_config().cluster_epoch,
            refresh.runtime_map().cluster_epoch()
        );
        assert_eq!(refresh.next_config().data_dir, config.data_dir);
        assert_eq!(refresh.next_config().pg_ids, vec![pg_id.get()]);
        assert_eq!(refresh.next_config().pg_routes.len(), 1);
        assert_eq!(refresh.next_config().pg_routes[0].state, PgState::Active);

        let installed_epoch = refresh.next_config().cluster_epoch;
        let lease = server.install_control_plane_refresh(refresh).unwrap();
        assert_eq!(lease.node_id(), node_id);
        assert!(
            !lease.serving(),
            "peering completion bumps the epoch before the node observes it"
        );
        let installed_config = server.config_snapshot();
        assert_eq!(installed_config.cluster_epoch, installed_epoch);
        assert_eq!(installed_config.pg_routes[0].state, PgState::Active);
        assert!(installed_config.route_map_valid_until_ms().is_some());
        let installed_heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert_eq!(installed_heartbeat.observed_epoch, installed_epoch);
        assert_eq!(
            installed_heartbeat.pg_observations[0].state,
            PgState::Active
        );
        assert!(
            authority
                .snapshot()
                .node(node_id)
                .unwrap()
                .pg_observation(pg_id)
                .is_none(),
            "peering completion clears observations until the node heartbeats the new epoch"
        );
        let active_lease = server
            .heartbeat_control_plane(&mut authority, 12, 1_000, 1_004)
            .unwrap();
        assert!(active_lease.serving());

        let observation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .pg_observation(pg_id)
            .unwrap();
        assert_eq!(observation.state(), PgState::Active);
        assert_eq!(observation.observed_epoch(), installed_epoch);
    }

    #[test]
    fn storage_node_control_plane_refresh_loop_installs_runtime_maps() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        crate::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let mut config = config;
        config.route_map_valid_until_ms = Some(1);
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        let now = Arc::new(AtomicU64::new(1_003));
        let loop_now = Arc::clone(&now);
        let mut refresh_loop = Arc::clone(&server)
            .spawn_control_plane_refresh_loop(
                authority,
                12,
                1_000,
                Duration::from_millis(5),
                move || loop_now.fetch_add(1, Ordering::SeqCst),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if refresh_loop.status().successes > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "control-plane refresh loop did not install a runtime map: {:?}",
                refresh_loop.status()
            );
            thread::sleep(Duration::from_millis(1));
        }

        let installed_config = server.config_snapshot();
        assert!(installed_config.cluster_epoch > runtime_map.cluster_epoch());
        assert!(installed_config.route_map_valid_until_ms().is_some());
        assert_eq!(installed_config.pg_routes.len(), 1);
        assert_eq!(installed_config.pg_routes[0].state, PgState::Active);
        assert_eq!(refresh_loop.status().failures, 0);

        refresh_loop.stop();
        let attempts_after_stop = refresh_loop.status().attempts;
        thread::sleep(Duration::from_millis(15));
        assert_eq!(refresh_loop.status().attempts, attempts_after_stop);
    }

    #[test]
    fn storage_node_control_plane_refresh_loop_rejects_zero_interval() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let config = StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_valid_until_ms: None,
            data_dir: tmp.path().join("node"),
            default_ec_shape: EcShape { k: 1, m: 0 },
            pg_ids: vec![0],
            socket_path,
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: node_id,
                acting_set: vec![node_id],
            }],

            historical_pg_routes: Vec::new(),
        };
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();

        assert!(matches!(
            Arc::clone(&server).spawn_control_plane_refresh_loop(
                authority,
                12,
                1_000,
                Duration::ZERO,
                || 1_000,
            ),
            Err(StorageNodeServerError::ControlPlaneRefreshLoopZeroInterval)
        ));
    }

    #[test]
    fn metadata_command_lock_wait_emits_diagnostic() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let first = locks.acquire(NodeId::new(7), pg_id, None);
        let before = observability::metrics_snapshot();
        let (wait_tx, wait_rx) = mpsc::channel();
        locks.set_before_wait_hook(Arc::new(move |actual_pg_id| {
            assert_eq!(actual_pg_id, pg_id);
            let _ = wait_tx.send(());
        }));
        let waiting_locks = locks.clone();

        let waiter = thread::spawn(move || {
            let _attached =
                observability::AttachedTrace::new(observability::TraceContext::from_ids(
                    "trace-metadata-command-lock-wait".to_string(),
                    "request-metadata-command-lock-wait".to_string(),
                ));
            let _guard = waiting_locks.acquire(NodeId::new(7), pg_id, None);
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter should enter metadata-command lock wait");
        drop(first);
        waiter
            .join()
            .expect("waiter should acquire and release lock");

        let after = observability::metrics_snapshot();
        assert!(
            after.metadata_command_session_wait_total > before.metadata_command_session_wait_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| record.request_id == "request-metadata-command-lock-wait")
            .expect("lock wait should be recorded in flight recorder");
        assert_eq!(record.event, "metadata_command_session_wait");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("wait_us="));
    }

    #[test]
    fn metadata_command_lock_wait_emits_blocked_holder_diagnostic() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let first = locks.acquire(
            NodeId::new(7),
            pg_id,
            Some(StorageNodeMetadataCommandLockContext {
                request_id: 41,
                kind: StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            }),
        );
        locks.update_context(
            pg_id,
            Some(StorageNodeMetadataCommandLockContext {
                request_id: 43,
                kind: StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            }),
        );
        let (wait_tx, wait_rx) = mpsc::channel();
        locks.set_before_wait_hook(Arc::new(move |actual_pg_id| {
            assert_eq!(actual_pg_id, pg_id);
            let _ = wait_tx.send(());
        }));
        let waiting_locks = locks.clone();

        let waiter = thread::spawn(move || {
            let _attached =
                observability::AttachedTrace::new(observability::TraceContext::from_ids(
                    "trace-metadata-command-lock-blocked".to_string(),
                    "request-metadata-command-lock-blocked".to_string(),
                ));
            let _guard = waiting_locks.acquire(
                NodeId::new(7),
                pg_id,
                Some(StorageNodeMetadataCommandLockContext {
                    request_id: 42,
                    kind: StorageRpcMessageKind::MetadataCommandPendingEnvelope,
                }),
            );
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter should enter metadata-command lock wait");
        let deadline = Instant::now() + Duration::from_secs(3);
        let record = loop {
            if let Some(record) = observability::flight_recorder_snapshot()
                .into_iter()
                .rev()
                .find(|record| {
                    record.request_id == "request-metadata-command-lock-blocked"
                        && record.event == "metadata_command_lock_wait_blocked"
                })
            {
                break record;
            }
            assert!(
                Instant::now() < deadline,
                "blocked lock diagnostic should be emitted before waiter acquires"
            );
            thread::sleep(Duration::from_millis(25));
        };
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("waiter_request_id=42"));
        assert!(record
            .detail
            .contains("waiter_kind=\"metadata command pending envelope\""));
        assert!(record.detail.contains("holder_request_id=41"));
        assert!(record
            .detail
            .contains("holder_kind=\"metadata command PG lock acquire\""));
        assert!(record.detail.contains("holder_held_us="));
        assert!(record.detail.contains("holder_current_request_id=43"));
        assert!(record
            .detail
            .contains("holder_current_kind=\"metadata command apply and record\""));
        assert!(record.detail.contains("holder_current_elapsed_us="));
        locks.update_context(pg_id, None);
        {
            let held = locks.state.held.lock().unwrap_or_else(|e| e.into_inner());
            let holder = held
                .get(&pg_id)
                .expect("holder should still be present before release");
            assert!(holder.current_context.is_none());
            assert!(holder.current_started_at.is_none());
        }

        drop(first);
        waiter
            .join()
            .expect("waiter should acquire and release lock");
    }

    #[test]
    fn storage_node_rpc_metadata_command_wait_records_frame_trace() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
            scope_bucket: Some(command.bucket_name().clone()),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg_guard = server
            .metadata_command_locks
            .acquire(NodeId::new(7), PgId::new(0), None);
        let (wait_tx, wait_rx) = mpsc::channel();
        server
            .metadata_command_locks
            .set_before_wait_hook(Arc::new(move |actual_pg_id| {
                assert_eq!(actual_pg_id, PgId::new(0));
                let _ = wait_tx.send(());
            }));
        let socket_path = config.socket_path.clone();
        let accept = thread::spawn(move || server.accept_one().unwrap());
        let before = observability::metrics_snapshot();

        let client = thread::spawn(move || {
            let mut client = UnixStream::connect(socket_path).unwrap();
            send_frame(
                &mut client,
                11,
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                encode_metadata_command_pending_slot_request(&request).unwrap(),
            )
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("RPC handler should enter metadata-command lock wait");
        drop(pg_guard);
        let response = client.join().expect("client should receive response");
        accept.join().unwrap();
        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();

        let after = observability::metrics_snapshot();
        assert!(
            after.metadata_command_session_wait_total > before.metadata_command_session_wait_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-11"
                    && record.event == "metadata_command_session_wait"
            })
            .expect("storage-node RPC wait should be recorded without caller-attached trace");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("wait_us="));
    }

    fn private_socket_dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn bind_error(config: StorageNodeProcessConfig) -> StorageNodeServerError {
        match StorageNodeServer::bind(config) {
            Ok(_) => panic!("expected storage-node bind to fail"),
            Err(error) => error,
        }
    }

    fn read_handle_acquire_payload(read_operation_id: &str, location: ShardLocation) -> Vec<u8> {
        encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.to_string(),
            locations: vec![location],
            shard_keys: vec![test_shard_key(location.shard_index().get())],
        })
        .unwrap()
    }

    fn test_location(epoch: u64, pg_id: u32, node_id: u32) -> ShardLocation {
        test_location_with_shard(epoch, pg_id, node_id, 0)
    }

    fn test_location_with_shard(
        epoch: u64,
        pg_id: u32,
        node_id: u32,
        shard_index: u8,
    ) -> ShardLocation {
        ShardLocation::new(
            ClusterEpoch::new(epoch).unwrap(),
            DataPgId::new(PgId::new(pg_id)),
            ShardIndex::new(shard_index),
            NodeId::new(node_id),
        )
    }

    fn test_shard_key(shard_index: u8) -> ShardKey {
        ShardKey::new(&[0x42; 16], 99, shard_index)
    }

    fn test_metadata_command(pg_id: u32, log_index: u64) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                crate::tests::bucket_name("metadata-rpc-bucket"),
                crate::tests::object_key("object"),
                crate::tests::stream_session_id("metadata-rpc"),
                GenerationId::new(1).unwrap(),
                123,
            )),
        )
    }

    fn create_probe_bucket_direct(store: &crate::PgStore, bucket: &BucketName) {
        let owner = crate::OwnerIdentity::from_principal("owner");
        store
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &AclGrants::default(),
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            })
            .unwrap();
    }

    fn put_probe_lifecycle_direct(store: &crate::PgStore, bucket: &BucketName) {
        store
            .put_bucket_subresource(
                bucket,
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: BucketSubresourceAux::None,
                },
            )
            .unwrap();
    }

    fn test_metadata_checkpoint_with_bucket(
        bucket_name: &str,
    ) -> (BucketName, crate::pg_store::MetadataCommandCheckpoint) {
        let source_tmp = test_util::tempdir();
        let source_node = crate::node::SharedStorageNode::open(source_tmp.path(), &[0]).unwrap();
        let bucket = crate::tests::bucket_name(bucket_name);
        let checkpoint = {
            let source_pg = source_node.get_pg(0).unwrap();
            create_probe_bucket_direct(&source_pg, &bucket);
            put_probe_lifecycle_direct(&source_pg, &bucket);
            source_pg.refresh_metadata_command_state_digest().unwrap();
            source_pg
                .metadata_command_checkpoint(11, ClusterEpoch::INITIAL)
                .unwrap()
        };
        (bucket, checkpoint)
    }

    fn test_bucket_write_reservation_proof(
        bucket: crate::BucketName,
        key: &crate::ObjectKey,
    ) -> BucketWriteReservationProof {
        BucketWriteReservationProof {
            bucket,
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "storage-node-rpc-test".to_string(),
            created_at: 1,
            lease_deadline: None,
            target_context: Some(key.as_str().to_string()),
        }
    }

    fn read_handle_release_payload(read_operation_id: &str) -> Vec<u8> {
        encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.to_string(),
        })
        .unwrap()
    }

    #[test]
    fn metadata_checkpoint_success_response_returns_structured_error_when_frame_too_large() {
        let success = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint export",
            b"ok",
            256,
        )
        .unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&success).unwrap(),
            Ok(b"ok".to_vec())
        );

        let oversized_payload = vec![42; 300];
        let response = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint export",
            &oversized_payload,
            256,
        )
        .unwrap();
        let error = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error
            .message
            .contains("metadata command checkpoint export response is too large"));
        assert!(error
            .message
            .contains("exceeds storage RPC payload limit 256 bytes"));

        let response = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint candidates",
            &oversized_payload,
            256,
        )
        .unwrap();
        let error = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error
            .message
            .contains("metadata command checkpoint candidates response is too large"));
    }

    #[test]
    fn metadata_checkpoint_candidates_for_frame_skips_oversized_newest_candidate() {
        let tmp = test_util::tempdir();
        let node = crate::node::SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let pg = node.get_pg(0).unwrap();
        let bucket = crate::tests::bucket_name("metadata-checkpoint-frame-candidate");
        create_probe_bucket_direct(&pg, &bucket);
        pg.refresh_metadata_command_state_digest().unwrap();
        let small = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();

        let large_body = format!(
            "<LifecycleConfiguration>{}</LifecycleConfiguration>",
            "x".repeat(4096)
        );
        pg.put_bucket_subresource(
            &bucket,
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: &large_body,
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let large = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();

        let small_payload = encode_metadata_command_checkpoint_candidates_response(
            &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                checkpoints: vec![small.clone()],
            },
        )
        .unwrap();
        let large_payload = encode_metadata_command_checkpoint_candidates_response(
            &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                checkpoints: vec![large],
            },
        )
        .unwrap();
        let small_response_len = encode_storage_rpc_success_response(&small_payload).len();
        let large_response_len = encode_storage_rpc_success_response(&large_payload).len();
        assert!(large_response_len > small_response_len);

        let candidates = metadata_command_checkpoint_candidates_for_frame(
            &pg,
            ClusterEpoch::INITIAL,
            u64::MAX,
            1,
            small_response_len,
        )
        .unwrap();

        assert_eq!(candidates, vec![small]);
    }

    fn send_frame(
        client: &mut UnixStream,
        request_id: u64,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> StorageRpcFrame {
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        write_storage_rpc_frame_to(client, &request).unwrap();
        read_storage_rpc_frame_from(client).unwrap()
    }

    fn send_read_handle_acquire(
        config: StorageNodeProcessConfig,
        location: ShardLocation,
    ) -> StorageRpcErrorResponse {
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 7,
            kind: StorageRpcMessageKind::ReadHandlesAcquire,
            payload: read_handle_acquire_payload("read-op", location),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err()
    }

    fn wait_for_read_handle_count(
        server: &StorageNodeServer,
        location: ShardLocation,
        expected: usize,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let actual = server.read_handle_count(location);
            if actual == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "read handle count for {location:?} stayed at {actual}, expected {expected}"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn storage_node_server_answers_health_request() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 42,
            kind: StorageRpcMessageKind::Health,
            payload: Vec::new(),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        assert_eq!(response.request_id, 42);
        assert_eq!(response.kind, StorageRpcMessageKind::Health);
        let health_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let health = decode_health_response(&health_payload).unwrap();
        assert_eq!(health.node_id, NodeId::new(7));
        assert_eq!(health.cluster_epoch, ClusterEpoch::new(1).unwrap());
    }

    #[test]
    fn storage_node_server_accepts_second_client_while_first_session_is_held() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let accept_thread = thread::spawn(move || {
            server_for_thread.accept_and_spawn().unwrap();
            server_for_thread.accept_and_spawn().unwrap();
        });
        let location = test_location(1, 0, 7);

        let mut held_client = UnixStream::connect(&socket_path).unwrap();
        let acquire = send_frame(
            &mut held_client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("held-read", location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let mut health_client = UnixStream::connect(socket_path).unwrap();
        let health = send_frame(
            &mut health_client,
            8,
            StorageRpcMessageKind::Health,
            Vec::new(),
        );
        let health_payload = decode_storage_rpc_response_payload(&health.payload)
            .unwrap()
            .unwrap();
        let health = decode_health_response(&health_payload).unwrap();
        assert_eq!(health.node_id, NodeId::new(7));

        drop(health_client);
        drop(held_client);
        accept_thread.join().unwrap();
        wait_for_read_handle_count(&server, location, 0);
    }

    #[test]
    fn storage_node_server_disconnect_releases_handles_for_cleanup_probe() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        drop(client);
        join.join().unwrap();
        wait_for_read_handle_count(&server, location, 0);

        let mut handles = server
            .read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let shard_key = test_shard_key(location.shard_index().get());
        handles
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap();
        assert_eq!(handles.count(location), 1);
        handles.release(&[(location, shard_key)]);
        assert_eq!(handles.count(location), 0);
    }

    #[test]
    fn storage_node_server_rejects_non_private_socket_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o777),
        )
        .unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketDirectoryNotPrivate { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_special_mode_socket_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o2700),
        )
        .unwrap();

        let err = bind_error(config.clone());

        assert!(matches!(
            err,
            StorageNodeServerError::SocketDirectoryNotPrivate { mode: 0o2700, .. }
        ));
        let _ = fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        );
    }

    #[test]
    fn storage_node_server_creates_private_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        let mode = fs::metadata(&config.data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn storage_node_server_tightens_existing_readable_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        fs::create_dir_all(&config.data_dir).unwrap();
        fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o755)).unwrap();

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        let mode = fs::metadata(&config.data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn storage_node_server_rejects_writable_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        fs::create_dir_all(&config.data_dir).unwrap();
        fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o777)).unwrap();

        let err = bind_error(config.clone());

        assert!(matches!(
            err,
            StorageNodeServerError::Io { source, .. }
                if source.kind() == io::ErrorKind::PermissionDenied
        ));
        let _ = fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o700));
    }

    #[test]
    fn storage_node_server_rejects_second_owner_for_same_data_dir() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _first = StorageNodeServer::bind(config.clone()).unwrap();
        let mut second = config;
        second.socket_path = tmp.path().join("sock").join("other.sock");

        let err = bind_error(second);

        assert!(matches!(
            err,
            StorageNodeServerError::DataDirAlreadyLocked { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_socket_paths() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicateSocketPath { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_pg_routes() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes.push(config.pg_routes[0].clone());

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicatePgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_unconfigured_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].pg_id = 9;

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::RoutePgNotConfigured { pg_id: 9 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_primary_outside_acting_set() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::RoutePrimaryNotInActingSet {
                pg_id: 0,
                primary_node_id: 8
            }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_bind_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);
        private_socket_dir(config.socket_path.parent().unwrap());

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_server_opens_only_configured_pg_directories() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![2];
        config.pg_routes = vec![test_route(2)];
        private_socket_dir(config.socket_path.parent().unwrap());

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        assert!(config.data_dir.join("pg-0002").is_dir());
        assert!(!config.data_dir.join("pg-0000").exists());
        assert!(!config.data_dir.join("pg-0001").exists());
    }

    #[test]
    fn storage_node_static_config_rejects_inconsistent_pg_routes() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");
        second.socket_path = tmp.path().join("sock").join("storage-2.sock");
        second.pg_routes[0].primary_node_id = NodeId::new(8);
        second.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::InconsistentPgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_removes_stale_socket_path_on_restart() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let stale = UnixListener::bind(&config.socket_path).unwrap();
        drop(stale);
        assert!(config.socket_path.exists());

        let _server = StorageNodeServer::bind(config).unwrap();
    }

    #[test]
    fn storage_node_server_restart_reopens_existing_pg_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = ShardKey::new(&[0xA5; 16], 7, 0);
        {
            let server = StorageNodeServer::bind(config.clone()).unwrap();
            let pg = server._node.get_pg(0).unwrap();
            pg.write_shard(&shard_key, b"persistent shard").unwrap();
        }

        let restarted = StorageNodeServer::bind(config).unwrap();
        assert_eq!(
            restarted._node.read_shard_file(0, &shard_key).unwrap(),
            b"persistent shard"
        );
        let pg = restarted._node.get_pg(0).unwrap();
        assert_eq!(pg.read_shard(&shard_key).unwrap().data, b"persistent shard");
    }

    #[test]
    fn storage_node_server_rejects_active_socket_owner() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _active = UnixListener::bind(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_existing_non_socket_path() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        File::create(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_returns_unsupported_operation_error() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let before = observability::metrics_snapshot();

        let mut client = UnixStream::connect(socket_path).unwrap();
        let frame_bytes =
            encode_storage_rpc_frame(9, StorageRpcMessageKind::ClaimHeartbeat, b"").unwrap();
        client.write_all(&frame_bytes).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::UnsupportedOperation);
        let after = observability::metrics_snapshot();
        assert!(after.storage_rpc_error_total > before.storage_rpc_error_total);
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-9" && record.event == "storage_rpc_error"
            })
            .expect("storage-node RPC error should be recorded");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("rpc_kind=ClaimHeartbeat"));
        assert!(record.detail.contains("error_code=UnsupportedOperation"));
        assert!(record.detail.contains("message_len="));
        assert!(record.detail.contains("message_hash="));
    }

    #[test]
    fn storage_node_server_reads_shard_with_expected_ack() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"read payload".to_vec();
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(&payload),
        };
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &payload)
            .unwrap();
        drop(node);
        let request = StorageRpcShardReadRequest {
            location,
            shard_key: shard_key.clone(),
            expected_ack,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardRead,
            encode_shard_read_request(&request).unwrap(),
        );
        let mismatched_request = StorageRpcShardReadRequest {
            expected_ack: WriteAck {
                stored_size: expected_ack.stored_size,
                crc64: expected_ack.crc64 ^ 1,
            },
            ..request
        };
        let mismatch = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardRead,
            encode_shard_read_request(&mismatched_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let read_payload = decode_shard_read_response(&response_payload, expected_ack).unwrap();
        assert_eq!(read_payload, payload);
        let error = decode_storage_rpc_response_payload(&mismatch.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert!(error.message.contains("ack mismatch"));
    }

    #[test]
    fn storage_node_server_preserves_missing_historical_shard_error() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let request = StorageRpcShardReadRequest {
            location,
            shard_key: test_shard_key(0),
            expected_ack: WriteAck {
                stored_size: 9,
                crc64: 0x1234,
            },
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardHistoricalRead,
            encode_shard_read_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::NotFound);
        assert_eq!(error.message, "not found");
    }

    #[test]
    fn storage_node_server_reads_shard_range_with_expected_ack() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"read range payload".to_vec();
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(&payload),
        };
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &payload)
            .unwrap();
        drop(node);
        let request = StorageRpcShardReadRangeRequest {
            location,
            shard_key,
            expected_ack,
            offset: 5,
            length: 5,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardReadRange,
            encode_shard_read_range_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let read_payload =
            decode_shard_read_range_response(&response_payload, request.length as usize).unwrap();
        assert_eq!(read_payload, payload[5..10]);
    }

    #[test]
    fn storage_node_server_lists_scavenger_shard_files_over_rpc() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let key = test_shard_key(0);
        let payload = b"remote shard scavenger file";
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            node.get_pg(0).unwrap().write_shard(&key, payload).unwrap();
        }
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcScavengerListFilesRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            data_pg_id: DataPgId::new(PgId::new(0)),
        };
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardScavengerListFiles,
            encode_scavenger_list_files_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let scan = decode_scavenger_list_files_response(&response_payload).unwrap();
        assert_eq!(scan.files.len(), 1);
        assert_eq!(scan.files[0].key, key);
        assert_eq!(scan.files[0].size, payload.len() as u64);
        assert!(scan.errors.is_empty());
    }

    #[test]
    fn storage_node_server_retries_lost_shard_write_without_overwrite() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"first payload".to_vec();
        let request = StorageRpcShardWriteRequest {
            location,
            shard_key: shard_key.clone(),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload: payload.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        let first_payload = decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let first_ack = decode_shard_write_ack(
            &first_payload,
            request.expected_size,
            request.expected_crc64,
        )
        .unwrap();
        let retry = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        let retry_payload = decode_storage_rpc_response_payload(&retry.payload)
            .unwrap()
            .unwrap();
        let retry_ack = decode_shard_write_ack(
            &retry_payload,
            request.expected_size,
            request.expected_crc64,
        )
        .unwrap();

        let different = b"different payload".to_vec();
        let different_request = StorageRpcShardWriteRequest {
            location,
            shard_key: shard_key.clone(),
            expected_size: different.len() as u64,
            expected_crc64: checksum::crc64::checksum(&different),
            payload: different,
        };
        let mismatch = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&different_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        assert_eq!(retry_ack, first_ack);
        let error = decode_storage_rpc_response_payload(&mismatch.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(reopened.read_shard_file(0, &shard_key).unwrap(), payload);
    }

    #[test]
    fn storage_node_server_repair_write_replaces_existing_shard() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let corrupt = b"corrupt shard".to_vec();
        let repaired = b"repaired shard".to_vec();
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &corrupt)
            .unwrap();
        drop(node);

        let request = StorageRpcShardWriteRequest {
            location,
            shard_key: shard_key.clone(),
            expected_size: repaired.len() as u64,
            expected_crc64: checksum::crc64::checksum(&repaired),
            payload: repaired.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardRepairWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let ack = decode_shard_write_ack(&payload, request.expected_size, request.expected_crc64)
            .unwrap();
        assert_eq!(ack.stored_size, request.expected_size);
        assert_eq!(ack.crc64, request.expected_crc64);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(reopened.read_shard_file(0, &shard_key).unwrap(), repaired);
    }

    #[test]
    fn storage_node_server_rejects_corrupt_shard_write_without_file() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"corrupt-before-write".to_vec();
        let request = StorageRpcShardWriteRequest {
            location,
            shard_key: shard_key.clone(),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload,
        };
        let mut request_payload = encode_shard_write_request(&request).unwrap();
        let last = request_payload
            .last_mut()
            .expect("test shard write payload must be nonempty");
        *last ^= 0x01;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardWrite,
            request_payload,
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        assert!(error.message.contains("checksum mismatch"));
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_retries_lost_shard_delete_as_terminal_success() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let request = StorageRpcShardDeleteRequest {
            location,
            shard_key: shard_key.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .write_shard_file_if_absent(0, &shard_key, b"delete me")
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardDelete,
                encode_shard_delete_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_validates_shard_delete_route_before_missing_success() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let requests = [
            (
                test_location(2, 0, 7),
                StorageRpcErrorCode::StaleShardLocation,
            ),
            (test_location(1, 0, 8), StorageRpcErrorCode::UnknownNode),
            (test_location(1, 9, 7), StorageRpcErrorCode::UnknownPg),
        ];
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for (index, (location, expected_code)) in requests.into_iter().enumerate() {
            let request = StorageRpcShardDeleteRequest {
                location,
                shard_key: shard_key.clone(),
            };
            let response = send_frame(
                &mut client,
                index as u64 + 1,
                StorageRpcMessageKind::ShardDelete,
                encode_shard_delete_request(&request).unwrap(),
            );
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, expected_code);
        }
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_server_retries_lost_shard_ack_record_exactly() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardAckRecord,
                encode_shard_ack_batch_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        let validate = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ShardAckValidate,
            encode_shard_ack_batch_request(&request).unwrap(),
        );
        decode_storage_rpc_response_payload(&validate.payload)
            .unwrap()
            .unwrap();

        let mismatch = StorageRpcShardAckBatchRequest {
            items: vec![StorageRpcShardAckItem {
                shard_key,
                ack: WriteAck {
                    stored_size: 13,
                    crc64: 0x1234,
                },
            }],
            ..request
        };
        let mismatch_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::ShardAckRecord,
            encode_shard_ack_batch_request(&mismatch).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&mismatch_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&mismatch.items[0].shard_key, ack)
            .unwrap();
    }

    #[test]
    fn storage_node_server_loads_and_retries_lost_shard_ack_delete() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let record = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let item = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            shard_key: shard_key.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let record_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardAckRecord,
            encode_shard_ack_batch_request(&record).unwrap(),
        );
        decode_storage_rpc_response_payload(&record_response.payload)
            .unwrap()
            .unwrap();

        let load_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardAckLoad,
            encode_shard_ack_item_request(&item),
        );
        let load_payload = decode_storage_rpc_response_payload(&load_response.payload)
            .unwrap()
            .unwrap();
        let loaded = decode_shard_ack_item_response(&load_payload).unwrap();
        assert_eq!(loaded.shard_key, shard_key);
        assert_eq!(loaded.ack, ack);

        for request_id in [3, 4] {
            let delete_response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardAckDelete,
                encode_shard_ack_item_request(&item),
            );
            decode_storage_rpc_response_payload(&delete_response.payload)
                .unwrap()
                .unwrap();
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        assert!(matches!(
            pg.stat_shard(&shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_shard_ack_metadata_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.node_id = NodeId::new(8);
        config.pg_routes[0].primary_node_id = NodeId::new(7);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let existing_key = test_shard_key(0);
        let new_key = test_shard_key(1);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.register_written_shards_batch_exact(&[(&existing_key, ack)])
                .unwrap();
        }
        let record = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(8),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: new_key.clone(),
                ack,
            }],
        };
        let existing_record = StorageRpcShardAckBatchRequest {
            items: vec![StorageRpcShardAckItem {
                shard_key: existing_key.clone(),
                ack,
            }],
            ..record.clone()
        };
        let existing_item = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(8),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            shard_key: existing_key.clone(),
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let requests = [
            (
                StorageRpcMessageKind::ShardAckRecord,
                encode_shard_ack_batch_request(&record).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardAckValidate,
                encode_shard_ack_batch_request(&existing_record).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardAckLoad,
                encode_shard_ack_item_request(&existing_item),
            ),
            (
                StorageRpcMessageKind::ShardAckDelete,
                encode_shard_ack_item_request(&existing_item),
            ),
        ];
        for (index, (kind, payload)) in requests.into_iter().enumerate() {
            let response = send_frame(&mut client, index as u64 + 1, kind, payload);
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
        }
        let historical_load = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::ShardAckHistoricalLoad,
            encode_shard_ack_item_request(&existing_item),
        );
        let historical_payload = decode_storage_rpc_response_payload(&historical_load.payload)
            .unwrap()
            .unwrap();
        let historical_item = decode_shard_ack_item_response(&historical_payload).unwrap();
        assert_eq!(historical_item.shard_key, existing_key);
        assert_eq!(historical_item.ack, ack);
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&existing_key, ack).unwrap();
        assert!(matches!(pg.stat_shard(&new_key), Err(StoreError::NotFound)));
    }

    #[test]
    fn storage_node_server_shard_scavenger_metadata_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.node_id = NodeId::new(8);
        config.pg_routes[0].primary_node_id = NodeId::new(7);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(8),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };
        let observation_key = ShardScavengerObservationKey {
            node_id: 8,
            data_pg_id: 0,
            shard_index: shard_key.shard_index(),
            shard_key,
        };
        let observation = ShardScavengerObservationRecord {
            key: observation_key.clone(),
            data_size: Some(12),
            crc64: Some(0x1234),
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
            last_error: None,
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let requests = [
            (
                StorageRpcMessageKind::ShardScavengerShardRows,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerPayloadReferences,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationRecord,
                encode_scavenger_observation_record_request(
                    &StorageRpcScavengerObservationRecordRequest {
                        route: route.clone(),
                        observation,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservations,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationResolve,
                encode_scavenger_observation_key_request(
                    &StorageRpcScavengerObservationKeyRequest {
                        route,
                        key: observation_key,
                    },
                )
                .unwrap(),
            ),
        ];
        for (index, (kind, payload)) in requests.into_iter().enumerate() {
            let response = send_frame(&mut client, index as u64 + 1, kind, payload);
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        assert!(pg.list_shard_scavenger_observations().unwrap().is_empty());
    }

    #[test]
    fn storage_node_server_rejects_stale_shard_ack_validate_before_rows() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        pg.register_written_shards_batch_exact(&[(&shard_key, ack)])
            .unwrap();
        drop(pg);
        let stale_request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardAckValidate,
            encode_shard_ack_batch_request(&stale_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&shard_key, ack).unwrap();
    }

    #[test]
    fn storage_node_server_returns_metadata_command_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_command_state_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state, expected);
    }

    #[test]
    fn storage_node_server_compacts_metadata_command_log() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        pg.record_current_metadata_command_checkpoint(7, config.cluster_epoch)
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandLogCompact,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded =
            crate::storage_rpc::decode_metadata_command_log_compact_response(&payload).unwrap();
        assert_eq!(
            decoded.status,
            crate::pg_store::MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 1,
                compacted_before: 2,
            }
        );
    }

    #[test]
    fn storage_node_server_rejects_peering_metadata_command_log_compaction() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandLogCompact,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_log_hash_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        let expected = pg
            .retained_metadata_command_log_hashes(
                7,
                config.cluster_epoch,
                MetadataCommandLogIndex::new(1).unwrap(),
                MetadataCommandLogIndex::new(1).unwrap(),
            )
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            encode_metadata_command_log_hash_range_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_log_hash_range_response(&payload).unwrap();
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_log_entry_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        let expected = pg
            .retained_metadata_command_log_entries(
                7,
                config.cluster_epoch,
                MetadataCommandLogIndex::new(1).unwrap(),
                MetadataCommandLogIndex::new(1).unwrap(),
            )
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            encode_metadata_command_log_hash_range_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded =
            crate::storage_rpc::decode_metadata_command_log_entry_range_response(&payload).unwrap();
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_replay_apply() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert!(matches!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    applied_log_index: 1,
                    ..
                }
            )
        ));
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_destination_checks() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = crate::storage_rpc::decode_metadata_command_bool_response(&payload).unwrap();
        assert!(decoded.value);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_empty_state_initialize() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .state_digest;
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize,
            encode_metadata_command_transfer_empty_state_request(
                &StorageRpcMetadataCommandTransferEmptyStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_eq!(decoded.state.applied_log_hash, 0);
        assert_eq!(decoded.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_matching_state_initialize() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command)
                .unwrap()
                .state_digest
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize,
            encode_metadata_command_transfer_matching_state_request(
                &StorageRpcMetadataCommandTransferMatchingStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    applied_log_index: 0,
                    applied_log_hash: 0,
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_eq!(decoded.state.applied_log_hash, 0);
        assert_eq!(decoded.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_checkpoint_base_install() {
        let (bucket, checkpoint) = test_metadata_checkpoint_with_bucket("metadata-rpc-checkpoint");

        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_ne!(decoded.state.state_digest, 0);

        let destination_node =
            crate::node::SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();
        let destination_pg = destination_node.get_pg(0).unwrap();
        let loaded = destination_pg.head_bucket(&bucket).unwrap();
        assert_eq!(loaded.name, bucket);
        let lifecycle = destination_pg
            .get_bucket_subresource(&bucket, BucketSubresourceKind::Lifecycle)
            .unwrap()
            .unwrap();
        assert_eq!(lifecycle.body, "<LifecycleConfiguration/>");
    }

    #[test]
    fn storage_node_server_rejects_active_metadata_transfer_checkpoint_base_install() {
        let (_bucket, checkpoint) =
            test_metadata_checkpoint_with_bucket("metadata-rpc-checkpoint-active");
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
        assert!(
            error.message.contains("route is active"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_rejects_unproven_metadata_transfer_matching_state_proof() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command)
                .unwrap()
                .state_digest
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize,
            encode_metadata_command_transfer_matching_state_request(
                &StorageRpcMetadataCommandTransferMatchingStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    applied_log_index: 7,
                    applied_log_hash: 0x1234,
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert!(
            error.message.contains("unsupported unproven proof tuple"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_adopt_and_validate() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let expected_state_digest;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command).unwrap();
            expected_state_digest = pg.metadata_command_replica_state().unwrap().state_digest;
        }
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                destination_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let adopt_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt,
            encode_metadata_command_transfer_adopt_request(
                &StorageRpcMetadataCommandTransferAdoptRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                    commands: vec![MetadataTransferCommand {
                        command: rebased,
                        pre_state_digest: 0,
                        post_state_digest: expected_state_digest,
                    }],
                },
            )
            .unwrap(),
        );
        let validate_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: destination_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let adopt_payload = decode_storage_rpc_response_payload(&adopt_response.payload)
            .unwrap()
            .unwrap();
        let adopted = decode_metadata_command_state_response(&adopt_payload).unwrap();
        assert_eq!(adopted.state.state_digest, expected_state_digest);

        let validate_payload = decode_storage_rpc_response_payload(&validate_response.payload)
            .unwrap()
            .unwrap();
        let validated = decode_metadata_command_state_response(&validate_payload).unwrap();
        assert_eq!(validated.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_classifies_active_historical_transfer_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let source_route_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(4).unwrap();
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: source_route_epoch,
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        });
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
        );
    }

    #[test]
    fn storage_node_server_allows_historical_peering_metadata_transfer_reads_not_adopt() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let source_route_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(4).unwrap();
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(8)];
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: source_route_epoch,
            state: PgState::Peering,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        });
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let expected_state_digest;
        let expected_entries;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command).unwrap();
            expected_state_digest = pg.metadata_command_replica_state().unwrap().state_digest;
            expected_entries = pg
                .retained_metadata_command_log_entries(
                    7,
                    command.id().cluster_epoch(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                )
                .unwrap();
        }
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let adopt_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt,
            encode_metadata_command_transfer_adopt_request(
                &StorageRpcMetadataCommandTransferAdoptRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                    commands: vec![MetadataTransferCommand {
                        command: rebased,
                        pre_state_digest: 0,
                        post_state_digest: expected_state_digest,
                    }],
                },
            )
            .unwrap(),
        );
        let state_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let entries_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            encode_metadata_command_log_hash_range_request(
                &StorageRpcMetadataCommandLogHashRangeRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: command.id().cluster_epoch(),
                    pg_id: PgId::new(0),
                    first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                    last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                },
            ),
        );
        let validate_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: command.id().cluster_epoch(),
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let adopt_error = decode_storage_rpc_response_payload(&adopt_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(adopt_error.code, StorageRpcErrorCode::StaleShardLocation);

        let state_payload = decode_storage_rpc_response_payload(&state_response.payload)
            .unwrap()
            .unwrap();
        let state = decode_metadata_command_state_response(&state_payload).unwrap();
        assert_eq!(state.state.state_digest, expected_state_digest);

        let entries_payload = decode_storage_rpc_response_payload(&entries_response.payload)
            .unwrap()
            .unwrap();
        let entries =
            crate::storage_rpc::decode_metadata_command_log_entry_range_response(&entries_payload)
                .unwrap();
        assert_eq!(entries.entries, expected_entries);

        let validate_payload = decode_storage_rpc_response_payload(&validate_response.payload)
            .unwrap()
            .unwrap();
        let validated = decode_metadata_command_state_response(&validate_payload).unwrap();
        assert_eq!(validated.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_rejects_peering_replay_apply_while_active() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_rejects_normal_apply_while_peering() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_returns_metadata_command_read_and_allocator_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let first = test_metadata_command(0, 1);
        let applied = test_metadata_command(0, 2);
        let pending = test_metadata_command(0, 3);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        pg.record_metadata_command_abandoned(7, &first).unwrap();
        pg.apply_metadata_command_and_record(7, &applied).unwrap();
        let applied_hashes = pg
            .applied_metadata_command_log_entry_hashes(7, &applied)
            .unwrap()
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(7, &pending, Some(&bucket))
            .unwrap();
        drop(pg);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let state_request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let max_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            encode_metadata_command_state_request(&state_request),
        );
        let max_payload = decode_storage_rpc_response_payload(&max_response.payload)
            .unwrap()
            .unwrap();
        let max = decode_metadata_command_max_log_index_response(&max_payload).unwrap();
        assert_eq!(max.max_log_index, 2);

        let pending_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            encode_metadata_command_state_request(&state_request),
        );
        let pending_payload = decode_storage_rpc_response_payload(&pending_response.payload)
            .unwrap()
            .unwrap();
        let decoded_pending =
            decode_metadata_command_pending_envelope_response(&pending_payload).unwrap();
        assert_eq!(
            decoded_pending.command.unwrap().command_bytes(),
            pending.command_bytes()
        );

        let replay_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&state_request),
        );
        let replay_payload = decode_storage_rpc_response_payload(&replay_response.payload)
            .unwrap()
            .unwrap();
        let replay_state = decode_metadata_command_state_response(&replay_payload).unwrap();
        assert_eq!(replay_state.state.applied_log_index, 2);

        let applied_hashes_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: applied.clone(),
            })
            .unwrap(),
        );
        let applied_hashes_payload =
            decode_storage_rpc_response_payload(&applied_hashes_response.payload)
                .unwrap()
                .unwrap();
        let decoded_hashes =
            decode_metadata_command_applied_hashes_response(&applied_hashes_payload).unwrap();
        assert_eq!(
            decoded_hashes.outcome,
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some(applied_hashes))
        );

        let matching_response = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            encode_metadata_command_matching_applied_request(
                &StorageRpcMetadataCommandMatchingAppliedRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    pg_id: PgId::new(0),
                    command: applied.clone(),
                    expected_previous_log_hash: applied_hashes.0,
                },
            )
            .unwrap(),
        );
        let matching_payload = decode_storage_rpc_response_payload(&matching_response.payload)
            .unwrap()
            .unwrap();
        let matching = decode_metadata_command_bool_outcome_response(&matching_payload).unwrap();
        assert_eq!(
            matching.outcome,
            StorageRpcMetadataCommandBoolOutcome::Value(true)
        );

        let abandoned_response = send_frame(
            &mut client,
            6,
            StorageRpcMessageKind::MetadataCommandAbandoned,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: first.clone(),
            })
            .unwrap(),
        );
        let abandoned_payload = decode_storage_rpc_response_payload(&abandoned_response.payload)
            .unwrap()
            .unwrap();
        let abandoned = decode_metadata_command_bool_outcome_response(&abandoned_payload).unwrap();
        assert_eq!(
            abandoned.outcome,
            StorageRpcMetadataCommandBoolOutcome::Value(true)
        );

        let before_conflict = observability::metrics_snapshot();
        let next_response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::MetadataCommandNextId,
            encode_metadata_command_next_id_request(&StorageRpcMetadataCommandNextIdRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                min_log_index: 1,
            }),
        );
        let next_payload = decode_storage_rpc_response_payload(&next_response.payload)
            .unwrap()
            .unwrap();
        let next = decode_metadata_command_next_id_response(&next_payload).unwrap();
        assert_eq!(
            next.outcome,
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 3,
            }
        );
        let after_conflict = observability::metrics_snapshot();
        assert!(
            after_conflict.metadata_command_conflict_total
                > before_conflict.metadata_command_conflict_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-7"
                    && record.event == "metadata_command_conflict"
            })
            .expect("storage-node RPC conflict should be recorded without caller-attached trace");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("log_index=3"));
        assert!(record.detail.contains("kind=log_conflict"));
        assert!(record
            .detail
            .contains("command_kind=ReserveObjectGeneration"));
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_server_preserves_stale_object_version_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-version-rpc");
        let key = crate::tests::object_key("object");
        let pg = server._node.get_pg(0).unwrap();
        let applied = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                VersionId::from_u64(1),
            )),
        );
        pg.apply_metadata_command_and_record(7, &applied).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket,
                key,
                VersionId::from_u64(1),
            )),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id: VersionId::from_u64(1)
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_bucket_metadata_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-bucket-rpc");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let initial = pg.head_bucket_record_raw(&bucket).unwrap();
        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                initial.clone().with_execution_generation(2),
                crate::AclGrants::default(),
                true,
                false,
            )),
        );
        pg.apply_metadata_command_and_record(7, &newer).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                initial.with_execution_generation(1),
                crate::AclGrants::default(),
                false,
                true,
            )),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name: bucket,
                bucket_execution_generation: 1,
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_object_write_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-object-rpc");
        let key = crate::tests::object_key("object");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let first = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(1),
                owner: owner.clone(),
                write_sequence: 1,
                last_modified_millis: 123,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &first).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(2),
                owner,
                write_sequence: 1,
                last_modified_millis: 124,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence: 1,
                generation_id: None,
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_delete_marker_target_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-marker-delete-rpc");
        let key = crate::tests::object_key("object");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let first_marker = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: owner.clone(),
                write_sequence: 1,
                last_modified_millis: 123,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &first_marker)
            .unwrap();

        let reservation_id = crate::SessionId::try_from("76".repeat(16)).unwrap();
        let reserve = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                GenerationId::MIN,
                124,
            )),
        );
        pg.apply_metadata_command_and_record(7, &reserve).unwrap();
        let replacement_live = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(4).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: crate::PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: owner.clone(),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: crate::ObjectEtag::single_part(0),
                    ec: EcShape { k: 2, m: 1 },
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                },
                segments: Vec::new(),
                generation_reservation_id: reservation_id,
                write_sequence: 2,
                last_modified_millis: 124,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
                stream_create_bucket_write_reservation: None,
            })),
        );
        pg.apply_metadata_command_and_record(7, &replacement_live)
            .unwrap();
        let newer_marker = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(5).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner,
                write_sequence: 3,
                last_modified_millis: 125,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &newer_marker)
            .unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(6).unwrap(),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 },
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            })),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence: 1,
                generation_id: None,
            }
        );
    }

    #[test]
    fn storage_node_server_allocates_next_metadata_command_id() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let first = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .record_metadata_command_abandoned(7, &first)
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandNextId,
            encode_metadata_command_next_id_request(&StorageRpcMetadataCommandNextIdRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                min_log_index: 5,
            }),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let next = decode_metadata_command_next_id_response(&payload).unwrap();
        assert_eq!(
            next.outcome,
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                log_index: 5,
            }
        );
    }

    #[test]
    fn storage_node_server_serializes_metadata_command_pending_insert_per_pg() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config).unwrap();
        let handler = server.connection_handler();
        let pg_id = PgId::new(0);
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id,
            command: command.clone(),
            scope_bucket: Some(command.bucket_name().clone()),
        };
        let pg_guard = server
            .metadata_command_locks
            .acquire(NodeId::new(7), pg_id, None);
        let (tx, rx) = mpsc::channel();
        let handler_for_thread = handler.clone();
        let join = thread::spawn(move || {
            let session = StorageNodeSession::new(&handler_for_thread.read_handles);
            let response = handler_for_thread
                .metadata_command_pending_slot_insert_response(&session, request)
                .unwrap();
            tx.send(response).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(pg_guard);
        let response = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_pending_slot_insert_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted
        );
        assert!(server
            ._node
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_some());
    }

    #[test]
    fn storage_node_server_metadata_command_pg_lock_spans_connection_session() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let socket_path = config.socket_path.clone();
        let server = StorageNodeServer::bind(config).unwrap();
        let _server_thread = thread::spawn(move || server.serve_forever().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };
        let payload = encode_metadata_command_state_request(&request);
        let mut owner = UnixStream::connect(&socket_path).unwrap();
        let acquire = send_frame(
            &mut owner,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            payload.clone(),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();

        let owner_read = send_frame(
            &mut owner,
            2,
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            payload.clone(),
        );
        let owner_read_payload = decode_storage_rpc_response_payload(&owner_read.payload)
            .unwrap()
            .unwrap();
        let owner_max =
            decode_metadata_command_max_log_index_response(&owner_read_payload).unwrap();
        assert_eq!(owner_max.max_log_index, 0);

        let (tx, rx) = mpsc::channel();
        let blocked_socket_path = socket_path.clone();
        let blocked_payload = payload.clone();
        let blocked = thread::spawn(move || {
            let mut client = UnixStream::connect(blocked_socket_path).unwrap();
            let response = send_frame(
                &mut client,
                1,
                StorageRpcMessageKind::MetadataCommandMaxLogIndex,
                blocked_payload,
            );
            tx.send(response).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        let release = send_frame(
            &mut owner,
            3,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            payload,
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        let response = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        blocked.join().unwrap();
        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_max_log_index_response(&payload).unwrap();
        assert_eq!(decoded.max_log_index, 0);
    }

    #[test]
    fn storage_node_server_build_mark_deleting_returns_already_deleting_bucket() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("mark-deleting-already-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            crate::PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            crate::PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
        }

        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                bucket: bucket.clone(),
            },
            command_id,
        };
        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::BucketMarkDeletingCommandBuild,
            encode_bucket_mark_deleting_command_build_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_bucket_mark_deleting_command_build_response(&payload).unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
                assert_eq!(info.name, bucket);
                assert_eq!(info.state, BucketState::Deleting);
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(_) => {
                panic!("expected already-deleting mark bucket response")
            }
        }
    }

    #[test]
    fn storage_node_server_returns_metadata_command_acceptance() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandAcceptance,
            encode_metadata_command_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_acceptance_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
                crate::metadata_command::MetadataCommandAcceptance::Apply
            )
        );
    }

    #[test]
    fn storage_node_server_rejects_stale_metadata_command_route_before_acceptance() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: test_metadata_command(0, 1),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandAcceptance,
            encode_metadata_command_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_retries_lost_pending_slot_insert_exactly() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
            scope_bucket: Some(crate::tests::bucket_name("metadata-rpc-bucket")),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                encode_metadata_command_pending_slot_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        let conflict = StorageRpcMetadataCommandPendingSlotRequest {
            command: test_metadata_command(0, 2),
            ..request
        };
        let before_conflict = observability::metrics_snapshot();
        let conflict_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(&conflict).unwrap(),
        );
        let after_conflict = observability::metrics_snapshot();
        assert!(
            after_conflict.metadata_command_conflict_total
                > before_conflict.metadata_command_conflict_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-3"
                    && record.event == "metadata_command_conflict"
            })
            .expect(
                "storage-node pending conflict should be recorded without caller-attached trace",
            );
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("log_index=2"));
        assert!(record.detail.contains("kind=pending_slot_conflict"));
        assert!(record
            .detail
            .contains("command_kind=ReserveObjectGeneration"));
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&conflict_response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_pending_slot_insert_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                existing_log_index: 1,
                candidate_log_index: 2,
            }
        );
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pending = reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_bytes(), command.command_bytes());
    }

    #[test]
    fn storage_node_server_rejects_mismatched_pending_slot_scope_bucket() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: test_metadata_command(0, 1),
            scope_bucket: Some(crate::tests::bucket_name("wrong-scope-bucket")),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_node_server_retries_lost_pending_slot_remove_as_not_found() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let pg = server._node.get_pg(0).unwrap();
        pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
            .unwrap();
        pg.record_metadata_command_abandoned(7, &command).unwrap();
        drop(pg);
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for (request_id, expected_removed) in [(1, true), (2, false)] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
                encode_metadata_command_request(&request).unwrap(),
            );
            let payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            let decoded = decode_metadata_command_pending_slot_remove_response(&payload).unwrap();
            assert_eq!(decoded.removed, expected_removed);
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_wrong_node() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 0, 8));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownNode);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_stale_location_epoch() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(2, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_unknown_pg() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 9, 7));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_stale_route_epoch() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_inactive_pg() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_non_acting_set() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
    }

    #[test]
    fn storage_node_read_handle_state_delete_fence_is_atomic_with_acquire() {
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(7);
        let other_shard_key = ShardKey::new(&[0x99; 16], 99, 7);
        let mut state = StorageNodeReadHandleState::default();

        state.try_begin_delete(location, &shard_key).unwrap();
        state
            .try_acquire(&[(location, other_shard_key.clone())])
            .unwrap();
        state.release(&[(location, other_shard_key)]);
        let error = state
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ShardDeleteInProgress);
        assert!(error.message.contains("being deleted"));
        state.finish_delete(location, &shard_key);

        state.try_acquire(&[(location, shard_key.clone())]).unwrap();
        let error = state.try_begin_delete(location, &shard_key).unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error.message.contains("active read handles"));
        state.release(&[(location, shard_key.clone())]);
        state.try_begin_delete(location, &shard_key).unwrap();
    }

    #[test]
    fn storage_node_server_validates_route_before_acquiring_read_handle() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );

        let success_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![location]);
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_acquire_without_extra_count() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );

        for response in [first, second] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
            assert_eq!(acquired.locations, vec![location]);
        }
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_release_without_error_or_leak() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let first_release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        let second_release = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );

        for response in [first_release, second_release] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            decode_read_handle_release_response(&success_payload).unwrap();
            assert_eq!(server.read_handle_count(location), 0);
        }
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_session_rejects_read_operation_count_over_limit() {
        let shared_handles = Mutex::new(StorageNodeReadHandleState::default());
        let mut session = StorageNodeSession::new(&shared_handles);
        let location = test_location(1, 0, 7);

        for i in 0..STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION {
            session
                .acquire_read_handles(StorageRpcReadHandleAcquireRequest {
                    read_operation_id: format!("read-op-{i}"),
                    locations: vec![location],
                    shard_keys: vec![test_shard_key(location.shard_index().get())],
                })
                .unwrap();
        }
        let error = session
            .acquire_read_handles(StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-over-limit".to_string(),
                locations: vec![location],
                shard_keys: vec![test_shard_key(location.shard_index().get())],
            })
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert_eq!(
            shared_handles.lock().unwrap().count(location),
            STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION
        );
    }

    #[test]
    fn storage_node_read_handle_state_rejects_aggregate_limits() {
        let location = test_location(1, 0, 7);
        let mut operations_exhausted = StorageNodeReadHandleState {
            live_read_operations: STORAGE_NODE_MAX_LIVE_READ_OPERATIONS,
            ..StorageNodeReadHandleState::default()
        };
        let shard_key = test_shard_key(location.shard_index().get());
        let error = operations_exhausted
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);

        let mut locations_exhausted = StorageNodeReadHandleState {
            live_read_handle_locations: STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS,
            ..StorageNodeReadHandleState::default()
        };
        let error = locations_exhausted
            .try_acquire(&[(location, shard_key)])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
    }

    #[test]
    fn storage_node_active_session_state_rejects_over_limit() {
        let mut active_sessions = StorageNodeActiveSessionState::default();

        assert!(active_sessions.try_acquire(2));
        assert!(active_sessions.try_acquire(2));
        assert!(!active_sessions.try_acquire(2));
        active_sessions.release();
        assert!(active_sessions.try_acquire(2));
    }

    #[test]
    fn storage_node_active_sessions_release_notifies_capacity() {
        let active_sessions = Arc::new(StorageNodeActiveSessions::default());
        let first = active_sessions.try_acquire(1).unwrap();
        assert!(active_sessions.try_acquire(1).is_none());

        drop(first);
        assert!(active_sessions.try_acquire(1).is_some());
    }

    #[test]
    fn storage_node_server_release_removes_completed_read_operation() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first_acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first_acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 1);

        let release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);

        let second_acquire = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );
        let success_payload = decode_storage_rpc_response_payload(&second_acquire.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![second_location]);
        assert_eq!(server.read_handle_count(first_location), 0);
        assert_eq!(server.read_handle_count(second_location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(second_location), 0);
    }

    #[test]
    fn storage_node_server_rejects_read_operation_id_reuse_for_different_locations() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );

        let error = decode_storage_rpc_response_payload(&second.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert_eq!(server.read_handle_count(first_location), 1);
        assert_eq!(server.read_handle_count(second_location), 0);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);
    }
}
