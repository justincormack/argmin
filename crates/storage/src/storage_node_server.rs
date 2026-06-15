use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{ClusterRuntimeMapSnapshot, NodeHeartbeat, PgRouteSnapshot};
use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError};
use crate::metadata_command::{MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload};
use crate::node::SharedStorageNode;
use crate::node_client::{
    BucketMetadataNodeClient, BucketWriteReservationNodeClient,
    BuildAbortMultipartUploadCommandReq, BuildAuthorizedAbortMultipartUploadCommandReq,
    BuildCompleteMultipartObjectCommandReq, BuildCreateMultipartUploadCommandReq,
    BuildCreateStreamUploadCommandReq, BuildDeleteCurrentObjectCommandReq,
    BuildDeleteSpecificObjectVersionCommandReq, BuildDirectPutCommitCommandReq,
    BuildInsertDeleteMarkerCommandReq, BuildPutObjectMetadataCommandReq,
    BuildStreamPartCommitCommandReq, BuildStreamPutCommitCommandReq, CreateBucketCommandBuild,
    CreateStreamUploadPrecondition, DirectPutMetadataNodeClient, InsertDeleteMarkerStalePayload,
    LocalStorageNodeClient, MarkBucketDeletingCommandBuild, ObjectGenerationMetadataNodeClient,
    ObjectListingMetadataNodeClient, ObjectMutationMetadataNodeClient,
    ObjectReadMetadataNodeClient, ObjectVersionMetadataNodeClient, ShardScavengerNodeClient,
};
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
    decode_metadata_command_matching_applied_request, decode_metadata_command_next_id_request,
    decode_metadata_command_pending_slot_replace_request,
    decode_metadata_command_pending_slot_request, decode_metadata_command_request,
    decode_metadata_command_state_request, decode_multipart_completion_preflight_request,
    decode_multipart_completion_snapshot_request, decode_multipart_parts_list_request,
    decode_multipart_upload_load_request, decode_multipart_upload_match_request,
    decode_object_delete_snapshot_request, decode_object_generation_reservation_request,
    decode_object_payload_reclaim_claim_acquire_request,
    decode_object_payload_reclaim_claim_record_request,
    decode_object_payload_reclaim_exists_request, decode_object_read_auth_subject_request,
    decode_object_read_snapshot_request, decode_object_request,
    decode_object_tags_for_subject_request, decode_proof_release_request,
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
    encode_completed_multipart_order_command_build_response,
    encode_completed_multipart_uploads_list_response, encode_create_bucket_command_build_response,
    encode_direct_put_command_build_response, encode_direct_put_commit_snapshot_response,
    encode_health_response, encode_lifecycle_sweep_buckets_response,
    encode_lifecycle_sweep_claim_optional_record_response,
    encode_lifecycle_sweep_claim_record_response, encode_lifecycle_sweep_roots_response,
    encode_list_multipart_uploads_response, encode_list_object_versions_response,
    encode_list_objects_response, encode_metadata_command_acceptance_response,
    encode_metadata_command_applied_hashes_response, encode_metadata_command_bool_outcome_response,
    encode_metadata_command_bool_response, encode_metadata_command_max_log_index_response,
    encode_metadata_command_next_id_response, encode_metadata_command_pending_envelope_response,
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
    encode_put_object_metadata_snapshot_response, encode_read_handle_acquire_response,
    encode_read_handle_release_response, encode_scavenger_list_files_response,
    encode_scavenger_observations_response, encode_scavenger_payload_references_response,
    encode_scavenger_shard_rows_response, encode_shard_ack_item_response,
    encode_shard_read_range_response, encode_shard_read_response, encode_shard_write_ack,
    encode_storage_rpc_error_response, encode_storage_rpc_success_response,
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
    StorageRpcBucketWriteReservationsListResponse, StorageRpcCompleteMultipartCommandBuildRequest,
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
    StorageRpcMetadataCommandBoolResponse, StorageRpcMetadataCommandMatchingAppliedRequest,
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
    StorageRpcPayloadReclaimRootResponse, StorageRpcProofReleaseRequest,
    StorageRpcPutObjectMetadataCommandBuildRequest, StorageRpcPutObjectMetadataSnapshotOutcome,
    StorageRpcPutObjectMetadataSnapshotRequest, StorageRpcPutObjectMetadataSnapshotResponse,
    StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleAcquireResponse,
    StorageRpcReadHandleReleaseRequest, StorageRpcReadHandleReleaseResponse,
    StorageRpcScavengerListFilesRequest, StorageRpcScavengerObservationKeyRequest,
    StorageRpcScavengerObservationRecordRequest, StorageRpcShardAckBatchRequest,
    StorageRpcShardAckItem, StorageRpcShardAckItemRequest, StorageRpcShardDeleteRequest,
    StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest, StorageRpcShardWriteRequest,
    StorageRpcStreamError, StorageRpcStreamPartCommitCommandBuildRequest,
    StorageRpcStreamPartFinalizeSnapshotRequest, StorageRpcStreamPartFinalizeSnapshotResponse,
    StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcStreamPutFinalizeSnapshotRequest,
    StorageRpcStreamPutFinalizeSnapshotResponse, StorageRpcStreamSegmentAppendPrepareOutcome,
    StorageRpcStreamSegmentAppendPrepareRequest, StorageRpcStreamSegmentAppendPrepareResponse,
    StorageRpcStreamUploadMatchRequest, StorageRpcStreamUploadMatchResponse,
    StorageRpcStreamUploadSegmentsOutcome, StorageRpcStreamUploadSegmentsResponse,
    StorageRpcStreamUploadSessionOutcome, StorageRpcStreamUploadSessionRequest,
    StorageRpcStreamUploadSessionResponse, StorageRpcStreamUploadsListRequest,
    StorageRpcStreamUploadsListResponse, StorageRpcStreamUploadsPgListRequest,
    STORAGE_RPC_FRAME_ENCODING_VERSION,
};
use crate::traits::ShardStore;
use crate::types::{BucketState, ClusterEpoch, GenerationId, PgId, PgState, SessionId, WriteAck};
use crate::{
    BucketName, BucketWriteDrainError, EcShape, NodeId, ObjectPgActionError, ShardKey,
    ShardLocation,
};

#[cfg(test)]
type MetadataCommandBeforeWaitHook = Arc<dyn Fn(PgId) + Send + Sync>;

const DATA_DIR_LOCK_FILE: &str = ".argmin-storage-node.lock";
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
    pub data_dir: PathBuf,
    pub default_ec_shape: EcShape,
    pub pg_ids: Vec<u32>,
    pub socket_path: PathBuf,
    pub pg_routes: Vec<StorageNodePgRoute>,
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
            .filter(|route| route.acting_set().contains(&node_id))
            .map(StorageNodePgRoute::from)
            .collect();
        let pg_ids: Vec<u32> = pg_routes.iter().map(|route| route.pg_id).collect();
        validate_pg_ids(&pg_ids)?;
        validate_pg_routes(&pg_ids, &pg_routes)?;

        Ok(Self {
            node_id,
            cluster_epoch: runtime_map.cluster_epoch(),
            data_dir: data_dir.into(),
            default_ec_shape,
            pg_ids,
            socket_path: PathBuf::from(node.endpoint()),
            pg_routes,
        })
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
    #[error("failed to open storage node: {0}")]
    Store(#[from] StoreError),
    #[error("storage RPC stream error: {message}")]
    RpcStream { message: String },
    #[error("storage RPC response payload error: {message}")]
    ResponsePayload { message: String },
    #[error("storage-node active session limit {limit} is exhausted")]
    TooManyActiveSessions { limit: usize },
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
    config: StorageNodeProcessConfig,
    _data_dir_lock: StorageNodeDataDirLock,
    _node: Arc<SharedStorageNode>,
    listener: UnixListener,
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    active_sessions: Arc<StorageNodeActiveSessions>,
    metadata_command_locks: StorageNodeMetadataCommandLocks,
}

impl StorageNodeServer {
    pub fn bind(config: StorageNodeProcessConfig) -> Result<Self, StorageNodeServerError> {
        validate_pg_ids(&config.pg_ids)?;
        validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
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
            config,
            _data_dir_lock: data_dir_lock,
            _node: Arc::new(node),
            listener,
            read_handles: Arc::new(Mutex::new(StorageNodeReadHandleState::default())),
            active_sessions: Arc::new(StorageNodeActiveSessions::default()),
            metadata_command_locks: StorageNodeMetadataCommandLocks::default(),
        })
    }

    pub fn accept_one(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (mut stream, _) =
            self.listener
                .accept()
                .map_err(|source| StorageNodeServerError::Io {
                    context: "accept storage-node connection",
                    path: self.config.socket_path.clone(),
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

    fn accept_and_spawn(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (mut stream, _) =
            self.listener
                .accept()
                .map_err(|source| StorageNodeServerError::Io {
                    context: "accept storage-node connection",
                    path: self.config.socket_path.clone(),
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
            config: self.config.clone(),
            node: Arc::clone(&self._node),
            read_handles: Arc::clone(&self.read_handles),
            metadata_command_locks: self.metadata_command_locks.clone(),
        }
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
            StorageRpcMessageKind::ShardRead => match decode_shard_read_request(&frame.payload) {
                Ok(request) => self.shard_read_response(request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == request.pg_id.get())
            .expect("validated proof-release PG route must exist");
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_lifecycle_sweep_claim_route(&request, "lifecycle sweep claim release")
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
            Ok((target, segment)) => StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
                target,
                segment: Box::new(segment),
            },
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
        let expected_object_parts: Vec<crate::types::ObjectPartRecord> = request
            .request
            .part_records
            .iter()
            .map(|part| crate::types::ObjectPartRecord {
                bucket: request.request.bucket.clone(),
                key: request.request.key.clone(),
                version_id: request.version_id,
                part_number: part.part_number,
                size: part.size,
                etag: part.etag.clone(),
                etag_kind: part.etag_kind,
                part_okh: part.part_okh,
                part_vid: part.part_vid,
                ec_k: part.ec_k,
                ec_m: part.ec_m,
                data_pg_id: self
                    .node
                    .pg_topology()
                    .object_generation_multipart_part_data_pg(
                        &request.request.bucket,
                        &request.request.key,
                        request.request.generation_id,
                        part.part_number,
                    )
                    .get(),
                checksum: part.checksum.clone(),
            })
            .collect();
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

    fn shard_read_response(
        &self,
        request: StorageRpcShardReadRequest,
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
        if let Err(error) = self.validate_shard_location(request.location) {
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

    fn shard_ack_delete_response(
        &self,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg(request.pg_id, "shard ack delete") {
            return encode_storage_rpc_error_response(&error);
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

    fn metadata_command_replica_state_response(
        &self,
        session: &StorageNodeSession<'_>,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
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

    fn validate_pg_route(
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
        if route.state != PgState::Active {
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

fn store_error_response(error: StoreError) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::Internal,
        message: error.to_string(),
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
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::Internal,
        message: error.to_string(),
    }
}

fn object_pg_error_response(error: ObjectPgActionError) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::Internal,
        message: error.to_string(),
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
        let _ = fs::remove_file(&self.config.socket_path);
    }
}

struct StorageNodeDataDirLock {
    _file: File,
}

impl StorageNodeDataDirLock {
    fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        fs::create_dir_all(data_dir).map_err(|source| StorageNodeServerError::Io {
            context: "create storage-node data directory",
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
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_dir() || mode & 0o077 != 0 {
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
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::metadata_command::{
        BucketWriteReservationProof, CommitDirectPutObjectCommand, CreateBucketCommand,
        DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
        MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
        MetadataCommandPayload, PutBucketAclCommand, ReserveObjectGenerationCommand,
        ReserveObjectVersionCommand,
    };
    use crate::storage_rpc::{
        decode_bucket_mark_deleting_command_build_response, decode_health_response,
        decode_metadata_command_acceptance_response,
        decode_metadata_command_applied_hashes_response,
        decode_metadata_command_bool_outcome_response,
        decode_metadata_command_max_log_index_response, decode_metadata_command_next_id_response,
        decode_metadata_command_pending_envelope_response,
        decode_metadata_command_pending_slot_insert_response,
        decode_metadata_command_pending_slot_remove_response,
        decode_metadata_command_state_outcome_response, decode_metadata_command_state_response,
        decode_read_handle_acquire_response, decode_read_handle_release_response,
        decode_scavenger_list_files_response, decode_shard_ack_item_response,
        decode_shard_read_range_response, decode_shard_read_response, decode_shard_write_ack,
        decode_storage_rpc_response_payload, encode_bucket_mark_deleting_command_build_request,
        encode_bucket_pg_request, encode_metadata_command_matching_applied_request,
        encode_metadata_command_next_id_request, encode_metadata_command_pending_slot_request,
        encode_metadata_command_request, encode_metadata_command_state_request,
        encode_read_handle_acquire_request, encode_read_handle_release_request,
        encode_scavenger_list_files_request, encode_scavenger_observation_key_request,
        encode_scavenger_observation_record_request, encode_shard_ack_batch_request,
        encode_shard_ack_item_request, encode_shard_delete_request,
        encode_shard_read_range_request, encode_shard_read_request, encode_shard_write_request,
        encode_storage_rpc_frame, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
        StorageRpcBucketMarkDeletingCommandBuildOutcome,
        StorageRpcBucketMarkDeletingCommandBuildRequest, StorageRpcBucketPgRequest,
        StorageRpcBucketRequest, StorageRpcMetadataCommandAcceptanceOutcome,
        StorageRpcMetadataCommandMatchingAppliedRequest, StorageRpcMetadataCommandNextIdRequest,
        StorageRpcMetadataCommandPendingSlotInsertOutcome,
        StorageRpcMetadataCommandPendingSlotRequest, StorageRpcMetadataCommandRequest,
        StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateRequest,
        StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleReleaseRequest,
        StorageRpcScavengerListFilesRequest, StorageRpcScavengerObservationKeyRequest,
        StorageRpcScavengerObservationRecordRequest, StorageRpcShardAckBatchRequest,
        StorageRpcShardAckItem, StorageRpcShardAckItemRequest, StorageRpcShardDeleteRequest,
        StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest, StorageRpcShardWriteRequest,
    };
    use crate::traits::{PgMetadataStore, ShardStore};
    use crate::types::{
        DataPgId, GenerationId, PgId, ShardIndex, ShardKey, ShardScavengerObservationKey,
        ShardScavengerObservationReason, ShardScavengerObservationRecord, VersionId,
    };

    fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
        StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
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
    fn storage_node_server_rejects_read_handle_acquire_for_wrong_route_epoch() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::WrongClusterEpoch);
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
