// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use placement::NodeId;
use s3_types::{AclGrants, BucketVersioningState};

use super::engine::SharedStorageNode;
use super::BucketPgId;
use super::CreateBucketCommandBuildAuthority;
use super::MetadataCommandDecodeAuthority;
use super::ObjectMetadataPgId;
use super::ObjectMetadataScanPgId;
use super::PreparedRetainedStreamUploadAbort;
use crate::control_plane::{CanonicalStateDigest, MetadataCommandLogHash, PgMetadataReadRoute};
use crate::error::{BucketSnapshotLoadError, MetadataError, ObjectPgActionError, StoreError};
use crate::metadata_command::{
    AbortMultipartUploadCommand, AdvanceMultipartCompletionBarrierCommand, BucketPropertyMutation,
    BucketRecord, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MarkBucketDeletingCommand, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandLogHashRangeEntry, MetadataCommandLogIndex,
    MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind, MetadataCommandPayload,
    MetadataCommandReplicaState, MetadataTransferCommand, ObjectPayloadReclaimClaimProof,
    ObjectPayloadReclaimCommand, PutBucketAclCommand, PutBucketPropertyCommand,
    PutBucketSubresourceCommand, PutBucketVersioningCommand, PutObjectMetadataCommand,
    PutObjectMetadataMutation, COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::pg_store::{
    MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus, PgStore, ScavengerShardFileScan,
    ScavengerShardRow,
};
use crate::node_runtime::traits::{
    DurableBucketWriteReservationAcquire, DurableBucketWriteReservationHeartbeat, PgMetadataStore,
    ShardStore,
};
use crate::pg_topology::PgTopology;
use crate::storage_rpc::{
    decode_abort_multipart_cleanup_response, decode_aborting_multipart_upload_buckets_response,
    decode_bucket_delete_attempt_outcome_optional_record_response,
    decode_bucket_delete_begin_roots_response,
    decode_bucket_delete_finalize_claim_optional_record_response,
    decode_bucket_delete_finalize_roots_response, decode_bucket_execution_generations_response,
    decode_bucket_fast_path_identities_response, decode_bucket_info_outcome_response,
    decode_bucket_list_response, decode_bucket_mark_deleting_command_build_response,
    decode_bucket_metadata_control_command_build_response, decode_bucket_snapshot_response,
    decode_bucket_subresource_get_response, decode_bucket_write_drain_begin_response,
    decode_bucket_write_drain_optional_record_response,
    decode_bucket_write_reservation_record_response, decode_create_bucket_command_build_response,
    decode_direct_put_command_build_response, decode_direct_put_commit_snapshot_response,
    decode_historical_shard_read_response, decode_lifecycle_sweep_buckets_response,
    decode_lifecycle_sweep_claim_optional_record_response,
    decode_lifecycle_sweep_claim_record_response, decode_lifecycle_sweep_roots_response,
    decode_list_multipart_uploads_response, decode_list_object_versions_response,
    decode_list_objects_response, decode_metadata_command_acceptance_response,
    decode_metadata_command_applied_hashes_response, decode_metadata_command_bool_outcome_response,
    decode_metadata_command_bool_response, decode_metadata_command_checkpoint_candidates_response,
    decode_metadata_command_checkpoint_response, decode_metadata_command_log_compact_response,
    decode_metadata_command_log_entry_range_response,
    decode_metadata_command_log_hash_range_response,
    decode_metadata_command_max_log_index_response, decode_metadata_command_next_id_response,
    decode_metadata_command_pending_envelope_response,
    decode_metadata_command_pending_slot_cleanup_response,
    decode_metadata_command_pending_slot_insert_response,
    decode_metadata_command_pending_slot_remove_response,
    decode_metadata_command_state_outcome_response, decode_metadata_command_state_response,
    decode_multipart_completion_barrier_command_build_response,
    decode_multipart_completion_preflight_response, decode_multipart_completion_snapshot_response,
    decode_multipart_completion_stale_source_response, decode_multipart_management_lookup_response,
    decode_multipart_parts_list_response, decode_multipart_upload_load_response,
    decode_multipart_upload_match_response, decode_object_delete_snapshot_response,
    decode_object_generation_reservation_response, decode_object_generation_response,
    decode_object_lifecycle_version_list_response, decode_object_metadata_command_build_response,
    decode_object_payload_lease_control_response,
    decode_object_payload_reclaim_claim_optional_record_response,
    decode_object_payload_reclaim_response, decode_object_read_auth_subject_response,
    decode_object_read_snapshot_response, decode_object_version_response,
    decode_payload_reclaim_root_response, decode_placed_segment_backfill_reference_page_response,
    decode_placed_segment_shard_backfill_claim_optional_record_response,
    decode_placed_segment_shard_backfill_count_response,
    decode_placed_segment_shard_backfills_response,
    decode_placed_segment_shard_repair_claim_optional_record_response,
    decode_placed_segment_shard_repairs_response, decode_put_object_metadata_snapshot_response,
    decode_read_handle_acquire_response, decode_read_handle_release_response,
    decode_scavenger_list_files_response, decode_scavenger_observations_response,
    decode_scavenger_payload_references_response, decode_scavenger_shard_rows_response,
    decode_shard_ack_item_response, decode_shard_read_range_response, decode_shard_read_response,
    decode_shard_write_ack, decode_storage_rpc_response_payload,
    decode_storage_rpc_response_payload_with_connection_disposition,
    decode_stream_part_finalize_snapshot_response, decode_stream_put_finalize_snapshot_response,
    decode_stream_segment_append_prepare_response, decode_stream_upload_match_response,
    decode_stream_upload_segments_response, decode_stream_upload_session_response,
    decode_stream_uploads_list_response, encode_abort_multipart_cleanup_request,
    encode_abort_multipart_command_build_request,
    encode_authorized_abort_multipart_command_build_request, encode_bucket_batch_request,
    encode_bucket_delete_attempt_outcome_record_request, encode_bucket_delete_begin_roots_request,
    encode_bucket_delete_finalize_claim_acquire_request,
    encode_bucket_delete_finalize_claim_record_request,
    encode_bucket_delete_finalize_roots_request, encode_bucket_list_request,
    encode_bucket_mark_deleting_command_build_request,
    encode_bucket_metadata_control_command_build_request,
    encode_bucket_metadata_control_pending_match_request, encode_bucket_pg_request,
    encode_bucket_request, encode_bucket_snapshot_request, encode_bucket_subresource_get_request,
    encode_bucket_write_drain_begin_request, encode_bucket_write_drain_clear_expired_request,
    encode_bucket_write_drain_heartbeat_request, encode_bucket_write_drain_record_request,
    encode_bucket_write_reservation_acquire_request,
    encode_bucket_write_reservation_heartbeat_request,
    encode_bucket_write_reservation_proof_request, encode_bucket_write_reservation_record_request,
    encode_complete_multipart_command_build_request, encode_create_bucket_command_build_request,
    encode_create_multipart_upload_command_build_request,
    encode_create_stream_upload_command_build_request,
    encode_delete_current_object_command_build_request,
    encode_delete_specific_object_command_build_request, encode_direct_put_command_build_request,
    encode_direct_put_commit_snapshot_request, encode_historical_shard_read_request,
    encode_insert_delete_marker_command_build_request,
    encode_lifecycle_sweep_claim_acquire_request, encode_lifecycle_sweep_claim_error_request,
    encode_lifecycle_sweep_claim_heartbeat_request, encode_lifecycle_sweep_claim_record_request,
    encode_lifecycle_sweep_roots_request, encode_list_multipart_uploads_request,
    encode_list_object_versions_request, encode_list_objects_request,
    encode_metadata_command_checkpoint_candidates_request,
    encode_metadata_command_log_hash_range_request,
    encode_metadata_command_matching_applied_request, encode_metadata_command_next_id_request,
    encode_metadata_command_pending_slot_replace_request,
    encode_metadata_command_pending_slot_request,
    encode_metadata_command_recovery_pending_slot_replace_request,
    encode_metadata_command_recovery_request, encode_metadata_command_request,
    encode_metadata_command_state_request, encode_metadata_command_transfer_adopt_request,
    encode_metadata_command_transfer_checkpoint_base_request,
    encode_metadata_command_transfer_empty_state_request,
    encode_metadata_command_transfer_matching_state_request,
    encode_multipart_completion_barrier_command_build_request,
    encode_multipart_completion_preflight_request, encode_multipart_completion_snapshot_request,
    encode_multipart_parts_list_request, encode_multipart_upload_load_request,
    encode_multipart_upload_match_request, encode_object_delete_snapshot_request,
    encode_object_generation_reservation_request, encode_object_payload_lease_control_request,
    encode_object_payload_reclaim_claim_acquire_request,
    encode_object_payload_reclaim_claim_record_request,
    encode_object_payload_reclaim_command_build_request,
    encode_object_payload_reclaim_exists_request, encode_object_read_auth_subject_request,
    encode_object_read_snapshot_request, encode_object_request,
    encode_placed_segment_backfill_reference_page_request,
    encode_placed_segment_shard_backfill_claim_acquire_request,
    encode_placed_segment_shard_backfill_claim_error_request,
    encode_placed_segment_shard_backfill_claim_record_request,
    encode_placed_segment_shard_backfill_item_request,
    encode_placed_segment_shard_backfill_record_request,
    encode_placed_segment_shard_repair_claim_acquire_request,
    encode_placed_segment_shard_repair_claim_error_request,
    encode_placed_segment_shard_repair_claim_record_request,
    encode_placed_segment_shard_repair_item_request,
    encode_placed_segment_shard_repair_record_request, encode_proof_release_request,
    encode_put_object_metadata_command_build_request, encode_put_object_metadata_snapshot_request,
    encode_read_handle_acquire_request, encode_read_handle_release_request,
    encode_scavenger_list_files_request, encode_scavenger_observation_key_request,
    encode_scavenger_observation_record_request, encode_shard_ack_batch_request,
    encode_shard_ack_item_request, encode_shard_delete_request, encode_shard_read_range_request,
    encode_shard_read_request, encode_shard_write_request,
    encode_stream_part_commit_command_build_request, encode_stream_part_finalize_snapshot_request,
    encode_stream_put_commit_command_build_request, encode_stream_put_finalize_snapshot_request,
    encode_stream_segment_append_prepare_request,
    encode_stream_upload_bucket_write_reservation_update_request,
    encode_stream_upload_match_request, encode_stream_upload_session_request,
    encode_stream_uploads_list_request, encode_stream_uploads_pg_list_request,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    StorageRpcAbortMultipartCleanupRequest, StorageRpcAbortMultipartCommandBuildRequest,
    StorageRpcAdmittedRouteEffectDeadline, StorageRpcAuthorizedAbortMultipartCommandBuildRequest,
    StorageRpcBucketBatchRequest, StorageRpcBucketDeleteAttemptOutcomeRecordRequest,
    StorageRpcBucketDeleteBeginRootsRequest, StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcBucketDeleteFinalizeRootsRequest,
    StorageRpcBucketInfoOutcome, StorageRpcBucketListRequest,
    StorageRpcBucketMarkDeletingCommandBuildOutcome,
    StorageRpcBucketMarkDeletingCommandBuildRequest,
    StorageRpcBucketMetadataControlCommandBuildRequest, StorageRpcBucketMetadataControlMutation,
    StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcBucketPgRequest,
    StorageRpcBucketRequest, StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotRequest,
    StorageRpcBucketSubresourceGetOutcome, StorageRpcBucketSubresourceGetRequest,
    StorageRpcBucketWriteDrainBeginOutcome, StorageRpcBucketWriteDrainBeginRequest,
    StorageRpcBucketWriteDrainClearExpiredRequest, StorageRpcBucketWriteDrainHeartbeatRequest,
    StorageRpcBucketWriteDrainRecordRequest, StorageRpcBucketWriteReservationAcquireOutcome,
    StorageRpcBucketWriteReservationAcquireRequest,
    StorageRpcBucketWriteReservationHeartbeatRequest, StorageRpcBucketWriteReservationProofRequest,
    StorageRpcBucketWriteReservationRecordRequest, StorageRpcCompleteMultipartCommandBuildRequest,
    StorageRpcCreateBucketCommandBuildOutcome, StorageRpcCreateBucketCommandBuildRequest,
    StorageRpcCreateBucketConfig, StorageRpcCreateMultipartUploadCommandBuildRequest,
    StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcCreateStreamUploadPrecondition,
    StorageRpcDeleteCurrentObjectCommandBuildRequest,
    StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcDirectPutCommandBuildOutcome,
    StorageRpcDirectPutCommandBuildRequest, StorageRpcDirectPutCommitSnapshotRequest,
    StorageRpcErrorCode, StorageRpcErrorResponse, StorageRpcFrame,
    StorageRpcHistoricalShardReadRequest, StorageRpcInsertDeleteMarkerCommandBuildRequest,
    StorageRpcInsertDeleteMarkerStalePayload, StorageRpcLifecycleSweepClaimAcquireRequest,
    StorageRpcLifecycleSweepClaimErrorRequest, StorageRpcLifecycleSweepClaimHeartbeatRequest,
    StorageRpcLifecycleSweepClaimRecordRequest, StorageRpcLifecycleSweepRootsRequest,
    StorageRpcListMultipartUploadsRequest, StorageRpcListObjectVersionsRequest,
    StorageRpcListObjectsRequest, StorageRpcMessageKind,
    StorageRpcMetadataCommandAcceptanceOutcome, StorageRpcMetadataCommandAppliedHashesOutcome,
    StorageRpcMetadataCommandBoolOutcome, StorageRpcMetadataCommandCheckpointCandidatesRequest,
    StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcMetadataCommandMatchingAppliedRequest,
    StorageRpcMetadataCommandNextIdOutcome, StorageRpcMetadataCommandNextIdRequest,
    StorageRpcMetadataCommandPendingSlotCleanupOutcome,
    StorageRpcMetadataCommandPendingSlotInsertOutcome,
    StorageRpcMetadataCommandPendingSlotReplaceRequest,
    StorageRpcMetadataCommandPendingSlotRequest,
    StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest,
    StorageRpcMetadataCommandRecoveryRequest, StorageRpcMetadataCommandRequest,
    StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateRequest,
    StorageRpcMetadataCommandTransferAdoptRequest,
    StorageRpcMetadataCommandTransferCheckpointBaseRequest,
    StorageRpcMetadataCommandTransferEmptyStateRequest,
    StorageRpcMetadataCommandTransferMatchingStateRequest,
    StorageRpcMultipartCompletionBarrierCommandBuildRequest,
    StorageRpcMultipartCompletionPreflightOutcome, StorageRpcMultipartCompletionPreflightRequest,
    StorageRpcMultipartCompletionSnapshotOutcome, StorageRpcMultipartCompletionSnapshotRequest,
    StorageRpcMultipartPartsListOutcome, StorageRpcMultipartPartsListRequest,
    StorageRpcMultipartUploadLoadOutcome, StorageRpcMultipartUploadLoadRequest,
    StorageRpcMultipartUploadMatchRequest, StorageRpcObjectDeleteSnapshotRequest,
    StorageRpcObjectDeleteSnapshotResponse, StorageRpcObjectGenerationReservationOutcome,
    StorageRpcObjectGenerationReservationRequest, StorageRpcObjectMetadataCommandBuildOutcome,
    StorageRpcObjectPayloadLeaseControlOperation, StorageRpcObjectPayloadLeaseControlRequest,
    StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    StorageRpcObjectPayloadReclaimClaimRecordRequest,
    StorageRpcObjectPayloadReclaimCommandBuildRequest, StorageRpcObjectPayloadReclaimExistsRequest,
    StorageRpcObjectReadAuthSubjectOutcome, StorageRpcObjectReadAuthSubjectRequest,
    StorageRpcObjectReadSnapshotOutcome, StorageRpcObjectReadSnapshotRequest,
    StorageRpcObjectRequest, StorageRpcOperationDeadline,
    StorageRpcPlacedSegmentBackfillReferencePageRequest,
    StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
    StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
    StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
    StorageRpcPlacedSegmentShardBackfillItemRequest,
    StorageRpcPlacedSegmentShardBackfillRecordRequest,
    StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
    StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
    StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
    StorageRpcPlacedSegmentShardRepairItemRequest, StorageRpcPlacedSegmentShardRepairRecordRequest,
    StorageRpcProofReleaseRequest, StorageRpcPutObjectMetadataCommandBuildRequest,
    StorageRpcPutObjectMetadataSnapshotOutcome, StorageRpcPutObjectMetadataSnapshotRequest,
    StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleReleaseRequest,
    StorageRpcScavengerListFilesRequest, StorageRpcScavengerObservationKeyRequest,
    StorageRpcScavengerObservationRecordRequest, StorageRpcShardAckBatchRequest,
    StorageRpcShardAckItem, StorageRpcShardAckItemRequest, StorageRpcShardDeleteRequest,
    StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest, StorageRpcShardWriteRequest,
    StorageRpcStreamError, StorageRpcStreamPartCommitCommandBuildRequest,
    StorageRpcStreamPartFinalizeSnapshotOutcome, StorageRpcStreamPartFinalizeSnapshotRequest,
    StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcStreamPutFinalizeSnapshotRequest,
    StorageRpcStreamSegmentAppendPrepareOutcome, StorageRpcStreamSegmentAppendPrepareRequest,
    StorageRpcStreamUploadBucketWriteReservationUpdateRequest, StorageRpcStreamUploadMatchRequest,
    StorageRpcStreamUploadSegmentsOutcome, StorageRpcStreamUploadSessionOutcome,
    StorageRpcStreamUploadSessionRequest, StorageRpcStreamUploadsListRequest,
    StorageRpcStreamUploadsPgListRequest, STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT,
};
#[cfg(test)]
use crate::storage_rpc::{
    decode_cluster_map_history_reference_summary_response,
    encode_cluster_map_history_reference_summary_request,
    StorageRpcClusterMapHistoryReferenceSummaryRequest,
};
use crate::storage_rpc_auth::{
    read_storage_rpc_auth_transport_frame_with_limit,
    write_storage_rpc_auth_transport_frame_with_limit, StorageRpcClientAuthConfig,
    StorageRpcRequestProof,
};
use crate::storage_rpc_transport::{
    BoxStorageRpcStream, StorageRpcClientEndpoint, StorageRpcEndpointConnectFailure,
};
#[cfg(test)]
use crate::types::BucketSnapshotTagsRequest;
use crate::types::{
    AbortMultipartUploadCleanup, AbortingMultipartUploadBucketWitness, AdmittedRouteEffectFence,
    AuthorizedMultipartUploadRecord, BucketDeleteAttemptOutcomeRecord,
    BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketFastPathIdentity, BucketInfo,
    BucketName, BucketSnapshot, BucketSnapshotRequest, BucketState, BucketSubresourceKind,
    BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch, CommitDirectPutObjectReq,
    CompleteMultipartCommitCleanup, CompleteMultipartCommitRequest, CreateBucketConfig,
    CreateMultipartUploadReq, CreateStreamUploadReq, DirectPutCommitSnapshot,
    DirectPutCommitStorageSnapshot, EcShape, GenerationId, LifecycleSweepClaimRecord,
    LifecycleSweepRoot, ListMultipartUploadsReq, ListMultipartUploadsResp, ListObjectVersionsReq,
    ListObjectVersionsResp, ListObjectsReq, ListObjectsResp, ListPartsReq, ListedMultipartParts,
    LiveObjectRecord, MultipartCompletionPreflight, MultipartCompletionSnapshot,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimRecord,
    MultipartUploadManagementLookup, MultipartUploadRecord, ObjectEtag, ObjectKey, ObjectLayout,
    ObjectPartRecord, ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind,
    ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity, ObjectReadSnapshot,
    ObjectReadSnapshotMode, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, OwnerIdentity, PayloadReclaimRoot, PgId,
    PlacedSegmentBackfillReferenceCursor, PlacedSegmentBackfillReferencePage,
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardBackfillRecord, PlacedSegmentShardBackfillWorkItem,
    PlacedSegmentShardRepairClaimAcquire, PlacedSegmentShardRepairClaimRecord,
    PlacedSegmentShardRepairRecord, PlacedSegmentShardRepairWorkItem,
    PrepareStreamUploadSegmentAppendReq, PutLiveObjectReq, SessionId, ShardKey,
    ShardScavengerObservation, ShardScavengerObservationKey, ShardScavengerObservationRecord,
    ShardScavengerPayloadReference, StoredObject, StreamPutCommitInput,
    StreamPutFinalizeStorageSnapshot, StreamUploadCommandRecord, StreamUploadPartStorageSnapshot,
    StreamUploadRecord, StreamUploadRecordPage, StreamUploadSegmentRecord, StreamUploadState,
    StreamUploadTarget, TerminalStreamCleanupRecord, UploadId, UploadState, VersionId, WriteAck,
};
use crate::BucketDeleteBeginRoot;
use crate::DataPgId;

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

fn validate_placed_segment_shard_backfill_route(
    pg_id: PgId,
    work_item: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.data_pg_id != pg_id.get() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill work item data PG {} does not match routed PG {}",
                work_item.request.data_pg_id,
                pg_id.get()
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_epoch(
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardBackfillClaimRecord,
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

#[path = "node_client/interface.rs"]
mod interface;
#[path = "node_client/local.rs"]
mod local;
#[path = "node_client/unix_admission.rs"]
mod unix_admission;
#[path = "node_client/unix_helpers.rs"]
mod unix_helpers;
#[path = "node_client/unix_object_rpc.rs"]
mod unix_object_rpc;
#[path = "node_client/unix_rpc.rs"]
mod unix_rpc;
#[path = "node_client/unix_sessions.rs"]
mod unix_sessions;

pub(crate) use interface::*;

#[cfg(test)]
use unix_object_rpc::{
    validate_list_multipart_uploads_response, validate_list_object_versions_response,
    validate_list_objects_response,
};

#[cfg(test)]
pub(crate) use unix_admission::shared_unix_storage_node_rpc_admission_with_settings;
pub(crate) use unix_admission::{
    listing_probe_admission_class, shared_unix_storage_node_rpc_admission,
    storage_rpc_admission_class, UnixStorageNodeObjectPayloadLeaseAdmissionAcquire,
    UnixStorageNodeObjectPayloadLeaseAdmissionPermit, UnixStorageNodeRpcAdmission,
    UnixStorageNodeRpcAdmissionAcquire, UnixStorageNodeRpcAdmissionClass,
    UnixStorageNodeRpcAdmissionPermit, UnixStorageNodeRpcAdmissionSettings,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalUnixStorageNodeClientAdmissionSettings {
    pub rpc_admission_limit: usize,
    pub rpc_admission_wait_timeout: Duration,
    pub rpc_control_admission_wait_timeout: Duration,
}

impl LocalUnixStorageNodeClientAdmissionSettings {
    pub const DEFAULT: Self = Self {
        rpc_admission_limit: UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
        rpc_admission_wait_timeout: UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
        rpc_control_admission_wait_timeout:
            UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
    };

    pub fn rpc_admission_limit(self) -> usize {
        self.rpc_admission_limit
    }

    pub fn rpc_admission_wait_timeout(self) -> Duration {
        self.rpc_admission_wait_timeout
    }

    pub fn rpc_control_admission_wait_timeout(self) -> Duration {
        self.rpc_control_admission_wait_timeout
    }
}

impl From<LocalUnixStorageNodeClientAdmissionSettings> for UnixStorageNodeRpcAdmissionSettings {
    fn from(settings: LocalUnixStorageNodeClientAdmissionSettings) -> Self {
        Self {
            limit: settings.rpc_admission_limit,
            wait_timeout: settings.rpc_admission_wait_timeout,
            control_wait_timeout: settings.rpc_control_admission_wait_timeout,
        }
    }
}

fn storage_rpc_io_timeout(rpc_auth: Option<&StorageRpcClientAuthConfig>) -> Duration {
    rpc_auth
        .map(|auth| auth.transport_limits().io_timeout())
        .unwrap_or(STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT)
}

fn storage_rpc_response_error(
    node_id: NodeId,
    kind: StorageRpcMessageKind,
    error: StorageRpcErrorResponse,
) -> StoreError {
    match error.code {
        StorageRpcErrorCode::ShardDeleteInProgress => StoreError::StorageRpcShardDeleteInProgress {
            node_id: node_id.as_u32(),
            operation: kind.operation_name(),
            detail: crate::StorageNodeFailureDetail::new(error.message),
        },
        StorageRpcErrorCode::ResourceExhausted => StoreError::StorageRpcResourceExhausted {
            node_id: node_id.as_u32(),
            operation: kind.operation_name(),
            detail: crate::StorageNodeFailureDetail::new(error.message),
        },
        StorageRpcErrorCode::NotFound => StoreError::NotFound,
        code => StoreError::StorageRpc {
            node_id: node_id.as_u32(),
            operation: kind.operation_name(),
            failure: code,
            detail: crate::StorageNodeFailureDetail::new(error.message),
        },
    }
}

fn storage_rpc_stream_error(
    node_id: NodeId,
    operation: &'static str,
    error: StorageRpcStreamError,
) -> StoreError {
    let code = match &error {
        StorageRpcStreamError::Io(source)
            if matches!(
                source.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) =>
        {
            StorageRpcErrorCode::TransportTimeout
        }
        StorageRpcStreamError::Io(source)
            if matches!(
                source.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            StorageRpcErrorCode::TransportClosed
        }
        _ => StorageRpcErrorCode::PayloadDecode,
    };
    let _ = observability::emit_flight_event(
        "storage_rpc_client",
        "storage_rpc_client_stream_error",
        format!(
            "node_id={} operation={operation:?} failure={code:?} error={error}",
            node_id.as_u32()
        ),
    );
    StoreError::StorageRpc {
        node_id: node_id.as_u32(),
        operation,
        failure: code,
        detail: crate::StorageNodeFailureDetail::new(error.to_string()),
    }
}

pub(crate) fn complete_multipart_expected_object_parts(
    request: &CompleteMultipartCommitRequest,
    version_id: VersionId,
    topology: &PgTopology,
) -> Vec<ObjectPartRecord> {
    request
        .part_records
        .iter()
        .map(|part| ObjectPartRecord {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            version_id,
            part_number: part.part_number,
            size: part.size,
            payload_crc64: part.payload_crc64,
            etag: part.etag.clone(),
            etag_kind: part.etag_kind,
            part_vid: part.part_vid,
            placement_cluster_epoch: part.placement_cluster_epoch,
            ec_k: part.ec_k,
            ec_m: part.ec_m,
            data_pg_id: topology
                .object_generation_multipart_part_data_pg(
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                    part.part_number,
                )
                .get(),
            checksum: part.checksum.clone(),
        })
        .collect()
}

fn load_multipart_upload_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
) -> Result<MultipartUploadRecord, MetadataError> {
    let upload = pg.get_multipart_upload(upload_id)?;
    if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
        return Err(MetadataError::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    Ok(upload)
}

fn load_in_progress_multipart_upload_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
) -> Result<MultipartUploadRecord, MetadataError> {
    let upload = load_multipart_upload_from_pg(pg, bucket, key, upload_id)?;
    if upload.state != UploadState::InProgress {
        return Err(MetadataError::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    Ok(upload)
}

fn validate_stream_upload_session_binding(
    session: &StreamUploadRecord,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<(), ObjectPgActionError> {
    validate_stream_upload_session_bucket_key(session, bucket, key)?;
    if session.state != StreamUploadState::InProgress {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "stream session is not in progress".to_string(),
        });
    }
    Ok(())
}

fn validate_stream_upload_session_bucket_key(
    session: &StreamUploadRecord,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<(), ObjectPgActionError> {
    if session.bucket != bucket.as_str() || session.key != key.as_str() {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "session bucket/key mismatch".to_string(),
        });
    }
    Ok(())
}

fn stream_upload_matches_command(
    existing: &StreamUploadRecord,
    create: &CreateStreamUploadCommand,
) -> bool {
    StreamUploadCommandRecord::from(existing) == create.session
        && existing.next_segment_vid == create.initial_next_segment_vid
        && existing.cleanup_after == create.cleanup_after
        && stream_upload_bucket_write_reservation_matches_command(existing, create)
}

fn stream_upload_bucket_write_reservation_matches_command(
    existing: &StreamUploadRecord,
    create: &CreateStreamUploadCommand,
) -> bool {
    match create.session.target {
        StreamUploadTarget::PutObject => existing
            .bucket_write_reservation
            .as_ref()
            .is_some_and(|proof| proof.has_same_stable_identity(&create.bucket_write_reservation)),
        StreamUploadTarget::UploadPart { .. } => existing.bucket_write_reservation.is_none(),
    }
}

fn multipart_upload_matches_command(
    existing: &MultipartUploadRecord,
    create: &CreateMultipartUploadCommand,
) -> bool {
    existing == create.upload()
}

fn reject_duplicate_stream_segment_index(
    pg: &PgStore,
    session_id: &SessionId,
    segment_index: u32,
) -> Result<(), ObjectPgActionError> {
    let existing_segments = pg.list_stream_segments(session_id)?;
    if existing_segments
        .iter()
        .any(|segment| segment.segment_index == segment_index)
    {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: format!("duplicate segment_index {segment_index}"),
        });
    }
    Ok(())
}

fn validate_stream_put_finalize_session(
    session: &StreamUploadRecord,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<(), ObjectPgActionError> {
    validate_stream_upload_session_binding(session, bucket, key)?;
    if !matches!(session.target, StreamUploadTarget::PutObject) {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "session is not a PutObject session".to_string(),
        });
    }
    Ok(())
}

fn validate_stream_part_finalize_session(
    session: &StreamUploadRecord,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
    part_number: u32,
) -> Result<(), ObjectPgActionError> {
    validate_stream_upload_session_binding(session, bucket, key)?;
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

fn load_stream_put_finalize_snapshot_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    session_id: &SessionId,
) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
    let session = pg.get_stream_upload(session_id)?;
    validate_stream_put_finalize_session(&session, bucket, key)?;
    let existing_etag = match PgMetadataStore::get_object_meta(pg, bucket, key) {
        Ok(stored) => stored.as_live().map(|record| record.etag.format()),
        Err(MetadataError::ObjectNotFound) => None,
        Err(other) => return Err(other.into()),
    };
    let generation_id =
        PgMetadataStore::get_object_generation_reservation(pg, bucket, key, session_id)?;
    let (stale_payload_source, stale_payload) =
        snapshot_direct_put_stale_payload_for_snapshot(pg, bucket, key, 0)?;
    let staging_segments = pg.list_stream_segments(session_id)?;
    Ok(StreamPutFinalizeStorageSnapshot {
        session,
        existing_etag,
        generation_id,
        stale_payload_source,
        stale_payload,
        staging_segments,
    })
}

fn load_stream_part_finalize_snapshot_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
    session_id: &SessionId,
    part_number: u32,
) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
    let session = match pg.get_stream_upload(session_id) {
        Ok(session) => session,
        Err(error @ MetadataError::StreamSessionNotFound { .. }) => {
            // A terminal multipart command removes both records. Preserve a live
            // upload's missing-session error, but report the terminal upload state.
            load_in_progress_multipart_upload_from_pg(pg, bucket, key, upload_id)?;
            return Err(error.into());
        }
        Err(other) => return Err(other.into()),
    };
    validate_stream_part_finalize_session(&session, bucket, key, upload_id, part_number)?;
    let upload = load_in_progress_multipart_upload_from_pg(pg, bucket, key, upload_id)?;
    let existing_part = match PgMetadataStore::get_multipart_part(pg, upload_id, part_number) {
        Ok(existing) => Some(existing),
        Err(MetadataError::PartNotFound { .. }) => None,
        Err(other) => return Err(other.into()),
    };
    let existing_part_generation = existing_part.as_ref().map(|part| part.generation);
    let staging_segments = pg.list_stream_segments(session_id)?;
    let displaced_segments =
        PgMetadataStore::get_all_multipart_part_segments_for_upload(pg, upload_id)?
            .into_iter()
            .filter(|segment| segment.part_number == part_number)
            .collect::<Vec<_>>();
    Ok(StreamUploadPartStorageSnapshot {
        auth_snapshot: crate::StreamUploadPartSnapshot {
            session,
            upload,
            existing_part_generation,
            staging_segments,
        },
        existing_part,
        displaced_segments,
    })
}

fn load_direct_put_commit_snapshot_from_pg(
    pg: &PgStore,
    node_id: NodeId,
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
    generation_id: GenerationId,
) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
    let reserved_generation =
        match PgMetadataStore::get_object_generation_reservation(pg, bucket, key, reservation_id) {
            Ok(reserved_generation) => reserved_generation,
            Err(error @ MetadataError::ObjectGenerationReservationNotFound { .. }) => {
                let current = load_current_object_optional_from_pg(pg, bucket, key)?;
                if current
                    .as_ref()
                    .and_then(StoredObject::as_live)
                    .is_some_and(|live| live.generation_id == generation_id)
                {
                    let existing_etag = current
                        .as_ref()
                        .and_then(|stored| stored.as_live().map(|record| record.etag.format()));
                    let version_id = current
                        .as_ref()
                        .expect("matched current direct PUT object must be present")
                        .version_id();
                    return Ok(DirectPutCommitStorageSnapshot {
                        auth_snapshot: DirectPutCommitSnapshot { existing_etag },
                        committed_segments: Some(PgMetadataStore::get_object_segments(
                            pg, bucket, key, version_id,
                        )?),
                        committed_stale_generation_id:
                            applied_direct_put_stale_generation_id_from_log(
                                pg,
                                node_id,
                                bucket,
                                key,
                                reservation_id,
                                generation_id,
                            )?,
                        current,
                        stale_payload_source: None,
                        stale_payload: None,
                    });
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
    if reserved_generation != generation_id {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: format!(
                "object generation reservation mismatch: reserved {} but commit requested {}",
                reserved_generation.get(),
                generation_id.get()
            ),
        });
    }

    let current = load_current_object_optional_from_pg(pg, bucket, key)?;
    let existing_etag = current
        .as_ref()
        .and_then(|stored| stored.as_live().map(|record| record.etag.format()));
    let (stale_payload_source, stale_payload) =
        snapshot_direct_put_stale_payload_for_snapshot(pg, bucket, key, 0)?;
    Ok(DirectPutCommitStorageSnapshot {
        auth_snapshot: DirectPutCommitSnapshot { existing_etag },
        current,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source,
        stale_payload,
    })
}

fn applied_direct_put_stale_generation_id_from_log(
    pg: &PgStore,
    node_id: NodeId,
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
    generation_id: GenerationId,
) -> Result<Option<GenerationId>, StoreError> {
    let cluster_epoch = pg.metadata_command_replica_state()?.cluster_epoch;
    let max_log_index = pg.max_metadata_command_log_index(cluster_epoch)?;
    let Some(last_log_index) = MetadataCommandLogIndex::new(max_log_index) else {
        return Ok(None);
    };
    let entries = pg.retained_metadata_command_log_entries(
        node_id.as_u32(),
        cluster_epoch,
        MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        last_log_index,
    )?;
    for entry in entries.into_iter().rev() {
        let MetadataCommandLogRangeEntryKind::Applied(command) = entry.kind else {
            continue;
        };
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            continue;
        };
        if commit.matches_request(bucket, key, reservation_id, generation_id) {
            return Ok(commit.stale_payload.as_ref().map(|payload| match payload {
                ObjectPayloadReclaimCommand::Segments(reclaim) => reclaim.generation_id,
                ObjectPayloadReclaimCommand::Multipart(reclaim) => reclaim.generation_id,
            }));
        }
    }
    Ok(None)
}

fn snapshot_upload_part_stream_cleanup_from_pg(
    pg: &PgStore,
    upload_id: &UploadId,
) -> Result<
    (
        Vec<TerminalStreamCleanupRecord>,
        Vec<StreamUploadSegmentRecord>,
    ),
    ObjectPgActionError,
> {
    let mut stream_uploads = PgMetadataStore::list_all_stream_uploads(pg)?
        .into_iter()
        .filter(|session| {
            matches!(
                &session.target,
                StreamUploadTarget::UploadPart {
                    upload_id: session_upload_id,
                    ..
                } if session_upload_id == upload_id
            )
        })
        .collect::<Vec<_>>();
    stream_uploads.sort_by(|a, b| a.session_id.as_str().cmp(b.session_id.as_str()));

    let mut stream_upload_segments = Vec::new();
    for session in &stream_uploads {
        stream_upload_segments.extend(PgMetadataStore::list_stream_segments(
            pg,
            &session.session_id,
        )?);
    }

    Ok((
        stream_uploads
            .iter()
            .map(TerminalStreamCleanupRecord::from)
            .collect(),
        stream_upload_segments,
    ))
}

fn snapshot_complete_multipart_cleanup_from_pg(
    pg: &PgStore,
    upload_id: &UploadId,
    selected_part_numbers: &BTreeSet<u32>,
) -> Result<
    (
        Vec<MultipartPartSegmentRecord>,
        CompleteMultipartCommitCleanup,
    ),
    ObjectPgActionError,
> {
    let all_parts = PgMetadataStore::list_multipart_parts(
        pg,
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
        PgMetadataStore::get_all_multipart_part_segments_for_upload(pg, upload_id)?;
    let mut selected_streaming_segments = Vec::new();
    let mut omitted_streaming_segments = Vec::new();
    for segment in all_streaming_segments {
        if selected_part_numbers.contains(&segment.part_number) {
            selected_streaming_segments.push(segment);
        } else {
            omitted_streaming_segments.push(segment);
        }
    }
    let (stream_uploads, stream_upload_segments) =
        snapshot_upload_part_stream_cleanup_from_pg(pg, upload_id)?;

    Ok((
        selected_streaming_segments,
        CompleteMultipartCommitCleanup {
            omitted_parts,
            omitted_streaming_segments,
            stream_uploads,
            stream_upload_segments,
        },
    ))
}

fn snapshot_direct_put_stale_payload_command(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    created_at: u64,
) -> Result<Option<ObjectPayloadReclaimCommand>, MetadataError> {
    let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
        Ok(stored) => stored,
        Err(MetadataError::ObjectNotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    let record = match stored {
        StoredObject::Live(record) => record,
        StoredObject::DeleteMarker(_) => return Ok(None),
    };

    Ok(Some(snapshot_live_object_payload_reclaim_command(
        pg, bucket, key, &record, created_at,
    )?))
}

fn snapshot_direct_put_stale_payload_for_snapshot(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    created_at: u64,
) -> Result<(Option<StoredObject>, Option<ObjectPayloadReclaimCommand>), MetadataError> {
    let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
        Ok(stored) => stored,
        Err(MetadataError::ObjectNotFound) => return Ok((None, None)),
        Err(error) => return Err(error),
    };
    let StoredObject::Live(record) = &stored else {
        return Ok((None, None));
    };
    let reclaim =
        snapshot_live_object_payload_reclaim_command(pg, bucket, key, record, created_at)?;
    Ok((Some(stored), Some(reclaim)))
}

fn load_null_live_stale_payload_source_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<Option<StoredObject>, MetadataError> {
    let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
        Ok(stored) => stored,
        Err(MetadataError::ObjectNotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    match stored {
        StoredObject::Live(_) => Ok(Some(stored)),
        StoredObject::DeleteMarker(_) => Ok(None),
    }
}

fn normalize_reclaim_created_at(reclaim: &mut Option<ObjectPayloadReclaimCommand>) {
    match reclaim {
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => reclaim.created_at = 0,
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => reclaim.created_at = 0,
        None => {}
    }
}

fn terminal_stream_cleanup_rows_match(
    left_uploads: &[TerminalStreamCleanupRecord],
    right_uploads: &[TerminalStreamCleanupRecord],
    left_segments: &[StreamUploadSegmentRecord],
    right_segments: &[StreamUploadSegmentRecord],
) -> bool {
    left_uploads.len() == right_uploads.len()
        && left_uploads
            .iter()
            .zip(right_uploads)
            .all(|(left, right)| left == right)
        && left_segments.len() == right_segments.len()
        && left_segments
            .iter()
            .zip(right_segments)
            .all(|(left, right)| left == right)
}

fn normalize_delete_target_created_at(target: &mut Option<DeleteObjectVersionTarget>) {
    let Some(DeleteObjectVersionTarget::Live { payload, .. }) = target else {
        return;
    };
    let mut reclaim = Some(payload.clone());
    normalize_reclaim_created_at(&mut reclaim);
    if let Some(normalized) = reclaim {
        *payload = normalized;
    }
}

fn delete_target_matches_expected(
    actual: Option<&DeleteObjectVersionTarget>,
    expected: Option<&DeleteObjectVersionTarget>,
) -> bool {
    let mut actual = actual.cloned();
    let mut expected = expected.cloned();
    normalize_delete_target_created_at(&mut actual);
    normalize_delete_target_created_at(&mut expected);
    actual == expected
}

fn reclaim_matches_bucket_key(
    reclaim: Option<&ObjectPayloadReclaimCommand>,
    bucket: &BucketName,
    key: &ObjectKey,
) -> bool {
    match reclaim {
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => {
            reclaim.bucket == *bucket && reclaim.key == *key
        }
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
            reclaim.bucket == *bucket && reclaim.key == *key
        }
        None => true,
    }
}

fn reclaim_matches_bucket_key_generation(
    reclaim: Option<&ObjectPayloadReclaimCommand>,
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
) -> bool {
    match reclaim {
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => {
            reclaim.bucket == *bucket
                && reclaim.key == *key
                && reclaim.generation_id == generation_id
        }
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
            reclaim.bucket == *bucket
                && reclaim.key == *key
                && reclaim.generation_id == generation_id
        }
        None => true,
    }
}

fn reclaim_matches_snapshot_live_object(
    reclaim: Option<&ObjectPayloadReclaimCommand>,
    source: &Option<StoredObject>,
) -> bool {
    match (reclaim, source) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(_), Some(StoredObject::Live(live))) if !live.version_id.is_null() => false,
        (Some(reclaim), Some(StoredObject::Live(live))) => match (reclaim, live.layout) {
            (ObjectPayloadReclaimCommand::Segments(reclaim), ObjectLayout::Standard) => {
                reclaim.generation_id == live.generation_id
            }
            (
                ObjectPayloadReclaimCommand::Multipart(reclaim),
                ObjectLayout::MultipartManifest { .. },
            ) => reclaim.generation_id == live.generation_id,
            _ => false,
        },
        (Some(_), Some(StoredObject::DeleteMarker(_))) => false,
    }
}

fn lifecycle_sweep_claim_identity_matches(
    actual: &LifecycleSweepClaimRecord,
    expected: &LifecycleSweepClaimRecord,
) -> bool {
    actual.bucket == expected.bucket
        && actual.bucket_incarnation_generation == expected.bucket_incarnation_generation
        && actual.claim_id == expected.claim_id
        && actual.owner_token == expected.owner_token
        && actual.cluster_epoch == expected.cluster_epoch
        && actual.pg_id == expected.pg_id
        && actual.claimed_at == expected.claimed_at
}

fn snapshot_live_object_payload_reclaim_command(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    record: &LiveObjectRecord,
    created_at: u64,
) -> Result<ObjectPayloadReclaimCommand, MetadataError> {
    match record.layout {
        ObjectLayout::Standard => {
            let segments =
                PgMetadataStore::get_object_segments(pg, bucket, key, record.version_id)?;
            Ok(ObjectPayloadReclaimCommand::Segments(
                ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id: record.generation_id,
                    created_at,
                    segments: segments
                        .into_iter()
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
                },
            ))
        }
        ObjectLayout::MultipartManifest { .. } => {
            let parts = PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?;
            let mut streaming_segments = Vec::new();
            for part in &parts {
                streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                    pg,
                    bucket,
                    key,
                    record.version_id,
                    part.part_number,
                )?);
            }
            Ok(ObjectPayloadReclaimCommand::Multipart(
                MultipartReclaimRecord::from_object_parts(
                    bucket,
                    key,
                    record.generation_id,
                    created_at,
                    &parts,
                    &streaming_segments,
                ),
            ))
        }
    }
}

fn load_current_object_optional_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<Option<StoredObject>, MetadataError> {
    match PgMetadataStore::get_object_meta(pg, bucket, key) {
        Ok(stored) => Ok(Some(stored)),
        Err(MetadataError::ObjectNotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

fn load_object_version_optional_from_pg(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    version_id: VersionId,
) -> Result<Option<StoredObject>, MetadataError> {
    match PgMetadataStore::get_object_version(pg, bucket, key, version_id) {
        Ok(stored) => Ok(Some(stored)),
        Err(MetadataError::ObjectNotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

fn delete_command_target_from_stored(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    stored: Option<&StoredObject>,
) -> Result<Option<DeleteObjectVersionTarget>, MetadataError> {
    match stored {
        None => Ok(None),
        Some(StoredObject::DeleteMarker(marker)) => {
            let write_sequence = pg
                .object_write_sequence(bucket.as_str(), key.as_str(), marker.version_id)?
                .ok_or(MetadataError::ObjectNotFound)?;
            Ok(Some(DeleteObjectVersionTarget::DeleteMarker {
                write_sequence,
            }))
        }
        Some(StoredObject::Live(record)) => {
            Ok(Some(live_delete_command_target(pg, bucket, key, record)?))
        }
    }
}

fn load_object_delete_snapshot_from_stored(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    stored: Option<StoredObject>,
) -> Result<ObjectDeleteStorageSnapshot, MetadataError> {
    let target = delete_command_target_from_stored(pg, bucket, key, stored.as_ref())?;
    Ok(ObjectDeleteStorageSnapshot { stored, target })
}

fn live_delete_command_target(
    pg: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    record: &LiveObjectRecord,
) -> Result<DeleteObjectVersionTarget, MetadataError> {
    let payload = snapshot_live_object_payload_reclaim_command(
        pg,
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

fn pending_bucket_command_matches_current(
    current: BucketRecord,
    target: &BucketRecord,
    build_expected: impl FnOnce(BucketRecord) -> Result<BucketRecord, BucketSnapshotLoadError>,
) -> Result<bool, BucketSnapshotLoadError> {
    if current.bucket_execution_generation == target.bucket_execution_generation {
        return Ok(current.command_metadata_eq(target));
    }
    if current.bucket_execution_generation > target.bucket_execution_generation {
        return Ok(false);
    }
    Ok(build_expected(current)?.command_metadata_eq(target))
}

fn bucket_property_command_matches_mutation(
    command: &PutBucketPropertyCommand,
    bucket: &BucketName,
    mutation: &BucketPropertyMutation,
) -> bool {
    if command.bucket.name != *bucket || command.effect != mutation.effect() {
        return false;
    }
    match mutation {
        BucketPropertyMutation::ObjectLock(config) => command.bucket.object_lock == *config,
        BucketPropertyMutation::Encryption(config) => command.bucket.encryption == *config,
        BucketPropertyMutation::PublicAccessBlock(config) => {
            command.bucket.public_access_block == *config
        }
        BucketPropertyMutation::OwnershipControls(config) => {
            command.bucket.ownership_controls == *config
        }
        BucketPropertyMutation::AbacEnabled(enabled) => {
            command.bucket.bucket_abac_enabled == *enabled
        }
    }
}

#[derive(Clone)]
pub(in crate::node_runtime) struct LocalStorageNodeClient {
    node_id: NodeId,
    storage_node: Arc<SharedStorageNode>,
}

/// Storage-local authority carried by a metadata read route.
///
/// Active reads rely on the current primary route selected by the cluster
/// map. Peering reads additionally bind every local store access to the exact
/// replica proof certified by that map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataReadAuthorization {
    pg_id: PgId,
    kind: MetadataReadAuthorizationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataReadAuthorizationKind {
    Active,
    Peering(PgMetadataReadRoute),
}

impl MetadataReadAuthorization {
    pub(crate) const fn active(pg_id: PgId) -> Self {
        Self {
            pg_id,
            kind: MetadataReadAuthorizationKind::Active,
        }
    }

    pub(crate) const fn peering(pg_id: PgId, read_route: PgMetadataReadRoute) -> Self {
        Self {
            pg_id,
            kind: MetadataReadAuthorizationKind::Peering(read_route),
        }
    }

    pub(crate) const fn pg_id(self) -> PgId {
        self.pg_id
    }

    pub(crate) const fn peering_route(self) -> Option<PgMetadataReadRoute> {
        match self.kind {
            MetadataReadAuthorizationKind::Active => None,
            MetadataReadAuthorizationKind::Peering(read_route) => Some(read_route),
        }
    }

    pub(crate) const fn is_active(self) -> bool {
        matches!(self.kind, MetadataReadAuthorizationKind::Active)
    }
}

#[allow(dead_code)]
pub(crate) struct UnixStorageNodeClient {
    node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    endpoint: StorageRpcClientEndpoint,
    pg_topology: Option<Arc<PgTopology>>,
    next_request_id: AtomicU64,
    rpc_admission: Arc<UnixStorageNodeRpcAdmission>,
    rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
}

#[derive(Debug)]
enum StorageRpcRequestDispatchFailure {
    NotSent(StoreError),
    MayHaveApplied(StoreError),
}

impl StorageRpcRequestDispatchFailure {
    fn into_source(self) -> StoreError {
        match self {
            Self::NotSent(source) | Self::MayHaveApplied(source) => source,
        }
    }

    fn into_metadata_command_apply_error(self) -> MetadataCommandApplyError {
        match self {
            Self::NotSent(source) => MetadataCommandApplyError::not_sent(source),
            Self::MayHaveApplied(source) => MetadataCommandApplyError::may_have_applied(source),
        }
    }

    fn into_metadata_command_pending_slot_replace_error(
        self,
    ) -> MetadataCommandPendingSlotReplaceError {
        match self {
            Self::NotSent(source) => MetadataCommandPendingSlotReplaceError::not_sent(source),
            Self::MayHaveApplied(source) => {
                MetadataCommandPendingSlotReplaceError::may_have_applied(source)
            }
        }
    }

    fn into_metadata_command_pending_slot_insert_error(
        self,
    ) -> MetadataCommandPendingSlotInsertError {
        match self {
            Self::NotSent(source) => MetadataCommandPendingSlotInsertError::not_sent(source),
            Self::MayHaveApplied(source) => {
                MetadataCommandPendingSlotInsertError::may_have_applied(source)
            }
        }
    }
}

pub(crate) fn storage_rpc_deadline_expired(context: &'static str) -> StoreError {
    StoreError::OperationDeadlineExceeded { context }
}

fn storage_rpc_endpoint_deadline_expired(node_id: NodeId, context: &'static str) -> StoreError {
    StoreError::StorageRpc {
        node_id: node_id.as_u32(),
        operation: context,
        failure: StorageRpcErrorCode::TransportTimeout,
        detail: crate::StorageNodeFailureDetail::new(
            "storage-node RPC absolute operation deadline expired",
        ),
    }
}

fn storage_rpc_endpoint_connect_error(
    node_id: NodeId,
    operation: &'static str,
    failure: StorageRpcEndpointConnectFailure,
) -> StoreError {
    match failure {
        StorageRpcEndpointConnectFailure::Transport(source) => StoreError::StorageRpc {
            node_id: node_id.as_u32(),
            operation,
            failure: if source.kind() == io::ErrorKind::TimedOut {
                StorageRpcErrorCode::TransportTimeout
            } else {
                StorageRpcErrorCode::TransportClosed
            },
            detail: crate::StorageNodeFailureDetail::new(source.to_string()),
        },
        StorageRpcEndpointConnectFailure::Internal(source) => StoreError::Io {
            context: operation,
            source,
        },
    }
}

fn write_unix_storage_rpc_request<W: Write>(
    writer: &mut W,
    node_id: NodeId,
    auth: Option<&StorageRpcClientAuthConfig>,
    frame: &StorageRpcFrame,
    operation: &'static str,
) -> Result<Option<StorageRpcRequestProof>, StoreError> {
    let Some(auth) = auth else {
        write_storage_rpc_frame_to(writer, frame)
            .map_err(|error| storage_rpc_stream_error(node_id, operation, error))?;
        return Ok(None);
    };
    let signed_request = auth
        .sign_request(node_id, crate::clock::current_time_millis(), frame)
        .map_err(|error| storage_rpc_auth_store_error(node_id, operation, error))?;
    let (envelope, request_proof) = signed_request.into_parts();
    write_storage_rpc_auth_transport_frame_with_limit(
        writer,
        &envelope,
        auth.transport_limits().max_frame_bytes(),
    )
    .map_err(|error| {
        storage_rpc_stream_error(node_id, operation, StorageRpcStreamError::Io(error))
    })?;
    Ok(Some(request_proof))
}

fn write_unix_storage_rpc_request_classified<W: Write>(
    writer: &mut W,
    node_id: NodeId,
    auth: Option<&StorageRpcClientAuthConfig>,
    frame: &StorageRpcFrame,
    operation: &'static str,
) -> Result<Option<StorageRpcRequestProof>, StorageRpcRequestDispatchFailure> {
    let Some(auth) = auth else {
        return write_storage_rpc_frame_to(writer, frame)
            .map(|()| None)
            .map_err(|error| {
                StorageRpcRequestDispatchFailure::MayHaveApplied(storage_rpc_stream_error(
                    node_id, operation, error,
                ))
            });
    };
    let signed_request = auth
        .sign_request(node_id, crate::clock::current_time_millis(), frame)
        .map_err(|error| {
            StorageRpcRequestDispatchFailure::NotSent(storage_rpc_auth_store_error(
                node_id, operation, error,
            ))
        })?;
    let (envelope, request_proof) = signed_request.into_parts();
    write_storage_rpc_auth_transport_frame_with_limit(
        writer,
        &envelope,
        auth.transport_limits().max_frame_bytes(),
    )
    .map_err(|error| {
        StorageRpcRequestDispatchFailure::MayHaveApplied(storage_rpc_stream_error(
            node_id,
            operation,
            StorageRpcStreamError::Io(error),
        ))
    })?;
    Ok(Some(request_proof))
}

fn read_unix_storage_rpc_response<R: Read>(
    reader: &mut R,
    node_id: NodeId,
    auth: Option<&StorageRpcClientAuthConfig>,
    request_proof: Option<&StorageRpcRequestProof>,
    operation: &'static str,
) -> Result<StorageRpcFrame, StoreError> {
    let Some(auth) = auth else {
        return read_storage_rpc_frame_from(reader)
            .map_err(|error| storage_rpc_stream_error(node_id, operation, error));
    };
    let request_proof = request_proof.ok_or_else(|| {
        storage_rpc_auth_store_error(
            node_id,
            operation,
            "authenticated storage RPC request transcript is missing",
        )
    })?;
    let envelope = read_storage_rpc_auth_transport_frame_with_limit(
        reader,
        auth.transport_limits().max_frame_bytes(),
    )
    .map_err(|error| {
        storage_rpc_stream_error(node_id, operation, StorageRpcStreamError::Io(error))
    })?;
    auth.verify_response(
        node_id,
        crate::clock::current_time_millis(),
        request_proof,
        &envelope,
    )
    .map(|verified| verified.into_frame())
    .map_err(|error| storage_rpc_auth_store_error(node_id, operation, error))
}

fn storage_rpc_auth_store_error(
    node_id: NodeId,
    operation: &'static str,
    error: impl std::fmt::Debug,
) -> StoreError {
    let _ = observability::emit_flight_event(
        "storage_rpc_client",
        "storage_rpc_client_auth_error",
        format!(
            "node_id={} operation={operation:?} error={error:?}",
            node_id.as_u32()
        ),
    );
    StoreError::StorageRpc {
        node_id: node_id.as_u32(),
        operation,
        failure: StorageRpcErrorCode::PayloadDecode,
        detail: crate::StorageNodeFailureDetail::new(format!(
            "storage RPC authentication failed: {error:?}"
        )),
    }
}

struct LocalStorageNodeReadHandleLease;

#[allow(dead_code)]
impl UnixStorageNodeClient {
    fn validate_bucket_route_subject(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        let pg_topology = self.pg_topology.as_ref().ok_or_else(|| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                operation,
                "bucket client has no installed PG topology".to_string(),
            ))
        })?;
        if pg_topology.bucket_pg_for(bucket) != pg_id.get() {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                operation,
                "bucket does not belong to the scoped bucket metadata PG".to_string(),
            )));
        }
        Ok(())
    }

    fn head_bucket_with_kind(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_info_outcome_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket info response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcBucketInfoOutcome::Info(info) => {
                if info.name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket info response",
                        "response bucket name does not match request".to_string(),
                    )));
                }
                Ok(info)
            }
            StorageRpcBucketInfoOutcome::BucketNotFound { name } => {
                if name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket info response",
                        "bucket-not-found response name does not match request".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketNotFound { name },
                ))
            }
        }
    }

    fn bucket_metadata_control_pending_match(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        mutation: StorageRpcBucketMetadataControlMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let request = StorageRpcBucketMetadataControlPendingMatchRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            command: command.clone(),
            mutation,
        };
        let payload =
            encode_bucket_metadata_control_pending_match_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket metadata control pending match request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketMetadataControlPendingMatch,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_metadata_command_bool_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket metadata control pending match response",
                error.to_string(),
            ))
        })?;
        Ok(response.value)
    }

    fn bucket_metadata_control_command_build(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: StorageRpcBucketMetadataControlMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let request = StorageRpcBucketMetadataControlCommandBuildRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            command_id,
            mutation,
        };
        let payload =
            encode_bucket_metadata_control_command_build_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket metadata control command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketMetadataControlCommandBuild,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_metadata_control_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket metadata control command build response",
                error.to_string(),
            ))
        })?;
        self.validate_bucket_metadata_control_command_build_response(
            response.command,
            bucket,
            command_id,
            &request.mutation,
        )
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

fn metadata_command_log_conflict_error(
    expected_cluster_epoch: ClusterEpoch,
    expected_pg_id: PgId,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
    conflict: MetadataCommandLogConflictRpcFields,
) -> StoreError {
    if conflict.cluster_epoch != expected_cluster_epoch || conflict.pg_id != expected_pg_id.get() {
        return rpc_payload_error(
            decode_context,
            "metadata command log conflict route mismatch".to_string(),
        );
    }
    if MetadataCommandLogIndex::new(conflict.log_index).is_none() {
        return rpc_payload_error(
            decode_context,
            "metadata command log conflict index must not be zero".to_string(),
        );
    }
    StoreError::MetadataCommandLogConflict {
        node_id: conflict.node_id,
        pg_id: conflict.pg_id,
        cluster_epoch: conflict.cluster_epoch,
        log_index: conflict.log_index,
    }
}

fn metadata_command_terminal_entry_pending_error(
    expected_node_id: NodeId,
    expected_cluster_epoch: ClusterEpoch,
    expected_pg_id: PgId,
    expected_log_index: MetadataCommandLogIndex,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
    pending: MetadataCommandLogConflictRpcFields,
) -> StoreError {
    if !metadata_command_cleanup_response_subject_matches(
        expected_node_id,
        expected_cluster_epoch,
        expected_pg_id,
        expected_log_index,
        pending,
    ) {
        return rpc_payload_error(
            decode_context,
            "metadata command terminal-entry-pending subject mismatch".to_string(),
        );
    }
    StoreError::MetadataCommandTerminalEntryPending {
        node_id: pending.node_id,
        pg_id: pending.pg_id,
        cluster_epoch: pending.cluster_epoch,
        log_index: pending.log_index,
    }
}

fn metadata_command_terminal_log_conflict_error(
    expected_node_id: NodeId,
    expected_cluster_epoch: ClusterEpoch,
    expected_pg_id: PgId,
    expected_log_index: MetadataCommandLogIndex,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
    conflict: MetadataCommandLogConflictRpcFields,
) -> StoreError {
    if !metadata_command_cleanup_response_subject_matches(
        expected_node_id,
        expected_cluster_epoch,
        expected_pg_id,
        expected_log_index,
        conflict,
    ) {
        return rpc_payload_error(
            decode_context,
            "metadata command terminal log-conflict subject mismatch".to_string(),
        );
    }
    StoreError::MetadataCommandLogConflict {
        node_id: conflict.node_id,
        pg_id: conflict.pg_id,
        cluster_epoch: conflict.cluster_epoch,
        log_index: conflict.log_index,
    }
}

fn metadata_command_cleanup_response_subject_matches(
    expected_node_id: NodeId,
    expected_cluster_epoch: ClusterEpoch,
    expected_pg_id: PgId,
    expected_log_index: MetadataCommandLogIndex,
    response: MetadataCommandLogConflictRpcFields,
) -> bool {
    response.node_id == expected_node_id.as_u32()
        && response.cluster_epoch == expected_cluster_epoch
        && response.pg_id == expected_pg_id.get()
        && response.log_index == expected_log_index.get()
}

fn stale_bucket_metadata_command_error(
    command: &MetadataCommandEnvelope,
    name: BucketName,
    bucket_execution_generation: u64,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
) -> Result<MetadataError, StoreError> {
    let expected = match command.payload() {
        MetadataCommandPayload::PutBucketVersioning(command) => Some((
            command.bucket_name(),
            command.bucket.bucket_execution_generation,
        )),
        MetadataCommandPayload::PutBucketAcl(command) => Some((
            command.bucket_name(),
            command.bucket.bucket_execution_generation,
        )),
        MetadataCommandPayload::PutBucketProperty(command) => Some((
            command.bucket_name(),
            command.bucket.bucket_execution_generation,
        )),
        MetadataCommandPayload::PutBucketSubresource(command) => {
            Some((&command.name, command.bucket_execution_generation))
        }
        MetadataCommandPayload::MarkBucketDeleting(command) => Some((
            command.bucket_name(),
            command.bucket.bucket_execution_generation,
        )),
        _ => None,
    };
    let Some((expected_name, expected_generation)) = expected else {
        return Err(rpc_payload_error(
            decode_context,
            "stale bucket metadata command outcome is impossible for command kind".to_string(),
        ));
    };
    if name != *expected_name || bucket_execution_generation != expected_generation {
        return Err(rpc_payload_error(
            decode_context,
            "stale bucket metadata command outcome identity mismatch".to_string(),
        ));
    }
    Ok(MetadataError::StaleBucketMetadataCommand {
        name,
        bucket_execution_generation,
    })
}

fn stale_object_write_command_error(
    command: &MetadataCommandEnvelope,
    bucket: BucketName,
    key: ObjectKey,
    write_sequence: u64,
    generation_id: Option<GenerationId>,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
) -> Result<MetadataError, StoreError> {
    let expected = match command.payload() {
        MetadataCommandPayload::CommitDirectPutObject(command) => Some((
            &command.object.bucket,
            &command.object.key,
            command.write_sequence,
            Some(command.object.generation_id),
        )),
        MetadataCommandPayload::CommitMultipartObject(command) => Some((
            &command.object.bucket,
            &command.object.key,
            command.write_sequence,
            Some(command.object.generation_id),
        )),
        MetadataCommandPayload::InsertDeleteMarker(command) => {
            Some((&command.bucket, &command.key, command.write_sequence, None))
        }
        MetadataCommandPayload::DeleteObjectVersion(command) => match command.target {
            DeleteObjectVersionTarget::DeleteMarker { write_sequence } => {
                Some((&command.bucket, &command.key, write_sequence, None))
            }
            DeleteObjectVersionTarget::Live { .. } => None,
        },
        _ => None,
    };
    let Some((expected_bucket, expected_key, expected_write_sequence, expected_generation_id)) =
        expected
    else {
        return Err(rpc_payload_error(
            decode_context,
            "stale object write command outcome is impossible for command kind".to_string(),
        ));
    };
    if bucket != *expected_bucket
        || key != *expected_key
        || write_sequence != expected_write_sequence
        || generation_id != expected_generation_id
    {
        return Err(rpc_payload_error(
            decode_context,
            "stale object write command outcome identity mismatch".to_string(),
        ));
    }
    Ok(MetadataError::StaleObjectWriteCommand {
        bucket,
        key,
        write_sequence,
        generation_id: generation_id.map(GenerationId::get),
    })
}

fn stream_upload_no_such_upload_error(
    command: &MetadataCommandEnvelope,
    session_id: SessionId,
    upload_id: UploadId,
    decode_context: &'static str,
    rpc_payload_error: impl FnOnce(&'static str, String) -> StoreError,
) -> Result<MetadataError, StoreError> {
    let Some((expected_session_id, expected_upload_id)) =
        command.payload().stream_upload_no_such_upload_subject()
    else {
        return Err(rpc_payload_error(
            decode_context,
            "stream upload NoSuchUpload outcome is impossible for command subject".to_string(),
        ));
    };
    if &session_id != expected_session_id {
        return Err(rpc_payload_error(
            decode_context,
            "stream upload NoSuchUpload session identity mismatch".to_string(),
        ));
    }
    if &upload_id != expected_upload_id {
        return Err(rpc_payload_error(
            decode_context,
            "stream upload NoSuchUpload upload identity mismatch".to_string(),
        ));
    }
    Ok(MetadataError::NoSuchUpload {
        upload_id: expected_upload_id.as_str().to_string(),
    })
}

#[derive(Clone, Copy)]
struct MetadataCommandLogConflictRpcFields {
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
}

#[cfg(test)]
#[path = "node_client/tests.rs"]
mod tests;
