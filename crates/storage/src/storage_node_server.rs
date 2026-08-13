// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::clients::LocalStorageNodeClient;
use super::engine::SharedStorageNode;
use super::MetadataCommandDecodeAuthority;
use super::PreparedRetainedStreamUploadAbort;
use crate::cluster::ShardLocation;
use crate::control_plane::{
    ClusterRuntimeMapSnapshot, ControlPlaneError, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneHeartbeatSink, HeartbeatLease, NodeHeartbeat, PendingMetadataCommandObservation,
    PendingMetadataCommandRecovery, PgMetadataReadRoute, PgRouteSnapshot,
    CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
};
use crate::control_plane_lease::{
    validate_process_lease_clock, BoundRouteMapLease, CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
};
use crate::data_dir::prepare_private_data_dir;
use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError, StoreFailure};
use crate::metadata_command::{
    is_stream_create_bucket_write_operation_kind, validate_metadata_command_recovery_certificate,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
    ObjectPayloadReclaimCommand, PutObjectMetadataMutation,
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
#[cfg(test)]
use crate::node_client::MetadataCommandNodeClient;
use crate::node_client::{
    complete_multipart_expected_object_parts, AcquireObjectPayloadReclaimClaimReq,
    BucketMetadataNodeClient, BucketMetadataScanRoute, BucketWriteReservationNodeClient,
    BucketWriteReservationRoute, BucketWriteReservationScanRoute,
    BuildAbortMultipartUploadCommandReq, BuildAuthorizedAbortMultipartUploadCommandReq,
    BuildCompleteMultipartObjectCommandReq, BuildCreateMultipartUploadCommandReq,
    BuildCreateStreamUploadCommandReq, BuildDeleteCurrentObjectCommandReq,
    BuildDeleteObjectPayloadReclaimCommandReq, BuildDeleteSpecificObjectVersionCommandReq,
    BuildDirectPutCommitCommandReq, BuildInsertDeleteMarkerCommandReq,
    BuildPutObjectMetadataCommandReq, BuildStreamPartCommitCommandReq,
    BuildStreamPutCommitCommandReq, CreateBucketCommandBuild, CreateStreamUploadPrecondition,
    DirectPutMetadataNodeClient, InsertDeleteMarkerStalePayload, MarkBucketDeletingCommandBuild,
    MetadataReadAuthorization, ObjectDeleteStorageSnapshot, ObjectGenerationMetadataNodeClient,
    ObjectMutationMetadataNodeClient, ObjectVersionMetadataNodeClient,
    RetainedBucketWriteReservationNodeClient, RetainedMetadataCommandNodeClient,
    RetainedObjectMutationMetadataNodeClient, ShardAckNodeClient, ShardScavengerNodeClient,
    ShardScavengerObservationNodeClient,
};
use crate::node_runtime::pg_store::{
    initialize_pg_durable_identity, inspect_pg_shard_inventory, sync_initialized_pg_store_layout,
    verify_pg_durable_identity, MetadataCommandCheckpoint, PgStore,
};
use crate::node_runtime::traits::{
    DurableBucketWriteReservationAcquire, PgMetadataStore, ShardStore,
};
#[cfg(test)]
use crate::storage_rpc::encode_placed_segment_backfill_reference_page_request;
use crate::storage_rpc::{
    decode_abort_multipart_cleanup_request, decode_abort_multipart_command_build_request,
    decode_authorized_abort_multipart_command_build_request, decode_bucket_batch_request,
    decode_bucket_delete_attempt_outcome_record_request, decode_bucket_delete_begin_roots_request,
    decode_bucket_delete_finalize_claim_acquire_request,
    decode_bucket_delete_finalize_claim_record_request,
    decode_bucket_delete_finalize_roots_request, decode_bucket_list_request,
    decode_bucket_mark_deleting_command_build_request,
    decode_bucket_metadata_control_command_build_request,
    decode_bucket_metadata_control_pending_match_request, decode_bucket_pg_request,
    decode_bucket_request, decode_bucket_snapshot_request, decode_bucket_subresource_get_request,
    decode_bucket_write_drain_begin_request, decode_bucket_write_drain_clear_expired_request,
    decode_bucket_write_drain_heartbeat_request, decode_bucket_write_drain_record_request,
    decode_bucket_write_reservation_acquire_request,
    decode_bucket_write_reservation_heartbeat_request,
    decode_bucket_write_reservation_proof_request, decode_bucket_write_reservation_record_request,
    decode_cluster_map_history_reference_summary_request,
    decode_complete_multipart_command_build_request, decode_create_bucket_command_build_request,
    decode_create_multipart_upload_command_build_request,
    decode_create_stream_upload_command_build_request,
    decode_delete_current_object_command_build_request,
    decode_delete_specific_object_command_build_request, decode_direct_put_command_build_request,
    decode_direct_put_commit_snapshot_request, decode_historical_shard_read_request,
    decode_insert_delete_marker_command_build_request,
    decode_lifecycle_sweep_claim_acquire_request, decode_lifecycle_sweep_claim_error_request,
    decode_lifecycle_sweep_claim_heartbeat_request, decode_lifecycle_sweep_claim_record_request,
    decode_lifecycle_sweep_roots_request, decode_list_multipart_uploads_request,
    decode_list_object_versions_request, decode_list_objects_request,
    decode_metadata_command_checkpoint_candidates_request,
    decode_metadata_command_log_entry_range_request,
    decode_metadata_command_log_hash_range_request,
    decode_metadata_command_matching_applied_request, decode_metadata_command_next_id_request,
    decode_metadata_command_pending_slot_replace_request,
    decode_metadata_command_pending_slot_request,
    decode_metadata_command_recovery_pending_slot_replace_request,
    decode_metadata_command_recovery_request, decode_metadata_command_request,
    decode_metadata_command_state_request, decode_metadata_command_transfer_adopt_request,
    decode_metadata_command_transfer_checkpoint_base_request,
    decode_metadata_command_transfer_empty_state_request,
    decode_metadata_command_transfer_matching_state_request,
    decode_multipart_completion_barrier_command_build_request,
    decode_multipart_completion_preflight_request, decode_multipart_completion_snapshot_request,
    decode_multipart_parts_list_request, decode_multipart_upload_load_request,
    decode_multipart_upload_match_request, decode_object_delete_snapshot_request,
    decode_object_generation_reservation_request, decode_object_payload_lease_control_request,
    decode_object_payload_reclaim_claim_acquire_request,
    decode_object_payload_reclaim_claim_record_request,
    decode_object_payload_reclaim_command_build_request,
    decode_object_payload_reclaim_exists_request, decode_object_read_auth_subject_request,
    decode_object_read_snapshot_request, decode_object_request,
    decode_placed_segment_backfill_reference_page_request,
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
    decode_stream_segment_append_prepare_request,
    decode_stream_upload_bucket_write_reservation_update_request,
    decode_stream_upload_match_request, decode_stream_upload_session_request,
    decode_stream_uploads_list_request, decode_stream_uploads_pg_list_request,
    encode_abort_multipart_cleanup_response, encode_aborting_multipart_upload_buckets_response,
    encode_bucket_delete_attempt_outcome_optional_record_response,
    encode_bucket_delete_begin_roots_response,
    encode_bucket_delete_finalize_claim_optional_record_response,
    encode_bucket_delete_finalize_roots_response, encode_bucket_execution_generations_response,
    encode_bucket_fast_path_identities_response, encode_bucket_info_outcome_response,
    encode_bucket_list_response, encode_bucket_mark_deleting_command_build_response,
    encode_bucket_metadata_control_command_build_response, encode_bucket_snapshot_response,
    encode_bucket_subresource_get_response, encode_bucket_write_drain_begin_response,
    encode_bucket_write_drain_optional_record_response,
    encode_bucket_write_reservation_record_response,
    encode_bucket_write_reservations_list_response,
    encode_cluster_map_history_reference_summary_response,
    encode_create_bucket_command_build_response, encode_direct_put_command_build_response,
    encode_direct_put_commit_snapshot_response, encode_health_response,
    encode_historical_shard_read_response, encode_lifecycle_sweep_buckets_response,
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
    encode_multipart_completion_barrier_command_build_response,
    encode_multipart_completion_preflight_response, encode_multipart_completion_snapshot_response,
    encode_multipart_completion_stale_source_response, encode_multipart_management_lookup_response,
    encode_multipart_parts_list_response, encode_multipart_upload_load_response,
    encode_multipart_upload_match_response, encode_object_delete_snapshot_response,
    encode_object_generation_reservation_response, encode_object_generation_response,
    encode_object_lifecycle_version_list_response, encode_object_metadata_command_build_response,
    encode_object_payload_lease_control_response,
    encode_object_payload_reclaim_claim_optional_record_response,
    encode_object_payload_reclaim_response, encode_object_read_auth_subject_response,
    encode_object_read_snapshot_response, encode_object_version_response,
    encode_payload_reclaim_root_response, encode_placed_segment_backfill_reference_page_response,
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
    set_storage_rpc_response_connection_reusable, validate_read_handle_acquire_request,
    validate_read_handle_release_request, write_storage_rpc_frame_to,
    StorageRpcAbortMultipartCleanupResponse, StorageRpcAbortMultipartCommandBuildRequest,
    StorageRpcAbortingMultipartUploadBucketsResponse, StorageRpcAdmittedRouteEffectDeadline,
    StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcBucketBatchRequest,
    StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse,
    StorageRpcBucketDeleteAttemptOutcomeRecordRequest, StorageRpcBucketDeleteBeginRootsRequest,
    StorageRpcBucketDeleteBeginRootsResponse, StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse,
    StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcBucketDeleteFinalizeRootsRequest,
    StorageRpcBucketDeleteFinalizeRootsResponse, StorageRpcBucketExecutionGenerationsResponse,
    StorageRpcBucketFastPathIdentitiesResponse, StorageRpcBucketInfoOutcome,
    StorageRpcBucketInfoOutcomeResponse, StorageRpcBucketListRequest, StorageRpcBucketListResponse,
    StorageRpcBucketMarkDeletingCommandBuildOutcome,
    StorageRpcBucketMarkDeletingCommandBuildRequest,
    StorageRpcBucketMarkDeletingCommandBuildResponse,
    StorageRpcBucketMetadataControlCommandBuildRequest,
    StorageRpcBucketMetadataControlCommandBuildResponse, StorageRpcBucketMetadataControlMutation,
    StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcBucketPgRequest,
    StorageRpcBucketRequest, StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotRequest,
    StorageRpcBucketSnapshotResponse, StorageRpcBucketSubresourceGetOutcome,
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
    StorageRpcCompleteMultipartCommandBuildRequest, StorageRpcCreateBucketCommandBuildOutcome,
    StorageRpcCreateBucketCommandBuildRequest, StorageRpcCreateBucketCommandBuildResponse,
    StorageRpcCreateMultipartUploadCommandBuildRequest,
    StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcCreateStreamUploadPrecondition,
    StorageRpcDeleteCurrentObjectCommandBuildRequest,
    StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcDirectPutCommandBuildOutcome,
    StorageRpcDirectPutCommandBuildRequest, StorageRpcDirectPutCommandBuildResponse,
    StorageRpcDirectPutCommitSnapshotRequest, StorageRpcDirectPutCommitSnapshotResponse,
    StorageRpcErrorCode, StorageRpcErrorResponse, StorageRpcFrame, StorageRpcHealthResponse,
    StorageRpcHistoricalShardReadRequest, StorageRpcInsertDeleteMarkerCommandBuildRequest,
    StorageRpcLifecycleSweepBucketsResponse, StorageRpcLifecycleSweepClaimAcquireRequest,
    StorageRpcLifecycleSweepClaimErrorRequest, StorageRpcLifecycleSweepClaimHeartbeatRequest,
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
    StorageRpcMetadataCommandPendingSlotRequest,
    StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest,
    StorageRpcMetadataCommandRecoveryRequest, StorageRpcMetadataCommandRequest,
    StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateOutcomeResponse,
    StorageRpcMetadataCommandStateRequest, StorageRpcMetadataCommandStateResponse,
    StorageRpcMetadataCommandTransferAdoptRequest,
    StorageRpcMetadataCommandTransferCheckpointBaseRequest,
    StorageRpcMetadataCommandTransferEmptyStateRequest,
    StorageRpcMetadataCommandTransferMatchingStateRequest,
    StorageRpcMultipartCompletionBarrierCommandBuildRequest,
    StorageRpcMultipartCompletionBarrierCommandBuildResponse,
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
    StorageRpcObjectMetadataCommandBuildResponse, StorageRpcObjectPayloadLeaseControlOperation,
    StorageRpcObjectPayloadLeaseControlRequest, StorageRpcObjectPayloadLeaseControlResponse,
    StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse,
    StorageRpcObjectPayloadReclaimClaimRecordRequest,
    StorageRpcObjectPayloadReclaimCommandBuildRequest, StorageRpcObjectPayloadReclaimExistsRequest,
    StorageRpcObjectPayloadReclaimResponse, StorageRpcObjectReadAuthSubjectOutcome,
    StorageRpcObjectReadAuthSubjectRequest, StorageRpcObjectReadAuthSubjectResponse,
    StorageRpcObjectReadSnapshotOutcome, StorageRpcObjectReadSnapshotRequest,
    StorageRpcObjectReadSnapshotResponse, StorageRpcObjectRequest, StorageRpcObjectVersionResponse,
    StorageRpcPayloadReclaimRootResponse, StorageRpcPlacedSegmentBackfillReferencePageRequest,
    StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
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
    StorageRpcShardDeleteRequest, StorageRpcShardLocation, StorageRpcShardReadRangeRequest,
    StorageRpcShardReadRequest, StorageRpcShardWriteRequest, StorageRpcStreamError,
    StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcStreamPartFinalizeSnapshotRequest,
    StorageRpcStreamPartFinalizeSnapshotResponse, StorageRpcStreamPutCommitCommandBuildRequest,
    StorageRpcStreamPutFinalizeSnapshotRequest, StorageRpcStreamPutFinalizeSnapshotResponse,
    StorageRpcStreamSegmentAppendPrepareOutcome, StorageRpcStreamSegmentAppendPrepareRequest,
    StorageRpcStreamSegmentAppendPrepareResponse,
    StorageRpcStreamUploadBucketWriteReservationUpdateRequest, StorageRpcStreamUploadMatchRequest,
    StorageRpcStreamUploadMatchResponse, StorageRpcStreamUploadSegmentsOutcome,
    StorageRpcStreamUploadSegmentsResponse, StorageRpcStreamUploadSessionOutcome,
    StorageRpcStreamUploadSessionRequest, StorageRpcStreamUploadSessionResponse,
    StorageRpcStreamUploadsListRequest, StorageRpcStreamUploadsListResponse,
    StorageRpcStreamUploadsPgListRequest, STORAGE_RPC_FRAME_ENCODING_VERSION,
    STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES, STORAGE_RPC_MAX_PAYLOAD_LEN,
    STORAGE_RPC_SERVER_IDLE_TIMEOUT,
};
use crate::storage_rpc_auth::{
    write_storage_rpc_auth_transport_frame_with_limit, StorageRpcResponseSigningContext,
    StorageRpcServerAuthConfig,
};
use crate::storage_rpc_transport::{
    accepted_tls_tcp_stream, accepted_unix_stream, storage_rpc_tls_server_config,
    validate_storage_rpc_tls_server_config, BoxStorageRpcStream,
};
use crate::types::{
    AdmittedRouteEffectFence, BucketState, ClusterEpoch, GenerationId, PgId, PgState, SessionId,
    WriteAck,
};
use crate::types::{
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardRepairClaimAcquire, PlacedSegmentShardRepairClaimRecord,
};
use crate::DataPgId;
use crate::{
    BucketDeleteBeginRoot, BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketInfo,
    BucketName, BucketSnapshot, BucketSnapshotRequest, BucketSubresourceKind,
    BucketWriteDrainRecord, BucketWriteReservationProof, BucketWriteReservationRecord,
    CreateMultipartUploadReq, CreateStreamUploadReq, EcShape, LifecycleSweepClaimRecord,
    LifecycleSweepRoot, MultipartUploadRecord, NodeId, ObjectKey, ObjectPayloadReclaimClaimRecord,
    ObjectPayloadReclaimKind, ObjectPgActionError, PayloadReclaimRoot,
    PrepareStreamUploadSegmentAppendReq, RouteMapValidity, ShardKey, StreamUploadRecord,
    StreamUploadSegmentRecord, StreamUploadTarget, UploadId,
};
use crate::{BucketFastPathIdentity, BucketPgId, ObjectMetadataPgId, ObjectMetadataScanPgId};
use checksum::{ChecksumAlgorithm, ChecksumHasher};

#[cfg(test)]
type MetadataCommandBeforeWaitHook = Arc<dyn Fn(PgId) + Send + Sync>;
#[cfg(test)]
type StorageRpcResponseEnvelopeTestHook =
    Arc<dyn Fn(StorageRpcMessageKind, &mut Vec<u8>) + Send + Sync>;
#[cfg(test)]
type MetadataCommandBeforeCommitTestHook =
    Arc<dyn Fn(NodeId, &MetadataCommandEnvelope) + Send + Sync>;

fn metadata_command_state_result_response(
    command: &MetadataCommandEnvelope,
    result: Result<crate::metadata_command::MetadataCommandReplicaState, BucketSnapshotLoadError>,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let outcome = match result {
        Ok(state) => StorageRpcMetadataCommandStateOutcome::State(state),
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
                Some(command.payload().kind_name()),
            );
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            }
        }
        Err(BucketSnapshotLoadError::Store(error)) => {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
        Err(BucketSnapshotLoadError::Metadata(
            crate::MetadataError::ObjectGenerationReservationConflict {
                reservation_id,
                generation_id,
            },
        )) => StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
            reservation_id: SessionId::try_from(reservation_id).map_err(|_| {
                crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "stored reservation id is invalid",
                )
            })?,
            generation_id: GenerationId::new(generation_id).ok_or(
                crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "stored generation id is invalid",
                ),
            )?,
        },
        Err(BucketSnapshotLoadError::Metadata(
            crate::MetadataError::ObjectVersionReservationConflict { version_id },
        )) => {
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict { version_id }
        }
        Err(BucketSnapshotLoadError::Metadata(
            crate::MetadataError::StaleBucketMetadataCommand {
                name,
                bucket_execution_generation,
            },
        )) => StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name,
            bucket_execution_generation,
        },
        Err(BucketSnapshotLoadError::Metadata(crate::MetadataError::StaleObjectWriteCommand {
            bucket,
            key,
            write_sequence,
            generation_id,
        })) => StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            bucket,
            key,
            write_sequence,
            generation_id: generation_id
                .map(|generation_id| {
                    GenerationId::new(generation_id).ok_or(
                        crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "stored stale object generation id is invalid",
                        ),
                    )
                })
                .transpose()?,
        },
        Err(BucketSnapshotLoadError::Metadata(crate::MetadataError::StreamSegmentConflict {
            segment_index,
        })) => StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index },
        Err(BucketSnapshotLoadError::Metadata(error)) => {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: error.to_string(),
            });
        }
    };
    let payload = encode_metadata_command_state_outcome_response(
        &StorageRpcMetadataCommandStateOutcomeResponse { outcome },
    );
    Ok(encode_storage_rpc_success_response(&payload))
}

const STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME: &str = ".argmin-storage-node.lock";
const STORAGE_NODE_INCARNATION_FILE: &str = "control-plane-node-incarnation";
const STORAGE_NODE_INCARNATION_TMP_FILE: &str = ".control-plane-node-incarnation.tmp";
const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const STORAGE_NODE_MAX_ACTIVE_SESSIONS: usize = 1024;
const STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION: usize = 4096;
const STORAGE_NODE_MAX_LIVE_READ_OPERATIONS: usize = 16 * 1024;
const STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS: usize = 64 * 1024;
// The persisted text expands the bounded binary runtime-map representation.
// Eight times the control-plane frame bound leaves ample room for decimal and
// hex encoding while keeping corrupt durable input bounded before parsing.
const CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES: usize = CONTROL_PLANE_RPC_MAX_FRAME_BYTES * 8;

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

include!("storage_node_server/config.rs");

include!("storage_node_server/server.rs");

include!("storage_node_server/admission.rs");

include!("storage_node_server/routes.rs");

impl StorageNodeConnectionHandler {
    fn read_request_frame(
        &self,
        stream: &mut BoxStorageRpcStream,
    ) -> Result<(StorageRpcFrame, Option<StorageRpcResponseSigningContext>), StorageRpcStreamError>
    {
        let Some(auth) = self.rpc_auth.as_deref() else {
            return read_storage_rpc_request_frame_from(stream).map(|frame| (frame, None));
        };
        let (envelope, _pre_auth_byte_reservation) = auth
            .read_request_envelope(stream)
            .map_err(StorageRpcStreamError::Io)?;
        auth.verify_request(
            self.config.node_id,
            crate::clock::current_time_millis(),
            &envelope,
        )
        .map(|verified| {
            let (frame, response_signing_context) =
                verified.into_frame_and_response_signing_context();
            (frame, Some(response_signing_context))
        })
        .map_err(|error| {
            StorageRpcStreamError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("storage RPC authentication rejected: {error:?}"),
            ))
        })
    }

    fn write_response_frame(
        &self,
        stream: &mut BoxStorageRpcStream,
        request_auth: Option<&StorageRpcResponseSigningContext>,
        response: &StorageRpcFrame,
    ) -> Result<(), StorageRpcStreamError> {
        let Some(auth) = self.rpc_auth.as_deref() else {
            return write_storage_rpc_frame_to(stream, response);
        };
        let request_auth = request_auth.ok_or_else(|| {
            StorageRpcStreamError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "authenticated storage RPC response has no request credential",
            ))
        })?;
        let envelope = auth
            .sign_response(
                request_auth,
                self.config.node_id,
                crate::clock::current_time_millis(),
                response,
            )
            .map_err(|error| {
                StorageRpcStreamError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("storage RPC response authentication failed: {error}"),
                ))
            })?;
        #[cfg(test)]
        let mut envelope = envelope;
        #[cfg(test)]
        if let Some(hook) = self
            .response_envelope_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            hook(response.kind, &mut envelope);
        }
        write_storage_rpc_auth_transport_frame_with_limit(
            stream,
            &envelope,
            auth.transport_limits().max_frame_bytes(),
        )
        .map_err(StorageRpcStreamError::Io)
    }

    fn refresh_config_snapshot(&mut self) {
        let runtime_route = self
            .runtime_route_source
            .read()
            .unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        if let Some(hook) = self
            .runtime_route_capture_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            hook();
        }
        self.config = Arc::clone(&runtime_route.config);
        self.route_map_lease = runtime_route.route_map_lease;
    }

    fn metadata_command_pg_guard(
        &self,
        session: &StorageNodeSession,
        pg_id: PgId,
    ) -> Result<Option<StorageNodeMetadataCommandGuard>, StorageRpcErrorResponse> {
        if session.holds_metadata_command_pg_lock(pg_id) {
            Ok(None)
        } else {
            Ok(Some(self.metadata_command_locks.acquire(
                self.config.node_id,
                pg_id,
                session.current_rpc_context(),
            )?))
        }
    }

    fn validate_metadata_mutation_route_not_expired(&self) -> Result<(), StorageRpcErrorResponse> {
        if self.current_route_map_lease_is_valid() {
            return Ok(());
        }
        let valid_until_ms = self.config.route_map_valid_until_ms().unwrap_or(0);
        let now_ms = crate::clock::current_time_millis();
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "storage-node route map for cluster epoch {} expired at {valid_until_ms}ms before metadata mutation at {now_ms}ms",
                self.config.cluster_epoch.get()
            ),
        })
    }

    fn current_route_map_lease(&self) -> Option<BoundRouteMapLease> {
        self.route_map_lease
    }

    fn current_route_map_lease_is_valid(&self) -> bool {
        if self.config.route_map_validity == RouteMapValidity::Forever {
            return true;
        }
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        if validate_process_lease_clock(
            crate::clock::current_time_millis(),
            crate::clock::clock_health_time_millis(),
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .is_err()
        {
            self.runtime_route_source
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .route_map_lease = None;
            return false;
        }
        self.current_route_map_lease()
            .is_some_and(|lease| lease.is_valid_at_monotonic(local_monotonic_ms))
    }

    fn require_current_route_map_valid_rpc(&self) -> Result<(), StorageRpcErrorResponse> {
        if self.current_route_map_lease_is_valid() {
            return Ok(());
        }
        let valid_until_ms = self.config.route_map_valid_until_ms().unwrap_or(0);
        let now_ms = crate::clock::current_time_millis();
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "storage-node route map for cluster epoch {} expired at {valid_until_ms}ms, now {now_ms}ms",
                self.config.cluster_epoch.get()
            ),
        })
    }

    fn handle_session(
        &mut self,
        stream: &mut BoxStorageRpcStream,
        mut session_guard: StorageNodeActiveSessionGuard,
    ) -> Result<(), StorageNodeServerError> {
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self.node));
        let mut set_request_deadline = false;
        loop {
            if set_request_deadline {
                let deadline = Instant::now()
                    .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                    .ok_or_else(|| StorageNodeServerError::RpcStream {
                        message: "storage-node RPC request deadline overflowed".to_string(),
                    })?;
                stream.set_operation_deadline(deadline).map_err(|error| {
                    StorageNodeServerError::RpcStream {
                        message: format!("set storage-node RPC request deadline: {error}"),
                    }
                })?;
            }
            set_request_deadline = true;
            let (frame, request_auth) = match self.read_request_frame(stream) {
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
                Err(StorageRpcStreamError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
                {
                    if session.has_active_read_state() && !session.has_metadata_command_pg_locks() {
                        continue;
                    }
                    return Ok(());
                }
                Err(error) => return Err(rpc_stream_error(error)),
            };
            let object_payload_lease_cleanup = frame.kind
                == StorageRpcMessageKind::ObjectPayloadLeaseControl
                && decode_object_payload_lease_control_request(&frame.payload)
                    .is_ok_and(|request| {
                        matches!(
                            request.operation,
                            StorageRpcObjectPayloadLeaseControlOperation::Release
                                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
                                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
                                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear
                        )
                    });
            let admission_class = if object_payload_lease_cleanup
                || matches!(
                    frame.kind,
                    StorageRpcMessageKind::MetadataCommandPgLockRelease
                        | StorageRpcMessageKind::ProofRelease
                        | StorageRpcMessageKind::BucketWriteReservationRelease
                        | StorageRpcMessageKind::BucketWriteDrainClear
                        | StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease
                        | StorageRpcMessageKind::BucketDeleteReplicaHead
                        | StorageRpcMessageKind::LifecycleSweepClaimRelease
                        | StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease
                        | StorageRpcMessageKind::ReadHandlesRelease
                        | StorageRpcMessageKind::ShardHistoricalRead
                        | StorageRpcMessageKind::ShardDelete
                        | StorageRpcMessageKind::ShardAckHistoricalLoad
                        | StorageRpcMessageKind::ShardAckDelete
                        | StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare
                        | StorageRpcMessageKind::MetadataCommandRetainedAbortApply
                        | StorageRpcMessageKind::MetadataCommandRetainedAbortFinish
                ) {
                StorageNodeRouteAdmissionClass::RetainedCleanup
            } else {
                StorageNodeRouteAdmissionClass::Active
            };
            let route_permit = self.route_admission.acquire(admission_class);
            self.refresh_config_snapshot();
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
            let mut response = match self.dispatch_frame(&mut session, &route_permit, &frame) {
                Ok(response) => response,
                Err(error) => {
                    session.clear_metadata_command_lock_context(&self.metadata_command_locks);
                    return Err(error);
                }
            };
            let connection_reusable = session_guard.classify_connection(
                session.has_active_read_state() || session.has_metadata_command_pg_locks(),
            );
            set_storage_rpc_response_connection_reusable(
                &mut response.payload,
                connection_reusable,
            )
            .map_err(|error| StorageNodeServerError::ResponsePayload {
                message: error.to_string(),
            })?;
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
            if let Err(error) = self.write_response_frame(stream, request_auth.as_ref(), &response)
            {
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
            if !connection_reusable {
                return Ok(());
            }
        }
    }

    fn dispatch_frame(
        &self,
        session: &mut StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        frame: &StorageRpcFrame,
    ) -> Result<StorageRpcFrame, StorageNodeServerError> {
        let command_decode_authority = MetadataCommandDecodeAuthority::new();
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
                    Ok(request) => {
                        self.read_handles_acquire_response(session, route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ReadHandlesRelease => {
                match decode_read_handle_release_request(&frame.payload) {
                    Ok(request) => {
                        self.read_handles_release_response(session, route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadLeaseControl => {
                match decode_object_payload_lease_control_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_lease_control_response(session, route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ProofRelease => {
                match decode_proof_release_request(&frame.payload) {
                    Ok(request) => self.proof_release_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationAcquire => {
                match decode_bucket_write_reservation_acquire_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_reservation_acquire_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationValidate => {
                match decode_bucket_write_reservation_proof_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_reservation_validate_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationHeartbeat => {
                match decode_bucket_write_reservation_heartbeat_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_reservation_heartbeat_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationRelease => {
                match decode_bucket_write_reservation_record_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_reservation_release_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainBegin => {
                match decode_bucket_write_drain_begin_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_begin_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainClear => {
                match decode_bucket_write_drain_record_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_clear_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainClearExpired => {
                match decode_bucket_write_drain_clear_expired_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_drain_clear_expired_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainHeartbeat => {
                match decode_bucket_write_drain_heartbeat_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_drain_heartbeat_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainGet => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_get_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord => {
                match decode_bucket_delete_attempt_outcome_record_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_attempt_outcome_record_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_attempt_outcome_get_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteDrainExists => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_write_drain_exists_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketWriteReservationsList => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_write_reservations_list_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeRoots => {
                match decode_bucket_delete_finalize_roots_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_finalize_roots_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteBeginRoots => {
                match decode_bucket_delete_begin_roots_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_begin_roots_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeClaimGet => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_finalize_claim_get_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire => {
                match decode_bucket_delete_finalize_claim_acquire_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_finalize_claim_acquire_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease => {
                match decode_bucket_delete_finalize_claim_record_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_delete_finalize_claim_release_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepBucketsList => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => {
                        self.lifecycle_sweep_buckets_list_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepRoots => {
                match decode_lifecycle_sweep_roots_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_roots_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimAcquire => {
                match decode_lifecycle_sweep_claim_acquire_request(&frame.payload) {
                    Ok(request) => {
                        self.lifecycle_sweep_claim_acquire_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimHeartbeat => {
                match decode_lifecycle_sweep_claim_heartbeat_request(&frame.payload) {
                    Ok(request) => {
                        self.lifecycle_sweep_claim_heartbeat_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimError => {
                match decode_lifecycle_sweep_claim_error_request(&frame.payload) {
                    Ok(request) => self.lifecycle_sweep_claim_error_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::LifecycleSweepClaimRelease => {
                match decode_lifecycle_sweep_claim_record_request(&frame.payload) {
                    Ok(request) => {
                        self.lifecycle_sweep_claim_release_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectListPage => {
                match decode_list_objects_request(&frame.payload) {
                    Ok(request) => self.object_list_page_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectVersionListPage => {
                match decode_list_object_versions_request(&frame.payload) {
                    Ok(request) => self.object_version_list_page_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadListPage => {
                match decode_list_multipart_uploads_request(&frame.payload) {
                    Ok(request) => {
                        self.object_multipart_upload_list_page_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectGenerationNext => {
                match decode_object_request(&frame.payload) {
                    Ok(request) => self.object_generation_next_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectGenerationReservation => {
                match decode_object_generation_reservation_request(&frame.payload) {
                    Ok(request) => {
                        self.object_generation_reservation_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::DirectPutCommitSnapshotLoad => {
                match decode_direct_put_commit_snapshot_request(&frame.payload) {
                    Ok(request) => self.direct_put_commit_snapshot_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::DirectPutCommitCommandBuild => {
                match decode_direct_put_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.direct_put_commit_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectReadAuthSubjectLoad => {
                match decode_object_read_auth_subject_request(&frame.payload) {
                    Ok(request) => self.object_read_auth_subject_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectReadSnapshotLoad => {
                match decode_object_read_snapshot_request(&frame.payload) {
                    Ok(request) => self.object_read_snapshot_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad => {
                match decode_put_object_metadata_snapshot_request(&frame.payload) {
                    Ok(request) => {
                        self.put_object_metadata_snapshot_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMetadataPutCommandBuild => {
                match decode_put_object_metadata_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.put_object_metadata_command_build_response(route_permit, request)
                    }
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
                            self.object_lifecycle_version_list_response(route_permit, request)
                        }
                        _ => {
                            self.object_delete_snapshot_response(route_permit, frame.kind, request)
                        }
                    },
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild => {
                match decode_delete_specific_object_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.delete_specific_object_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild => {
                match decode_delete_current_object_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.delete_current_object_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild => {
                match decode_insert_delete_marker_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.insert_delete_marker_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadMatch => {
                match decode_stream_upload_match_request(&frame.payload) {
                    Ok(request) => self.stream_upload_match_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadSessionLoad => {
                match decode_stream_upload_session_request(&frame.payload) {
                    Ok(request) => self.stream_upload_session_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare => {
                match decode_stream_upload_session_request(&frame.payload) {
                    Ok(request) => self.retained_stream_upload_abort_prepare_response(
                        session,
                        route_permit,
                        request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate => {
                match decode_stream_upload_bucket_write_reservation_update_request(&frame.payload) {
                    Ok(request) => self.stream_upload_bucket_write_reservation_update_response(
                        route_permit,
                        request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad => {
                match decode_stream_upload_session_request(&frame.payload) {
                    Ok(request) => self.stream_upload_segments_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadsList => {
                match decode_stream_uploads_list_request(&frame.payload) {
                    Ok(request) => self.stream_uploads_list_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadsPgList => {
                match decode_stream_uploads_pg_list_request(&frame.payload) {
                    Ok(request) => self.stream_uploads_pg_list_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => {
                        self.aborting_multipart_upload_buckets_list_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_payload_reclaim_root_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimExists => {
                match decode_object_payload_reclaim_exists_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_reclaim_exists_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimRoot => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_root_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimLoad => {
                match decode_object_payload_reclaim_exists_request(&frame.payload) {
                    Ok(request) => self.object_payload_reclaim_load_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire => {
                match decode_object_payload_reclaim_claim_acquire_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_reclaim_claim_acquire_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimClaimGet => {
                match decode_metadata_command_state_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_reclaim_claim_get_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease => {
                match decode_object_payload_reclaim_claim_record_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_reclaim_claim_release_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare => {
                match decode_stream_segment_append_prepare_request(&frame.payload) {
                    Ok(request) => {
                        self.stream_segment_append_prepare_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadMatch => {
                match decode_multipart_upload_match_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
                    Ok(request) => self.multipart_upload_match_response(route_permit, request),
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
                    Ok(request) => {
                        self.multipart_upload_load_response(route_permit, frame.kind, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad => {
                match decode_multipart_completion_snapshot_request(&frame.payload) {
                    Ok(request) => {
                        self.multipart_completion_snapshot_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad => {
                match decode_multipart_completion_preflight_request(&frame.payload) {
                    Ok(request) => {
                        self.multipart_completion_preflight_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartPartsList => {
                match decode_multipart_parts_list_request(&frame.payload) {
                    Ok(request) => self.multipart_parts_list_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartManagementLookup => {
                match decode_multipart_upload_load_request(&frame.payload) {
                    Ok(request) => self.multipart_management_lookup_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild => {
                match decode_create_stream_upload_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.create_stream_upload_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartUploadCommandBuild => {
                match decode_create_multipart_upload_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.create_multipart_upload_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad => {
                match decode_stream_put_finalize_snapshot_request(&frame.payload) {
                    Ok(request) => {
                        self.stream_put_finalize_snapshot_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild => {
                match decode_stream_put_commit_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.stream_put_commit_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad => {
                match decode_stream_part_finalize_snapshot_request(&frame.payload) {
                    Ok(request) => {
                        self.stream_part_finalize_snapshot_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild => {
                match decode_stream_part_commit_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.stream_part_commit_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild => {
                match decode_complete_multipart_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.complete_multipart_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAbortCommandBuild => {
                match decode_abort_multipart_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.abort_multipart_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild => {
                match decode_object_payload_reclaim_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.object_payload_reclaim_command_build_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad => {
                match decode_abort_multipart_cleanup_request(&frame.payload) {
                    Ok(request) => self.abort_multipart_cleanup_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild => {
                match decode_authorized_abort_multipart_command_build_request(&frame.payload) {
                    Ok(request) => self
                        .authorized_abort_multipart_command_build_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad => {
                match decode_object_request(&frame.payload) {
                    Ok(request) => {
                        self.multipart_completion_stale_source_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MultipartCompletionBarrierCommandBuild => {
                match decode_multipart_completion_barrier_command_build_request(&frame.payload) {
                    Ok(request) => {
                        self.multipart_completion_barrier_command_build_response(request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ObjectVersionNext => match decode_object_request(&frame.payload)
            {
                Ok(request) => self.object_version_next_response(route_permit, request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardWrite => match decode_shard_write_request(&frame.payload) {
                Ok(request) => self.shard_write_response(route_permit, request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardRepairWrite => {
                match decode_shard_write_request(&frame.payload) {
                    Ok(request) => self.shard_repair_write_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardRead => match decode_shard_read_request(&frame.payload) {
                Ok(request) => self.shard_read_response(route_permit, request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::ShardHistoricalRead => {
                match decode_historical_shard_read_request(&frame.payload) {
                    Ok(request) => self.shard_historical_read_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardReadRange => {
                match decode_shard_read_range_request(&frame.payload) {
                    Ok(request) => self.shard_read_range_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardDelete => {
                match decode_shard_delete_request(&frame.payload) {
                    Ok(request) => self.shard_delete_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckRecord => {
                match decode_shard_ack_batch_request(&frame.payload) {
                    Ok(request) => self.shard_ack_record_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckValidate => {
                match decode_shard_ack_batch_request(&frame.payload) {
                    Ok(request) => self.shard_ack_validate_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckLoad => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_load_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckHistoricalLoad => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_historical_load_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardAckDelete => {
                match decode_shard_ack_item_request(&frame.payload) {
                    Ok(request) => self.shard_ack_delete_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerListFiles => {
                match decode_scavenger_list_files_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_list_files_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerShardRows => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => self.shard_scavenger_shard_rows_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ShardScavengerPayloadReferences => {
                match decode_bucket_pg_request(&frame.payload) {
                    Ok(request) => {
                        self.shard_scavenger_payload_references_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::PlacedSegmentBackfillReferencePage => {
                match decode_placed_segment_backfill_reference_page_request(&frame.payload) {
                    Ok(request) => {
                        self.placed_segment_backfill_reference_page_response(route_permit, request)
                    }
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
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => self.metadata_command_acceptance_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => {
                        self.metadata_command_abandon_acceptance_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPublicationStart => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => {
                        self.metadata_command_publication_start_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert => {
                match decode_metadata_command_pending_slot_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
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
                match decode_metadata_command_pending_slot_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
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
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
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
                match decode_metadata_command_pending_slot_replace_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
                    Ok(request) => {
                        self.metadata_command_pending_slot_replace_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace => {
                match decode_metadata_command_recovery_pending_slot_replace_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
                    Ok(request) => self
                        .metadata_command_recovery_pending_slot_replace_response(session, request),
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
                match decode_metadata_command_transfer_adopt_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
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
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => self.metadata_command_applied_hashes_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog => {
                match decode_metadata_command_matching_applied_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
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
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => self.metadata_command_abandoned_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRecordAbandoned => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
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
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => {
                        self.metadata_command_apply_and_record_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRetainedAbortApply => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => self.metadata_command_retained_abort_apply_response(
                        session,
                        route_permit,
                        request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRetainedAbortFinish => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
                    Ok(request) => self.metadata_command_retained_abort_finish_response(
                        session,
                        route_permit,
                        request,
                    ),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord => {
                match decode_metadata_command_recovery_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
                    Ok(request) => {
                        self.metadata_command_recovery_apply_and_record_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned => {
                match decode_metadata_command_recovery_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
                    Ok(request) => {
                        self.metadata_command_recovery_record_abandoned_response(session, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord => {
                match decode_metadata_command_request(&frame.payload, &command_decode_authority) {
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
                Ok(request) => self.bucket_head_response(route_permit, request, false),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketDeleteReplicaHead => {
                match decode_bucket_request(&frame.payload) {
                    Ok(request) => self.bucket_delete_replica_head_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketHeadInfo => match decode_bucket_request(&frame.payload) {
                Ok(request) => self.bucket_head_response(route_permit, request, true),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketSnapshotLoad => {
                match decode_bucket_snapshot_request(&frame.payload) {
                    Ok(request) => self.bucket_snapshot_response(route_permit, request),
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
                match decode_bucket_metadata_control_pending_match_request(
                    &frame.payload,
                    &command_decode_authority,
                ) {
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
                    Ok(request) => self.bucket_subresource_get_response(route_permit, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketList => match decode_bucket_list_request(&frame.payload) {
                Ok(request) => self.bucket_list_response(route_permit, request),
                Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }),
            },
            StorageRpcMessageKind::BucketExecutionGenerations => {
                match decode_bucket_batch_request(&frame.payload) {
                    Ok(request) => {
                        self.bucket_execution_generations_response(route_permit, request)
                    }
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::BucketFastPathIdentities => {
                match decode_bucket_batch_request(&frame.payload) {
                    Ok(request) => self.bucket_fast_path_identities_response(route_permit, request),
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
        session: &mut StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcReadHandleAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_read_handle_acquire_route(
            route_permit,
            session,
            &request,
            "read handle acquire",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.acquire() {
            Ok(locations) => {
                let payload =
                    encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                        locations: locations.into_iter().map(Into::into).collect(),
                    })?;
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&error)?,
        };
        Ok(response)
    }

    fn read_handles_release_response(
        &self,
        session: &mut StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcReadHandleReleaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_read_handle_release_route(
            route_permit,
            session,
            &request,
            "read handle release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        if let Err(error) = route.release() {
            return encode_storage_rpc_error_response(&error);
        }
        let payload = encode_read_handle_release_response(&StorageRpcReadHandleReleaseResponse);
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_lease_control_response(
        &self,
        session: &mut StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadLeaseControlRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let active = matches!(
            request.operation,
            StorageRpcObjectPayloadLeaseControlOperation::Acquire
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin
                | StorageRpcObjectPayloadLeaseControlOperation::Count
        );
        let active_control;
        let retained_control;
        if active {
            active_control = match self.active_object_payload_lease_control(route_permit, &request)
            {
                Ok(control) => Some(control),
                Err(error) => return encode_storage_rpc_error_response(&error),
            };
            retained_control = None;
        } else {
            retained_control =
                match self.retained_object_payload_lease_control(route_permit, &request) {
                    Ok(control) => Some(control),
                    Err(error) => return encode_storage_rpc_error_response(&error),
                };
            active_control = None;
        }
        let value = match request.operation {
            StorageRpcObjectPayloadLeaseControlOperation::Acquire => {
                if let Err(error) = active_control
                    .as_ref()
                    .expect("active operation has active capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                match session.acquire_object_payload_lease(
                    request.route_cluster_epoch,
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                ) {
                    Ok(acquired) => u64::from(acquired),
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
            StorageRpcObjectPayloadLeaseControlOperation::Release => {
                if let Err(error) = retained_control
                    .as_ref()
                    .expect("retained operation has retained capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                match session.release_object_payload_lease(
                    request.route_cluster_epoch,
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                ) {
                    Ok(remaining) => remaining as u64,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin => {
                if let Err(error) = active_control
                    .as_ref()
                    .expect("active operation has active capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                u64::from(
                    self.node.try_begin_object_payload_reclaim(
                        &request.bucket,
                        &request.key,
                        request.generation_id,
                        request
                            .reclaim_authority
                            .as_ref()
                            .expect("validated reclaim begin authority"),
                    ),
                )
            }
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish => {
                if let Err(error) = retained_control
                    .as_ref()
                    .expect("retained operation has retained capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                if !self.node.finish_object_payload_reclaim(
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                    request
                        .reclaim_authority
                        .as_ref()
                        .expect("validated reclaim finish authority"),
                    false,
                ) {
                    return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "object-payload reclaim finish authority mismatch".to_string(),
                    });
                }
                0
            }
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence => {
                if let Err(error) = retained_control
                    .as_ref()
                    .expect("retained operation has retained capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                if !self.node.finish_object_payload_reclaim(
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                    request
                        .reclaim_authority
                        .as_ref()
                        .expect("validated reclaim finish authority"),
                    true,
                ) {
                    return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "object-payload reclaim finish authority mismatch".to_string(),
                    });
                }
                0
            }
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear => {
                if let Err(error) = retained_control
                    .as_ref()
                    .expect("retained operation has retained capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                if !self.node.clear_object_payload_reclaim_fence(
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                    request
                        .reclaim_authority
                        .as_ref()
                        .expect("validated reclaim clear authority"),
                ) {
                    return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "object-payload reclaim fence-clear authority mismatch"
                            .to_string(),
                    });
                }
                0
            }
            StorageRpcObjectPayloadLeaseControlOperation::Count => {
                if let Err(error) = active_control
                    .as_ref()
                    .expect("active operation has active capability")
                    .require_valid_now()
                {
                    return encode_storage_rpc_error_response(&error);
                }
                self.node.object_payload_lease_count(
                    &request.bucket,
                    &request.key,
                    request.generation_id,
                ) as u64
            }
        };
        let payload = encode_object_payload_lease_control_response(
            StorageRpcObjectPayloadLeaseControlResponse { value },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn proof_release_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcProofReleaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_metadata_command_proof_route(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.proof,
            "metadata command bucket write proof release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.release() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_generation_next_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request,
            "object generation allocation",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.next_generation_id() {
            Ok(generation_id) => {
                let payload =
                    encode_object_generation_response(&StorageRpcObjectGenerationResponse {
                        generation_id,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn bucket_write_reservation_acquire_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteReservationAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.bucket,
            "bucket write reservation acquire",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.acquire_write_reservation(
            DurableBucketWriteReservationAcquire {
                name: &request.bucket,
                reservation_id: &request.reservation_id,
                owner_token: &request.owner_token,
                cluster_epoch: request.cluster_epoch,
                operation_kind: &request.operation_kind,
                created_at: request.created_at,
                lease_deadline: request.lease_deadline,
                target_context: request.target_context.as_deref(),
            },
            admitted_route_effect_fence(request.cluster_epoch, request.effect_deadline),
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDraining,
            ))) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Draining,
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ))) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
                            name,
                        },
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_reservation_validate_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteReservationProofRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.proof.bucket,
            "bucket write reservation validate",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.validate_write_reservation(&request.proof) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_reservation_heartbeat_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteReservationHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.proof.bucket,
            "bucket write reservation heartbeat",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.route_cluster_epoch, request.effect_deadline);
        match route.heartbeat_write_reservation(
            &request.proof,
            request.lease_deadline,
            effect_fence,
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_reservation_record_response(
                    &StorageRpcBucketWriteReservationRecordResponse {
                        outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_reservation_release_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteReservationRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_bucket_write_reservation_route(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.record,
            "bucket write reservation release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.release() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_drain_begin_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteDrainBeginRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request.bucket,
            "bucket write drain begin",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.begin_write_drain(
            &request.drain_id,
            &request.owner_token,
            request.bucket.cluster_epoch,
            request.created_at,
            request.lease_deadline,
            admitted_route_effect_fence(request.bucket.cluster_epoch, request.effect_deadline),
        ) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_begin_response(
                    &StorageRpcBucketWriteDrainBeginResponse {
                        outcome: StorageRpcBucketWriteDrainBeginOutcome::Acquired(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDrainConflict { .. },
            ))) => {
                let payload = encode_bucket_write_drain_begin_response(
                    &StorageRpcBucketWriteDrainBeginResponse {
                        outcome: StorageRpcBucketWriteDrainBeginOutcome::Conflict,
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_drain_clear_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteDrainRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_bucket_write_drain_route(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.record,
            "bucket write drain clear",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.clear() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_drain_clear_expired_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketWriteDrainClearExpiredRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request.bucket,
            "bucket write drain clear expired",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.clear_expired_write_drain(request.now) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_optional_record_response(
                    &StorageRpcBucketWriteDrainOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_drain_heartbeat_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: crate::storage_rpc::StorageRpcBucketWriteDrainHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.route_cluster_epoch,
            request.pg_id,
            &request.record.bucket,
            "bucket write drain heartbeat",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.heartbeat_write_drain(&request.record, request.lease_deadline) {
            Ok(record) => {
                let payload = encode_bucket_write_drain_optional_record_response(
                    &StorageRpcBucketWriteDrainOptionalRecordResponse {
                        record: Some(record),
                    },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => encode_storage_rpc_error_response(
                &bucket_write_drain_heartbeat_error_response(error),
            ),
        }
    }

    fn bucket_write_drain_exists_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route =
            match self.active_bucket_route(route_permit, &request, "bucket write drain exists") {
                Ok(route) => route,
                Err(error) => return encode_storage_rpc_error_response(&error),
            };
        match route.write_drain_exists() {
            Ok(value) => {
                let payload =
                    encode_metadata_command_bool_response(&StorageRpcMetadataCommandBoolResponse {
                        value,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_drain_get_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(route_permit, &request, "bucket write drain get")
        {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.write_drain() {
            Ok(record) => {
                let payload = encode_bucket_write_drain_optional_record_response(
                    &StorageRpcBucketWriteDrainOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_attempt_outcome_record_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketDeleteAttemptOutcomeRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.record.bucket,
            "bucket delete attempt outcome record",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = route
            .open_local_bucket_write_reservation_route(&local_client)
            .and_then(|route| {
                route
                    .record_bucket_delete_attempt_outcome(&request.record)
                    .map_err(StorageNodeBucketRouteError::Bucket)
            });
        match result {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_attempt_outcome_get_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request,
            "bucket delete attempt outcome get",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = route
            .open_local_bucket_write_reservation_route(&local_client)
            .and_then(|route| {
                route
                    .bucket_delete_attempt_outcome()
                    .map_err(StorageNodeBucketRouteError::Bucket)
            });
        match result {
            Ok(record) => {
                let payload = encode_bucket_delete_attempt_outcome_optional_record_response(
                    &StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_write_reservations_list_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request,
            "bucket write reservations list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let result = route
            .open_local_bucket_write_reservation_route(&local_client)
            .and_then(|route| {
                route
                    .durable_bucket_write_reservations()
                    .map_err(StorageNodeBucketRouteError::Bucket)
            });
        match result {
            Ok(records) => {
                let payload = encode_bucket_write_reservations_list_response(
                    &StorageRpcBucketWriteReservationsListResponse { records },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_finalize_roots_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketDeleteFinalizeRootsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
            "bucket delete roots",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.get_bucket_delete_finalize_roots(request.now, request.limit) {
            Ok(roots) => {
                let payload = encode_bucket_delete_finalize_roots_response(
                    &StorageRpcBucketDeleteFinalizeRootsResponse { roots },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_begin_roots_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketDeleteBeginRootsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
            "bucket delete begin roots",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.get_bucket_delete_begin_roots(
            request.now,
            request.start_after_bucket.as_ref(),
            request.limit,
        ) {
            Ok(roots) => {
                let payload = encode_bucket_delete_begin_roots_response(
                    &StorageRpcBucketDeleteBeginRootsResponse { roots },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_finalize_claim_acquire_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request.bucket,
            "bucket delete finalize claim acquire",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.acquire_bucket_delete_finalize_claim(
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
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_finalize_claim_get_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request,
            "bucket delete finalize claim get",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.bucket_delete_finalize_claim() {
            Ok(record) => {
                let payload = encode_bucket_delete_finalize_claim_optional_record_response(
                    &StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_finalize_claim_release_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketDeleteFinalizeClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_bucket_delete_finalize_claim_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.record,
            "bucket delete finalize claim release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.release() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_buckets_list_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "lifecycle sweep bucket list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_buckets_with_lifecycle() {
            Ok(buckets) => {
                let payload = encode_lifecycle_sweep_buckets_response(
                    &StorageRpcLifecycleSweepBucketsResponse { buckets },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_roots_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcLifecycleSweepRootsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "lifecycle sweep roots",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.get_lifecycle_sweep_roots(request.now, request.limit) {
            Ok(roots) => {
                let payload = encode_lifecycle_sweep_roots_response(
                    &StorageRpcLifecycleSweepRootsResponse { roots },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_claim_acquire_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcLifecycleSweepClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route(
            route_permit,
            &request.bucket,
            "lifecycle sweep claim acquire",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.acquire_lifecycle_sweep_claim(
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
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_claim_heartbeat_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcLifecycleSweepClaimHeartbeatRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.record.node_id,
            request.record.cluster_epoch,
            request.record.pg_id,
            &request.record.claim.bucket,
            "lifecycle sweep claim heartbeat",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.heartbeat_lifecycle_sweep_claim(
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
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_claim_error_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcLifecycleSweepClaimErrorRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_route_for_parts(
            route_permit,
            request.record.node_id,
            request.record.cluster_epoch,
            request.record.pg_id,
            &request.record.claim.bucket,
            "lifecycle sweep claim error",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.record_lifecycle_sweep_claim_error(&request.record.claim, &request.last_error) {
            Ok(record) => {
                let payload = encode_lifecycle_sweep_claim_record_response(
                    &StorageRpcLifecycleSweepClaimRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn lifecycle_sweep_claim_release_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcLifecycleSweepClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_lifecycle_sweep_claim_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.claim,
            "lifecycle sweep claim release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.release() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_list_page_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcListObjectsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object list page",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_objects_page(&request.request) {
            Ok(response) => {
                let payload =
                    encode_list_objects_response(&StorageRpcListObjectsResponse { response })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_version_list_page_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcListObjectVersionsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object version list page",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_object_versions_page(&request.request) {
            Ok(response) => {
                let payload =
                    encode_list_object_versions_response(&StorageRpcListObjectVersionsResponse {
                        response,
                    })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_multipart_upload_list_page_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcListMultipartUploadsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object multipart upload list page",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_multipart_uploads_page(&request.request) {
            Ok(response) => {
                let payload = encode_list_multipart_uploads_response(
                    &StorageRpcListMultipartUploadsResponse { response },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_version_next_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route =
            match self.active_object_route(route_permit, &request, "object version allocation") {
                Ok(route) => route,
                Err(error) => return encode_storage_rpc_error_response(&error),
            };
        match route.next_version_id() {
            Ok(version_id) => {
                let payload =
                    encode_object_version_response(&StorageRpcObjectVersionResponse { version_id });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn object_generation_reservation_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectGenerationReservationRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object generation reservation lookup",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let outcome = match route.generation_reservation(&request.reservation_id) {
            Ok(generation_id) => StorageRpcObjectGenerationReservationOutcome::Found(generation_id),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                crate::MetadataError::ObjectGenerationReservationNotFound { reservation_id },
            ))) => StorageRpcObjectGenerationReservationOutcome::NotFound {
                reservation_id: SessionId::try_from(reservation_id).map_err(|_| {
                    crate::storage_rpc::StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "stored reservation id is invalid",
                    )
                })?,
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcDirectPutCommitSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "direct PUT commit snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_direct_put_commit_snapshot(&request.reservation_id, request.generation_id)
        {
            Ok(snapshot) => {
                let payload = encode_direct_put_commit_snapshot_response(
                    &StorageRpcDirectPutCommitSnapshotResponse { snapshot },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn direct_put_commit_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
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
        if request.object.key != request.request.key {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "direct PUT command request key does not match routed object key"
                    .to_string(),
            });
        }
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "direct PUT commit command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.build_direct_put_commit_command(
            &request.request,
            request.version_id,
            &request.expected_snapshot,
        ) {
            Ok(command) => {
                let payload = encode_direct_put_command_build_response(
                    &StorageRpcDirectPutCommandBuildResponse {
                        outcome: StorageRpcDirectPutCommandBuildOutcome::Command(Box::new(command)),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleDirectPutCommitSnapshot,
            )) => {
                let payload = encode_direct_put_command_build_response(
                    &StorageRpcDirectPutCommandBuildResponse {
                        outcome: StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Store(
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                },
            ))) => {
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
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn put_object_metadata_snapshot_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcPutObjectMetadataSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object metadata PUT snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let outcome = match route.load_put_object_metadata_snapshot(request.version_id) {
            Ok(stored) => StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(Box::new(stored)),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::ObjectNotFound,
            ))) => StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcPutObjectMetadataCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "object metadata PUT command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.build_put_object_metadata_command(
            request.requested_version_id,
            &request.expected_stored,
            request.version_id,
            request.mutation,
            &request.bucket_write_reservation,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        kind: StorageRpcMessageKind,
        request: StorageRpcObjectDeleteSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object delete snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let stored = match kind {
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad => {
                route.load_current_object_delete_snapshot()
            }
            StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad => {
                let Some(version_id) = request.version_id else {
                    return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "specific delete snapshot requires version id".to_string(),
                    });
                };
                route.load_specific_object_delete_snapshot(version_id)
            }
            _ => unreachable!("object delete snapshot response called with non-delete kind"),
        };
        let snapshot = match stored {
            Ok(snapshot) => snapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectDeleteSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if request.version_id.is_some() {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "lifecycle version list request must not include version id".to_string(),
            });
        }
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object lifecycle version list load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let versions = match route.list_object_versions_for_lifecycle() {
            Ok(versions) => versions,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcDeleteSpecificObjectCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "delete-specific object command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.build_delete_specific_object_version_command(
            request.version_id,
            request.expected_stored.as_ref(),
            request.expected_target.as_ref(),
            request.expected_version_list.as_deref(),
            &request.bucket_write_reservation,
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("DeleteObjectVersion"),
                ) {
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

    fn delete_current_object_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcDeleteCurrentObjectCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "delete-current object command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.build_delete_current_object_command(
            request.expected_current.as_ref(),
            request.expected_target.as_ref(),
            &request.bucket_write_reservation,
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("DeleteObjectVersion"),
                ) {
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

    fn insert_delete_marker_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcInsertDeleteMarkerCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "insert-delete-marker command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let stale_payload = match request.stale_payload {
            crate::storage_rpc::StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim) => {
                InsertDeleteMarkerStalePayload::Explicit(reclaim)
            }
            crate::storage_rpc::StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                created_at,
            } => InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at },
        };
        let response = match route.build_insert_delete_marker_command(
            request.expected_current.as_ref(),
            request.version_id,
            &request.owner,
            stale_payload,
            request.expected_stale_payload_source.as_ref(),
            &request.bucket_write_reservation,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadMatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream upload match",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let exists = match route
            .matching_stream_upload_exists(&request.request, request.expected_command.as_ref())
        {
            Ok(exists) => exists,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_stream_upload_match_response(&StorageRpcStreamUploadMatchResponse { exists });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_upload_session_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadSessionRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream upload session load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let outcome = match route.load_stream_upload_session(&request.session_id) {
            Ok(session) => StorageRpcStreamUploadSessionOutcome::Loaded(Box::new(session)),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::StreamSessionNotFound { .. },
            ))) => StorageRpcStreamUploadSessionOutcome::NotFound {
                session_id: request.session_id,
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload =
            encode_stream_upload_session_response(&StorageRpcStreamUploadSessionResponse {
                outcome,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn retained_stream_upload_abort_prepare_response(
        &self,
        session: &StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadSessionRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_primary_stream_abort_session_route(
            route_permit,
            &request,
            "retained stream abort prepare",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.object.pg_id);
        let outcome = match route.prepare() {
            Ok(Some(prepared)) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
                prepared.command().clone(),
            )),
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(error, Some("AbortStreamUpload"))
                {
                    Ok(outcome) => outcome,
                    Err(error) => return encode_storage_rpc_error_response(&error),
                }
            }
        };
        let payload = encode_object_metadata_command_build_response(
            &StorageRpcObjectMetadataCommandBuildResponse { outcome },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_upload_bucket_write_reservation_update_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadBucketWriteReservationUpdateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream upload bucket write reservation update",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.update_stream_upload_bucket_write_reservation(
            &request.session_id,
            &request.current,
            &request.renewed,
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline),
        ) {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn stream_upload_segments_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadSessionRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream upload segments load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let outcome = match route.load_stream_upload_segments(&request.session_id) {
            Ok(segments) => StorageRpcStreamUploadSegmentsOutcome::Loaded(segments),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::StreamSessionNotFound { .. },
            ))) => StorageRpcStreamUploadSegmentsOutcome::NotFound {
                session_id: request.session_id,
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadsListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.bucket.node_id,
            request.bucket.cluster_epoch,
            request.bucket.pg_id,
            "object stream uploads list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let page = match route.list_stream_uploads_for_bucket_page(
            &request.bucket.bucket,
            request.session_id_marker.as_ref(),
            request.limit,
        ) {
            Ok(page) => page,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_stream_uploads_list_response(&StorageRpcStreamUploadsListResponse {
            uploads: page.uploads,
            next_session_id_marker: page.next_session_id_marker,
        })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn stream_uploads_pg_list_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamUploadsPgListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object stream uploads PG list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let page = match route
            .list_all_stream_uploads_page(request.session_id_marker.as_ref(), request.limit)
        {
            Ok(page) => page,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_stream_uploads_list_response(&StorageRpcStreamUploadsListResponse {
            uploads: page.uploads,
            next_session_id_marker: page.next_session_id_marker,
        })?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn aborting_multipart_upload_buckets_list_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "aborting multipart upload buckets list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let witnesses = match route.list_aborting_multipart_upload_bucket_witnesses() {
            Ok(witnesses) => witnesses,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        };
        let payload = encode_aborting_multipart_upload_buckets_response(
            &StorageRpcAbortingMultipartUploadBucketsResponse { witnesses },
        )?;
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn bucket_payload_reclaim_root_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object bucket payload reclaim root",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let root = match route.bucket_payload_reclaim_root(&request.bucket) {
            Ok(root) => root,
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let payload =
            encode_payload_reclaim_root_response(&StorageRpcPayloadReclaimRootResponse { root });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_exists_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadReclaimExistsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object payload reclaim exists",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let exists = match route.payload_reclaim_exists(request.generation_id) {
            Ok(exists) => exists,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object payload reclaim root",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let root = match route.payload_reclaim_root() {
            Ok(root) => root,
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let payload =
            encode_payload_reclaim_root_response(&StorageRpcPayloadReclaimRootResponse { root });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_load_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadReclaimExistsRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object payload reclaim load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let reclaim = match route.load_object_payload_reclaim(request.generation_id) {
            Ok(reclaim) => reclaim,
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object payload reclaim claim acquire",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        match route.acquire_object_payload_reclaim_claim(
            request.bucket_incarnation_generation,
            request.generation_id,
            request.reclaim_kind,
            &request.claim_id,
            &request.owner_token,
            request.claimed_at,
            request.lease_deadline,
            request.now,
            effect_fence,
        ) {
            Ok(record) => {
                let payload = encode_object_payload_reclaim_claim_optional_record_response(
                    &StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_payload_reclaim_claim_get_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "object payload reclaim claim get",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.object_payload_reclaim_claim() {
            Ok(record) => {
                let payload = encode_object_payload_reclaim_claim_optional_record_response(
                    &StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse { record },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn object_payload_reclaim_claim_release_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadReclaimClaimRecordRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_object_payload_reclaim_claim_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.record,
            "object payload reclaim claim release",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.release() {
            Ok(()) => Ok(encode_storage_rpc_success_response(&[])),
            Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn stream_segment_append_prepare_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamSegmentAppendPrepareRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream segment append prepare",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let outcome = match route.prepare_stream_segment_append(&request.request, effect_fence) {
            Ok((target, segment)) => StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
                target,
                segment: Box::new(segment),
            },
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::StreamSessionNotFound { .. },
            ))) => StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
                session_id: request.request.session_id,
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMultipartUploadMatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "multipart upload match",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let initiated_at = match route.matching_multipart_upload_initiated_at(
            &request.request,
            request.expected_command.as_ref(),
        ) {
            Ok(initiated_at) => initiated_at,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        kind: StorageRpcMessageKind,
        request: StorageRpcMultipartUploadLoadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "multipart upload load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let outcome = match kind {
            StorageRpcMessageKind::ObjectMultipartUploadLoad => {
                match route.load_multipart_upload(&request.upload_id) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(StorageNodeMultipartUploadRouteError::Upload(
                        BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload { .. }),
                    )) => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                        upload_id: request.upload_id,
                    },
                    Err(StorageNodeMultipartUploadRouteError::Route(error)) => {
                        return encode_storage_rpc_error_response(&error);
                    }
                    Err(StorageNodeMultipartUploadRouteError::Upload(error)) => {
                        return encode_storage_rpc_error_response(&bucket_snapshot_error_response(
                            error,
                        ));
                    }
                }
            }
            StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad => {
                match route.load_in_progress_multipart_upload(&request.upload_id) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                        MetadataError::NoSuchUpload { .. },
                    ))) => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                        upload_id: request.upload_id,
                    },
                    Err(StorageNodeObjectRouteError::Route(error)) => {
                        return encode_storage_rpc_error_response(&error);
                    }
                    Err(StorageNodeObjectRouteError::Object(error)) => {
                        return encode_storage_rpc_error_response(&object_pg_error_response(error));
                    }
                }
            }
            StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad => {
                match route.load_in_progress_multipart_upload_for_listing(&request.upload_id) {
                    Ok(upload) => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(upload)),
                    Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                        MetadataError::NoSuchUpload { .. },
                    ))) => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                        upload_id: request.upload_id,
                    },
                    Err(StorageNodeObjectRouteError::Route(error)) => {
                        return encode_storage_rpc_error_response(&error);
                    }
                    Err(StorageNodeObjectRouteError::Object(error)) => {
                        return encode_storage_rpc_error_response(&object_pg_error_response(error));
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMultipartCompletionSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "multipart completion snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome = match route
            .load_multipart_completion_snapshot(&authorized_upload, &request.requested_part_numbers)
        {
            Ok(snapshot) => {
                StorageRpcMultipartCompletionSnapshotOutcome::Loaded(Box::new(snapshot))
            }
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::NoSuchUpload { .. },
            ))) => StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
                upload_id: authorized_upload.upload_id.clone(),
            },
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::PartNotFound { part_number, .. },
            ))) => StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: authorized_upload.upload_id.clone(),
                part_number,
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMultipartCompletionPreflightRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "multipart completion preflight load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome = match route.load_multipart_completion_preflight(&authorized_upload) {
            Ok(preflight) => StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::NoSuchUpload { .. },
            ))) => StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
                upload_id: authorized_upload.upload_id.clone(),
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMultipartPartsListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_route(
            route_permit,
            &request.object,
            "multipart parts list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            request.authorized_upload,
        );
        let outcome = match route.list_multipart_parts_for_authorized_upload(
            &authorized_upload,
            request.part_number_marker,
            request.max_parts,
        ) {
            Ok(listed) => StorageRpcMultipartPartsListOutcome::Loaded(Box::new(listed)),
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::NoSuchUpload { .. },
            ))) => StorageRpcMultipartPartsListOutcome::NoSuchUpload {
                upload_id: authorized_upload.upload_id.clone(),
            },
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMultipartUploadLoadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_route(
            route_permit,
            &request.object,
            "multipart management lookup",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let lookup = match route.lookup_multipart_upload_management(&request.upload_id) {
            Ok(lookup) => lookup,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcCreateStreamUploadCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "stream upload command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
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
        let response = match route.build_create_stream_upload_command(
            &request.request,
            request.cleanup_after,
            precondition,
            &request.bucket_write_reservation,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::NoSuchUpload { .. },
            ))) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcCreateMultipartUploadCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "multipart upload command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.build_create_multipart_upload_command(
            &request.request,
            request.expected_current.as_ref(),
            &request.bucket_write_reservation,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("CreateMultipartUpload"),
                ) {
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

    fn stream_put_finalize_snapshot_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamPutFinalizeSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream PUT finalize snapshot",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let snapshot = match route.load_stream_put_finalize_snapshot(&request.session_id) {
            Ok(snapshot) => snapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamPutCommitCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "stream PUT commit command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let response = match route.build_stream_put_commit_command(
            &request.session_id,
            request.total_size,
            &request.expected_snapshot,
            &request.commit,
            &request.bucket_write_reservation,
            effect_fence,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleStreamFinalizeSnapshot,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("CommitDirectPutObject"),
                ) {
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

    fn stream_part_finalize_snapshot_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamPartFinalizeSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "stream part finalize snapshot",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let snapshot = match route.load_stream_part_finalize_snapshot(
            &request.upload_id,
            &request.session_id,
            request.part_number,
        ) {
            Ok(snapshot) => snapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcStreamPartCommitCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "stream part commit command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let response = match route.build_stream_part_commit_command(
            &request.upload_id,
            &request.session_id,
            request.part_number,
            &request.expected_snapshot,
            &request.part,
            &request.segments,
            &request.bucket_write_reservation,
            effect_fence,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleStreamFinalizeSnapshot,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcCompleteMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "complete multipart command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.build_complete_multipart_object_command(
            &request.request,
            request.version_id,
            &request.bucket_write_reservation,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleMultipartCompletionSnapshot,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("CommitMultipartObject"),
                ) {
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

    fn multipart_completion_stale_source_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request,
            "multipart completion stale source load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let source = match route.load_multipart_completion_stale_payload_source() {
            Ok(source) => source,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload = encode_multipart_completion_stale_source_response(
            &StorageRpcMultipartCompletionStaleSourceResponse { source },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn abort_multipart_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcAbortMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "abort multipart command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let response = match route.build_abort_multipart_upload_command(
            &request.upload_id,
            request.expected_cleanup.as_ref(),
            &request.bucket_write_reservation,
            effect_fence,
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("AbortMultipartUpload"),
                ) {
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

    fn abort_multipart_cleanup_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: crate::storage_rpc::StorageRpcAbortMultipartCleanupRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "abort multipart cleanup load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let cleanup = match route.load_abort_multipart_upload_cleanup(&request.upload_id) {
            Ok(cleanup) => cleanup,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                return encode_storage_rpc_error_response(&object_pg_error_response(error));
            }
        };
        let payload =
            encode_abort_multipart_cleanup_response(&StorageRpcAbortMultipartCleanupResponse {
                cleanup,
            });
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn object_payload_reclaim_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectPayloadReclaimCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_route(
            route_permit,
            &request.object,
            "object payload reclaim command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let response = match route.build_delete_object_payload_reclaim_command(
            request.generation_id,
            &request.payload,
            &request.claim,
            effect_fence,
        ) {
            Ok(command) => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command)),
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("DeleteObjectPayloadReclaim"),
                ) {
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

    fn authorized_abort_multipart_command_build_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcAuthorizedAbortMultipartCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_mutation_route(
            route_permit,
            &request.object,
            &request.bucket_write_reservation,
            "authorized abort multipart command build",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let authorized_upload = crate::types::AuthorizedMultipartUploadAbort::assume_authorized(
            request.authorized_upload,
        );
        let effect_fence =
            admitted_route_effect_fence(request.object.cluster_epoch, request.effect_deadline);
        let response = match route.build_authorized_abort_multipart_upload_command(
            &authorized_upload,
            request.expected_cleanup.as_ref(),
            &request.bucket_write_reservation,
            effect_fence,
        ) {
            Ok(Some(command)) => {
                StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(command))
            }
            Ok(None) => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
            Err(StorageNodeObjectRouteError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                match object_metadata_command_build_error_outcome(
                    error,
                    Some("AbortMultipartUpload"),
                ) {
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

    fn object_read_auth_subject_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectReadAuthSubjectRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_route(
            route_permit,
            &request.object,
            "object read auth subject load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_object_read_auth_subject(request.version_id) {
            Ok(subject) => {
                let payload = encode_object_read_auth_subject_response(
                    &StorageRpcObjectReadAuthSubjectResponse {
                        outcome: StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(subject)),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Object(ObjectPgActionError::Metadata(
                MetadataError::ObjectNotFound,
            ))) => {
                let payload = encode_object_read_auth_subject_response(
                    &StorageRpcObjectReadAuthSubjectResponse {
                        outcome: StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn object_read_snapshot_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcObjectReadSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_object_route(
            route_permit,
            &request.object,
            "object read snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_object_read_snapshot_for_subject(
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
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::StaleObjectReadSubject,
            )) => {
                let payload =
                    encode_object_read_snapshot_response(&StorageRpcObjectReadSnapshotResponse {
                        outcome: StorageRpcObjectReadSnapshotOutcome::StaleSubject,
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeObjectRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectRouteError::Object(error)) => {
                encode_storage_rpc_error_response(&object_pg_error_response(error))
            }
        }
    }

    fn bucket_head_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
        filtered: bool,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_bucket_route(route_permit, &request, "bucket head") {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.head_bucket(filtered) {
            Ok(info) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::Info(info),
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ))) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::BucketNotFound { name },
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_delete_replica_head_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.bucket_delete_replica_route(
            route_permit,
            &request,
            "bucket delete replica head",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.head_bucket() {
            Ok(info) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::Info(info),
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ))) => {
                let payload =
                    encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                        outcome: StorageRpcBucketInfoOutcome::BucketNotFound { name },
                    });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_snapshot_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketSnapshotRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_bucket_route(
            route_permit,
            &request.bucket,
            "bucket snapshot load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_snapshot(request.request) {
            Ok(snapshot) => {
                let payload = encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::Loaded(Box::new(snapshot)),
                });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ))) => {
                let payload = encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound { name },
                });
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
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
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "create bucket command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let bucket_pg_id = self.node.bucket_metadata_pg_for(&request.bucket);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let config = request.config.as_create_bucket_config();
        let route = match BucketMetadataNodeClient::open_bucket_metadata_route(
            &local_client,
            request.cluster_epoch,
            bucket_pg_id,
            &request.bucket,
        ) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        match route.build_create_bucket_command(request.command_id, &config) {
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

    fn multipart_completion_barrier_command_build_response(
        &self,
        request: StorageRpcMultipartCompletionBarrierCommandBuildRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_primary_pg_for_bucket(
            request.pg_id,
            &request.bucket,
            "multipart completion barrier command build",
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let bucket_pg_id = self.node.bucket_metadata_pg_for(&request.bucket);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match BucketMetadataNodeClient::open_bucket_metadata_route(
            &local_client,
            request.cluster_epoch,
            bucket_pg_id,
            &request.bucket,
        ) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        match route.build_advance_multipart_completion_barrier_command(
            request.command_id,
            &request.completion_target_context,
            &request.bucket_write_reservation,
        ) {
            Ok((barrier_sequence, command)) => {
                let payload = encode_multipart_completion_barrier_command_build_response(
                    &StorageRpcMultipartCompletionBarrierCommandBuildResponse {
                        barrier_sequence,
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
        let bucket_pg_id = self.node.bucket_metadata_pg_for(&request.bucket.bucket);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match BucketMetadataNodeClient::open_bucket_metadata_route(
            &local_client,
            request.bucket.cluster_epoch,
            bucket_pg_id,
            &request.bucket.bucket,
        ) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let result = match (&request.mutation, request.command.payload()) {
            (
                StorageRpcBucketMetadataControlMutation::MarkDeleting,
                MetadataCommandPayload::MarkBucketDeleting(command),
            ) => route.pending_mark_bucket_deleting_command_matches_current(command),
            (
                StorageRpcBucketMetadataControlMutation::Versioning(state),
                MetadataCommandPayload::PutBucketVersioning(command),
            ) => route.pending_put_bucket_versioning_command_matches_current(command, *state),
            (
                StorageRpcBucketMetadataControlMutation::Acl {
                    acl_grants,
                    summary,
                },
                MetadataCommandPayload::PutBucketAcl(command),
            ) => {
                route.pending_put_bucket_acl_command_matches_current(command, acl_grants, *summary)
            }
            (
                StorageRpcBucketMetadataControlMutation::Property(mutation),
                MetadataCommandPayload::PutBucketProperty(command),
            ) => route.pending_put_bucket_property_command_matches_current(command, mutation),
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
        let bucket_pg_id = self.node.bucket_metadata_pg_for(&request.bucket.bucket);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match BucketMetadataNodeClient::open_bucket_metadata_route(
            &local_client,
            request.bucket.cluster_epoch,
            bucket_pg_id,
            &request.bucket.bucket,
        ) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        let result = match &request.mutation {
            StorageRpcBucketMetadataControlMutation::MarkDeleting => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "mark-deleting command build uses a dedicated RPC".to_string(),
                });
            }
            StorageRpcBucketMetadataControlMutation::Versioning(state) => {
                route.build_put_bucket_versioning_command(request.command_id, *state)
            }
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants,
                summary,
            } => route.build_put_bucket_acl_command(request.command_id, acl_grants, *summary),
            StorageRpcBucketMetadataControlMutation::Property(mutation) => {
                route.build_put_bucket_property_command(request.command_id, mutation)
            }
            StorageRpcBucketMetadataControlMutation::Subresource(mutation) => {
                route.build_put_bucket_subresource_command(request.command_id, mutation)
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
        let bucket_pg_id = self.node.bucket_metadata_pg_for(&request.bucket.bucket);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match BucketMetadataNodeClient::open_bucket_metadata_route(
            &local_client,
            request.bucket.cluster_epoch,
            bucket_pg_id,
            &request.bucket.bucket,
        ) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&bucket_snapshot_error_response(error));
            }
        };
        match route.build_mark_bucket_deleting_command(request.command_id) {
            Ok(MarkBucketDeletingCommandBuild::AlreadyDeleting) => {
                let info = match route.head_bucket_raw() {
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketSubresourceGetRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_bucket_route(
            route_permit,
            &request.bucket,
            "bucket subresource get",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.get_subresource(request.kind) {
            Ok(body) => {
                let payload = encode_bucket_subresource_get_response(
                    &StorageRpcBucketSubresourceGetResponse {
                        outcome: StorageRpcBucketSubresourceGetOutcome::Loaded(body),
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ))) => {
                let payload = encode_bucket_subresource_get_response(
                    &StorageRpcBucketSubresourceGetResponse {
                        outcome: StorageRpcBucketSubresourceGetOutcome::BucketNotFound { name },
                    },
                );
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_list_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketListRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.metadata_read_bucket_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "bucket list",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_buckets(&request.owner_canonical_id) {
            Ok(buckets) => {
                let payload =
                    encode_bucket_list_response(&StorageRpcBucketListResponse { buckets })?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_execution_generations_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "bucket execution generations",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_bucket_execution_generations(&request.buckets) {
            Ok(generations) => {
                let payload = encode_bucket_execution_generations_response(
                    &StorageRpcBucketExecutionGenerationsResponse { generations },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn bucket_fast_path_identities_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_bucket_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "bucket fast-path identities",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.load_bucket_fast_path_identities(&request.buckets) {
            Ok(identities) => {
                let payload = encode_bucket_fast_path_identities_response(
                    &StorageRpcBucketFastPathIdentitiesResponse { identities },
                )?;
                Ok(encode_storage_rpc_success_response(&payload))
            }
            Err(StorageNodeBucketRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                encode_storage_rpc_error_response(&bucket_snapshot_error_response(error))
            }
        }
    }

    fn shard_write_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardWriteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let effect_fence =
            admitted_route_effect_fence(request.location.cluster_epoch, request.effect_deadline);
        let route = match self.active_shard_route(
            route_permit,
            request.location,
            &request.shard_key,
            "shard write",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.write_if_absent(&request.payload, effect_fence) {
            Ok(ack) => {
                let payload = encode_shard_write_ack(ack);
                encode_storage_rpc_success_response(&payload)
            }
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_repair_write_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardWriteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if request.effect_deadline.is_some() {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "shard repair write must not carry a frontend effect deadline".to_string(),
            });
        }
        let route = match self.active_shard_route(
            route_permit,
            request.location,
            &request.shard_key,
            "shard repair write",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.repair_write(&request.payload) {
            Ok(ack) => {
                let payload = encode_shard_write_ack(ack);
                encode_storage_rpc_success_response(&payload)
            }
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_read_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_shard_route(
            route_permit,
            request.location,
            &request.shard_key,
            "shard read",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        self.shard_read_file_response(route.read(), request)
    }

    fn shard_historical_read_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcHistoricalShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_shard_inspection_route(
            route_permit,
            &request,
            "historical shard inspection",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.read() {
            Ok(payload) => encode_storage_rpc_success_response(
                &encode_historical_shard_read_response(&payload)?,
            ),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_read_file_response(
        &self,
        payload: Result<Vec<u8>, StorageNodeDataRouteError>,
        request: StorageRpcShardReadRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let response = match payload {
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
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_read_range_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardReadRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_shard_route(
            route_permit,
            request.location,
            &request.shard_key,
            "shard range read",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.read() {
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
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_delete_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardDeleteRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_shard_payload_delete_route(
            route_permit,
            &request,
            "shard payload delete",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.delete() {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_ack_record_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardAckBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "shard ack record",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.record_shard_acks(&request.items) {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_ack_validate_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardAckBatchRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "shard ack validate",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.validate_shard_acks(&request.items) {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_ack_load_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "shard ack load",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.load_shard_ack(&request.shard_key) {
            Ok(ack) => encode_storage_rpc_success_response(&encode_shard_ack_item_response(
                &StorageRpcShardAckItem {
                    shard_key: request.shard_key,
                    ack,
                },
            )),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_ack_historical_load_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_shard_ack_inspection_route(
            route_permit,
            &request,
            "historical shard ack inspection",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.load() {
            Ok(ack) => encode_storage_rpc_success_response(&encode_shard_ack_item_response(
                &StorageRpcShardAckItem {
                    shard_key: request.shard_key,
                    ack,
                },
            )),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_ack_delete_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcShardAckItemRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_shard_ack_delete_route(
            route_permit,
            &request,
            "shard ack delete",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.delete() {
            Ok(()) => encode_storage_rpc_success_response(&[]),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
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
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcScavengerListFilesRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_data_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.data_pg_id,
            "shard scavenger list files",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let response = match route.list_shard_files() {
            Ok(scan) => {
                let payload = encode_scavenger_list_files_response(&scan);
                encode_storage_rpc_success_response(&payload)
            }
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)?
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
        };
        Ok(response)
    }

    fn shard_scavenger_shard_rows_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "shard scavenger shard rows",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_scavenger_shard_rows() {
            Ok(rows) => Ok(encode_storage_rpc_success_response(
                &encode_scavenger_shard_rows_response(&rows),
            )),
            Err(StorageNodeDataRouteError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeDataRouteError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))
            }
        }
    }

    fn shard_scavenger_payload_references_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcBucketPgRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            "shard scavenger payload references",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route.list_shard_scavenger_payload_references() {
            Ok(references) => Ok(encode_storage_rpc_success_response(
                &encode_scavenger_payload_references_response(&references),
            )),
            Err(StorageNodeObjectScanStoreError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectScanStoreError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))
            }
        }
    }

    fn placed_segment_backfill_reference_page_response(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcPlacedSegmentBackfillReferencePageRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.active_primary_object_scan_route(
            route_permit,
            request.route.node_id,
            request.route.cluster_epoch,
            request.route.pg_id,
            "placed segment backfill reference page",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match route
            .list_placed_segment_backfill_reference_page(request.after.as_ref(), request.limit)
        {
            Ok(page) => Ok(encode_storage_rpc_success_response(
                &encode_placed_segment_backfill_reference_page_response(&page)?,
            )),
            Err(StorageNodeObjectScanStoreError::Route(error)) => {
                encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeObjectScanStoreError::Store(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))
            }
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.observation.key.data_pg_id,
            "shard scavenger observation record",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let observation_route =
            match local_client.open_shard_scavenger_observation_route(data_pg_id) {
                Ok(observation_route) => observation_route,
                Err(error) => {
                    return encode_storage_rpc_error_response(&store_error_response(error));
                }
            };
        match observation_route.record_shard_scavenger_observation(&request.observation) {
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
        let data_pg_id = self.validated_data_pg(request.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let observation_route =
            match local_client.open_shard_scavenger_observation_route(data_pg_id) {
                Ok(observation_route) => observation_route,
                Err(error) => {
                    return encode_storage_rpc_error_response(&store_error_response(error));
                }
            };
        match observation_route.list_shard_scavenger_observations() {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.key.data_pg_id,
            "shard scavenger observation resolve",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let observation_route =
            match local_client.open_shard_scavenger_observation_route(data_pg_id) {
                Ok(observation_route) => observation_route,
                Err(error) => {
                    return encode_storage_rpc_error_response(&store_error_response(error));
                }
            };
        match observation_route.resolve_shard_scavenger_observation(&request.key) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.work_item.request.data_pg_id,
            "placed segment shard repair record",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route
            .record_placed_segment_shard_repair(&request.work_item, request.last_error.as_deref())
        {
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
        let data_pg_id = self.validated_data_pg(request.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.cluster_epoch, data_pg_id) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.list_placed_segment_shard_repairs() {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.work_item.request.data_pg_id,
            "placed segment shard repair resolve",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.resolve_placed_segment_shard_repair(&request.work_item) {
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
        let data_pg_id = self.validated_data_pg(request.route.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let Some(lease_deadline) = request.lease_deadline else {
            return encode_storage_rpc_error_response(&store_error_response(
                StoreError::PayloadShardSetMismatch {
                    reason: "durable repair claim lease deadline is required".to_string(),
                },
            ));
        };
        let acquire = PlacedSegmentShardRepairClaimAcquire {
            claim_id: request.claim_id,
            owner_token: request.owner_token,
            cluster_epoch: request.route.cluster_epoch,
            claimed_at: request.claimed_at,
            lease_deadline,
            now: request.now,
        };
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.acquire_placed_segment_shard_repair_claim(&acquire) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.claim.work_item.request.data_pg_id,
            "placed segment shard repair claim complete",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.complete_placed_segment_shard_repair_claim(&request.claim) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.claim.work_item.request.data_pg_id,
            "placed segment shard repair claim error",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.record_placed_segment_shard_repair_claim_error(
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.work_item.request.data_pg_id,
            "placed segment shard backfill record",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.record_placed_segment_shard_backfill(
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
        let data_pg_id = self.validated_data_pg(request.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.cluster_epoch, data_pg_id) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.list_placed_segment_shard_backfills() {
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
        let data_pg_id = self.validated_data_pg(request.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.cluster_epoch, data_pg_id) {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.count_placed_segment_shard_backfills() {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.work_item.request.data_pg_id,
            "placed segment shard backfill exists",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.placed_segment_shard_backfill_exists(&request.work_item) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.work_item.request.data_pg_id,
            "placed segment shard backfill resolve",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.resolve_placed_segment_shard_backfill(&request.work_item) {
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
        let data_pg_id = self.validated_data_pg(request.route.pg_id);
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let Some(lease_deadline) = request.lease_deadline else {
            return encode_storage_rpc_error_response(&store_error_response(
                StoreError::PayloadShardSetMismatch {
                    reason: "durable backfill claim lease deadline is required".to_string(),
                },
            ));
        };
        let acquire = PlacedSegmentShardBackfillClaimAcquire {
            claim_id: request.claim_id,
            owner_token: request.owner_token,
            cluster_epoch: request.route.cluster_epoch,
            claimed_at: request.claimed_at,
            lease_deadline,
            now: request.now,
        };
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.acquire_placed_segment_shard_backfill_claim(&acquire) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.claim.work_item.request.data_pg_id,
            "placed segment shard backfill claim complete",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.complete_placed_segment_shard_backfill_claim(&request.claim) {
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
        let data_pg_id = match validated_request_data_pg(
            &self.node,
            request.route.pg_id,
            request.claim.work_item.request.data_pg_id,
            "placed segment shard backfill claim error",
        ) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let local_client = LocalStorageNodeClient::new(self.config.node_id, Arc::clone(&self.node));
        let route = match local_client.open_shard_ack_route(request.route.cluster_epoch, data_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                return encode_storage_rpc_error_response(&store_error_response(error));
            }
        };
        match route.record_placed_segment_shard_backfill_claim_error(
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route_validation = if request.cluster_epoch < self.config.cluster_epoch {
            self.validate_metadata_command_recovery_read(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
                false,
            )
            .or_else(|_| {
                self.validate_pg_route_for_metadata_transfer_inspection(
                    request.node_id,
                    request.cluster_epoch,
                    request.pg_id,
                )
            })
        } else {
            self.validate_pg_route_for_metadata_transfer_inspection(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
            )
        };
        if let Err(error) = route_validation {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route_validation = if request.cluster_epoch < self.config.cluster_epoch {
            self.validate_metadata_command_recovery_read(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
                false,
            )
        } else {
            self.validate_pg_route_for_metadata_transfer_inspection(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
            )
        };
        if let Err(error) = route_validation {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        pg: &PgStore,
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            let slot = pg.pending_metadata_command_slot(
                self.config.node_id.as_u32(),
                request.cluster_epoch,
            )?;
            let publication_started = slot.as_ref().is_some_and(|slot| slot.publication_started);
            let command = pg.pending_metadata_command_envelope(
                self.config.node_id.as_u32(),
                request.cluster_epoch,
            )?;
            Ok((command, publication_started))
        }) {
            Ok((command, publication_started)) => {
                let payload = encode_metadata_command_pending_envelope_response(
                    &StorageRpcMetadataCommandPendingEnvelopeResponse {
                        command,
                        publication_started,
                    },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_validate_replay_state_response(
        &self,
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_peering_inspection(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandCheckpointCandidatesRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandTransferAdoptRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_mutation(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            false,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandTransferEmptyStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_mutation(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            false,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandTransferMatchingStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_mutation(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            false,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandTransferCheckpointBaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_transfer_mutation(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            false,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandMatchingAppliedRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        let mutation_fence = if request.cluster_epoch == self.config.cluster_epoch {
            if let Err(error) =
                self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
            {
                return encode_storage_rpc_error_response(&error);
            }
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease())
        } else {
            let required_binding = StorageNodeMetadataCommandLockBinding {
                pg_id: request.pg_id,
                cluster_epoch: request.cluster_epoch,
                authority: StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
            };
            if !session.holds_metadata_command_pg_lock_with_binding(required_binding) {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: format!(
                        "historical metadata command abandonment for PG {} epoch {} requires the exact held recovery-primary lock binding",
                        request.pg_id.get(),
                        request.cluster_epoch.get(),
                    ),
                });
            }
            if let Err(error) = self
                .validate_historical_active_pg_route_for_metadata_command_recovery(
                    request.node_id,
                    request.cluster_epoch,
                    request.pg_id,
                )
            {
                return encode_storage_rpc_error_response(&error);
            }
            match self
                .bounded_historical_metadata_command_fence(request.cluster_epoch, request.pg_id)
            {
                Ok(fence) => fence,
                Err(error) => return encode_storage_rpc_error_response(&error),
            }
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        if let Err(error) = mutation_fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.metadata_command_apply_and_record_response_with_allowed_states(
            session,
            request,
            &[PgState::Active],
        )
    }

    fn metadata_command_retained_abort_apply_response(
        &self,
        session: &StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_stream_abort_command_route(
            route_permit,
            &request,
            "retained stream abort apply",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let response = match route.apply() {
            Ok(state) => encode_metadata_command_state_outcome_response(
                &StorageRpcMetadataCommandStateOutcomeResponse {
                    outcome: StorageRpcMetadataCommandStateOutcome::State(state),
                },
            ),
            Err(StorageNodeRetainedStreamAbortApplyError::Apply(
                BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                }),
            )) => encode_metadata_command_state_outcome_response(
                &StorageRpcMetadataCommandStateOutcomeResponse {
                    outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                    },
                },
            ),
            Err(StorageNodeRetainedStreamAbortApplyError::Route(error)) => {
                return encode_storage_rpc_error_response(&error);
            }
            Err(StorageNodeRetainedStreamAbortApplyError::Apply(error)) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::Internal,
                    message: error.to_string(),
                });
            }
        };
        Ok(encode_storage_rpc_success_response(&response))
    }

    fn metadata_command_retained_abort_finish_response(
        &self,
        session: &StorageNodeSession,
        route_permit: &StorageNodeRouteAdmissionPermit,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let route = match self.retained_primary_stream_abort_command_route(
            route_permit,
            &request,
            "retained stream abort finish",
        ) {
            Ok(route) => route,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let removed = match route.finish() {
            Ok(removed) => removed,
            Err(StorageNodeRetainedStreamAbortFinishError::Route(error)) => {
                return encode_storage_rpc_error_response(&error)
            }
            Err(StorageNodeRetainedStreamAbortFinishError::Finish(error)) => {
                return encode_storage_rpc_error_response(&store_error_response(error))
            }
        };
        let payload = encode_metadata_command_pending_slot_remove_response(
            &StorageRpcMetadataCommandPendingSlotRemoveResponse { removed },
        );
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn metadata_command_recovery_apply_and_record_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRecoveryRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let mutation_fence = match self.validate_reissued_metadata_command_recovery(
            request.node_id,
            request.pg_id,
            &request.authorized_source,
            request.abandoned_source.as_ref(),
            &request.command,
        ) {
            Ok(fence) => fence,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        self.metadata_command_apply_and_record_response_with_mutation_fence(
            session,
            StorageRpcMetadataCommandRequest {
                node_id: request.node_id,
                cluster_epoch: request.cluster_epoch,
                pg_id: request.pg_id,
                command: request.command,
            },
            mutation_fence,
        )
    }

    fn metadata_command_recovery_record_abandoned_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRecoveryRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let mutation_fence = match self.validate_reissued_metadata_command_recovery(
            request.node_id,
            request.pg_id,
            &request.authorized_source,
            request.abandoned_source.as_ref(),
            &request.command,
        ) {
            Ok(fence) => fence,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        if let Err(error) = mutation_fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match self.node.get_pg(request.pg_id.get()).and_then(|pg| {
            pg.record_metadata_command_abandoned(self.config.node_id.as_u32(), &request.command)
        }) {
            Ok(state) => encode_metadata_command_state_outcome_response(
                &StorageRpcMetadataCommandStateOutcomeResponse {
                    outcome: StorageRpcMetadataCommandStateOutcome::State(state),
                },
            ),
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
                encode_metadata_command_state_outcome_response(
                    &StorageRpcMetadataCommandStateOutcomeResponse {
                        outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                            node_id,
                            pg_id,
                            cluster_epoch,
                            log_index,
                        },
                    },
                )
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(encode_storage_rpc_success_response(&response))
    }

    fn metadata_command_peering_replay_apply_and_record_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        let mutation_fence = match self.validate_pg_route_for_metadata_transfer_mutation(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            true,
        ) {
            Ok(fence) => fence,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        self.metadata_command_apply_and_record_response_with_mutation_fence(
            session,
            request,
            mutation_fence,
        )
    }

    fn metadata_command_apply_and_record_response_with_allowed_states(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
        allowed_states: &[PgState],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        let mutation_fence = if allowed_states == [PgState::Active] {
            match self.validate_pg_route_for_metadata_command_apply(
                request.node_id,
                request.pg_id,
                &request.command,
            ) {
                Ok(fence) => fence,
                Err(error) => return encode_storage_rpc_error_response(&error),
            }
        } else {
            if let Err(error) = self.validate_pg_route_with_allowed_states(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
                allowed_states,
            ) {
                return encode_storage_rpc_error_response(&error);
            }
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease())
        };
        self.metadata_command_apply_and_record_response_with_mutation_fence(
            session,
            request,
            mutation_fence,
        )
    }

    fn metadata_command_apply_and_record_response_with_mutation_fence(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
        mutation_fence: StorageNodeRouteFence,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        if let Err(error) = mutation_fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        #[cfg(test)]
        if let Some(hook) = self
            .metadata_command_before_commit_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            hook(request.node_id, &request.command);
        }
        let response = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => metadata_command_state_result_response(
                &request.command,
                pg.apply_metadata_command_and_record_with_commit_guard(
                    self.config.node_id.as_u32(),
                    &request.command,
                    || {
                        mutation_fence.validate_store_at(
                            crate::clock::current_time_millis(),
                            crate::clock::monotonic_time_millis(),
                        )
                    },
                ),
            )?,
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_acceptance_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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

    fn metadata_command_publication_start_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        let required_binding = StorageNodeMetadataCommandLockBinding {
            pg_id: request.pg_id,
            cluster_epoch: request.cluster_epoch,
            authority: if request.cluster_epoch < self.config.cluster_epoch {
                StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary
            } else {
                StorageNodeMetadataCommandLockAuthority::CurrentPrimary
            },
        };
        if !session.holds_metadata_command_pg_lock_with_binding(required_binding) {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "metadata command publication start for PG {} epoch {} requires the exact held primary lock binding",
                    request.pg_id.get(),
                    request.cluster_epoch.get(),
                ),
            });
        }
        let response = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => {
                let result = pg
                    .mark_pending_metadata_command_publication_started(
                        self.config.node_id.as_u32(),
                        &request.command,
                    )
                    .and_then(|()| {
                        pg.metadata_command_replica_state()
                            .map_err(BucketSnapshotLoadError::Store)
                    });
                metadata_command_state_result_response(&request.command, result)?
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_abandon_acceptance_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = validate_metadata_command_request_epoch(&request) {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
        if let Err(error) =
            admitted_route_effect_fence(request.cluster_epoch, request.effect_deadline)
                .require_valid_for(request.command.id().cluster_epoch())
        {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
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
        session: &StorageNodeSession,
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
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
        if let Err(error) =
            admitted_route_effect_fence(request.cluster_epoch, request.effect_deadline)
                .require_valid_for(request.command.id().cluster_epoch())
        {
            return encode_storage_rpc_error_response(&store_error_response(error));
        }
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
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
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
        let response = match self.node.cluster_map_history_route_references() {
            Ok(references) => {
                let payload = encode_cluster_map_history_reference_summary_response(
                    &StorageRpcClusterMapHistoryReferenceSummaryResponse { references },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }
}

include!("storage_node_server/request_validation.rs");

include!("storage_node_server/session.rs");

include!("storage_node_server/tests.rs");
