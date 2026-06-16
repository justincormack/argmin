use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use placement::NodeId;
use s3_types::{AclGrants, BucketVersioningState};

use crate::error::{
    BucketSnapshotLoadError, BucketWriteDrainError, MetadataError, ObjectPgActionError, StoreError,
};
use crate::metadata_command::{
    AbortMultipartUploadCommand, AdvanceCompletedMultipartUploadSequenceCommand,
    BucketPropertyMutation, BucketRecord, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MarkBucketDeletingCommand, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
    MetadataCommandReplicaState, ObjectPayloadReclaimCommand, PutBucketAclCommand,
    PutBucketPropertyCommand, PutBucketSubresourceCommand, PutBucketVersioningCommand,
    PutObjectMetadataCommand, PutObjectMetadataMutation,
};
use crate::node::SharedStorageNode;
use crate::pg_store::{ScavengerShardFileScan, ScavengerShardRow};
use crate::storage_rpc::{
    decode_abort_multipart_cleanup_response,
    decode_bucket_delete_finalize_claim_optional_record_response,
    decode_bucket_delete_finalize_roots_response, decode_bucket_delete_finalized_response,
    decode_bucket_execution_generations_response, decode_bucket_fast_path_identities_response,
    decode_bucket_info_outcome_response, decode_bucket_list_response,
    decode_bucket_mark_deleting_command_build_response,
    decode_bucket_metadata_control_command_build_response, decode_bucket_snapshot_pair_response,
    decode_bucket_snapshot_response, decode_bucket_subresource_get_response,
    decode_bucket_write_drain_begin_response, decode_bucket_write_drain_optional_record_response,
    decode_bucket_write_reservation_record_response,
    decode_completed_multipart_order_command_build_response,
    decode_completed_multipart_uploads_list_response, decode_create_bucket_command_build_response,
    decode_direct_put_command_build_response, decode_direct_put_commit_snapshot_response,
    decode_lifecycle_sweep_buckets_response, decode_lifecycle_sweep_claim_optional_record_response,
    decode_lifecycle_sweep_claim_record_response, decode_lifecycle_sweep_roots_response,
    decode_list_multipart_uploads_response, decode_list_object_versions_response,
    decode_list_objects_response, decode_metadata_command_acceptance_response,
    decode_metadata_command_applied_hashes_response, decode_metadata_command_bool_outcome_response,
    decode_metadata_command_bool_response, decode_metadata_command_max_log_index_response,
    decode_metadata_command_next_id_response, decode_metadata_command_pending_envelope_response,
    decode_metadata_command_pending_slot_insert_response,
    decode_metadata_command_pending_slot_remove_response,
    decode_metadata_command_state_outcome_response, decode_metadata_command_state_response,
    decode_multipart_completion_preflight_response, decode_multipart_completion_snapshot_response,
    decode_multipart_completion_stale_source_response, decode_multipart_management_lookup_response,
    decode_multipart_parts_list_response, decode_multipart_upload_load_response,
    decode_multipart_upload_match_response, decode_object_delete_snapshot_response,
    decode_object_generation_reservation_response, decode_object_generation_response,
    decode_object_lifecycle_version_list_response, decode_object_metadata_command_build_response,
    decode_object_payload_reclaim_claim_optional_record_response,
    decode_object_payload_reclaim_response, decode_object_read_auth_subject_response,
    decode_object_read_snapshot_response, decode_object_tags_for_subject_response,
    decode_object_version_response, decode_payload_reclaim_root_response,
    decode_put_object_metadata_snapshot_response, decode_read_handle_acquire_response,
    decode_read_handle_release_response, decode_scavenger_list_files_response,
    decode_scavenger_observations_response, decode_scavenger_payload_references_response,
    decode_scavenger_shard_rows_response, decode_shard_ack_item_response,
    decode_shard_read_range_response, decode_shard_read_response, decode_shard_write_ack,
    decode_storage_rpc_response_payload, decode_stream_part_finalize_snapshot_response,
    decode_stream_put_finalize_snapshot_response, decode_stream_segment_append_prepare_response,
    decode_stream_upload_match_response, decode_stream_upload_segments_response,
    decode_stream_upload_session_response, decode_stream_uploads_list_response,
    encode_abort_multipart_cleanup_request, encode_abort_multipart_command_build_request,
    encode_authorized_abort_multipart_command_build_request, encode_bucket_batch_request,
    encode_bucket_delete_finalize_claim_acquire_request,
    encode_bucket_delete_finalize_claim_record_request,
    encode_bucket_delete_finalize_roots_request, encode_bucket_list_request,
    encode_bucket_mark_deleting_command_build_request,
    encode_bucket_metadata_control_command_build_request,
    encode_bucket_metadata_control_pending_match_request, encode_bucket_pg_request,
    encode_bucket_request, encode_bucket_snapshot_pair_request, encode_bucket_snapshot_request,
    encode_bucket_subresource_get_request, encode_bucket_write_drain_begin_request,
    encode_bucket_write_drain_clear_expired_request, encode_bucket_write_drain_heartbeat_request,
    encode_bucket_write_drain_record_request, encode_bucket_write_reservation_acquire_request,
    encode_bucket_write_reservation_heartbeat_request,
    encode_bucket_write_reservation_proof_request, encode_bucket_write_reservation_record_request,
    encode_complete_multipart_command_build_request,
    encode_completed_multipart_order_command_build_request,
    encode_completed_multipart_uploads_list_request, encode_create_bucket_command_build_request,
    encode_create_multipart_upload_command_build_request,
    encode_create_stream_upload_command_build_request,
    encode_delete_current_object_command_build_request,
    encode_delete_specific_object_command_build_request, encode_direct_put_command_build_request,
    encode_direct_put_commit_snapshot_request, encode_insert_delete_marker_command_build_request,
    encode_lifecycle_sweep_claim_acquire_request, encode_lifecycle_sweep_claim_error_request,
    encode_lifecycle_sweep_claim_heartbeat_request, encode_lifecycle_sweep_claim_record_request,
    encode_lifecycle_sweep_roots_request, encode_list_multipart_uploads_request,
    encode_list_object_versions_request, encode_list_objects_request,
    encode_metadata_command_matching_applied_request, encode_metadata_command_next_id_request,
    encode_metadata_command_pending_slot_replace_request,
    encode_metadata_command_pending_slot_request, encode_metadata_command_request,
    encode_metadata_command_state_request, encode_multipart_completion_preflight_request,
    encode_multipart_completion_snapshot_request, encode_multipart_parts_list_request,
    encode_multipart_upload_load_request, encode_multipart_upload_match_request,
    encode_object_delete_snapshot_request, encode_object_generation_reservation_request,
    encode_object_payload_reclaim_claim_acquire_request,
    encode_object_payload_reclaim_claim_record_request,
    encode_object_payload_reclaim_exists_request, encode_object_read_auth_subject_request,
    encode_object_read_snapshot_request, encode_object_request,
    encode_object_tags_for_subject_request, encode_proof_release_request,
    encode_put_object_metadata_command_build_request, encode_put_object_metadata_snapshot_request,
    encode_read_handle_acquire_request, encode_read_handle_release_request,
    encode_scavenger_list_files_request, encode_scavenger_observation_key_request,
    encode_scavenger_observation_record_request, encode_shard_ack_batch_request,
    encode_shard_ack_item_request, encode_shard_delete_request, encode_shard_read_range_request,
    encode_shard_read_request, encode_shard_write_request,
    encode_stream_part_commit_command_build_request, encode_stream_part_finalize_snapshot_request,
    encode_stream_put_commit_command_build_request, encode_stream_put_finalize_snapshot_request,
    encode_stream_segment_append_prepare_request, encode_stream_upload_match_request,
    encode_stream_upload_session_request, encode_stream_uploads_list_request,
    encode_stream_uploads_pg_list_request, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    StorageRpcAbortMultipartCleanupRequest, StorageRpcAbortMultipartCommandBuildRequest,
    StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcBucketBatchRequest,
    StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
    StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcBucketDeleteFinalizeRootsRequest,
    StorageRpcBucketDeleteFinalizedOutcome, StorageRpcBucketDeleteFinalizedResponse,
    StorageRpcBucketInfoOutcome, StorageRpcBucketListRequest,
    StorageRpcBucketMarkDeletingCommandBuildOutcome,
    StorageRpcBucketMarkDeletingCommandBuildRequest,
    StorageRpcBucketMetadataControlCommandBuildRequest, StorageRpcBucketMetadataControlMutation,
    StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcBucketPgRequest,
    StorageRpcBucketRequest, StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotPairOutcome,
    StorageRpcBucketSnapshotPairRequest, StorageRpcBucketSnapshotRequest,
    StorageRpcBucketSubresourceGetRequest, StorageRpcBucketWriteDrainBeginOutcome,
    StorageRpcBucketWriteDrainBeginRequest, StorageRpcBucketWriteDrainClearExpiredRequest,
    StorageRpcBucketWriteDrainHeartbeatRequest, StorageRpcBucketWriteDrainRecordRequest,
    StorageRpcBucketWriteReservationAcquireOutcome, StorageRpcBucketWriteReservationAcquireRequest,
    StorageRpcBucketWriteReservationHeartbeatRequest, StorageRpcBucketWriteReservationProofRequest,
    StorageRpcBucketWriteReservationRecordRequest, StorageRpcCompleteMultipartCommandBuildRequest,
    StorageRpcCompletedMultipartOrderCommandBuildRequest,
    StorageRpcCompletedMultipartUploadsListRequest, StorageRpcCreateBucketCommandBuildOutcome,
    StorageRpcCreateBucketCommandBuildRequest, StorageRpcCreateBucketConfig,
    StorageRpcCreateMultipartUploadCommandBuildRequest,
    StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcCreateStreamUploadPrecondition,
    StorageRpcDeleteCurrentObjectCommandBuildRequest,
    StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcDirectPutCommandBuildOutcome,
    StorageRpcDirectPutCommandBuildRequest, StorageRpcDirectPutCommitSnapshotRequest,
    StorageRpcErrorCode, StorageRpcErrorResponse, StorageRpcFrame,
    StorageRpcInsertDeleteMarkerCommandBuildRequest, StorageRpcInsertDeleteMarkerStalePayload,
    StorageRpcLifecycleSweepClaimAcquireRequest, StorageRpcLifecycleSweepClaimErrorRequest,
    StorageRpcLifecycleSweepClaimHeartbeatRequest, StorageRpcLifecycleSweepClaimRecordRequest,
    StorageRpcLifecycleSweepRootsRequest, StorageRpcListMultipartUploadsRequest,
    StorageRpcListObjectVersionsRequest, StorageRpcListObjectsRequest, StorageRpcMessageKind,
    StorageRpcMetadataCommandAcceptanceOutcome, StorageRpcMetadataCommandAppliedHashesOutcome,
    StorageRpcMetadataCommandBoolOutcome, StorageRpcMetadataCommandMatchingAppliedRequest,
    StorageRpcMetadataCommandNextIdOutcome, StorageRpcMetadataCommandNextIdRequest,
    StorageRpcMetadataCommandPendingSlotInsertOutcome,
    StorageRpcMetadataCommandPendingSlotReplaceRequest,
    StorageRpcMetadataCommandPendingSlotRequest, StorageRpcMetadataCommandRequest,
    StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateRequest,
    StorageRpcMultipartCompletionPreflightOutcome, StorageRpcMultipartCompletionPreflightRequest,
    StorageRpcMultipartCompletionSnapshotOutcome, StorageRpcMultipartCompletionSnapshotRequest,
    StorageRpcMultipartPartsListOutcome, StorageRpcMultipartPartsListRequest,
    StorageRpcMultipartUploadLoadOutcome, StorageRpcMultipartUploadLoadRequest,
    StorageRpcMultipartUploadMatchRequest, StorageRpcObjectDeleteSnapshotRequest,
    StorageRpcObjectDeleteSnapshotResponse, StorageRpcObjectGenerationReservationOutcome,
    StorageRpcObjectGenerationReservationRequest, StorageRpcObjectMetadataCommandBuildOutcome,
    StorageRpcObjectPayloadReclaimClaimAcquireRequest,
    StorageRpcObjectPayloadReclaimClaimRecordRequest, StorageRpcObjectPayloadReclaimExistsRequest,
    StorageRpcObjectReadAuthSubjectOutcome, StorageRpcObjectReadAuthSubjectRequest,
    StorageRpcObjectReadSnapshotOutcome, StorageRpcObjectReadSnapshotRequest,
    StorageRpcObjectRequest, StorageRpcObjectTagsForSubjectOutcome,
    StorageRpcObjectTagsForSubjectRequest, StorageRpcPayloadReclaimRootResponse,
    StorageRpcProofReleaseRequest, StorageRpcPutObjectMetadataCommandBuildRequest,
    StorageRpcPutObjectMetadataSnapshotOutcome, StorageRpcPutObjectMetadataSnapshotRequest,
    StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleReleaseRequest,
    StorageRpcScavengerListFilesRequest, StorageRpcScavengerObservationKeyRequest,
    StorageRpcScavengerObservationRecordRequest, StorageRpcShardAckBatchRequest,
    StorageRpcShardAckItem, StorageRpcShardAckItemRequest, StorageRpcShardDeleteRequest,
    StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest, StorageRpcShardWriteRequest,
    StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcStreamPartFinalizeSnapshotRequest,
    StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcStreamPutFinalizeSnapshotRequest,
    StorageRpcStreamSegmentAppendPrepareOutcome, StorageRpcStreamSegmentAppendPrepareRequest,
    StorageRpcStreamUploadMatchRequest, StorageRpcStreamUploadSegmentsOutcome,
    StorageRpcStreamUploadSessionOutcome, StorageRpcStreamUploadSessionRequest,
    StorageRpcStreamUploadsListRequest, StorageRpcStreamUploadsPgListRequest,
};
use crate::traits::{DurableBucketWriteReservationHeartbeat, PgMetadataStore, ShardStore};
use crate::types::{
    AbortMultipartUploadCleanup, AuthorizedMultipartUploadRecord, BucketDeleteFinalizeClaimRecord,
    BucketDeleteFinalizeRoot, BucketFastPathIdentity, BucketInfo, BucketName, BucketSnapshot,
    BucketSnapshotPair, BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState,
    BucketSubresourceKind, BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch,
    CommitDirectPutObjectReq, CompleteMultipartCommitCleanup, CompleteMultipartCommitRequest,
    CompletedMultipartUploadRecordPage, CreateBucketConfig, CreateMultipartUploadReq,
    CreateStreamUploadReq, DataPgId, DirectPutCommitSnapshot, DirectPutCommitStorageSnapshot,
    EcShape, GenerationId, LifecycleSweepBuckets, LifecycleSweepClaimRecord, LifecycleSweepRoot,
    ListMultipartUploadsReq, ListMultipartUploadsResp, ListObjectVersionsReq,
    ListObjectVersionsResp, ListObjectsReq, ListObjectsResp, ListPartsReq, ListedMultipartParts,
    LiveObjectRecord, MultipartCompletionPreflight, MultipartCompletionSnapshot,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadManagementLookup,
    MultipartUploadRecord, ObjectEtag, ObjectKey, ObjectLayout, ObjectPartRecord,
    ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind, ObjectReadAuthSubject,
    ObjectReadAuthSubjectIdentity, ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectSegmentRecord,
    ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord, OwnerIdentity,
    PayloadReclaimRoot, PgId, PrepareStreamUploadSegmentAppendReq, PutLiveObjectReq, SessionId,
    ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
    ShardScavengerObservationRecord, ShardScavengerPayloadReference, StoredObject,
    StreamPutCommitInput, StreamPutFinalizeStorageSnapshot, StreamUploadCommandRecord,
    StreamUploadPartStorageSnapshot, StreamUploadRecord, StreamUploadRecordPage,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, TerminalStreamCleanupRecord,
    UploadId, UploadState, VersionId, WriteAck,
};

mod unix_admission;
mod unix_rpc;
mod unix_sessions;

#[cfg(test)]
pub(crate) use unix_admission::shared_unix_storage_node_rpc_admission_with_wait_timeout;
pub(crate) use unix_admission::{
    listing_probe_admission_class, shared_unix_storage_node_rpc_admission,
    storage_rpc_admission_class, UnixStorageNodeRpcAdmission, UnixStorageNodeRpcAdmissionAcquire,
    UnixStorageNodeRpcAdmissionClass, UnixStorageNodeRpcAdmissionPermit,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT,
};

fn merge_bucket_snapshot_pair_request(
    source: BucketSnapshotRequest,
    destination: BucketSnapshotRequest,
) -> BucketSnapshotRequest {
    BucketSnapshotRequest {
        policy: source.policy || destination.policy,
        tags: match (source.tags, destination.tags) {
            (BucketSnapshotTagsRequest::Always, _) | (_, BucketSnapshotTagsRequest::Always) => {
                BucketSnapshotTagsRequest::Always
            }
            (BucketSnapshotTagsRequest::IfBucketAbacEnabled, _)
            | (_, BucketSnapshotTagsRequest::IfBucketAbacEnabled) => {
                BucketSnapshotTagsRequest::IfBucketAbacEnabled
            }
            (BucketSnapshotTagsRequest::NotRequested, BucketSnapshotTagsRequest::NotRequested) => {
                BucketSnapshotTagsRequest::NotRequested
            }
        },
        lifecycle: source.lifecycle || destination.lifecycle,
        cors: source.cors || destination.cors,
    }
}

fn load_multipart_upload_from_pg(
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
        && stream_upload_bucket_write_reservation_matches_command(existing, create)
}

fn stream_upload_bucket_write_reservation_matches_command(
    existing: &StreamUploadRecord,
    create: &CreateStreamUploadCommand,
) -> bool {
    match create.session.target {
        StreamUploadTarget::PutObject => {
            existing.bucket_write_reservation.as_ref() == Some(&create.bucket_write_reservation)
        }
        StreamUploadTarget::UploadPart { .. } => existing.bucket_write_reservation.is_none(),
    }
}

fn multipart_upload_matches_command(
    existing: &MultipartUploadRecord,
    create: &CreateMultipartUploadCommand,
) -> bool {
    *existing == create.upload
}

fn reject_duplicate_stream_segment_index(
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
    session_id: &SessionId,
    part_number: u32,
) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
    let session = pg.get_stream_upload(session_id)?;
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
    pg: &crate::PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
    generation_id: GenerationId,
) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
    let reserved_generation =
        PgMetadataStore::get_object_generation_reservation(pg, bucket, key, reservation_id)?;
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
        stale_payload_source,
        stale_payload,
    })
}

fn snapshot_upload_part_stream_cleanup_from_pg(
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
                if part.part_okh == [0u8; 16] {
                    streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                        pg,
                        bucket,
                        key,
                        record.version_id,
                        part.part_number,
                    )?);
                }
            }
            Ok(ObjectPayloadReclaimCommand::Multipart(
                multipart_reclaim_from_parts(
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
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
    pg: &crate::PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    stored: Option<StoredObject>,
) -> Result<ObjectDeleteStorageSnapshot, MetadataError> {
    let target = delete_command_target_from_stored(pg, bucket, key, stored.as_ref())?;
    Ok(ObjectDeleteStorageSnapshot { stored, target })
}

fn live_delete_command_target(
    pg: &crate::PgStore,
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

fn multipart_reclaim_from_parts(
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
    created_at: u64,
    parts: &[ObjectPartRecord],
    streaming_segments: &[MultipartPartSegmentRecord],
) -> MultipartReclaimRecord {
    use std::collections::BTreeMap;

    let mut segments_by_part: BTreeMap<u32, Vec<MultipartReclaimPartSegmentRecord>> =
        BTreeMap::new();
    for segment in streaming_segments {
        segments_by_part
            .entry(segment.part_number)
            .or_default()
            .push(MultipartReclaimPartSegmentRecord {
                part_number: segment.part_number,
                segment_index: segment.segment_index,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            });
    }

    MultipartReclaimRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_id,
        created_at,
        parts: parts
            .iter()
            .map(|part| {
                if part.part_okh == [0u8; 16] {
                    MultipartReclaimPartRecord::Segments {
                        part_number: part.part_number,
                        segments: segments_by_part
                            .remove(&part.part_number)
                            .unwrap_or_default(),
                    }
                } else {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number: part.part_number,
                        part_okh: part.part_okh,
                        part_vid: part.part_vid,
                        data_pg_id: part.data_pg_id,
                        ec: EcShape {
                            k: part.ec_k,
                            m: part.ec_m,
                        },
                    }
                }
            })
            .collect(),
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarkBucketDeletingCommandBuild {
    AlreadyDeleting,
    Command(Box<MetadataCommandEnvelope>),
}

#[derive(Debug, Clone)]
pub(crate) enum CreateBucketCommandBuild {
    Exists(BucketInfo),
    Command(Box<MetadataCommandEnvelope>),
}

pub(crate) trait BucketMetadataNodeClient: Send + Sync {
    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError>;

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError>;

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError>;

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError>;

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError>;

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError>;

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError>;

    fn load_bucket_execution_generations(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError>;

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError>;
}

pub(crate) trait BucketWriteReservationNodeClient: Send + Sync {
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

    #[allow(clippy::too_many_arguments)]
    fn acquire_completion_durable_bucket_write_reservation(
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
        self.acquire_durable_bucket_write_reservation(
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
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
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

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError>;

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError>;

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError>;

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError>;

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;
}

pub(crate) trait ObjectGenerationMetadataNodeClient: Send + Sync {
    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError>;
}

pub(crate) trait ObjectVersionMetadataNodeClient: Send + Sync {
    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError>;

    fn next_completion_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id(pg_id, bucket, key)
    }
}

pub(crate) trait DirectPutMetadataNodeClient: Send + Sync {
    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError>;

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait ObjectListingMetadataNodeClient: Send + Sync {
    fn list_objects_page(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError>;

    fn list_object_versions_page(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError>;

    fn list_multipart_uploads_page(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError>;
}

pub(crate) trait ObjectMutationMetadataNodeClient: Send + Sync {
    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError>;

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError>;

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError>;

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError>;

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError>;

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError>;

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError>;

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError>;

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError>;

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError>;

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError>;

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError>;

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError>;

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError>;

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError>;

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError>;

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_multipart_completion_stale_payload_source(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError>;

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError>;

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;
}

pub(crate) trait ObjectReadMetadataNodeClient: Send + Sync {
    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError>;

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: VersionId,
    ) -> Result<Option<String>, ObjectPgActionError>;
}

pub(crate) struct BuildStreamPutCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) session_id: &'a SessionId,
    pub(crate) total_size: u64,
    pub(crate) expected_snapshot: &'a StreamPutFinalizeStorageSnapshot,
    pub(crate) commit: &'a StreamPutCommitInput,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDirectPutCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CommitDirectPutObjectReq,
    pub(crate) version_id: VersionId,
    pub(crate) expected_snapshot: &'a DirectPutCommitStorageSnapshot,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) enum CreateStreamUploadPrecondition<'a> {
    PutObjectNoCurrentCheck {
        require_generation_reservation: bool,
    },
    PutObject {
        expected_current: Option<&'a StoredObject>,
        require_generation_reservation: bool,
    },
    UploadPart {
        expected_upload: &'a MultipartUploadRecord,
    },
}

pub(crate) struct BuildCreateStreamUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CreateStreamUploadReq,
    pub(crate) precondition: CreateStreamUploadPrecondition<'a>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCreateMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CreateMultipartUploadReq,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildStreamPartCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) session_id: &'a SessionId,
    pub(crate) part_number: u32,
    pub(crate) expected_snapshot: &'a StreamUploadPartStorageSnapshot,
    pub(crate) part: &'a MultipartPartRecord,
    pub(crate) segments: &'a [MultipartPartSegmentRecord],
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCompleteMultipartObjectCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CompleteMultipartCommitRequest,
    pub(crate) version_id: VersionId,
    pub(crate) expected_object_parts: &'a [ObjectPartRecord],
    pub(crate) completion_order: u64,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

struct AbortMultipartCommandValidation<'a> {
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    upload_id: &'a UploadId,
    expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildAbortMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

pub(crate) struct BuildAuthorizedAbortMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) authorized_upload: &'a AuthorizedMultipartUploadRecord,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

pub(crate) struct BuildPutObjectMetadataCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) requested_version_id: Option<VersionId>,
    pub(crate) expected_stored: &'a StoredObject,
    pub(crate) version_id: VersionId,
    pub(crate) mutation: PutObjectMetadataMutation,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteSpecificObjectVersionCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) version_id: VersionId,
    pub(crate) expected_stored: Option<&'a StoredObject>,
    pub(crate) expected_target: Option<&'a DeleteObjectVersionTarget>,
    pub(crate) expected_version_list: Option<&'a [StoredObject]>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteCurrentObjectCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) expected_target: Option<&'a DeleteObjectVersionTarget>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectDeleteStorageSnapshot {
    pub(crate) stored: Option<StoredObject>,
    pub(crate) target: Option<DeleteObjectVersionTarget>,
}

pub(crate) struct BuildInsertDeleteMarkerCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) version_id: VersionId,
    pub(crate) owner: &'a OwnerIdentity,
    pub(crate) stale_payload: InsertDeleteMarkerStalePayload,
    pub(crate) expected_stale_payload_source: Option<&'a StoredObject>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) enum InsertDeleteMarkerStalePayload {
    Explicit(Option<ObjectPayloadReclaimCommand>),
    SnapshotCurrentNullLive { created_at: u64 },
}

pub(crate) trait PlacedShardNodeClient: Send + Sync {
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
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError>;

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError>;

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError>;
}

pub(crate) trait ShardReadHandleLease: Send {
    fn release(&mut self) -> Result<(), StoreError>;
}

pub(crate) trait ShardReadHandleNodeClient: Send + Sync {
    fn acquire_read_handles(
        &self,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleLease>, StoreError>;
}

pub(crate) trait ShardAckNodeClient: Send + Sync {
    fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError>;

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError>;

    fn load_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<WriteAck, StoreError>;

    fn delete_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<(), StoreError>;
}

pub(crate) trait ShardScavengerNodeClient: Send + Sync {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError>;

    fn list_scavenger_shard_rows(&self, pg_id: PgId) -> Result<Vec<ScavengerShardRow>, StoreError>;

    fn list_shard_scavenger_payload_references(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError>;

    fn record_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError>;

    fn list_shard_scavenger_observations(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError>;

    fn resolve_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError>;
}

pub(crate) trait MetadataCommandNodeClient: Send + Sync {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandNodeClient>, StoreError>;

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError>;

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError>;

    fn next_completion_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least(pg_id, cluster_epoch, min_log_index)
    }

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

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
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

pub(crate) trait StorageNodeClient:
    ShardScavengerNodeClient + MetadataCommandNodeClient
{
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

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError>;

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError>;

    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError>;

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError>;

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError>;

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError>;

    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError>;

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError>;

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError>;

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: s3_types::VersionId,
    ) -> Result<Option<String>, ObjectPgActionError>;

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError>;

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError>;

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError>;

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError>;

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError>;

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError>;

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError>;

    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<s3_types::VersionId, ObjectPgActionError>;

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError>;

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError>;

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError>;

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError>;

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError>;

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError>;

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError>;

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError>;

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError>;

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError>;

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError>;

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError>;

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
}

#[derive(Clone)]
pub(crate) struct LocalStorageNodeClient {
    node_id: NodeId,
    storage_node: Arc<SharedStorageNode>,
}

#[allow(dead_code)]
pub(crate) struct UnixStorageNodeClient {
    node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    socket_path: PathBuf,
    next_request_id: AtomicU64,
    rpc_admission: Arc<UnixStorageNodeRpcAdmission>,
}

struct LocalStorageNodeReadHandleLease;

#[allow(dead_code)]
impl UnixStorageNodeClient {
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
            StorageRpcBucketInfoOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
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
        let response =
            decode_bucket_metadata_control_command_build_response(&response).map_err(|error| {
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

#[derive(Clone, Copy)]
struct MetadataCommandLogConflictRpcFields {
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
}

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

impl ObjectListingMetadataNodeClient for UnixStorageNodeClient {
    fn list_objects_page(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListObjectsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                start_after: req.start_after.clone(),
                start_at: req.start_at.clone(),
                max_keys: req.max_keys,
            },
        };
        let payload = encode_list_objects_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode object list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectListPage,
                payload,
                listing_probe_admission_class(req.max_keys),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_objects_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object list response", error.to_string()),
            )
        })?;
        validate_list_objects_response(self, &response.response, req)?;
        Ok(response.response)
    }

    fn list_object_versions_page(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectVersionsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListObjectVersionsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                key_marker: req.key_marker.clone(),
                version_id_marker: req.version_id_marker,
                start_at: req.start_at.clone(),
                max_keys: req.max_keys,
            },
        };
        let payload = encode_list_object_versions_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode object version list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectVersionListPage,
                payload,
                listing_probe_admission_class(req.max_keys),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_object_versions_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object version list response", error.to_string()),
            )
        })?;
        validate_list_object_versions_response(self, &response.response, req)?;
        Ok(response.response)
    }

    fn list_multipart_uploads_page(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListMultipartUploadsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListMultipartUploadsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                key_marker: req.key_marker.clone(),
                upload_id_marker: req.upload_id_marker.clone(),
                max_uploads: req.max_uploads,
            },
        };
        let payload = encode_list_multipart_uploads_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode multipart upload list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectMultipartUploadListPage,
                payload,
                listing_probe_admission_class(req.max_uploads),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_multipart_uploads_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode multipart upload list response", error.to_string()),
            )
        })?;
        validate_list_multipart_uploads_response(self, &response.response, req)?;
        Ok(response.response)
    }
}

fn validate_list_objects_response(
    client: &UnixStorageNodeClient,
    response: &ListObjectsResp,
    req: &ListObjectsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.objects.len() > req.max_keys as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate object list response",
            "object list response exceeds requested max keys".to_string(),
        )));
    }
    for object in &response.objects {
        if object.bucket() != &req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "object list response bucket does not match request".to_string(),
            )));
        }
    }
    match (response.is_truncated, response.next_start_after.as_ref()) {
        (false, None) => {}
        (false, Some(_)) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "non-truncated object list response has next marker".to_string(),
            )));
        }
        (true, Some(marker))
            if response
                .objects
                .last()
                .is_some_and(|object| marker == object.key()) => {}
        (true, _) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "truncated object list response marker does not match last object".to_string(),
            )));
        }
    }
    Ok(())
}

fn validate_list_object_versions_response(
    client: &UnixStorageNodeClient,
    response: &ListObjectVersionsResp,
    req: &ListObjectVersionsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.versions.len() > req.max_keys as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate object version list response",
            "object version list response exceeds requested max keys".to_string(),
        )));
    }
    for object in &response.versions {
        if object.bucket() != &req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object version list response",
                "object version list response bucket does not match request".to_string(),
            )));
        }
    }
    match (
        response.is_truncated,
        response.next_key_marker.as_ref(),
        response.next_version_id_marker,
    ) {
        (false, None, None) => {}
        (false, _, _) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object version list response",
                "non-truncated object version list response has next marker".to_string(),
            )));
        }
        (true, Some(key_marker), Some(version_marker))
            if response.versions.last().is_some_and(|object| {
                key_marker == object.key() && version_marker == object.version_id()
            }) => {}
        (true, _, _) => {
            return Err(BucketSnapshotLoadError::Store(
                client.rpc_payload_error(
                    "validate object version list response",
                    "truncated object version list response marker does not match last version"
                        .to_string(),
                ),
            ));
        }
    }
    Ok(())
}

fn validate_list_multipart_uploads_response(
    client: &UnixStorageNodeClient,
    response: &ListMultipartUploadsResp,
    req: &ListMultipartUploadsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.uploads.len() > req.max_uploads as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate multipart upload list response",
            "multipart upload list response exceeds requested max uploads".to_string(),
        )));
    }
    for upload in &response.uploads {
        if upload.bucket != req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate multipart upload list response",
                "multipart upload list response bucket does not match request".to_string(),
            )));
        }
    }
    match (
        response.is_truncated,
        response.next_key_marker.as_ref(),
        response.next_upload_id_marker.as_ref(),
    ) {
        (false, None, None) => {}
        (false, _, _) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate multipart upload list response",
                "non-truncated multipart upload list response has next marker".to_string(),
            )));
        }
        (true, Some(key_marker), Some(upload_id_marker))
            if response.uploads.last().is_some_and(|upload| {
                key_marker == &upload.key && upload_id_marker == &upload.upload_id
            }) => {}
        (true, _, _) => {
            return Err(BucketSnapshotLoadError::Store(
                client.rpc_payload_error(
                    "validate multipart upload list response",
                    "truncated multipart upload list response marker does not match last upload"
                        .to_string(),
                ),
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
    client: &UnixStorageNodeClient,
    pg_id: PgId,
    bucket: &BucketName,
    reservation_id: &str,
    owner_token: &str,
    cluster_epoch: ClusterEpoch,
    operation_kind: &str,
    created_at: u64,
    lease_deadline: Option<u64>,
    target_context: Option<&str>,
    class: UnixStorageNodeRpcAdmissionClass,
) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
    let request = StorageRpcBucketWriteReservationAcquireRequest {
        node_id: client.node_id,
        cluster_epoch,
        pg_id,
        bucket: bucket.clone(),
        reservation_id: reservation_id.to_string(),
        owner_token: owner_token.to_string(),
        operation_kind: operation_kind.to_string(),
        created_at,
        lease_deadline,
        target_context: target_context.map(str::to_string),
    };
    let payload = encode_bucket_write_reservation_acquire_request(&request).map_err(|error| {
        BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "encode bucket write reservation acquire request",
            error.to_string(),
        ))
    })?;
    let response = client
        .rpc_request_with_admission_class(
            StorageRpcMessageKind::BucketWriteReservationAcquire,
            payload,
            class,
        )
        .map_err(BucketSnapshotLoadError::Store)?;
    let response = decode_bucket_write_reservation_record_response(&response).map_err(|error| {
        BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "decode bucket write reservation acquire response",
            error.to_string(),
        ))
    })?;
    let record = match response.outcome {
        StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => record,
        StorageRpcBucketWriteReservationAcquireOutcome::Draining => {
            return Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDraining,
            ));
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { name }
            if name == *bucket =>
        {
            return Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ));
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { .. } => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate bucket write reservation acquire response",
                "bucket not found response identity does not match request".to_string(),
            )));
        }
    };
    if record.bucket != *bucket
        || record.reservation_id != reservation_id
        || record.owner_token != owner_token
        || record.cluster_epoch != cluster_epoch
        || record.operation_kind != operation_kind
        || record.created_at != created_at
        || record.lease_deadline != lease_deadline
        || record.target_context.as_deref() != target_context
    {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate bucket write reservation acquire response",
            "reservation response identity does not match request".to_string(),
        )));
    }
    Ok(record)
}

impl BucketWriteReservationNodeClient for UnixStorageNodeClient {
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainExists, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_metadata_command_bool_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write drain exists response",
                error.to_string(),
            ))
        })?;
        Ok(response.value)
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
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
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
            storage_rpc_admission_class(StorageRpcMessageKind::BucketWriteReservationAcquire),
        )
    }

    fn acquire_completion_durable_bucket_write_reservation(
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
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
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
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }
    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationProofRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            proof: proof.clone(),
        };
        let payload = encode_bucket_write_reservation_proof_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket write reservation proof request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketWriteReservationValidate,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write reservation validate response",
            &response,
        )
    }

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationHeartbeatRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            proof: proof.clone(),
            lease_deadline,
        };
        let payload =
            encode_bucket_write_reservation_heartbeat_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write reservation heartbeat request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketWriteReservationHeartbeat,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_write_reservation_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write reservation heartbeat response",
                    error.to_string(),
                ))
            })?;
        let record = match response.outcome {
            StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => record,
            StorageRpcBucketWriteReservationAcquireOutcome::Draining
            | StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { .. } => {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket write reservation heartbeat response",
                    "heartbeat response returned non-record outcome".to_string(),
                )));
            }
        };
        if !proof.matches_record(&record) || record.lease_deadline != Some(lease_deadline) {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservation heartbeat response",
                "heartbeat response identity does not match request".to_string(),
            )));
        }
        Ok(record)
    }

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: record.clone(),
        };
        let payload =
            encode_bucket_write_reservation_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write reservation release request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketWriteReservationRelease,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write reservation release response",
            &response,
        )
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcProofReleaseRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            proof: proof.clone(),
        };
        let payload = encode_proof_release_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode proof release request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ProofRelease, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_proof_release_response(&response)?;
        Ok(())
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
        let request = StorageRpcBucketWriteDrainBeginRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            drain_id: drain_id.to_string(),
            owner_token: owner_token.to_string(),
            created_at,
            lease_deadline,
        };
        let payload =
            encode_bucket_write_drain_begin_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain begin request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainBegin, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_write_drain_begin_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write drain begin response",
                error.to_string(),
            ))
        })?;
        let record = match response.outcome {
            StorageRpcBucketWriteDrainBeginOutcome::Acquired(record) => record,
            StorageRpcBucketWriteDrainBeginOutcome::Conflict => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict {
                        drain_id: drain_id.to_string(),
                    },
                ));
            }
        };
        if record.bucket != *bucket
            || record.drain_id != drain_id
            || record.owner_token != owner_token
            || record.cluster_epoch != cluster_epoch
            || record.created_at != created_at
            || record.lease_deadline != lease_deadline
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain begin response",
                "drain response identity does not match request".to_string(),
            )));
        }
        Ok(record)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: record.clone(),
        };
        let payload =
            encode_bucket_write_drain_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain clear request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainClear, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write drain clear response",
            &response,
        )
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainClearExpiredRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            now,
        };
        let payload =
            encode_bucket_write_drain_clear_expired_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain clear expired request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainClearExpired, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_write_drain_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write drain clear expired response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket write drain clear expired response",
                    "expired drain bucket does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainHeartbeatRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: record.clone(),
            lease_deadline,
        };
        let payload = encode_bucket_write_drain_heartbeat_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket write drain heartbeat request",
                error.to_string(),
            ))
        })?;
        let kind = StorageRpcMessageKind::BucketWriteDrainHeartbeat;
        let response = self
            .rpc_request_result(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = match response {
            Ok(response) => response,
            Err(error) if error.code == StorageRpcErrorCode::BucketWriteDrainConflict => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict {
                        drain_id: error.message,
                    },
                ));
            }
            Err(error) if error.code == StorageRpcErrorCode::BucketWriteDrainNotFound => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainNotFound {
                        drain_id: error.message,
                    },
                ));
            }
            Err(error) => {
                return Err(BucketSnapshotLoadError::Store(
                    self.rpc_response_error(kind, error),
                ));
            }
        };
        let response =
            decode_bucket_write_drain_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write drain heartbeat response",
                    error.to_string(),
                ))
            })?;
        let Some(renewed) = response.record else {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain heartbeat response",
                "heartbeat response returned no drain record".to_string(),
            )));
        };
        if renewed.bucket != record.bucket
            || renewed.drain_id != record.drain_id
            || renewed.owner_token != record.owner_token
            || renewed.cluster_epoch != record.cluster_epoch
            || renewed.bucket_execution_generation != record.bucket_execution_generation
            || renewed.lease_deadline != Some(lease_deadline)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain heartbeat response",
                "heartbeat response identity does not match request".to_string(),
            )));
        }
        Ok(renewed)
    }

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteReservationsList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = crate::storage_rpc::decode_bucket_write_reservations_list_response(
            &response,
        )
        .map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write reservations list response",
                error.to_string(),
            ))
        })?;
        if response
            .records
            .iter()
            .any(|record| record.bucket != *bucket)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservations list response",
                "reservation bucket does not match request".to_string(),
            )));
        }
        Ok(response.records)
    }

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketDeleteFinalized, payload)
            .map_err(BucketWriteDrainError::Store)?;
        let response =
            decode_bucket_delete_finalized_response(&response).map_err(|error| {
                BucketWriteDrainError::Store(self.rpc_payload_error(
                    "decode bucket delete finalized response",
                    error.to_string(),
                ))
            })?;
        self.validate_bucket_delete_finalized_response(response, bucket)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteFinalizeRootsRequest {
            route: StorageRpcBucketPgRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
            },
            now,
            limit,
        };
        let payload = encode_bucket_delete_finalize_roots_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket delete finalize roots request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketDeleteFinalizeRoots, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_delete_finalize_roots_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete finalize roots response",
                    error.to_string(),
                ))
            })?;
        if response.roots.len() > limit {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket delete finalize roots response",
                "root count exceeds request limit".to_string(),
            )));
        }
        Ok(response.roots)
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
        let request = StorageRpcBucketDeleteFinalizeClaimAcquireRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            bucket_incarnation_generation,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload =
            encode_bucket_delete_finalize_claim_acquire_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket delete finalize claim acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire,
            payload,
        )?;
        let response = decode_bucket_delete_finalize_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete finalize claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket delete finalize claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteFinalizeClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: claim.clone(),
        };
        let payload =
            encode_bucket_delete_finalize_claim_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket delete finalize claim release request",
                    error.to_string(),
                ))
            })?;
        let kind = StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease;
        let response = self.rpc_request_bucket_snapshot(kind, payload)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket delete finalize claim release response",
            &response,
        )
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepRootsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            now,
            limit,
        };
        let payload = encode_lifecycle_sweep_roots_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode lifecycle sweep roots request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::LifecycleSweepRoots, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_lifecycle_sweep_roots_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode lifecycle sweep roots response", error.to_string()),
            )
        })?;
        if response.roots.len() > limit {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep roots response",
                "root count exceeds request limit".to_string(),
            )));
        }
        Ok(response.roots)
    }

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError> {
        let request = StorageRpcBucketPgRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        };
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode lifecycle sweep buckets request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::LifecycleSweepBucketsList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_lifecycle_sweep_buckets_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep buckets response",
                    error.to_string(),
                ))
            })?;
        Ok(response.buckets)
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
        let request = StorageRpcLifecycleSweepClaimAcquireRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            bucket_incarnation_generation,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload = encode_lifecycle_sweep_claim_acquire_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim acquire request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimAcquire,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate lifecycle sweep claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimHeartbeatRequest {
            record: StorageRpcLifecycleSweepClaimRecordRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                claim: claim.clone(),
            },
            heartbeat_at,
            lease_deadline,
        };
        let payload =
            encode_lifecycle_sweep_claim_heartbeat_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode lifecycle sweep claim heartbeat request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim heartbeat response",
                    error.to_string(),
                ))
            })?;
        if !lifecycle_sweep_claim_identity_matches(&response.record, claim)
            || response.record.heartbeat_at != heartbeat_at
            || response.record.lease_deadline != lease_deadline
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep claim heartbeat response",
                "claim response identity does not match request".to_string(),
            )));
        }
        Ok(response.record)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimErrorRequest {
            record: StorageRpcLifecycleSweepClaimRecordRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                claim: claim.clone(),
            },
            last_error: last_error.to_string(),
        };
        let payload = encode_lifecycle_sweep_claim_error_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim error request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimError,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim error response",
                    error.to_string(),
                ))
            })?;
        if !lifecycle_sweep_claim_identity_matches(&response.record, claim)
            || response.record.last_error.as_deref() != Some(last_error)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep claim error response",
                "claim response identity does not match request".to_string(),
            )));
        }
        Ok(response.record)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            claim: claim.clone(),
        };
        let payload = encode_lifecycle_sweep_claim_record_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim release request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimRelease,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode lifecycle sweep claim release response",
            &response,
        )
    }
}

impl ObjectGenerationMetadataNodeClient for UnixStorageNodeClient {
    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectGenerationReservationRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            reservation_id: reservation_id.clone(),
        };
        let payload = encode_object_generation_reservation_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectGenerationReservation, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_generation_reservation_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object generation reservation response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectGenerationReservationOutcome::Found(generation_id) => Ok(generation_id),
            StorageRpcObjectGenerationReservationOutcome::NotFound { reservation_id } => Err(
                ObjectPgActionError::Metadata(MetadataError::ObjectGenerationReservationNotFound {
                    reservation_id: reservation_id.into_string(),
                }),
            ),
        }
    }

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectGenerationNext, payload)
            .map_err(ObjectPgActionError::Store)?;
        decode_object_generation_response(&response)
            .map(|response| response.generation_id)
            .map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error("decode object generation response", error.to_string()),
                )
            })
    }
}

impl ObjectVersionMetadataNodeClient for UnixStorageNodeClient {
    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id_with_admission_class(
            pg_id,
            bucket,
            key,
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectVersionNext),
        )
    }

    fn next_completion_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id_with_admission_class(
            pg_id,
            bucket,
            key,
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }
}

impl UnixStorageNodeClient {
    fn next_object_version_id_with_admission_class(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Result<VersionId, ObjectPgActionError> {
        let request = StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectVersionNext,
                payload,
                class,
            )
            .map_err(ObjectPgActionError::Store)?;
        decode_object_version_response(&response)
            .map(|response| response.version_id)
            .map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error("decode object version response", error.to_string()),
                )
            })
    }
}

impl DirectPutMetadataNodeClient for UnixStorageNodeClient {
    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcDirectPutCommitSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            reservation_id: reservation_id.clone(),
            generation_id,
        };
        let payload = encode_direct_put_commit_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::DirectPutCommitSnapshotLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_commit_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode direct PUT commit snapshot response",
                error.to_string(),
            ))
        })?;
        self.validate_direct_put_commit_snapshot_response(&response.snapshot, bucket, key)?;
        Ok(response.snapshot)
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcDirectPutCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: request.cluster_epoch,
                pg_id: request.pg_id,
                bucket: request.request.bucket.clone(),
                key: request.request.key.clone(),
            },
            request: request.request.clone(),
            version_id: request.version_id,
            expected_snapshot: request.expected_snapshot.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_direct_put_command_build_request(&rpc_request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode direct PUT commit command build request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::DirectPutCommitCommandBuild, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_command_build_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode direct PUT commit command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcDirectPutCommandBuildOutcome::Command(command) => {
                self.validate_direct_put_command_build_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleDirectPutCommitSnapshot)
            }
            StorageRpcDirectPutCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != request.pg_id.get() {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode direct PUT commit command build response",
                        "metadata command log conflict route mismatch".to_string(),
                    )));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode direct PUT commit command build response",
                        "metadata command log conflict index must not be zero".to_string(),
                    )));
                }
                Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: conflict_pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ))
            }
        }
    }
}

impl ObjectMutationMetadataNodeClient for UnixStorageNodeClient {
    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        let request = StorageRpcStreamUploadMatchRequest {
            object: self.object_request(pg_id, &create.bucket, &create.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_stream_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream upload match request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload match response", error.to_string()),
            )
        })?;
        self.validate_stream_upload_match_response(response.exists, expected_command)?;
        Ok(response.exists)
    }

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadMatchRequest {
            object: self.object_request(pg_id, &create.bucket, &create.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_multipart_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode multipart upload match request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode multipart upload match response", error.to_string()),
            )
        })?;
        self.validate_multipart_upload_match_response(response.initiated_at, expected_command)?;
        Ok(response.initiated_at)
    }

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_upload_session_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadSessionLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_session_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload session response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcStreamUploadSessionOutcome::Loaded(session) => {
                self.validate_stream_upload_session_response(
                    &session,
                    bucket,
                    key,
                    session_id,
                    "validate stream upload session response",
                )?;
                Ok(*session)
            }
            StorageRpcStreamUploadSessionOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != *session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream upload session response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartUploadLoad, payload)
            .map_err(BucketSnapshotLoadError::from)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            BucketSnapshotLoadError::from(
                self.rpc_payload_error("decode multipart upload load response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    None,
                    "validate multipart upload load response",
                )
                .map_err(|error| match error {
                    ObjectPgActionError::Store(store) => BucketSnapshotLoadError::Store(store),
                    ObjectPgActionError::Metadata(metadata) => {
                        BucketSnapshotLoadError::Metadata(metadata)
                    }
                    other => BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate multipart upload load response",
                        other.to_string(),
                    )),
                })?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(BucketSnapshotLoadError::from(self.rpc_payload_error(
                        "validate multipart upload load response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    },
                ))
            }
        }
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode in-progress multipart upload load response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    "validate in-progress multipart upload load response",
                )?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate in-progress multipart upload load response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode in-progress multipart upload listing response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    "validate in-progress multipart upload listing response",
                )?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate in-progress multipart upload listing response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
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
        let request = StorageRpcMultipartCompletionSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
            requested_part_numbers: requested_part_numbers.to_vec(),
        };
        let payload = encode_multipart_completion_snapshot_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode multipart completion snapshot request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion snapshot response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcMultipartCompletionSnapshotOutcome::Loaded(snapshot) => {
                self.validate_multipart_completion_snapshot_response(
                    &snapshot,
                    authorized_upload,
                    requested_part_numbers,
                )?;
                Ok(*snapshot)
            }
            StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
            StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: returned_upload_id,
                part_number,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part upload id does not match request".to_string(),
                    )));
                }
                if !requested_part_numbers.contains(&part_number) {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part number does not match request".to_string(),
                    )));
                }
                Err(MetadataError::PartNotFound {
                    upload_id: upload_id.to_string(),
                    part_number,
                }
                .into())
            }
        }
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let request = StorageRpcMultipartCompletionPreflightRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
        };
        let payload = encode_multipart_completion_preflight_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode multipart completion preflight request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_preflight_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion preflight response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight) => Ok(preflight),
            StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion preflight response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
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
        let request = StorageRpcMultipartPartsListRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
            part_number_marker,
            max_parts,
        };
        let payload = encode_multipart_parts_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode multipart parts list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartPartsList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_parts_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode multipart parts list response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartPartsListOutcome::Loaded(listed) => {
                self.validate_listed_multipart_parts_response(
                    &listed,
                    authorized_upload,
                    part_number_marker,
                    max_parts,
                )?;
                Ok(*listed)
            }
            StorageRpcMultipartPartsListOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart parts list response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartManagementLookup,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_management_lookup_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode multipart management lookup response",
                error.to_string(),
            ))
        })?;
        self.validate_multipart_management_lookup_response(
            &response.lookup,
            bucket,
            key,
            upload_id,
        )?;
        Ok(response.lookup)
    }

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let precondition = match request.precondition {
            CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation,
            } => StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation,
            },
            CreateStreamUploadPrecondition::PutObject {
                expected_current,
                require_generation_reservation,
            } => StorageRpcCreateStreamUploadPrecondition::PutObject {
                expected_current: expected_current.cloned(),
                require_generation_reservation,
            },
            CreateStreamUploadPrecondition::UploadPart { expected_upload } => {
                StorageRpcCreateStreamUploadPrecondition::UploadPart {
                    expected_upload: expected_upload.clone(),
                }
            }
        };
        let rpc_request = StorageRpcCreateStreamUploadCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            precondition,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_create_stream_upload_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream upload command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild,
            request.pg_id,
            payload,
            "decode stream upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_create_stream_upload_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None if matches!(
                request.precondition,
                CreateStreamUploadPrecondition::UploadPart { .. }
            ) =>
            {
                let StreamUploadTarget::UploadPart { upload_id, .. } = &request.request.target
                else {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode stream upload command build response",
                        "stream upload command build missing outcome for non-upload-part target"
                            .to_string(),
                    )));
                };
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode stream upload command build response",
                "stream upload command build cannot return missing".to_string(),
            ))),
        }
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcCreateMultipartUploadCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            expected_current: request.expected_current.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_create_multipart_upload_command_build_request(&rpc_request).map_err(
            |error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode multipart upload command build request",
                    error.to_string(),
                ))
            },
        )?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartUploadCommandBuild,
            request.pg_id,
            payload,
            "decode multipart upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_create_multipart_upload_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode multipart upload command build response",
                "multipart upload command build cannot return missing".to_string(),
            ))),
        }
    }

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_upload_session_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_segments_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload segments response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcStreamUploadSegmentsOutcome::Loaded(segments) => {
                self.validate_stream_upload_segments_response(
                    &segments,
                    session_id,
                    "validate stream upload segments response",
                )?;
                Ok(segments)
            }
            StorageRpcStreamUploadSegmentsOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != *session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream upload segments response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream uploads list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectStreamUploadsList,
                payload,
                listing_probe_admission_class(limit),
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream uploads list response", error.to_string()),
            )
        })?;
        if response.uploads.len() > limit as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "response exceeded requested page limit".to_string(),
            )));
        }
        for upload in &response.uploads {
            if &upload.bucket != bucket {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate stream uploads list response",
                    "upload bucket does not match request".to_string(),
                )));
            }
            if session_id_marker.is_some_and(|marker| upload.session_id.as_str() <= marker.as_str())
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate stream uploads list response",
                    "upload is not after requested marker".to_string(),
                )));
            }
        }
        if response
            .uploads
            .windows(2)
            .any(|pair| pair[0].session_id.as_str() >= pair[1].session_id.as_str())
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "uploads are not strictly ordered by session id".to_string(),
            )));
        }
        if response.next_session_id_marker.as_ref()
            != response.uploads.last().map(|upload| &upload.session_id)
            && response.next_session_id_marker.is_some()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "next marker does not match the last returned upload".to_string(),
            )));
        }
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsPgListRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_pg_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream uploads PG list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadsPgList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream uploads PG list response", error.to_string()),
            )
        })?;
        if response.uploads.len() > limit as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "response exceeded requested page limit".to_string(),
            )));
        }
        if response
            .uploads
            .windows(2)
            .any(|pair| pair[0].session_id.as_str() >= pair[1].session_id.as_str())
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "uploads are not strictly ordered by session id".to_string(),
            )));
        }
        if session_id_marker.is_some()
            && response.uploads.iter().any(|upload| {
                session_id_marker
                    .is_some_and(|marker| upload.session_id.as_str() <= marker.as_str())
            })
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "upload is not after requested marker".to_string(),
            )));
        }
        if response.next_session_id_marker.as_ref()
            != response.uploads.last().map(|upload| &upload.session_id)
            && response.next_session_id_marker.is_some()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "next marker does not match the last returned upload".to_string(),
            )));
        }
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError> {
        let request = StorageRpcCompletedMultipartUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            upload_id_marker: upload_id_marker.cloned(),
            limit,
        };
        let payload =
            encode_completed_multipart_uploads_list_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode completed multipart upload list request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectCompletedMultipartUploadsList,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_completed_multipart_uploads_list_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode completed multipart upload list response",
                    error.to_string(),
                ))
            })?;
        if response.records.len() > limit as usize {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart upload list response",
                "response exceeded requested page limit".to_string(),
            )));
        }
        for record in &response.records {
            if &record.bucket != bucket {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate completed multipart upload list response",
                    "record bucket does not match request".to_string(),
                )));
            }
            if upload_id_marker.is_some_and(|marker| record.upload_id.as_str() <= marker.as_str()) {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate completed multipart upload list response",
                    "record is not after requested marker".to_string(),
                )));
            }
        }
        if response
            .records
            .windows(2)
            .any(|pair| pair[0].upload_id.as_str() >= pair[1].upload_id.as_str())
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart upload list response",
                "records are not strictly ordered by upload id".to_string(),
            )));
        }
        if response.next_upload_id_marker.as_ref()
            != response.records.last().map(|record| &record.upload_id)
            && response.next_upload_id_marker.is_some()
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart upload list response",
                "next marker does not match the last returned record".to_string(),
            )));
        }
        Ok(CompletedMultipartUploadRecordPage {
            records: response.records,
            next_upload_id_marker: response.next_upload_id_marker,
        })
    }

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self.object_request(pg_id, bucket, key),
            generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimExists, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_metadata_command_bool_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode object payload reclaim exists response",
                error.to_string(),
            ))
        })?;
        Ok(response.value)
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode object bucket payload reclaim root response",
                error.to_string(),
            ))
        })?;
        self.validate_bucket_payload_reclaim_root_response(&response, bucket)?;
        Ok(response.root)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimRoot, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode object payload reclaim root response",
                error.to_string(),
            ))
        })?;
        Ok(response.root)
    }

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self.object_request(pg_id, bucket, key),
            generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_object_payload_reclaim_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object payload reclaim response", error.to_string()),
            )
        })?;
        if !reclaim_matches_bucket_key_generation(
            response.reclaim.as_ref(),
            bucket,
            key,
            generation_id,
        ) {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate object payload reclaim response",
                "response reclaim payload does not match request".to_string(),
            )));
        }
        Ok(response.reclaim)
    }

    #[allow(clippy::too_many_arguments)]
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
        let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            bucket_incarnation_generation,
            generation_id,
            reclaim_kind,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload =
            encode_object_payload_reclaim_claim_acquire_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode object payload reclaim claim acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
            payload,
        )?;
        let response = decode_object_payload_reclaim_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode object payload reclaim claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.key != *key
                || record.generation_id != generation_id
                || record.reclaim_kind != reclaim_kind
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate object payload reclaim claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: claim.clone(),
        };
        let payload =
            encode_object_payload_reclaim_claim_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode object payload reclaim claim release request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode object payload reclaim claim release response",
            &response,
        )
    }

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let expected_session =
            self.load_stream_upload_session(pg_id, bucket, key, &request.session_id)?;
        let expected_target = expected_session.target;
        let rpc_request = StorageRpcStreamSegmentAppendPrepareRequest {
            object: self.object_request(pg_id, bucket, key),
            request: request.clone(),
        };
        let payload = encode_stream_segment_append_prepare_request(&rpc_request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_segment_append_prepare_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream segment append prepare response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcStreamSegmentAppendPrepareOutcome::Prepared { target, segment } => {
                self.validate_stream_segment_append_prepare_response(
                    &segment,
                    &target,
                    &expected_target,
                    request,
                    "validate stream segment append prepare response",
                )?;
                Ok((target, *segment))
            }
            StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != request.session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream segment append prepare response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: request.session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcStreamPutFinalizeSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_put_finalize_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_put_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.validate_stream_put_finalize_snapshot_response(
            &response.snapshot,
            bucket,
            key,
            session_id,
        )?;
        Ok(response.snapshot)
    }

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcStreamPutCommitCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            session_id: request.session_id.clone(),
            total_size: request.total_size,
            expected_snapshot: request.expected_snapshot.clone(),
            commit: request.commit.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_stream_put_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream PUT commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT commit command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_stream_put_commit_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT commit command build response",
                    "stream PUT commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode stream PUT commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
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
        let request = StorageRpcStreamPartFinalizeSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
            session_id: session_id.clone(),
            part_number,
        };
        let payload = encode_stream_part_finalize_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_part_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.validate_stream_part_finalize_snapshot_response(
            &response.snapshot,
            bucket,
            key,
            upload_id,
            session_id,
            part_number,
        )?;
        Ok(response.snapshot)
    }

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcStreamPartCommitCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            upload_id: request.upload_id.clone(),
            session_id: request.session_id.clone(),
            part_number: request.part_number,
            expected_snapshot: request.expected_snapshot.clone(),
            part: request.part.clone(),
            segments: request.segments.to_vec(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_stream_part_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream part commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part commit command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_stream_part_commit_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part commit command build response",
                    "stream part commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode stream part commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn load_multipart_completion_stale_payload_source(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let request = self.object_request(pg_id, bucket, key);
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_stale_source_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion stale source response",
                    error.to_string(),
                ))
            })?;
        if let Some(source) = response.source.as_ref() {
            match source {
                StoredObject::Live(live)
                    if live.bucket == *bucket && live.key == *key && live.version_id.is_null() => {}
                _ => {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion stale source response",
                        "stale source must be null live object for requested object".to_string(),
                    )));
                }
            }
        }
        Ok(response.source)
    }

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcCompleteMultipartCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            version_id: request.version_id,
            completion_order: request.completion_order,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_complete_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode complete multipart command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild,
            request.pg_id,
            payload,
            "decode complete multipart command build response",
            ObjectPgActionError::StaleMultipartCompletionSnapshot,
            |command| self.validate_complete_multipart_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode complete multipart command build response",
                "complete multipart command build cannot return missing".to_string(),
            ))),
        }
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let bucket = request.bucket;
        let key = request.key;
        let upload_id = request.upload_id;
        let rpc_request = StorageRpcAbortMultipartCommandBuildRequest {
            object: self.object_request(request.pg_id, bucket, key),
            upload_id: upload_id.clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_abort_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAbortCommandBuild,
            request.pg_id,
            payload,
            "decode abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: request.pg_id,
                        cluster_epoch: request.cluster_epoch,
                        bucket,
                        key,
                        upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: &request.bucket_write_reservation,
                    },
                )
            },
        )
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let bucket = &request.authorized_upload.record().bucket;
        let key = &request.authorized_upload.record().key;
        let upload_id = &request.authorized_upload.record().upload_id;
        let rpc_request = StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
            object: self.object_request(request.pg_id, bucket, key),
            authorized_upload: request.authorized_upload.record().clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_authorized_abort_multipart_command_build_request(&rpc_request)
            .map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode authorized abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild,
            request.pg_id,
            payload,
            "decode authorized abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: request.pg_id,
                        cluster_epoch: request.cluster_epoch,
                        bucket,
                        key,
                        upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: &request.bucket_write_reservation,
                    },
                )
            },
        )
    }

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        let request = StorageRpcAbortMultipartCleanupRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_abort_multipart_cleanup_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_abort_multipart_cleanup_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode abort multipart cleanup response",
                    error.to_string(),
                ))
            })?;
        if let Some(cleanup) = response.cleanup.as_ref() {
            self.validate_abort_cleanup_snapshot_response(cleanup, bucket, key, upload_id)?;
        }
        Ok(response.cleanup)
    }

    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let request = StorageRpcPutObjectMetadataSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id,
        };
        let payload = encode_put_object_metadata_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_put_object_metadata_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT snapshot response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(stored) => {
                self.validate_stored_object_response(
                    &stored,
                    bucket,
                    key,
                    version_id,
                    "validate object metadata PUT snapshot response",
                )?;
                Ok(*stored)
            }
            StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound => {
                Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound))
            }
        }
    }

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcPutObjectMetadataCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            requested_version_id: request.requested_version_id,
            expected_stored: request.expected_stored.clone(),
            version_id: request.version_id,
            mutation: request.mutation.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_put_object_metadata_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode object metadata PUT command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_put_object_metadata_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT command build response",
                    "PUT metadata command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode object metadata PUT command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.load_object_delete_snapshot(
            pg_id,
            bucket,
            key,
            None,
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
        )
    }

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.load_object_delete_snapshot(
            pg_id,
            bucket,
            key,
            Some(version_id),
            StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
        )
    }

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        self.load_object_lifecycle_version_list(pg_id, bucket, key)
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let rpc_request = StorageRpcDeleteSpecificObjectCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            version_id: request.version_id,
            expected_stored: request.expected_stored.cloned(),
            expected_target: request.expected_target.cloned(),
            expected_version_list: request.expected_version_list.map(<[StoredObject]>::to_vec),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_specific_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode delete-specific object command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
            request.pg_id,
            payload,
            "decode delete-specific object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_delete_specific_object_command_response(command, &request),
        )
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let rpc_request = StorageRpcDeleteCurrentObjectCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            expected_current: request.expected_current.cloned(),
            expected_target: request.expected_target.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_current_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode delete-current object command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
            request.pg_id,
            payload,
            "decode delete-current object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_delete_current_object_command_response(command, &request),
        )
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let stale_payload = match &request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(reclaim) => {
                StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim.clone())
            }
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
                StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: *created_at,
                }
            }
        };
        let rpc_request = StorageRpcInsertDeleteMarkerCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            expected_current: request.expected_current.cloned(),
            expected_stale_payload_source: request.expected_stale_payload_source.cloned(),
            version_id: request.version_id,
            owner: request.owner.clone(),
            stale_payload,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_insert_delete_marker_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode insert-delete-marker command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
            request.pg_id,
            payload,
            "decode insert-delete-marker command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_insert_delete_marker_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode insert-delete-marker command build response",
                "insert-delete-marker command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl ObjectReadMetadataNodeClient for UnixStorageNodeClient {
    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let request = StorageRpcObjectReadAuthSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
        };
        let payload = encode_object_read_auth_subject_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectReadAuthSubjectLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_read_auth_subject_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode object read auth subject response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectReadAuthSubjectOutcome::Loaded(subject) => {
                self.validate_object_read_subject_response(&subject, bucket, key, version_id)?;
                Ok(*subject)
            }
            StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound => {
                Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound))
            }
        }
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
        let request = StorageRpcObjectReadSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
            expected_identity: expected_identity.clone(),
            snapshot_mode,
        };
        let payload = encode_object_read_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectReadSnapshotLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_read_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode object read snapshot response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcObjectReadSnapshotOutcome::Loaded(snapshot) => {
                self.validate_object_read_snapshot_response(
                    &snapshot,
                    bucket,
                    key,
                    version_id,
                    expected_identity,
                    snapshot_mode,
                )?;
                Ok(*snapshot)
            }
            StorageRpcObjectReadSnapshotOutcome::StaleSubject => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
        }
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
        let request = StorageRpcObjectTagsForSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
            expected_identity: expected_identity.clone(),
            authorized_version_id,
        };
        let payload = encode_object_tags_for_subject_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectTagsForSubjectLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_tags_for_subject_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object tags for subject response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectTagsForSubjectOutcome::Loaded(tags) => Ok(tags),
            StorageRpcObjectTagsForSubjectOutcome::StaleSubject => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
        }
    }
}

impl UnixStorageNodeClient {
    fn bucket_pg_request(&self, pg_id: PgId) -> StorageRpcBucketPgRequest {
        StorageRpcBucketPgRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        }
    }

    fn object_request(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> StorageRpcObjectRequest {
        StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }
    }

    fn validate_stream_upload_session_response(
        &self,
        session: &StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if session.session_id != *session_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream session id does not match request".to_string(),
            )));
        }
        validate_stream_upload_session_binding(session, bucket, key).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(context, error.to_string()))
        })
    }

    fn validate_stream_upload_segments_response(
        &self,
        segments: &[StreamUploadSegmentRecord],
        session_id: &SessionId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let mut seen = BTreeSet::new();
        for segment in segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "stream segment session id does not match request".to_string(),
                )));
            }
            if !seen.insert(segment.segment_index) {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "duplicate stream segment index in response".to_string(),
                )));
            }
        }
        if !segments
            .windows(2)
            .all(|pair| pair[0].segment_index < pair[1].segment_index)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segments are not strictly ascending".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_stream_segment_append_prepare_response(
        &self,
        segment: &StreamUploadSegmentRecord,
        returned_target: &StreamUploadTarget,
        expected_target: &StreamUploadTarget,
        request: &PrepareStreamUploadSegmentAppendReq,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if returned_target != expected_target {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segment append target does not match session".to_string(),
            )));
        }
        if segment.session_id != request.session_id
            || segment.segment_index != request.segment_index
            || segment.size != request.size
            || segment.segment_crc64 != request.segment_crc64
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segment append response does not match request".to_string(),
            )));
        }
        if matches!(expected_target, StreamUploadTarget::UploadPart { .. })
            && segment.segment_okh != request.segment_okh
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream upload-part segment OKH does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn load_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        kind: StorageRpcMessageKind,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcObjectDeleteSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id,
        };
        let payload = encode_object_delete_snapshot_request(&request);
        let response = self
            .rpc_request(kind, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_delete_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode object delete snapshot response", error.to_string()),
            )
        })?;
        if let Some(stored) = response.stored.as_ref() {
            self.validate_stored_object_response(
                stored,
                bucket,
                key,
                version_id,
                "validate object delete snapshot response",
            )?;
        }
        if let Some(target) = response.target.as_ref() {
            self.validate_delete_target_response(
                target,
                bucket,
                key,
                "validate object delete snapshot response",
            )?;
        }
        if !self.delete_snapshot_target_matches_stored(&response) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object delete snapshot response",
                "delete target does not match snapshot object".to_string(),
            )));
        }
        Ok(ObjectDeleteStorageSnapshot {
            stored: response.stored,
            target: response.target,
        })
    }

    fn load_object_lifecycle_version_list(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let request = StorageRpcObjectDeleteSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id: None,
        };
        let payload = encode_object_delete_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_lifecycle_version_list_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object lifecycle version list response",
                    error.to_string(),
                ))
            })?;
        for stored in &response.versions {
            self.validate_stored_object_response(
                stored,
                bucket,
                key,
                None,
                "validate object lifecycle version list response",
            )?;
        }
        Ok(response.versions)
    }

    fn validate_delete_target_response(
        &self,
        target: &DeleteObjectVersionTarget,
        bucket: &BucketName,
        key: &ObjectKey,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => Ok(()),
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                payload,
            } => {
                let payload_matches = match (payload, layout) {
                    (ObjectPayloadReclaimCommand::Segments(reclaim), ObjectLayout::Standard) => {
                        reclaim.generation_id == *generation_id
                    }
                    (
                        ObjectPayloadReclaimCommand::Multipart(reclaim),
                        ObjectLayout::MultipartManifest { .. },
                    ) => reclaim.generation_id == *generation_id,
                    _ => false,
                };
                if !reclaim_matches_bucket_key(Some(payload), bucket, key) || !payload_matches {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "delete target payload does not match request".to_string(),
                    )));
                }
                Ok(())
            }
        }
    }

    fn delete_snapshot_target_matches_stored(
        &self,
        response: &StorageRpcObjectDeleteSnapshotResponse,
    ) -> bool {
        match (&response.stored, &response.target) {
            (None, None) => true,
            (
                Some(StoredObject::DeleteMarker(_)),
                Some(DeleteObjectVersionTarget::DeleteMarker { .. }),
            ) => true,
            (
                Some(StoredObject::Live(live)),
                Some(DeleteObjectVersionTarget::Live {
                    generation_id,
                    layout,
                    payload,
                }),
            ) => {
                *generation_id == live.generation_id
                    && *layout == live.layout
                    && reclaim_matches_bucket_key(Some(payload), &live.bucket, &live.key)
            }
            _ => false,
        }
    }

    fn object_metadata_command_build_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        payload: Vec<u8>,
        decode_context: &'static str,
        stale_snapshot_error: ObjectPgActionError,
        validate: impl FnOnce(&MetadataCommandEnvelope) -> Result<(), ObjectPgActionError>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let response = self
            .rpc_request(kind, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error(decode_context, error.to_string()),
                )
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                validate(&command)?;
                Ok(Some(*command))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => Err(stale_snapshot_error),
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => Ok(None),
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                pg_id,
                decode_context,
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn metadata_command_log_conflict_error(
        &self,
        pg_id: PgId,
        decode_context: &'static str,
        node_id: u32,
        conflict_pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> ObjectPgActionError {
        if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
            return ObjectPgActionError::Store(self.rpc_payload_error(
                decode_context,
                "metadata command log conflict route mismatch".to_string(),
            ));
        }
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return ObjectPgActionError::Store(self.rpc_payload_error(
                decode_context,
                "metadata command log conflict index must not be zero".to_string(),
            ));
        }
        ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: conflict_pg_id,
            cluster_epoch,
            log_index,
        })
    }

    fn validate_stream_upload_match_response(
        &self,
        exists: bool,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<(), ObjectPgActionError> {
        if exists && expected_command.is_none() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream upload match response",
                "positive stream upload match requires expected command".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_multipart_upload_match_response(
        &self,
        initiated_at: Option<u64>,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<(), ObjectPgActionError> {
        let Some(initiated_at) = initiated_at else {
            return Ok(());
        };
        let Some(expected_command) = expected_command else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate multipart upload match response",
                "positive multipart upload match requires expected command".to_string(),
            )));
        };
        if initiated_at != expected_command.upload.initiated_at {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate multipart upload match response",
                "multipart upload match timestamp does not match expected command".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_multipart_upload_response(
        &self,
        upload: &MultipartUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_state: Option<UploadState>,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if upload.bucket != *bucket
            || upload.key != *key
            || upload.upload_id != *upload_id
            || expected_state.is_some_and(|state| upload.state != state)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "multipart upload identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_multipart_completion_snapshot_response(
        &self,
        snapshot: &MultipartCompletionSnapshot,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart completion snapshot response";
        let upload = authorized_upload.record();
        let requested = requested_part_numbers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if snapshot.part_records.len() != requested_part_numbers.len() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot selected part count does not match request".to_string(),
            )));
        }
        for (part, requested_part_number) in snapshot
            .part_records
            .iter()
            .zip(requested_part_numbers.iter())
        {
            if part.upload_id != upload.upload_id || part.part_number != *requested_part_number {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot selected part identity does not match request".to_string(),
                )));
            }
        }
        for segment in &snapshot.selected_streaming_segments {
            if segment.bucket != upload.bucket
                || segment.key != upload.key
                || segment.upload_id != upload.upload_id
                || !requested.contains(&segment.part_number)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot selected segment identity does not match request".to_string(),
                )));
            }
        }
        for part in &snapshot.cleanup.omitted_parts {
            if part.upload_id != upload.upload_id || requested.contains(&part.part_number) {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot omitted part identity does not match request".to_string(),
                )));
            }
        }
        for segment in &snapshot.cleanup.omitted_streaming_segments {
            if segment.bucket != upload.bucket
                || segment.key != upload.key
                || segment.upload_id != upload.upload_id
                || requested.contains(&segment.part_number)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot omitted segment identity does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &snapshot.cleanup.stream_uploads,
            &snapshot.cleanup.stream_upload_segments,
            &upload.bucket,
            &upload.key,
            &upload.upload_id,
            context,
        )?;
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            let Some(live) = source.as_live() else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot stale payload source is not live".to_string(),
                )));
            };
            if live.bucket != upload.bucket || live.key != upload.key || !live.version_id.is_null()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot stale payload source identity does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    fn validate_listed_multipart_parts_response(
        &self,
        listed: &ListedMultipartParts,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart parts list response";
        if listed.upload != *authorized_upload.record()
            || listed.response.parts.len() > max_parts as usize
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "listed multipart parts response shape does not match request".to_string(),
            )));
        }
        let mut previous_part_number = part_number_marker;
        for part in &listed.response.parts {
            if part.upload_id != authorized_upload.upload_id
                || previous_part_number.is_some_and(|marker| part.part_number <= marker)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "listed multipart part identity does not match request".to_string(),
                )));
            }
            previous_part_number = Some(part.part_number);
        }
        match (
            listed.response.is_truncated,
            listed.response.next_part_number_marker,
            listed.response.parts.last(),
        ) {
            (false, None, _) => {}
            (false, Some(next_marker), None)
                if max_parts == 0 && next_marker == part_number_marker.unwrap_or(0) => {}
            (false, Some(_), _) => {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "non-truncated multipart parts response has next marker".to_string(),
                )));
            }
            (true, Some(next_marker), Some(last)) if next_marker == last.part_number => {}
            (true, _, _) => {
                return Err(ObjectPgActionError::Store(
                    self.rpc_payload_error(
                        context,
                        "truncated multipart parts response marker does not match last part"
                            .to_string(),
                    ),
                ));
            }
        }
        Ok(())
    }

    fn validate_multipart_management_lookup_response(
        &self,
        lookup: &MultipartUploadManagementLookup,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart management lookup response";
        match lookup {
            MultipartUploadManagementLookup::InProgress(upload) => self
                .validate_multipart_upload_response(
                    upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    context,
                ),
            MultipartUploadManagementLookup::NonInProgress(upload) => {
                if upload.bucket != *bucket
                    || upload.key != *key
                    || upload.upload_id != *upload_id
                    || upload.state == UploadState::InProgress
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "non-in-progress lookup identity does not match request".to_string(),
                    )));
                }
                Ok(())
            }
            MultipartUploadManagementLookup::Completed(completed) => {
                if completed.bucket != *bucket
                    || completed.key != *key
                    || completed.upload_id != *upload_id
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "completed lookup identity does not match request".to_string(),
                    )));
                }
                Ok(())
            }
            MultipartUploadManagementLookup::Missing => Ok(()),
        }
    }

    fn validate_stored_object_response(
        &self,
        stored: &StoredObject,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if stored.bucket() != bucket
            || stored.key() != key
            || version_id.is_some_and(|version_id| stored.version_id() != version_id)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stored object identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_object_metadata_command_route(
        &self,
        command: &MetadataCommandEnvelope,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if command.id().cluster_epoch() != cluster_epoch || command.id().pg_id() != pg_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "command id route does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_put_object_metadata_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate object metadata PUT command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not object metadata PUT".to_string(),
            )));
        };
        let live = request
            .expected_stored
            .as_live()
            .ok_or(MetadataError::MethodNotAllowedOnDeleteMarker)?;
        let expected = PutObjectMetadataCommand::from_live_object_and_mutation(
            live.clone(),
            request.mutation.clone(),
            request.bucket_write_reservation.clone(),
        );
        if update.as_ref() != &expected || request.version_id != live.version_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_delete_specific_object_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate delete-specific object command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not object version delete".to_string(),
            )));
        };
        if delete.bucket != *request.bucket
            || delete.key != *request.key
            || delete.version_id != request.version_id
            || delete.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        self.validate_delete_target_response(&delete.target, request.bucket, request.key, context)?;
        if !delete_target_matches_expected(Some(&delete.target), request.expected_target) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response delete target does not match request snapshot".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_delete_current_object_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let Some(StoredObject::Live(expected)) = request.expected_current else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate delete-current object command build response",
                "missing/delete-marker current must not return delete command".to_string(),
            )));
        };
        let specific = BuildDeleteSpecificObjectVersionCommandReq {
            pg_id: request.pg_id,
            cluster_epoch: request.cluster_epoch,
            bucket: request.bucket,
            key: request.key,
            version_id: expected.version_id,
            expected_stored: request.expected_current,
            expected_target: request.expected_target,
            expected_version_list: None,
            bucket_write_reservation: request.bucket_write_reservation,
        };
        self.validate_delete_specific_object_command_response(command, &specific)
    }

    fn validate_insert_delete_marker_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate insert-delete-marker command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not insert delete marker".to_string(),
            )));
        };
        if marker.bucket != *request.bucket
            || marker.key != *request.key
            || marker.version_id != request.version_id
            || marker.owner != *request.owner
            || marker.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        match &request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(expected) => {
                let mut actual = marker.stale_payload.clone();
                normalize_reclaim_created_at(&mut actual);
                if &actual != expected {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "response stale payload does not match request".to_string(),
                    )));
                }
            }
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { .. } => {
                if let Some(payload) = marker.stale_payload.as_ref() {
                    self.validate_reclaim_payload_response(
                        payload,
                        request.bucket,
                        request.key,
                        context,
                    )?;
                }
                if !reclaim_matches_snapshot_live_object(
                    marker.stale_payload.as_ref(),
                    &request.expected_stale_payload_source.cloned(),
                ) {
                    return Err(ObjectPgActionError::Store(
                        self.rpc_payload_error(
                            context,
                            "response stale payload does not match current null live snapshot"
                                .to_string(),
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_create_stream_upload_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream upload command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not create stream upload".to_string(),
            )));
        };
        if create.session.session_id != request.request.session_id
            || create.session.bucket != request.request.bucket
            || create.session.key != request.request.key
            || create.session.target != request.request.target
            || create.session.state != StreamUploadState::InProgress
            || create.session.encryption != request.request.encryption
            || create.initial_next_segment_vid != GenerationId::MIN
            || create.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_create_multipart_upload_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart upload command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CreateMultipartUpload(create) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not create multipart upload".to_string(),
            )));
        };
        let upload = &create.upload;
        if upload.upload_id != request.request.upload_id
            || upload.bucket != request.request.bucket
            || upload.key != request.request.key
            || upload.state != UploadState::InProgress
            || upload.tags != request.request.tags
            || upload.metadata_blob != request.request.metadata_blob
            || upload.system_metadata_blob != request.request.system_metadata_blob
            || upload.initiator != request.request.initiator
            || upload.owner != request.request.owner
            || upload.acl_grants != request.request.acl_grants
            || upload.public_read != request.request.public_read
            || upload.object_lock != request.request.object_lock
            || upload.checksum != request.request.checksum
            || upload.encryption != request.request.encryption
            || create.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_stream_put_finalize_snapshot_response(
        &self,
        snapshot: &StreamPutFinalizeStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream PUT finalize snapshot response";
        if snapshot.session.session_id != *session_id
            || snapshot.session.bucket != *bucket
            || snapshot.session.key != *key
            || snapshot.session.target != StreamUploadTarget::PutObject
            || snapshot.session.state != StreamUploadState::InProgress
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot session identity does not match request".to_string(),
            )));
        }
        for segment in &snapshot.staging_segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot segment identity does not match request".to_string(),
                )));
            }
        }
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            if source.bucket() != bucket || source.key() != key {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "stale payload source identity does not match request".to_string(),
                )));
            }
        }
        if !reclaim_matches_bucket_key(snapshot.stale_payload.as_ref(), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stale payload identity does not match request".to_string(),
            )));
        }
        if !reclaim_matches_snapshot_live_object(
            snapshot.stale_payload.as_ref(),
            &snapshot.stale_payload_source,
        ) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stale payload shape does not match source live object".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_stream_part_finalize_snapshot_response(
        &self,
        snapshot: &StreamUploadPartStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream part finalize snapshot response";
        let auth = &snapshot.auth_snapshot;
        if auth.session.session_id != *session_id
            || auth.session.bucket != *bucket
            || auth.session.key != *key
            || auth.session.target
                != (StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                })
            || auth.session.state != StreamUploadState::InProgress
            || auth.upload.upload_id != *upload_id
            || auth.upload.bucket != *bucket
            || auth.upload.key != *key
            || auth.upload.state != UploadState::InProgress
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot identity does not match request".to_string(),
            )));
        }
        for segment in &auth.staging_segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot segment identity does not match request".to_string(),
                )));
            }
        }
        if snapshot
            .existing_part
            .as_ref()
            .is_some_and(|part| part.upload_id != *upload_id || part.part_number != part_number)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "existing part identity does not match request".to_string(),
            )));
        }
        for segment in &snapshot.displaced_segments {
            if segment.bucket != *bucket
                || segment.key != *key
                || segment.upload_id != *upload_id
                || segment.part_number != part_number
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "displaced segment identity does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    fn validate_stream_put_commit_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream PUT commit command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not stream PUT commit".to_string(),
            )));
        };
        if !commit.matches_request(
            request.bucket,
            request.key,
            request.session_id,
            request.expected_snapshot.generation_id,
        ) || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        let object = &commit.object;
        let expected_etag = ObjectEtag::single_part(request.commit.etag_crc64);
        if object.generation_id != request.expected_snapshot.generation_id
            || object.version_id != request.commit.version_id
            || object.owner != request.commit.owner
            || object.acl_grants != request.commit.acl_grants
            || object.public_read != request.commit.public_read
            || object.size != request.commit.size
            || object.etag != expected_etag
            || object.layout != ObjectLayout::Standard
            || object.tags != request.commit.tags
            || object.metadata_blob.as_ref() != Some(&request.commit.metadata_blob)
            || object.system_metadata_blob.as_ref() != Some(&request.commit.system_metadata_blob)
            || object.object_lock != request.commit.object_lock
            || object.encryption != request.commit.encryption
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command object does not match request".to_string(),
            )));
        }
        let segments_total: u64 = request
            .expected_snapshot
            .staging_segments
            .iter()
            .map(|segment| segment.size)
            .sum();
        if segments_total != request.total_size
            || commit.segments.len() != request.expected_snapshot.staging_segments.len()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command segment shape does not match request".to_string(),
            )));
        }
        for (actual, expected) in commit
            .segments
            .iter()
            .zip(request.expected_snapshot.staging_segments.iter())
        {
            if actual.bucket != *request.bucket
                || actual.key != *request.key
                || actual.version_id != request.commit.version_id
                || actual.segment_index != expected.segment_index
                || actual.size != expected.size
                || actual.segment_crc64 != expected.segment_crc64
                || actual.segment_okh != expected.segment_okh
                || actual.segment_vid != expected.segment_vid
                || actual.data_pg_id != expected.data_pg_id
                || actual.ec_k != expected.ec_k
                || actual.ec_m != expected.ec_m
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response command segment does not match snapshot".to_string(),
                )));
            }
        }
        if request.commit.version_id.is_null() {
            let mut actual_stale_payload = commit.stale_payload.clone();
            normalize_reclaim_created_at(&mut actual_stale_payload);
            if actual_stale_payload != request.expected_snapshot.stale_payload {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response command stale payload does not match expected snapshot".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "versioned stream PUT response must not reclaim stale null payload".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_stream_part_commit_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream part commit command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not stream part commit".to_string(),
            )));
        };
        if !commit.matches_request(
            request.bucket,
            request.key,
            request.upload_id,
            request.session_id,
            request.part_number,
        ) || commit.upload != request.expected_snapshot.auth_snapshot.upload
            || commit.part != *request.part
            || commit.segments != request.segments
            || commit.existing_part != request.expected_snapshot.existing_part
            || commit.displaced_segments != request.expected_snapshot.displaced_segments
            || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_complete_multipart_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate complete multipart command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not complete multipart".to_string(),
            )));
        };
        let parts_count = std::num::NonZeroU32::new(
            u32::try_from(request.request.part_records.len()).map_err(|_| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error(context, "request part count exceeds u32".to_string()),
                )
            })?,
        )
        .ok_or_else(|| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "complete multipart response requires at least one part".to_string(),
            ))
        })?;
        let expected_etag = ObjectEtag::MultipartComposite {
            crc64: request.request.etag_crc64,
            parts: parts_count,
        };
        if !commit.matches_request(
            &request.request.bucket,
            &request.request.key,
            &request.request.upload_id,
            request.request.generation_id,
            &request.request.part_records,
        ) || commit.object.version_id != request.version_id
            || commit.object.owner != request.request.owner
            || commit.object.acl_grants != request.request.acl_grants
            || commit.object.public_read != request.request.public_read
            || commit.object.size != request.request.size
            || commit.object.etag != expected_etag
            || commit.object.layout != (ObjectLayout::MultipartManifest { parts_count })
            || commit.object.tags != request.request.tags
            || commit.object.metadata_blob != request.request.metadata_blob
            || commit.object.system_metadata_blob != request.request.system_metadata_blob
            || commit.object.object_lock != request.request.object_lock
            || commit.object.encryption != request.request.encryption
            || commit.completion_order != request.completion_order
            || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        for part in &commit.parts {
            if part.bucket != request.request.bucket
                || part.key != request.request.key
                || part.version_id != request.version_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response object part identity does not match request".to_string(),
                )));
            }
        }
        if commit.parts != request.expected_object_parts {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response object part placement does not match expected topology".to_string(),
            )));
        }
        for segment in commit
            .selected_streaming_segments
            .iter()
            .chain(commit.omitted_streaming_segments.iter())
        {
            if segment.bucket != request.request.bucket
                || segment.key != request.request.key
                || segment.upload_id != request.request.upload_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response multipart segment identity does not match request".to_string(),
                )));
            }
        }
        let mut selected_part_numbers = BTreeMap::new();
        for part in &request.request.part_records {
            if selected_part_numbers
                .insert(part.part_number, part)
                .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "request contains duplicate multipart part numbers".to_string(),
                )));
            }
        }
        let mut omitted_part_numbers = BTreeMap::new();
        for part in &commit.omitted_parts {
            if part.upload_id != request.request.upload_id
                || selected_part_numbers.contains_key(&part.part_number)
                || omitted_part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response omitted part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut expected_selected_streaming_segments =
            request.request.selected_streaming_segments.clone();
        for segment in &mut expected_selected_streaming_segments {
            segment.version_id = request.version_id.to_u64();
        }
        if commit.selected_streaming_segments != expected_selected_streaming_segments {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response selected streaming segment cleanup does not match request".to_string(),
            )));
        }
        let mut omitted_segment_ids = BTreeMap::new();
        for segment in &commit.omitted_streaming_segments {
            if selected_part_numbers.contains_key(&segment.part_number)
                || omitted_segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response omitted streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        if commit.omitted_parts != request.request.expected_cleanup.omitted_parts
            || commit.omitted_streaming_segments
                != request.request.expected_cleanup.omitted_streaming_segments
            || !terminal_stream_cleanup_rows_match(
                &commit.stream_uploads,
                &request.request.expected_cleanup.stream_uploads,
                &commit.stream_upload_segments,
                &request.request.expected_cleanup.stream_upload_segments,
            )
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response cleanup does not match expected snapshot".to_string(),
            )));
        }
        self.validate_terminal_stream_cleanup_response(
            &commit.stream_uploads,
            &commit.stream_upload_segments,
            &request.request.bucket,
            &request.request.key,
            &request.request.upload_id,
            context,
        )?;
        if request.version_id.is_null() {
            if let Some(payload) = commit.stale_payload.as_ref() {
                self.validate_reclaim_payload_response(
                    payload,
                    &request.request.bucket,
                    &request.request.key,
                    context,
                )?;
                if !reclaim_matches_snapshot_live_object(
                    Some(payload),
                    &request.request.expected_stale_payload_source,
                ) {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "response stale payload does not match expected source".to_string(),
                    )));
                }
            } else if request.request.expected_stale_payload_source.is_some() {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response missing expected stale payload".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(
                self.rpc_payload_error(
                    context,
                    "versioned complete multipart response must not reclaim stale null payload"
                        .to_string(),
                ),
            ));
        }
        Ok(())
    }

    fn validate_abort_multipart_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &AbortMultipartCommandValidation<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate abort multipart command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::AbortMultipartUpload(abort) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not abort multipart".to_string(),
            )));
        };
        if abort.bucket != *request.bucket
            || abort.key != *request.key
            || abort.upload_id != *request.upload_id
            || abort.bucket_write_reservation != *request.bucket_write_reservation
            || abort.cleanup.upload.bucket != *request.bucket
            || abort.cleanup.upload.key != *request.key
            || abort.cleanup.upload.upload_id != *request.upload_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        if request.expected_cleanup != Some(&abort.cleanup) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response cleanup does not match expected snapshot".to_string(),
            )));
        }
        let mut part_numbers = BTreeMap::new();
        for part in &abort.cleanup.parts {
            if part.upload_id != *request.upload_id
                || part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in &abort.cleanup.streaming_segments {
            if segment.bucket != *request.bucket
                || segment.key != *request.key
                || segment.upload_id != *request.upload_id
                || segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &abort.cleanup.stream_uploads,
            &abort.cleanup.stream_upload_segments,
            request.bucket,
            request.key,
            request.upload_id,
            context,
        )?;
        Ok(())
    }

    fn validate_abort_cleanup_snapshot_response(
        &self,
        cleanup: &AbortMultipartUploadCleanup,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate abort multipart cleanup response";
        if cleanup.upload.bucket != *bucket
            || cleanup.upload.key != *key
            || cleanup.upload.upload_id != *upload_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "cleanup upload does not match request".to_string(),
            )));
        }
        let mut part_numbers = BTreeMap::new();
        for part in &cleanup.parts {
            if part.upload_id != *upload_id
                || part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in &cleanup.streaming_segments {
            if segment.bucket != *bucket
                || segment.key != *key
                || segment.upload_id != *upload_id
                || segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &cleanup.stream_uploads,
            &cleanup.stream_upload_segments,
            bucket,
            key,
            upload_id,
            context,
        )
    }

    fn validate_terminal_stream_cleanup_response(
        &self,
        stream_uploads: &[crate::types::TerminalStreamCleanupRecord],
        stream_upload_segments: &[StreamUploadSegmentRecord],
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let mut sessions = BTreeMap::new();
        for stream in stream_uploads {
            let StreamUploadTarget::UploadPart {
                upload_id: stream_upload_id,
                ..
            } = &stream.target
            else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream cleanup target is not upload-part".to_string(),
                )));
            };
            if stream.bucket != *bucket
                || stream.key != *key
                || stream_upload_id != upload_id
                || sessions.insert(stream.session_id.clone(), stream).is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in stream_upload_segments {
            if !sessions.contains_key(&segment.session_id)
                || segment_ids
                    .insert((segment.session_id.clone(), segment.segment_index), ())
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream segment cleanup does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    fn validate_reclaim_payload_response(
        &self,
        payload: &ObjectPayloadReclaimCommand,
        bucket: &BucketName,
        key: &ObjectKey,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if !reclaim_matches_bucket_key(Some(payload), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response reclaim payload does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_object_read_subject_response(
        &self,
        subject: &ObjectReadAuthSubject,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<(), ObjectPgActionError> {
        if subject.stored.bucket() != bucket
            || subject.stored.key() != key
            || version_id.is_some_and(|version_id| subject.stored.version_id() != version_id)
            || !subject.identity.matches_stored(&subject.stored)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read auth subject response",
                "subject identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_object_read_snapshot_response(
        &self,
        snapshot: &ObjectReadSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<(), ObjectPgActionError> {
        if snapshot.stored.bucket() != bucket
            || snapshot.stored.key() != key
            || version_id.is_some_and(|version_id| snapshot.stored.version_id() != version_id)
            || !expected_identity.matches_stored(&snapshot.stored)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read snapshot response",
                "snapshot identity does not match request".to_string(),
            )));
        }

        let stored_version_id = snapshot.stored.version_id();
        for segment in &snapshot.object_segments {
            if &segment.bucket != bucket
                || &segment.key != key
                || segment.version_id != stored_version_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "object segment identity does not match snapshot".to_string(),
                )));
            }
        }
        for part in &snapshot.multipart_parts {
            if &part.bucket != bucket || &part.key != key || part.version_id != stored_version_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart part identity does not match snapshot".to_string(),
                )));
            }
        }
        for segment in &snapshot.multipart_part_segments {
            if &segment.bucket != bucket
                || &segment.key != key
                || segment.version_id != stored_version_id.to_u64()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment identity does not match snapshot".to_string(),
                )));
            }
        }

        match &snapshot.stored {
            StoredObject::DeleteMarker(_) => {
                if !snapshot.object_segments.is_empty()
                    || !snapshot.multipart_parts.is_empty()
                    || !snapshot.multipart_part_segments.is_empty()
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate object read snapshot response",
                        "delete-marker snapshot must not include payload layout".to_string(),
                    )));
                }
            }
            StoredObject::Live(record) => match (record.layout, snapshot_mode) {
                (_, ObjectReadSnapshotMode::MetadataOnly)
                | (ObjectLayout::Standard, ObjectReadSnapshotMode::MultipartParts)
                | (
                    ObjectLayout::MultipartManifest { .. },
                    ObjectReadSnapshotMode::StandardSegments,
                ) => {
                    if !snapshot.object_segments.is_empty()
                        || !snapshot.multipart_parts.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "snapshot mode must not include payload layout".to_string(),
                        )));
                    }
                }
                (ObjectLayout::Standard, ObjectReadSnapshotMode::StandardSegments)
                | (ObjectLayout::Standard, ObjectReadSnapshotMode::FullPayloadLayout) => {
                    if !snapshot.multipart_parts.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(
                            self.rpc_payload_error(
                                "validate object read snapshot response",
                                "standard object snapshot must not include multipart layout"
                                    .to_string(),
                            ),
                        ));
                    }
                }
                (
                    ObjectLayout::MultipartManifest { parts_count },
                    ObjectReadSnapshotMode::MultipartParts,
                ) => {
                    if !snapshot.object_segments.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "multipart-parts snapshot must not include segment layout".to_string(),
                        )));
                    }
                    self.validate_object_read_multipart_manifest_snapshot(
                        snapshot,
                        parts_count,
                        false,
                    )?;
                }
                (
                    ObjectLayout::MultipartManifest { parts_count },
                    ObjectReadSnapshotMode::FullPayloadLayout,
                ) => {
                    if !snapshot.object_segments.is_empty() {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "multipart snapshot must not include standard segments".to_string(),
                        )));
                    }
                    self.validate_object_read_multipart_manifest_snapshot(
                        snapshot,
                        parts_count,
                        true,
                    )?;
                }
            },
        }
        Ok(())
    }

    fn validate_object_read_multipart_manifest_snapshot(
        &self,
        snapshot: &ObjectReadSnapshot,
        parts_count: std::num::NonZeroU32,
        require_segment_layout: bool,
    ) -> Result<(), ObjectPgActionError> {
        if snapshot.multipart_parts.len() != parts_count.get() as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read snapshot response",
                "multipart snapshot part count does not match manifest".to_string(),
            )));
        }

        let mut parts_by_number = BTreeMap::new();
        for part in &snapshot.multipart_parts {
            if parts_by_number.insert(part.part_number, part).is_some() {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart snapshot contains duplicate part numbers".to_string(),
                )));
            }
        }

        let mut segment_counts_by_part = BTreeMap::<u32, usize>::new();
        for segment in &snapshot.multipart_part_segments {
            let Some(part) = parts_by_number.get(&segment.part_number) else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment has no matching part row".to_string(),
                )));
            };
            if part.part_okh != [0u8; 16] {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment belongs to shard-set part row".to_string(),
                )));
            }
            *segment_counts_by_part
                .entry(segment.part_number)
                .or_default() += 1;
        }

        if require_segment_layout {
            for part in &snapshot.multipart_parts {
                if part.part_okh == [0u8; 16]
                    && part.size > 0
                    && !segment_counts_by_part.contains_key(&part.part_number)
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate object read snapshot response",
                        "segmented multipart part has no segment rows".to_string(),
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_direct_put_command_build_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if command.id().cluster_epoch() != request.cluster_epoch
            || command.id().pg_id() != request.pg_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "command id route does not match request".to_string(),
            )));
        }
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command payload is not direct PUT commit".to_string(),
            )));
        };
        if !commit.matches_request(
            &request.request.bucket,
            &request.request.key,
            &request.request.generation_reservation_id,
            request.request.generation_id,
        ) || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command identity does not match request".to_string(),
            )));
        }
        let object = &commit.object;
        let expected_etag = ObjectEtag::single_part(request.request.etag_crc64);
        if object.version_id != request.version_id
            || object.owner != request.request.owner
            || object.acl_grants != request.request.acl_grants
            || object.public_read != request.request.public_read
            || object.size != request.request.size
            || object.etag != expected_etag
            || object.ec != request.request.ec
            || object.layout != ObjectLayout::Standard
            || object.tags != request.request.tags
            || object.metadata_blob.as_ref() != Some(&request.request.metadata_blob)
            || object.system_metadata_blob.as_ref() != Some(&request.request.system_metadata_blob)
            || object.object_lock != request.request.object_lock
            || object.encryption != request.request.encryption
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command object does not match request".to_string(),
            )));
        }
        let [segment] = commit.segments.as_slice() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command must contain one segment".to_string(),
            )));
        };
        if segment.bucket != request.request.bucket
            || segment.key != request.request.key
            || segment.version_id != request.version_id
            || segment.segment_index != request.request.segment_index
            || segment.size != request.request.size
            || segment.segment_crc64 != request.request.segment_crc64
            || segment.segment_okh != request.request.segment_okh
            || segment.segment_vid != request.request.segment_vid
            || segment.data_pg_id != request.request.data_pg_id
            || segment.ec_k != request.request.ec.k
            || segment.ec_m != request.request.ec.m
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command segment does not match request".to_string(),
            )));
        }
        if request.version_id.is_null() {
            let mut actual_stale_payload = commit.stale_payload.clone();
            normalize_reclaim_created_at(&mut actual_stale_payload);
            if actual_stale_payload != request.expected_snapshot.stale_payload {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit command build response",
                    "response command stale payload does not match expected snapshot".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "versioned direct PUT response must not reclaim stale null payload".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_direct_put_commit_snapshot_response(
        &self,
        snapshot: &DirectPutCommitStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), ObjectPgActionError> {
        let expected_etag = match snapshot.current.as_ref() {
            Some(current) => {
                if current.bucket() != bucket || current.key() != key {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate direct PUT commit snapshot response",
                        "current object identity does not match request".to_string(),
                    )));
                }
                current.as_live().map(|record| record.etag.format())
            }
            None => None,
        };
        if snapshot.auth_snapshot.existing_etag != expected_etag {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "auth snapshot etag does not match current object".to_string(),
            )));
        }
        if !reclaim_matches_bucket_key(snapshot.stale_payload.as_ref(), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "stale payload identity does not match request".to_string(),
            )));
        }
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            if source.bucket() != bucket || source.key() != key {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit snapshot response",
                    "stale payload source identity does not match request".to_string(),
                )));
            }
        }
        if !reclaim_matches_snapshot_live_object(
            snapshot.stale_payload.as_ref(),
            &snapshot.stale_payload_source,
        ) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "stale payload shape does not match source live object".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_empty_bucket_write_reservation_response(
        &self,
        context: &'static str,
        response: &[u8],
    ) -> Result<(), BucketSnapshotLoadError> {
        if response.is_empty() {
            Ok(())
        } else {
            Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                context,
                "bucket write reservation response payload must be empty".to_string(),
            )))
        }
    }

    fn validate_bucket_delete_finalized_response(
        &self,
        response: StorageRpcBucketDeleteFinalizedResponse,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        match response.outcome {
            StorageRpcBucketDeleteFinalizedOutcome::Deleted => Ok(()),
            StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { name } if name == *bucket => {
                Err(BucketWriteDrainError::Metadata(
                    MetadataError::BucketNotFound { name },
                ))
            }
            StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { .. } => {
                Err(BucketWriteDrainError::Store(self.rpc_payload_error(
                    "validate bucket delete finalized response",
                    "bucket not found response name does not match request".to_string(),
                )))
            }
        }
    }

    fn validate_proof_release_response(
        &self,
        response: &[u8],
    ) -> Result<(), BucketSnapshotLoadError> {
        if response.is_empty() {
            Ok(())
        } else {
            Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode proof release response",
                "proof release response payload must be empty".to_string(),
            )))
        }
    }

    fn validate_create_bucket_command_build_outcome(
        &self,
        outcome: StorageRpcCreateBucketCommandBuildOutcome,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        match outcome {
            StorageRpcCreateBucketCommandBuildOutcome::Exists(info) => {
                if info.name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate create-bucket command build response",
                        "exists response bucket name does not match request".to_string(),
                    )));
                }
                Ok(CreateBucketCommandBuild::Exists(info))
            }
            StorageRpcCreateBucketCommandBuildOutcome::Command(command) => {
                if command.id() != command_id {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate create-bucket command build response",
                        "response command id does not match request".to_string(),
                    )));
                }
                match command.payload() {
                    MetadataCommandPayload::CreateBucket(create)
                        if create.matches_create_config(config) => {}
                    _ => {
                        return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                            "validate create-bucket command build response",
                            "response command payload does not match request".to_string(),
                        )));
                    }
                }
                Ok(CreateBucketCommandBuild::Command(command))
            }
        }
    }

    fn validate_completed_multipart_order_command_build_response(
        &self,
        completion_order: u64,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart order command build response",
                "response command id does not match request".to_string(),
            )));
        }
        if completion_order == 0 {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart order command build response",
                "response completion order must not be zero".to_string(),
            )));
        }
        match command.payload() {
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                if advance.bucket == *bucket && advance.completion_order == completion_order => {}
            _ => {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate completed multipart order command build response",
                    "response command payload does not match request".to_string(),
                )))
            }
        }
        Ok((completion_order, command))
    }

    fn validate_bucket_metadata_control_command_build_response(
        &self,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &StorageRpcBucketMetadataControlMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket metadata control command build response",
                "response command id does not match request".to_string(),
            )));
        }
        let matches = match (command.payload(), mutation) {
            (
                MetadataCommandPayload::PutBucketVersioning(versioning),
                StorageRpcBucketMetadataControlMutation::Versioning(state),
            ) => versioning.bucket.name == *bucket && versioning.bucket.versioning == *state,
            (
                MetadataCommandPayload::PutBucketAcl(acl),
                StorageRpcBucketMetadataControlMutation::Acl {
                    acl_grants,
                    public_read,
                    public_write,
                },
            ) => {
                acl.bucket.name == *bucket
                    && acl.bucket.acl_grants == *acl_grants
                    && acl.bucket.public_read == *public_read
                    && acl.bucket.public_write == *public_write
            }
            (
                MetadataCommandPayload::PutBucketProperty(property),
                StorageRpcBucketMetadataControlMutation::Property(mutation),
            ) => bucket_property_command_matches_mutation(property, bucket, mutation),
            (
                MetadataCommandPayload::PutBucketSubresource(subresource),
                StorageRpcBucketMetadataControlMutation::Subresource(mutation),
            ) => subresource.matches_mutation(bucket, mutation),
            _ => false,
        };
        if !matches {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket metadata control command build response",
                "response command payload does not match request".to_string(),
            )));
        }
        Ok(command)
    }

    fn validate_mark_bucket_deleting_command_build_response(
        &self,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket mark-deleting command build response",
                "response command id does not match request".to_string(),
            )));
        }
        let valid = matches!(
            command.payload(),
            MetadataCommandPayload::MarkBucketDeleting(mark)
                if mark.bucket.name == *bucket && mark.bucket.state == BucketState::Deleting
        );
        if !valid {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket mark-deleting command build response",
                "response command payload does not match request".to_string(),
            )));
        }
        Ok(command)
    }

    fn validate_mark_bucket_deleting_already_deleting_response(
        &self,
        info: &BucketInfo,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        if info.name == *bucket && info.state == BucketState::Deleting {
            return Ok(());
        }
        Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
            "validate bucket mark-deleting command build response",
            "already-deleting response identity does not match request".to_string(),
        )))
    }

    fn validate_bucket_snapshot_pair_response(
        &self,
        pair: &BucketSnapshotPair,
        source: (&BucketName, BucketSnapshotRequest),
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<(), BucketSnapshotLoadError> {
        match pair {
            BucketSnapshotPair::Same { bucket } => {
                if source.0 != destination.0 {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "same-bucket response for distinct bucket request".to_string(),
                    )));
                }
                let expected_request = merge_bucket_snapshot_pair_request(source.1, destination.1);
                if bucket.bucket.name != *source.0 || bucket.request != expected_request {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "same-bucket snapshot identity does not match request".to_string(),
                    )));
                }
            }
            BucketSnapshotPair::Distinct {
                source: source_snapshot,
                destination: destination_snapshot,
            } => {
                if source.0 == destination.0 {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "distinct-bucket response for same bucket request".to_string(),
                    )));
                }
                if source_snapshot.bucket.name != *source.0
                    || source_snapshot.request != source.1
                    || destination_snapshot.bucket.name != *destination.0
                    || destination_snapshot.request != destination.1
                {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "distinct-bucket snapshot identity does not match request".to_string(),
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_bucket_payload_reclaim_root_response(
        &self,
        response: &StorageRpcPayloadReclaimRootResponse,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        if response
            .root
            .as_ref()
            .is_some_and(|root| &root.bucket != bucket)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate object bucket payload reclaim root response",
                "payload reclaim root bucket does not match request".to_string(),
            )));
        }
        Ok(())
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
            let merged_request = merge_bucket_snapshot_pair_request(source.1, destination.1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use std::thread;

    use crate::storage_node_server::{
        StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer,
    };
    use crate::storage_rpc::{
        encode_metadata_command_acceptance_response,
        encode_metadata_command_applied_hashes_response,
        encode_metadata_command_bool_outcome_response, encode_metadata_command_next_id_response,
        encode_metadata_command_pending_slot_insert_response,
        encode_metadata_command_state_outcome_response, encode_read_handle_acquire_response,
        encode_storage_rpc_success_response, read_storage_rpc_frame_from,
        write_storage_rpc_frame_to, StorageRpcMetadataCommandAcceptanceResponse,
        StorageRpcMetadataCommandAppliedHashesResponse,
        StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcMetadataCommandNextIdResponse,
        StorageRpcMetadataCommandPendingSlotInsertResponse,
        StorageRpcMetadataCommandStateOutcomeResponse, StorageRpcReadHandleAcquireResponse,
    };
    use crate::types::{
        DeleteMarkerRecord, EtagKind, ObjectEncryption, ObjectLockState, SerializedMetadataBlob,
        SerializedSystemMetadataBlob, SerializedTagSet, StorageClass, StreamUploadPartSnapshot,
    };
    use crate::CompletedMultipartUploadRecord;

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
                state: crate::types::PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            }],
        }
    }

    fn private_socket_dir(path: &std::path::Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn upload_part_stream_upload_match_accepts_existing_row_without_create_proof() {
        let bucket = crate::tests::bucket_name("upload-part-stream-match-bucket");
        let key = crate::tests::object_key("upload-part-stream-match-key");
        let upload_id = crate::tests::multipart_upload_id("upload-part-stream-match-upload");
        let session_id = SessionId::try_from("b6".repeat(16)).unwrap();
        let request = CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        };
        let proof = crate::metadata_command::BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "upload-part-stream-create-proof".to_string(),
            owner_token: "upload-part-stream-create-owner".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "upload-part-stream-create".to_string(),
            created_at: 10,
            lease_deadline: None,
            target_context: Some(key.as_str().to_string()),
        };
        let command = CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
            request, 10, proof,
        );
        let existing = StreamUploadRecord {
            session_id,
            bucket,
            key,
            target: StreamUploadTarget::UploadPart {
                upload_id,
                part_number: 1,
            },
            state: StreamUploadState::InProgress,
            created_at: command.session.created_at,
            encryption: ObjectEncryption::None,
            next_segment_vid: command.initial_next_segment_vid,
            bucket_write_reservation: None,
        };

        assert!(
            stream_upload_matches_command(&existing, &command),
            "UploadPart stream-create replay must match the applied row without a stored create proof"
        );
    }

    fn test_live_stored_object(
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        layout: ObjectLayout,
    ) -> StoredObject {
        StoredObject::Live(LiveObjectRecord {
            bucket,
            key,
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id,
            size: 0,
            etag: ObjectEtag::single_part(0),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        })
    }

    fn test_delete_marker_stored_object(bucket: BucketName, key: ObjectKey) -> StoredObject {
        StoredObject::DeleteMarker(DeleteMarkerRecord {
            bucket,
            key,
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            last_modified: 0,
        })
    }

    fn test_segments_reclaim(
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
    ) -> ObjectPayloadReclaimCommand {
        ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
            bucket,
            key,
            generation_id,
            created_at: 0,
            segments: Vec::new(),
        })
    }

    fn test_bucket_write_reservation_proof(
        bucket: BucketName,
        key: &ObjectKey,
    ) -> BucketWriteReservationProof {
        BucketWriteReservationProof {
            bucket,
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "object-mutation-test".to_string(),
            created_at: 1,
            lease_deadline: None,
            target_context: Some(key.as_str().to_string()),
        }
    }

    fn test_multipart_upload_record(
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
        state: UploadState,
    ) -> MultipartUploadRecord {
        MultipartUploadRecord {
            upload_id,
            bucket,
            key,
            initiated_at: 1,
            state,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_generation_id: GenerationId::new(30).unwrap(),
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        }
    }

    fn test_multipart_part_record(upload_id: UploadId, part_number: u32) -> MultipartPartRecord {
        MultipartPartRecord {
            upload_id,
            part_number,
            generation: 1,
            size: 12,
            etag: vec![part_number as u8; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [part_number as u8; 16],
            part_vid: GenerationId::new(40 + u64::from(part_number)).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 10 + u64::from(part_number),
            checksum: None,
        }
    }

    fn test_multipart_layout() -> ObjectLayout {
        ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(1).unwrap(),
        }
    }

    fn test_object_read_multipart_part(
        bucket: &BucketName,
        key: &ObjectKey,
        part_number: u32,
        part_okh: [u8; 16],
    ) -> ObjectPartRecord {
        ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            part_number,
            size: 12,
            etag: vec![part_number as u8; 16],
            etag_kind: EtagKind::Crc64,
            part_okh,
            part_vid: GenerationId::new(20 + u64::from(part_number)).unwrap(),
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 0,
            checksum: None,
        }
    }

    fn test_object_read_multipart_segment(
        bucket: &BucketName,
        key: &ObjectKey,
        part_number: u32,
    ) -> MultipartPartSegmentRecord {
        MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: UploadId::try_from("u".repeat(128)).unwrap(),
            version_id: VersionId::Null.to_u64(),
            part_number,
            segment_index: 0,
            size: 12,
            segment_crc64: Some(99),
            segment_okh: [7; 16],
            segment_vid: GenerationId::new(30 + u64::from(part_number)).unwrap(),
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        }
    }

    fn assert_object_read_snapshot_rejected(
        snapshot: &ObjectReadSnapshot,
        snapshot_mode: ObjectReadSnapshotMode,
    ) {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let identity = ObjectReadAuthSubjectIdentity::for_stored(&snapshot.stored);
        let err = client
            .validate_object_read_snapshot_response(
                snapshot,
                snapshot.stored.bucket(),
                snapshot.stored.key(),
                Some(snapshot.stored.version_id()),
                &identity,
                snapshot_mode,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate object read snapshot response",
                ..
            })
        ));
    }

    fn assert_direct_put_snapshot_rejected(
        snapshot: &DirectPutCommitStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
    ) {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let err = client
            .validate_direct_put_commit_snapshot_response(snapshot, bucket, key)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate direct PUT commit snapshot response",
                ..
            })
        ));
    }

    fn assert_direct_put_snapshot_accepted(
        snapshot: &DirectPutCommitStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
    ) {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        client
            .validate_direct_put_commit_snapshot_response(snapshot, bucket, key)
            .unwrap();
    }

    fn test_unix_storage_node_client() -> UnixStorageNodeClient {
        let tmp = test_util::tempdir();
        UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        )
    }

    fn test_unix_storage_node_client_with_rpc_admission_timeout(
        limit: usize,
        wait_timeout: Duration,
    ) -> UnixStorageNodeClient {
        let tmp = test_util::tempdir();
        UnixStorageNodeClient::with_rpc_admission(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
            Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
                limit,
                wait_timeout,
                wait_timeout,
            )),
        )
    }

    fn test_metadata_command(pg_id: u32, log_index: u64) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(
                crate::metadata_command::ReserveObjectGenerationCommand::new(
                    crate::tests::bucket_name("metadata-rpc-bucket"),
                    crate::tests::object_key("object"),
                    crate::tests::stream_session_id("metadata-rpc"),
                    GenerationId::new(1).unwrap(),
                    123,
                ),
            ),
        )
    }

    #[test]
    fn unix_object_list_response_requires_truncated_marker_identity() {
        let client = test_unix_storage_node_client();
        let bucket = crate::tests::bucket_name("list-marker-bucket");
        let key = crate::tests::object_key("list-marker-key");
        let object = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            ObjectLayout::Standard,
        );
        let req = ListObjectsReq {
            bucket,
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 1,
        };
        let mut response = ListObjectsResp {
            objects: vec![object],
            is_truncated: true,
            next_start_after: None,
        };
        assert!(validate_list_objects_response(&client, &response, &req).is_err());

        response.next_start_after = Some(crate::tests::object_key("wrong-marker"));
        assert!(validate_list_objects_response(&client, &response, &req).is_err());

        response.next_start_after = Some(key);
        validate_list_objects_response(&client, &response, &req).unwrap();
    }

    #[test]
    fn unix_object_version_list_response_requires_truncated_marker_identity() {
        let client = test_unix_storage_node_client();
        let bucket = crate::tests::bucket_name("version-list-marker-bucket");
        let key = crate::tests::object_key("version-list-marker-key");
        let version_id = VersionId::from_u64(44);
        let mut object = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            ObjectLayout::Standard,
        );
        let StoredObject::Live(record) = &mut object else {
            unreachable!("test helper always builds a live object");
        };
        record.version_id = version_id;
        let req = ListObjectVersionsReq {
            bucket,
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 1,
        };
        let mut response = ListObjectVersionsResp {
            versions: vec![object],
            is_truncated: true,
            next_key_marker: Some(key.clone()),
            next_version_id_marker: None,
        };
        assert!(validate_list_object_versions_response(&client, &response, &req).is_err());

        response.next_version_id_marker = Some(VersionId::from_u64(45));
        assert!(validate_list_object_versions_response(&client, &response, &req).is_err());

        response.next_version_id_marker = Some(version_id);
        validate_list_object_versions_response(&client, &response, &req).unwrap();
    }

    #[test]
    fn unix_multipart_upload_list_response_requires_truncated_marker_identity() {
        let client = test_unix_storage_node_client();
        let bucket = crate::tests::bucket_name("mpu-list-marker-bucket");
        let key = crate::tests::object_key("mpu-list-marker-key");
        let upload_id = UploadId::try_from("u".repeat(crate::UPLOAD_ID_LEN)).unwrap();
        let upload = test_multipart_upload_record(
            bucket.clone(),
            key.clone(),
            upload_id.clone(),
            UploadState::InProgress,
        );
        let req = ListMultipartUploadsReq {
            bucket,
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1,
        };
        let mut response = ListMultipartUploadsResp {
            uploads: vec![upload],
            is_truncated: true,
            next_key_marker: Some(key.clone()),
            next_upload_id_marker: None,
        };
        assert!(validate_list_multipart_uploads_response(&client, &response, &req).is_err());

        response.next_upload_id_marker =
            Some(UploadId::try_from("v".repeat(crate::UPLOAD_ID_LEN)).unwrap());
        assert!(validate_list_multipart_uploads_response(&client, &response, &req).is_err());

        response.next_upload_id_marker = Some(upload_id);
        validate_list_multipart_uploads_response(&client, &response, &req).unwrap();
    }

    fn test_bucket_info(
        name: BucketName,
        owner: &crate::CanonicalUserId,
        acl_grants: &crate::AclGrants,
    ) -> BucketInfo {
        BucketInfo {
            name,
            owner_principal: "owner".to_string(),
            owner_canonical_id: owner.clone(),
            created_at: 123,
            region: 0,
            state: BucketState::Active,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            acl_grants: acl_grants.clone(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            bucket_abac_enabled: false,
            encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
        }
    }

    #[test]
    fn unix_create_bucket_build_response_rejects_mismatched_identity() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("create-bucket-rpc-expected");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );

        let wrong_bucket = crate::tests::bucket_name("create-bucket-rpc-wrong");
        let err = client
            .validate_create_bucket_command_build_outcome(
                StorageRpcCreateBucketCommandBuildOutcome::Exists(test_bucket_info(
                    wrong_bucket,
                    &owner,
                    &acl_grants,
                )),
                &bucket,
                command_id,
                &config,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate create-bucket command build response",
                ..
            })
        ));

        let other_owner = crate::CanonicalUserId::from_principal("other-owner");
        let bad_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "other-owner",
            owner_canonical_id: &other_owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let bad_command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&bad_config, 123, 1).unwrap(),
            ),
        );
        let err = client
            .validate_create_bucket_command_build_outcome(
                StorageRpcCreateBucketCommandBuildOutcome::Command(Box::new(bad_command)),
                &bucket,
                command_id,
                &config,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate create-bucket command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_completed_multipart_order_build_response_rejects_mismatched_identity() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("completed-order-rpc-expected");
        let wrong_bucket = crate::tests::bucket_name("completed-order-rpc-wrong");
        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: wrong_bucket,
                    completion_order: 3,
                },
            ),
        );

        let err = client
            .validate_completed_multipart_order_command_build_response(
                3, command, &bucket, command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate completed multipart order command build response",
                ..
            })
        ));

        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: bucket.clone(),
                    completion_order: 0,
                },
            ),
        );
        let err = client
            .validate_completed_multipart_order_command_build_response(
                0, command, &bucket, command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate completed multipart order command build response",
                ..
            })
        ));

        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: bucket.clone(),
                    completion_order: 4,
                },
            ),
        );
        let err = client
            .validate_completed_multipart_order_command_build_response(
                3, command, &bucket, command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate completed multipart order command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_mark_bucket_deleting_build_response_rejects_mismatched_identity() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("mark-deleting-rpc-expected");
        let wrong_bucket = crate::tests::bucket_name("mark-deleting-rpc-wrong");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let wrong_bucket_record = BucketRecord::from_create_config(
            &crate::CreateBucketConfig {
                name: wrong_bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            1,
        )
        .unwrap();
        let wrong_bucket_command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                wrong_bucket_record,
            )),
        );

        let err = client
            .validate_mark_bucket_deleting_command_build_response(
                wrong_bucket_command,
                &bucket,
                command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate bucket mark-deleting command build response",
                ..
            })
        ));

        let active_bucket_record = BucketRecord::from_create_config(
            &crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            1,
        )
        .unwrap();
        let active_state_command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand {
                bucket: active_bucket_record,
            }),
        );
        let err = client
            .validate_mark_bucket_deleting_command_build_response(
                active_state_command,
                &bucket,
                command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate bucket mark-deleting command build response",
                ..
            })
        ));

        let create_bucket_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let wrong_payload_command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&create_bucket_config, 123, 1).unwrap(),
            ),
        );

        let err = client
            .validate_mark_bucket_deleting_command_build_response(
                wrong_payload_command,
                &bucket,
                command_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate bucket mark-deleting command build response",
                ..
            })
        ));

        let mut active_info = test_bucket_info(bucket.clone(), &owner, &acl_grants);
        active_info.state = BucketState::Active;
        let err = client
            .validate_mark_bucket_deleting_already_deleting_response(&active_info, &bucket)
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate bucket mark-deleting command build response",
                ..
            })
        ));

        let mut deleting_info = test_bucket_info(wrong_bucket, &owner, &acl_grants);
        deleting_info.state = BucketState::Deleting;
        let err = client
            .validate_mark_bucket_deleting_already_deleting_response(&deleting_info, &bucket)
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "validate bucket mark-deleting command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_read_snapshot_response_rejects_missing_multipart_parts() {
        let bucket = crate::tests::bucket_name("object-read-missing-part-rpc");
        let key = crate::tests::object_key("object-read-missing-part-rpc-key");
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            ObjectLayout::MultipartManifest {
                parts_count: std::num::NonZeroU32::new(2).unwrap(),
            },
        );
        let snapshot = ObjectReadSnapshot {
            stored,
            object_segments: Vec::new(),
            multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1, [5; 16])],
            multipart_part_segments: Vec::new(),
        };

        assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::MultipartParts);
    }

    #[test]
    fn unix_object_read_snapshot_response_rejects_orphan_multipart_segments() {
        let bucket = crate::tests::bucket_name("object-read-orphan-seg-rpc");
        let key = crate::tests::object_key("object-read-orphan-seg-rpc-key");
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            test_multipart_layout(),
        );
        let snapshot = ObjectReadSnapshot {
            stored,
            object_segments: Vec::new(),
            multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1, [0; 16])],
            multipart_part_segments: vec![test_object_read_multipart_segment(&bucket, &key, 2)],
        };

        assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::FullPayloadLayout);
    }

    #[test]
    fn unix_object_read_snapshot_response_rejects_segments_for_shard_set_parts() {
        let bucket = crate::tests::bucket_name("object-read-shard-set-seg-rpc");
        let key = crate::tests::object_key("object-read-shard-set-seg-rpc-key");
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            test_multipart_layout(),
        );
        let snapshot = ObjectReadSnapshot {
            stored,
            object_segments: Vec::new(),
            multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1, [5; 16])],
            multipart_part_segments: vec![test_object_read_multipart_segment(&bucket, &key, 1)],
        };

        assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::FullPayloadLayout);
    }

    #[test]
    fn unix_object_read_snapshot_response_accepts_zero_byte_multipart_part_without_segments() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("object-read-zero-part-rpc");
        let key = crate::tests::object_key("object-read-zero-part-rpc-key");
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            GenerationId::new(10).unwrap(),
            ObjectLayout::MultipartManifest {
                parts_count: std::num::NonZeroU32::new(2).unwrap(),
            },
        );
        let mut zero_part = test_object_read_multipart_part(&bucket, &key, 2, [0; 16]);
        zero_part.size = 0;
        let snapshot = ObjectReadSnapshot {
            stored,
            object_segments: Vec::new(),
            multipart_parts: vec![
                test_object_read_multipart_part(&bucket, &key, 1, [0; 16]),
                zero_part,
            ],
            multipart_part_segments: vec![test_object_read_multipart_segment(&bucket, &key, 1)],
        };
        let identity = ObjectReadAuthSubjectIdentity::for_stored(&snapshot.stored);

        client
            .validate_object_read_snapshot_response(
                &snapshot,
                &bucket,
                &key,
                Some(VersionId::Null),
                &identity,
                ObjectReadSnapshotMode::FullPayloadLayout,
            )
            .unwrap();
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_mismatched_auth_etag() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-validate");
        let key = crate::tests::object_key("direct-put-snapshot-validate-key");
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: Some("unexpected-etag".to_string()),
            },
            current: None,
            stale_payload_source: None,
            stale_payload: None,
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_mismatched_stale_payload_identity() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-stale");
        let key = crate::tests::object_key("direct-put-snapshot-stale-key");
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: None,
            stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
                ObjectSegmentsReclaimRecord {
                    bucket: crate::tests::bucket_name("wrong-direct-put-snapshot-stale"),
                    key: key.clone(),
                    generation_id: GenerationId::new(9).unwrap(),
                    created_at: 0,
                    segments: Vec::new(),
                },
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_stale_payload_without_source() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-stale-shape");
        let key = crate::tests::object_key("direct-put-snapshot-stale-shape-key");
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: None,
            stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
                ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id: GenerationId::new(9).unwrap(),
                    created_at: 0,
                    segments: Vec::new(),
                },
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_stale_source_without_payload() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-source-only");
        let key = crate::tests::object_key("direct-put-snapshot-source-only-key");
        let generation_id = GenerationId::new(9).unwrap();
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: Some(test_live_stored_object(
                bucket.clone(),
                key.clone(),
                generation_id,
                ObjectLayout::Standard,
            )),
            stale_payload: None,
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_stale_payload_delete_marker_source() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-delete-marker-source");
        let key = crate::tests::object_key("direct-put-snapshot-delete-marker-source-key");
        let generation_id = GenerationId::new(9).unwrap();
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: Some(test_delete_marker_stored_object(
                bucket.clone(),
                key.clone(),
            )),
            stale_payload: Some(test_segments_reclaim(
                bucket.clone(),
                key.clone(),
                generation_id,
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_stale_payload_generation_mismatch() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-generation");
        let key = crate::tests::object_key("direct-put-snapshot-generation-key");
        let source_generation = GenerationId::new(9).unwrap();
        let reclaim_generation = GenerationId::new(10).unwrap();
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: Some(test_live_stored_object(
                bucket.clone(),
                key.clone(),
                source_generation,
                ObjectLayout::Standard,
            )),
            stale_payload: Some(test_segments_reclaim(
                bucket.clone(),
                key.clone(),
                reclaim_generation,
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_numbered_stale_source() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-numbered-source");
        let key = crate::tests::object_key("direct-put-snapshot-numbered-source-key");
        let generation_id = GenerationId::new(9).unwrap();
        let mut source = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            ObjectLayout::Standard,
        );
        let StoredObject::Live(source_record) = &mut source else {
            unreachable!("test helper always builds a live object");
        };
        source_record.version_id = VersionId::from_u64(2);
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: Some(source),
            stale_payload: Some(test_segments_reclaim(
                bucket.clone(),
                key.clone(),
                generation_id,
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_rejects_stale_payload_layout_mismatch() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-layout");
        let key = crate::tests::object_key("direct-put-snapshot-layout-key");
        let generation_id = GenerationId::new(9).unwrap();
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: None,
            },
            current: None,
            stale_payload_source: Some(test_live_stored_object(
                bucket.clone(),
                key.clone(),
                generation_id,
                test_multipart_layout(),
            )),
            stale_payload: Some(test_segments_reclaim(
                bucket.clone(),
                key.clone(),
                generation_id,
            )),
        };

        assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_direct_put_snapshot_response_accepts_stale_null_source_under_numbered_current() {
        let bucket = crate::tests::bucket_name("direct-put-snapshot-null-source");
        let key = crate::tests::object_key("direct-put-snapshot-null-source-key");
        let current_generation = GenerationId::new(10).unwrap();
        let stale_generation = GenerationId::new(9).unwrap();
        let mut current = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            current_generation,
            ObjectLayout::Standard,
        );
        let StoredObject::Live(current_record) = &mut current else {
            unreachable!("test helper always builds a live object");
        };
        current_record.version_id = VersionId::from_u64(2);
        let expected_etag = current_record.etag.format();
        let snapshot = DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot {
                existing_etag: Some(expected_etag),
            },
            current: Some(current),
            stale_payload_source: Some(test_live_stored_object(
                bucket.clone(),
                key.clone(),
                stale_generation,
                ObjectLayout::Standard,
            )),
            stale_payload: Some(test_segments_reclaim(
                bucket.clone(),
                key.clone(),
                stale_generation,
            )),
        };

        assert_direct_put_snapshot_accepted(&snapshot, &bucket, &key);
    }

    #[test]
    fn unix_delete_specific_command_response_rejects_wrong_reclaim_target() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("delete-specific-target-rpc");
        let key = crate::tests::object_key("delete-specific-target-rpc-key");
        let generation_id = GenerationId::new(9).unwrap();
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            ObjectLayout::Standard,
        );
        let expected_target = DeleteObjectVersionTarget::Live {
            generation_id,
            layout: ObjectLayout::Standard,
            payload: test_segments_reclaim(bucket.clone(), key.clone(), generation_id),
        };
        let bad_generation_id = GenerationId::new(10).unwrap();
        let bad_target = DeleteObjectVersionTarget::Live {
            generation_id: bad_generation_id,
            layout: ObjectLayout::Standard,
            payload: test_segments_reclaim(bucket.clone(), key.clone(), bad_generation_id),
        };
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: proof.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                target: bad_target,
            })),
        );

        let err = client
            .validate_delete_specific_object_command_response(
                &command,
                &BuildDeleteSpecificObjectVersionCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    version_id: VersionId::Null,
                    expected_stored: Some(&stored),
                    expected_target: Some(&expected_target),
                    expected_version_list: None,
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate delete-specific object command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_insert_delete_marker_response_rejects_wrong_snapshot_stale_payload() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("insert-marker-stale-rpc");
        let key = crate::tests::object_key("insert-marker-stale-rpc-key");
        let generation_id = GenerationId::new(9).unwrap();
        let stored = test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            ObjectLayout::Standard,
        );
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: proof.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 1,
                last_modified_millis: 1,
                stale_payload: Some(test_segments_reclaim(
                    crate::tests::bucket_name("wrong-insert-marker-stale-rpc"),
                    key.clone(),
                    generation_id,
                )),
            }),
        );
        let owner = OwnerIdentity::from_principal("owner");

        let err = client
            .validate_insert_delete_marker_command_response(
                &command,
                &BuildInsertDeleteMarkerCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    expected_current: Some(&stored),
                    version_id: VersionId::Null,
                    owner: &owner,
                    stale_payload: InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                        created_at: 1,
                    },
                    expected_stale_payload_source: Some(&stored),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate insert-delete-marker command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_proof_release_response_rejects_non_empty_payload() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );

        client.validate_proof_release_response(&[]).unwrap();
        let err = client
            .validate_proof_release_response(b"unexpected")
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode proof release response",
                ..
            })
        ));
    }

    #[test]
    fn unix_bucket_write_reservation_client_acquires_validates_and_releases() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-write-reservation-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
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
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..4)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let lease_deadline = crate::clock::current_time_millis().saturating_add(60_000);
        let record = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &bucket,
            "reservation-remote-1",
            "owner-token-remote-1",
            ClusterEpoch::new(1).unwrap(),
            "put-object",
            10,
            Some(lease_deadline),
            Some("key=a"),
        )
        .unwrap();
        assert_eq!(record.bucket, bucket);
        assert_eq!(record.reservation_id, "reservation-remote-1");

        BucketWriteReservationNodeClient::validate_bucket_write_reservation_proof(
            &client,
            PgId::new(0),
            &BucketWriteReservationProof::from(&record),
        )
        .unwrap();
        let renewed_deadline = lease_deadline.saturating_add(60_000);
        let renewed = BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &BucketWriteReservationProof::from(&record),
            renewed_deadline,
        )
        .unwrap();
        assert_eq!(renewed.lease_deadline, Some(renewed_deadline));
        BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &renewed,
        )
        .unwrap();
        for thread in server_threads {
            thread.join().unwrap();
        }

        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        assert!(PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &bucket,
            "reservation-remote-1",
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn unix_bucket_write_reservation_client_preserves_draining_signal() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-write-reservation-draining-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
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
            PgMetadataStore::begin_durable_bucket_write_drain(
                &*pg,
                &bucket,
                "drain-remote-1",
                "drain-owner-remote-1",
                ClusterEpoch::new(1).unwrap(),
                10,
                Some(20),
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let err = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &bucket,
            "reservation-remote-1",
            "owner-token-remote-1",
            ClusterEpoch::new(1).unwrap(),
            "put-object",
            30,
            Some(40),
            Some("key=a"),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_write_reservation_client_preserves_bucket_not_found() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-write-reservation-missing-rpc");
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let err = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &bucket,
            "reservation-remote-1",
            "owner-token-remote-1",
            ClusterEpoch::new(1).unwrap(),
            "put-object",
            30,
            Some(40),
            Some("key=a"),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })
                if name == bucket
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_write_reservation_client_routes_drain_and_finalize_coordination() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-drain-finalize-rpc");
        let finalize_bucket = crate::tests::bucket_name("bucket-finalize-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let finalize_bucket_incarnation_generation = {
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
                "reservation-for-drain-list",
                "reservation-owner-for-drain-list",
                ClusterEpoch::new(1).unwrap(),
                "put-object",
                10,
                Some(20),
                Some("key=a"),
            )
            .unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &finalize_bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::mark_bucket_deleting(&*pg, &finalize_bucket).unwrap();
            PgMetadataStore::head_bucket_raw(&*pg, &finalize_bucket)
                .unwrap()
                .bucket_incarnation_generation
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..13)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let reservations = BucketWriteReservationNodeClient::durable_bucket_write_reservations(
            &client,
            PgId::new(0),
            &bucket,
        )
        .unwrap();
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].reservation_id, "reservation-for-drain-list");

        let drain = BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
            &client,
            PgId::new(0),
            &bucket,
            "drain-rpc-1",
            "drain-owner-rpc-1",
            ClusterEpoch::new(1).unwrap(),
            30,
            Some(40),
        )
        .unwrap();
        assert_eq!(drain.bucket, bucket);
        assert_eq!(drain.drain_id, "drain-rpc-1");
        assert!(
            BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
                &client,
                PgId::new(0),
                &bucket,
            )
            .unwrap()
        );
        BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
            &client,
            PgId::new(0),
            &drain,
        )
        .unwrap();
        assert!(
            !BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
                &client,
                PgId::new(0),
                &bucket,
            )
            .unwrap()
        );

        BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
            &client,
            PgId::new(0),
            &bucket,
            "expired-drain-rpc-1",
            "expired-drain-owner-rpc-1",
            ClusterEpoch::new(1).unwrap(),
            50,
            Some(55),
        )
        .unwrap();
        let expired = BucketWriteReservationNodeClient::clear_expired_durable_bucket_write_drain(
            &client,
            PgId::new(0),
            &bucket,
            60,
        )
        .unwrap()
        .expect("expired drain should clear");
        assert_eq!(expired.drain_id, "expired-drain-rpc-1");

        let claim = BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
            &client,
            PgId::new(0),
            &finalize_bucket,
            finalize_bucket_incarnation_generation,
            "finalize-claim-rpc-1",
            "finalize-claim-owner-rpc-1",
            ClusterEpoch::new(1).unwrap(),
            70,
            Some(80),
            70,
        )
        .unwrap()
        .expect("finalize claim should acquire");
        assert_eq!(claim.bucket, finalize_bucket);
        assert_eq!(claim.claim_id, "finalize-claim-rpc-1");
        let replacement_claim =
            BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
                &client,
                PgId::new(0),
                &finalize_bucket,
                finalize_bucket_incarnation_generation,
                "finalize-claim-rpc-2",
                "finalize-claim-owner-rpc-2",
                ClusterEpoch::new(1).unwrap(),
                81,
                Some(100),
                81,
            )
            .unwrap()
            .expect("expired finalizer claim should be stealable");
        let stale_release = BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
            &client,
            PgId::new(0),
            &claim,
        )
        .unwrap_err();
        assert!(matches!(
            stale_release,
            BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict { .. })
        ));
        BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
            &client,
            PgId::new(0),
            &replacement_claim,
        )
        .unwrap();

        let roots = BucketWriteReservationNodeClient::get_bucket_delete_finalize_roots(
            &client,
            PgId::new(0),
            90,
            16,
        )
        .unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].bucket, finalize_bucket);

        BucketWriteReservationNodeClient::delete_finalized_bucket(
            &client,
            PgId::new(0),
            &finalize_bucket,
        )
        .unwrap();

        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_bucket_write_reservation_client_routes_lifecycle_sweep_coordination() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-lifecycle-sweep-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let bucket_incarnation_generation = {
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
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
            PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..6)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let buckets =
            BucketWriteReservationNodeClient::list_lifecycle_sweep_buckets(&client, PgId::new(0))
                .unwrap();
        assert_eq!(buckets.lifecycle_buckets.len(), 1);
        assert_eq!(buckets.lifecycle_buckets[0].name, bucket);
        assert!(buckets.aborting_buckets.is_empty());

        let roots = BucketWriteReservationNodeClient::get_lifecycle_sweep_roots(
            &client,
            PgId::new(0),
            10,
            16,
        )
        .unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].bucket, bucket);
        assert_eq!(
            roots[0].source,
            crate::types::LifecycleSweepRootSource::LifecycleConfig
        );

        let claim = BucketWriteReservationNodeClient::acquire_lifecycle_sweep_claim(
            &client,
            PgId::new(0),
            &bucket,
            bucket_incarnation_generation,
            "lifecycle-claim-rpc-1",
            "lifecycle-owner-rpc-1",
            ClusterEpoch::new(1).unwrap(),
            20,
            Some(40),
            20,
        )
        .unwrap()
        .expect("lifecycle claim should acquire");
        assert_eq!(claim.bucket, bucket);
        assert_eq!(claim.claim_id, "lifecycle-claim-rpc-1");

        let heartbeat = BucketWriteReservationNodeClient::heartbeat_lifecycle_sweep_claim(
            &client,
            PgId::new(0),
            &claim,
            30,
            Some(50),
        )
        .unwrap();
        assert_eq!(heartbeat.heartbeat_at, 30);
        assert_eq!(heartbeat.lease_deadline, Some(50));

        let error_record = BucketWriteReservationNodeClient::record_lifecycle_sweep_claim_error(
            &client,
            PgId::new(0),
            &heartbeat,
            "transient lifecycle error",
        )
        .unwrap();
        assert_eq!(
            error_record.last_error.as_deref(),
            Some("transient lifecycle error")
        );

        BucketWriteReservationNodeClient::release_lifecycle_sweep_claim(
            &client,
            PgId::new(0),
            &error_record,
        )
        .unwrap();

        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_bucket_delete_finalized_response_rejects_wrong_not_found_bucket() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("delete-finalized-not-found-expected");
        let wrong_bucket = crate::tests::bucket_name("delete-finalized-not-found-wrong");

        let err = client
            .validate_bucket_delete_finalized_response(
                StorageRpcBucketDeleteFinalizedResponse {
                    outcome: StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound {
                        name: wrong_bucket,
                    },
                },
                &bucket,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketWriteDrainError::Store(StoreError::StorageRpc {
                operation: "validate bucket delete finalized response",
                ..
            })
        ));
    }

    #[test]
    fn unix_bucket_metadata_client_loads_bucket_snapshot() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-snapshot-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
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
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Policy,
                    body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                    aux: crate::types::BucketSubresourceAux::policy(false),
                },
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let request = BucketSnapshotRequest {
            policy: true,
            tags: BucketSnapshotTagsRequest::Always,
            lifecycle: false,
            cors: true,
        };
        let snapshot =
            BucketMetadataNodeClient::load_bucket_snapshot(&client, PgId::new(0), &bucket, request)
                .unwrap();
        assert_eq!(snapshot.bucket.name, bucket);
        assert_eq!(snapshot.request, request);
        assert_eq!(
            snapshot.policy,
            crate::types::LoadedBucketSubresource::Loaded(
                "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string()
            )
        );
        assert_eq!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::Missing
        );
        assert_eq!(
            snapshot.lifecycle,
            crate::types::LoadedBucketSubresource::NotRequested
        );
        assert_eq!(
            snapshot.cors,
            crate::types::LoadedBucketSubresource::Missing
        );
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_metadata_client_loads_bucket_snapshot_pair() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let source_bucket = crate::tests::bucket_name("bucket-snapshot-pair-source-rpc");
        let destination_bucket = crate::tests::bucket_name("bucket-snapshot-pair-dest-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            for bucket in [&source_bucket, &destination_bucket] {
                PgMetadataStore::create_bucket(
                    &*pg,
                    bucket,
                    "owner",
                    &owner,
                    &crate::AclGrants::default(),
                    false,
                    false,
                )
                .unwrap();
            }
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &source_bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Tagging,
                    body: "<Tagging/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &destination_bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Cors,
                    body: "<CORSConfiguration/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let pair = BucketMetadataNodeClient::load_bucket_snapshot_pair(
            &client,
            PgId::new(0),
            (
                &source_bucket,
                BucketSnapshotRequest {
                    tags: BucketSnapshotTagsRequest::Always,
                    ..Default::default()
                },
            ),
            PgId::new(0),
            (
                &destination_bucket,
                BucketSnapshotRequest {
                    cors: true,
                    ..Default::default()
                },
            ),
        )
        .unwrap();
        assert_eq!(pair.source().bucket.name, source_bucket);
        assert_eq!(pair.destination().bucket.name, destination_bucket);
        assert_eq!(
            pair.source().tags,
            crate::types::LoadedBucketSubresource::Loaded("<Tagging/>".to_string())
        );
        assert_eq!(
            pair.destination().cors,
            crate::types::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
        );
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_metadata_client_builds_completed_multipart_order_command() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("completed-order-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
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
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );

        let (completion_order, command) =
            BucketMetadataNodeClient::build_advance_completed_multipart_upload_sequence_command(
                &client,
                PgId::new(0),
                &bucket,
                command_id,
            )
            .unwrap();

        assert_eq!(completion_order, 1);
        assert_eq!(command.id(), command_id);
        match command.payload() {
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance) => {
                assert_eq!(advance.bucket, bucket);
                assert_eq!(advance.completion_order, completion_order);
            }
            other => panic!("unexpected command payload: {other:?}"),
        }
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_metadata_client_routes_bucket_control_operations() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bucket-control-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
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
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Policy,
                    body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                    aux: crate::types::BucketSubresourceAux::policy(false),
                },
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..6)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let command_id = |log_index| {
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            )
        };

        let versioning = BucketMetadataNodeClient::build_put_bucket_versioning_command(
            &client,
            PgId::new(0),
            &bucket,
            command_id(1),
            BucketVersioningState::Enabled,
        )
        .unwrap();
        let MetadataCommandPayload::PutBucketVersioning(versioning_command) = versioning.payload()
        else {
            panic!("unexpected versioning command payload");
        };
        assert!(
            BucketMetadataNodeClient::pending_put_bucket_versioning_command_matches_current(
                &client,
                PgId::new(0),
                &bucket,
                versioning_command,
                BucketVersioningState::Enabled,
            )
            .unwrap()
        );

        let acl = BucketMetadataNodeClient::build_put_bucket_acl_command(
            &client,
            PgId::new(0),
            &bucket,
            command_id(2),
            &crate::AclGrants::default(),
            true,
            false,
        )
        .unwrap();
        match acl.payload() {
            MetadataCommandPayload::PutBucketAcl(command) => {
                assert_eq!(command.bucket.name, bucket);
                assert!(command.bucket.public_read);
                assert!(!command.bucket.public_write);
            }
            other => panic!("unexpected ACL command payload: {other:?}"),
        }

        let property = BucketMetadataNodeClient::build_put_bucket_property_command(
            &client,
            PgId::new(0),
            &bucket,
            command_id(3),
            &BucketPropertyMutation::AbacEnabled(true),
        )
        .unwrap();
        match property.payload() {
            MetadataCommandPayload::PutBucketProperty(command) => {
                assert_eq!(command.bucket.name, bucket);
                assert!(command.bucket.bucket_abac_enabled);
            }
            other => panic!("unexpected property command payload: {other:?}"),
        }

        let subresource = BucketSubresourceMutation::Put {
            kind: BucketSubresourceKind::Lifecycle,
            body: "<LifecycleConfiguration/>".to_string(),
            aux: crate::types::BucketSubresourceAux::None,
        };
        let subresource_command = BucketMetadataNodeClient::build_put_bucket_subresource_command(
            &client,
            PgId::new(0),
            &bucket,
            command_id(4),
            &subresource,
        )
        .unwrap();
        match subresource_command.payload() {
            MetadataCommandPayload::PutBucketSubresource(command) => {
                assert!(command.matches_mutation(&bucket, &subresource));
            }
            other => panic!("unexpected subresource command payload: {other:?}"),
        }

        let policy = BucketMetadataNodeClient::get_bucket_subresource(
            &client,
            PgId::new(0),
            &bucket,
            BucketSubresourceKind::Policy,
        )
        .unwrap();
        assert_eq!(
            policy.as_deref(),
            Some("{\"Version\":\"2012-10-17\",\"Statement\":[]}")
        );

        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_bucket_metadata_client_releases_bucket_write_proof() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("proof-release-rpc-bucket");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let reservation = {
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
                "reservation-1",
                "owner-token-1",
                ClusterEpoch::new(1).unwrap(),
                "put-object",
                10,
                Some(20),
                Some("key=a"),
            )
            .unwrap()
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        client
            .release_metadata_command_bucket_write_reservation(
                PgId::new(0),
                &BucketWriteReservationProof::from(&reservation),
            )
            .unwrap();
        server_thread.join().unwrap();

        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unix_bucket_metadata_client_rejects_proof_release_wrong_bucket_pg() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: crate::types::PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
            StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: crate::types::PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
        ];
        let owner = crate::CanonicalUserId::from_principal("owner");
        let (bucket, correct_pg_id, wrong_pg_id, reservation) = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let (bucket, correct_pg_id, wrong_pg_id) = (0..100)
                .map(|index| crate::tests::bucket_name(format!("proof-release-wrong-pg-{index}")))
                .find_map(|bucket| {
                    let correct_pg_id = node.pg_topology().bucket_pg_for(&bucket);
                    (correct_pg_id < 2).then(|| {
                        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
                        (bucket, correct_pg_id, wrong_pg_id)
                    })
                })
                .expect("two-PG topology must place a test bucket");
            let pg = node.get_pg(correct_pg_id).unwrap();
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
            let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                &bucket,
                "reservation-1",
                "owner-token-1",
                ClusterEpoch::new(1).unwrap(),
                "put-object",
                10,
                Some(20),
                Some("key=a"),
            )
            .unwrap();
            (bucket, correct_pg_id, wrong_pg_id, reservation)
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let err = client
            .release_metadata_command_bucket_write_reservation(
                PgId::new(wrong_pg_id),
                &BucketWriteReservationProof::from(&reservation),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "proof release",
                ..
            })
        ));
        server_thread.join().unwrap();

        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(correct_pg_id).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn unix_bucket_metadata_client_rejects_proof_release_on_non_primary() {
        let tmp = test_util::tempdir();
        let mut primary_config = test_config(&tmp);
        primary_config.node_id = NodeId::new(7);
        primary_config.data_dir = tmp.path().join("primary-node");
        primary_config.pg_routes[0].primary_node_id = NodeId::new(7);
        primary_config.pg_routes[0].acting_set = vec![NodeId::new(8), NodeId::new(7)];
        let mut replica_config = primary_config.clone();
        replica_config.node_id = NodeId::new(8);
        replica_config.data_dir = tmp.path().join("replica-node");
        replica_config.socket_path = tmp.path().join("sock").join("replica-storage.sock");
        let bucket = crate::tests::bucket_name("proof-release-replica-bucket");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let reservation = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &primary_config.data_dir,
                &primary_config.pg_ids,
                primary_config.default_ec_shape,
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
                "reservation-1",
                "owner-token-1",
                ClusterEpoch::new(1).unwrap(),
                "put-object",
                10,
                Some(20),
                Some("key=a"),
            )
            .unwrap()
        };
        private_socket_dir(replica_config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(replica_config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(8),
            ClusterEpoch::new(1).unwrap(),
            replica_config.socket_path.clone(),
        );

        let err = client
            .release_metadata_command_bucket_write_reservation(
                PgId::new(0),
                &BucketWriteReservationProof::from(&reservation),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "proof release",
                ..
            })
        ));
        server_thread.join().unwrap();

        let node = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &primary_config.pg_ids,
            primary_config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn unix_object_generation_metadata_client_routes_generation_reads() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("object-generation-rpc-bucket");
        let key = crate::tests::object_key("object-generation-rpc-key");
        let reservation_id = crate::tests::stream_session_id("obj-gen-rpc");
        let reserved_generation = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap()
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..2)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        assert_eq!(
            client
                .object_generation_reservation(PgId::new(0), &bucket, &key, &reservation_id)
                .unwrap(),
            reserved_generation
        );
        assert_eq!(
            client
                .next_object_generation_id(PgId::new(0), &bucket, &key)
                .unwrap(),
            GenerationId::new(reserved_generation.get() + 1).unwrap()
        );
        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_object_version_metadata_client_routes_version_reads() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let bucket = crate::tests::bucket_name("object-version-rpc-bucket");
        let key = crate::tests::object_key("object-version-rpc-key");

        assert_eq!(
            ObjectVersionMetadataNodeClient::next_object_version_id(
                &client,
                PgId::new(0),
                &bucket,
                &key
            )
            .unwrap(),
            VersionId::from_u64(1)
        );
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_object_read_metadata_client_loads_subject_and_snapshot() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("object-read-rpc-bucket");
        let key = crate::tests::object_key("object-read-rpc-key");
        let generation_id = GenerationId::new(9).unwrap();
        let segment_vid = GenerationId::new(10).unwrap();
        let segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            segment_index: 0,
            size: 12,
            segment_crc64: Some(99),
            segment_okh: [3; 16],
            segment_vid,
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        };
        {
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
                &crate::CanonicalUserId::from_principal("owner"),
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::put_object_with_segments(
                &*pg,
                &PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size: 12,
                    etag: ObjectEtag::single_part(99),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: Some(SerializedTagSet::new(
                        "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"
                            .to_string(),
                    )),
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                std::slice::from_ref(&segment),
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..3)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let subject = ObjectReadMetadataNodeClient::load_object_read_auth_subject(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            None,
        )
        .unwrap();
        assert_eq!(subject.stored.bucket(), &bucket);
        assert_eq!(subject.stored.key(), &key);

        let snapshot = ObjectReadMetadataNodeClient::load_object_read_snapshot_for_subject(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            None,
            &subject.identity,
            ObjectReadSnapshotMode::StandardSegments,
        )
        .unwrap();
        assert_eq!(snapshot.stored, subject.stored);
        assert_eq!(snapshot.object_segments, vec![segment]);
        assert!(snapshot.multipart_parts.is_empty());
        assert!(snapshot.multipart_part_segments.is_empty());

        let tags = ObjectReadMetadataNodeClient::get_object_tags_for_subject(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            None,
            &subject.identity,
            VersionId::Null,
        )
        .unwrap();
        assert_eq!(
            tags.as_deref(),
            Some("<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>")
        );

        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_object_mutation_metadata_client_loads_snapshots_and_builds_commands() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("object-mutation-rpc-bucket");
        let key = crate::tests::object_key("object-mutation-rpc-key");
        let listed_stream_request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("mut-rpc-listed"),
            bucket: bucket.clone(),
            key: crate::tests::object_key("object-mutation-listed-stream-key"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        };
        let completed_upload = CompletedMultipartUploadRecord {
            upload_id: crate::tests::multipart_upload_id("mutCompletedRpc"),
            bucket: bucket.clone(),
            key: crate::tests::object_key("object-mutation-completed-key"),
            completion_order: 7,
            completed_at: 11,
            initiator: None,
            owner: OwnerIdentity::from_principal("owner"),
        };
        let generation_id = GenerationId::new(19).unwrap();
        let reclaim_generation_id = GenerationId::new(21).unwrap();
        let segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            segment_index: 0,
            size: 12,
            segment_crc64: Some(100),
            segment_okh: [4; 16],
            segment_vid: GenerationId::new(20).unwrap(),
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        };
        {
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
                &crate::CanonicalUserId::from_principal("owner"),
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::put_object_with_segments(
                &*pg,
                &PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size: 12,
                    etag: ObjectEtag::single_part(100),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                std::slice::from_ref(&segment),
            )
            .unwrap();
            PgMetadataStore::create_stream_upload(&*pg, &listed_stream_request).unwrap();
            pg.connection()
                .execute(
                    "INSERT INTO completed_multipart_uploads \
                     (upload_id, bucket, key, completion_order, completed_at, \
                      owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    rusqlite::params![
                        completed_upload.upload_id.as_str(),
                        completed_upload.bucket.as_str(),
                        completed_upload.key.as_str(),
                        completed_upload.completion_order as i64,
                        completed_upload.completed_at as i64,
                        completed_upload.owner.principal.as_str(),
                        completed_upload.owner.canonical_id.as_str(),
                        Option::<&str>::None,
                        Option::<&str>::None,
                    ],
                )
                .unwrap();
            pg.put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id: reclaim_generation_id,
                created_at: 12,
                segments: vec![ObjectSegmentsReclaimSegmentRecord {
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                }],
            })
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..18)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "object-mutation-test".to_string(),
            created_at: 1,
            lease_deadline: None,
            target_context: Some(key.as_str().to_string()),
        };

        let stored = ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            None,
        )
        .unwrap();
        assert_eq!(stored.bucket(), &bucket);
        assert_eq!(stored.key(), &key);

        let put_command = ObjectMutationMetadataNodeClient::build_put_object_metadata_command(
            &client,
            BuildPutObjectMetadataCommandReq {
                pg_id: PgId::new(0),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                requested_version_id: None,
                expected_stored: &stored,
                version_id: VersionId::Null,
                mutation: PutObjectMetadataMutation::PutTags("<Tagging/>".to_string()),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap();
        assert!(matches!(
            put_command.payload(),
            MetadataCommandPayload::PutObjectMetadata(update)
                if update.object.bucket == bucket && update.object.key == key
        ));

        let current = ObjectMutationMetadataNodeClient::load_current_object_delete_snapshot(
            &client,
            PgId::new(0),
            &bucket,
            &key,
        )
        .unwrap();
        assert_eq!(current.stored.as_ref(), Some(&stored));
        let stream_uploads = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
            &client,
            PgId::new(0),
            &bucket,
            None,
            10,
        )
        .unwrap();
        assert!(stream_uploads.uploads.iter().any(|upload| upload.session_id
            == listed_stream_request.session_id
            && upload.bucket == listed_stream_request.bucket
            && upload.key == listed_stream_request.key));
        let completed_uploads =
            ObjectMutationMetadataNodeClient::list_completed_multipart_upload_records_for_bucket_page(
                &client,
                PgId::new(0),
                &bucket,
                None,
                10,
            )
            .unwrap();
        assert_eq!(completed_uploads.records, vec![completed_upload.clone()]);
        let reclaim_root = ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &client,
            PgId::new(0),
            &bucket,
        )
        .unwrap()
        .expect("seeded reclaim root should exist");
        assert_eq!(reclaim_root.bucket, bucket);
        assert_eq!(reclaim_root.key, key);
        assert_eq!(reclaim_root.generation_id, reclaim_generation_id);
        let pg_reclaim_root =
            ObjectMutationMetadataNodeClient::get_payload_reclaim_root(&client, PgId::new(0))
                .unwrap()
                .expect("seeded PG reclaim root should exist");
        assert_eq!(pg_reclaim_root, reclaim_root);
        let object_reclaim = ObjectMutationMetadataNodeClient::get_object_payload_reclaim(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            reclaim_generation_id,
        )
        .unwrap()
        .expect("seeded object reclaim should exist");
        assert!(matches!(
            &object_reclaim,
            ObjectPayloadReclaimCommand::Segments(reclaim)
                if reclaim.bucket == bucket
                    && reclaim.key == key
                    && reclaim.generation_id == reclaim_generation_id
        ));
        let claim = ObjectMutationMetadataNodeClient::acquire_object_payload_reclaim_claim(
            &client,
            PgId::new(0),
            &bucket,
            1,
            &key,
            reclaim_generation_id,
            object_reclaim.kind(),
            "object-reclaim-claim",
            "object-reclaim-owner",
            ClusterEpoch::new(1).unwrap(),
            100,
            Some(1_000),
            100,
        )
        .unwrap()
        .expect("seeded object reclaim claim should be acquired");
        assert_eq!(claim.bucket, bucket);
        assert_eq!(claim.key, key);
        assert_eq!(claim.generation_id, reclaim_generation_id);
        ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
            &client,
            PgId::new(0),
            &claim,
        )
        .unwrap();
        client
            .validate_bucket_payload_reclaim_root_response(
                &StorageRpcPayloadReclaimRootResponse {
                    root: Some(PayloadReclaimRoot {
                        bucket: crate::tests::bucket_name("wrong-reclaim-root-bucket"),
                        key: key.clone(),
                        generation_id: reclaim_generation_id,
                    }),
                },
                &bucket,
            )
            .unwrap_err();
        assert!(ObjectMutationMetadataNodeClient::payload_reclaim_exists(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            reclaim_generation_id,
        )
        .unwrap());
        assert!(!ObjectMutationMetadataNodeClient::payload_reclaim_exists(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            GenerationId::new(22).unwrap(),
        )
        .unwrap());
        let delete_command = ObjectMutationMetadataNodeClient::build_delete_current_object_command(
            &client,
            BuildDeleteCurrentObjectCommandReq {
                pg_id: PgId::new(0),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current.stored.as_ref(),
                expected_target: current.target.as_ref(),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap()
        .expect("live current object should build delete command");
        assert!(matches!(
            delete_command.payload(),
            MetadataCommandPayload::DeleteObjectVersion(delete)
                if delete.bucket == bucket && delete.key == key
        ));

        let stream_request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("mut-stream-rpc"),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        };
        assert!(
            !ObjectMutationMetadataNodeClient::matching_stream_upload_exists(
                &client,
                PgId::new(0),
                &stream_request,
                None,
            )
            .unwrap()
        );
        let stream_command = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &client,
            BuildCreateStreamUploadCommandReq {
                pg_id: PgId::new(0),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &stream_request,
                precondition: CreateStreamUploadPrecondition::PutObject {
                    expected_current: Some(&stored),
                    require_generation_reservation: false,
                },
                bucket_write_reservation: &proof,
            },
        )
        .unwrap();
        let MetadataCommandPayload::CreateStreamUpload(stream_create) = stream_command.payload()
        else {
            panic!("expected create stream upload command");
        };
        assert_eq!(stream_create.session.bucket, bucket);
        assert_eq!(stream_create.session.key, key);
        assert_eq!(stream_create.bucket_write_reservation, proof);
        client
            .validate_stream_upload_match_response(true, Some(stream_create.as_ref()))
            .unwrap();
        let err = client
            .validate_stream_upload_match_response(true, None)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream upload match response",
                ..
            })
        ));

        let mut bad_stream_payload = stream_command.payload().clone();
        let MetadataCommandPayload::CreateStreamUpload(bad_stream_create) = &mut bad_stream_payload
        else {
            panic!("expected create stream upload command");
        };
        bad_stream_create.session.key = crate::tests::object_key("wrong-stream-key");
        let bad_stream_command =
            MetadataCommandEnvelope::new(stream_command.id(), bad_stream_payload);
        let err = client
            .validate_create_stream_upload_command_response(
                &bad_stream_command,
                &BuildCreateStreamUploadCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    request: &stream_request,
                    precondition: CreateStreamUploadPrecondition::PutObject {
                        expected_current: Some(&stored),
                        require_generation_reservation: false,
                    },
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream upload command build response",
                ..
            })
        ));

        let missing_upload_id = crate::tests::multipart_upload_id("mut-stream-rpc-missing-upload");
        let missing_upload = test_multipart_upload_record(
            bucket.clone(),
            key.clone(),
            missing_upload_id.clone(),
            UploadState::InProgress,
        );
        let missing_upload_part_stream_request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("mut-rpc-miss"),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::UploadPart {
                upload_id: missing_upload_id.clone(),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        };
        let err = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &client,
            BuildCreateStreamUploadCommandReq {
                pg_id: PgId::new(0),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &missing_upload_part_stream_request,
                precondition: CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &missing_upload,
                },
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { upload_id })
                if upload_id == missing_upload_id.as_str()
        ));

        let multipart_request = CreateMultipartUploadReq {
            upload_id: crate::tests::multipart_upload_id("object-mutation-mpu-rpc"),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        assert_eq!(
            ObjectMutationMetadataNodeClient::matching_multipart_upload_initiated_at(
                &client,
                PgId::new(0),
                &multipart_request,
                None,
            )
            .unwrap(),
            None
        );
        let multipart_command =
            ObjectMutationMetadataNodeClient::build_create_multipart_upload_command(
                &client,
                BuildCreateMultipartUploadCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    request: &multipart_request,
                    expected_current: Some(&stored),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap();
        let MetadataCommandPayload::CreateMultipartUpload(multipart_create) =
            multipart_command.payload()
        else {
            panic!("expected create multipart upload command");
        };
        assert_eq!(multipart_create.upload.bucket, bucket);
        assert_eq!(multipart_create.upload.key, key);
        assert_eq!(multipart_create.bucket_write_reservation, proof);
        client
            .validate_multipart_upload_match_response(
                Some(multipart_create.upload.initiated_at),
                Some(multipart_create.as_ref()),
            )
            .unwrap();
        let err = client
            .validate_multipart_upload_match_response(
                Some(multipart_create.upload.initiated_at),
                None,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart upload match response",
                ..
            })
        ));
        let err = client
            .validate_multipart_upload_match_response(
                Some(multipart_create.upload.initiated_at + 1),
                Some(multipart_create.as_ref()),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart upload match response",
                ..
            })
        ));

        let mut bad_multipart_payload = multipart_command.payload().clone();
        let MetadataCommandPayload::CreateMultipartUpload(bad_multipart_create) =
            &mut bad_multipart_payload
        else {
            panic!("expected create multipart upload command");
        };
        bad_multipart_create.upload.owner = OwnerIdentity::from_principal("other-owner");
        let bad_multipart_command =
            MetadataCommandEnvelope::new(multipart_command.id(), bad_multipart_payload);
        let err = client
            .validate_create_multipart_upload_command_response(
                &bad_multipart_command,
                &BuildCreateMultipartUploadCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    request: &multipart_request,
                    expected_current: Some(&stored),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart upload command build response",
                ..
            })
        ));

        for thread in server_threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn unix_stream_uploads_list_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let bucket = crate::tests::bucket_name("stream-upload-list-primary");
        let err = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
            &client,
            PgId::new(0),
            &bucket,
            None,
            1,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "object stream uploads list",
                ..
            })
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_stream_uploads_list_rejects_wrong_pg_rows() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: crate::types::PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
            StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: crate::types::PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
            },
        ];
        let (bucket, wrong_pg_id) = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let topology = node.pg_topology();
            let bucket = crate::tests::bucket_name("stream-upload-list-wrong-pg");
            let key = (0..100)
                .map(|index| crate::tests::object_key(format!("key-{index}")))
                .find(|key| topology.object_pg_for(&bucket, key) == 1)
                .expect("two-PG topology must place a test object on PG 1");
            let wrong_pg_id = 0;
            let session_id = crate::SessionId::try_from("ef".repeat(16)).unwrap();
            let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
            crate::PgMetadataStore::create_stream_upload(
                &*wrong_pg,
                &crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::PutObject,
                    encryption: crate::ObjectEncryption::None,
                },
            )
            .unwrap();
            wrong_pg.refresh_metadata_command_state_digest().unwrap();
            (bucket, wrong_pg_id)
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let err = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
            &client,
            PgId::new(wrong_pg_id),
            &bucket,
            None,
            10,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "object stream uploads list",
                ..
            })
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_payload_reclaim_exists_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let bucket = crate::tests::bucket_name("reclaim-primary-rpc-bucket");
        let key = crate::tests::object_key("reclaim-primary-rpc-key");

        let err = ObjectMutationMetadataNodeClient::payload_reclaim_exists(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            GenerationId::new(1).unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "object payload reclaim exists",
                ..
            })
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_bucket_payload_reclaim_root_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let bucket = crate::tests::bucket_name("bucket-reclaim-primary-rpc-bucket");

        let err = ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &client,
            PgId::new(0),
            &bucket,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "object bucket payload reclaim root",
                ..
            })
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_object_payload_reclaim_root_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let err = ObjectMutationMetadataNodeClient::get_payload_reclaim_root(&client, PgId::new(0))
            .unwrap_err();

        assert!(matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "object payload reclaim root",
                ..
            })
        ));
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_multipart_read_responses() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("multipart-read-rpc-bucket");
        let key = crate::tests::object_key("multipart-read-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("multipart-read-rpc-upload");
        let upload = test_multipart_upload_record(
            bucket.clone(),
            key.clone(),
            upload_id.clone(),
            UploadState::InProgress,
        );
        let authorized_upload = AuthorizedMultipartUploadRecord::assume_authorized(upload.clone());
        let part = test_multipart_part_record(upload_id.clone(), 1);

        client
            .validate_multipart_upload_response(
                &upload,
                &bucket,
                &key,
                &upload_id,
                Some(UploadState::InProgress),
                "validate in-progress multipart upload load response",
            )
            .unwrap();
        let mut wrong_upload = upload.clone();
        wrong_upload.key = crate::tests::object_key("wrong-multipart-read-key");
        let err = client
            .validate_multipart_upload_response(
                &wrong_upload,
                &bucket,
                &key,
                &upload_id,
                Some(UploadState::InProgress),
                "validate in-progress multipart upload load response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate in-progress multipart upload load response",
                ..
            })
        ));

        let snapshot = MultipartCompletionSnapshot {
            existing_etag: None,
            stale_payload_source: None,
            part_records: vec![part.clone()],
            selected_streaming_segments: Vec::new(),
            cleanup: CompleteMultipartCommitCleanup::default(),
        };
        client
            .validate_multipart_completion_snapshot_response(&snapshot, &authorized_upload, &[1])
            .unwrap();
        let mut bad_snapshot = snapshot.clone();
        bad_snapshot.cleanup.omitted_parts.push(part.clone());
        let err = client
            .validate_multipart_completion_snapshot_response(
                &bad_snapshot,
                &authorized_upload,
                &[1],
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart completion snapshot response",
                ..
            })
        ));

        let listed = ListedMultipartParts {
            upload: upload.clone(),
            response: crate::types::ListPartsResp {
                parts: vec![part.clone()],
                is_truncated: false,
                next_part_number_marker: None,
            },
        };
        client
            .validate_listed_multipart_parts_response(&listed, &authorized_upload, None, 1)
            .unwrap();
        let mut bad_listed = listed.clone();
        bad_listed.response.parts[0].upload_id =
            crate::tests::multipart_upload_id("wrong-listed-upload");
        let err = client
            .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let mut bad_listed = listed.clone();
        bad_listed.response.parts = vec![
            test_multipart_part_record(upload_id.clone(), 2),
            test_multipart_part_record(upload_id.clone(), 1),
        ];
        let err = client
            .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 2)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let err = client
            .validate_listed_multipart_parts_response(&listed, &authorized_upload, Some(1), 1)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let mut bad_listed = listed.clone();
        bad_listed.response.next_part_number_marker = Some(1);
        let err = client
            .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let mut bad_listed = listed.clone();
        bad_listed.response.is_truncated = true;
        bad_listed.response.next_part_number_marker = Some(2);
        let err = client
            .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let mut truncated_listed = listed.clone();
        truncated_listed.response.is_truncated = true;
        truncated_listed.response.next_part_number_marker = Some(1);
        client
            .validate_listed_multipart_parts_response(
                &truncated_listed,
                &authorized_upload,
                None,
                1,
            )
            .unwrap();
        let zero_page_listed = ListedMultipartParts {
            upload: upload.clone(),
            response: crate::types::ListPartsResp {
                parts: Vec::new(),
                is_truncated: false,
                next_part_number_marker: Some(0),
            },
        };
        client
            .validate_listed_multipart_parts_response(
                &zero_page_listed,
                &authorized_upload,
                None,
                0,
            )
            .unwrap();
        let mut bad_zero_page_listed = zero_page_listed.clone();
        bad_zero_page_listed.response.is_truncated = true;
        let err = client
            .validate_listed_multipart_parts_response(
                &bad_zero_page_listed,
                &authorized_upload,
                None,
                0,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));
        let mut bad_zero_page_listed = zero_page_listed.clone();
        bad_zero_page_listed.response.next_part_number_marker = Some(1);
        let err = client
            .validate_listed_multipart_parts_response(
                &bad_zero_page_listed,
                &authorized_upload,
                None,
                0,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart parts list response",
                ..
            })
        ));

        client
            .validate_multipart_management_lookup_response(
                &MultipartUploadManagementLookup::InProgress(Box::new(upload.clone())),
                &bucket,
                &key,
                &upload_id,
            )
            .unwrap();
        let err = client
            .validate_multipart_management_lookup_response(
                &MultipartUploadManagementLookup::Completed(CompletedMultipartUploadRecord {
                    upload_id,
                    bucket,
                    key: crate::tests::object_key("wrong-completed-key"),
                    completion_order: 1,
                    completed_at: 2,
                    initiator: None,
                    owner: OwnerIdentity::from_principal("owner"),
                }),
                &upload.bucket,
                &upload.key,
                &upload.upload_id,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate multipart management lookup response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_mutation_client_loads_multipart_upload_over_rpc() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("multipart-upload-load-rpc-bucket");
        let key = crate::tests::object_key("multipart-upload-load-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("multipart-upload-load-rpc-upload");
        {
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
                &crate::CanonicalUserId::from_principal("owner"),
                &AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::create_multipart_upload(
                &*pg,
                &CreateMultipartUploadReq {
                    upload_id: upload_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: None,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let upload = ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            &upload_id,
        )
        .unwrap();
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key, key);
        assert_eq!(upload.upload_id, upload_id);
        assert_eq!(upload.state, UploadState::InProgress);
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_direct_put_metadata_client_loads_commit_snapshot() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("direct-put-snapshot-rpc-bucket");
        let key = crate::tests::object_key("direct-put-snapshot-rpc-key");
        let reservation_id = crate::tests::stream_session_id("dp-snap-rpc");
        let reserved_generation = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap()
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );

        let snapshot = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            &reservation_id,
            reserved_generation,
        )
        .unwrap();
        assert_eq!(snapshot.auth_snapshot.existing_etag, None);
        assert_eq!(snapshot.current, None);
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_direct_put_metadata_client_builds_commit_command() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("direct-put-build-rpc-bucket");
        let key = crate::tests::object_key("direct-put-build-rpc-key");
        let reservation_id = crate::tests::stream_session_id("dp-build-rpc");
        let reserved_generation = {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap()
        };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_thread = {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        };
        let build_server_thread = thread::spawn(move || server.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            config.socket_path.clone(),
        );
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "direct-put".to_string(),
            created_at: 123,
            lease_deadline: None,
            target_context: Some(key.as_str().to_string()),
        };
        let request = CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_reservation_id: reservation_id.clone(),
            versioning: BucketVersioningState::Suspended,
            owner: OwnerIdentity {
                principal: "owner".to_string(),
                canonical_id: crate::CanonicalUserId::from_principal("owner"),
            },
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            generation_id: reserved_generation,
            size: 12,
            etag_crc64: 99,
            ec: EcShape { k: 4, m: 2 },
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            segment_index: 0,
            segment_crc64: Some(99),
            segment_okh: [7; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 0,
            bucket_write_reservation: proof.clone(),
        };
        let snapshot = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
            &client,
            PgId::new(0),
            &bucket,
            &key,
            &reservation_id,
            reserved_generation,
        )
        .unwrap();

        let command = DirectPutMetadataNodeClient::build_direct_put_commit_command(
            &client,
            BuildDirectPutCommitCommandReq {
                pg_id: PgId::new(0),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &request,
                version_id: VersionId::Null,
                expected_snapshot: &snapshot,
                bucket_write_reservation: &proof,
            },
        )
        .unwrap();

        assert_eq!(command.id().pg_id(), PgId::new(0));
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            panic!("expected direct PUT commit command");
        };
        assert!(commit.matches_request(&bucket, &key, &reservation_id, reserved_generation));
        assert_eq!(commit.bucket_write_reservation, proof);

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
            panic!("expected direct PUT commit command");
        };
        bad_commit.stale_payload = Some(ObjectPayloadReclaimCommand::Segments(
            ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id: reserved_generation,
                created_at: 0,
                segments: vec![ObjectSegmentsReclaimSegmentRecord {
                    segment_index: 99,
                    segment_okh: [9; 16],
                    segment_vid: GenerationId::new(11).unwrap(),
                    data_pg_id: 0,
                    ec: EcShape { k: 4, m: 2 },
                }],
            },
        ));
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_direct_put_command_build_response(
                &bad_command,
                &BuildDirectPutCommitCommandReq {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    request: &request,
                    version_id: VersionId::Null,
                    expected_snapshot: &snapshot,
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate direct PUT commit command build response",
                ..
            })
        ));
        server_thread.join().unwrap();
        build_server_thread.join().unwrap();
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_stream_append_read_responses() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("stream-append-rpc-bucket");
        let key = crate::tests::object_key("stream-append-rpc-key");
        let session_id = crate::tests::stream_session_id("append-rpc");
        let session = StreamUploadRecord {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            state: StreamUploadState::InProgress,
            created_at: 1,
            encryption: ObjectEncryption::None,
            next_segment_vid: GenerationId::new(2).unwrap(),
            bucket_write_reservation: None,
        };
        client
            .validate_stream_upload_session_response(
                &session,
                &bucket,
                &key,
                &session_id,
                "validate stream append read response",
            )
            .unwrap();
        let mut bad_session = session.clone();
        bad_session.session_id = crate::tests::stream_session_id("wrong-rpc");
        let err = client
            .validate_stream_upload_session_response(
                &bad_session,
                &bucket,
                &key,
                &session_id,
                "validate stream append read response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream append read response",
                ..
            })
        ));

        let request = PrepareStreamUploadSegmentAppendReq {
            session_id: session_id.clone(),
            segment_index: 0,
            size: 16,
            segment_crc64: Some(44),
            segment_okh: [3; 16],
        };
        let segment = StreamUploadSegmentRecord {
            session_id: session_id.clone(),
            segment_index: 0,
            size: request.size,
            segment_crc64: request.segment_crc64,
            segment_okh: request.segment_okh,
            segment_vid: GenerationId::new(1).unwrap(),
            data_pg_id: 0,
            ec_k: 1,
            ec_m: 0,
        };
        client
            .validate_stream_segment_append_prepare_response(
                &segment,
                &StreamUploadTarget::PutObject,
                &StreamUploadTarget::PutObject,
                &request,
                "validate stream append read response",
            )
            .unwrap();
        let mut bad_segment = segment.clone();
        bad_segment.size += 1;
        let err = client
            .validate_stream_segment_append_prepare_response(
                &bad_segment,
                &StreamUploadTarget::PutObject,
                &StreamUploadTarget::PutObject,
                &request,
                "validate stream append read response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream append read response",
                ..
            })
        ));
        let upload_part_target = StreamUploadTarget::UploadPart {
            upload_id: crate::tests::multipart_upload_id("append-rpc-upload"),
            part_number: 1,
        };
        let mut bad_upload_part_segment = segment.clone();
        bad_upload_part_segment.segment_okh = [9; 16];
        let err = client
            .validate_stream_segment_append_prepare_response(
                &bad_upload_part_segment,
                &StreamUploadTarget::PutObject,
                &upload_part_target,
                &request,
                "validate stream append read response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream append read response",
                ..
            })
        ));
        let err = client
            .validate_stream_segment_append_prepare_response(
                &bad_upload_part_segment,
                &upload_part_target,
                &upload_part_target,
                &request,
                "validate stream append read response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream append read response",
                ..
            })
        ));
        client
            .validate_stream_upload_segments_response(
                &[
                    segment.clone(),
                    StreamUploadSegmentRecord {
                        segment_index: 1,
                        segment_vid: GenerationId::new(2).unwrap(),
                        ..segment.clone()
                    },
                ],
                &session_id,
                "validate stream append read response",
            )
            .unwrap();
        let err = client
            .validate_stream_upload_segments_response(
                &[
                    StreamUploadSegmentRecord {
                        segment_index: 1,
                        segment_vid: GenerationId::new(2).unwrap(),
                        ..segment.clone()
                    },
                    segment,
                ],
                &session_id,
                "validate stream append read response",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream append read response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_stream_put_commit_response() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("stream-put-rpc-bucket");
        let key = crate::tests::object_key("stream-put-rpc-key");
        let session_id = crate::tests::stream_session_id("stream-put-rpc");
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let segment = StreamUploadSegmentRecord {
            session_id: session_id.clone(),
            segment_index: 0,
            size: 12,
            segment_crc64: Some(99),
            segment_okh: [4; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        };
        let snapshot = StreamPutFinalizeStorageSnapshot {
            session: StreamUploadRecord {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::PutObject,
                state: StreamUploadState::InProgress,
                created_at: 1,
                encryption: ObjectEncryption::None,
                next_segment_vid: GenerationId::new(11).unwrap(),
                bucket_write_reservation: None,
            },
            existing_etag: None,
            generation_id: GenerationId::new(20).unwrap(),
            stale_payload_source: None,
            stale_payload: None,
            staging_segments: vec![segment.clone()],
        };
        let commit_input = StreamPutCommitInput {
            versioning: BucketVersioningState::Suspended,
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            size: 12,
            etag_crc64: 99,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: commit_input.owner.clone(),
                    acl_grants: commit_input.acl_grants.clone(),
                    public_read: commit_input.public_read,
                    generation_id: snapshot.generation_id,
                    size: commit_input.size,
                    etag: ObjectEtag::single_part(commit_input.etag_crc64),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: commit_input.tags.clone(),
                    metadata_blob: Some(commit_input.metadata_blob.clone()),
                    system_metadata_blob: Some(commit_input.system_metadata_blob.clone()),
                    object_lock: commit_input.object_lock,
                    encryption: commit_input.encryption.clone(),
                },
                segments: vec![ObjectSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    segment_index: segment.segment_index,
                    size: segment.size,
                    segment_crc64: segment.segment_crc64,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                }],
                generation_reservation_id: session_id.clone(),
                write_sequence: 1,
                last_modified_millis: 1,
                stale_payload: None,
                bucket_write_reservation: proof.clone(),
                stream_create_bucket_write_reservation: None,
            })),
        );
        let request = BuildStreamPutCommitCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            session_id: &session_id,
            total_size: 12,
            expected_snapshot: &snapshot,
            commit: &commit_input,
            bucket_write_reservation: &proof,
        };
        client
            .validate_stream_put_commit_command_response(&command, &request)
            .unwrap();

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
            panic!("expected stream PUT commit command");
        };
        bad_commit.object.generation_id = GenerationId::new(21).unwrap();
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_stream_put_commit_command_response(&bad_command, &request)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream PUT commit command build response",
                ..
            })
        ));

        let stale_generation = GenerationId::new(50).unwrap();
        let mut snapshot_with_stale = snapshot.clone();
        snapshot_with_stale.stale_payload_source = Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            stale_generation,
            ObjectLayout::Standard,
        ));
        snapshot_with_stale.stale_payload = Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            stale_generation,
        ));
        let mut stale_payload = command.payload().clone();
        let MetadataCommandPayload::CommitDirectPutObject(stale_commit) = &mut stale_payload else {
            panic!("expected stream PUT commit command");
        };
        stale_commit.stale_payload = snapshot_with_stale.stale_payload.clone();
        let stale_command = MetadataCommandEnvelope::new(command.id(), stale_payload);
        let stale_request = BuildStreamPutCommitCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            session_id: &session_id,
            total_size: 12,
            expected_snapshot: &snapshot_with_stale,
            commit: &commit_input,
            bucket_write_reservation: &proof,
        };
        client
            .validate_stream_put_commit_command_response(&stale_command, &stale_request)
            .unwrap();

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
            panic!("expected stream PUT commit command");
        };
        bad_commit.stale_payload = Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            GenerationId::new(51).unwrap(),
        ));
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_stream_put_commit_command_response(&bad_command, &stale_request)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream PUT commit command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_stream_part_commit_response() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("stream-part-rpc-bucket");
        let key = crate::tests::object_key("stream-part-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("stream-part-rpc-upload");
        let session_id = crate::tests::stream_session_id("stream-part-rpc");
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let upload = MultipartUploadRecord {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: 1,
            state: UploadState::InProgress,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_generation_id: GenerationId::new(30).unwrap(),
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        let snapshot = StreamUploadPartStorageSnapshot {
            auth_snapshot: StreamUploadPartSnapshot {
                session: StreamUploadRecord {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: StreamUploadTarget::UploadPart {
                        upload_id: upload_id.clone(),
                        part_number: 1,
                    },
                    state: StreamUploadState::InProgress,
                    created_at: 1,
                    encryption: ObjectEncryption::None,
                    next_segment_vid: GenerationId::new(31).unwrap(),
                    bucket_write_reservation: None,
                },
                upload: upload.clone(),
                existing_part_generation: None,
                staging_segments: vec![StreamUploadSegmentRecord {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: 12,
                    segment_crc64: Some(100),
                    segment_okh: [5; 16],
                    segment_vid: GenerationId::new(32).unwrap(),
                    data_pg_id: 0,
                    ec_k: 4,
                    ec_m: 2,
                }],
            },
            existing_part: None,
            displaced_segments: Vec::new(),
        };
        let part = MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 1,
            size: 12,
            etag: vec![1; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [6; 16],
            part_vid: GenerationId::new(40).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 10,
            checksum: None,
        };
        let segments = vec![MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: 1,
            part_number: 1,
            segment_index: 0,
            size: 12,
            segment_crc64: Some(100),
            segment_okh: [5; 16],
            segment_vid: GenerationId::new(32).unwrap(),
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        }];
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                upload,
                part: part.clone(),
                segments: segments.clone(),
                existing_part: None,
                displaced_segments: Vec::new(),
                bucket_write_reservation: proof.clone(),
            })),
        );
        let request = BuildStreamPartCommitCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            upload_id: &upload_id,
            session_id: &session_id,
            part_number: 1,
            expected_snapshot: &snapshot,
            part: &part,
            segments: &segments,
            bucket_write_reservation: &proof,
        };
        client
            .validate_stream_part_commit_command_response(&command, &request)
            .unwrap();

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitStreamPart(bad_commit) = &mut bad_payload else {
            panic!("expected stream part commit command");
        };
        bad_commit.segments[0].key = crate::tests::object_key("wrong-stream-part-key");
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_stream_part_commit_command_response(&bad_command, &request)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate stream part commit command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_complete_multipart_response() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("complete-mpu-rpc-bucket");
        let key = crate::tests::object_key("complete-mpu-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("complete-mpu-rpc-upload");
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let part = MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 1,
            size: 12,
            etag: vec![1; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [6; 16],
            part_vid: GenerationId::new(40).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 10,
            checksum: None,
        };
        let request = CompleteMultipartCommitRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            versioning: BucketVersioningState::Enabled,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(30).unwrap(),
            size: 12,
            etag_crc64: [8; 8],
            tags: None,
            metadata_blob: Some(SerializedMetadataBlob::default()),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
            expected_stale_payload_source: None,
            part_records: vec![part.clone()],
            selected_streaming_segments: Vec::new(),
            expected_cleanup: CompleteMultipartCommitCleanup::default(),
        };
        let version_id = VersionId::from_u64(9);
        let parts_count = std::num::NonZeroU32::new(1).unwrap();
        let expected_object_parts = vec![ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            part_number: part.part_number,
            size: part.size,
            etag: part.etag.clone(),
            etag_kind: part.etag_kind,
            part_okh: part.part_okh,
            part_vid: part.part_vid,
            ec_k: part.ec_k,
            ec_m: part.ec_m,
            data_pg_id: 0,
            checksum: part.checksum.clone(),
        }];
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: upload_id.clone(),
                bucket_write_reservation: proof.clone(),
                object: PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id,
                    owner: request.owner.clone(),
                    acl_grants: request.acl_grants.clone(),
                    public_read: request.public_read,
                    generation_id: request.generation_id,
                    size: request.size,
                    etag: ObjectEtag::MultipartComposite {
                        crc64: request.etag_crc64,
                        parts: parts_count,
                    },
                    ec: EcShape { k: 0, m: 0 },
                    layout: ObjectLayout::MultipartManifest { parts_count },
                    tags: request.tags.clone(),
                    metadata_blob: request.metadata_blob.clone(),
                    system_metadata_blob: request.system_metadata_blob.clone(),
                    object_lock: request.object_lock,
                    encryption: request.encryption.clone(),
                },
                parts: expected_object_parts.clone(),
                selected_streaming_segments: Vec::new(),
                omitted_parts: Vec::new(),
                omitted_streaming_segments: Vec::new(),
                stream_uploads: Vec::new(),
                stream_upload_segments: Vec::new(),
                write_sequence: 1,
                completion_order: 2,
                completed_at_millis: 3,
                initiator: None,
                last_modified_millis: 3,
                stale_payload: None,
            })),
        );
        let build = BuildCompleteMultipartObjectCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &request,
            version_id,
            expected_object_parts: &expected_object_parts,
            completion_order: 2,
            bucket_write_reservation: &proof,
        };
        client
            .validate_complete_multipart_command_response(&command, &build)
            .unwrap();

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(bad_commit) = &mut bad_payload else {
            panic!("expected complete multipart command");
        };
        bad_commit.parts[0].key = crate::tests::object_key("wrong-complete-mpu-key");
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_complete_multipart_command_response(&bad_command, &build)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate complete multipart command build response",
                ..
            })
        ));

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(bad_commit) = &mut bad_payload else {
            panic!("expected complete multipart command");
        };
        bad_commit.parts[0].data_pg_id = 424_242;
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_complete_multipart_command_response(&bad_command, &build)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate complete multipart command build response",
                ..
            })
        ));

        let mut missing_cleanup_request = request.clone();
        missing_cleanup_request.expected_cleanup.omitted_parts = vec![MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 2,
            generation: 1,
            size: 9,
            etag: vec![2; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [7; 16],
            part_vid: GenerationId::new(41).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 11,
            checksum: None,
        }];
        let missing_cleanup_build = BuildCompleteMultipartObjectCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &missing_cleanup_request,
            version_id,
            expected_object_parts: &expected_object_parts,
            completion_order: 2,
            bucket_write_reservation: &proof,
        };
        let err = client
            .validate_complete_multipart_command_response(&command, &missing_cleanup_build)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate complete multipart command build response",
                ..
            })
        ));

        let mut bad_cleanup_payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(bad_cleanup) = &mut bad_cleanup_payload
        else {
            panic!("expected complete multipart command");
        };
        bad_cleanup.omitted_parts.push(part.clone());
        let bad_cleanup_command = MetadataCommandEnvelope::new(command.id(), bad_cleanup_payload);
        let err = client
            .validate_complete_multipart_command_response(&bad_cleanup_command, &build)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate complete multipart command build response",
                ..
            })
        ));

        let stale_source_generation = GenerationId::new(50).unwrap();
        let mut null_request = request.clone();
        null_request.versioning = BucketVersioningState::Suspended;
        null_request.expected_stale_payload_source = Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            stale_source_generation,
            ObjectLayout::Standard,
        ));
        let mut null_expected_object_parts = expected_object_parts.clone();
        for part in &mut null_expected_object_parts {
            part.version_id = VersionId::Null;
        }
        let null_build = BuildCompleteMultipartObjectCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &null_request,
            version_id: VersionId::Null,
            expected_object_parts: &null_expected_object_parts,
            completion_order: 2,
            bucket_write_reservation: &proof,
        };
        let mut stale_payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(stale_commit) = &mut stale_payload else {
            panic!("expected complete multipart command");
        };
        stale_commit.object.version_id = VersionId::Null;
        for part in &mut stale_commit.parts {
            part.version_id = VersionId::Null;
        }
        stale_commit.stale_payload = Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            GenerationId::new(51).unwrap(),
        ));
        let stale_command = MetadataCommandEnvelope::new(command.id(), stale_payload);
        let err = client
            .validate_complete_multipart_command_response(&stale_command, &null_build)
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate complete multipart command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_object_mutation_client_rejects_malformed_abort_multipart_response() {
        let tmp = test_util::tempdir();
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            tmp.path().join("unused.sock"),
        );
        let bucket = crate::tests::bucket_name("abort-mpu-rpc-bucket");
        let key = crate::tests::object_key("abort-mpu-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("abort-mpu-rpc-upload");
        let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        let upload = MultipartUploadRecord {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: 1,
            state: UploadState::InProgress,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_generation_id: GenerationId::new(30).unwrap(),
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        let cleanup = crate::types::AbortMultipartUploadCleanup {
            upload,
            parts: Vec::new(),
            streaming_segments: Vec::new(),
            stream_uploads: Vec::new(),
            stream_upload_segments: Vec::new(),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                cleanup: cleanup.clone(),
                bucket_write_reservation: proof.clone(),
            })),
        );
        client
            .validate_abort_multipart_command_response(
                &command,
                &AbortMultipartCommandValidation {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    upload_id: &upload_id,
                    expected_cleanup: Some(&cleanup),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap();

        let mut bad_payload = command.payload().clone();
        let MetadataCommandPayload::AbortMultipartUpload(bad_abort) = &mut bad_payload else {
            panic!("expected abort multipart command");
        };
        bad_abort.upload_id = crate::tests::multipart_upload_id("abort-mpu-rpc-wrong");
        let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
        let err = client
            .validate_abort_multipart_command_response(
                &bad_command,
                &AbortMultipartCommandValidation {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    upload_id: &upload_id,
                    expected_cleanup: Some(&cleanup),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate abort multipart command build response",
                ..
            })
        ));

        let mut bad_cleanup_payload = command.payload().clone();
        let MetadataCommandPayload::AbortMultipartUpload(bad_cleanup) = &mut bad_cleanup_payload
        else {
            panic!("expected abort multipart command");
        };
        bad_cleanup.cleanup.parts.push(MultipartPartRecord {
            upload_id: crate::tests::multipart_upload_id("abort-mpu-rpc-other"),
            part_number: 1,
            generation: 1,
            size: 12,
            etag: vec![1; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [6; 16],
            part_vid: GenerationId::new(40).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 10,
            checksum: None,
        });
        let bad_cleanup_command = MetadataCommandEnvelope::new(command.id(), bad_cleanup_payload);
        let err = client
            .validate_abort_multipart_command_response(
                &bad_cleanup_command,
                &AbortMultipartCommandValidation {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    upload_id: &upload_id,
                    expected_cleanup: Some(&cleanup),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate abort multipart command build response",
                ..
            })
        ));

        let mut expected_missing_cleanup = cleanup.clone();
        expected_missing_cleanup.parts.push(MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 1,
            size: 12,
            etag: vec![1; 8],
            etag_kind: EtagKind::Crc64,
            part_okh: [6; 16],
            part_vid: GenerationId::new(40).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 10,
            checksum: None,
        });
        let err = client
            .validate_abort_multipart_command_response(
                &command,
                &AbortMultipartCommandValidation {
                    pg_id: PgId::new(0),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    bucket: &bucket,
                    key: &key,
                    upload_id: &upload_id,
                    expected_cleanup: Some(&expected_missing_cleanup),
                    bucket_write_reservation: &proof,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "validate abort multipart command build response",
                ..
            })
        ));
    }

    #[test]
    fn unix_storage_node_client_writes_deletes_and_validates_ack_rows() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..5 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let key = ShardKey::new(&[0x55; 16], 11, 0);
        let data_pg_id = DataPgId::new(PgId::new(0));

        assert_eq!(client.node_id(), NodeId::new(7));
        let ack = client
            .write_placed_shard(data_pg_id, &key, b"remote payload")
            .unwrap();
        let read_back = client.read_placed_shard(data_pg_id, &key, ack).unwrap();
        assert_eq!(read_back, b"remote payload");
        client
            .register_written_shard_acks(PgId::new(0), &[(&key, ack)])
            .unwrap();
        client
            .validate_written_shard_acks(PgId::new(0), &[(&key, ack)])
            .unwrap();
        client.delete_placed_shard(data_pg_id, &key).unwrap();
        server_thread.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&key, ack).unwrap();
    }

    #[test]
    fn unix_storage_node_client_reads_metadata_command_state_and_acceptance() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let first = test_metadata_command(0, 1);
        let applied = test_metadata_command(0, 2);
        let pending = test_metadata_command(0, 3);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let applied_hashes;
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            pg.record_metadata_command_abandoned(7, &first).unwrap();
            pg.apply_metadata_command_and_record(7, &applied).unwrap();
            applied_hashes = pg
                .applied_metadata_command_log_entry_hashes(7, &applied)
                .unwrap()
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &pending, Some(&bucket))
                .unwrap();
        }
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..9 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );

        let state =
            MetadataCommandNodeClient::metadata_command_replica_state(&client, PgId::new(0))
                .unwrap();
        let max_log_index = MetadataCommandNodeClient::max_metadata_command_log_index(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();
        let pending_read = MetadataCommandNodeClient::pending_metadata_command_envelope(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap()
        .unwrap();
        let replay_state =
            MetadataCommandNodeClient::validate_metadata_command_replay_state_preserving_pending_slot(
                &client,
                PgId::new(0),
                ClusterEpoch::new(1).unwrap(),
            )
            .unwrap();
        let remote_hashes = MetadataCommandNodeClient::applied_metadata_command_log_entry_hashes(
            &client,
            PgId::new(0),
            &applied,
        )
        .unwrap();
        let matching = MetadataCommandNodeClient::has_matching_applied_metadata_command_log_entry(
            &client,
            PgId::new(0),
            &applied,
            applied_hashes.0,
        )
        .unwrap();
        let abandoned =
            MetadataCommandNodeClient::metadata_command_abandoned(&client, PgId::new(0), &first)
                .unwrap();
        let next_conflict = MetadataCommandNodeClient::next_metadata_command_id_at_least(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
            MetadataCommandLogIndex::new(5).unwrap(),
        )
        .unwrap_err();
        let acceptance =
            MetadataCommandNodeClient::metadata_command_acceptance(&client, PgId::new(0), &applied)
                .unwrap();

        assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(max_log_index, 2);
        assert_eq!(pending_read.command_bytes(), pending.command_bytes());
        assert_eq!(replay_state.applied_log_index, 2);
        assert_eq!(remote_hashes, Some(applied_hashes));
        assert!(matching);
        assert!(abandoned);
        assert!(matches!(
            next_conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 3,
                ..
            }
        ));
        assert_eq!(acceptance, MetadataCommandAcceptance::AlreadyApplied);
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_storage_node_client_applies_metadata_command_idempotently() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..2 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let command = test_metadata_command(0, 1);

        let applied = MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap();
        let retried = MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap();
        server_thread.join().unwrap();

        assert_eq!(applied.applied_log_index, 1);
        assert_eq!(retried.applied_log_index, 1);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let state = reopened
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state.applied_log_index, 1);
    }

    #[test]
    fn unix_storage_node_client_inserts_pending_metadata_command_slot_idempotently() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..5 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let command = test_metadata_command(0, 1);
        let replacement = test_metadata_command(0, 2);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");

        MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command,
            Some(&bucket),
        )
        .unwrap();
        MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command,
            Some(&bucket),
        )
        .unwrap();
        assert!(
            MetadataCommandNodeClient::replace_pending_metadata_command_slot_for_reissue(
                &client,
                PgId::new(0),
                &command,
                &replacement,
                Some(&bucket),
            )
            .unwrap()
        );
        assert!(
            MetadataCommandNodeClient::replace_pending_metadata_command_slot_for_reissue(
                &client,
                PgId::new(0),
                &command,
                &replacement,
                Some(&bucket),
            )
            .unwrap(),
            "replacing after a lost response should be idempotent"
        );
        let conflict = MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &test_metadata_command(0, 3),
            Some(&bucket),
        )
        .unwrap_err();
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandPendingConflict {
                pg_id: 0,
                existing_log_index: 2,
                candidate_log_index: 3,
                ..
            }
        ));
        server_thread.join().unwrap();

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
        assert_eq!(pending.command_bytes(), replacement.command_bytes());
    }

    #[test]
    fn unix_storage_node_client_inserts_bucket_control_pending_slot_idempotently() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..2 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let command = test_metadata_command(0, 1);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");

        assert!(
            MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &command,
                &bucket,
            )
            .unwrap()
        );
        assert!(
            MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &command,
                &bucket,
            )
            .unwrap(),
            "retrying after a lost response should observe the existing exact slot"
        );
        server_thread.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pending = reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_bytes, command.command_bytes());
        assert_eq!(pending.scope_bucket.as_ref(), Some(&bucket));
    }

    #[test]
    fn unix_storage_node_client_removes_pending_metadata_command_slot_idempotently() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
            pg.record_metadata_command_abandoned(7, &command).unwrap();
        }
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..2 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );

        assert!(
            MetadataCommandNodeClient::remove_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &command
            )
            .unwrap()
        );
        assert!(
            !MetadataCommandNodeClient::remove_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &command
            )
            .unwrap()
        );
        server_thread.join().unwrap();

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
    fn unix_storage_node_client_records_abandoned_metadata_command_idempotently() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..2 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let command = test_metadata_command(0, 1);

        let first = MetadataCommandNodeClient::record_metadata_command_abandoned(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap();
        let second = MetadataCommandNodeClient::record_metadata_command_abandoned(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap();
        server_thread.join().unwrap();

        assert_eq!(first, second);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(reopened
            .get_pg(0)
            .unwrap()
            .metadata_command_abandoned(7, &command)
            .unwrap());
    }

    #[test]
    fn unix_storage_node_client_rejects_mismatched_next_id_conflict_response() {
        fn next_id_error_from_fake_response(
            outcome: StorageRpcMetadataCommandNextIdOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_next_id_response(
                    &StorageRpcMetadataCommandNextIdResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err = MetadataCommandNodeClient::next_metadata_command_id_at_least(
                &client,
                PgId::new(0),
                ClusterEpoch::new(1).unwrap(),
                MetadataCommandLogIndex::new(1).unwrap(),
            )
            .unwrap_err();

            join.join().unwrap();
            err
        }

        let wrong_route =
            next_id_error_from_fake_response(StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            });
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command next id response",
                ..
            }
        ));

        let zero_index =
            next_id_error_from_fake_response(StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            });
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command next id response",
                ..
            }
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_pending_slot_log_conflict() {
        fn pending_insert_error_from_fake_response(
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload =
                    crate::storage_rpc::encode_metadata_command_pending_slot_insert_response(
                        &crate::storage_rpc::StorageRpcMetadataCommandPendingSlotInsertResponse {
                            outcome,
                        },
                    );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err = MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &test_metadata_command(0, 1),
                Some(&crate::tests::bucket_name("metadata-rpc-bucket")),
            )
            .unwrap_err();

            join.join().unwrap();
            err
        }

        let conflict = pending_insert_error_from_fake_response(
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            }
        ));

        let wrong_route = pending_insert_error_from_fake_response(
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command pending slot insert response",
                ..
            }
        ));

        let zero_index = pending_insert_error_from_fake_response(
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        );
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command pending slot insert response",
                ..
            }
        ));
    }

    fn metadata_command_session_result_from_fake_response<R>(
        target_payload: Vec<u8>,
        call: impl FnOnce(Box<dyn MetadataCommandNodeClient>) -> R,
    ) -> R {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let acquire = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(
                acquire.kind,
                StorageRpcMessageKind::MetadataCommandPgLockAcquire
            );
            let acquire_response = StorageRpcFrame {
                request_id: acquire.request_id,
                kind: acquire.kind,
                payload: encode_storage_rpc_success_response(&[]),
            };
            write_storage_rpc_frame_to(&mut stream, &acquire_response).unwrap();

            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&target_payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();

            let release = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(
                release.kind,
                StorageRpcMessageKind::MetadataCommandPgLockRelease
            );
            let release_response = StorageRpcFrame {
                request_id: release.request_id,
                kind: release.kind,
                payload: encode_storage_rpc_success_response(&[]),
            };
            write_storage_rpc_frame_to(&mut stream, &release_response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
        let session = MetadataCommandNodeClient::open_metadata_command_critical_section(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();

        let result = call(session);
        join.join().unwrap();
        result
    }

    #[test]
    fn unix_storage_node_session_rejects_malformed_log_conflicts() {
        let command = test_metadata_command(0, 1);

        let pending_payload = encode_metadata_command_pending_slot_insert_response(
            &StorageRpcMetadataCommandPendingSlotInsertResponse {
                outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            },
        );
        let pending_error =
            metadata_command_session_result_from_fake_response(pending_payload, |session| {
                session
                    .try_insert_pending_metadata_command_slot(
                        PgId::new(0),
                        &command,
                        Some(&crate::tests::bucket_name("metadata-rpc-bucket")),
                    )
                    .unwrap_err()
            });
        assert!(matches!(
            pending_error,
            StoreError::StorageRpc {
                operation: "decode metadata command pending slot insert response",
                ..
            }
        ));

        let bucket_control_payload = encode_metadata_command_bool_outcome_response(
            &StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            },
        );
        let bucket_control_error =
            metadata_command_session_result_from_fake_response(bucket_control_payload, |session| {
                session
                    .try_insert_bucket_control_pending_metadata_command_slot(
                        PgId::new(0),
                        &command,
                        &crate::tests::bucket_name("metadata-rpc-bucket"),
                    )
                    .unwrap_err()
            });
        assert!(matches!(
            bucket_control_error,
            StoreError::StorageRpc {
                operation: "decode metadata command bucket-control pending slot insert response",
                ..
            }
        ));

        let acceptance_payload = encode_metadata_command_acceptance_response(
            &StorageRpcMetadataCommandAcceptanceResponse {
                outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 0,
                },
            },
        );
        let acceptance_error =
            metadata_command_session_result_from_fake_response(acceptance_payload, |session| {
                session
                    .metadata_command_acceptance(PgId::new(0), &command)
                    .unwrap_err()
            });
        assert!(matches!(
            acceptance_error,
            StoreError::StorageRpc {
                operation: "decode metadata command acceptance response",
                ..
            }
        ));

        let hashes_payload = encode_metadata_command_applied_hashes_response(
            &StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            },
        );
        let hashes_error =
            metadata_command_session_result_from_fake_response(hashes_payload, |session| {
                session
                    .applied_metadata_command_log_entry_hashes(PgId::new(0), &command)
                    .unwrap_err()
            });
        assert!(matches!(
            hashes_error,
            StoreError::StorageRpc {
                operation: "decode metadata command applied hashes response",
                ..
            }
        ));

        let apply_payload = encode_metadata_command_state_outcome_response(
            &StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            },
        );
        let apply_error =
            metadata_command_session_result_from_fake_response(apply_payload, |session| {
                session
                    .apply_metadata_command_and_record(PgId::new(0), &command)
                    .unwrap_err()
            });
        assert!(matches!(
            apply_error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let abandoned_payload = encode_metadata_command_state_outcome_response(
            &StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 0,
                },
            },
        );
        let abandoned_error =
            metadata_command_session_result_from_fake_response(abandoned_payload, |session| {
                session
                    .record_metadata_command_abandoned(PgId::new(0), &command)
                    .unwrap_err()
            });
        assert!(matches!(
            abandoned_error,
            StoreError::StorageRpc {
                operation: "decode metadata command record abandoned response",
                ..
            }
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_applied_hash_log_conflict() {
        fn applied_hashes_error_from_fake_response(
            outcome: StorageRpcMetadataCommandAppliedHashesOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_applied_hashes_response(
                    &StorageRpcMetadataCommandAppliedHashesResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err = MetadataCommandNodeClient::applied_metadata_command_log_entry_hashes(
                &client,
                PgId::new(0),
                &test_metadata_command(0, 1),
            )
            .unwrap_err();

            join.join().unwrap();
            err
        }

        let conflict = applied_hashes_error_from_fake_response(
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            }
        ));

        let wrong_route = applied_hashes_error_from_fake_response(
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command applied hashes response",
                ..
            }
        ));

        let zero_index = applied_hashes_error_from_fake_response(
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        );
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command applied hashes response",
                ..
            }
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_record_abandoned_log_conflict() {
        fn record_abandoned_error_from_fake_response(
            outcome: StorageRpcMetadataCommandStateOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_state_outcome_response(
                    &StorageRpcMetadataCommandStateOutcomeResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err = MetadataCommandNodeClient::record_metadata_command_abandoned(
                &client,
                PgId::new(0),
                &test_metadata_command(0, 1),
            )
            .unwrap_err();

            join.join().unwrap();
            err
        }

        let conflict = record_abandoned_error_from_fake_response(
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            }
        ));

        let wrong_route = record_abandoned_error_from_fake_response(
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command record abandoned response",
                ..
            }
        ));

        let zero_index = record_abandoned_error_from_fake_response(
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        );
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command record abandoned response",
                ..
            }
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_apply_metadata_command_log_conflict() {
        fn apply_error_from_fake_response(
            outcome: StorageRpcMetadataCommandStateOutcome,
        ) -> BucketSnapshotLoadError {
            apply_error_from_fake_response_for_command(outcome, test_metadata_command(0, 1))
        }

        fn apply_error_from_fake_response_for_command(
            outcome: StorageRpcMetadataCommandStateOutcome,
            command: MetadataCommandEnvelope,
        ) -> BucketSnapshotLoadError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_state_outcome_response(
                    &StorageRpcMetadataCommandStateOutcomeResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err = MetadataCommandNodeClient::apply_metadata_command_and_record(
                &client,
                PgId::new(0),
                &command,
            )
            .unwrap_err();

            join.join().unwrap();
            err
        }

        let conflict =
            apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            });
        assert!(matches!(
            conflict,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            })
        ));

        let wrong_route =
            apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            });
        assert!(matches!(
            wrong_route,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let zero_index =
            apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            });
        assert!(matches!(
            zero_index,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let stale_version = apply_error_from_fake_response(
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id: VersionId::from_u64(7),
            },
        );
        assert!(matches!(
            stale_version,
            BucketSnapshotLoadError::Metadata(MetadataError::ObjectVersionReservationConflict {
                version_id
            }) if version_id == VersionId::from_u64(7)
        ));

        let bucket = crate::tests::bucket_name("stale-bucket-rpc");
        let stale_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Delete {
                    kind: BucketSubresourceKind::Cors,
                },
                11,
            )),
        );
        let stale_bucket = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name: bucket.clone(),
                bucket_execution_generation: 11,
            },
            stale_command.clone(),
        );
        assert!(matches!(
            stale_bucket,
            BucketSnapshotLoadError::Metadata(MetadataError::StaleBucketMetadataCommand {
                ref name,
                bucket_execution_generation: 11,
            }) if name == &bucket
        ));

        let stale_bucket_mismatch = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name: crate::tests::bucket_name("wrong-stale-bucket-rpc"),
                bucket_execution_generation: 11,
            },
            stale_command,
        );
        assert!(matches!(
            stale_bucket_mismatch,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let object_bucket = crate::tests::bucket_name("stale-object-rpc");
        let object_key = crate::tests::object_key("object");
        let stale_object_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: object_bucket.clone(),
                key: object_key.clone(),
                version_id: VersionId::from_u64(7),
                owner: crate::OwnerIdentity::from_principal("owner"),
                write_sequence: 3,
                last_modified_millis: 123,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(
                    object_bucket.clone(),
                    &object_key,
                ),
            }),
        );
        let stale_object = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket: object_bucket.clone(),
                key: object_key.clone(),
                write_sequence: 3,
                generation_id: None,
            },
            stale_object_command.clone(),
        );
        assert!(matches!(
            stale_object,
            BucketSnapshotLoadError::Metadata(MetadataError::StaleObjectWriteCommand {
                ref bucket,
                ref key,
                write_sequence: 3,
                generation_id: None,
            }) if bucket == &object_bucket && key == &object_key
        ));

        let stale_delete_marker_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: object_bucket.clone(),
                key: object_key.clone(),
                version_id: VersionId::Null,
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 5 },
                bucket_write_reservation: test_bucket_write_reservation_proof(
                    object_bucket.clone(),
                    &object_key,
                ),
            })),
        );
        let stale_delete_marker = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket: object_bucket.clone(),
                key: object_key.clone(),
                write_sequence: 5,
                generation_id: None,
            },
            stale_delete_marker_command.clone(),
        );
        assert!(matches!(
            stale_delete_marker,
            BucketSnapshotLoadError::Metadata(MetadataError::StaleObjectWriteCommand {
                ref bucket,
                ref key,
                write_sequence: 5,
                generation_id: None,
            }) if bucket == &object_bucket && key == &object_key
        ));

        let stale_delete_marker_generation_mismatch = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket: object_bucket.clone(),
                key: object_key.clone(),
                write_sequence: 5,
                generation_id: Some(GenerationId::MIN),
            },
            stale_delete_marker_command,
        );
        assert!(matches!(
            stale_delete_marker_generation_mismatch,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let stale_object_mismatch = apply_error_from_fake_response_for_command(
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket: object_bucket,
                key: object_key,
                write_sequence: 4,
                generation_id: None,
            },
            stale_object_command,
        );
        assert!(matches!(
            stale_object_mismatch,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));

        let impossible_stale_bucket = apply_error_from_fake_response(
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name: bucket,
                bucket_execution_generation: 11,
            },
        );
        assert!(matches!(
            impossible_stale_bucket,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                operation: "decode metadata command apply and record response",
                ..
            })
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_bucket_control_pending_slot_log_conflict() {
        fn bucket_control_error_from_fake_response(
            outcome: StorageRpcMetadataCommandBoolOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );

            let err =
                MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
                    &client,
                    PgId::new(0),
                    &test_metadata_command(0, 1),
                    &crate::tests::bucket_name("metadata-rpc-bucket"),
                )
                .unwrap_err();

            join.join().unwrap();
            err
        }

        let conflict = bucket_control_error_from_fake_response(
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            }
        ));

        let wrong_route = bucket_control_error_from_fake_response(
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command bucket-control pending slot insert response",
                ..
            }
        ));

        let zero_index = bucket_control_error_from_fake_response(
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        );
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command bucket-control pending slot insert response",
                ..
            }
        ));
    }

    #[test]
    fn unix_storage_node_client_preserves_bool_metadata_command_log_conflicts() {
        fn bool_metadata_error_from_fake_response(
            kind: StorageRpcMessageKind,
            outcome: StorageRpcMetadataCommandBoolOutcome,
        ) -> StoreError {
            let tmp = test_util::tempdir();
            let socket_path = tmp.path().join("sock").join("storage.sock");
            private_socket_dir(socket_path.parent().unwrap());
            let listener = UnixListener::bind(&socket_path).unwrap();
            let join = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_storage_rpc_frame_from(&mut stream).unwrap();
                let payload = encode_metadata_command_bool_outcome_response(
                    &StorageRpcMetadataCommandBoolOutcomeResponse { outcome },
                );
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                write_storage_rpc_frame_to(&mut stream, &response).unwrap();
            });
            let client = UnixStorageNodeClient::new(
                NodeId::new(7),
                ClusterEpoch::new(1).unwrap(),
                socket_path,
            );
            let command = test_metadata_command(0, 1);

            let err = match kind {
                StorageRpcMessageKind::MetadataCommandMatchingAppliedLog => {
                    MetadataCommandNodeClient::has_matching_applied_metadata_command_log_entry(
                        &client,
                        PgId::new(0),
                        &command,
                        0,
                    )
                    .unwrap_err()
                }
                StorageRpcMessageKind::MetadataCommandAbandoned => {
                    MetadataCommandNodeClient::metadata_command_abandoned(
                        &client,
                        PgId::new(0),
                        &command,
                    )
                    .unwrap_err()
                }
                _ => panic!("unsupported bool metadata command test kind"),
            };

            join.join().unwrap();
            err
        }

        for kind in [
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            StorageRpcMessageKind::MetadataCommandAbandoned,
        ] {
            let conflict = bool_metadata_error_from_fake_response(
                kind,
                StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            );
            assert!(matches!(
                conflict,
                StoreError::MetadataCommandLogConflict {
                    pg_id: 0,
                    log_index: 1,
                    ..
                }
            ));

            let wrong_route = bool_metadata_error_from_fake_response(
                kind,
                StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 1,
                },
            );
            assert!(matches!(
                wrong_route,
                StoreError::StorageRpc {
                    operation: "decode metadata command matching applied response"
                        | "decode metadata command abandoned response",
                    ..
                }
            ));

            let zero_index = bool_metadata_error_from_fake_response(
                kind,
                StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    log_index: 0,
                },
            );
            assert!(matches!(
                zero_index,
                StoreError::StorageRpc {
                    operation: "decode metadata command matching applied response"
                        | "decode metadata command abandoned response",
                    ..
                }
            ));
        }
    }

    #[test]
    fn unix_storage_node_client_read_into_requires_full_shard_buffer() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let server_thread = thread::spawn(move || {
            for _ in 0..2 {
                server.accept_one().unwrap();
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let key = ShardKey::new(&[0x56; 16], 12, 0);
        let data_pg_id = DataPgId::new(PgId::new(0));
        let ack = client
            .write_placed_shard(data_pg_id, &key, b"remote payload")
            .unwrap();

        let mut short = vec![0; ack.stored_size as usize - 1];
        let err = PlacedShardNodeClient::read_placed_shard_into(
            &client, data_pg_id, &key, ack, &mut short,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StorageRpc {
                operation: "shard read range",
                ref message,
                ..
            } if message.contains("expected")
        ));
        let ranged = client
            .read_placed_shard_range(data_pg_id, &key, ack, 0, short.len() as u64)
            .unwrap();
        assert_eq!(ranged, b"remote payloa");
        drop(client);
        server_thread.join().unwrap();
    }

    #[test]
    fn unix_storage_node_read_handle_session_is_idempotent_and_disconnect_releases() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let server_thread = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let location = crate::cluster::ShardLocation::new(
            config.cluster_epoch,
            DataPgId::new(PgId::new(0)),
            crate::ShardIndex::new(0),
            config.node_id,
        );
        let key = ShardKey::new(&[0x55; 16], 55, 0);
        let mut session = client.open_read_handle_session().unwrap();

        assert_eq!(
            session
                .acquire_read_handles("read-op", vec![(location, key.clone())])
                .unwrap(),
            vec![location]
        );
        assert_eq!(
            session
                .acquire_read_handles("read-op", vec![(location, key.clone())])
                .unwrap(),
            vec![location]
        );
        assert_eq!(server.read_handle_count(location), 1);
        session.release_read_handles("read-op").unwrap();
        session.release_read_handles("read-op").unwrap();
        assert_eq!(server.read_handle_count(location), 0);
        session
            .acquire_read_handles("read-op-disconnect", vec![(location, key)])
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);
        drop(session);
        server_thread.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn unix_storage_node_delete_fails_while_read_handle_active() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..4)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one().unwrap())
            })
            .collect();
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let key = ShardKey::new(&[0x66; 16], 12, 0);
        let data_pg_id = DataPgId::new(PgId::new(0));
        let location = crate::cluster::ShardLocation::new(
            config.cluster_epoch,
            data_pg_id,
            key.shard_index(),
            config.node_id,
        );
        let mut session = client.open_read_handle_session().unwrap();

        client
            .write_placed_shard(data_pg_id, &key, b"protected payload")
            .unwrap();
        session
            .acquire_read_handles("protected-read", vec![(location, key.clone())])
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let err = client.delete_placed_shard(data_pg_id, &key).unwrap_err();
        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                operation: "shard delete",
                ref message,
                ..
            } if message.contains("active read handles")
        ));

        session.release_read_handles("protected-read").unwrap();
        assert_eq!(server.read_handle_count(location), 0);
        client.delete_placed_shard(data_pg_id, &key).unwrap();
        drop(session);
        for join in server_threads {
            join.join().unwrap();
        }

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn unix_storage_node_rpc_admission_exhaustion_is_typed_before_connect() {
        let client =
            test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
        let _held = client
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();
        let before = observability::metrics_snapshot();

        let err = client
            .rpc_request_result(StorageRpcMessageKind::ShardWrite, Vec::new())
            .unwrap_err();
        let after = observability::metrics_snapshot();
        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "shard write",
                ref message,
            } if message.contains("admission limit 1")
        ));
        assert!(after.storage_rpc_admission_total > before.storage_rpc_admission_total);
        assert!(
            after.storage_rpc_admission_timeout_total > before.storage_rpc_admission_timeout_total
        );
    }

    #[test]
    fn unix_storage_node_rpc_admission_wait_metric_records_released_capacity() {
        let client = Arc::new(test_unix_storage_node_client_with_rpc_admission_timeout(
            1,
            Duration::from_secs(1),
        ));
        let held = client
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();
        let before = observability::metrics_snapshot();
        let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
        let client_for_thread = Arc::clone(&client);
        let join = thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            client_for_thread.acquire_rpc_admission(StorageRpcMessageKind::ShardWrite)
        });

        attempt_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(50));
        drop(held);
        let permit = join.join().unwrap().unwrap();
        drop(permit);
        let after = observability::metrics_snapshot();

        assert!(after.storage_rpc_admission_total > before.storage_rpc_admission_total);
        assert!(after.storage_rpc_admission_wait_total > before.storage_rpc_admission_wait_total);
        assert!(
            after.storage_rpc_admission_wait_us_total > before.storage_rpc_admission_wait_us_total
        );
    }

    #[test]
    fn unix_storage_node_rpc_admission_is_shared_by_node_and_socket() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("missing.sock");
        let rpc_admission = shared_unix_storage_node_rpc_admission_with_wait_timeout(
            NodeId::new(7),
            &socket_path,
            1,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        let client_a = UnixStorageNodeClient::with_rpc_admission(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            socket_path.clone(),
            Arc::clone(&rpc_admission),
        );
        let client_b =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
        let _held = client_a
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();

        let err = client_b
            .rpc_request_result(StorageRpcMessageKind::ShardWrite, Vec::new())
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "shard write",
                ref message,
            } if message.contains("admission limit")
        ));
    }

    #[test]
    fn unix_storage_node_shard_write_admission_exhausts_before_socket_write() {
        let client =
            test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
        let _held = client
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();
        let key = ShardKey::new(&[0x55; 16], 55, 0);
        let err = client
            .write_placed_shard(DataPgId::new(PgId::new(0)), &key, &[0x5a; 4096])
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "shard write",
                ref message,
            } if message.contains("admission limit 1")
        ));
    }

    #[test]
    fn unix_storage_node_read_handle_session_admission_exhausts_before_connect() {
        let client =
            test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
        let _held = client
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();
        let err = match client.open_read_handle_session() {
            Ok(_) => panic!("read-handle session admission unexpectedly succeeded"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "read handles acquire",
                ref message,
            } if message.contains("admission limit 1")
        ));
    }

    #[test]
    fn unix_storage_node_metadata_session_admission_exhausts_before_connect() {
        let client =
            test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
        let _held = client
            .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
            .unwrap();
        let err = match client.open_metadata_command_critical_section(PgId::new(0)) {
            Ok(_) => panic!("metadata-command session admission unexpectedly succeeded"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "metadata command PG lock acquire",
                ref message,
            } if message.contains("admission limit 1")
        ));
    }

    #[test]
    fn unix_storage_node_read_handle_session_rejects_mismatched_acquire_response() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let mismatched_location = crate::cluster::ShardLocation::new(
                ClusterEpoch::new(1).unwrap(),
                DataPgId::new(PgId::new(0)),
                crate::ShardIndex::new(1),
                NodeId::new(7),
            );
            let payload =
                encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                    locations: vec![mismatched_location],
                })
                .unwrap();
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
        let requested_location = crate::cluster::ShardLocation::new(
            ClusterEpoch::new(1).unwrap(),
            DataPgId::new(PgId::new(0)),
            crate::ShardIndex::new(0),
            NodeId::new(7),
        );
        let mut session = client.open_read_handle_session().unwrap();

        let err = session
            .acquire_read_handles(
                "read-op",
                vec![(
                    requested_location,
                    ShardKey::new(&[0x77; 16], 77, requested_location.shard_index().get()),
                )],
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::StorageRpc {
                operation: "validate read handle acquire response",
                ..
            }
        ));
        drop(session);
        join.join().unwrap();
    }
}
