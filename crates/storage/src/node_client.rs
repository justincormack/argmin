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

mod interface;
mod local;
mod unix_admission;
mod unix_helpers;
mod unix_object_rpc;
mod unix_rpc;
mod unix_sessions;

pub(crate) use interface::*;

#[cfg(test)]
use unix_object_rpc::{
    validate_list_multipart_uploads_response, validate_list_object_versions_response,
    validate_list_objects_response,
};

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
