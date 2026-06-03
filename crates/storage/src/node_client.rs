use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    decode_abort_multipart_cleanup_response, decode_bucket_info_outcome_response,
    decode_bucket_snapshot_pair_response, decode_bucket_snapshot_response,
    decode_bucket_write_reservation_record_response,
    decode_completed_multipart_order_command_build_response,
    decode_create_bucket_command_build_response, decode_direct_put_command_build_response,
    decode_direct_put_commit_snapshot_response, decode_metadata_command_acceptance_response,
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
    decode_object_read_auth_subject_response, decode_object_read_snapshot_response,
    decode_object_tags_for_subject_response, decode_object_version_response,
    decode_put_object_metadata_snapshot_response, decode_read_handle_acquire_response,
    decode_read_handle_release_response, decode_scavenger_list_files_response,
    decode_shard_read_range_response, decode_shard_read_response, decode_shard_write_ack,
    decode_storage_rpc_response_payload, decode_stream_part_finalize_snapshot_response,
    decode_stream_put_finalize_snapshot_response, decode_stream_segment_append_prepare_response,
    decode_stream_upload_match_response, decode_stream_upload_segments_response,
    decode_stream_upload_session_response, encode_abort_multipart_cleanup_request,
    encode_abort_multipart_command_build_request,
    encode_authorized_abort_multipart_command_build_request, encode_bucket_request,
    encode_bucket_snapshot_pair_request, encode_bucket_snapshot_request,
    encode_bucket_write_reservation_acquire_request, encode_bucket_write_reservation_proof_request,
    encode_bucket_write_reservation_record_request,
    encode_complete_multipart_command_build_request,
    encode_completed_multipart_order_command_build_request,
    encode_create_bucket_command_build_request,
    encode_create_multipart_upload_command_build_request,
    encode_create_stream_upload_command_build_request,
    encode_delete_current_object_command_build_request,
    encode_delete_specific_object_command_build_request, encode_direct_put_command_build_request,
    encode_direct_put_commit_snapshot_request, encode_insert_delete_marker_command_build_request,
    encode_metadata_command_matching_applied_request, encode_metadata_command_next_id_request,
    encode_metadata_command_pending_slot_replace_request,
    encode_metadata_command_pending_slot_request, encode_metadata_command_request,
    encode_metadata_command_state_request, encode_multipart_completion_preflight_request,
    encode_multipart_completion_snapshot_request, encode_multipart_parts_list_request,
    encode_multipart_upload_load_request, encode_multipart_upload_match_request,
    encode_object_delete_snapshot_request, encode_object_generation_reservation_request,
    encode_object_read_auth_subject_request, encode_object_read_snapshot_request,
    encode_object_request, encode_object_tags_for_subject_request, encode_proof_release_request,
    encode_put_object_metadata_command_build_request, encode_put_object_metadata_snapshot_request,
    encode_read_handle_acquire_request, encode_read_handle_release_request,
    encode_scavenger_list_files_request, encode_shard_ack_batch_request,
    encode_shard_delete_request, encode_shard_read_range_request, encode_shard_read_request,
    encode_shard_write_request, encode_stream_part_commit_command_build_request,
    encode_stream_part_finalize_snapshot_request, encode_stream_put_commit_command_build_request,
    encode_stream_put_finalize_snapshot_request, encode_stream_segment_append_prepare_request,
    encode_stream_upload_match_request, encode_stream_upload_session_request,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    StorageRpcAbortMultipartCleanupRequest, StorageRpcAbortMultipartCommandBuildRequest,
    StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcBucketInfoOutcome,
    StorageRpcBucketRequest, StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotPairOutcome,
    StorageRpcBucketSnapshotPairRequest, StorageRpcBucketSnapshotRequest,
    StorageRpcBucketWriteReservationAcquireOutcome, StorageRpcBucketWriteReservationAcquireRequest,
    StorageRpcBucketWriteReservationProofRequest, StorageRpcBucketWriteReservationRecordRequest,
    StorageRpcCompleteMultipartCommandBuildRequest,
    StorageRpcCompletedMultipartOrderCommandBuildRequest,
    StorageRpcCreateBucketCommandBuildOutcome, StorageRpcCreateBucketCommandBuildRequest,
    StorageRpcCreateBucketConfig, StorageRpcCreateMultipartUploadCommandBuildRequest,
    StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcCreateStreamUploadPrecondition,
    StorageRpcDeleteCurrentObjectCommandBuildRequest,
    StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcDirectPutCommandBuildOutcome,
    StorageRpcDirectPutCommandBuildRequest, StorageRpcDirectPutCommitSnapshotRequest,
    StorageRpcErrorResponse, StorageRpcFrame, StorageRpcInsertDeleteMarkerCommandBuildRequest,
    StorageRpcInsertDeleteMarkerStalePayload, StorageRpcMessageKind,
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
    StorageRpcObjectReadAuthSubjectOutcome, StorageRpcObjectReadAuthSubjectRequest,
    StorageRpcObjectReadSnapshotOutcome, StorageRpcObjectReadSnapshotRequest,
    StorageRpcObjectRequest, StorageRpcObjectTagsForSubjectOutcome,
    StorageRpcObjectTagsForSubjectRequest, StorageRpcProofReleaseRequest,
    StorageRpcPutObjectMetadataCommandBuildRequest, StorageRpcPutObjectMetadataSnapshotOutcome,
    StorageRpcPutObjectMetadataSnapshotRequest, StorageRpcReadHandleAcquireRequest,
    StorageRpcReadHandleReleaseRequest, StorageRpcScavengerListFilesRequest,
    StorageRpcShardAckBatchRequest, StorageRpcShardAckItem, StorageRpcShardDeleteRequest,
    StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest, StorageRpcShardWriteRequest,
    StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcStreamPartFinalizeSnapshotRequest,
    StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcStreamPutFinalizeSnapshotRequest,
    StorageRpcStreamSegmentAppendPrepareOutcome, StorageRpcStreamSegmentAppendPrepareRequest,
    StorageRpcStreamUploadMatchRequest, StorageRpcStreamUploadSegmentsOutcome,
    StorageRpcStreamUploadSessionOutcome, StorageRpcStreamUploadSessionRequest,
};
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::{
    AbortMultipartUploadCleanup, AuthorizedMultipartUploadRecord, BucketDeleteFinalizeClaimRecord,
    BucketDeleteFinalizeRoot, BucketFastPathIdentity, BucketInfo, BucketName, BucketSnapshot,
    BucketSnapshotPair, BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState,
    BucketSubresourceKind, BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch,
    CommitDirectPutObjectReq, CompleteMultipartCommitCleanup, CompleteMultipartCommitRequest,
    CompletedMultipartUploadRecord, CreateBucketConfig, CreateMultipartUploadReq,
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
    StreamUploadPartStorageSnapshot, StreamUploadRecord, StreamUploadSegmentRecord,
    StreamUploadState, StreamUploadTarget, TerminalStreamCleanupRecord, UploadId, UploadState,
    VersionId, WriteAck,
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
        Some(StoredObject::DeleteMarker(_)) => Ok(Some(DeleteObjectVersionTarget::DeleteMarker)),
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
}

pub(crate) trait BucketWriteReservationNodeClient: Send + Sync {
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
        locations: Vec<crate::cluster::ShardLocation>,
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
}

pub(crate) trait ShardScavengerNodeClient: Send + Sync {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError>;
}

pub(crate) trait MetadataCommandNodeClient: Send + Sync {
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

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

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

    fn list_all_stream_uploads(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError>;

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

    fn list_completed_multipart_upload_records_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<CompletedMultipartUploadRecord>, BucketSnapshotLoadError>;

    fn list_objects(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError>;

    fn list_object_versions(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError>;

    fn list_multipart_uploads(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError>;

    fn load_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<WriteAck, StoreError>;

    fn delete_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<(), StoreError>;

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
}

pub(crate) struct UnixStorageNodeReadHandleSession {
    node_id: NodeId,
    stream: UnixStream,
    next_request_id: u64,
}

struct UnixStorageNodeReadHandleLease {
    session: UnixStorageNodeReadHandleSession,
    read_operation_id: String,
    released: bool,
}

struct LocalStorageNodeReadHandleLease;

#[allow(dead_code)]
impl UnixStorageNodeClient {
    pub(crate) fn new(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            node_id,
            cluster_epoch,
            socket_path: socket_path.into(),
            next_request_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn open_read_handle_session(
        &self,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let stream = UnixStream::connect(&self.socket_path).map_err(|source| StoreError::Io {
            context: "connect storage-node read-handle RPC socket",
            source,
        })?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            stream,
            next_request_id: 1,
        })
    }

    pub(crate) fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        let expected_size = data.len() as u64;
        let expected_crc64 = checksum::crc64::checksum(data);
        let request = StorageRpcShardWriteRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_size,
            expected_crc64,
            payload: data.to_vec(),
        };
        let payload = encode_shard_write_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard write request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardWrite, payload)?;
        decode_shard_write_ack(&response, expected_size, expected_crc64).map_err(|error| {
            self.rpc_payload_error("decode shard write response", error.to_string())
        })
    }

    pub(crate) fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_ack,
        };
        let payload = encode_shard_read_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard read request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardRead, payload)?;
        decode_shard_read_response(&response, expected_ack).map_err(|error| {
            self.rpc_payload_error("decode shard read response", error.to_string())
        })
    }

    pub(crate) fn read_placed_shard_range(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRangeRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_ack,
            offset,
            length,
        };
        let payload = encode_shard_read_range_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard read range request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardReadRange, payload)?;
        decode_shard_read_range_response(&response, length as usize).map_err(|error| {
            self.rpc_payload_error("decode shard read range response", error.to_string())
        })
    }

    pub(crate) fn delete_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        let request = StorageRpcShardDeleteRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
        };
        let payload = encode_shard_delete_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard delete request", error.to_string())
        })?;
        self.rpc_request(StorageRpcMessageKind::ShardDelete, payload)
            .map(|_| ())
    }

    pub(crate) fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckRecord, payload)
            .map(|_| ())
    }

    pub(crate) fn validate_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckValidate, payload)
            .map(|_| ())
    }

    pub(crate) fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        let request = StorageRpcScavengerListFilesRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            data_pg_id,
        };
        let payload = encode_scavenger_list_files_request(&request);
        let response = self.rpc_request(StorageRpcMessageKind::ShardScavengerListFiles, payload)?;
        decode_scavenger_list_files_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode shard scavenger list files response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandReplicaState, payload)?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replica state response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn max_metadata_command_log_index(&self, pg_id: PgId) -> Result<u64, StoreError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandMaxLogIndex, payload)?;
        decode_metadata_command_max_log_index_response(&response)
            .map(|response| response.max_log_index)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command max log index response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
        )?;
        let response =
            decode_metadata_command_pending_envelope_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    error.to_string(),
                )
            })?;
        if let Some(command) = response.command.as_ref() {
            if command.id().cluster_epoch() != self.cluster_epoch || command.id().pg_id() != pg_id {
                return Err(self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    "metadata command pending envelope route mismatch".to_string(),
                ));
            }
        }
        Ok(response.command)
    }

    pub(crate) fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            min_log_index: min_log_index.get(),
        };
        let payload = encode_metadata_command_next_id_request(&request);
        let response = self.rpc_request(StorageRpcMessageKind::MetadataCommandNextId, payload)?;
        let decoded = decode_metadata_command_next_id_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command next id response",
                error.to_string(),
            )
        })?;
        let (cluster_epoch, decoded_pg_id, log_index) = match decoded.outcome {
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch,
                pg_id,
                log_index,
            } => (cluster_epoch, pg_id, log_index),
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || pg_id != request.pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                });
            }
        };
        let Some(log_index) = MetadataCommandLogIndex::new(log_index) else {
            return Err(self.rpc_payload_error(
                "decode metadata command next id response",
                "metadata command log index must not be zero".to_string(),
            ));
        };
        if cluster_epoch != self.cluster_epoch || decoded_pg_id != pg_id {
            return Err(self.rpc_payload_error(
                "decode metadata command next id response",
                "metadata command id route mismatch".to_string(),
            ));
        }
        Ok(MetadataCommandId::new(
            cluster_epoch,
            decoded_pg_id,
            log_index,
        ))
    }

    fn encode_metadata_command_state_request(&self, pg_id: PgId) -> Vec<u8> {
        encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        })
    }

    fn encode_metadata_command_request(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error("encode metadata command request", error.to_string())
        })
    }

    pub(crate) fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            pg_id,
            command,
        )
    }

    pub(crate) fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            pg_id,
            command,
        )
    }

    pub(crate) fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        preserve_pending_slot: bool,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let kind = if preserve_pending_slot {
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        } else {
            StorageRpcMessageKind::MetadataCommandValidateReplayState
        };
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(kind, payload)?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replay state response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
        )?;
        let response =
            decode_metadata_command_applied_hashes_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command applied hashes response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes) => Ok(hashes),
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command applied hashes response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command applied hashes response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    pub(crate) fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandMatchingAppliedRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            expected_previous_log_hash,
        };
        let payload =
            encode_metadata_command_matching_applied_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command matching applied request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command matching applied response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandAbandoned, payload)?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command abandoned response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
        )?;
        let response =
            decode_metadata_command_state_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command record abandoned response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command record abandoned response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command record abandoned response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                ..
            } => Err(self.rpc_payload_error(
                "decode metadata command record abandoned response",
                "record abandoned response cannot contain object generation reservation conflict"
                    .to_string(),
            )),
        }
    }

    pub(crate) fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_metadata_command_state_outcome_response(&response)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command apply and record response",
                    error.to_string(),
                )
            })
            .map_err(BucketSnapshotLoadError::Store)?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "decode metadata command apply and record response",
                        "metadata command log conflict route mismatch".to_string(),
                    )));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "decode metadata command apply and record response",
                        "metadata command log conflict index must not be zero".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Store(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: conflict_pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ))
            }
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                reservation_id,
                generation_id,
            } => Err(BucketSnapshotLoadError::Metadata(
                MetadataError::ObjectGenerationReservationConflict {
                    reservation_id: reservation_id.into_string(),
                    generation_id: generation_id.get(),
                },
            )),
        }
    }

    pub(crate) fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            payload,
        )?;
        let response =
            decode_metadata_command_pending_slot_insert_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => Ok(()),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            } => Err(StoreError::MetadataCommandPendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            }),
        }
    }

    pub(crate) fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command bucket-control pending slot request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
            payload,
        )?;
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command bucket-control pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(value),
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command bucket-control pending slot insert response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command bucket-control pending slot insert response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    pub(crate) fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        let payload = encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot remove request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot remove response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotReplaceRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload =
            encode_metadata_command_pending_slot_replace_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command pending slot replace request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot replace response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_acceptance_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        let payload = encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error("encode metadata command request", error.to_string())
        })?;
        let response = self.rpc_request(kind, payload)?;
        let response = decode_metadata_command_acceptance_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command acceptance response",
                error.to_string(),
            )
        })?;
        match response.outcome {
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance) => Ok(acceptance),
            StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command acceptance response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command acceptance response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    fn encode_shard_ack_batch(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardAckBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            items: shard_batch
                .iter()
                .map(|(shard_key, ack)| StorageRpcShardAckItem {
                    shard_key: (*shard_key).clone(),
                    ack: *ack,
                })
                .collect(),
        };
        encode_shard_ack_batch_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard ack batch request", error.to_string())
        })
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
            StorageRpcBucketInfoOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
        }
    }

    fn rpc_request(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|source| StoreError::Io {
                context: "connect storage-node RPC socket",
                source,
            })?;
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        write_storage_rpc_frame_to(&mut stream, &request).map_err(|error| {
            self.rpc_payload_error("write storage RPC request", error.to_string())
        })?;
        let response = read_storage_rpc_frame_from(&mut stream).map_err(|error| {
            self.rpc_payload_error("read storage RPC response", error.to_string())
        })?;
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate storage RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        match decode_storage_rpc_response_payload(&response.payload).map_err(|error| {
            self.rpc_payload_error("decode storage RPC response", error.to_string())
        })? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn shard_location(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> crate::cluster::ShardLocation {
        crate::cluster::ShardLocation::new(
            self.cluster_epoch,
            data_pg_id,
            key.shard_index(),
            self.node_id,
        )
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation: kind.operation_name(),
            message: format!("{:?}: {}", error.code, error.message),
        }
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            message,
        }
    }
}

#[allow(dead_code)]
impl UnixStorageNodeReadHandleSession {
    pub(crate) fn acquire_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
        locations: Vec<crate::cluster::ShardLocation>,
    ) -> Result<Vec<crate::cluster::ShardLocation>, StoreError> {
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.into(),
            locations,
        };
        let payload = encode_read_handle_acquire_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle acquire request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesAcquire, payload)?;
        let response = decode_read_handle_acquire_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle acquire response", error.to_string())
        })?;
        if response.locations != request.locations {
            return Err(self.rpc_payload_error(
                "validate read handle acquire response",
                format!(
                    "expected locations {:?}, got {:?}",
                    request.locations, response.locations
                ),
            ));
        }
        Ok(response.locations)
    }

    pub(crate) fn release_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.into(),
        };
        let payload = encode_read_handle_release_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle release request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesRelease, payload)?;
        decode_read_handle_release_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle release response", error.to_string())
        })?;
        Ok(())
    }

    fn rpc_request(
        &mut self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or_else(|| {
            self.rpc_payload_error(
                "allocate read-handle request id",
                "storage-node read-handle request id overflowed".to_string(),
            )
        })?;
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        write_storage_rpc_frame_to(&mut self.stream, &request).map_err(|error| {
            self.rpc_payload_error("write read-handle RPC request", error.to_string())
        })?;
        let response = read_storage_rpc_frame_from(&mut self.stream).map_err(|error| {
            self.rpc_payload_error("read read-handle RPC response", error.to_string())
        })?;
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate read-handle RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        match decode_storage_rpc_response_payload(&response.payload).map_err(|error| {
            self.rpc_payload_error("decode read-handle RPC response", error.to_string())
        })? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation: kind.operation_name(),
            message: format!("{:?}: {}", error.code, error.message),
        }
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            message,
        }
    }
}

impl PlacedShardNodeClient for UnixStorageNodeClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::write_placed_shard(self, data_pg_id, key, data)
    }

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        UnixStorageNodeClient::read_placed_shard(self, data_pg_id, key, expected_ack)
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        if dst.len() as u64 != expected_ack.stored_size {
            return Err(self.rpc_payload_error(
                "shard read range",
                format!(
                    "remote shard read buffer is {} bytes for expected {} byte shard",
                    dst.len(),
                    expected_ack.stored_size
                ),
            ));
        }
        let data = UnixStorageNodeClient::read_placed_shard_range(
            self,
            data_pg_id,
            key,
            expected_ack,
            0,
            dst.len() as u64,
        )?;
        dst.copy_from_slice(&data);
        Ok(())
    }

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_placed_shard(self, data_pg_id, key)
    }
}

impl ShardAckNodeClient for UnixStorageNodeClient {
    fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::register_written_shard_acks(self, pg_id, shard_batch)
    }

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::validate_written_shard_acks(self, pg_id, &[(key, ack)])
    }
}

impl ShardScavengerNodeClient for UnixStorageNodeClient {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_files(self, data_pg_id)
    }
}

impl MetadataCommandNodeClient for UnixStorageNodeClient {
    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::max_metadata_command_log_index(self, pg_id)
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::next_metadata_command_id_at_least(self, pg_id, min_log_index)
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::pending_metadata_command_envelope(self, pg_id)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::try_insert_pending_metadata_command_slot(
            self, pg_id, command, bucket,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
            self, pg_id, command, bucket,
        )
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::remove_pending_metadata_command_slot(self, pg_id, command)
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::replace_pending_metadata_command_slot_for_reissue(
            self,
            pg_id,
            previous,
            replacement,
            bucket,
        )
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::validate_metadata_command_replay_state(self, pg_id, false)
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::validate_metadata_command_replay_state(self, pg_id, true)
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        UnixStorageNodeClient::metadata_command_acceptance(self, pg_id, command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        UnixStorageNodeClient::metadata_command_abandon_acceptance(self, pg_id, command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        UnixStorageNodeClient::applied_metadata_command_log_entry_hashes(self, pg_id, command)
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::has_matching_applied_metadata_command_log_entry(
            self,
            pg_id,
            command,
            expected_previous_log_hash,
        )
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        UnixStorageNodeClient::apply_metadata_command_and_record(self, pg_id, command)
    }

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::record_metadata_command_abandoned(self, pg_id, command)
    }

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::metadata_command_abandoned(self, pg_id, command)
    }
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

impl ShardReadHandleNodeClient for UnixStorageNodeClient {
    fn acquire_read_handles(
        &self,
        read_operation_id: &str,
        locations: Vec<crate::cluster::ShardLocation>,
    ) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        let mut session = self.open_read_handle_session()?;
        session.acquire_read_handles(read_operation_id, locations)?;
        Ok(Box::new(UnixStorageNodeReadHandleLease {
            session,
            read_operation_id: read_operation_id.to_string(),
            released: false,
        }))
    }
}

impl ShardReadHandleLease for UnixStorageNodeReadHandleLease {
    fn release(&mut self) -> Result<(), StoreError> {
        if self.released {
            return Ok(());
        }
        self.session
            .release_read_handles(self.read_operation_id.as_str())?;
        self.released = true;
        Ok(())
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
        _locations: Vec<crate::cluster::ShardLocation>,
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
}

impl ShardScavengerNodeClient for LocalStorageNodeClient {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        self.storage_node
            .list_scavenger_shard_files(data_pg_id.get())
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
}

impl BucketWriteReservationNodeClient for LocalStorageNodeClient {
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

impl BucketMetadataNodeClient for UnixStorageNodeClient {
    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadRaw, pg_id, bucket)
    }

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadInfo, pg_id, bucket)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            request,
        };
        let payload = encode_bucket_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketSnapshotLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_snapshot_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket snapshot response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
                if snapshot.bucket.name != *bucket || snapshot.request != request.request {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot response",
                        "bucket snapshot response identity does not match request".to_string(),
                    )));
                }
                Ok(*snapshot)
            }
            StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
        }
    }

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotPairRequest {
            source: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: source_pg_id,
                    bucket: source.0.clone(),
                },
                request: source.1,
            },
            destination: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: destination_pg_id,
                    bucket: destination.0.clone(),
                },
                request: destination.1,
            },
        };
        let payload = encode_bucket_snapshot_pair_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketSnapshotPairLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_snapshot_pair_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket snapshot pair response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcBucketSnapshotPairOutcome::Loaded(pair) => {
                self.validate_bucket_snapshot_pair_response(&pair, source, destination)?;
                Ok(*pair)
            }
            StorageRpcBucketSnapshotPairOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
        }
    }

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        let request = StorageRpcCreateBucketCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            command_id,
            config: StorageRpcCreateBucketConfig {
                name: BucketName::try_from(config.name).map_err(|reason| {
                    BucketSnapshotLoadError::Metadata(MetadataError::InvalidBucketName {
                        reason: reason.to_string(),
                    })
                })?,
                owner_principal: config.owner_principal.to_string(),
                owner_canonical_id: config.owner_canonical_id.clone(),
                acl_grants: config.acl_grants.clone(),
                public_read: config.public_read,
                public_write: config.public_write,
                versioning: config.versioning,
                object_lock: config.object_lock,
            },
        };
        let payload = encode_create_bucket_command_build_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode create-bucket command build request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketCreateCommandBuild, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_create_bucket_command_build_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode create-bucket command build response",
                error.to_string(),
            ))
        })?;
        self.validate_create_bucket_command_build_outcome(
            response.outcome,
            bucket,
            command_id,
            config,
        )
    }

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        let request = StorageRpcCompletedMultipartOrderCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            command_id,
        };
        let payload =
            encode_completed_multipart_order_command_build_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode completed multipart order command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::CompletedMultipartOrderCommandBuild,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_completed_multipart_order_command_build_response(&response).map_err(
            |error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode completed multipart order command build response",
                    error.to_string(),
                ))
            },
        )?;
        self.validate_completed_multipart_order_command_build_response(
            response.completion_order,
            response.command,
            bucket,
            command_id,
        )
    }
}

impl BucketWriteReservationNodeClient for UnixStorageNodeClient {
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
        let request = StorageRpcBucketWriteReservationAcquireRequest {
            node_id: self.node_id,
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
        let payload =
            encode_bucket_write_reservation_acquire_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write reservation acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketWriteReservationAcquire,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_write_reservation_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
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
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservation acquire response",
                "reservation response identity does not match request".to_string(),
            )));
        }
        Ok(record)
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
        let request = StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectVersionNext, payload)
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
            payload,
            "decode stream upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_create_stream_upload_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
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
            DeleteObjectVersionTarget::DeleteMarker => Ok(()),
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
                Some(DeleteObjectVersionTarget::DeleteMarker),
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
        }
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
}

impl StorageNodeClient for LocalStorageNodeClient {
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

    fn list_completed_multipart_upload_records_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<CompletedMultipartUploadRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_completed_multipart_upload_records_for_bucket(bucket.as_str())?)
    }

    fn list_objects(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_objects(req)?)
    }

    fn list_object_versions(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_object_versions(req)?)
    }

    fn list_multipart_uploads(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_multipart_uploads(req)?)
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
            let pg = self.storage_node.get_pg(source_pg_id.get())?;
            let bucket =
                SharedStorageNode::load_bucket_snapshot_from_pg(&pg, source.0, merged_request)?;
            drop(pg);
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let guards = self
            .storage_node
            .lock_bucket_pair_pgs(source_pg_id.get(), destination_pg_id.get())?;
        match guards {
            crate::node::BucketPairPgGuards::Same { bucket } => {
                let source_snapshot =
                    SharedStorageNode::load_bucket_snapshot_from_pg(&bucket, source.0, source.1)?;
                let destination_snapshot = SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket,
                    destination.0,
                    destination.1,
                )?;
                drop(bucket);
                Ok(BucketSnapshotPair::Distinct {
                    source: Box::new(source_snapshot),
                    destination: Box::new(destination_snapshot),
                })
            }
            crate::node::BucketPairPgGuards::Distinct {
                source: source_pg,
                destination: destination_pg,
            } => {
                let source_snapshot = SharedStorageNode::load_bucket_snapshot_from_pg(
                    &source_pg, source.0, source.1,
                )?;
                let destination_snapshot = SharedStorageNode::load_bucket_snapshot_from_pg(
                    &destination_pg,
                    destination.0,
                    destination.1,
                )?;
                drop(source_pg);
                drop(destination_pg);
                Ok(BucketSnapshotPair::Distinct {
                    source: Box::new(source_snapshot),
                    destination: Box::new(destination_snapshot),
                })
            }
        }
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

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::list_buckets(&*pg, owner_canonical_id)?)
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

    fn list_all_stream_uploads(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.list_all_stream_uploads()?)
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
        encode_metadata_command_applied_hashes_response,
        encode_metadata_command_bool_outcome_response, encode_metadata_command_next_id_response,
        encode_metadata_command_state_outcome_response, encode_read_handle_acquire_response,
        encode_storage_rpc_success_response, read_storage_rpc_frame_from,
        write_storage_rpc_frame_to, StorageRpcMetadataCommandAppliedHashesResponse,
        StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcMetadataCommandNextIdResponse,
        StorageRpcMetadataCommandStateOutcomeResponse, StorageRpcReadHandleAcquireResponse,
    };
    use crate::types::{
        DeleteMarkerRecord, EtagKind, ObjectEncryption, ObjectLockState, SerializedMetadataBlob,
        SerializedSystemMetadataBlob, SerializedTagSet, StorageClass, StreamUploadPartSnapshot,
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

        let record = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &bucket,
            "reservation-remote-1",
            "owner-token-remote-1",
            ClusterEpoch::new(1).unwrap(),
            "put-object",
            10,
            Some(20),
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
        BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
            &client,
            PgId::new(0),
            &record,
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
        let generation_id = GenerationId::new(19).unwrap();
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
        }
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_threads: Vec<_> = (0..8)
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
                parts: vec![ObjectPartRecord {
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
                }],
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
        let null_build = BuildCompleteMultipartObjectCommandReq {
            pg_id: PgId::new(0),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &null_request,
            version_id: VersionId::Null,
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
                &test_metadata_command(0, 1),
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
        let mut session = client.open_read_handle_session().unwrap();

        assert_eq!(
            session
                .acquire_read_handles("read-op", vec![location])
                .unwrap(),
            vec![location]
        );
        assert_eq!(
            session
                .acquire_read_handles("read-op", vec![location])
                .unwrap(),
            vec![location]
        );
        assert_eq!(server.read_handle_count(location), 1);
        session.release_read_handles("read-op").unwrap();
        session.release_read_handles("read-op").unwrap();
        assert_eq!(server.read_handle_count(location), 0);
        session
            .acquire_read_handles("read-op-disconnect", vec![location])
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
            .acquire_read_handles("protected-read", vec![location])
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let err = client.delete_placed_shard(data_pg_id, &key).unwrap_err();
        assert!(matches!(
            err,
            StoreError::StorageRpc {
                operation: "shard delete",
                ref message,
                ..
            } if message.contains("ResourceExhausted") && message.contains("active read handles")
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
            .acquire_read_handles("read-op", vec![requested_location])
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
