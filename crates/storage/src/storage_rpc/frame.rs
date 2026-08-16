// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub(crate) fn encode_storage_rpc_frame(
    request_id: u64,
    kind: StorageRpcMessageKind,
    payload: &[u8],
) -> Result<Vec<u8>, StorageRpcFrameError> {
    encode_storage_rpc_frame_with_limit(request_id, kind, payload, STORAGE_RPC_MAX_PAYLOAD_LEN)
}

pub(crate) fn encode_storage_rpc_frame_with_limit(
    request_id: u64,
    kind: StorageRpcMessageKind,
    payload: &[u8],
    max_payload_len: usize,
) -> Result<Vec<u8>, StorageRpcFrameError> {
    if payload.len() > max_payload_len {
        return Err(StorageRpcFrameError::PayloadTooLarge {
            len: payload.len(),
            limit: max_payload_len,
        });
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| StorageRpcFrameError::PayloadTooLarge {
            len: payload.len(),
            limit: u32::MAX as usize,
        })?;
    let mut out =
        Vec::with_capacity(4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8 + 2 + 4 + 8 + payload.len());
    put_bytes(&mut out, STORAGE_RPC_FRAME_MAGIC);
    put_u16(&mut out, STORAGE_RPC_FRAME_ENCODING_VERSION);
    put_u64(&mut out, request_id);
    put_u16(&mut out, kind as u16);
    put_u32(&mut out, payload_len);
    put_u64(
        &mut out,
        storage_rpc_frame_checksum(
            STORAGE_RPC_FRAME_ENCODING_VERSION,
            request_id,
            kind as u16,
            payload_len,
            payload,
        ),
    );
    out.extend_from_slice(payload);
    Ok(out)
}

pub(crate) fn decode_storage_rpc_frame(
    bytes: &[u8],
) -> Result<StorageRpcFrame, StorageRpcFrameError> {
    decode_storage_rpc_frame_with_limit(bytes, STORAGE_RPC_MAX_PAYLOAD_LEN)
}

pub(crate) fn validate_storage_rpc_request_frame_payload_limit(
    frame: &StorageRpcFrame,
) -> Result<(), StorageRpcFrameError> {
    let limit = message_kind_request_max_payload_len(frame.kind, STORAGE_RPC_MAX_PAYLOAD_LEN);
    if frame.payload.len() > limit {
        return Err(StorageRpcFrameError::PayloadTooLarge {
            len: frame.payload.len(),
            limit,
        });
    }
    Ok(())
}

pub(crate) fn decode_storage_rpc_frame_with_limit(
    bytes: &[u8],
    max_payload_len: usize,
) -> Result<StorageRpcFrame, StorageRpcFrameError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let magic = decoder
        .read_bytes()
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    if magic != STORAGE_RPC_FRAME_MAGIC {
        return Err(StorageRpcFrameError::UnknownMagic);
    }
    let version = decoder
        .read_u16()
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    if version != STORAGE_RPC_FRAME_ENCODING_VERSION {
        return Err(StorageRpcFrameError::UnsupportedVersion(version));
    }
    let request_id = decoder
        .read_u64()
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    let raw_kind = decoder
        .read_u16()
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    let kind = StorageRpcMessageKind::from_u16(raw_kind)?;
    let payload_len = decoder
        .read_u32()
        .map_err(|_| StorageRpcFrameError::Truncated)? as usize;
    let effective_max_payload_len = max_payload_len;
    if payload_len > effective_max_payload_len {
        return Err(StorageRpcFrameError::PayloadTooLarge {
            len: payload_len,
            limit: effective_max_payload_len,
        });
    }
    let expected_checksum = decoder
        .read_u64()
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    let payload = decoder
        .read_exact(payload_len)
        .map_err(|_| StorageRpcFrameError::Truncated)?;
    if storage_rpc_frame_checksum(version, request_id, raw_kind, payload_len as u32, payload)
        != expected_checksum
    {
        return Err(StorageRpcFrameError::PayloadChecksumMismatch);
    }
    decoder
        .finish()
        .map_err(|_| StorageRpcFrameError::TrailingBytes)?;
    Ok(StorageRpcFrame {
        request_id,
        kind,
        payload: payload.to_vec(),
    })
}

pub(crate) fn write_storage_rpc_frame_to<W: Write>(
    writer: &mut W,
    frame: &StorageRpcFrame,
) -> Result<(), StorageRpcStreamError> {
    let bytes = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload)?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub(crate) fn read_storage_rpc_frame_from<R: Read>(
    reader: &mut R,
) -> Result<StorageRpcFrame, StorageRpcStreamError> {
    read_storage_rpc_frame_from_with_limit(reader, STORAGE_RPC_MAX_PAYLOAD_LEN)
}

pub(crate) fn read_storage_rpc_request_frame_from<R: Read>(
    reader: &mut R,
) -> Result<StorageRpcFrame, StorageRpcStreamError> {
    read_storage_rpc_frame_from_with_limit_and_caps(
        reader,
        STORAGE_RPC_MAX_PAYLOAD_LEN,
        message_kind_request_max_payload_len,
    )
}

pub(crate) fn read_storage_rpc_frame_from_with_limit<R: Read>(
    reader: &mut R,
    max_payload_len: usize,
) -> Result<StorageRpcFrame, StorageRpcStreamError> {
    read_storage_rpc_frame_from_with_limit_and_caps(reader, max_payload_len, |_, limit| limit)
}

fn read_storage_rpc_frame_from_with_limit_and_caps<R: Read>(
    reader: &mut R,
    max_payload_len: usize,
    effective_payload_limit: fn(StorageRpcMessageKind, usize) -> usize,
) -> Result<StorageRpcFrame, StorageRpcStreamError> {
    let magic_len = read_u32_from(reader)?;
    if magic_len as usize != STORAGE_RPC_FRAME_MAGIC.len() {
        return Err(StorageRpcFrameError::UnknownMagic.into());
    }
    let mut bytes = Vec::with_capacity(4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8 + 2 + 4 + 8);
    put_u32(&mut bytes, magic_len);
    let mut magic = vec![0; STORAGE_RPC_FRAME_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    bytes.extend_from_slice(&magic);
    let version = read_u16_from(reader)?;
    put_u16(&mut bytes, version);
    let request_id = read_u64_from(reader)?;
    put_u64(&mut bytes, request_id);
    let raw_kind = read_u16_from(reader)?;
    put_u16(&mut bytes, raw_kind);
    let kind = StorageRpcMessageKind::from_u16(raw_kind)?;
    let payload_len = read_u32_from(reader)?;
    put_u32(&mut bytes, payload_len);
    let effective_max_payload_len = effective_payload_limit(kind, max_payload_len);
    if payload_len as usize > effective_max_payload_len {
        return Err(StorageRpcFrameError::PayloadTooLarge {
            len: payload_len as usize,
            limit: effective_max_payload_len,
        }
        .into());
    }
    let checksum = read_u64_from(reader)?;
    put_u64(&mut bytes, checksum);
    let mut payload = vec![0; payload_len as usize];
    reader.read_exact(&mut payload)?;
    bytes.extend_from_slice(&payload);
    Ok(decode_storage_rpc_frame_with_limit(
        &bytes,
        max_payload_len,
    )?)
}

fn message_kind_request_max_payload_len(
    kind: StorageRpcMessageKind,
    generic_max_payload_len: usize,
) -> usize {
    let kind_max_payload_len = match kind {
        StorageRpcMessageKind::Health => STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN,
        StorageRpcMessageKind::MetadataCommand => {
            STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardWrite | StorageRpcMessageKind::ShardRepairWrite => {
            STORAGE_RPC_MAX_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ReadHandlesAcquire => {
            STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ReadHandlesRelease => {
            STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadLeaseControl => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_LEASE_CONTROL_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardRead => STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardHistoricalRead => {
            STORAGE_RPC_MAX_HISTORICAL_SHARD_READ_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardReadRange => STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardDelete => STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardAckLoad
        | StorageRpcMessageKind::ShardAckHistoricalLoad
        | StorageRpcMessageKind::ShardAckDelete => STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardAckRecord | StorageRpcMessageKind::ShardAckValidate => {
            STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardScavengerListFiles => {
            STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardScavengerShardRows
        | StorageRpcMessageKind::ShardScavengerPayloadReferences
        | StorageRpcMessageKind::ShardScavengerObservations => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentBackfillReferencePage => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardScavengerObservationRecord => {
            STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardScavengerObservationResolve => {
            STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairRecord => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairResolve => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MAX_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairs => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardRepairClaimError => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_ERROR_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillRecord => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillResolve => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MAX_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfills => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillCount => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillExists => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MAX_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError => {
            STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_ERROR_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ClaimHeartbeat => STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN,
        StorageRpcMessageKind::ClaimRelease => STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN,
        StorageRpcMessageKind::ProofRelease => {
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN
                + STORAGE_RPC_MAX_OPERATION_DEADLINE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandReplicaState => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandAcceptance
        | StorageRpcMessageKind::MetadataCommandAbandonAcceptance
        | StorageRpcMessageKind::MetadataCommandAppliedLogHashes
        | StorageRpcMessageKind::MetadataCommandPublicationStart
        | StorageRpcMessageKind::MetadataCommandAbandoned
        | StorageRpcMessageKind::MetadataCommandRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandRetainedAbortApply
        | StorageRpcMessageKind::MetadataCommandRetainedAbortFinish
        | StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord => {
            STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned => {
            STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandPendingSlotRemove => {
            STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandPendingSlotReplace => {
            STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace => {
            STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandMatchingAppliedLog => {
            STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandRetainedLogHashes
        | StorageRpcMessageKind::MetadataCommandRetainedLogEntries => {
            STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandMaxLogIndex
        | StorageRpcMessageKind::MetadataCommandPendingEnvelope
        | StorageRpcMessageKind::MetadataCommandValidateReplayState
        | StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        | StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize
        | StorageRpcMessageKind::MetadataCommandCheckpointExport
        | StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent
        | StorageRpcMessageKind::MetadataCommandLogCompact => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ClusterMapHistoryReferenceSummary => 4 + 8,
        StorageRpcMessageKind::MetadataCommandCheckpointCandidates => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8 + 4
        }
        StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize => {
            STORAGE_RPC_MAX_METADATA_COMMAND_TRANSFER_EMPTY_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize => {
            STORAGE_RPC_MAX_METADATA_COMMAND_TRANSFER_MATCHING_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall => {
            STORAGE_RPC_MAX_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandTransferStateAdopt => STORAGE_RPC_MAX_PAYLOAD_LEN,
        StorageRpcMessageKind::MetadataCommandNextId => {
            STORAGE_RPC_MAX_METADATA_COMMAND_NEXT_ID_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandPgLockAcquire
        | StorageRpcMessageKind::MetadataCommandPgLockRelease => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteReplicaHead
        | StorageRpcMessageKind::BucketHeadRaw
        | StorageRpcMessageKind::BucketHeadInfo => STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
        StorageRpcMessageKind::BucketSnapshotLoad => {
            STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketCreateCommandBuild => {
            STORAGE_RPC_MAX_CREATE_BUCKET_COMMAND_BUILD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MultipartCompletionBarrierCommandBuild => {
            STORAGE_RPC_MAX_MULTIPART_COMPLETION_BARRIER_COMMAND_BUILD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectReadAuthSubjectLoad => {
            STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectReadSnapshotLoad => {
            STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad
        | StorageRpcMessageKind::ObjectLifecycleVersionListLoad
        | StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad => {
            STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad => {
            STORAGE_RPC_MAX_MULTIPART_UPLOAD_LOAD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad => {
            STORAGE_RPC_MAX_MULTIPART_COMPLETION_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad => {
            STORAGE_RPC_MAX_MULTIPART_COMPLETION_PREFLIGHT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartPartsList => {
            STORAGE_RPC_MAX_MULTIPART_PARTS_LIST_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartManagementLookup => {
            STORAGE_RPC_MAX_MULTIPART_MANAGEMENT_LOOKUP_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad => {
            STORAGE_RPC_MAX_MULTIPART_ABORT_CLEANUP_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadSessionLoad
        | StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare => {
            STORAGE_RPC_MAX_STREAM_UPLOAD_SESSION_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate => {
            STORAGE_RPC_MAX_STREAM_UPLOAD_BUCKET_WRITE_RESERVATION_UPDATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadsList => {
            STORAGE_RPC_MAX_STREAM_UPLOADS_LIST_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadsPgList => {
            STORAGE_RPC_MAX_STREAM_UPLOADS_PG_LIST_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot => {
            STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimRoot => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimLoad => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimClaimGet => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimExists => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad => {
            STORAGE_RPC_MAX_STREAM_UPLOAD_SEGMENTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare => {
            STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad => {
            STORAGE_RPC_MAX_STREAM_PUT_FINALIZE_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad => {
            STORAGE_RPC_MAX_STREAM_PART_FINALIZE_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMetadataPutCommandBuild
        | StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild
        | StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild
        | StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild
        | StorageRpcMessageKind::ObjectStreamUploadMatch
        | StorageRpcMessageKind::ObjectMultipartUploadMatch
        | StorageRpcMessageKind::ObjectStreamUploadCommandBuild
        | StorageRpcMessageKind::ObjectMultipartUploadCommandBuild => {
            STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild
        | StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild => {
            STORAGE_RPC_MAX_STREAM_FINALIZE_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild => {
            STORAGE_RPC_MAX_MULTIPART_COMPLETION_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild => {
            STORAGE_RPC_MAX_MULTIPART_ABORT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild => {
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectGenerationNext => {
            STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectGenerationReservation => {
            STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::DirectPutCommitSnapshotLoad => {
            STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::DirectPutCommitCommandBuild => {
            STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectVersionNext => {
            STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteReservationAcquire => {
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteReservationValidate => {
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteReservationHeartbeat => {
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteReservationRelease => {
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteDrainBegin => {
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_BEGIN_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteDrainClear => {
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteDrainHeartbeat => {
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_HEARTBEAT_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteDrainClearExpired => {
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_EXPIRED_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord => {
            STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketWriteDrainExists
        | StorageRpcMessageKind::BucketWriteDrainGet
        | StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimGet
        | StorageRpcMessageKind::BucketWriteReservationsList => {
            STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteFinalizeRoots => {
            STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteBeginRoots => {
            STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire => {
            STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease => {
            STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketMetadataControlPendingMatch
        | StorageRpcMessageKind::BucketMetadataControlCommandBuild
        | StorageRpcMessageKind::BucketMarkDeletingCommandBuild => {
            STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketSubresourceGet => {
            STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN
        }
        StorageRpcMessageKind::LifecycleSweepBucketsList => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::LifecycleSweepRoots => {
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::LifecycleSweepClaimAcquire => {
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::LifecycleSweepClaimHeartbeat => {
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 8 + 1 + 8
        }
        StorageRpcMessageKind::LifecycleSweepClaimError => {
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN
        }
        StorageRpcMessageKind::LifecycleSweepClaimRelease => {
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectListPage => STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN,
        StorageRpcMessageKind::ObjectVersionListPage => {
            STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectMultipartUploadListPage => {
            STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketList => STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN,
        StorageRpcMessageKind::BucketExecutionGenerations
        | StorageRpcMessageKind::BucketFastPathIdentities => {
            STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN
        }
    };
    kind_max_payload_len.min(generic_max_payload_len)
}

pub(crate) fn encode_health_response(response: &StorageRpcHealthResponse) -> Vec<u8> {
    let mut out = Vec::new();
    put_u16(&mut out, response.protocol_version);
    put_u32(&mut out, response.node_id.as_u32());
    put_u64(&mut out, response.cluster_epoch.get());
    out
}

pub(crate) fn decode_health_response(
    bytes: &[u8],
) -> Result<StorageRpcHealthResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let protocol_version = decoder.read_u16()?;
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    decoder.finish()?;
    Ok(StorageRpcHealthResponse {
        protocol_version,
        node_id,
        cluster_epoch,
    })
}

pub(crate) fn encode_storage_rpc_success_response(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, 0);
    put_u8(&mut out, 0);
    put_bytes(&mut out, payload);
    out
}

pub(crate) fn encode_storage_rpc_error_response(
    error: &StorageRpcErrorResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if error.message.is_empty() {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "error response message must not be empty",
        ));
    }
    let mut out = Vec::new();
    put_u8(&mut out, 1);
    put_u8(&mut out, 0);
    put_u16(&mut out, error.code.as_u16());
    put_string(&mut out, &error.message);
    Ok(out)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedStorageRpcResponsePayload {
    pub(crate) response: Result<Vec<u8>, StorageRpcErrorResponse>,
    pub(crate) connection_reusable: bool,
}

pub(crate) fn set_storage_rpc_response_connection_reusable(
    bytes: &mut [u8],
    connection_reusable: bool,
) -> Result<(), StorageRpcPayloadError> {
    let Some(encoded) = bytes.get_mut(1) else {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "response envelope has no connection disposition",
        ));
    };
    *encoded = u8::from(connection_reusable);
    Ok(())
}

pub(crate) fn decode_storage_rpc_response_payload_with_connection_disposition(
    bytes: &[u8],
) -> Result<DecodedStorageRpcResponsePayload, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let response_tag = decoder.read_u8()?;
    let connection_reusable = match decoder.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown response connection disposition",
            ))
        }
    };
    let response = match response_tag {
        0 => Ok(decoder.read_bytes()?.to_vec()),
        1 => {
            let code = StorageRpcErrorCode::from_u16(decoder.read_u16()?)?;
            let message = decoder.read_string()?;
            if message.is_empty() {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "error response message must not be empty",
                ));
            }
            Err(StorageRpcErrorResponse { code, message })
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown response tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(DecodedStorageRpcResponsePayload {
        response,
        connection_reusable,
    })
}

pub(crate) fn decode_storage_rpc_response_payload(
    bytes: &[u8],
) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcPayloadError> {
    decode_storage_rpc_response_payload_with_connection_disposition(bytes)
        .map(|decoded| decoded.response)
}
