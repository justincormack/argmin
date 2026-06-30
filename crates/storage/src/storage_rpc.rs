use crate::{
    cluster::ShardLocation,
    metadata_command::{
        decode_metadata_command_envelope, BucketPropertyMutation, BucketSubresourceMutation,
        BucketWriteReservationProof, CreateMultipartUploadCommand, CreateStreamUploadCommand,
        DeleteObjectVersionTarget, MetadataCommandAcceptance, MetadataCommandLogHashRangeEntry,
        MetadataCommandLogIndex, MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind,
        MetadataCommandReplicaState, MetadataTransferCommand, ObjectPayloadReclaimCommand,
        PutObjectMetadataMutation,
    },
    pg_store::{
        MetadataCheckpointRow, MetadataCheckpointTableBlock, MetadataCheckpointTableDigest,
        MetadataCheckpointValue, MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus,
        PgClusterMapHistoryReferenceSummary, ScavengerShardFile, ScavengerShardFileScan,
        ScavengerShardRow,
    },
    types::{
        AbortMultipartUploadCleanup, BucketDeleteAttemptOutcomeKind,
        BucketDeleteAttemptOutcomeRecord, BucketDeleteAttemptPhase,
        BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketEncryptionConfig,
        BucketFastPathIdentity, BucketInfo, BucketObjectOwnership, BucketOwnershipControls,
        BucketSnapshot, BucketSnapshotPair, BucketSnapshotRequest, BucketSnapshotTagsRequest,
        BucketState, BucketSubresourceAux, BucketSubresourceKind, BucketWriteDrainRecord,
        BucketWriteDrainState, BucketWriteReservationRecord, ChecksumAlgorithm, ChecksumBytes,
        ChecksumType, ClusterEpoch, CommitDirectPutObjectReq, CompleteMultipartCommitCleanup,
        CompleteMultipartCommitRequest, CompletedMultipartUploadRecord, CreateBucketConfig,
        CreateMultipartUploadReq, CreateStreamUploadReq, DataPgId, DeleteMarkerRecord,
        DirectPutCommitStorageSnapshot, EcShape, EffectiveBucketEncryptionConfig, EtagKind,
        GenerationId, LifecycleSweepBuckets, LifecycleSweepClaimRecord, LifecycleSweepRoot,
        LifecycleSweepRootSource, ListMultipartUploadsReq, ListMultipartUploadsResp,
        ListObjectVersionsReq, ListObjectVersionsResp, ListObjectsReq, ListObjectsResp,
        ListPartsResp, ListedMultipartParts, LiveObjectRecord, LoadedBucketSubresource,
        ManagedEncryptionAlgorithm, MultipartChecksumConfig, MultipartCompletionPreflight,
        MultipartCompletionSnapshot, MultipartPartRecord, MultipartPartSegmentRecord,
        MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
        MultipartUploadManagementLookup, MultipartUploadRecord, ObjectEncryption,
        ObjectEncryptionType, ObjectEtag, ObjectKey, ObjectLayout, ObjectLockState,
        ObjectPartRecord, ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind,
        ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity, ObjectReadSnapshot,
        ObjectReadSnapshotMode, ObjectRetention, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
        ObjectSegmentsReclaimSegmentRecord, OwnerIdentity, PayloadReclaimRoot, PgId,
        PlacedSegmentShardBackfillClaimRecord, PlacedSegmentShardBackfillRecord,
        PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimRecord,
        PlacedSegmentShardRepairRecord, PlacedSegmentShardRepairWorkItem,
        PrepareStreamUploadSegmentAppendReq, PublicAccessBlockConfig, SegmentStoredBytesRequest,
        SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet, SessionId,
        ShardIndex, ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
        ShardScavengerObservationReason, ShardScavengerObservationRecord,
        ShardScavengerPayloadReference, ShardScavengerPlacedShardSetReference,
        ShardScavengerReclaimShardSetReference, ShardScavengerRoutedMultipartPartReference,
        StorageClass, StoredLegalHoldStatus, StoredObject, StreamPutCommitInput,
        StreamPutFinalizeStorageSnapshot, StreamUploadPartSnapshot,
        StreamUploadPartStorageSnapshot, StreamUploadRecord, StreamUploadSegmentRecord,
        StreamUploadState, StreamUploadTarget, TerminalStreamCleanupRecord, UploadId, UploadState,
        VersionId, WriteAck, BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
        PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
        PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN, PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
        PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
        PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN,
        PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN, PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT,
        PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN, SESSION_ID_LEN, SHARD_KEY_LEN,
        UPLOAD_ID_LEN,
    },
    BucketDeleteBeginRoot, BucketName, NodeId,
};
use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, RetentionPeriod,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::num::NonZeroU32;

const STORAGE_RPC_FRAME_MAGIC: &[u8] = b"argmin-storage-rpc-frame";
pub(crate) const STORAGE_RPC_FRAME_ENCODING_VERSION: u16 = 1;
pub(crate) const STORAGE_RPC_MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;
const STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN: usize = 0;
pub(crate) const STORAGE_RPC_MAX_READ_OPERATION_ID_LEN: usize = 256;
pub(crate) const STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS: usize = 1024;
pub(crate) const STORAGE_RPC_MAX_SHARD_ACK_ITEMS: usize = 4096;
const STORAGE_RPC_SHARD_LOCATION_LEN: usize = 8 + 4 + 1 + 4;
const STORAGE_RPC_SHARD_KEY_FIELD_LEN: usize = 4 + SHARD_KEY_LEN;
const STORAGE_RPC_WRITE_ACK_LEN: usize = 8 + 8;
const STORAGE_RPC_SHARD_ACK_ROUTE_LEN: usize = 4 + 8 + 4;
const STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN: usize =
    STORAGE_RPC_SHARD_ACK_ROUTE_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN;
const STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN: usize = STORAGE_RPC_SHARD_ACK_ROUTE_LEN
    + 4
    + STORAGE_RPC_MAX_SHARD_ACK_ITEMS
        * (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN);
const STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS: usize = 4096;
const STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN: usize = 4096;
const STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN: usize = STORAGE_RPC_SHARD_ACK_ROUTE_LEN;
const STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN: usize = STORAGE_RPC_SHARD_KEY_FIELD_LEN + 8;
const STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS: usize = 100_000;
const STORAGE_RPC_SCAVENGER_OBSERVATION_KEY_LEN: usize =
    4 + 4 + 1 + STORAGE_RPC_SHARD_KEY_FIELD_LEN;
const STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN: usize = 4096;
const STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_SCAVENGER_OBSERVATION_KEY_LEN
        + 1
        + 8
        + 1
        + 8
        + 1
        + 1
        + 1
        + 1
        + 4
        + STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN;
const STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + STORAGE_RPC_SCAVENGER_OBSERVATION_KEY_LEN;
const STORAGE_RPC_SCAVENGER_PAYLOAD_REFERENCE_MIN_LEN: usize = 1 + 4 + 16 + 8 + 2;
const STORAGE_RPC_SCAVENGER_OBSERVATION_MIN_LEN: usize =
    STORAGE_RPC_SCAVENGER_OBSERVATION_KEY_LEN + 8 + 8 + 8 + 1 + 1 + 1 + 1 + 1 + 1 + 1;
const STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MIN_LEN: usize = 4 + 16 + 8 + 8 + 8 + 2 + 1;
const STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MAX_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MIN_LEN;
const STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MIN_LEN: usize = 4 + 16 + 8 + 8 + 8 + 2 + 8 + 8;
const STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MAX_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MIN_LEN;
const STORAGE_RPC_PLACED_SEGMENT_REPAIR_RECORD_MIN_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MIN_LEN + 8 + 8 + 8 + 1;
const STORAGE_RPC_PLACED_SEGMENT_BACKFILL_RECORD_MIN_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MIN_LEN + 1 + 8 + 8 + 8 + 1;
const STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MAX_LEN
        + 1
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN;
const STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MAX_LEN
        + 1
        + 1
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN;
const STORAGE_RPC_PLACED_SEGMENT_REPAIR_CLAIM_RECORD_MIN_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MIN_LEN + 4 + 4 + 8 + 8 + 1 + 8 + 1;
const STORAGE_RPC_PLACED_SEGMENT_BACKFILL_CLAIM_RECORD_MIN_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MIN_LEN + 1 + 4 + 4 + 8 + 8 + 1 + 8 + 1;
const STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_REPAIR_WORK_ITEM_MAX_LEN
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN
        + 8
        + 8
        + 1
        + 8
        + 8
        + 1
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN;
const STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_PLACED_SEGMENT_BACKFILL_WORK_ITEM_MAX_LEN
        + 1
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN
        + 8
        + 8
        + 1
        + 8
        + 8
        + 1
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN;
const STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN
        + 8
        + 1
        + 8
        + 8;
const STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN
        + 8
        + 1
        + 8
        + 8;
const STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_ERROR_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_MAX_PLACED_SEGMENT_REPAIR_CLAIM_RECORD_PAYLOAD_LEN
        + 4
        + PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN
        + 8;
const STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_ERROR_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_CLAIM_RECORD_PAYLOAD_LEN
        + 4
        + PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN
        + 8;
const STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN: usize =
    STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN;
const STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN: usize =
    STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN;
const STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 8 + 8;
const STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN: usize = 4
    + STORAGE_RPC_MAX_READ_OPERATION_ID_LEN
    + 4
    + STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS
        * (STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN);
const STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN: usize =
    4 + STORAGE_RPC_MAX_READ_OPERATION_ID_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN: usize = 4 + 8 + 4;
const STORAGE_RPC_MAX_METADATA_COMMAND_NEXT_ID_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES: u64 = 4096;
const STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 16;
// Keep the worst-case all-applied retained-entry response within the 64 MiB
// frame cap after success-response wrapping.
pub(crate) const STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES: u64 = 31;
pub(crate) const STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES: usize = 4;
const STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_METADATA_CHECKPOINT_TABLES: usize = 128;
const STORAGE_RPC_MAX_METADATA_CHECKPOINT_COLUMNS: usize = 256;
const STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROWS: usize = 100_000;
const STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROW_VALUES: usize = 256;
const STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_METADATA_COMMAND_ITEM_PAYLOAD_LEN: usize =
    8 + 4 + STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_MAX_METADATA_COMMAND_ITEM_PAYLOAD_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1 + 4 + STORAGE_RPC_MAX_BUCKET_NAME_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 2 * STORAGE_RPC_MAX_METADATA_COMMAND_ITEM_PAYLOAD_LEN
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_NAME_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_BUCKET_NAME_LEN: usize = 63;
const STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN: usize = 4 + STORAGE_RPC_MAX_BUCKET_NAME_LEN;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN: usize = 256;
const STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN: usize = 1024;
const STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN: usize = 128;
const STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN: usize = 1024;
const STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN: usize = STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
    + 8
    + 8
    + 8
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN
    + 8
    + 1
    + 8
    + 1
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN;
const STORAGE_RPC_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_MAX_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 8
        + 8
        + 1
        + 1
        + 4
        + BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN
        + 1
        + 4
        + 8;
const STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN: usize = 1024;
const STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN: usize = 1024;
const STORAGE_RPC_MAX_BUCKET_ACL_GRANTS_LEN: usize = 64 * 1024;
const STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN: usize =
    4 + 8 + 4 + STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN;
const STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS: u32 = 100_000;
const STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN;
const STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize * STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN;
const STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 4;
const STORAGE_RPC_MAX_BUCKET_SNAPSHOT_PAIR_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN * 2;
const STORAGE_RPC_MAX_OBJECT_KEY_LEN: usize = 1024;
const STORAGE_RPC_MAX_LIST_PAGE_ITEMS: u32 = 100_000;
const STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS: u32 = 1024;
const STORAGE_RPC_MAX_STREAM_UPLOADS_LIST_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1 + 4 + SESSION_ID_LEN + 4;
const STORAGE_RPC_MAX_STREAM_UPLOADS_PG_LIST_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1 + 4 + SESSION_ID_LEN + 4;
const STORAGE_RPC_MAX_COMPLETED_MULTIPART_UPLOADS_LIST_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1 + 4 + UPLOAD_ID_LEN + 4;
const STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 3 * (1 + 4 + STORAGE_RPC_MAX_OBJECT_KEY_LEN) + 4;
const STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 3 * (1 + 4 + STORAGE_RPC_MAX_OBJECT_KEY_LEN)
        + 1
        + 8
        + 4;
const STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 2 * (1 + 4 + STORAGE_RPC_MAX_OBJECT_KEY_LEN)
        + 1
        + 4
        + UPLOAD_ID_LEN
        + 4;
const STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN: usize =
    4 + 8 + 4 + STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN + 4 + STORAGE_RPC_MAX_OBJECT_KEY_LEN;
const STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 4 + SESSION_ID_LEN;
const STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_OBJECT_PAYLOAD_RECLAIM_CLAIM_RECORD_MAX_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
        + 8
        + 4
        + STORAGE_RPC_MAX_OBJECT_KEY_LEN
        + 8
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 8
        + 1
        + 8
        + 4;
const STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN
        + 8
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 1
        + 8
        + 8;
const STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_OBJECT_PAYLOAD_RECLAIM_CLAIM_RECORD_MAX_LEN;
const STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 1 + 8;
const STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_OBJECT_TAGS_FOR_SUBJECT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_MULTIPART_UPLOAD_LOAD_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 4 + UPLOAD_ID_LEN;
const STORAGE_RPC_MAX_MULTIPART_COMPLETION_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_MULTIPART_COMPLETION_PREFLIGHT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_MULTIPART_COMPLETION_SNAPSHOT_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_MULTIPART_PARTS_LIST_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_MULTIPART_COMPLETION_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1 + 4 + 4;
const STORAGE_RPC_MAX_MULTIPART_MANAGEMENT_LOOKUP_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_MULTIPART_UPLOAD_LOAD_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_MULTIPART_ABORT_CLEANUP_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 4 + UPLOAD_ID_LEN;
const STORAGE_RPC_MAX_STREAM_PUT_FINALIZE_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 4 + SESSION_ID_LEN;
const STORAGE_RPC_MAX_STREAM_PART_FINALIZE_SNAPSHOT_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_STREAM_PUT_FINALIZE_SNAPSHOT_REQUEST_PAYLOAD_LEN + 4 + UPLOAD_ID_LEN + 4;
const STORAGE_RPC_MAX_STREAM_UPLOAD_SESSION_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 4 + SESSION_ID_LEN;
const STORAGE_RPC_MAX_STREAM_UPLOAD_SEGMENTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_STREAM_UPLOAD_SESSION_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_STREAM_UPLOAD_SESSION_REQUEST_PAYLOAD_LEN + 4 + 8 + 8 + 8 + 4 + 16;
const STORAGE_RPC_MAX_STREAM_FINALIZE_COMMAND_BUILD_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_MULTIPART_COMPLETION_COMMAND_BUILD_REQUEST_PAYLOAD_LEN: usize =
    2 * 1024 * 1024;
const STORAGE_RPC_MAX_MULTIPART_ABORT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN: usize =
    4 + 4 + 8 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 8 + 2;
const STORAGE_RPC_MIN_OBJECT_PART_RECORD_LEN: usize =
    4 + 4 + 8 + 4 + 8 + 8 + 4 + 1 + 4 + 16 + 8 + 8 + 2 + 4 + 1;
const STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN: usize =
    4 + SESSION_ID_LEN + 4 + 8 + 8 + 8 + 4 + 16 + 8 + 4 + 8 + 2;
const STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN: usize =
    4 + SESSION_ID_LEN + 4 + 4 + 1 + 1 + 8 + 1 + 1;
const STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN: usize =
    4 + UPLOAD_ID_LEN + 4 + 4 + 8 + 8 + 4 + 1 + 16 + 8 + 8 + 2 + 8 + 1;
const STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN: usize =
    4 + 4 + 4 + UPLOAD_ID_LEN + 8 + 4 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 8 + 2;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN
        + 8
        + 1
        + 8
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN;
const STORAGE_RPC_BUCKET_WRITE_DRAIN_RECORD_MAX_LEN: usize = STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
    + 8
    + 8
    + 1
    + 8
    + 9;
const STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_BEGIN_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 9;
const STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_BUCKET_WRITE_DRAIN_RECORD_MAX_LEN;
const STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_HEARTBEAT_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_EXPIRED_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_MAX_LEN;
const STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATIONS_LIST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN;
const STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8 + 4;
const STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + 8
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_NAME_LEN
        + 4;
const STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS: usize = 1024;
const STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS: usize = 1024;
const STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS: usize = 1024;
const STORAGE_RPC_BUCKET_DELETE_FINALIZE_ROOT_MAX_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN + 8;
const STORAGE_RPC_BUCKET_DELETE_BEGIN_ROOT_MAX_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN + 8 + 8;
const STORAGE_RPC_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_MAX_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
        + 8
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 4
        + 8
        + 9
        + 8
        + 5
        + STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN;
const STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 8
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 9
        + 8;
const STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        + STORAGE_RPC_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_MAX_LEN;
const STORAGE_RPC_MAX_CREATE_BUCKET_COMMAND_BUILD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        + 8
        + 4
        + STORAGE_RPC_MAX_BUCKET_NAME_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN
        + 4
        + s3_types::CANONICAL_USER_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_ACL_GRANTS_LEN
        + 12;
const STORAGE_RPC_MAX_COMPLETED_MULTIPART_ORDER_COMMAND_BUILD_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 8;
const STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
const STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1;
const STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN;
const STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_BEGIN_PAYLOAD_LEN + 8 + 8;
const STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN: usize =
    STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN + 8 + 4 + 4096;
const STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 4 + 4096;
const STORAGE_RPC_MAX_BUCKET_CLAIM_TOKEN_PAYLOAD_LEN: usize = STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
    + 8
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
    + 4
    + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
    + 8
    + 4;
const STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_TOKEN_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
        + 8
        + 4
        + STORAGE_RPC_MAX_OBJECT_KEY_LEN
        + 8
        + 1
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
        + 4
        + STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN
        + 8
        + 4;
const STORAGE_RPC_MAX_DURABLE_CLAIM_TOKEN_PAYLOAD_LEN: usize = 1
    + if STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_TOKEN_PAYLOAD_LEN
        > STORAGE_RPC_MAX_BUCKET_CLAIM_TOKEN_PAYLOAD_LEN
    {
        STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_TOKEN_PAYLOAD_LEN
    } else {
        STORAGE_RPC_MAX_BUCKET_CLAIM_TOKEN_PAYLOAD_LEN
    };
const STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_DURABLE_CLAIM_TOKEN_PAYLOAD_LEN + 8 + 1 + 8;
const STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_DURABLE_CLAIM_TOKEN_PAYLOAD_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum StorageRpcMessageKind {
    Health = 1,
    MetadataCommand = 2,
    ShardWrite = 3,
    ShardRead = 4,
    ShardReadRange = 5,
    ShardDelete = 6,
    ReadHandlesAcquire = 7,
    ReadHandlesRelease = 8,
    ClaimHeartbeat = 9,
    ClaimRelease = 10,
    ProofRelease = 11,
    ShardAckRecord = 12,
    ShardAckValidate = 13,
    ShardScavengerListFiles = 14,
    MetadataCommandReplicaState = 15,
    MetadataCommandAcceptance = 16,
    MetadataCommandAbandonAcceptance = 17,
    MetadataCommandPendingSlotInsert = 18,
    MetadataCommandPendingSlotRemove = 19,
    MetadataCommandMaxLogIndex = 20,
    MetadataCommandNextId = 21,
    MetadataCommandPendingEnvelope = 22,
    MetadataCommandValidateReplayState = 23,
    MetadataCommandValidateReplayStatePreservingPending = 24,
    MetadataCommandReplicaStateCanInitialize = 145,
    MetadataCommandTransferStateAdopt = 146,
    MetadataCommandTransferEmptyStateInitialize = 147,
    MetadataCommandTransferMatchingStateInitialize = 148,
    MetadataCommandTransferCheckpointBaseInstall = 149,
    MetadataCommandCheckpointExport = 150,
    MetadataCommandCheckpointCandidates = 151,
    MetadataCommandCheckpointRecordCurrent = 152,
    MetadataCommandLogCompact = 153,
    ClusterMapHistoryReferenceSummary = 154,
    MetadataCommandAppliedLogHashes = 25,
    MetadataCommandMatchingAppliedLog = 26,
    MetadataCommandAbandoned = 27,
    MetadataCommandRecordAbandoned = 28,
    MetadataCommandPendingSlotReplace = 29,
    MetadataCommandBucketControlPendingSlotInsert = 30,
    MetadataCommandApplyAndRecord = 31,
    BucketHeadRaw = 32,
    BucketHeadInfo = 33,
    BucketCreateCommandBuild = 34,
    ObjectGenerationNext = 35,
    ObjectGenerationReservation = 36,
    ObjectVersionNext = 37,
    BucketWriteReservationAcquire = 38,
    BucketWriteReservationValidate = 39,
    BucketWriteReservationRelease = 40,
    BucketWriteReservationHeartbeat = 123,
    BucketSnapshotLoad = 41,
    BucketSnapshotPairLoad = 42,
    DirectPutCommitSnapshotLoad = 43,
    DirectPutCommitCommandBuild = 44,
    CompletedMultipartOrderCommandBuild = 45,
    ObjectReadAuthSubjectLoad = 46,
    ObjectReadSnapshotLoad = 47,
    ObjectTagsForSubjectLoad = 48,
    ObjectMetadataPutSnapshotLoad = 49,
    ObjectMetadataPutCommandBuild = 50,
    ObjectDeleteCurrentSnapshotLoad = 51,
    ObjectDeleteSpecificSnapshotLoad = 52,
    ObjectDeleteSpecificCommandBuild = 53,
    ObjectDeleteCurrentCommandBuild = 54,
    ObjectInsertDeleteMarkerCommandBuild = 55,
    ObjectLifecycleVersionListLoad = 56,
    ObjectStreamUploadMatch = 57,
    ObjectMultipartUploadMatch = 58,
    ObjectStreamUploadCommandBuild = 59,
    ObjectMultipartUploadCommandBuild = 60,
    ObjectStreamPutFinalizeSnapshotLoad = 61,
    ObjectStreamPutCommitCommandBuild = 62,
    ObjectStreamPartFinalizeSnapshotLoad = 63,
    ObjectStreamPartCommitCommandBuild = 64,
    ObjectMultipartCompleteCommandBuild = 65,
    ObjectMultipartAbortCommandBuild = 66,
    ObjectMultipartAuthorizedAbortCommandBuild = 67,
    ObjectMultipartCompletionStaleSourceLoad = 68,
    ObjectMultipartAbortCleanupLoad = 69,
    ObjectMultipartUploadLoad = 70,
    ObjectMultipartInProgressUploadLoad = 71,
    ObjectMultipartInProgressUploadForListingLoad = 72,
    ObjectMultipartCompletionSnapshotLoad = 73,
    ObjectMultipartCompletionPreflightLoad = 74,
    ObjectMultipartPartsList = 75,
    ObjectMultipartManagementLookup = 76,
    ObjectStreamUploadSessionLoad = 77,
    ObjectStreamUploadSegmentsLoad = 78,
    ObjectStreamSegmentAppendPrepare = 79,
    BucketWriteDrainBegin = 80,
    BucketWriteDrainClear = 81,
    BucketWriteDrainClearExpired = 82,
    BucketWriteReservationsList = 83,
    BucketDeleteFinalized = 84,
    BucketDeleteFinalizeRoots = 85,
    BucketDeleteFinalizeClaimAcquire = 86,
    BucketDeleteFinalizeClaimRelease = 87,
    BucketMetadataControlPendingMatch = 88,
    BucketMetadataControlCommandBuild = 89,
    BucketSubresourceGet = 90,
    LifecycleSweepBucketsList = 91,
    LifecycleSweepRoots = 92,
    LifecycleSweepClaimAcquire = 93,
    LifecycleSweepClaimHeartbeat = 94,
    LifecycleSweepClaimError = 95,
    LifecycleSweepClaimRelease = 96,
    ObjectListPage = 97,
    ObjectVersionListPage = 98,
    ObjectMultipartUploadListPage = 99,
    BucketList = 100,
    BucketExecutionGenerations = 101,
    BucketFastPathIdentities = 102,
    BucketMarkDeletingCommandBuild = 103,
    BucketWriteDrainExists = 104,
    ObjectStreamUploadsList = 105,
    ObjectCompletedMultipartUploadsList = 106,
    ObjectPayloadReclaimExists = 107,
    ObjectBucketPayloadReclaimRoot = 108,
    ObjectPayloadReclaimRoot = 109,
    ObjectPayloadReclaimLoad = 110,
    ObjectPayloadReclaimClaimAcquire = 111,
    ObjectPayloadReclaimClaimRelease = 112,
    ShardAckLoad = 113,
    ShardAckDelete = 114,
    ShardScavengerShardRows = 115,
    ShardScavengerPayloadReferences = 116,
    ShardScavengerObservationRecord = 117,
    ShardScavengerObservations = 118,
    ShardScavengerObservationResolve = 119,
    ObjectStreamUploadsPgList = 120,
    MetadataCommandPgLockAcquire = 121,
    MetadataCommandPgLockRelease = 122,
    BucketWriteDrainGet = 155,
    BucketDeleteAttemptOutcomeRecord = 156,
    BucketDeleteAttemptOutcomeGet = 157,
    BucketDeleteBeginRoots = 158,
    BucketWriteDrainHeartbeat = 124,
    MetadataCommandRetainedLogHashes = 125,
    MetadataCommandRetainedLogEntries = 126,
    MetadataCommandPeeringReplayApplyAndRecord = 127,
    PlacedSegmentShardRepairRecord = 128,
    PlacedSegmentShardRepairs = 129,
    PlacedSegmentShardRepairResolve = 130,
    PlacedSegmentShardRepairClaimAcquire = 131,
    PlacedSegmentShardRepairClaimComplete = 132,
    PlacedSegmentShardRepairClaimError = 133,
    ShardRepairWrite = 134,
    PlacedSegmentShardBackfillRecord = 135,
    PlacedSegmentShardBackfills = 136,
    PlacedSegmentShardBackfillResolve = 137,
    PlacedSegmentShardBackfillClaimAcquire = 138,
    PlacedSegmentShardBackfillClaimComplete = 139,
    PlacedSegmentShardBackfillClaimError = 140,
    PlacedSegmentShardBackfillCount = 141,
    ShardHistoricalRead = 142,
    PlacedSegmentShardBackfillExists = 143,
    ShardAckHistoricalLoad = 144,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum StorageRpcErrorCode {
    FrameDecode = 1,
    PayloadDecode = 2,
    UnknownNode = 3,
    UnknownPg = 4,
    WrongClusterEpoch = 5,
    InactivePgRoute = 6,
    StaleShardLocation = 7,
    NonActingSetAccess = 8,
    UnsupportedOperation = 9,
    Internal = 10,
    ResourceExhausted = 11,
    ReclaimClaimNotFound = 12,
    ShardDeleteInProgress = 13,
    BucketWriteDrainConflict = 14,
    BucketWriteDrainNotFound = 15,
    ReclaimClaimConflict = 16,
    NotFound = 17,
    BucketWriteReservationConflict = 18,
    BucketWriteReservationNotFound = 19,
    MetadataCommandContention = 20,
    MetadataTransferHistoricalRouteActive = 21,
}

impl StorageRpcErrorCode {
    fn from_u16(value: u16) -> Result<Self, StorageRpcPayloadError> {
        match value {
            1 => Ok(Self::FrameDecode),
            2 => Ok(Self::PayloadDecode),
            3 => Ok(Self::UnknownNode),
            4 => Ok(Self::UnknownPg),
            5 => Ok(Self::WrongClusterEpoch),
            6 => Ok(Self::InactivePgRoute),
            7 => Ok(Self::StaleShardLocation),
            8 => Ok(Self::NonActingSetAccess),
            9 => Ok(Self::UnsupportedOperation),
            10 => Ok(Self::Internal),
            11 => Ok(Self::ResourceExhausted),
            12 => Ok(Self::ReclaimClaimNotFound),
            13 => Ok(Self::ShardDeleteInProgress),
            14 => Ok(Self::BucketWriteDrainConflict),
            15 => Ok(Self::BucketWriteDrainNotFound),
            16 => Ok(Self::ReclaimClaimConflict),
            17 => Ok(Self::NotFound),
            18 => Ok(Self::BucketWriteReservationConflict),
            19 => Ok(Self::BucketWriteReservationNotFound),
            20 => Ok(Self::MetadataCommandContention),
            21 => Ok(Self::MetadataTransferHistoricalRouteActive),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown storage RPC error code",
            )),
        }
    }
}

impl StorageRpcMessageKind {
    pub(crate) fn operation_name(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::MetadataCommand => "metadata command",
            Self::ShardWrite => "shard write",
            Self::ShardRepairWrite => "shard repair write",
            Self::ShardRead => "shard read",
            Self::ShardHistoricalRead => "shard historical read",
            Self::ShardReadRange => "shard read range",
            Self::ShardDelete => "shard delete",
            Self::ReadHandlesAcquire => "read handles acquire",
            Self::ReadHandlesRelease => "read handles release",
            Self::ClaimHeartbeat => "claim heartbeat",
            Self::ClaimRelease => "claim release",
            Self::ProofRelease => "proof release",
            Self::ShardAckRecord => "shard ack record",
            Self::ShardAckValidate => "shard ack validate",
            Self::ShardAckLoad => "shard ack load",
            Self::ShardAckHistoricalLoad => "shard ack historical load",
            Self::ShardAckDelete => "shard ack delete",
            Self::ShardScavengerListFiles => "shard scavenger list files",
            Self::ShardScavengerShardRows => "shard scavenger shard rows",
            Self::ShardScavengerPayloadReferences => "shard scavenger payload references",
            Self::ShardScavengerObservationRecord => "shard scavenger observation record",
            Self::ShardScavengerObservations => "shard scavenger observations",
            Self::ShardScavengerObservationResolve => "shard scavenger observation resolve",
            Self::MetadataCommandReplicaState => "metadata command replica state",
            Self::MetadataCommandAcceptance => "metadata command acceptance",
            Self::MetadataCommandAbandonAcceptance => "metadata command abandon acceptance",
            Self::MetadataCommandPendingSlotInsert => "metadata command pending slot insert",
            Self::MetadataCommandPendingSlotRemove => "metadata command pending slot remove",
            Self::MetadataCommandMaxLogIndex => "metadata command max log index",
            Self::MetadataCommandNextId => "metadata command next id",
            Self::MetadataCommandPendingEnvelope => "metadata command pending envelope",
            Self::MetadataCommandValidateReplayState => "metadata command validate replay state",
            Self::MetadataCommandValidateReplayStatePreservingPending => {
                "metadata command validate replay state preserving pending"
            }
            Self::MetadataCommandReplicaStateCanInitialize => {
                "metadata command replica state can initialize"
            }
            Self::MetadataCommandTransferStateAdopt => "metadata command transfer state adopt",
            Self::MetadataCommandTransferEmptyStateInitialize => {
                "metadata command transfer empty state initialize"
            }
            Self::MetadataCommandTransferMatchingStateInitialize => {
                "metadata command transfer matching state initialize"
            }
            Self::MetadataCommandTransferCheckpointBaseInstall => {
                "metadata command transfer checkpoint base install"
            }
            Self::MetadataCommandCheckpointExport => "metadata command checkpoint export",
            Self::MetadataCommandCheckpointCandidates => "metadata command checkpoint candidates",
            Self::MetadataCommandCheckpointRecordCurrent => {
                "metadata command checkpoint record current"
            }
            Self::MetadataCommandLogCompact => "metadata command log compact",
            Self::ClusterMapHistoryReferenceSummary => "cluster map history reference summary",
            Self::MetadataCommandAppliedLogHashes => "metadata command applied log hashes",
            Self::MetadataCommandMatchingAppliedLog => "metadata command matching applied log",
            Self::MetadataCommandRetainedLogHashes => "metadata command retained log hashes",
            Self::MetadataCommandRetainedLogEntries => "metadata command retained log entries",
            Self::MetadataCommandAbandoned => "metadata command abandoned",
            Self::MetadataCommandRecordAbandoned => "metadata command record abandoned",
            Self::MetadataCommandPendingSlotReplace => "metadata command pending slot replace",
            Self::MetadataCommandBucketControlPendingSlotInsert => {
                "metadata command bucket-control pending slot insert"
            }
            Self::MetadataCommandApplyAndRecord => "metadata command apply and record",
            Self::MetadataCommandPeeringReplayApplyAndRecord => {
                "metadata command peering replay apply and record"
            }
            Self::MetadataCommandPgLockAcquire => "metadata command PG lock acquire",
            Self::MetadataCommandPgLockRelease => "metadata command PG lock release",
            Self::BucketHeadRaw => "bucket head raw",
            Self::BucketHeadInfo => "bucket head info",
            Self::BucketCreateCommandBuild => "bucket create command build",
            Self::ObjectGenerationNext => "object generation next",
            Self::ObjectGenerationReservation => "object generation reservation",
            Self::ObjectVersionNext => "object version next",
            Self::BucketWriteReservationAcquire => "bucket write reservation acquire",
            Self::BucketWriteReservationValidate => "bucket write reservation validate",
            Self::BucketWriteReservationRelease => "bucket write reservation release",
            Self::BucketWriteReservationHeartbeat => "bucket write reservation heartbeat",
            Self::BucketSnapshotLoad => "bucket snapshot load",
            Self::BucketSnapshotPairLoad => "bucket snapshot pair load",
            Self::DirectPutCommitSnapshotLoad => "direct PUT commit snapshot load",
            Self::DirectPutCommitCommandBuild => "direct PUT commit command build",
            Self::CompletedMultipartOrderCommandBuild => "completed multipart order command build",
            Self::ObjectReadAuthSubjectLoad => "object read auth subject load",
            Self::ObjectReadSnapshotLoad => "object read snapshot load",
            Self::ObjectTagsForSubjectLoad => "object tags for subject load",
            Self::ObjectMetadataPutSnapshotLoad => "object metadata PUT snapshot load",
            Self::ObjectMetadataPutCommandBuild => "object metadata PUT command build",
            Self::ObjectDeleteCurrentSnapshotLoad => "object delete current snapshot load",
            Self::ObjectDeleteSpecificSnapshotLoad => "object delete specific snapshot load",
            Self::ObjectDeleteSpecificCommandBuild => "object delete specific command build",
            Self::ObjectDeleteCurrentCommandBuild => "object delete current command build",
            Self::ObjectInsertDeleteMarkerCommandBuild => {
                "object insert delete marker command build"
            }
            Self::ObjectLifecycleVersionListLoad => "object lifecycle version list load",
            Self::ObjectStreamUploadMatch => "object stream upload match",
            Self::ObjectMultipartUploadMatch => "object multipart upload match",
            Self::ObjectStreamUploadCommandBuild => "object stream upload command build",
            Self::ObjectMultipartUploadCommandBuild => "object multipart upload command build",
            Self::ObjectStreamPutFinalizeSnapshotLoad => "object stream PUT finalize snapshot load",
            Self::ObjectStreamPutCommitCommandBuild => "object stream PUT commit command build",
            Self::ObjectStreamPartFinalizeSnapshotLoad => {
                "object stream part finalize snapshot load"
            }
            Self::ObjectStreamPartCommitCommandBuild => "object stream part commit command build",
            Self::ObjectMultipartCompleteCommandBuild => "object multipart complete command build",
            Self::ObjectMultipartAbortCommandBuild => "object multipart abort command build",
            Self::ObjectMultipartAuthorizedAbortCommandBuild => {
                "object multipart authorized abort command build"
            }
            Self::ObjectMultipartCompletionStaleSourceLoad => {
                "object multipart completion stale source load"
            }
            Self::ObjectMultipartAbortCleanupLoad => "object multipart abort cleanup load",
            Self::ObjectMultipartUploadLoad => "object multipart upload load",
            Self::ObjectMultipartInProgressUploadLoad => "object multipart in-progress upload load",
            Self::ObjectMultipartInProgressUploadForListingLoad => {
                "object multipart in-progress upload listing load"
            }
            Self::ObjectMultipartCompletionSnapshotLoad => {
                "object multipart completion snapshot load"
            }
            Self::ObjectMultipartCompletionPreflightLoad => {
                "object multipart completion preflight load"
            }
            Self::ObjectMultipartPartsList => "object multipart parts list",
            Self::ObjectMultipartManagementLookup => "object multipart management lookup",
            Self::ObjectStreamUploadSessionLoad => "object stream upload session load",
            Self::ObjectStreamUploadSegmentsLoad => "object stream upload segments load",
            Self::ObjectStreamSegmentAppendPrepare => "object stream segment append prepare",
            Self::BucketWriteDrainBegin => "bucket write drain begin",
            Self::BucketWriteDrainClear => "bucket write drain clear",
            Self::BucketWriteDrainClearExpired => "bucket write drain clear expired",
            Self::BucketWriteDrainGet => "bucket write drain get",
            Self::BucketDeleteAttemptOutcomeRecord => "bucket delete attempt outcome record",
            Self::BucketDeleteAttemptOutcomeGet => "bucket delete attempt outcome get",
            Self::BucketWriteDrainHeartbeat => "bucket write drain heartbeat",
            Self::BucketWriteDrainExists => "bucket write drain exists",
            Self::BucketWriteReservationsList => "bucket write reservations list",
            Self::BucketDeleteFinalized => "bucket delete finalized",
            Self::BucketDeleteFinalizeRoots => "bucket delete finalize roots",
            Self::BucketDeleteBeginRoots => "bucket delete begin roots",
            Self::BucketDeleteFinalizeClaimAcquire => "bucket delete finalize claim acquire",
            Self::BucketDeleteFinalizeClaimRelease => "bucket delete finalize claim release",
            Self::BucketMetadataControlPendingMatch => "bucket metadata control pending match",
            Self::BucketMetadataControlCommandBuild => "bucket metadata control command build",
            Self::BucketSubresourceGet => "bucket subresource get",
            Self::LifecycleSweepBucketsList => "lifecycle sweep buckets list",
            Self::LifecycleSweepRoots => "lifecycle sweep roots",
            Self::LifecycleSweepClaimAcquire => "lifecycle sweep claim acquire",
            Self::LifecycleSweepClaimHeartbeat => "lifecycle sweep claim heartbeat",
            Self::LifecycleSweepClaimError => "lifecycle sweep claim error",
            Self::LifecycleSweepClaimRelease => "lifecycle sweep claim release",
            Self::ObjectListPage => "object list page",
            Self::ObjectVersionListPage => "object version list page",
            Self::ObjectMultipartUploadListPage => "object multipart upload list page",
            Self::BucketList => "bucket list",
            Self::BucketExecutionGenerations => "bucket execution generations",
            Self::BucketFastPathIdentities => "bucket fast path identities",
            Self::BucketMarkDeletingCommandBuild => "bucket mark deleting command build",
            Self::ObjectStreamUploadsList => "object stream uploads list",
            Self::ObjectStreamUploadsPgList => "object stream uploads PG list",
            Self::ObjectCompletedMultipartUploadsList => "object completed multipart uploads list",
            Self::ObjectPayloadReclaimExists => "object payload reclaim exists",
            Self::ObjectBucketPayloadReclaimRoot => "object bucket payload reclaim root",
            Self::ObjectPayloadReclaimRoot => "object payload reclaim root",
            Self::ObjectPayloadReclaimLoad => "object payload reclaim load",
            Self::ObjectPayloadReclaimClaimAcquire => "object payload reclaim claim acquire",
            Self::ObjectPayloadReclaimClaimRelease => "object payload reclaim claim release",
            Self::PlacedSegmentShardRepairRecord => "placed segment shard repair record",
            Self::PlacedSegmentShardRepairs => "placed segment shard repairs",
            Self::PlacedSegmentShardRepairResolve => "placed segment shard repair resolve",
            Self::PlacedSegmentShardRepairClaimAcquire => {
                "placed segment shard repair claim acquire"
            }
            Self::PlacedSegmentShardRepairClaimComplete => {
                "placed segment shard repair claim complete"
            }
            Self::PlacedSegmentShardRepairClaimError => "placed segment shard repair claim error",
            Self::PlacedSegmentShardBackfillRecord => "placed segment shard backfill record",
            Self::PlacedSegmentShardBackfills => "placed segment shard backfills",
            Self::PlacedSegmentShardBackfillResolve => "placed segment shard backfill resolve",
            Self::PlacedSegmentShardBackfillClaimAcquire => {
                "placed segment shard backfill claim acquire"
            }
            Self::PlacedSegmentShardBackfillClaimComplete => {
                "placed segment shard backfill claim complete"
            }
            Self::PlacedSegmentShardBackfillClaimError => {
                "placed segment shard backfill claim error"
            }
            Self::PlacedSegmentShardBackfillCount => "placed segment shard backfill count",
            Self::PlacedSegmentShardBackfillExists => "placed segment shard backfill exists",
        }
    }

    fn from_u16(value: u16) -> Result<Self, StorageRpcFrameError> {
        match value {
            1 => Ok(Self::Health),
            2 => Ok(Self::MetadataCommand),
            3 => Ok(Self::ShardWrite),
            4 => Ok(Self::ShardRead),
            5 => Ok(Self::ShardReadRange),
            6 => Ok(Self::ShardDelete),
            7 => Ok(Self::ReadHandlesAcquire),
            8 => Ok(Self::ReadHandlesRelease),
            9 => Ok(Self::ClaimHeartbeat),
            10 => Ok(Self::ClaimRelease),
            11 => Ok(Self::ProofRelease),
            12 => Ok(Self::ShardAckRecord),
            13 => Ok(Self::ShardAckValidate),
            14 => Ok(Self::ShardScavengerListFiles),
            15 => Ok(Self::MetadataCommandReplicaState),
            16 => Ok(Self::MetadataCommandAcceptance),
            17 => Ok(Self::MetadataCommandAbandonAcceptance),
            18 => Ok(Self::MetadataCommandPendingSlotInsert),
            19 => Ok(Self::MetadataCommandPendingSlotRemove),
            20 => Ok(Self::MetadataCommandMaxLogIndex),
            21 => Ok(Self::MetadataCommandNextId),
            22 => Ok(Self::MetadataCommandPendingEnvelope),
            23 => Ok(Self::MetadataCommandValidateReplayState),
            24 => Ok(Self::MetadataCommandValidateReplayStatePreservingPending),
            145 => Ok(Self::MetadataCommandReplicaStateCanInitialize),
            146 => Ok(Self::MetadataCommandTransferStateAdopt),
            147 => Ok(Self::MetadataCommandTransferEmptyStateInitialize),
            148 => Ok(Self::MetadataCommandTransferMatchingStateInitialize),
            149 => Ok(Self::MetadataCommandTransferCheckpointBaseInstall),
            150 => Ok(Self::MetadataCommandCheckpointExport),
            151 => Ok(Self::MetadataCommandCheckpointCandidates),
            152 => Ok(Self::MetadataCommandCheckpointRecordCurrent),
            153 => Ok(Self::MetadataCommandLogCompact),
            154 => Ok(Self::ClusterMapHistoryReferenceSummary),
            25 => Ok(Self::MetadataCommandAppliedLogHashes),
            26 => Ok(Self::MetadataCommandMatchingAppliedLog),
            27 => Ok(Self::MetadataCommandAbandoned),
            28 => Ok(Self::MetadataCommandRecordAbandoned),
            29 => Ok(Self::MetadataCommandPendingSlotReplace),
            30 => Ok(Self::MetadataCommandBucketControlPendingSlotInsert),
            31 => Ok(Self::MetadataCommandApplyAndRecord),
            32 => Ok(Self::BucketHeadRaw),
            33 => Ok(Self::BucketHeadInfo),
            34 => Ok(Self::BucketCreateCommandBuild),
            35 => Ok(Self::ObjectGenerationNext),
            36 => Ok(Self::ObjectGenerationReservation),
            37 => Ok(Self::ObjectVersionNext),
            38 => Ok(Self::BucketWriteReservationAcquire),
            39 => Ok(Self::BucketWriteReservationValidate),
            40 => Ok(Self::BucketWriteReservationRelease),
            123 => Ok(Self::BucketWriteReservationHeartbeat),
            41 => Ok(Self::BucketSnapshotLoad),
            42 => Ok(Self::BucketSnapshotPairLoad),
            43 => Ok(Self::DirectPutCommitSnapshotLoad),
            44 => Ok(Self::DirectPutCommitCommandBuild),
            45 => Ok(Self::CompletedMultipartOrderCommandBuild),
            46 => Ok(Self::ObjectReadAuthSubjectLoad),
            47 => Ok(Self::ObjectReadSnapshotLoad),
            48 => Ok(Self::ObjectTagsForSubjectLoad),
            49 => Ok(Self::ObjectMetadataPutSnapshotLoad),
            50 => Ok(Self::ObjectMetadataPutCommandBuild),
            51 => Ok(Self::ObjectDeleteCurrentSnapshotLoad),
            52 => Ok(Self::ObjectDeleteSpecificSnapshotLoad),
            53 => Ok(Self::ObjectDeleteSpecificCommandBuild),
            54 => Ok(Self::ObjectDeleteCurrentCommandBuild),
            55 => Ok(Self::ObjectInsertDeleteMarkerCommandBuild),
            56 => Ok(Self::ObjectLifecycleVersionListLoad),
            57 => Ok(Self::ObjectStreamUploadMatch),
            58 => Ok(Self::ObjectMultipartUploadMatch),
            59 => Ok(Self::ObjectStreamUploadCommandBuild),
            60 => Ok(Self::ObjectMultipartUploadCommandBuild),
            61 => Ok(Self::ObjectStreamPutFinalizeSnapshotLoad),
            62 => Ok(Self::ObjectStreamPutCommitCommandBuild),
            63 => Ok(Self::ObjectStreamPartFinalizeSnapshotLoad),
            64 => Ok(Self::ObjectStreamPartCommitCommandBuild),
            65 => Ok(Self::ObjectMultipartCompleteCommandBuild),
            66 => Ok(Self::ObjectMultipartAbortCommandBuild),
            67 => Ok(Self::ObjectMultipartAuthorizedAbortCommandBuild),
            68 => Ok(Self::ObjectMultipartCompletionStaleSourceLoad),
            69 => Ok(Self::ObjectMultipartAbortCleanupLoad),
            70 => Ok(Self::ObjectMultipartUploadLoad),
            71 => Ok(Self::ObjectMultipartInProgressUploadLoad),
            72 => Ok(Self::ObjectMultipartInProgressUploadForListingLoad),
            73 => Ok(Self::ObjectMultipartCompletionSnapshotLoad),
            74 => Ok(Self::ObjectMultipartCompletionPreflightLoad),
            75 => Ok(Self::ObjectMultipartPartsList),
            76 => Ok(Self::ObjectMultipartManagementLookup),
            77 => Ok(Self::ObjectStreamUploadSessionLoad),
            78 => Ok(Self::ObjectStreamUploadSegmentsLoad),
            79 => Ok(Self::ObjectStreamSegmentAppendPrepare),
            80 => Ok(Self::BucketWriteDrainBegin),
            81 => Ok(Self::BucketWriteDrainClear),
            82 => Ok(Self::BucketWriteDrainClearExpired),
            83 => Ok(Self::BucketWriteReservationsList),
            84 => Ok(Self::BucketDeleteFinalized),
            85 => Ok(Self::BucketDeleteFinalizeRoots),
            86 => Ok(Self::BucketDeleteFinalizeClaimAcquire),
            87 => Ok(Self::BucketDeleteFinalizeClaimRelease),
            88 => Ok(Self::BucketMetadataControlPendingMatch),
            89 => Ok(Self::BucketMetadataControlCommandBuild),
            90 => Ok(Self::BucketSubresourceGet),
            91 => Ok(Self::LifecycleSweepBucketsList),
            92 => Ok(Self::LifecycleSweepRoots),
            93 => Ok(Self::LifecycleSweepClaimAcquire),
            94 => Ok(Self::LifecycleSweepClaimHeartbeat),
            95 => Ok(Self::LifecycleSweepClaimError),
            96 => Ok(Self::LifecycleSweepClaimRelease),
            97 => Ok(Self::ObjectListPage),
            98 => Ok(Self::ObjectVersionListPage),
            99 => Ok(Self::ObjectMultipartUploadListPage),
            100 => Ok(Self::BucketList),
            101 => Ok(Self::BucketExecutionGenerations),
            102 => Ok(Self::BucketFastPathIdentities),
            103 => Ok(Self::BucketMarkDeletingCommandBuild),
            104 => Ok(Self::BucketWriteDrainExists),
            105 => Ok(Self::ObjectStreamUploadsList),
            106 => Ok(Self::ObjectCompletedMultipartUploadsList),
            107 => Ok(Self::ObjectPayloadReclaimExists),
            108 => Ok(Self::ObjectBucketPayloadReclaimRoot),
            109 => Ok(Self::ObjectPayloadReclaimRoot),
            110 => Ok(Self::ObjectPayloadReclaimLoad),
            111 => Ok(Self::ObjectPayloadReclaimClaimAcquire),
            112 => Ok(Self::ObjectPayloadReclaimClaimRelease),
            113 => Ok(Self::ShardAckLoad),
            114 => Ok(Self::ShardAckDelete),
            115 => Ok(Self::ShardScavengerShardRows),
            116 => Ok(Self::ShardScavengerPayloadReferences),
            117 => Ok(Self::ShardScavengerObservationRecord),
            118 => Ok(Self::ShardScavengerObservations),
            119 => Ok(Self::ShardScavengerObservationResolve),
            120 => Ok(Self::ObjectStreamUploadsPgList),
            121 => Ok(Self::MetadataCommandPgLockAcquire),
            122 => Ok(Self::MetadataCommandPgLockRelease),
            155 => Ok(Self::BucketWriteDrainGet),
            156 => Ok(Self::BucketDeleteAttemptOutcomeRecord),
            157 => Ok(Self::BucketDeleteAttemptOutcomeGet),
            158 => Ok(Self::BucketDeleteBeginRoots),
            124 => Ok(Self::BucketWriteDrainHeartbeat),
            125 => Ok(Self::MetadataCommandRetainedLogHashes),
            126 => Ok(Self::MetadataCommandRetainedLogEntries),
            127 => Ok(Self::MetadataCommandPeeringReplayApplyAndRecord),
            128 => Ok(Self::PlacedSegmentShardRepairRecord),
            129 => Ok(Self::PlacedSegmentShardRepairs),
            130 => Ok(Self::PlacedSegmentShardRepairResolve),
            131 => Ok(Self::PlacedSegmentShardRepairClaimAcquire),
            132 => Ok(Self::PlacedSegmentShardRepairClaimComplete),
            133 => Ok(Self::PlacedSegmentShardRepairClaimError),
            134 => Ok(Self::ShardRepairWrite),
            135 => Ok(Self::PlacedSegmentShardBackfillRecord),
            136 => Ok(Self::PlacedSegmentShardBackfills),
            137 => Ok(Self::PlacedSegmentShardBackfillResolve),
            138 => Ok(Self::PlacedSegmentShardBackfillClaimAcquire),
            139 => Ok(Self::PlacedSegmentShardBackfillClaimComplete),
            140 => Ok(Self::PlacedSegmentShardBackfillClaimError),
            141 => Ok(Self::PlacedSegmentShardBackfillCount),
            142 => Ok(Self::ShardHistoricalRead),
            143 => Ok(Self::PlacedSegmentShardBackfillExists),
            144 => Ok(Self::ShardAckHistoricalLoad),
            _ => Err(StorageRpcFrameError::UnknownMessageKind(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcFrame {
    pub(crate) request_id: u64,
    pub(crate) kind: StorageRpcMessageKind,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcHealthResponse {
    pub(crate) protocol_version: u16,
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcErrorResponse {
    pub(crate) code: StorageRpcErrorCode,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StorageRpcFrameError {
    #[error("storage RPC payload length {len} exceeds limit {limit}")]
    PayloadTooLarge { len: usize, limit: usize },
    #[error("truncated storage RPC frame")]
    Truncated,
    #[error("storage RPC frame contains trailing bytes")]
    TrailingBytes,
    #[error("unknown storage RPC frame magic")]
    UnknownMagic,
    #[error("unsupported storage RPC frame encoding version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown storage RPC message kind {0}")]
    UnknownMessageKind(u16),
    #[error("storage RPC payload checksum mismatch")]
    PayloadChecksumMismatch,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StorageRpcStreamError {
    #[error("storage RPC stream I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Frame(#[from] StorageRpcFrameError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StorageRpcPayloadError {
    #[error("truncated storage RPC payload")]
    Truncated,
    #[error("storage RPC payload contains trailing bytes")]
    TrailingBytes,
    #[error("storage RPC payload length {len} exceeds limit {limit}")]
    PayloadTooLarge { len: usize, limit: usize },
    #[error("invalid metadata command envelope")]
    InvalidMetadataCommandEnvelope,
    #[error("metadata command checksum mismatch")]
    MetadataCommandChecksumMismatch,
    #[error("metadata command route mismatch: {0}")]
    MetadataCommandRouteMismatch(&'static str),
    #[error("invalid metadata command pending slot request: {0}")]
    InvalidMetadataCommandPendingSlotRequest(&'static str),
    #[error("invalid metadata command log compaction status {0}")]
    InvalidMetadataCommandLogCompactionStatus(u8),
    #[error("invalid bucket metadata request: {0}")]
    InvalidBucketMetadataRequest(&'static str),
    #[error("invalid object metadata request: {0}")]
    InvalidObjectMetadataRequest(&'static str),
    #[error("shard write size mismatch: expected {expected}, actual {actual}")]
    ShardWriteSizeMismatch { expected: u64, actual: u64 },
    #[error("shard write checksum mismatch")]
    ShardWriteChecksumMismatch,
    #[error("shard location shard index does not match shard key")]
    ShardLocationMismatch,
    #[error("invalid read handle acquire request: {0}")]
    InvalidReadHandleAcquireRequest(&'static str),
    #[error("invalid read handle release request: {0}")]
    InvalidReadHandleReleaseRequest(&'static str),
    #[error("invalid shard ack batch request: {0}")]
    InvalidShardAckBatchRequest(&'static str),
    #[error("invalid durable claim token: {0}")]
    InvalidDurableClaimToken(&'static str),
    #[error("invalid bucket write reservation proof: {0}")]
    InvalidBucketWriteReservationProof(&'static str),
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    #[error("invalid checksum metadata: {0}")]
    InvalidChecksumMetadata(&'static str),
    #[error("invalid response envelope: {0}")]
    InvalidResponseEnvelope(&'static str),
    #[error("invalid {field}: count {count} exceeds maximum {max}")]
    InvalidCount {
        field: &'static str,
        count: u64,
        max: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandItem {
    pub(crate) command_checksum: u64,
    pub(crate) command_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketPgRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketListRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) owner_canonical_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketListResponse {
    pub(crate) buckets: Vec<BucketInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketBatchRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) buckets: Vec<BucketName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketExecutionGenerationsResponse {
    pub(crate) generations: HashMap<BucketName, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketFastPathIdentitiesResponse {
    pub(crate) identities: HashMap<BucketName, BucketFastPathIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainBeginRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) drain_id: String,
    pub(crate) owner_token: String,
    pub(crate) created_at: u64,
    pub(crate) lease_deadline: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcBucketWriteDrainBeginOutcome {
    Acquired(BucketWriteDrainRecord),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainBeginResponse {
    pub(crate) outcome: StorageRpcBucketWriteDrainBeginOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketWriteDrainRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainHeartbeatRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketWriteDrainRecord,
    pub(crate) lease_deadline: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainClearExpiredRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainOptionalRecordResponse {
    pub(crate) record: Option<BucketWriteDrainRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketDeleteAttemptOutcomeRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse {
    pub(crate) record: Option<BucketDeleteAttemptOutcomeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationsListResponse {
    pub(crate) records: Vec<BucketWriteReservationRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcBucketDeleteFinalizedOutcome {
    Deleted,
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizedResponse {
    pub(crate) outcome: StorageRpcBucketDeleteFinalizedOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizeRootsRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) now: u64,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizeRootsResponse {
    pub(crate) roots: Vec<BucketDeleteFinalizeRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteBeginRootsRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) now: u64,
    pub(crate) start_after_bucket: Option<BucketName>,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteBeginRootsResponse {
    pub(crate) roots: Vec<BucketDeleteBeginRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizeClaimAcquireRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse {
    pub(crate) record: Option<BucketDeleteFinalizeClaimRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketDeleteFinalizeClaimRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketDeleteFinalizeClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectGenerationReservationRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) reservation_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimExistsRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) generation_id: GenerationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimResponse {
    pub(crate) reclaim: Option<ObjectPayloadReclaimCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimClaimAcquireRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) generation_id: GenerationId,
    pub(crate) reclaim_kind: ObjectPayloadReclaimKind,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse {
    pub(crate) record: Option<ObjectPayloadReclaimClaimRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimClaimRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: ObjectPayloadReclaimClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPayloadReclaimRootResponse {
    pub(crate) root: Option<PayloadReclaimRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcDirectPutCommitSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) reservation_id: SessionId,
    pub(crate) generation_id: GenerationId,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcDirectPutCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CommitDirectPutObjectReq,
    pub(crate) version_id: VersionId,
    pub(crate) expected_snapshot: DirectPutCommitStorageSnapshot,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectGenerationResponse {
    pub(crate) generation_id: GenerationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectVersionResponse {
    pub(crate) version_id: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcObjectGenerationReservationOutcome {
    Found(GenerationId),
    NotFound { reservation_id: SessionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectGenerationReservationResponse {
    pub(crate) outcome: StorageRpcObjectGenerationReservationOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcDirectPutCommitSnapshotResponse {
    pub(crate) snapshot: DirectPutCommitStorageSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectReadAuthSubjectRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: Option<VersionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcObjectReadAuthSubjectOutcome {
    Loaded(Box<ObjectReadAuthSubject>),
    ObjectNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectReadAuthSubjectResponse {
    pub(crate) outcome: StorageRpcObjectReadAuthSubjectOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectReadSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: Option<VersionId>,
    pub(crate) expected_identity: ObjectReadAuthSubjectIdentity,
    pub(crate) snapshot_mode: ObjectReadSnapshotMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcObjectReadSnapshotOutcome {
    Loaded(Box<ObjectReadSnapshot>),
    StaleSubject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectReadSnapshotResponse {
    pub(crate) outcome: StorageRpcObjectReadSnapshotOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectTagsForSubjectRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: Option<VersionId>,
    pub(crate) expected_identity: ObjectReadAuthSubjectIdentity,
    pub(crate) authorized_version_id: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcObjectTagsForSubjectOutcome {
    Loaded(Option<String>),
    StaleSubject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectTagsForSubjectResponse {
    pub(crate) outcome: StorageRpcObjectTagsForSubjectOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPutObjectMetadataSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: Option<VersionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcPutObjectMetadataSnapshotOutcome {
    Loaded(Box<StoredObject>),
    ObjectNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPutObjectMetadataSnapshotResponse {
    pub(crate) outcome: StorageRpcPutObjectMetadataSnapshotOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPutObjectMetadataCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) requested_version_id: Option<VersionId>,
    pub(crate) expected_stored: StoredObject,
    pub(crate) version_id: VersionId,
    pub(crate) mutation: PutObjectMetadataMutation,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcObjectMetadataCommandBuildOutcome {
    Command(Box<crate::metadata_command::MetadataCommandEnvelope>),
    StaleSnapshot,
    Missing,
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectMetadataCommandBuildResponse {
    pub(crate) outcome: StorageRpcObjectMetadataCommandBuildOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectDeleteSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: Option<VersionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectDeleteSnapshotResponse {
    pub(crate) stored: Option<StoredObject>,
    pub(crate) target: Option<DeleteObjectVersionTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectLifecycleVersionListResponse {
    pub(crate) versions: Vec<StoredObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartCompletionStaleSourceResponse {
    pub(crate) source: Option<StoredObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartUploadLoadRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMultipartUploadLoadOutcome {
    Loaded(Box<MultipartUploadRecord>),
    NoSuchUpload { upload_id: UploadId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartUploadLoadResponse {
    pub(crate) outcome: StorageRpcMultipartUploadLoadOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartCompletionSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) authorized_upload: MultipartUploadRecord,
    pub(crate) requested_part_numbers: Vec<u32>,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcMultipartCompletionSnapshotOutcome {
    Loaded(Box<MultipartCompletionSnapshot>),
    NoSuchUpload {
        upload_id: UploadId,
    },
    PartNotFound {
        upload_id: UploadId,
        part_number: u32,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcMultipartCompletionSnapshotResponse {
    pub(crate) outcome: StorageRpcMultipartCompletionSnapshotOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartCompletionPreflightRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) authorized_upload: MultipartUploadRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMultipartCompletionPreflightOutcome {
    Loaded(MultipartCompletionPreflight),
    NoSuchUpload { upload_id: UploadId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartCompletionPreflightResponse {
    pub(crate) outcome: StorageRpcMultipartCompletionPreflightOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartPartsListRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) authorized_upload: MultipartUploadRecord,
    pub(crate) part_number_marker: Option<u32>,
    pub(crate) max_parts: u32,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcMultipartPartsListOutcome {
    Loaded(Box<ListedMultipartParts>),
    NoSuchUpload { upload_id: UploadId },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcMultipartPartsListResponse {
    pub(crate) outcome: StorageRpcMultipartPartsListOutcome,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcMultipartManagementLookupResponse {
    pub(crate) lookup: MultipartUploadManagementLookup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcDeleteSpecificObjectCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) version_id: VersionId,
    pub(crate) expected_stored: Option<StoredObject>,
    pub(crate) expected_target: Option<DeleteObjectVersionTarget>,
    pub(crate) expected_version_list: Option<Vec<StoredObject>>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcDeleteCurrentObjectCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) expected_current: Option<StoredObject>,
    pub(crate) expected_target: Option<DeleteObjectVersionTarget>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcInsertDeleteMarkerStalePayload {
    Explicit(Option<ObjectPayloadReclaimCommand>),
    SnapshotCurrentNullLive { created_at: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcInsertDeleteMarkerCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) expected_current: Option<StoredObject>,
    pub(crate) expected_stale_payload_source: Option<StoredObject>,
    pub(crate) version_id: VersionId,
    pub(crate) owner: OwnerIdentity,
    pub(crate) stale_payload: StorageRpcInsertDeleteMarkerStalePayload,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcCreateStreamUploadPrecondition {
    PutObjectNoCurrentCheck {
        require_generation_reservation: bool,
    },
    PutObject {
        expected_current: Option<StoredObject>,
        require_generation_reservation: bool,
    },
    UploadPart {
        expected_upload: MultipartUploadRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadMatchRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CreateStreamUploadReq,
    pub(crate) expected_command: Option<CreateStreamUploadCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadMatchResponse {
    pub(crate) exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadSessionRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcStreamUploadSessionOutcome {
    Loaded(Box<StreamUploadRecord>),
    NotFound { session_id: SessionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadSessionResponse {
    pub(crate) outcome: StorageRpcStreamUploadSessionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcStreamUploadSegmentsOutcome {
    Loaded(Vec<StreamUploadSegmentRecord>),
    NotFound { session_id: SessionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadSegmentsResponse {
    pub(crate) outcome: StorageRpcStreamUploadSegmentsOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadsListRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) session_id_marker: Option<SessionId>,
    pub(crate) limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadsPgListRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) session_id_marker: Option<SessionId>,
    pub(crate) limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamUploadsListResponse {
    pub(crate) uploads: Vec<StreamUploadRecord>,
    pub(crate) next_session_id_marker: Option<SessionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCompletedMultipartUploadsListRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) upload_id_marker: Option<UploadId>,
    pub(crate) limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCompletedMultipartUploadsListResponse {
    pub(crate) records: Vec<CompletedMultipartUploadRecord>,
    pub(crate) next_upload_id_marker: Option<UploadId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamSegmentAppendPrepareRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: PrepareStreamUploadSegmentAppendReq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcStreamSegmentAppendPrepareOutcome {
    Prepared {
        target: StreamUploadTarget,
        segment: Box<StreamUploadSegmentRecord>,
    },
    NotFound {
        session_id: SessionId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamSegmentAppendPrepareResponse {
    pub(crate) outcome: StorageRpcStreamSegmentAppendPrepareOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartUploadMatchRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CreateMultipartUploadReq,
    pub(crate) expected_command: Option<CreateMultipartUploadCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMultipartUploadMatchResponse {
    pub(crate) initiated_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCreateStreamUploadCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CreateStreamUploadReq,
    pub(crate) precondition: StorageRpcCreateStreamUploadPrecondition,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCreateMultipartUploadCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CreateMultipartUploadReq,
    pub(crate) expected_current: Option<StoredObject>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPutFinalizeSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPutFinalizeSnapshotResponse {
    pub(crate) snapshot: StreamPutFinalizeStorageSnapshot,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcStreamPutCommitCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) session_id: SessionId,
    pub(crate) total_size: u64,
    pub(crate) expected_snapshot: StreamPutFinalizeStorageSnapshot,
    pub(crate) commit: StreamPutCommitInput,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPartFinalizeSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
    pub(crate) session_id: SessionId,
    pub(crate) part_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPartFinalizeSnapshotResponse {
    pub(crate) snapshot: StreamUploadPartStorageSnapshot,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcStreamPartCommitCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
    pub(crate) session_id: SessionId,
    pub(crate) part_number: u32,
    pub(crate) expected_snapshot: StreamUploadPartStorageSnapshot,
    pub(crate) part: MultipartPartRecord,
    pub(crate) segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcCompleteMultipartCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CompleteMultipartCommitRequest,
    pub(crate) version_id: VersionId,
    pub(crate) completion_order: u64,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAbortMultipartCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
    pub(crate) expected_cleanup: Option<AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAbortMultipartCleanupRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAbortMultipartCleanupResponse {
    pub(crate) cleanup: Option<AbortMultipartUploadCleanup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) authorized_upload: MultipartUploadRecord,
    pub(crate) expected_cleanup: Option<AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

fn object_payload_reclaim_matches_object(
    reclaim: &ObjectPayloadReclaimCommand,
    bucket: &BucketName,
    key: &ObjectKey,
) -> bool {
    match reclaim {
        ObjectPayloadReclaimCommand::Segments(reclaim) => {
            reclaim.bucket == *bucket && reclaim.key == *key
        }
        ObjectPayloadReclaimCommand::Multipart(reclaim) => {
            reclaim.bucket == *bucket && reclaim.key == *key
        }
    }
}

fn delete_target_matches_object(
    target: &DeleteObjectVersionTarget,
    bucket: &BucketName,
    key: &ObjectKey,
) -> bool {
    match target {
        DeleteObjectVersionTarget::DeleteMarker { .. } => true,
        DeleteObjectVersionTarget::Live { payload, .. } => {
            object_payload_reclaim_matches_object(payload, bucket, key)
        }
    }
}

fn object_payload_reclaim_matches_snapshot_live_object(
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

fn validate_create_stream_upload_request_identity(
    object: &StorageRpcObjectRequest,
    request: &CreateStreamUploadReq,
) -> Result<(), StorageRpcPayloadError> {
    if request.bucket != object.bucket || request.key != object.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload request identity mismatch",
        ));
    }
    match &request.target {
        StreamUploadTarget::PutObject => {}
        StreamUploadTarget::UploadPart { .. } => {}
    }
    Ok(())
}

fn validate_create_multipart_upload_request_identity(
    object: &StorageRpcObjectRequest,
    request: &CreateMultipartUploadReq,
) -> Result<(), StorageRpcPayloadError> {
    if request.bucket != object.bucket || request.key != object.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload request identity mismatch",
        ));
    }
    Ok(())
}

fn validate_multipart_upload_record_identity(
    object: &StorageRpcObjectRequest,
    upload: &MultipartUploadRecord,
    error: &'static str,
) -> Result<(), StorageRpcPayloadError> {
    if upload.bucket != object.bucket || upload.key != object.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
    }
    Ok(())
}

fn validate_complete_multipart_request_identity(
    object: &StorageRpcObjectRequest,
    request: &CompleteMultipartCommitRequest,
) -> Result<(), StorageRpcPayloadError> {
    if request.bucket != object.bucket || request.key != object.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "complete multipart request identity mismatch",
        ));
    }
    for part in &request.part_records {
        if part.upload_id != request.upload_id {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart part identity mismatch",
            ));
        }
    }
    for segment in request
        .selected_streaming_segments
        .iter()
        .chain(request.expected_cleanup.omitted_streaming_segments.iter())
    {
        if segment.bucket != object.bucket
            || segment.key != object.key
            || segment.upload_id != request.upload_id
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart segment identity mismatch",
            ));
        }
    }
    for part in &request.expected_cleanup.omitted_parts {
        if part.upload_id != request.upload_id {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart omitted part identity mismatch",
            ));
        }
    }
    validate_terminal_stream_cleanup_identity(
        &request.expected_cleanup.stream_uploads,
        &request.expected_cleanup.stream_upload_segments,
        object,
        &request.upload_id,
        "complete multipart stream cleanup identity mismatch",
    )?;
    if let Some(stored) = request.expected_stale_payload_source.as_ref() {
        let Some(live) = stored.as_live() else {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart stale source must be live",
            ));
        };
        if live.bucket != object.bucket || live.key != object.key || !live.version_id.is_null() {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart stale source identity mismatch",
            ));
        }
    }
    Ok(())
}

fn validate_terminal_stream_cleanup_identity(
    stream_uploads: &[TerminalStreamCleanupRecord],
    stream_upload_segments: &[StreamUploadSegmentRecord],
    object: &StorageRpcObjectRequest,
    upload_id: &UploadId,
    error: &'static str,
) -> Result<(), StorageRpcPayloadError> {
    for stream in stream_uploads {
        match &stream.target {
            StreamUploadTarget::UploadPart {
                upload_id: stream_upload_id,
                ..
            } if stream_upload_id == upload_id => {}
            _ => return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error)),
        }
        if stream.bucket != object.bucket || stream.key != object.key {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
        }
    }
    for segment in stream_upload_segments {
        if !stream_uploads
            .iter()
            .any(|stream| stream.session_id == segment.session_id)
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
        }
    }
    Ok(())
}

fn validate_abort_multipart_cleanup_identity(
    cleanup: &AbortMultipartUploadCleanup,
    object: &StorageRpcObjectRequest,
    upload_id: &UploadId,
    error: &'static str,
) -> Result<(), StorageRpcPayloadError> {
    if cleanup.upload.bucket != object.bucket
        || cleanup.upload.key != object.key
        || cleanup.upload.upload_id != *upload_id
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
    }
    for part in &cleanup.parts {
        if part.upload_id != *upload_id {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
        }
    }
    for segment in &cleanup.streaming_segments {
        if segment.bucket != object.bucket
            || segment.key != object.key
            || segment.upload_id != *upload_id
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(error));
        }
    }
    validate_terminal_stream_cleanup_identity(
        &cleanup.stream_uploads,
        &cleanup.stream_upload_segments,
        object,
        upload_id,
        error,
    )
}

fn validate_create_stream_upload_precondition_identity(
    object: &StorageRpcObjectRequest,
    precondition: &StorageRpcCreateStreamUploadPrecondition,
) -> Result<(), StorageRpcPayloadError> {
    match precondition {
        StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck { .. } => Ok(()),
        StorageRpcCreateStreamUploadPrecondition::PutObject {
            expected_current, ..
        } => {
            if expected_current.as_ref().is_some_and(|stored| {
                stored.bucket() != &object.bucket || stored.key() != &object.key
            }) {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "stream upload PUT precondition identity mismatch",
                ));
            }
            Ok(())
        }
        StorageRpcCreateStreamUploadPrecondition::UploadPart { expected_upload } => {
            if expected_upload.bucket != object.bucket || expected_upload.key != object.key {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "stream upload part precondition identity mismatch",
                ));
            }
            Ok(())
        }
    }
}

fn create_stream_upload_command_matches_request(
    command: &CreateStreamUploadCommand,
    request: &CreateStreamUploadReq,
) -> bool {
    command.session.session_id == request.session_id
        && command.session.bucket == request.bucket
        && command.session.key == request.key
        && command.session.target == request.target
        && command.session.state == StreamUploadState::InProgress
        && command.session.encryption == request.encryption
        && command.initial_next_segment_vid == GenerationId::MIN
}

fn create_multipart_upload_command_matches_request(
    command: &CreateMultipartUploadCommand,
    request: &CreateMultipartUploadReq,
) -> bool {
    command.upload.upload_id == request.upload_id
        && command.upload.bucket == request.bucket
        && command.upload.key == request.key
        && command.upload.state == UploadState::InProgress
        && command.upload.tags == request.tags
        && command.upload.metadata_blob == request.metadata_blob
        && command.upload.system_metadata_blob == request.system_metadata_blob
        && command.upload.initiator == request.initiator
        && command.upload.owner == request.owner
        && command.upload.acl_grants == request.acl_grants
        && command.upload.public_read == request.public_read
        && command.upload.object_lock == request.object_lock
        && command.upload.checksum == request.checksum
        && command.upload.encryption == request.encryption
}

fn validate_stream_put_finalize_snapshot_identity(
    object: &StorageRpcObjectRequest,
    session_id: &SessionId,
    snapshot: &StreamPutFinalizeStorageSnapshot,
) -> Result<(), StorageRpcPayloadError> {
    if snapshot.session.session_id != *session_id
        || snapshot.session.bucket != object.bucket
        || snapshot.session.key != object.key
        || snapshot.session.target != StreamUploadTarget::PutObject
        || snapshot.session.state != StreamUploadState::InProgress
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT finalize snapshot identity mismatch",
        ));
    }
    for segment in &snapshot.staging_segments {
        if segment.session_id != *session_id {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream PUT finalize segment identity mismatch",
            ));
        }
    }
    if snapshot
        .stale_payload_source
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || snapshot.stale_payload.as_ref().is_some_and(|reclaim| {
            !object_payload_reclaim_matches_object(reclaim, &object.bucket, &object.key)
        })
        || !object_payload_reclaim_matches_snapshot_live_object(
            snapshot.stale_payload.as_ref(),
            &snapshot.stale_payload_source,
        )
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT finalize stale payload identity mismatch",
        ));
    }
    Ok(())
}

fn validate_stream_part_finalize_snapshot_identity(
    object: &StorageRpcObjectRequest,
    upload_id: &UploadId,
    session_id: &SessionId,
    part_number: u32,
    snapshot: &StreamUploadPartStorageSnapshot,
) -> Result<(), StorageRpcPayloadError> {
    let auth = &snapshot.auth_snapshot;
    if auth.session.session_id != *session_id
        || auth.session.bucket != object.bucket
        || auth.session.key != object.key
        || auth.session.target
            != (StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number,
            })
        || auth.session.state != StreamUploadState::InProgress
        || auth.upload.upload_id != *upload_id
        || auth.upload.bucket != object.bucket
        || auth.upload.key != object.key
        || auth.upload.state != UploadState::InProgress
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part finalize snapshot identity mismatch",
        ));
    }
    for segment in &auth.staging_segments {
        if segment.session_id != *session_id {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part finalize staging segment identity mismatch",
            ));
        }
    }
    if snapshot
        .existing_part
        .as_ref()
        .is_some_and(|part| part.upload_id != *upload_id || part.part_number != part_number)
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part finalize existing part identity mismatch",
        ));
    }
    for segment in &snapshot.displaced_segments {
        if segment.bucket != object.bucket
            || segment.key != object.key
            || segment.upload_id != *upload_id
            || segment.part_number != part_number
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part finalize displaced segment identity mismatch",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcDirectPutCommandBuildOutcome {
    Command(Box<crate::metadata_command::MetadataCommandEnvelope>),
    StaleSnapshot,
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcDirectPutCommandBuildResponse {
    pub(crate) outcome: StorageRpcDirectPutCommandBuildOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCreateBucketConfig {
    pub(crate) name: BucketName,
    pub(crate) owner_principal: String,
    pub(crate) owner_canonical_id: CanonicalUserId,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) versioning: BucketVersioningState,
    pub(crate) object_lock: BucketObjectLockConfig,
    pub(crate) ownership_controls: BucketOwnershipControls,
}

impl StorageRpcCreateBucketConfig {
    pub(crate) fn as_create_bucket_config(&self) -> CreateBucketConfig<'_> {
        CreateBucketConfig {
            name: self.name.as_str(),
            owner_principal: &self.owner_principal,
            owner_canonical_id: &self.owner_canonical_id,
            acl_grants: &self.acl_grants,
            public_read: self.public_read,
            public_write: self.public_write,
            versioning: self.versioning,
            object_lock: self.object_lock,
            ownership_controls: self.ownership_controls,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCreateBucketCommandBuildRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
    pub(crate) command_id: crate::metadata_command::MetadataCommandId,
    pub(crate) config: StorageRpcCreateBucketConfig,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcBucketInfoOutcome {
    Info(BucketInfo),
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketInfoOutcomeResponse {
    pub(crate) outcome: StorageRpcBucketInfoOutcome,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcCreateBucketCommandBuildOutcome {
    Exists(BucketInfo),
    Command(Box<crate::metadata_command::MetadataCommandEnvelope>),
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcCreateBucketCommandBuildResponse {
    pub(crate) outcome: StorageRpcCreateBucketCommandBuildOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcCompletedMultipartOrderCommandBuildRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
    pub(crate) command_id: crate::metadata_command::MetadataCommandId,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcCompletedMultipartOrderCommandBuildResponse {
    pub(crate) completion_order: u64,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcBucketMetadataControlMutation {
    MarkDeleting,
    Versioning(BucketVersioningState),
    Acl {
        acl_grants: AclGrants,
        public_read: bool,
        public_write: bool,
    },
    Property(BucketPropertyMutation),
    Subresource(BucketSubresourceMutation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketMetadataControlPendingMatchRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) mutation: StorageRpcBucketMetadataControlMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketMetadataControlCommandBuildRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) command_id: crate::metadata_command::MetadataCommandId,
    pub(crate) mutation: StorageRpcBucketMetadataControlMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketMetadataControlCommandBuildResponse {
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketMarkDeletingCommandBuildRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) command_id: crate::metadata_command::MetadataCommandId,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcBucketMarkDeletingCommandBuildOutcome {
    AlreadyDeleting(BucketInfo),
    Command(Box<crate::metadata_command::MetadataCommandEnvelope>),
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketMarkDeletingCommandBuildResponse {
    pub(crate) outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketSubresourceGetRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) kind: BucketSubresourceKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketSubresourceGetResponse {
    pub(crate) body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepRootsRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) now: u64,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepRootsResponse {
    pub(crate) roots: Vec<LifecycleSweepRoot>,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcLifecycleSweepBucketsResponse {
    pub(crate) buckets: LifecycleSweepBuckets,
}

pub(crate) struct StorageRpcListObjectsRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) request: ListObjectsReq,
}

pub(crate) struct StorageRpcListObjectsResponse {
    pub(crate) response: ListObjectsResp,
}

pub(crate) struct StorageRpcListObjectVersionsRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) request: ListObjectVersionsReq,
}

pub(crate) struct StorageRpcListObjectVersionsResponse {
    pub(crate) response: ListObjectVersionsResp,
}

pub(crate) struct StorageRpcListMultipartUploadsRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) request: ListMultipartUploadsReq,
}

pub(crate) struct StorageRpcListMultipartUploadsResponse {
    pub(crate) response: ListMultipartUploadsResp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimAcquireRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) claim: LifecycleSweepClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimHeartbeatRequest {
    pub(crate) record: StorageRpcLifecycleSweepClaimRecordRequest,
    pub(crate) heartbeat_at: u64,
    pub(crate) lease_deadline: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimErrorRequest {
    pub(crate) record: StorageRpcLifecycleSweepClaimRecordRequest,
    pub(crate) last_error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimOptionalRecordResponse {
    pub(crate) record: Option<LifecycleSweepClaimRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcLifecycleSweepClaimRecordResponse {
    pub(crate) record: LifecycleSweepClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandPendingSlotRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) scope_bucket: Option<BucketName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandPendingSlotReplaceRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) previous: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) replacement: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) scope_bucket: Option<BucketName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandPendingSlotInsertOutcome {
    Inserted,
    PendingConflict {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        existing_log_index: u64,
        candidate_log_index: u64,
    },
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandPendingSlotInsertResponse {
    pub(crate) outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandPendingSlotRemoveResponse {
    pub(crate) removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandNextIdRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) min_log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandMaxLogIndexResponse {
    pub(crate) max_log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandNextIdOutcome {
    Allocated {
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: u64,
    },
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandNextIdResponse {
    pub(crate) outcome: StorageRpcMetadataCommandNextIdOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandPendingEnvelopeResponse {
    pub(crate) command: Option<crate::metadata_command::MetadataCommandEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandMatchingAppliedRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) expected_previous_log_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandAppliedHashesOutcome {
    Hashes(Option<(u64, u64)>),
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandAppliedHashesResponse {
    pub(crate) outcome: StorageRpcMetadataCommandAppliedHashesOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandBoolResponse {
    pub(crate) value: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandBoolOutcome {
    Value(bool),
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandBoolOutcomeResponse {
    pub(crate) outcome: StorageRpcMetadataCommandBoolOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandStateOutcome {
    State(MetadataCommandReplicaState),
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
    ObjectGenerationReservationConflict {
        reservation_id: SessionId,
        generation_id: GenerationId,
    },
    ObjectVersionReservationConflict {
        version_id: VersionId,
    },
    StaleBucketMetadataCommand {
        name: BucketName,
        bucket_execution_generation: u64,
    },
    StaleObjectWriteCommand {
        bucket: BucketName,
        key: ObjectKey,
        write_sequence: u64,
        generation_id: Option<GenerationId>,
    },
    StreamSegmentConflict {
        segment_index: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandStateOutcomeResponse {
    pub(crate) outcome: StorageRpcMetadataCommandStateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandStateRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferAdoptRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) expected_state_digest: u64,
    pub(crate) commands: Vec<MetadataTransferCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferEmptyStateRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) expected_state_digest: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferMatchingStateRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) applied_log_index: u64,
    pub(crate) applied_log_hash: u64,
    pub(crate) expected_state_digest: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferCheckpointBaseRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) checkpoint: MetadataCommandCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandCheckpointResponse {
    pub(crate) checkpoint: MetadataCommandCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandCheckpointCandidatesRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) max_applied_log_index: u64,
    pub(crate) limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandCheckpointCandidatesResponse {
    pub(crate) checkpoints: Vec<MetadataCommandCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandLogCompactResponse {
    pub(crate) status: MetadataCommandLogCompactionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcClusterMapHistoryReferenceSummaryRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcClusterMapHistoryReferenceSummaryResponse {
    pub(crate) summary: PgClusterMapHistoryReferenceSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandLogHashRangeRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) first_log_index: MetadataCommandLogIndex,
    pub(crate) last_log_index: MetadataCommandLogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandLogHashRangeResponse {
    pub(crate) entries: Vec<MetadataCommandLogHashRangeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandLogEntryRangeResponse {
    pub(crate) entries: Vec<MetadataCommandLogRangeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandStateResponse {
    pub(crate) state: MetadataCommandReplicaState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcMetadataCommandAcceptanceOutcome {
    Acceptance(MetadataCommandAcceptance),
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandAcceptanceResponse {
    pub(crate) outcome: StorageRpcMetadataCommandAcceptanceOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardWriteItem {
    pub(crate) expected_size: u64,
    pub(crate) expected_crc64: u64,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardWriteRequest {
    pub(crate) location: ShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_size: u64,
    pub(crate) expected_crc64: u64,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardReadRequest {
    pub(crate) location: ShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_ack: WriteAck,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardReadRangeRequest {
    pub(crate) location: ShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_ack: WriteAck,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardDeleteRequest {
    pub(crate) location: ShardLocation,
    pub(crate) shard_key: ShardKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardAckItem {
    pub(crate) shard_key: ShardKey,
    pub(crate) ack: WriteAck,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardAckBatchRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) items: Vec<StorageRpcShardAckItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardAckItemRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) shard_key: ShardKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcScavengerListFilesRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) data_pg_id: DataPgId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcScavengerObservationRecordRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) observation: ShardScavengerObservationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcScavengerObservationKeyRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) key: ShardScavengerObservationKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairRecordRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) work_item: PlacedSegmentShardRepairWorkItem,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairItemRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) work_item: PlacedSegmentShardRepairWorkItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
    pub(crate) record: Option<PlacedSegmentShardRepairClaimRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim: PlacedSegmentShardRepairClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim: PlacedSegmentShardRepairClaimRecord,
    pub(crate) last_error: String,
    pub(crate) next_attempt_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillRecordRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) work_item: PlacedSegmentShardBackfillWorkItem,
    pub(crate) remaining_tolerance: u8,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillItemRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) work_item: PlacedSegmentShardBackfillWorkItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
    pub(crate) record: Option<PlacedSegmentShardBackfillClaimRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillClaimRecordRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim: PlacedSegmentShardBackfillClaimRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) claim: PlacedSegmentShardBackfillClaimRecord,
    pub(crate) last_error: String,
    pub(crate) next_attempt_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleAcquireRequest {
    pub(crate) read_operation_id: String,
    pub(crate) locations: Vec<ShardLocation>,
    pub(crate) shard_keys: Vec<ShardKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleAcquireResponse {
    pub(crate) locations: Vec<ShardLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleReleaseRequest {
    pub(crate) read_operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleReleaseResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketClaimToken {
    pub(crate) bucket: BucketName,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimClaimToken {
    pub(crate) bucket: BucketName,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) key: ObjectKey,
    pub(crate) generation_id: GenerationId,
    pub(crate) reclaim_kind: ObjectPayloadReclaimKind,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcDurableClaimToken {
    ObjectPayloadReclaim(StorageRpcObjectPayloadReclaimClaimToken),
    BucketDeleteFinalize(StorageRpcBucketClaimToken),
    LifecycleSweep(StorageRpcBucketClaimToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcClaimHeartbeatRequest {
    pub(crate) token: StorageRpcDurableClaimToken,
    pub(crate) heartbeat_at: u64,
    pub(crate) lease_deadline: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcClaimReleaseRequest {
    pub(crate) token: StorageRpcDurableClaimToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcProofReleaseRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationAcquireRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
    pub(crate) reservation_id: String,
    pub(crate) owner_token: String,
    pub(crate) operation_kind: String,
    pub(crate) created_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) target_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationProofRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationHeartbeatRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
    pub(crate) lease_deadline: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketWriteReservationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcBucketWriteReservationAcquireOutcome {
    Acquired(BucketWriteReservationRecord),
    Draining,
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationRecordResponse {
    pub(crate) outcome: StorageRpcBucketWriteReservationAcquireOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketSnapshotRequest {
    pub(crate) bucket: StorageRpcBucketRequest,
    pub(crate) request: BucketSnapshotRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketSnapshotPairRequest {
    pub(crate) source: StorageRpcBucketSnapshotRequest,
    pub(crate) destination: StorageRpcBucketSnapshotRequest,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcBucketSnapshotOutcome {
    Loaded(Box<BucketSnapshot>),
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketSnapshotResponse {
    pub(crate) outcome: StorageRpcBucketSnapshotOutcome,
}

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcBucketSnapshotPairOutcome {
    Loaded(Box<BucketSnapshotPair>),
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketSnapshotPairResponse {
    pub(crate) outcome: StorageRpcBucketSnapshotPairOutcome,
}

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
        StorageRpcMessageKind::ShardRead | StorageRpcMessageKind::ShardHistoricalRead => {
            STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN
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
        }
        StorageRpcMessageKind::MetadataCommandReplicaState => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandAcceptance
        | StorageRpcMessageKind::MetadataCommandAbandonAcceptance
        | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
        | StorageRpcMessageKind::MetadataCommandAppliedLogHashes
        | StorageRpcMessageKind::MetadataCommandAbandoned
        | StorageRpcMessageKind::MetadataCommandRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord => {
            STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert => {
            STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandPendingSlotReplace => {
            STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN
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
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8
        }
        StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8 + 8 + 8
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
        StorageRpcMessageKind::BucketHeadRaw | StorageRpcMessageKind::BucketHeadInfo => {
            STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketSnapshotLoad => {
            STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketSnapshotPairLoad => {
            STORAGE_RPC_MAX_BUCKET_SNAPSHOT_PAIR_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::BucketCreateCommandBuild => {
            STORAGE_RPC_MAX_CREATE_BUCKET_COMMAND_BUILD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::CompletedMultipartOrderCommandBuild => {
            STORAGE_RPC_MAX_COMPLETED_MULTIPART_ORDER_COMMAND_BUILD_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectReadAuthSubjectLoad => {
            STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectReadSnapshotLoad => {
            STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectTagsForSubjectLoad => {
            STORAGE_RPC_MAX_OBJECT_TAGS_FOR_SUBJECT_REQUEST_PAYLOAD_LEN
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
        StorageRpcMessageKind::ObjectStreamUploadSessionLoad => {
            STORAGE_RPC_MAX_STREAM_UPLOAD_SESSION_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadsList => {
            STORAGE_RPC_MAX_STREAM_UPLOADS_LIST_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectStreamUploadsPgList => {
            STORAGE_RPC_MAX_STREAM_UPLOADS_PG_LIST_REQUEST_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ObjectCompletedMultipartUploadsList => {
            STORAGE_RPC_MAX_COMPLETED_MULTIPART_UPLOADS_LIST_REQUEST_PAYLOAD_LEN
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
        | StorageRpcMessageKind::BucketWriteReservationsList
        | StorageRpcMessageKind::BucketDeleteFinalized => {
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
    put_u16(&mut out, error.code as u16);
    put_string(&mut out, &error.message);
    Ok(out)
}

pub(crate) fn decode_storage_rpc_response_payload(
    bytes: &[u8],
) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let response = match decoder.read_u8()? {
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
    Ok(response)
}

pub(crate) fn encode_metadata_command_item(
    item: &StorageRpcMetadataCommandItem,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if item.command_bytes.len() > STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: item.command_bytes.len(),
            limit: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN,
        });
    }
    if checksum::crc64::checksum(&item.command_bytes) != item.command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    let envelope = decode_metadata_command_envelope(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    if envelope.checksum_crc64() != item.command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    let mut out = Vec::new();
    put_u64(&mut out, item.command_checksum);
    put_bytes(&mut out, &item.command_bytes);
    Ok(out)
}

pub(crate) fn decode_metadata_command_item(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let item = decoder.read_metadata_command_item()?;
    decoder.finish()?;
    Ok(item)
}

fn validate_metadata_command_item(
    command_checksum: u64,
    command_bytes: Vec<u8>,
) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
    if checksum::crc64::checksum(&command_bytes) != command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    let envelope = decode_metadata_command_envelope(&command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    if envelope.checksum_crc64() != command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    Ok(StorageRpcMetadataCommandItem {
        command_checksum,
        command_bytes,
    })
}

fn metadata_command_envelope_from_item(
    item: &StorageRpcMetadataCommandItem,
) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
    decode_metadata_command_envelope(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
}

pub(crate) fn encode_metadata_command_request(
    request: &StorageRpcMetadataCommandRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.command.id())?;
    let item = StorageRpcMetadataCommandItem {
        command_checksum: request.command.checksum_crc64(),
        command_bytes: request.command.command_bytes(),
    };
    let command_payload = encode_metadata_command_item(&item)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    out.extend_from_slice(&command_payload);
    Ok(out)
}

pub(crate) fn decode_metadata_command_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    decoder.finish()?;
    let command = metadata_command_envelope_from_item(&item)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    Ok(StorageRpcMetadataCommandRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
    })
}

pub(crate) fn encode_bucket_request(request: &StorageRpcBucketRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    out
}

pub(crate) fn decode_bucket_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    decoder.finish()?;
    Ok(StorageRpcBucketRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
    })
}

pub(crate) fn encode_stream_uploads_list_request(
    request: &StorageRpcStreamUploadsListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_cleanup_list_limit(request.limit)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_optional_string(
        &mut out,
        request.session_id_marker.as_ref().map(SessionId::as_str),
    );
    put_u32(&mut out, request.limit);
    Ok(out)
}

pub(crate) fn decode_stream_uploads_list_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let session_id_marker = decoder.read_optional_session_id()?;
    let limit = decoder.read_u32()?;
    validate_cleanup_list_limit(limit)?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsListRequest {
        bucket,
        session_id_marker,
        limit,
    })
}

pub(crate) fn encode_stream_uploads_pg_list_request(
    request: &StorageRpcStreamUploadsPgListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_cleanup_list_limit(request.limit)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_optional_string(
        &mut out,
        request.session_id_marker.as_ref().map(SessionId::as_str),
    );
    put_u32(&mut out, request.limit);
    Ok(out)
}

pub(crate) fn decode_stream_uploads_pg_list_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsPgListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let session_id_marker = decoder.read_optional_session_id()?;
    let limit = decoder.read_u32()?;
    validate_cleanup_list_limit(limit)?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsPgListRequest {
        node_id,
        cluster_epoch,
        pg_id,
        session_id_marker,
        limit,
    })
}

pub(crate) fn encode_completed_multipart_uploads_list_request(
    request: &StorageRpcCompletedMultipartUploadsListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_cleanup_list_limit(request.limit)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_optional_string(
        &mut out,
        request.upload_id_marker.as_ref().map(UploadId::as_str),
    );
    put_u32(&mut out, request.limit);
    Ok(out)
}

pub(crate) fn decode_completed_multipart_uploads_list_request(
    bytes: &[u8],
) -> Result<StorageRpcCompletedMultipartUploadsListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let upload_id_marker = decoder.read_optional_upload_id()?;
    let limit = decoder.read_u32()?;
    validate_cleanup_list_limit(limit)?;
    decoder.finish()?;
    Ok(StorageRpcCompletedMultipartUploadsListRequest {
        bucket,
        upload_id_marker,
        limit,
    })
}

pub(crate) fn encode_bucket_list_request(
    request: &StorageRpcBucketListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.owner_canonical_id.len() > STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.owner_canonical_id.len(),
            limit: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, &request.owner_canonical_id);
    Ok(out)
}

pub(crate) fn decode_bucket_list_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let owner_canonical_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN + 1,
            limit: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        },
    )?;
    decoder.finish()?;
    Ok(StorageRpcBucketListRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        owner_canonical_id,
    })
}

pub(crate) fn encode_bucket_list_response(
    response: &StorageRpcBucketListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.buckets.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.buckets.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for bucket in &response.buckets {
        put_bucket_info(&mut out, bucket);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_list_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket list count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut buckets = Vec::new();
    for _ in 0..count {
        buckets.push(decoder.read_bucket_info()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketListResponse { buckets })
}

pub(crate) fn encode_bucket_batch_request(
    request: &StorageRpcBucketBatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(request.buckets.len())?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_u32(
        &mut out,
        u32::try_from(request.buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.buckets.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for bucket in &request.buckets {
        put_string(&mut out, bucket.as_str());
    }
    Ok(out)
}

pub(crate) fn decode_bucket_batch_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketBatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket batch count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut buckets = Vec::new();
    for _ in 0..count {
        buckets.push(decoder.read_bucket_name()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketBatchRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        buckets,
    })
}

pub(crate) fn encode_bucket_execution_generations_response(
    response: &StorageRpcBucketExecutionGenerationsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.generations.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.generations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.generations.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for (bucket, generation) in &response.generations {
        put_string(&mut out, bucket.as_str());
        put_u64(&mut out, *generation);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_execution_generations_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketExecutionGenerationsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket execution generation count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut generations = HashMap::new();
    for _ in 0..count {
        let bucket = decoder.read_bucket_name()?;
        let generation = decoder.read_u64()?;
        if generations.insert(bucket, generation).is_some() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "duplicate bucket execution generation",
            ));
        }
    }
    decoder.finish()?;
    Ok(StorageRpcBucketExecutionGenerationsResponse { generations })
}

pub(crate) fn encode_bucket_fast_path_identities_response(
    response: &StorageRpcBucketFastPathIdentitiesResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.identities.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.identities.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.identities.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for (bucket, identity) in &response.identities {
        put_string(&mut out, bucket.as_str());
        put_bucket_fast_path_identity(&mut out, *identity);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_fast_path_identities_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketFastPathIdentitiesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket fast-path identity count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut identities = HashMap::new();
    for _ in 0..count {
        let bucket = decoder.read_bucket_name()?;
        let identity = decoder.read_bucket_fast_path_identity()?;
        if identities.insert(bucket, identity).is_some() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "duplicate bucket fast-path identity",
            ));
        }
    }
    decoder.finish()?;
    Ok(StorageRpcBucketFastPathIdentitiesResponse { identities })
}

pub(crate) fn encode_bucket_snapshot_request(request: &StorageRpcBucketSnapshotRequest) -> Vec<u8> {
    let mut out = encode_bucket_request(&request.bucket);
    put_bucket_snapshot_request(&mut out, request.request);
    out
}

pub(crate) fn decode_bucket_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let request = decoder.read_rpc_bucket_snapshot_request()?;
    decoder.finish()?;
    Ok(request)
}

pub(crate) fn encode_bucket_snapshot_pair_request(
    request: &StorageRpcBucketSnapshotPairRequest,
) -> Vec<u8> {
    let mut out = encode_bucket_snapshot_request(&request.source);
    out.extend_from_slice(&encode_bucket_snapshot_request(&request.destination));
    out
}

pub(crate) fn decode_bucket_snapshot_pair_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotPairRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let source = decoder.read_rpc_bucket_snapshot_request()?;
    let destination = decoder.read_rpc_bucket_snapshot_request()?;
    decoder.finish()?;
    Ok(StorageRpcBucketSnapshotPairRequest {
        source,
        destination,
    })
}

pub(crate) fn encode_object_request(request: &StorageRpcObjectRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    put_string(&mut out, request.key.as_str());
    out
}

pub(crate) fn decode_object_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    decoder.finish()?;
    Ok(StorageRpcObjectRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        key,
    })
}

pub(crate) fn encode_object_payload_reclaim_exists_request(
    request: &StorageRpcObjectPayloadReclaimExistsRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_u64(&mut out, request.generation_id.get());
    out
}

pub(crate) fn decode_object_payload_reclaim_exists_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimExistsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimExistsRequest {
        object,
        generation_id,
    })
}

pub(crate) fn encode_object_payload_reclaim_response(
    response: &StorageRpcObjectPayloadReclaimResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_object_payload_reclaim(&mut out, response.reclaim.as_ref());
    out
}

pub(crate) fn decode_object_payload_reclaim_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let reclaim = decoder.read_optional_object_payload_reclaim()?;
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimResponse { reclaim })
}

pub(crate) fn encode_payload_reclaim_root_response(
    response: &StorageRpcPayloadReclaimRootResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_payload_reclaim_root(&mut out, response.root.as_ref());
    out
}

pub(crate) fn decode_payload_reclaim_root_response(
    bytes: &[u8],
) -> Result<StorageRpcPayloadReclaimRootResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let root = decoder.read_optional_payload_reclaim_root()?;
    decoder.finish()?;
    Ok(StorageRpcPayloadReclaimRootResponse { root })
}

pub(crate) fn encode_object_payload_reclaim_claim_acquire_request(
    request: &StorageRpcObjectPayloadReclaimClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&request.claim_id, &request.owner_token)?;
    if request
        .lease_deadline
        .is_some_and(|lease_deadline| lease_deadline <= request.claimed_at)
    {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_object_payload_reclaim_exists_request(
        &StorageRpcObjectPayloadReclaimExistsRequest {
            object: request.object.clone(),
            generation_id: request.generation_id,
        },
    );
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_u8(&mut out, request.reclaim_kind as u8);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let generation_id = decoder.read_generation_id()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let reclaim_kind = decoder.read_object_payload_reclaim_kind()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
        object,
        bucket_incarnation_generation,
        generation_id,
        reclaim_kind,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    };
    encode_object_payload_reclaim_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_object_payload_reclaim_claim_optional_record_response(
    response: &StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_object_payload_reclaim_claim_record(record)?;
            put_u8(&mut out, 1);
            put_object_payload_reclaim_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_object_payload_reclaim_claim_record()?;
            validate_object_payload_reclaim_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional object payload reclaim claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_object_payload_reclaim_claim_record_request(
    request: &StorageRpcObjectPayloadReclaimClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    if request.pg_id.get() != request.record.pg_id {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route PG must match claim PG",
        ));
    }
    validate_object_payload_reclaim_claim_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_object_payload_reclaim_claim_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_object_payload_reclaim_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    if pg_id.get() != record.pg_id {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route PG must match claim PG",
        ));
    }
    validate_object_payload_reclaim_claim_record(&record)?;
    Ok(StorageRpcObjectPayloadReclaimClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_multipart_completion_stale_source_response(
    response: &StorageRpcMultipartCompletionStaleSourceResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_stored_object(&mut out, response.source.as_ref());
    out
}

pub(crate) fn decode_multipart_completion_stale_source_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionStaleSourceResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let source = decoder.read_optional_stored_object()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionStaleSourceResponse { source })
}

pub(crate) fn encode_multipart_upload_load_request(
    request: &StorageRpcMultipartUploadLoadRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    out
}

pub(crate) fn decode_multipart_upload_load_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadLoadRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadLoadRequest { object, upload_id })
}

pub(crate) fn encode_multipart_upload_load_response(
    response: &StorageRpcMultipartUploadLoadResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
            put_u8(&mut out, 0);
            put_multipart_upload_record(&mut out, upload);
        }
        StorageRpcMultipartUploadLoadOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_upload_load_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadLoadResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(
            decoder.read_multipart_upload_record()?,
        )),
        1 => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart upload load outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadLoadResponse { outcome })
}

pub(crate) fn encode_multipart_completion_snapshot_request(
    request: &StorageRpcMultipartCompletionSnapshotRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart completion snapshot request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_u32(
        &mut out,
        u32::try_from(request.requested_part_numbers.len())
            .expect("requested part number count must fit in u32"),
    );
    for part_number in &request.requested_part_numbers {
        put_u32(&mut out, *part_number);
    }
    Ok(out)
}

pub(crate) fn decode_multipart_completion_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart completion snapshot request identity mismatch",
    )?;
    let part_number_count = decoder.read_bounded_remaining_count(
        4,
        "multipart completion requested part count exceeds payload",
    )?;
    let mut requested_part_numbers = Vec::new();
    for _ in 0..part_number_count {
        requested_part_numbers.push(decoder.read_u32()?);
    }
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionSnapshotRequest {
        object,
        authorized_upload,
        requested_part_numbers,
    })
}

pub(crate) fn encode_multipart_completion_snapshot_response(
    response: &StorageRpcMultipartCompletionSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartCompletionSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_multipart_completion_snapshot(&mut out, snapshot);
        }
        StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
        StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id,
            part_number,
        } => {
            put_u8(&mut out, 2);
            put_string(&mut out, upload_id.as_str());
            put_u32(&mut out, *part_number);
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_completion_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartCompletionSnapshotOutcome::Loaded(Box::new(
            decoder.read_multipart_completion_snapshot()?,
        )),
        1 => StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        2 => StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id: decoder.read_upload_id()?,
            part_number: decoder.read_u32()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart completion snapshot outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionSnapshotResponse { outcome })
}

pub(crate) fn encode_multipart_completion_preflight_request(
    request: &StorageRpcMultipartCompletionPreflightRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart completion preflight request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    Ok(out)
}

pub(crate) fn decode_multipart_completion_preflight_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionPreflightRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart completion preflight request identity mismatch",
    )?;
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionPreflightRequest {
        object,
        authorized_upload,
    })
}

pub(crate) fn encode_multipart_completion_preflight_response(
    response: &StorageRpcMultipartCompletionPreflightResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight) => {
            put_u8(&mut out, 0);
            put_optional_string(&mut out, preflight.existing_etag.as_deref());
        }
        StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_multipart_completion_preflight_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionPreflightResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartCompletionPreflightOutcome::Loaded(MultipartCompletionPreflight {
            existing_etag: decoder.read_optional_string()?,
        }),
        1 => StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart completion preflight outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionPreflightResponse { outcome })
}

pub(crate) fn encode_multipart_parts_list_request(
    request: &StorageRpcMultipartPartsListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart parts list request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_optional_u32(&mut out, request.part_number_marker);
    put_u32(&mut out, request.max_parts);
    Ok(out)
}

pub(crate) fn decode_multipart_parts_list_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartPartsListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart parts list request identity mismatch",
    )?;
    let part_number_marker = decoder.read_optional_u32()?;
    let max_parts = decoder.read_u32()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartPartsListRequest {
        object,
        authorized_upload,
        part_number_marker,
        max_parts,
    })
}

pub(crate) fn encode_multipart_parts_list_response(
    response: &StorageRpcMultipartPartsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartPartsListOutcome::Loaded(listed) => {
            put_u8(&mut out, 0);
            put_listed_multipart_parts(&mut out, listed);
        }
        StorageRpcMultipartPartsListOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_parts_list_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartPartsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartPartsListOutcome::Loaded(Box::new(
            decoder.read_listed_multipart_parts()?,
        )),
        1 => StorageRpcMultipartPartsListOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart parts list outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartPartsListResponse { outcome })
}

pub(crate) fn encode_multipart_management_lookup_response(
    response: &StorageRpcMultipartManagementLookupResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_multipart_upload_management_lookup(&mut out, &response.lookup);
    out
}

pub(crate) fn decode_multipart_management_lookup_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartManagementLookupResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let lookup = decoder.read_multipart_upload_management_lookup()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartManagementLookupResponse { lookup })
}

pub(crate) fn encode_object_generation_reservation_request(
    request: &StorageRpcObjectGenerationReservationRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.reservation_id.as_str());
    out
}

pub(crate) fn decode_object_generation_reservation_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationReservationRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    let reservation_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationReservationRequest {
        object: StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        },
        reservation_id,
    })
}

pub(crate) fn encode_direct_put_commit_snapshot_request(
    request: &StorageRpcDirectPutCommitSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.reservation_id.as_str());
    put_u64(&mut out, request.generation_id.get());
    out
}

pub(crate) fn decode_direct_put_commit_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommitSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    let reservation_id = decoder.read_session_id()?;
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommitSnapshotRequest {
        object: StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        },
        reservation_id,
        generation_id,
    })
}

pub(crate) fn encode_object_read_auth_subject_request(
    request: &StorageRpcObjectReadAuthSubjectRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_object_read_auth_subject_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadAuthSubjectRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectReadAuthSubjectRequest { object, version_id })
}

pub(crate) fn encode_object_read_snapshot_request(
    request: &StorageRpcObjectReadSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    put_stored_object(&mut out, request.expected_identity.stored());
    put_object_read_snapshot_mode(&mut out, request.snapshot_mode);
    out
}

pub(crate) fn decode_object_read_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    let expected_stored = decoder.read_stored_object()?;
    let snapshot_mode = decoder.read_object_read_snapshot_mode()?;
    decoder.finish()?;
    Ok(StorageRpcObjectReadSnapshotRequest {
        object,
        version_id,
        expected_identity: ObjectReadAuthSubjectIdentity::for_stored(&expected_stored),
        snapshot_mode,
    })
}

pub(crate) fn encode_object_tags_for_subject_request(
    request: &StorageRpcObjectTagsForSubjectRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    put_stored_object(&mut out, request.expected_identity.stored());
    put_u64(&mut out, request.authorized_version_id.to_u64());
    out
}

pub(crate) fn decode_object_tags_for_subject_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectTagsForSubjectRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    let expected_stored = decoder.read_stored_object()?;
    let authorized_version_id = VersionId::from_u64(decoder.read_u64()?);
    decoder.finish()?;
    Ok(StorageRpcObjectTagsForSubjectRequest {
        object,
        version_id,
        expected_identity: ObjectReadAuthSubjectIdentity::for_stored(&expected_stored),
        authorized_version_id,
    })
}

pub(crate) fn encode_put_object_metadata_snapshot_request(
    request: &StorageRpcPutObjectMetadataSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_put_object_metadata_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcPutObjectMetadataSnapshotRequest { object, version_id })
}

pub(crate) fn encode_object_delete_snapshot_request(
    request: &StorageRpcObjectDeleteSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_object_delete_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectDeleteSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectDeleteSnapshotRequest { object, version_id })
}

pub(crate) fn encode_put_object_metadata_command_build_request(
    request: &StorageRpcPutObjectMetadataCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_stored.bucket() != &request.object.bucket
        || request.expected_stored.key() != &request.object.key
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object metadata command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.requested_version_id);
    put_stored_object(&mut out, &request.expected_stored);
    put_u64(&mut out, request.version_id.to_u64());
    put_put_object_metadata_mutation(&mut out, &request.mutation);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_put_object_metadata_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let requested_version_id = decoder.read_optional_version_id()?;
    let expected_stored = decoder.read_stored_object()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let mutation = decoder.read_put_object_metadata_mutation()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_stored.bucket() != &object.bucket
        || expected_stored.key() != &object.key
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object metadata command build request identity mismatch",
        ));
    }
    Ok(StorageRpcPutObjectMetadataCommandBuildRequest {
        object,
        requested_version_id,
        expected_stored,
        version_id,
        mutation,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_delete_specific_object_command_build_request(
    request: &StorageRpcDeleteSpecificObjectCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_stored.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.expected_target.as_ref().is_some_and(|target| {
        !delete_target_matches_object(target, &request.object.bucket, &request.object.key)
    }) || request
        .expected_version_list
        .as_ref()
        .is_some_and(|versions| {
            versions.iter().any(|stored| {
                stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
            })
        })
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-specific command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_u64(&mut out, request.version_id.to_u64());
    put_optional_stored_object(&mut out, request.expected_stored.as_ref());
    put_optional_delete_object_version_target(&mut out, request.expected_target.as_ref());
    put_optional_stored_object_list(&mut out, request.expected_version_list.as_deref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_delete_specific_object_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let expected_stored = decoder.read_optional_stored_object()?;
    let expected_target = decoder.read_optional_delete_object_version_target()?;
    let expected_version_list = decoder.read_optional_stored_object_list()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_stored
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_target.as_ref().is_some_and(|target| {
            !delete_target_matches_object(target, &object.bucket, &object.key)
        })
        || expected_version_list.as_ref().is_some_and(|versions| {
            versions
                .iter()
                .any(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        })
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-specific command build request identity mismatch",
        ));
    }
    Ok(StorageRpcDeleteSpecificObjectCommandBuildRequest {
        object,
        version_id,
        expected_stored,
        expected_target,
        expected_version_list,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_delete_current_object_command_build_request(
    request: &StorageRpcDeleteCurrentObjectCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.expected_target.as_ref().is_some_and(|target| {
        !delete_target_matches_object(target, &request.object.bucket, &request.object.key)
    }) || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-current command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_optional_delete_object_version_target(&mut out, request.expected_target.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_delete_current_object_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDeleteCurrentObjectCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let expected_target = decoder.read_optional_delete_object_version_target()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_target.as_ref().is_some_and(|target| {
            !delete_target_matches_object(target, &object.bucket, &object.key)
        })
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-current command build request identity mismatch",
        ));
    }
    Ok(StorageRpcDeleteCurrentObjectCommandBuildRequest {
        object,
        expected_current,
        expected_target,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_insert_delete_marker_command_build_request(
    request: &StorageRpcInsertDeleteMarkerCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request
        .expected_stale_payload_source
        .as_ref()
        .is_some_and(|stored| {
            stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
        })
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "insert-delete-marker command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_optional_stored_object(&mut out, request.expected_stale_payload_source.as_ref());
    put_u64(&mut out, request.version_id.to_u64());
    put_owner_identity(&mut out, &request.owner);
    put_insert_delete_marker_stale_payload(&mut out, &request.stale_payload);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_insert_delete_marker_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcInsertDeleteMarkerCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let expected_stale_payload_source = decoder.read_optional_stored_object()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let owner = decoder.read_owner_identity()?;
    let stale_payload = decoder.read_insert_delete_marker_stale_payload()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_stale_payload_source
            .as_ref()
            .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "insert-delete-marker command build request identity mismatch",
        ));
    }
    Ok(StorageRpcInsertDeleteMarkerCommandBuildRequest {
        object,
        expected_current,
        expected_stale_payload_source,
        version_id,
        owner,
        stale_payload,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_stream_upload_match_request(
    request: &StorageRpcStreamUploadMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_stream_upload_request_identity(&request.object, &request.request)?;
    if request.expected_command.as_ref().is_some_and(|command| {
        !create_stream_upload_command_matches_request(command, &request.request)
    }) {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload match expected command identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_stream_upload_req(&mut out, &request.request);
    put_optional_create_stream_upload_command(&mut out, request.expected_command.as_ref());
    Ok(out)
}

pub(crate) fn decode_stream_upload_match_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_stream_upload_req()?;
    let expected_command = decoder.read_optional_create_stream_upload_command()?;
    decoder.finish()?;
    validate_create_stream_upload_request_identity(&object, &request)?;
    if expected_command
        .as_ref()
        .is_some_and(|command| !create_stream_upload_command_matches_request(command, &request))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload match expected command identity mismatch",
        ));
    }
    Ok(StorageRpcStreamUploadMatchRequest {
        object,
        request,
        expected_command,
    })
}

pub(crate) fn encode_multipart_upload_match_request(
    request: &StorageRpcMultipartUploadMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_multipart_upload_request_identity(&request.object, &request.request)?;
    if request.expected_command.as_ref().is_some_and(|command| {
        !create_multipart_upload_command_matches_request(command, &request.request)
    }) {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload match expected command identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_multipart_upload_req(&mut out, &request.request);
    put_optional_create_multipart_upload_command(&mut out, request.expected_command.as_ref());
    Ok(out)
}

pub(crate) fn decode_multipart_upload_match_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_multipart_upload_req()?;
    let expected_command = decoder.read_optional_create_multipart_upload_command()?;
    decoder.finish()?;
    validate_create_multipart_upload_request_identity(&object, &request)?;
    if expected_command
        .as_ref()
        .is_some_and(|command| !create_multipart_upload_command_matches_request(command, &request))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload match expected command identity mismatch",
        ));
    }
    Ok(StorageRpcMultipartUploadMatchRequest {
        object,
        request,
        expected_command,
    })
}

pub(crate) fn encode_stream_upload_match_response(
    response: &StorageRpcStreamUploadMatchResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_bool(&mut out, response.exists);
    out
}

pub(crate) fn decode_stream_upload_match_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadMatchResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let exists = decoder.read_bool()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadMatchResponse { exists })
}

pub(crate) fn encode_stream_upload_session_request(
    request: &StorageRpcStreamUploadSessionRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    out
}

pub(crate) fn decode_stream_upload_session_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSessionRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSessionRequest { object, session_id })
}

pub(crate) fn encode_stream_upload_session_response(
    response: &StorageRpcStreamUploadSessionResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamUploadSessionOutcome::Loaded(session) => {
            put_u8(&mut out, 1);
            put_stream_upload_record(&mut out, session);
        }
        StorageRpcStreamUploadSessionOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_stream_upload_session_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSessionResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => StorageRpcStreamUploadSessionOutcome::Loaded(Box::new(
            decoder.read_stream_upload_record()?,
        )),
        2 => StorageRpcStreamUploadSessionOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload session outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSessionResponse { outcome })
}

pub(crate) fn encode_stream_uploads_list_response(
    response: &StorageRpcStreamUploadsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let count = u32::try_from(response.uploads.len()).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: response.uploads.len(),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        }
    })?;
    if count > STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count as usize,
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        });
    }
    let mut out = Vec::new();
    put_u32(&mut out, count);
    for upload in &response.uploads {
        put_stream_upload_record(&mut out, upload);
    }
    put_optional_string(
        &mut out,
        response
            .next_session_id_marker
            .as_ref()
            .map(SessionId::as_str),
    );
    Ok(out)
}

pub(crate) fn decode_stream_uploads_list_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let upload_count = decoder.read_limited_bounded_remaining_count(
        STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
        "too many stream uploads",
        STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS,
    )?;
    let mut uploads = Vec::new();
    for _ in 0..upload_count {
        uploads.push(decoder.read_stream_upload_record()?);
    }
    let next_session_id_marker = decoder.read_optional_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsListResponse {
        uploads,
        next_session_id_marker,
    })
}

pub(crate) fn encode_completed_multipart_uploads_list_response(
    response: &StorageRpcCompletedMultipartUploadsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let count = u32::try_from(response.records.len()).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: response.records.len(),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        }
    })?;
    if count > STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count as usize,
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        });
    }
    let mut out = Vec::new();
    put_u32(&mut out, count);
    for record in &response.records {
        put_completed_multipart_upload_record(&mut out, record);
    }
    put_optional_string(
        &mut out,
        response
            .next_upload_id_marker
            .as_ref()
            .map(UploadId::as_str),
    );
    Ok(out)
}

pub(crate) fn decode_completed_multipart_uploads_list_response(
    bytes: &[u8],
) -> Result<StorageRpcCompletedMultipartUploadsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "completed multipart upload record count exceeds payload",
        STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS,
    )?;
    let mut records = Vec::new();
    for _ in 0..count {
        records.push(decoder.read_completed_multipart_upload_record()?);
    }
    let next_upload_id_marker = decoder.read_optional_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcCompletedMultipartUploadsListResponse {
        records,
        next_upload_id_marker,
    })
}

pub(crate) fn encode_stream_upload_segments_response(
    response: &StorageRpcStreamUploadSegmentsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamUploadSegmentsOutcome::Loaded(segments) => {
            put_u8(&mut out, 1);
            put_u32(
                &mut out,
                u32::try_from(segments.len()).map_err(|_| {
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "too many stream upload segments",
                    )
                })?,
            );
            for segment in segments {
                put_stream_upload_segment_record(&mut out, segment);
            }
        }
        StorageRpcStreamUploadSegmentsOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_stream_upload_segments_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSegmentsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => {
            let segment_count = decoder.read_bounded_remaining_count(
                STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
                "too many stream upload segments",
            )?;
            let mut segments = Vec::new();
            for _ in 0..segment_count {
                segments.push(decoder.read_stream_upload_segment_record()?);
            }
            StorageRpcStreamUploadSegmentsOutcome::Loaded(segments)
        }
        2 => StorageRpcStreamUploadSegmentsOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload segments outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSegmentsResponse { outcome })
}

pub(crate) fn encode_stream_segment_append_prepare_request(
    request: &StorageRpcStreamSegmentAppendPrepareRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_prepare_stream_segment_append_req(&mut out, &request.request);
    out
}

pub(crate) fn decode_stream_segment_append_prepare_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamSegmentAppendPrepareRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_prepare_stream_segment_append_req()?;
    decoder.finish()?;
    Ok(StorageRpcStreamSegmentAppendPrepareRequest { object, request })
}

pub(crate) fn encode_stream_segment_append_prepare_response(
    response: &StorageRpcStreamSegmentAppendPrepareResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamSegmentAppendPrepareOutcome::Prepared { target, segment } => {
            put_u8(&mut out, 1);
            put_stream_upload_target(&mut out, target);
            put_stream_upload_segment_record(&mut out, segment);
        }
        StorageRpcStreamSegmentAppendPrepareOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_stream_segment_append_prepare_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamSegmentAppendPrepareResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
            target: decoder.read_stream_upload_target()?,
            segment: Box::new(decoder.read_stream_upload_segment_record()?),
        },
        2 => StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream segment append prepare outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamSegmentAppendPrepareResponse { outcome })
}

pub(crate) fn encode_multipart_upload_match_response(
    response: &StorageRpcMultipartUploadMatchResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_u64(&mut out, response.initiated_at);
    out
}

pub(crate) fn decode_multipart_upload_match_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadMatchResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let initiated_at = decoder.read_optional_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadMatchResponse { initiated_at })
}

pub(crate) fn encode_create_stream_upload_command_build_request(
    request: &StorageRpcCreateStreamUploadCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_stream_upload_request_identity(&request.object, &request.request)?;
    validate_create_stream_upload_precondition_identity(&request.object, &request.precondition)?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload command build reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_stream_upload_req(&mut out, &request.request);
    put_create_stream_upload_precondition(&mut out, &request.precondition);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_create_stream_upload_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_stream_upload_req()?;
    let precondition = decoder.read_create_stream_upload_precondition()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_create_stream_upload_request_identity(&object, &request)?;
    validate_create_stream_upload_precondition_identity(&object, &precondition)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload command build reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcCreateStreamUploadCommandBuildRequest {
        object,
        request,
        precondition,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_create_multipart_upload_command_build_request(
    request: &StorageRpcCreateMultipartUploadCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_multipart_upload_request_identity(&request.object, &request.request)?;
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_multipart_upload_req(&mut out, &request.request);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_create_multipart_upload_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateMultipartUploadCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_multipart_upload_req()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_create_multipart_upload_request_identity(&object, &request)?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload command build request identity mismatch",
        ));
    }
    Ok(StorageRpcCreateMultipartUploadCommandBuildRequest {
        object,
        request,
        expected_current,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_stream_put_finalize_snapshot_request(
    request: &StorageRpcStreamPutFinalizeSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    out
}

pub(crate) fn decode_stream_put_finalize_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutFinalizeSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPutFinalizeSnapshotRequest { object, session_id })
}

pub(crate) fn encode_stream_put_finalize_snapshot_response(
    response: &StorageRpcStreamPutFinalizeSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_put_finalize_snapshot_identity(
        &StorageRpcObjectRequest {
            node_id: NodeId::new(0),
            cluster_epoch: ClusterEpoch::new(1).expect("nonzero epoch"),
            pg_id: PgId::new(0),
            bucket: response.snapshot.session.bucket.clone(),
            key: response.snapshot.session.key.clone(),
        },
        &response.snapshot.session.session_id,
        &response.snapshot,
    )?;
    let mut out = Vec::new();
    put_stream_put_finalize_storage_snapshot(&mut out, &response.snapshot);
    Ok(out)
}

pub(crate) fn decode_stream_put_finalize_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutFinalizeSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_stream_put_finalize_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPutFinalizeSnapshotResponse { snapshot })
}

pub(crate) fn encode_stream_put_commit_command_build_request(
    request: &StorageRpcStreamPutCommitCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_put_finalize_snapshot_identity(
        &request.object,
        &request.session_id,
        &request.expected_snapshot,
    )?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT commit reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    put_u64(&mut out, request.total_size);
    put_stream_put_finalize_storage_snapshot(&mut out, &request.expected_snapshot);
    put_stream_put_commit_input(&mut out, &request.commit);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_stream_put_commit_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    let total_size = decoder.read_u64()?;
    let expected_snapshot = decoder.read_stream_put_finalize_storage_snapshot()?;
    let commit = decoder.read_stream_put_commit_input()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_stream_put_finalize_snapshot_identity(&object, &session_id, &expected_snapshot)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT commit reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcStreamPutCommitCommandBuildRequest {
        object,
        session_id,
        total_size,
        expected_snapshot,
        commit,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_stream_part_finalize_snapshot_request(
    request: &StorageRpcStreamPartFinalizeSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_string(&mut out, request.session_id.as_str());
    put_u32(&mut out, request.part_number);
    out
}

pub(crate) fn decode_stream_part_finalize_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartFinalizeSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let session_id = decoder.read_session_id()?;
    let part_number = decoder.read_u32()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPartFinalizeSnapshotRequest {
        object,
        upload_id,
        session_id,
        part_number,
    })
}

pub(crate) fn encode_stream_part_finalize_snapshot_response(
    response: &StorageRpcStreamPartFinalizeSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let auth = &response.snapshot.auth_snapshot;
    let StreamUploadTarget::UploadPart {
        upload_id,
        part_number,
    } = &auth.session.target
    else {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part finalize response target mismatch",
        ));
    };
    validate_stream_part_finalize_snapshot_identity(
        &StorageRpcObjectRequest {
            node_id: NodeId::new(0),
            cluster_epoch: ClusterEpoch::new(1).expect("nonzero epoch"),
            pg_id: PgId::new(0),
            bucket: auth.session.bucket.clone(),
            key: auth.session.key.clone(),
        },
        upload_id,
        &auth.session.session_id,
        *part_number,
        &response.snapshot,
    )?;
    let mut out = Vec::new();
    put_stream_part_finalize_storage_snapshot(&mut out, &response.snapshot);
    Ok(out)
}

pub(crate) fn decode_stream_part_finalize_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartFinalizeSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_stream_part_finalize_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPartFinalizeSnapshotResponse { snapshot })
}

pub(crate) fn encode_stream_part_commit_command_build_request(
    request: &StorageRpcStreamPartCommitCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_part_finalize_snapshot_identity(
        &request.object,
        &request.upload_id,
        &request.session_id,
        request.part_number,
        &request.expected_snapshot,
    )?;
    if request.part.upload_id != request.upload_id
        || request.part.part_number != request.part_number
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part commit request identity mismatch",
        ));
    }
    for segment in &request.segments {
        if segment.bucket != request.object.bucket
            || segment.key != request.object.key
            || segment.upload_id != request.upload_id
            || segment.part_number != request.part_number
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part commit segment identity mismatch",
            ));
        }
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_string(&mut out, request.session_id.as_str());
    put_u32(&mut out, request.part_number);
    put_stream_part_finalize_storage_snapshot(&mut out, &request.expected_snapshot);
    put_multipart_part_record(&mut out, &request.part);
    put_u32(
        &mut out,
        u32::try_from(request.segments.len()).expect("stream part segment count must fit in u32"),
    );
    for segment in &request.segments {
        put_multipart_part_segment_record(&mut out, segment);
    }
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_stream_part_commit_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let session_id = decoder.read_session_id()?;
    let part_number = decoder.read_u32()?;
    let expected_snapshot = decoder.read_stream_part_finalize_storage_snapshot()?;
    let part = decoder.read_multipart_part_record()?;
    let segment_count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
        "stream part commit segment count exceeds payload",
    )?;
    let mut segments = Vec::new();
    for _ in 0..segment_count {
        segments.push(decoder.read_multipart_part_segment_record()?);
    }
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_stream_part_finalize_snapshot_identity(
        &object,
        &upload_id,
        &session_id,
        part_number,
        &expected_snapshot,
    )?;
    if part.upload_id != upload_id
        || part.part_number != part_number
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part commit request identity mismatch",
        ));
    }
    for segment in &segments {
        if segment.bucket != object.bucket
            || segment.key != object.key
            || segment.upload_id != upload_id
            || segment.part_number != part_number
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part commit segment identity mismatch",
            ));
        }
    }
    Ok(StorageRpcStreamPartCommitCommandBuildRequest {
        object,
        upload_id,
        session_id,
        part_number,
        expected_snapshot,
        part,
        segments,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_complete_multipart_command_build_request(
    request: &StorageRpcCompleteMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_complete_multipart_request_identity(&request.object, &request.request)?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "complete multipart reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_complete_multipart_commit_request(&mut out, &request.request);
    put_u64(&mut out, request.version_id.to_u64());
    put_u64(&mut out, request.completion_order);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_complete_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCompleteMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_complete_multipart_commit_request()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let completion_order = decoder.read_u64()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_complete_multipart_request_identity(&object, &request)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "complete multipart reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcCompleteMultipartCommandBuildRequest {
        object,
        request,
        version_id,
        completion_order,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_abort_multipart_command_build_request(
    request: &StorageRpcAbortMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "abort multipart reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_optional_abort_multipart_upload_cleanup(&mut out, request.expected_cleanup.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_abort_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let expected_cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "abort multipart request identity mismatch",
        ));
    }
    if let Some(cleanup) = expected_cleanup.as_ref() {
        validate_abort_multipart_cleanup_identity(
            cleanup,
            &object,
            &upload_id,
            "abort multipart request identity mismatch",
        )?;
    }
    Ok(StorageRpcAbortMultipartCommandBuildRequest {
        object,
        upload_id,
        expected_cleanup,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_abort_multipart_cleanup_request(
    request: &StorageRpcAbortMultipartCleanupRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    out
}

pub(crate) fn decode_abort_multipart_cleanup_request(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCleanupRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcAbortMultipartCleanupRequest { object, upload_id })
}

pub(crate) fn encode_abort_multipart_cleanup_response(
    response: &StorageRpcAbortMultipartCleanupResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_abort_multipart_upload_cleanup(&mut out, response.cleanup.as_ref());
    out
}

pub(crate) fn decode_abort_multipart_cleanup_response(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCleanupResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    decoder.finish()?;
    Ok(StorageRpcAbortMultipartCleanupResponse { cleanup })
}

pub(crate) fn encode_authorized_abort_multipart_command_build_request(
    request: &StorageRpcAuthorizedAbortMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.authorized_upload.bucket != request.object.bucket
        || request.authorized_upload.key != request.object.key
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "authorized abort multipart request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_optional_abort_multipart_upload_cleanup(&mut out, request.expected_cleanup.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_authorized_abort_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    let expected_cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if authorized_upload.bucket != object.bucket
        || authorized_upload.key != object.key
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "authorized abort multipart request identity mismatch",
        ));
    }
    if let Some(cleanup) = expected_cleanup.as_ref() {
        if cleanup.upload != authorized_upload {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "authorized abort multipart request identity mismatch",
            ));
        }
        validate_abort_multipart_cleanup_identity(
            cleanup,
            &object,
            &authorized_upload.upload_id,
            "authorized abort multipart request identity mismatch",
        )?;
    }
    Ok(StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
        object,
        authorized_upload,
        expected_cleanup,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_direct_put_command_build_request(
    request: &StorageRpcDirectPutCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.object.bucket != request.request.bucket || request.object.key != request.request.key
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object route must match direct PUT request",
        ));
    }
    if request.request.bucket_write_reservation != request.bucket_write_reservation
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "bucket write reservation proof must match direct PUT bucket",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_commit_direct_put_object_req(&mut out, &request.request);
    put_u64(&mut out, request.version_id.to_u64());
    put_direct_put_commit_storage_snapshot(&mut out, &request.expected_snapshot);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_direct_put_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_commit_direct_put_object_req()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let expected_snapshot = decoder.read_direct_put_commit_storage_snapshot()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if object.bucket != request.bucket || object.key != request.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object route must match direct PUT request",
        ));
    }
    if request.bucket_write_reservation != bucket_write_reservation
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "bucket write reservation proof must match direct PUT bucket",
        ));
    }
    Ok(StorageRpcDirectPutCommandBuildRequest {
        object,
        request,
        version_id,
        expected_snapshot,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_object_generation_response(
    response: &StorageRpcObjectGenerationResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.generation_id.get());
    out
}

pub(crate) fn decode_object_generation_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationResponse { generation_id })
}

pub(crate) fn encode_object_version_response(
    response: &StorageRpcObjectVersionResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.version_id.to_u64());
    out
}

pub(crate) fn decode_object_version_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectVersionResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let raw = decoder.read_u64()?;
    decoder.finish()?;
    let version_id = VersionId::from_u64(raw);
    if version_id.is_null() {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "object version response must not contain null version",
        ));
    }
    Ok(StorageRpcObjectVersionResponse { version_id })
}

pub(crate) fn encode_object_read_auth_subject_response(
    response: &StorageRpcObjectReadAuthSubjectResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectReadAuthSubjectOutcome::Loaded(subject) => {
            put_u8(&mut out, 0);
            put_object_read_auth_subject(&mut out, subject);
        }
        StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_object_read_auth_subject_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadAuthSubjectResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(
            decoder.read_object_read_auth_subject()?,
        )),
        1 => StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object read auth subject outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectReadAuthSubjectResponse { outcome })
}

pub(crate) fn encode_object_read_snapshot_response(
    response: &StorageRpcObjectReadSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectReadSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_object_read_snapshot(&mut out, snapshot);
        }
        StorageRpcObjectReadSnapshotOutcome::StaleSubject => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_object_read_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectReadSnapshotOutcome::Loaded(Box::new(
            decoder.read_object_read_snapshot()?,
        )),
        1 => StorageRpcObjectReadSnapshotOutcome::StaleSubject,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object read snapshot outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectReadSnapshotResponse { outcome })
}

pub(crate) fn encode_object_tags_for_subject_response(
    response: &StorageRpcObjectTagsForSubjectResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectTagsForSubjectOutcome::Loaded(tags) => {
            put_u8(&mut out, 0);
            put_optional_string(&mut out, tags.as_deref());
        }
        StorageRpcObjectTagsForSubjectOutcome::StaleSubject => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_object_tags_for_subject_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectTagsForSubjectResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectTagsForSubjectOutcome::Loaded(decoder.read_optional_string()?),
        1 => StorageRpcObjectTagsForSubjectOutcome::StaleSubject,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object tags for subject outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectTagsForSubjectResponse { outcome })
}

pub(crate) fn encode_put_object_metadata_snapshot_response(
    response: &StorageRpcPutObjectMetadataSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(stored) => {
            put_u8(&mut out, 0);
            put_stored_object(&mut out, stored);
        }
        StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_put_object_metadata_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(Box::new(
            decoder.read_stored_object()?,
        )),
        1 => StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object metadata PUT snapshot outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPutObjectMetadataSnapshotResponse { outcome })
}

pub(crate) fn encode_object_delete_snapshot_response(
    response: &StorageRpcObjectDeleteSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_stored_object(&mut out, response.stored.as_ref());
    put_optional_delete_object_version_target(&mut out, response.target.as_ref());
    out
}

pub(crate) fn decode_object_delete_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectDeleteSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let stored = decoder.read_optional_stored_object()?;
    let target = decoder.read_optional_delete_object_version_target()?;
    decoder.finish()?;
    Ok(StorageRpcObjectDeleteSnapshotResponse { stored, target })
}

pub(crate) fn encode_object_lifecycle_version_list_response(
    response: &StorageRpcObjectLifecycleVersionListResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.versions);
    out
}

pub(crate) fn decode_object_lifecycle_version_list_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectLifecycleVersionListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let versions = decoder.read_stored_object_list()?;
    decoder.finish()?;
    Ok(StorageRpcObjectLifecycleVersionListResponse { versions })
}

pub(crate) fn encode_object_metadata_command_build_response(
    response: &StorageRpcObjectMetadataCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 0);
            put_metadata_command_envelope_response_item(&mut out, command);
        }
        StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => put_u8(&mut out, 1),
        StorageRpcObjectMetadataCommandBuildOutcome::Missing => put_u8(&mut out, 2),
        StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 3);
            put_u32(&mut out, *node_id);
            put_u32(&mut out, *pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, *log_index);
        }
    }
    out
}

pub(crate) fn decode_object_metadata_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectMetadataCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
            decoder.read_metadata_command_envelope_response_item()?,
        )),
        1 => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
        2 => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
        3 => StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object metadata command build outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectMetadataCommandBuildResponse { outcome })
}

pub(crate) fn encode_object_generation_reservation_response(
    response: &StorageRpcObjectGenerationReservationResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectGenerationReservationOutcome::Found(generation_id) => {
            put_u8(&mut out, 0);
            put_u64(&mut out, generation_id.get());
        }
        StorageRpcObjectGenerationReservationOutcome::NotFound { reservation_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, reservation_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_object_generation_reservation_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationReservationResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectGenerationReservationOutcome::Found(decoder.read_generation_id()?),
        1 => StorageRpcObjectGenerationReservationOutcome::NotFound {
            reservation_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "unknown object generation reservation response tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationReservationResponse { outcome })
}

pub(crate) fn encode_direct_put_commit_snapshot_response(
    response: &StorageRpcDirectPutCommitSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_direct_put_commit_storage_snapshot(&mut out, &response.snapshot);
    out
}

pub(crate) fn decode_direct_put_commit_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommitSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_direct_put_commit_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommitSnapshotResponse { snapshot })
}

pub(crate) fn encode_direct_put_command_build_response(
    response: &StorageRpcDirectPutCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcDirectPutCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 0);
            put_metadata_command_envelope_response_item(&mut out, command);
        }
        StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot => put_u8(&mut out, 1),
        StorageRpcDirectPutCommandBuildOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, *node_id);
            put_u32(&mut out, *pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, *log_index);
        }
    }
    out
}

pub(crate) fn decode_direct_put_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcDirectPutCommandBuildOutcome::Command(Box::new(
            decoder.read_metadata_command_envelope_response_item()?,
        )),
        1 => StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
        2 => StorageRpcDirectPutCommandBuildOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid direct PUT command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommandBuildResponse { outcome })
}

pub(crate) fn encode_create_bucket_command_build_request(
    request: &StorageRpcCreateBucketCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.bucket != request.config.name {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "request bucket must match create-bucket config name",
        ));
    }
    if request.command_id.cluster_epoch() != request.cluster_epoch
        || request.command_id.pg_id() != request.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&StorageRpcBucketRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        bucket: request.bucket.clone(),
    });
    put_u64(&mut out, request.command_id.log_index().get());
    put_create_bucket_config(&mut out, &request.config);
    Ok(out)
}

pub(crate) fn decode_create_bucket_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateBucketCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    let config = decoder.read_create_bucket_config()?;
    decoder.finish()?;
    if bucket != config.name {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "request bucket must match create-bucket config name",
        ));
    }
    Ok(StorageRpcCreateBucketCommandBuildRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        command_id: crate::metadata_command::MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            log_index,
        ),
        config,
    })
}

pub(crate) fn encode_completed_multipart_order_command_build_request(
    request: &StorageRpcCompletedMultipartOrderCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.cluster_epoch
        || request.command_id.pg_id() != request.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&StorageRpcBucketRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        bucket: request.bucket.clone(),
    });
    put_u64(&mut out, request.command_id.log_index().get());
    Ok(out)
}

pub(crate) fn decode_completed_multipart_order_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCompletedMultipartOrderCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    decoder.finish()?;
    Ok(StorageRpcCompletedMultipartOrderCommandBuildRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        command_id: crate::metadata_command::MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            log_index,
        ),
    })
}

pub(crate) fn encode_bucket_metadata_control_pending_match_request(
    request: &StorageRpcBucketMetadataControlPendingMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command.id().cluster_epoch() != request.bucket.cluster_epoch
        || request.command.id().pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "pending command route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_bytes(&mut out, &request.command.command_bytes());
    put_bucket_metadata_control_mutation(&mut out, &request.mutation);
    Ok(out)
}

pub(crate) fn decode_bucket_metadata_control_pending_match_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let command = decoder.read_metadata_command_envelope_bytes()?;
    let mutation = decoder.read_bucket_metadata_control_mutation()?;
    decoder.finish()?;
    if command.id().cluster_epoch() != bucket.cluster_epoch || command.id().pg_id() != bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "pending command route must match request route",
        ));
    }
    Ok(StorageRpcBucketMetadataControlPendingMatchRequest {
        bucket,
        command,
        mutation,
    })
}

pub(crate) fn encode_bucket_metadata_control_command_build_request(
    request: &StorageRpcBucketMetadataControlCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.bucket.cluster_epoch
        || request.command_id.pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.command_id.log_index().get());
    put_bucket_metadata_control_mutation(&mut out, &request.mutation);
    Ok(out)
}

pub(crate) fn decode_bucket_metadata_control_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketMetadataControlCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    let mutation = decoder.read_bucket_metadata_control_mutation()?;
    decoder.finish()?;
    Ok(StorageRpcBucketMetadataControlCommandBuildRequest {
        command_id: crate::metadata_command::MetadataCommandId::new(
            bucket.cluster_epoch,
            bucket.pg_id,
            log_index,
        ),
        bucket,
        mutation,
    })
}

pub(crate) fn encode_bucket_mark_deleting_command_build_request(
    request: &StorageRpcBucketMarkDeletingCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.bucket.cluster_epoch
        || request.command_id.pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.command_id.log_index().get());
    Ok(out)
}

pub(crate) fn decode_bucket_mark_deleting_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketMarkDeletingCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    decoder.finish()?;
    Ok(StorageRpcBucketMarkDeletingCommandBuildRequest {
        command_id: crate::metadata_command::MetadataCommandId::new(
            bucket.cluster_epoch,
            bucket.pg_id,
            log_index,
        ),
        bucket,
    })
}

pub(crate) fn encode_bucket_subresource_get_request(
    request: &StorageRpcBucketSubresourceGetRequest,
) -> Vec<u8> {
    let mut out = encode_bucket_request(&request.bucket);
    put_bucket_subresource_kind(&mut out, request.kind);
    out
}

pub(crate) fn decode_bucket_subresource_get_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketSubresourceGetRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let kind = decoder.read_bucket_subresource_kind()?;
    decoder.finish()?;
    Ok(StorageRpcBucketSubresourceGetRequest { bucket, kind })
}

pub(crate) fn encode_bucket_info_outcome_response(
    response: &StorageRpcBucketInfoOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketInfoOutcome::Info(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcBucketInfoOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    out
}

pub(crate) fn decode_bucket_info_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketInfoOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketInfoOutcome::Info(decoder.read_bucket_info()?),
        1 => StorageRpcBucketInfoOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket info outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketInfoOutcomeResponse { outcome })
}

pub(crate) fn encode_bucket_snapshot_response(
    response: &StorageRpcBucketSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_bucket_snapshot(&mut out, snapshot);
        }
        StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    out
}

pub(crate) fn decode_bucket_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketSnapshotOutcome::Loaded(Box::new(decoder.read_bucket_snapshot()?)),
        1 => StorageRpcBucketSnapshotOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket snapshot outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketSnapshotResponse { outcome })
}

pub(crate) fn encode_bucket_snapshot_pair_response(
    response: &StorageRpcBucketSnapshotPairResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketSnapshotPairOutcome::Loaded(pair) => {
            put_u8(&mut out, 0);
            put_bucket_snapshot_pair(&mut out, pair);
        }
        StorageRpcBucketSnapshotPairOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    out
}

pub(crate) fn decode_bucket_snapshot_pair_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotPairResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketSnapshotPairOutcome::Loaded(Box::new(
            decoder.read_bucket_snapshot_pair()?,
        )),
        1 => StorageRpcBucketSnapshotPairOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket snapshot pair outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketSnapshotPairResponse { outcome })
}

pub(crate) fn encode_create_bucket_command_build_response(
    response: &StorageRpcCreateBucketCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcCreateBucketCommandBuildOutcome::Exists(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcCreateBucketCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_create_bucket_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcCreateBucketCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcCreateBucketCommandBuildOutcome::Exists(decoder.read_bucket_info()?),
        1 => {
            let command = decoder.read_metadata_command_envelope_bytes()?;
            StorageRpcCreateBucketCommandBuildOutcome::Command(Box::new(command))
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid create-bucket command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcCreateBucketCommandBuildResponse { outcome })
}

pub(crate) fn encode_completed_multipart_order_command_build_response(
    response: &StorageRpcCompletedMultipartOrderCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.completion_order);
    put_bytes(&mut out, &response.command.command_bytes());
    out
}

pub(crate) fn decode_completed_multipart_order_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcCompletedMultipartOrderCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let completion_order = decoder.read_u64()?;
    let command = decoder.read_metadata_command_envelope_bytes()?;
    decoder.finish()?;
    Ok(StorageRpcCompletedMultipartOrderCommandBuildResponse {
        completion_order,
        command,
    })
}

pub(crate) fn encode_bucket_metadata_control_command_build_response(
    response: &StorageRpcBucketMetadataControlCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, &response.command.command_bytes());
    out
}

pub(crate) fn decode_bucket_metadata_control_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketMetadataControlCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let command = decoder.read_metadata_command_envelope_bytes()?;
    decoder.finish()?;
    Ok(StorageRpcBucketMetadataControlCommandBuildResponse { command })
}

pub(crate) fn encode_bucket_mark_deleting_command_build_response(
    response: &StorageRpcBucketMarkDeletingCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_bucket_mark_deleting_command_build_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketMarkDeletingCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(
            decoder.read_bucket_info()?,
        ),
        1 => {
            let command = decoder.read_metadata_command_envelope_bytes()?;
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(Box::new(command))
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket mark-deleting command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketMarkDeletingCommandBuildResponse { outcome })
}

pub(crate) fn encode_bucket_subresource_get_response(
    response: &StorageRpcBucketSubresourceGetResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_string(&mut out, response.body.as_deref());
    out
}

pub(crate) fn decode_bucket_subresource_get_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketSubresourceGetResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let body = decoder.read_optional_string()?;
    decoder.finish()?;
    Ok(StorageRpcBucketSubresourceGetResponse { body })
}

pub(crate) fn encode_lifecycle_sweep_roots_request(
    request: &StorageRpcLifecycleSweepRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_u64(&mut out, request.now);
    put_u64(
        &mut out,
        u64::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u64::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let limit = usize::try_from(decoder.read_u64()?).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: usize::MAX,
            limit: usize::MAX,
        }
    })?;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    Ok(StorageRpcLifecycleSweepRootsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        now,
        limit,
    })
}

pub(crate) fn encode_lifecycle_sweep_roots_response(
    response: &StorageRpcLifecycleSweepRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.roots.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.roots.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for root in &response.roots {
        put_lifecycle_sweep_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder
        .read_bounded_remaining_count(4 + 8 + 1, "lifecycle sweep root count exceeds payload")?;
    if count > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_lifecycle_sweep_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepRootsResponse { roots })
}

fn validate_cleanup_list_limit(value: u32) -> Result<(), StorageRpcPayloadError> {
    if value == 0 {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "cleanup list limit must be nonzero",
        ));
    }
    if value > STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value as usize,
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_list_page_item_limit(value: u32) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value as usize,
            limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_list_page_item_count(value: usize) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value,
            limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_bucket_metadata_item_count(value: usize) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value,
            limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
        });
    }
    Ok(())
}

pub(crate) fn encode_list_objects_request(
    request: &StorageRpcListObjectsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_keys)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.start_after.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.start_at.as_ref().map(|key| key.as_str()),
    );
    put_u32(&mut out, request.request.max_keys);
    Ok(out)
}

pub(crate) fn decode_list_objects_request(
    bytes: &[u8],
) -> Result<StorageRpcListObjectsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let request = ListObjectsReq {
        bucket: decoder.read_bucket_name()?,
        prefix: decoder.read_optional_object_key()?,
        start_after: decoder.read_optional_object_key()?,
        start_at: decoder.read_optional_object_key()?,
        max_keys: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_keys)?;
    Ok(StorageRpcListObjectsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_objects_response(
    response: &StorageRpcListObjectsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.objects.len())?;
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.response.objects);
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_start_after
            .as_ref()
            .map(|key| key.as_str()),
    );
    Ok(out)
}

pub(crate) fn decode_list_objects_response(
    bytes: &[u8],
) -> Result<StorageRpcListObjectsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let objects = decoder.read_stored_object_list_with_limit(STORAGE_RPC_MAX_LIST_PAGE_ITEMS)?;
    let is_truncated = decoder.read_bool()?;
    let next_start_after = decoder.read_optional_object_key()?;
    decoder.finish()?;
    Ok(StorageRpcListObjectsResponse {
        response: ListObjectsResp {
            objects,
            is_truncated,
            next_start_after,
        },
    })
}

pub(crate) fn encode_list_object_versions_request(
    request: &StorageRpcListObjectVersionsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_keys)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.key_marker.as_ref().map(|key| key.as_str()),
    );
    put_optional_version_id(&mut out, request.request.version_id_marker);
    put_optional_string(
        &mut out,
        request.request.start_at.as_ref().map(|key| key.as_str()),
    );
    put_u32(&mut out, request.request.max_keys);
    Ok(out)
}

pub(crate) fn decode_list_object_versions_request(
    bytes: &[u8],
) -> Result<StorageRpcListObjectVersionsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let request = ListObjectVersionsReq {
        bucket: decoder.read_bucket_name()?,
        prefix: decoder.read_optional_object_key()?,
        key_marker: decoder.read_optional_object_key()?,
        version_id_marker: decoder.read_optional_version_id()?,
        start_at: decoder.read_optional_object_key()?,
        max_keys: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_keys)?;
    Ok(StorageRpcListObjectVersionsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_object_versions_response(
    response: &StorageRpcListObjectVersionsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.versions.len())?;
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.response.versions);
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_key_marker
            .as_ref()
            .map(|key| key.as_str()),
    );
    put_optional_version_id(&mut out, response.response.next_version_id_marker);
    Ok(out)
}

pub(crate) fn decode_list_object_versions_response(
    bytes: &[u8],
) -> Result<StorageRpcListObjectVersionsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let versions = decoder.read_stored_object_list_with_limit(STORAGE_RPC_MAX_LIST_PAGE_ITEMS)?;
    let is_truncated = decoder.read_bool()?;
    let next_key_marker = decoder.read_optional_object_key()?;
    let next_version_id_marker = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcListObjectVersionsResponse {
        response: ListObjectVersionsResp {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        },
    })
}

pub(crate) fn encode_list_multipart_uploads_request(
    request: &StorageRpcListMultipartUploadsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_uploads)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.key_marker.as_ref().map(|key| key.as_str()),
    );
    match request.request.upload_id_marker.as_ref() {
        None => put_u8(&mut out, 0),
        Some(upload_id) => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    put_u32(&mut out, request.request.max_uploads);
    Ok(out)
}

pub(crate) fn decode_list_multipart_uploads_request(
    bytes: &[u8],
) -> Result<StorageRpcListMultipartUploadsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let request = ListMultipartUploadsReq {
        bucket: decoder.read_bucket_name()?,
        prefix: decoder.read_optional_object_key()?,
        key_marker: decoder.read_optional_object_key()?,
        upload_id_marker: decoder.read_optional_upload_id()?,
        max_uploads: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_uploads)?;
    Ok(StorageRpcListMultipartUploadsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_multipart_uploads_response(
    response: &StorageRpcListMultipartUploadsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.uploads.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.response.uploads.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.response.uploads.len(),
                limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
            }
        })?,
    );
    for upload in &response.response.uploads {
        put_multipart_upload_record(&mut out, upload);
    }
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_key_marker
            .as_ref()
            .map(|key| key.as_str()),
    );
    match response.response.next_upload_id_marker.as_ref() {
        None => put_u8(&mut out, 0),
        Some(upload_id) => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_list_multipart_uploads_response(
    bytes: &[u8],
) -> Result<StorageRpcListMultipartUploadsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let upload_count = decoder.read_limited_bounded_remaining_count(
        4 + UPLOAD_ID_LEN + 4 + STORAGE_RPC_MAX_BUCKET_NAME_LEN + 4,
        "multipart upload list count exceeds payload",
        STORAGE_RPC_MAX_LIST_PAGE_ITEMS,
    )?;
    let mut uploads = Vec::new();
    for _ in 0..upload_count {
        uploads.push(decoder.read_multipart_upload_record()?);
    }
    let is_truncated = decoder.read_bool()?;
    let next_key_marker = decoder.read_optional_object_key()?;
    let next_upload_id_marker = decoder.read_optional_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcListMultipartUploadsResponse {
        response: ListMultipartUploadsResp {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        },
    })
}

pub(crate) fn encode_lifecycle_sweep_buckets_response(
    response: &StorageRpcLifecycleSweepBucketsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.buckets.lifecycle_buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.buckets.lifecycle_buckets.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for bucket in &response.buckets.lifecycle_buckets {
        put_bucket_info(&mut out, bucket);
    }
    put_u32(
        &mut out,
        u32::try_from(response.buckets.aborting_buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.buckets.aborting_buckets.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for bucket in &response.buckets.aborting_buckets {
        put_string(&mut out, bucket.as_str());
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_buckets_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepBucketsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let lifecycle_count =
        decoder.read_bounded_remaining_count(1, "lifecycle bucket count exceeds payload")?;
    let mut lifecycle_buckets = Vec::new();
    for _ in 0..lifecycle_count {
        lifecycle_buckets.push(decoder.read_bucket_info()?);
    }
    let aborting_count =
        decoder.read_bounded_remaining_count(1, "aborting bucket count exceeds payload")?;
    let mut aborting_buckets = Vec::new();
    for _ in 0..aborting_count {
        aborting_buckets.push(decoder.read_bucket_name()?);
    }
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepBucketsResponse {
        buckets: LifecycleSweepBuckets {
            lifecycle_buckets,
            aborting_buckets,
        },
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_acquire_request(
    request: &StorageRpcLifecycleSweepClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &request.claim_id,
        &request.owner_token,
        "lifecycle-sweep",
        None,
    )?;
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    validate_bucket_write_reservation_identity(&claim_id, &owner_token, "lifecycle-sweep", None)?;
    Ok(StorageRpcLifecycleSweepClaimAcquireRequest {
        bucket,
        bucket_incarnation_generation,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_record_request(
    request: &StorageRpcLifecycleSweepClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&request.claim)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_lifecycle_sweep_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&claim)?;
    Ok(StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_heartbeat_request(
    request: &StorageRpcLifecycleSweepClaimHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_lifecycle_sweep_claim_record_request(&request.record)?;
    put_u64(&mut out, request.heartbeat_at);
    put_optional_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    let heartbeat_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    decoder.finish()?;
    let record = StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    };
    if record.cluster_epoch != record.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&record.claim)?;
    Ok(StorageRpcLifecycleSweepClaimHeartbeatRequest {
        record,
        heartbeat_at,
        lease_deadline,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_error_request(
    request: &StorageRpcLifecycleSweepClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_lifecycle_sweep_claim_record_request(&request.record)?;
    put_string(&mut out, &request.last_error);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        4096,
        StorageRpcPayloadError::InvalidDurableClaimToken("lifecycle error exceeds maximum length"),
    )?;
    decoder.finish()?;
    let record = StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    };
    if record.cluster_epoch != record.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&record.claim)?;
    Ok(StorageRpcLifecycleSweepClaimErrorRequest { record, last_error })
}

pub(crate) fn encode_lifecycle_sweep_claim_optional_record_response(
    response: &StorageRpcLifecycleSweepClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_lifecycle_sweep_claim_record(record)?;
            put_u8(&mut out, 1);
            put_lifecycle_sweep_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_lifecycle_sweep_claim_record()?;
            validate_lifecycle_sweep_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional lifecycle sweep claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_lifecycle_sweep_claim_record_response(
    response: &StorageRpcLifecycleSweepClaimRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_lifecycle_sweep_claim_record(&response.record)?;
    let mut out = Vec::new();
    put_lifecycle_sweep_claim_record(&mut out, &response.record);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_record_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = decoder.read_lifecycle_sweep_claim_record()?;
    decoder.finish()?;
    validate_lifecycle_sweep_claim_record(&record)?;
    Ok(StorageRpcLifecycleSweepClaimRecordResponse { record })
}

pub(crate) fn encode_metadata_command_pending_slot_request(
    request: &StorageRpcMetadataCommandPendingSlotRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.command.id())?;
    let command_request = StorageRpcMetadataCommandRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        command: request.command.clone(),
    };
    let mut out = encode_metadata_command_request(&command_request)?;
    match request.scope_bucket.as_ref() {
        None => put_u8(&mut out, 0),
        Some(bucket) => {
            put_u8(&mut out, 1);
            put_string(&mut out, bucket.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_pending_slot_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    let command = metadata_command_envelope_from_item(&item)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    let scope_bucket = match decoder.read_u8()? {
        0 => None,
        1 => Some(decoder.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "invalid scope bucket name",
            )
        })?),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional scope bucket tag",
                ),
            )
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
        scope_bucket,
    })
}

pub(crate) fn encode_metadata_command_pending_slot_replace_request(
    request: &StorageRpcMetadataCommandPendingSlotReplaceRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.previous.id())?;
    validate_metadata_command_route(
        request.cluster_epoch,
        request.pg_id,
        request.replacement.id(),
    )?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    let previous = StorageRpcMetadataCommandItem {
        command_checksum: request.previous.checksum_crc64(),
        command_bytes: request.previous.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&previous)?);
    let replacement = StorageRpcMetadataCommandItem {
        command_checksum: request.replacement.checksum_crc64(),
        command_bytes: request.replacement.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&replacement)?);
    match request.scope_bucket.as_ref() {
        None => put_u8(&mut out, 0),
        Some(bucket) => {
            put_u8(&mut out, 1);
            put_string(&mut out, bucket.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_pending_slot_replace_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotReplaceRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let previous_item = decoder.read_metadata_command_item()?;
    let previous = metadata_command_envelope_from_item(&previous_item)?;
    validate_metadata_command_route(cluster_epoch, pg_id, previous.id())?;
    let replacement_item = decoder.read_metadata_command_item()?;
    let replacement = metadata_command_envelope_from_item(&replacement_item)?;
    validate_metadata_command_route(cluster_epoch, pg_id, replacement.id())?;
    let scope_bucket = match decoder.read_u8()? {
        0 => None,
        1 => Some(decoder.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "invalid scope bucket name",
            )
        })?),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional scope bucket tag",
                ),
            )
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotReplaceRequest {
        node_id,
        cluster_epoch,
        pg_id,
        previous,
        replacement,
        scope_bucket,
    })
}

pub(crate) fn encode_metadata_command_pending_slot_insert_response(
    response: &StorageRpcMetadataCommandPendingSlotInsertResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => put_u8(&mut out, 0),
        StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
            pg_id,
            cluster_epoch,
            existing_log_index,
            candidate_log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, existing_log_index);
            put_u64(&mut out, candidate_log_index);
        }
        StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_pending_slot_insert_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotInsertResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted,
        1 => StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            existing_log_index: decoder.read_u64()?,
            candidate_log_index: decoder.read_u64()?,
        },
        2 => StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command pending slot insert outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotInsertResponse { outcome })
}

pub(crate) fn encode_metadata_command_pending_slot_remove_response(
    response: &StorageRpcMetadataCommandPendingSlotRemoveResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, u8::from(response.removed));
    out
}

pub(crate) fn decode_metadata_command_pending_slot_remove_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotRemoveResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let removed = match decoder.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command pending slot remove outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotRemoveResponse { removed })
}

pub(crate) fn encode_metadata_command_next_id_request(
    request: &StorageRpcMetadataCommandNextIdRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.min_log_index);
    out
}

pub(crate) fn decode_metadata_command_next_id_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandNextIdRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let min_log_index = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandNextIdRequest {
        node_id,
        cluster_epoch,
        pg_id,
        min_log_index,
    })
}

pub(crate) fn encode_metadata_command_log_hash_range_request(
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.first_log_index.get());
    put_u64(&mut out, request.last_log_index.get());
    out
}

pub(crate) fn decode_metadata_command_log_hash_range_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    decode_metadata_command_log_range_request_with_limit(
        bytes,
        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES,
        "metadata command hash range",
    )
}

pub(crate) fn decode_metadata_command_log_entry_range_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    decode_metadata_command_log_range_request_with_limit(
        bytes,
        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES,
        "metadata command entry range",
    )
}

fn decode_metadata_command_log_range_request_with_limit(
    bytes: &[u8],
    max_entries: u64,
    context: &'static str,
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let first_log_index = MetadataCommandLogIndex::new(decoder.read_u64()?).ok_or({
        StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range first log index must not be zero",
        )
    })?;
    let last_log_index = MetadataCommandLogIndex::new(decoder.read_u64()?).ok_or({
        StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range last log index must not be zero",
        )
    })?;
    let (ordered_message, too_large_message) = match context {
        "metadata command entry range" => (
            "metadata command entry range must be ordered",
            "metadata command entry range is too large",
        ),
        _ => (
            "metadata command hash range must be ordered",
            "metadata command hash range is too large",
        ),
    };
    if last_log_index.get() < first_log_index.get() {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            ordered_message,
        ));
    }
    let requested = last_log_index.get() - first_log_index.get() + 1;
    if requested > max_entries {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            too_large_message,
        ));
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogHashRangeRequest {
        node_id,
        cluster_epoch,
        pg_id,
        first_log_index,
        last_log_index,
    })
}

pub(crate) fn encode_metadata_command_max_log_index_response(
    response: &StorageRpcMetadataCommandMaxLogIndexResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.max_log_index);
    out
}

pub(crate) fn decode_metadata_command_max_log_index_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandMaxLogIndexResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let max_log_index = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index })
}

pub(crate) fn encode_metadata_command_log_hash_range_response(
    response: &StorageRpcMetadataCommandLogHashRangeResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.entries.len() > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES as usize {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range response is too large",
        ));
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.entries.len()).expect("bounded response count fits u32"),
    );
    for entry in &response.entries {
        put_u64(&mut out, entry.log_index);
        put_u64(&mut out, entry.previous_log_hash);
        put_u64(&mut out, entry.log_hash);
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_log_hash_range_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if u64::from(count) > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range response is too large",
        ));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let log_index = decoder.read_u64()?;
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range response log index must not be zero",
            ));
        }
        entries.push(MetadataCommandLogHashRangeEntry {
            log_index,
            previous_log_hash: decoder.read_u64()?,
            log_hash: decoder.read_u64()?,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogHashRangeResponse { entries })
}

pub(crate) fn encode_metadata_command_log_entry_range_response(
    response: &StorageRpcMetadataCommandLogEntryRangeResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.entries.len() > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES as usize {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command entry range response is too large",
        ));
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.entries.len()).expect("bounded response count fits u32"),
    );
    for entry in &response.entries {
        if MetadataCommandLogIndex::new(entry.log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range response log index must not be zero",
            ));
        }
        put_u64(&mut out, entry.log_index);
        put_u64(&mut out, entry.previous_log_hash);
        put_u64(&mut out, entry.log_hash);
        match entry.pre_state_digest {
            Some(pre_state_digest) => {
                put_u8(&mut out, 1);
                put_u64(&mut out, pre_state_digest);
            }
            None => put_u8(&mut out, 0),
        }
        match entry.post_state_digest {
            Some(post_state_digest) => {
                put_u8(&mut out, 1);
                put_u64(&mut out, post_state_digest);
            }
            None => put_u8(&mut out, 0),
        }
        match &entry.kind {
            MetadataCommandLogRangeEntryKind::Applied(command) => {
                if command.id().log_index().get() != entry.log_index {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "metadata command entry range applied command log index mismatch",
                    ));
                }
                put_u8(&mut out, 0);
                let item = StorageRpcMetadataCommandItem {
                    command_checksum: command.checksum_crc64(),
                    command_bytes: command.command_bytes(),
                };
                out.extend_from_slice(&encode_metadata_command_item(&item)?);
            }
            MetadataCommandLogRangeEntryKind::Abandoned {
                original_command_checksum,
            } => {
                put_u8(&mut out, 1);
                put_u64(&mut out, *original_command_checksum);
            }
        }
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_log_entry_range_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogEntryRangeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if u64::from(count) > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command entry range response is too large",
        ));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let log_index = decoder.read_u64()?;
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range response log index must not be zero",
            ));
        }
        let previous_log_hash = decoder.read_u64()?;
        let log_hash = decoder.read_u64()?;
        let pre_state_digest = match decoder.read_u8()? {
            0 => None,
            1 => Some(decoder.read_u64()?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "metadata command entry range pre-state digest flag is invalid",
                ));
            }
        };
        let post_state_digest = match decoder.read_u8()? {
            0 => None,
            1 => Some(decoder.read_u64()?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "metadata command entry range post-state digest flag is invalid",
                ));
            }
        };
        let kind = match decoder.read_u8()? {
            0 => {
                let item = decoder.read_metadata_command_item()?;
                let command = metadata_command_envelope_from_item(&item)?;
                if command.id().log_index().get() != log_index {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "metadata command entry range applied command log index mismatch",
                    ));
                }
                MetadataCommandLogRangeEntryKind::Applied(Box::new(command))
            }
            1 => MetadataCommandLogRangeEntryKind::Abandoned {
                original_command_checksum: decoder.read_u64()?,
            },
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "unknown metadata command entry range kind",
                ));
            }
        };
        entries.push(MetadataCommandLogRangeEntry {
            log_index,
            previous_log_hash,
            log_hash,
            pre_state_digest,
            post_state_digest,
            kind,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogEntryRangeResponse { entries })
}

pub(crate) fn encode_metadata_command_next_id_response(
    response: &StorageRpcMetadataCommandNextIdResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandNextIdOutcome::Allocated {
            cluster_epoch,
            pg_id,
            log_index,
        } => {
            put_u8(&mut out, 0);
            put_u64(&mut out, cluster_epoch.get());
            put_u32(&mut out, pg_id.get());
            put_u64(&mut out, log_index);
        }
        StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_next_id_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandNextIdResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandNextIdOutcome::Allocated {
            cluster_epoch: decoder.read_cluster_epoch()?,
            pg_id: PgId::new(decoder.read_u32()?),
            log_index: decoder.read_u64()?,
        },
        1 => StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command next id outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandNextIdResponse { outcome })
}

pub(crate) fn encode_metadata_command_pending_envelope_response(
    response: &StorageRpcMetadataCommandPendingEnvelopeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.command.as_ref() {
        None => put_u8(&mut out, 0),
        Some(command) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, command.checksum_crc64());
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_metadata_command_pending_envelope_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingEnvelopeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let command = match decoder.read_u8()? {
        0 => None,
        1 => {
            let item = decoder.read_metadata_command_item()?;
            Some(metadata_command_envelope_from_item(&item)?)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command pending envelope tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingEnvelopeResponse { command })
}

pub(crate) fn encode_metadata_command_matching_applied_request(
    request: &StorageRpcMetadataCommandMatchingAppliedRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let command_request = StorageRpcMetadataCommandRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        command: request.command.clone(),
    };
    let mut out = encode_metadata_command_request(&command_request)?;
    put_u64(&mut out, request.expected_previous_log_hash);
    Ok(out)
}

pub(crate) fn decode_metadata_command_matching_applied_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandMatchingAppliedRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    let command = metadata_command_envelope_from_item(&item)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    let expected_previous_log_hash = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandMatchingAppliedRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
        expected_previous_log_hash,
    })
}

pub(crate) fn encode_metadata_command_applied_hashes_response(
    response: &StorageRpcMetadataCommandAppliedHashesResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None) => put_u8(&mut out, 0),
        StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((
            previous_log_hash,
            log_hash,
        ))) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, previous_log_hash);
            put_u64(&mut out, log_hash);
        }
        StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_applied_hashes_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandAppliedHashesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None),
        1 => StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((
            decoder.read_u64()?,
            decoder.read_u64()?,
        ))),
        2 => StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command applied hashes outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandAppliedHashesResponse { outcome })
}

pub(crate) fn encode_metadata_command_bool_response(
    response: &StorageRpcMetadataCommandBoolResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, u8::from(response.value));
    out
}

pub(crate) fn decode_metadata_command_bool_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandBoolResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let value = match decoder.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command bool response tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandBoolResponse { value })
}

pub(crate) fn encode_metadata_command_bool_outcome_response(
    response: &StorageRpcMetadataCommandBoolOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandBoolOutcome::Value(value) => {
            put_u8(&mut out, 0);
            put_u8(&mut out, u8::from(value));
        }
        StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_bool_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let value = match decoder.read_u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "invalid metadata command bool outcome value tag",
                    ))
                }
            };
            StorageRpcMetadataCommandBoolOutcome::Value(value)
        }
        1 => StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command bool outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandBoolOutcomeResponse { outcome })
}

pub(crate) fn encode_metadata_command_state_outcome_response(
    response: &StorageRpcMetadataCommandStateOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandStateOutcome::State(state) => {
            put_u8(&mut out, 0);
            put_u64(&mut out, state.cluster_epoch.get());
            put_u64(&mut out, state.applied_log_index);
            put_u64(&mut out, state.applied_log_hash);
            put_u64(&mut out, state.state_digest);
        }
        StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
        StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
            ref reservation_id,
            generation_id,
        } => {
            put_u8(&mut out, 2);
            put_string(&mut out, reservation_id.as_str());
            put_u64(&mut out, generation_id.get());
        }
        StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict { version_id } => {
            put_u8(&mut out, 3);
            put_u64(&mut out, version_id.to_u64());
        }
        StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            ref name,
            bucket_execution_generation,
        } => {
            put_u8(&mut out, 4);
            put_string(&mut out, name.as_str());
            put_u64(&mut out, bucket_execution_generation);
        }
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            ref bucket,
            ref key,
            write_sequence,
            generation_id,
        } => {
            put_u8(&mut out, 5);
            put_string(&mut out, bucket.as_str());
            put_string(&mut out, key.as_str());
            put_u64(&mut out, write_sequence);
            match generation_id {
                Some(generation_id) => {
                    put_u8(&mut out, 1);
                    put_u64(&mut out, generation_id.get());
                }
                None => put_u8(&mut out, 0),
            }
        }
        StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index } => {
            put_u8(&mut out, 6);
            put_u32(&mut out, segment_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_state_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandStateOutcome::State(MetadataCommandReplicaState {
            cluster_epoch: decoder.read_cluster_epoch()?,
            applied_log_index: decoder.read_u64()?,
            applied_log_hash: decoder.read_u64()?,
            state_digest: decoder.read_u64()?,
        }),
        1 => StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        2 => StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
            reservation_id: decoder.read_session_id()?,
            generation_id: decoder.read_generation_id()?,
        },
        3 => StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
            version_id: VersionId::from_u64(decoder.read_u64()?),
        },
        4 => StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name: decoder.read_bucket_name()?,
            bucket_execution_generation: decoder.read_u64()?,
        },
        5 => {
            let bucket = decoder.read_bucket_name()?;
            let key = decoder.read_object_key()?;
            let write_sequence = decoder.read_u64()?;
            let generation_id = match decoder.read_u8()? {
                0 => None,
                1 => Some(decoder.read_generation_id()?),
                _ => {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "invalid stale object write command generation presence tag",
                    ));
                }
            };
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence,
                generation_id,
            }
        }
        6 => StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict {
            segment_index: decoder.read_u32()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command state outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateOutcomeResponse { outcome })
}

pub(crate) fn encode_metadata_command_state_request(
    request: &StorageRpcMetadataCommandStateRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    out
}

pub(crate) fn decode_metadata_command_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
    })
}

pub(crate) fn encode_metadata_command_transfer_adopt_request(
    request: &StorageRpcMetadataCommandTransferAdoptRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.expected_state_digest);
    put_u32(
        &mut out,
        u32::try_from(request.commands.len()).map_err(|_| {
            StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command transfer adopt request command count exceeds u32",
            )
        })?,
    );
    for transfer_command in &request.commands {
        let command = &transfer_command.command;
        validate_metadata_command_route(request.cluster_epoch, request.pg_id, command.id())?;
        put_u64(&mut out, transfer_command.pre_state_digest);
        put_u64(&mut out, transfer_command.post_state_digest);
        let item = StorageRpcMetadataCommandItem {
            command_checksum: command.checksum_crc64(),
            command_bytes: command.command_bytes(),
        };
        out.extend_from_slice(&encode_metadata_command_item(&item)?);
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_transfer_adopt_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferAdoptRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let expected_state_digest = decoder.read_u64()?;
    let count = decoder.read_u32()?;
    let mut commands = Vec::new();
    for _ in 0..count {
        let pre_state_digest = decoder.read_u64()?;
        let post_state_digest = decoder.read_u64()?;
        let item = decoder.read_metadata_command_item()?;
        let command = metadata_command_envelope_from_item(&item)?;
        validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
        commands.push(MetadataTransferCommand {
            command,
            pre_state_digest,
            post_state_digest,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferAdoptRequest {
        node_id,
        cluster_epoch,
        pg_id,
        expected_state_digest,
        commands,
    })
}

pub(crate) fn encode_metadata_command_transfer_empty_state_request(
    request: &StorageRpcMetadataCommandTransferEmptyStateRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.expected_state_digest);
    out
}

pub(crate) fn decode_metadata_command_transfer_empty_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferEmptyStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let expected_state_digest = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferEmptyStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
        expected_state_digest,
    })
}

pub(crate) fn encode_metadata_command_transfer_matching_state_request(
    request: &StorageRpcMetadataCommandTransferMatchingStateRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.applied_log_index);
    put_u64(&mut out, request.applied_log_hash);
    put_u64(&mut out, request.expected_state_digest);
    out
}

pub(crate) fn decode_metadata_command_transfer_matching_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferMatchingStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = decoder.read_u64()?;
    let expected_state_digest = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferMatchingStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
        applied_log_index,
        applied_log_hash,
        expected_state_digest,
    })
}

pub(crate) fn encode_metadata_command_transfer_checkpoint_base_request(
    request: &StorageRpcMetadataCommandTransferCheckpointBaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    encode_metadata_command_checkpoint(&mut out, &request.checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_transfer_checkpoint_base_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferCheckpointBaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferCheckpointBaseRequest {
        node_id,
        cluster_epoch,
        pg_id,
        checkpoint,
    })
}

pub(crate) fn encode_metadata_command_checkpoint_response(
    response: &StorageRpcMetadataCommandCheckpointResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    encode_metadata_command_checkpoint(&mut out, &response.checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointResponse { checkpoint })
}

pub(crate) fn encode_metadata_command_checkpoint_candidates_request(
    request: &StorageRpcMetadataCommandCheckpointCandidatesRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.max_applied_log_index);
    put_u32(&mut out, request.limit);
    out
}

pub(crate) fn decode_metadata_command_checkpoint_candidates_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointCandidatesRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let max_applied_log_index = decoder.read_u64()?;
    let limit = decoder.read_u32()?;
    if usize::try_from(limit)
        .map(|limit| limit > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES)
        .unwrap_or(true)
    {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate limit",
            count: u64::from(limit),
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointCandidatesRequest {
        node_id,
        cluster_epoch,
        pg_id,
        max_applied_log_index,
        limit,
    })
}

pub(crate) fn encode_metadata_command_checkpoint_candidates_response(
    response: &StorageRpcMetadataCommandCheckpointCandidatesResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.checkpoints.len() > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate count",
            count: response.checkpoints.len() as u64,
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    let mut out = Vec::new();
    put_u32(&mut out, checked_u32_len(response.checkpoints.len())?);
    for checkpoint in &response.checkpoints {
        encode_metadata_command_checkpoint(&mut out, checkpoint)?;
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_candidates_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointCandidatesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if usize::try_from(count)
        .map(|count| count > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES)
        .unwrap_or(true)
    {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate count",
            count: u64::from(count),
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    let mut checkpoints = Vec::with_capacity(count as usize);
    for _ in 0..count {
        checkpoints.push(decode_metadata_command_checkpoint(&mut decoder)?);
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointCandidatesResponse { checkpoints })
}

pub(crate) fn encode_metadata_command_log_compact_response(
    response: &StorageRpcMetadataCommandLogCompactResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.status {
        MetadataCommandLogCompactionStatus::NoCheckpoint { retained_entries } => {
            out.push(0);
            put_u64(&mut out, retained_entries);
        }
        MetadataCommandLogCompactionStatus::PendingCommand { retained_entries } => {
            out.push(1);
            put_u64(&mut out, retained_entries);
        }
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries,
            compacted_before,
        } => {
            out.push(2);
            put_u64(&mut out, deleted_entries);
            put_u64(&mut out, compacted_before);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_log_compact_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogCompactResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let tag = decoder.read_u8()?;
    let status = match tag {
        0 => MetadataCommandLogCompactionStatus::NoCheckpoint {
            retained_entries: decoder.read_u64()?,
        },
        1 => MetadataCommandLogCompactionStatus::PendingCommand {
            retained_entries: decoder.read_u64()?,
        },
        2 => MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: decoder.read_u64()?,
            compacted_before: decoder.read_u64()?,
        },
        _ => return Err(StorageRpcPayloadError::InvalidMetadataCommandLogCompactionStatus(tag)),
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogCompactResponse { status })
}

pub(crate) fn encode_cluster_map_history_reference_summary_request(
    request: &StorageRpcClusterMapHistoryReferenceSummaryRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    out
}

pub(crate) fn decode_cluster_map_history_reference_summary_request(
    bytes: &[u8],
) -> Result<StorageRpcClusterMapHistoryReferenceSummaryRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    decoder.finish()?;
    Ok(StorageRpcClusterMapHistoryReferenceSummaryRequest {
        node_id,
        cluster_epoch,
    })
}

pub(crate) fn encode_cluster_map_history_reference_summary_response(
    response: &StorageRpcClusterMapHistoryReferenceSummaryResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_u64(
        &mut out,
        response
            .summary
            .oldest_live_placement_epoch
            .map(ClusterEpoch::get),
    );
    put_optional_u64(
        &mut out,
        response
            .summary
            .oldest_durable_backfill_epoch
            .map(ClusterEpoch::get),
    );
    out
}

pub(crate) fn decode_cluster_map_history_reference_summary_response(
    bytes: &[u8],
) -> Result<StorageRpcClusterMapHistoryReferenceSummaryResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let oldest_live_placement_epoch = decode_optional_cluster_epoch(decoder.read_optional_u64()?)?;
    let oldest_durable_backfill_epoch =
        decode_optional_cluster_epoch(decoder.read_optional_u64()?)?;
    decoder.finish()?;
    Ok(StorageRpcClusterMapHistoryReferenceSummaryResponse {
        summary: PgClusterMapHistoryReferenceSummary {
            oldest_live_placement_epoch,
            oldest_durable_backfill_epoch,
        },
    })
}

fn decode_optional_cluster_epoch(
    value: Option<u64>,
) -> Result<Option<ClusterEpoch>, StorageRpcPayloadError> {
    value
        .map(|value| {
            ClusterEpoch::new(value).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
                "cluster epoch must not be zero",
            ))
        })
        .transpose()
}

pub(crate) fn encode_metadata_command_checkpoint_payload(
    checkpoint: &MetadataCommandCheckpoint,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    encode_metadata_command_checkpoint(&mut out, checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_payload(
    bytes: &[u8],
) -> Result<MetadataCommandCheckpoint, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(checkpoint)
}

fn encode_metadata_command_checkpoint(
    out: &mut Vec<u8>,
    checkpoint: &MetadataCommandCheckpoint,
) -> Result<(), StorageRpcPayloadError> {
    put_u64(out, checkpoint.cluster_epoch.get());
    put_u32(out, checkpoint.pg_id.get());
    put_u64(out, checkpoint.applied_log_index);
    put_u64(out, checkpoint.applied_log_hash);
    put_u64(out, checkpoint.state_digest);
    put_u8(out, checkpoint.canonical_state_encoding_version);
    put_u32(out, checked_u32_len(checkpoint.table_digests.len())?);
    for digest in &checkpoint.table_digests {
        put_string(out, &digest.table_name);
        put_u64(out, digest.row_count);
        put_u64(out, digest.row_hash_xor);
        put_u64(out, digest.row_hash_sum);
        put_u64(out, digest.table_digest);
    }
    put_u32(out, checked_u32_len(checkpoint.table_blocks.len())?);
    for block in &checkpoint.table_blocks {
        encode_metadata_checkpoint_table_block(out, block)?;
    }
    put_u64(out, checkpoint.checkpoint_crc64);
    Ok(())
}

fn decode_metadata_command_checkpoint(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCommandCheckpoint, StorageRpcPayloadError> {
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = decoder.read_u64()?;
    let state_digest = decoder.read_u64()?;
    let canonical_state_encoding_version = decoder.read_u8()?;
    let table_digest_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_TABLES)?;
    let mut table_digests = Vec::with_capacity(table_digest_count);
    for _ in 0..table_digest_count {
        table_digests.push(MetadataCheckpointTableDigest {
            table_name: decoder.read_string()?,
            row_count: decoder.read_u64()?,
            row_hash_xor: decoder.read_u64()?,
            row_hash_sum: decoder.read_u64()?,
            table_digest: decoder.read_u64()?,
        });
    }
    let table_block_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_TABLES)?;
    let mut table_blocks = Vec::with_capacity(table_block_count);
    for _ in 0..table_block_count {
        table_blocks.push(decode_metadata_checkpoint_table_block(decoder)?);
    }
    let checkpoint_crc64 = decoder.read_u64()?;
    Ok(MetadataCommandCheckpoint {
        cluster_epoch,
        pg_id,
        applied_log_index,
        applied_log_hash,
        state_digest,
        canonical_state_encoding_version,
        table_digests,
        table_blocks,
        checkpoint_crc64,
    })
}

fn encode_metadata_checkpoint_table_block(
    out: &mut Vec<u8>,
    block: &MetadataCheckpointTableBlock,
) -> Result<(), StorageRpcPayloadError> {
    put_string(out, &block.table_name);
    put_string_vec(out, &block.columns)?;
    put_string_vec(out, &block.order_columns)?;
    put_string(out, &block.filter);
    put_u32(out, checked_u32_len(block.rows.len())?);
    for row in &block.rows {
        encode_metadata_checkpoint_row(out, row)?;
    }
    put_u64(out, block.row_count);
    put_u64(out, block.row_hash_xor);
    put_u64(out, block.row_hash_sum);
    put_u64(out, block.table_digest);
    Ok(())
}

fn decode_metadata_checkpoint_table_block(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointTableBlock, StorageRpcPayloadError> {
    let table_name = decoder.read_string()?;
    let columns =
        decoder.read_string_vec_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_COLUMNS)?;
    let order_columns =
        decoder.read_string_vec_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_COLUMNS)?;
    let filter = decoder.read_string()?;
    let row_count = decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROWS)?;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(decode_metadata_checkpoint_row(decoder)?);
    }
    Ok(MetadataCheckpointTableBlock {
        table_name,
        columns,
        order_columns,
        filter,
        rows,
        row_count: decoder.read_u64()?,
        row_hash_xor: decoder.read_u64()?,
        row_hash_sum: decoder.read_u64()?,
        table_digest: decoder.read_u64()?,
    })
}

fn encode_metadata_checkpoint_row(
    out: &mut Vec<u8>,
    row: &MetadataCheckpointRow,
) -> Result<(), StorageRpcPayloadError> {
    put_u32(out, checked_u32_len(row.values.len())?);
    for value in &row.values {
        encode_metadata_checkpoint_value(out, value)?;
    }
    put_u64(out, row.row_digest);
    Ok(())
}

fn decode_metadata_checkpoint_row(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointRow, StorageRpcPayloadError> {
    let value_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROW_VALUES)?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(decode_metadata_checkpoint_value(decoder)?);
    }
    Ok(MetadataCheckpointRow {
        values,
        row_digest: decoder.read_u64()?,
    })
}

fn encode_metadata_checkpoint_value(
    out: &mut Vec<u8>,
    value: &MetadataCheckpointValue,
) -> Result<(), StorageRpcPayloadError> {
    match value {
        MetadataCheckpointValue::Null => put_u8(out, 0),
        MetadataCheckpointValue::Integer(value) => {
            put_u8(out, 1);
            put_u64(out, *value as u64);
        }
        MetadataCheckpointValue::RealBits(value) => {
            put_u8(out, 2);
            put_u64(out, *value);
        }
        MetadataCheckpointValue::Text(value) => {
            put_u8(out, 3);
            if value.len() > STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: value.len(),
                    limit: STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN,
                });
            }
            put_bytes(out, value);
        }
        MetadataCheckpointValue::Blob(value) => {
            put_u8(out, 4);
            if value.len() > STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: value.len(),
                    limit: STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN,
                });
            }
            put_bytes(out, value);
        }
    }
    Ok(())
}

fn decode_metadata_checkpoint_value(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointValue, StorageRpcPayloadError> {
    match decoder.read_u8()? {
        0 => Ok(MetadataCheckpointValue::Null),
        1 => Ok(MetadataCheckpointValue::Integer(decoder.read_u64()? as i64)),
        2 => Ok(MetadataCheckpointValue::RealBits(decoder.read_u64()?)),
        3 => Ok(MetadataCheckpointValue::Text(
            decoder
                .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN)?
                .to_vec(),
        )),
        4 => Ok(MetadataCheckpointValue::Blob(
            decoder
                .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN)?
                .to_vec(),
        )),
        _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "invalid metadata checkpoint value tag",
        )),
    }
}

pub(crate) fn encode_metadata_command_state_response(
    response: &StorageRpcMetadataCommandStateResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.state.cluster_epoch.get());
    put_u64(&mut out, response.state.applied_log_index);
    put_u64(&mut out, response.state.applied_log_hash);
    put_u64(&mut out, response.state.state_digest);
    out
}

pub(crate) fn decode_metadata_command_state_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = decoder.read_u64()?;
    let state_digest = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateResponse {
        state: MetadataCommandReplicaState {
            cluster_epoch,
            applied_log_index,
            applied_log_hash,
            state_digest,
        },
    })
}

pub(crate) fn encode_metadata_command_acceptance_response(
    response: &StorageRpcMetadataCommandAcceptanceResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::Apply,
        ) => put_u8(&mut out, 1),
        StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::AlreadyApplied,
        ) => put_u8(&mut out, 2),
        StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 3);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_acceptance_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandAcceptanceResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => {
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(MetadataCommandAcceptance::Apply)
        }
        2 => StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::AlreadyApplied,
        ),
        3 => StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command acceptance tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandAcceptanceResponse { outcome })
}

fn validate_metadata_command_route(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    command_id: crate::metadata_command::MetadataCommandId,
) -> Result<(), StorageRpcPayloadError> {
    if command_id.cluster_epoch() != cluster_epoch {
        return Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(
            "command epoch does not match RPC route",
        ));
    }
    if command_id.pg_id() != pg_id {
        return Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(
            "command PG does not match RPC route",
        ));
    }
    Ok(())
}

pub(crate) fn encode_shard_write_item(
    item: &StorageRpcShardWriteItem,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_write_payload(item.expected_size, item.expected_crc64, &item.payload)?;
    let mut out = Vec::new();
    put_u64(&mut out, item.expected_size);
    put_u64(&mut out, item.expected_crc64);
    put_bytes(&mut out, &item.payload);
    Ok(out)
}

pub(crate) fn decode_shard_write_item(
    bytes: &[u8],
) -> Result<StorageRpcShardWriteItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let expected_size = decoder.read_u64()?;
    let expected_crc64 = decoder.read_u64()?;
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_write_payload(expected_size, expected_crc64, &payload)?;
    Ok(StorageRpcShardWriteItem {
        expected_size,
        expected_crc64,
        payload,
    })
}

pub(crate) fn encode_shard_write_request(
    request: &StorageRpcShardWriteRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    validate_shard_write_payload(
        request.expected_size,
        request.expected_crc64,
        &request.payload,
    )?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_size);
    put_u64(&mut out, request.expected_crc64);
    put_bytes(&mut out, &request.payload);
    Ok(out)
}

pub(crate) fn decode_shard_write_request(
    bytes: &[u8],
) -> Result<StorageRpcShardWriteRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_size = decoder.read_u64()?;
    let expected_crc64 = decoder.read_u64()?;
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    validate_shard_write_payload(expected_size, expected_crc64, &payload)?;
    Ok(StorageRpcShardWriteRequest {
        location,
        shard_key,
        expected_size,
        expected_crc64,
        payload,
    })
}

pub(crate) fn encode_shard_write_ack(ack: WriteAck) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, ack.stored_size);
    put_u64(&mut out, ack.crc64);
    out
}

pub(crate) fn decode_shard_write_ack(
    bytes: &[u8],
    expected_size: u64,
    expected_crc64: u64,
) -> Result<WriteAck, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let stored_size = decoder.read_u64()?;
    let crc64 = decoder.read_u64()?;
    decoder.finish()?;
    if stored_size != expected_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_size,
            actual: stored_size,
        });
    }
    if crc64 != expected_crc64 {
        return Err(StorageRpcPayloadError::ShardWriteChecksumMismatch);
    }
    Ok(WriteAck { stored_size, crc64 })
}

pub(crate) fn encode_shard_read_request(
    request: &StorageRpcShardReadRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_ack.stored_size);
    put_u64(&mut out, request.expected_ack.crc64);
    Ok(out)
}

pub(crate) fn decode_shard_read_request(
    bytes: &[u8],
) -> Result<StorageRpcShardReadRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_ack = WriteAck {
        stored_size: decoder.read_u64()?,
        crc64: decoder.read_u64()?,
    };
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    Ok(StorageRpcShardReadRequest {
        location,
        shard_key,
        expected_ack,
    })
}

pub(crate) fn encode_shard_read_range_request(
    request: &StorageRpcShardReadRangeRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    validate_shard_read_range(
        request.expected_ack.stored_size,
        request.offset,
        request.length,
    )?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_ack.stored_size);
    put_u64(&mut out, request.expected_ack.crc64);
    put_u64(&mut out, request.offset);
    put_u64(&mut out, request.length);
    Ok(out)
}

pub(crate) fn decode_shard_read_range_request(
    bytes: &[u8],
) -> Result<StorageRpcShardReadRangeRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_ack = WriteAck {
        stored_size: decoder.read_u64()?,
        crc64: decoder.read_u64()?,
    };
    let offset = decoder.read_u64()?;
    let length = decoder.read_u64()?;
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    validate_shard_read_range(expected_ack.stored_size, offset, length)?;
    Ok(StorageRpcShardReadRangeRequest {
        location,
        shard_key,
        expected_ack,
        offset,
        length,
    })
}

pub(crate) fn encode_shard_read_response(
    payload: &[u8],
    expected_ack: WriteAck,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_payload_matches_ack(payload, expected_ack)?;
    let mut out = Vec::new();
    put_bytes(&mut out, payload);
    Ok(out)
}

pub(crate) fn decode_shard_read_response(
    bytes: &[u8],
    expected_ack: WriteAck,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_payload_matches_ack(&payload, expected_ack)?;
    Ok(payload)
}

pub(crate) fn encode_shard_read_range_response(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, payload);
    out
}

pub(crate) fn decode_shard_read_range_response(
    bytes: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    if payload.len() != expected_len {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_len as u64,
            actual: payload.len() as u64,
        });
    }
    Ok(payload)
}

pub(crate) fn encode_shard_delete_request(
    request: &StorageRpcShardDeleteRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    Ok(out)
}

pub(crate) fn decode_shard_delete_request(
    bytes: &[u8],
) -> Result<StorageRpcShardDeleteRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    Ok(StorageRpcShardDeleteRequest {
        location,
        shard_key,
    })
}

pub(crate) fn encode_shard_ack_batch_request(
    request: &StorageRpcShardAckBatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_ack_batch(request.items.len())?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_u32(
        &mut out,
        u32::try_from(request.items.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.items.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for item in &request.items {
        put_bytes(&mut out, item.shard_key.as_bytes());
        put_u64(&mut out, item.ack.stored_size);
        put_u64(&mut out, item.ack.crc64);
    }
    Ok(out)
}

pub(crate) fn decode_shard_ack_batch_request(
    bytes: &[u8],
) -> Result<StorageRpcShardAckBatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item_count = decoder.read_u32()? as usize;
    validate_shard_ack_batch(item_count)?;
    if decoder.remaining_len()
        != item_count * (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN)
    {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        let shard_key = decoder.read_shard_key()?;
        let stored_size = decoder.read_u64()?;
        let crc64 = decoder.read_u64()?;
        items.push(StorageRpcShardAckItem {
            shard_key,
            ack: WriteAck { stored_size, crc64 },
        });
    }
    decoder.finish()?;
    Ok(StorageRpcShardAckBatchRequest {
        node_id,
        cluster_epoch,
        pg_id,
        items,
    })
}

pub(crate) fn encode_shard_ack_item_request(request: &StorageRpcShardAckItemRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bytes(&mut out, request.shard_key.as_bytes());
    out
}

pub(crate) fn decode_shard_ack_item_request(
    bytes: &[u8],
) -> Result<StorageRpcShardAckItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let shard_key = decoder.read_shard_key()?;
    decoder.finish()?;
    Ok(StorageRpcShardAckItemRequest {
        node_id,
        cluster_epoch,
        pg_id,
        shard_key,
    })
}

pub(crate) fn encode_shard_ack_item_response(item: &StorageRpcShardAckItem) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, item.shard_key.as_bytes());
    put_u64(&mut out, item.ack.stored_size);
    put_u64(&mut out, item.ack.crc64);
    out
}

pub(crate) fn decode_shard_ack_item_response(
    bytes: &[u8],
) -> Result<StorageRpcShardAckItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let shard_key = decoder.read_shard_key()?;
    let stored_size = decoder.read_u64()?;
    let crc64 = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcShardAckItem {
        shard_key,
        ack: WriteAck { stored_size, crc64 },
    })
}

pub(crate) fn encode_scavenger_list_files_request(
    request: &StorageRpcScavengerListFilesRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.data_pg_id.get());
    out
}

pub(crate) fn decode_scavenger_list_files_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerListFilesRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let data_pg_id = DataPgId::new(PgId::new(decoder.read_u32()?));
    decoder.finish()?;
    Ok(StorageRpcScavengerListFilesRequest {
        node_id,
        cluster_epoch,
        data_pg_id,
    })
}

pub(crate) fn encode_scavenger_list_files_response(scan: &ScavengerShardFileScan) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(scan.files.len()).expect("scavenger file count must fit in u32"),
    );
    for file in &scan.files {
        put_bytes(&mut out, file.key.as_bytes());
        put_u64(&mut out, file.size);
    }
    put_u32(
        &mut out,
        u32::try_from(scan.errors.len()).expect("scavenger scan error count must fit in u32"),
    );
    for error in &scan.errors {
        put_string(&mut out, error);
    }
    out
}

pub(crate) fn decode_scavenger_list_files_response(
    bytes: &[u8],
) -> Result<ScavengerShardFileScan, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let file_count = decoder.read_u32()? as usize;
    let file_bytes = file_count
        .checked_mul(STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN)
        .ok_or(StorageRpcPayloadError::PayloadTooLarge {
            len: file_count,
            limit: usize::MAX / STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN,
        })?;
    if file_bytes > decoder.remaining_len() {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        files.push(ScavengerShardFile {
            key: decoder.read_shard_key()?,
            size: decoder.read_u64()?,
        });
    }
    let error_count = decoder.read_u32()? as usize;
    if error_count > STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: error_count,
            limit: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS,
        });
    }
    let mut errors = Vec::with_capacity(error_count);
    for _ in 0..error_count {
        errors.push(decoder.read_string_with_limit(
            STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN,
            StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN + 1,
                limit: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN,
            },
        )?);
    }
    decoder.finish()?;
    Ok(ScavengerShardFileScan { files, errors })
}

pub(crate) fn encode_scavenger_shard_rows_response(rows: &[ScavengerShardRow]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(rows.len()).expect("scavenger shard row count must fit in u32"),
    );
    for row in rows {
        put_bytes(&mut out, row.key.as_bytes());
        put_u64(&mut out, row.ack.stored_size);
        put_u64(&mut out, row.ack.crc64);
    }
    out
}

pub(crate) fn decode_scavenger_shard_rows_response(
    bytes: &[u8],
) -> Result<Vec<ScavengerShardRow>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    let item_bytes = count
        .checked_mul(STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN)
        .ok_or(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: usize::MAX / (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN),
        })?;
    if item_bytes > decoder.remaining_len() {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        rows.push(ScavengerShardRow {
            key: decoder.read_shard_key()?,
            ack: WriteAck {
                stored_size: decoder.read_u64()?,
                crc64: decoder.read_u64()?,
            },
        });
    }
    decoder.finish()?;
    Ok(rows)
}

pub(crate) fn encode_scavenger_payload_references_response(
    references: &[ShardScavengerPayloadReference],
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(references.len()).expect("scavenger reference count must fit in u32"),
    );
    for reference in references {
        put_scavenger_payload_reference(&mut out, reference);
    }
    out
}

pub(crate) fn decode_scavenger_payload_references_response(
    bytes: &[u8],
) -> Result<Vec<ShardScavengerPayloadReference>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_SCAVENGER_PAYLOAD_REFERENCE_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut references = Vec::with_capacity(count);
    for _ in 0..count {
        references.push(decoder.read_scavenger_payload_reference()?);
    }
    decoder.finish()?;
    Ok(references)
}

pub(crate) fn encode_scavenger_observation_record_request(
    request: &StorageRpcScavengerObservationRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_scavenger_observation_record(&mut out, &request.observation);
    Ok(out)
}

pub(crate) fn decode_scavenger_observation_record_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerObservationRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let observation = decoder.read_scavenger_observation_record()?;
    decoder.finish()?;
    Ok(StorageRpcScavengerObservationRecordRequest { route, observation })
}

pub(crate) fn encode_scavenger_observations_response(
    observations: &[ShardScavengerObservation],
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(observations.len()).expect("scavenger observation count must fit in u32"),
    );
    for observation in observations {
        put_scavenger_observation(&mut out, observation);
    }
    out
}

pub(crate) fn decode_scavenger_observations_response(
    bytes: &[u8],
) -> Result<Vec<ShardScavengerObservation>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_SCAVENGER_OBSERVATION_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut observations = Vec::with_capacity(count);
    for _ in 0..count {
        observations.push(decoder.read_scavenger_observation()?);
    }
    decoder.finish()?;
    Ok(observations)
}

pub(crate) fn encode_scavenger_observation_key_request(
    request: &StorageRpcScavengerObservationKeyRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_scavenger_observation_key(&mut out, &request.key);
    Ok(out)
}

pub(crate) fn decode_scavenger_observation_key_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerObservationKeyRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let key = decoder.read_scavenger_observation_key()?;
    decoder.finish()?;
    Ok(StorageRpcScavengerObservationKeyRequest { route, key })
}

pub(crate) fn encode_placed_segment_shard_repair_record_request(
    request: &StorageRpcPlacedSegmentShardRepairRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_work_item(&mut out, &request.work_item);
    put_optional_string(&mut out, request.last_error.as_deref());
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_repair_work_item()?;
    let last_error = decoder.read_optional_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        },
    )?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairRecordRequest {
        route,
        work_item,
        last_error,
    };
    encode_placed_segment_shard_repair_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_item_request(
    request: &StorageRpcPlacedSegmentShardRepairItemRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_work_item(&mut out, &request.work_item);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_item_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_repair_work_item()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairItemRequest { route, work_item };
    encode_placed_segment_shard_repair_item_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_acquire_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_identity(&request.claim_id, &request.owner_token)?;
    let Some(lease_deadline) = request.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline is required",
        ));
    };
    if lease_deadline <= request.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim_id = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
        route,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    };
    encode_placed_segment_shard_repair_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_optional_record_response(
    response: &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_placed_segment_shard_repair_claim_record(record)?;
            put_u8(&mut out, 1);
            put_placed_segment_shard_repair_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_placed_segment_shard_repair_claim_record()?;
            validate_placed_segment_shard_repair_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "placed segment repair claim optional record tag must be 0 or 1",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_placed_segment_shard_repair_claim_record_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_record(&request.claim)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_repair_claim_record()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest { route, claim };
    encode_placed_segment_shard_repair_claim_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_error_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_record(&request.claim)?;
    if request.last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.last_error.len(),
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_claim_record(&mut out, &request.claim);
    put_string(&mut out, &request.last_error);
    put_u64(&mut out, request.next_attempt_after);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_repair_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        },
    )?;
    let next_attempt_after = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
        route,
        claim,
        last_error,
        next_attempt_after,
    };
    encode_placed_segment_shard_repair_claim_error_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repairs_response(
    repairs: &[PlacedSegmentShardRepairRecord],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if repairs.len() > PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: repairs.len(),
            limit: PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(repairs.len()).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: repairs.len(),
            limit: u32::MAX as usize,
        })?,
    );
    for repair in repairs {
        validate_placed_segment_shard_repair_work_item(&repair.work_item)?;
        if let Some(last_error) = repair.last_error.as_deref() {
            if last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: last_error.len(),
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                });
            }
        }
        put_placed_segment_shard_repair_record(&mut out, repair);
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repairs_response(
    bytes: &[u8],
) -> Result<Vec<PlacedSegmentShardRepairRecord>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_PLACED_SEGMENT_REPAIR_RECORD_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut repairs = Vec::with_capacity(count);
    for _ in 0..count {
        repairs.push(decoder.read_placed_segment_shard_repair_record()?);
    }
    decoder.finish()?;
    Ok(repairs)
}

pub(crate) fn encode_placed_segment_shard_backfill_record_request(
    request: &StorageRpcPlacedSegmentShardBackfillRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&request.work_item)?;
    validate_placed_segment_shard_backfill_remaining_tolerance(
        &request.work_item,
        request.remaining_tolerance,
    )?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_work_item(&mut out, &request.work_item);
    put_u8(&mut out, request.remaining_tolerance);
    put_optional_string(&mut out, request.last_error.as_deref());
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_backfill_work_item()?;
    let remaining_tolerance = decoder.read_u8()?;
    let last_error = decoder.read_optional_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        },
    )?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillRecordRequest {
        route,
        work_item,
        remaining_tolerance,
        last_error,
    };
    encode_placed_segment_shard_backfill_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_item_request(
    request: &StorageRpcPlacedSegmentShardBackfillItemRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_work_item(&mut out, &request.work_item);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_item_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_backfill_work_item()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillItemRequest { route, work_item };
    encode_placed_segment_shard_backfill_item_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_acquire_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_identity(&request.claim_id, &request.owner_token)?;
    let Some(lease_deadline) = request.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline is required",
        ));
    };
    if lease_deadline <= request.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim_id = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
        route,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    };
    encode_placed_segment_shard_backfill_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_optional_record_response(
    response: &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_placed_segment_shard_backfill_claim_record(record)?;
            put_u8(&mut out, 1);
            put_placed_segment_shard_backfill_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse, StorageRpcPayloadError>
{
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_placed_segment_shard_backfill_claim_record()?;
            validate_placed_segment_shard_backfill_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "placed segment backfill claim optional record tag must be 0 or 1",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_record_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_record(&request.claim)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_backfill_claim_record()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimRecordRequest { route, claim };
    encode_placed_segment_shard_backfill_claim_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_error_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_record(&request.claim)?;
    if request.last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.last_error.len(),
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_claim_record(&mut out, &request.claim);
    put_string(&mut out, &request.last_error);
    put_u64(&mut out, request.next_attempt_after);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_backfill_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        },
    )?;
    let next_attempt_after = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
        route,
        claim,
        last_error,
        next_attempt_after,
    };
    encode_placed_segment_shard_backfill_claim_error_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfills_response(
    backfills: &[PlacedSegmentShardBackfillRecord],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if backfills.len() > PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: backfills.len(),
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(backfills.len()).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: backfills.len(),
            limit: u32::MAX as usize,
        })?,
    );
    for backfill in backfills {
        validate_placed_segment_shard_backfill_work_item(&backfill.work_item)?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &backfill.work_item,
            backfill.remaining_tolerance,
        )?;
        if let Some(last_error) = backfill.last_error.as_deref() {
            if last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: last_error.len(),
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                });
            }
        }
        put_placed_segment_shard_backfill_record(&mut out, backfill);
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfills_response(
    bytes: &[u8],
) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_PLACED_SEGMENT_BACKFILL_RECORD_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut backfills = Vec::with_capacity(count);
    for _ in 0..count {
        backfills.push(decoder.read_placed_segment_shard_backfill_record()?);
    }
    decoder.finish()?;
    Ok(backfills)
}

pub(crate) fn encode_placed_segment_shard_backfill_count_response(
    count: usize,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u64(
        &mut out,
        u64::try_from(count).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: u64::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_count_response(
    bytes: &[u8],
) -> Result<usize, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = usize::try_from(decoder.read_u64()?).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: usize::MAX,
            limit: usize::MAX,
        }
    })?;
    decoder.finish()?;
    Ok(count)
}

pub(crate) fn encode_read_handle_acquire_request(
    request: &StorageRpcReadHandleAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_read_operation_id(&request.read_operation_id)?;
    if request.locations.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire must include at least one shard location",
        ));
    }
    if request.locations.len() != request.shard_keys.len() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire locations and shard keys must have the same length",
        ));
    }
    validate_read_handle_location_count(request.locations.len())?;
    validate_read_handle_locations(&request.locations)?;
    for (location, shard_key) in request.locations.iter().zip(request.shard_keys.iter()) {
        validate_shard_location_matches_key(location, shard_key)?;
    }
    let mut out = Vec::new();
    put_string(&mut out, &request.read_operation_id);
    put_u32(
        &mut out,
        u32::try_from(request.locations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.locations.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for location in &request.locations {
        put_shard_location(&mut out, *location);
    }
    for shard_key in &request.shard_keys {
        put_bytes(&mut out, shard_key.as_bytes());
    }
    Ok(out)
}

pub(crate) fn decode_read_handle_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let read_operation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_READ_OPERATION_ID_LEN,
        StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id exceeds maximum length",
        ),
    )?;
    let location_count = decoder.read_u32()? as usize;
    if location_count == 0 {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire must include at least one shard location",
        ));
    }
    validate_read_handle_location_count(location_count)?;
    if location_count
        > decoder.remaining_len()
            / (STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN)
    {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut locations = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        locations.push(decoder.read_shard_location()?);
    }
    let mut shard_keys = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        shard_keys.push(decoder.read_shard_key()?);
    }
    decoder.finish()?;
    validate_read_operation_id(&read_operation_id)?;
    validate_read_handle_locations(&locations)?;
    for (location, shard_key) in locations.iter().zip(shard_keys.iter()) {
        validate_shard_location_matches_key(location, shard_key)?;
    }
    Ok(StorageRpcReadHandleAcquireRequest {
        read_operation_id,
        locations,
        shard_keys,
    })
}

pub(crate) fn encode_read_handle_acquire_response(
    response: &StorageRpcReadHandleAcquireResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.locations.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire response must include at least one shard location",
        ));
    }
    validate_read_handle_location_count(response.locations.len())?;
    validate_read_handle_locations(&response.locations)?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.locations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.locations.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for location in &response.locations {
        put_shard_location(&mut out, *location);
    }
    Ok(out)
}

pub(crate) fn decode_read_handle_acquire_response(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleAcquireResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location_count = decoder.read_u32()? as usize;
    if location_count == 0 {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire response must include at least one shard location",
        ));
    }
    if location_count > decoder.remaining_len() / STORAGE_RPC_SHARD_LOCATION_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut locations = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        locations.push(decoder.read_shard_location()?);
    }
    decoder.finish()?;
    validate_read_handle_locations(&locations)?;
    Ok(StorageRpcReadHandleAcquireResponse { locations })
}

pub(crate) fn encode_read_handle_release_request(
    request: &StorageRpcReadHandleReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_read_handle_release_operation_id(&request.read_operation_id)?;
    let mut out = Vec::new();
    put_string(&mut out, &request.read_operation_id);
    Ok(out)
}

pub(crate) fn decode_read_handle_release_request(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let read_operation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_READ_OPERATION_ID_LEN,
        StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id exceeds maximum length",
        ),
    )?;
    decoder.finish()?;
    validate_read_handle_release_operation_id(&read_operation_id)?;
    Ok(StorageRpcReadHandleReleaseRequest { read_operation_id })
}

pub(crate) fn encode_read_handle_release_response(
    _response: &StorageRpcReadHandleReleaseResponse,
) -> Vec<u8> {
    Vec::new()
}

pub(crate) fn decode_read_handle_release_response(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleReleaseResponse, StorageRpcPayloadError> {
    let decoder = StorageRpcDecoder::new(bytes);
    decoder.finish()?;
    Ok(StorageRpcReadHandleReleaseResponse)
}

pub(crate) fn encode_claim_heartbeat_request(
    request: &StorageRpcClaimHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_claim_token(&request.token)?;
    if request
        .lease_deadline
        .is_some_and(|lease_deadline| lease_deadline <= request.heartbeat_at)
    {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim heartbeat lease deadline must be after heartbeat time",
        ));
    }
    let mut out = Vec::new();
    put_claim_token(&mut out, &request.token);
    put_u64(&mut out, request.heartbeat_at);
    put_optional_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_claim_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcClaimHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let token = decoder.read_claim_token()?;
    let heartbeat_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    decoder.finish()?;
    let request = StorageRpcClaimHeartbeatRequest {
        token,
        heartbeat_at,
        lease_deadline,
    };
    encode_claim_heartbeat_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_claim_release_request(
    request: &StorageRpcClaimReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_claim_token(&request.token)?;
    let mut out = Vec::new();
    put_claim_token(&mut out, &request.token);
    Ok(out)
}

pub(crate) fn decode_claim_release_request(
    bytes: &[u8],
) -> Result<StorageRpcClaimReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let token = decoder.read_claim_token()?;
    decoder.finish()?;
    validate_claim_token(&token)?;
    Ok(StorageRpcClaimReleaseRequest { token })
}

pub(crate) fn encode_proof_release_request(
    request: &StorageRpcProofReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.proof.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match proof epoch",
        ));
    }
    validate_bucket_write_reservation_proof(&request.proof)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_reservation_proof(&mut out, &request.proof);
    Ok(out)
}

pub(crate) fn decode_proof_release_request(
    bytes: &[u8],
) -> Result<StorageRpcProofReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let proof = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if cluster_epoch != proof.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match proof epoch",
        ));
    }
    validate_bucket_write_reservation_proof(&proof)?;
    Ok(StorageRpcProofReleaseRequest {
        node_id,
        cluster_epoch,
        pg_id,
        proof,
    })
}

pub(crate) fn encode_bucket_write_reservation_acquire_request(
    request: &StorageRpcBucketWriteReservationAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &request.reservation_id,
        &request.owner_token,
        &request.operation_kind,
        request.target_context.as_deref(),
    )?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    put_string(&mut out, &request.reservation_id);
    put_string(&mut out, &request.owner_token);
    put_string(&mut out, &request.operation_kind);
    put_u64(&mut out, request.created_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_optional_string(&mut out, request.target_context.as_deref());
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name().map_err(|_| {
        StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
    })?;
    let reservation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let operation_kind = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind exceeds maximum length",
        ),
    )?;
    let created_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let target_context = decoder.read_optional_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "target context exceeds maximum length",
        ),
    )?;
    decoder.finish()?;
    validate_bucket_write_reservation_identity(
        &reservation_id,
        &owner_token,
        &operation_kind,
        target_context.as_deref(),
    )?;
    Ok(StorageRpcBucketWriteReservationAcquireRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        reservation_id,
        owner_token,
        operation_kind,
        created_at,
        lease_deadline,
        target_context,
    })
}

pub(crate) fn encode_bucket_write_reservation_proof_request(
    request: &StorageRpcBucketWriteReservationProofRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    encode_proof_release_request(&StorageRpcProofReleaseRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        proof: request.proof.clone(),
    })
}

pub(crate) fn decode_bucket_write_reservation_proof_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationProofRequest, StorageRpcPayloadError> {
    let request = decode_proof_release_request(bytes)?;
    Ok(StorageRpcBucketWriteReservationProofRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        proof: request.proof,
    })
}

pub(crate) fn encode_bucket_write_reservation_heartbeat_request(
    request: &StorageRpcBucketWriteReservationHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_write_reservation_proof_request(
        &StorageRpcBucketWriteReservationProofRequest {
            node_id: request.node_id,
            cluster_epoch: request.cluster_epoch,
            pg_id: request.pg_id,
            proof: request.proof.clone(),
        },
    )?;
    put_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationHeartbeatRequest, StorageRpcPayloadError> {
    if bytes.len() < 8 {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let proof_len = bytes.len() - 8;
    let proof_request = decode_bucket_write_reservation_proof_request(&bytes[..proof_len])?;
    let mut decoder = StorageRpcDecoder::new(&bytes[proof_len..]);
    let lease_deadline = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationHeartbeatRequest {
        node_id: proof_request.node_id,
        cluster_epoch: proof_request.cluster_epoch,
        pg_id: proof_request.pg_id,
        proof: proof_request.proof,
        lease_deadline,
    })
}

pub(crate) fn encode_bucket_write_reservation_record_request(
    request: &StorageRpcBucketWriteReservationRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match reservation epoch",
        ));
    }
    validate_bucket_write_reservation_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_reservation_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_reservation_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match reservation epoch",
        ));
    }
    validate_bucket_write_reservation_record(&record)?;
    Ok(StorageRpcBucketWriteReservationRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_write_reservation_record_response(
    response: &StorageRpcBucketWriteReservationRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => {
            validate_bucket_write_reservation_record(record)?;
            put_u8(&mut out, 0);
            put_bucket_write_reservation_record(&mut out, record);
        }
        StorageRpcBucketWriteReservationAcquireOutcome::Draining => {
            put_u8(&mut out, 1);
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 2);
            put_string(&mut out, name.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let record = decoder.read_bucket_write_reservation_record()?;
            validate_bucket_write_reservation_record(&record)?;
            StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record)
        }
        1 => StorageRpcBucketWriteReservationAcquireOutcome::Draining,
        2 => StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown bucket write reservation acquire outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationRecordResponse { outcome })
}

pub(crate) fn encode_bucket_write_drain_begin_request(
    request: &StorageRpcBucketWriteDrainBeginRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&request.drain_id, &request.owner_token)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_string(&mut out, &request.drain_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.created_at);
    put_optional_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_begin_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainBeginRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let drain_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let created_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    decoder.finish()?;
    validate_bucket_write_drain_identity(&drain_id, &owner_token)?;
    Ok(StorageRpcBucketWriteDrainBeginRequest {
        bucket,
        drain_id,
        owner_token,
        created_at,
        lease_deadline,
    })
}

pub(crate) fn encode_bucket_write_drain_begin_response(
    response: &StorageRpcBucketWriteDrainBeginResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketWriteDrainBeginOutcome::Acquired(record) => {
            validate_bucket_write_drain_record(record)?;
            put_u8(&mut out, 0);
            put_bucket_write_drain_record(&mut out, record);
        }
        StorageRpcBucketWriteDrainBeginOutcome::Conflict => put_u8(&mut out, 1),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_begin_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainBeginResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let record = decoder.read_bucket_write_drain_record()?;
            validate_bucket_write_drain_record(&record)?;
            StorageRpcBucketWriteDrainBeginOutcome::Acquired(record)
        }
        1 => StorageRpcBucketWriteDrainBeginOutcome::Conflict,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown bucket write drain begin outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainBeginResponse { outcome })
}

pub(crate) fn encode_bucket_write_drain_record_request(
    request: &StorageRpcBucketWriteDrainRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match drain epoch",
        ));
    }
    validate_bucket_write_drain_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_drain_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_drain_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match drain epoch",
        ));
    }
    validate_bucket_write_drain_record(&record)?;
    Ok(StorageRpcBucketWriteDrainRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_write_drain_heartbeat_request(
    request: &StorageRpcBucketWriteDrainHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match drain epoch",
        ));
    }
    validate_bucket_write_drain_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_drain_record(&mut out, &request.record);
    put_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_drain_record()?;
    let lease_deadline = decoder.read_u64()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match drain epoch",
        ));
    }
    validate_bucket_write_drain_record(&record)?;
    Ok(StorageRpcBucketWriteDrainHeartbeatRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
        lease_deadline,
    })
}

pub(crate) fn encode_bucket_write_drain_clear_expired_request(
    request: &StorageRpcBucketWriteDrainClearExpiredRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_clear_expired_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainClearExpiredRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainClearExpiredRequest { bucket, now })
}

pub(crate) fn encode_bucket_write_drain_optional_record_response(
    response: &StorageRpcBucketWriteDrainOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_write_drain_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_write_drain_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_write_drain_record()?;
            validate_bucket_write_drain_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket write drain record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_delete_attempt_outcome_record_request(
    request: &StorageRpcBucketDeleteAttemptOutcomeRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_delete_attempt_outcome_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_delete_attempt_outcome_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_attempt_outcome_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteAttemptOutcomeRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_delete_attempt_outcome_record()?;
    decoder.finish()?;
    validate_bucket_delete_attempt_outcome_record(&record)?;
    Ok(StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_delete_attempt_outcome_optional_record_response(
    response: &StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_delete_attempt_outcome_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_delete_attempt_outcome_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_attempt_outcome_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_delete_attempt_outcome_record()?;
            validate_bucket_delete_attempt_outcome_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket delete attempt outcome record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_write_reservations_list_response(
    response: &StorageRpcBucketWriteReservationsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.records.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.records.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for record in &response.records {
        validate_bucket_write_reservation_record(record)?;
        put_bucket_write_reservation_record(&mut out, record);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservations_list_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN.min(1),
        "bucket write reservation count exceeds payload",
    )?;
    let mut records = Vec::new();
    for _ in 0..count {
        let record = decoder.read_bucket_write_reservation_record()?;
        validate_bucket_write_reservation_record(&record)?;
        records.push(record);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationsListResponse { records })
}

pub(crate) fn encode_bucket_delete_finalized_response(
    response: &StorageRpcBucketDeleteFinalizedResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketDeleteFinalizedOutcome::Deleted => put_u8(&mut out, 0),
        StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalized_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizedResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketDeleteFinalizedOutcome::Deleted,
        1 => StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown bucket delete finalized outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteFinalizedResponse { outcome })
}

pub(crate) fn encode_bucket_pg_request(
    request: &StorageRpcBucketPgRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    Ok(out)
}

pub(crate) fn decode_bucket_pg_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketPgRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let request = decoder.read_bucket_pg_request()?;
    decoder.finish()?;
    Ok(request)
}

pub(crate) fn encode_bucket_delete_finalize_roots_request(
    request: &StorageRpcBucketDeleteFinalizeRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_u64(&mut out, request.now);
    put_u32(
        &mut out,
        u32::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u32::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let limit = decoder.read_u32()? as usize;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    Ok(StorageRpcBucketDeleteFinalizeRootsRequest { route, now, limit })
}

pub(crate) fn encode_bucket_delete_finalize_roots_response(
    response: &StorageRpcBucketDeleteFinalizeRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.roots.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.roots.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for root in &response.roots {
        put_bucket_delete_finalize_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_DELETE_FINALIZE_ROOT_MAX_LEN.min(1),
        "bucket delete finalize root count exceeds payload",
    )?;
    if count > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_bucket_delete_finalize_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteFinalizeRootsResponse { roots })
}

pub(crate) fn encode_bucket_delete_begin_roots_request(
    request: &StorageRpcBucketDeleteBeginRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_u64(&mut out, request.now);
    put_optional_string(
        &mut out,
        request.start_after_bucket.as_ref().map(BucketName::as_str),
    );
    put_u32(
        &mut out,
        u32::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u32::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_bucket_delete_begin_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteBeginRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let start_after_bucket = decoder
        .read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_NAME_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("bucket name exceeds maximum length"),
        )?
        .map(BucketName::try_from)
        .transpose()
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid bucket name"))?;
    let limit = decoder.read_u32()? as usize;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    Ok(StorageRpcBucketDeleteBeginRootsRequest {
        route,
        now,
        start_after_bucket,
        limit,
    })
}

pub(crate) fn encode_bucket_delete_begin_roots_response(
    response: &StorageRpcBucketDeleteBeginRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.roots.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.roots.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for root in &response.roots {
        put_bucket_delete_begin_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_begin_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteBeginRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_DELETE_BEGIN_ROOT_MAX_LEN.min(1),
        "bucket delete begin root count exceeds payload",
    )?;
    if count > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_bucket_delete_begin_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteBeginRootsResponse { roots })
}

pub(crate) fn encode_bucket_delete_finalize_claim_acquire_request(
    request: &StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&request.claim_id, &request.owner_token)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "claim id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    validate_bucket_write_drain_identity(&claim_id, &owner_token)?;
    Ok(StorageRpcBucketDeleteFinalizeClaimAcquireRequest {
        bucket,
        bucket_incarnation_generation,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    })
}

pub(crate) fn encode_bucket_delete_finalize_claim_optional_record_response(
    response: &StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_delete_finalize_claim_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_delete_finalize_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_delete_finalize_claim_record()?;
            validate_bucket_delete_finalize_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket delete finalize claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_delete_finalize_claim_record_request(
    request: &StorageRpcBucketDeleteFinalizeClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match claim epoch",
        ));
    }
    validate_bucket_delete_finalize_claim_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_delete_finalize_claim_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_delete_finalize_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match claim epoch",
        ));
    }
    validate_bucket_delete_finalize_claim_record(&record)?;
    Ok(StorageRpcBucketDeleteFinalizeClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_optional_checksum_metadata(checksum: Option<&ChecksumBytes>) -> Vec<u8> {
    let mut out = Vec::new();
    match checksum {
        None => put_u8(&mut out, 0),
        Some(checksum) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, checksum.as_slice());
        }
    }
    out
}

pub(crate) fn decode_optional_checksum_metadata(
    bytes: &[u8],
) -> Result<Option<ChecksumBytes>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let tag = decoder.read_u8()?;
    let checksum = match tag {
        0 => None,
        1 => Some(
            ChecksumBytes::new(decoder.read_bytes()?)
                .map_err(StorageRpcPayloadError::InvalidChecksumMetadata)?,
        ),
        _ => {
            return Err(StorageRpcPayloadError::InvalidChecksumMetadata(
                "invalid optional checksum tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(checksum)
}

fn validate_shard_write_payload(
    expected_size: u64,
    expected_crc64: u64,
    payload: &[u8],
) -> Result<(), StorageRpcPayloadError> {
    validate_shard_payload_matches_ack(
        payload,
        WriteAck {
            stored_size: expected_size,
            crc64: expected_crc64,
        },
    )
}

fn validate_shard_payload_matches_ack(
    payload: &[u8],
    expected_ack: WriteAck,
) -> Result<(), StorageRpcPayloadError> {
    let actual_size = payload.len() as u64;
    if actual_size != expected_ack.stored_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_ack.stored_size,
            actual: actual_size,
        });
    }
    if checksum::crc64::checksum(payload) != expected_ack.crc64 {
        return Err(StorageRpcPayloadError::ShardWriteChecksumMismatch);
    }
    Ok(())
}

fn validate_shard_read_range(
    stored_size: u64,
    offset: u64,
    length: u64,
) -> Result<(), StorageRpcPayloadError> {
    let end = offset
        .checked_add(length)
        .ok_or(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: stored_size,
            actual: u64::MAX,
        })?;
    if end > stored_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: stored_size,
            actual: end,
        });
    }
    Ok(())
}

fn validate_shard_location_matches_key(
    location: &ShardLocation,
    shard_key: &ShardKey,
) -> Result<(), StorageRpcPayloadError> {
    if location.shard_index() != shard_key.shard_index() {
        return Err(StorageRpcPayloadError::ShardLocationMismatch);
    }
    Ok(())
}

fn validate_read_operation_id(id: &str) -> Result<(), StorageRpcPayloadError> {
    if id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id must not be empty",
        ));
    }
    if id.len() > STORAGE_RPC_MAX_READ_OPERATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_read_handle_release_operation_id(id: &str) -> Result<(), StorageRpcPayloadError> {
    if id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id must not be empty",
        ));
    }
    if id.len() > STORAGE_RPC_MAX_READ_OPERATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_read_handle_location_count(
    location_count: usize,
) -> Result<(), StorageRpcPayloadError> {
    if location_count > STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire includes too many shard locations",
        ));
    }
    Ok(())
}

fn validate_read_handle_locations(
    locations: &[ShardLocation],
) -> Result<(), StorageRpcPayloadError> {
    for pair in locations.windows(2) {
        if shard_location_sort_key(pair[0]) >= shard_location_sort_key(pair[1]) {
            return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ));
        }
    }
    Ok(())
}

fn shard_location_sort_key(location: ShardLocation) -> (u64, u32, u8, u32) {
    (
        location.cluster_epoch().get(),
        location.data_pg_id().get(),
        location.shard_index().get(),
        location.node_id().as_u32(),
    )
}

fn validate_shard_ack_batch(item_count: usize) -> Result<(), StorageRpcPayloadError> {
    if item_count == 0 {
        return Err(StorageRpcPayloadError::InvalidShardAckBatchRequest(
            "shard ack batch must include at least one item",
        ));
    }
    if item_count > STORAGE_RPC_MAX_SHARD_ACK_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: item_count,
            limit: STORAGE_RPC_MAX_SHARD_ACK_ITEMS,
        });
    }
    Ok(())
}

fn validate_claim_token(token: &StorageRpcDurableClaimToken) -> Result<(), StorageRpcPayloadError> {
    let (claim_id, owner_token) = match token {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(token) => {
            (token.claim_id.as_str(), token.owner_token.as_str())
        }
        StorageRpcDurableClaimToken::BucketDeleteFinalize(token)
        | StorageRpcDurableClaimToken::LifecycleSweep(token) => {
            (token.claim_id.as_str(), token.owner_token.as_str())
        }
    };
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id exceeds maximum length",
        ));
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_work_item(
    work_item: &PlacedSegmentShardRepairWorkItem,
) -> Result<(), StorageRpcPayloadError> {
    let total = work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment repair EC shard count overflow",
        ))?;
    if work_item.request.ec.k == 0 || work_item.shard_index.get() >= total {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "invalid placed segment shard repair work item",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_record(
    record: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&record.work_item)?;
    validate_placed_segment_shard_repair_claim_identity(&record.claim_id, &record.owner_token)?;
    if record.attempt_count == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim attempt count must be nonzero",
        ));
    }
    let Some(lease_deadline) = record.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim lease deadline is required",
        ));
    };
    if lease_deadline <= record.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim lease deadline must be after claimed time",
        ));
    }
    if let Some(last_error) = record.last_error.as_deref() {
        if last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: last_error.len(),
                limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
            });
        }
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id exceeds maximum length",
        ));
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_work_item(
    work_item: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StorageRpcPayloadError> {
    work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment backfill EC shard count overflow",
        ))?;
    if work_item.request.ec.k == 0 {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "invalid placed segment shard backfill work item",
        ));
    }
    if work_item.source_cluster_epoch.get() > work_item.desired_cluster_epoch.get() {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment shard backfill source epoch must not exceed desired epoch",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_remaining_tolerance(
    work_item: &PlacedSegmentShardBackfillWorkItem,
    remaining_tolerance: u8,
) -> Result<(), StorageRpcPayloadError> {
    if remaining_tolerance > work_item.request.ec.m {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment shard backfill remaining tolerance exceeds EC m",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_record(
    record: &PlacedSegmentShardBackfillClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&record.work_item)?;
    validate_placed_segment_shard_backfill_remaining_tolerance(
        &record.work_item,
        record.remaining_tolerance,
    )?;
    validate_placed_segment_shard_backfill_claim_identity(&record.claim_id, &record.owner_token)?;
    if record.attempt_count == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim attempt count must be nonzero",
        ));
    }
    let Some(lease_deadline) = record.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim lease deadline is required",
        ));
    };
    if lease_deadline <= record.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim lease deadline must be after claimed time",
        ));
    }
    if let Some(last_error) = record.last_error.as_deref() {
        if last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: last_error.len(),
                limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
            });
        }
    }
    Ok(())
}

fn validate_lifecycle_sweep_claim_record(
    record: &LifecycleSweepClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &record.claim_id,
        &record.owner_token,
        "lifecycle-sweep",
        None,
    )?;
    if record.cluster_epoch.get() == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim epoch must not be zero",
        ));
    }
    Ok(())
}

fn validate_bucket_write_reservation_proof(
    proof: &BucketWriteReservationProof,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &proof.reservation_id,
        &proof.owner_token,
        &proof.operation_kind,
        proof.target_context.as_deref(),
    )
}

fn validate_bucket_write_reservation_record(
    record: &BucketWriteReservationRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &record.reservation_id,
        &record.owner_token,
        &record.operation_kind,
        record.target_context.as_deref(),
    )
}

fn validate_bucket_write_drain_record(
    record: &BucketWriteDrainRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.drain_id, &record.owner_token)
}

fn validate_bucket_delete_attempt_outcome_record(
    record: &BucketDeleteAttemptOutcomeRecord,
) -> Result<(), StorageRpcPayloadError> {
    if record.drain_id.is_empty()
        || record.drain_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ));
    }
    if record.detail.len() > BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "bucket delete attempt outcome detail exceeds maximum length",
        ));
    }
    if record.cluster_epoch.get() == 0 {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "outcome epoch must not be zero",
        ));
    }
    Ok(())
}

fn validate_bucket_delete_finalize_claim_record(
    record: &BucketDeleteFinalizeClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.claim_id, &record.owner_token)?;
    if record.last_error.as_ref().is_some_and(|last_error| {
        last_error.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN
    }) {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "last error exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_object_payload_reclaim_claim_record(
    record: &ObjectPayloadReclaimClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.claim_id, &record.owner_token)?;
    if record.last_error.as_ref().is_some_and(|last_error| {
        last_error.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN
    }) {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "last error exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_bucket_write_drain_identity(
    drain_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if drain_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id must not be empty",
        ));
    }
    if drain_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token must not be empty",
        ));
    }
    if owner_token.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_bucket_write_reservation_identity(
    reservation_id: &str,
    owner_token: &str,
    operation_kind: &str,
    target_context: Option<&str>,
) -> Result<(), StorageRpcPayloadError> {
    if reservation_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id must not be empty",
        ));
    }
    if reservation_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id exceeds maximum length",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token must not be empty",
        ));
    }
    if owner_token.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ));
    }
    if operation_kind.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind must not be empty",
        ));
    }
    if operation_kind.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind exceeds maximum length",
        ));
    }
    if target_context
        .is_some_and(|context| context.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN)
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "target context exceeds maximum length",
        ));
    }
    Ok(())
}

fn storage_rpc_frame_checksum(
    version: u16,
    request_id: u64,
    raw_kind: u16,
    payload_len: u32,
    payload: &[u8],
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    hasher.update(STORAGE_RPC_FRAME_MAGIC);
    hasher.update(&version.to_le_bytes());
    hasher.update(&request_id.to_le_bytes());
    hasher.update(&raw_kind.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

struct StorageRpcDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> StorageRpcDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn finish(&self) -> Result<(), StorageRpcPayloadError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageRpcPayloadError::TrailingBytes)
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], StorageRpcPayloadError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or(StorageRpcPayloadError::Truncated)?;
        if end > self.bytes.len() {
            return Err(StorageRpcPayloadError::Truncated);
        }
        let slice = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(slice)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_bytes_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(too_large_error);
        }
        self.read_exact(len)
    }

    fn read_bytes_with_payload_limit(
        &mut self,
        limit: usize,
    ) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(StorageRpcPayloadError::PayloadTooLarge { len, limit });
        }
        self.read_exact(len)
    }

    fn read_metadata_command_item(
        &mut self,
    ) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
        let command_checksum = self.read_u64()?;
        let command_bytes = self
            .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN)?
            .to_vec();
        validate_metadata_command_item(command_checksum, command_bytes)
    }

    fn read_metadata_command_envelope_bytes(
        &mut self,
    ) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
        let command_bytes = self
            .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN)?
            .to_vec();
        decode_metadata_command_envelope(&command_bytes)
            .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
    }

    fn read_string(&mut self) -> Result<String, StorageRpcPayloadError> {
        std::str::from_utf8(self.read_bytes()?)
            .map(str::to_owned)
            .map_err(|_| StorageRpcPayloadError::InvalidUtf8)
    }

    fn read_string_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<String, StorageRpcPayloadError> {
        std::str::from_utf8(self.read_bytes_with_limit(limit, too_large_error)?)
            .map(str::to_owned)
            .map_err(|_| StorageRpcPayloadError::InvalidUtf8)
    }

    fn read_count_with_limit(&mut self, limit: usize) -> Result<usize, StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(StorageRpcPayloadError::PayloadTooLarge { len, limit });
        }
        Ok(len)
    }

    fn read_string_vec_with_limit(
        &mut self,
        limit: usize,
    ) -> Result<Vec<String>, StorageRpcPayloadError> {
        let count = self.read_count_with_limit(limit)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.read_string()?);
        }
        Ok(values)
    }

    fn read_bucket_name(&mut self) -> Result<BucketName, StorageRpcPayloadError> {
        BucketName::try_from(self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_NAME_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("bucket name exceeds maximum length"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid bucket name"))
    }

    fn read_object_key(&mut self) -> Result<ObjectKey, StorageRpcPayloadError> {
        ObjectKey::try_from(self.read_string_with_limit(
            STORAGE_RPC_MAX_OBJECT_KEY_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("object key exceeds maximum length"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid object key"))
    }

    fn read_optional_object_key(&mut self) -> Result<Option<ObjectKey>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_object_key()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object key tag",
            )),
        }
    }

    fn read_rpc_object_request(
        &mut self,
    ) -> Result<StorageRpcObjectRequest, StorageRpcPayloadError> {
        let node_id = NodeId::new(self.read_u32()?);
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = PgId::new(self.read_u32()?);
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        Ok(StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        })
    }

    fn read_session_id(&mut self) -> Result<SessionId, StorageRpcPayloadError> {
        SessionId::try_from(self.read_string_with_limit(
            SESSION_ID_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("session id is too large"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid session id"))
    }

    fn read_optional_session_id(&mut self) -> Result<Option<SessionId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_session_id()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional session id tag",
            )),
        }
    }

    fn read_generation_id(&mut self) -> Result<GenerationId, StorageRpcPayloadError> {
        GenerationId::new(self.read_u64()?).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
            "generation id must not be zero",
        ))
    }

    fn read_optional_payload_reclaim_root(
        &mut self,
    ) -> Result<Option<PayloadReclaimRoot>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(PayloadReclaimRoot {
                bucket: self.read_bucket_name()?,
                key: self.read_object_key()?,
                generation_id: self.read_generation_id()?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional payload reclaim root tag",
            )),
        }
    }

    fn read_object_payload_reclaim_kind(
        &mut self,
    ) -> Result<ObjectPayloadReclaimKind, StorageRpcPayloadError> {
        ObjectPayloadReclaimKind::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidDurableClaimToken("invalid object reclaim kind"),
        )
    }

    fn read_shard_key(&mut self) -> Result<ShardKey, StorageRpcPayloadError> {
        let bytes = self.read_bytes()?;
        ShardKey::from_bytes(bytes).map_err(|_| StorageRpcPayloadError::Truncated)
    }

    fn read_shard_location(&mut self) -> Result<ShardLocation, StorageRpcPayloadError> {
        let cluster_epoch = ClusterEpoch::new(self.read_u64()?).ok_or(
            StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "cluster epoch must not be zero",
            ),
        )?;
        let data_pg_id = DataPgId::new(PgId::new(self.read_u32()?));
        let shard_index = ShardIndex::new(self.read_u8()?);
        let node_id = NodeId::new(self.read_u32()?);
        Ok(ShardLocation::new(
            cluster_epoch,
            data_pg_id,
            shard_index,
            node_id,
        ))
    }

    fn read_bucket_claim_token(
        &mut self,
    ) -> Result<StorageRpcBucketClaimToken, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        Ok(StorageRpcBucketClaimToken {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
        })
    }

    fn read_object_payload_reclaim_claim_token(
        &mut self,
    ) -> Result<StorageRpcObjectPayloadReclaimClaimToken, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let reclaim_kind = self.read_object_payload_reclaim_kind()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        Ok(StorageRpcObjectPayloadReclaimClaimToken {
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
        })
    }

    fn read_claim_token(&mut self) -> Result<StorageRpcDurableClaimToken, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcDurableClaimToken::ObjectPayloadReclaim(
                self.read_object_payload_reclaim_claim_token()?,
            )),
            1 => Ok(StorageRpcDurableClaimToken::BucketDeleteFinalize(
                self.read_bucket_claim_token()?,
            )),
            2 => Ok(StorageRpcDurableClaimToken::LifecycleSweep(
                self.read_bucket_claim_token()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "invalid claim token kind",
            )),
        }
    }

    fn read_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<BucketWriteReservationProof, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let reservation_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "reservation id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let operation_kind = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "operation kind exceeds maximum length",
            ),
        )?;
        let created_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let target_context = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "target context exceeds maximum length",
            ),
        )?;
        Ok(BucketWriteReservationProof {
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            bucket_execution_generation,
            bucket_incarnation_generation,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        })
    }

    fn read_bucket_write_reservation_record(
        &mut self,
    ) -> Result<BucketWriteReservationRecord, StorageRpcPayloadError> {
        let proof = self.read_bucket_write_reservation_proof()?;
        Ok(BucketWriteReservationRecord {
            bucket: proof.bucket,
            reservation_id: proof.reservation_id,
            owner_token: proof.owner_token,
            cluster_epoch: proof.cluster_epoch,
            bucket_execution_generation: proof.bucket_execution_generation,
            bucket_incarnation_generation: proof.bucket_incarnation_generation,
            operation_kind: proof.operation_kind,
            created_at: proof.created_at,
            lease_deadline: proof.lease_deadline,
            target_context: proof.target_context,
        })
    }

    fn read_bucket_write_drain_record(
        &mut self,
    ) -> Result<BucketWriteDrainRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let drain_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "drain id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let state = match self.read_u8()? {
            0 => BucketWriteDrainState::Draining,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket write drain state",
                ));
            }
        };
        let created_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        Ok(BucketWriteDrainRecord {
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            bucket_execution_generation,
            state,
            created_at,
            lease_deadline,
        })
    }

    fn read_bucket_delete_attempt_outcome_record(
        &mut self,
    ) -> Result<BucketDeleteAttemptOutcomeRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let drain_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "drain id exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let outcome = match self.read_u8()? {
            0 => BucketDeleteAttemptOutcomeKind::Retryable,
            1 => BucketDeleteAttemptOutcomeKind::NotEmpty,
            2 => BucketDeleteAttemptOutcomeKind::StaleGeneration,
            3 => BucketDeleteAttemptOutcomeKind::MarkDeleting,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket delete attempt outcome",
                ));
            }
        };
        let phase = match self.read_u8()? {
            0 => BucketDeleteAttemptPhase::Initial,
            1 => BucketDeleteAttemptPhase::ReservationWait,
            2 => BucketDeleteAttemptPhase::PostReservationObjectDrain,
            3 => BucketDeleteAttemptPhase::StreamCleanup,
            4 => BucketDeleteAttemptPhase::FinalVisibilityCheck,
            5 => BucketDeleteAttemptPhase::MarkDeleting,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket delete attempt phase",
                ));
            }
        };
        let detail = self.read_string_with_limit(
            BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "bucket delete attempt outcome detail exceeds maximum length",
            ),
        )?;
        let post_reservation_next_object_pg_id = self.read_optional_u32()?;
        let updated_at = self.read_u64()?;
        Ok(BucketDeleteAttemptOutcomeRecord {
            bucket,
            drain_id,
            cluster_epoch,
            bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id,
            updated_at,
        })
    }

    fn read_bucket_delete_finalize_root(
        &mut self,
    ) -> Result<BucketDeleteFinalizeRoot, StorageRpcPayloadError> {
        Ok(BucketDeleteFinalizeRoot {
            bucket: self.read_bucket_name()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_delete_begin_root(
        &mut self,
    ) -> Result<BucketDeleteBeginRoot, StorageRpcPayloadError> {
        Ok(BucketDeleteBeginRoot {
            bucket: self.read_bucket_name()?,
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_delete_finalize_claim_record(
        &mut self,
    ) -> Result<BucketDeleteFinalizeClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "claim id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "last error exceeds maximum length",
            ),
        )?;
        Ok(BucketDeleteFinalizeClaimRecord {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_object_payload_reclaim_claim_record(
        &mut self,
    ) -> Result<ObjectPayloadReclaimClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let reclaim_kind = self.read_object_payload_reclaim_kind()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("last error exceeds maximum length"),
        )?;
        Ok(ObjectPayloadReclaimClaimRecord {
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_lifecycle_sweep_root(&mut self) -> Result<LifecycleSweepRoot, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let source = match self.read_u8()? {
            0 => LifecycleSweepRootSource::ExpiredClaim,
            1 => LifecycleSweepRootSource::BusyClaim,
            2 => LifecycleSweepRootSource::LifecycleConfig,
            3 => LifecycleSweepRootSource::AbortingMultipartUpload,
            _ => {
                return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                    "invalid lifecycle sweep root source",
                ))
            }
        };
        Ok(LifecycleSweepRoot {
            bucket,
            bucket_incarnation_generation,
            source,
        })
    }

    fn read_lifecycle_sweep_claim_record(
        &mut self,
    ) -> Result<LifecycleSweepClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let heartbeat_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            4096,
            StorageRpcPayloadError::InvalidDurableClaimToken(
                "lifecycle claim error exceeds maximum length",
            ),
        )?;
        Ok(LifecycleSweepClaimRecord {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            heartbeat_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_bucket_request(&mut self) -> Result<StorageRpcBucketRequest, StorageRpcPayloadError> {
        Ok(StorageRpcBucketRequest {
            node_id: NodeId::new(self.read_u32()?),
            cluster_epoch: self.read_cluster_epoch()?,
            pg_id: PgId::new(self.read_u32()?),
            bucket: self.read_bucket_name()?,
        })
    }

    fn read_bucket_pg_request(
        &mut self,
    ) -> Result<StorageRpcBucketPgRequest, StorageRpcPayloadError> {
        Ok(StorageRpcBucketPgRequest {
            node_id: NodeId::new(self.read_u32()?),
            cluster_epoch: self.read_cluster_epoch()?,
            pg_id: PgId::new(self.read_u32()?),
        })
    }

    fn read_cluster_epoch(&mut self) -> Result<ClusterEpoch, StorageRpcPayloadError> {
        ClusterEpoch::new(self.read_u64()?).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
            "cluster epoch must not be zero",
        ))
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional u64 tag",
            )),
        }
    }

    fn read_optional_u32(&mut self) -> Result<Option<u32>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u32()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional u32 tag",
            )),
        }
    }

    fn read_optional_version_id(&mut self) -> Result<Option<VersionId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(VersionId::from_u64(self.read_u64()?))),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional version id tag",
            )),
        }
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional string tag",
            )),
        }
    }

    fn read_optional_string_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<Option<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string_with_limit(limit, too_large_error)?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional string tag",
            )),
        }
    }

    fn read_create_bucket_config(
        &mut self,
    ) -> Result<StorageRpcCreateBucketConfig, StorageRpcPayloadError> {
        Ok(StorageRpcCreateBucketConfig {
            name: self.read_bucket_name()?,
            owner_principal: self.read_string_with_limit(
                STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "owner principal is too large",
                ),
            )?,
            owner_canonical_id: self.read_canonical_user_id()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            public_write: self.read_bool()?,
            versioning: self.read_bucket_versioning_state()?,
            object_lock: self.read_bucket_object_lock_config()?,
            ownership_controls: self.read_bucket_ownership_controls()?,
        })
    }

    fn read_bucket_info(&mut self) -> Result<BucketInfo, StorageRpcPayloadError> {
        Ok(BucketInfo {
            name: self.read_bucket_name()?,
            owner_principal: self.read_string_with_limit(
                STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
                StorageRpcPayloadError::InvalidResponseEnvelope("owner principal is too large"),
            )?,
            owner_canonical_id: self.read_canonical_user_id()?,
            created_at: self.read_u64()?,
            region: self.read_u16()?,
            state: self.read_bucket_state()?,
            versioning: self.read_bucket_versioning_state()?,
            object_lock: self.read_bucket_object_lock_config()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            public_write: self.read_bool()?,
            public_access_block: self.read_optional_public_access_block_config()?,
            ownership_controls: self.read_optional_bucket_ownership_controls()?,
            bucket_policy_present: self.read_bool()?,
            bucket_policy_public: self.read_bool()?,
            bucket_policy_generation: self.read_u64()?,
            bucket_lifecycle_present: self.read_bool()?,
            bucket_lifecycle_generation: self.read_u64()?,
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
            bucket_abac_enabled: self.read_bool()?,
            encryption: self.read_effective_bucket_encryption_config()?,
        })
    }

    fn read_bucket_fast_path_identity(
        &mut self,
    ) -> Result<BucketFastPathIdentity, StorageRpcPayloadError> {
        Ok(BucketFastPathIdentity {
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_snapshot_request(
        &mut self,
    ) -> Result<BucketSnapshotRequest, StorageRpcPayloadError> {
        Ok(BucketSnapshotRequest {
            policy: self.read_bool()?,
            tags: match self.read_u8()? {
                0 => BucketSnapshotTagsRequest::NotRequested,
                1 => BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                2 => BucketSnapshotTagsRequest::Always,
                _ => {
                    return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "invalid bucket snapshot tags request",
                    ));
                }
            },
            lifecycle: self.read_bool()?,
            cors: self.read_bool()?,
        })
    }

    fn read_rpc_bucket_snapshot_request(
        &mut self,
    ) -> Result<StorageRpcBucketSnapshotRequest, StorageRpcPayloadError> {
        let node_id = NodeId::new(self.read_u32()?);
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = PgId::new(self.read_u32()?);
        let bucket = self.read_bucket_name()?;
        let request = self.read_bucket_snapshot_request()?;
        Ok(StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id,
                cluster_epoch,
                pg_id,
                bucket,
            },
            request,
        })
    }

    fn read_bucket_snapshot(&mut self) -> Result<BucketSnapshot, StorageRpcPayloadError> {
        Ok(BucketSnapshot {
            bucket: self.read_bucket_info()?,
            request: self.read_bucket_snapshot_request()?,
            policy: self.read_loaded_bucket_subresource()?,
            tags: self.read_loaded_bucket_subresource()?,
            lifecycle: self.read_loaded_bucket_subresource()?,
            cors: self.read_loaded_bucket_subresource()?,
        })
    }

    fn read_bucket_snapshot_pair(&mut self) -> Result<BucketSnapshotPair, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(BucketSnapshotPair::Same {
                bucket: Box::new(self.read_bucket_snapshot()?),
            }),
            1 => Ok(BucketSnapshotPair::Distinct {
                source: Box::new(self.read_bucket_snapshot()?),
                destination: Box::new(self.read_bucket_snapshot()?),
            }),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket snapshot pair tag",
            )),
        }
    }

    fn read_loaded_bucket_subresource(
        &mut self,
    ) -> Result<LoadedBucketSubresource<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(LoadedBucketSubresource::NotRequested),
            1 => Ok(LoadedBucketSubresource::Missing),
            2 => Ok(LoadedBucketSubresource::Loaded(self.read_string()?)),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid loaded bucket subresource tag",
            )),
        }
    }

    fn read_direct_put_commit_storage_snapshot(
        &mut self,
    ) -> Result<DirectPutCommitStorageSnapshot, StorageRpcPayloadError> {
        let existing_etag = self.read_optional_string()?;
        let current = self.read_optional_stored_object()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let stale_payload = self.read_optional_object_payload_reclaim()?;
        Ok(DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot { existing_etag },
            current,
            stale_payload_source,
            stale_payload,
        })
    }

    fn read_optional_object_payload_reclaim(
        &mut self,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_object_payload_reclaim()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object payload reclaim tag",
            )),
        }
    }

    fn read_object_payload_reclaim(
        &mut self,
    ) -> Result<ObjectPayloadReclaimCommand, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectPayloadReclaimCommand::Segments(
                self.read_object_segments_reclaim_record()?,
            )),
            1 => Ok(ObjectPayloadReclaimCommand::Multipart(
                self.read_multipart_reclaim_record()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object payload reclaim tag",
            )),
        }
    }

    fn read_object_segments_reclaim_record(
        &mut self,
    ) -> Result<ObjectSegmentsReclaimRecord, StorageRpcPayloadError> {
        const MIN_SEGMENT_LEN: usize = 4 + 4 + 16 + 8 + 4 + 2;

        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let created_at = self.read_u64()?;
        let segment_count = self.read_bounded_remaining_count(
            MIN_SEGMENT_LEN,
            "object segment reclaim count exceeds payload",
        )?;
        let mut segments = Vec::new();
        for _ in 0..segment_count {
            segments.push(ObjectSegmentsReclaimSegmentRecord {
                segment_index: self.read_u32()?,
                segment_okh: self.read_fixed_16_bytes("object segment reclaim OKH")?,
                segment_vid: self.read_generation_id()?,
                data_pg_id: self.read_u32()?,
                ec: self.read_ec_shape()?,
            });
        }
        Ok(ObjectSegmentsReclaimRecord {
            bucket,
            key,
            generation_id,
            created_at,
            segments,
        })
    }

    fn read_multipart_reclaim_record(
        &mut self,
    ) -> Result<MultipartReclaimRecord, StorageRpcPayloadError> {
        const MIN_PART_LEN: usize = 1 + 4;
        const MIN_SEGMENT_LEN: usize = 4 + 4 + 16 + 8 + 4 + 2;

        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let created_at = self.read_u64()?;
        let part_count = self.read_bounded_remaining_count(
            MIN_PART_LEN,
            "multipart reclaim count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            parts.push(match self.read_u8()? {
                0 => MultipartReclaimPartRecord::ShardSet {
                    part_number: self.read_u32()?,
                    part_okh: self.read_fixed_16_bytes("multipart reclaim part OKH")?,
                    part_vid: self.read_generation_id()?,
                    data_pg_id: self.read_u32()?,
                    ec: self.read_ec_shape()?,
                },
                1 => {
                    let part_number = self.read_u32()?;
                    let segment_count = self.read_bounded_remaining_count(
                        MIN_SEGMENT_LEN,
                        "multipart reclaim segment count exceeds payload",
                    )?;
                    let mut segments = Vec::new();
                    for _ in 0..segment_count {
                        segments.push(MultipartReclaimPartSegmentRecord {
                            part_number,
                            segment_index: self.read_u32()?,
                            segment_okh: self
                                .read_fixed_16_bytes("multipart reclaim segment OKH")?,
                            segment_vid: self.read_generation_id()?,
                            data_pg_id: self.read_u32()?,
                            ec: self.read_ec_shape()?,
                        });
                    }
                    MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    }
                }
                _ => {
                    return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart reclaim part tag",
                    ));
                }
            });
        }
        Ok(MultipartReclaimRecord {
            bucket,
            key,
            generation_id,
            created_at,
            parts,
        })
    }

    fn read_bounded_remaining_count(
        &mut self,
        min_item_len: usize,
        message: &'static str,
    ) -> Result<usize, StorageRpcPayloadError> {
        let count = self.read_u32()? as usize;
        let remaining = self.bytes.len().saturating_sub(self.cursor);
        if count > remaining / min_item_len {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                message,
            ));
        }
        Ok(count)
    }

    fn read_limited_bounded_remaining_count(
        &mut self,
        min_item_len: usize,
        message: &'static str,
        limit: u32,
    ) -> Result<usize, StorageRpcPayloadError> {
        let count = self.read_u32()? as usize;
        if count > limit as usize {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: count,
                limit: limit as usize,
            });
        }
        let remaining = self.bytes.len().saturating_sub(self.cursor);
        if count > remaining / min_item_len {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                message,
            ));
        }
        Ok(count)
    }

    fn read_fixed_16_bytes(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; 16], StorageRpcPayloadError> {
        self.read_bytes()?
            .try_into()
            .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest(field))
    }

    fn read_commit_direct_put_object_req(
        &mut self,
    ) -> Result<CommitDirectPutObjectReq, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_reservation_id = self.read_session_id()?;
        let versioning = match self.read_u8()? {
            0 => BucketVersioningState::Disabled,
            1 => BucketVersioningState::Enabled,
            2 => BucketVersioningState::Suspended,
            _ => {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid bucket versioning state",
                ));
            }
        };
        let owner = self.read_owner_identity()?;
        let acl_grants = self.read_acl_grants()?;
        let public_read = self.read_bool()?;
        let generation_id = self.read_generation_id()?;
        let size = self.read_u64()?;
        let etag_crc64 = self.read_u64()?;
        let ec = self.read_ec_shape()?;
        let tags = self.read_optional_serialized_tag_set()?;
        let metadata_blob = SerializedMetadataBlob::new(self.read_bytes()?.to_vec());
        let system_metadata_blob = SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec());
        let object_lock = self.read_object_lock_state()?;
        let encryption = self.read_object_encryption()?;
        let segment_index = self.read_u32()?;
        let segment_crc64 = self.read_u64()?;
        let segment_okh_bytes = self.read_bytes()?;
        let segment_okh: [u8; 16] = segment_okh_bytes.try_into().map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "direct PUT segment object key hash must be 16 bytes",
            )
        })?;
        let segment_vid = self.read_generation_id()?;
        let data_pg_id = self.read_u32()?;
        let bucket_write_reservation = self.read_bucket_write_reservation_proof()?;
        Ok(CommitDirectPutObjectReq {
            bucket,
            key,
            generation_reservation_id,
            versioning,
            owner,
            acl_grants,
            public_read,
            generation_id,
            size,
            etag_crc64,
            ec,
            tags,
            metadata_blob,
            system_metadata_blob,
            object_lock,
            encryption,
            segment_index,
            segment_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            bucket_write_reservation,
        })
    }

    fn read_optional_stored_object(
        &mut self,
    ) -> Result<Option<StoredObject>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_stored_object()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stored object tag",
            )),
        }
    }

    fn read_optional_stored_object_list(
        &mut self,
    ) -> Result<Option<Vec<StoredObject>>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let count = self.read_bounded_remaining_count(1, "stored object list too large")?;
                let mut objects = Vec::new();
                for _ in 0..count {
                    objects.push(self.read_stored_object()?);
                }
                Ok(Some(objects))
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stored object list tag",
            )),
        }
    }

    fn read_stored_object_list(&mut self) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let count = self.read_bounded_remaining_count(1, "stored object list too large")?;
        self.read_stored_object_list_items(count)
    }

    fn read_stored_object_list_with_limit(
        &mut self,
        limit: u32,
    ) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let count =
            self.read_limited_bounded_remaining_count(1, "stored object list too large", limit)?;
        self.read_stored_object_list_items(count)
    }

    fn read_stored_object_list_items(
        &mut self,
        count: usize,
    ) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let mut objects = Vec::new();
        for _ in 0..count {
            objects.push(self.read_stored_object()?);
        }
        Ok(objects)
    }

    fn read_stored_object(&mut self) -> Result<StoredObject, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StoredObject::Live(self.read_live_object_record()?)),
            1 => Ok(StoredObject::DeleteMarker(
                self.read_delete_marker_record()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stored object tag",
            )),
        }
    }

    fn read_put_object_metadata_mutation(
        &mut self,
    ) -> Result<PutObjectMetadataMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(PutObjectMetadataMutation::PutTags(self.read_string()?)),
            1 => Ok(PutObjectMetadataMutation::DeleteTags),
            2 => Ok(PutObjectMetadataMutation::PutRetention(ObjectRetention {
                retain_until_unix_seconds: self.read_u64()?,
                mode: self.read_object_lock_mode()?,
            })),
            3 => Ok(PutObjectMetadataMutation::PutLegalHold(
                StoredLegalHoldStatus::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid stored legal hold status",
                    ),
                )?,
            )),
            4 => Ok(PutObjectMetadataMutation::PutAcl {
                acl_grants: self.read_acl_grants()?,
                public_read: self.read_bool()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object metadata mutation tag",
            )),
        }
    }

    fn read_insert_delete_marker_stale_payload(
        &mut self,
    ) -> Result<StorageRpcInsertDeleteMarkerStalePayload, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcInsertDeleteMarkerStalePayload::Explicit(
                self.read_optional_object_payload_reclaim()?,
            )),
            1 => Ok(
                StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: self.read_u64()?,
                },
            ),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid insert delete marker stale payload tag",
            )),
        }
    }

    fn read_create_stream_upload_req(
        &mut self,
    ) -> Result<CreateStreamUploadReq, StorageRpcPayloadError> {
        Ok(CreateStreamUploadReq {
            session_id: SessionId::try_from(self.read_string()?).map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream session id")
            })?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_prepare_stream_segment_append_req(
        &mut self,
    ) -> Result<PrepareStreamUploadSegmentAppendReq, StorageRpcPayloadError> {
        Ok(PrepareStreamUploadSegmentAppendReq {
            session_id: self.read_session_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("stream segment append OKH")?,
        })
    }

    fn read_create_multipart_upload_req(
        &mut self,
    ) -> Result<CreateMultipartUploadReq, StorageRpcPayloadError> {
        Ok(CreateMultipartUploadReq {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            initiator: self.read_optional_owner_identity()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            object_lock: self.read_object_lock_state()?,
            checksum: self.read_optional_multipart_checksum_config()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_optional_owner_identity(
        &mut self,
    ) -> Result<Option<OwnerIdentity>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_owner_identity()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional owner identity tag",
            )),
        }
    }

    fn read_optional_multipart_checksum_config(
        &mut self,
    ) -> Result<Option<MultipartChecksumConfig>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let algorithm = ChecksumAlgorithm::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart checksum algorithm",
                    ),
                )?;
                let checksum_type = ChecksumType::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart checksum type",
                    ),
                )?;
                Ok(Some(
                    MultipartChecksumConfig::new(algorithm, Some(checksum_type)).map_err(|_| {
                        StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "invalid multipart checksum config",
                        )
                    })?,
                ))
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart checksum tag",
            )),
        }
    }

    fn read_stream_upload_target(&mut self) -> Result<StreamUploadTarget, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StreamUploadTarget::PutObject),
            1 => Ok(StreamUploadTarget::UploadPart {
                upload_id: self.read_upload_id()?,
                part_number: self.read_u32()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload target tag",
            )),
        }
    }

    fn read_create_stream_upload_precondition(
        &mut self,
    ) -> Result<StorageRpcCreateStreamUploadPrecondition, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(
                StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation: self.read_bool()?,
                },
            ),
            1 => Ok(StorageRpcCreateStreamUploadPrecondition::PutObject {
                expected_current: self.read_optional_stored_object()?,
                require_generation_reservation: self.read_bool()?,
            }),
            2 => Ok(StorageRpcCreateStreamUploadPrecondition::UploadPart {
                expected_upload: self.read_multipart_upload_record()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload precondition tag",
            )),
        }
    }

    fn read_optional_create_stream_upload_command(
        &mut self,
    ) -> Result<Option<CreateStreamUploadCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_create_stream_upload_command()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stream upload command tag",
            )),
        }
    }

    fn read_create_stream_upload_command(
        &mut self,
    ) -> Result<CreateStreamUploadCommand, StorageRpcPayloadError> {
        Ok(CreateStreamUploadCommand {
            session: self.read_stream_upload_command_record()?,
            initial_next_segment_vid: GenerationId::new(self.read_u64()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid initial stream segment generation",
                ),
            )?,
            bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
        })
    }

    fn read_optional_create_multipart_upload_command(
        &mut self,
    ) -> Result<Option<CreateMultipartUploadCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_create_multipart_upload_command()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart upload command tag",
            )),
        }
    }

    fn read_create_multipart_upload_command(
        &mut self,
    ) -> Result<CreateMultipartUploadCommand, StorageRpcPayloadError> {
        Ok(CreateMultipartUploadCommand {
            upload: self.read_multipart_upload_record()?,
            bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
        })
    }

    fn read_multipart_upload_record(
        &mut self,
    ) -> Result<MultipartUploadRecord, StorageRpcPayloadError> {
        Ok(MultipartUploadRecord {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            initiated_at: self.read_u64()?,
            state: UploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid multipart upload state",
                ),
            )?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            initiator: self.read_optional_owner_identity()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            object_generation_id: GenerationId::new(self.read_u64()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid multipart object generation id",
                ),
            )?,
            object_lock: self.read_object_lock_state()?,
            checksum: self.read_optional_multipart_checksum_config()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_completed_multipart_upload_record(
        &mut self,
    ) -> Result<CompletedMultipartUploadRecord, StorageRpcPayloadError> {
        Ok(CompletedMultipartUploadRecord {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            completion_order: self.read_u64()?,
            completed_at: self.read_u64()?,
            initiator: self.read_optional_owner_identity()?,
            owner: self.read_owner_identity()?,
        })
    }

    fn read_multipart_completion_snapshot(
        &mut self,
    ) -> Result<MultipartCompletionSnapshot, StorageRpcPayloadError> {
        let existing_etag = self.read_optional_string()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "multipart completion snapshot part count exceeds payload",
        )?;
        let mut part_records = Vec::new();
        for _ in 0..part_count {
            part_records.push(self.read_multipart_part_record()?);
        }
        let selected_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "multipart completion snapshot segment count exceeds payload",
        )?;
        let mut selected_streaming_segments = Vec::new();
        for _ in 0..selected_segment_count {
            selected_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let cleanup = self.read_complete_multipart_commit_cleanup()?;
        Ok(MultipartCompletionSnapshot {
            existing_etag,
            stale_payload_source,
            part_records,
            selected_streaming_segments,
            cleanup,
        })
    }

    fn read_list_parts_resp(&mut self) -> Result<ListPartsResp, StorageRpcPayloadError> {
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "multipart parts list count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            parts.push(self.read_multipart_part_record()?);
        }
        let is_truncated = self.read_bool()?;
        let next_part_number_marker = self.read_optional_u32()?;
        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    fn read_listed_multipart_parts(
        &mut self,
    ) -> Result<ListedMultipartParts, StorageRpcPayloadError> {
        Ok(ListedMultipartParts {
            upload: self.read_multipart_upload_record()?,
            response: self.read_list_parts_resp()?,
        })
    }

    fn read_multipart_upload_management_lookup(
        &mut self,
    ) -> Result<MultipartUploadManagementLookup, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(MultipartUploadManagementLookup::InProgress(Box::new(
                self.read_multipart_upload_record()?,
            ))),
            1 => Ok(MultipartUploadManagementLookup::NonInProgress(Box::new(
                self.read_multipart_upload_record()?,
            ))),
            2 => Ok(MultipartUploadManagementLookup::Completed(
                self.read_completed_multipart_upload_record()?,
            )),
            3 => Ok(MultipartUploadManagementLookup::Missing),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart management lookup tag",
            )),
        }
    }

    fn read_stream_upload_command_record(
        &mut self,
    ) -> Result<crate::types::StreamUploadCommandRecord, StorageRpcPayloadError> {
        Ok(crate::types::StreamUploadCommandRecord {
            session_id: SessionId::try_from(self.read_string()?).map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream session id")
            })?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream upload state"),
            )?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_stream_upload_record(&mut self) -> Result<StreamUploadRecord, StorageRpcPayloadError> {
        Ok(StreamUploadRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream upload state"),
            )?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
            next_segment_vid: self.read_generation_id()?,
            bucket_write_reservation: self.read_optional_bucket_write_reservation_proof()?,
        })
    }

    fn read_optional_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<Option<BucketWriteReservationProof>, StorageRpcPayloadError> {
        match self.read_bool()? {
            true => Ok(Some(self.read_bucket_write_reservation_proof()?)),
            false => Ok(None),
        }
    }

    fn read_stream_upload_segment_record(
        &mut self,
    ) -> Result<StreamUploadSegmentRecord, StorageRpcPayloadError> {
        Ok(StreamUploadSegmentRecord {
            session_id: self.read_session_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("stream upload segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_terminal_stream_cleanup_record(
        &mut self,
    ) -> Result<TerminalStreamCleanupRecord, StorageRpcPayloadError> {
        Ok(TerminalStreamCleanupRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid stream cleanup state",
                ),
            )?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_stream_put_finalize_storage_snapshot(
        &mut self,
    ) -> Result<StreamPutFinalizeStorageSnapshot, StorageRpcPayloadError> {
        let session = self.read_stream_upload_record()?;
        let existing_etag = self.read_optional_string()?;
        let generation_id = self.read_generation_id()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let stale_payload = self.read_optional_object_payload_reclaim()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "stream PUT finalize segment count exceeds payload",
        )?;
        let mut staging_segments = Vec::new();
        for _ in 0..segment_count {
            staging_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(StreamPutFinalizeStorageSnapshot {
            session,
            existing_etag,
            generation_id,
            stale_payload_source,
            stale_payload,
            staging_segments,
        })
    }

    fn read_stream_put_commit_input(
        &mut self,
    ) -> Result<StreamPutCommitInput, StorageRpcPayloadError> {
        Ok(StreamPutCommitInput {
            versioning: self.read_bucket_versioning_state()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            size: self.read_u64()?,
            etag_crc64: self.read_u64()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_complete_multipart_commit_request(
        &mut self,
    ) -> Result<CompleteMultipartCommitRequest, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let upload_id = self.read_upload_id()?;
        let versioning = self.read_bucket_versioning_state()?;
        let owner = self.read_owner_identity()?;
        let acl_grants = self.read_acl_grants()?;
        let public_read = self.read_bool()?;
        let generation_id = self.read_generation_id()?;
        let size = self.read_u64()?;
        let etag_crc64 = self.read_bytes()?.try_into().map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart etag crc64 must be 8 bytes",
            )
        })?;
        let tags = self.read_optional_serialized_tag_set()?;
        let metadata_blob = self.read_optional_serialized_metadata_blob()?;
        let system_metadata_blob = self.read_optional_serialized_system_metadata_blob()?;
        let object_lock = self.read_object_lock_state()?;
        let encryption = self.read_object_encryption()?;
        let expected_stale_payload_source = self.read_optional_stored_object()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "complete multipart part count exceeds payload",
        )?;
        let mut part_records = Vec::new();
        for _ in 0..part_count {
            part_records.push(self.read_multipart_part_record()?);
        }
        let selected_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "complete multipart selected segment count exceeds payload",
        )?;
        let mut selected_streaming_segments = Vec::new();
        for _ in 0..selected_segment_count {
            selected_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let expected_cleanup = self.read_complete_multipart_commit_cleanup()?;
        Ok(CompleteMultipartCommitRequest {
            bucket,
            key,
            upload_id,
            versioning,
            owner,
            acl_grants,
            public_read,
            generation_id,
            size,
            etag_crc64,
            tags,
            metadata_blob,
            system_metadata_blob,
            object_lock,
            encryption,
            expected_stale_payload_source,
            part_records,
            selected_streaming_segments,
            expected_cleanup,
        })
    }

    fn read_complete_multipart_commit_cleanup(
        &mut self,
    ) -> Result<CompleteMultipartCommitCleanup, StorageRpcPayloadError> {
        let omitted_part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "complete multipart omitted part count exceeds payload",
        )?;
        let mut omitted_parts = Vec::new();
        for _ in 0..omitted_part_count {
            omitted_parts.push(self.read_multipart_part_record()?);
        }
        let omitted_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "complete multipart omitted segment count exceeds payload",
        )?;
        let mut omitted_streaming_segments = Vec::new();
        for _ in 0..omitted_segment_count {
            omitted_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let stream_upload_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
            "complete multipart stream cleanup count exceeds payload",
        )?;
        let mut stream_uploads = Vec::new();
        for _ in 0..stream_upload_count {
            stream_uploads.push(self.read_terminal_stream_cleanup_record()?);
        }
        let stream_upload_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "complete multipart stream segment cleanup count exceeds payload",
        )?;
        let mut stream_upload_segments = Vec::new();
        for _ in 0..stream_upload_segment_count {
            stream_upload_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(CompleteMultipartCommitCleanup {
            omitted_parts,
            omitted_streaming_segments,
            stream_uploads,
            stream_upload_segments,
        })
    }

    fn read_optional_abort_multipart_upload_cleanup(
        &mut self,
    ) -> Result<Option<AbortMultipartUploadCleanup>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_abort_multipart_upload_cleanup()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional abort multipart cleanup tag",
            )),
        }
    }

    fn read_abort_multipart_upload_cleanup(
        &mut self,
    ) -> Result<AbortMultipartUploadCleanup, StorageRpcPayloadError> {
        let upload = self.read_multipart_upload_record()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "abort multipart part cleanup count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            parts.push(self.read_multipart_part_record()?);
        }
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "abort multipart segment cleanup count exceeds payload",
        )?;
        let mut streaming_segments = Vec::new();
        for _ in 0..segment_count {
            streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let stream_upload_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
            "abort multipart stream cleanup count exceeds payload",
        )?;
        let mut stream_uploads = Vec::new();
        for _ in 0..stream_upload_count {
            stream_uploads.push(self.read_terminal_stream_cleanup_record()?);
        }
        let stream_upload_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "abort multipart stream segment cleanup count exceeds payload",
        )?;
        let mut stream_upload_segments = Vec::new();
        for _ in 0..stream_upload_segment_count {
            stream_upload_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(AbortMultipartUploadCleanup {
            upload,
            parts,
            streaming_segments,
            stream_uploads,
            stream_upload_segments,
        })
    }

    fn read_stream_upload_part_snapshot(
        &mut self,
    ) -> Result<StreamUploadPartSnapshot, StorageRpcPayloadError> {
        let session = self.read_stream_upload_record()?;
        let upload = self.read_multipart_upload_record()?;
        let existing_part_generation = self.read_optional_u32()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "stream part finalize segment count exceeds payload",
        )?;
        let mut staging_segments = Vec::new();
        for _ in 0..segment_count {
            staging_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(StreamUploadPartSnapshot {
            session,
            upload,
            existing_part_generation,
            staging_segments,
        })
    }

    fn read_optional_multipart_part_record(
        &mut self,
    ) -> Result<Option<MultipartPartRecord>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_multipart_part_record()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart part tag",
            )),
        }
    }

    fn read_multipart_part_record(
        &mut self,
    ) -> Result<MultipartPartRecord, StorageRpcPayloadError> {
        Ok(MultipartPartRecord {
            upload_id: self.read_upload_id()?,
            part_number: self.read_u32()?,
            generation: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid etag kind"),
            )?,
            part_okh: self.read_fixed_16_bytes("multipart part OKH")?,
            part_vid: self.read_generation_id()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            last_modified: self.read_u64()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn read_stream_part_finalize_storage_snapshot(
        &mut self,
    ) -> Result<StreamUploadPartStorageSnapshot, StorageRpcPayloadError> {
        let auth_snapshot = self.read_stream_upload_part_snapshot()?;
        let existing_part = self.read_optional_multipart_part_record()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "stream part finalize displaced segment count exceeds payload",
        )?;
        let mut displaced_segments = Vec::new();
        for _ in 0..segment_count {
            displaced_segments.push(self.read_multipart_part_segment_record()?);
        }
        Ok(StreamUploadPartStorageSnapshot {
            auth_snapshot,
            existing_part,
            displaced_segments,
        })
    }

    fn read_optional_delete_object_version_target(
        &mut self,
    ) -> Result<Option<DeleteObjectVersionTarget>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_delete_object_version_target()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional delete object version target tag",
            )),
        }
    }

    fn read_delete_object_version_target(
        &mut self,
    ) -> Result<DeleteObjectVersionTarget, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(DeleteObjectVersionTarget::DeleteMarker {
                write_sequence: self.read_u64()?,
            }),
            1 => Ok(DeleteObjectVersionTarget::Live {
                generation_id: GenerationId::new(self.read_u64()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid delete target generation id",
                    ),
                )?,
                layout: self.read_object_layout()?,
                payload: self.read_object_payload_reclaim()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid delete object version target tag",
            )),
        }
    }

    fn read_metadata_command_envelope_response_item(
        &mut self,
    ) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
        let item = self.read_metadata_command_item()?;
        metadata_command_envelope_from_item(&item)
    }

    fn read_live_object_record(&mut self) -> Result<LiveObjectRecord, StorageRpcPayloadError> {
        Ok(LiveObjectRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            generation_id: self.read_generation_id()?,
            size: self.read_u64()?,
            etag: self.read_object_etag()?,
            last_modified: self.read_u64()?,
            became_noncurrent_at: self.read_optional_u64()?,
            storage_class: self.read_storage_class()?,
            ec: self.read_ec_shape()?,
            layout: self.read_object_layout()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: self.read_optional_serialized_metadata_blob()?,
            system_metadata_blob: self.read_optional_serialized_system_metadata_blob()?,
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_upload_id(&mut self) -> Result<UploadId, StorageRpcPayloadError> {
        UploadId::try_from(self.read_string_with_limit(
            UPLOAD_ID_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("upload id is too large"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid upload id"))
    }

    fn read_optional_upload_id(&mut self) -> Result<Option<UploadId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_upload_id()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional upload id tag",
            )),
        }
    }

    fn read_object_read_auth_subject(
        &mut self,
    ) -> Result<ObjectReadAuthSubject, StorageRpcPayloadError> {
        let stored = self.read_stored_object()?;
        Ok(ObjectReadAuthSubject {
            identity: ObjectReadAuthSubjectIdentity::for_stored(&stored),
            stored,
        })
    }

    fn read_object_read_snapshot(&mut self) -> Result<ObjectReadSnapshot, StorageRpcPayloadError> {
        let stored = self.read_stored_object()?;
        let object_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN,
            "object read segment count exceeds payload",
        )?;
        let mut object_segments = Vec::new();
        for _ in 0..object_segment_count {
            object_segments.push(self.read_object_segment_record()?);
        }
        let multipart_part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_OBJECT_PART_RECORD_LEN,
            "object read part count exceeds payload",
        )?;
        let mut multipart_parts = Vec::new();
        for _ in 0..multipart_part_count {
            multipart_parts.push(self.read_object_part_record()?);
        }
        let multipart_part_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "object read multipart segment count exceeds payload",
        )?;
        let mut multipart_part_segments = Vec::new();
        for _ in 0..multipart_part_segment_count {
            multipart_part_segments.push(self.read_multipart_part_segment_record()?);
        }
        Ok(ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        })
    }

    fn read_object_segment_record(
        &mut self,
    ) -> Result<ObjectSegmentRecord, StorageRpcPayloadError> {
        Ok(ObjectSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("object segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_object_part_record(&mut self) -> Result<ObjectPartRecord, StorageRpcPayloadError> {
        Ok(ObjectPartRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            part_number: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid etag kind"),
            )?,
            part_okh: self.read_fixed_16_bytes("object part OKH")?,
            part_vid: self.read_generation_id()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            data_pg_id: self.read_u32()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn read_multipart_part_segment_record(
        &mut self,
    ) -> Result<MultipartPartSegmentRecord, StorageRpcPayloadError> {
        Ok(MultipartPartSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            upload_id: self.read_upload_id()?,
            version_id: self.read_u64()?,
            part_number: self.read_u32()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("multipart part segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_optional_checksum_bytes(
        &mut self,
    ) -> Result<Option<ChecksumBytes>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Some(
                ChecksumBytes::new(self.read_bytes()?)
                    .map_err(StorageRpcPayloadError::InvalidChecksumMetadata),
            )
            .transpose(),
            _ => Err(StorageRpcPayloadError::InvalidChecksumMetadata(
                "invalid optional checksum tag",
            )),
        }
    }

    fn read_object_read_snapshot_mode(
        &mut self,
    ) -> Result<ObjectReadSnapshotMode, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectReadSnapshotMode::MetadataOnly),
            1 => Ok(ObjectReadSnapshotMode::StandardSegments),
            2 => Ok(ObjectReadSnapshotMode::MultipartParts),
            3 => Ok(ObjectReadSnapshotMode::FullPayloadLayout),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object read snapshot mode",
            )),
        }
    }

    fn read_delete_marker_record(&mut self) -> Result<DeleteMarkerRecord, StorageRpcPayloadError> {
        Ok(DeleteMarkerRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            last_modified: self.read_u64()?,
        })
    }

    fn read_canonical_user_id(&mut self) -> Result<CanonicalUserId, StorageRpcPayloadError> {
        let value = self.read_string_with_limit(
            s3_types::CANONICAL_USER_ID_LEN,
            StorageRpcPayloadError::InvalidBucketMetadataRequest("canonical user id is too large"),
        )?;
        CanonicalUserId::parse_stored(&value).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid canonical user id"),
        )
    }

    fn read_acl_grants(&mut self) -> Result<AclGrants, StorageRpcPayloadError> {
        let value = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_ACL_GRANTS_LEN,
            StorageRpcPayloadError::InvalidBucketMetadataRequest("ACL grants are too large"),
        )?;
        AclGrants::parse(&value)
            .map_err(|_| StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid ACL grants"))
    }

    fn read_owner_identity(&mut self) -> Result<OwnerIdentity, StorageRpcPayloadError> {
        let principal = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("owner principal is too large"),
        )?;
        let canonical_id = self.read_canonical_user_id()?;
        Ok(OwnerIdentity {
            principal,
            canonical_id,
        })
    }

    fn read_bool(&mut self) -> Result<bool, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bool tag",
            )),
        }
    }

    fn read_bucket_state(&mut self) -> Result<BucketState, StorageRpcPayloadError> {
        BucketState::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket state"),
        )
    }

    fn read_bucket_versioning_state(
        &mut self,
    ) -> Result<BucketVersioningState, StorageRpcPayloadError> {
        BucketVersioningState::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket versioning state"),
        )
    }

    fn read_bucket_metadata_control_mutation(
        &mut self,
    ) -> Result<StorageRpcBucketMetadataControlMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcBucketMetadataControlMutation::Versioning(
                self.read_bucket_versioning_state()?,
            )),
            1 => Ok(StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: self.read_acl_grants()?,
                public_read: self.read_bool()?,
                public_write: self.read_bool()?,
            }),
            2 => Ok(StorageRpcBucketMetadataControlMutation::Property(
                self.read_bucket_property_mutation()?,
            )),
            3 => Ok(StorageRpcBucketMetadataControlMutation::Subresource(
                self.read_bucket_subresource_mutation()?,
            )),
            4 => Ok(StorageRpcBucketMetadataControlMutation::MarkDeleting),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket metadata control mutation tag",
            )),
        }
    }

    fn read_bucket_property_mutation(
        &mut self,
    ) -> Result<BucketPropertyMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(BucketPropertyMutation::ObjectLock(
                self.read_bucket_object_lock_config()?,
            )),
            1 => Ok(BucketPropertyMutation::Encryption(
                self.read_bucket_encryption_config()?,
            )),
            2 => Ok(BucketPropertyMutation::PublicAccessBlock(
                self.read_optional_public_access_block_config()?,
            )),
            3 => Ok(BucketPropertyMutation::OwnershipControls(
                self.read_optional_bucket_ownership_controls()?,
            )),
            4 => Ok(BucketPropertyMutation::AbacEnabled(self.read_bool()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket property mutation tag",
            )),
        }
    }

    fn read_bucket_encryption_config(
        &mut self,
    ) -> Result<BucketEncryptionConfig, StorageRpcPayloadError> {
        let default_encryption = match self.read_u8()? {
            0 => None,
            1 => Some(ManagedEncryptionAlgorithm::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid managed encryption algorithm",
                ),
            )?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid bucket encryption tag",
                ))
            }
        };
        Ok(BucketEncryptionConfig {
            default_encryption,
            sse_c_blocked: self.read_bool()?,
        })
    }

    fn read_bucket_subresource_mutation(
        &mut self,
    ) -> Result<BucketSubresourceMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            1 => {
                let kind = self.read_bucket_subresource_kind()?;
                let body = self.read_string_with_limit(
                    STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
                    StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "bucket subresource body is too large",
                    ),
                )?;
                let aux = self.read_bucket_subresource_aux(kind)?;
                Ok(BucketSubresourceMutation::Put { kind, body, aux })
            }
            2 => Ok(BucketSubresourceMutation::Delete {
                kind: self.read_bucket_subresource_kind()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket subresource mutation tag",
            )),
        }
    }

    fn read_bucket_subresource_kind(
        &mut self,
    ) -> Result<BucketSubresourceKind, StorageRpcPayloadError> {
        BucketSubresourceKind::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket subresource kind"),
        )
    }

    fn read_bucket_subresource_aux(
        &mut self,
        kind: BucketSubresourceKind,
    ) -> Result<BucketSubresourceAux, StorageRpcPayloadError> {
        let aux = match self.read_u8()? {
            0 => BucketSubresourceAux::None,
            1 if kind == BucketSubresourceKind::Policy => {
                BucketSubresourceAux::policy(self.read_bool()?)
            }
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid bucket subresource aux",
                ))
            }
        };
        if !kind.supports_aux(aux) {
            return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "bucket subresource kind does not support aux",
            ));
        }
        Ok(aux)
    }

    fn read_storage_class(&mut self) -> Result<StorageClass, StorageRpcPayloadError> {
        StorageClass::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid storage class"),
        )
    }

    fn read_ec_shape(&mut self) -> Result<EcShape, StorageRpcPayloadError> {
        Ok(EcShape {
            k: self.read_u8()?,
            m: self.read_u8()?,
        })
    }

    fn read_16_bytes(&mut self) -> Result<[u8; 16], StorageRpcPayloadError> {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(self.read_exact(16)?);
        Ok(bytes)
    }

    fn read_scavenger_observation_reason(
        &mut self,
    ) -> Result<ShardScavengerObservationReason, StorageRpcPayloadError> {
        ShardScavengerObservationReason::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid shard scavenger observation reason",
            ),
        )
    }

    fn read_scavenger_observation_key(
        &mut self,
    ) -> Result<ShardScavengerObservationKey, StorageRpcPayloadError> {
        let node_id = self.read_u32()?;
        let data_pg_id = self.read_u32()?;
        let shard_index = ShardIndex::new(self.read_u8()?);
        let shard_key = self.read_shard_key()?;
        if shard_key.shard_index() != shard_index {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "shard scavenger observation key shard index mismatch",
            ));
        }
        Ok(ShardScavengerObservationKey {
            node_id,
            data_pg_id,
            shard_index,
            shard_key,
        })
    }

    fn read_scavenger_observation_record(
        &mut self,
    ) -> Result<ShardScavengerObservationRecord, StorageRpcPayloadError> {
        Ok(ShardScavengerObservationRecord {
            key: self.read_scavenger_observation_key()?,
            data_size: self.read_optional_u64()?,
            crc64: self.read_optional_u64()?,
            file_exists: self.read_bool()?,
            shard_row_exists: self.read_bool()?,
            reason: self.read_scavenger_observation_reason()?,
            last_error: self.read_optional_string_with_limit(
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN + 1,
                    limit: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                },
            )?,
        })
    }

    fn read_scavenger_observation(
        &mut self,
    ) -> Result<ShardScavengerObservation, StorageRpcPayloadError> {
        Ok(ShardScavengerObservation {
            key: self.read_scavenger_observation_key()?,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            data_size: self.read_optional_u64()?,
            crc64: self.read_optional_u64()?,
            file_exists: self.read_bool()?,
            shard_row_exists: self.read_bool()?,
            reason: self.read_scavenger_observation_reason()?,
            last_error: self.read_optional_string_with_limit(
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN + 1,
                    limit: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                },
            )?,
            resolved_at: self.read_optional_u64()?,
        })
    }

    fn read_segment_stored_bytes_request(
        &mut self,
    ) -> Result<SegmentStoredBytesRequest, StorageRpcPayloadError> {
        Ok(SegmentStoredBytesRequest {
            data_pg_id: self.read_u32()?,
            segment_okh: self.read_16_bytes()?,
            segment_vid: self.read_generation_id()?,
            stored_size: self.read_u64()? as usize,
            segment_crc64: self.read_u64()?,
            ec: self.read_ec_shape()?,
        })
    }

    fn read_placed_segment_shard_repair_work_item(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairWorkItem, StorageRpcPayloadError> {
        let request = self.read_segment_stored_bytes_request()?;
        let shard_index = ShardIndex::new(self.read_u8()?);
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        validate_placed_segment_shard_repair_work_item(&work_item)?;
        Ok(work_item)
    }

    fn read_placed_segment_shard_repair_record(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairRecord, StorageRpcPayloadError> {
        Ok(PlacedSegmentShardRepairRecord {
            work_item: self.read_placed_segment_shard_repair_work_item()?,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_repair_claim_record(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairClaimRecord, StorageRpcPayloadError> {
        Ok(PlacedSegmentShardRepairClaimRecord {
            work_item: self.read_placed_segment_shard_repair_work_item()?,
            claim_id: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
            )?,
            owner_token: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken(
                    "owner token exceeds maximum length",
                ),
            )?,
            cluster_epoch: self.read_cluster_epoch()?,
            claimed_at: self.read_u64()?,
            lease_deadline: self.read_optional_u64()?,
            attempt_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_backfill_work_item(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillWorkItem, StorageRpcPayloadError> {
        let request = self.read_segment_stored_bytes_request()?;
        let source_cluster_epoch = self.read_cluster_epoch()?;
        let desired_cluster_epoch = self.read_cluster_epoch()?;
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request,
            source_cluster_epoch,
            desired_cluster_epoch,
        };
        validate_placed_segment_shard_backfill_work_item(&work_item)?;
        Ok(work_item)
    }

    fn read_placed_segment_shard_backfill_record(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillRecord, StorageRpcPayloadError> {
        let work_item = self.read_placed_segment_shard_backfill_work_item()?;
        let remaining_tolerance = self.read_u8()?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &work_item,
            remaining_tolerance,
        )?;
        Ok(PlacedSegmentShardBackfillRecord {
            work_item,
            remaining_tolerance,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_backfill_claim_record(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillClaimRecord, StorageRpcPayloadError> {
        let work_item = self.read_placed_segment_shard_backfill_work_item()?;
        let remaining_tolerance = self.read_u8()?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &work_item,
            remaining_tolerance,
        )?;
        Ok(PlacedSegmentShardBackfillClaimRecord {
            work_item,
            remaining_tolerance,
            claim_id: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
            )?,
            owner_token: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken(
                    "owner token exceeds maximum length",
                ),
            )?,
            cluster_epoch: self.read_cluster_epoch()?,
            claimed_at: self.read_u64()?,
            lease_deadline: self.read_optional_u64()?,
            attempt_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_scavenger_payload_reference(
        &mut self,
    ) -> Result<ShardScavengerPayloadReference, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ShardScavengerPayloadReference::Placed(
                ShardScavengerPlacedShardSetReference {
                    data_pg_id: self.read_u32()?,
                    okh: self.read_16_bytes()?,
                    generation_id: self.read_generation_id()?,
                    placement_cluster_epoch: self.read_cluster_epoch()?,
                    stored_size: self.read_u64()?,
                    crc64: self.read_u64()?,
                    ec: self.read_ec_shape()?,
                },
            )),
            1 => Ok(ShardScavengerPayloadReference::RoutedMultipartPart(
                ShardScavengerRoutedMultipartPartReference {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    object_generation_id: self.read_generation_id()?,
                    part_number: self.read_u32()?,
                    stored_size: self.read_u64()?,
                    crc64: self.read_u64()?,
                    part_okh: self.read_16_bytes()?,
                    part_vid: self.read_generation_id()?,
                    placement_cluster_epoch: self.read_cluster_epoch()?,
                    ec: self.read_ec_shape()?,
                },
            )),
            2 => Ok(ShardScavengerPayloadReference::ReclaimOnly(
                ShardScavengerReclaimShardSetReference {
                    data_pg_id: self.read_u32()?,
                    okh: self.read_16_bytes()?,
                    generation_id: self.read_generation_id()?,
                    ec: self.read_ec_shape()?,
                },
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid shard scavenger payload reference tag",
            )),
        }
    }

    fn read_object_etag(&mut self) -> Result<ObjectEtag, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => {
                let mut crc64 = [0u8; 8];
                crc64.copy_from_slice(self.read_exact(8)?);
                Ok(ObjectEtag::SinglePart(crc64))
            }
            1 => {
                let mut crc64 = [0u8; 8];
                crc64.copy_from_slice(self.read_exact(8)?);
                let parts = NonZeroU32::new(self.read_u32()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "multipart etag parts must not be zero",
                    ),
                )?;
                Ok(ObjectEtag::MultipartComposite { crc64, parts })
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object etag tag",
            )),
        }
    }

    fn read_object_layout(&mut self) -> Result<ObjectLayout, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectLayout::Standard),
            1 => {
                let parts_count = NonZeroU32::new(self.read_u32()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "multipart layout parts must not be zero",
                    ),
                )?;
                Ok(ObjectLayout::MultipartManifest { parts_count })
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object layout tag",
            )),
        }
    }

    fn read_bucket_object_lock_config(
        &mut self,
    ) -> Result<BucketObjectLockConfig, StorageRpcPayloadError> {
        let enabled = self.read_bool()?;
        let default_retention = match self.read_u8()? {
            0 => None,
            1 => Some(ObjectLockDefaultRetention {
                mode: self.read_object_lock_mode()?,
                period: self.read_retention_period()?,
            }),
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid object-lock default-retention tag",
                ));
            }
        };
        Ok(BucketObjectLockConfig {
            enabled,
            default_retention,
        })
    }

    fn read_object_lock_mode(&mut self) -> Result<ObjectLockMode, StorageRpcPayloadError> {
        ObjectLockMode::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid object-lock mode"),
        )
    }

    fn read_retention_period(&mut self) -> Result<RetentionPeriod, StorageRpcPayloadError> {
        let value = self.read_u32()?;
        let value =
            NonZeroU32::new(value).ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "retention period must not be zero",
            ))?;
        match self.read_u8()? {
            0 => Ok(RetentionPeriod::Days(value)),
            1 => Ok(RetentionPeriod::Years(value)),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid retention period tag",
            )),
        }
    }

    fn read_object_lock_state(&mut self) -> Result<ObjectLockState, StorageRpcPayloadError> {
        let retention = match self.read_u8()? {
            0 => None,
            1 => Some(ObjectRetention {
                retain_until_unix_seconds: self.read_u64()?,
                mode: self.read_object_lock_mode()?,
            }),
            _ => {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid object-lock retention tag",
                ))
            }
        };
        let legal_hold = StoredLegalHoldStatus::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stored legal hold status",
            ),
        )?;
        Ok(ObjectLockState {
            retention,
            legal_hold,
        })
    }

    fn read_object_encryption(&mut self) -> Result<ObjectEncryption, StorageRpcPayloadError> {
        let encryption_type = ObjectEncryptionType::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid object encryption type"),
        )?;
        let state = self.read_optional_bytes_value()?;
        ObjectEncryption::decode(encryption_type, state).map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid object encryption state")
        })
    }

    fn read_optional_serialized_tag_set(
        &mut self,
    ) -> Result<Option<SerializedTagSet>, StorageRpcPayloadError> {
        Ok(self.read_optional_string()?.map(SerializedTagSet::new))
    }

    fn read_optional_serialized_metadata_blob(
        &mut self,
    ) -> Result<Option<SerializedMetadataBlob>, StorageRpcPayloadError> {
        Ok(self
            .read_optional_bytes_value()?
            .map(SerializedMetadataBlob::new))
    }

    fn read_optional_serialized_system_metadata_blob(
        &mut self,
    ) -> Result<Option<SerializedSystemMetadataBlob>, StorageRpcPayloadError> {
        Ok(self
            .read_optional_bytes_value()?
            .map(SerializedSystemMetadataBlob::new))
    }

    fn read_optional_bytes_value(&mut self) -> Result<Option<Vec<u8>>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_bytes()?.to_vec())),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional bytes tag",
            )),
        }
    }

    fn read_optional_public_access_block_config(
        &mut self,
    ) -> Result<Option<PublicAccessBlockConfig>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(PublicAccessBlockConfig {
                block_public_acls: self.read_bool()?,
                ignore_public_acls: self.read_bool()?,
                block_public_policy: self.read_bool()?,
                restrict_public_buckets: self.read_bool()?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid public-access-block tag",
            )),
        }
    }

    fn read_optional_bucket_ownership_controls(
        &mut self,
    ) -> Result<Option<BucketOwnershipControls>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "invalid object ownership",
                    ),
                )?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid ownership-controls tag",
            )),
        }
    }

    fn read_bucket_ownership_controls(
        &mut self,
    ) -> Result<BucketOwnershipControls, StorageRpcPayloadError> {
        Ok(BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid object ownership"),
            )?,
        })
    }

    fn read_effective_bucket_encryption_config(
        &mut self,
    ) -> Result<EffectiveBucketEncryptionConfig, StorageRpcPayloadError> {
        Ok(EffectiveBucketEncryptionConfig {
            default_encryption: ManagedEncryptionAlgorithm::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid managed encryption algorithm",
                ),
            )?,
            sse_c_blocked: self.read_bool()?,
        })
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    fn read_u8(&mut self) -> Result<u8, StorageRpcPayloadError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, StorageRpcPayloadError> {
        let mut bytes = [0u8; 2];
        bytes.copy_from_slice(self.read_exact(2)?);
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, StorageRpcPayloadError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, StorageRpcPayloadError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(bytes))
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("storage RPC byte slice length must fit in u32");
    put_u32(out, len);
    out.extend_from_slice(bytes);
}

fn checked_u32_len(len: usize) -> Result<u32, StorageRpcPayloadError> {
    u32::try_from(len).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
        len,
        limit: u32::MAX as usize,
    })
}

fn put_string_vec(out: &mut Vec<u8>, values: &[String]) -> Result<(), StorageRpcPayloadError> {
    put_u32(out, checked_u32_len(values.len())?);
    for value in values {
        put_string(out, value);
    }
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_shard_location(out: &mut Vec<u8>, location: ShardLocation) {
    put_u64(out, location.cluster_epoch().get());
    put_u32(out, location.data_pg_id().get());
    put_u8(out, location.shard_index().get());
    put_u32(out, location.node_id().as_u32());
}

fn put_claim_token(out: &mut Vec<u8>, token: &StorageRpcDurableClaimToken) {
    match token {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(token) => {
            put_u8(out, 0);
            put_string(out, token.bucket.as_str());
            put_u64(out, token.bucket_incarnation_generation);
            put_string(out, token.key.as_str());
            put_u64(out, token.generation_id.get());
            put_u8(out, token.reclaim_kind as u8);
            put_string(out, &token.claim_id);
            put_string(out, &token.owner_token);
            put_u64(out, token.cluster_epoch.get());
            put_u32(out, token.pg_id);
        }
        StorageRpcDurableClaimToken::BucketDeleteFinalize(token) => {
            put_u8(out, 1);
            put_bucket_claim_token(out, token);
        }
        StorageRpcDurableClaimToken::LifecycleSweep(token) => {
            put_u8(out, 2);
            put_bucket_claim_token(out, token);
        }
    }
}

fn put_bucket_claim_token(out: &mut Vec<u8>, token: &StorageRpcBucketClaimToken) {
    put_string(out, token.bucket.as_str());
    put_u64(out, token.bucket_incarnation_generation);
    put_string(out, &token.claim_id);
    put_string(out, &token.owner_token);
    put_u64(out, token.cluster_epoch.get());
    put_u32(out, token.pg_id);
}

fn put_bucket_write_reservation_proof(out: &mut Vec<u8>, proof: &BucketWriteReservationProof) {
    put_string(out, proof.bucket.as_str());
    put_string(out, &proof.reservation_id);
    put_string(out, &proof.owner_token);
    put_u64(out, proof.cluster_epoch.get());
    put_u64(out, proof.bucket_execution_generation);
    put_u64(out, proof.bucket_incarnation_generation);
    put_string(out, &proof.operation_kind);
    put_u64(out, proof.created_at);
    put_optional_u64(out, proof.lease_deadline);
    put_optional_string(out, proof.target_context.as_deref());
}

fn put_bucket_write_reservation_record(out: &mut Vec<u8>, record: &BucketWriteReservationRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.reservation_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.operation_kind);
    put_u64(out, record.created_at);
    put_optional_u64(out, record.lease_deadline);
    put_optional_string(out, record.target_context.as_deref());
}

fn put_bucket_write_drain_record(out: &mut Vec<u8>, record: &BucketWriteDrainRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.drain_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u8(
        out,
        match record.state {
            BucketWriteDrainState::Draining => 0,
        },
    );
    put_u64(out, record.created_at);
    put_optional_u64(out, record.lease_deadline);
}

fn put_bucket_delete_attempt_outcome_record(
    out: &mut Vec<u8>,
    record: &BucketDeleteAttemptOutcomeRecord,
) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.drain_id);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u8(out, record.outcome as u8);
    put_u8(out, record.phase as u8);
    put_string(out, &record.detail);
    put_optional_u32(out, record.post_reservation_next_object_pg_id);
    put_u64(out, record.updated_at);
}

fn put_bucket_delete_finalize_root(out: &mut Vec<u8>, root: &BucketDeleteFinalizeRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_incarnation_generation);
}

fn put_bucket_delete_begin_root(out: &mut Vec<u8>, root: &BucketDeleteBeginRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_execution_generation);
    put_u64(out, root.bucket_incarnation_generation);
}

fn put_bucket_delete_finalize_claim_record(
    out: &mut Vec<u8>,
    record: &BucketDeleteFinalizeClaimRecord,
) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_object_payload_reclaim_claim_record(
    out: &mut Vec<u8>,
    record: &ObjectPayloadReclaimClaimRecord,
) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, record.key.as_str());
    put_u64(out, record.generation_id.get());
    put_u8(out, record.reclaim_kind as u8);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_lifecycle_sweep_root(out: &mut Vec<u8>, root: &LifecycleSweepRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_incarnation_generation);
    put_u8(
        out,
        match root.source {
            LifecycleSweepRootSource::ExpiredClaim => 0,
            LifecycleSweepRootSource::BusyClaim => 1,
            LifecycleSweepRootSource::LifecycleConfig => 2,
            LifecycleSweepRootSource::AbortingMultipartUpload => 3,
        },
    );
}

fn put_lifecycle_sweep_claim_record(out: &mut Vec<u8>, record: &LifecycleSweepClaimRecord) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_u64(out, record.heartbeat_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_create_bucket_config(out: &mut Vec<u8>, config: &StorageRpcCreateBucketConfig) {
    put_string(out, config.name.as_str());
    put_string(out, &config.owner_principal);
    put_string(out, config.owner_canonical_id.as_str());
    put_string(out, &config.acl_grants.serialized());
    put_bool(out, config.public_read);
    put_bool(out, config.public_write);
    put_u8(out, config.versioning as u8);
    put_bucket_object_lock_config(out, &config.object_lock);
    put_u8(out, config.ownership_controls.object_ownership as u8);
}

fn put_bucket_info(out: &mut Vec<u8>, info: &BucketInfo) {
    put_string(out, info.name.as_str());
    put_string(out, &info.owner_principal);
    put_string(out, info.owner_canonical_id.as_str());
    put_u64(out, info.created_at);
    put_u16(out, info.region);
    put_u8(out, info.state as u8);
    put_u8(out, info.versioning as u8);
    put_bucket_object_lock_config(out, &info.object_lock);
    put_string(out, &info.acl_grants.serialized());
    put_bool(out, info.public_read);
    put_bool(out, info.public_write);
    put_optional_public_access_block_config(out, info.public_access_block);
    put_optional_bucket_ownership_controls(out, info.ownership_controls);
    put_bool(out, info.bucket_policy_present);
    put_bool(out, info.bucket_policy_public);
    put_u64(out, info.bucket_policy_generation);
    put_bool(out, info.bucket_lifecycle_present);
    put_u64(out, info.bucket_lifecycle_generation);
    put_u64(out, info.bucket_execution_generation);
    put_u64(out, info.bucket_incarnation_generation);
    put_bool(out, info.bucket_abac_enabled);
    put_u8(out, info.encryption.default_encryption as u8);
    put_bool(out, info.encryption.sse_c_blocked);
}

fn put_bucket_fast_path_identity(out: &mut Vec<u8>, identity: BucketFastPathIdentity) {
    put_u64(out, identity.bucket_execution_generation);
    put_u64(out, identity.bucket_incarnation_generation);
}

fn put_bucket_snapshot_request(out: &mut Vec<u8>, request: BucketSnapshotRequest) {
    put_bool(out, request.policy);
    put_u8(
        out,
        match request.tags {
            BucketSnapshotTagsRequest::NotRequested => 0,
            BucketSnapshotTagsRequest::IfBucketAbacEnabled => 1,
            BucketSnapshotTagsRequest::Always => 2,
        },
    );
    put_bool(out, request.lifecycle);
    put_bool(out, request.cors);
}

fn put_bucket_snapshot(out: &mut Vec<u8>, snapshot: &BucketSnapshot) {
    put_bucket_info(out, &snapshot.bucket);
    put_bucket_snapshot_request(out, snapshot.request);
    put_loaded_bucket_subresource(out, &snapshot.policy);
    put_loaded_bucket_subresource(out, &snapshot.tags);
    put_loaded_bucket_subresource(out, &snapshot.lifecycle);
    put_loaded_bucket_subresource(out, &snapshot.cors);
}

fn put_bucket_snapshot_pair(out: &mut Vec<u8>, pair: &BucketSnapshotPair) {
    match pair {
        BucketSnapshotPair::Same { bucket } => {
            put_u8(out, 0);
            put_bucket_snapshot(out, bucket);
        }
        BucketSnapshotPair::Distinct {
            source,
            destination,
        } => {
            put_u8(out, 1);
            put_bucket_snapshot(out, source);
            put_bucket_snapshot(out, destination);
        }
    }
}

fn put_bucket_metadata_control_mutation(
    out: &mut Vec<u8>,
    mutation: &StorageRpcBucketMetadataControlMutation,
) {
    match mutation {
        StorageRpcBucketMetadataControlMutation::Versioning(state) => {
            put_u8(out, 0);
            put_u8(out, *state as u8);
        }
        StorageRpcBucketMetadataControlMutation::Acl {
            acl_grants,
            public_read,
            public_write,
        } => {
            put_u8(out, 1);
            put_string(out, &acl_grants.serialized());
            put_bool(out, *public_read);
            put_bool(out, *public_write);
        }
        StorageRpcBucketMetadataControlMutation::Property(mutation) => {
            put_u8(out, 2);
            put_bucket_property_mutation(out, mutation);
        }
        StorageRpcBucketMetadataControlMutation::Subresource(mutation) => {
            put_u8(out, 3);
            put_bucket_subresource_mutation(out, mutation);
        }
        StorageRpcBucketMetadataControlMutation::MarkDeleting => {
            put_u8(out, 4);
        }
    }
}

fn put_bucket_property_mutation(out: &mut Vec<u8>, mutation: &BucketPropertyMutation) {
    match mutation {
        BucketPropertyMutation::ObjectLock(config) => {
            put_u8(out, 0);
            put_bucket_object_lock_config(out, config);
        }
        BucketPropertyMutation::Encryption(config) => {
            put_u8(out, 1);
            put_bucket_encryption_config(out, *config);
        }
        BucketPropertyMutation::PublicAccessBlock(config) => {
            put_u8(out, 2);
            put_optional_public_access_block_config(out, *config);
        }
        BucketPropertyMutation::OwnershipControls(config) => {
            put_u8(out, 3);
            put_optional_bucket_ownership_controls(out, *config);
        }
        BucketPropertyMutation::AbacEnabled(enabled) => {
            put_u8(out, 4);
            put_bool(out, *enabled);
        }
    }
}

fn put_bucket_encryption_config(out: &mut Vec<u8>, config: BucketEncryptionConfig) {
    match config.default_encryption {
        None => put_u8(out, 0),
        Some(algorithm) => {
            put_u8(out, 1);
            put_u8(out, algorithm as u8);
        }
    }
    put_bool(out, config.sse_c_blocked);
}

fn put_bucket_subresource_mutation(out: &mut Vec<u8>, mutation: &BucketSubresourceMutation) {
    match mutation {
        BucketSubresourceMutation::Put { kind, body, aux } => {
            put_u8(out, 1);
            put_bucket_subresource_kind(out, *kind);
            put_string(out, body);
            put_bucket_subresource_aux(out, *aux);
        }
        BucketSubresourceMutation::Delete { kind } => {
            put_u8(out, 2);
            put_bucket_subresource_kind(out, *kind);
        }
    }
}

fn put_bucket_subresource_kind(out: &mut Vec<u8>, kind: BucketSubresourceKind) {
    put_u8(out, kind as u8);
}

fn put_bucket_subresource_aux(out: &mut Vec<u8>, aux: BucketSubresourceAux) {
    match aux {
        BucketSubresourceAux::None => put_u8(out, 0),
        BucketSubresourceAux::Policy { is_public } => {
            put_u8(out, 1);
            put_bool(out, is_public);
        }
    }
}

fn put_loaded_bucket_subresource(out: &mut Vec<u8>, subresource: &LoadedBucketSubresource<String>) {
    match subresource {
        LoadedBucketSubresource::NotRequested => put_u8(out, 0),
        LoadedBucketSubresource::Missing => put_u8(out, 1),
        LoadedBucketSubresource::Loaded(value) => {
            put_u8(out, 2);
            put_string(out, value);
        }
    }
}

fn put_direct_put_commit_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &DirectPutCommitStorageSnapshot,
) {
    put_optional_string(out, snapshot.auth_snapshot.existing_etag.as_deref());
    put_optional_stored_object(out, snapshot.current.as_ref());
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_optional_object_payload_reclaim(out, snapshot.stale_payload.as_ref());
}

fn put_optional_object_payload_reclaim(
    out: &mut Vec<u8>,
    reclaim: Option<&ObjectPayloadReclaimCommand>,
) {
    match reclaim {
        None => put_u8(out, 0),
        Some(reclaim) => {
            put_u8(out, 1);
            put_object_payload_reclaim(out, reclaim);
        }
    }
}

fn put_optional_payload_reclaim_root(out: &mut Vec<u8>, root: Option<&PayloadReclaimRoot>) {
    match root {
        Some(root) => {
            put_u8(out, 1);
            put_string(out, root.bucket.as_str());
            put_string(out, root.key.as_str());
            put_u64(out, root.generation_id.get());
        }
        None => put_u8(out, 0),
    }
}

fn put_object_payload_reclaim(out: &mut Vec<u8>, reclaim: &ObjectPayloadReclaimCommand) {
    match reclaim {
        ObjectPayloadReclaimCommand::Segments(reclaim) => {
            put_u8(out, 0);
            put_object_segments_reclaim_record(out, reclaim);
        }
        ObjectPayloadReclaimCommand::Multipart(reclaim) => {
            put_u8(out, 1);
            put_multipart_reclaim_record(out, reclaim);
        }
    }
}

fn put_object_segments_reclaim_record(out: &mut Vec<u8>, reclaim: &ObjectSegmentsReclaimRecord) {
    put_string(out, reclaim.bucket.as_str());
    put_string(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.segments.len() as u32);
    for segment in &reclaim.segments {
        put_u32(out, segment.segment_index);
        put_bytes(out, &segment.segment_okh);
        put_u64(out, segment.segment_vid.get());
        put_u32(out, segment.data_pg_id);
        put_ec_shape(out, segment.ec);
    }
}

fn put_multipart_reclaim_record(out: &mut Vec<u8>, reclaim: &MultipartReclaimRecord) {
    put_string(out, reclaim.bucket.as_str());
    put_string(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.parts.len() as u32);
    for part in &reclaim.parts {
        match part {
            MultipartReclaimPartRecord::ShardSet {
                part_number,
                part_okh,
                part_vid,
                data_pg_id,
                ec,
            } => {
                put_u8(out, 0);
                put_u32(out, *part_number);
                put_bytes(out, part_okh);
                put_u64(out, part_vid.get());
                put_u32(out, *data_pg_id);
                put_ec_shape(out, *ec);
            }
            MultipartReclaimPartRecord::Segments {
                part_number,
                segments,
            } => {
                put_u8(out, 1);
                put_u32(out, *part_number);
                put_u32(out, segments.len() as u32);
                for segment in segments {
                    put_u32(out, segment.segment_index);
                    put_bytes(out, &segment.segment_okh);
                    put_u64(out, segment.segment_vid.get());
                    put_u32(out, segment.data_pg_id);
                    put_ec_shape(out, segment.ec);
                }
            }
        }
    }
}

fn put_commit_direct_put_object_req(out: &mut Vec<u8>, request: &CommitDirectPutObjectReq) {
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_string(out, request.generation_reservation_id.as_str());
    put_u8(out, request.versioning as u8);
    put_owner_identity(out, &request.owner);
    put_string(out, &request.acl_grants.serialized());
    put_bool(out, request.public_read);
    put_u64(out, request.generation_id.get());
    put_u64(out, request.size);
    put_u64(out, request.etag_crc64);
    put_ec_shape(out, request.ec);
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, request.metadata_blob.as_slice());
    put_bytes(out, request.system_metadata_blob.as_slice());
    put_object_lock_state(out, request.object_lock);
    put_object_encryption(out, &request.encryption);
    put_u32(out, request.segment_index);
    put_u64(out, request.segment_crc64);
    put_bytes(out, &request.segment_okh);
    put_u64(out, request.segment_vid.get());
    put_u32(out, request.data_pg_id);
    put_bucket_write_reservation_proof(out, &request.bucket_write_reservation);
}

fn put_optional_stored_object(out: &mut Vec<u8>, stored: Option<&StoredObject>) {
    match stored {
        None => put_u8(out, 0),
        Some(stored) => {
            put_u8(out, 1);
            put_stored_object(out, stored);
        }
    }
}

fn put_optional_stored_object_list(out: &mut Vec<u8>, stored: Option<&[StoredObject]>) {
    match stored {
        None => put_u8(out, 0),
        Some(stored) => {
            put_u8(out, 1);
            put_stored_object_list(out, stored);
        }
    }
}

fn put_stored_object_list(out: &mut Vec<u8>, stored: &[StoredObject]) {
    put_u32(
        out,
        u32::try_from(stored.len()).expect("stored object list count must fit in u32"),
    );
    for stored in stored {
        put_stored_object(out, stored);
    }
}

fn put_stored_object(out: &mut Vec<u8>, stored: &StoredObject) {
    match stored {
        StoredObject::Live(record) => {
            put_u8(out, 0);
            put_live_object_record(out, record);
        }
        StoredObject::DeleteMarker(record) => {
            put_u8(out, 1);
            put_delete_marker_record(out, record);
        }
    }
}

fn put_put_object_metadata_mutation(out: &mut Vec<u8>, mutation: &PutObjectMetadataMutation) {
    match mutation {
        PutObjectMetadataMutation::PutTags(tags) => {
            put_u8(out, 0);
            put_string(out, tags);
        }
        PutObjectMetadataMutation::DeleteTags => put_u8(out, 1),
        PutObjectMetadataMutation::PutRetention(retention) => {
            put_u8(out, 2);
            put_u64(out, retention.retain_until_unix_seconds);
            put_u8(out, retention.mode as u8);
        }
        PutObjectMetadataMutation::PutLegalHold(legal_hold) => {
            put_u8(out, 3);
            put_u8(out, *legal_hold as u8);
        }
        PutObjectMetadataMutation::PutAcl {
            acl_grants,
            public_read,
        } => {
            put_u8(out, 4);
            put_string(out, &acl_grants.serialized());
            put_bool(out, *public_read);
        }
    }
}

fn put_insert_delete_marker_stale_payload(
    out: &mut Vec<u8>,
    stale_payload: &StorageRpcInsertDeleteMarkerStalePayload,
) {
    match stale_payload {
        StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim) => {
            put_u8(out, 0);
            put_optional_object_payload_reclaim(out, reclaim.as_ref());
        }
        StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
            put_u8(out, 1);
            put_u64(out, *created_at);
        }
    }
}

fn put_create_stream_upload_req(out: &mut Vec<u8>, request: &CreateStreamUploadReq) {
    put_string(out, request.session_id.as_str());
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_stream_upload_target(out, &request.target);
    put_object_encryption(out, &request.encryption);
}

fn put_prepare_stream_segment_append_req(
    out: &mut Vec<u8>,
    request: &PrepareStreamUploadSegmentAppendReq,
) {
    put_string(out, request.session_id.as_str());
    put_u32(out, request.segment_index);
    put_u64(out, request.size);
    put_u64(out, request.segment_crc64);
    put_u64(out, request.payload_crc64);
    put_bytes(out, &request.segment_okh);
}

fn put_stream_upload_target(out: &mut Vec<u8>, target: &StreamUploadTarget) {
    match target {
        StreamUploadTarget::PutObject => put_u8(out, 0),
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number,
        } => {
            put_u8(out, 1);
            put_string(out, upload_id.as_str());
            put_u32(out, *part_number);
        }
    }
}

fn put_create_stream_upload_precondition(
    out: &mut Vec<u8>,
    precondition: &StorageRpcCreateStreamUploadPrecondition,
) {
    match precondition {
        StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
            require_generation_reservation,
        } => {
            put_u8(out, 0);
            put_bool(out, *require_generation_reservation);
        }
        StorageRpcCreateStreamUploadPrecondition::PutObject {
            expected_current,
            require_generation_reservation,
        } => {
            put_u8(out, 1);
            put_optional_stored_object(out, expected_current.as_ref());
            put_bool(out, *require_generation_reservation);
        }
        StorageRpcCreateStreamUploadPrecondition::UploadPart { expected_upload } => {
            put_u8(out, 2);
            put_multipart_upload_record(out, expected_upload);
        }
    }
}

fn put_optional_create_stream_upload_command(
    out: &mut Vec<u8>,
    command: Option<&CreateStreamUploadCommand>,
) {
    match command {
        None => put_u8(out, 0),
        Some(command) => {
            put_u8(out, 1);
            put_create_stream_upload_command(out, command);
        }
    }
}

fn put_create_stream_upload_command(out: &mut Vec<u8>, command: &CreateStreamUploadCommand) {
    put_stream_upload_command_record(out, &command.session);
    put_u64(out, command.initial_next_segment_vid.get());
    put_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn put_stream_upload_command_record(
    out: &mut Vec<u8>,
    record: &crate::types::StreamUploadCommandRecord,
) {
    put_string(out, record.session_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_stream_upload_target(out, &record.target);
    put_u8(out, record.state as u8);
    put_u64(out, record.created_at);
    put_object_encryption(out, &record.encryption);
}

fn put_stream_upload_record(out: &mut Vec<u8>, record: &StreamUploadRecord) {
    put_string(out, record.session_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_stream_upload_target(out, &record.target);
    put_u8(out, record.state as u8);
    put_u64(out, record.created_at);
    put_object_encryption(out, &record.encryption);
    put_u64(out, record.next_segment_vid.get());
    put_optional_bucket_write_reservation_proof(out, record.bucket_write_reservation.as_ref());
}

fn put_optional_bucket_write_reservation_proof(
    out: &mut Vec<u8>,
    proof: Option<&BucketWriteReservationProof>,
) {
    match proof {
        Some(proof) => {
            put_bool(out, true);
            put_bucket_write_reservation_proof(out, proof);
        }
        None => put_bool(out, false),
    }
}

fn put_stream_upload_segment_record(out: &mut Vec<u8>, segment: &StreamUploadSegmentRecord) {
    put_string(out, segment.session_id.as_str());
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_u64(out, segment.payload_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_terminal_stream_cleanup_record(out: &mut Vec<u8>, stream: &TerminalStreamCleanupRecord) {
    put_string(out, stream.session_id.as_str());
    put_string(out, stream.bucket.as_str());
    put_string(out, stream.key.as_str());
    put_stream_upload_target(out, &stream.target);
    put_u8(out, stream.state as u8);
    put_u64(out, stream.created_at);
    put_object_encryption(out, &stream.encryption);
}

fn put_stream_put_finalize_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &StreamPutFinalizeStorageSnapshot,
) {
    put_stream_upload_record(out, &snapshot.session);
    put_optional_string(out, snapshot.existing_etag.as_deref());
    put_u64(out, snapshot.generation_id.get());
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_optional_object_payload_reclaim(out, snapshot.stale_payload.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.staging_segments.len())
            .expect("stream segment count must fit in u32"),
    );
    for segment in &snapshot.staging_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_stream_put_commit_input(out: &mut Vec<u8>, commit: &StreamPutCommitInput) {
    put_u8(out, commit.versioning as u8);
    put_u64(out, commit.version_id.to_u64());
    put_owner_identity(out, &commit.owner);
    put_string(out, &commit.acl_grants.serialized());
    put_bool(out, commit.public_read);
    put_u64(out, commit.size);
    put_u64(out, commit.etag_crc64);
    put_optional_string(out, commit.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, commit.metadata_blob.as_slice());
    put_bytes(out, commit.system_metadata_blob.as_slice());
    put_object_lock_state(out, commit.object_lock);
    put_object_encryption(out, &commit.encryption);
}

fn put_complete_multipart_commit_request(
    out: &mut Vec<u8>,
    request: &CompleteMultipartCommitRequest,
) {
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_string(out, request.upload_id.as_str());
    put_u8(out, request.versioning as u8);
    put_owner_identity(out, &request.owner);
    put_string(out, &request.acl_grants.serialized());
    put_bool(out, request.public_read);
    put_u64(out, request.generation_id.get());
    put_u64(out, request.size);
    put_bytes(out, &request.etag_crc64);
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_optional_bytes(
        out,
        request.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    put_optional_bytes(
        out,
        request
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    put_object_lock_state(out, request.object_lock);
    put_object_encryption(out, &request.encryption);
    put_optional_stored_object(out, request.expected_stale_payload_source.as_ref());
    put_u32(
        out,
        u32::try_from(request.part_records.len())
            .expect("complete multipart part count must fit in u32"),
    );
    for part in &request.part_records {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(request.selected_streaming_segments.len())
            .expect("complete multipart selected segment count must fit in u32"),
    );
    for segment in &request.selected_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_complete_multipart_commit_cleanup(out, &request.expected_cleanup);
}

fn put_complete_multipart_commit_cleanup(
    out: &mut Vec<u8>,
    cleanup: &CompleteMultipartCommitCleanup,
) {
    put_u32(
        out,
        u32::try_from(cleanup.omitted_parts.len())
            .expect("complete multipart omitted part count must fit in u32"),
    );
    for part in &cleanup.omitted_parts {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(cleanup.omitted_streaming_segments.len())
            .expect("complete multipart omitted segment count must fit in u32"),
    );
    for segment in &cleanup.omitted_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_uploads.len())
            .expect("complete multipart stream cleanup count must fit in u32"),
    );
    for stream in &cleanup.stream_uploads {
        put_terminal_stream_cleanup_record(out, stream);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_upload_segments.len())
            .expect("complete multipart stream segment cleanup count must fit in u32"),
    );
    for segment in &cleanup.stream_upload_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_optional_abort_multipart_upload_cleanup(
    out: &mut Vec<u8>,
    cleanup: Option<&AbortMultipartUploadCleanup>,
) {
    match cleanup {
        None => put_u8(out, 0),
        Some(cleanup) => {
            put_u8(out, 1);
            put_abort_multipart_upload_cleanup(out, cleanup);
        }
    }
}

fn put_abort_multipart_upload_cleanup(out: &mut Vec<u8>, cleanup: &AbortMultipartUploadCleanup) {
    put_multipart_upload_record(out, &cleanup.upload);
    put_u32(
        out,
        u32::try_from(cleanup.parts.len()).expect("abort multipart part count must fit in u32"),
    );
    for part in &cleanup.parts {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(cleanup.streaming_segments.len())
            .expect("abort multipart segment count must fit in u32"),
    );
    for segment in &cleanup.streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_uploads.len())
            .expect("abort multipart stream cleanup count must fit in u32"),
    );
    for stream in &cleanup.stream_uploads {
        put_terminal_stream_cleanup_record(out, stream);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_upload_segments.len())
            .expect("abort multipart stream segment cleanup count must fit in u32"),
    );
    for segment in &cleanup.stream_upload_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_stream_upload_part_snapshot(out: &mut Vec<u8>, snapshot: &StreamUploadPartSnapshot) {
    put_stream_upload_record(out, &snapshot.session);
    put_multipart_upload_record(out, &snapshot.upload);
    put_optional_u32(out, snapshot.existing_part_generation);
    put_u32(
        out,
        u32::try_from(snapshot.staging_segments.len())
            .expect("stream part staging segment count must fit in u32"),
    );
    for segment in &snapshot.staging_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_optional_multipart_part_record(out: &mut Vec<u8>, part: Option<&MultipartPartRecord>) {
    match part {
        None => put_u8(out, 0),
        Some(part) => {
            put_u8(out, 1);
            put_multipart_part_record(out, part);
        }
    }
}

fn put_multipart_part_record(out: &mut Vec<u8>, part: &MultipartPartRecord) {
    put_string(out, part.upload_id.as_str());
    put_u32(out, part.part_number);
    put_u32(out, part.generation);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u64(out, part.last_modified);
    put_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn put_stream_part_finalize_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &StreamUploadPartStorageSnapshot,
) {
    put_stream_upload_part_snapshot(out, &snapshot.auth_snapshot);
    put_optional_multipart_part_record(out, snapshot.existing_part.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.displaced_segments.len())
            .expect("stream part displaced segment count must fit in u32"),
    );
    for segment in &snapshot.displaced_segments {
        put_multipart_part_segment_record(out, segment);
    }
}

fn put_create_multipart_upload_req(out: &mut Vec<u8>, request: &CreateMultipartUploadReq) {
    put_string(out, request.upload_id.as_str());
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, request.metadata_blob.as_slice());
    put_bytes(out, request.system_metadata_blob.as_slice());
    put_optional_owner_identity(out, request.initiator.as_ref());
    put_owner_identity(out, &request.owner);
    put_string(out, &request.acl_grants.serialized());
    put_bool(out, request.public_read);
    put_object_lock_state(out, request.object_lock);
    put_optional_multipart_checksum_config(out, request.checksum);
    put_object_encryption(out, &request.encryption);
}

fn put_optional_owner_identity(out: &mut Vec<u8>, owner: Option<&OwnerIdentity>) {
    match owner {
        None => put_u8(out, 0),
        Some(owner) => {
            put_u8(out, 1);
            put_owner_identity(out, owner);
        }
    }
}

fn put_optional_multipart_checksum_config(
    out: &mut Vec<u8>,
    checksum: Option<MultipartChecksumConfig>,
) {
    match checksum {
        None => put_u8(out, 0),
        Some(checksum) => {
            put_u8(out, 1);
            put_u8(out, checksum.algorithm() as u8);
            put_u8(out, checksum.checksum_type() as u8);
        }
    }
}

fn put_optional_create_multipart_upload_command(
    out: &mut Vec<u8>,
    command: Option<&CreateMultipartUploadCommand>,
) {
    match command {
        None => put_u8(out, 0),
        Some(command) => {
            put_u8(out, 1);
            put_create_multipart_upload_command(out, command);
        }
    }
}

fn put_create_multipart_upload_command(out: &mut Vec<u8>, command: &CreateMultipartUploadCommand) {
    put_multipart_upload_record(out, &command.upload);
    put_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn put_multipart_upload_record(out: &mut Vec<u8>, record: &MultipartUploadRecord) {
    put_string(out, record.upload_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.initiated_at);
    put_u8(out, record.state as u8);
    put_optional_string(out, record.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, record.metadata_blob.as_slice());
    put_bytes(out, record.system_metadata_blob.as_slice());
    put_optional_owner_identity(out, record.initiator.as_ref());
    put_owner_identity(out, &record.owner);
    put_string(out, &record.acl_grants.serialized());
    put_bool(out, record.public_read);
    put_u64(out, record.object_generation_id.get());
    put_object_lock_state(out, record.object_lock);
    put_optional_multipart_checksum_config(out, record.checksum);
    put_object_encryption(out, &record.encryption);
}

fn put_completed_multipart_upload_record(
    out: &mut Vec<u8>,
    record: &CompletedMultipartUploadRecord,
) {
    put_string(out, record.upload_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.completion_order);
    put_u64(out, record.completed_at);
    put_optional_owner_identity(out, record.initiator.as_ref());
    put_owner_identity(out, &record.owner);
}

fn put_multipart_completion_snapshot(out: &mut Vec<u8>, snapshot: &MultipartCompletionSnapshot) {
    put_optional_string(out, snapshot.existing_etag.as_deref());
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.part_records.len())
            .expect("multipart completion snapshot part count must fit in u32"),
    );
    for part in &snapshot.part_records {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(snapshot.selected_streaming_segments.len())
            .expect("multipart completion snapshot segment count must fit in u32"),
    );
    for segment in &snapshot.selected_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_complete_multipart_commit_cleanup(out, &snapshot.cleanup);
}

fn put_list_parts_resp(out: &mut Vec<u8>, response: &ListPartsResp) {
    put_u32(
        out,
        u32::try_from(response.parts.len()).expect("multipart parts count must fit in u32"),
    );
    for part in &response.parts {
        put_multipart_part_record(out, part);
    }
    put_bool(out, response.is_truncated);
    put_optional_u32(out, response.next_part_number_marker);
}

fn put_listed_multipart_parts(out: &mut Vec<u8>, listed: &ListedMultipartParts) {
    put_multipart_upload_record(out, &listed.upload);
    put_list_parts_resp(out, &listed.response);
}

fn put_multipart_upload_management_lookup(
    out: &mut Vec<u8>,
    lookup: &MultipartUploadManagementLookup,
) {
    match lookup {
        MultipartUploadManagementLookup::InProgress(upload) => {
            put_u8(out, 0);
            put_multipart_upload_record(out, upload);
        }
        MultipartUploadManagementLookup::NonInProgress(upload) => {
            put_u8(out, 1);
            put_multipart_upload_record(out, upload);
        }
        MultipartUploadManagementLookup::Completed(completed) => {
            put_u8(out, 2);
            put_completed_multipart_upload_record(out, completed);
        }
        MultipartUploadManagementLookup::Missing => put_u8(out, 3),
    }
}

fn put_optional_delete_object_version_target(
    out: &mut Vec<u8>,
    target: Option<&DeleteObjectVersionTarget>,
) {
    match target {
        None => put_u8(out, 0),
        Some(target) => {
            put_u8(out, 1);
            put_delete_object_version_target(out, target);
        }
    }
}

fn put_delete_object_version_target(out: &mut Vec<u8>, target: &DeleteObjectVersionTarget) {
    match target {
        DeleteObjectVersionTarget::DeleteMarker { write_sequence } => {
            put_u8(out, 0);
            put_u64(out, *write_sequence);
        }
        DeleteObjectVersionTarget::Live {
            generation_id,
            layout,
            payload,
        } => {
            put_u8(out, 1);
            put_u64(out, generation_id.get());
            put_object_layout(out, *layout);
            put_object_payload_reclaim(out, payload);
        }
    }
}

fn put_metadata_command_envelope_response_item(
    out: &mut Vec<u8>,
    command: &crate::metadata_command::MetadataCommandEnvelope,
) {
    put_u64(out, command.checksum_crc64());
    put_bytes(out, &command.command_bytes());
}

fn put_object_read_auth_subject(out: &mut Vec<u8>, subject: &ObjectReadAuthSubject) {
    put_stored_object(out, &subject.stored);
}

fn put_object_read_snapshot(out: &mut Vec<u8>, snapshot: &ObjectReadSnapshot) {
    put_stored_object(out, &snapshot.stored);
    put_u32(
        out,
        u32::try_from(snapshot.object_segments.len())
            .expect("object segment count must fit in u32"),
    );
    for segment in &snapshot.object_segments {
        put_object_segment_record(out, segment);
    }
    put_u32(
        out,
        u32::try_from(snapshot.multipart_parts.len()).expect("object part count must fit in u32"),
    );
    for part in &snapshot.multipart_parts {
        put_object_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(snapshot.multipart_part_segments.len())
            .expect("multipart part segment count must fit in u32"),
    );
    for segment in &snapshot.multipart_part_segments {
        put_multipart_part_segment_record(out, segment);
    }
}

fn put_object_segment_record(out: &mut Vec<u8>, segment: &ObjectSegmentRecord) {
    put_string(out, segment.bucket.as_str());
    put_string(out, segment.key.as_str());
    put_u64(out, segment.version_id.to_u64());
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_object_part_record(out: &mut Vec<u8>, part: &ObjectPartRecord) {
    put_string(out, part.bucket.as_str());
    put_string(out, part.key.as_str());
    put_u64(out, part.version_id.to_u64());
    put_u32(out, part.part_number);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u32(out, part.data_pg_id);
    put_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn put_multipart_part_segment_record(out: &mut Vec<u8>, segment: &MultipartPartSegmentRecord) {
    put_string(out, segment.bucket.as_str());
    put_string(out, segment.key.as_str());
    put_string(out, segment.upload_id.as_str());
    put_u64(out, segment.version_id);
    put_u32(out, segment.part_number);
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_optional_version_id(out: &mut Vec<u8>, version_id: Option<VersionId>) {
    match version_id {
        None => put_u8(out, 0),
        Some(version_id) => {
            put_u8(out, 1);
            put_u64(out, version_id.to_u64());
        }
    }
}

fn put_object_read_snapshot_mode(out: &mut Vec<u8>, mode: ObjectReadSnapshotMode) {
    put_u8(
        out,
        match mode {
            ObjectReadSnapshotMode::MetadataOnly => 0,
            ObjectReadSnapshotMode::StandardSegments => 1,
            ObjectReadSnapshotMode::MultipartParts => 2,
            ObjectReadSnapshotMode::FullPayloadLayout => 3,
        },
    );
}

fn put_live_object_record(out: &mut Vec<u8>, record: &LiveObjectRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.version_id.to_u64());
    put_owner_identity(out, &record.owner);
    put_string(out, &record.acl_grants.serialized());
    put_bool(out, record.public_read);
    put_u64(out, record.generation_id.get());
    put_u64(out, record.size);
    put_object_etag(out, record.etag);
    put_u64(out, record.last_modified);
    put_optional_u64(out, record.became_noncurrent_at);
    put_u8(out, record.storage_class as u8);
    put_ec_shape(out, record.ec);
    put_object_layout(out, record.layout);
    put_optional_string(out, record.tags.as_ref().map(|tags| tags.as_str()));
    put_optional_bytes(
        out,
        record.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    put_optional_bytes(
        out,
        record
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    put_object_lock_state(out, record.object_lock);
    put_object_encryption(out, &record.encryption);
}

fn put_delete_marker_record(out: &mut Vec<u8>, record: &DeleteMarkerRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.version_id.to_u64());
    put_owner_identity(out, &record.owner);
    put_u64(out, record.last_modified);
}

fn put_owner_identity(out: &mut Vec<u8>, owner: &OwnerIdentity) {
    put_string(out, &owner.principal);
    put_string(out, owner.canonical_id.as_str());
}

fn put_ec_shape(out: &mut Vec<u8>, ec: EcShape) {
    put_u8(out, ec.k);
    put_u8(out, ec.m);
}

fn put_scavenger_observation_key(out: &mut Vec<u8>, key: &ShardScavengerObservationKey) {
    put_u32(out, key.node_id);
    put_u32(out, key.data_pg_id);
    put_u8(out, key.shard_index.get());
    put_bytes(out, key.shard_key.as_bytes());
}

fn put_scavenger_observation_record(
    out: &mut Vec<u8>,
    observation: &ShardScavengerObservationRecord,
) {
    put_scavenger_observation_key(out, &observation.key);
    put_optional_u64(out, observation.data_size);
    put_optional_u64(out, observation.crc64);
    put_bool(out, observation.file_exists);
    put_bool(out, observation.shard_row_exists);
    put_u8(out, observation.reason as u8);
    put_optional_string(out, observation.last_error.as_deref());
}

fn put_scavenger_observation(out: &mut Vec<u8>, observation: &ShardScavengerObservation) {
    put_scavenger_observation_key(out, &observation.key);
    put_u64(out, observation.first_seen_at);
    put_u64(out, observation.last_seen_at);
    put_u64(out, observation.observation_count);
    put_optional_u64(out, observation.data_size);
    put_optional_u64(out, observation.crc64);
    put_bool(out, observation.file_exists);
    put_bool(out, observation.shard_row_exists);
    put_u8(out, observation.reason as u8);
    put_optional_string(out, observation.last_error.as_deref());
    put_optional_u64(out, observation.resolved_at);
}

fn put_segment_stored_bytes_request(out: &mut Vec<u8>, request: &SegmentStoredBytesRequest) {
    put_u32(out, request.data_pg_id);
    out.extend_from_slice(&request.segment_okh);
    put_u64(out, request.segment_vid.get());
    put_u64(out, request.stored_size as u64);
    put_u64(out, request.segment_crc64);
    put_ec_shape(out, request.ec);
}

fn put_placed_segment_shard_repair_work_item(
    out: &mut Vec<u8>,
    work_item: &PlacedSegmentShardRepairWorkItem,
) {
    put_segment_stored_bytes_request(out, &work_item.request);
    put_u8(out, work_item.shard_index.get());
}

fn put_placed_segment_shard_repair_record(
    out: &mut Vec<u8>,
    repair: &PlacedSegmentShardRepairRecord,
) {
    put_placed_segment_shard_repair_work_item(out, &repair.work_item);
    put_u64(out, repair.first_seen_at);
    put_u64(out, repair.last_seen_at);
    put_u64(out, repair.observation_count);
    put_optional_string(out, repair.last_error.as_deref());
}

fn put_placed_segment_shard_repair_claim_record(
    out: &mut Vec<u8>,
    claim: &PlacedSegmentShardRepairClaimRecord,
) {
    put_placed_segment_shard_repair_work_item(out, &claim.work_item);
    put_string(out, &claim.claim_id);
    put_string(out, &claim.owner_token);
    put_u64(out, claim.cluster_epoch.get());
    put_u64(out, claim.claimed_at);
    put_optional_u64(out, claim.lease_deadline);
    put_u64(out, claim.attempt_count);
    put_optional_string(out, claim.last_error.as_deref());
}

fn put_placed_segment_shard_backfill_work_item(
    out: &mut Vec<u8>,
    work_item: &PlacedSegmentShardBackfillWorkItem,
) {
    put_segment_stored_bytes_request(out, &work_item.request);
    put_u64(out, work_item.source_cluster_epoch.get());
    put_u64(out, work_item.desired_cluster_epoch.get());
}

fn put_placed_segment_shard_backfill_record(
    out: &mut Vec<u8>,
    backfill: &PlacedSegmentShardBackfillRecord,
) {
    put_placed_segment_shard_backfill_work_item(out, &backfill.work_item);
    put_u8(out, backfill.remaining_tolerance);
    put_u64(out, backfill.first_seen_at);
    put_u64(out, backfill.last_seen_at);
    put_u64(out, backfill.observation_count);
    put_optional_string(out, backfill.last_error.as_deref());
}

fn put_placed_segment_shard_backfill_claim_record(
    out: &mut Vec<u8>,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) {
    put_placed_segment_shard_backfill_work_item(out, &claim.work_item);
    put_u8(out, claim.remaining_tolerance);
    put_string(out, &claim.claim_id);
    put_string(out, &claim.owner_token);
    put_u64(out, claim.cluster_epoch.get());
    put_u64(out, claim.claimed_at);
    put_optional_u64(out, claim.lease_deadline);
    put_u64(out, claim.attempt_count);
    put_optional_string(out, claim.last_error.as_deref());
}

fn put_scavenger_payload_reference(out: &mut Vec<u8>, reference: &ShardScavengerPayloadReference) {
    match reference {
        ShardScavengerPayloadReference::Placed(reference) => {
            put_u8(out, 0);
            put_u32(out, reference.data_pg_id);
            out.extend_from_slice(&reference.okh);
            put_u64(out, reference.generation_id.get());
            put_u64(out, reference.placement_cluster_epoch.get());
            put_u64(out, reference.stored_size);
            put_u64(out, reference.crc64);
            put_ec_shape(out, reference.ec);
        }
        ShardScavengerPayloadReference::RoutedMultipartPart(reference) => {
            put_u8(out, 1);
            put_string(out, reference.bucket.as_str());
            put_string(out, reference.key.as_str());
            put_u64(out, reference.object_generation_id.get());
            put_u32(out, reference.part_number);
            put_u64(out, reference.stored_size);
            put_u64(out, reference.crc64);
            out.extend_from_slice(&reference.part_okh);
            put_u64(out, reference.part_vid.get());
            put_u64(out, reference.placement_cluster_epoch.get());
            put_ec_shape(out, reference.ec);
        }
        ShardScavengerPayloadReference::ReclaimOnly(reference) => {
            put_u8(out, 2);
            put_u32(out, reference.data_pg_id);
            out.extend_from_slice(&reference.okh);
            put_u64(out, reference.generation_id.get());
            put_ec_shape(out, reference.ec);
        }
    }
}

fn put_object_etag(out: &mut Vec<u8>, etag: ObjectEtag) {
    match etag {
        ObjectEtag::SinglePart(crc64) => {
            put_u8(out, 0);
            out.extend_from_slice(&crc64);
        }
        ObjectEtag::MultipartComposite { crc64, parts } => {
            put_u8(out, 1);
            out.extend_from_slice(&crc64);
            put_u32(out, parts.get());
        }
    }
}

fn put_object_layout(out: &mut Vec<u8>, layout: ObjectLayout) {
    match layout {
        ObjectLayout::Standard => put_u8(out, 0),
        ObjectLayout::MultipartManifest { parts_count } => {
            put_u8(out, 1);
            put_u32(out, parts_count.get());
        }
    }
}

fn put_object_lock_state(out: &mut Vec<u8>, object_lock: ObjectLockState) {
    match object_lock.retention {
        None => put_u8(out, 0),
        Some(retention) => {
            put_u8(out, 1);
            put_u64(out, retention.retain_until_unix_seconds);
            put_u8(out, retention.mode as u8);
        }
    }
    put_u8(out, object_lock.legal_hold as u8);
}

fn put_object_encryption(out: &mut Vec<u8>, encryption: &ObjectEncryption) {
    put_u8(out, encryption.encryption_type() as u8);
    put_optional_bytes(out, encryption.encode_state().as_deref());
}

fn put_bucket_object_lock_config(out: &mut Vec<u8>, config: &BucketObjectLockConfig) {
    put_bool(out, config.enabled);
    match config.default_retention {
        None => put_u8(out, 0),
        Some(retention) => {
            put_u8(out, 1);
            put_u8(out, retention.mode as u8);
            match retention.period {
                RetentionPeriod::Days(days) => {
                    put_u32(out, days.get());
                    put_u8(out, 0);
                }
                RetentionPeriod::Years(years) => {
                    put_u32(out, years.get());
                    put_u8(out, 1);
                }
            }
        }
    }
}

fn put_optional_public_access_block_config(
    out: &mut Vec<u8>,
    config: Option<PublicAccessBlockConfig>,
) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_bool(out, config.block_public_acls);
            put_bool(out, config.ignore_public_acls);
            put_bool(out, config.block_public_policy);
            put_bool(out, config.restrict_public_buckets);
        }
    }
}

fn put_optional_bucket_ownership_controls(
    out: &mut Vec<u8>,
    controls: Option<BucketOwnershipControls>,
) {
    match controls {
        None => put_u8(out, 0),
        Some(controls) => {
            put_u8(out, 1);
            put_u8(out, controls.object_ownership as u8);
        }
    }
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    put_u8(out, u8::from(value));
}

fn put_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
    }
}

fn put_optional_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u32(out, value);
        }
    }
}

fn put_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_string(out, value);
        }
    }
}

fn put_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_bytes(out, value);
        }
    }
}

fn read_u16_from<R: Read>(reader: &mut R) -> Result<u16, std::io::Error> {
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32_from<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_from<R: Read>(reader: &mut R) -> Result<u64, std::io::Error> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::{
        metadata_command::{
            AdvanceCompletedMultipartUploadSequenceCommand, CreateBucketCommand,
            MarkBucketDeletingCommand, MetadataCommandEnvelope, MetadataCommandId,
            MetadataCommandLogIndex, MetadataCommandPayload,
        },
        types::{
            AclGrants, BucketDeleteAttemptOutcomeKind, BucketDeleteAttemptOutcomeRecord,
            BucketDeleteFinalizeClaimRecord, BucketObjectLockConfig, BucketVersioningState,
            BucketWriteDrainRecord, BucketWriteDrainState, CanonicalUserId, ClusterEpoch,
            CreateBucketConfig, GenerationId, ObjectKey, PgId, SessionId,
        },
    };

    #[test]
    fn storage_rpc_frame_round_trips() {
        let payload = b"hello rpc".to_vec();
        let bytes = encode_storage_rpc_frame(7, StorageRpcMessageKind::Health, &payload).unwrap();

        let decoded = decode_storage_rpc_frame(&bytes).unwrap();

        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.kind, StorageRpcMessageKind::Health);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn storage_rpc_frame_encoding_is_stable() {
        let payload = b"abc";
        let bytes = encode_storage_rpc_frame(
            0x0102_0304_0506_0708,
            StorageRpcMessageKind::ShardWrite,
            payload,
        )
        .unwrap();
        let expected_checksum = storage_rpc_frame_checksum(
            STORAGE_RPC_FRAME_ENCODING_VERSION,
            0x0102_0304_0506_0708,
            StorageRpcMessageKind::ShardWrite as u16,
            3,
            payload,
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(&24u32.to_le_bytes());
        expected.extend_from_slice(STORAGE_RPC_FRAME_MAGIC);
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        expected.extend_from_slice(&(StorageRpcMessageKind::ShardWrite as u16).to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(&expected_checksum.to_le_bytes());
        expected.extend_from_slice(payload);

        assert_eq!(bytes, expected);
    }

    #[test]
    fn storage_rpc_frame_rejects_trailing_bytes() {
        let mut bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, b"ok").unwrap();
        bytes.push(0);

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::TrailingBytes)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_unknown_message_kind() {
        let payload = b"ok";
        let mut bytes = Vec::new();
        put_bytes(&mut bytes, STORAGE_RPC_FRAME_MAGIC);
        put_u16(&mut bytes, STORAGE_RPC_FRAME_ENCODING_VERSION);
        put_u64(&mut bytes, 1);
        put_u16(&mut bytes, 999);
        put_u32(&mut bytes, payload.len() as u32);
        put_u64(
            &mut bytes,
            storage_rpc_frame_checksum(
                STORAGE_RPC_FRAME_ENCODING_VERSION,
                1,
                999,
                payload.len() as u32,
                payload,
            ),
        );
        bytes.extend_from_slice(payload);

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::UnknownMessageKind(999))
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_valid_kind_flip() {
        let payload = b"ok";
        let mut bytes =
            encode_storage_rpc_frame(1, StorageRpcMessageKind::ShardRead, payload).unwrap();
        let kind_offset = 4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8;
        bytes[kind_offset..kind_offset + 2]
            .copy_from_slice(&(StorageRpcMessageKind::ShardDelete as u16).to_le_bytes());

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::PayloadChecksumMismatch)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_bad_payload_checksum_before_payload_decode() {
        let mut bytes =
            encode_storage_rpc_frame(1, StorageRpcMessageKind::ShardWrite, b"payload").unwrap();
        let checksum_offset = 4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8 + 2 + 4;
        bytes[checksum_offset] ^= 0x55;

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::PayloadChecksumMismatch)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_oversized_payload_on_encode_and_decode() {
        assert_eq!(
            encode_storage_rpc_frame_with_limit(1, StorageRpcMessageKind::Health, b"abcd", 3),
            Err(StorageRpcFrameError::PayloadTooLarge { len: 4, limit: 3 })
        );

        let bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, b"abcd").unwrap();
        assert_eq!(
            decode_storage_rpc_frame_with_limit(&bytes, 3),
            Err(StorageRpcFrameError::PayloadTooLarge { len: 4, limit: 3 })
        );
    }

    #[test]
    fn payload_record_count_guards_include_placement_epoch() {
        for (min_record_len, old_record_len, message) in [
            (
                STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN,
                4 + 4 + 8 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "object segment count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_OBJECT_PART_RECORD_LEN,
                4 + 4 + 8 + 4 + 8 + 8 + 4 + 1 + 4 + 16 + 8 + 2 + 4 + 1,
                "object part count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
                4 + SESSION_ID_LEN + 4 + 8 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "stream upload segment count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
                4 + UPLOAD_ID_LEN + 4 + 4 + 8 + 8 + 4 + 1 + 16 + 8 + 2 + 8 + 1,
                "multipart part count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
                4 + 4 + 4 + UPLOAD_ID_LEN + 8 + 4 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "multipart part segment count exceeds payload",
            ),
        ] {
            assert_eq!(min_record_len, old_record_len + 8);

            let mut bytes = Vec::new();
            put_u32(&mut bytes, 1);
            bytes.resize(bytes.len() + old_record_len, 0);

            let mut decoder = StorageRpcDecoder::new(&bytes);
            assert_eq!(
                decoder.read_bounded_remaining_count(min_record_len, message),
                Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    message
                ))
            );
        }
    }

    #[test]
    fn storage_rpc_stream_frame_round_trips() {
        let frame = StorageRpcFrame {
            request_id: 11,
            kind: StorageRpcMessageKind::Health,
            payload: b"stream".to_vec(),
        };
        let mut bytes = Vec::new();
        write_storage_rpc_frame_to(&mut bytes, &frame).unwrap();

        let decoded = read_storage_rpc_frame_from(&mut Cursor::new(bytes)).unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn storage_rpc_stream_frame_rejects_oversized_payload_before_allocating() {
        let payload = b"abcd";
        let bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, payload).unwrap();

        let err = read_storage_rpc_frame_from_with_limit(&mut Cursor::new(bytes), 3).unwrap_err();

        assert!(matches!(
            err,
            StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge {
                len: 4,
                limit: 3
            })
        ));
    }

    #[test]
    fn storage_rpc_response_payload_round_trips_success_and_error() {
        let success = encode_storage_rpc_success_response(b"ok");
        assert_eq!(
            decode_storage_rpc_response_payload(&success).unwrap(),
            Ok(b"ok".to_vec())
        );

        let error = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnknownPg,
            message: "unknown PG 9".to_string(),
        };
        let error_bytes = encode_storage_rpc_error_response(&error).unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&error_bytes).unwrap(),
            Err(error)
        );
    }

    #[test]
    fn storage_rpc_health_response_round_trips() {
        let response = StorageRpcHealthResponse {
            protocol_version: STORAGE_RPC_FRAME_ENCODING_VERSION,
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        };

        let bytes = encode_health_response(&response);

        assert_eq!(decode_health_response(&bytes).unwrap(), response);
    }

    #[test]
    fn metadata_command_item_rejects_stale_checksum() {
        let command = test_metadata_command();
        let command_bytes = command.command_bytes();
        let item = StorageRpcMetadataCommandItem {
            command_checksum: command.checksum_crc64(),
            command_bytes,
        };
        let mut bytes = encode_metadata_command_item(&item).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 0x01;

        assert_eq!(
            decode_metadata_command_item(&bytes),
            Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch)
        );
    }

    #[test]
    fn metadata_command_item_rejects_non_canonical_bytes_with_matching_crc() {
        let command_bytes = b"metadata command".to_vec();
        let item = StorageRpcMetadataCommandItem {
            command_checksum: checksum::crc64::checksum(&command_bytes),
            command_bytes,
        };

        assert_eq!(
            encode_metadata_command_item(&item),
            Err(StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
        );
    }

    #[test]
    fn metadata_command_item_rejects_oversized_command_before_allocating() {
        let mut bytes = Vec::new();
        put_u64(&mut bytes, 0);
        put_u32(
            &mut bytes,
            u32::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1).unwrap(),
        );

        assert_eq!(
            decode_metadata_command_item(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1,
                limit: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN,
            })
        );
    }

    #[test]
    fn command_envelope_response_rejects_oversized_command_before_allocating() {
        let mut bytes = Vec::new();
        put_u8(&mut bytes, 1);
        put_u32(
            &mut bytes,
            u32::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1).unwrap(),
        );

        assert!(matches!(
            decode_create_bucket_command_build_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1
                && limit == STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN
        ));
    }

    #[test]
    fn metadata_command_request_carries_route_and_command_identity() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
        };

        let bytes = encode_metadata_command_request(&request).unwrap();
        let decoded = decode_metadata_command_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_request_rejects_route_command_mismatch() {
        let command = test_metadata_command();
        let wrong_pg = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: PgId::new(command.id().pg_id().get() + 1),
            command: command.clone(),
        };
        assert!(matches!(
            encode_metadata_command_request(&wrong_pg),
            Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(_))
        ));

        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command,
        };
        let mut bytes = encode_metadata_command_request(&request).unwrap();
        bytes[12..16].copy_from_slice(&(request.pg_id.get() + 1).to_le_bytes());

        assert!(matches!(
            decode_metadata_command_request(&bytes),
            Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(_))
        ));
    }

    #[test]
    fn metadata_command_pending_slot_request_carries_scope_bucket() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
            scope_bucket: Some(BucketName::try_from("pending-scope").unwrap()),
        };

        let bytes = encode_metadata_command_pending_slot_request(&request).unwrap();
        let decoded = decode_metadata_command_pending_slot_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_pending_slot_replace_request_round_trips() {
        let previous = test_metadata_command();
        let replacement = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                previous.id().cluster_epoch(),
                previous.id().pg_id(),
                MetadataCommandLogIndex::new(previous.id().log_index().get() + 1).unwrap(),
            ),
            previous.payload().clone(),
        );
        let request = StorageRpcMetadataCommandPendingSlotReplaceRequest {
            node_id: NodeId::new(7),
            cluster_epoch: previous.id().cluster_epoch(),
            pg_id: previous.id().pg_id(),
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: Some(previous.bucket_name().clone()),
        };

        let bytes = encode_metadata_command_pending_slot_replace_request(&request).unwrap();
        let decoded = decode_metadata_command_pending_slot_replace_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.previous.command_bytes(), previous.command_bytes());
        assert_eq!(
            decoded.replacement.command_bytes(),
            replacement.command_bytes()
        );
    }

    #[test]
    fn metadata_command_pending_slot_insert_response_round_trips_conflict() {
        let response = StorageRpcMetadataCommandPendingSlotInsertResponse {
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id: 9,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                existing_log_index: 7,
                candidate_log_index: 8,
            },
        };

        let bytes = encode_metadata_command_pending_slot_insert_response(&response);
        let decoded = decode_metadata_command_pending_slot_insert_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_pending_slot_insert_response_round_trips_log_conflict() {
        let response = StorageRpcMetadataCommandPendingSlotInsertResponse {
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 9,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 11,
            },
        };

        let bytes = encode_metadata_command_pending_slot_insert_response(&response);
        let decoded = decode_metadata_command_pending_slot_insert_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_pending_slot_remove_response_round_trips() {
        for removed in [false, true] {
            let response = StorageRpcMetadataCommandPendingSlotRemoveResponse { removed };

            let bytes = encode_metadata_command_pending_slot_remove_response(&response);
            let decoded = decode_metadata_command_pending_slot_remove_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_next_id_request_and_response_round_trip() {
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            min_log_index: 9,
        };

        let bytes = encode_metadata_command_next_id_request(&request);
        let decoded = decode_metadata_command_next_id_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let response = StorageRpcMetadataCommandNextIdResponse {
            outcome: StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                log_index: 10,
            },
        };

        let bytes = encode_metadata_command_next_id_response(&response);
        let decoded = decode_metadata_command_next_id_response(&bytes).unwrap();

        assert_eq!(decoded, response);

        let conflict = StorageRpcMetadataCommandNextIdResponse {
            outcome: StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 11,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 12,
            },
        };

        let bytes = encode_metadata_command_next_id_response(&conflict);
        let decoded = decode_metadata_command_next_id_response(&bytes).unwrap();

        assert_eq!(decoded, conflict);
    }

    #[test]
    fn metadata_command_transfer_adopt_request_round_trips() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandTransferAdoptRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            expected_state_digest: 1234,
            commands: vec![MetadataTransferCommand {
                command: command.clone(),
                pre_state_digest: 4321,
                post_state_digest: 1234,
            }],
        };

        let bytes = encode_metadata_command_transfer_adopt_request(&request).unwrap();
        let decoded = decode_metadata_command_transfer_adopt_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            decoded.commands[0].command.command_bytes(),
            command.command_bytes()
        );
        assert_eq!(decoded.commands[0].pre_state_digest, 4321);
        assert_eq!(decoded.commands[0].post_state_digest, 1234);
    }

    #[test]
    fn metadata_command_transfer_empty_state_request_round_trips() {
        let request = StorageRpcMetadataCommandTransferEmptyStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            expected_state_digest: 1234,
        };

        let bytes = encode_metadata_command_transfer_empty_state_request(&request);
        let decoded = decode_metadata_command_transfer_empty_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_transfer_matching_state_request_round_trips() {
        let request = StorageRpcMetadataCommandTransferMatchingStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            applied_log_index: 4,
            applied_log_hash: 5678,
            expected_state_digest: 1234,
        };

        let bytes = encode_metadata_command_transfer_matching_state_request(&request);
        let decoded = decode_metadata_command_transfer_matching_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_transfer_checkpoint_base_request_round_trips() {
        let checkpoint = MetadataCommandCheckpoint {
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            applied_log_index: 7,
            applied_log_hash: 0x1234,
            state_digest: 0x5678,
            canonical_state_encoding_version: 1,
            table_digests: vec![MetadataCheckpointTableDigest {
                table_name: "buckets".to_string(),
                row_count: 1,
                row_hash_xor: 0x11,
                row_hash_sum: 0x11,
                table_digest: 0x22,
            }],
            table_blocks: vec![MetadataCheckpointTableBlock {
                table_name: "buckets".to_string(),
                columns: vec![
                    "name".to_string(),
                    "created_at".to_string(),
                    "ratio".to_string(),
                    "body".to_string(),
                    "missing".to_string(),
                ],
                order_columns: vec!["name".to_string()],
                filter: "all_rows".to_string(),
                rows: vec![MetadataCheckpointRow {
                    values: vec![
                        MetadataCheckpointValue::Text(b"bucket".to_vec()),
                        MetadataCheckpointValue::Integer(-7),
                        MetadataCheckpointValue::RealBits(1.25f64.to_bits()),
                        MetadataCheckpointValue::Blob(vec![1, 2, 3]),
                        MetadataCheckpointValue::Null,
                    ],
                    row_digest: 0x33,
                }],
                row_count: 1,
                row_hash_xor: 0x33,
                row_hash_sum: 0x33,
                table_digest: 0x44,
            }],
            checkpoint_crc64: 0x99,
        };
        let request = StorageRpcMetadataCommandTransferCheckpointBaseRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(4).unwrap(),
            pg_id: PgId::new(11),
            checkpoint,
        };

        let bytes = encode_metadata_command_transfer_checkpoint_base_request(&request).unwrap();
        let decoded = decode_metadata_command_transfer_checkpoint_base_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let response = StorageRpcMetadataCommandCheckpointResponse {
            checkpoint: request.checkpoint.clone(),
        };
        let bytes = encode_metadata_command_checkpoint_response(&response).unwrap();
        let decoded = decode_metadata_command_checkpoint_response(&bytes).unwrap();

        assert_eq!(decoded, response);

        let bytes = encode_metadata_command_checkpoint_payload(&request.checkpoint).unwrap();
        let decoded = decode_metadata_command_checkpoint_payload(&bytes).unwrap();

        assert_eq!(decoded, request.checkpoint);

        let candidates_request = StorageRpcMetadataCommandCheckpointCandidatesRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            max_applied_log_index: 99,
            limit: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u32,
        };
        let bytes = encode_metadata_command_checkpoint_candidates_request(&candidates_request);
        let decoded = decode_metadata_command_checkpoint_candidates_request(&bytes).unwrap();

        assert_eq!(decoded, candidates_request);

        let candidates_response = StorageRpcMetadataCommandCheckpointCandidatesResponse {
            checkpoints: vec![request.checkpoint],
        };
        let bytes =
            encode_metadata_command_checkpoint_candidates_response(&candidates_response).unwrap();
        let decoded = decode_metadata_command_checkpoint_candidates_response(&bytes).unwrap();

        assert_eq!(decoded, candidates_response);

        for status in [
            MetadataCommandLogCompactionStatus::NoCheckpoint {
                retained_entries: 3,
            },
            MetadataCommandLogCompactionStatus::PendingCommand {
                retained_entries: 4,
            },
            MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 5,
                compacted_before: 6,
            },
        ] {
            let response = StorageRpcMetadataCommandLogCompactResponse { status };
            let bytes = encode_metadata_command_log_compact_response(&response);
            let decoded = decode_metadata_command_log_compact_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }

        let history_request = StorageRpcClusterMapHistoryReferenceSummaryRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        };
        let bytes = encode_cluster_map_history_reference_summary_request(&history_request);
        let decoded = decode_cluster_map_history_reference_summary_request(&bytes).unwrap();
        assert_eq!(decoded, history_request);

        let history_response = StorageRpcClusterMapHistoryReferenceSummaryResponse {
            summary: PgClusterMapHistoryReferenceSummary {
                oldest_live_placement_epoch: Some(ClusterEpoch::new(2).unwrap()),
                oldest_durable_backfill_epoch: Some(ClusterEpoch::new(5).unwrap()),
            },
        };
        let bytes = encode_cluster_map_history_reference_summary_response(&history_response);
        let decoded = decode_cluster_map_history_reference_summary_response(&bytes).unwrap();
        assert_eq!(decoded, history_response);

        let empty_history_response = StorageRpcClusterMapHistoryReferenceSummaryResponse {
            summary: PgClusterMapHistoryReferenceSummary::default(),
        };
        let bytes = encode_cluster_map_history_reference_summary_response(&empty_history_response);
        let decoded = decode_cluster_map_history_reference_summary_response(&bytes).unwrap();
        assert_eq!(decoded, empty_history_response);

        assert!(matches!(
            decode_metadata_command_log_compact_response(&[99]),
            Err(StorageRpcPayloadError::InvalidMetadataCommandLogCompactionStatus(99))
        ));
    }

    #[test]
    fn metadata_command_max_log_index_response_round_trips() {
        let response = StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index: 42 };

        let bytes = encode_metadata_command_max_log_index_response(&response);
        let decoded = decode_metadata_command_max_log_index_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_hash_range_request_round_trips() {
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            first_log_index: MetadataCommandLogIndex::new(2).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(5).unwrap(),
        };

        let bytes = encode_metadata_command_log_hash_range_request(&request);
        let decoded = decode_metadata_command_log_hash_range_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_log_hash_range_request_rejects_invalid_ranges() {
        let mut bytes = encode_metadata_command_log_hash_range_request(
            &StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                first_log_index: MetadataCommandLogIndex::new(5).unwrap(),
                last_log_index: MetadataCommandLogIndex::new(4).unwrap(),
            },
        );
        assert!(matches!(
            decode_metadata_command_log_hash_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range must be ordered"
            ))
        ));

        bytes = encode_metadata_command_log_hash_range_request(
            &StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                last_log_index: MetadataCommandLogIndex::new(
                    STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES + 1,
                )
                .unwrap(),
            },
        );
        assert!(matches!(
            decode_metadata_command_log_hash_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range is too large"
            ))
        ));
    }

    #[test]
    fn metadata_command_log_hash_range_response_round_trips() {
        let response = StorageRpcMetadataCommandLogHashRangeResponse {
            entries: vec![
                MetadataCommandLogHashRangeEntry {
                    log_index: 2,
                    previous_log_hash: 0x11,
                    log_hash: 0x22,
                },
                MetadataCommandLogHashRangeEntry {
                    log_index: 4,
                    previous_log_hash: 0x33,
                    log_hash: 0x44,
                },
            ],
        };

        let bytes = encode_metadata_command_log_hash_range_response(&response).unwrap();
        let decoded = decode_metadata_command_log_hash_range_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_entry_range_request_rejects_large_ranges() {
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES + 1,
            )
            .unwrap(),
        };

        let bytes = encode_metadata_command_log_hash_range_request(&request);
        assert!(matches!(
            decode_metadata_command_log_entry_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range is too large"
            ))
        ));
    }

    #[test]
    fn metadata_command_log_entry_range_response_round_trips() {
        let command = test_metadata_command();
        let response = StorageRpcMetadataCommandLogEntryRangeResponse {
            entries: vec![
                MetadataCommandLogRangeEntry {
                    log_index: command.id().log_index().get(),
                    previous_log_hash: 0x11,
                    log_hash: 0x22,
                    pre_state_digest: Some(0x21),
                    post_state_digest: Some(0x23),
                    kind: MetadataCommandLogRangeEntryKind::Applied(Box::new(command.clone())),
                },
                MetadataCommandLogRangeEntry {
                    log_index: 9,
                    previous_log_hash: 0x33,
                    log_hash: 0x44,
                    pre_state_digest: None,
                    post_state_digest: None,
                    kind: MetadataCommandLogRangeEntryKind::Abandoned {
                        original_command_checksum: 0x55,
                    },
                },
            ],
        };

        let bytes = encode_metadata_command_log_entry_range_response(&response).unwrap();
        let decoded = decode_metadata_command_log_entry_range_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_entry_range_worst_case_response_fits_frame_cap() {
        let worst_case_applied_entry_len =
            8 + 8 + 8 + 1 + 8 + 1 + 8 + 4 + STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN;
        let response_payload_len =
            4 + usize::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES).unwrap()
                * worst_case_applied_entry_len;
        let success_wrapped_len = 1 + 4 + response_payload_len;

        assert!(
            success_wrapped_len <= STORAGE_RPC_MAX_PAYLOAD_LEN,
            "entry range cap must fit a worst-case all-applied response after success wrapping"
        );
        let one_more_success_wrapped_len = success_wrapped_len + worst_case_applied_entry_len;
        assert!(
            one_more_success_wrapped_len > STORAGE_RPC_MAX_PAYLOAD_LEN,
            "test should prove the cap is tight against the frame limit"
        );
    }

    #[test]
    fn metadata_command_log_entry_range_response_rejects_unknown_kind() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, 1);
        put_u64(&mut bytes, 1);
        put_u64(&mut bytes, 0x11);
        put_u64(&mut bytes, 0x22);
        put_u8(&mut bytes, 0);
        put_u8(&mut bytes, 0);
        put_u8(&mut bytes, 9);

        assert!(matches!(
            decode_metadata_command_log_entry_range_response(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command entry range kind"
            ))
        ));
    }

    #[test]
    fn metadata_command_pending_envelope_response_round_trips() {
        let command = test_metadata_command();
        for response in [
            StorageRpcMetadataCommandPendingEnvelopeResponse { command: None },
            StorageRpcMetadataCommandPendingEnvelopeResponse {
                command: Some(command.clone()),
            },
        ] {
            let bytes = encode_metadata_command_pending_envelope_response(&response);
            let decoded = decode_metadata_command_pending_envelope_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_matching_applied_request_round_trips() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandMatchingAppliedRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
            expected_previous_log_hash: 0xabc,
        };

        let bytes = encode_metadata_command_matching_applied_request(&request).unwrap();
        let decoded = decode_metadata_command_matching_applied_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_applied_hashes_response_round_trips_outcomes() {
        for response in [
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None),
            },
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((0x12, 0x34))),
            },
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_applied_hashes_response(&response);
            let decoded = decode_metadata_command_applied_hashes_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_bool_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandBoolResponse { value: false },
            StorageRpcMetadataCommandBoolResponse { value: true },
        ] {
            let bytes = encode_metadata_command_bool_response(&response);
            let decoded = decode_metadata_command_bool_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_bool_outcome_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::Value(false),
            },
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::Value(true),
            },
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_bool_outcome_response(&response);
            let decoded = decode_metadata_command_bool_outcome_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_state_outcome_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::State(
                    MetadataCommandReplicaState {
                        cluster_epoch: ClusterEpoch::new(3).unwrap(),
                        applied_log_index: 44,
                        applied_log_hash: 0x55,
                        state_digest: 0x66,
                    },
                ),
            },
            StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_state_outcome_response(&response);
            let decoded = decode_metadata_command_state_outcome_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_state_request_carries_route() {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
        };

        let bytes = encode_metadata_command_state_request(&request);
        let decoded = decode_metadata_command_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_state_response_round_trips() {
        let response = StorageRpcMetadataCommandStateResponse {
            state: MetadataCommandReplicaState {
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                applied_log_index: 44,
                applied_log_hash: 0x55,
                state_digest: 0x66,
            },
        };

        let bytes = encode_metadata_command_state_response(&response);
        let decoded = decode_metadata_command_state_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_acceptance_response_round_trips() {
        for acceptance in [
            MetadataCommandAcceptance::Apply,
            MetadataCommandAcceptance::AlreadyApplied,
        ] {
            let response = StorageRpcMetadataCommandAcceptanceResponse {
                outcome: StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance),
            };

            let bytes = encode_metadata_command_acceptance_response(&response);
            let decoded = decode_metadata_command_acceptance_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }

        let conflict = StorageRpcMetadataCommandAcceptanceResponse {
            outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id: 7,
                pg_id: 11,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 13,
            },
        };
        let bytes = encode_metadata_command_acceptance_response(&conflict);
        let decoded = decode_metadata_command_acceptance_response(&bytes).unwrap();
        assert_eq!(decoded, conflict);

        assert_eq!(
            decode_metadata_command_acceptance_response(&[99]),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command acceptance tag"
            ))
        );
    }

    #[test]
    fn shard_write_item_rejects_semantic_corruption_after_frame_decode() {
        let payload = b"shard payload".to_vec();
        let item = StorageRpcShardWriteItem {
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload,
        };
        let mut item_bytes = encode_shard_write_item(&item).unwrap();
        let last = item_bytes.last_mut().unwrap();
        *last ^= 0x80;
        let frame = encode_storage_rpc_frame(9, StorageRpcMessageKind::ShardWrite, &item_bytes)
            .expect("corrupted semantic payload still has valid transport frame");
        let decoded_frame = decode_storage_rpc_frame(&frame).unwrap();

        assert_eq!(
            decode_shard_write_item(&decoded_frame.payload),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        );
    }

    #[test]
    fn shard_write_request_carries_idempotency_identity() {
        let payload = b"payload bytes".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: test_shard_location(2),
            shard_key: test_shard_key(2),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload,
        };

        let bytes = encode_shard_write_request(&request).unwrap();
        let decoded = decode_shard_write_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn shard_write_request_rejects_location_key_mismatch() {
        let payload = b"payload bytes".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: test_shard_location(3),
            shard_key: test_shard_key(2),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload,
        };

        assert_eq!(
            encode_shard_write_request(&request),
            Err(StorageRpcPayloadError::ShardLocationMismatch)
        );
    }

    #[test]
    fn shard_write_ack_must_match_request_expectation() {
        let payload = b"payload bytes";
        let expected_size = payload.len() as u64;
        let expected_crc64 = checksum::crc64::checksum(payload);
        let ack = WriteAck {
            stored_size: expected_size,
            crc64: expected_crc64,
        };
        let bytes = encode_shard_write_ack(ack);
        let decoded = decode_shard_write_ack(&bytes, expected_size, expected_crc64).unwrap();

        assert_eq!(decoded.stored_size, ack.stored_size);
        assert_eq!(decoded.crc64, ack.crc64);
        assert!(matches!(
            decode_shard_write_ack(&bytes, expected_size, expected_crc64 ^ 1),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        ));
    }

    #[test]
    fn shard_read_request_and_response_carry_expected_ack() {
        let payload = b"payload bytes";
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(payload),
        };
        let request = StorageRpcShardReadRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
            expected_ack,
        };

        let bytes = encode_shard_read_request(&request).unwrap();
        let decoded = decode_shard_read_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = encode_shard_read_response(payload, expected_ack).unwrap();
        assert_eq!(
            decode_shard_read_response(&response, expected_ack).unwrap(),
            payload
        );
        assert!(matches!(
            decode_shard_read_response(
                &response,
                WriteAck {
                    stored_size: expected_ack.stored_size,
                    crc64: expected_ack.crc64 ^ 1,
                },
            ),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        ));
    }

    #[test]
    fn shard_read_range_request_carries_expected_ack_and_range() {
        let payload = b"payload bytes";
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(payload),
        };
        let request = StorageRpcShardReadRangeRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
            expected_ack,
            offset: 2,
            length: 5,
        };

        let bytes = encode_shard_read_range_request(&request).unwrap();
        let decoded = decode_shard_read_range_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = encode_shard_read_range_response(&payload[2..7]);
        assert_eq!(
            decode_shard_read_range_response(&response, request.length as usize).unwrap(),
            &payload[2..7]
        );
        assert!(matches!(
            decode_shard_read_range_response(&response, request.length as usize + 1),
            Err(StorageRpcPayloadError::ShardWriteSizeMismatch { .. })
        ));
        assert!(matches!(
            encode_shard_read_range_request(&StorageRpcShardReadRangeRequest {
                offset: payload.len() as u64,
                length: 1,
                ..request
            }),
            Err(StorageRpcPayloadError::ShardWriteSizeMismatch { .. })
        ));
    }

    #[test]
    fn shard_delete_request_carries_operation_key() {
        let request = StorageRpcShardDeleteRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
        };

        let bytes = encode_shard_delete_request(&request).unwrap();
        let decoded = decode_shard_delete_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn shard_ack_batch_request_carries_route_and_exact_acks() {
        let request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
            items: vec![StorageRpcShardAckItem {
                shard_key: test_shard_key(2),
                ack: WriteAck {
                    stored_size: 123,
                    crc64: 0xBEEF,
                },
            }],
        };

        let bytes = encode_shard_ack_batch_request(&request).unwrap();
        let decoded = decode_shard_ack_batch_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert!(matches!(
            encode_shard_ack_batch_request(&StorageRpcShardAckBatchRequest {
                items: Vec::new(),
                ..request
            }),
            Err(StorageRpcPayloadError::InvalidShardAckBatchRequest(_))
        ));
    }

    #[test]
    fn shard_ack_item_request_and_response_carry_identity() {
        let request = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
            shard_key: test_shard_key(2),
        };
        let response = StorageRpcShardAckItem {
            shard_key: request.shard_key.clone(),
            ack: WriteAck {
                stored_size: 123,
                crc64: 0xBEEF,
            },
        };

        let request_bytes = encode_shard_ack_item_request(&request);
        assert_eq!(
            decode_shard_ack_item_request(&request_bytes).unwrap(),
            request
        );

        let response_bytes = encode_shard_ack_item_response(&response);
        assert_eq!(
            decode_shard_ack_item_response(&response_bytes).unwrap(),
            response
        );
    }

    #[test]
    fn placed_segment_shard_repair_rpc_round_trips_and_rejects_bad_shard_index() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 3,
                segment_okh: [0x5A; 16],
                segment_vid: GenerationId::new(88).unwrap(),
                stored_size: 4096,
                segment_crc64: 0xCAFE,
                ec: EcShape { k: 2, m: 1 },
            },
            shard_index: ShardIndex::new(2),
        };
        let record_request = StorageRpcPlacedSegmentShardRepairRecordRequest {
            route: route.clone(),
            work_item,
            last_error: Some("missing shard file".to_string()),
        };

        let record_bytes = encode_placed_segment_shard_repair_record_request(&record_request)
            .expect("repair record request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_record_request(&record_bytes).unwrap(),
            record_request
        );

        let item_request = StorageRpcPlacedSegmentShardRepairItemRequest {
            route: route.clone(),
            work_item,
        };
        let item_bytes = encode_placed_segment_shard_repair_item_request(&item_request)
            .expect("repair item request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_item_request(&item_bytes).unwrap(),
            item_request
        );

        let repair = PlacedSegmentShardRepairRecord {
            work_item,
            first_seen_at: 10,
            last_seen_at: 20,
            observation_count: 2,
            last_error: Some("still bad".to_string()),
        };
        assert!(matches!(
            encode_placed_segment_shard_repairs_response(&vec![
                repair.clone();
                PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT
                    + 1
            ]),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert_eq!(
            decode_placed_segment_shard_repairs_response(
                &encode_placed_segment_shard_repairs_response(std::slice::from_ref(&repair))
                    .unwrap()
            )
            .unwrap(),
            vec![repair]
        );
        let claim_acquire = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: Some(20),
            now: 10,
        };
        let claim_acquire_bytes =
            encode_placed_segment_shard_repair_claim_acquire_request(&claim_acquire)
                .expect("repair claim acquire request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_claim_acquire_request(&claim_acquire_bytes).unwrap(),
            claim_acquire
        );
        let missing_lease_acquire = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: None,
            now: 10,
        };
        assert!(matches!(
            encode_placed_segment_shard_repair_claim_acquire_request(&missing_lease_acquire),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim = PlacedSegmentShardRepairClaimRecord {
            work_item,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: route.cluster_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: Some("previous failure".to_string()),
        };
        let claim_response = StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
            record: Some(claim.clone()),
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_optional_record_response(
                &encode_placed_segment_shard_repair_claim_optional_record_response(&claim_response)
                    .unwrap()
            )
            .unwrap(),
            claim_response
        );
        let missing_lease_claim = PlacedSegmentShardRepairClaimRecord {
            lease_deadline: None,
            ..claim.clone()
        };
        assert!(matches!(
            encode_placed_segment_shard_repair_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
                    record: Some(missing_lease_claim)
                }
            ),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim_record_request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
            route: route.clone(),
            claim: claim.clone(),
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_record_request(
                &encode_placed_segment_shard_repair_claim_record_request(&claim_record_request)
                    .unwrap()
            )
            .unwrap(),
            claim_record_request
        );

        let claim_error_request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
            route,
            claim,
            last_error: "repair still failed".to_string(),
            next_attempt_after: 30,
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_error_request(
                &encode_placed_segment_shard_repair_claim_error_request(&claim_error_request)
                    .unwrap()
            )
            .unwrap(),
            claim_error_request
        );

        let mut bad = item_bytes;
        *bad.last_mut().unwrap() = 3;
        assert!(matches!(
            decode_placed_segment_shard_repair_item_request(&bad),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_rpc_round_trips_and_rejects_reversed_epochs() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
        };
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 3,
                segment_okh: [0x5B; 16],
                segment_vid: GenerationId::new(89).unwrap(),
                stored_size: 4096,
                segment_crc64: 0xCAFE,
                ec: EcShape { k: 2, m: 1 },
            },
            source_cluster_epoch: ClusterEpoch::new(1).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
        };
        let record_request = StorageRpcPlacedSegmentShardBackfillRecordRequest {
            route: route.clone(),
            work_item,
            remaining_tolerance: 1,
            last_error: Some("missing desired shard".to_string()),
        };

        let record_bytes = encode_placed_segment_shard_backfill_record_request(&record_request)
            .expect("backfill record request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_record_request(&record_bytes).unwrap(),
            record_request
        );

        let item_request = StorageRpcPlacedSegmentShardBackfillItemRequest {
            route: route.clone(),
            work_item,
        };
        let item_bytes = encode_placed_segment_shard_backfill_item_request(&item_request)
            .expect("backfill item request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_item_request(&item_bytes).unwrap(),
            item_request
        );

        let backfill = PlacedSegmentShardBackfillRecord {
            work_item,
            remaining_tolerance: 1,
            first_seen_at: 10,
            last_seen_at: 20,
            observation_count: 2,
            last_error: Some("still missing".to_string()),
        };
        assert!(matches!(
            encode_placed_segment_shard_backfills_response(&vec![
                backfill.clone();
                PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT
                    + 1
            ]),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert_eq!(
            decode_placed_segment_shard_backfills_response(
                &encode_placed_segment_shard_backfills_response(std::slice::from_ref(&backfill))
                    .unwrap()
            )
            .unwrap(),
            vec![backfill]
        );
        assert_eq!(
            decode_placed_segment_shard_backfill_count_response(
                &encode_placed_segment_shard_backfill_count_response(
                    PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT + 1
                )
                .unwrap()
            )
            .unwrap(),
            PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT + 1
        );

        let claim_acquire = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: Some(20),
            now: 10,
        };
        let claim_acquire_bytes =
            encode_placed_segment_shard_backfill_claim_acquire_request(&claim_acquire)
                .expect("backfill claim acquire request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_acquire_request(&claim_acquire_bytes)
                .unwrap(),
            claim_acquire
        );
        let missing_lease_acquire = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: None,
            now: 10,
        };
        assert!(matches!(
            encode_placed_segment_shard_backfill_claim_acquire_request(&missing_lease_acquire),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim = PlacedSegmentShardBackfillClaimRecord {
            work_item,
            remaining_tolerance: 1,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: route.cluster_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: Some("previous failure".to_string()),
        };
        let claim_response = StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
            record: Some(claim.clone()),
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_optional_record_response(
                &encode_placed_segment_shard_backfill_claim_optional_record_response(
                    &claim_response
                )
                .unwrap()
            )
            .unwrap(),
            claim_response
        );
        let missing_lease_claim = PlacedSegmentShardBackfillClaimRecord {
            lease_deadline: None,
            ..claim.clone()
        };
        assert!(matches!(
            encode_placed_segment_shard_backfill_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
                    record: Some(missing_lease_claim)
                }
            ),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim_record_request = StorageRpcPlacedSegmentShardBackfillClaimRecordRequest {
            route: route.clone(),
            claim: claim.clone(),
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_record_request(
                &encode_placed_segment_shard_backfill_claim_record_request(&claim_record_request)
                    .unwrap()
            )
            .unwrap(),
            claim_record_request
        );

        let claim_error_request = StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
            route,
            claim,
            last_error: "backfill still failed".to_string(),
            next_attempt_after: 30,
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_error_request(
                &encode_placed_segment_shard_backfill_claim_error_request(&claim_error_request)
                    .unwrap()
            )
            .unwrap(),
            claim_error_request
        );

        let mut reversed_epoch_bytes = item_bytes;
        let source_epoch_start = STORAGE_RPC_SHARD_ACK_ROUTE_LEN + 4 + 16 + 8 + 8 + 8 + 2;
        let desired_epoch_start = source_epoch_start + 8;
        reversed_epoch_bytes[source_epoch_start..source_epoch_start + 8]
            .copy_from_slice(&2_u64.to_be_bytes());
        reversed_epoch_bytes[desired_epoch_start..desired_epoch_start + 8]
            .copy_from_slice(&1_u64.to_be_bytes());
        assert!(matches!(
            decode_placed_segment_shard_backfill_item_request(&reversed_epoch_bytes),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn multipart_completion_snapshot_response_preserves_missing_part() {
        let upload_id = crate::tests::multipart_upload_id("completion-missing-part-upload");
        let response = StorageRpcMultipartCompletionSnapshotResponse {
            outcome: StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: upload_id.clone(),
                part_number: 9999,
            },
        };

        let bytes = encode_multipart_completion_snapshot_response(&response).unwrap();
        let decoded = decode_multipart_completion_snapshot_response(&bytes).unwrap();

        let StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id: decoded_upload_id,
            part_number,
        } = decoded.outcome
        else {
            panic!("expected missing part outcome");
        };
        assert_eq!(decoded_upload_id, upload_id);
        assert_eq!(part_number, 9999);
    }

    #[test]
    fn scavenger_list_files_request_and_response_round_trip() {
        let request = StorageRpcScavengerListFilesRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            data_pg_id: DataPgId::new(PgId::new(3)),
        };

        let request_bytes = encode_scavenger_list_files_request(&request);
        assert_eq!(
            decode_scavenger_list_files_request(&request_bytes).unwrap(),
            request
        );

        let scan = ScavengerShardFileScan {
            files: vec![ScavengerShardFile {
                key: test_shard_key(2),
                size: 123,
            }],
            errors: vec!["bad prefix".to_string()],
        };
        let response_bytes = encode_scavenger_list_files_response(&scan);
        let decoded = decode_scavenger_list_files_response(&response_bytes).unwrap();

        assert_eq!(decoded.files.len(), 1);
        assert_eq!(decoded.files[0].key, scan.files[0].key);
        assert_eq!(decoded.files[0].size, scan.files[0].size);
        assert_eq!(decoded.errors, scan.errors);
    }

    #[test]
    fn scavenger_metadata_messages_round_trip() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let key = test_shard_key(2);
        let rows = vec![ScavengerShardRow {
            key: key.clone(),
            ack: WriteAck {
                stored_size: 123,
                crc64: 0xBEEF,
            },
        }];
        let decoded_rows =
            decode_scavenger_shard_rows_response(&encode_scavenger_shard_rows_response(&rows))
                .unwrap();
        assert_eq!(decoded_rows.len(), rows.len());
        assert_eq!(decoded_rows[0].key, rows[0].key);
        assert_eq!(decoded_rows[0].ack, rows[0].ack);

        let references = vec![
            ShardScavengerPayloadReference::Placed(ShardScavengerPlacedShardSetReference {
                data_pg_id: 3,
                okh: [7; 16],
                generation_id: GenerationId::new(5).unwrap(),
                placement_cluster_epoch: ClusterEpoch::new(11).unwrap(),
                stored_size: 4096,
                crc64: 0xBEEF,
                ec: EcShape { k: 2, m: 1 },
            }),
            ShardScavengerPayloadReference::ReclaimOnly(ShardScavengerReclaimShardSetReference {
                data_pg_id: 4,
                okh: [9; 16],
                generation_id: GenerationId::new(10).unwrap(),
                ec: EcShape { k: 2, m: 1 },
            }),
            ShardScavengerPayloadReference::RoutedMultipartPart(
                ShardScavengerRoutedMultipartPartReference {
                    bucket: crate::tests::bucket_name("scavenger-codec-bucket"),
                    key: crate::tests::object_key("scavenger-codec-key"),
                    object_generation_id: GenerationId::new(6).unwrap(),
                    part_number: 7,
                    stored_size: 8192,
                    crc64: 0xCAFE,
                    part_okh: [8; 16],
                    part_vid: GenerationId::new(9).unwrap(),
                    placement_cluster_epoch: ClusterEpoch::new(12).unwrap(),
                    ec: EcShape { k: 4, m: 2 },
                },
            ),
        ];
        assert_eq!(
            decode_scavenger_payload_references_response(
                &encode_scavenger_payload_references_response(&references)
            )
            .unwrap(),
            references
        );

        let observation_key = ShardScavengerObservationKey {
            node_id: 7,
            data_pg_id: 3,
            shard_index: key.shard_index(),
            shard_key: key,
        };
        let record = ShardScavengerObservationRecord {
            key: observation_key.clone(),
            data_size: Some(123),
            crc64: Some(0xBEEF),
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
            last_error: Some("scan delayed".to_string()),
        };
        let record_request = StorageRpcScavengerObservationRecordRequest {
            route: route.clone(),
            observation: record.clone(),
        };
        assert_eq!(
            decode_scavenger_observation_record_request(
                &encode_scavenger_observation_record_request(&record_request).unwrap()
            )
            .unwrap(),
            record_request
        );

        let observations = vec![ShardScavengerObservation {
            key: observation_key.clone(),
            first_seen_at: 10,
            last_seen_at: 11,
            observation_count: 2,
            data_size: record.data_size,
            crc64: record.crc64,
            file_exists: record.file_exists,
            shard_row_exists: record.shard_row_exists,
            reason: record.reason,
            last_error: record.last_error.clone(),
            resolved_at: Some(12),
        }];
        assert_eq!(
            decode_scavenger_observations_response(&encode_scavenger_observations_response(
                &observations
            ))
            .unwrap(),
            observations
        );
        let minimal_observation = ShardScavengerObservation {
            key: observation_key.clone(),
            first_seen_at: 10,
            last_seen_at: 11,
            observation_count: 1,
            data_size: None,
            crc64: None,
            file_exists: false,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: None,
            resolved_at: None,
        };
        assert_eq!(
            encode_scavenger_observations_response(&[minimal_observation]).len(),
            4 + STORAGE_RPC_SCAVENGER_OBSERVATION_MIN_LEN
        );

        let key_request = StorageRpcScavengerObservationKeyRequest {
            route,
            key: observation_key,
        };
        assert_eq!(
            decode_scavenger_observation_key_request(
                &encode_scavenger_observation_key_request(&key_request).unwrap()
            )
            .unwrap(),
            key_request
        );
    }

    #[test]
    fn scavenger_metadata_decoders_reject_oversized_counts_before_allocation() {
        let mut oversized = Vec::new();
        put_u32(
            &mut oversized,
            u32::try_from(STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS + 1).unwrap(),
        );
        assert!(matches!(
            decode_scavenger_shard_rows_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            decode_scavenger_payload_references_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            decode_scavenger_observations_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));

        let mut truncated = Vec::new();
        put_u32(
            &mut truncated,
            STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS as u32,
        );
        assert!(matches!(
            decode_scavenger_payload_references_response(&truncated),
            Err(StorageRpcPayloadError::Truncated)
        ));
        assert!(matches!(
            decode_scavenger_observations_response(&truncated),
            Err(StorageRpcPayloadError::Truncated)
        ));
    }

    #[test]
    fn scavenger_observation_key_requires_matching_shard_index() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let mut bytes = encode_bucket_pg_request(&route).unwrap();
        put_u32(&mut bytes, 7);
        put_u32(&mut bytes, 3);
        put_u8(&mut bytes, 1);
        put_bytes(&mut bytes, test_shard_key(2).as_bytes());

        assert!(matches!(
            decode_scavenger_observation_key_request(&bytes),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn read_handle_acquire_request_requires_idempotency_key_and_locations() {
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "read-op-1".to_string(),
            locations: vec![test_shard_location(0), test_shard_location(1)],
            shard_keys: vec![test_shard_key(0), test_shard_key(1)],
        };

        let bytes = encode_read_handle_acquire_request(&request).unwrap();
        let decoded = decode_read_handle_acquire_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: String::new(),
                locations: vec![test_shard_location(0)],
                shard_keys: vec![test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id must not be empty",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "x".repeat(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1),
                locations: vec![test_shard_location(0)],
                shard_keys: vec![test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id exceeds maximum length",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-2".to_string(),
                locations: Vec::new(),
                shard_keys: Vec::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire must include at least one shard location",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-too-many-locations".to_string(),
                locations: (0..=STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS)
                    .map(test_shard_location_for_data_pg)
                    .collect(),
                shard_keys: (0..=STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS)
                    .map(|_| test_shard_key(0))
                    .collect(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire includes too many shard locations",
            ))
        );
    }

    #[test]
    fn read_handle_acquire_request_rejects_corrupt_location_count_before_allocating() {
        let mut bytes = Vec::new();
        put_string(&mut bytes, "read-op-oom");
        put_u32(&mut bytes, u32::MAX);

        assert_eq!(
            decode_read_handle_acquire_request(&bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire includes too many shard locations",
            ))
        );
    }

    #[test]
    fn read_handle_request_decoders_reject_oversized_ids_before_copying() {
        let mut acquire_bytes = Vec::new();
        put_u32(
            &mut acquire_bytes,
            u32::try_from(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1).unwrap(),
        );
        assert_eq!(
            decode_read_handle_acquire_request(&acquire_bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id exceeds maximum length",
            ))
        );

        let mut release_bytes = Vec::new();
        put_u32(
            &mut release_bytes,
            u32::try_from(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1).unwrap(),
        );
        assert_eq!(
            decode_read_handle_release_request(&release_bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id exceeds maximum length",
            ))
        );
    }

    #[test]
    fn read_handle_acquire_request_rejects_noncanonical_location_sets() {
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-duplicate".to_string(),
                locations: vec![test_shard_location(1), test_shard_location(1)],
                shard_keys: vec![test_shard_key(1), test_shard_key(1)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-unsorted".to_string(),
                locations: vec![test_shard_location(1), test_shard_location(0)],
                shard_keys: vec![test_shard_key(1), test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
    }

    #[test]
    fn storage_rpc_request_frame_rejects_payload_over_kind_limit_before_allocating() {
        for (kind, payload_len, limit) in [
            (
                StorageRpcMessageKind::Health,
                STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClusterMapHistoryReferenceSummary,
                4 + 8 + 1,
                4 + 8,
            ),
            (
                StorageRpcMessageKind::MetadataCommand,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardWrite,
                STORAGE_RPC_MAX_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ReadHandlesAcquire,
                STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ReadHandlesRelease,
                STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardRead,
                STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardReadRange,
                STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardDelete,
                STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckLoad,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckHistoricalLoad,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckDelete,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckRecord,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckValidate,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerListFiles,
                STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerShardRows,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerPayloadReferences,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservations,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationRecord,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationResolve,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClaimHeartbeat,
                STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClaimRelease,
                STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ProofRelease,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandReplicaState,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAcceptance,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
                STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAbandoned,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecordAbandoned,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectGenerationNext,
                STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectGenerationReservation,
                STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectVersionNext,
                STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectPayloadReclaimExists,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::DirectPutCommitSnapshotLoad,
                STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::DirectPutCommitCommandBuild,
                STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectReadAuthSubjectLoad,
                STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectReadSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectTagsForSubjectLoad,
                STORAGE_RPC_MAX_OBJECT_TAGS_FOR_SUBJECT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_TAGS_FOR_SUBJECT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationAcquire,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationValidate,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationRelease,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketSnapshotLoad,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketSnapshotPairLoad,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_PAIR_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_PAIR_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteDrainExists,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteDrainGet,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
                STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMetadataControlPendingMatch,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMetadataControlCommandBuild,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMarkDeletingCommandBuild,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketSubresourceGet,
                STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketList,
                STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketExecutionGenerations,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketFastPathIdentities,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepBucketsList,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepRoots,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimAcquire,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 8 + 1 + 8 + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 8 + 1 + 8,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimError,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimRelease,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectListPage,
                STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectVersionListPage,
                STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMultipartUploadListPage,
                STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN,
            ),
        ] {
            let mut bytes = Vec::new();
            put_bytes(&mut bytes, STORAGE_RPC_FRAME_MAGIC);
            put_u16(&mut bytes, STORAGE_RPC_FRAME_ENCODING_VERSION);
            put_u64(&mut bytes, 7);
            put_u16(&mut bytes, kind as u16);
            put_u32(
                &mut bytes,
                u32::try_from(payload_len).expect("test payload length fits in u32"),
            );

            assert!(matches!(
                read_storage_rpc_request_frame_from(&mut Cursor::new(bytes)),
                Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge {
                    len,
                    limit: actual_limit,
                })) if len == payload_len && actual_limit == limit
            ));
        }
    }

    #[test]
    fn bucket_delete_coordination_max_record_requests_fit_kind_caps() {
        let bucket = BucketName::try_from("a".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap();
        let cluster_epoch = ClusterEpoch::INITIAL;
        let node_id = NodeId::new(1);
        let pg_id = PgId::new(2);
        let drain_id = "d".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN);
        let owner_token = "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN);

        let drain_payload =
            encode_bucket_write_drain_record_request(&StorageRpcBucketWriteDrainRecordRequest {
                node_id,
                cluster_epoch,
                pg_id,
                record: BucketWriteDrainRecord {
                    bucket: bucket.clone(),
                    drain_id: drain_id.clone(),
                    owner_token: owner_token.clone(),
                    cluster_epoch,
                    bucket_execution_generation: 3,
                    state: BucketWriteDrainState::Draining,
                    created_at: 4,
                    lease_deadline: Some(5),
                },
            })
            .unwrap();
        assert_eq!(
            drain_payload.len(),
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN
        );
        let drain_frame = encode_storage_rpc_frame(
            11,
            StorageRpcMessageKind::BucketWriteDrainClear,
            &drain_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(drain_frame)).unwrap();
        assert_eq!(decoded.payload, drain_payload);

        let mut oversized_drain_payload = drain_payload.clone();
        oversized_drain_payload.push(0);
        let oversized_drain_frame = encode_storage_rpc_frame(
            12,
            StorageRpcMessageKind::BucketWriteDrainClear,
            &oversized_drain_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_drain_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN
        ));

        let outcome_route_epoch = ClusterEpoch::new(2).unwrap();
        let outcome_payload = encode_bucket_delete_attempt_outcome_record_request(
            &StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
                node_id,
                cluster_epoch: outcome_route_epoch,
                pg_id,
                record: BucketDeleteAttemptOutcomeRecord {
                    bucket: bucket.clone(),
                    drain_id: drain_id.clone(),
                    cluster_epoch,
                    bucket_execution_generation: 3,
                    outcome: BucketDeleteAttemptOutcomeKind::Retryable,
                    phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
                    detail: "e".repeat(BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN),
                    post_reservation_next_object_pg_id: Some(7),
                    updated_at: 6,
                },
            },
        )
        .unwrap();
        assert_eq!(
            outcome_payload.len(),
            STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN
        );
        let outcome_frame = encode_storage_rpc_frame(
            13,
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
            &outcome_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(outcome_frame)).unwrap();
        assert_eq!(decoded.payload, outcome_payload);
        let decoded_request =
            decode_bucket_delete_attempt_outcome_record_request(&outcome_payload).unwrap();
        assert_eq!(decoded_request.cluster_epoch, outcome_route_epoch);
        assert_eq!(decoded_request.record.cluster_epoch, cluster_epoch);

        let mut oversized_outcome_payload = outcome_payload.clone();
        oversized_outcome_payload.push(0);
        let oversized_outcome_frame = encode_storage_rpc_frame(
            14,
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
            &oversized_outcome_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_outcome_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN
        ));

        let claim_payload = encode_bucket_delete_finalize_claim_record_request(
            &StorageRpcBucketDeleteFinalizeClaimRecordRequest {
                node_id,
                cluster_epoch,
                pg_id,
                record: BucketDeleteFinalizeClaimRecord {
                    bucket,
                    bucket_incarnation_generation: 6,
                    claim_id: drain_id,
                    owner_token,
                    cluster_epoch,
                    pg_id: pg_id.get(),
                    claimed_at: 7,
                    lease_deadline: Some(8),
                    attempt_count: 9,
                    last_error: Some("e".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN)),
                },
            },
        )
        .unwrap();
        assert_eq!(
            claim_payload.len(),
            STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN
        );
        let claim_frame = encode_storage_rpc_frame(
            13,
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
            &claim_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(claim_frame)).unwrap();
        assert_eq!(decoded.payload, claim_payload);

        let mut oversized_claim_payload = claim_payload;
        oversized_claim_payload.push(0);
        let oversized_claim_frame = encode_storage_rpc_frame(
            14,
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
            &oversized_claim_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_claim_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN
        ));
    }

    #[test]
    fn shard_read_response_can_exceed_shard_read_request_frame_limit() {
        let payload = vec![0x4a; STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 1];
        let frame_bytes =
            encode_storage_rpc_frame(9, StorageRpcMessageKind::ShardRead, &payload).unwrap();
        let frame = read_storage_rpc_frame_from(&mut Cursor::new(frame_bytes)).unwrap();

        assert_eq!(frame.kind, StorageRpcMessageKind::ShardRead);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn read_handle_acquire_response_round_trips_canonical_locations() {
        let response = StorageRpcReadHandleAcquireResponse {
            locations: vec![test_shard_location(0), test_shard_location(1)],
        };

        let bytes = encode_read_handle_acquire_response(&response).unwrap();
        let decoded = decode_read_handle_acquire_response(&bytes).unwrap();

        assert_eq!(decoded, response);
        assert_eq!(
            encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                locations: Vec::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire response must include at least one shard location",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                locations: vec![test_shard_location(1), test_shard_location(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
    }

    #[test]
    fn read_handle_release_request_and_response_round_trip() {
        let request = StorageRpcReadHandleReleaseRequest {
            read_operation_id: "read-op-release".to_string(),
        };

        let request_bytes = encode_read_handle_release_request(&request).unwrap();
        let decoded_request = decode_read_handle_release_request(&request_bytes).unwrap();

        assert_eq!(decoded_request, request);
        assert_eq!(
            encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
                read_operation_id: String::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id must not be empty",
            ))
        );
        assert_eq!(
            encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
                read_operation_id: "x".repeat(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id exceeds maximum length",
            ))
        );
        let response_bytes =
            encode_read_handle_release_response(&StorageRpcReadHandleReleaseResponse);
        let decoded_response = decode_read_handle_release_response(&response_bytes).unwrap();
        assert_eq!(decoded_response, StorageRpcReadHandleReleaseResponse);
        assert_eq!(
            decode_read_handle_release_response(&[1]),
            Err(StorageRpcPayloadError::TrailingBytes)
        );
    }

    #[test]
    fn claim_heartbeat_and_release_requests_are_token_fenced() {
        let token = test_claim_token();
        let heartbeat = StorageRpcClaimHeartbeatRequest {
            token: token.clone(),
            heartbeat_at: 100,
            lease_deadline: Some(160),
        };
        let heartbeat_bytes = encode_claim_heartbeat_request(&heartbeat).unwrap();
        let decoded_heartbeat = decode_claim_heartbeat_request(&heartbeat_bytes).unwrap();

        assert_eq!(decoded_heartbeat, heartbeat);
        assert_eq!(
            encode_claim_heartbeat_request(&StorageRpcClaimHeartbeatRequest {
                token: token.clone(),
                heartbeat_at: 100,
                lease_deadline: Some(100),
            }),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "claim heartbeat lease deadline must be after heartbeat time",
            ))
        );

        let release = StorageRpcClaimReleaseRequest { token };
        let release_bytes = encode_claim_release_request(&release).unwrap();
        let decoded_release = decode_claim_release_request(&release_bytes).unwrap();

        assert_eq!(decoded_release, release);
    }

    #[test]
    fn object_reclaim_claim_release_request_carries_full_work_identity() {
        let release = StorageRpcClaimReleaseRequest {
            token: test_object_reclaim_claim_token(),
        };

        let bytes = encode_claim_release_request(&release).unwrap();
        let decoded = decode_claim_release_request(&bytes).unwrap();

        assert_eq!(decoded, release);
    }

    #[test]
    fn proof_release_request_carries_full_reservation_identity() {
        let request = StorageRpcProofReleaseRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            proof: test_bucket_write_reservation_proof(),
        };

        let bytes = encode_proof_release_request(&request).unwrap();
        let decoded = decode_proof_release_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn object_listing_requests_and_responses_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let prefix = ObjectKey::try_from("prefix/").unwrap();
        let key_marker = ObjectKey::try_from("prefix/key").unwrap();
        let upload_id = UploadId::try_from("u".repeat(UPLOAD_ID_LEN)).unwrap();

        let objects = StorageRpcListObjectsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectsReq {
                bucket: bucket.clone(),
                prefix: Some(prefix.clone()),
                start_after: Some(key_marker.clone()),
                start_at: None,
                max_keys: 9,
            },
        };
        let bytes = encode_list_objects_request(&objects).unwrap();
        let decoded = decode_list_objects_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, objects.node_id);
        assert_eq!(decoded.cluster_epoch, objects.cluster_epoch);
        assert_eq!(decoded.pg_id, objects.pg_id);
        assert_eq!(decoded.request.bucket, objects.request.bucket);
        assert_eq!(decoded.request.prefix, objects.request.prefix);
        assert_eq!(decoded.request.start_after, objects.request.start_after);
        assert_eq!(decoded.request.start_at, objects.request.start_at);
        assert_eq!(decoded.request.max_keys, objects.request.max_keys);

        let response = StorageRpcListObjectsResponse {
            response: ListObjectsResp {
                objects: Vec::new(),
                is_truncated: true,
                next_start_after: Some(key_marker.clone()),
            },
        };
        let bytes = encode_list_objects_response(&response).unwrap();
        let decoded = decode_list_objects_response(&bytes).unwrap();
        assert!(decoded.response.objects.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(decoded.response.next_start_after, Some(key_marker.clone()));

        let versions = StorageRpcListObjectVersionsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: Some(prefix.clone()),
                key_marker: Some(key_marker.clone()),
                version_id_marker: Some(VersionId::from_u64(42)),
                start_at: None,
                max_keys: 10,
            },
        };
        let bytes = encode_list_object_versions_request(&versions).unwrap();
        let decoded = decode_list_object_versions_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, versions.node_id);
        assert_eq!(decoded.cluster_epoch, versions.cluster_epoch);
        assert_eq!(decoded.pg_id, versions.pg_id);
        assert_eq!(decoded.request.bucket, versions.request.bucket);
        assert_eq!(decoded.request.prefix, versions.request.prefix);
        assert_eq!(decoded.request.key_marker, versions.request.key_marker);
        assert_eq!(
            decoded.request.version_id_marker,
            versions.request.version_id_marker
        );
        assert_eq!(decoded.request.start_at, versions.request.start_at);
        assert_eq!(decoded.request.max_keys, versions.request.max_keys);

        let response = StorageRpcListObjectVersionsResponse {
            response: ListObjectVersionsResp {
                versions: Vec::new(),
                is_truncated: true,
                next_key_marker: Some(key_marker.clone()),
                next_version_id_marker: Some(VersionId::from_u64(43)),
            },
        };
        let bytes = encode_list_object_versions_response(&response).unwrap();
        let decoded = decode_list_object_versions_response(&bytes).unwrap();
        assert!(decoded.response.versions.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(decoded.response.next_key_marker, Some(key_marker.clone()));
        assert_eq!(
            decoded.response.next_version_id_marker,
            Some(VersionId::from_u64(43))
        );

        let uploads = StorageRpcListMultipartUploadsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListMultipartUploadsReq {
                bucket,
                prefix: Some(prefix),
                key_marker: Some(key_marker.clone()),
                upload_id_marker: Some(upload_id.clone()),
                max_uploads: 11,
            },
        };
        let bytes = encode_list_multipart_uploads_request(&uploads).unwrap();
        let decoded = decode_list_multipart_uploads_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, uploads.node_id);
        assert_eq!(decoded.cluster_epoch, uploads.cluster_epoch);
        assert_eq!(decoded.pg_id, uploads.pg_id);
        assert_eq!(decoded.request.bucket, uploads.request.bucket);
        assert_eq!(decoded.request.prefix, uploads.request.prefix);
        assert_eq!(decoded.request.key_marker, uploads.request.key_marker);
        assert_eq!(
            decoded.request.upload_id_marker,
            uploads.request.upload_id_marker
        );
        assert_eq!(decoded.request.max_uploads, uploads.request.max_uploads);

        let response = StorageRpcListMultipartUploadsResponse {
            response: ListMultipartUploadsResp {
                uploads: Vec::new(),
                is_truncated: true,
                next_key_marker: Some(key_marker),
                next_upload_id_marker: Some(upload_id),
            },
        };
        let bytes = encode_list_multipart_uploads_response(&response).unwrap();
        let decoded = decode_list_multipart_uploads_response(&bytes).unwrap();
        assert!(decoded.response.uploads.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(
            decoded.response.next_key_marker,
            response.response.next_key_marker
        );
        assert_eq!(
            decoded.response.next_upload_id_marker,
            response.response.next_upload_id_marker
        );
    }

    #[test]
    fn object_listing_requests_reject_unbounded_page_limits() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let objects = StorageRpcListObjectsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectsReq {
                bucket: bucket.clone(),
                prefix: None,
                start_after: None,
                start_at: None,
                max_keys: STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1,
            },
        };
        assert_eq!(
            encode_list_objects_request(&objects),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
            })
        );

        let mut bytes = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
            node_id: objects.node_id,
            cluster_epoch: objects.cluster_epoch,
            pg_id: objects.pg_id,
        })
        .unwrap();
        put_string(&mut bytes, bucket.as_str());
        put_optional_string(&mut bytes, None);
        put_optional_string(&mut bytes, None);
        put_optional_string(&mut bytes, None);
        put_u32(&mut bytes, STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1);
        assert!(matches!(
            decode_list_objects_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == (STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1) as usize
                && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn object_listing_responses_reject_oversized_counts_before_allocating() {
        let too_many = STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1;

        let mut object_bytes = Vec::new();
        put_u32(&mut object_bytes, too_many);
        assert!(matches!(
            decode_list_objects_response(&object_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));

        let mut version_bytes = Vec::new();
        put_u32(&mut version_bytes, too_many);
        assert!(matches!(
            decode_list_object_versions_response(&version_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));

        let mut upload_bytes = Vec::new();
        put_u32(&mut upload_bytes, too_many);
        assert!(matches!(
            decode_list_multipart_uploads_response(&upload_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn bucket_metadata_read_requests_and_responses_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let list_request = StorageRpcBucketListRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            owner_canonical_id: CanonicalUserId::from_principal("owner").to_string(),
        };
        let bytes = encode_bucket_list_request(&list_request).unwrap();
        let decoded = decode_bucket_list_request(&bytes).unwrap();
        assert_eq!(decoded, list_request);

        let info = test_bucket_info("bucket");
        let bytes = encode_bucket_list_response(&StorageRpcBucketListResponse {
            buckets: vec![info.clone()],
        })
        .unwrap();
        let decoded = decode_bucket_list_response(&bytes).unwrap();
        assert_eq!(decoded.buckets.len(), 1);
        assert_eq!(decoded.buckets[0].name, info.name);
        assert_eq!(
            decoded.buckets[0].owner_canonical_id,
            info.owner_canonical_id
        );
        assert_eq!(
            decoded.buckets[0].bucket_execution_generation,
            info.bucket_execution_generation
        );

        let batch_request = StorageRpcBucketBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            buckets: vec![bucket.clone()],
        };
        let bytes = encode_bucket_batch_request(&batch_request).unwrap();
        let decoded = decode_bucket_batch_request(&bytes).unwrap();
        assert_eq!(decoded, batch_request);

        let generations = StorageRpcBucketExecutionGenerationsResponse {
            generations: HashMap::from([(bucket.clone(), 11)]),
        };
        let bytes = encode_bucket_execution_generations_response(&generations).unwrap();
        let decoded = decode_bucket_execution_generations_response(&bytes).unwrap();
        assert_eq!(decoded, generations);

        let identities = StorageRpcBucketFastPathIdentitiesResponse {
            identities: HashMap::from([(
                bucket,
                BucketFastPathIdentity {
                    bucket_execution_generation: 11,
                    bucket_incarnation_generation: 17,
                },
            )]),
        };
        let bytes = encode_bucket_fast_path_identities_response(&identities).unwrap();
        let decoded = decode_bucket_fast_path_identities_response(&bytes).unwrap();
        assert_eq!(decoded, identities);
    }

    #[test]
    fn bucket_mark_deleting_command_build_request_and_response_round_trip() {
        let bucket_request = StorageRpcBucketRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: BucketName::try_from("bucket").unwrap(),
        };
        let command_id = MetadataCommandId::new(
            bucket_request.cluster_epoch,
            bucket_request.pg_id,
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: bucket_request.clone(),
            command_id,
        };

        let bytes = encode_bucket_mark_deleting_command_build_request(&request).unwrap();
        let decoded = decode_bucket_mark_deleting_command_build_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let wrong_command_id = MetadataCommandId::new(
            bucket_request.cluster_epoch,
            PgId::new(4),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        assert_eq!(
            encode_bucket_mark_deleting_command_build_request(
                &StorageRpcBucketMarkDeletingCommandBuildRequest {
                    bucket: bucket_request.clone(),
                    command_id: wrong_command_id,
                }
            ),
            Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "command id route must match request route",
            ))
        );

        let mut deleting_info = test_bucket_info("bucket");
        deleting_info.state = BucketState::Deleting;
        let already_deleting = StorageRpcBucketMarkDeletingCommandBuildResponse {
            outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(
                deleting_info,
            ),
        };
        let bytes = encode_bucket_mark_deleting_command_build_response(&already_deleting);
        let decoded = decode_bucket_mark_deleting_command_build_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
                assert_eq!(info.name.as_str(), "bucket");
                assert_eq!(info.state, BucketState::Deleting);
                assert_eq!(info.bucket_execution_generation, 17);
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(_) => {
                panic!("expected already-deleting response")
            }
        }

        let command = test_mark_bucket_deleting_command(command_id);
        let command_response = StorageRpcBucketMarkDeletingCommandBuildResponse {
            outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(Box::new(command)),
        };
        let bytes = encode_bucket_mark_deleting_command_build_response(&command_response);
        let decoded = decode_bucket_mark_deleting_command_build_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command) => {
                assert_eq!(command.id(), command_id);
                assert!(matches!(
                    command.payload(),
                    MetadataCommandPayload::MarkBucketDeleting(mark)
                        if mark.bucket.name.as_str() == "bucket"
                            && mark.bucket.state == BucketState::Deleting
                ));
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(_) => {
                panic!("expected command response")
            }
        }

        let pending_match = StorageRpcBucketMetadataControlPendingMatchRequest {
            bucket: bucket_request,
            command: test_mark_bucket_deleting_command(command_id),
            mutation: StorageRpcBucketMetadataControlMutation::MarkDeleting,
        };
        let bytes = encode_bucket_metadata_control_pending_match_request(&pending_match).unwrap();
        let decoded = decode_bucket_metadata_control_pending_match_request(&bytes).unwrap();
        assert_eq!(decoded, pending_match);
    }

    #[test]
    fn bucket_metadata_read_responses_reject_oversized_counts_before_allocating() {
        let too_many = STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS + 1;

        let mut list_bytes = Vec::new();
        put_u32(&mut list_bytes, too_many);
        assert!(matches!(
            decode_bucket_list_response(&list_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));

        let mut generation_bytes = Vec::new();
        put_u32(&mut generation_bytes, too_many);
        assert!(matches!(
            decode_bucket_execution_generations_response(&generation_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));

        let mut identity_bytes = Vec::new();
        put_u32(&mut identity_bytes, too_many);
        assert!(matches!(
            decode_bucket_fast_path_identities_response(&identity_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));
    }

    #[test]
    fn bucket_snapshot_request_and_response_round_trip() {
        let request = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("bucket").unwrap(),
            },
            request: BucketSnapshotRequest {
                policy: true,
                tags: BucketSnapshotTagsRequest::Always,
                lifecycle: true,
                cors: true,
            },
        };
        let bytes = encode_bucket_snapshot_request(&request);
        let decoded = decode_bucket_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let snapshot = BucketSnapshot {
            bucket: test_bucket_info("bucket"),
            request: request.request,
            policy: LoadedBucketSubresource::Loaded("{\"Statement\":[]}".to_string()),
            tags: LoadedBucketSubresource::Missing,
            lifecycle: LoadedBucketSubresource::Loaded("<LifecycleConfiguration/>".to_string()),
            cors: LoadedBucketSubresource::NotRequested,
        };
        let response = StorageRpcBucketSnapshotResponse {
            outcome: StorageRpcBucketSnapshotOutcome::Loaded(Box::new(snapshot)),
        };
        let bytes = encode_bucket_snapshot_response(&response);
        let decoded = decode_bucket_snapshot_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
                assert_eq!(snapshot.bucket.name.as_str(), "bucket");
                assert_eq!(snapshot.request, request.request);
                assert!(matches!(
                    snapshot.policy,
                    LoadedBucketSubresource::Loaded(ref body)
                        if body == "{\"Statement\":[]}"
                ));
                assert!(matches!(snapshot.tags, LoadedBucketSubresource::Missing));
                assert!(matches!(
                    snapshot.cors,
                    LoadedBucketSubresource::NotRequested
                ));
            }
            StorageRpcBucketSnapshotOutcome::BucketNotFound { .. } => {
                panic!("expected loaded bucket snapshot response")
            }
        }

        let response = StorageRpcBucketSnapshotResponse {
            outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound {
                name: BucketName::try_from("missing-bucket").unwrap(),
            },
        };
        let bytes = encode_bucket_snapshot_response(&response);
        let decoded = decode_bucket_snapshot_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => {
                assert_eq!(name.as_str(), "missing-bucket");
            }
            StorageRpcBucketSnapshotOutcome::Loaded(_) => {
                panic!("expected bucket-not-found snapshot response")
            }
        }
    }

    #[test]
    fn bucket_snapshot_pair_request_and_response_round_trip() {
        let source = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("source-bucket").unwrap(),
            },
            request: BucketSnapshotRequest {
                tags: BucketSnapshotTagsRequest::Always,
                ..Default::default()
            },
        };
        let destination = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(4),
                bucket: BucketName::try_from("destination-bucket").unwrap(),
            },
            request: BucketSnapshotRequest {
                cors: true,
                ..Default::default()
            },
        };
        let request = StorageRpcBucketSnapshotPairRequest {
            source: source.clone(),
            destination: destination.clone(),
        };
        let bytes = encode_bucket_snapshot_pair_request(&request);
        let decoded = decode_bucket_snapshot_pair_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let pair = BucketSnapshotPair::Distinct {
            source: Box::new(BucketSnapshot {
                bucket: test_bucket_info("source-bucket"),
                request: source.request,
                policy: LoadedBucketSubresource::NotRequested,
                tags: LoadedBucketSubresource::Loaded("<Tagging/>".to_string()),
                lifecycle: LoadedBucketSubresource::NotRequested,
                cors: LoadedBucketSubresource::NotRequested,
            }),
            destination: Box::new(BucketSnapshot {
                bucket: test_bucket_info("destination-bucket"),
                request: destination.request,
                policy: LoadedBucketSubresource::NotRequested,
                tags: LoadedBucketSubresource::NotRequested,
                lifecycle: LoadedBucketSubresource::NotRequested,
                cors: LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string()),
            }),
        };
        let response = StorageRpcBucketSnapshotPairResponse {
            outcome: StorageRpcBucketSnapshotPairOutcome::Loaded(Box::new(pair)),
        };
        let bytes = encode_bucket_snapshot_pair_response(&response);
        let decoded = decode_bucket_snapshot_pair_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotPairOutcome::Loaded(pair) => {
                assert_eq!(pair.source().bucket.name.as_str(), "source-bucket");
                assert_eq!(
                    pair.destination().bucket.name.as_str(),
                    "destination-bucket"
                );
                assert!(matches!(
                    pair.source().tags,
                    LoadedBucketSubresource::Loaded(ref body) if body == "<Tagging/>"
                ));
                assert!(matches!(
                    pair.destination().cors,
                    LoadedBucketSubresource::Loaded(ref body) if body == "<CORSConfiguration/>"
                ));
            }
            StorageRpcBucketSnapshotPairOutcome::BucketNotFound { .. } => {
                panic!("expected loaded bucket snapshot pair response")
            }
        }

        let response = StorageRpcBucketSnapshotPairResponse {
            outcome: StorageRpcBucketSnapshotPairOutcome::BucketNotFound {
                name: BucketName::try_from("missing-bucket").unwrap(),
            },
        };
        let bytes = encode_bucket_snapshot_pair_response(&response);
        let decoded = decode_bucket_snapshot_pair_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotPairOutcome::BucketNotFound { name } => {
                assert_eq!(name.as_str(), "missing-bucket");
            }
            StorageRpcBucketSnapshotPairOutcome::Loaded(_) => {
                panic!("expected bucket-not-found snapshot pair response")
            }
        }
    }

    #[test]
    fn bucket_write_reservation_requests_round_trip() {
        let acquire = StorageRpcBucketWriteReservationAcquireRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: BucketName::try_from("bucket").unwrap(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token-1".to_string(),
            operation_kind: "put-object".to_string(),
            created_at: 10,
            lease_deadline: Some(20),
            target_context: Some("key=a".to_string()),
        };
        let bytes = encode_bucket_write_reservation_acquire_request(&acquire).unwrap();
        let decoded = decode_bucket_write_reservation_acquire_request(&bytes).unwrap();
        assert_eq!(decoded, acquire);

        let record = BucketWriteReservationRecord {
            bucket: acquire.bucket.clone(),
            reservation_id: acquire.reservation_id.clone(),
            owner_token: acquire.owner_token.clone(),
            cluster_epoch: acquire.cluster_epoch,
            bucket_execution_generation: 2,
            bucket_incarnation_generation: 3,
            operation_kind: acquire.operation_kind.clone(),
            created_at: acquire.created_at,
            lease_deadline: acquire.lease_deadline,
            target_context: acquire.target_context.clone(),
        };
        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record.clone()),
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::Draining,
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
                name: acquire.bucket.clone(),
            },
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let release = StorageRpcBucketWriteReservationRecordRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            record: record.clone(),
        };
        let bytes = encode_bucket_write_reservation_record_request(&release).unwrap();
        let decoded = decode_bucket_write_reservation_record_request(&bytes).unwrap();
        assert_eq!(decoded, release);

        let proof = StorageRpcBucketWriteReservationProofRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            proof: BucketWriteReservationProof::from(&record),
        };
        let bytes = encode_bucket_write_reservation_proof_request(&proof).unwrap();
        let decoded = decode_bucket_write_reservation_proof_request(&bytes).unwrap();
        assert_eq!(decoded, proof);
    }

    #[test]
    fn cleanup_list_requests_reject_limit_over_protocol_max() {
        let bucket = crate::tests::bucket_name("cleanup-list-limit");
        let stream_request = StorageRpcStreamUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
            },
            session_id_marker: Some(
                SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            ),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1,
        };
        assert_eq!(
            encode_stream_uploads_list_request(&stream_request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );

        let stream_pg_request = StorageRpcStreamUploadsPgListRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            session_id_marker: Some(
                SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            ),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1,
        };
        assert_eq!(
            encode_stream_uploads_pg_list_request(&stream_pg_request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );

        let completed_request = StorageRpcCompletedMultipartUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket,
            },
            upload_id_marker: Some(crate::tests::multipart_upload_id("cleanup-list-marker")),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1,
        };
        assert_eq!(
            encode_completed_multipart_uploads_list_request(&completed_request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );
    }

    #[test]
    fn cleanup_list_responses_reject_count_over_protocol_max_before_items() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1);
        assert!(matches!(
            decode_stream_uploads_list_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize
                && limit == STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize
        ));

        let mut bytes = Vec::new();
        put_u32(&mut bytes, STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1);
        assert!(matches!(
            decode_completed_multipart_uploads_list_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize
                && limit == STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn lifecycle_sweep_roots_request_rejects_limit_over_protocol_max() {
        let request = StorageRpcLifecycleSweepRootsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            now: 100,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
        };
        assert_eq!(
            encode_lifecycle_sweep_roots_request(&request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
                limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
            })
        );

        let mut bytes = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
            node_id: request.node_id,
            cluster_epoch: request.cluster_epoch,
            pg_id: request.pg_id,
        })
        .unwrap();
        put_u64(&mut bytes, request.now);
        put_u64(
            &mut bytes,
            u64::try_from(STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1).unwrap(),
        );
        assert_eq!(
            decode_lifecycle_sweep_roots_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
                limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
            })
        );
    }

    #[test]
    fn stream_uploads_list_response_rejects_unbounded_item_count() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1);

        assert_eq!(
            decode_stream_uploads_list_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );
    }

    #[test]
    fn object_generation_reservation_request_and_response_round_trip() {
        let request = StorageRpcObjectGenerationReservationRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("bucket").unwrap(),
                key: ObjectKey::try_from("key").unwrap(),
            },
            reservation_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
        };

        let bytes = encode_object_generation_reservation_request(&request);
        let decoded = decode_object_generation_reservation_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = StorageRpcObjectGenerationReservationResponse {
            outcome: StorageRpcObjectGenerationReservationOutcome::Found(
                GenerationId::new(42).unwrap(),
            ),
        };
        let bytes = encode_object_generation_reservation_response(&response);
        let decoded = decode_object_generation_reservation_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectGenerationReservationResponse {
            outcome: StorageRpcObjectGenerationReservationOutcome::NotFound {
                reservation_id: request.reservation_id,
            },
        };
        let bytes = encode_object_generation_reservation_response(&response);
        let decoded = decode_object_generation_reservation_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn object_read_auth_subject_and_snapshot_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let owner = OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let stored = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            owner: owner.clone(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(9).unwrap(),
            size: 12,
            etag: ObjectEtag::single_part(55),
            last_modified: 10,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::MultipartManifest {
                parts_count: NonZeroU32::new(1).unwrap(),
            },
            tags: None,
            metadata_blob: Some(SerializedMetadataBlob::new(vec![1, 2, 3])),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::new(vec![4, 5, 6])),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        let request = StorageRpcObjectReadAuthSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id: Some(VersionId::from_u64(7)),
        };

        let bytes = encode_object_read_auth_subject_request(&request);
        let decoded = decode_object_read_auth_subject_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let subject = ObjectReadAuthSubject {
            identity: ObjectReadAuthSubjectIdentity::for_stored(&stored),
            stored: stored.clone(),
        };
        let response = StorageRpcObjectReadAuthSubjectResponse {
            outcome: StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(subject.clone())),
        };
        let bytes = encode_object_read_auth_subject_response(&response);
        let decoded = decode_object_read_auth_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectReadAuthSubjectResponse {
            outcome: StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
        };
        let bytes = encode_object_read_auth_subject_response(&response);
        let decoded = decode_object_read_auth_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let snapshot_request = StorageRpcObjectReadSnapshotRequest {
            object: request.object,
            version_id: request.version_id,
            expected_identity: subject.identity,
            snapshot_mode: ObjectReadSnapshotMode::FullPayloadLayout,
        };
        let bytes = encode_object_read_snapshot_request(&snapshot_request);
        let decoded = decode_object_read_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, snapshot_request);

        let checksum = ChecksumBytes::new([1, 2, 3, 4]).unwrap();
        let object_segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            segment_index: 0,
            size: 12,
            segment_crc64: 98,
            segment_okh: [2; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 5,
            placement_cluster_epoch: ClusterEpoch::new(9).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let part = ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            part_number: 1,
            size: 12,
            payload_crc64: 99,
            etag: vec![8; 16],
            etag_kind: EtagKind::MultipartComposite,
            part_okh: [3; 16],
            part_vid: GenerationId::new(11).unwrap(),
            placement_cluster_epoch: ClusterEpoch::new(10).unwrap(),
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 5,
            checksum: Some(checksum),
        };
        let segment = MultipartPartSegmentRecord {
            bucket,
            key,
            upload_id: UploadId::try_from("u".repeat(128)).unwrap(),
            version_id: 7,
            part_number: 1,
            segment_index: 0,
            size: 12,
            segment_crc64: 99,
            segment_okh: [4; 16],
            segment_vid: GenerationId::new(12).unwrap(),
            data_pg_id: 5,
            placement_cluster_epoch: ClusterEpoch::new(11).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let response = StorageRpcObjectReadSnapshotResponse {
            outcome: StorageRpcObjectReadSnapshotOutcome::Loaded(Box::new(ObjectReadSnapshot {
                stored,
                object_segments: vec![object_segment],
                multipart_parts: vec![part],
                multipart_part_segments: vec![segment],
            })),
        };
        let bytes = encode_object_read_snapshot_response(&response);
        let decoded = decode_object_read_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectReadSnapshotResponse {
            outcome: StorageRpcObjectReadSnapshotOutcome::StaleSubject,
        };
        let bytes = encode_object_read_snapshot_response(&response);
        let decoded = decode_object_read_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let tags_request = StorageRpcObjectTagsForSubjectRequest {
            object: snapshot_request.object,
            version_id: snapshot_request.version_id,
            expected_identity: snapshot_request.expected_identity,
            authorized_version_id: VersionId::from_u64(7),
        };
        let bytes = encode_object_tags_for_subject_request(&tags_request);
        let decoded = decode_object_tags_for_subject_request(&bytes).unwrap();
        assert_eq!(decoded, tags_request);

        let response = StorageRpcObjectTagsForSubjectResponse {
            outcome: StorageRpcObjectTagsForSubjectOutcome::Loaded(Some(
                "<Tagging><TagSet/></Tagging>".to_string(),
            )),
        };
        let bytes = encode_object_tags_for_subject_response(&response);
        let decoded = decode_object_tags_for_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectTagsForSubjectResponse {
            outcome: StorageRpcObjectTagsForSubjectOutcome::StaleSubject,
        };
        let bytes = encode_object_tags_for_subject_response(&response);
        let decoded = decode_object_tags_for_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn direct_put_commit_snapshot_request_and_response_round_trip() {
        let request = StorageRpcDirectPutCommitSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("bucket").unwrap(),
                key: ObjectKey::try_from("key").unwrap(),
            },
            reservation_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            generation_id: GenerationId::new(9).unwrap(),
        };

        let bytes = encode_direct_put_commit_snapshot_request(&request);
        let decoded = decode_direct_put_commit_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = StorageRpcDirectPutCommitSnapshotResponse {
            snapshot: DirectPutCommitStorageSnapshot {
                auth_snapshot: crate::DirectPutCommitSnapshot {
                    existing_etag: Some("\"0123456789abcdef\"".to_string()),
                },
                current: None,
                stale_payload_source: None,
                stale_payload: None,
            },
        };
        let bytes = encode_direct_put_commit_snapshot_response(&response);
        let decoded = decode_direct_put_commit_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn direct_put_command_build_request_and_stale_response_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let reservation_id = SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap();
        let owner = OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "direct-put".to_string(),
            created_at: 123,
            lease_deadline: None,
            target_context: Some("key".to_string()),
        };
        let commit = CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_reservation_id: reservation_id,
            versioning: BucketVersioningState::Suspended,
            owner,
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(9).unwrap(),
            size: 12,
            etag_crc64: 99,
            ec: EcShape { k: 4, m: 2 },
            tags: Some(SerializedTagSet::new("<Tagging/>".to_string())),
            metadata_blob: SerializedMetadataBlob::new(vec![1, 2, 3]),
            system_metadata_blob: SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
            segment_index: 0,
            segment_crc64: 99,
            segment_okh: [7; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 3,
            bucket_write_reservation: proof.clone(),
        };
        let request = StorageRpcDirectPutCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            request: commit,
            version_id: VersionId::Null,
            expected_snapshot: DirectPutCommitStorageSnapshot {
                auth_snapshot: crate::DirectPutCommitSnapshot {
                    existing_etag: None,
                },
                current: None,
                stale_payload_source: None,
                stale_payload: None,
            },
            bucket_write_reservation: proof,
        };

        let bytes = encode_direct_put_command_build_request(&request).unwrap();
        let decoded = decode_direct_put_command_build_request(&bytes).unwrap();
        assert_eq!(decoded.object, request.object);
        assert_eq!(decoded.request.bucket, bucket);
        assert_eq!(decoded.request.key, key);
        assert_eq!(decoded.request.generation_id, request.request.generation_id);
        assert_eq!(decoded.request.segment_okh, request.request.segment_okh);
        assert_eq!(
            decoded.bucket_write_reservation,
            request.bucket_write_reservation
        );

        let mut wrong_proof_request = request.clone();
        wrong_proof_request.bucket_write_reservation.bucket =
            BucketName::try_from("other-bucket").unwrap();
        assert_eq!(
            encode_direct_put_command_build_request(&wrong_proof_request),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "bucket write reservation proof must match direct PUT bucket"
            ))
        );

        let response = StorageRpcDirectPutCommandBuildResponse {
            outcome: StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
        };
        let bytes = encode_direct_put_command_build_response(&response);
        let decoded = decode_direct_put_command_build_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcDirectPutCommandBuildResponse {
            outcome: StorageRpcDirectPutCommandBuildOutcome::LogConflict {
                node_id: 7,
                pg_id: 3,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 9,
            },
        };
        let bytes = encode_direct_put_command_build_response(&response);
        let decoded = decode_direct_put_command_build_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn object_metadata_command_build_response_round_trips_log_conflict() {
        let response = StorageRpcObjectMetadataCommandBuildResponse {
            outcome: StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id: 7,
                pg_id: 3,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 9,
            },
        };

        let bytes = encode_object_metadata_command_build_response(&response);
        let decoded = decode_object_metadata_command_build_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn completed_multipart_order_command_build_request_and_response_round_trip() {
        let bucket = BucketName::try_from("completed-order-bucket").unwrap();
        let command_id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let request = StorageRpcCompletedMultipartOrderCommandBuildRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: bucket.clone(),
            command_id,
        };

        let bytes = encode_completed_multipart_order_command_build_request(&request).unwrap();
        let decoded = decode_completed_multipart_order_command_build_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let wrong_route = StorageRpcCompletedMultipartOrderCommandBuildRequest {
            pg_id: PgId::new(4),
            ..request.clone()
        };
        assert_eq!(
            encode_completed_multipart_order_command_build_request(&wrong_route),
            Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "command id route must match request route"
            ))
        );

        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: bucket.clone(),
                    completion_order: 11,
                },
            ),
        );
        let response = StorageRpcCompletedMultipartOrderCommandBuildResponse {
            completion_order: 11,
            command,
        };
        let bytes = encode_completed_multipart_order_command_build_response(&response);
        let decoded = decode_completed_multipart_order_command_build_response(&bytes).unwrap();
        assert_eq!(decoded.completion_order, 11);
        assert_eq!(decoded.command, response.command);
    }

    #[test]
    fn object_version_response_round_trip_rejects_null_version() {
        let response = StorageRpcObjectVersionResponse {
            version_id: VersionId::from_u64(42),
        };
        let bytes = encode_object_version_response(&response);
        let decoded = decode_object_version_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        assert_eq!(
            decode_object_version_response(&0_u64.to_be_bytes()),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "object version response must not contain null version"
            ))
        );
    }

    #[test]
    fn user_checksum_metadata_survives_rpc_payload_round_trip() {
        let checksum = ChecksumBytes::new([1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let payload = encode_optional_checksum_metadata(Some(&checksum));
        let frame =
            encode_storage_rpc_frame(3, StorageRpcMessageKind::MetadataCommand, &payload).unwrap();
        let decoded_frame = decode_storage_rpc_frame(&frame).unwrap();
        let decoded_checksum = decode_optional_checksum_metadata(&decoded_frame.payload).unwrap();

        assert_eq!(decoded_checksum, Some(checksum));
    }

    fn test_metadata_command() -> MetadataCommandEnvelope {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let command = CreateBucketCommand::from_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command))
    }

    fn test_mark_bucket_deleting_command(command_id: MetadataCommandId) -> MetadataCommandEnvelope {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let bucket = crate::metadata_command::BucketRecord::from_create_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                bucket,
            )),
        )
    }

    fn test_bucket_info(name: &str) -> BucketInfo {
        BucketInfo {
            name: BucketName::try_from(name).unwrap(),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 123,
            region: 7,
            state: BucketState::Active,
            versioning: BucketVersioningState::Enabled,
            object_lock: BucketObjectLockConfig::default(),
            acl_grants: AclGrants::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: true,
            bucket_policy_public: false,
            bucket_policy_generation: 11,
            bucket_lifecycle_present: true,
            bucket_lifecycle_generation: 13,
            bucket_execution_generation: 17,
            bucket_incarnation_generation: 19,
            bucket_abac_enabled: true,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }
    }

    fn test_shard_location(shard_index: u8) -> ShardLocation {
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(11)),
            ShardIndex::new(shard_index),
            NodeId::new(u32::from(shard_index) + 100),
        )
    }

    fn test_shard_location_for_data_pg(data_pg_id: usize) -> ShardLocation {
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(
                u32::try_from(data_pg_id).expect("test PG id fits in u32"),
            )),
            ShardIndex::new(0),
            NodeId::new(100),
        )
    }

    fn test_shard_key(shard_index: u8) -> ShardKey {
        ShardKey::new(&[0x42; 16], 77, shard_index)
    }

    fn test_claim_token() -> StorageRpcDurableClaimToken {
        StorageRpcDurableClaimToken::LifecycleSweep(StorageRpcBucketClaimToken {
            bucket: BucketName::try_from("bucket-claim").unwrap(),
            bucket_incarnation_generation: 17,
            claim_id: "claim-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: 23,
        })
    }

    fn test_object_reclaim_claim_token() -> StorageRpcDurableClaimToken {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(
            StorageRpcObjectPayloadReclaimClaimToken {
                bucket: BucketName::try_from("bucket-reclaim").unwrap(),
                bucket_incarnation_generation: 17,
                key: ObjectKey::try_from("key").unwrap(),
                generation_id: GenerationId::new(19).unwrap(),
                reclaim_kind: ObjectPayloadReclaimKind::Multipart,
                claim_id: "claim-id".to_string(),
                owner_token: "owner-token".to_string(),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: 23,
            },
        )
    }

    fn test_bucket_write_reservation_proof() -> BucketWriteReservationProof {
        BucketWriteReservationProof {
            bucket: BucketName::try_from("bucket-proof").unwrap(),
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 31,
            bucket_incarnation_generation: 37,
            operation_kind: "put-object".to_string(),
            created_at: 41,
            lease_deadline: Some(43),
            target_context: Some("key/context".to_string()),
        }
    }
}
