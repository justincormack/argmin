// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cluster::ShardLocation,
    control_plane::{CanonicalStateDigest, MetadataCommandLogHash},
    metadata_command::{
        decode_metadata_command_envelope, validate_metadata_command_envelope_bytes,
        BucketPropertyMutation, BucketSubresourceMutation, BucketWriteReservationProof,
        CreateMultipartUploadCommand, CreateStreamUploadCommand, DeleteObjectVersionTarget,
        MetadataCommandAcceptance, MetadataCommandLogHashRangeEntry, MetadataCommandLogIndex,
        MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind,
        MetadataCommandReplicaState, MetadataTransferCommand, ObjectPayloadReclaimClaimProof,
        ObjectPayloadReclaimCommand, PutObjectMetadataMutation,
        COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    },
    node_runtime::MetadataCommandDecodeAuthority,
    pg_store::{
        decode_staging_intent, encode_staging_intent, MetadataCheckpointRow,
        MetadataCheckpointTableBlock, MetadataCheckpointTableDigest, MetadataCheckpointValue,
        MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus,
        MetadataTransferStagingIntent, MetadataTransferStagingReceipt,
        PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
        PgClusterMapHistoryRouteReferences, ScavengerShardFile, ScavengerShardFileScan,
        ScavengerShardRow, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES, MAX_STAGING_EVIDENCE_BYTES,
        MAX_STAGING_INTENT_BYTES, METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION,
        METADATA_COMMAND_CHECKPOINT_MAGIC, METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
    },
    types::{
        AbortMultipartUploadCleanup, AbortingMultipartUploadBucketWitness, BucketAclSummary,
        BucketDeleteAttemptOutcomeKind, BucketDeleteAttemptOutcomeRecord, BucketDeleteAttemptPhase,
        BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketEncryptionConfig,
        BucketFastPathIdentity, BucketInfo, BucketObjectOwnership, BucketOwnershipControls,
        BucketSnapshot, BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState,
        BucketSubresourceAux, BucketSubresourceKind, BucketWriteDrainRecord, BucketWriteDrainState,
        BucketWriteReservationRecord, ChecksumAlgorithm, ChecksumBytes, ChecksumType, ClusterEpoch,
        CommitDirectPutObjectReq, CompleteMultipartCommitCleanup, CompleteMultipartCommitRequest,
        CreateBucketConfig, CreateMultipartUploadReq, CreateStreamUploadReq, DeleteMarkerRecord,
        DirectPutCommitStorageSnapshot, EcShape, EffectiveBucketEncryptionConfig, EtagKind,
        GenerationId, LifecycleSweepClaimRecord, LifecycleSweepRoot, LifecycleSweepRootSource,
        ListMultipartUploadsPageStart, ListMultipartUploadsReq, ListMultipartUploadsResp,
        ListObjectVersionsReq, ListObjectVersionsResp, ListObjectsReq, ListObjectsResp,
        ListPartsResp, ListedMultipartParts, LiveObjectRecord, LoadedBucketSubresource,
        ManagedEncryptionAlgorithm, MultipartChecksumConfig, MultipartCompletionFingerprint,
        MultipartCompletionPreflight, MultipartCompletionReplay, MultipartCompletionSnapshot,
        MultipartCompletionSubject, MultipartObjectIdentity, MultipartPartRecord,
        MultipartPartSegmentRecord, MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord,
        MultipartReclaimRecord, MultipartUploadIdKey, MultipartUploadManagementLookup,
        MultipartUploadRecord, ObjectEncryption, ObjectEncryptionType, ObjectEtag, ObjectKey,
        ObjectLayout, ObjectLockState, ObjectPartRecord, ObjectPayloadReclaimClaimRecord,
        ObjectPayloadReclaimKind, ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity,
        ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectRetention, ObjectSegmentRecord,
        ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord, OwnerIdentity,
        PayloadReclaimRoot, PgId, PlacedSegmentShardBackfillClaimRecord,
        PlacedSegmentShardBackfillRecord, PlacedSegmentShardBackfillWorkItem,
        PlacedSegmentShardRepairClaimRecord, PlacedSegmentShardRepairRecord,
        PlacedSegmentShardRepairWorkItem, PrepareStreamUploadSegmentAppendReq,
        PublicAccessBlockConfig, SegmentStoredBytesRequest, SerializedBucketTagSet,
        SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet, SessionId,
        ShardIndex, ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
        ShardScavengerObservationReason, ShardScavengerObservationRecord,
        ShardScavengerPayloadReference, ShardScavengerPlacedShardSetReference,
        ShardScavengerReclaimShardSetReference, ShardScavengerReferenceCursor,
        ShardScavengerReferencePage, ShardScavengerReferencePageItem, StorageClass,
        StoredLegalHoldStatus, StoredObject, StreamPutCommitInput,
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
        SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT, UPLOAD_ID_LEN,
    },
    BucketDeleteBeginRoot, BucketName, NodeId,
};
use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, RetentionPeriod, StoredAclGrants,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::num::{NonZeroU16, NonZeroU32};

include!("storage_rpc/limits.rs");

include!("storage_rpc/types.rs");

include!("storage_rpc/frame.rs");

include!("storage_rpc/operation_codec.rs");

include!("storage_rpc/metadata_command_codec.rs");

include!("storage_rpc/shard_maintenance_codec.rs");

include!("storage_rpc/decoder.rs");

include!("storage_rpc/encoder.rs");

include!("storage_rpc/tests.rs");
