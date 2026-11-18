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
        MetadataCheckpointRow, MetadataCheckpointTableBlock, MetadataCheckpointTableDigest,
        MetadataCheckpointValue, MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus,
        PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
        PgClusterMapHistoryRouteReferences, ScavengerShardFile, ScavengerShardFileScan,
        ScavengerShardRow, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
        METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION, METADATA_COMMAND_CHECKPOINT_MAGIC,
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
        PublicAccessBlockConfig, SegmentShardIntegrityRecord, SegmentStoredBytesRequest,
        SerializedBucketTagSet, SerializedMetadataBlob, SerializedSystemMetadataBlob,
        SerializedTagSet, SessionId, ShardIndex, ShardKey, ShardScavengerObservation,
        ShardScavengerObservationKey, ShardScavengerObservationReason,
        ShardScavengerObservationRecord, ShardScavengerPayloadReference,
        ShardScavengerPlacedShardSetReference, ShardScavengerProtectedShardSetReference,
        ShardScavengerReferenceCursor, ShardScavengerReferencePage,
        ShardScavengerReferencePageItem, StorageClass, StoredLegalHoldStatus, StoredObject,
        StreamPutCommitInput, StreamPutFinalizeStorageSnapshot, StreamUploadPartSnapshot,
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

// Candidate complete-context framing; live dispatch still selects the current
// command grammar. This module returns only borrowed, untrusted wire records.
#[allow(dead_code)] // Candidate complete application; live dispatch remains gated.
#[path = "storage_rpc/complete_application.rs"]
pub(crate) mod complete_application;
#[allow(dead_code)]
#[path = "storage_rpc/complete_applied.rs"]
pub(crate) mod complete_applied;
#[allow(dead_code)] // Candidate framing only; admitted installation remains gated.
pub(crate) mod complete_direct_install;
#[allow(dead_code)]
#[path = "storage_rpc/complete_direct_introduction.rs"]
pub(crate) mod complete_direct_introduction;
#[path = "storage_rpc/complete_direct_introduction_install.rs"]
pub(crate) mod complete_direct_introduction_install;
#[allow(dead_code)] // Candidate sender/receiver; live dispatch remains gated.
pub(crate) mod complete_direct_prepare;
#[allow(dead_code)] // Candidate sender/receiver; live dispatch remains gated.
pub(crate) mod complete_direct_snapshot;
#[allow(dead_code)] // Selected at coordinated complete-command activation.
#[path = "storage_rpc/complete_log_budget.rs"]
pub(crate) mod complete_log_budget;
#[allow(dead_code)]
#[path = "storage_rpc/complete_log_range.rs"]
pub(crate) mod complete_log_range;
#[allow(dead_code)]
#[path = "storage_rpc/complete_pending.rs"]
pub(crate) mod complete_pending;
#[allow(dead_code)] // Candidate payload grammar; not selected by live RPC dispatch.
#[path = "storage_rpc/complete_publication.rs"]
pub(crate) mod complete_publication;
pub(crate) mod complete_settled_prefix;
#[allow(dead_code)] // Candidate framing only; receiver and owning sender remain gated.
pub(crate) mod complete_staging_ack;
pub(crate) mod complete_staging_file;
pub(crate) mod complete_stream_allocation;
pub(crate) mod complete_stream_append_install;
#[allow(dead_code)] // Candidate framing; owning sender and admitted receiver are not installed.
pub(crate) mod complete_stream_append_prepare;
pub(crate) mod complete_stream_file_primary;
pub(crate) mod complete_window_primary;

#[path = "storage_rpc/complete_abandonment.rs"]
pub(crate) mod complete_abandonment;
#[path = "storage_rpc/complete_replica_abandonment.rs"]
#[allow(dead_code)] // Candidate framing; live sender/dispatch not selected.
pub(crate) mod complete_replica_abandonment;
#[allow(dead_code)] // Candidate receiver framing; live dispatch is not selected.
#[path = "storage_rpc/complete_retirement.rs"]
pub(crate) mod complete_retirement;

include!("storage_rpc/shard_maintenance_codec.rs");

include!("storage_rpc/decoder.rs");

include!("storage_rpc/encoder.rs");

include!("storage_rpc/tests.rs");

#[cfg(test)]
#[path = "storage_rpc/stream_creation_format_tests.rs"]
mod stream_creation_format_tests;

#[cfg(test)]
#[path = "storage_rpc/stream_append_format_tests.rs"]
mod stream_append_format_tests;

#[cfg(test)]
#[path = "storage_rpc/shard_ack_format_tests.rs"]
mod shard_ack_format_tests;

#[cfg(test)]
#[path = "storage_rpc/bucket_reservation_format_tests.rs"]
mod bucket_reservation_format_tests;

#[cfg(test)]
#[path = "storage_rpc/checkpoint_transfer_format_tests.rs"]
mod checkpoint_transfer_format_tests;

#[cfg(test)]
mod staging_scavenger_format_tests;

#[cfg(test)]
mod retained_log_budget_format_tests;
