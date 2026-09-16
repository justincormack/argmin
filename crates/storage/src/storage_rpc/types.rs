// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

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
    MetadataCommandRecoveryApplyAndRecord = 162,
    MetadataCommandRecoveryPendingSlotReplace = 163,
    MetadataCommandRecoveryRecordAbandoned = 168,
    MetadataCommandPublicationStart = 172,
    PlacedSegmentBackfillReferencePage = 169,
    ShardScavengerReferencePage = 174,
    MetadataTransferStagingIntentCreate = 175,
    MetadataTransferStagingArtifactPublish = 176,
    MetadataTransferStagingProofPublish = 177,
    MetadataTransferStagingTombstone = 178,
    ShardScavengerReferenceMatch = 173,
    ObjectPayloadReclaimCommandBuild = 170,
    MetadataCommandRetainedAbortApply = 165,
    MetadataCommandRetainedAbortFinish = 166,
    BucketDeleteReplicaHead = 167,
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
    DirectPutCommitSnapshotLoad = 43,
    DirectPutCommitCommandBuild = 44,
    MultipartCompletionBarrierCommandBuild = 45,
    ObjectReadAuthSubjectLoad = 46,
    ObjectReadSnapshotLoad = 47,
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
    ObjectStreamUploadRetainedAbortPrepare = 164,
    ObjectAbortingMultipartUploadBucketsList = 171,
    ObjectStreamUploadSegmentsLoad = 78,
    ObjectStreamSegmentAppendPrepare = 79,
    ObjectStreamUploadBucketWriteReservationUpdate = 84,
    BucketWriteDrainBegin = 80,
    BucketWriteDrainClear = 81,
    BucketWriteDrainClearExpired = 82,
    BucketWriteReservationsList = 83,
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
    ObjectPayloadReclaimClaimGet = 160,
    ObjectPayloadLeaseControl = 161,
    BucketWriteDrainGet = 155,
    BucketDeleteAttemptOutcomeRecord = 156,
    BucketDeleteAttemptOutcomeGet = 157,
    BucketDeleteBeginRoots = 158,
    BucketDeleteFinalizeClaimGet = 159,
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

/// Opaque diagnostic identifying a failure reported by a storage node.
///
/// Its wire representation and concrete protocol codes are private to the
/// storage crate. Callers use [`crate::StorageNodeFailureClass`] when they need
/// a semantic classification.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageNodeFailure(StorageRpcWireErrorCode);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum StorageRpcWireErrorCode {
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
    TransportTimeout = 22,
    ShardIntegrity = 23,
    TransportClosed = 24,
    MultipartConditionalRequestConflict = 25,
    MetadataCommandIntegrity = 26,
    MetadataCommandMutationUncertain = 27,
    StagingAuthorizationNotObserved = 28,
}

pub(crate) type StorageRpcErrorCode = StorageNodeFailure;

#[allow(non_upper_case_globals)]
impl StorageNodeFailure {
    pub(crate) const FrameDecode: Self = Self(StorageRpcWireErrorCode::FrameDecode);
    pub(crate) const PayloadDecode: Self = Self(StorageRpcWireErrorCode::PayloadDecode);
    pub(crate) const UnknownNode: Self = Self(StorageRpcWireErrorCode::UnknownNode);
    pub(crate) const UnknownPg: Self = Self(StorageRpcWireErrorCode::UnknownPg);
    pub(crate) const WrongClusterEpoch: Self = Self(StorageRpcWireErrorCode::WrongClusterEpoch);
    pub(crate) const InactivePgRoute: Self = Self(StorageRpcWireErrorCode::InactivePgRoute);
    pub(crate) const StaleShardLocation: Self = Self(StorageRpcWireErrorCode::StaleShardLocation);
    pub(crate) const NonActingSetAccess: Self = Self(StorageRpcWireErrorCode::NonActingSetAccess);
    pub(crate) const UnsupportedOperation: Self =
        Self(StorageRpcWireErrorCode::UnsupportedOperation);
    pub(crate) const Internal: Self = Self(StorageRpcWireErrorCode::Internal);
    pub(crate) const ResourceExhausted: Self = Self(StorageRpcWireErrorCode::ResourceExhausted);
    pub(crate) const ReclaimClaimNotFound: Self =
        Self(StorageRpcWireErrorCode::ReclaimClaimNotFound);
    pub(crate) const ShardDeleteInProgress: Self =
        Self(StorageRpcWireErrorCode::ShardDeleteInProgress);
    pub(crate) const BucketWriteDrainConflict: Self =
        Self(StorageRpcWireErrorCode::BucketWriteDrainConflict);
    pub(crate) const BucketWriteDrainNotFound: Self =
        Self(StorageRpcWireErrorCode::BucketWriteDrainNotFound);
    pub(crate) const ReclaimClaimConflict: Self =
        Self(StorageRpcWireErrorCode::ReclaimClaimConflict);
    pub(crate) const NotFound: Self = Self(StorageRpcWireErrorCode::NotFound);
    pub(crate) const BucketWriteReservationConflict: Self =
        Self(StorageRpcWireErrorCode::BucketWriteReservationConflict);
    pub(crate) const BucketWriteReservationNotFound: Self =
        Self(StorageRpcWireErrorCode::BucketWriteReservationNotFound);
    pub(crate) const MetadataCommandContention: Self =
        Self(StorageRpcWireErrorCode::MetadataCommandContention);
    pub(crate) const MetadataTransferHistoricalRouteActive: Self =
        Self(StorageRpcWireErrorCode::MetadataTransferHistoricalRouteActive);
    pub(crate) const TransportTimeout: Self = Self(StorageRpcWireErrorCode::TransportTimeout);
    pub(crate) const ShardIntegrity: Self = Self(StorageRpcWireErrorCode::ShardIntegrity);
    pub(crate) const TransportClosed: Self = Self(StorageRpcWireErrorCode::TransportClosed);
    pub(crate) const MultipartConditionalRequestConflict: Self =
        Self(StorageRpcWireErrorCode::MultipartConditionalRequestConflict);
    pub(crate) const MetadataCommandIntegrity: Self =
        Self(StorageRpcWireErrorCode::MetadataCommandIntegrity);
    pub(crate) const MetadataCommandMutationUncertain: Self =
        Self(StorageRpcWireErrorCode::MetadataCommandMutationUncertain);
    pub(crate) const StagingAuthorizationNotObserved: Self =
        Self(StorageRpcWireErrorCode::StagingAuthorizationNotObserved);

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
            22 => Ok(Self::TransportTimeout),
            23 => Ok(Self::ShardIntegrity),
            24 => Ok(Self::TransportClosed),
            25 => Ok(Self::MultipartConditionalRequestConflict),
            26 => Ok(Self::MetadataCommandIntegrity),
            27 => Ok(Self::MetadataCommandMutationUncertain),
            28 => Ok(Self::StagingAuthorizationNotObserved),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown storage RPC error code",
            )),
        }
    }

    fn as_u16(self) -> u16 {
        self.0 as u16
    }

    pub(crate) fn wire_code(self) -> StorageRpcWireErrorCode {
        self.0
    }
}

impl std::fmt::Debug for StorageNodeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StorageNodeFailure")
    }
}

impl std::fmt::Display for StorageNodeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("storage-node failure")
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
            Self::PlacedSegmentBackfillReferencePage => {
                "placed segment backfill reference page"
            }
            Self::ShardScavengerReferencePage => "shard scavenger reference page",
            Self::MetadataTransferStagingIntentCreate => {
                "metadata transfer staging intent create"
            }
            Self::MetadataTransferStagingArtifactPublish => {
                "metadata transfer staging artifact publish"
            }
            Self::MetadataTransferStagingProofPublish => {
                "metadata transfer staging proof publish"
            }
            Self::MetadataTransferStagingTombstone => {
                "metadata transfer staging tombstone"
            }
            Self::ShardScavengerReferenceMatch => "shard scavenger reference match",
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
            Self::MetadataCommandRecoveryApplyAndRecord => {
                "metadata command recovery apply and record"
            }
            Self::MetadataCommandRecoveryPendingSlotReplace => {
                "metadata command recovery pending slot replace"
            }
            Self::MetadataCommandRecoveryRecordAbandoned => {
                "metadata command recovery record abandoned"
            }
            Self::MetadataCommandPublicationStart => "metadata command publication start",
            Self::MetadataCommandRetainedAbortApply => {
                "metadata command retained stream abort apply"
            }
            Self::MetadataCommandRetainedAbortFinish => {
                "metadata command retained stream abort finish"
            }
            Self::BucketDeleteReplicaHead => "bucket delete replica head",
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
            Self::DirectPutCommitSnapshotLoad => "direct PUT commit snapshot load",
            Self::DirectPutCommitCommandBuild => "direct PUT commit command build",
            Self::MultipartCompletionBarrierCommandBuild => {
                "multipart completion barrier command build"
            }
            Self::ObjectReadAuthSubjectLoad => "object read auth subject load",
            Self::ObjectReadSnapshotLoad => "object read snapshot load",
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
            Self::ObjectPayloadReclaimCommandBuild => "object payload reclaim command build",
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
            Self::ObjectStreamUploadRetainedAbortPrepare => {
                "object stream upload retained abort prepare"
            }
            Self::ObjectAbortingMultipartUploadBucketsList => {
                "object aborting multipart upload buckets list"
            }
            Self::ObjectStreamUploadSegmentsLoad => "object stream upload segments load",
            Self::ObjectStreamSegmentAppendPrepare => "object stream segment append prepare",
            Self::ObjectStreamUploadBucketWriteReservationUpdate => {
                "object stream upload bucket write reservation update"
            }
            Self::BucketWriteDrainBegin => "bucket write drain begin",
            Self::BucketWriteDrainClear => "bucket write drain clear",
            Self::BucketWriteDrainClearExpired => "bucket write drain clear expired",
            Self::BucketWriteDrainGet => "bucket write drain get",
            Self::BucketDeleteAttemptOutcomeRecord => "bucket delete attempt outcome record",
            Self::BucketDeleteAttemptOutcomeGet => "bucket delete attempt outcome get",
            Self::BucketWriteDrainHeartbeat => "bucket write drain heartbeat",
            Self::BucketWriteDrainExists => "bucket write drain exists",
            Self::BucketWriteReservationsList => "bucket write reservations list",
            Self::BucketDeleteFinalizeRoots => "bucket delete finalize roots",
            Self::BucketDeleteBeginRoots => "bucket delete begin roots",
            Self::BucketDeleteFinalizeClaimGet => "bucket delete finalize claim get",
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
            Self::ObjectPayloadReclaimExists => "object payload reclaim exists",
            Self::ObjectBucketPayloadReclaimRoot => "object bucket payload reclaim root",
            Self::ObjectPayloadReclaimRoot => "object payload reclaim root",
            Self::ObjectPayloadReclaimLoad => "object payload reclaim load",
            Self::ObjectPayloadReclaimClaimAcquire => "object payload reclaim claim acquire",
            Self::ObjectPayloadReclaimClaimRelease => "object payload reclaim claim release",
            Self::ObjectPayloadReclaimClaimGet => "object payload reclaim claim get",
            Self::ObjectPayloadLeaseControl => "object payload lease control",
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

    pub(crate) fn from_u16(value: u16) -> Result<Self, StorageRpcFrameError> {
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
            162 => Ok(Self::MetadataCommandRecoveryApplyAndRecord),
            163 => Ok(Self::MetadataCommandRecoveryPendingSlotReplace),
            168 => Ok(Self::MetadataCommandRecoveryRecordAbandoned),
            172 => Ok(Self::MetadataCommandPublicationStart),
            169 => Ok(Self::PlacedSegmentBackfillReferencePage),
            174 => Ok(Self::ShardScavengerReferencePage),
            175 => Ok(Self::MetadataTransferStagingIntentCreate),
            176 => Ok(Self::MetadataTransferStagingArtifactPublish),
            177 => Ok(Self::MetadataTransferStagingProofPublish),
            178 => Ok(Self::MetadataTransferStagingTombstone),
            173 => Ok(Self::ShardScavengerReferenceMatch),
            170 => Ok(Self::ObjectPayloadReclaimCommandBuild),
            165 => Ok(Self::MetadataCommandRetainedAbortApply),
            166 => Ok(Self::MetadataCommandRetainedAbortFinish),
            167 => Ok(Self::BucketDeleteReplicaHead),
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
            43 => Ok(Self::DirectPutCommitSnapshotLoad),
            44 => Ok(Self::DirectPutCommitCommandBuild),
            45 => Ok(Self::MultipartCompletionBarrierCommandBuild),
            46 => Ok(Self::ObjectReadAuthSubjectLoad),
            47 => Ok(Self::ObjectReadSnapshotLoad),
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
            164 => Ok(Self::ObjectStreamUploadRetainedAbortPrepare),
            171 => Ok(Self::ObjectAbortingMultipartUploadBucketsList),
            78 => Ok(Self::ObjectStreamUploadSegmentsLoad),
            79 => Ok(Self::ObjectStreamSegmentAppendPrepare),
            84 => Ok(Self::ObjectStreamUploadBucketWriteReservationUpdate),
            80 => Ok(Self::BucketWriteDrainBegin),
            81 => Ok(Self::BucketWriteDrainClear),
            82 => Ok(Self::BucketWriteDrainClearExpired),
            83 => Ok(Self::BucketWriteReservationsList),
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
            159 => Ok(Self::BucketDeleteFinalizeClaimGet),
            160 => Ok(Self::ObjectPayloadReclaimClaimGet),
            161 => Ok(Self::ObjectPayloadLeaseControl),
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
    #[error("unknown metadata checkpoint magic")]
    UnknownMetadataCheckpointMagic,
    #[error("unsupported metadata checkpoint encoding version {actual}")]
    UnsupportedMetadataCheckpointEncodingVersion { actual: u16 },
    #[error("unsupported metadata proof carrier: {0}")]
    UnsupportedMetadataProofCarrier(&'static str),
    #[error("invalid bucket metadata request: {0}")]
    InvalidBucketMetadataRequest(&'static str),
    #[error("invalid metadata-transfer staging request: {0}")]
    InvalidMetadataTransferStaging(&'static str),
    #[error("invalid object metadata request: {0}")]
    InvalidObjectMetadataRequest(&'static str),
    #[error("shard write size mismatch: expected {expected}, actual {actual}")]
    ShardWriteSizeMismatch { expected: u64, actual: u64 },
    #[error("shard write checksum mismatch")]
    ShardWriteChecksumMismatch,
    #[error("shard location shard index does not match shard key")]
    ShardLocationMismatch,
    #[error("invalid shard write request: {0}")]
    InvalidShardWriteRequest(&'static str),
    #[error("invalid read handle acquire request: {0}")]
    InvalidReadHandleAcquireRequest(&'static str),
    #[error("invalid read handle release request: {0}")]
    InvalidReadHandleReleaseRequest(&'static str),
    #[error("invalid shard ack batch request: {0}")]
    InvalidShardAckBatchRequest(&'static str),
    #[error("invalid durable claim token: {0}")]
    InvalidDurableClaimToken(&'static str),
    #[error("invalid cluster-map history route reference: {0}")]
    InvalidClusterMapHistoryRouteReference(&'static str),
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
pub(crate) struct StorageRpcMetadataCommandRecoveryRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) authorized_source: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) abandoned_source: Option<crate::metadata_command::MetadataCommandEnvelope>,
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
pub(crate) struct StorageRpcShardScavengerReferencePageRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) after: Option<ShardScavengerReferenceCursor>,
    pub(crate) limit: NonZeroU16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardScavengerReferenceMatchRequest {
    pub(crate) route: StorageRpcBucketPgRequest,
    pub(crate) cursor: ShardScavengerReferenceCursor,
    pub(crate) expected: ShardScavengerPlacedShardSetReference,
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
    pub(crate) lease_deadline: u64,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) record: BucketWriteDrainRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteDrainHeartbeatRequest {
    pub(crate) node_id: NodeId,
    pub(crate) route_cluster_epoch: ClusterEpoch,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum StorageRpcObjectPayloadLeaseControlOperation {
    Acquire = 1,
    Release = 2,
    ReclaimBegin = 3,
    ReclaimFinish = 4,
    ReclaimFinishKeepFence = 5,
    ReclaimFenceClear = 6,
    Count = 7,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadLeaseControlRequest {
    pub(crate) node_id: NodeId,
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) generation_id: GenerationId,
    pub(crate) operation: StorageRpcObjectPayloadLeaseControlOperation,
    pub(crate) reclaim_authority: Option<ObjectPayloadReclaimClaimProof>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadLeaseControlResponse {
    pub(crate) value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimResponse {
    pub(crate) reclaim: Option<ObjectPayloadReclaimCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcObjectPayloadReclaimCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) generation_id: GenerationId,
    pub(crate) payload: ObjectPayloadReclaimCommand,
    pub(crate) claim: ObjectPayloadReclaimClaimRecord,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
pub(crate) struct StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) session_id: SessionId,
    pub(crate) current: BucketWriteReservationProof,
    pub(crate) renewed: BucketWriteReservationProof,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
pub(crate) struct StorageRpcStreamSegmentAppendPrepareRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: PrepareStreamUploadSegmentAppendReq,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
    pub(crate) cleanup_after: Option<u64>,
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
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPartFinalizeSnapshotRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
    pub(crate) session_id: SessionId,
    pub(crate) part_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcStreamPartFinalizeSnapshotOutcome {
    Loaded(Box<StreamUploadPartStorageSnapshot>),
    NoSuchUpload { upload_id: UploadId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcStreamPartFinalizeSnapshotResponse {
    pub(crate) outcome: StorageRpcStreamPartFinalizeSnapshotOutcome,
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
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcCompleteMultipartCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) request: CompleteMultipartCommitRequest,
    pub(crate) version_id: VersionId,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAbortMultipartCommandBuildRequest {
    pub(crate) object: StorageRpcObjectRequest,
    pub(crate) upload_id: UploadId,
    pub(crate) expected_cleanup: Option<AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
pub(crate) struct StorageRpcMultipartCompletionBarrierCommandBuildRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) bucket: BucketName,
    pub(crate) command_id: crate::metadata_command::MetadataCommandId,
    pub(crate) completion_target_context: String,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcMultipartCompletionBarrierCommandBuildResponse {
    pub(crate) barrier_sequence: u64,
    pub(crate) command: crate::metadata_command::MetadataCommandEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcBucketMetadataControlMutation {
    MarkDeleting,
    Versioning(BucketVersioningState),
    Acl {
        acl_grants: AclGrants,
        summary: BucketAclSummary,
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
pub(crate) enum StorageRpcBucketSubresourceGetOutcome {
    Loaded(Option<String>),
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketSubresourceGetResponse {
    pub(crate) outcome: StorageRpcBucketSubresourceGetOutcome,
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
    pub(crate) buckets: Vec<BucketInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcAbortingMultipartUploadBucketsResponse {
    pub(crate) witnesses: Vec<AbortingMultipartUploadBucketWitness>,
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
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    pub(crate) operation_deadline: Option<StorageRpcOperationDeadline>,
}

/// A transport-independent operation deadline for another process.
///
/// Monotonic clocks are process-local and never cross the RPC boundary. The
/// sender projects its local deadline onto this conservative wall deadline;
/// the receiver binds it once to its own monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageRpcOperationDeadline {
    pub(crate) portable_wall_valid_until_ms: u64,
}

impl StorageRpcOperationDeadline {
    pub(crate) fn from_instant(operation_deadline: Instant) -> Self {
        // Sample wall time before Instant. Descheduling between the samples
        // therefore shortens, rather than extends, the projected deadline.
        let sender_wall_ms = crate::clock::current_time_millis();
        let sender_instant = Instant::now();
        let remaining = operation_deadline.saturating_duration_since(sender_instant);
        let remaining_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
        Self::from_clock_samples(sender_wall_ms, remaining_ms)
    }

    fn from_clock_samples(sender_wall_ms: u64, remaining_ms: u64) -> Self {
        Self {
            portable_wall_valid_until_ms: sender_wall_ms
                .saturating_add(remaining_ms)
                .saturating_sub(
                    crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                ),
        }
    }

    pub(crate) fn local_deadline(self) -> Instant {
        // Sample monotonic time before wall time. Descheduling between the
        // samples therefore shortens the receiver's local deadline.
        let local_instant = Instant::now();
        let remaining_ms = self.remaining_on_receiver(crate::clock::current_time_millis());
        local_instant
            .checked_add(Duration::from_millis(remaining_ms))
            .unwrap_or(local_instant)
    }

    fn remaining_on_receiver(self, receiver_wall_ms: u64) -> u64 {
        self.portable_wall_valid_until_ms
            .saturating_sub(receiver_wall_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageRpcAdmittedRouteEffectDeadline {
    pub(crate) authority_valid_until_ms: u64,
    pub(crate) portable_wall_valid_until_ms: u64,
}

impl StorageRpcAdmittedRouteEffectDeadline {
    pub(crate) fn bounded_by_operation_deadline(
        existing: Option<Self>,
        operation_deadline: Instant,
    ) -> Self {
        Self::bounded_by_operation_deadline_with_clock_skew(
            existing,
            operation_deadline,
            crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
    }

    fn bounded_by_operation_deadline_with_clock_skew(
        existing: Option<Self>,
        operation_deadline: Instant,
        clock_skew_budget_ms: u64,
    ) -> Self {
        // Sample wall time first. A deschedule before the monotonic sample then
        // shortens the portable deadline instead of extending it.
        let sender_wall_ms = crate::clock::current_time_millis();
        let sender_monotonic_now = Instant::now();
        let remaining = operation_deadline.saturating_duration_since(sender_monotonic_now);
        Self::bounded_by_operation_remaining_with_clock_skew(
            existing,
            sender_wall_ms,
            remaining,
            clock_skew_budget_ms,
        )
    }

    pub(crate) fn bounded_by_operation_remaining(
        existing: Option<Self>,
        sender_wall_ms: u64,
        remaining: Duration,
    ) -> Self {
        Self::bounded_by_operation_remaining_with_clock_skew(
            existing,
            sender_wall_ms,
            remaining,
            crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
    }

    fn bounded_by_operation_remaining_with_clock_skew(
        existing: Option<Self>,
        sender_wall_ms: u64,
        remaining: Duration,
        clock_skew_budget_ms: u64,
    ) -> Self {
        // The receiving host may be behind the sender by the supported skew.
        // Subtract it here so rebinding the portable wall deadline cannot grant
        // more monotonic time than remained on the caller.
        let operation_wall_deadline = sender_wall_ms
            .saturating_add(u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX))
            .saturating_sub(clock_skew_budget_ms);
        Self::intersect_existing_operation_deadline(existing, operation_wall_deadline)
    }

    pub(crate) fn intersect_existing_operation_deadline(
        existing: Option<Self>,
        operation_wall_deadline: u64,
    ) -> Self {
        match existing {
            Some(existing) => Self {
                authority_valid_until_ms: existing.authority_valid_until_ms,
                portable_wall_valid_until_ms: existing
                    .portable_wall_valid_until_ms
                    .min(operation_wall_deadline),
            },
            None => Self {
                authority_valid_until_ms: u64::MAX,
                portable_wall_valid_until_ms: operation_wall_deadline,
            },
        }
    }

    pub(crate) fn local_operation_deadline(self) -> Instant {
        // Sample monotonic time before wall time so descheduling between the
        // samples can only shorten the receiver-side deadline.
        let local_monotonic_now = Instant::now();
        let local_wall_ms = crate::clock::current_time_millis();
        let remaining_ms = self.remaining_on_receiver_wall(local_wall_ms);
        local_monotonic_now
            .checked_add(Duration::from_millis(remaining_ms))
            .unwrap_or(local_monotonic_now)
    }

    pub(crate) fn remaining_on_receiver_wall(self, receiver_wall_ms: u64) -> u64 {
        self.portable_wall_valid_until_ms
            .saturating_sub(receiver_wall_ms)
    }
}

fn admitted_route_effect_deadline_is_conservative(
    deadline: StorageRpcAdmittedRouteEffectDeadline,
) -> bool {
    deadline.portable_wall_valid_until_ms
        <= deadline
            .authority_valid_until_ms
            .saturating_sub(crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS)
}

fn put_admitted_route_effect_deadline(
    out: &mut Vec<u8>,
    deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
) {
    match deadline {
        None => put_u8(out, 0),
        Some(deadline) => {
            put_u8(out, 1);
            put_u64(out, deadline.authority_valid_until_ms);
            put_u64(out, deadline.portable_wall_valid_until_ms);
        }
    }
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
pub(crate) struct StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) authorized_source: crate::metadata_command::MetadataCommandEnvelope,
    pub(crate) abandoned_source: Option<crate::metadata_command::MetadataCommandEnvelope>,
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
pub(crate) enum StorageRpcMetadataCommandPendingSlotCleanupOutcome {
    Value(bool),
    TerminalEntryPending {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
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
pub(crate) struct StorageRpcMetadataCommandPendingSlotCleanupResponse {
    pub(crate) outcome: StorageRpcMetadataCommandPendingSlotCleanupOutcome,
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
    pub(crate) publication_started: bool,
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
    StreamUploadNoSuchUpload {
        session_id: SessionId,
        upload_id: UploadId,
    },
    LogGap {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
        expected_log_index: u64,
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
    pub(crate) expected_state_digest: CanonicalStateDigest,
    pub(crate) commands: Vec<MetadataTransferCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferEmptyStateRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) expected_state_digest: CanonicalStateDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferMatchingStateRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) applied_log_index: u64,
    pub(crate) applied_log_hash: MetadataCommandLogHash,
    pub(crate) expected_state_digest: CanonicalStateDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandTransferCheckpointBaseRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) checkpoint: MetadataCommandCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataTransferStagingIntentCreateRequest {
    pub(crate) authorization:
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    pub(crate) intent: crate::pg_store::MetadataTransferStagingIntent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataTransferStagingArtifactPublishRequest {
    pub(crate) authorization:
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    pub(crate) intent: crate::pg_store::MetadataTransferStagingIntent,
    pub(crate) artifact: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataTransferStagingProofPublishRequest {
    pub(crate) authorization:
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    pub(crate) intent: crate::pg_store::MetadataTransferStagingIntent,
    pub(crate) target_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataTransferStagingTombstoneRequest {
    pub(crate) authorization:
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    pub(crate) intent: crate::pg_store::MetadataTransferStagingIntent,
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
    pub(crate) references: PgClusterMapHistoryRouteReferences,
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

/// Raw shard location carried on the Unix RPC boundary.
///
/// Decoding this value does not confer a `DataPgId`. The storage-node server
/// must validate the route (including retained-route rules where applicable)
/// before promoting it to the typed `ShardLocation` used by node state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageRpcShardLocation {
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) shard_index: ShardIndex,
    pub(crate) node_id: NodeId,
}

impl From<ShardLocation> for StorageRpcShardLocation {
    fn from(location: ShardLocation) -> Self {
        Self {
            cluster_epoch: location.cluster_epoch(),
            pg_id: location.data_pg_id().pg_id(),
            shard_index: location.shard_index(),
            node_id: location.node_id(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardWriteRequest {
    pub(crate) location: StorageRpcShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_size: u64,
    pub(crate) expected_crc64: u64,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardReadRequest {
    pub(crate) location: StorageRpcShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_ack: WriteAck,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcHistoricalShardReadRequest {
    pub(crate) location: StorageRpcShardLocation,
    pub(crate) shard_key: ShardKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardReadRangeRequest {
    pub(crate) location: StorageRpcShardLocation,
    pub(crate) shard_key: ShardKey,
    pub(crate) expected_ack: WriteAck,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardDeleteRequest {
    pub(crate) location: StorageRpcShardLocation,
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
    pub(crate) data_pg_id: PgId,
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
    pub(crate) locations: Vec<StorageRpcShardLocation>,
    pub(crate) shard_keys: Vec<ShardKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleAcquireResponse {
    pub(crate) locations: Vec<StorageRpcShardLocation>,
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
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
    pub(crate) operation_deadline: Option<StorageRpcOperationDeadline>,
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
    pub(crate) lease_deadline: u64,
    pub(crate) target_context: Option<String>,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationProofRequest {
    pub(crate) node_id: NodeId,
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationHeartbeatRequest {
    pub(crate) node_id: NodeId,
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) proof: BucketWriteReservationProof,
    pub(crate) lease_deadline: u64,
    pub(crate) effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcBucketWriteReservationRecordRequest {
    pub(crate) node_id: NodeId,
    pub(crate) route_cluster_epoch: ClusterEpoch,
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

#[derive(Debug, Clone)]
pub(crate) enum StorageRpcBucketSnapshotOutcome {
    Loaded(Box<BucketSnapshot>),
    BucketNotFound { name: BucketName },
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRpcBucketSnapshotResponse {
    pub(crate) outcome: StorageRpcBucketSnapshotOutcome,
}
