use crate::{
    cluster::ShardLocation,
    metadata_command::{
        decode_metadata_command_envelope, BucketWriteReservationProof, MetadataCommandAcceptance,
        MetadataCommandReplicaState,
    },
    pg_store::{ScavengerShardFile, ScavengerShardFileScan},
    types::{
        ChecksumBytes, ClusterEpoch, DataPgId, GenerationId, ObjectKey, ObjectPayloadReclaimKind,
        PgId, ShardIndex, ShardKey, WriteAck, SHARD_KEY_LEN,
    },
    BucketName, NodeId,
};
use std::io::{Read, Write};

const STORAGE_RPC_FRAME_MAGIC: &[u8] = b"argmin-storage-rpc-frame";
pub(crate) const STORAGE_RPC_FRAME_ENCODING_VERSION: u16 = 1;
pub(crate) const STORAGE_RPC_MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;
pub(crate) const STORAGE_RPC_MAX_READ_OPERATION_ID_LEN: usize = 256;
pub(crate) const STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS: usize = 1024;
pub(crate) const STORAGE_RPC_MAX_SHARD_ACK_ITEMS: usize = 4096;
const STORAGE_RPC_SHARD_LOCATION_LEN: usize = 8 + 4 + 1 + 4;
const STORAGE_RPC_SHARD_KEY_FIELD_LEN: usize = 4 + SHARD_KEY_LEN;
const STORAGE_RPC_WRITE_ACK_LEN: usize = 8 + 8;
const STORAGE_RPC_SHARD_ACK_ROUTE_LEN: usize = 4 + 8 + 4;
const STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN: usize = STORAGE_RPC_SHARD_ACK_ROUTE_LEN
    + 4
    + STORAGE_RPC_MAX_SHARD_ACK_ITEMS
        * (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN);
const STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS: usize = 4096;
const STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN: usize = 4096;
const STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN: usize = STORAGE_RPC_SHARD_ACK_ROUTE_LEN;
const STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN: usize = STORAGE_RPC_SHARD_KEY_FIELD_LEN + 8;
const STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN: usize =
    STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN;
const STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN: usize =
    STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN;
const STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 8 + 8;
const STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN: usize = 4
    + STORAGE_RPC_MAX_READ_OPERATION_ID_LEN
    + 4
    + STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS * STORAGE_RPC_SHARD_LOCATION_LEN;
const STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN: usize =
    4 + STORAGE_RPC_MAX_READ_OPERATION_ID_LEN;
const STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN: usize = 4 + 8 + 4;
const STORAGE_RPC_MAX_METADATA_COMMAND_NEXT_ID_PAYLOAD_LEN: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 8;

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
    MetadataCommandAppliedLogHashes = 25,
    MetadataCommandMatchingAppliedLog = 26,
    MetadataCommandAbandoned = 27,
    MetadataCommandRecordAbandoned = 28,
    MetadataCommandPendingSlotReplace = 29,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum StorageRpcErrorCode {
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
            Self::ShardRead => "shard read",
            Self::ShardReadRange => "shard read range",
            Self::ShardDelete => "shard delete",
            Self::ReadHandlesAcquire => "read handles acquire",
            Self::ReadHandlesRelease => "read handles release",
            Self::ClaimHeartbeat => "claim heartbeat",
            Self::ClaimRelease => "claim release",
            Self::ProofRelease => "proof release",
            Self::ShardAckRecord => "shard ack record",
            Self::ShardAckValidate => "shard ack validate",
            Self::ShardScavengerListFiles => "shard scavenger list files",
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
            Self::MetadataCommandAppliedLogHashes => "metadata command applied log hashes",
            Self::MetadataCommandMatchingAppliedLog => "metadata command matching applied log",
            Self::MetadataCommandAbandoned => "metadata command abandoned",
            Self::MetadataCommandRecordAbandoned => "metadata command record abandoned",
            Self::MetadataCommandPendingSlotReplace => "metadata command pending slot replace",
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
            25 => Ok(Self::MetadataCommandAppliedLogHashes),
            26 => Ok(Self::MetadataCommandMatchingAppliedLog),
            27 => Ok(Self::MetadataCommandAbandoned),
            28 => Ok(Self::MetadataCommandRecordAbandoned),
            29 => Ok(Self::MetadataCommandPendingSlotReplace),
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
pub(crate) enum StorageRpcMetadataCommandStateOutcome {
    State(MetadataCommandReplicaState),
    LogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
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
pub(crate) struct StorageRpcMetadataCommandStateResponse {
    pub(crate) state: MetadataCommandReplicaState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandAcceptanceResponse {
    pub(crate) acceptance: MetadataCommandAcceptance,
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
pub(crate) struct StorageRpcScavengerListFilesRequest {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) data_pg_id: DataPgId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcReadHandleAcquireRequest {
    pub(crate) read_operation_id: String,
    pub(crate) locations: Vec<ShardLocation>,
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
    pub(crate) proof: BucketWriteReservationProof,
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
        StorageRpcMessageKind::ReadHandlesAcquire => {
            STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ReadHandlesRelease => {
            STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardRead => STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardReadRange => STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardDelete => STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN,
        StorageRpcMessageKind::ShardAckRecord | StorageRpcMessageKind::ShardAckValidate => {
            STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN
        }
        StorageRpcMessageKind::ShardScavengerListFiles => {
            STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandReplicaState => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandMaxLogIndex
        | StorageRpcMessageKind::MetadataCommandPendingEnvelope
        | StorageRpcMessageKind::MetadataCommandValidateReplayState
        | StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending => {
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
        }
        StorageRpcMessageKind::MetadataCommandNextId => {
            STORAGE_RPC_MAX_METADATA_COMMAND_NEXT_ID_PAYLOAD_LEN
        }
        _ => generic_max_payload_len,
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
    if item.command_bytes.len() > STORAGE_RPC_MAX_PAYLOAD_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: item.command_bytes.len(),
            limit: STORAGE_RPC_MAX_PAYLOAD_LEN,
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
    let command_checksum = decoder.read_u64()?;
    let command_bytes = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
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
    let command_checksum = decoder.read_u64()?;
    let command_bytes = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    let item_bytes = {
        let mut out = Vec::new();
        put_u64(&mut out, command_checksum);
        put_bytes(&mut out, &command_bytes);
        out
    };
    let item = decode_metadata_command_item(&item_bytes)?;
    let command = decode_metadata_command_envelope(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    Ok(StorageRpcMetadataCommandRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
    })
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
    let command_checksum = decoder.read_u64()?;
    let command_bytes = decoder.read_bytes()?.to_vec();
    let item_bytes = {
        let mut out = Vec::new();
        put_u64(&mut out, command_checksum);
        put_bytes(&mut out, &command_bytes);
        out
    };
    let item = decode_metadata_command_item(&item_bytes)?;
    let command = decode_metadata_command_envelope(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
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
    let previous_checksum = decoder.read_u64()?;
    let previous_bytes = decoder.read_bytes()?.to_vec();
    let previous_item_bytes = {
        let mut out = Vec::new();
        put_u64(&mut out, previous_checksum);
        put_bytes(&mut out, &previous_bytes);
        out
    };
    let previous_item = decode_metadata_command_item(&previous_item_bytes)?;
    let previous = decode_metadata_command_envelope(&previous_item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    validate_metadata_command_route(cluster_epoch, pg_id, previous.id())?;
    let replacement_checksum = decoder.read_u64()?;
    let replacement_bytes = decoder.read_bytes()?.to_vec();
    let replacement_item_bytes = {
        let mut out = Vec::new();
        put_u64(&mut out, replacement_checksum);
        put_bytes(&mut out, &replacement_bytes);
        out
    };
    let replacement_item = decode_metadata_command_item(&replacement_item_bytes)?;
    let replacement = decode_metadata_command_envelope(&replacement_item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
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
            let command_checksum = decoder.read_u64()?;
            let command_bytes = decoder.read_bytes()?.to_vec();
            let item_bytes = {
                let mut out = Vec::new();
                put_u64(&mut out, command_checksum);
                put_bytes(&mut out, &command_bytes);
                out
            };
            let item = decode_metadata_command_item(&item_bytes)?;
            Some(
                decode_metadata_command_envelope(&item.command_bytes)
                    .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?,
            )
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
    let command_checksum = decoder.read_u64()?;
    let command_bytes = decoder.read_bytes()?.to_vec();
    let item_bytes = {
        let mut out = Vec::new();
        put_u64(&mut out, command_checksum);
        put_bytes(&mut out, &command_bytes);
        out
    };
    let item = decode_metadata_command_item(&item_bytes)?;
    let command = decode_metadata_command_envelope(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
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
    put_u8(
        &mut out,
        match response.acceptance {
            MetadataCommandAcceptance::Apply => 1,
            MetadataCommandAcceptance::AlreadyApplied => 2,
        },
    );
    out
}

pub(crate) fn decode_metadata_command_acceptance_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandAcceptanceResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let acceptance = match decoder.read_u8()? {
        1 => MetadataCommandAcceptance::Apply,
        2 => MetadataCommandAcceptance::AlreadyApplied,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command acceptance tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandAcceptanceResponse { acceptance })
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

pub(crate) fn encode_read_handle_acquire_request(
    request: &StorageRpcReadHandleAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_read_operation_id(&request.read_operation_id)?;
    if request.locations.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire must include at least one shard location",
        ));
    }
    validate_read_handle_location_count(request.locations.len())?;
    validate_read_handle_locations(&request.locations)?;
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
    if location_count > decoder.remaining_len() / STORAGE_RPC_SHARD_LOCATION_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut locations = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        locations.push(decoder.read_shard_location()?);
    }
    decoder.finish()?;
    validate_read_operation_id(&read_operation_id)?;
    validate_read_handle_locations(&locations)?;
    Ok(StorageRpcReadHandleAcquireRequest {
        read_operation_id,
        locations,
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
    validate_bucket_write_reservation_proof(&request.proof)?;
    let mut out = Vec::new();
    put_bucket_write_reservation_proof(&mut out, &request.proof);
    Ok(out)
}

pub(crate) fn decode_proof_release_request(
    bytes: &[u8],
) -> Result<StorageRpcProofReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let proof = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_bucket_write_reservation_proof(&proof)?;
    Ok(StorageRpcProofReleaseRequest { proof })
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

fn validate_bucket_write_reservation_proof(
    proof: &BucketWriteReservationProof,
) -> Result<(), StorageRpcPayloadError> {
    if proof.reservation_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id must not be empty",
        ));
    }
    if proof.owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token must not be empty",
        ));
    }
    if proof.operation_kind.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind must not be empty",
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

    fn read_bucket_name(&mut self) -> Result<BucketName, StorageRpcPayloadError> {
        BucketName::try_from(self.read_string()?)
            .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid bucket name"))
    }

    fn read_object_key(&mut self) -> Result<ObjectKey, StorageRpcPayloadError> {
        ObjectKey::try_from(self.read_string()?)
            .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid object key"))
    }

    fn read_generation_id(&mut self) -> Result<GenerationId, StorageRpcPayloadError> {
        GenerationId::new(self.read_u64()?).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
            "generation id must not be zero",
        ))
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
        let claim_id = self.read_string()?;
        let owner_token = self.read_string()?;
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
        let claim_id = self.read_string()?;
        let owner_token = self.read_string()?;
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
        let reservation_id = self.read_string()?;
        let owner_token = self.read_string()?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let operation_kind = self.read_string()?;
        let created_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let target_context = self.read_optional_string()?;
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

    fn read_optional_string(&mut self) -> Result<Option<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional string tag",
            )),
        }
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

fn put_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
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
            CreateBucketCommand, MetadataCommandEnvelope, MetadataCommandId,
            MetadataCommandLogIndex, MetadataCommandPayload,
        },
        types::{
            AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
            ClusterEpoch, CreateBucketConfig, PgId,
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
    fn metadata_command_max_log_index_response_round_trips() {
        let response = StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index: 42 };

        let bytes = encode_metadata_command_max_log_index_response(&response);
        let decoded = decode_metadata_command_max_log_index_response(&bytes).unwrap();

        assert_eq!(decoded, response);
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
            let response = StorageRpcMetadataCommandAcceptanceResponse { acceptance };

            let bytes = encode_metadata_command_acceptance_response(&response);
            let decoded = decode_metadata_command_acceptance_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }

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
    fn read_handle_acquire_request_requires_idempotency_key_and_locations() {
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "read-op-1".to_string(),
            locations: vec![test_shard_location(0), test_shard_location(1)],
        };

        let bytes = encode_read_handle_acquire_request(&request).unwrap();
        let decoded = decode_read_handle_acquire_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: String::new(),
                locations: vec![test_shard_location(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id must not be empty",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "x".repeat(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1),
                locations: vec![test_shard_location(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id exceeds maximum length",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-2".to_string(),
                locations: Vec::new(),
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
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-unsorted".to_string(),
                locations: vec![test_shard_location(1), test_shard_location(0)],
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
                StorageRpcMessageKind::MetadataCommandReplicaState,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
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
            proof: test_bucket_write_reservation_proof(),
        };

        let bytes = encode_proof_release_request(&request).unwrap();
        let decoded = decode_proof_release_request(&bytes).unwrap();

        assert_eq!(decoded, request);
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
