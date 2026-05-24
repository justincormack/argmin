use crate::{metadata_command::decode_metadata_command_envelope, types::ChecksumBytes};

const STORAGE_RPC_FRAME_MAGIC: &[u8] = b"argmin-storage-rpc-frame";
const STORAGE_RPC_FRAME_ENCODING_VERSION: u16 = 1;
pub(crate) const STORAGE_RPC_MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

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
}

impl StorageRpcMessageKind {
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
    #[error("shard write size mismatch: expected {expected}, actual {actual}")]
    ShardWriteSizeMismatch { expected: u64, actual: u64 },
    #[error("shard write checksum mismatch")]
    ShardWriteChecksumMismatch,
    #[error("invalid checksum metadata: {0}")]
    InvalidChecksumMetadata(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcMetadataCommandItem {
    pub(crate) command_checksum: u64,
    pub(crate) command_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcShardWriteItem {
    pub(crate) expected_size: u64,
    pub(crate) expected_crc64: u64,
    pub(crate) payload: Vec<u8>,
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
    let payload_len = decoder
        .read_u32()
        .map_err(|_| StorageRpcFrameError::Truncated)? as usize;
    if payload_len > max_payload_len {
        return Err(StorageRpcFrameError::PayloadTooLarge {
            len: payload_len,
            limit: max_payload_len,
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
    let kind = StorageRpcMessageKind::from_u16(raw_kind)?;
    decoder
        .finish()
        .map_err(|_| StorageRpcFrameError::TrailingBytes)?;
    Ok(StorageRpcFrame {
        request_id,
        kind,
        payload: payload.to_vec(),
    })
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
    let actual_size = payload.len() as u64;
    if actual_size != expected_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_size,
            actual: actual_size,
        });
    }
    if checksum::crc64::checksum(payload) != expected_crc64 {
        return Err(StorageRpcPayloadError::ShardWriteChecksumMismatch);
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
}
