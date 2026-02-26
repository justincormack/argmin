/// Core types for the storage layer.
use crate::error::StoreError;

/// Length of a composite shard key in bytes.
///
/// Layout: object_key_hash (16 bytes) || version_id (8 bytes) || shard_index (1 byte)
pub const SHARD_KEY_LEN: usize = 25;

/// 25-byte composite shard key. Opaque to the storage layer.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ShardKey([u8; SHARD_KEY_LEN]);

impl ShardKey {
    /// Construct a shard key from its components.
    pub fn new(object_key_hash: &[u8; 16], version_id: u64, shard_index: u8) -> Self {
        let mut buf = [0u8; SHARD_KEY_LEN];
        buf[..16].copy_from_slice(object_key_hash);
        buf[16..24].copy_from_slice(&version_id.to_be_bytes());
        buf[24] = shard_index;
        Self(buf)
    }

    /// Parse a shard key from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != SHARD_KEY_LEN {
            return Err(StoreError::InvalidKeyLength {
                len: bytes.len(),
                expected: SHARD_KEY_LEN,
            });
        }
        let mut buf = [0u8; SHARD_KEY_LEN];
        buf.copy_from_slice(bytes);
        Ok(Self(buf))
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8; SHARD_KEY_LEN] {
        &self.0
    }

    /// Hex-encode the full key (for file paths).
    pub fn hex(&self) -> String {
        let mut s = String::with_capacity(SHARD_KEY_LEN * 2);
        for b in &self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Return the first byte's hex representation as a two-character prefix.
    /// Used for directory fan-out: `shards/<prefix>/<full_hex>`.
    pub fn hex_prefix(&self) -> String {
        format!("{:02x}", self.0[0])
    }
}

impl std::fmt::Debug for ShardKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardKey({})", self.hex())
    }
}

/// Acknowledgment returned after a successful shard write.
#[derive(Debug)]
pub struct WriteAck {
    /// CRC64-NVME checksum of the stored data.
    pub crc64: u64,
    /// Size of the stored data in bytes.
    pub stored_size: u64,
}

/// Data read back from a shard.
#[derive(Debug)]
pub struct ShardData {
    /// The shard contents.
    pub data: Vec<u8>,
    /// Verified CRC64-NVME checksum.
    pub crc64: u64,
}

/// Metadata about a stored shard (returned by stat).
#[derive(Debug)]
pub struct ShardStat {
    /// Size of the shard data in bytes.
    pub size: u64,
    /// CRC64-NVME checksum.
    pub crc64: u64,
    /// Creation timestamp (unix seconds).
    pub created_at: u64,
    /// Last integrity verification timestamp, if any.
    pub last_verified: Option<u64>,
}

/// Shard status in the per-PG database.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Live = 0,
    Deleting = 1,
    Quarantined = 2,
}

impl ShardStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Live),
            1 => Some(Self::Deleting),
            2 => Some(Self::Quarantined),
            _ => None,
        }
    }
}

/// Object record stored in per-PG metadata.
#[derive(Debug, Clone)]
pub struct ObjectRecord {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
    /// Binary etag (e.g. CRC64-NVME bytes), max 64 bytes.
    pub etag: Vec<u8>,
    /// 0 = CRC64-NVME.
    pub etag_kind: u8,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    pub storage_class: u8,
    pub ec_k: u8,
    pub ec_m: u8,
    /// 0 = Live, 1 = DeleteMarker, 2 = PendingDelete.
    pub status: u8,
}

/// Bucket metadata.
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub owner_id: u64,
    /// Creation timestamp (unix milliseconds).
    pub created_at: u64,
    pub region: u16,
    /// 0 = Disabled, 1 = Enabled, 2 = Suspended.
    pub versioning: u8,
}

/// Request to store object metadata.
pub struct PutObjectMetaReq {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: u8,
    pub ec_k: u8,
    pub ec_m: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_status_from_u8_live() {
        assert_eq!(ShardStatus::from_u8(0), Some(ShardStatus::Live));
    }

    #[test]
    fn shard_status_from_u8_deleting() {
        assert_eq!(ShardStatus::from_u8(1), Some(ShardStatus::Deleting));
    }

    #[test]
    fn shard_status_from_u8_quarantined() {
        assert_eq!(ShardStatus::from_u8(2), Some(ShardStatus::Quarantined));
    }

    #[test]
    fn shard_status_from_u8_invalid() {
        assert_eq!(ShardStatus::from_u8(3), None);
        assert_eq!(ShardStatus::from_u8(255), None);
    }

    #[test]
    fn shard_key_debug_contains_hex() {
        let key = ShardKey::new(&[0xAB; 16], 42, 3);
        let debug = format!("{:?}", key);
        assert!(debug.starts_with("ShardKey("));
        assert!(debug.contains("ab")); // hex of 0xAB
    }

    #[test]
    fn shard_key_round_trip() {
        let key = ShardKey::new(&[1; 16], 12345, 7);
        let bytes = key.as_bytes();
        let key2 = ShardKey::from_bytes(bytes).unwrap();
        assert_eq!(key, key2);
    }

    #[test]
    fn shard_key_from_bytes_wrong_length() {
        assert!(ShardKey::from_bytes(&[0; 10]).is_err());
        assert!(ShardKey::from_bytes(&[0; 26]).is_err());
    }

    #[test]
    fn shard_key_hex_prefix() {
        let key = ShardKey::new(&[0xDE; 16], 0, 0);
        assert_eq!(key.hex_prefix(), "de");
    }
}

/// Request to list objects in a PG.
pub struct ListObjectsReq {
    pub bucket: String,
    pub prefix: Option<String>,
    pub start_after: Option<String>,
    pub max_keys: u32,
}

/// Response from a list objects query.
pub struct ListObjectsResp {
    pub objects: Vec<ObjectRecord>,
    pub is_truncated: bool,
    pub next_start_after: Option<String>,
}
