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

/// Checksum algorithm for multipart uploads.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32 = 0,
    Crc32c = 1,
    Sha1 = 2,
    Sha256 = 3,
    Crc64nvme = 4,
}

impl ChecksumAlgorithm {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Crc32),
            1 => Some(Self::Crc32c),
            2 => Some(Self::Sha1),
            3 => Some(Self::Sha256),
            4 => Some(Self::Crc64nvme),
            _ => None,
        }
    }

    /// Parse from an S3 API header value. Accepts the canonical uppercase
    /// form used by S3 (`SHA256`, `CRC32`, etc.).
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            "CRC64NVME" => Some(Self::Crc64nvme),
            _ => None,
        }
    }

    /// S3 API canonical name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Crc64nvme => "CRC64NVME",
        }
    }

    /// The `x-amz-checksum-*` header suffix for this algorithm.
    pub fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Crc64nvme => "x-amz-checksum-crc64nvme",
        }
    }
}

/// Checksum type for multipart uploads: COMPOSITE (SHA) or FULL_OBJECT (CRC).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    Composite = 0,
    FullObject = 1,
}

impl ChecksumType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Composite),
            1 => Some(Self::FullObject),
            _ => None,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "COMPOSITE" => Some(Self::Composite),
            "FULL_OBJECT" => Some(Self::FullObject),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    /// Return the default checksum type for a given algorithm.
    pub fn default_for(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => Self::Composite,
            ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c | ChecksumAlgorithm::Crc64nvme => {
                Self::FullObject
            }
        }
    }
}

/// Object data layout discriminator.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataLayout {
    /// Single contiguous payload (current model).
    InlineLegacy = 0,
    /// Composite manifest of independent parts.
    MultipartManifest = 1,
}

impl DataLayout {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::InlineLegacy),
            1 => Some(Self::MultipartManifest),
            _ => None,
        }
    }
}

/// Object record stored in per-PG metadata.
#[derive(Debug, Clone)]
pub struct ObjectRecord {
    pub bucket: String,
    pub key: String,
    pub version_id: u64,
    pub size: u64,
    /// Total stored size: metadata blob + user data, before EC padding.
    pub total_size: u64,
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
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<String>,
    /// Object data layout (InlineLegacy or MultipartManifest).
    pub data_layout: DataLayout,
    /// Number of parts (set for MultipartManifest objects).
    pub parts_count: Option<u32>,
    /// Serialized user metadata headers (set for MultipartManifest objects).
    pub metadata_blob: Option<Vec<u8>>,
}

/// Bucket metadata.
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub owner_principal: String,
    /// Creation timestamp (unix milliseconds).
    pub created_at: u64,
    pub region: u16,
    /// 0 = Disabled, 1 = Enabled, 2 = Suspended.
    pub versioning: u8,
    pub public_read: bool,
    /// Serialized CORS configuration XML (None = no CORS config).
    pub cors_config: Option<String>,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<String>,
    /// Serialized public access block configuration XML (None = no config).
    pub public_access_block: Option<String>,
    /// Ownership controls value (None = not set).
    pub ownership_controls: Option<String>,
}

/// Request to store object metadata.
pub struct PutObjectMetaReq {
    pub bucket: String,
    pub key: String,
    pub version_id: u64,
    pub size: u64,
    pub total_size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: u8,
    pub ec_k: u8,
    pub ec_m: u8,
    /// 0 = Live, 1 = DeleteMarker.
    pub status: u8,
    /// Object data layout. None defaults to InlineLegacy (0).
    pub data_layout: Option<DataLayout>,
    /// Number of parts (set for MultipartManifest objects).
    pub parts_count: Option<u32>,
    /// Serialized user metadata headers (set for MultipartManifest objects).
    pub metadata_blob: Option<Vec<u8>>,
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

/// Request to list all object versions in a PG.
pub struct ListObjectVersionsReq {
    pub bucket: String,
    pub prefix: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<u64>,
    pub max_keys: u32,
}

/// Response from a list object versions query.
pub struct ListObjectVersionsResp {
    pub versions: Vec<ObjectRecord>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<u64>,
}

// ── Multipart upload types ─────────────────────────────────────────

/// Multipart upload state machine.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadState {
    InProgress = 0,
    Completing = 1,
    Aborting = 2,
}

impl UploadState {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::InProgress),
            1 => Some(Self::Completing),
            2 => Some(Self::Aborting),
            _ => None,
        }
    }
}

/// In-progress multipart upload record.
#[derive(Debug, Clone)]
pub struct MultipartUploadRecord {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    /// Initiation timestamp (unix milliseconds).
    pub initiated_at: u64,
    pub state: UploadState,
    /// Serialized user metadata headers.
    pub metadata_blob: Vec<u8>,
    pub owner_principal: Option<String>,
    /// Checksum algorithm requested for this upload.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Checksum type (COMPOSITE or FULL_OBJECT).
    pub checksum_type: Option<ChecksumType>,
}

/// In-progress multipart part record.
#[derive(Debug, Clone)]
pub struct MultipartPartRecord {
    pub upload_id: String,
    pub part_number: u32,
    pub generation: u32,
    pub size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: u8,
    /// 16-byte object key hash for shard keys.
    pub part_okh: [u8; 16],
    /// Per-part shard key version field.
    pub part_vid: u64,
    pub ec_k: u8,
    pub ec_m: u8,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<Vec<u8>>,
}

/// Committed part record in the object manifest.
#[derive(Debug, Clone)]
pub struct ObjectPartRecord {
    pub bucket: String,
    pub key: String,
    pub version_id: u64,
    pub part_number: u32,
    pub size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: u8,
    pub part_okh: [u8; 16],
    pub part_vid: u64,
    pub ec_k: u8,
    pub ec_m: u8,
    /// PG where this part's shards are stored.
    pub shard_pg_id: u32,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<Vec<u8>>,
}

/// Request to create a multipart upload.
pub struct CreateMultipartUploadReq {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata_blob: Vec<u8>,
    pub owner_principal: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
}

/// Request to list multipart uploads.
pub struct ListMultipartUploadsReq {
    pub bucket: String,
    pub prefix: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub max_uploads: u32,
}

/// Response from listing multipart uploads.
pub struct ListMultipartUploadsResp {
    pub uploads: Vec<MultipartUploadRecord>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

/// Request to list parts of a multipart upload.
pub struct ListPartsReq {
    pub upload_id: String,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Response from listing parts of a multipart upload.
#[derive(Debug)]
pub struct ListPartsResp {
    pub parts: Vec<MultipartPartRecord>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    // ── DataLayout enum tests ──────────────────────────────────────

    #[test]
    fn data_layout_from_u8_inline_legacy() {
        assert_eq!(DataLayout::from_u8(0), Some(DataLayout::InlineLegacy));
    }

    #[test]
    fn data_layout_from_u8_multipart_manifest() {
        assert_eq!(DataLayout::from_u8(1), Some(DataLayout::MultipartManifest));
    }

    #[test]
    fn data_layout_from_u8_invalid() {
        assert_eq!(DataLayout::from_u8(2), None);
        assert_eq!(DataLayout::from_u8(255), None);
    }

    // ── ChecksumAlgorithm enum tests ────────────────────────────────

    #[test]
    fn checksum_algorithm_from_u8_round_trip() {
        for v in 0..=4u8 {
            let algo = ChecksumAlgorithm::from_u8(v).unwrap();
            assert_eq!(algo as u8, v);
        }
        assert_eq!(ChecksumAlgorithm::from_u8(5), None);
        assert_eq!(ChecksumAlgorithm::from_u8(255), None);
    }

    #[test]
    fn checksum_algorithm_from_str() {
        assert_eq!(
            ChecksumAlgorithm::from_str("SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::from_str("CRC64NVME"),
            Some(ChecksumAlgorithm::Crc64nvme)
        );
        assert_eq!(ChecksumAlgorithm::from_str("bogus"), None);
    }

    #[test]
    fn checksum_algorithm_as_str_round_trip() {
        for v in 0..=4u8 {
            let algo = ChecksumAlgorithm::from_u8(v).unwrap();
            assert_eq!(ChecksumAlgorithm::from_str(algo.as_str()), Some(algo));
        }
    }

    // ── ChecksumType enum tests ──────────────────────────────────────

    #[test]
    fn checksum_type_from_u8_round_trip() {
        assert_eq!(ChecksumType::from_u8(0), Some(ChecksumType::Composite));
        assert_eq!(ChecksumType::from_u8(1), Some(ChecksumType::FullObject));
        assert_eq!(ChecksumType::from_u8(2), None);
    }

    #[test]
    fn checksum_type_from_str() {
        assert_eq!(
            ChecksumType::from_str("COMPOSITE"),
            Some(ChecksumType::Composite)
        );
        assert_eq!(
            ChecksumType::from_str("FULL_OBJECT"),
            Some(ChecksumType::FullObject)
        );
        assert_eq!(ChecksumType::from_str("bogus"), None);
    }

    #[test]
    fn checksum_type_default_for_algorithm() {
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Sha256),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Sha1),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc32),
            ChecksumType::FullObject
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc32c),
            ChecksumType::FullObject
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc64nvme),
            ChecksumType::FullObject
        );
    }

    // ── UploadState enum tests ─────────────────────────────────────

    #[test]
    fn upload_state_from_u8_valid() {
        assert_eq!(UploadState::from_u8(0), Some(UploadState::InProgress));
        assert_eq!(UploadState::from_u8(1), Some(UploadState::Completing));
        assert_eq!(UploadState::from_u8(2), Some(UploadState::Aborting));
    }

    #[test]
    fn upload_state_from_u8_invalid() {
        assert_eq!(UploadState::from_u8(3), None);
        assert_eq!(UploadState::from_u8(255), None);
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

    // ── Property-based tests ────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_shard_key_round_trip(
            hash in any::<[u8; 16]>(),
            version_id in any::<u64>(),
            shard_index in any::<u8>(),
        ) {
            let key = ShardKey::new(&hash, version_id, shard_index);
            let bytes = key.as_bytes();
            let parsed = ShardKey::from_bytes(bytes).unwrap();
            prop_assert_eq!(key, parsed);
        }

        #[test]
        fn prop_shard_key_hex_length_and_prefix(
            hash in any::<[u8; 16]>(),
            version_id in any::<u64>(),
            shard_index in any::<u8>(),
        ) {
            let key = ShardKey::new(&hash, version_id, shard_index);
            let hex = key.hex();
            prop_assert_eq!(hex.len(), SHARD_KEY_LEN * 2);
            prop_assert_eq!(&hex[..2], &key.hex_prefix());
        }
    }
}
