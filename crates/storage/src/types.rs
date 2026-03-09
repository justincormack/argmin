/// Core types for the storage layer.
use crate::error::StoreError;

// ── String newtypes ───────────────────────────────────────────────

macro_rules! string_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            pub fn into_string(self) -> String {
                self.0
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.0.as_str() == *other
            }
        }

        impl PartialEq<$name> for str {
            fn eq(&self, other: &$name) -> bool {
                self == other.0
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl PartialEq<String> for $name {
            fn eq(&self, other: &String) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for String {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl rusqlite::types::ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                self.0.to_sql()
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
                String::column_result(value).map(Self)
            }
        }
    };
}

string_newtype!(
    /// S3 bucket name.
    BucketName
);

string_newtype!(
    /// S3 object key.
    ObjectKey
);

string_newtype!(
    /// Multipart upload identifier.
    UploadId
);

string_newtype!(
    /// Streaming upload session identifier.
    SessionId
);

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

    /// Parse from an S3 API header value. Only accepts the canonical
    /// uppercase form (`SHA256`, `CRC32`, `CRC32C`, `SHA1`, `CRC64NVME`).
    pub fn parse(s: &str) -> Option<Self> {
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

    /// XML element name for this checksum (e.g. `ChecksumCRC32`).
    pub fn xml_element_name(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
            Self::Crc64nvme => "ChecksumCRC64NVME",
        }
    }

    /// Expected raw byte length for this algorithm's checksum value.
    pub fn expected_byte_length(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64nvme => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
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

    pub fn parse(s: &str) -> Option<Self> {
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
            // SHA: only COMPOSITE is supported
            ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => Self::Composite,
            // CRC32/CRC32C: default is COMPOSITE (FULL_OBJECT available but must be explicit)
            ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c => Self::Composite,
            // CRC64NVME: only FULL_OBJECT is supported (no composite)
            ChecksumAlgorithm::Crc64nvme => Self::FullObject,
        }
    }
}

/// A checksum with its algorithm — eliminates correlated `Option<algo>` +
/// `Option<bytes>` pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChecksum {
    algorithm: ChecksumAlgorithm,
    bytes: Vec<u8>,
}

impl RawChecksum {
    /// Construct a `RawChecksum`, validating that the byte length matches the algorithm.
    pub fn new(algorithm: ChecksumAlgorithm, bytes: Vec<u8>) -> Result<Self, &'static str> {
        let expected = algorithm.expected_byte_length();
        if bytes.len() != expected {
            return Err("checksum byte length does not match algorithm");
        }
        Ok(Self { algorithm, bytes })
    }

    /// The checksum algorithm.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The raw checksum bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Object lifecycle state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    Live = 0,
    DeleteMarker = 1,
}

impl ObjectState {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Live),
            1 => Some(Self::DeleteMarker),
            _ => None,
        }
    }

    pub fn is_delete_marker(self) -> bool {
        matches!(self, Self::DeleteMarker)
    }
}

impl std::fmt::Display for ObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "Live"),
            Self::DeleteMarker => write!(f, "DeleteMarker"),
        }
    }
}

/// Bucket versioning state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketVersioningState {
    Disabled = 0,
    Enabled = 1,
    Suspended = 2,
}

impl BucketVersioningState {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Disabled),
            1 => Some(Self::Enabled),
            2 => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// ETag kind discriminator.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtagKind {
    /// CRC64-NVME single-part ETag.
    Crc64 = 0,
    /// Multipart composite ETag (CRC64-NVME with part count suffix).
    MultipartComposite = 1,
}

impl EtagKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Crc64),
            1 => Some(Self::MultipartComposite),
            _ => None,
        }
    }
}

/// Storage class for objects.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    Standard = 0,
}

impl StorageClass {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Standard),
            _ => None,
        }
    }
}

/// Object data layout discriminator.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataLayout {
    /// Internal chunk manifest (normal PutObject writes).
    ChunkManifestInternal = 0,
    /// Composite manifest of independent parts (S3 multipart).
    MultipartManifest = 1,
}

impl DataLayout {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::ChunkManifestInternal),
            1 => Some(Self::MultipartManifest),
            _ => None,
        }
    }
}

/// Object version identifier.
///
/// `Null` represents the single unversioned copy (version_id=0 in the database).
/// `Versioned` represents an explicit version (version_id≥1 in the database).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionId {
    /// The null version (unversioned bucket).
    Null,
    /// An explicit version in a versioning-enabled bucket.
    Versioned(std::num::NonZeroU64),
}

impl VersionId {
    /// Convert from the raw u64 stored in the database.
    pub fn from_u64(v: u64) -> Self {
        match std::num::NonZeroU64::new(v) {
            Some(nz) => Self::Versioned(nz),
            None => Self::Null,
        }
    }

    /// Convert to the raw u64 for database storage.
    pub fn to_u64(self) -> u64 {
        match self {
            Self::Null => 0,
            Self::Versioned(v) => v.get(),
        }
    }

    /// Returns true if this is the null (unversioned) version.
    pub fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this is an explicit versioned ID.
    pub fn is_versioned(self) -> bool {
        matches!(self, Self::Versioned(_))
    }
}

impl std::fmt::Display for VersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::Versioned(v) => write!(f, "{v}"),
        }
    }
}

// ── Composite helper types ─────────────────────────────────────────

/// Erasure coding shape (k data shards, m parity shards).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcShape {
    pub k: u8,
    pub m: u8,
}

/// Object-level ETag — either a single-part CRC64-NVME or a multipart composite.
///
/// Eliminates the correlated `etag: Vec<u8>` + `etag_kind: EtagKind` +
/// `parts_count: Option<u32>` triple. Invalid combinations (e.g. multipart
/// without a parts count, or single-part with one) are unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectEtag {
    /// CRC64-NVME of the object data (8 bytes, big-endian).
    SinglePart([u8; 8]),
    /// Composite ETag for multipart uploads: CRC64-NVME of concatenated
    /// per-part CRC64s, plus the number of parts.
    MultipartComposite {
        crc64: [u8; 8],
        parts: std::num::NonZeroU32,
    },
}

impl ObjectEtag {
    /// Construct a single-part ETag from a CRC64-NVME value.
    pub fn single_part(crc64: u64) -> Self {
        Self::SinglePart(crc64.to_be_bytes())
    }

    /// Construct a multipart composite ETag.
    ///
    /// # Panics
    /// Panics if `parts` is zero.
    pub fn multipart(crc64_bytes: [u8; 8], parts: u32) -> Self {
        Self::MultipartComposite {
            crc64: crc64_bytes,
            parts: std::num::NonZeroU32::new(parts)
                .expect("multipart etag requires non-zero parts count"),
        }
    }

    /// The raw CRC64-NVME bytes (8 bytes, big-endian).
    pub fn as_bytes(&self) -> &[u8; 8] {
        match self {
            Self::SinglePart(b) => b,
            Self::MultipartComposite { crc64, .. } => crc64,
        }
    }

    /// The CRC64-NVME value as a u64.
    pub fn crc64(&self) -> u64 {
        u64::from_be_bytes(*self.as_bytes())
    }

    /// The ETag kind discriminant for SQL writes.
    pub fn etag_kind(&self) -> EtagKind {
        match self {
            Self::SinglePart(_) => EtagKind::Crc64,
            Self::MultipartComposite { .. } => EtagKind::MultipartComposite,
        }
    }

    /// The parts count (None for single-part).
    pub fn parts_count(&self) -> Option<u32> {
        match self {
            Self::SinglePart(_) => None,
            Self::MultipartComposite { parts, .. } => Some(parts.get()),
        }
    }

    /// Reconstruct from raw SQL columns.
    pub fn from_parts(
        etag_bytes: &[u8],
        etag_kind: EtagKind,
        parts_count: Option<u32>,
    ) -> Result<Self, &'static str> {
        if etag_bytes.len() != 8 {
            return Err("etag must be exactly 8 bytes");
        }
        let mut crc64 = [0u8; 8];
        crc64.copy_from_slice(etag_bytes);
        match (etag_kind, parts_count) {
            (EtagKind::Crc64, None) => Ok(Self::SinglePart(crc64)),
            (EtagKind::MultipartComposite, Some(n)) => {
                let parts = std::num::NonZeroU32::new(n)
                    .ok_or("multipart composite with zero parts count")?;
                Ok(Self::MultipartComposite { crc64, parts })
            }
            (EtagKind::Crc64, Some(_)) => Err("single-part etag must not have parts_count"),
            (EtagKind::MultipartComposite, None) => {
                Err("multipart composite etag missing parts_count")
            }
        }
    }

    /// Format as a quoted hex ETag string (S3 wire format).
    ///
    /// Single-part: `"abcdef1234567890"`
    /// Multipart: `"abcdef1234567890-3"`
    pub fn format(&self) -> String {
        match self {
            Self::SinglePart(b) => {
                let crc = u64::from_be_bytes(*b);
                format!("\"{:016x}\"", crc)
            }
            Self::MultipartComposite { crc64, parts } => {
                let crc = u64::from_be_bytes(*crc64);
                format!("\"{:016x}-{}\"", crc, parts)
            }
        }
    }
}

/// Object data layout — encodes the `data_layout` column plus `parts_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLayout {
    /// Internal chunk manifest (normal PutObject writes).
    ChunkManifest,
    /// Composite manifest of independent parts (S3 multipart).
    MultipartManifest { parts_count: std::num::NonZeroU32 },
}

impl ObjectLayout {
    /// Convert from raw SQL columns.
    pub fn from_parts(
        data_layout: DataLayout,
        parts_count: Option<u32>,
    ) -> Result<Self, &'static str> {
        match (data_layout, parts_count) {
            (DataLayout::ChunkManifestInternal, None) => Ok(Self::ChunkManifest),
            (DataLayout::MultipartManifest, Some(n)) => {
                let nz = std::num::NonZeroU32::new(n)
                    .ok_or("multipart manifest with zero parts_count")?;
                Ok(Self::MultipartManifest { parts_count: nz })
            }
            (DataLayout::ChunkManifestInternal, Some(_)) => {
                Err("chunk manifest must not have parts_count")
            }
            (DataLayout::MultipartManifest, None) => {
                Err("multipart manifest missing parts_count")
            }
        }
    }

    /// The underlying data layout discriminant for SQL writes.
    pub fn data_layout(self) -> DataLayout {
        match self {
            Self::ChunkManifest => DataLayout::ChunkManifestInternal,
            Self::MultipartManifest { .. } => DataLayout::MultipartManifest,
        }
    }

    /// The parts count for SQL writes (None for chunk manifest).
    pub fn parts_count(self) -> Option<u32> {
        match self {
            Self::ChunkManifest => None,
            Self::MultipartManifest { parts_count } => Some(parts_count.get()),
        }
    }
}

// ── Variant-based object model ────────────────────────────────────

/// An object record read from storage — either a live object or a delete marker.
#[derive(Debug, Clone)]
pub enum StoredObject {
    Live(LiveObjectRecord),
    DeleteMarker(DeleteMarkerRecord),
}

impl StoredObject {
    pub fn bucket(&self) -> &BucketName {
        match self {
            Self::Live(r) => &r.bucket,
            Self::DeleteMarker(r) => &r.bucket,
        }
    }

    pub fn key(&self) -> &ObjectKey {
        match self {
            Self::Live(r) => &r.key,
            Self::DeleteMarker(r) => &r.key,
        }
    }

    pub fn version_id(&self) -> VersionId {
        match self {
            Self::Live(r) => r.version_id,
            Self::DeleteMarker(r) => r.version_id,
        }
    }

    pub fn last_modified(&self) -> u64 {
        match self {
            Self::Live(r) => r.last_modified,
            Self::DeleteMarker(r) => r.last_modified,
        }
    }

    pub fn is_delete_marker(&self) -> bool {
        matches!(self, Self::DeleteMarker(_))
    }

    pub fn as_live(&self) -> Option<&LiveObjectRecord> {
        match self {
            Self::Live(r) => Some(r),
            Self::DeleteMarker(_) => None,
        }
    }

    pub fn into_live(self) -> Option<LiveObjectRecord> {
        match self {
            Self::Live(r) => Some(r),
            Self::DeleteMarker(_) => None,
        }
    }
}

/// A live object record (not a delete marker).
#[derive(Debug, Clone)]
pub struct LiveObjectRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub size: u64,
    pub etag: ObjectEtag,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    pub storage_class: StorageClass,
    pub ec: EcShape,
    pub layout: ObjectLayout,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<String>,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<Vec<u8>>,
}

/// A delete marker record.
#[derive(Debug, Clone)]
pub struct DeleteMarkerRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
}

/// Bucket metadata.
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: BucketName,
    pub owner_principal: String,
    /// Creation timestamp (unix milliseconds).
    pub created_at: u64,
    pub region: u16,
    pub versioning: BucketVersioningState,
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
pub enum PutObjectReq {
    Live(PutLiveObjectReq),
    DeleteMarker(PutDeleteMarkerReq),
}

/// Request to store a live object.
///
/// Invariant: the ETag variant must match the layout — `SinglePart` with
/// `ChunkManifest`, `MultipartComposite` with `MultipartManifest`. Use
/// [`PutLiveObjectReq::validate`] or rely on `put_object_meta` which calls it.
pub struct PutLiveObjectReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub size: u64,
    pub etag: ObjectEtag,
    pub ec: EcShape,
    pub layout: ObjectLayout,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<Vec<u8>>,
}

impl PutLiveObjectReq {
    /// Validate that the etag variant is consistent with the layout.
    pub fn validate(&self) -> Result<(), &'static str> {
        match (&self.etag, &self.layout) {
            (ObjectEtag::SinglePart(_), ObjectLayout::ChunkManifest) => Ok(()),
            (ObjectEtag::MultipartComposite { parts, .. }, ObjectLayout::MultipartManifest { parts_count }) => {
                if parts == parts_count {
                    Ok(())
                } else {
                    Err("etag parts count does not match layout parts count")
                }
            }
            (ObjectEtag::SinglePart(_), ObjectLayout::MultipartManifest { .. }) => {
                Err("single-part etag with multipart layout")
            }
            (ObjectEtag::MultipartComposite { .. }, ObjectLayout::ChunkManifest) => {
                Err("multipart composite etag with chunk manifest layout")
            }
        }
    }
}

/// Request to store a delete marker.
pub struct PutDeleteMarkerReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
}

/// Request to finalize a multipart upload into a live object.
///
/// Layout is always `MultipartManifest`. The `parts_count` is derived from the
/// manifest slice passed alongside this request — it is not a separate field,
/// so divergence between the stored count and the actual manifest is impossible.
///
/// The ETag is the composite CRC64 bytes; the storage layer constructs the
/// `MultipartComposite` variant using the parts slice length, so the caller
/// cannot produce a variant mismatch.
pub struct CommitMultipartReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub size: u64,
    /// Composite CRC64-NVME bytes (CRC of concatenated per-part CRC64s).
    pub etag_crc64: [u8; 8],
    pub ec: EcShape,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<Vec<u8>>,
}

/// Request to finalize a streaming PutObject into a live object.
///
/// Layout is always `ChunkManifest` — no parts_count field.
/// The ETag is a single-part CRC64; the storage layer constructs the
/// `SinglePart` variant, so the caller cannot produce a variant mismatch.
pub struct CommitStreamPutReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub size: u64,
    /// CRC64-NVME of the object data.
    pub etag_crc64: u64,
    pub ec: EcShape,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<Vec<u8>>,
}

/// Request to list objects in a PG.
pub struct ListObjectsReq {
    pub bucket: BucketName,
    pub prefix: Option<ObjectKey>,
    pub start_after: Option<ObjectKey>,
    pub max_keys: u32,
}

/// Response from a list objects query.
pub struct ListObjectsResp {
    pub objects: Vec<StoredObject>,
    pub is_truncated: bool,
    pub next_start_after: Option<ObjectKey>,
}

/// Request to list all object versions in a PG.
pub struct ListObjectVersionsReq {
    pub bucket: BucketName,
    pub prefix: Option<ObjectKey>,
    pub key_marker: Option<ObjectKey>,
    pub version_id_marker: Option<VersionId>,
    pub max_keys: u32,
}

/// Response from a list object versions query.
pub struct ListObjectVersionsResp {
    pub versions: Vec<StoredObject>,
    pub is_truncated: bool,
    pub next_key_marker: Option<ObjectKey>,
    pub next_version_id_marker: Option<VersionId>,
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
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
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
    pub upload_id: UploadId,
    pub part_number: u32,
    pub generation: u32,
    pub size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
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
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub part_number: u32,
    pub size: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
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
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub metadata_blob: Vec<u8>,
    pub owner_principal: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
}

/// Request to list multipart uploads.
pub struct ListMultipartUploadsReq {
    pub bucket: BucketName,
    pub prefix: Option<ObjectKey>,
    pub key_marker: Option<ObjectKey>,
    pub upload_id_marker: Option<UploadId>,
    pub max_uploads: u32,
}

/// Response from listing multipart uploads.
pub struct ListMultipartUploadsResp {
    pub uploads: Vec<MultipartUploadRecord>,
    pub is_truncated: bool,
    pub next_key_marker: Option<ObjectKey>,
    pub next_upload_id_marker: Option<UploadId>,
}

/// Request to list parts of a multipart upload.
pub struct ListPartsReq {
    pub upload_id: UploadId,
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

// ── Streaming upload types ─────────────────────────────────────────

/// Streaming upload operation kind.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamUploadKind {
    PutObject = 0,
    UploadPart = 1,
}

impl StreamUploadKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::PutObject),
            1 => Some(Self::UploadPart),
            _ => None,
        }
    }
}

/// Strongly-typed stream upload target.
///
/// This makes invalid combinations (such as `UploadPart` without upload ID)
/// unrepresentable at the type level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamUploadTarget {
    PutObject,
    UploadPart { upload_id: UploadId, part_number: u32 },
}

impl StreamUploadTarget {
    pub fn op_kind(&self) -> StreamUploadKind {
        match self {
            Self::PutObject => StreamUploadKind::PutObject,
            Self::UploadPart { .. } => StreamUploadKind::UploadPart,
        }
    }

    pub fn upload_id(&self) -> Option<&str> {
        match self {
            Self::PutObject => None,
            Self::UploadPart { upload_id, .. } => Some(upload_id.as_str()),
        }
    }

    pub fn part_number(&self) -> Option<u32> {
        match self {
            Self::PutObject => None,
            Self::UploadPart { part_number, .. } => Some(*part_number),
        }
    }
}

/// Streaming upload session state machine.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamUploadState {
    InProgress = 0,
    Completing = 1,
    Completed = 2,
    Aborted = 3,
}

impl StreamUploadState {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::InProgress),
            1 => Some(Self::Completing),
            2 => Some(Self::Completed),
            3 => Some(Self::Aborted),
            _ => None,
        }
    }
}

/// In-progress streaming upload session record.
#[derive(Debug, Clone)]
pub struct StreamUploadRecord {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub state: StreamUploadState,
    pub created_at: u64,
}

/// Request to create a streaming upload session.
pub struct CreateStreamUploadReq {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
}

/// Staging chunk record for an in-progress streaming session.
#[derive(Debug, Clone)]
pub struct StreamUploadChunkRecord {
    pub session_id: SessionId,
    pub chunk_index: u32,
    pub size: u64,
    /// 16-byte object key hash for shard keys.
    pub chunk_okh: [u8; 16],
    /// Version field for shard keys.
    pub chunk_vid: u64,
    /// PG where this chunk's shards are stored.
    pub shard_pg_id: u32,
    pub ec_k: u8,
    pub ec_m: u8,
}

/// Committed chunk record for a normal PutObject.
#[derive(Debug, Clone)]
pub struct StreamObjectChunkRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub chunk_index: u32,
    pub size: u64,
    pub chunk_okh: [u8; 16],
    pub chunk_vid: u64,
    pub shard_pg_id: u32,
    pub ec_k: u8,
    pub ec_m: u8,
}

/// Committed chunk record for a multipart part.
#[derive(Debug, Clone)]
pub struct MultipartPartChunkRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub version_id: u64,
    pub part_number: u32,
    pub chunk_index: u32,
    pub size: u64,
    pub chunk_okh: [u8; 16],
    pub chunk_vid: u64,
    pub shard_pg_id: u32,
    pub ec_k: u8,
    pub ec_m: u8,
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
    fn data_layout_from_u8_chunk_manifest_internal() {
        assert_eq!(
            DataLayout::from_u8(0),
            Some(DataLayout::ChunkManifestInternal)
        );
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
            ChecksumAlgorithm::parse("SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("CRC64NVME"),
            Some(ChecksumAlgorithm::Crc64nvme)
        );
        assert_eq!(ChecksumAlgorithm::parse("bogus"), None);
    }

    #[test]
    fn checksum_algorithm_as_str_round_trip() {
        for v in 0..=4u8 {
            let algo = ChecksumAlgorithm::from_u8(v).unwrap();
            assert_eq!(ChecksumAlgorithm::parse(algo.as_str()), Some(algo));
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
            ChecksumType::parse("COMPOSITE"),
            Some(ChecksumType::Composite)
        );
        assert_eq!(
            ChecksumType::parse("FULL_OBJECT"),
            Some(ChecksumType::FullObject)
        );
        assert_eq!(ChecksumType::parse("bogus"), None);
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
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc32c),
            ChecksumType::Composite
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

    // ── StreamUploadKind enum tests ──────────────────────────────────

    #[test]
    fn stream_upload_kind_from_u8_valid() {
        assert_eq!(
            StreamUploadKind::from_u8(0),
            Some(StreamUploadKind::PutObject)
        );
        assert_eq!(
            StreamUploadKind::from_u8(1),
            Some(StreamUploadKind::UploadPart)
        );
    }

    #[test]
    fn stream_upload_kind_from_u8_invalid() {
        assert_eq!(StreamUploadKind::from_u8(2), None);
        assert_eq!(StreamUploadKind::from_u8(255), None);
    }

    // ── StreamUploadState enum tests ─────────────────────────────────

    #[test]
    fn stream_upload_state_from_u8_valid() {
        assert_eq!(
            StreamUploadState::from_u8(0),
            Some(StreamUploadState::InProgress)
        );
        assert_eq!(
            StreamUploadState::from_u8(1),
            Some(StreamUploadState::Completing)
        );
        assert_eq!(
            StreamUploadState::from_u8(2),
            Some(StreamUploadState::Completed)
        );
        assert_eq!(
            StreamUploadState::from_u8(3),
            Some(StreamUploadState::Aborted)
        );
    }

    #[test]
    fn stream_upload_state_from_u8_invalid() {
        assert_eq!(StreamUploadState::from_u8(4), None);
        assert_eq!(StreamUploadState::from_u8(255), None);
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
