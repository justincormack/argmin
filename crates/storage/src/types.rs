/// Core types for the storage layer.
use crate::error::StoreError;
use std::{num::NonZeroU64, str::FromStr};

pub use checksum::{
    ChecksumAlgorithm, ChecksumBytes, ChecksumType, InvalidChecksumConfig, MultipartChecksumConfig,
    RawChecksum,
};
pub use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId, LegalHoldStatus,
    ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus, VersionId,
};

/// Internal immutable payload generation identifier.
///
/// This is distinct from the external S3 `VersionId`. Every physical payload
/// generation should eventually have a fresh `GenerationId`, even when the
/// visible S3 version is the null version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenerationId(NonZeroU64);

impl GenerationId {
    pub const MIN: Self = Self(NonZeroU64::MIN);

    #[must_use]
    pub fn new(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for GenerationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

// ── String newtypes ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BucketNameError {
    #[error("bucket name must be 3-63 characters, got {length}")]
    InvalidLength { length: usize },
    #[error("bucket name must start with a lowercase letter or digit")]
    InvalidStartCharacter,
    #[error("bucket name must end with a lowercase letter or digit")]
    InvalidEndCharacter,
    #[error("bucket name must contain only lowercase letters, digits, hyphens, and periods")]
    InvalidCharacterSet,
    #[error("bucket name must not contain consecutive periods")]
    ConsecutivePeriods,
    #[error("bucket name must not contain dot-dash or dash-dot")]
    DotDashOrDashDot,
    #[error("bucket name must not start with xn-- (reserved for IDN)")]
    ReservedPrefix,
    #[error("bucket name must not be formatted as an IP address")]
    IpAddressFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ObjectKeyError {
    #[error("object key must be 1-1024 bytes, got {length}")]
    InvalidLength { length: usize },
    #[error("object key must not contain null bytes")]
    ContainsNullByte,
}

fn validate_bucket_name(name: &str) -> Result<(), BucketNameError> {
    if name.len() < 3 || name.len() > 63 {
        return Err(BucketNameError::InvalidLength { length: name.len() });
    }

    let first = name.as_bytes()[0];
    let last = name.as_bytes()[name.len() - 1];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(BucketNameError::InvalidStartCharacter);
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return Err(BucketNameError::InvalidEndCharacter);
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return Err(BucketNameError::InvalidCharacterSet);
    }
    if name.contains("..") {
        return Err(BucketNameError::ConsecutivePeriods);
    }
    if name.contains(".-") || name.contains("-.") {
        return Err(BucketNameError::DotDashOrDashDot);
    }
    if name.starts_with("xn--") {
        return Err(BucketNameError::ReservedPrefix);
    }

    let mut parts = name.split('.');
    if let (Some(a), Some(b), Some(c), Some(d), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        if [a, b, c, d]
            .into_iter()
            .all(|part| part.parse::<u8>().is_ok())
        {
            return Err(BucketNameError::IpAddressFormat);
        }
    }

    Ok(())
}

fn validate_object_key(key: &str) -> Result<(), ObjectKeyError> {
    if key.is_empty() || key.len() > 1024 {
        return Err(ObjectKeyError::InvalidLength { length: key.len() });
    }
    if key.as_bytes().contains(&0) {
        return Err(ObjectKeyError::ContainsNullByte);
    }
    Ok(())
}

macro_rules! validated_string_newtype {
    ($(#[$meta:meta])* $name:ident, $error:ident, $validate:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Result<Self, $error> {
                let s = s.into();
                $validate(&s)?;
                Ok(Self(s))
            }

            pub fn into_string(self) -> String {
                self.0
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = $error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::try_from(s)
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        impl PartialEq<$name> for str {
            fn eq(&self, other: &$name) -> bool {
                self == other.as_str()
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.as_str()
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
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                let value = String::column_result(value)?;
                Self::try_from(value).map_err(|error| {
                    rusqlite::types::FromSqlError::Other(Box::new(error))
                })
            }
        }
    };
}

macro_rules! string_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
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
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                String::column_result(value).map(Self)
            }
        }
    };
}

validated_string_newtype!(
    /// S3 bucket name.
    BucketName,
    BucketNameError,
    validate_bucket_name
);

validated_string_newtype!(
    /// S3 object key.
    ObjectKey,
    ObjectKeyError,
    validate_object_key
);

/// Return the smallest valid UTF-8 string that sorts after every key beginning
/// with `prefix`, or `None` if no such bound exists.
#[must_use]
pub fn key_prefix_upper_bound(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }

    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        while next <= char::MAX as u32 {
            if let Some(next_char) = char::from_u32(next) {
                let mut upper = String::new();
                upper.extend(chars.iter().copied());
                upper.push(next_char);
                return Some(upper);
            }
            next += 1;
        }
    }

    None
}

/// Return the smallest valid object key that sorts after every key beginning
/// with `prefix`, or `None` if no such bound exists.
#[must_use]
pub fn object_key_prefix_upper_bound(prefix: &ObjectKey) -> Option<ObjectKey> {
    key_prefix_upper_bound(prefix.as_str())
        .and_then(|upper_bound| ObjectKey::try_from(upper_bound).ok())
}

/// Return the common-prefix key ending at the first `delimiter` occurrence
/// after `prefix`, if `key` matches the requested prefix and delimiter.
///
/// The derived value is reconstructed from a prefix slice of an already-valid
/// `ObjectKey`, so validity is expected to hold unless the slicing invariant is
/// broken locally.
#[must_use]
pub fn object_key_common_prefix(
    key: &ObjectKey,
    prefix: &str,
    delimiter: &str,
) -> Option<ObjectKey> {
    if delimiter.is_empty() {
        return None;
    }

    let key_str = key.as_str();
    let after_prefix = key_str.strip_prefix(prefix)?;
    let pos = after_prefix.find(delimiter)?;
    let common_prefix = &key_str[..prefix.len() + pos + delimiter.len()];
    Some(
        ObjectKey::try_from(common_prefix.to_owned())
            .expect("prefix slice of a valid object key must remain a valid object key"),
    )
}

string_newtype!(
    /// Multipart upload identifier.
    UploadId
);

string_newtype!(
    /// Streaming upload session identifier.
    SessionId
);

/// Serialized user metadata blob.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SerializedMetadataBlob(Vec<u8>);

impl SerializedMetadataBlob {
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for SerializedMetadataBlob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for SerializedMetadataBlob {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<SerializedMetadataBlob> for Vec<u8> {
    fn from(value: SerializedMetadataBlob) -> Self {
        value.0
    }
}

impl std::fmt::Debug for SerializedMetadataBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedMetadataBlob")
            .field("len", &self.0.len())
            .finish()
    }
}

/// Serialized system metadata blob.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SerializedSystemMetadataBlob(Vec<u8>);

impl SerializedSystemMetadataBlob {
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for SerializedSystemMetadataBlob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for SerializedSystemMetadataBlob {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<SerializedSystemMetadataBlob> for Vec<u8> {
    fn from(value: SerializedSystemMetadataBlob) -> Self {
        value.0
    }
}

impl std::fmt::Debug for SerializedSystemMetadataBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedSystemMetadataBlob")
            .field("len", &self.0.len())
            .finish()
    }
}

/// Serialized tag-set XML.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SerializedTagSet(String);

impl SerializedTagSet {
    #[must_use]
    pub fn new(xml: String) -> Self {
        Self(xml)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::ops::Deref for SerializedTagSet {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl From<String> for SerializedTagSet {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SerializedTagSet {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<SerializedTagSet> for String {
    fn from(value: SerializedTagSet) -> Self {
        value.0
    }
}

impl std::fmt::Debug for SerializedTagSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedTagSet")
            .field("xml_len", &self.0.len())
            .finish()
    }
}

/// Stored object encryption discriminator.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectEncryptionType {
    None = 0,
    SseCustomer = 1,
    SseS3 = 2,
}

impl ObjectEncryptionType {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::SseCustomer),
            2 => Some(Self::SseS3),
            _ => None,
        }
    }
}

/// Service-managed server-side encryption algorithms exposed through the S3 API.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedEncryptionAlgorithm {
    Aes256 = 1,
}

impl ManagedEncryptionAlgorithm {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Aes256),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aes256 => "AES256",
        }
    }
}

pub const OBJECT_ENCRYPTION_WRAP_NONCE_LEN: usize = 12;
pub const OBJECT_ENCRYPTION_WRAPPED_DEK_LEN: usize = 48;
pub const OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN: usize = 6;
pub const OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN: usize = 2;
pub const OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN: usize = 12;
pub const OBJECT_ENCRYPTION_SEGMENT_TAG_LEN: usize = 16;
pub const SSE_C_VALIDATOR_SALT_LEN: usize = 16;
pub const SSE_C_VALIDATOR_HMAC_LEN: usize = 32;
pub const SSE_C_WRAP_SALT_LEN: usize = 16;
pub const SSE_C_WRAP_NONCE_LEN: usize = OBJECT_ENCRYPTION_WRAP_NONCE_LEN;
pub const SSE_C_WRAPPED_DEK_LEN: usize = OBJECT_ENCRYPTION_WRAPPED_DEK_LEN;
pub const SSE_C_SEGMENT_NONCE_PREFIX_LEN: usize = OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN;
pub const SSE_C_SEGMENT_NONCE_SCOPE_LEN: usize = OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN;
pub const SSE_C_CHECKSUM_NONCE_LEN: usize = OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN;
pub const SSE_S3_WRAP_NONCE_LEN: usize = OBJECT_ENCRYPTION_WRAP_NONCE_LEN;
pub const SSE_S3_WRAPPED_DEK_LEN: usize = OBJECT_ENCRYPTION_WRAPPED_DEK_LEN;
pub const SSE_S3_SEGMENT_NONCE_PREFIX_LEN: usize = OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN;
pub const SSE_S3_CHECKSUM_NONCE_LEN: usize = OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN;

/// Stored per-object `SSE-C` state.
#[derive(Clone, PartialEq, Eq)]
pub struct SseCustomerObjectState {
    pub validator_key_id: u32,
    pub validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
    pub validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
    pub wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
    pub wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
    pub wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
    pub segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    pub checksum_nonce: [u8; SSE_C_CHECKSUM_NONCE_LEN],
    pub encrypted_checksum_metadata: Vec<u8>,
}

impl std::fmt::Debug for SseCustomerObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseCustomerObjectState")
            .field("validator_key_id", &self.validator_key_id)
            .field(
                "encrypted_checksum_metadata_len",
                &self.encrypted_checksum_metadata.len(),
            )
            .field(
                "secret_material",
                &observability::redacted("sse_customer_state"),
            )
            .finish()
    }
}

impl SseCustomerObjectState {
    const VERSION: u8 = 3;
    const FIXED_ENCODED_LEN: usize = 1
        + 4
        + SSE_C_VALIDATOR_SALT_LEN
        + SSE_C_VALIDATOR_HMAC_LEN
        + SSE_C_WRAP_SALT_LEN
        + SSE_C_WRAP_NONCE_LEN
        + SSE_C_WRAPPED_DEK_LEN
        + SSE_C_SEGMENT_NONCE_PREFIX_LEN
        + SSE_C_CHECKSUM_NONCE_LEN
        + 2;

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let checksum_len = u16::try_from(self.encrypted_checksum_metadata.len())
            .expect("encrypted checksum metadata length should fit in u16");
        let mut out =
            Vec::with_capacity(Self::FIXED_ENCODED_LEN + self.encrypted_checksum_metadata.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.validator_key_id.to_be_bytes());
        out.extend_from_slice(&self.validator_salt);
        out.extend_from_slice(&self.validator_hmac);
        out.extend_from_slice(&self.wrap_salt);
        out.extend_from_slice(&self.wrap_nonce);
        out.extend_from_slice(&self.wrapped_dek);
        out.extend_from_slice(&self.segment_nonce_prefix);
        out.extend_from_slice(&self.checksum_nonce);
        out.extend_from_slice(&checksum_len.to_be_bytes());
        out.extend_from_slice(&self.encrypted_checksum_metadata);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(format!(
                "invalid SSE-C state length {} (minimum {})",
                bytes.len(),
                Self::FIXED_ENCODED_LEN
            ));
        }
        if bytes[0] != Self::VERSION {
            return Err(format!("unsupported SSE-C state version {}", bytes[0]));
        }

        let mut cursor = 1;
        let take = |cursor: &mut usize, len: usize| {
            let start = *cursor;
            let end = start + len;
            *cursor = end;
            &bytes[start..end]
        };

        let validator_key_id = u32::from_be_bytes(
            take(&mut cursor, 4)
                .try_into()
                .expect("slice length checked"),
        );
        let validator_salt = take(&mut cursor, SSE_C_VALIDATOR_SALT_LEN)
            .try_into()
            .expect("slice length checked");
        let validator_hmac = take(&mut cursor, SSE_C_VALIDATOR_HMAC_LEN)
            .try_into()
            .expect("slice length checked");
        let wrap_salt = take(&mut cursor, SSE_C_WRAP_SALT_LEN)
            .try_into()
            .expect("slice length checked");
        let wrap_nonce = take(&mut cursor, SSE_C_WRAP_NONCE_LEN)
            .try_into()
            .expect("slice length checked");
        let wrapped_dek = take(&mut cursor, SSE_C_WRAPPED_DEK_LEN)
            .try_into()
            .expect("slice length checked");
        let segment_nonce_prefix = take(&mut cursor, SSE_C_SEGMENT_NONCE_PREFIX_LEN)
            .try_into()
            .expect("slice length checked");
        let checksum_nonce = take(&mut cursor, SSE_C_CHECKSUM_NONCE_LEN)
            .try_into()
            .expect("slice length checked");
        let checksum_len = u16::from_be_bytes(
            take(&mut cursor, 2)
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        if cursor + checksum_len != bytes.len() {
            return Err(format!(
                "invalid SSE-C checksum metadata length {} (remaining {})",
                checksum_len,
                bytes.len().saturating_sub(cursor)
            ));
        }
        let encrypted_checksum_metadata = take(&mut cursor, checksum_len).to_vec();

        Ok(Self {
            validator_key_id,
            validator_salt,
            validator_hmac,
            wrap_salt,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce,
            encrypted_checksum_metadata,
        })
    }
}

/// Stored per-object `SSE-S3` state.
#[derive(Clone, PartialEq, Eq)]
pub struct SseS3ObjectState {
    pub wrapping_key_id: u32,
    pub wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
    pub wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN],
    pub segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    pub checksum_nonce: [u8; SSE_S3_CHECKSUM_NONCE_LEN],
    pub encrypted_checksum_metadata: Vec<u8>,
}

impl std::fmt::Debug for SseS3ObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseS3ObjectState")
            .field("wrapping_key_id", &self.wrapping_key_id)
            .field(
                "encrypted_checksum_metadata_len",
                &self.encrypted_checksum_metadata.len(),
            )
            .field("secret_material", &observability::redacted("sse_s3_state"))
            .finish()
    }
}

impl SseS3ObjectState {
    const VERSION: u8 = 1;
    const FIXED_ENCODED_LEN: usize = 1
        + 4
        + SSE_S3_WRAP_NONCE_LEN
        + SSE_S3_WRAPPED_DEK_LEN
        + SSE_S3_SEGMENT_NONCE_PREFIX_LEN
        + SSE_S3_CHECKSUM_NONCE_LEN
        + 2;

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let checksum_len = u16::try_from(self.encrypted_checksum_metadata.len())
            .expect("encrypted checksum metadata length should fit in u16");
        let mut out =
            Vec::with_capacity(Self::FIXED_ENCODED_LEN + self.encrypted_checksum_metadata.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.wrapping_key_id.to_be_bytes());
        out.extend_from_slice(&self.wrap_nonce);
        out.extend_from_slice(&self.wrapped_dek);
        out.extend_from_slice(&self.segment_nonce_prefix);
        out.extend_from_slice(&self.checksum_nonce);
        out.extend_from_slice(&checksum_len.to_be_bytes());
        out.extend_from_slice(&self.encrypted_checksum_metadata);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(format!(
                "invalid SSE-S3 state length {} (minimum {})",
                bytes.len(),
                Self::FIXED_ENCODED_LEN
            ));
        }
        if bytes[0] != Self::VERSION {
            return Err(format!("unsupported SSE-S3 state version {}", bytes[0]));
        }

        let mut cursor = 1;
        let take = |cursor: &mut usize, len: usize| {
            let start = *cursor;
            let end = start + len;
            *cursor = end;
            &bytes[start..end]
        };

        let wrapping_key_id = u32::from_be_bytes(
            take(&mut cursor, 4)
                .try_into()
                .expect("slice length checked"),
        );
        let wrap_nonce = take(&mut cursor, SSE_S3_WRAP_NONCE_LEN)
            .try_into()
            .expect("slice length checked");
        let wrapped_dek = take(&mut cursor, SSE_S3_WRAPPED_DEK_LEN)
            .try_into()
            .expect("slice length checked");
        let segment_nonce_prefix = take(&mut cursor, SSE_S3_SEGMENT_NONCE_PREFIX_LEN)
            .try_into()
            .expect("slice length checked");
        let checksum_nonce = take(&mut cursor, SSE_S3_CHECKSUM_NONCE_LEN)
            .try_into()
            .expect("slice length checked");
        let checksum_len = u16::from_be_bytes(
            take(&mut cursor, 2)
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        if cursor + checksum_len != bytes.len() {
            return Err(format!(
                "invalid SSE-S3 checksum metadata length {} (remaining {})",
                checksum_len,
                bytes.len().saturating_sub(cursor)
            ));
        }
        let encrypted_checksum_metadata = take(&mut cursor, checksum_len).to_vec();

        Ok(Self {
            wrapping_key_id,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce,
            encrypted_checksum_metadata,
        })
    }
}

/// Persisted object encryption state.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum ObjectEncryption {
    #[default]
    None,
    SseCustomer(SseCustomerObjectState),
    SseS3(SseS3ObjectState),
}

impl std::fmt::Debug for ObjectEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::SseCustomer(state) => f.debug_tuple("SseCustomer").field(state).finish(),
            Self::SseS3(state) => f.debug_tuple("SseS3").field(state).finish(),
        }
    }
}

impl ObjectEncryption {
    #[must_use]
    pub fn encryption_type(&self) -> ObjectEncryptionType {
        match self {
            Self::None => ObjectEncryptionType::None,
            Self::SseCustomer(_) => ObjectEncryptionType::SseCustomer,
            Self::SseS3(_) => ObjectEncryptionType::SseS3,
        }
    }

    #[must_use]
    pub fn encode_state(&self) -> Option<Vec<u8>> {
        match self {
            Self::None => None,
            Self::SseCustomer(state) => Some(state.encode()),
            Self::SseS3(state) => Some(state.encode()),
        }
    }

    #[must_use]
    pub const fn is_encrypted(&self) -> bool {
        !matches!(self, Self::None)
    }

    #[must_use]
    pub const fn segment_ciphertext_extra_len(&self) -> usize {
        if self.is_encrypted() {
            OBJECT_ENCRYPTION_SEGMENT_TAG_LEN
        } else {
            0
        }
    }

    #[must_use]
    pub const fn uses_sse_customer_headers(&self) -> bool {
        matches!(self, Self::SseCustomer(_))
    }

    #[must_use]
    pub const fn managed_encryption_algorithm(&self) -> Option<ManagedEncryptionAlgorithm> {
        match self {
            Self::SseS3(_) => Some(ManagedEncryptionAlgorithm::Aes256),
            Self::None | Self::SseCustomer(_) => None,
        }
    }

    pub fn decode(
        encryption_type: ObjectEncryptionType,
        state: Option<Vec<u8>>,
    ) -> Result<Self, String> {
        match (encryption_type, state) {
            (ObjectEncryptionType::None, None) => Ok(Self::None),
            (ObjectEncryptionType::None, Some(_)) => {
                Err("unexpected encryption_state for unencrypted object".to_string())
            }
            (ObjectEncryptionType::SseCustomer, Some(bytes)) => {
                Ok(Self::SseCustomer(SseCustomerObjectState::decode(&bytes)?))
            }
            (ObjectEncryptionType::SseCustomer, None) => {
                Err("missing encryption_state for SSE-C object".to_string())
            }
            (ObjectEncryptionType::SseS3, Some(bytes)) => {
                Ok(Self::SseS3(SseS3ObjectState::decode(&bytes)?))
            }
            (ObjectEncryptionType::SseS3, None) => {
                Err("missing encryption_state for SSE-S3 object".to_string())
            }
        }
    }
}

/// Length of a composite shard key in bytes.
///
/// Layout: object_key_hash (16 bytes) || version_id (8 bytes) || shard_index (1 byte)
pub const SHARD_KEY_LEN: usize = 25;
pub const SHARD_KEY_HEX_LEN: usize = SHARD_KEY_LEN * 2;
pub const SHARD_KEY_HEX_PREFIX_LEN: usize = 2;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

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

    /// Hex-encode the full key into a fixed-size ASCII buffer.
    pub fn hex_bytes(&self) -> [u8; SHARD_KEY_HEX_LEN] {
        let mut hex = [0u8; SHARD_KEY_HEX_LEN];
        for (i, byte) in self.0.iter().enumerate() {
            hex[i * 2] = HEX_DIGITS[(byte >> 4) as usize];
            hex[i * 2 + 1] = HEX_DIGITS[(byte & 0x0f) as usize];
        }
        hex
    }

    /// Return the first byte's hex representation as a fixed-size ASCII prefix.
    pub fn hex_prefix_bytes(&self) -> [u8; SHARD_KEY_HEX_PREFIX_LEN] {
        [
            HEX_DIGITS[(self.0[0] >> 4) as usize],
            HEX_DIGITS[(self.0[0] & 0x0f) as usize],
        ]
    }

    /// Hex-encode the full key (for file paths).
    pub fn hex(&self) -> String {
        let hex = self.hex_bytes();
        std::str::from_utf8(&hex)
            .expect("shard key hex is ASCII")
            .to_owned()
    }

    /// Return the first byte's hex representation as a two-character prefix.
    /// Used for directory fan-out: `shards/<prefix>/<full_hex>`.
    pub fn hex_prefix(&self) -> String {
        let prefix = self.hex_prefix_bytes();
        std::str::from_utf8(&prefix)
            .expect("shard key prefix hex is ASCII")
            .to_owned()
    }
}

impl std::fmt::Display for ShardKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ShardKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardKey({self})")
    }
}

/// Acknowledgment returned after a successful shard write.
#[derive(Clone, Copy, Debug)]
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
    /// Internal standard segmented layout (normal PutObject writes).
    StandardInternal = 0,
    /// Composite manifest of independent parts (S3 multipart).
    MultipartManifest = 1,
}

impl DataLayout {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::StandardInternal),
            1 => Some(Self::MultipartManifest),
            _ => None,
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
    /// Standard internally segmented object payload.
    Standard,
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
            (DataLayout::StandardInternal, None) => Ok(Self::Standard),
            (DataLayout::MultipartManifest, Some(n)) => {
                let nz = std::num::NonZeroU32::new(n)
                    .ok_or("multipart manifest with zero parts_count")?;
                Ok(Self::MultipartManifest { parts_count: nz })
            }
            (DataLayout::StandardInternal, Some(_)) => {
                Err("standard layout must not have parts_count")
            }
            (DataLayout::MultipartManifest, None) => Err("multipart manifest missing parts_count"),
        }
    }

    /// The underlying data layout discriminant for SQL writes.
    pub fn data_layout(self) -> DataLayout {
        match self {
            Self::Standard => DataLayout::StandardInternal,
            Self::MultipartManifest { .. } => DataLayout::MultipartManifest,
        }
    }

    /// The parts count for SQL writes (None for the standard segmented layout).
    pub fn parts_count(self) -> Option<u32> {
        match self {
            Self::Standard => None,
            Self::MultipartManifest { parts_count } => Some(parts_count.get()),
        }
    }
}

// ── Variant-based object model ────────────────────────────────────

/// An object record read from storage — either a live object or a delete marker.
#[derive(Debug, Clone)]
// Boxing the live variant would add heap traffic on the hot metadata path.
#[allow(clippy::large_enum_variant)]
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

    pub fn became_noncurrent_at(&self) -> Option<u64> {
        match self {
            Self::Live(r) => r.became_noncurrent_at,
            Self::DeleteMarker(_) => None,
        }
    }

    pub fn owner(&self) -> &OwnerIdentity {
        match self {
            Self::Live(r) => &r.owner,
            Self::DeleteMarker(r) => &r.owner,
        }
    }

    pub fn public_read(&self) -> bool {
        match self {
            Self::Live(r) => r.public_read,
            Self::DeleteMarker(_) => false,
        }
    }

    pub fn acl_grants(&self) -> Option<&AclGrants> {
        match self {
            Self::Live(r) => Some(&r.acl_grants),
            Self::DeleteMarker(_) => None,
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

/// Durable owner identity stored on object and multipart metadata rows.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerIdentity {
    pub principal: String,
    pub canonical_id: CanonicalUserId,
}

impl std::fmt::Debug for OwnerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerIdentity")
            .field("principal", &observability::escaped(&self.principal))
            .field("canonical_id", &self.canonical_id)
            .finish()
    }
}

impl OwnerIdentity {
    pub const ANONYMOUS_UPLOAD_PRINCIPAL: &'static str = "__aws_anonymous_upload__";

    #[must_use]
    pub fn new(principal: impl Into<String>, canonical_id: CanonicalUserId) -> Self {
        Self {
            principal: principal.into(),
            canonical_id,
        }
    }

    #[must_use]
    pub fn from_principal(principal: impl Into<String>) -> Self {
        let principal = principal.into();
        Self {
            canonical_id: CanonicalUserId::from_principal(&principal),
            principal,
        }
    }

    #[must_use]
    pub fn anonymous_upload() -> Self {
        Self {
            principal: Self::ANONYMOUS_UPLOAD_PRINCIPAL.to_string(),
            canonical_id: CanonicalUserId::anonymous_upload(),
        }
    }
}

/// A live object record (not a delete marker).
#[derive(Debug, Clone)]
pub struct LiveObjectRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    pub etag: ObjectEtag,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    /// Timestamp when this version stopped being the current live version.
    pub became_noncurrent_at: Option<u64>,
    pub storage_class: StorageClass,
    pub ec: EcShape,
    pub layout: ObjectLayout,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<SerializedTagSet>,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<SerializedMetadataBlob>,
    /// Serialized system metadata headers.
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    /// First-class per-version Object Lock state.
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

/// A delete marker record.
#[derive(Debug, Clone)]
pub struct DeleteMarkerRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
}

/// Durable reclaim record for a simple single-shard-set payload generation.
///
/// This is used when the namespace-visible object row is removed or replaced
/// before the old shard set can be physically deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimplePayloadReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub ec: EcShape,
    pub created_at: u64,
}

/// Bucket-scoped reclaim root used for synchronous bucket draining.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadReclaimRoot {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
}

/// Segment entry for a durable standard-object reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSegmentsReclaimSegmentRecord {
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub shard_pg_id: u32,
    pub ec: EcShape,
}

/// Durable reclaim record for a standard segmented object payload generation.
///
/// This is used when the namespace-visible object row is removed or replaced
/// before the old segmented payload can be physically deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSegmentsReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub created_at: u64,
    pub segments: Vec<ObjectSegmentsReclaimSegmentRecord>,
}

/// Part storage kind for a multipart reclaim record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartReclaimPartKind {
    ShardSet = 0,
    Segments = 1,
}

impl MultipartReclaimPartKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::ShardSet),
            1 => Some(Self::Segments),
            _ => None,
        }
    }
}

/// Segment entry for a multipart part reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartReclaimPartSegmentRecord {
    pub part_number: u32,
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub shard_pg_id: u32,
    pub ec: EcShape,
}

/// Part entry for a durable multipart reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartReclaimPartRecord {
    ShardSet {
        part_number: u32,
        part_okh: [u8; 16],
        part_vid: GenerationId,
        shard_pg_id: u32,
        ec: EcShape,
    },
    Segments {
        part_number: u32,
        segments: Vec<MultipartReclaimPartSegmentRecord>,
    },
}

/// Durable reclaim record for a multipart payload generation.
///
/// This is used when the namespace-visible object row is removed or replaced
/// before the old multipart payload can be physically deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub created_at: u64,
    pub parts: Vec<MultipartReclaimPartRecord>,
}

/// Public access block configuration for a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PublicAccessBlockConfig {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

/// Object ownership mode relevant to bucket ownership controls.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketObjectOwnership {
    BucketOwnerEnforced = 0,
    BucketOwnerPreferred = 1,
    ObjectWriter = 2,
}

impl BucketObjectOwnership {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BucketOwnerEnforced => "BucketOwnerEnforced",
            Self::BucketOwnerPreferred => "BucketOwnerPreferred",
            Self::ObjectWriter => "ObjectWriter",
        }
    }

    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::BucketOwnerEnforced),
            1 => Some(Self::BucketOwnerPreferred),
            2 => Some(Self::ObjectWriter),
            _ => None,
        }
    }
}

/// Ownership controls configuration for a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketOwnershipControls {
    pub object_ownership: BucketObjectOwnership,
}

/// Bucket lifecycle state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketState {
    Active = 0,
    Deleting = 1,
}

impl BucketState {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Active),
            1 => Some(Self::Deleting),
            _ => None,
        }
    }
}

/// Bucket subresources whose payloads are stored opaquely.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketSubresourceKind {
    Cors = 0,
    Tagging = 1,
    Policy = 4,
    Lifecycle = 5,
}

impl BucketSubresourceKind {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Cors),
            1 => Some(Self::Tagging),
            4 => Some(Self::Policy),
            5 => Some(Self::Lifecycle),
            _ => None,
        }
    }
}

/// Typed auxiliary summary data associated with a stored bucket subresource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BucketSubresourceAux {
    #[default]
    None,
    Policy {
        is_public: bool,
    },
}

impl BucketSubresourceAux {
    #[must_use]
    pub const fn policy(is_public: bool) -> Self {
        Self::Policy { is_public }
    }

    #[must_use]
    pub const fn policy_is_public(self) -> Option<bool> {
        match self {
            Self::None => None,
            Self::Policy { is_public, .. } => Some(is_public),
        }
    }
}

impl BucketSubresourceKind {
    #[must_use]
    pub const fn supports_aux(self, aux: BucketSubresourceAux) -> bool {
        matches!(
            (self, aux),
            (BucketSubresourceKind::Cors, BucketSubresourceAux::None)
                | (BucketSubresourceKind::Tagging, BucketSubresourceAux::None)
                | (
                    BucketSubresourceKind::Policy,
                    BucketSubresourceAux::Policy { .. }
                )
                | (BucketSubresourceKind::Lifecycle, BucketSubresourceAux::None)
        )
    }
}

/// Generic storage-layer request for bucket subresource writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutBucketSubresource<'a> {
    pub kind: BucketSubresourceKind,
    pub body: &'a str,
    pub aux: BucketSubresourceAux,
}

/// Generic stored representation of an opaque bucket subresource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBucketSubresource {
    pub body: String,
    pub generation: Option<u64>,
    pub aux: BucketSubresourceAux,
}

/// Bucket metadata.
#[derive(Clone)]
pub struct BucketInfo {
    pub name: BucketName,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    /// Creation timestamp (unix milliseconds).
    pub created_at: u64,
    pub region: u16,
    pub state: BucketState,
    pub versioning: BucketVersioningState,
    pub object_lock: BucketObjectLockConfig,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub public_write: bool,
    pub write_reservations_blocked: bool,
    pub active_write_reservations: u32,
    /// Typed public access block configuration (None = no config).
    pub public_access_block: Option<PublicAccessBlockConfig>,
    /// Typed ownership controls value (None = not set).
    pub ownership_controls: Option<BucketOwnershipControls>,
    /// Whether a bucket policy subresource is currently stored.
    pub bucket_policy_present: bool,
    /// Whether the stored bucket policy is classified as public.
    pub bucket_policy_public: bool,
    /// Monotonic generation incremented on every bucket policy update/delete.
    pub bucket_policy_generation: u64,
    /// Whether a lifecycle configuration subresource is currently stored.
    pub bucket_lifecycle_present: bool,
    /// Monotonic generation incremented on every lifecycle update/delete.
    pub bucket_lifecycle_generation: u64,
    /// Effective bucket encryption semantics used on hot paths.
    pub encryption: EffectiveBucketEncryptionConfig,
}

impl std::fmt::Debug for BucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketInfo")
            .field("name", &self.name)
            .field(
                "owner_principal",
                &observability::escaped(&self.owner_principal),
            )
            .field("owner_canonical_id", &self.owner_canonical_id)
            .field("created_at", &self.created_at)
            .field("region", &self.region)
            .field("state", &self.state)
            .field("versioning", &self.versioning)
            .field("object_lock", &self.object_lock)
            .field("acl_grants", &self.acl_grants)
            .field("public_read", &self.public_read)
            .field("public_write", &self.public_write)
            .field(
                "write_reservations_blocked",
                &self.write_reservations_blocked,
            )
            .field("active_write_reservations", &self.active_write_reservations)
            .field("public_access_block", &self.public_access_block)
            .field("ownership_controls", &self.ownership_controls)
            .field("bucket_policy_present", &self.bucket_policy_present)
            .field("bucket_policy_public", &self.bucket_policy_public)
            .field("bucket_policy_generation", &self.bucket_policy_generation)
            .field("bucket_lifecycle_present", &self.bucket_lifecycle_present)
            .field(
                "bucket_lifecycle_generation",
                &self.bucket_lifecycle_generation,
            )
            .field("encryption", &self.encryption)
            .finish()
    }
}

/// Complete bucket metadata required when creating a bucket row.
#[derive(Debug, Clone, Copy)]
pub struct CreateBucketConfig<'a> {
    pub name: &'a str,
    pub owner_principal: &'a str,
    pub owner_canonical_id: &'a CanonicalUserId,
    pub acl_grants: &'a AclGrants,
    pub public_read: bool,
    pub public_write: bool,
    pub versioning: BucketVersioningState,
    pub object_lock: BucketObjectLockConfig,
}

/// Authoritative in-memory subset of bucket metadata used on hot object paths.
#[derive(Clone, PartialEq, Eq)]
pub struct BucketFastPathInfo {
    pub name: BucketName,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub created_at: u64,
    pub state: BucketState,
    pub versioning: BucketVersioningState,
    pub object_lock: BucketObjectLockConfig,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub public_write: bool,
    pub public_access_block: Option<PublicAccessBlockConfig>,
    pub ownership_controls: Option<BucketOwnershipControls>,
    pub bucket_policy_present: bool,
    pub bucket_policy_public: bool,
    pub bucket_policy_generation: u64,
    pub bucket_lifecycle_present: bool,
    pub bucket_lifecycle_generation: u64,
    pub encryption: EffectiveBucketEncryptionConfig,
}

impl std::fmt::Debug for BucketFastPathInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketFastPathInfo")
            .field("name", &self.name)
            .field(
                "owner_principal",
                &observability::escaped(&self.owner_principal),
            )
            .field("owner_canonical_id", &self.owner_canonical_id)
            .field("created_at", &self.created_at)
            .field("state", &self.state)
            .field("versioning", &self.versioning)
            .field("object_lock", &self.object_lock)
            .field("acl_grants", &self.acl_grants)
            .field("public_read", &self.public_read)
            .field("public_write", &self.public_write)
            .field("public_access_block", &self.public_access_block)
            .field("ownership_controls", &self.ownership_controls)
            .field("bucket_policy_present", &self.bucket_policy_present)
            .field("bucket_policy_public", &self.bucket_policy_public)
            .field("bucket_policy_generation", &self.bucket_policy_generation)
            .field("bucket_lifecycle_present", &self.bucket_lifecycle_present)
            .field(
                "bucket_lifecycle_generation",
                &self.bucket_lifecycle_generation,
            )
            .field("encryption", &self.encryption)
            .finish()
    }
}

/// Stored bucket encryption configuration subset currently implemented by Argmin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketEncryptionConfig {
    pub default_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_c_blocked: bool,
}

impl BucketEncryptionConfig {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.default_encryption.is_none() && !self.sse_c_blocked
    }

    #[must_use]
    pub const fn effective(self) -> EffectiveBucketEncryptionConfig {
        EffectiveBucketEncryptionConfig {
            default_encryption: match self.default_encryption {
                Some(value) => value,
                None => ManagedEncryptionAlgorithm::Aes256,
            },
            sse_c_blocked: self.sse_c_blocked,
        }
    }
}

/// Effective bucket encryption semantics after applying AWS defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveBucketEncryptionConfig {
    pub default_encryption: ManagedEncryptionAlgorithm,
    pub sse_c_blocked: bool,
}

impl Default for EffectiveBucketEncryptionConfig {
    fn default() -> Self {
        Self {
            default_encryption: ManagedEncryptionAlgorithm::Aes256,
            sse_c_blocked: false,
        }
    }
}

impl From<BucketInfo> for BucketFastPathInfo {
    fn from(info: BucketInfo) -> Self {
        Self {
            name: info.name,
            owner_principal: info.owner_principal,
            owner_canonical_id: info.owner_canonical_id,
            created_at: info.created_at,
            state: info.state,
            versioning: info.versioning,
            object_lock: info.object_lock,
            acl_grants: info.acl_grants,
            public_read: info.public_read,
            public_write: info.public_write,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
            bucket_policy_present: info.bucket_policy_present,
            bucket_policy_public: info.bucket_policy_public,
            bucket_policy_generation: info.bucket_policy_generation,
            bucket_lifecycle_present: info.bucket_lifecycle_present,
            bucket_lifecycle_generation: info.bucket_lifecycle_generation,
            encryption: info.encryption,
        }
    }
}

impl From<&BucketInfo> for BucketFastPathInfo {
    fn from(info: &BucketInfo) -> Self {
        Self {
            name: info.name.clone(),
            owner_principal: info.owner_principal.clone(),
            owner_canonical_id: info.owner_canonical_id.clone(),
            created_at: info.created_at,
            state: info.state,
            versioning: info.versioning,
            object_lock: info.object_lock,
            acl_grants: info.acl_grants.clone(),
            public_read: info.public_read,
            public_write: info.public_write,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
            bucket_policy_present: info.bucket_policy_present,
            bucket_policy_public: info.bucket_policy_public,
            bucket_policy_generation: info.bucket_policy_generation,
            bucket_lifecycle_present: info.bucket_lifecycle_present,
            bucket_lifecycle_generation: info.bucket_lifecycle_generation,
            encryption: info.encryption,
        }
    }
}

/// Request to store object metadata.
// This request is frequently assembled inline on hot paths; avoid heap indirection.
#[allow(clippy::large_enum_variant)]
pub enum PutObjectReq {
    Live(PutLiveObjectReq),
    DeleteMarker(PutDeleteMarkerReq),
}

/// Request to store a live object.
///
/// Invariant: the ETag variant must match the layout — `SinglePart` with the
/// standard layout, `MultipartComposite` with `MultipartManifest`. Use
/// [`PutLiveObjectReq::validate`] or rely on `put_object_meta` which calls it.
pub struct PutLiveObjectReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    pub etag: ObjectEtag,
    pub ec: EcShape,
    pub layout: ObjectLayout,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<SerializedTagSet>,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<SerializedMetadataBlob>,
    /// Serialized system metadata headers.
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    /// Per-version Object Lock state to persist on the committed version.
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

impl PutLiveObjectReq {
    /// Validate that the etag variant is consistent with the layout.
    pub fn validate(&self) -> Result<(), &'static str> {
        match (&self.etag, &self.layout) {
            (ObjectEtag::SinglePart(_), ObjectLayout::Standard) => Ok(()),
            (
                ObjectEtag::MultipartComposite { parts, .. },
                ObjectLayout::MultipartManifest { parts_count },
            ) => {
                if parts == parts_count {
                    Ok(())
                } else {
                    Err("etag parts count does not match layout parts count")
                }
            }
            (ObjectEtag::SinglePart(_), ObjectLayout::MultipartManifest { .. }) => {
                Err("single-part etag with multipart layout")
            }
            (ObjectEtag::MultipartComposite { .. }, ObjectLayout::Standard) => {
                Err("multipart composite etag with standard layout")
            }
        }
    }
}

/// Request to store a delete marker.
pub struct PutDeleteMarkerReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
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
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    /// Composite CRC64-NVME bytes (CRC of concatenated per-part CRC64s).
    pub etag_crc64: [u8; 8],
    pub ec: EcShape,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<SerializedTagSet>,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<SerializedMetadataBlob>,
    /// Serialized system metadata headers.
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    /// Per-version Object Lock state to persist on the committed version.
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

/// Request to finalize a streaming PutObject into a live object.
///
/// Layout is always the standard segmented layout — no parts_count field.
/// The ETag is a single-part CRC64; the storage layer constructs the
/// `SinglePart` variant, so the caller cannot produce a variant mismatch.
pub struct CommitStreamPutReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    /// CRC64-NVME of the object data.
    pub etag_crc64: u64,
    pub ec: EcShape,
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<SerializedTagSet>,
    /// Serialized user metadata headers.
    pub metadata_blob: Option<SerializedMetadataBlob>,
    /// Serialized system metadata headers.
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    /// Per-version Object Lock state to persist on the committed version.
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

/// Request to list objects in a PG.
pub struct ListObjectsReq {
    pub bucket: BucketName,
    pub prefix: Option<ObjectKey>,
    pub start_after: Option<ObjectKey>,
    pub start_at: Option<ObjectKey>,
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
    /// Serialized tagging XML (None = no tags).
    pub tags: Option<SerializedTagSet>,
    /// Serialized user metadata headers.
    pub metadata_blob: SerializedMetadataBlob,
    /// Serialized system metadata headers.
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub initiator: Option<OwnerIdentity>,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    /// Pending Object Lock state to apply to the committed object version.
    pub object_lock: ObjectLockState,
    /// Validated checksum configuration for this upload.
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
}

/// Completed multipart upload record retained for AbortMultipartUpload semantics.
#[derive(Debug, Clone)]
pub struct CompletedMultipartUploadRecord {
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub completed_at: u64,
    pub initiator: Option<OwnerIdentity>,
    pub owner: OwnerIdentity,
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
    /// Per-part payload generation for shard keys.
    pub part_vid: GenerationId,
    pub ec_k: u8,
    pub ec_m: u8,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<ChecksumBytes>,
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
    pub part_vid: GenerationId,
    pub ec_k: u8,
    pub ec_m: u8,
    /// PG where this part's shards are stored.
    pub shard_pg_id: u32,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<ChecksumBytes>,
}

/// Committed part record annotated with its byte offset in the completed object.
#[derive(Debug, Clone)]
pub struct ObjectPartRangeRecord {
    pub part: ObjectPartRecord,
    pub object_offset_start: u64,
}

/// Request to create a multipart upload.
pub struct CreateMultipartUploadReq {
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub initiator: Option<OwnerIdentity>,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    /// Pending Object Lock state to copy onto the completed version.
    pub object_lock: ObjectLockState,
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
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

/// Staging version id for multipart part segment rows before
/// `CompleteMultipartUpload` assigns a real object version.
pub const MULTIPART_PART_SEGMENT_STAGING_VERSION_ID: VersionId =
    VersionId::Versioned(std::num::NonZeroU64::new(u64::MAX).unwrap());

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
    UploadPart {
        upload_id: UploadId,
        part_number: u32,
    },
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
    pub encryption: ObjectEncryption,
}

/// Request to create a streaming upload session.
pub struct CreateStreamUploadReq {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub encryption: ObjectEncryption,
}

/// Staging segment record for an in-progress streaming session.
#[derive(Debug, Clone)]
pub struct StreamUploadSegmentRecord {
    pub session_id: SessionId,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the logical segment bytes.
    pub segment_crc64: Option<u64>,
    /// 16-byte object key hash for shard keys.
    pub segment_okh: [u8; 16],
    /// Payload generation for shard keys.
    pub segment_vid: GenerationId,
    /// PG where this segment's shards are stored.
    pub shard_pg_id: u32,
    pub ec_k: u8,
    pub ec_m: u8,
}

/// Committed segment record for a normal PutObject.
#[derive(Debug, Clone)]
pub struct ObjectSegmentRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the logical segment bytes.
    pub segment_crc64: Option<u64>,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub shard_pg_id: u32,
    pub ec_k: u8,
    pub ec_m: u8,
}

/// Committed segment record for a multipart part.
#[derive(Debug, Clone)]
pub struct MultipartPartSegmentRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub version_id: u64,
    pub part_number: u32,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the logical segment bytes.
    pub segment_crc64: Option<u64>,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
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
    fn data_layout_from_u8_standard_internal() {
        assert_eq!(DataLayout::from_u8(0), Some(DataLayout::StandardInternal));
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
    fn sse_s3_object_state_round_trip() {
        let state = SseS3ObjectState {
            wrapping_key_id: 7,
            wrap_nonce: [1u8; SSE_S3_WRAP_NONCE_LEN],
            wrapped_dek: [2u8; SSE_S3_WRAPPED_DEK_LEN],
            segment_nonce_prefix: [3u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            checksum_nonce: [4u8; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: vec![5, 6, 7, 8],
        };
        let encoded = state.encode();
        let decoded = SseS3ObjectState::decode(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn object_encryption_sse_s3_round_trip() {
        let encryption = ObjectEncryption::SseS3(SseS3ObjectState {
            wrapping_key_id: 9,
            wrap_nonce: [10u8; SSE_S3_WRAP_NONCE_LEN],
            wrapped_dek: [11u8; SSE_S3_WRAPPED_DEK_LEN],
            segment_nonce_prefix: [12u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            checksum_nonce: [13u8; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: vec![14, 15, 16],
        });
        let decoded =
            ObjectEncryption::decode(encryption.encryption_type(), encryption.encode_state())
                .unwrap();
        assert_eq!(decoded, encryption);
        assert!(decoded.is_encrypted());
        assert_eq!(
            decoded.segment_ciphertext_extra_len(),
            OBJECT_ENCRYPTION_SEGMENT_TAG_LEN
        );
        assert!(!decoded.uses_sse_customer_headers());
    }

    #[test]
    fn debug_redacts_blob_and_encryption_contents() {
        let metadata = SerializedMetadataBlob::from(b"top-secret-metadata".to_vec());
        let system_metadata = SerializedSystemMetadataBlob::from(b"checksum-secret".to_vec());
        let tags = SerializedTagSet::from("<Tagging>secret-tag</Tagging>");
        let metadata_debug = format!("{metadata:?}");
        let system_debug = format!("{system_metadata:?}");
        let tags_debug = format!("{tags:?}");
        assert!(metadata_debug.contains("len"));
        assert!(system_debug.contains("len"));
        assert!(tags_debug.contains("xml_len"));
        assert!(!metadata_debug.contains("top-secret-metadata"));
        assert!(!system_debug.contains("checksum-secret"));
        assert!(!tags_debug.contains("secret-tag"));

        let encryption = ObjectEncryption::SseCustomer(SseCustomerObjectState {
            validator_key_id: 42,
            validator_salt: [1u8; SSE_C_VALIDATOR_SALT_LEN],
            validator_hmac: [2u8; SSE_C_VALIDATOR_HMAC_LEN],
            wrap_salt: [3u8; SSE_C_WRAP_SALT_LEN],
            wrap_nonce: [4u8; SSE_C_WRAP_NONCE_LEN],
            wrapped_dek: [5u8; SSE_C_WRAPPED_DEK_LEN],
            segment_nonce_prefix: [6u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
            checksum_nonce: [7u8; SSE_C_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: vec![8, 9, 10],
        });
        let debug = format!("{encryption:?}");
        assert!(debug.contains("<redacted:sse_customer_state>"));
        assert!(!debug.contains("wrapped_dek"));
        assert!(!debug.contains("validator_hmac"));
    }

    #[test]
    fn bucket_info_debug_summarizes_raw_configs() {
        let info = BucketInfo {
            name: BucketName::try_from("bucket-1").unwrap(),
            owner_principal: "own\ner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 1,
            region: 0,
            state: BucketState::Active,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            acl_grants: AclGrants::default(),
            public_read: false,
            public_write: false,
            write_reservations_blocked: false,
            active_write_reservations: 0,
            public_access_block: Some(PublicAccessBlockConfig {
                block_public_acls: true,
                ignore_public_acls: false,
                block_public_policy: true,
                restrict_public_buckets: false,
            }),
            ownership_controls: Some(BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
            }),
            bucket_policy_present: true,
            bucket_policy_public: false,
            bucket_policy_generation: 7,
            bucket_lifecycle_present: true,
            bucket_lifecycle_generation: 8,
            encryption: EffectiveBucketEncryptionConfig::default(),
        };
        let debug = format!("{info:?}");
        assert!(debug.contains(r#""own\ner""#));
        assert!(debug.contains("bucket_policy_present"));
        assert!(debug.contains("bucket_lifecycle_present"));
        assert!(!debug.contains("secret-policy"));
        assert!(!debug.contains("<LifecycleConfiguration>secret</LifecycleConfiguration>"));

        let fast_debug = format!("{:?}", BucketFastPathInfo::from(&info));
        assert!(fast_debug.contains(r#""own\ner""#));
        assert!(!fast_debug.contains("secret-policy"));
        assert!(!fast_debug.contains("secret"));
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
        assert_eq!(key.hex_prefix_bytes(), *b"de");
    }

    #[test]
    fn shard_key_hex_bytes_match_hex_string() {
        let key = ShardKey::new(&[0xAB; 16], 42, 3);
        let hex = key.hex();
        let hex_bytes = key.hex_bytes();
        assert_eq!(std::str::from_utf8(&hex_bytes).unwrap(), hex);
    }

    #[test]
    fn string_newtype_debug_escapes_control_characters() {
        let bucket = BucketName::try_from("bucket-1").unwrap();
        let key = ObjectKey::try_from("obj\t\u{1b}[31m").unwrap();
        let upload = UploadId::from("up\nload");
        let session = SessionId::from("sess\rion");

        assert_eq!(format!("{bucket:?}"), r#""bucket-1""#);
        assert_eq!(format!("{key:?}"), r#""obj\t\u{1b}[31m""#);
        assert_eq!(format!("{upload:?}"), r#""up\nload""#);
        assert_eq!(format!("{session:?}"), r#""sess\rion""#);
    }

    #[test]
    fn bucket_name_try_from_rejects_invalid_inputs() {
        assert_eq!(
            BucketName::try_from("ab").unwrap_err(),
            BucketNameError::InvalidLength { length: 2 }
        );
        assert_eq!(
            BucketName::try_from("xn--bucket").unwrap_err(),
            BucketNameError::ReservedPrefix
        );
        assert_eq!(
            BucketName::try_from("192.168.0.1").unwrap_err(),
            BucketNameError::IpAddressFormat
        );
    }

    #[test]
    fn object_key_try_from_rejects_invalid_inputs() {
        assert_eq!(
            ObjectKey::try_from("").unwrap_err(),
            ObjectKeyError::InvalidLength { length: 0 }
        );
        assert_eq!(
            ObjectKey::try_from("nul\0key").unwrap_err(),
            ObjectKeyError::ContainsNullByte
        );
        assert_eq!(
            ObjectKey::try_from("x".repeat(1025)).unwrap_err(),
            ObjectKeyError::InvalidLength { length: 1025 }
        );
    }

    #[test]
    fn object_key_prefix_upper_bound_stays_typed() {
        let prefix = ObjectKey::try_from("foo").unwrap();
        assert_eq!(
            object_key_prefix_upper_bound(&prefix),
            Some(ObjectKey::try_from("fop").unwrap())
        );
    }

    #[test]
    fn object_key_prefix_upper_bound_returns_none_when_derived_bound_exceeds_limit() {
        let prefix = ObjectKey::try_from(format!("{}{}", "a".repeat(1023), '\x7f')).unwrap();
        assert_eq!(object_key_prefix_upper_bound(&prefix), None);
    }

    #[test]
    fn object_key_common_prefix_stays_typed() {
        let key = ObjectKey::try_from("photos/2025/image.jpg").unwrap();
        assert_eq!(
            object_key_common_prefix(&key, "photos/", "/"),
            Some(ObjectKey::try_from("photos/2025/").unwrap())
        );
    }

    #[test]
    fn object_key_common_prefix_returns_none_without_delimiter_match() {
        let key = ObjectKey::try_from("photos-top.jpg").unwrap();
        assert_eq!(object_key_common_prefix(&key, "photos-", "/"), None);
    }

    #[test]
    fn object_key_common_prefix_returns_none_for_empty_delimiter() {
        let key = ObjectKey::try_from("photos/2025/image.jpg").unwrap();
        assert_eq!(object_key_common_prefix(&key, "", ""), None);
    }

    #[test]
    fn bucket_name_from_sql_rejects_invalid_rows() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t (name TEXT NOT NULL)", [])
            .unwrap();
        conn.execute("INSERT INTO t (name) VALUES (?1)", ["BadBucket"])
            .unwrap();

        let err = conn
            .query_row("SELECT name FROM t", [], |row| row.get::<_, BucketName>(0))
            .unwrap_err();
        match err {
            rusqlite::Error::FromSqlConversionFailure(_, rusqlite::types::Type::Text, _) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn object_key_from_sql_rejects_invalid_rows() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t (key TEXT NOT NULL)", [])
            .unwrap();
        conn.execute("INSERT INTO t (key) VALUES (?1)", [String::new()])
            .unwrap();

        let err = conn
            .query_row("SELECT key FROM t", [], |row| row.get::<_, ObjectKey>(0))
            .unwrap_err();
        match err {
            rusqlite::Error::FromSqlConversionFailure(_, rusqlite::types::Type::Text, _) => {}
            other => panic!("unexpected error: {other:?}"),
        }
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
