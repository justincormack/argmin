/// Core types for the storage layer.
use crate::error::StoreError;
use ring::{hmac, rand::SecureRandom as _};
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

/// Monotonic cluster-map epoch used to fence distributed PG and shard actions.
///
/// This is deliberately separate from [`GenerationId`]. `GenerationId` names an
/// immutable object payload generation; `ClusterEpoch` names a cluster topology
/// and PG ownership generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClusterEpoch(NonZeroU64);

impl ClusterEpoch {
    /// First static epoch for the initial local multihost harness.
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    #[must_use]
    pub fn new(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for ClusterEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// Raw placement-group identifier.
///
/// Prefer one of the role-specific wrappers below at new cluster boundaries.
/// Existing single-node internals still use raw `u32` PG IDs until the storage
/// cluster boundary is introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PgId(u32);

impl PgId {
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for PgId {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<PgId> for u32 {
    fn from(value: PgId) -> Self {
        value.get()
    }
}

impl std::fmt::Display for PgId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// PG containing bucket metadata for one bucket name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BucketPgId(PgId);

impl BucketPgId {
    #[must_use]
    pub const fn new(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<BucketPgId> for PgId {
    fn from(value: BucketPgId) -> Self {
        value.pg_id()
    }
}

/// PG containing object metadata for one `(bucket, key)` namespace entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectMetadataPgId(PgId);

impl ObjectMetadataPgId {
    #[must_use]
    pub const fn new(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<ObjectMetadataPgId> for PgId {
    fn from(value: ObjectMetadataPgId) -> Self {
        value.pg_id()
    }
}

/// PG containing payload shard data for an object segment or multipart part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataPgId(PgId);

impl DataPgId {
    #[must_use]
    pub const fn new(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<DataPgId> for PgId {
    fn from(value: DataPgId) -> Self {
        value.pg_id()
    }
}

/// Placement-group availability state for one cluster epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgState {
    Active,
    Peering,
    Degraded,
    Backfilling,
    Inconsistent,
}

impl std::fmt::Display for PgState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Peering => f.write_str("peering"),
            Self::Degraded => f.write_str("degraded"),
            Self::Backfilling => f.write_str("backfilling"),
            Self::Inconsistent => f.write_str("inconsistent"),
        }
    }
}

/// EC shard index within one stripe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardIndex(u8);

impl ShardIndex {
    #[must_use]
    pub const fn new(index: u8) -> Self {
        Self(index)
    }

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl From<u8> for ShardIndex {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl From<ShardIndex> for u8 {
    fn from(value: ShardIndex) -> Self {
        value.get()
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UploadIdError {
    #[error("upload ID must be 128 characters, got {length}")]
    InvalidLength { length: usize },
    #[error("upload ID must contain only ASCII letters, digits, periods, and underscores")]
    InvalidCharacterSet,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionIdError {
    #[error("session ID must be 32 characters, got {length}")]
    InvalidLength { length: usize },
    #[error("session ID must contain only lowercase hexadecimal characters")]
    InvalidCharacterSet,
}

pub const UPLOAD_ID_LEN: usize = 128;
pub const UPLOAD_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._";
pub const MULTIPART_UPLOAD_ID_KEY_LEN: usize = 32;
pub const SESSION_ID_LEN: usize = 32;

/// Per-bucket secret used to authenticate multipart upload IDs.
///
/// The key is generated once for a bucket incarnation and is never exposed in
/// S3 responses or diagnostics. Deleting and recreating a bucket creates a new
/// key, making upload IDs from the previous incarnation invalid without
/// retaining terminal upload records.
#[derive(Clone, PartialEq, Eq)]
pub struct MultipartUploadIdKey([u8; MULTIPART_UPLOAD_ID_KEY_LEN]);

impl MultipartUploadIdKey {
    pub fn generate() -> Result<Self, String> {
        let mut bytes = [0u8; MULTIPART_UPLOAD_ID_KEY_LEN];
        ring::rand::SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| "failed to generate multipart upload ID key".to_string())?;
        Ok(Self(bytes))
    }

    pub fn from_bytes(bytes: [u8; MULTIPART_UPLOAD_ID_KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; MULTIPART_UPLOAD_ID_KEY_LEN] {
        &self.0
    }

    pub fn issue(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        initiator_principal: &str,
    ) -> Result<UploadId, String> {
        const NONCE_LEN: usize = 32;
        let mut random = [0u8; NONCE_LEN];
        ring::rand::SystemRandom::new()
            .fill(&mut random)
            .map_err(|_| "failed to generate multipart upload ID".to_string())?;
        let nonce: String = random
            .iter()
            .map(|byte| UPLOAD_ID_ALPHABET[(byte & 0x3f) as usize] as char)
            .collect();
        let initiator_claim = self.initiator_claim(initiator_principal);
        let tag = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.0),
            &multipart_upload_id_covered_bytes(bucket, key, nonce.as_bytes(), &initiator_claim),
        );
        let mut encoded = String::with_capacity(UPLOAD_ID_LEN);
        encoded.push_str(&nonce);
        for byte in initiator_claim {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}")
                .expect("writing multipart upload ID into String cannot fail");
        }
        for byte in tag.as_ref() {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}")
                .expect("writing multipart upload ID into String cannot fail");
        }
        UploadId::try_from(encoded)
            .map_err(|error| format!("generated invalid multipart upload ID: {error}"))
    }

    pub fn authenticates(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> bool {
        const NONCE_LEN: usize = 32;
        const INITIATOR_CLAIM_HEX_LEN: usize = 32;
        let (nonce, remainder) = upload_id.as_str().split_at(NONCE_LEN);
        let (encoded_claim, encoded_tag) = remainder.split_at(INITIATOR_CLAIM_HEX_LEN);
        let Some(initiator_claim) = decode_multipart_upload_id_hex::<16>(encoded_claim) else {
            return false;
        };
        let Some(tag) = decode_multipart_upload_id_hex::<MULTIPART_UPLOAD_ID_KEY_LEN>(encoded_tag)
        else {
            return false;
        };
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.0),
            &multipart_upload_id_covered_bytes(bucket, key, nonce.as_bytes(), &initiator_claim),
            &tag,
        )
        .is_ok()
    }

    pub fn was_issued_for_principal(
        &self,
        upload_id: &UploadId,
        initiator_principal: &str,
    ) -> bool {
        const NONCE_LEN: usize = 32;
        const INITIATOR_CLAIM_HEX_LEN: usize = 32;
        let encoded_claim = &upload_id.as_str()[NONCE_LEN..NONCE_LEN + INITIATOR_CLAIM_HEX_LEN];
        let Some(claim) = decode_multipart_upload_id_hex::<16>(encoded_claim) else {
            return false;
        };
        let expected = self.initiator_claim(initiator_principal);
        // `ring` only exposes constant-time verification for full HMAC tags. Compare the
        // fixed-size truncated claims by MACing both with a domain-separated derived key,
        // then asking `ring` to verify the full tag.
        let root_key = hmac::Key::new(hmac::HMAC_SHA256, &self.0);
        let comparison_key_bytes = hmac::sign(
            &root_key,
            b"argmin multipart upload initiator claim comparison v1\0",
        );
        let comparison_key = hmac::Key::new(hmac::HMAC_SHA256, comparison_key_bytes.as_ref());
        let claim_tag = hmac::sign(&comparison_key, &claim);
        hmac::verify(&comparison_key, &expected, claim_tag.as_ref()).is_ok()
    }

    fn initiator_claim(&self, initiator_principal: &str) -> [u8; 16] {
        const DOMAIN: &[u8] = b"argmin multipart upload initiator v1\0";
        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.0);
        let mut context = hmac::Context::with_key(&key);
        context.update(DOMAIN);
        context.update(&(initiator_principal.len() as u32).to_be_bytes());
        context.update(initiator_principal.as_bytes());
        let tag = context.sign();
        let mut claim = [0u8; 16];
        claim.copy_from_slice(&tag.as_ref()[..16]);
        claim
    }
}

fn decode_multipart_upload_id_hex<const N: usize>(encoded: &str) -> Option<[u8; N]> {
    if encoded.len() != N * 2 {
        return None;
    }
    let mut decoded = [0u8; N];
    for (index, encoded) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(encoded[0])?;
        let low = decode_hex_nibble(encoded[1])?;
        decoded[index] = (high << 4) | low;
    }
    Some(decoded)
}

fn multipart_upload_id_covered_bytes(
    bucket: &BucketName,
    key: &ObjectKey,
    nonce: &[u8],
    initiator_claim: &[u8; 16],
) -> Vec<u8> {
    const DOMAIN: &[u8] = b"argmin multipart upload id v1\0";
    let mut covered = Vec::with_capacity(
        DOMAIN.len()
            + 4
            + bucket.as_str().len()
            + 4
            + key.as_str().len()
            + nonce.len()
            + initiator_claim.len(),
    );
    covered.extend_from_slice(DOMAIN);
    covered.extend_from_slice(&(bucket.as_str().len() as u32).to_be_bytes());
    covered.extend_from_slice(bucket.as_str().as_bytes());
    covered.extend_from_slice(&(key.as_str().len() as u32).to_be_bytes());
    covered.extend_from_slice(key.as_str().as_bytes());
    covered.extend_from_slice(nonce);
    covered.extend_from_slice(initiator_claim);
    covered
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl std::fmt::Debug for MultipartUploadIdKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MultipartUploadIdKey(<redacted>)")
    }
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

fn validate_upload_id(upload_id: &str) -> Result<(), UploadIdError> {
    if upload_id.len() != UPLOAD_ID_LEN {
        return Err(UploadIdError::InvalidLength {
            length: upload_id.len(),
        });
    }
    if !upload_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_')
    {
        return Err(UploadIdError::InvalidCharacterSet);
    }
    Ok(())
}

fn validate_session_id(session_id: &str) -> Result<(), SessionIdError> {
    if session_id.len() != SESSION_ID_LEN {
        return Err(SessionIdError::InvalidLength {
            length: session_id.len(),
        });
    }
    if !session_id
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(SessionIdError::InvalidCharacterSet);
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

validated_string_newtype!(
    /// Multipart upload identifier.
    UploadId,
    UploadIdError,
    validate_upload_id
);

/// Streaming upload session identifier.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(String);

impl SessionId {
    pub fn new(s: impl Into<String>) -> Result<Self, SessionIdError> {
        let s = s.into();
        validate_session_id(&s)?;
        Ok(Self(s))
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
    }
}

impl TryFrom<String> for SessionId {
    type Error = SessionIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SessionId {
    type Error = SessionIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::str::FromStr for SessionId {
    type Err = SessionIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl PartialEq<str> for SessionId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for SessionId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<SessionId> for str {
    fn eq(&self, other: &SessionId) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SessionId> for &str {
    fn eq(&self, other: &SessionId) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<String> for SessionId {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<SessionId> for String {
    fn eq(&self, other: &SessionId) -> bool {
        *self == other.0
    }
}

impl rusqlite::types::ToSql for SessionId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl rusqlite::types::FromSql for SessionId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = String::column_result(value)?;
        Self::try_from(value).map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ObjectEncryptionDecodeError {
    #[error("unexpected encryption_state for unencrypted object")]
    UnexpectedStateForUnencryptedObject,
    #[error("missing encryption_state for SSE-C object")]
    MissingSseCustomerState,
    #[error("missing encryption_state for SSE-S3 object")]
    MissingSseS3State,
    #[error("invalid SSE-C state length {actual} (minimum {minimum})")]
    InvalidSseCustomerStateLength { actual: usize, minimum: usize },
    #[error("unsupported SSE-C state version {version}")]
    UnsupportedSseCustomerStateVersion { version: u8 },
    #[error("invalid SSE-C checksum metadata length {declared} (remaining {remaining})")]
    InvalidSseCustomerChecksumMetadataLength { declared: usize, remaining: usize },
    #[error("invalid SSE-S3 state length {actual} (minimum {minimum})")]
    InvalidSseS3StateLength { actual: usize, minimum: usize },
    #[error("unsupported SSE-S3 state version {version}")]
    UnsupportedSseS3StateVersion { version: u8 },
    #[error("invalid SSE-S3 checksum metadata length {declared} (remaining {remaining})")]
    InvalidSseS3ChecksumMetadataLength { declared: usize, remaining: usize },
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

    pub fn decode(bytes: &[u8]) -> Result<Self, ObjectEncryptionDecodeError> {
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(ObjectEncryptionDecodeError::InvalidSseCustomerStateLength {
                actual: bytes.len(),
                minimum: Self::FIXED_ENCODED_LEN,
            });
        }
        if bytes[0] != Self::VERSION {
            return Err(
                ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion {
                    version: bytes[0],
                },
            );
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
            return Err(
                ObjectEncryptionDecodeError::InvalidSseCustomerChecksumMetadataLength {
                    declared: checksum_len,
                    remaining: bytes.len().saturating_sub(cursor),
                },
            );
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

    pub fn decode(bytes: &[u8]) -> Result<Self, ObjectEncryptionDecodeError> {
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(ObjectEncryptionDecodeError::InvalidSseS3StateLength {
                actual: bytes.len(),
                minimum: Self::FIXED_ENCODED_LEN,
            });
        }
        if bytes[0] != Self::VERSION {
            return Err(ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion {
                version: bytes[0],
            });
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
            return Err(
                ObjectEncryptionDecodeError::InvalidSseS3ChecksumMetadataLength {
                    declared: checksum_len,
                    remaining: bytes.len().saturating_sub(cursor),
                },
            );
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
    ) -> Result<Self, ObjectEncryptionDecodeError> {
        match (encryption_type, state) {
            (ObjectEncryptionType::None, None) => Ok(Self::None),
            (ObjectEncryptionType::None, Some(_)) => {
                Err(ObjectEncryptionDecodeError::UnexpectedStateForUnencryptedObject)
            }
            (ObjectEncryptionType::SseCustomer, Some(bytes)) => {
                Ok(Self::SseCustomer(SseCustomerObjectState::decode(&bytes)?))
            }
            (ObjectEncryptionType::SseCustomer, None) => {
                Err(ObjectEncryptionDecodeError::MissingSseCustomerState)
            }
            (ObjectEncryptionType::SseS3, Some(bytes)) => {
                Ok(Self::SseS3(SseS3ObjectState::decode(&bytes)?))
            }
            (ObjectEncryptionType::SseS3, None) => {
                Err(ObjectEncryptionDecodeError::MissingSseS3State)
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

    /// Return the EC shard index embedded in this shard key.
    pub fn shard_index(&self) -> ShardIndex {
        ShardIndex::new(self.0[24])
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

    /// Parse a shard key from its fixed-width lowercase or uppercase hex form.
    pub fn from_hex(hex: &str) -> Result<Self, StoreError> {
        if hex.len() != SHARD_KEY_HEX_LEN {
            return Err(StoreError::InvalidShardKeyHex);
        }
        let mut bytes = [0u8; SHARD_KEY_LEN];
        for (index, out) in bytes.iter_mut().enumerate() {
            let high = hex_nibble(hex.as_bytes()[index * 2])?;
            let low = hex_nibble(hex.as_bytes()[index * 2 + 1])?;
            *out = (high << 4) | low;
        }
        Ok(Self(bytes))
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

fn hex_nibble(byte: u8) -> Result<u8, StoreError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StoreError::InvalidShardKeyHex),
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// Non-authoritative reason for a physical shard scavenger audit observation.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardScavengerObservationReason {
    /// A shard file exists on disk but the data-PG `shards` row is absent.
    FileWithoutShardRow = 0,
    /// A data-PG `shards` row exists but the shard file is absent.
    ShardRowWithoutFile = 1,
    /// Both row and file exist, but a completed scan found no durable reference.
    UnreferencedShardRowAndFile = 2,
    /// The reference scan did not complete, so no candidate from it is stable.
    ScanIncomplete = 3,
}

impl ShardScavengerObservationReason {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::FileWithoutShardRow),
            1 => Some(Self::ShardRowWithoutFile),
            2 => Some(Self::UnreferencedShardRowAndFile),
            3 => Some(Self::ScanIncomplete),
            _ => None,
        }
    }
}

/// Physical location identity for a shard scavenger audit observation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardScavengerObservationKey {
    pub node_id: u32,
    pub data_pg_id: u32,
    pub shard_index: ShardIndex,
    pub shard_key: ShardKey,
}

/// Input for recording one shard scavenger audit observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardScavengerObservationRecord {
    pub key: ShardScavengerObservationKey,
    pub data_size: Option<u64>,
    pub crc64: Option<u64>,
    pub file_exists: bool,
    pub shard_row_exists: bool,
    pub reason: ShardScavengerObservationReason,
    pub last_error: Option<String>,
}

/// Non-authoritative audit row for an apparent physical shard orphan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardScavengerObservation {
    pub key: ShardScavengerObservationKey,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub observation_count: u64,
    pub data_size: Option<u64>,
    pub crc64: Option<u64>,
    pub file_exists: bool,
    pub shard_row_exists: bool,
    pub reason: ShardScavengerObservationReason,
    pub last_error: Option<String>,
    pub resolved_at: Option<u64>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EcShape {
    pub k: u8,
    pub m: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentStoredBytesRequest {
    pub data_pg_id: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub stored_size: usize,
    pub segment_crc64: u64,
    pub ec: EcShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlacedSegmentShardRepairWorkItem {
    pub request: SegmentStoredBytesRequest,
    pub shard_index: ShardIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardRepairRecord {
    pub work_item: PlacedSegmentShardRepairWorkItem,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub observation_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardRepairClaimRecord {
    pub work_item: PlacedSegmentShardRepairWorkItem,
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardRepairClaimAcquireParams {
    pub claim_id: String,
    pub owner_token: String,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardRepairClaimAcquire {
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

pub const PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT: usize = 1024;
pub const PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN: usize = 4096;
pub const PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN: usize = 128;
pub const PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlacedSegmentShardBackfillWorkItem {
    pub request: SegmentStoredBytesRequest,
    pub source_cluster_epoch: ClusterEpoch,
    pub desired_cluster_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillRecord {
    pub work_item: PlacedSegmentShardBackfillWorkItem,
    pub remaining_tolerance: u8,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub observation_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillClaimRecord {
    pub work_item: PlacedSegmentShardBackfillWorkItem,
    pub remaining_tolerance: u8,
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillClaimAcquireParams {
    pub claim_id: String,
    pub owner_token: String,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillClaimAcquire {
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

pub const PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT: usize = 1024;
pub const PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN: usize = 4096;
pub const PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN: usize = 128;
pub const PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN: usize = 128;

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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Denormalized public ACL summary stored alongside bucket ACL grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketAclSummary {
    pub public_read: bool,
    pub public_write: bool,
}

/// A live object record (not a delete marker).
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMarkerRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
}

/// Bucket-scoped reclaim root used for synchronous bucket draining.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadReclaimRoot {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
}

/// Durable object payload reclaim kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectPayloadReclaimKind {
    ObjectSegments = 0,
    Multipart = 1,
}

impl ObjectPayloadReclaimKind {
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::ObjectSegments),
            1 => Some(Self::Multipart),
            _ => None,
        }
    }
}

/// Durable object payload reclaim worker claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPayloadReclaimClaimRecord {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub reclaim_kind: ObjectPayloadReclaimKind,
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

/// Durable bucket delete finalizer worker claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteFinalizeClaimRecord {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

/// Durable bucket delete finalization root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteFinalizeRoot {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
}

/// Durable lifecycle sweep worker claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleSweepClaimRecord {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub claimed_at: u64,
    pub heartbeat_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

/// Why a bucket was returned as lifecycle sweep work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleSweepRootSource {
    ExpiredClaim,
    BusyClaim,
    LifecycleConfig,
    AbortingMultipartUpload,
}

/// Durable lifecycle sweep root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleSweepRoot {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
    pub source: LifecycleSweepRootSource,
}

/// Segment entry for a durable standard-object reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSegmentsReclaimSegmentRecord {
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
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
    pub data_pg_id: u32,
    pub ec: EcShape,
}

/// Part entry for a durable multipart reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartReclaimPartRecord {
    ShardSet {
        part_number: u32,
        part_okh: [u8; 16],
        part_vid: GenerationId,
        data_pg_id: u32,
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

impl MultipartReclaimRecord {
    #[must_use]
    pub fn from_object_parts(
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        created_at: u64,
        parts: &[ObjectPartRecord],
        streaming_segments: &[MultipartPartSegmentRecord],
    ) -> Self {
        use std::collections::BTreeMap;

        let mut segments_by_part: BTreeMap<u32, Vec<MultipartReclaimPartSegmentRecord>> =
            BTreeMap::new();
        for segment in streaming_segments {
            segments_by_part
                .entry(segment.part_number)
                .or_default()
                .push(MultipartReclaimPartSegmentRecord {
                    part_number: segment.part_number,
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                });
        }

        Self {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at,
            parts: parts
                .iter()
                .map(|part| {
                    if part.part_okh == [0u8; 16] {
                        MultipartReclaimPartRecord::Segments {
                            part_number: part.part_number,
                            segments: segments_by_part
                                .remove(&part.part_number)
                                .unwrap_or_default(),
                        }
                    } else {
                        MultipartReclaimPartRecord::ShardSet {
                            part_number: part.part_number,
                            part_okh: part.part_okh,
                            part_vid: part.part_vid,
                            data_pg_id: part.data_pg_id,
                            ec: EcShape {
                                k: part.ec_k,
                                m: part.ec_m,
                            },
                        }
                    }
                })
                .collect(),
        }
    }
}

/// A payload shard set whose data PG is explicit in durable metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerPlacedShardSetReference {
    pub data_pg_id: u32,
    pub okh: [u8; 16],
    pub generation_id: GenerationId,
    pub placement_cluster_epoch: ClusterEpoch,
    pub stored_size: u64,
    pub crc64: u64,
    pub ec: EcShape,
}

/// A payload shard set referenced only by reclaim metadata.
///
/// Reclaim references prevent live shard-scavenger scans from treating cleanup
/// work as an unreferenced orphan, but they do not contain enough authoritative
/// payload metadata to schedule data repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerReclaimShardSetReference {
    pub data_pg_id: u32,
    pub okh: [u8; 16],
    pub generation_id: GenerationId,
    pub ec: EcShape,
}

/// A non-streamed MPU part whose data PG is derived from object topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerRoutedMultipartPartReference {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub object_generation_id: GenerationId,
    pub part_number: u32,
    pub stored_size: u64,
    pub crc64: u64,
    pub part_okh: [u8; 16],
    pub part_vid: GenerationId,
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec: EcShape,
}

/// Durable metadata reference to a payload shard set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardScavengerPayloadReference {
    Placed(ShardScavengerPlacedShardSetReference),
    ReclaimOnly(ShardScavengerReclaimShardSetReference),
    RoutedMultipartPart(ShardScavengerRoutedMultipartPartReference),
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

/// Durable bucket write reservation record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketWriteReservationRecord {
    pub bucket: BucketName,
    pub reservation_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
    pub operation_kind: String,
    pub created_at: u64,
    pub lease_deadline: u64,
    pub target_context: Option<String>,
}

/// Durable bucket write-drain state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BucketWriteDrainState {
    Draining = 0,
}

/// Durable bucket write-drain record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketWriteDrainRecord {
    pub bucket: BucketName,
    pub drain_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub state: BucketWriteDrainState,
    pub created_at: u64,
    pub lease_deadline: u64,
}

/// Maximum stored diagnostic detail for a durable DeleteBucket attempt outcome.
pub const BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN: usize = 1024;

/// Durable DeleteBucket attempt outcome kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BucketDeleteAttemptOutcomeKind {
    Retryable = 0,
    NotEmpty = 1,
    StaleGeneration = 2,
    MarkDeleting = 3,
}

/// Durable DeleteBucket begin phase reached by the recorded attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BucketDeleteAttemptPhase {
    Initial = 0,
    ReservationWait = 1,
    PostReservationObjectDrain = 2,
    StreamCleanup = 3,
    FinalVisibilityCheck = 4,
    FinalVisibilityProven = 5,
    MarkDeleting = 6,
}

/// Last durable DeleteBucket attempt outcome for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteAttemptOutcomeRecord {
    pub bucket: BucketName,
    pub drain_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub outcome: BucketDeleteAttemptOutcomeKind,
    pub phase: BucketDeleteAttemptPhase,
    pub detail: String,
    pub post_reservation_next_object_pg_id: Option<u32>,
    pub updated_at: u64,
}

/// Sanitized bucket row fields included in DeleteBucket local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugBucketRow {
    pub state: BucketState,
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
}

/// Sanitized durable bucket write-drain fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugDrain {
    pub drain_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub created_at: u64,
    pub lease_deadline: u64,
}

/// Durable bucket-delete finalizer claim fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugFinalizeClaim {
    pub bucket: BucketName,
    pub matches_bucket: bool,
    pub bucket_incarnation_generation: u64,
    pub claim_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

/// Pending bucket-PG metadata command fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugPendingCommand {
    pub kind: &'static str,
    pub target_bucket: BucketName,
    pub matches_bucket: bool,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub log_index: u64,
}

/// Sanitized object-version row sample included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugObjectVersionSample {
    pub object_pg_id: u32,
    pub kind: BucketDeleteDebugObjectVersionKind,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub generation_id: Option<GenerationId>,
    pub size: Option<u64>,
    pub layout: Option<ObjectLayout>,
    pub last_modified: u64,
    pub became_noncurrent_at: Option<u64>,
}

/// Sanitized object-version row kind included in local-debug output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketDeleteDebugObjectVersionKind {
    Live,
    DeleteMarker,
}

/// Object-version sample scan error fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugObjectVersionSampleError {
    pub object_pg_id: u32,
    pub detail: String,
}

/// Bucket-scoped payload reclaim root fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugPayloadReclaimRoot {
    pub object_pg_id: u32,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub reclaim_kind: Option<ObjectPayloadReclaimKind>,
    pub reclaim_created_at: Option<u64>,
    pub reclaim_item_count: Option<usize>,
}

/// Payload reclaim root scan error fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugPayloadReclaimRootError {
    pub object_pg_id: u32,
    pub detail: String,
}

/// Validity contract for a route map snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteMapValidity {
    Forever,
    Until(RouteMapValidUntilMs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RouteMapValidUntilMs(u64);

impl RouteMapValidUntilMs {
    #[must_use]
    pub const fn new(valid_until_ms: u64) -> Option<Self> {
        if valid_until_ms == u64::MAX {
            None
        } else {
            Some(Self(valid_until_ms))
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl RouteMapValidity {
    #[must_use]
    pub fn until_ms(valid_until_ms: u64) -> Option<Self> {
        RouteMapValidUntilMs::new(valid_until_ms).map(Self::Until)
    }

    #[must_use]
    pub const fn until_ms_saturating(valid_until_ms: u64) -> Self {
        debug_assert!(
            valid_until_ms != u64::MAX,
            "u64::MAX is reserved as the unbounded route-map validity sentinel"
        );
        let valid_until_ms = if valid_until_ms == u64::MAX {
            u64::MAX - 1
        } else {
            valid_until_ms
        };
        Self::Until(RouteMapValidUntilMs(valid_until_ms))
    }

    #[must_use]
    pub fn from_valid_until_ms(valid_until_ms: Option<u64>) -> Option<Self> {
        Some(match valid_until_ms {
            Some(valid_until_ms) => Self::Until(RouteMapValidUntilMs::new(valid_until_ms)?),
            None => Self::Forever,
        })
    }

    #[must_use]
    pub fn valid_until_ms(self) -> Option<u64> {
        match self {
            Self::Forever => None,
            Self::Until(valid_until_ms) => Some(valid_until_ms.get()),
        }
    }

    #[must_use]
    pub fn is_valid_at(self, now_ms: u64) -> bool {
        match self {
            Self::Forever => true,
            Self::Until(valid_until_ms) => valid_until_ms.get() > now_ms,
        }
    }
}

/// Durable object-payload reclaim worker claim fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugPayloadReclaimClaim {
    pub object_pg_id: u32,
    pub bucket: BucketName,
    pub matches_bucket: bool,
    pub bucket_incarnation_generation: u64,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub reclaim_kind: ObjectPayloadReclaimKind,
    pub claim_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: Option<u64>,
    pub attempt_count: u64,
    pub last_error: Option<String>,
}

/// Payload reclaim claim scan error fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugPayloadReclaimClaimError {
    pub object_pg_id: u32,
    pub detail: String,
}

/// Read-only durable state used by the local DeleteBucket debug endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDebugSnapshot {
    pub bucket: BucketName,
    pub pg_id: u32,
    pub cluster_epoch: ClusterEpoch,
    pub operation_epoch: ClusterEpoch,
    pub route_map_valid_until_ms: Option<u64>,
    pub bucket_pg_primary_node_id: u32,
    pub bucket_row: Option<BucketDeleteDebugBucketRow>,
    pub durable_write_drain: Option<BucketDeleteDebugDrain>,
    pub finalize_claim: Option<BucketDeleteDebugFinalizeClaim>,
    pub pending_metadata_command: Option<BucketDeleteDebugPendingCommand>,
    pub object_version_samples: Vec<BucketDeleteDebugObjectVersionSample>,
    pub object_version_sample_errors: Vec<BucketDeleteDebugObjectVersionSampleError>,
    pub payload_reclaim_roots: Vec<BucketDeleteDebugPayloadReclaimRoot>,
    pub payload_reclaim_root_errors: Vec<BucketDeleteDebugPayloadReclaimRootError>,
    pub payload_reclaim_claims: Vec<BucketDeleteDebugPayloadReclaimClaim>,
    pub payload_reclaim_claim_errors: Vec<BucketDeleteDebugPayloadReclaimClaimError>,
    pub attempt_outcome: Option<BucketDeleteAttemptOutcomeRecord>,
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
    /// Monotonic generation incremented on every bucket metadata mutation.
    pub bucket_execution_generation: u64,
    /// Stable generation identifying this bucket create/delete incarnation.
    pub bucket_incarnation_generation: u64,
    pub(crate) multipart_upload_id_key: MultipartUploadIdKey,
    /// Whether bucket ABAC is enabled for `s3:BucketTag/${TagKey}` evaluation.
    pub bucket_abac_enabled: bool,
    /// Effective bucket encryption semantics used on hot paths.
    pub encryption: EffectiveBucketEncryptionConfig,
}

impl BucketInfo {
    pub fn multipart_upload_id_key(&self) -> &MultipartUploadIdKey {
        &self.multipart_upload_id_key
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BucketSnapshotTagsRequest {
    #[default]
    NotRequested,
    IfBucketAbacEnabled,
    Always,
}

impl BucketSnapshotTagsRequest {
    #[must_use]
    pub const fn should_load(self, bucket: &BucketInfo) -> bool {
        match self {
            Self::NotRequested => false,
            Self::IfBucketAbacEnabled => bucket.bucket_abac_enabled,
            Self::Always => true,
        }
    }

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Always, _) | (_, Self::Always) => Self::Always,
            (Self::IfBucketAbacEnabled, _) | (_, Self::IfBucketAbacEnabled) => {
                Self::IfBucketAbacEnabled
            }
            (Self::NotRequested, Self::NotRequested) => Self::NotRequested,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketSnapshotRequest {
    pub policy: bool,
    pub tags: BucketSnapshotTagsRequest,
    pub lifecycle: bool,
    pub cors: bool,
}

impl BucketSnapshotRequest {
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self {
            policy: self.policy || other.policy,
            tags: self.tags.union(other.tags),
            lifecycle: self.lifecycle || other.lifecycle,
            cors: self.cors || other.cors,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LoadedBucketSubresource<T> {
    #[default]
    NotRequested,
    Missing,
    Loaded(T),
}

impl<T> LoadedBucketSubresource<T> {
    #[must_use]
    pub const fn is_requested(&self) -> bool {
        !matches!(self, Self::NotRequested)
    }

    #[must_use]
    pub fn as_ref(&self) -> LoadedBucketSubresource<&T> {
        match self {
            Self::NotRequested => LoadedBucketSubresource::NotRequested,
            Self::Missing => LoadedBucketSubresource::Missing,
            Self::Loaded(value) => LoadedBucketSubresource::Loaded(value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketSnapshot {
    pub bucket: BucketInfo,
    pub request: BucketSnapshotRequest,
    pub policy: LoadedBucketSubresource<String>,
    pub tags: LoadedBucketSubresource<String>,
    pub lifecycle: LoadedBucketSubresource<String>,
    pub cors: LoadedBucketSubresource<String>,
}

#[derive(Debug, Clone)]
pub enum BucketSnapshotPair {
    Same {
        bucket: Box<BucketSnapshot>,
    },
    Distinct {
        source: Box<BucketSnapshot>,
        destination: Box<BucketSnapshot>,
    },
}

impl BucketSnapshotPair {
    #[must_use]
    pub const fn source(&self) -> &BucketSnapshot {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { source, .. } => source,
        }
    }

    #[must_use]
    pub const fn destination(&self) -> &BucketSnapshot {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { destination, .. } => destination,
        }
    }
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
            .field(
                "bucket_execution_generation",
                &self.bucket_execution_generation,
            )
            .field(
                "bucket_incarnation_generation",
                &self.bucket_incarnation_generation,
            )
            .field("bucket_abac_enabled", &self.bucket_abac_enabled)
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
    pub ownership_controls: BucketOwnershipControls,
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
    pub public_access_block: Option<PublicAccessBlockConfig>,
    pub ownership_controls: Option<BucketOwnershipControls>,
    pub bucket_policy_present: bool,
    pub bucket_policy_public: bool,
    pub bucket_policy_generation: u64,
    pub policy: BucketFastPathPolicy,
    pub bucket_lifecycle_present: bool,
    pub bucket_lifecycle_generation: u64,
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
    pub multipart_upload_id_key: MultipartUploadIdKey,
    pub bucket_abac_enabled: bool,
    pub tags: BucketFastPathTags,
    pub encryption: EffectiveBucketEncryptionConfig,
}

/// Durable identity used to prove a bucket fast-path cache entry is current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketFastPathIdentity {
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
}

impl BucketFastPathInfo {
    #[must_use]
    pub const fn identity(&self) -> BucketFastPathIdentity {
        BucketFastPathIdentity {
            bucket_execution_generation: self.bucket_execution_generation,
            bucket_incarnation_generation: self.bucket_incarnation_generation,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum BucketFastPathPolicy {
    Absent,
    Loaded(String),
}

impl std::fmt::Debug for BucketFastPathPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("Absent"),
            Self::Loaded(_) => f.write_str("Loaded(<redacted>)"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum BucketFastPathTags {
    NotApplicable,
    Missing,
    Loaded(String),
}

impl std::fmt::Debug for BucketFastPathTags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotApplicable => f.write_str("NotApplicable"),
            Self::Missing => f.write_str("Missing"),
            Self::Loaded(_) => f.write_str("Loaded(<redacted>)"),
        }
    }
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
            .field("public_access_block", &self.public_access_block)
            .field("ownership_controls", &self.ownership_controls)
            .field("bucket_policy_present", &self.bucket_policy_present)
            .field("bucket_policy_public", &self.bucket_policy_public)
            .field("bucket_policy_generation", &self.bucket_policy_generation)
            .field("policy", &self.policy)
            .field("bucket_lifecycle_present", &self.bucket_lifecycle_present)
            .field(
                "bucket_lifecycle_generation",
                &self.bucket_lifecycle_generation,
            )
            .field(
                "bucket_execution_generation",
                &self.bucket_execution_generation,
            )
            .field(
                "bucket_incarnation_generation",
                &self.bucket_incarnation_generation,
            )
            .field("multipart_upload_id_key", &self.multipart_upload_id_key)
            .field("bucket_abac_enabled", &self.bucket_abac_enabled)
            .field("tags", &self.tags)
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
            sse_c_blocked: match self.default_encryption {
                Some(_) => self.sse_c_blocked,
                None => true,
            },
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
            sse_c_blocked: true,
        }
    }
}

impl From<&BucketSnapshot> for BucketFastPathInfo {
    fn from(snapshot: &BucketSnapshot) -> Self {
        Self {
            name: snapshot.bucket.name.clone(),
            owner_principal: snapshot.bucket.owner_principal.clone(),
            owner_canonical_id: snapshot.bucket.owner_canonical_id.clone(),
            created_at: snapshot.bucket.created_at,
            state: snapshot.bucket.state,
            versioning: snapshot.bucket.versioning,
            object_lock: snapshot.bucket.object_lock,
            public_access_block: snapshot.bucket.public_access_block,
            ownership_controls: snapshot.bucket.ownership_controls,
            bucket_policy_present: snapshot.bucket.bucket_policy_present,
            bucket_policy_public: snapshot.bucket.bucket_policy_public,
            bucket_policy_generation: snapshot.bucket.bucket_policy_generation,
            policy: match &snapshot.policy {
                LoadedBucketSubresource::Loaded(policy) => {
                    BucketFastPathPolicy::Loaded(policy.clone())
                }
                LoadedBucketSubresource::Missing | LoadedBucketSubresource::NotRequested => {
                    BucketFastPathPolicy::Absent
                }
            },
            bucket_lifecycle_present: snapshot.bucket.bucket_lifecycle_present,
            bucket_lifecycle_generation: snapshot.bucket.bucket_lifecycle_generation,
            bucket_execution_generation: snapshot.bucket.bucket_execution_generation,
            bucket_incarnation_generation: snapshot.bucket.bucket_incarnation_generation,
            multipart_upload_id_key: snapshot.bucket.multipart_upload_id_key.clone(),
            bucket_abac_enabled: snapshot.bucket.bucket_abac_enabled,
            tags: match &snapshot.tags {
                LoadedBucketSubresource::Loaded(tags) => BucketFastPathTags::Loaded(tags.clone()),
                LoadedBucketSubresource::Missing => BucketFastPathTags::Missing,
                LoadedBucketSubresource::NotRequested => BucketFastPathTags::NotApplicable,
            },
            encryption: snapshot.bucket.encryption,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn validate(&self) -> Result<(), PutLiveObjectValidationError> {
        validate_live_object_etag_layout(&self.etag, &self.layout)
    }
}

fn validate_live_object_etag_layout(
    etag: &ObjectEtag,
    layout: &ObjectLayout,
) -> Result<(), PutLiveObjectValidationError> {
    match (etag, layout) {
        (ObjectEtag::SinglePart(_), ObjectLayout::Standard) => Ok(()),
        (
            ObjectEtag::MultipartComposite { parts, .. },
            ObjectLayout::MultipartManifest { parts_count },
        ) => {
            if parts == parts_count {
                Ok(())
            } else {
                Err(PutLiveObjectValidationError::MultipartPartsCountMismatch {
                    etag_parts: parts.get(),
                    layout_parts: parts_count.get(),
                })
            }
        }
        (ObjectEtag::SinglePart(_), ObjectLayout::MultipartManifest { .. }) => {
            Err(PutLiveObjectValidationError::SinglePartEtagWithMultipartLayout)
        }
        (ObjectEtag::MultipartComposite { .. }, ObjectLayout::Standard) => {
            Err(PutLiveObjectValidationError::MultipartCompositeEtagWithStandardLayout)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutLiveObjectValidationError {
    #[error("etag parts count does not match layout parts count")]
    MultipartPartsCountMismatch { etag_parts: u32, layout_parts: u32 },
    #[error("single-part etag with multipart layout")]
    SinglePartEtagWithMultipartLayout,
    #[error("multipart composite etag with standard layout")]
    MultipartCompositeEtagWithStandardLayout,
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

#[derive(Debug, Clone)]
pub struct StreamPutFinalizeSnapshot {
    pub session: StreamUploadRecord,
    pub existing_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPutFinalizeStorageSnapshot {
    pub session: StreamUploadRecord,
    pub existing_etag: Option<String>,
    pub generation_id: GenerationId,
    pub(crate) stale_payload_source: Option<StoredObject>,
    pub(crate) stale_payload: Option<crate::metadata_command::ObjectPayloadReclaimCommand>,
    pub staging_segments: Vec<StreamUploadSegmentRecord>,
}

#[derive(Debug, Clone)]
pub struct StreamPutCommitInput {
    pub versioning: BucketVersioningState,
    pub version_id: VersionId,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub size: u64,
    pub etag_crc64: u64,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

#[derive(Debug, Clone)]
pub struct PreparedStreamPutCommit<T> {
    pub value: T,
    pub versioning: BucketVersioningState,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub size: u64,
    pub etag_crc64: u64,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
}

#[derive(Debug, Clone)]
pub struct WrittenShardAck {
    pub key: ShardKey,
    pub ack: WriteAck,
}

#[derive(Debug, Clone)]
pub struct DirectPutWrittenSegment {
    pub data_pg_id: u32,
    pub ec: EcShape,
    pub written_shards: Vec<WrittenShardAck>,
}

#[derive(Debug, Clone)]
pub struct CommitDirectPutObjectReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_reservation_id: SessionId,
    pub versioning: BucketVersioningState,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    /// CRC64-NVME of the object data.
    pub etag_crc64: u64,
    pub ec: EcShape,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
    pub segment_index: u32,
    pub segment_crc64: u64,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    pub bucket_write_reservation: crate::BucketWriteReservationProof,
}

#[derive(Debug, Clone)]
pub struct BeginUploadPartStreamSessionReq {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub part_number: u32,
    pub session_id: SessionId,
    pub bucket_write_reservation: crate::BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPutCommitSnapshot {
    pub existing_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPutCommitStorageSnapshot {
    pub auth_snapshot: DirectPutCommitSnapshot,
    pub current: Option<StoredObject>,
    pub committed_segments: Option<Vec<ObjectSegmentRecord>>,
    pub committed_stale_generation_id: Option<GenerationId>,
    pub(crate) stale_payload_source: Option<StoredObject>,
    pub(crate) stale_payload: Option<crate::metadata_command::ObjectPayloadReclaimCommand>,
}

#[derive(Debug, Clone)]
pub struct FinalizeDirectPutObjectOutcome {
    pub version_id: VersionId,
    pub encryption: ObjectEncryption,
    pub live_tags: Option<SerializedTagSet>,
    pub live_size: u64,
    pub live_last_modified: u64,
    pub stale_generation_id: Option<GenerationId>,
}

#[derive(Debug, Clone)]
pub struct FinalizeStreamPutOutcome<T> {
    pub value: T,
    pub version_id: VersionId,
    pub encryption: ObjectEncryption,
    pub live_tags: Option<SerializedTagSet>,
    pub live_size: u64,
    pub live_last_modified: u64,
    pub stale_generation_id: Option<GenerationId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReadSnapshot {
    pub stored: StoredObject,
    pub object_segments: Vec<ObjectSegmentRecord>,
    pub multipart_parts: Vec<ObjectPartRecord>,
    pub multipart_part_segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReadAuthSubject {
    pub stored: StoredObject,
    pub identity: ObjectReadAuthSubjectIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReadAuthSubjectIdentity {
    stored: StoredObject,
}

impl ObjectReadAuthSubjectIdentity {
    pub fn for_stored(stored: &StoredObject) -> Self {
        Self {
            stored: stored.clone(),
        }
    }

    pub(crate) fn stored(&self) -> &StoredObject {
        &self.stored
    }

    pub fn matches_stored(&self, stored: &StoredObject) -> bool {
        &self.stored == stored
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectReadSnapshotMode {
    MetadataOnly,
    StandardSegments,
    MultipartParts,
    FullPayloadLayout,
}

#[derive(Debug, Clone)]
pub struct ObjectReadSnapshotOutcome<T> {
    pub value: T,
    pub snapshot: ObjectReadSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletedSpecificObjectVersion {
    Missing,
    DeleteMarker,
    Live {
        generation_id: GenerationId,
        layout: ObjectLayout,
    },
}

#[derive(Debug)]
pub struct DeleteSpecificObjectVersionOutcome<T> {
    pub value: T,
    pub deleted: DeletedSpecificObjectVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletedCurrentObject {
    Missing,
    DeleteMarker,
    Live {
        generation_id: GenerationId,
        layout: ObjectLayout,
    },
}

#[derive(Debug)]
pub struct DeleteCurrentObjectOutcome<T> {
    pub value: T,
    pub deleted: DeletedCurrentObject,
}

#[derive(Debug)]
pub struct InsertCurrentDeleteMarkerOutcome<T> {
    pub value: T,
    pub version_id: VersionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpireCurrentObjectOutcome {
    pub reclaim_generation_id: Option<GenerationId>,
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
    pub start_at: Option<ObjectKey>,
    pub max_keys: u32,
}

/// Response from a list object versions query.
pub struct ListObjectVersionsResp {
    pub versions: Vec<StoredObject>,
    pub is_truncated: bool,
    pub next_key_marker: Option<ObjectKey>,
    pub next_version_id_marker: Option<VersionId>,
}

#[derive(Debug, Clone)]
pub struct ListedBucketObjects {
    pub objects: Vec<StoredObject>,
    pub common_prefixes: Vec<ObjectKey>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<ObjectKey>,
}

#[derive(Debug, Clone)]
pub struct ListedBucketObjectVersions {
    pub versions: Vec<StoredObject>,
    pub common_prefixes: Vec<ObjectKey>,
    pub is_truncated: bool,
    pub next_key_marker: Option<ObjectKey>,
    pub next_version_id_marker: Option<VersionId>,
}

#[derive(Debug, Clone)]
pub struct LifecycleSweepBuckets {
    pub lifecycle_buckets: Vec<BucketInfo>,
    pub aborting_buckets: Vec<BucketName>,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub initiator: OwnerIdentity,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    /// Final object payload generation reserved for this upload.
    pub object_generation_id: GenerationId,
    /// Pending Object Lock state to apply to the committed object version.
    pub object_lock: ObjectLockState,
    /// Validated checksum configuration for this upload.
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
}

/// Multipart upload record that has already passed caller authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedMultipartUploadRecord {
    upload: MultipartUploadRecord,
}

impl AuthorizedMultipartUploadRecord {
    pub fn assume_authorized(upload: MultipartUploadRecord) -> Self {
        Self { upload }
    }

    pub fn record(&self) -> &MultipartUploadRecord {
        &self.upload
    }

    pub fn into_record(self) -> MultipartUploadRecord {
        self.upload
    }
}

impl std::ops::Deref for AuthorizedMultipartUploadRecord {
    type Target = MultipartUploadRecord;

    fn deref(&self) -> &Self::Target {
        &self.upload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartUploadManagementLookup {
    InProgress(Box<MultipartUploadRecord>),
    NonInProgress(Box<MultipartUploadRecord>),
    Replay(Box<MultipartCompletionReplay>),
    Missing,
}

/// In-progress multipart part record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartRecord {
    pub upload_id: UploadId,
    pub part_number: u32,
    pub generation: u32,
    pub size: u64,
    /// CRC64-NVME over the logical part bytes used for internal storage repair.
    pub payload_crc64: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
    /// 16-byte object key hash for shard keys.
    pub part_okh: [u8; 16],
    /// Per-part payload generation for shard keys.
    pub part_vid: GenerationId,
    /// Cluster-map epoch used when this direct part shard set was written.
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<ChecksumBytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartCompletionSnapshot {
    pub existing_etag: Option<String>,
    pub stale_payload_source: Option<StoredObject>,
    pub part_records: Vec<MultipartPartRecord>,
    pub selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub cleanup: CompleteMultipartCommitCleanup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartCompletionPreflight {
    pub existing_etag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartCompletionFingerprint([u8; 32]);

impl MultipartCompletionFingerprint {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Exact completion replay state whose lifetime is the completed object row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartCompletionReplay {
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub fingerprint: MultipartCompletionFingerprint,
    pub version_id: VersionId,
    pub etag: ObjectEtag,
    pub size: u64,
    pub last_modified: u64,
    pub tags: Option<SerializedTagSet>,
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    pub encryption: ObjectEncryption,
}

#[derive(Debug, Clone)]
pub enum CompletedMultipartStalePayload {
    Segments {
        generation_id: GenerationId,
        segments: Vec<ObjectSegmentRecord>,
    },
    Multipart {
        generation_id: GenerationId,
        parts: Vec<ObjectPartRecord>,
        streaming_segments: Vec<MultipartPartSegmentRecord>,
    },
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartCommitRequest {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub completion_fingerprint: MultipartCompletionFingerprint,
    pub versioning: BucketVersioningState,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub generation_id: GenerationId,
    pub size: u64,
    pub etag_crc64: [u8; 8],
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: Option<SerializedMetadataBlob>,
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
    pub expected_stale_payload_source: Option<StoredObject>,
    pub part_records: Vec<MultipartPartRecord>,
    pub selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub expected_cleanup: CompleteMultipartCommitCleanup,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompleteMultipartCommitCleanup {
    pub omitted_parts: Vec<MultipartPartRecord>,
    pub omitted_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub stream_uploads: Vec<TerminalStreamCleanupRecord>,
    pub stream_upload_segments: Vec<StreamUploadSegmentRecord>,
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartCommitOutcome {
    pub version_id: VersionId,
    pub stale_payload: Option<CompletedMultipartStalePayload>,
    pub live_tags: Option<SerializedTagSet>,
    pub live_size: u64,
    pub live_last_modified: u64,
}

/// Committed part record in the object manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPartRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub part_number: u32,
    pub size: u64,
    /// CRC64-NVME over the logical part bytes used for internal storage repair.
    pub payload_crc64: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
    pub part_okh: [u8; 16],
    pub part_vid: GenerationId,
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
    /// PG where this part's shards are stored.
    pub data_pg_id: u32,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMultipartUploadReq {
    pub upload_id: UploadId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub initiator: OwnerIdentity,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    /// Pending Object Lock state to copy onto the completed version.
    pub object_lock: ObjectLockState,
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
}

#[derive(Debug)]
pub struct CreateMultipartUploadOutcome<T> {
    pub value: T,
    pub initiated_at: u64,
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

#[derive(Debug, Clone)]
pub struct ListedBucketMultipartUploads {
    pub uploads: Vec<MultipartUploadRecord>,
}

/// Request to list parts of a multipart upload.
pub struct ListPartsReq {
    pub upload_id: UploadId,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Response from listing parts of a multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartsResp {
    pub parts: Vec<MultipartPartRecord>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedMultipartParts {
    pub upload: MultipartUploadRecord,
    pub response: ListPartsResp,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadRecord {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub state: StreamUploadState,
    pub created_at: u64,
    pub encryption: ObjectEncryption,
    pub next_segment_vid: GenerationId,
    pub bucket_write_reservation: Option<crate::BucketWriteReservationProof>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadRecordPage {
    pub uploads: Vec<StreamUploadRecord>,
    pub next_session_id_marker: Option<SessionId>,
}

/// Command-owned stream session fields used by `CreateStreamUpload`.
///
/// The initial stream segment VID allocator floor is stored separately on the
/// command so retry matching must handle allocator state explicitly instead of
/// accidentally comparing broad runtime records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadCommandRecord {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub state: StreamUploadState,
    pub created_at: u64,
    pub encryption: ObjectEncryption,
}

impl From<&StreamUploadRecord> for StreamUploadCommandRecord {
    fn from(record: &StreamUploadRecord) -> Self {
        Self {
            session_id: record.session_id.clone(),
            bucket: record.bucket.clone(),
            key: record.key.clone(),
            target: record.target.clone(),
            state: record.state,
            created_at: record.created_at,
            encryption: record.encryption.clone(),
        }
    }
}

/// Command-owned stream session fields required by terminal MPU cleanup.
///
/// Runtime allocator state such as `next_segment_vid` is intentionally not part
/// of this record. Terminal MPU commands validate and delete the session, but
/// do not own the stream segment VID floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalStreamCleanupRecord {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub state: StreamUploadState,
    pub created_at: u64,
    pub encryption: ObjectEncryption,
}

impl From<&StreamUploadRecord> for TerminalStreamCleanupRecord {
    fn from(record: &StreamUploadRecord) -> Self {
        Self {
            session_id: record.session_id.clone(),
            bucket: record.bucket.clone(),
            key: record.key.clone(),
            target: record.target.clone(),
            state: record.state,
            created_at: record.created_at,
            encryption: record.encryption.clone(),
        }
    }
}

/// Request to create a streaming upload session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamUploadReq {
    pub session_id: SessionId,
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub target: StreamUploadTarget,
    pub encryption: ObjectEncryption,
}

/// Request to append one staging segment to an in-progress streaming session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareStreamUploadSegmentAppendReq {
    pub session_id: SessionId,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the bytes written to storage.
    pub segment_crc64: u64,
    /// CRC64-NVME over the user-visible plaintext payload bytes.
    pub payload_crc64: u64,
    /// 16-byte object key hash for shard keys.
    pub segment_okh: [u8; 16],
}

/// Staging segment record for an in-progress streaming session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadSegmentRecord {
    pub session_id: SessionId,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the bytes written to storage.
    pub segment_crc64: u64,
    /// CRC64-NVME over the user-visible plaintext payload bytes.
    pub payload_crc64: u64,
    /// 16-byte object key hash for shard keys.
    pub segment_okh: [u8; 16],
    /// Payload generation for shard keys.
    pub segment_vid: GenerationId,
    /// PG where this segment's shards are stored.
    pub data_pg_id: u32,
    /// Cluster-map epoch used to place this segment's shard set.
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadPartSnapshot {
    pub session: StreamUploadRecord,
    pub upload: MultipartUploadRecord,
    pub existing_part_generation: Option<u32>,
    pub staging_segments: Vec<StreamUploadSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamUploadPartStorageSnapshot {
    pub auth_snapshot: StreamUploadPartSnapshot,
    pub existing_part: Option<MultipartPartRecord>,
    pub displaced_segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug)]
pub struct PreparedStreamPartCommit<T> {
    pub value: T,
    pub part: MultipartPartRecord,
    pub segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug)]
pub struct FinalizeStreamPartOutcome<T> {
    pub value: T,
    pub last_modified: u64,
}

#[derive(Debug, Clone)]
pub struct FinalizeStreamPartCleanup {
    pub upload: MultipartUploadRecord,
    pub existing_part: Option<MultipartPartRecord>,
    pub displaced_segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortMultipartUploadCleanup {
    pub upload: MultipartUploadRecord,
    pub parts: Vec<MultipartPartRecord>,
    pub streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub stream_uploads: Vec<TerminalStreamCleanupRecord>,
    pub stream_upload_segments: Vec<StreamUploadSegmentRecord>,
}

/// Committed segment record for a normal PutObject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSegmentRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the logical segment bytes.
    pub segment_crc64: u64,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    /// Cluster-map epoch used to place this segment's shard set.
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
}

/// Committed segment record for a multipart part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartSegmentRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub version_id: u64,
    pub part_number: u32,
    pub segment_index: u32,
    pub size: u64,
    /// CRC64-NVME over the logical segment bytes.
    pub segment_crc64: u64,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    /// Cluster-map epoch used to place this segment's shard set.
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn cluster_epoch_is_distinct_from_generation_id() {
        let epoch = ClusterEpoch::new(1).unwrap();
        let generation = GenerationId::new(1).unwrap();

        assert_eq!(epoch, ClusterEpoch::INITIAL);
        assert_eq!(epoch.get(), generation.get());
        assert!(ClusterEpoch::new(0).is_none());
        assert!(GenerationId::new(0).is_none());
    }

    #[test]
    fn typed_pg_ids_preserve_raw_id_but_not_role() {
        let raw = PgId::new(7);
        let bucket_pg = BucketPgId::new(raw);
        let object_pg = ObjectMetadataPgId::new(raw);
        let data_pg = DataPgId::new(raw);

        assert_eq!(bucket_pg.get(), 7);
        assert_eq!(object_pg.get(), 7);
        assert_eq!(data_pg.get(), 7);
        assert_eq!(PgId::from(bucket_pg), raw);
        assert_eq!(PgId::from(object_pg), raw);
        assert_eq!(PgId::from(data_pg), raw);
    }

    #[test]
    fn shard_index_round_trips() {
        let index = ShardIndex::new(31);
        assert_eq!(index.get(), 31);
        assert_eq!(u8::from(index), 31);
        assert_eq!(ShardIndex::from(31), index);
    }

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
    fn object_encryption_decode_reports_typed_errors() {
        assert_eq!(
            ObjectEncryption::decode(ObjectEncryptionType::None, Some(vec![1])).unwrap_err(),
            ObjectEncryptionDecodeError::UnexpectedStateForUnencryptedObject
        );
        assert_eq!(
            ObjectEncryption::decode(ObjectEncryptionType::SseCustomer, None).unwrap_err(),
            ObjectEncryptionDecodeError::MissingSseCustomerState
        );
        assert_eq!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, None).unwrap_err(),
            ObjectEncryptionDecodeError::MissingSseS3State
        );

        assert_eq!(
            SseS3ObjectState::decode(&[]).unwrap_err(),
            ObjectEncryptionDecodeError::InvalidSseS3StateLength {
                actual: 0,
                minimum: SseS3ObjectState::FIXED_ENCODED_LEN,
            }
        );

        let state = SseS3ObjectState {
            wrapping_key_id: 7,
            wrap_nonce: [1u8; SSE_S3_WRAP_NONCE_LEN],
            wrapped_dek: [2u8; SSE_S3_WRAPPED_DEK_LEN],
            segment_nonce_prefix: [3u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            checksum_nonce: [4u8; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: Vec::new(),
        };
        let mut encoded = state.encode();
        encoded[0] = 99;
        assert_eq!(
            SseS3ObjectState::decode(&encoded).unwrap_err(),
            ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion { version: 99 }
        );

        let mut encoded = state.encode();
        let checksum_len_offset = SseS3ObjectState::FIXED_ENCODED_LEN - 2;
        encoded[checksum_len_offset..checksum_len_offset + 2].copy_from_slice(&1u16.to_be_bytes());
        assert_eq!(
            SseS3ObjectState::decode(&encoded).unwrap_err(),
            ObjectEncryptionDecodeError::InvalidSseS3ChecksumMetadataLength {
                declared: 1,
                remaining: 0,
            }
        );

        assert_eq!(
            SseCustomerObjectState::decode(&[]).unwrap_err(),
            ObjectEncryptionDecodeError::InvalidSseCustomerStateLength {
                actual: 0,
                minimum: SseCustomerObjectState::FIXED_ENCODED_LEN,
            }
        );
    }

    #[test]
    fn put_live_object_validate_reports_typed_etag_layout_errors() {
        let single = ObjectEtag::SinglePart([1; 8]);
        let multipart = ObjectEtag::MultipartComposite {
            crc64: [2; 8],
            parts: std::num::NonZeroU32::new(2).unwrap(),
        };
        let standard = ObjectLayout::Standard;
        let multipart_two = ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(2).unwrap(),
        };
        let multipart_three = ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(3).unwrap(),
        };

        assert_eq!(validate_live_object_etag_layout(&single, &standard), Ok(()));
        assert_eq!(
            validate_live_object_etag_layout(&multipart, &multipart_two),
            Ok(())
        );
        assert_eq!(
            validate_live_object_etag_layout(&multipart, &multipart_three).unwrap_err(),
            PutLiveObjectValidationError::MultipartPartsCountMismatch {
                etag_parts: 2,
                layout_parts: 3,
            }
        );
        assert_eq!(
            validate_live_object_etag_layout(&single, &multipart_two).unwrap_err(),
            PutLiveObjectValidationError::SinglePartEtagWithMultipartLayout
        );
        assert_eq!(
            validate_live_object_etag_layout(&multipart, &standard).unwrap_err(),
            PutLiveObjectValidationError::MultipartCompositeEtagWithStandardLayout
        );
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
            bucket_execution_generation: 9,
            bucket_incarnation_generation: 9,
            multipart_upload_id_key: MultipartUploadIdKey::from_bytes([1; 32]),
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        };
        let debug = format!("{info:?}");
        assert!(debug.contains(r#""own\ner""#));
        assert!(debug.contains("bucket_policy_present"));
        assert!(debug.contains("bucket_lifecycle_present"));
        assert!(!debug.contains("secret-policy"));
        assert!(!debug.contains("<LifecycleConfiguration>secret</LifecycleConfiguration>"));

        let fast_debug = format!(
            "{:?}",
            BucketFastPathInfo {
                name: info.name.clone(),
                owner_principal: info.owner_principal.clone(),
                owner_canonical_id: info.owner_canonical_id.clone(),
                created_at: info.created_at,
                state: info.state,
                versioning: info.versioning,
                object_lock: info.object_lock,
                public_access_block: info.public_access_block,
                ownership_controls: info.ownership_controls,
                bucket_policy_present: info.bucket_policy_present,
                bucket_policy_public: info.bucket_policy_public,
                bucket_policy_generation: info.bucket_policy_generation,
                policy: BucketFastPathPolicy::Loaded("secret-policy".to_string()),
                bucket_lifecycle_present: info.bucket_lifecycle_present,
                bucket_lifecycle_generation: info.bucket_lifecycle_generation,
                bucket_execution_generation: info.bucket_execution_generation,
                bucket_incarnation_generation: info.bucket_incarnation_generation,
                multipart_upload_id_key: info.multipart_upload_id_key.clone(),
                bucket_abac_enabled: info.bucket_abac_enabled,
                tags: BucketFastPathTags::Loaded("secret-tags".to_string()),
                encryption: info.encryption,
            }
        );
        assert!(fast_debug.contains(r#""own\ner""#));
        assert!(!fast_debug.contains("secret-policy"));
        assert!(!fast_debug.contains("secret-tags"));
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
        assert_eq!(key2.shard_index(), ShardIndex::new(7));
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
        let upload = UploadId::try_from(format!("{}._", "A".repeat(126))).unwrap();
        let session = SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap();

        assert_eq!(format!("{bucket:?}"), r#""bucket-1""#);
        assert_eq!(format!("{key:?}"), r#""obj\t\u{1b}[31m""#);
        assert_eq!(format!("{upload:?}"), format!(r#""{}._""#, "A".repeat(126)));
        assert_eq!(
            format!("{session:?}"),
            r#""0123456789abcdef0123456789abcdef""#
        );
    }

    #[test]
    fn session_id_try_from_rejects_invalid_inputs() {
        assert_eq!(
            SessionId::try_from("abc").unwrap_err(),
            SessionIdError::InvalidLength { length: 3 }
        );
        assert_eq!(
            SessionId::try_from("0123456789abcdef0123456789abcdeg").unwrap_err(),
            SessionIdError::InvalidCharacterSet
        );
        assert_eq!(
            SessionId::try_from("0123456789abcdef0123456789abcdeF").unwrap_err(),
            SessionIdError::InvalidCharacterSet
        );
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
    fn upload_id_try_from_rejects_invalid_inputs() {
        assert_eq!(
            UploadId::try_from("x".repeat(127)).unwrap_err(),
            UploadIdError::InvalidLength { length: 127 }
        );
        assert_eq!(
            UploadId::try_from("x".repeat(129)).unwrap_err(),
            UploadIdError::InvalidLength { length: 129 }
        );
        assert_eq!(
            UploadId::try_from(format!("{}-", "x".repeat(127))).unwrap_err(),
            UploadIdError::InvalidCharacterSet
        );
    }

    #[test]
    fn upload_id_try_from_accepts_aws_observed_shape() {
        let upload_id = UploadId::try_from(format!(
            "{}{}{}{}",
            "A".repeat(32),
            "a".repeat(32),
            "0".repeat(32),
            "._".repeat(16)
        ))
        .unwrap();
        assert_eq!(upload_id.as_str().len(), UPLOAD_ID_LEN);
    }

    #[test]
    fn multipart_upload_id_key_binds_issued_id_to_bucket_and_key() {
        let signing_key = MultipartUploadIdKey::from_bytes([0x5a; 32]);
        let bucket = BucketName::try_from("bucket-one").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let upload_id = signing_key.issue(&bucket, &key, "primary").unwrap();

        assert!(signing_key.authenticates(&bucket, &key, &upload_id));
        assert!(signing_key.was_issued_for_principal(&upload_id, "primary"));
        assert!(!signing_key.was_issued_for_principal(&upload_id, "alternate"));
        assert!(!signing_key.authenticates(
            &BucketName::try_from("bucket-two").unwrap(),
            &key,
            &upload_id
        ));
        assert!(!signing_key.authenticates(
            &bucket,
            &ObjectKey::try_from("path/to/other").unwrap(),
            &upload_id
        ));

        let mut mutated = upload_id.as_str().as_bytes().to_vec();
        mutated[0] = if mutated[0] == b'A' { b'B' } else { b'A' };
        let mutated = UploadId::try_from(String::from_utf8(mutated).unwrap()).unwrap();
        assert!(!signing_key.authenticates(&bucket, &key, &mutated));
        assert!(
            !MultipartUploadIdKey::from_bytes([0xa5; 32]).authenticates(&bucket, &key, &upload_id)
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

    #[test]
    fn upload_id_from_sql_rejects_invalid_rows() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE t (upload_id TEXT NOT NULL)", [])
            .unwrap();
        conn.execute("INSERT INTO t (upload_id) VALUES (?1)", ["short"])
            .unwrap();

        let err = conn
            .query_row("SELECT upload_id FROM t", [], |row| {
                row.get::<_, UploadId>(0)
            })
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
