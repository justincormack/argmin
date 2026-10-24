// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

/// Core types for the storage layer.
use crate::error::StoreError;
use argmin_crypto::hmac::Sha256Key;
use std::{num::NonZeroU64, str::FromStr, sync::Arc};

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

/// Immutable effective deadline carried from a frontend route admission to a
/// storage-node durable-effect boundary.
///
/// The route map used internally by a storage client can be renewed while a
/// request is in flight. This fence preserves both the authority timestamp and
/// the admission's conservatively bound process-monotonic deadline so a
/// renewal cannot extend its authority to acquire a reservation or install a
/// pending metadata command. Monotonic timestamps never cross an RPC boundary:
/// remote clients derive a portable wall-clock upper bound, and the receiving
/// host conservatively binds it to its own monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmittedRouteEffectFence {
    cluster_epoch: ClusterEpoch,
    deadline: Option<AdmittedRouteEffectDeadline>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmittedRouteEffectDeadline {
    authority_valid_until_ms: u64,
    local_valid_until_monotonic_ms: u64,
}

impl AdmittedRouteEffectFence {
    pub(crate) fn unbounded(cluster_epoch: ClusterEpoch) -> Self {
        Self {
            cluster_epoch,
            deadline: None,
        }
    }

    pub(crate) fn bounded(
        cluster_epoch: ClusterEpoch,
        authority_valid_until_ms: u64,
        local_valid_until_monotonic_ms: u64,
    ) -> Self {
        Self {
            cluster_epoch,
            deadline: Some(AdmittedRouteEffectDeadline {
                authority_valid_until_ms,
                local_valid_until_monotonic_ms,
            }),
        }
    }

    pub(crate) fn bind_portable(
        cluster_epoch: ClusterEpoch,
        authority_valid_until_ms: u64,
        portable_wall_valid_until_ms: u64,
    ) -> Self {
        // Sample monotonic time first so descheduling before the wall sample
        // can only shorten the locally bound deadline.
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let local_wall_ms = crate::clock::current_time_millis();
        // The sender derives this portable deadline from a process-local
        // monotonic lease that already subtracts the authority clock-skew
        // budget. Subtracting that budget again here would make the minimum
        // valid lease unusable across an RPC boundary.
        let remaining_ms = portable_wall_valid_until_ms.saturating_sub(local_wall_ms);
        let local_valid_until_monotonic_ms = local_monotonic_ms
            .checked_add(remaining_ms)
            .unwrap_or(local_monotonic_ms);
        Self::bounded(
            cluster_epoch,
            authority_valid_until_ms,
            local_valid_until_monotonic_ms,
        )
    }

    pub(crate) fn intersect(self, other: Self) -> Result<Self, StoreError> {
        if self.cluster_epoch != other.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: self.cluster_epoch,
                operation_epoch: other.cluster_epoch,
            });
        }
        let deadline = match (self.deadline, other.deadline) {
            (None, None) => None,
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (Some(left), Some(right)) => Some(AdmittedRouteEffectDeadline {
                authority_valid_until_ms: left
                    .authority_valid_until_ms
                    .min(right.authority_valid_until_ms),
                local_valid_until_monotonic_ms: left
                    .local_valid_until_monotonic_ms
                    .min(right.local_valid_until_monotonic_ms),
            }),
        };
        Ok(Self {
            cluster_epoch: self.cluster_epoch,
            deadline,
        })
    }

    pub(crate) fn deadline(self) -> Option<AdmittedRouteEffectDeadline> {
        self.deadline
    }

    pub(crate) fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub(crate) fn require_valid_for(self, operation_epoch: ClusterEpoch) -> Result<(), StoreError> {
        if self.cluster_epoch != operation_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: self.cluster_epoch,
                operation_epoch,
            });
        }
        let Some(deadline) = self.deadline else {
            return Ok(());
        };
        let now_ms = crate::clock::current_time_millis();
        let now_monotonic_ms = crate::clock::monotonic_time_millis();
        if crate::control_plane_lease::validate_process_lease_clock(
            now_ms,
            crate::clock::clock_health_time_millis(),
            crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .is_ok()
            && deadline.local_valid_until_monotonic_ms > now_monotonic_ms
        {
            return Ok(());
        }
        Err(StoreError::RouteMapExpired {
            cluster_epoch: self.cluster_epoch,
            valid_until_ms: deadline.authority_valid_until_ms,
            now_ms,
        })
    }
}

impl AdmittedRouteEffectDeadline {
    pub(crate) fn authority_valid_until_ms(self) -> u64 {
        self.authority_valid_until_ms
    }

    pub(crate) fn portable_wall_valid_until_ms(self) -> u64 {
        let remaining_ms = self
            .local_valid_until_monotonic_ms
            .saturating_sub(crate::clock::monotonic_time_millis());
        let projected_effective_wall_deadline_ms =
            crate::clock::current_time_millis().saturating_add(remaining_ms);
        projected_effective_wall_deadline_ms.min(
            self.authority_valid_until_ms
                .saturating_sub(crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS),
        )
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

pub(crate) const UPLOAD_ID_LEN: usize = 128;
const UPLOAD_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._";
pub(crate) const MULTIPART_UPLOAD_ID_KEY_LEN: usize = 32;
pub const SESSION_ID_LEN: usize = 32;

/// Per-bucket secret used to authenticate multipart upload IDs.
///
/// The key is generated once for a bucket incarnation and is never exposed in
/// S3 responses or diagnostics. Deleting and recreating a bucket creates a new
/// key, making upload IDs from the previous incarnation invalid without
/// retaining terminal upload records.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MultipartUploadIdKey([u8; MULTIPART_UPLOAD_ID_KEY_LEN]);

impl MultipartUploadIdKey {
    // The fixed 128-character ID encodes two 64-bit listing coordinates, a
    // 96-bit random nonce, a 128-bit initiator claim, and a 192-bit HMAC tag.
    // The truncated tag leaves room for epoch provenance while retaining a
    // security margin well beyond the other claims in the ID.
    const LISTING_COMPONENT_HEX_LEN: usize = 16;
    const NONCE_LEN: usize = 16;
    const INITIATOR_CLAIM_HEX_LEN: usize = 32;
    const AUTH_TAG_LEN: usize = 24;

    pub(crate) fn generate() -> Result<Self, String> {
        let mut bytes = [0u8; MULTIPART_UPLOAD_ID_KEY_LEN];
        argmin_crypto::random::fill(&mut bytes)
            .map_err(|_| "failed to generate multipart upload ID key".to_string())?;
        Ok(Self(bytes))
    }

    pub(crate) fn from_bytes(bytes: [u8; MULTIPART_UPLOAD_ID_KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; MULTIPART_UPLOAD_ID_KEY_LEN] {
        &self.0
    }

    pub(crate) fn issue(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        initiator_principal: &str,
    ) -> Result<UploadId, String> {
        let mut random = [0u8; Self::NONCE_LEN];
        argmin_crypto::random::fill(&mut random)
            .map_err(|_| "failed to generate multipart upload ID".to_string())?;
        let nonce: String = random
            .iter()
            .map(|byte| UPLOAD_ID_ALPHABET[(byte & 0x3f) as usize] as char)
            .collect();
        let initiator_claim = self.initiator_claim(initiator_principal);
        Ok(self.encode(bucket, key, 0, 0, nonce.as_bytes(), &initiator_claim))
    }

    pub(crate) fn with_listing_position(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        cluster_epoch: u64,
        log_index: u64,
    ) -> UploadId {
        let nonce_start = Self::LISTING_COMPONENT_HEX_LEN * 2;
        let claim_start = nonce_start + Self::NONCE_LEN;
        let nonce = &upload_id.as_str().as_bytes()[nonce_start..claim_start];
        let encoded_claim =
            &upload_id.as_str()[claim_start..claim_start + Self::INITIATOR_CLAIM_HEX_LEN];
        let initiator_claim = decode_multipart_upload_id_hex::<16>(encoded_claim)
            .expect("issued multipart upload ID claim should remain valid hexadecimal");
        self.encode(
            bucket,
            key,
            cluster_epoch,
            log_index,
            nonce,
            &initiator_claim,
        )
    }

    pub(crate) fn listing_position(upload_id: &UploadId) -> Option<(u64, u64)> {
        let (encoded_epoch, remainder) =
            upload_id.as_str().split_at(Self::LISTING_COMPONENT_HEX_LEN);
        let encoded_log_index = &remainder[..Self::LISTING_COMPONENT_HEX_LEN];
        let cluster_epoch =
            decode_multipart_upload_id_hex::<8>(encoded_epoch).map(u64::from_be_bytes)?;
        let log_index =
            decode_multipart_upload_id_hex::<8>(encoded_log_index).map(u64::from_be_bytes)?;
        Some((cluster_epoch, log_index))
    }

    pub(crate) fn has_same_issuance_identity(left: &UploadId, right: &UploadId) -> bool {
        let start = Self::LISTING_COMPONENT_HEX_LEN * 2;
        let end = start + Self::NONCE_LEN + Self::INITIATOR_CLAIM_HEX_LEN;
        left.as_str()[start..end] == right.as_str()[start..end]
    }

    fn encode(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        cluster_epoch: u64,
        log_index: u64,
        nonce: &[u8],
        initiator_claim: &[u8; 16],
    ) -> UploadId {
        let tag = Sha256Key::new(&self.0).sign(&multipart_upload_id_covered_bytes(
            bucket,
            key,
            cluster_epoch,
            log_index,
            nonce,
            initiator_claim,
        ));
        let mut encoded = String::with_capacity(UPLOAD_ID_LEN);
        use std::fmt::Write as _;
        write!(&mut encoded, "{cluster_epoch:016x}{log_index:016x}")
            .expect("writing multipart upload ID into String cannot fail");
        encoded.push_str(
            std::str::from_utf8(nonce)
                .expect("multipart upload ID nonce should contain only ASCII characters"),
        );
        for byte in *initiator_claim {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}")
                .expect("writing multipart upload ID into String cannot fail");
        }
        for byte in &tag[..Self::AUTH_TAG_LEN] {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}")
                .expect("writing multipart upload ID into String cannot fail");
        }
        UploadId::try_from(encoded).expect("encoded multipart upload ID should remain valid")
    }

    pub(crate) fn authenticates(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> bool {
        let Some((cluster_epoch, log_index)) = Self::listing_position(upload_id) else {
            return false;
        };
        let (_, remainder) = upload_id
            .as_str()
            .split_at(Self::LISTING_COMPONENT_HEX_LEN * 2);
        let (nonce, remainder) = remainder.split_at(Self::NONCE_LEN);
        let (encoded_claim, encoded_tag) = remainder.split_at(Self::INITIATOR_CLAIM_HEX_LEN);
        let Some(initiator_claim) = decode_multipart_upload_id_hex::<16>(encoded_claim) else {
            return false;
        };
        let Some(tag) = decode_multipart_upload_id_hex::<{ Self::AUTH_TAG_LEN }>(encoded_tag)
        else {
            return false;
        };
        let root_key = Sha256Key::new(&self.0);
        let expected = root_key.sign(&multipart_upload_id_covered_bytes(
            bucket,
            key,
            cluster_epoch,
            log_index,
            nonce.as_bytes(),
            &initiator_claim,
        ));
        let comparison_key_bytes = root_key.sign(b"argmin multipart upload id tag comparison v1\0");
        let comparison_key = Sha256Key::new(&comparison_key_bytes);
        let expected_comparison_tag = comparison_key.sign(&expected[..Self::AUTH_TAG_LEN]);
        comparison_key.verify(&tag, &expected_comparison_tag)
    }

    pub(crate) fn was_issued_for_principal(
        &self,
        upload_id: &UploadId,
        initiator_principal: &str,
    ) -> bool {
        let claim_start = Self::LISTING_COMPONENT_HEX_LEN * 2 + Self::NONCE_LEN;
        let encoded_claim =
            &upload_id.as_str()[claim_start..claim_start + Self::INITIATOR_CLAIM_HEX_LEN];
        let Some(claim) = decode_multipart_upload_id_hex::<16>(encoded_claim) else {
            return false;
        };
        let expected = self.initiator_claim(initiator_principal);
        // Compare the fixed-size truncated claims by MACing both with a
        // domain-separated derived key, then verifying the full tag.
        let root_key = Sha256Key::new(&self.0);
        let comparison_key_bytes =
            root_key.sign(b"argmin multipart upload initiator claim comparison v1\0");
        let comparison_key = Sha256Key::new(&comparison_key_bytes);
        let claim_tag = comparison_key.sign(&claim);
        comparison_key.verify(&expected, &claim_tag)
    }

    fn initiator_claim(&self, initiator_principal: &str) -> [u8; 16] {
        const DOMAIN: &[u8] = b"argmin multipart upload initiator v1\0";
        let key = Sha256Key::new(&self.0);
        let mut context = key.context();
        context.update(DOMAIN);
        context.update(&(initiator_principal.len() as u32).to_be_bytes());
        context.update(initiator_principal.as_bytes());
        let tag = context.finalize();
        let mut claim = [0u8; 16];
        claim.copy_from_slice(&tag[..16]);
        claim
    }
}

fn decode_multipart_upload_id_hex<const N: usize>(encoded: &str) -> Option<[u8; N]> {
    if encoded.len() != N * 2 {
        return None;
    }
    let mut decoded = [0u8; N];
    for (index, encoded) in encoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = decode_hex_nibble(encoded[0])?;
        let low = decode_hex_nibble(encoded[1])?;
        decoded[index] = (high << 4) | low;
    }
    Some(decoded)
}

fn multipart_upload_id_covered_bytes(
    bucket: &BucketName,
    key: &ObjectKey,
    cluster_epoch: u64,
    log_index: u64,
    nonce: &[u8],
    initiator_claim: &[u8; 16],
) -> Vec<u8> {
    const DOMAIN: &[u8] = b"argmin multipart upload id v3\0";
    let mut covered = Vec::with_capacity(
        DOMAIN.len()
            + 4
            + bucket.as_str().len()
            + 4
            + key.as_str().len()
            + 16
            + nonce.len()
            + initiator_claim.len(),
    );
    covered.extend_from_slice(DOMAIN);
    covered.extend_from_slice(&(bucket.as_str().len() as u32).to_be_bytes());
    covered.extend_from_slice(bucket.as_str().as_bytes());
    covered.extend_from_slice(&(key.as_str().len() as u32).to_be_bytes());
    covered.extend_from_slice(key.as_str().as_bytes());
    covered.extend_from_slice(&cluster_epoch.to_be_bytes());
    covered.extend_from_slice(&log_index.to_be_bytes());
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

/// Opaque authority for interpreting multipart upload IDs issued by one bucket incarnation.
///
/// The durable signing key, ID encoding, and issuance operations remain private to storage.
/// Higher layers may ask only the logical authorization questions needed to reproduce S3
/// behavior for terminal or nonexistent uploads.
///
/// The authority is cloneable because bucket snapshots and response state share it, but it is
/// deliberately not comparable: equality would expose whether two opaque authorities retain the
/// same hidden signing key.
///
/// ```compile_fail
/// use storage::MultipartUploadIdAuthority;
///
/// fn require_equality<T: PartialEq>() {}
/// require_equality::<MultipartUploadIdAuthority>();
/// ```
#[derive(Clone)]
pub struct MultipartUploadIdAuthority {
    key: MultipartUploadIdKey,
    _non_comparable: NonComparableAuthorityMarker,
}

/// Prevents an accidental `PartialEq`/`Eq` derive from comparing opaque key material while
/// retaining the authority's intentional `Clone` implementation.
#[derive(Clone)]
struct NonComparableAuthorityMarker;

impl MultipartUploadIdAuthority {
    fn new(key: MultipartUploadIdKey) -> Self {
        Self {
            key,
            _non_comparable: NonComparableAuthorityMarker,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn for_test() -> Self {
        Self::new(MultipartUploadIdKey::from_bytes(
            [1; MULTIPART_UPLOAD_ID_KEY_LEN],
        ))
    }

    #[must_use]
    pub fn authenticates(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> bool {
        self.key.authenticates(bucket, key, upload_id)
    }

    #[must_use]
    pub fn was_issued_for_principal(
        &self,
        upload_id: &UploadId,
        initiator_principal: &str,
    ) -> bool {
        self.key
            .was_issued_for_principal(upload_id, initiator_principal)
    }
}

impl std::fmt::Debug for MultipartUploadIdAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MultipartUploadIdAuthority(<opaque>)")
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

#[cfg(any(test, feature = "test-hooks"))]
impl UploadId {
    /// Construct a deterministic valid upload ID without exposing its encoding.
    pub fn for_test(seed: &str) -> Self {
        let mut encoded_seed = String::with_capacity(seed.len() * 2);
        for byte in seed.bytes() {
            use std::fmt::Write as _;
            write!(encoded_seed, "{byte:02x}")
                .expect("writing a test upload ID into String cannot fail");
        }
        let mut encoded = String::with_capacity(UPLOAD_ID_LEN);
        encoded.push_str(&encoded_seed[..encoded_seed.len().min(UPLOAD_ID_LEN)]);
        encoded.extend(std::iter::repeat_n('.', UPLOAD_ID_LEN - encoded.len()));
        Self::try_from(encoded).expect("storage must construct a valid test upload ID")
    }

    /// Construct a raw upload-ID value that fails only the length constraint.
    pub fn overlong_for_test() -> String {
        "a".repeat(UPLOAD_ID_LEN + 1)
    }
}

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

/// Validated object tags carried by storage.
///
/// The canonical XML representation is private to storage and is decoded only
/// through the `s3-types` owner. Callers can construct and inspect logical tag
/// values, but cannot inject an arbitrary persisted representation.
#[derive(Clone, PartialEq, Eq)]
pub struct SerializedTagSet {
    stored: s3_types::StoredTagSet,
}

impl SerializedTagSet {
    pub fn from_tag_set(tags: s3_types::TagSet) -> Result<Self, s3_types::TagSetValidationError> {
        let tags = s3_types::TagSet::new(tags.as_slice().to_vec(), s3_types::MAX_OBJECT_TAGS)?;
        Ok(Self {
            stored: s3_types::StoredTagSet::from_tag_set(tags),
        })
    }

    #[must_use]
    pub fn tag_set(&self) -> &s3_types::TagSet {
        self.stored.tag_set()
    }

    pub(crate) fn from_current_storage(
        serialized: String,
    ) -> Result<Self, s3_types::StoredTagSetParseError> {
        let stored = s3_types::StoredTagSet::parse_current(serialized, s3_types::MAX_OBJECT_TAGS)?;
        Ok(Self { stored })
    }

    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        self.stored.as_storage_str()
    }

    #[cfg(test)]
    pub(crate) fn new(xml: String) -> Self {
        let tags = s3_types::TagSet::parse_canonical_xml(&xml, s3_types::MAX_OBJECT_TAGS)
            .expect("storage tests must construct valid object tags");
        Self::from_tag_set(tags).expect("storage tests must respect the object tag count limit")
    }
}

impl Default for SerializedTagSet {
    fn default() -> Self {
        Self::from_tag_set(s3_types::TagSet::empty(s3_types::MAX_OBJECT_TAGS))
            .expect("an empty object tag set is valid")
    }
}

impl std::ops::Deref for SerializedTagSet {
    type Target = s3_types::TagSet;

    fn deref(&self) -> &Self::Target {
        self.tag_set()
    }
}

#[cfg(test)]
impl From<&str> for SerializedTagSet {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

#[cfg(test)]
impl From<String> for SerializedTagSet {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Debug for SerializedTagSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedTagSet")
            .field("tag_count", &self.stored.tag_set().len())
            .finish()
    }
}

/// Validated bucket tags carried by storage.
///
/// Bucket and object tags share the AWS tag grammar and canonical XML owner,
/// but have distinct cardinality limits and durable carriers. The XML remains
/// private to storage so callers cannot inject an alternative persisted
/// spelling.
#[derive(Clone, PartialEq, Eq)]
pub struct SerializedBucketTagSet {
    stored: s3_types::StoredTagSet,
}

impl SerializedBucketTagSet {
    pub fn from_tag_set(tags: s3_types::TagSet) -> Result<Self, s3_types::TagSetValidationError> {
        let tags = s3_types::TagSet::new(tags.as_slice().to_vec(), s3_types::MAX_BUCKET_TAGS)?;
        Ok(Self {
            stored: s3_types::StoredTagSet::from_tag_set(tags),
        })
    }

    #[must_use]
    pub fn tag_set(&self) -> &s3_types::TagSet {
        self.stored.tag_set()
    }

    pub(crate) fn from_current_storage(
        serialized: String,
    ) -> Result<Self, s3_types::StoredTagSetParseError> {
        let stored = s3_types::StoredTagSet::parse_current(serialized, s3_types::MAX_BUCKET_TAGS)?;
        Ok(Self { stored })
    }

    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        self.stored.as_storage_str()
    }

    #[cfg(test)]
    pub(crate) fn new(xml: String) -> Self {
        let tags = s3_types::TagSet::parse_canonical_xml(&xml, s3_types::MAX_BUCKET_TAGS)
            .expect("storage tests must construct valid bucket tags");
        Self::from_tag_set(tags).expect("storage tests must respect the bucket tag count limit")
    }
}

impl std::ops::Deref for SerializedBucketTagSet {
    type Target = s3_types::TagSet;

    fn deref(&self) -> &Self::Target {
        self.tag_set()
    }
}

impl std::fmt::Debug for SerializedBucketTagSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedBucketTagSet")
            .field("tag_count", &self.stored.tag_set().len())
            .finish()
    }
}

/// Storage-owned object encryption discriminator used by durable and wire codecs.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectEncryptionType {
    SseCustomer = 1,
    SseS3 = 2,
}

impl ObjectEncryptionType {
    #[must_use]
    pub(crate) fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::SseCustomer),
            2 => Some(Self::SseS3),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ObjectEncryptionDecodeError {
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
    #[error("SSE-C empty checksum metadata requires a zero nonce")]
    NoncanonicalSseCustomerEmptyChecksumNonce,
    #[error("invalid SSE-S3 state length {actual} (minimum {minimum})")]
    InvalidSseS3StateLength { actual: usize, minimum: usize },
    #[error("unsupported SSE-S3 state version {version}")]
    UnsupportedSseS3StateVersion { version: u8 },
    #[error("unsupported segment payload codec {codec}")]
    UnsupportedSegmentPayloadCodec { codec: u8 },
    #[error("unsupported object-encryption key binding {binding}")]
    UnsupportedObjectEncryptionKeyBinding { binding: u8 },
    #[error("invalid SSE-S3 checksum metadata length {declared} (remaining {remaining})")]
    InvalidSseS3ChecksumMetadataLength { declared: usize, remaining: usize },
    #[error("SSE-S3 empty checksum metadata requires a zero nonce")]
    NoncanonicalSseS3EmptyChecksumNonce,
}

/// Durable distinction between encryption state used while a write is still
/// provisional and state cryptographically bound to its committed object
/// identity.
///
/// The provisional form is required while a stream or multipart upload is in
/// progress. It must be replaced exactly once at terminal publication. The
/// committed form authenticates the storage-minted object subject as AEAD
/// associated data for both the wrapped DEK and encrypted checksum metadata.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectEncryptionKeyBinding {
    ProvisionalV1 = 1,
    ObjectSubjectV2 = 2,
}

impl ObjectEncryptionKeyBinding {
    const fn tag(self) -> u8 {
        self as u8
    }

    const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::ProvisionalV1),
            2 => Some(Self::ObjectSubjectV2),
            _ => None,
        }
    }
}

/// Logical object-encryption state could not be represented by storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ObjectEncryptionStateError {
    #[error("encrypted checksum metadata exceeds the storage limit")]
    EncryptedChecksumMetadataTooLong,
    #[error("empty encrypted checksum metadata requires a zero nonce")]
    NoncanonicalEmptyChecksumNonce,
    #[error("object encryption subject does not match the provisional encryption state")]
    ObjectEncryptionSubjectMismatch,
    #[error("object encryption state is already bound to its committed object subject")]
    ObjectEncryptionAlreadySubjectBound,
}

#[derive(Clone, PartialEq, Eq)]
struct EncryptedChecksumMetadata {
    encoded_len: u16,
    bytes: Vec<u8>,
}

impl EncryptedChecksumMetadata {
    const fn empty() -> Self {
        Self {
            encoded_len: 0,
            bytes: Vec::new(),
        }
    }

    fn try_new(bytes: Vec<u8>) -> Result<Self, ObjectEncryptionStateError> {
        let encoded_len = u16::try_from(bytes.len())
            .map_err(|_| ObjectEncryptionStateError::EncryptedChecksumMetadataTooLong)?;
        Ok(Self { encoded_len, bytes })
    }

    fn from_encoded(encoded_len: u16, bytes: Vec<u8>) -> Self {
        debug_assert_eq!(usize::from(encoded_len), bytes.len());
        Self { encoded_len, bytes }
    }

    const fn encoded_len(&self) -> u16 {
        self.encoded_len
    }

    fn len(&self) -> usize {
        usize::from(self.encoded_len)
    }

    const fn is_empty(&self) -> bool {
        self.encoded_len == 0
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
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

const OBJECT_ENCRYPTION_SUBJECT_DOMAIN: &[u8] = b"argmin:object-encryption-subject";
const OBJECT_ENCRYPTION_SUBJECT_VERSION: u8 = 2;
pub const OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN: usize = 16;
const OBJECT_ENCRYPTION_INSTANCE_ID_LEN: usize = 1 + OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN;
const OBJECT_ENCRYPTION_PURPOSE_SUBJECT_VERSION: u8 = 1;
const DEK_WRAP_ENCRYPTION_PURPOSE_TAG: u8 = 1;
const CHECKSUM_METADATA_ENCRYPTION_PURPOSE_TAG: u8 = 2;
const SEGMENT_ENCRYPTION_SUBJECT_VERSION: u8 = 1;
const STANDARD_OBJECT_SEGMENT_PROVENANCE_TAG: u8 = 1;
const MULTIPART_PART_SEGMENT_PROVENANCE_TAG: u8 = 2;

/// Durable codec used to protect encrypted payload segments.
///
/// The codec is independent of the AWS-visible SSE mode: either codec can be
/// paired with SSE-C or SSE-S3 key state. It is carried by the self-describing
/// object-encryption state so objects, multipart uploads, and stream sessions
/// retain one exact codec choice.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentPayloadCodec {
    SegmentAes256GcmV1 = 1,
    Cobblestone256SegmentV1 = 2,
}

impl SegmentPayloadCodec {
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::SegmentAes256GcmV1),
            2 => Some(Self::Cobblestone256SegmentV1),
            _ => None,
        }
    }

    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Return the canonical stored length for one logical segment.
    #[must_use]
    pub fn stored_size(self, logical_size: u64) -> Option<u64> {
        match self {
            Self::SegmentAes256GcmV1 => {
                logical_size.checked_add(OBJECT_ENCRYPTION_SEGMENT_TAG_LEN as u64)
            }
            Self::Cobblestone256SegmentV1 => {
                argmin_crypto::cobblestone::Cobblestone256MessageLayout::from_plaintext_len(
                    logical_size,
                )
                .ok()
                .map(|layout| layout.ciphertext_len())
            }
        }
    }

    /// Recover the logical length from a canonical stored segment length.
    #[must_use]
    pub fn logical_size(self, stored_size: u64) -> Option<u64> {
        match self {
            Self::SegmentAes256GcmV1 => {
                stored_size.checked_sub(OBJECT_ENCRYPTION_SEGMENT_TAG_LEN as u64)
            }
            Self::Cobblestone256SegmentV1 => {
                argmin_crypto::cobblestone::Cobblestone256MessageLayout::from_ciphertext_len(
                    stored_size,
                )
                .ok()
                .map(|layout| layout.plaintext_len())
            }
        }
    }
}

/// Compiler-exhaustive compatibility registry for durable payload profiles.
///
/// Keeping the AWS-visible key mode and the internal payload codec in one
/// closed enum makes every supported pair explicit. Adding either an
/// encryption-state variant or a payload codec therefore requires a decision
/// at each profile-owned policy boundary rather than becoming accepted by a
/// wildcard or an independent tag parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentPayloadProfile {
    SseCustomerSegmentAes256GcmV1,
    SseCustomerCobblestone256SegmentV1,
    SseS3SegmentAes256GcmV1,
    SseS3Cobblestone256SegmentV1,
}

impl SegmentPayloadProfile {
    const fn new(
        encryption_type: ObjectEncryptionType,
        payload_codec: SegmentPayloadCodec,
    ) -> Self {
        match (encryption_type, payload_codec) {
            (ObjectEncryptionType::SseCustomer, SegmentPayloadCodec::SegmentAes256GcmV1) => {
                Self::SseCustomerSegmentAes256GcmV1
            }
            (ObjectEncryptionType::SseCustomer, SegmentPayloadCodec::Cobblestone256SegmentV1) => {
                Self::SseCustomerCobblestone256SegmentV1
            }
            (ObjectEncryptionType::SseS3, SegmentPayloadCodec::SegmentAes256GcmV1) => {
                Self::SseS3SegmentAes256GcmV1
            }
            (ObjectEncryptionType::SseS3, SegmentPayloadCodec::Cobblestone256SegmentV1) => {
                Self::SseS3Cobblestone256SegmentV1
            }
        }
    }

    #[must_use]
    pub(crate) const fn encryption_type(self) -> ObjectEncryptionType {
        match self {
            Self::SseCustomerSegmentAes256GcmV1 | Self::SseCustomerCobblestone256SegmentV1 => {
                ObjectEncryptionType::SseCustomer
            }
            Self::SseS3SegmentAes256GcmV1 | Self::SseS3Cobblestone256SegmentV1 => {
                ObjectEncryptionType::SseS3
            }
        }
    }

    #[must_use]
    pub(crate) const fn payload_codec(self) -> SegmentPayloadCodec {
        match self {
            Self::SseCustomerSegmentAes256GcmV1 | Self::SseS3SegmentAes256GcmV1 => {
                SegmentPayloadCodec::SegmentAes256GcmV1
            }
            Self::SseCustomerCobblestone256SegmentV1 | Self::SseS3Cobblestone256SegmentV1 => {
                SegmentPayloadCodec::Cobblestone256SegmentV1
            }
        }
    }

    #[must_use]
    pub(crate) const fn requires_shard_integrity_witness(self) -> bool {
        match self {
            Self::SseCustomerSegmentAes256GcmV1 | Self::SseS3SegmentAes256GcmV1 => false,
            Self::SseCustomerCobblestone256SegmentV1 | Self::SseS3Cobblestone256SegmentV1 => true,
        }
    }

    #[must_use]
    pub(crate) fn stored_size(self, logical_size: u64) -> Option<u64> {
        match self {
            Self::SseCustomerSegmentAes256GcmV1 | Self::SseS3SegmentAes256GcmV1 => {
                SegmentPayloadCodec::SegmentAes256GcmV1.stored_size(logical_size)
            }
            Self::SseCustomerCobblestone256SegmentV1 | Self::SseS3Cobblestone256SegmentV1 => {
                SegmentPayloadCodec::Cobblestone256SegmentV1.stored_size(logical_size)
            }
        }
    }

    #[must_use]
    pub(crate) fn logical_size(self, stored_size: u64) -> Option<u64> {
        match self {
            Self::SseCustomerSegmentAes256GcmV1 | Self::SseS3SegmentAes256GcmV1 => {
                SegmentPayloadCodec::SegmentAes256GcmV1.logical_size(stored_size)
            }
            Self::SseCustomerCobblestone256SegmentV1 | Self::SseS3Cobblestone256SegmentV1 => {
                SegmentPayloadCodec::Cobblestone256SegmentV1.logical_size(stored_size)
            }
        }
    }
}

/// Opaque canonical identity authenticated by one object's payload encryption.
///
/// Storage constructs this value from an exact object-generation reservation
/// and the encryption instance bound to that reservation. Higher layers may
/// carry it to the encryption boundary, but cannot assemble or alter its
/// bucket, key, incarnation, generation, profile, or encryption-instance
/// fields independently.
pub struct ObjectEncryptionSubject {
    canonical: Box<[u8]>,
    encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_ID_LEN],
    immutable_encryption_binding: ObjectEncryptionImmutableBinding,
}

/// Opaque canonical associated data for wrapping one object's DEK.
///
/// Storage derives this value from an exact [`ObjectEncryptionSubject`]. The
/// encryption boundary may consume its bytes as AEAD associated data, but
/// cannot assemble or alter the logical object identity they contain.
///
/// ```compile_fail
/// use storage::DekWrapEncryptionSubject;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(subject: &DekWrapEncryptionSubject) {
///     require_clone(subject);
/// }
/// ```
pub struct DekWrapEncryptionSubject<'subject> {
    object: &'subject ObjectEncryptionSubject,
    canonical: Box<[u8]>,
}

/// Opaque canonical associated data for one object's encrypted checksum
/// metadata.
///
/// The distinct purpose tag prevents otherwise identical DEK-wrap and
/// checksum ciphertexts from being moved between cryptographic roles.
///
/// ```compile_fail
/// use storage::ChecksumEncryptionSubject;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(subject: &ChecksumEncryptionSubject) {
///     require_clone(subject);
/// }
/// ```
pub struct ChecksumEncryptionSubject<'subject> {
    object: &'subject ObjectEncryptionSubject,
    canonical: Box<[u8]>,
}

fn purpose_encryption_subject(object: &ObjectEncryptionSubject, purpose_tag: u8) -> Box<[u8]> {
    let mut canonical = Vec::with_capacity(object.canonical.len() + 2);
    canonical.extend_from_slice(&object.canonical);
    canonical.push(OBJECT_ENCRYPTION_PURPOSE_SUBJECT_VERSION);
    canonical.push(purpose_tag);
    canonical.into_boxed_slice()
}

impl DekWrapEncryptionSubject<'_> {
    /// Canonical associated data for the DEK-wrapping AEAD.
    #[must_use]
    pub fn aead_context(&self) -> &[u8] {
        &self.canonical
    }

    /// Returns whether this purpose subject was derived from the supplied
    /// complete immutable encryption state.
    #[must_use]
    pub fn matches_encryption(&self, encryption: &ObjectEncryption) -> bool {
        self.object.matches_encryption(encryption)
    }
}

impl ChecksumEncryptionSubject<'_> {
    /// Canonical associated data for the checksum-metadata AEAD.
    #[must_use]
    pub fn aead_context(&self) -> &[u8] {
        &self.canonical
    }

    /// Returns whether this purpose subject was derived from the supplied
    /// complete immutable encryption state. Checksum nonce/ciphertext
    /// enrichment is intentionally excluded from that immutable comparison.
    #[must_use]
    pub fn matches_encryption(&self, encryption: &ObjectEncryption) -> bool {
        self.object.matches_encryption(encryption)
    }
}

impl std::fmt::Debug for DekWrapEncryptionSubject<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DekWrapEncryptionSubject")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ChecksumEncryptionSubject<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChecksumEncryptionSubject")
            .finish_non_exhaustive()
    }
}

#[derive(PartialEq, Eq)]
enum ObjectEncryptionImmutableBinding {
    SseCustomer {
        payload_codec: SegmentPayloadCodec,
        key_binding: ObjectEncryptionKeyBinding,
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
        validator_key_id: u32,
        validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
        validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
        wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
        wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    },
    SseS3 {
        payload_codec: SegmentPayloadCodec,
        key_binding: ObjectEncryptionKeyBinding,
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
        wrapping_key_id: u32,
        wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    },
}

impl ObjectEncryptionImmutableBinding {
    fn from_encryption(encryption: &ObjectEncryption) -> Self {
        match encryption {
            ObjectEncryption::SseCustomer(state) => Self::SseCustomer {
                payload_codec: state.payload_codec(),
                key_binding: state.key_binding(),
                encryption_instance_id: *state.encryption_instance_id(),
                validator_key_id: state.validator_key_id(),
                validator_salt: *state.validator_salt(),
                validator_hmac: *state.validator_hmac(),
                wrap_salt: *state.wrap_salt(),
                wrap_nonce: *state.wrap_nonce(),
                wrapped_dek: *state.wrapped_dek(),
                segment_nonce_prefix: *state.segment_nonce_prefix(),
            },
            ObjectEncryption::SseS3(state) => Self::SseS3 {
                payload_codec: state.payload_codec(),
                key_binding: state.key_binding(),
                encryption_instance_id: *state.encryption_instance_id(),
                wrapping_key_id: state.wrapping_key_id(),
                wrap_nonce: *state.wrap_nonce(),
                wrapped_dek: *state.wrapped_dek(),
                segment_nonce_prefix: *state.segment_nonce_prefix(),
            },
        }
    }
}

impl ObjectEncryptionSubject {
    pub(crate) fn standard_object(
        bucket_incarnation_generation: u64,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        encryption: &ObjectEncryption,
    ) -> Self {
        let encryption_instance_id = encryption.encryption_instance_id();
        let domain_len = u8::try_from(OBJECT_ENCRYPTION_SUBJECT_DOMAIN.len())
            .expect("object encryption subject domain length must fit u8");
        let bucket_len = u16::try_from(bucket.as_str().len())
            .expect("validated bucket name length must fit u16");
        let key_len =
            u16::try_from(key.as_str().len()).expect("validated object key length must fit u16");
        let mut canonical = Vec::with_capacity(
            1 + OBJECT_ENCRYPTION_SUBJECT_DOMAIN.len()
                + 1
                + 1
                + 1
                + OBJECT_ENCRYPTION_INSTANCE_ID_LEN
                + 8
                + 2
                + usize::from(bucket_len)
                + 2
                + usize::from(key_len)
                + 8,
        );
        canonical.push(domain_len);
        canonical.extend_from_slice(OBJECT_ENCRYPTION_SUBJECT_DOMAIN);
        canonical.push(OBJECT_ENCRYPTION_SUBJECT_VERSION);
        canonical.push(encryption.payload_codec().tag());
        canonical.push(
            u8::try_from(OBJECT_ENCRYPTION_INSTANCE_ID_LEN)
                .expect("object encryption instance identifier length must fit u8"),
        );
        canonical.extend_from_slice(&encryption_instance_id);
        canonical.extend_from_slice(&bucket_incarnation_generation.to_be_bytes());
        canonical.extend_from_slice(&bucket_len.to_be_bytes());
        canonical.extend_from_slice(bucket.as_str().as_bytes());
        canonical.extend_from_slice(&key_len.to_be_bytes());
        canonical.extend_from_slice(key.as_str().as_bytes());
        canonical.extend_from_slice(&generation_id.get().to_be_bytes());
        Self {
            canonical: canonical.into_boxed_slice(),
            encryption_instance_id,
            immutable_encryption_binding: ObjectEncryptionImmutableBinding::from_encryption(
                encryption,
            ),
        }
    }

    /// Returns whether this subject was minted for the supplied encryption
    /// instance. This lets the encryption boundary reject crossed state while
    /// the canonical subject itself remains opaque.
    #[must_use]
    pub fn matches_encryption(&self, encryption: &ObjectEncryption) -> bool {
        self.encryption_instance_id == encryption.encryption_instance_id()
            && self.immutable_encryption_binding
                == ObjectEncryptionImmutableBinding::from_encryption(encryption)
    }

    /// Derives the purpose-separated DEK-wrap subject from this exact object
    /// identity.
    #[must_use]
    pub fn dek_wrap_subject(&self) -> DekWrapEncryptionSubject<'_> {
        DekWrapEncryptionSubject {
            object: self,
            canonical: purpose_encryption_subject(self, DEK_WRAP_ENCRYPTION_PURPOSE_TAG),
        }
    }

    /// Derives the purpose-separated checksum-metadata subject from this
    /// exact object identity.
    #[must_use]
    pub fn checksum_subject(&self) -> ChecksumEncryptionSubject<'_> {
        ChecksumEncryptionSubject {
            object: self,
            canonical: purpose_encryption_subject(self, CHECKSUM_METADATA_ENCRYPTION_PURPOSE_TAG),
        }
    }

    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    #[cfg(test)]
    fn validate_canonical_for_test(canonical: &[u8]) -> Result<Box<[u8]>, &'static str> {
        fn take<'a>(
            bytes: &'a [u8],
            cursor: &mut usize,
            len: usize,
        ) -> Result<&'a [u8], &'static str> {
            let end = cursor.checked_add(len).ok_or("length overflow")?;
            let value = bytes.get(*cursor..end).ok_or("truncated subject")?;
            *cursor = end;
            Ok(value)
        }

        let mut cursor = 0;
        let domain_len = usize::from(
            *take(canonical, &mut cursor, 1)?
                .first()
                .ok_or("missing domain length")?,
        );
        if take(canonical, &mut cursor, domain_len)? != OBJECT_ENCRYPTION_SUBJECT_DOMAIN {
            return Err("invalid domain");
        }
        if take(canonical, &mut cursor, 1)? != [OBJECT_ENCRYPTION_SUBJECT_VERSION] {
            return Err("unsupported version");
        }
        let profile = *take(canonical, &mut cursor, 1)?
            .first()
            .ok_or("missing profile")?;
        if SegmentPayloadCodec::from_u8(profile).is_none() {
            return Err("unsupported profile");
        }
        let instance_len = usize::from(
            *take(canonical, &mut cursor, 1)?
                .first()
                .ok_or("missing encryption instance length")?,
        );
        if instance_len != OBJECT_ENCRYPTION_INSTANCE_ID_LEN {
            return Err("invalid encryption instance length");
        }
        let encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_ID_LEN] =
            take(canonical, &mut cursor, instance_len)?
                .try_into()
                .map_err(|_| "invalid encryption instance")?;
        let bucket_incarnation_generation = u64::from_be_bytes(
            take(canonical, &mut cursor, 8)?
                .try_into()
                .map_err(|_| "invalid bucket incarnation")?,
        );
        if bucket_incarnation_generation == 0 {
            return Err("invalid bucket incarnation");
        }
        let bucket_len = usize::from(u16::from_be_bytes(
            take(canonical, &mut cursor, 2)?
                .try_into()
                .map_err(|_| "invalid bucket length")?,
        ));
        let bucket = std::str::from_utf8(take(canonical, &mut cursor, bucket_len)?)
            .map_err(|_| "invalid bucket encoding")?;
        BucketName::try_from(bucket).map_err(|_| "invalid bucket")?;
        let key_len = usize::from(u16::from_be_bytes(
            take(canonical, &mut cursor, 2)?
                .try_into()
                .map_err(|_| "invalid key length")?,
        ));
        let key = std::str::from_utf8(take(canonical, &mut cursor, key_len)?)
            .map_err(|_| "invalid key encoding")?;
        ObjectKey::try_from(key).map_err(|_| "invalid key")?;
        let generation = u64::from_be_bytes(
            take(canonical, &mut cursor, 8)?
                .try_into()
                .map_err(|_| "invalid generation")?,
        );
        GenerationId::new(generation).ok_or("invalid generation")?;
        if cursor != canonical.len() {
            return Err("trailing subject bytes");
        }
        let _ = encryption_instance_id;
        Ok(canonical.to_vec().into_boxed_slice())
    }
}

impl PartialEq for ObjectEncryptionSubject {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
            && self.immutable_encryption_binding == other.immutable_encryption_binding
    }
}

impl Eq for ObjectEncryptionSubject {}

impl std::fmt::Debug for ObjectEncryptionSubject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectEncryptionSubject")
            .finish_non_exhaustive()
    }
}

/// Opaque canonical identity for one encrypted standard-object segment.
///
/// The subject extends an owner-minted [`ObjectEncryptionSubject`] with the
/// segment provenance, index, and logical plaintext length. It is deliberately
/// non-cloneable and can only be derived from an existing object subject.
///
/// ```compile_fail
/// use storage::SegmentEncryptionSubject;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(subject: &SegmentEncryptionSubject) {
///     require_clone(subject);
/// }
/// ```
pub struct SegmentEncryptionSubject {
    object: ObjectEncryptionSubject,
    canonical: Box<[u8]>,
    provenance: SegmentEncryptionProvenance,
    segment_index: u32,
    logical_plaintext_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentEncryptionProvenance {
    StandardObject,
    MultipartPart,
}

impl SegmentEncryptionSubject {
    pub(crate) fn standard_object(
        object: ObjectEncryptionSubject,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> Self {
        let mut canonical = Vec::with_capacity(object.canonical.len() + 1 + 1 + 4 + 8);
        canonical.extend_from_slice(&object.canonical);
        canonical.push(SEGMENT_ENCRYPTION_SUBJECT_VERSION);
        canonical.push(STANDARD_OBJECT_SEGMENT_PROVENANCE_TAG);
        canonical.extend_from_slice(&segment_index.to_be_bytes());
        canonical.extend_from_slice(&logical_plaintext_len.to_be_bytes());
        Self {
            object,
            canonical: canonical.into_boxed_slice(),
            provenance: SegmentEncryptionProvenance::StandardObject,
            segment_index,
            logical_plaintext_len,
        }
    }

    pub(crate) fn multipart_part(
        object: ObjectEncryptionSubject,
        upload_id: &UploadId,
        part_number: u32,
        part_generation_id: GenerationId,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> Self {
        let upload_id_len = u16::try_from(upload_id.as_str().len())
            .expect("validated multipart upload ID length must fit u16");
        let mut canonical = Vec::with_capacity(
            object.canonical.len() + 1 + 1 + 2 + usize::from(upload_id_len) + 4 + 8 + 4 + 8,
        );
        canonical.extend_from_slice(&object.canonical);
        canonical.push(SEGMENT_ENCRYPTION_SUBJECT_VERSION);
        canonical.push(MULTIPART_PART_SEGMENT_PROVENANCE_TAG);
        canonical.extend_from_slice(&upload_id_len.to_be_bytes());
        canonical.extend_from_slice(upload_id.as_str().as_bytes());
        canonical.extend_from_slice(&part_number.to_be_bytes());
        canonical.extend_from_slice(&part_generation_id.get().to_be_bytes());
        canonical.extend_from_slice(&segment_index.to_be_bytes());
        canonical.extend_from_slice(&logical_plaintext_len.to_be_bytes());
        Self {
            object,
            canonical: canonical.into_boxed_slice(),
            provenance: SegmentEncryptionProvenance::MultipartPart,
            segment_index,
            logical_plaintext_len,
        }
    }

    /// Returns whether the supplied encryption state and segment geometry are
    /// the exact values from which this subject was minted.
    #[must_use]
    pub fn matches_standard_object_segment(
        &self,
        encryption: &ObjectEncryption,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> bool {
        self.provenance == SegmentEncryptionProvenance::StandardObject
            && self.object.matches_encryption(encryption)
            && self.segment_index == segment_index
            && self.logical_plaintext_len == logical_plaintext_len
    }

    /// Returns whether the supplied encryption state and segment geometry
    /// match this subject. Operation authorization must additionally use the
    /// provenance-specific standard-object or multipart predicate.
    #[must_use]
    pub fn matches_encryption_and_geometry(
        &self,
        encryption: &ObjectEncryption,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> bool {
        self.object.matches_encryption(encryption)
            && self.segment_index == segment_index
            && self.logical_plaintext_len == logical_plaintext_len
    }

    /// Returns whether this subject was minted for the exact multipart part,
    /// immutable part payload generation, encryption state, and segment
    /// geometry supplied by the caller.
    #[must_use]
    pub fn matches_multipart_part_segment(
        &self,
        encryption: &ObjectEncryption,
        upload_id: &UploadId,
        part_number: u32,
        part_generation_id: GenerationId,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> bool {
        if self.provenance != SegmentEncryptionProvenance::MultipartPart
            || !self.object.matches_encryption(encryption)
            || self.segment_index != segment_index
            || self.logical_plaintext_len != logical_plaintext_len
        {
            return false;
        }
        let upload_id_len = match u16::try_from(upload_id.as_str().len()) {
            Ok(len) => len,
            Err(_) => return false,
        };
        let mut expected = Vec::with_capacity(self.canonical.len());
        expected.extend_from_slice(&self.object.canonical);
        expected.push(SEGMENT_ENCRYPTION_SUBJECT_VERSION);
        expected.push(MULTIPART_PART_SEGMENT_PROVENANCE_TAG);
        expected.extend_from_slice(&upload_id_len.to_be_bytes());
        expected.extend_from_slice(upload_id.as_str().as_bytes());
        expected.extend_from_slice(&part_number.to_be_bytes());
        expected.extend_from_slice(&part_generation_id.get().to_be_bytes());
        expected.extend_from_slice(&segment_index.to_be_bytes());
        expected.extend_from_slice(&logical_plaintext_len.to_be_bytes());
        self.canonical.as_ref() == expected
    }

    pub(crate) fn logical_plaintext_len(&self) -> u64 {
        self.logical_plaintext_len
    }

    /// Canonical associated data for the segment payload codec.
    ///
    /// Storage mints this subject from durable object authority. Encryption
    /// code may authenticate the resulting bytes, but cannot construct or
    /// alter the bucket, key, generation, provenance, or segment geometry
    /// they encode.
    #[must_use]
    pub fn payload_aead_context(&self) -> &[u8] {
        &self.canonical
    }

    /// Recovers the purpose-separated DEK-wrap subject from the exact object
    /// identity embedded in this storage-minted segment subject.
    #[must_use]
    pub fn dek_wrap_subject(&self) -> DekWrapEncryptionSubject<'_> {
        self.object.dek_wrap_subject()
    }

    /// Recovers the purpose-separated checksum-metadata subject from the
    /// exact object identity embedded in this storage-minted segment subject.
    #[must_use]
    pub fn checksum_subject(&self) -> ChecksumEncryptionSubject<'_> {
        self.object.checksum_subject()
    }

    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
}

impl PartialEq for SegmentEncryptionSubject {
    fn eq(&self, other: &Self) -> bool {
        self.object == other.object && self.canonical == other.canonical
    }
}

impl Eq for SegmentEncryptionSubject {}

impl std::fmt::Debug for SegmentEncryptionSubject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentEncryptionSubject")
            .finish_non_exhaustive()
    }
}

/// Stored per-object `SSE-C` state.
#[derive(Clone, PartialEq, Eq)]
pub struct SseCustomerObjectState {
    payload_codec: SegmentPayloadCodec,
    key_binding: ObjectEncryptionKeyBinding,
    encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
    validator_key_id: u32,
    validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
    validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
    wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
    wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
    wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
    segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    checksum_nonce: [u8; SSE_C_CHECKSUM_NONCE_LEN],
    encrypted_checksum_metadata: EncryptedChecksumMetadata,
}

impl std::fmt::Debug for SseCustomerObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseCustomerObjectState")
            .field("payload_codec", &self.payload_codec)
            .field("key_binding", &self.key_binding)
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
    const VERSION: u8 = 6;
    const FIXED_ENCODED_LEN: usize = 1
        + 1
        + 1
        + OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN
        + 4
        + SSE_C_VALIDATOR_SALT_LEN
        + SSE_C_VALIDATOR_HMAC_LEN
        + SSE_C_WRAP_SALT_LEN
        + SSE_C_WRAP_NONCE_LEN
        + SSE_C_WRAPPED_DEK_LEN
        + SSE_C_SEGMENT_NONCE_PREFIX_LEN
        + SSE_C_CHECKSUM_NONCE_LEN
        + 2;

    /// Constructs deterministic encryption metadata for storage-owned tests.
    ///
    /// Production writers must use [`Self::new_with_encryption_instance_id`]
    /// so the stable identity cannot be omitted accidentally.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn new(
        validator_key_id: u32,
        validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
        validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
        wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
        wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    ) -> Self {
        let mut encryption_instance_id = [0; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN];
        encryption_instance_id[..SSE_C_WRAP_NONCE_LEN].copy_from_slice(&wrap_nonce);
        Self {
            payload_codec: SegmentPayloadCodec::SegmentAes256GcmV1,
            key_binding: ObjectEncryptionKeyBinding::ProvisionalV1,
            encryption_instance_id,
            validator_key_id,
            validator_salt,
            validator_hmac,
            wrap_salt,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0; SSE_C_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: EncryptedChecksumMetadata::empty(),
        }
    }

    // Keeping each persisted cryptographic component explicit at this
    // construction boundary is clearer than hiding them in positional tuples.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_with_encryption_instance_id(
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
        validator_key_id: u32,
        validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
        validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
        wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
        wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    ) -> Self {
        Self {
            payload_codec: SegmentPayloadCodec::SegmentAes256GcmV1,
            key_binding: ObjectEncryptionKeyBinding::ProvisionalV1,
            encryption_instance_id,
            validator_key_id,
            validator_salt,
            validator_hmac,
            wrap_salt,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0; SSE_C_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: EncryptedChecksumMetadata::empty(),
        }
    }

    /// Selects the durable payload codec while assembling a new encryption state.
    #[must_use]
    pub fn with_payload_codec(mut self, payload_codec: SegmentPayloadCodec) -> Self {
        self.payload_codec =
            SegmentPayloadProfile::new(ObjectEncryptionType::SseCustomer, payload_codec)
                .payload_codec();
        self
    }

    /// Sets the stable random identity shared by every cryptographic purpose
    /// for this object encryption instance.
    ///
    /// The identity is independent of the DEK-wrap nonce so terminal
    /// publication can rewrap the same DEK under a fresh nonce without
    /// changing the already-encrypted segment subjects.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn with_encryption_instance_id(
        mut self,
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
    ) -> Self {
        self.encryption_instance_id = encryption_instance_id;
        self
    }

    pub fn with_encrypted_checksum_metadata(
        &self,
        checksum_nonce: [u8; SSE_C_CHECKSUM_NONCE_LEN],
        encrypted_checksum_metadata: Vec<u8>,
    ) -> Result<Self, ObjectEncryptionStateError> {
        if encrypted_checksum_metadata.is_empty() && checksum_nonce != [0; SSE_C_CHECKSUM_NONCE_LEN]
        {
            return Err(ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce);
        }
        Ok(Self {
            checksum_nonce,
            encrypted_checksum_metadata: EncryptedChecksumMetadata::try_new(
                encrypted_checksum_metadata,
            )?,
            ..self.clone()
        })
    }

    #[must_use]
    pub const fn payload_codec(&self) -> SegmentPayloadCodec {
        self.payload_codec
    }

    #[must_use]
    pub const fn key_binding(&self) -> ObjectEncryptionKeyBinding {
        self.key_binding
    }

    #[must_use]
    pub const fn encryption_instance_id(&self) -> &[u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN] {
        &self.encryption_instance_id
    }

    #[must_use]
    pub const fn validator_key_id(&self) -> u32 {
        self.validator_key_id
    }

    #[must_use]
    pub const fn validator_salt(&self) -> &[u8; SSE_C_VALIDATOR_SALT_LEN] {
        &self.validator_salt
    }

    #[must_use]
    pub const fn validator_hmac(&self) -> &[u8; SSE_C_VALIDATOR_HMAC_LEN] {
        &self.validator_hmac
    }

    #[must_use]
    pub const fn wrap_salt(&self) -> &[u8; SSE_C_WRAP_SALT_LEN] {
        &self.wrap_salt
    }

    #[must_use]
    pub const fn wrap_nonce(&self) -> &[u8; SSE_C_WRAP_NONCE_LEN] {
        &self.wrap_nonce
    }

    #[must_use]
    pub const fn wrapped_dek(&self) -> &[u8; SSE_C_WRAPPED_DEK_LEN] {
        &self.wrapped_dek
    }

    #[must_use]
    pub const fn segment_nonce_prefix(&self) -> &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN] {
        &self.segment_nonce_prefix
    }

    #[must_use]
    pub const fn checksum_nonce(&self) -> &[u8; SSE_C_CHECKSUM_NONCE_LEN] {
        &self.checksum_nonce
    }

    #[must_use]
    pub fn encrypted_checksum_metadata(&self) -> &[u8] {
        self.encrypted_checksum_metadata.as_slice()
    }

    #[must_use]
    fn encode(&self) -> Vec<u8> {
        let checksum_len = self.encrypted_checksum_metadata.encoded_len();
        let mut out = Vec::with_capacity(Self::FIXED_ENCODED_LEN + usize::from(checksum_len));
        out.push(Self::VERSION);
        out.push(self.payload_codec.tag());
        out.push(self.key_binding.tag());
        out.extend_from_slice(&self.encryption_instance_id);
        out.extend_from_slice(&self.validator_key_id.to_be_bytes());
        out.extend_from_slice(&self.validator_salt);
        out.extend_from_slice(&self.validator_hmac);
        out.extend_from_slice(&self.wrap_salt);
        out.extend_from_slice(&self.wrap_nonce);
        out.extend_from_slice(&self.wrapped_dek);
        out.extend_from_slice(&self.segment_nonce_prefix);
        out.extend_from_slice(&self.checksum_nonce);
        out.extend_from_slice(&checksum_len.to_be_bytes());
        out.extend_from_slice(self.encrypted_checksum_metadata.as_slice());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, ObjectEncryptionDecodeError> {
        if bytes
            .first()
            .is_some_and(|version| *version != Self::VERSION)
        {
            return Err(
                ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion {
                    version: bytes[0],
                },
            );
        }
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(ObjectEncryptionDecodeError::InvalidSseCustomerStateLength {
                actual: bytes.len(),
                minimum: Self::FIXED_ENCODED_LEN,
            });
        }
        let payload_codec = SegmentPayloadCodec::from_u8(bytes[1]).ok_or(
            ObjectEncryptionDecodeError::UnsupportedSegmentPayloadCodec { codec: bytes[1] },
        )?;
        let key_binding = ObjectEncryptionKeyBinding::from_u8(bytes[2]).ok_or(
            ObjectEncryptionDecodeError::UnsupportedObjectEncryptionKeyBinding {
                binding: bytes[2],
            },
        )?;
        let profile = SegmentPayloadProfile::new(ObjectEncryptionType::SseCustomer, payload_codec);
        let mut cursor = 3;
        let take = |cursor: &mut usize, len: usize| {
            let start = *cursor;
            let end = start + len;
            *cursor = end;
            &bytes[start..end]
        };

        let encryption_instance_id = take(&mut cursor, OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN)
            .try_into()
            .expect("slice length checked");
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
        );
        let checksum_len_usize = usize::from(checksum_len);
        if cursor + checksum_len_usize != bytes.len() {
            return Err(
                ObjectEncryptionDecodeError::InvalidSseCustomerChecksumMetadataLength {
                    declared: checksum_len_usize,
                    remaining: bytes.len().saturating_sub(cursor),
                },
            );
        }
        let encrypted_checksum_metadata = EncryptedChecksumMetadata::from_encoded(
            checksum_len,
            take(&mut cursor, checksum_len_usize).to_vec(),
        );
        if encrypted_checksum_metadata.is_empty() && checksum_nonce != [0; SSE_C_CHECKSUM_NONCE_LEN]
        {
            return Err(ObjectEncryptionDecodeError::NoncanonicalSseCustomerEmptyChecksumNonce);
        }

        Ok(Self {
            payload_codec: profile.payload_codec(),
            key_binding,
            encryption_instance_id,
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
    payload_codec: SegmentPayloadCodec,
    key_binding: ObjectEncryptionKeyBinding,
    encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
    wrapping_key_id: u32,
    wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
    wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN],
    segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    checksum_nonce: [u8; SSE_S3_CHECKSUM_NONCE_LEN],
    encrypted_checksum_metadata: EncryptedChecksumMetadata,
}

impl std::fmt::Debug for SseS3ObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseS3ObjectState")
            .field("payload_codec", &self.payload_codec)
            .field("key_binding", &self.key_binding)
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
    const VERSION: u8 = 4;
    const FIXED_ENCODED_LEN: usize = 1
        + 1
        + 1
        + OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN
        + 4
        + SSE_S3_WRAP_NONCE_LEN
        + SSE_S3_WRAPPED_DEK_LEN
        + SSE_S3_SEGMENT_NONCE_PREFIX_LEN
        + SSE_S3_CHECKSUM_NONCE_LEN
        + 2;

    /// Constructs deterministic encryption metadata for storage-owned tests.
    ///
    /// Production writers must use [`Self::new_with_encryption_instance_id`]
    /// so the stable identity cannot be omitted accidentally.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn new(
        wrapping_key_id: u32,
        wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    ) -> Self {
        let mut encryption_instance_id = [0; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN];
        encryption_instance_id[..SSE_S3_WRAP_NONCE_LEN].copy_from_slice(&wrap_nonce);
        Self {
            payload_codec: SegmentPayloadCodec::SegmentAes256GcmV1,
            key_binding: ObjectEncryptionKeyBinding::ProvisionalV1,
            encryption_instance_id,
            wrapping_key_id,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: EncryptedChecksumMetadata::empty(),
        }
    }

    #[must_use]
    pub fn new_with_encryption_instance_id(
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
        wrapping_key_id: u32,
        wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
        wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN],
        segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    ) -> Self {
        Self {
            payload_codec: SegmentPayloadCodec::SegmentAes256GcmV1,
            key_binding: ObjectEncryptionKeyBinding::ProvisionalV1,
            encryption_instance_id,
            wrapping_key_id,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: EncryptedChecksumMetadata::empty(),
        }
    }

    /// Selects the durable payload codec while assembling a new encryption state.
    #[must_use]
    pub fn with_payload_codec(mut self, payload_codec: SegmentPayloadCodec) -> Self {
        self.payload_codec =
            SegmentPayloadProfile::new(ObjectEncryptionType::SseS3, payload_codec).payload_codec();
        self
    }

    /// Sets the stable random identity shared by every cryptographic purpose
    /// for this object encryption instance.
    ///
    /// This value remains stable when terminal publication later rewraps the
    /// DEK with a fresh nonce under the committed object subject.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn with_encryption_instance_id(
        mut self,
        encryption_instance_id: [u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
    ) -> Self {
        self.encryption_instance_id = encryption_instance_id;
        self
    }

    pub fn with_encrypted_checksum_metadata(
        &self,
        checksum_nonce: [u8; SSE_S3_CHECKSUM_NONCE_LEN],
        encrypted_checksum_metadata: Vec<u8>,
    ) -> Result<Self, ObjectEncryptionStateError> {
        if encrypted_checksum_metadata.is_empty()
            && checksum_nonce != [0; SSE_S3_CHECKSUM_NONCE_LEN]
        {
            return Err(ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce);
        }
        Ok(Self {
            checksum_nonce,
            encrypted_checksum_metadata: EncryptedChecksumMetadata::try_new(
                encrypted_checksum_metadata,
            )?,
            ..self.clone()
        })
    }

    #[must_use]
    pub const fn payload_codec(&self) -> SegmentPayloadCodec {
        self.payload_codec
    }

    #[must_use]
    pub const fn key_binding(&self) -> ObjectEncryptionKeyBinding {
        self.key_binding
    }

    #[must_use]
    pub const fn encryption_instance_id(&self) -> &[u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN] {
        &self.encryption_instance_id
    }

    #[must_use]
    pub const fn wrapping_key_id(&self) -> u32 {
        self.wrapping_key_id
    }

    #[must_use]
    pub const fn wrap_nonce(&self) -> &[u8; SSE_S3_WRAP_NONCE_LEN] {
        &self.wrap_nonce
    }

    #[must_use]
    pub const fn wrapped_dek(&self) -> &[u8; SSE_S3_WRAPPED_DEK_LEN] {
        &self.wrapped_dek
    }

    #[must_use]
    pub const fn segment_nonce_prefix(&self) -> &[u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN] {
        &self.segment_nonce_prefix
    }

    #[must_use]
    pub const fn checksum_nonce(&self) -> &[u8; SSE_S3_CHECKSUM_NONCE_LEN] {
        &self.checksum_nonce
    }

    #[must_use]
    pub fn encrypted_checksum_metadata(&self) -> &[u8] {
        self.encrypted_checksum_metadata.as_slice()
    }

    #[must_use]
    fn encode(&self) -> Vec<u8> {
        let checksum_len = self.encrypted_checksum_metadata.encoded_len();
        let mut out = Vec::with_capacity(Self::FIXED_ENCODED_LEN + usize::from(checksum_len));
        out.push(Self::VERSION);
        out.push(self.payload_codec.tag());
        out.push(self.key_binding.tag());
        out.extend_from_slice(&self.encryption_instance_id);
        out.extend_from_slice(&self.wrapping_key_id.to_be_bytes());
        out.extend_from_slice(&self.wrap_nonce);
        out.extend_from_slice(&self.wrapped_dek);
        out.extend_from_slice(&self.segment_nonce_prefix);
        out.extend_from_slice(&self.checksum_nonce);
        out.extend_from_slice(&checksum_len.to_be_bytes());
        out.extend_from_slice(self.encrypted_checksum_metadata.as_slice());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, ObjectEncryptionDecodeError> {
        if bytes
            .first()
            .is_some_and(|version| *version != Self::VERSION)
        {
            return Err(ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion {
                version: bytes[0],
            });
        }
        if bytes.len() < Self::FIXED_ENCODED_LEN {
            return Err(ObjectEncryptionDecodeError::InvalidSseS3StateLength {
                actual: bytes.len(),
                minimum: Self::FIXED_ENCODED_LEN,
            });
        }
        let payload_codec = SegmentPayloadCodec::from_u8(bytes[1]).ok_or(
            ObjectEncryptionDecodeError::UnsupportedSegmentPayloadCodec { codec: bytes[1] },
        )?;
        let key_binding = ObjectEncryptionKeyBinding::from_u8(bytes[2]).ok_or(
            ObjectEncryptionDecodeError::UnsupportedObjectEncryptionKeyBinding {
                binding: bytes[2],
            },
        )?;
        let profile = SegmentPayloadProfile::new(ObjectEncryptionType::SseS3, payload_codec);
        let mut cursor = 3;
        let take = |cursor: &mut usize, len: usize| {
            let start = *cursor;
            let end = start + len;
            *cursor = end;
            &bytes[start..end]
        };

        let encryption_instance_id = take(&mut cursor, OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN)
            .try_into()
            .expect("slice length checked");
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
        );
        let checksum_len_usize = usize::from(checksum_len);
        if cursor + checksum_len_usize != bytes.len() {
            return Err(
                ObjectEncryptionDecodeError::InvalidSseS3ChecksumMetadataLength {
                    declared: checksum_len_usize,
                    remaining: bytes.len().saturating_sub(cursor),
                },
            );
        }
        let encrypted_checksum_metadata = EncryptedChecksumMetadata::from_encoded(
            checksum_len,
            take(&mut cursor, checksum_len_usize).to_vec(),
        );
        if encrypted_checksum_metadata.is_empty()
            && checksum_nonce != [0; SSE_S3_CHECKSUM_NONCE_LEN]
        {
            return Err(ObjectEncryptionDecodeError::NoncanonicalSseS3EmptyChecksumNonce);
        }

        Ok(Self {
            payload_codec: profile.payload_codec(),
            key_binding,
            encryption_instance_id,
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
#[derive(Clone, PartialEq, Eq)]
pub enum ObjectEncryption {
    SseCustomer(SseCustomerObjectState),
    SseS3(SseS3ObjectState),
}

impl std::fmt::Debug for ObjectEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SseCustomer(state) => f.debug_tuple("SseCustomer").field(state).finish(),
            Self::SseS3(state) => f.debug_tuple("SseS3").field(state).finish(),
        }
    }
}

impl ObjectEncryption {
    fn encryption_instance_id(&self) -> [u8; OBJECT_ENCRYPTION_INSTANCE_ID_LEN] {
        let (mode, random_id) = match self {
            Self::SseCustomer(state) => (
                ObjectEncryptionType::SseCustomer,
                state.encryption_instance_id(),
            ),
            Self::SseS3(state) => (ObjectEncryptionType::SseS3, state.encryption_instance_id()),
        };
        let mut instance_id = [0; OBJECT_ENCRYPTION_INSTANCE_ID_LEN];
        instance_id[0] = mode as u8;
        instance_id[1..].copy_from_slice(random_id);
        instance_id
    }

    #[must_use]
    pub(crate) const fn encryption_type(&self) -> ObjectEncryptionType {
        self.payload_profile().encryption_type()
    }

    #[must_use]
    pub(crate) fn encode_state(&self) -> Option<Vec<u8>> {
        match self {
            Self::SseCustomer(state) => Some(state.encode()),
            Self::SseS3(state) => Some(state.encode()),
        }
    }

    #[must_use]
    pub const fn payload_codec(&self) -> SegmentPayloadCodec {
        self.payload_profile().payload_codec()
    }

    #[must_use]
    pub const fn key_binding(&self) -> ObjectEncryptionKeyBinding {
        match self {
            Self::SseCustomer(state) => state.key_binding(),
            Self::SseS3(state) => state.key_binding(),
        }
    }

    /// Replaces provisional DEK/checksum ciphertext with ciphertext bound to
    /// the exact storage-minted committed-object subject.
    ///
    /// This transition is deliberately one-way. The subject must have been
    /// minted from the complete provisional state, preventing a caller from
    /// combining one write's identity with another write's encryption
    /// material. All immutable segment-encryption fields remain unchanged.
    pub fn bind_committed_object_ciphertexts(
        &self,
        subject: &ObjectEncryptionSubject,
        wrap_nonce: [u8; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
        wrapped_dek: [u8; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
        checksum_nonce: [u8; OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
        encrypted_checksum_metadata: Vec<u8>,
    ) -> Result<Self, ObjectEncryptionStateError> {
        if self.key_binding() != ObjectEncryptionKeyBinding::ProvisionalV1 {
            return Err(ObjectEncryptionStateError::ObjectEncryptionAlreadySubjectBound);
        }
        if !subject.matches_encryption(self) {
            return Err(ObjectEncryptionStateError::ObjectEncryptionSubjectMismatch);
        }
        if encrypted_checksum_metadata.is_empty()
            && checksum_nonce != [0; OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN]
        {
            return Err(ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce);
        }
        let encrypted_checksum_metadata =
            EncryptedChecksumMetadata::try_new(encrypted_checksum_metadata)?;
        Ok(match self {
            Self::SseCustomer(state) => Self::SseCustomer(SseCustomerObjectState {
                key_binding: ObjectEncryptionKeyBinding::ObjectSubjectV2,
                wrap_nonce,
                wrapped_dek,
                checksum_nonce,
                encrypted_checksum_metadata,
                ..state.clone()
            }),
            Self::SseS3(state) => Self::SseS3(SseS3ObjectState {
                key_binding: ObjectEncryptionKeyBinding::ObjectSubjectV2,
                wrap_nonce,
                wrapped_dek,
                checksum_nonce,
                encrypted_checksum_metadata,
                ..state.clone()
            }),
        })
    }

    #[must_use]
    pub(crate) const fn payload_profile(&self) -> SegmentPayloadProfile {
        match self {
            Self::SseCustomer(state) => {
                SegmentPayloadProfile::new(ObjectEncryptionType::SseCustomer, state.payload_codec())
            }
            Self::SseS3(state) => {
                SegmentPayloadProfile::new(ObjectEncryptionType::SseS3, state.payload_codec())
            }
        }
    }

    #[must_use]
    pub fn segment_stored_size(&self, logical_size: u64) -> Option<u64> {
        self.payload_profile().stored_size(logical_size)
    }

    #[must_use]
    pub fn segment_logical_size(&self, stored_size: u64) -> Option<u64> {
        self.payload_profile().logical_size(stored_size)
    }

    #[must_use]
    pub const fn uses_sse_customer_headers(&self) -> bool {
        matches!(self, Self::SseCustomer(_))
    }

    #[must_use]
    pub const fn managed_encryption_algorithm(&self) -> Option<ManagedEncryptionAlgorithm> {
        match self {
            Self::SseS3(_) => Some(ManagedEncryptionAlgorithm::Aes256),
            Self::SseCustomer(_) => None,
        }
    }

    pub(crate) fn decode(
        encryption_type: ObjectEncryptionType,
        state: Option<Vec<u8>>,
    ) -> Result<Self, ObjectEncryptionDecodeError> {
        match (encryption_type, state) {
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
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
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
    pub(crate) fn from_hex(hex: &str) -> Result<Self, StoreError> {
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

/// Immutable integrity evidence for one complete erasure-coded segment.
///
/// The witness can be minted only from the exact, canonically ordered shard
/// acknowledgements for a segment. Besides retaining every shard checksum, it
/// proves that all acknowledgements use the canonical shard size and that the
/// systematic acknowledgements compose to the stored segment checksum. The
/// witness is storage-owned so terminal publishers cannot assemble equivalent-
/// looking evidence from independently selected physical fields.
#[derive(PartialEq, Eq)]
pub(crate) struct SegmentShardIntegrityWitness {
    data_pg_id: u32,
    durable: SegmentShardIntegrityRecord,
    ordered_shard_acks: Box<[(ShardKey, WriteAck)]>,
}

impl std::fmt::Debug for SegmentShardIntegrityWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentShardIntegrityWitness")
            .field("shard_size", &self.durable.shard_size)
            .field("shard_count", &self.ordered_shard_acks.len())
            .finish_non_exhaustive()
    }
}

/// Durable physical-integrity evidence for one complete segment shard set.
///
/// Shard identities are derived from the containing segment record. The
/// position of each CRC is its shard index, and every shard has the one
/// canonical stored size recorded here. Construction remains storage-owned so
/// callers cannot persist a partial or reordered acknowledgement projection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SegmentShardIntegrityRecord {
    shard_size: u64,
    ordered_crc64s: Box<[u64]>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SegmentShardIntegrityWitnessError {
    #[error("segment shard integrity witness encoding is truncated")]
    TruncatedEncoding,
    #[error("unsupported segment shard integrity witness version {version}")]
    UnsupportedVersion { version: u8 },
    #[error("segment shard integrity witness has trailing bytes")]
    TrailingEncoding,
    #[error("EC shape must contain at least one data shard")]
    MissingDataShard,
    #[error("EC shape {data_shards}+{parity_shards} overflows its shard index space")]
    InvalidShardCount { data_shards: u8, parity_shards: u8 },
    #[error("expected {expected} shard acknowledgements, got {actual}")]
    ShardCount { expected: usize, actual: usize },
    #[error("expected shard {expected} at ordered position {position}, got {actual}")]
    ShardIdentity {
        position: usize,
        expected: ShardKey,
        actual: ShardKey,
    },
    #[error(
        "shard {shard_index} has stored size {actual}, expected canonical shard size {expected}"
    )]
    NoncanonicalShardSize {
        shard_index: u8,
        expected: u64,
        actual: u64,
    },
    #[error("systematic shard acknowledgements do not compose to the stored segment checksum")]
    SystematicChecksumMismatch,
}

impl SegmentShardIntegrityWitness {
    pub(crate) fn from_ordered_shard_acks(
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        stored_segment_size: u64,
        segment_crc64: u64,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<Self, SegmentShardIntegrityWitnessError> {
        if ec.k == 0 {
            return Err(SegmentShardIntegrityWitnessError::MissingDataShard);
        }
        let expected_count = usize::from(ec.k).checked_add(usize::from(ec.m)).ok_or(
            SegmentShardIntegrityWitnessError::InvalidShardCount {
                data_shards: ec.k,
                parity_shards: ec.m,
            },
        )?;
        if expected_count > usize::from(u8::MAX) + 1 {
            return Err(SegmentShardIntegrityWitnessError::InvalidShardCount {
                data_shards: ec.k,
                parity_shards: ec.m,
            });
        }
        if shard_batch.len() != expected_count {
            return Err(SegmentShardIntegrityWitnessError::ShardCount {
                expected: expected_count,
                actual: shard_batch.len(),
            });
        }

        let shard_size = stored_segment_size.div_ceil(u64::from(ec.k));
        let mut ordered_shard_acks = Vec::with_capacity(expected_count);
        for (position, (actual_key, ack)) in shard_batch.iter().enumerate() {
            let shard_index = u8::try_from(position).map_err(|_| {
                SegmentShardIntegrityWitnessError::InvalidShardCount {
                    data_shards: ec.k,
                    parity_shards: ec.m,
                }
            })?;
            let expected_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index);
            if expected_key != **actual_key {
                return Err(SegmentShardIntegrityWitnessError::ShardIdentity {
                    position,
                    expected: expected_key,
                    actual: (*actual_key).clone(),
                });
            }
            if ack.stored_size != shard_size {
                return Err(SegmentShardIntegrityWitnessError::NoncanonicalShardSize {
                    shard_index,
                    expected: shard_size,
                    actual: ack.stored_size,
                });
            }
            ordered_shard_acks.push(((*actual_key).clone(), *ack));
        }

        let systematic_acks = shard_batch
            .get(..usize::from(ec.k))
            .expect("validated shard count contains every systematic acknowledgement");
        let systematic_acks = systematic_acks
            .iter()
            .map(|(_, ack)| *ack)
            .collect::<Vec<_>>();
        if !systematic_data_shard_acks_match_segment(
            &systematic_acks,
            stored_segment_size,
            segment_crc64,
        ) {
            return Err(SegmentShardIntegrityWitnessError::SystematicChecksumMismatch);
        }

        Ok(Self {
            data_pg_id,
            durable: SegmentShardIntegrityRecord {
                shard_size,
                ordered_crc64s: ordered_shard_acks
                    .iter()
                    .map(|(_, ack)| ack.crc64)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            },
            ordered_shard_acks: ordered_shard_acks.into_boxed_slice(),
        })
    }

    pub(crate) fn data_pg_id(&self) -> u32 {
        self.data_pg_id
    }

    pub(crate) fn registration_batch(&self) -> Vec<(&ShardKey, WriteAck)> {
        self.ordered_shard_acks
            .iter()
            .map(|(key, ack)| (key, *ack))
            .collect()
    }

    pub(crate) fn durable_record(&self) -> SegmentShardIntegrityRecord {
        self.durable.clone()
    }
}

impl SegmentShardIntegrityRecord {
    const ENCODING_VERSION: u8 = 1;

    pub(crate) fn validate(
        &self,
        ec: EcShape,
        stored_segment_size: u64,
        segment_crc64: u64,
    ) -> Result<(), SegmentShardIntegrityWitnessError> {
        if ec.k == 0 {
            return Err(SegmentShardIntegrityWitnessError::MissingDataShard);
        }
        let expected_count = usize::from(ec.k).checked_add(usize::from(ec.m)).ok_or(
            SegmentShardIntegrityWitnessError::InvalidShardCount {
                data_shards: ec.k,
                parity_shards: ec.m,
            },
        )?;
        if expected_count > usize::from(u8::MAX) + 1 {
            return Err(SegmentShardIntegrityWitnessError::InvalidShardCount {
                data_shards: ec.k,
                parity_shards: ec.m,
            });
        }
        if self.ordered_crc64s.len() != expected_count {
            return Err(SegmentShardIntegrityWitnessError::ShardCount {
                expected: expected_count,
                actual: self.ordered_crc64s.len(),
            });
        }

        let expected_shard_size = stored_segment_size.div_ceil(u64::from(ec.k));
        if self.shard_size != expected_shard_size {
            return Err(SegmentShardIntegrityWitnessError::NoncanonicalShardSize {
                shard_index: 0,
                expected: expected_shard_size,
                actual: self.shard_size,
            });
        }
        let systematic_acks = self
            .ordered_crc64s
            .iter()
            .take(usize::from(ec.k))
            .map(|crc64| WriteAck {
                crc64: *crc64,
                stored_size: self.shard_size,
            })
            .collect::<Vec<_>>();
        if !systematic_data_shard_acks_match_segment(
            &systematic_acks,
            stored_segment_size,
            segment_crc64,
        ) {
            return Err(SegmentShardIntegrityWitnessError::SystematicChecksumMismatch);
        }
        Ok(())
    }

    pub(crate) fn expected_ack(&self, shard_index: usize) -> Option<WriteAck> {
        self.ordered_crc64s
            .get(shard_index)
            .copied()
            .map(|crc64| WriteAck {
                crc64,
                stored_size: self.shard_size,
            })
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let count = u16::try_from(self.ordered_crc64s.len())
            .expect("validated segment witness shard count fits u16");
        let mut bytes = Vec::with_capacity(1 + 8 + 2 + self.ordered_crc64s.len() * 8);
        bytes.push(Self::ENCODING_VERSION);
        bytes.extend_from_slice(&self.shard_size.to_be_bytes());
        bytes.extend_from_slice(&count.to_be_bytes());
        for crc64 in &self.ordered_crc64s {
            bytes.extend_from_slice(&crc64.to_be_bytes());
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SegmentShardIntegrityWitnessError> {
        let Some((&version, remainder)) = bytes.split_first() else {
            return Err(SegmentShardIntegrityWitnessError::TruncatedEncoding);
        };
        if version != Self::ENCODING_VERSION {
            return Err(SegmentShardIntegrityWitnessError::UnsupportedVersion { version });
        }
        let Some(shard_size_bytes) = remainder.get(..8) else {
            return Err(SegmentShardIntegrityWitnessError::TruncatedEncoding);
        };
        let shard_size = u64::from_be_bytes(
            shard_size_bytes
                .try_into()
                .expect("checked witness shard-size width"),
        );
        let Some(count_bytes) = remainder.get(8..10) else {
            return Err(SegmentShardIntegrityWitnessError::TruncatedEncoding);
        };
        let count = usize::from(u16::from_be_bytes(
            count_bytes
                .try_into()
                .expect("checked witness shard-count width"),
        ));
        let expected_len = 1usize
            .checked_add(8)
            .and_then(|len| len.checked_add(2))
            .and_then(|len| len.checked_add(count.checked_mul(8)?))
            .ok_or(SegmentShardIntegrityWitnessError::TrailingEncoding)?;
        if bytes.len() < expected_len {
            return Err(SegmentShardIntegrityWitnessError::TruncatedEncoding);
        }
        if bytes.len() != expected_len {
            return Err(SegmentShardIntegrityWitnessError::TrailingEncoding);
        }
        let ordered_crc64s = bytes[11..]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| u64::from_be_bytes(*chunk))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            shard_size,
            ordered_crc64s,
        })
    }
}

pub(crate) fn systematic_data_shard_acks_match_segment(
    shard_acks: &[WriteAck],
    stored_segment_size: u64,
    segment_crc64: u64,
) -> bool {
    let Some((first, remaining)) = shard_acks.split_first() else {
        return false;
    };
    let mut combined_crc64 = first.crc64;
    let mut padded_size = first.stored_size;
    for ack in remaining {
        combined_crc64 = checksum::crc64::combine(combined_crc64, ack.crc64, ack.stored_size);
        let Some(next_size) = padded_size.checked_add(ack.stored_size) else {
            return false;
        };
        padded_size = next_size;
    }

    let Some(padding_size) = padded_size.checked_sub(stored_segment_size) else {
        return false;
    };
    let Ok(padding_size) = usize::try_from(padding_size) else {
        return false;
    };
    // Erasure coding pads the concatenated systematic shards only at the end,
    // by fewer than k bytes. Extend the segment checksum over those zeros so
    // it can be compared with the composed shard acknowledgements without
    // rescanning the assembled payload.
    const ZERO_PADDING: [u8; u8::MAX as usize] = [0; u8::MAX as usize];
    let Some(padding) = ZERO_PADDING.get(..padding_size) else {
        return false;
    };
    let padded_segment_crc64 = checksum::crc64::combine(
        segment_crc64,
        checksum::crc64::checksum(padding),
        padding_size as u64,
    );
    combined_crc64 == padded_segment_crc64
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
pub(crate) enum ShardScavengerObservationReason {
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
    pub(crate) fn from_u8(v: u8) -> Option<Self> {
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
pub(crate) struct ShardScavengerObservationKey {
    pub(crate) node_id: u32,
    pub(crate) data_pg_id: u32,
    pub(crate) shard_index: ShardIndex,
    pub(crate) shard_key: ShardKey,
}

/// Input for recording one shard scavenger audit observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerObservationRecord {
    pub(crate) key: ShardScavengerObservationKey,
    pub(crate) data_size: Option<u64>,
    pub(crate) crc64: Option<u64>,
    pub(crate) file_exists: bool,
    pub(crate) shard_row_exists: bool,
    pub(crate) reason: ShardScavengerObservationReason,
    pub(crate) last_error: Option<String>,
}

/// Non-authoritative audit row for an apparent physical shard orphan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerObservation {
    pub(crate) key: ShardScavengerObservationKey,
    pub(crate) first_seen_at: u64,
    pub(crate) last_seen_at: u64,
    pub(crate) observation_count: u64,
    pub(crate) data_size: Option<u64>,
    pub(crate) crc64: Option<u64>,
    pub(crate) file_exists: bool,
    pub(crate) shard_row_exists: bool,
    pub(crate) reason: ShardScavengerObservationReason,
    pub(crate) last_error: Option<String>,
    pub(crate) resolved_at: Option<u64>,
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
pub(crate) struct SegmentStoredBytesRequest {
    pub(crate) data_pg_id: u32,
    pub(crate) segment_okh: [u8; 16],
    pub(crate) segment_vid: GenerationId,
    pub(crate) stored_size: usize,
    pub(crate) segment_crc64: u64,
    pub(crate) ec: EcShape,
}

/// Opaque storage-owned description of one persisted object payload segment.
///
/// Callers may use the logical segment metadata needed to assemble an object
/// response, but placement coordinates and erasure-coding details remain an
/// implementation detail of the storage crate.
#[derive(Clone, PartialEq, Eq)]
pub struct ObjectPayloadSegment {
    subject: ObjectPayloadSubject,
    record: ObjectPayloadSegmentRecord,
    stored_size: usize,
    encryption_subject: Option<Arc<SegmentEncryptionSubject>>,
}

#[derive(Clone, PartialEq, Eq)]
struct ObjectPayloadSubject {
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

#[derive(Clone, PartialEq, Eq)]
enum ObjectPayloadSegmentRecord {
    Object(ObjectSegmentRecord),
    Multipart(MultipartPartSegmentRecord),
}

impl std::fmt::Debug for ObjectPayloadSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectPayloadSegment")
            .field("segment_index", &self.segment_index())
            .field("size", &self.size())
            .finish_non_exhaustive()
    }
}

pub(crate) fn validate_segment_shard_integrity_for_profile(
    payload_profile: SegmentPayloadProfile,
    shard_integrity: Option<&SegmentShardIntegrityRecord>,
    ec: EcShape,
    stored_size: u64,
    segment_crc64: u64,
) -> Result<(), &'static str> {
    let Some(shard_integrity) = shard_integrity else {
        return if payload_profile.requires_shard_integrity_witness() {
            Err("Cobblestone segment is missing its shard integrity witness")
        } else {
            Ok(())
        };
    };
    shard_integrity
        .validate(ec, stored_size, segment_crc64)
        .map_err(|_| "segment shard integrity witness does not match segment geometry")
}

impl ObjectPayloadSegment {
    fn from_object_record(
        subject: &ObjectPayloadSubject,
        record: ObjectSegmentRecord,
        payload_profile: SegmentPayloadProfile,
    ) -> Result<Self, &'static str> {
        let stored_size_u64 = payload_profile
            .stored_size(record.size)
            .ok_or("object segment has invalid stored codec geometry")?;
        validate_segment_shard_integrity_for_profile(
            payload_profile,
            record.shard_integrity.as_ref(),
            EcShape {
                k: record.ec_k,
                m: record.ec_m,
            },
            stored_size_u64,
            record.segment_crc64,
        )?;
        let stored_size = usize::try_from(stored_size_u64)
            .map_err(|_| "object segment stored size exceeds platform limits")?;
        Ok(Self {
            subject: subject.clone(),
            record: ObjectPayloadSegmentRecord::Object(record),
            stored_size,
            encryption_subject: None,
        })
    }

    fn from_multipart_record(
        subject: &ObjectPayloadSubject,
        record: MultipartPartSegmentRecord,
        payload_profile: SegmentPayloadProfile,
    ) -> Result<Self, &'static str> {
        let stored_size_u64 = payload_profile
            .stored_size(record.size)
            .ok_or("multipart segment has invalid stored codec geometry")?;
        validate_segment_shard_integrity_for_profile(
            payload_profile,
            record.shard_integrity.as_ref(),
            EcShape {
                k: record.ec_k,
                m: record.ec_m,
            },
            stored_size_u64,
            record.segment_crc64,
        )?;
        let stored_size = usize::try_from(stored_size_u64)
            .map_err(|_| "multipart segment stored size exceeds platform limits")?;
        Ok(Self {
            subject: subject.clone(),
            record: ObjectPayloadSegmentRecord::Multipart(record),
            stored_size,
            encryption_subject: None,
        })
    }

    pub fn segment_index(&self) -> u32 {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => record.segment_index,
            ObjectPayloadSegmentRecord::Multipart(record) => record.segment_index,
        }
    }

    pub(crate) fn shard_integrity(&self) -> Option<&SegmentShardIntegrityRecord> {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => record.shard_integrity.as_ref(),
            ObjectPayloadSegmentRecord::Multipart(record) => record.shard_integrity.as_ref(),
        }
    }

    pub fn size(&self) -> u64 {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => record.size,
            ObjectPayloadSegmentRecord::Multipart(record) => record.size,
        }
    }

    pub fn part_number(&self) -> Option<u32> {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(_) => None,
            ObjectPayloadSegmentRecord::Multipart(record) => Some(record.part_number),
        }
    }

    /// Exact storage-minted logical subject authenticated by this segment's
    /// payload codec.
    ///
    /// Metadata-only and low-level diagnostic snapshots need not carry this
    /// authority. A production payload read binds it from the admitted bucket
    /// incarnation before transferring the snapshot to the reader.
    #[must_use]
    pub fn encryption_subject(&self) -> Option<&SegmentEncryptionSubject> {
        self.encryption_subject.as_deref()
    }

    pub(crate) fn matches_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.subject.bucket == *bucket
            && self.subject.key == *key
            && self.subject.generation_id == generation_id
    }

    pub(crate) fn placement_cluster_epoch(&self) -> ClusterEpoch {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => record.placement_cluster_epoch,
            ObjectPayloadSegmentRecord::Multipart(record) => record.placement_cluster_epoch,
        }
    }

    pub(crate) fn stored_bytes_request(&self) -> SegmentStoredBytesRequest {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => SegmentStoredBytesRequest {
                data_pg_id: record.data_pg_id,
                segment_okh: record.segment_okh,
                segment_vid: record.segment_vid,
                stored_size: self.stored_size,
                segment_crc64: record.segment_crc64,
                ec: EcShape {
                    k: record.ec_k,
                    m: record.ec_m,
                },
            },
            ObjectPayloadSegmentRecord::Multipart(record) => SegmentStoredBytesRequest {
                data_pg_id: record.data_pg_id,
                segment_okh: record.segment_okh,
                segment_vid: record.segment_vid,
                stored_size: self.stored_size,
                segment_crc64: record.segment_crc64,
                ec: EcShape {
                    k: record.ec_k,
                    m: record.ec_m,
                },
            },
        }
    }

    pub(crate) fn object_record(&self) -> Option<&ObjectSegmentRecord> {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(record) => Some(record),
            ObjectPayloadSegmentRecord::Multipart(_) => None,
        }
    }

    pub(crate) fn multipart_record(&self) -> Option<&MultipartPartSegmentRecord> {
        match &self.record {
            ObjectPayloadSegmentRecord::Object(_) => None,
            ObjectPayloadSegmentRecord::Multipart(record) => Some(record),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_segment_index(&self, segment_index: u32) -> Self {
        let mut changed = self.clone();
        match &mut changed.record {
            ObjectPayloadSegmentRecord::Object(record) => record.segment_index = segment_index,
            ObjectPayloadSegmentRecord::Multipart(record) => record.segment_index = segment_index,
        }
        changed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PlacedSegmentShardRepairWorkItem {
    pub(crate) request: SegmentStoredBytesRequest,
    pub(crate) shard_index: ShardIndex,
    pub(crate) shard_integrity: Option<SegmentShardIntegrityRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardRepairRecord {
    pub(crate) work_item: PlacedSegmentShardRepairWorkItem,
    pub(crate) first_seen_at: u64,
    pub(crate) last_seen_at: u64,
    pub(crate) observation_count: u64,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardRepairClaimRecord {
    pub(crate) work_item: PlacedSegmentShardRepairWorkItem,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) attempt_count: u64,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardRepairClaimAcquireParams {
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: u64,
    pub(crate) now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardRepairClaimAcquire {
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: u64,
    pub(crate) now: u64,
}

pub(crate) const PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT: usize = 1024;
pub(crate) const PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN: usize = 4096;
pub(crate) const PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN: usize = 128;
pub(crate) const PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PlacedSegmentShardBackfillWorkItem {
    pub request: SegmentStoredBytesRequest,
    pub source_cluster_epoch: ClusterEpoch,
    pub desired_cluster_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillRecord {
    pub work_item: PlacedSegmentShardBackfillWorkItem,
    pub remaining_tolerance: u8,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub observation_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillClaimRecord {
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
pub(crate) struct PlacedSegmentShardBackfillClaimAcquireParams {
    pub claim_id: String,
    pub owner_token: String,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillClaimAcquire {
    pub claim_id: String,
    pub owner_token: String,
    pub cluster_epoch: ClusterEpoch,
    pub claimed_at: u64,
    pub lease_deadline: u64,
    pub now: u64,
}

pub(crate) const PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT: usize = 1024;
pub(crate) const PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN: usize = 4096;
pub(crate) const PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN: usize = 128;
pub(crate) const PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN: usize = 128;

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
pub(crate) struct PayloadReclaimRoot {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
}

/// Durable object payload reclaim kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ObjectPayloadReclaimKind {
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
pub(crate) struct ObjectPayloadReclaimClaimRecord {
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
pub(crate) struct BucketDeleteFinalizeClaimRecord {
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BucketDeleteFinalizeRoot {
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
pub(crate) struct ObjectSegmentsReclaimSegmentRecord {
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
pub(crate) struct ObjectSegmentsReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub created_at: u64,
    pub segments: Vec<ObjectSegmentsReclaimSegmentRecord>,
}

/// Segment entry for a multipart part reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartReclaimPartSegmentRecord {
    pub part_number: u32,
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    pub ec: EcShape,
}

/// Part entry for a durable multipart reclaim record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartReclaimPartRecord {
    pub part_number: u32,
    pub segments: Vec<MultipartReclaimPartSegmentRecord>,
}

/// Durable reclaim record for a multipart payload generation.
///
/// This is used when the namespace-visible object row is removed or replaced
/// before the old multipart payload can be physically deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartReclaimRecord {
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
                .map(|part| MultipartReclaimPartRecord {
                    part_number: part.part_number,
                    segments: segments_by_part
                        .remove(&part.part_number)
                        .unwrap_or_default(),
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
    pub shard_integrity: Option<SegmentShardIntegrityRecord>,
}

pub(crate) const SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT: u16 = 64;

/// Stable, storage-owned resume position for placed-segment backfill discovery.
///
/// Each variant follows the native primary-key order of one metadata table so
/// the PG store can resume with an indexed keyset query. The cursor is opaque
/// outside storage and is safe to discard; a later pass will rediscover any
/// rows inserted before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardScavengerReferenceCursor {
    ObjectSegment {
        bucket: BucketName,
        key: ObjectKey,
        version_id: u64,
        segment_index: u32,
    },
    StreamUploadSegment {
        session_id: SessionId,
        segment_index: u32,
    },
    MultipartPartSegment {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
        part_number: u32,
        segment_index: u32,
    },
    ObjectReclaimSegment {
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
    },
    MultipartReclaimSegment {
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        part_number: u32,
        segment_index: u32,
    },
    PendingPlacedCommand {
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: u64,
        command_checksum: u64,
        reference_index: u32,
    },
    PendingReclaimCommand {
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: u64,
        command_checksum: u64,
        reference_index: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerReferencePageItem {
    pub cursor: ShardScavengerReferenceCursor,
    pub reference: ShardScavengerPayloadReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardScavengerReferencePage {
    pub items: Vec<ShardScavengerReferencePageItem>,
    pub complete: bool,
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

/// Durable metadata reference to a payload shard set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardScavengerPayloadReference {
    Placed(ShardScavengerPlacedShardSetReference),
    ReclaimOnly(ShardScavengerReclaimShardSetReference),
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

/// Bucket subresources whose payloads remain opaque outside storage.
///
/// Tagging is deliberately absent: bucket tags cross the public storage
/// boundary through [`SerializedBucketTagSet`] instead of the generic string
/// interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpaqueBucketSubresourceKind {
    Cors,
    Policy,
    Lifecycle,
}

impl OpaqueBucketSubresourceKind {
    pub(crate) const fn stored_kind(self) -> BucketSubresourceKind {
        match self {
            Self::Cors => BucketSubresourceKind::Cors,
            Self::Policy => BucketSubresourceKind::Policy,
            Self::Lifecycle => BucketSubresourceKind::Lifecycle,
        }
    }
}

/// Storage-owned discriminator for all persisted bucket subresources.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BucketSubresourceKind {
    Cors = 0,
    Tagging = 1,
    Policy = 4,
    Lifecycle = 5,
}

impl BucketSubresourceKind {
    #[must_use]
    pub(crate) fn from_u8(v: u8) -> Option<Self> {
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
pub(crate) enum BucketSubresourceAux {
    #[default]
    None,
    Policy {
        is_public: bool,
    },
}

impl BucketSubresourceAux {
    #[must_use]
    pub(crate) const fn policy(is_public: bool) -> Self {
        Self::Policy { is_public }
    }

    #[must_use]
    pub(crate) const fn policy_is_public(self) -> Option<bool> {
        match self {
            Self::None => None,
            Self::Policy { is_public, .. } => Some(is_public),
        }
    }
}

impl BucketSubresourceKind {
    #[must_use]
    pub(crate) const fn supports_aux(self, aux: BucketSubresourceAux) -> bool {
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

/// Storage-layer request for bucket subresource writes.
///
/// The representation tuple is storage-private. Public constructors keep the
/// subresource kind, value representation, and auxiliary data consistent by
/// construction, and tagging accepts only the validated bucket-tag carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutBucketSubresource<'a> {
    pub(crate) kind: BucketSubresourceKind,
    pub(crate) body: &'a str,
    pub(crate) aux: BucketSubresourceAux,
}

impl<'a> PutBucketSubresource<'a> {
    #[must_use]
    pub const fn cors(body: &'a str) -> Self {
        Self {
            kind: BucketSubresourceKind::Cors,
            body,
            aux: BucketSubresourceAux::None,
        }
    }

    #[must_use]
    pub fn tagging(tags: &'a SerializedBucketTagSet) -> Self {
        Self {
            kind: BucketSubresourceKind::Tagging,
            body: tags.as_str(),
            aux: BucketSubresourceAux::None,
        }
    }

    #[must_use]
    pub const fn policy(body: &'a str, is_public: bool) -> Self {
        Self {
            kind: BucketSubresourceKind::Policy,
            body,
            aux: BucketSubresourceAux::Policy { is_public },
        }
    }

    #[must_use]
    pub const fn lifecycle(body: &'a str) -> Self {
        Self {
            kind: BucketSubresourceKind::Lifecycle,
            body,
            aux: BucketSubresourceAux::None,
        }
    }

    #[must_use]
    pub(crate) const fn kind(self) -> BucketSubresourceKind {
        self.kind
    }

    #[must_use]
    pub(crate) fn body(self) -> &'a str {
        self.body
    }

    #[must_use]
    pub(crate) const fn aux(self) -> BucketSubresourceAux {
        self.aux
    }
}

/// Generic stored representation of an opaque bucket subresource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredBucketSubresource {
    pub(crate) body: String,
    pub(crate) generation: Option<u64>,
    pub(crate) aux: BucketSubresourceAux,
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
pub(crate) const BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN: usize = 1024;

/// Durable DeleteBucket attempt outcome kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BucketDeleteAttemptOutcomeKind {
    Retryable = 0,
    NotEmpty = 1,
    StaleGeneration = 2,
    MarkDeleting = 3,
}

/// Durable DeleteBucket begin phase reached by the recorded attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BucketDeleteAttemptPhase {
    Initial = 0,
    ReservationWait = 1,
    PostReservationObjectDrain = 2,
    StreamCleanup = 3,
    FinalVisibilityCheck = 4,
    FinalVisibilityProven = 5,
    MarkDeleting = 6,
    PostReservationStreamCleanup = 7,
}

/// Last durable DeleteBucket attempt outcome for a bucket.
///
/// A `FinalVisibilityProven` record matching the live write drain is also the
/// durable authorization consumed atomically when installing
/// `MarkBucketDeleting`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteAttemptOutcomeRecord {
    pub bucket: BucketName,
    pub drain_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub outcome: BucketDeleteAttemptOutcomeKind,
    pub phase: BucketDeleteAttemptPhase,
    pub detail: String,
    pub post_reservation_next_object_pg_id: Option<u32>,
    pub stream_cleanup_next_object_pg_id: Option<u32>,
    pub stream_cleanup_next_session_id_marker: Option<SessionId>,
    pub stream_cleanup_aborted_uploads: bool,
    pub final_visibility_next_object_pg_id: Option<u32>,
    pub finalizer_next_object_pg_id: Option<u32>,
    pub updated_at: u64,
}

/// Sanitized bucket row fields included in DeleteBucket local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugBucketRow {
    pub state: BucketState,
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
}

/// Sanitized durable bucket write-drain fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugDrain {
    pub drain_id: String,
    pub cluster_epoch: ClusterEpoch,
    pub bucket_execution_generation: u64,
    pub created_at: u64,
    pub lease_deadline: u64,
}

/// Durable bucket-delete finalizer claim fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugFinalizeClaim {
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
pub(crate) struct BucketDeleteDebugPendingCommand {
    pub kind: &'static str,
    pub target_bucket: BucketName,
    pub matches_bucket: bool,
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: u32,
    pub log_index: u64,
}

/// Sanitized object-version row sample included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugObjectVersionSample {
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
pub(crate) enum BucketDeleteDebugObjectVersionKind {
    Live,
    DeleteMarker,
}

/// Object-version sample scan error fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugObjectVersionSampleError {
    pub object_pg_id: u32,
    pub detail: String,
}

/// Bucket-scoped payload reclaim root fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugPayloadReclaimRoot {
    pub object_pg_id: u32,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub reclaim_kind: Option<ObjectPayloadReclaimKind>,
    pub reclaim_created_at: Option<u64>,
    pub reclaim_item_count: Option<usize>,
}

/// Payload reclaim root scan error fields included in local-debug output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugPayloadReclaimRootError {
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
pub(crate) struct BucketDeleteDebugPayloadReclaimClaim {
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
pub(crate) struct BucketDeleteDebugPayloadReclaimClaimError {
    pub object_pg_id: u32,
    pub detail: String,
}

/// Read-only durable state used by the local DeleteBucket debug endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketDeleteDebugSnapshot {
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

/// Opaque, storage-rendered diagnostics for an accepted bucket deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDeleteDiagnostic(String);

impl BucketDeleteDiagnostic {
    pub(crate) fn from_snapshot(snapshot: &BucketDeleteDebugSnapshot) -> Self {
        Self(render_bucket_delete_debug_snapshot(snapshot))
    }

    #[must_use]
    pub fn into_text(self) -> String {
        self.0
    }
}

fn render_bucket_delete_debug_snapshot(snapshot: &BucketDeleteDebugSnapshot) -> String {
    use std::fmt::Write as _;

    let mut body = String::new();
    writeln!(
        &mut body,
        "bucket={:?} pg_id={} cluster_epoch={} operation_epoch={} route_map_valid_until_ms={} bucket_pg_primary_node_id={}",
        snapshot.bucket.as_str(),
        snapshot.pg_id,
        snapshot.cluster_epoch.get(),
        snapshot.operation_epoch.get(),
        debug_optional_u64(snapshot.route_map_valid_until_ms),
        snapshot.bucket_pg_primary_node_id,
    )
    .expect("write to String");
    match &snapshot.bucket_row {
        Some(row) => {
            writeln!(
                &mut body,
                "bucket_row=present state={} bucket_execution_generation={} bucket_incarnation_generation={}",
                debug_bucket_state(row.state),
                row.bucket_execution_generation,
                row.bucket_incarnation_generation,
            )
            .expect("write to String");
        }
        None => body.push_str("bucket_row=absent\n"),
    }

    match &snapshot.durable_write_drain {
        Some(drain) => {
            writeln!(
                &mut body,
                "durable_write_drain=present drain_id={:?} cluster_epoch={} bucket_execution_generation={} created_at={} lease_deadline={}",
                drain.drain_id,
                drain.cluster_epoch.get(),
                drain.bucket_execution_generation,
                drain.created_at,
                drain.lease_deadline,
            )
            .expect("write to String");
        }
        None => body.push_str("durable_write_drain=absent\n"),
    }

    match &snapshot.pending_metadata_command {
        Some(command) => {
            writeln!(
                &mut body,
                "pending_metadata_command=present kind={} target_bucket={:?} matches_bucket={} cluster_epoch={} pg_id={} log_index={}",
                command.kind,
                command.target_bucket.as_str(),
                command.matches_bucket,
                command.cluster_epoch.get(),
                command.pg_id,
                command.log_index,
            )
            .expect("write to String");
        }
        None => body.push_str("pending_metadata_command=absent\n"),
    }

    match &snapshot.finalize_claim {
        Some(claim) => {
            writeln!(
                &mut body,
                "finalize_claim=present bucket={:?} matches_bucket={} bucket_incarnation_generation={} claim_id={:?} cluster_epoch={} pg_id={} claimed_at={} lease_deadline={} attempt_count={} last_error={}",
                claim.bucket.as_str(),
                claim.matches_bucket,
                claim.bucket_incarnation_generation,
                claim.claim_id,
                claim.cluster_epoch.get(),
                claim.pg_id,
                claim.claimed_at,
                debug_optional_u64(claim.lease_deadline),
                claim.attempt_count,
                observability::escaped(claim.last_error.as_deref().unwrap_or("")),
            )
            .expect("write to String");
        }
        None => body.push_str("finalize_claim=absent\n"),
    }

    writeln!(
        &mut body,
        "object_version_samples_count={}",
        snapshot.object_version_samples.len()
    )
    .expect("write to String");
    for (index, sample) in snapshot.object_version_samples.iter().enumerate() {
        writeln!(
            &mut body,
            "object_version_sample index={} object_pg_id={} kind={} key={:?} version_id={} generation_id={} size={} layout={} last_modified={} became_noncurrent_at={}",
            index,
            sample.object_pg_id,
            debug_object_version_kind(sample.kind),
            sample.key.as_str(),
            sample.version_id,
            debug_optional_generation_id(sample.generation_id),
            debug_optional_u64(sample.size),
            debug_optional_object_layout(sample.layout),
            sample.last_modified,
            debug_optional_u64(sample.became_noncurrent_at),
        )
        .expect("write to String");
    }
    writeln!(
        &mut body,
        "object_version_sample_errors_count={}",
        snapshot.object_version_sample_errors.len()
    )
    .expect("write to String");
    for (index, error) in snapshot.object_version_sample_errors.iter().enumerate() {
        writeln!(
            &mut body,
            "object_version_sample_error index={} object_pg_id={} detail={}",
            index,
            error.object_pg_id,
            observability::escaped(&error.detail),
        )
        .expect("write to String");
    }

    writeln!(
        &mut body,
        "payload_reclaim_roots_count={}",
        snapshot.payload_reclaim_roots.len()
    )
    .expect("write to String");
    for (index, root) in snapshot.payload_reclaim_roots.iter().enumerate() {
        writeln!(
            &mut body,
            "payload_reclaim_root index={} object_pg_id={} key={:?} generation_id={} reclaim_kind={} reclaim_created_at={} reclaim_item_count={}",
            index,
            root.object_pg_id,
            root.key.as_str(),
            root.generation_id.get(),
            debug_optional_reclaim_kind(root.reclaim_kind),
            debug_optional_u64(root.reclaim_created_at),
            debug_optional_usize(root.reclaim_item_count),
        )
        .expect("write to String");
    }
    writeln!(
        &mut body,
        "payload_reclaim_root_errors_count={}",
        snapshot.payload_reclaim_root_errors.len()
    )
    .expect("write to String");
    for (index, error) in snapshot.payload_reclaim_root_errors.iter().enumerate() {
        writeln!(
            &mut body,
            "payload_reclaim_root_error index={} object_pg_id={} detail={}",
            index,
            error.object_pg_id,
            observability::escaped(&error.detail),
        )
        .expect("write to String");
    }

    writeln!(
        &mut body,
        "payload_reclaim_claims_count={}",
        snapshot.payload_reclaim_claims.len()
    )
    .expect("write to String");
    for (index, claim) in snapshot.payload_reclaim_claims.iter().enumerate() {
        writeln!(
            &mut body,
            "payload_reclaim_claim index={} object_pg_id={} bucket={:?} matches_bucket={} bucket_incarnation_generation={} key={:?} generation_id={} reclaim_kind={} claim_id={:?} cluster_epoch={} claimed_at={} lease_deadline={} attempt_count={} last_error={}",
            index,
            claim.object_pg_id,
            claim.bucket.as_str(),
            claim.matches_bucket,
            claim.bucket_incarnation_generation,
            claim.key.as_str(),
            claim.generation_id.get(),
            debug_optional_reclaim_kind(Some(claim.reclaim_kind)),
            claim.claim_id,
            claim.cluster_epoch.get(),
            claim.claimed_at,
            debug_optional_u64(claim.lease_deadline),
            claim.attempt_count,
            observability::escaped(claim.last_error.as_deref().unwrap_or("")),
        )
        .expect("write to String");
    }
    writeln!(
        &mut body,
        "payload_reclaim_claim_errors_count={}",
        snapshot.payload_reclaim_claim_errors.len()
    )
    .expect("write to String");
    for (index, error) in snapshot.payload_reclaim_claim_errors.iter().enumerate() {
        writeln!(
            &mut body,
            "payload_reclaim_claim_error index={} object_pg_id={} detail={}",
            index,
            error.object_pg_id,
            observability::escaped(&error.detail),
        )
        .expect("write to String");
    }

    match &snapshot.attempt_outcome {
        Some(record) => {
            writeln!(
                &mut body,
                "attempt_outcome=present present=1 drain_id={:?} cluster_epoch={} bucket_execution_generation={} outcome={} phase={} post_reservation_next_object_pg_id={} finalizer_next_object_pg_id={} updated_at={} detail={}",
                record.drain_id,
                record.cluster_epoch.get(),
                record.bucket_execution_generation,
                debug_bucket_delete_attempt_outcome(record.outcome),
                debug_bucket_delete_attempt_phase(record.phase),
                debug_optional_u32(record.post_reservation_next_object_pg_id),
                debug_optional_u32(record.finalizer_next_object_pg_id),
                record.updated_at,
                observability::escaped(&record.detail),
            )
            .expect("write to String");
        }
        None => body.push_str("attempt_outcome=absent present=0\n"),
    }
    body
}

fn debug_bucket_state(state: BucketState) -> &'static str {
    match state {
        BucketState::Active => "active",
        BucketState::Deleting => "deleting",
    }
}

fn debug_optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn debug_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn debug_optional_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn debug_optional_generation_id(value: Option<GenerationId>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.get().to_string())
}

fn debug_object_version_kind(kind: BucketDeleteDebugObjectVersionKind) -> &'static str {
    match kind {
        BucketDeleteDebugObjectVersionKind::Live => "live",
        BucketDeleteDebugObjectVersionKind::DeleteMarker => "delete_marker",
    }
}

fn debug_optional_object_layout(value: Option<ObjectLayout>) -> String {
    match value {
        None => "none".to_string(),
        Some(ObjectLayout::Standard) => "standard".to_string(),
        Some(ObjectLayout::MultipartManifest { parts_count }) => {
            format!("multipart_manifest:{parts_count}")
        }
    }
}

fn debug_optional_reclaim_kind(value: Option<ObjectPayloadReclaimKind>) -> &'static str {
    match value {
        None => "none",
        Some(ObjectPayloadReclaimKind::ObjectSegments) => "object_segments",
        Some(ObjectPayloadReclaimKind::Multipart) => "multipart",
    }
}

fn debug_bucket_delete_attempt_outcome(outcome: BucketDeleteAttemptOutcomeKind) -> &'static str {
    match outcome {
        BucketDeleteAttemptOutcomeKind::Retryable => "retryable",
        BucketDeleteAttemptOutcomeKind::NotEmpty => "not_empty",
        BucketDeleteAttemptOutcomeKind::StaleGeneration => "stale_generation",
        BucketDeleteAttemptOutcomeKind::MarkDeleting => "mark_deleting",
    }
}

fn debug_bucket_delete_attempt_phase(phase: BucketDeleteAttemptPhase) -> &'static str {
    match phase {
        BucketDeleteAttemptPhase::Initial => "initial",
        BucketDeleteAttemptPhase::ReservationWait => "reservation_wait",
        BucketDeleteAttemptPhase::PostReservationObjectDrain => "post_reservation_object_drain",
        BucketDeleteAttemptPhase::StreamCleanup => "stream_cleanup",
        BucketDeleteAttemptPhase::FinalVisibilityCheck => "final_visibility_check",
        BucketDeleteAttemptPhase::FinalVisibilityProven => "final_visibility_proven",
        BucketDeleteAttemptPhase::MarkDeleting => "mark_deleting",
        BucketDeleteAttemptPhase::PostReservationStreamCleanup => "post_reservation_stream_cleanup",
    }
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
    pub fn multipart_upload_id_authority(&self) -> MultipartUploadIdAuthority {
        MultipartUploadIdAuthority::new(self.multipart_upload_id_key.clone())
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
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketSnapshotRequest {
    pub policy: bool,
    pub tags: BucketSnapshotTagsRequest,
    pub lifecycle: bool,
    pub cors: bool,
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
    pub tags: LoadedBucketSubresource<SerializedBucketTagSet>,
    pub lifecycle: LoadedBucketSubresource<String>,
    pub cors: LoadedBucketSubresource<String>,
    bucket_incarnation_read_subject: BucketIncarnationReadSubject,
}

impl BucketSnapshot {
    pub(crate) fn from_authoritative_load(
        bucket: BucketInfo,
        request: BucketSnapshotRequest,
        policy: LoadedBucketSubresource<String>,
        tags: LoadedBucketSubresource<SerializedBucketTagSet>,
        lifecycle: LoadedBucketSubresource<String>,
        cors: LoadedBucketSubresource<String>,
    ) -> Self {
        let bucket_incarnation_read_subject =
            BucketIncarnationReadSubject::from_authoritative_bucket(&bucket);
        Self {
            bucket,
            request,
            policy,
            tags,
            lifecycle,
            cors,
            bucket_incarnation_read_subject,
        }
    }

    #[must_use]
    pub fn bucket_incarnation_read_subject(&self) -> BucketIncarnationReadSubject {
        self.bucket_incarnation_read_subject.clone()
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
#[derive(Clone)]
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
    pub multipart_upload_id_authority: MultipartUploadIdAuthority,
    pub bucket_abac_enabled: bool,
    pub tags: BucketFastPathTags,
    pub encryption: EffectiveBucketEncryptionConfig,
    bucket_incarnation_read_subject: BucketIncarnationReadSubject,
}

/// Durable identity used to prove a bucket fast-path cache entry is current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketFastPathIdentity {
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
}

/// Opaque bucket-incarnation identity used when minting payload-read
/// encryption subjects.
///
/// The value is derived from an authoritative bucket snapshot or validated
/// fast-path entry. Callers can carry it into an admitted object-read route,
/// but cannot assemble a bucket/incarnation pair independently.
#[derive(Clone, PartialEq, Eq)]
pub struct BucketIncarnationReadSubject {
    bucket: BucketName,
    bucket_incarnation_generation: u64,
}

impl BucketIncarnationReadSubject {
    fn from_authoritative_bucket(bucket: &BucketInfo) -> Self {
        Self {
            bucket: bucket.name.clone(),
            bucket_incarnation_generation: bucket.bucket_incarnation_generation,
        }
    }

    pub(crate) fn matches_bucket(&self, bucket: &BucketName) -> bool {
        self.bucket == *bucket
    }

    pub(crate) const fn bucket_incarnation_generation(&self) -> u64 {
        self.bucket_incarnation_generation
    }
}

impl std::fmt::Debug for BucketIncarnationReadSubject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BucketIncarnationReadSubject")
            .finish_non_exhaustive()
    }
}

impl BucketFastPathInfo {
    #[must_use]
    pub const fn identity(&self) -> BucketFastPathIdentity {
        BucketFastPathIdentity {
            bucket_execution_generation: self.bucket_execution_generation,
            bucket_incarnation_generation: self.bucket_incarnation_generation,
        }
    }

    #[must_use]
    pub fn bucket_incarnation_read_subject(&self) -> BucketIncarnationReadSubject {
        self.bucket_incarnation_read_subject.clone()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn cache_test_fixture(name: BucketName) -> Self {
        let bucket_incarnation_read_subject = BucketIncarnationReadSubject {
            bucket: name.clone(),
            bucket_incarnation_generation: 1,
        };
        Self {
            name,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 0,
            state: BucketState::Active,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            policy: BucketFastPathPolicy::Absent,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_execution_generation: 0,
            bucket_incarnation_generation: 1,
            multipart_upload_id_authority: MultipartUploadIdAuthority::for_test(),
            bucket_abac_enabled: false,
            tags: BucketFastPathTags::NotApplicable,
            encryption: EffectiveBucketEncryptionConfig::default(),
            bucket_incarnation_read_subject,
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
    Loaded(SerializedBucketTagSet),
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
            .field(
                "multipart_upload_id_authority",
                &self.multipart_upload_id_authority,
            )
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
            multipart_upload_id_authority: snapshot.bucket.multipart_upload_id_authority(),
            bucket_abac_enabled: snapshot.bucket.bucket_abac_enabled,
            tags: match &snapshot.tags {
                LoadedBucketSubresource::Loaded(tags) => BucketFastPathTags::Loaded(tags.clone()),
                LoadedBucketSubresource::Missing => BucketFastPathTags::Missing,
                LoadedBucketSubresource::NotRequested => BucketFastPathTags::NotApplicable,
            },
            encryption: snapshot.bucket.encryption,
            bucket_incarnation_read_subject: snapshot.bucket_incarnation_read_subject.clone(),
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
#[cfg(test)]
pub(crate) struct CommitMultipartReq {
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
pub(crate) struct DirectPutWrittenSegment {
    pub data_pg_id: u32,
    pub ec: EcShape,
    pub written_shards: Vec<WrittenShardAck>,
}

/// Opaque authority over one direct PutObject payload written through an
/// admitted object route.
///
/// Its lifetime is tied to the exact non-cloneable route admission which
/// issued it. Storage retains that owner together with the subject, placement,
/// bucket incarnation, integrity, and cleanup identity, so callers cannot
/// cross publication domains or combine a payload write with independently
/// constructed physical metadata. An armed handle cleans issuer-owned staging
/// when dropped.
pub struct DirectPutPayloadWrite<'admission> {
    pub(crate) owner: &'admission crate::cluster::StorageClusterRouteAdmission,
    pub(crate) armed: std::cell::Cell<bool>,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) generation_reservation_id: SessionId,
    pub(crate) generation_id: GenerationId,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) encryption_subject: SegmentEncryptionSubject,
    pub(crate) logical_size: u64,
    pub(crate) segment_index: u32,
    pub(crate) segment_crc64: u64,
    pub(crate) segment_okh: [u8; 16],
    pub(crate) segment_vid: GenerationId,
    pub(crate) placement_cluster_epoch: ClusterEpoch,
    pub(crate) written: DirectPutWrittenSegment,
}

impl std::fmt::Debug for DirectPutPayloadWrite<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectPutPayloadWrite")
            .finish_non_exhaustive()
    }
}

/// S3-visible metadata prepared by server-core for a direct PutObject commit.
///
/// The payload subject, generation, layout, placement, and integrity fields
/// are supplied exclusively by [`DirectPutPayloadWrite`].
#[derive(Debug, Clone)]
pub struct PreparedDirectPutObjectCommit {
    pub versioning: BucketVersioningState,
    pub owner: OwnerIdentity,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    /// CRC64-NVME of the user-visible object data.
    pub etag_crc64: u64,
    pub tags: Option<SerializedTagSet>,
    pub metadata_blob: SerializedMetadataBlob,
    pub system_metadata_blob: SerializedSystemMetadataBlob,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
    pub bucket_write_reservation: crate::BucketWriteReservationProof,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitDirectPutObjectReq {
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
    pub object_segments: Vec<ObjectPayloadSegment>,
    pub multipart_parts: Vec<ObjectReadMultipartPart>,
    pub multipart_part_segments: Vec<ObjectPayloadSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ObjectPayloadPlacementDiagnosticError {
    #[error("current object is a delete marker")]
    DeleteMarker,
    #[error("current object has no standard payload segments")]
    NoStandardPayloadSegments,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectPayloadPlacementDiagnosticOutcome {
    Success,
    Mismatch,
    Conflict,
}

/// Bounded outcome selected by the storage-owned metadata-checkpoint
/// diagnostic operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCheckpointDiagnosticOutcome {
    Success,
    InvalidInput,
    Conflict,
}

/// Opaque, storage-rendered object payload-placement diagnostics.
pub struct ObjectPayloadPlacementDiagnostic {
    outcome: ObjectPayloadPlacementDiagnosticOutcome,
    text: String,
}

impl ObjectPayloadPlacementDiagnostic {
    pub(crate) fn success(text: String) -> Self {
        Self {
            outcome: ObjectPayloadPlacementDiagnosticOutcome::Success,
            text,
        }
    }

    pub(crate) fn conflict(text: String) -> Self {
        Self {
            outcome: ObjectPayloadPlacementDiagnosticOutcome::Conflict,
            text,
        }
    }

    pub(crate) fn mismatch(text: String) -> Self {
        Self {
            outcome: ObjectPayloadPlacementDiagnosticOutcome::Mismatch,
            text,
        }
    }

    #[must_use]
    pub fn outcome(&self) -> ObjectPayloadPlacementDiagnosticOutcome {
        self.outcome
    }

    #[must_use]
    pub fn into_text(self) -> String {
        self.text
    }
}

impl std::fmt::Debug for ObjectPayloadPlacementDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectPayloadPlacementDiagnostic")
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

/// Opaque, storage-rendered metadata-checkpoint diagnostics.
pub struct MetadataCheckpointDiagnostic {
    outcome: MetadataCheckpointDiagnosticOutcome,
    text: String,
}

impl MetadataCheckpointDiagnostic {
    pub(crate) fn new(outcome: MetadataCheckpointDiagnosticOutcome, text: String) -> Self {
        Self { outcome, text }
    }

    #[must_use]
    pub fn outcome(&self) -> MetadataCheckpointDiagnosticOutcome {
        self.outcome
    }

    #[must_use]
    pub fn into_text(self) -> String {
        self.text
    }
}

impl std::fmt::Debug for MetadataCheckpointDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataCheckpointDiagnostic")
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

impl ObjectReadSnapshot {
    pub(crate) fn from_records(
        stored: StoredObject,
        object_segments: Vec<ObjectSegmentRecord>,
        multipart_parts: Vec<ObjectPartRecord>,
        multipart_part_segments: Vec<MultipartPartSegmentRecord>,
    ) -> Result<Self, &'static str> {
        let live = stored.as_live();
        if live.is_none()
            && (!object_segments.is_empty()
                || !multipart_parts.is_empty()
                || !multipart_part_segments.is_empty())
        {
            return Err("non-live object snapshot contains payload layout");
        }
        if let Some(live) = live {
            if object_segments.iter().any(|record| {
                record.bucket != live.bucket
                    || record.key != live.key
                    || record.version_id != live.version_id
            }) {
                return Err("object segment does not match snapshot subject");
            }
            if multipart_parts.iter().any(|record| {
                record.bucket != live.bucket
                    || record.key != live.key
                    || record.version_id != live.version_id
            }) {
                return Err("multipart part does not match snapshot subject");
            }
            if multipart_part_segments.iter().any(|record| {
                record.bucket != live.bucket
                    || record.key != live.key
                    || record.version_id != live.version_id.to_u64()
            }) {
                return Err("multipart segment does not match snapshot subject");
            }
        }
        let Some(live) = live else {
            return Ok(Self {
                stored,
                object_segments: Vec::new(),
                multipart_parts: Vec::new(),
                multipart_part_segments: Vec::new(),
            });
        };
        let payload_profile = live.encryption.payload_profile();
        let subject = ObjectPayloadSubject {
            bucket: live.bucket.clone(),
            key: live.key.clone(),
            generation_id: live.generation_id,
        };
        Ok(Self {
            stored,
            object_segments: object_segments
                .into_iter()
                .map(|record| {
                    ObjectPayloadSegment::from_object_record(&subject, record, payload_profile)
                })
                .collect::<Result<Vec<_>, _>>()?,
            multipart_parts: multipart_parts
                .into_iter()
                .map(ObjectReadMultipartPart::from_record)
                .collect(),
            multipart_part_segments: multipart_part_segments
                .into_iter()
                .map(|record| {
                    ObjectPayloadSegment::from_multipart_record(&subject, record, payload_profile)
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub(crate) fn bind_payload_encryption_subjects(
        &mut self,
        bucket_incarnation_generation: u64,
    ) -> Result<(), &'static str> {
        if self
            .object_segments
            .iter()
            .chain(&self.multipart_part_segments)
            .any(|segment| segment.encryption_subject.is_some())
        {
            return Err("object payload encryption subjects are already bound");
        }
        let Some(live) = self.stored.as_live() else {
            return Ok(());
        };
        for segment in &mut self.object_segments {
            if segment.part_number().is_some() {
                return Err("standard object payload contains a multipart segment");
            }
            segment.encryption_subject = Some(Arc::new(SegmentEncryptionSubject::standard_object(
                ObjectEncryptionSubject::standard_object(
                    bucket_incarnation_generation,
                    &live.bucket,
                    &live.key,
                    live.generation_id,
                    &live.encryption,
                ),
                segment.segment_index(),
                segment.size(),
            )));
        }

        let part_generations = self
            .multipart_parts
            .iter()
            .map(|part| (part.record.part_number, part.record.part_vid))
            .collect::<std::collections::BTreeMap<_, _>>();
        if part_generations.len() != self.multipart_parts.len() {
            return Err("multipart snapshot contains duplicate part numbers");
        }
        for segment in &mut self.multipart_part_segments {
            let ObjectPayloadSegmentRecord::Multipart(record) = &segment.record else {
                return Err("multipart payload contains a standard object segment");
            };
            let part_generation_id = part_generations
                .get(&record.part_number)
                .copied()
                .ok_or("multipart segment has no matching part generation")?;
            segment.encryption_subject = Some(Arc::new(SegmentEncryptionSubject::multipart_part(
                ObjectEncryptionSubject::standard_object(
                    bucket_incarnation_generation,
                    &live.bucket,
                    &live.key,
                    live.generation_id,
                    &live.encryption,
                ),
                &record.upload_id,
                record.part_number,
                part_generation_id,
                record.segment_index,
                record.size,
            )));
        }
        Ok(())
    }

    /// Render storage-owned physical placement details for the local debug endpoint.
    ///
    /// This diagnostic text is not a stable wire or persistence format. Keeping the
    /// rendering here prevents physical placement coordinates from becoming part of
    /// the object-read interface used by other crates.
    pub(crate) fn payload_placement_diagnostic(
        &self,
    ) -> Result<String, ObjectPayloadPlacementDiagnosticError> {
        use std::fmt::Write as _;

        let StoredObject::Live(live) = &self.stored else {
            return Err(ObjectPayloadPlacementDiagnosticError::DeleteMarker);
        };
        if self.object_segments.is_empty() {
            return Err(ObjectPayloadPlacementDiagnosticError::NoStandardPayloadSegments);
        }

        let mut diagnostic = String::new();
        writeln!(diagnostic, "generation_id={}", live.generation_id.get())
            .expect("writing to a String cannot fail");
        writeln!(diagnostic, "segment_count={}", self.object_segments.len())
            .expect("writing to a String cannot fail");
        for segment in &self.object_segments {
            writeln!(
                diagnostic,
                "segment_index={} data_pg_id={} placement_cluster_epoch={}",
                segment.segment_index(),
                segment.stored_bytes_request().data_pg_id,
                segment.placement_cluster_epoch().get()
            )
            .expect("writing to a String cannot fail");
        }
        Ok(diagnostic)
    }
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

/// Placement-verifiable evidence that one object-metadata PG contains an
/// aborting multipart upload for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortingMultipartUploadBucketWitness {
    pub bucket: BucketName,
    pub key: ObjectKey,
}

// ── Multipart upload types ─────────────────────────────────────────

/// Zero-sized structural guard carried by multipart authorization candidates and capabilities.
///
/// It deliberately implements neither `Clone` nor `Copy`. Because it is a private field, an
/// accidental derive on an enclosing public capability fails to compile instead of silently
/// turning one authorization decision into reusable authority.
struct LinearMultipartCapabilityMarker;

/// Multipart upload state machine.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadState {
    InProgress = 0,
    Completing = 1,
    Aborting = 2,
}

/// Identity of the current object observed when a multipart upload is
/// initiated or completed. Conditional completion uses this to distinguish a
/// stable current object from an intervening write even when the current ETag
/// would otherwise satisfy the request condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultipartObjectIdentity {
    Live {
        version_id: VersionId,
        generation_id: GenerationId,
    },
    DeleteMarker {
        version_id: VersionId,
        write_sequence: u64,
    },
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
pub(crate) struct MultipartUploadRecord {
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
    /// Current object identity at CreateMultipartUpload linearization.
    pub initiated_object_identity: Option<MultipartObjectIdentity>,
    /// Pending Object Lock state to apply to the committed object version.
    pub object_lock: ObjectLockState,
    /// Validated checksum configuration for this upload.
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
}

/// Logical multipart-upload state used by lifecycle policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartLifecycleUpload {
    key: ObjectKey,
    upload_id: UploadId,
    initiated_at: u64,
    state: UploadState,
}

impl MultipartLifecycleUpload {
    pub(crate) fn from_record(upload: MultipartUploadRecord) -> Self {
        Self {
            key: upload.key,
            upload_id: upload.upload_id,
            initiated_at: upload.initiated_at,
            state: upload.state,
        }
    }

    #[must_use]
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }

    #[must_use]
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    #[must_use]
    pub fn initiated_at(&self) -> u64 {
        self.initiated_at
    }

    #[must_use]
    pub fn state(&self) -> UploadState {
        self.state
    }

    #[must_use]
    pub fn into_key_and_upload_id(self) -> (ObjectKey, UploadId) {
        (self.key, self.upload_id)
    }
}

/// Multipart upload record that has already passed caller authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthorizedMultipartUploadRecord {
    upload: MultipartUploadRecord,
}

impl AuthorizedMultipartUploadRecord {
    pub(crate) fn assume_authorized(upload: MultipartUploadRecord) -> Self {
        Self { upload }
    }

    pub(crate) fn record(&self) -> &MultipartUploadRecord {
        &self.upload
    }
}

impl std::ops::Deref for AuthorizedMultipartUploadRecord {
    type Target = MultipartUploadRecord;

    fn deref(&self) -> &Self::Target {
        &self.upload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MultipartUploadManagementLookup {
    InProgress(Box<MultipartUploadRecord>),
    NonInProgress(Box<MultipartUploadRecord>),
    Replay(Box<MultipartCompletionReplay>),
    Missing,
}

/// Opaque multipart upload identity used while authorizing an abort request.
///
/// Storage retains the complete durable upload record. Higher layers may inspect only the
/// logical ownership identities needed for authorization, then consume an in-progress candidate
/// into the capability required by the storage-owned abort mutation.
///
/// ```compile_fail
/// use storage::MultipartUploadAbortCandidate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<MultipartUploadAbortCandidate>();
/// ```
pub struct MultipartUploadAbortCandidate(MultipartUploadRecord, LinearMultipartCapabilityMarker);

impl MultipartUploadAbortCandidate {
    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.0.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.0.initiator
    }

    #[must_use]
    pub fn into_authorized_abort(self) -> AuthorizedMultipartUploadAbort {
        AuthorizedMultipartUploadAbort::assume_authorized(self.0)
    }
}

impl std::fmt::Debug for MultipartUploadAbortCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartUploadAbortCandidate")
            .field("upload_id", &self.0.upload_id)
            .finish_non_exhaustive()
    }
}

/// Logical ownership identity for multipart-upload management authorization.
///
/// This projection supports authorization of a terminal upload without retaining either its
/// durable record or a path to an operation capability.
#[derive(Debug)]
pub struct MultipartUploadAuthorizationIdentity {
    owner: OwnerIdentity,
    initiator: OwnerIdentity,
}

impl MultipartUploadAuthorizationIdentity {
    fn from_upload(upload: MultipartUploadRecord) -> Self {
        Self {
            owner: upload.owner,
            initiator: upload.initiator,
        }
    }

    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.initiator
    }
}

/// Opaque capability authorizing mutation of one exact in-progress upload by abort.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartUploadAbort;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartUploadAbort>();
/// ```
pub struct AuthorizedMultipartUploadAbort(MultipartUploadRecord, LinearMultipartCapabilityMarker);

impl AuthorizedMultipartUploadAbort {
    pub(crate) fn assume_authorized(upload: MultipartUploadRecord) -> Self {
        Self(upload, LinearMultipartCapabilityMarker)
    }

    pub(crate) fn record(&self) -> &MultipartUploadRecord {
        &self.0
    }

    #[must_use]
    pub fn upload_id(&self) -> &UploadId {
        &self.0.upload_id
    }
}

impl std::fmt::Debug for AuthorizedMultipartUploadAbort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartUploadAbort")
            .field("upload_id", &self.0.upload_id)
            .finish_non_exhaustive()
    }
}

/// Storage-owned logical classification for authorizing AbortMultipartUpload.
#[derive(Debug)]
pub enum MultipartUploadAbortLookup {
    InProgress(Box<MultipartUploadAbortCandidate>),
    NonInProgress(MultipartUploadAuthorizationIdentity),
    Replay(UploadId),
    Missing,
}

impl MultipartUploadAbortLookup {
    pub(crate) fn from_management_lookup(lookup: MultipartUploadManagementLookup) -> Self {
        match lookup {
            MultipartUploadManagementLookup::InProgress(upload) => Self::InProgress(Box::new(
                MultipartUploadAbortCandidate(*upload, LinearMultipartCapabilityMarker),
            )),
            MultipartUploadManagementLookup::NonInProgress(upload) => {
                Self::NonInProgress(MultipartUploadAuthorizationIdentity::from_upload(*upload))
            }
            MultipartUploadManagementLookup::Replay(replay) => Self::Replay(replay.upload_id),
            MultipartUploadManagementLookup::Missing => Self::Missing,
        }
    }
}

/// Opaque active-upload identity used while authorizing ListParts.
///
/// Storage retains the complete durable upload record. Higher layers may inspect only the
/// logical ownership identities needed for authorization, then consume the candidate into the
/// capability required by the storage-owned listing operation.
///
/// ```compile_fail
/// use storage::MultipartUploadListPartsCandidate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<MultipartUploadListPartsCandidate>();
/// ```
pub struct MultipartUploadListPartsCandidate(
    MultipartUploadRecord,
    LinearMultipartCapabilityMarker,
);

impl MultipartUploadListPartsCandidate {
    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.0.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.0.initiator
    }

    #[must_use]
    pub fn into_authorized_list_parts(self) -> AuthorizedMultipartUploadListParts {
        AuthorizedMultipartUploadListParts::assume_authorized(self.0)
    }
}

impl std::fmt::Debug for MultipartUploadListPartsCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartUploadListPartsCandidate")
            .finish_non_exhaustive()
    }
}

/// Opaque capability authorizing ListParts for one exact in-progress upload.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartUploadListParts;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartUploadListParts>();
/// ```
pub struct AuthorizedMultipartUploadListParts(
    MultipartUploadRecord,
    LinearMultipartCapabilityMarker,
);

impl AuthorizedMultipartUploadListParts {
    pub(crate) fn assume_authorized(upload: MultipartUploadRecord) -> Self {
        Self(upload, LinearMultipartCapabilityMarker)
    }

    pub(crate) fn record(&self) -> &MultipartUploadRecord {
        &self.0
    }
}

impl std::fmt::Debug for AuthorizedMultipartUploadListParts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartUploadListParts")
            .finish_non_exhaustive()
    }
}

/// Storage-owned logical classification for authorizing ListParts.
#[derive(Debug)]
pub enum MultipartUploadListPartsLookup {
    InProgress(Box<MultipartUploadListPartsCandidate>),
    NonInProgress(MultipartUploadAuthorizationIdentity),
    Replay(UploadId),
    Missing,
}

impl MultipartUploadListPartsLookup {
    pub(crate) fn from_management_lookup(lookup: MultipartUploadManagementLookup) -> Self {
        match lookup {
            MultipartUploadManagementLookup::InProgress(upload) => Self::InProgress(Box::new(
                MultipartUploadListPartsCandidate(*upload, LinearMultipartCapabilityMarker),
            )),
            MultipartUploadManagementLookup::NonInProgress(upload) => {
                Self::NonInProgress(MultipartUploadAuthorizationIdentity::from_upload(*upload))
            }
            MultipartUploadManagementLookup::Replay(replay) => Self::Replay(replay.upload_id),
            MultipartUploadManagementLookup::Missing => Self::Missing,
        }
    }
}

/// Opaque in-progress upload used while authorizing UploadPart and UploadPartCopy.
///
/// Storage retains the complete durable upload record. Higher layers may inspect only the
/// logical ownership, object key, checksum configuration, and encryption state required by the
/// S3 authorization and encryption layers, then consume the candidate into the capability
/// required to create the storage-owned part stream session.
///
/// ```compile_fail
/// use storage::MultipartUploadPartCandidate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<MultipartUploadPartCandidate>();
/// ```
pub struct MultipartUploadPartCandidate(MultipartUploadRecord, LinearMultipartCapabilityMarker);

impl MultipartUploadPartCandidate {
    pub(crate) fn from_record(upload: MultipartUploadRecord) -> Self {
        Self(upload, LinearMultipartCapabilityMarker)
    }

    #[must_use]
    pub fn key(&self) -> &ObjectKey {
        &self.0.key
    }

    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.0.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.0.initiator
    }

    #[must_use]
    pub fn checksum_config(&self) -> Option<MultipartChecksumConfig> {
        self.0.checksum
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.0.encryption
    }

    #[must_use]
    pub fn into_authorized_part(self, part_number: u32) -> AuthorizedMultipartUploadPart {
        AuthorizedMultipartUploadPart::assume_authorized(self.0, part_number)
    }
}

impl std::fmt::Debug for MultipartUploadPartCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartUploadPartCandidate")
            .finish_non_exhaustive()
    }
}

/// Opaque capability authorizing stream-session creation for one exact in-progress upload.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartUploadPart;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartUploadPart>();
/// ```
pub struct AuthorizedMultipartUploadPart {
    upload: MultipartUploadRecord,
    part_number: u32,
    _linear: LinearMultipartCapabilityMarker,
}

impl AuthorizedMultipartUploadPart {
    pub(crate) fn assume_authorized(upload: MultipartUploadRecord, part_number: u32) -> Self {
        Self {
            upload,
            part_number,
            _linear: LinearMultipartCapabilityMarker,
        }
    }

    pub(crate) fn record(&self) -> &MultipartUploadRecord {
        &self.upload
    }

    pub(crate) fn part_number(&self) -> u32 {
        self.part_number
    }
}

impl std::fmt::Debug for AuthorizedMultipartUploadPart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartUploadPart")
            .finish_non_exhaustive()
    }
}

/// Logical S3 state needed after an in-progress multipart completion is authorized.
pub struct MultipartUploadCompletionContext {
    checksum: Option<MultipartChecksumConfig>,
    system_metadata_blob: SerializedSystemMetadataBlob,
    object_lock: ObjectLockState,
}

impl std::fmt::Debug for MultipartUploadCompletionContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartUploadCompletionContext")
            .field("checksum", &self.checksum)
            .field("object_lock", &self.object_lock)
            .finish_non_exhaustive()
    }
}

impl MultipartUploadCompletionContext {
    #[must_use]
    pub fn checksum_config(&self) -> Option<MultipartChecksumConfig> {
        self.checksum
    }

    #[must_use]
    pub fn system_metadata_blob(&self) -> &SerializedSystemMetadataBlob {
        &self.system_metadata_blob
    }

    #[must_use]
    pub fn object_lock(&self) -> ObjectLockState {
        self.object_lock
    }
}

/// Opaque in-progress upload used while authorizing CompleteMultipartUpload.
///
/// ```compile_fail
/// use storage::MultipartUploadCompletionCandidate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<MultipartUploadCompletionCandidate>();
/// ```
pub struct MultipartUploadCompletionCandidate(
    MultipartUploadRecord,
    LinearMultipartCapabilityMarker,
);

impl MultipartUploadCompletionCandidate {
    pub(crate) fn from_record(upload: MultipartUploadRecord) -> Self {
        Self(upload, LinearMultipartCapabilityMarker)
    }

    #[must_use]
    pub fn key(&self) -> &ObjectKey {
        &self.0.key
    }

    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.0.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.0.initiator
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.0.encryption
    }

    #[must_use]
    pub fn into_authorized_completion(
        self,
    ) -> (
        AuthorizedMultipartUploadCompletion,
        MultipartUploadCompletionContext,
    ) {
        let context = MultipartUploadCompletionContext {
            checksum: self.0.checksum,
            system_metadata_blob: self.0.system_metadata_blob.clone(),
            object_lock: self.0.object_lock,
        };
        (
            AuthorizedMultipartUploadCompletion::assume_authorized(self.0),
            context,
        )
    }
}

impl std::fmt::Debug for MultipartUploadCompletionCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartUploadCompletionCandidate")
            .finish_non_exhaustive()
    }
}

/// Linear capability authorizing snapshot acquisition for one exact in-progress upload.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartUploadCompletion;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartUploadCompletion>();
/// ```
pub struct AuthorizedMultipartUploadCompletion(
    MultipartUploadRecord,
    LinearMultipartCapabilityMarker,
);

impl AuthorizedMultipartUploadCompletion {
    pub(crate) fn assume_authorized(upload: MultipartUploadRecord) -> Self {
        Self(upload, LinearMultipartCapabilityMarker)
    }

    pub(crate) fn into_record(self) -> MultipartUploadRecord {
        self.0
    }
}

impl std::fmt::Debug for AuthorizedMultipartUploadCompletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartUploadCompletion")
            .finish_non_exhaustive()
    }
}

/// Opaque completed-upload replay state used while authorizing terminal replay.
///
/// ```compile_fail
/// use storage::MultipartCompletionReplayCandidate;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<MultipartCompletionReplayCandidate>();
/// ```
pub struct MultipartCompletionReplayCandidate(
    MultipartCompletionReplay,
    LinearMultipartCapabilityMarker,
);

impl MultipartCompletionReplayCandidate {
    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.0.encryption
    }

    #[must_use]
    pub fn into_authorized_replay(self) -> AuthorizedMultipartCompletionReplay {
        let replay = self.0;
        AuthorizedMultipartCompletionReplay {
            upload_id: replay.upload_id,
            fingerprint: replay.fingerprint,
            version_id: replay.version_id,
            etag: replay.etag,
            size: replay.size,
            last_modified: replay.last_modified,
            tags: replay.tags.map(|tags| (*tags).clone()),
            encryption: replay.encryption,
            _linear: LinearMultipartCapabilityMarker,
        }
    }
}

impl std::fmt::Debug for MultipartCompletionReplayCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartCompletionReplayCandidate")
            .finish_non_exhaustive()
    }
}

/// Logical replay projection returned after CompleteMultipartUpload authorization.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartCompletionReplay;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartCompletionReplay>();
/// ```
pub struct AuthorizedMultipartCompletionReplay {
    upload_id: UploadId,
    fingerprint: MultipartCompletionFingerprint,
    version_id: VersionId,
    etag: ObjectEtag,
    size: u64,
    last_modified: u64,
    tags: Option<s3_types::TagSet>,
    encryption: ObjectEncryption,
    _linear: LinearMultipartCapabilityMarker,
}

impl AuthorizedMultipartCompletionReplay {
    #[must_use]
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    #[must_use]
    pub fn fingerprint(&self) -> MultipartCompletionFingerprint {
        self.fingerprint
    }

    #[must_use]
    pub fn version_id(&self) -> VersionId {
        self.version_id
    }

    #[must_use]
    pub fn etag(&self) -> ObjectEtag {
        self.etag
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[must_use]
    pub fn last_modified(&self) -> u64 {
        self.last_modified
    }

    #[must_use]
    pub fn tags(&self) -> Option<&s3_types::TagSet> {
        self.tags.as_ref()
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.encryption
    }
}

impl std::fmt::Debug for AuthorizedMultipartCompletionReplay {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartCompletionReplay")
            .finish_non_exhaustive()
    }
}

/// Storage-owned logical classification for CompleteMultipartUpload authorization.
#[derive(Debug)]
pub enum MultipartUploadCompletionLookup {
    InProgress(Box<MultipartUploadCompletionCandidate>),
    Replay(Box<MultipartCompletionReplayCandidate>),
    Unavailable,
}

impl MultipartUploadCompletionLookup {
    pub(crate) fn from_management_lookup(lookup: MultipartUploadManagementLookup) -> Self {
        match lookup {
            MultipartUploadManagementLookup::InProgress(upload) => Self::InProgress(Box::new(
                MultipartUploadCompletionCandidate::from_record(*upload),
            )),
            MultipartUploadManagementLookup::Replay(replay) => Self::Replay(Box::new(
                MultipartCompletionReplayCandidate(*replay, LinearMultipartCapabilityMarker),
            )),
            MultipartUploadManagementLookup::NonInProgress(_)
            | MultipartUploadManagementLookup::Missing => Self::Unavailable,
        }
    }

    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn from_in_progress(upload: MultipartUploadCompletionCandidate) -> Self {
        Self::InProgress(Box::new(upload))
    }
}

/// In-progress multipart part record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartPartRecord {
    pub upload_id: UploadId,
    pub part_number: u32,
    pub generation: u32,
    pub size: u64,
    /// CRC64-NVME over the logical part bytes used for internal storage repair.
    pub payload_crc64: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
    /// Stable identity for this replacement generation of the part.
    pub part_vid: GenerationId,
    /// Cluster-map epoch recorded when this part generation was committed.
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
    /// Last modified timestamp (unix milliseconds).
    pub last_modified: u64,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<ChecksumBytes>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MultipartCompletionSubject {
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
    generation_id: GenerationId,
}

impl MultipartCompletionSubject {
    pub(crate) fn new(
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
        generation_id: GenerationId,
    ) -> Self {
        Self {
            bucket,
            key,
            upload_id,
            generation_id,
        }
    }

    pub(crate) fn from_upload(upload: &MultipartUploadRecord) -> Self {
        Self::new(
            upload.bucket.clone(),
            upload.key.clone(),
            upload.upload_id.clone(),
            upload.object_generation_id,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MultipartCompletionSnapshot {
    subject: MultipartCompletionSubject,
    pub(crate) existing_etag: Option<String>,
    pub(crate) current_object_identity: Option<MultipartObjectIdentity>,
    pub(crate) stale_payload_source: Option<StoredObject>,
    pub(crate) part_records: Vec<MultipartPartRecord>,
    pub(crate) selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) cleanup: CompleteMultipartCommitCleanup,
    logical_parts: Vec<MultipartCompletionPart>,
}

impl std::fmt::Debug for MultipartCompletionSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartCompletionSnapshot")
            .field("existing_etag", &self.existing_etag)
            .field("parts", &self.logical_parts)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartCompletionPart {
    pub part_number: u32,
    pub size: u64,
    /// CRC64-NVME over the logical part bytes.
    pub payload_crc64: u64,
    pub etag: Vec<u8>,
    pub checksum: Option<ChecksumBytes>,
}

impl MultipartCompletionSnapshot {
    pub(crate) fn from_storage(
        subject: MultipartCompletionSubject,
        existing_etag: Option<String>,
        current_object_identity: Option<MultipartObjectIdentity>,
        stale_payload_source: Option<StoredObject>,
        part_records: Vec<MultipartPartRecord>,
        selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
        cleanup: CompleteMultipartCommitCleanup,
    ) -> Self {
        let logical_parts = part_records
            .iter()
            .map(|part| MultipartCompletionPart {
                part_number: part.part_number,
                size: part.size,
                payload_crc64: part.payload_crc64,
                etag: part.etag.clone(),
                checksum: part.checksum.clone(),
            })
            .collect();
        Self {
            subject,
            existing_etag,
            current_object_identity,
            stale_payload_source,
            part_records,
            selected_streaming_segments,
            cleanup,
            logical_parts,
        }
    }

    #[must_use]
    pub fn existing_etag(&self) -> Option<&str> {
        self.existing_etag.as_deref()
    }

    #[must_use]
    pub fn parts(&self) -> &[MultipartCompletionPart] {
        &self.logical_parts
    }

    #[cfg(test)]
    pub(crate) fn test_subject(&self) -> (&BucketName, &ObjectKey, &UploadId, GenerationId) {
        (
            &self.subject.bucket,
            &self.subject.key,
            &self.subject.upload_id,
            self.subject.generation_id,
        )
    }

    #[must_use]
    fn into_commit_request_with_defaults(
        self,
        input: CompleteMultipartCommitInput,
        defaults: MultipartCompletionCommitDefaults,
    ) -> CompleteMultipartCommitRequest {
        CompleteMultipartCommitRequest {
            bucket: self.subject.bucket,
            key: self.subject.key,
            upload_id: self.subject.upload_id,
            completion_fingerprint: input.completion_fingerprint,
            versioning: input.versioning,
            owner: defaults.owner,
            acl_grants: defaults.acl_grants,
            public_read: defaults.public_read,
            generation_id: self.subject.generation_id,
            size: input.size,
            etag_crc64: input.etag_crc64,
            tags: defaults.tags,
            metadata_blob: Some(defaults.metadata_blob),
            system_metadata_blob: input.system_metadata_blob,
            object_lock: input.object_lock,
            encryption: input.encryption,
            expected_stale_payload_source: self.stale_payload_source,
            expected_current_object_identity: self.current_object_identity,
            conditional_completion: input.conditional_completion,
            part_records: self.part_records,
            selected_streaming_segments: self.selected_streaming_segments,
            expected_cleanup: self.cleanup,
        }
    }
}

struct MultipartCompletionCommitDefaults {
    owner: OwnerIdentity,
    acl_grants: AclGrants,
    public_read: bool,
    tags: Option<SerializedTagSet>,
    metadata_blob: SerializedMetadataBlob,
}

/// Completion snapshot bound to the exact upload authorized by the caller.
///
/// ```compile_fail
/// use storage::AuthorizedMultipartCompletionSnapshot;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<AuthorizedMultipartCompletionSnapshot>();
/// ```
pub struct AuthorizedMultipartCompletionSnapshot {
    snapshot: MultipartCompletionSnapshot,
    defaults: MultipartCompletionCommitDefaults,
    _linear: LinearMultipartCapabilityMarker,
}

impl AuthorizedMultipartCompletionSnapshot {
    pub(crate) fn new(
        snapshot: MultipartCompletionSnapshot,
        upload: MultipartUploadRecord,
    ) -> Self {
        Self {
            snapshot,
            defaults: MultipartCompletionCommitDefaults {
                owner: upload.owner,
                acl_grants: upload.acl_grants,
                public_read: upload.public_read,
                tags: upload.tags,
                metadata_blob: upload.metadata_blob,
            },
            _linear: LinearMultipartCapabilityMarker,
        }
    }

    #[must_use]
    pub fn existing_etag(&self) -> Option<&str> {
        self.snapshot.existing_etag()
    }

    #[must_use]
    pub fn parts(&self) -> &[MultipartCompletionPart] {
        self.snapshot.parts()
    }

    #[must_use]
    pub fn into_commit_request(
        self,
        input: CompleteMultipartCommitInput,
    ) -> CompleteMultipartCommitRequest {
        self.snapshot
            .into_commit_request_with_defaults(input, self.defaults)
    }
}

impl std::fmt::Debug for AuthorizedMultipartCompletionSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMultipartCompletionSnapshot")
            .field("existing_etag", &self.existing_etag())
            .field("parts", &self.parts())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartCompletionPreflight {
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
pub(crate) struct MultipartCompletionReplay {
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
pub struct CompleteMultipartCommitInput {
    pub completion_fingerprint: MultipartCompletionFingerprint,
    pub versioning: BucketVersioningState,
    pub size: u64,
    pub etag_crc64: [u8; 8],
    pub system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    pub object_lock: ObjectLockState,
    pub encryption: ObjectEncryption,
    pub conditional_completion: bool,
}

#[derive(Clone)]
pub struct CompleteMultipartCommitRequest {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) upload_id: UploadId,
    pub(crate) completion_fingerprint: MultipartCompletionFingerprint,
    pub(crate) versioning: BucketVersioningState,
    pub(crate) owner: OwnerIdentity,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) generation_id: GenerationId,
    pub(crate) size: u64,
    pub(crate) etag_crc64: [u8; 8],
    pub(crate) tags: Option<SerializedTagSet>,
    pub(crate) metadata_blob: Option<SerializedMetadataBlob>,
    pub(crate) system_metadata_blob: Option<SerializedSystemMetadataBlob>,
    pub(crate) object_lock: ObjectLockState,
    pub(crate) encryption: ObjectEncryption,
    pub(crate) expected_stale_payload_source: Option<StoredObject>,
    pub(crate) expected_current_object_identity: Option<MultipartObjectIdentity>,
    pub(crate) conditional_completion: bool,
    pub(crate) part_records: Vec<MultipartPartRecord>,
    pub(crate) selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) expected_cleanup: CompleteMultipartCommitCleanup,
}

impl std::fmt::Debug for CompleteMultipartCommitRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompleteMultipartCommitRequest")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &self.upload_id)
            .field("size", &self.size)
            .field("conditional_completion", &self.conditional_completion)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CompleteMultipartCommitCleanup {
    pub(crate) omitted_parts: Vec<MultipartPartRecord>,
    pub(crate) omitted_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) stream_uploads: Vec<TerminalStreamCleanupRecord>,
    pub(crate) stream_upload_segments: Vec<StreamUploadSegmentRecord>,
}

#[derive(Clone)]
pub struct CompleteMultipartCommitOutcome {
    pub(crate) version_id: VersionId,
    pub(crate) stale_payload_generation_id: Option<GenerationId>,
    pub(crate) live_tags: Option<SerializedTagSet>,
    pub(crate) live_size: u64,
    pub(crate) live_last_modified: u64,
}

impl CompleteMultipartCommitOutcome {
    #[must_use]
    pub fn version_id(&self) -> VersionId {
        self.version_id
    }

    #[must_use]
    pub fn stale_payload_generation_id(&self) -> Option<GenerationId> {
        self.stale_payload_generation_id
    }

    #[must_use]
    pub fn live_tags(&self) -> Option<&s3_types::TagSet> {
        self.live_tags.as_deref()
    }

    #[must_use]
    pub fn live_size(&self) -> u64 {
        self.live_size
    }

    #[must_use]
    pub fn live_last_modified(&self) -> u64 {
        self.live_last_modified
    }
}

impl std::fmt::Debug for CompleteMultipartCommitOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let live_tag_count = self.live_tags.as_ref().map(|tags| tags.len());
        formatter
            .debug_struct("CompleteMultipartCommitOutcome")
            .field("version_id", &self.version_id)
            .field(
                "stale_payload_generation_id",
                &self.stale_payload_generation_id(),
            )
            .field("live_tag_count", &live_tag_count)
            .field("live_size", &self.live_size)
            .field("live_last_modified", &self.live_last_modified)
            .finish()
    }
}

/// Committed part record in the object manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectPartRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub part_number: u32,
    pub size: u64,
    /// CRC64-NVME over the logical part bytes used for internal storage repair.
    pub payload_crc64: u64,
    pub etag: Vec<u8>,
    pub etag_kind: EtagKind,
    pub part_vid: GenerationId,
    pub placement_cluster_epoch: ClusterEpoch,
    pub ec_k: u8,
    pub ec_m: u8,
    /// PG where this part's shards are stored.
    pub data_pg_id: u32,
    /// Raw checksum bytes for this part (None if no checksum).
    pub checksum: Option<ChecksumBytes>,
}

/// Logical multipart-manifest part exposed by an object-read snapshot.
///
/// Storage retains the complete durable row, including placement and encoding
/// details, for RPC validation and retained payload authority. Callers can
/// inspect only the values required to implement S3 read semantics.
#[derive(Clone)]
pub struct ObjectReadMultipartPart {
    record: ObjectPartRecord,
}

impl ObjectReadMultipartPart {
    pub(crate) fn from_record(record: ObjectPartRecord) -> Self {
        Self { record }
    }

    pub(crate) fn record(&self) -> &ObjectPartRecord {
        &self.record
    }

    #[must_use]
    pub fn part_number(&self) -> u32 {
        self.record.part_number
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        self.record.size
    }

    #[must_use]
    pub fn payload_crc64(&self) -> u64 {
        self.record.payload_crc64
    }

    #[must_use]
    pub fn checksum(&self) -> Option<&ChecksumBytes> {
        self.record.checksum.as_ref()
    }
}

impl std::fmt::Debug for ObjectReadMultipartPart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectReadMultipartPart")
            .field("part_number", &self.part_number())
            .field("size", &self.size())
            .field("payload_crc64", &self.payload_crc64())
            .field("has_checksum", &self.checksum().is_some())
            .finish()
    }
}

impl PartialEq for ObjectReadMultipartPart {
    fn eq(&self, other: &Self) -> bool {
        self.part_number() == other.part_number()
            && self.size() == other.size()
            && self.payload_crc64() == other.payload_crc64()
            && self.checksum() == other.checksum()
    }
}

impl Eq for ObjectReadMultipartPart {}

/// Committed part record annotated with its byte offset in the completed object.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ObjectPartRangeRecord {
    pub part: ObjectPartRecord,
    pub object_offset_start: u64,
}

/// Logical authorized input for creating a multipart upload.
///
/// Bucket and key are derived from the admitted storage route rather than
/// supplied by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMultipartUploadInput {
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

/// Private storage command input for creating a multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateMultipartUploadReq {
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
    pub object_lock: ObjectLockState,
    pub checksum: Option<MultipartChecksumConfig>,
    pub encryption: ObjectEncryption,
}

impl CreateMultipartUploadReq {
    pub(crate) fn from_authorized_input(
        upload_id: UploadId,
        bucket: BucketName,
        key: ObjectKey,
        input: CreateMultipartUploadInput,
    ) -> Self {
        Self {
            upload_id,
            bucket,
            key,
            tags: input.tags,
            metadata_blob: input.metadata_blob,
            system_metadata_blob: input.system_metadata_blob,
            initiator: input.initiator,
            owner: input.owner,
            acl_grants: input.acl_grants,
            public_read: input.public_read,
            object_lock: input.object_lock,
            checksum: input.checksum,
            encryption: input.encryption,
        }
    }
}

#[derive(Debug)]
pub struct CreateMultipartUploadOutcome<T> {
    pub value: T,
    pub upload_id: UploadId,
    pub initiated_at: u64,
}

/// Request to list multipart uploads.
pub(crate) struct ListMultipartUploadsReq {
    pub bucket: BucketName,
    pub prefix: Option<ObjectKey>,
    pub page_start: Option<ListMultipartUploadsPageStart>,
    pub max_uploads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ListMultipartUploadsPageStart {
    After {
        key_marker: ObjectKey,
        upload_id_marker: Option<UploadId>,
    },
    /// Internal inclusive page start used to jump over a common prefix.
    At(ObjectKey),
}

/// Response from listing multipart uploads.
pub(crate) struct ListMultipartUploadsResp {
    pub uploads: Vec<MultipartUploadRecord>,
    pub is_truncated: bool,
    pub next_key_marker: Option<ObjectKey>,
    pub next_upload_id_marker: Option<UploadId>,
}

/// Logical S3 fields for one listed multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedMultipartUpload {
    pub key: ObjectKey,
    pub upload_id: UploadId,
    pub initiated_at: u64,
    pub owner: OwnerIdentity,
    pub initiator: OwnerIdentity,
    pub checksum: Option<MultipartChecksumConfig>,
}

/// Opaque storage-owned result of listing multipart uploads for a bucket.
///
/// Complete durable multipart-upload records remain private to storage.
#[derive(Clone)]
pub struct ListedBucketMultipartUploads {
    pub(crate) uploads: Vec<ListedMultipartUpload>,
    pub(crate) common_prefixes: Vec<ObjectKey>,
    pub(crate) is_truncated: bool,
    pub(crate) next_marker: Option<MultipartUploadListMarker>,
}

impl ListedBucketMultipartUploads {
    pub(crate) fn from_storage(
        uploads: Vec<MultipartUploadRecord>,
        common_prefixes: Vec<ObjectKey>,
        is_truncated: bool,
        next_marker: Option<MultipartUploadListMarker>,
    ) -> Self {
        let uploads = uploads
            .into_iter()
            .map(|upload| ListedMultipartUpload {
                key: upload.key,
                upload_id: upload.upload_id,
                initiated_at: upload.initiated_at,
                owner: upload.owner,
                initiator: upload.initiator,
                checksum: upload.checksum,
            })
            .collect();
        Self {
            uploads,
            common_prefixes,
            is_truncated,
            next_marker,
        }
    }

    #[must_use]
    pub fn uploads(&self) -> &[ListedMultipartUpload] {
        &self.uploads
    }

    #[must_use]
    pub fn common_prefixes(&self) -> &[ObjectKey] {
        &self.common_prefixes
    }

    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.is_truncated
    }

    #[must_use]
    pub fn next_marker(&self) -> Option<&MultipartUploadListMarker> {
        self.next_marker.as_ref()
    }
}

impl std::fmt::Debug for ListedBucketMultipartUploads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListedBucketMultipartUploads")
            .field("uploads", &self.uploads)
            .field("common_prefixes", &self.common_prefixes)
            .field("is_truncated", &self.is_truncated)
            .field("next_marker", &self.next_marker)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartUploadListMarker {
    Upload { key: ObjectKey, upload_id: UploadId },
    CommonPrefix(ObjectKey),
}

/// Request to list parts of a multipart upload.
pub(crate) struct ListPartsReq {
    pub upload_id: UploadId,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Response from listing parts of a multipart upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListPartsResp {
    pub parts: Vec<MultipartPartRecord>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
}

/// Logical S3 fields for one listed multipart part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedMultipartPart {
    pub part_number: u32,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub checksum: Option<ChecksumBytes>,
}

/// Opaque storage-owned result of listing parts for an authorized upload.
///
/// Durable upload and part records remain private to storage. Callers receive
/// only the logical fields needed to render the S3 ListParts response.
#[derive(Clone)]
pub struct ListedMultipartParts {
    pub(crate) upload: MultipartUploadRecord,
    pub(crate) response: ListPartsResp,
    parts: Vec<ListedMultipartPart>,
}

impl ListedMultipartParts {
    pub(crate) fn from_storage(
        upload: MultipartUploadRecord,
        response: ListPartsResp,
    ) -> Result<Self, &'static str> {
        let parts = response
            .parts
            .iter()
            .map(|part| {
                let etag = ObjectEtag::from_parts(&part.etag, part.etag_kind, None)?.format();
                Ok(ListedMultipartPart {
                    part_number: part.part_number,
                    size: part.size,
                    etag,
                    last_modified: part.last_modified,
                    checksum: part.checksum.clone(),
                })
            })
            .collect::<Result<Vec<_>, &'static str>>()?;
        Ok(Self {
            upload,
            response,
            parts,
        })
    }

    #[must_use]
    pub fn parts(&self) -> &[ListedMultipartPart] {
        &self.parts
    }

    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.response.is_truncated
    }

    #[must_use]
    pub fn next_part_number_marker(&self) -> Option<u32> {
        self.response.next_part_number_marker
    }

    #[must_use]
    pub fn owner(&self) -> &OwnerIdentity {
        &self.upload.owner
    }

    #[must_use]
    pub fn initiator(&self) -> &OwnerIdentity {
        &self.upload.initiator
    }

    #[must_use]
    pub fn checksum_config(&self) -> Option<MultipartChecksumConfig> {
        self.upload.checksum
    }

    #[must_use]
    pub fn key(&self) -> &ObjectKey {
        &self.upload.key
    }

    #[must_use]
    pub fn initiated_at(&self) -> u64 {
        self.upload.initiated_at
    }
}

impl std::fmt::Debug for ListedMultipartParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListedMultipartParts")
            .field("parts", &self.parts)
            .field("is_truncated", &self.is_truncated())
            .field("next_part_number_marker", &self.next_part_number_marker())
            .field("owner", self.owner())
            .field("initiator", self.initiator())
            .field("checksum_config", &self.checksum_config())
            .field("key", self.key())
            .field("initiated_at", &self.initiated_at())
            .finish()
    }
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
    /// Bucket incarnation in which this staging session was authorized.
    pub bucket_incarnation_generation: u64,
    /// Fixed authority deadline after which an unfinished frontend stream is
    /// durably eligible for cleanup by a later runtime-map generation.
    pub cleanup_after: Option<u64>,
    pub encryption: ObjectEncryption,
    pub next_segment_vid: GenerationId,
    pub bucket_write_reservation: Option<crate::BucketWriteReservationProof>,
}

impl StreamUploadRecord {
    pub(crate) fn has_same_append_authority(&self, current: &Self) -> bool {
        self.has_same_stable_identity_ignoring_allocator(current)
    }

    pub(crate) fn has_same_stable_identity(&self, current: &Self) -> bool {
        self.has_same_stable_identity_ignoring_allocator(current)
            && self.next_segment_vid == current.next_segment_vid
    }

    fn has_same_stable_identity_ignoring_allocator(&self, current: &Self) -> bool {
        let reservation_matches = match (
            self.bucket_write_reservation.as_ref(),
            current.bucket_write_reservation.as_ref(),
        ) {
            (Some(expected), Some(current)) => expected.has_same_stable_identity(current),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        };
        self.session_id == current.session_id
            && self.bucket == current.bucket
            && self.key == current.key
            && self.target == current.target
            && self.state == current.state
            && self.created_at == current.created_at
            && self.bucket_incarnation_generation == current.bucket_incarnation_generation
            && self.cleanup_after == current.cleanup_after
            && self.encryption == current.encryption
            && reservation_matches
    }
}

/// Exact durable state captured by an authorized streamed PutObject append.
///
/// This is comparison evidence rather than independently sufficient
/// authority: the storage node must match it against the session and object
/// generation while holding the object-PG mutation lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExpectedStreamAppendState {
    PutObject {
        session: StreamUploadRecord,
        generation_id: GenerationId,
    },
    UploadPart {
        session: StreamUploadRecord,
        upload: Box<MultipartUploadRecord>,
        part_generation_id: GenerationId,
    },
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
pub(crate) struct StreamUploadCommandRecord {
    pub(crate) session_id: SessionId,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) target: StreamUploadTarget,
    pub(crate) state: StreamUploadState,
    pub(crate) created_at: u64,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) encryption: ObjectEncryption,
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
            bucket_incarnation_generation: record.bucket_incarnation_generation,
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
pub(crate) struct TerminalStreamCleanupRecord {
    pub(crate) session_id: SessionId,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) target: StreamUploadTarget,
    pub(crate) state: StreamUploadState,
    pub(crate) created_at: u64,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) encryption: ObjectEncryption,
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
            bucket_incarnation_generation: record.bucket_incarnation_generation,
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
}

/// Logical input for writing and publishing one streaming-upload segment.
///
/// Storage derives the physical payload identity, placement, erasure-coding
/// shape, stored-byte checksum, shard acknowledgements, and durable segment
/// record. Callers provide the transformed bytes and the S3-visible plaintext
/// checksum without choosing their physical representation.
#[derive(Clone, Copy)]
pub struct StreamSegmentAppendInput<'a> {
    pub session_id: &'a SessionId,
    pub segment_index: u32,
    /// CRC64-NVME over the user-visible plaintext payload bytes.
    pub payload_crc64: u64,
    pub storage_bytes: &'a [u8],
}

impl std::fmt::Debug for StreamSegmentAppendInput<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamSegmentAppendInput")
            .field("session_id", self.session_id)
            .field("segment_index", &self.segment_index)
            .field("storage_bytes_len", &self.storage_bytes.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSegmentAppendOutcome {
    pub target: StreamUploadTarget,
    pub logical_size: u64,
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
    pub(crate) shard_integrity: Option<SegmentShardIntegrityRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPartFinalizeSnapshot {
    pub upload_checksum: Option<MultipartChecksumConfig>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub staged_size: u64,
    pub staged_payload_crc64: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct StreamPartFinalizeInput<'a> {
    pub upload_id: &'a UploadId,
    pub session_id: &'a SessionId,
    pub part_number: u32,
    pub total_size: u64,
    pub payload_crc64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamUploadPartSnapshot {
    pub(crate) session: StreamUploadRecord,
    pub(crate) upload: MultipartUploadRecord,
    pub(crate) existing_part_generation: Option<u32>,
    pub(crate) staging_segments: Vec<StreamUploadSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamUploadPartStorageSnapshot {
    pub(crate) auth_snapshot: StreamUploadPartSnapshot,
    /// Immutable payload generation reserved under the stream session ID
    /// before the first UploadPart segment can be written.
    pub(crate) part_generation_id: GenerationId,
    pub(crate) existing_part: Option<MultipartPartRecord>,
    pub(crate) displaced_segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug)]
pub struct PreparedStreamPartCommit<T> {
    pub value: T,
    pub last_modified: u64,
    pub checksum: Option<ChecksumBytes>,
}

#[derive(Debug)]
pub struct FinalizeStreamPartOutcome<T> {
    pub value: T,
    pub last_modified: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FinalizeStreamPartCleanup {
    pub(crate) displaced_segments: Vec<MultipartPartSegmentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortMultipartUploadCleanup {
    pub(crate) upload: MultipartUploadRecord,
    pub(crate) parts: Vec<MultipartPartRecord>,
    pub(crate) streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) stream_uploads: Vec<TerminalStreamCleanupRecord>,
    pub(crate) stream_upload_segments: Vec<StreamUploadSegmentRecord>,
}

impl AbortMultipartUploadCleanup {
    pub(crate) fn matches_upload_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> bool {
        self.upload.bucket == *bucket
            && self.upload.key == *key
            && self.upload.upload_id == *upload_id
            && self.parts.iter().all(|part| part.upload_id == *upload_id)
            && self.streaming_segments.iter().all(|segment| {
                segment.bucket == *bucket && segment.key == *key && segment.upload_id == *upload_id
            })
            && self.stream_uploads.iter().all(|stream| {
                stream.bucket == *bucket
                    && stream.key == *key
                    && matches!(
                        &stream.target,
                        StreamUploadTarget::UploadPart {
                            upload_id: stream_upload_id,
                            ..
                        } if stream_upload_id == upload_id
                    )
            })
            && self.stream_upload_segments.iter().all(|segment| {
                self.stream_uploads
                    .iter()
                    .any(|stream| stream.session_id == segment.session_id)
            })
    }
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
    pub(crate) shard_integrity: Option<SegmentShardIntegrityRecord>,
}

/// Committed segment record for a multipart part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultipartPartSegmentRecord {
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
    pub(crate) shard_integrity: Option<SegmentShardIntegrityRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn integrity_witness_fixture(payload: &[u8], ec: EcShape) -> (Vec<ShardKey>, Vec<WriteAck>) {
        let shard_size = payload.len().div_ceil(usize::from(ec.k));
        let mut padded = payload.to_vec();
        padded.resize(shard_size * usize::from(ec.k), 0);
        let mut acks = padded
            .chunks_exact(shard_size)
            .map(|shard| WriteAck {
                crc64: checksum::crc64::checksum(shard),
                stored_size: shard.len() as u64,
            })
            .collect::<Vec<_>>();
        acks.extend((0..ec.m).map(|parity_index| {
            let parity = vec![parity_index.wrapping_add(0x80); shard_size];
            WriteAck {
                crc64: checksum::crc64::checksum(&parity),
                stored_size: parity.len() as u64,
            }
        }));
        let okh = [0x31; 16];
        let generation = GenerationId::new(47).unwrap();
        let keys = (0..acks.len())
            .map(|index| ShardKey::new(&okh, generation.get(), index as u8))
            .collect();
        (keys, acks)
    }

    #[test]
    fn segment_shard_integrity_witness_requires_complete_canonical_evidence() {
        let payload = b"stored segment requiring systematic padding";
        let ec = EcShape { k: 3, m: 2 };
        let okh = [0x31; 16];
        let generation = GenerationId::new(47).unwrap();
        let (keys, acks) = integrity_witness_fixture(payload, ec);
        let batch = keys
            .iter()
            .zip(&acks)
            .map(|(key, ack)| (key, *ack))
            .collect::<Vec<_>>();

        let witness = SegmentShardIntegrityWitness::from_ordered_shard_acks(
            5,
            ec,
            &okh,
            generation,
            payload.len() as u64,
            checksum::crc64::checksum(payload),
            &batch,
        )
        .unwrap();
        assert_eq!(witness.data_pg_id(), 5);
        assert_eq!(witness.durable.shard_size, payload.len().div_ceil(3) as u64);
        assert_eq!(
            witness
                .ordered_shard_acks
                .iter()
                .map(|(_, ack)| ack.crc64)
                .collect::<Vec<_>>(),
            [
                acks[0].crc64,
                acks[1].crc64,
                acks[2].crc64,
                acks[3].crc64,
                acks[4].crc64,
            ]
        );
        let encoded = witness.durable_record().encode();
        const EXPECTED_ENCODING: &[u8] = &[
            1, 0, 0, 0, 0, 0, 0, 0, 15, 0, 5, 196, 48, 218, 3, 230, 243, 188, 219, 216, 45, 131,
            143, 247, 70, 25, 76, 211, 145, 109, 245, 99, 18, 223, 152, 62, 187, 83, 197, 193, 135,
            182, 9, 97, 105, 190, 4, 118, 14, 108, 176,
        ];
        assert_eq!(encoded, EXPECTED_ENCODING);
        assert_eq!(
            SegmentShardIntegrityRecord::decode(EXPECTED_ENCODING).unwrap(),
            witness.durable
        );
        for profile in [
            SegmentPayloadProfile::SseCustomerSegmentAes256GcmV1,
            SegmentPayloadProfile::SseS3SegmentAes256GcmV1,
        ] {
            assert_eq!(
                validate_segment_shard_integrity_for_profile(
                    profile,
                    None,
                    ec,
                    payload.len() as u64,
                    checksum::crc64::checksum(payload),
                ),
                Ok(())
            );
        }
        for profile in [
            SegmentPayloadProfile::SseCustomerCobblestone256SegmentV1,
            SegmentPayloadProfile::SseS3Cobblestone256SegmentV1,
        ] {
            assert_eq!(
                validate_segment_shard_integrity_for_profile(
                    profile,
                    None,
                    ec,
                    payload.len() as u64,
                    checksum::crc64::checksum(payload),
                ),
                Err("Cobblestone segment is missing its shard integrity witness")
            );
            assert_eq!(
                validate_segment_shard_integrity_for_profile(
                    profile,
                    Some(&witness.durable),
                    ec,
                    payload.len() as u64,
                    checksum::crc64::checksum(payload),
                ),
                Ok(())
            );
        }
        for truncated_len in 0..EXPECTED_ENCODING.len() {
            assert_eq!(
                SegmentShardIntegrityRecord::decode(&EXPECTED_ENCODING[..truncated_len]),
                Err(SegmentShardIntegrityWitnessError::TruncatedEncoding)
            );
        }
        let mut unsupported = EXPECTED_ENCODING.to_vec();
        unsupported[0] = 2;
        assert_eq!(
            SegmentShardIntegrityRecord::decode(&unsupported),
            Err(SegmentShardIntegrityWitnessError::UnsupportedVersion { version: 2 })
        );
        let mut trailing = EXPECTED_ENCODING.to_vec();
        trailing.push(0);
        assert_eq!(
            SegmentShardIntegrityRecord::decode(&trailing),
            Err(SegmentShardIntegrityWitnessError::TrailingEncoding)
        );

        let mut missing_crc = EXPECTED_ENCODING[..EXPECTED_ENCODING.len() - 8].to_vec();
        missing_crc[9..11].copy_from_slice(&4_u16.to_be_bytes());
        assert_eq!(
            SegmentShardIntegrityRecord::decode(&missing_crc)
                .unwrap()
                .validate(ec, payload.len() as u64, checksum::crc64::checksum(payload)),
            Err(SegmentShardIntegrityWitnessError::ShardCount {
                expected: 5,
                actual: 4,
            })
        );

        let mut noncanonical_size = EXPECTED_ENCODING.to_vec();
        noncanonical_size[1..9].copy_from_slice(&16_u64.to_be_bytes());
        assert!(matches!(
            SegmentShardIntegrityRecord::decode(&noncanonical_size)
                .unwrap()
                .validate(ec, payload.len() as u64, checksum::crc64::checksum(payload)),
            Err(SegmentShardIntegrityWitnessError::NoncanonicalShardSize { .. })
        ));

        let mut reordered_crc = EXPECTED_ENCODING.to_vec();
        let first_crc = reordered_crc[11..19].to_vec();
        let second_crc = reordered_crc[19..27].to_vec();
        reordered_crc[11..19].copy_from_slice(&second_crc);
        reordered_crc[19..27].copy_from_slice(&first_crc);
        assert_eq!(
            SegmentShardIntegrityRecord::decode(&reordered_crc)
                .unwrap()
                .validate(ec, payload.len() as u64, checksum::crc64::checksum(payload)),
            Err(SegmentShardIntegrityWitnessError::SystematicChecksumMismatch)
        );

        let mut crossed_keys = keys.clone();
        crossed_keys.swap(0, 1);
        let crossed_batch = crossed_keys
            .iter()
            .zip(&acks)
            .map(|(key, ack)| (key, *ack))
            .collect::<Vec<_>>();
        assert!(matches!(
            SegmentShardIntegrityWitness::from_ordered_shard_acks(
                5,
                ec,
                &okh,
                generation,
                payload.len() as u64,
                checksum::crc64::checksum(payload),
                &crossed_batch,
            ),
            Err(SegmentShardIntegrityWitnessError::ShardIdentity { position: 0, .. })
        ));

        let mut wrong_size = acks.clone();
        wrong_size[ec.k as usize].stored_size += 1;
        let wrong_size_batch = keys
            .iter()
            .zip(&wrong_size)
            .map(|(key, ack)| (key, *ack))
            .collect::<Vec<_>>();
        assert!(matches!(
            SegmentShardIntegrityWitness::from_ordered_shard_acks(
                5,
                ec,
                &okh,
                generation,
                payload.len() as u64,
                checksum::crc64::checksum(payload),
                &wrong_size_batch,
            ),
            Err(SegmentShardIntegrityWitnessError::NoncanonicalShardSize { shard_index: 3, .. })
        ));

        let mut wrong_crc = acks.clone();
        wrong_crc[1].crc64 ^= 1;
        let wrong_crc_batch = keys
            .iter()
            .zip(&wrong_crc)
            .map(|(key, ack)| (key, *ack))
            .collect::<Vec<_>>();
        assert_eq!(
            SegmentShardIntegrityWitness::from_ordered_shard_acks(
                5,
                ec,
                &okh,
                generation,
                payload.len() as u64,
                checksum::crc64::checksum(payload),
                &wrong_crc_batch,
            ),
            Err(SegmentShardIntegrityWitnessError::SystematicChecksumMismatch)
        );
    }

    fn management_lookup_test_upload(state: UploadState) -> MultipartUploadRecord {
        MultipartUploadRecord {
            upload_id: UploadId::for_test("management-lookup"),
            bucket: BucketName::try_from("management-lookup-bucket").unwrap(),
            key: ObjectKey::try_from("private-durable-key").unwrap(),
            initiated_at: 17,
            state,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("lookup-initiator"),
            owner: OwnerIdentity::from_principal("lookup-owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_generation_id: GenerationId::new(23).unwrap(),
            initiated_object_identity: None,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: crate::tests::metadata_only_test_encryption(),
        }
    }

    #[test]
    fn object_read_multipart_part_exposes_and_compares_only_logical_read_state() {
        let first_record = ObjectPartRecord {
            bucket: BucketName::try_from("private-first-bucket").unwrap(),
            key: ObjectKey::try_from("private-first-key").unwrap(),
            version_id: VersionId::from_u64(29),
            part_number: 3,
            size: 17,
            payload_crc64: 23,
            etag: vec![31, 37],
            etag_kind: EtagKind::MultipartComposite,
            part_vid: GenerationId::new(41).unwrap(),
            placement_cluster_epoch: ClusterEpoch::new(43).unwrap(),
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 47,
            checksum: None,
        };
        let mut physically_distinct_record = first_record.clone();
        physically_distinct_record.bucket = BucketName::try_from("private-second-bucket").unwrap();
        physically_distinct_record.key = ObjectKey::try_from("private-second-key").unwrap();
        physically_distinct_record.version_id = VersionId::from_u64(53);
        physically_distinct_record.etag = vec![59, 61];
        physically_distinct_record.etag_kind = EtagKind::Crc64;
        physically_distinct_record.part_vid = GenerationId::new(67).unwrap();
        physically_distinct_record.placement_cluster_epoch = ClusterEpoch::new(71).unwrap();
        physically_distinct_record.ec_k = 2;
        physically_distinct_record.ec_m = 1;
        physically_distinct_record.data_pg_id = 73;

        let first = ObjectReadMultipartPart::from_record(first_record);
        let physically_distinct = ObjectReadMultipartPart::from_record(physically_distinct_record);

        assert_eq!(first.part_number(), 3);
        assert_eq!(first.size(), 17);
        assert_eq!(first.payload_crc64(), 23);
        assert_eq!(first.checksum(), None);
        assert_eq!(first, physically_distinct);

        let debug = format!("{first:?}");
        assert!(debug.contains("part_number: 3"));
        assert!(debug.contains("size: 17"));
        assert!(debug.contains("payload_crc64: 23"));
        assert!(!debug.contains("private-first-bucket"));
        assert!(!debug.contains("private-first-key"));
        assert!(!debug.contains("data_pg_id"));
        assert!(!debug.contains("placement_cluster_epoch"));
        assert!(!debug.contains("part_vid"));
        assert!(!debug.contains("etag"));
    }

    #[test]
    fn multipart_abort_lookup_exposes_only_logical_authorization_state() {
        let upload = management_lookup_test_upload(UploadState::InProgress);
        let upload_id = upload.upload_id.clone();
        let lookup = MultipartUploadAbortLookup::from_management_lookup(
            MultipartUploadManagementLookup::InProgress(Box::new(upload)),
        );
        let MultipartUploadAbortLookup::InProgress(candidate) = lookup else {
            panic!("in-progress management result must remain abortable");
        };
        assert_eq!(candidate.owner().principal, "lookup-owner");
        assert_eq!(candidate.initiator().principal, "lookup-initiator");
        assert_eq!(
            format!("{candidate:?}"),
            format!("MultipartUploadAbortCandidate {{ upload_id: {upload_id:?}, .. }}")
        );

        let authorized = candidate.into_authorized_abort();
        assert_eq!(authorized.upload_id(), &upload_id);
        assert_eq!(authorized.record().key.as_str(), "private-durable-key");
        assert_eq!(
            format!("{authorized:?}"),
            format!("AuthorizedMultipartUploadAbort {{ upload_id: {upload_id:?}, .. }}")
        );

        let terminal = management_lookup_test_upload(UploadState::Completing);
        let lookup = MultipartUploadAbortLookup::from_management_lookup(
            MultipartUploadManagementLookup::NonInProgress(Box::new(terminal)),
        );
        let MultipartUploadAbortLookup::NonInProgress(identity) = lookup else {
            panic!("non-in-progress management result must not carry an abort capability");
        };
        assert_eq!(identity.owner().principal, "lookup-owner");
        assert_eq!(identity.initiator().principal, "lookup-initiator");
    }

    #[test]
    fn multipart_list_parts_lookup_exposes_only_logical_authorization_state() {
        let upload = management_lookup_test_upload(UploadState::InProgress);
        let upload_id = upload.upload_id.clone();
        let lookup = MultipartUploadListPartsLookup::from_management_lookup(
            MultipartUploadManagementLookup::InProgress(Box::new(upload)),
        );
        let MultipartUploadListPartsLookup::InProgress(candidate) = lookup else {
            panic!("in-progress management result must remain listable");
        };
        assert_eq!(candidate.owner().principal, "lookup-owner");
        assert_eq!(candidate.initiator().principal, "lookup-initiator");
        assert_eq!(
            format!("{candidate:?}"),
            "MultipartUploadListPartsCandidate { .. }"
        );

        let authorized = candidate.into_authorized_list_parts();
        assert_eq!(authorized.record().upload_id, upload_id);
        assert_eq!(authorized.record().key.as_str(), "private-durable-key");
        assert_eq!(
            format!("{authorized:?}"),
            "AuthorizedMultipartUploadListParts { .. }"
        );

        let terminal = management_lookup_test_upload(UploadState::Completing);
        let lookup = MultipartUploadListPartsLookup::from_management_lookup(
            MultipartUploadManagementLookup::NonInProgress(Box::new(terminal)),
        );
        let MultipartUploadListPartsLookup::NonInProgress(identity) = lookup else {
            panic!("non-in-progress management result must not carry a listing capability");
        };
        assert_eq!(identity.owner().principal, "lookup-owner");
        assert_eq!(identity.initiator().principal, "lookup-initiator");
    }

    #[test]
    fn multipart_part_candidate_exposes_only_logical_authorization_state() {
        let mut upload = management_lookup_test_upload(UploadState::InProgress);
        upload.checksum = Some(
            MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32, None)
                .expect("CRC32 is a valid multipart checksum configuration"),
        );
        let upload_id = upload.upload_id.clone();
        let candidate = MultipartUploadPartCandidate::from_record(upload);
        assert_eq!(candidate.key().as_str(), "private-durable-key");
        assert_eq!(candidate.owner().principal, "lookup-owner");
        assert_eq!(candidate.initiator().principal, "lookup-initiator");
        assert_eq!(
            candidate.checksum_config().map(|config| config.algorithm()),
            Some(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(
            candidate.encryption(),
            &crate::tests::metadata_only_test_encryption()
        );
        assert_eq!(
            format!("{candidate:?}"),
            "MultipartUploadPartCandidate { .. }"
        );

        let authorized = candidate.into_authorized_part(7);
        assert_eq!(authorized.record().upload_id, upload_id);
        assert_eq!(authorized.record().key.as_str(), "private-durable-key");
        assert_eq!(authorized.part_number(), 7);
        assert_eq!(
            format!("{authorized:?}"),
            "AuthorizedMultipartUploadPart { .. }"
        );
    }

    #[test]
    fn multipart_completion_lookup_exposes_only_logical_authorization_state() {
        let mut upload = management_lookup_test_upload(UploadState::InProgress);
        upload.checksum = Some(
            MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32, None)
                .expect("CRC32 is a valid multipart checksum configuration"),
        );
        let upload_id = upload.upload_id.clone();
        let lookup = MultipartUploadCompletionLookup::from_management_lookup(
            MultipartUploadManagementLookup::InProgress(Box::new(upload)),
        );
        let MultipartUploadCompletionLookup::InProgress(candidate) = lookup else {
            panic!("in-progress management result must remain completable");
        };
        assert_eq!(candidate.key().as_str(), "private-durable-key");
        assert_eq!(candidate.owner().principal, "lookup-owner");
        assert_eq!(candidate.initiator().principal, "lookup-initiator");
        assert_eq!(
            candidate.encryption(),
            &crate::tests::metadata_only_test_encryption()
        );
        assert_eq!(
            format!("{candidate:?}"),
            "MultipartUploadCompletionCandidate { .. }"
        );

        let (authorized, context) = candidate.into_authorized_completion();
        assert_eq!(authorized.0.upload_id, upload_id);
        assert_eq!(authorized.0.key.as_str(), "private-durable-key");
        assert_eq!(
            context.checksum_config().map(|config| config.algorithm()),
            Some(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(
            format!("{authorized:?}"),
            "AuthorizedMultipartUploadCompletion { .. }"
        );
        assert!(!format!("{context:?}").contains("system_metadata_blob"));

        let replay_upload_id = UploadId::for_test("completion-replay");
        let replay = MultipartCompletionReplay {
            upload_id: replay_upload_id.clone(),
            bucket: BucketName::try_from("private-replay-bucket").unwrap(),
            key: ObjectKey::try_from("private-replay-key").unwrap(),
            fingerprint: MultipartCompletionFingerprint::from_bytes([7; 32]),
            version_id: VersionId::Null,
            etag: ObjectEtag::SinglePart([11; 8]),
            size: 13,
            last_modified: 17,
            tags: Some(SerializedTagSet::default()),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::new(vec![19, 23])),
            encryption: crate::tests::metadata_only_test_encryption(),
        };
        let lookup = MultipartUploadCompletionLookup::from_management_lookup(
            MultipartUploadManagementLookup::Replay(Box::new(replay)),
        );
        let MultipartUploadCompletionLookup::Replay(candidate) = lookup else {
            panic!("completion replay must remain replayable");
        };
        assert_eq!(
            format!("{candidate:?}"),
            "MultipartCompletionReplayCandidate { .. }"
        );
        let replay = candidate.into_authorized_replay();
        assert_eq!(replay.upload_id(), &replay_upload_id);
        assert_eq!(replay.fingerprint().as_bytes(), &[7; 32]);
        assert_eq!(replay.size(), 13);
        assert_eq!(replay.last_modified(), 17);
        assert!(replay.tags().is_some());
        assert_eq!(
            replay.encryption(),
            &crate::tests::metadata_only_test_encryption()
        );
        assert_eq!(
            format!("{replay:?}"),
            "AuthorizedMultipartCompletionReplay { .. }"
        );

        assert!(matches!(
            MultipartUploadCompletionLookup::from_management_lookup(
                MultipartUploadManagementLookup::NonInProgress(Box::new(
                    management_lookup_test_upload(UploadState::Completing),
                )),
            ),
            MultipartUploadCompletionLookup::Unavailable
        ));
        assert!(matches!(
            MultipartUploadCompletionLookup::from_management_lookup(
                MultipartUploadManagementLookup::Missing,
            ),
            MultipartUploadCompletionLookup::Unavailable
        ));
    }

    #[test]
    fn multipart_completion_outcome_exposes_only_logical_publication_state() {
        let live_tags = s3_types::TagSet::from_pairs(
            vec![(
                "customer-secret-key".to_string(),
                "customer-secret-value".to_string(),
            )],
            s3_types::MAX_OBJECT_TAGS,
        )
        .unwrap();
        let outcome = CompleteMultipartCommitOutcome {
            version_id: VersionId::from_u64(29),
            stale_payload_generation_id: Some(GenerationId::new(31).unwrap()),
            live_tags: Some(SerializedTagSet::from_tag_set(live_tags).unwrap()),
            live_size: 37,
            live_last_modified: 41,
        };

        assert_eq!(outcome.version_id(), VersionId::from_u64(29));
        assert_eq!(
            outcome.stale_payload_generation_id(),
            Some(GenerationId::new(31).unwrap())
        );
        assert!(outcome.live_tags().is_some());
        assert_eq!(outcome.live_size(), 37);
        assert_eq!(outcome.live_last_modified(), 41);

        let debug = format!("{outcome:?}");
        assert!(debug.contains("stale_payload_generation_id"));
        assert!(debug.contains("live_tag_count: Some(1)"));
        assert!(!debug.contains("customer-secret-key"));
        assert!(!debug.contains("customer-secret-value"));
        assert!(!debug.contains("segments"));
        assert!(!debug.contains("parts"));
        assert!(!debug.contains("streaming_segments"));
    }

    #[test]
    fn portable_route_effect_deadline_does_not_subtract_clock_skew_twice() {
        let epoch = ClusterEpoch::new(7).unwrap();
        let fence = crate::clock::with_time_and_monotonic_override(10_000, 50_000, || {
            AdmittedRouteEffectFence::bind_portable(epoch, 12_000, 11_000)
        });

        crate::clock::with_time_and_monotonic_override(10_999, 50_999, || {
            fence.require_valid_for(epoch).unwrap();
        });
        let error = crate::clock::with_time_and_monotonic_override(11_000, 51_000, || {
            fence.require_valid_for(epoch).unwrap_err()
        });
        assert!(matches!(
            error,
            StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 12_000,
                now_ms: 11_000,
            } if cluster_epoch == epoch
        ));
    }

    #[test]
    fn object_payload_segment_debug_exposes_only_logical_layout() {
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let subject = ObjectPayloadSubject {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id: GenerationId::new(29).unwrap(),
        };
        let segment = ObjectSegmentRecord {
            shard_integrity: None,
            bucket,
            key,
            version_id: VersionId::Null,
            segment_index: 3,
            size: 9,
            segment_crc64: 17,
            segment_okh: [23; 16],
            segment_vid: GenerationId::new(29).unwrap(),
            data_pg_id: 31,
            placement_cluster_epoch: ClusterEpoch::new(37).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let segment = ObjectPayloadSegment::from_object_record(
            &subject,
            segment,
            SegmentPayloadProfile::SseS3SegmentAes256GcmV1,
        )
        .unwrap();

        assert_eq!(segment.segment_index(), 3);
        assert_eq!(segment.size(), 9);
        assert_eq!(
            format!("{segment:?}"),
            "ObjectPayloadSegment { segment_index: 3, size: 9, .. }"
        );
    }

    #[test]
    fn object_read_snapshot_binds_standard_payload_subject_once() {
        let bucket = BucketName::try_from("subject-bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("subject-key".to_string()).unwrap();
        let generation_id = GenerationId::new(29).unwrap();
        let encryption = crate::tests::metadata_only_test_encryption();
        let stored = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id,
            size: 9,
            etag: ObjectEtag::single_part(17),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 1, m: 0 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: encryption.clone(),
        });
        let segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            segment_index: 3,
            size: 9,
            segment_crc64: 17,
            segment_okh: [23; 16],
            segment_vid: generation_id,
            data_pg_id: 31,
            placement_cluster_epoch: ClusterEpoch::new(37).unwrap(),
            ec_k: 1,
            ec_m: 0,
            shard_integrity: None,
        };
        let mut snapshot =
            ObjectReadSnapshot::from_records(stored, vec![segment], vec![], vec![]).unwrap();
        assert!(snapshot.object_segments[0].encryption_subject().is_none());

        snapshot.bind_payload_encryption_subjects(41).unwrap();
        let expected = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(41, &bucket, &key, generation_id, &encryption),
            3,
            9,
        );
        let actual = snapshot.object_segments[0].encryption_subject().unwrap();
        assert_eq!(actual.canonical_bytes(), expected.canonical_bytes());
        assert!(actual.matches_standard_object_segment(&encryption, 3, 9));
        assert_eq!(
            snapshot.bind_payload_encryption_subjects(43),
            Err("object payload encryption subjects are already bound")
        );
    }

    #[test]
    fn object_read_snapshot_binds_multipart_payload_provenance() {
        let bucket = BucketName::try_from("multipart-subject-bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("multipart-subject-key".to_string()).unwrap();
        let upload_id = UploadId::try_from("ab".repeat(UPLOAD_ID_LEN / 2)).unwrap();
        let generation_id = GenerationId::new(47).unwrap();
        let part_generation_id = GenerationId::new(53).unwrap();
        let version_id = VersionId::from_u64(59);
        let encryption = crate::tests::metadata_only_test_encryption();
        let stored = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id,
            size: 11,
            etag: ObjectEtag::multipart(17_u64.to_be_bytes(), 1),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 1, m: 0 },
            layout: ObjectLayout::MultipartManifest {
                parts_count: std::num::NonZeroU32::new(1).unwrap(),
            },
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: encryption.clone(),
        });
        let part = ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            part_number: 7,
            size: 11,
            payload_crc64: 17,
            etag: 17_u64.to_be_bytes().to_vec(),
            etag_kind: EtagKind::Crc64,
            part_vid: part_generation_id,
            placement_cluster_epoch: ClusterEpoch::new(61).unwrap(),
            ec_k: 1,
            ec_m: 0,
            data_pg_id: 67,
            checksum: None,
        };
        let segment = MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: version_id.to_u64(),
            part_number: 7,
            segment_index: 2,
            size: 11,
            segment_crc64: 17,
            segment_okh: [71; 16],
            segment_vid: GenerationId::new(73).unwrap(),
            data_pg_id: 67,
            placement_cluster_epoch: ClusterEpoch::new(61).unwrap(),
            ec_k: 1,
            ec_m: 0,
            shard_integrity: None,
        };
        let mut snapshot =
            ObjectReadSnapshot::from_records(stored, vec![], vec![part], vec![segment]).unwrap();

        snapshot.bind_payload_encryption_subjects(79).unwrap();
        let actual = snapshot.multipart_part_segments[0]
            .encryption_subject()
            .unwrap();
        assert!(actual.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            part_generation_id,
            2,
            11,
        ));
        assert!(!actual.matches_standard_object_segment(&encryption, 2, 11));
    }

    #[test]
    fn object_payload_placement_diagnostic_is_storage_owned() {
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let generation_id = GenerationId::new(29).unwrap();
        let stored = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id,
            size: 9,
            etag: ObjectEtag::single_part(17),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: crate::tests::metadata_only_test_encryption(),
        });
        let segment = ObjectSegmentRecord {
            shard_integrity: None,
            bucket,
            key,
            version_id: VersionId::Null,
            segment_index: 3,
            size: 9,
            segment_crc64: 17,
            segment_okh: [23; 16],
            segment_vid: generation_id,
            data_pg_id: 31,
            placement_cluster_epoch: ClusterEpoch::new(37).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let snapshot =
            ObjectReadSnapshot::from_records(stored.clone(), vec![segment.clone()], vec![], vec![])
                .expect("matching payload snapshot");

        assert_eq!(
            snapshot.payload_placement_diagnostic().unwrap(),
            concat!(
                "generation_id=29\n",
                "segment_count=1\n",
                "segment_index=3 data_pg_id=31 placement_cluster_epoch=37\n",
            )
        );

        let StoredObject::Live(mut invalid_live) = stored else {
            panic!("fixture must be a live object");
        };
        let ObjectEncryption::SseS3(state) = invalid_live.encryption else {
            panic!("fixture must use SSE-S3");
        };
        invalid_live.encryption = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                state.wrapping_key_id(),
                *state.wrap_nonce(),
                *state.wrapped_dek(),
                *state.segment_nonce_prefix(),
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let mut invalid_segment = segment;
        invalid_segment.size = 1_u64 << 52;
        assert_eq!(
            ObjectReadSnapshot::from_records(
                StoredObject::Live(invalid_live),
                vec![invalid_segment],
                vec![],
                vec![],
            )
            .unwrap_err(),
            "object segment has invalid stored codec geometry"
        );
    }

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
    fn object_encryption_encoding_matches_golden_baseline() {
        let customer_state =
            SseCustomerObjectState::new(7, [1; 16], [2; 32], [3; 16], [4; 12], [5; 48], [6; 6])
                .with_encrypted_checksum_metadata([7; 12], vec![8, 9, 10, 11])
                .unwrap();
        let customer = ObjectEncryption::SseCustomer(customer_state);
        let mut historical_customer_v3 = vec![3, 0, 0, 0, 7];
        historical_customer_v3.extend_from_slice(&[1; 16]);
        historical_customer_v3.extend_from_slice(&[2; 32]);
        historical_customer_v3.extend_from_slice(&[3; 16]);
        historical_customer_v3.extend_from_slice(&[4; 12]);
        historical_customer_v3.extend_from_slice(&[5; 48]);
        historical_customer_v3.extend_from_slice(&[6; 6]);
        historical_customer_v3.extend_from_slice(&[7; 12]);
        historical_customer_v3.extend_from_slice(&[0, 4, 8, 9, 10, 11]);
        let mut historical_customer_v4 =
            vec![4, SegmentPayloadCodec::SegmentAes256GcmV1.tag(), 0, 0, 0, 7];
        historical_customer_v4.extend_from_slice(&[1; 16]);
        historical_customer_v4.extend_from_slice(&[2; 32]);
        historical_customer_v4.extend_from_slice(&[3; 16]);
        historical_customer_v4.extend_from_slice(&[4; 12]);
        historical_customer_v4.extend_from_slice(&[5; 48]);
        historical_customer_v4.extend_from_slice(&[6; 6]);
        historical_customer_v4.extend_from_slice(&[7; 12]);
        historical_customer_v4.extend_from_slice(&[0, 4, 8, 9, 10, 11]);
        let mut historical_customer_v5 = vec![5, SegmentPayloadCodec::SegmentAes256GcmV1.tag()];
        historical_customer_v5.extend_from_slice(&[4; 12]);
        historical_customer_v5.extend_from_slice(&[0; 4]);
        historical_customer_v5.extend_from_slice(&[0, 0, 0, 7]);
        historical_customer_v5.extend_from_slice(&[1; 16]);
        historical_customer_v5.extend_from_slice(&[2; 32]);
        historical_customer_v5.extend_from_slice(&[3; 16]);
        historical_customer_v5.extend_from_slice(&[4; 12]);
        historical_customer_v5.extend_from_slice(&[5; 48]);
        historical_customer_v5.extend_from_slice(&[6; 6]);
        historical_customer_v5.extend_from_slice(&[7; 12]);
        historical_customer_v5.extend_from_slice(&[0, 4, 8, 9, 10, 11]);
        let mut customer_golden = vec![
            6,
            SegmentPayloadCodec::SegmentAes256GcmV1.tag(),
            ObjectEncryptionKeyBinding::ProvisionalV1.tag(),
        ];
        customer_golden.extend_from_slice(&historical_customer_v5[2..]);
        assert_eq!(customer.encryption_type() as u8, 1);
        assert_eq!(customer.encode_state().unwrap(), customer_golden);
        assert!(matches!(
            ObjectEncryption::decode(
                ObjectEncryptionType::SseCustomer,
                Some(historical_customer_v3),
            ),
            Err(ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion { version: 3 })
        ));
        assert!(matches!(
            ObjectEncryption::decode(
                ObjectEncryptionType::SseCustomer,
                Some(historical_customer_v4),
            ),
            Err(ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion { version: 4 })
        ));
        assert!(matches!(
            ObjectEncryption::decode(
                ObjectEncryptionType::SseCustomer,
                Some(historical_customer_v5),
            ),
            Err(ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion { version: 5 })
        ));
        for binding in [0, 3] {
            let mut malformed = customer_golden.clone();
            malformed[2] = binding;
            assert!(matches!(
                ObjectEncryption::decode(ObjectEncryptionType::SseCustomer, Some(malformed)),
                Err(ObjectEncryptionDecodeError::UnsupportedObjectEncryptionKeyBinding {
                    binding: actual,
                }) if actual == binding
            ));
        }
        assert_eq!(
            ObjectEncryption::decode(
                ObjectEncryptionType::from_u8(1).unwrap(),
                Some(customer_golden),
            )
            .unwrap(),
            customer
        );

        let managed_state = SseS3ObjectState::new(9, [10; 12], [11; 48], [12; 6])
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1)
            .with_encrypted_checksum_metadata([13; 12], vec![14, 15, 16])
            .unwrap();
        let managed = ObjectEncryption::SseS3(managed_state);
        let mut historical_managed_v1 = vec![1, 0, 0, 0, 9];
        historical_managed_v1.extend_from_slice(&[10; 12]);
        historical_managed_v1.extend_from_slice(&[11; 48]);
        historical_managed_v1.extend_from_slice(&[12; 6]);
        historical_managed_v1.extend_from_slice(&[13; 12]);
        historical_managed_v1.extend_from_slice(&[0, 3, 14, 15, 16]);
        let mut historical_managed_v2 = vec![
            2,
            SegmentPayloadCodec::Cobblestone256SegmentV1.tag(),
            0,
            0,
            0,
            9,
        ];
        historical_managed_v2.extend_from_slice(&[10; 12]);
        historical_managed_v2.extend_from_slice(&[11; 48]);
        historical_managed_v2.extend_from_slice(&[12; 6]);
        historical_managed_v2.extend_from_slice(&[13; 12]);
        historical_managed_v2.extend_from_slice(&[0, 3, 14, 15, 16]);
        let mut historical_managed_v3 = vec![3, SegmentPayloadCodec::Cobblestone256SegmentV1.tag()];
        historical_managed_v3.extend_from_slice(&[10; 12]);
        historical_managed_v3.extend_from_slice(&[0; 4]);
        historical_managed_v3.extend_from_slice(&[0, 0, 0, 9]);
        historical_managed_v3.extend_from_slice(&[10; 12]);
        historical_managed_v3.extend_from_slice(&[11; 48]);
        historical_managed_v3.extend_from_slice(&[12; 6]);
        historical_managed_v3.extend_from_slice(&[13; 12]);
        historical_managed_v3.extend_from_slice(&[0, 3, 14, 15, 16]);
        let mut managed_golden = vec![
            4,
            SegmentPayloadCodec::Cobblestone256SegmentV1.tag(),
            ObjectEncryptionKeyBinding::ProvisionalV1.tag(),
        ];
        managed_golden.extend_from_slice(&historical_managed_v3[2..]);
        assert_eq!(managed.encryption_type() as u8, 2);
        assert_eq!(managed.encode_state().unwrap(), managed_golden);
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, Some(historical_managed_v1)),
            Err(ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion { version: 1 })
        ));
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, Some(historical_managed_v2)),
            Err(ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion { version: 2 })
        ));
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, Some(historical_managed_v3)),
            Err(ObjectEncryptionDecodeError::UnsupportedSseS3StateVersion { version: 3 })
        ));
        for binding in [0, 3] {
            let mut malformed = managed_golden.clone();
            malformed[2] = binding;
            assert!(matches!(
                ObjectEncryption::decode(ObjectEncryptionType::SseS3, Some(malformed)),
                Err(ObjectEncryptionDecodeError::UnsupportedObjectEncryptionKeyBinding {
                    binding: actual,
                }) if actual == binding
            ));
        }
        assert_eq!(
            ObjectEncryption::decode(
                ObjectEncryptionType::from_u8(2).unwrap(),
                Some(managed_golden),
            )
            .unwrap(),
            managed
        );
    }

    #[test]
    fn committed_object_ciphertext_binding_is_exact_and_one_way() {
        let bucket = BucketName::try_from("subject-bound-bucket").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let generation = GenerationId::new(19).unwrap();
        let provisional = ObjectEncryption::SseS3(
            SseS3ObjectState::new_with_encryption_instance_id(
                [3; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
                7,
                [4; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
                [5; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
                [6; OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let subject =
            ObjectEncryptionSubject::standard_object(11, &bucket, &key, generation, &provisional);
        let committed = provisional
            .bind_committed_object_ciphertexts(
                &subject,
                [7; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
                [8; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
                [9; OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
                vec![10, 11, 12],
            )
            .unwrap();
        assert_eq!(
            provisional.key_binding(),
            ObjectEncryptionKeyBinding::ProvisionalV1
        );
        assert_eq!(
            committed.key_binding(),
            ObjectEncryptionKeyBinding::ObjectSubjectV2
        );
        assert_eq!(
            committed.encryption_instance_id(),
            provisional.encryption_instance_id()
        );
        assert_eq!(committed.payload_codec(), provisional.payload_codec());
        assert_eq!(
            committed
                .bind_committed_object_ciphertexts(
                    &subject,
                    [13; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
                    [14; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
                    [0; OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
                    Vec::new(),
                )
                .unwrap_err(),
            ObjectEncryptionStateError::ObjectEncryptionAlreadySubjectBound
        );

        let crossed_encryption = ObjectEncryption::SseS3(
            SseS3ObjectState::new_with_encryption_instance_id(
                [23; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN],
                7,
                [4; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
                [5; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
                [6; OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let crossed_subject = ObjectEncryptionSubject::standard_object(
            11,
            &bucket,
            &key,
            generation,
            &crossed_encryption,
        );
        assert_eq!(
            provisional
                .bind_committed_object_ciphertexts(
                    &crossed_subject,
                    [7; OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
                    [8; OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
                    [0; OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
                    Vec::new(),
                )
                .unwrap_err(),
            ObjectEncryptionStateError::ObjectEncryptionSubjectMismatch
        );
    }

    #[test]
    fn segment_payload_codec_cross_product_and_geometry_is_exhaustive() {
        let customer = |codec| {
            ObjectEncryption::SseCustomer(
                SseCustomerObjectState::new(7, [1; 16], [2; 32], [3; 16], [4; 12], [5; 48], [6; 6])
                    .with_payload_codec(codec),
            )
        };
        let managed = |codec| {
            ObjectEncryption::SseS3(
                SseS3ObjectState::new(9, [10; 12], [11; 48], [12; 6]).with_payload_codec(codec),
            )
        };

        for (encryption, expected_profile) in [
            (
                customer(SegmentPayloadCodec::SegmentAes256GcmV1),
                SegmentPayloadProfile::SseCustomerSegmentAes256GcmV1,
            ),
            (
                customer(SegmentPayloadCodec::Cobblestone256SegmentV1),
                SegmentPayloadProfile::SseCustomerCobblestone256SegmentV1,
            ),
            (
                managed(SegmentPayloadCodec::SegmentAes256GcmV1),
                SegmentPayloadProfile::SseS3SegmentAes256GcmV1,
            ),
            (
                managed(SegmentPayloadCodec::Cobblestone256SegmentV1),
                SegmentPayloadProfile::SseS3Cobblestone256SegmentV1,
            ),
        ] {
            assert_eq!(encryption.payload_profile(), expected_profile);
            assert_eq!(
                expected_profile.encryption_type(),
                encryption.encryption_type()
            );
            assert_eq!(expected_profile.payload_codec(), encryption.payload_codec());
            let encoded = encryption.encode_state().unwrap();
            assert_eq!(
                ObjectEncryption::decode(encryption.encryption_type(), Some(encoded)).unwrap(),
                encryption
            );
        }

        for logical_size in [0, 1, 16_383, 16_384, 16_385, 8 * 1024 * 1024] {
            for codec in [
                SegmentPayloadCodec::SegmentAes256GcmV1,
                SegmentPayloadCodec::Cobblestone256SegmentV1,
            ] {
                let stored_size = codec.stored_size(logical_size).unwrap();
                assert_eq!(codec.logical_size(stored_size), Some(logical_size));
            }
        }

        let mut invalid_customer = customer(SegmentPayloadCodec::SegmentAes256GcmV1)
            .encode_state()
            .unwrap();
        invalid_customer[1] = 0;
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseCustomer, Some(invalid_customer)),
            Err(ObjectEncryptionDecodeError::UnsupportedSegmentPayloadCodec { codec: 0 })
        ));
        let mut invalid_managed = managed(SegmentPayloadCodec::SegmentAes256GcmV1)
            .encode_state()
            .unwrap();
        invalid_managed[1] = 3;
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, Some(invalid_managed)),
            Err(ObjectEncryptionDecodeError::UnsupportedSegmentPayloadCodec { codec: 3 })
        ));
        assert_eq!(ObjectEncryptionType::from_u8(0), None);
        assert_eq!(ObjectEncryptionType::from_u8(3), None);
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseCustomer, None),
            Err(ObjectEncryptionDecodeError::MissingSseCustomerState)
        ));
        assert!(matches!(
            ObjectEncryption::decode(ObjectEncryptionType::SseS3, None),
            Err(ObjectEncryptionDecodeError::MissingSseS3State)
        ));
    }

    #[test]
    fn object_encryption_subject_has_canonical_exact_encoding_and_rejects_crossed_fields() {
        let bucket = BucketName::try_from("subject-bucket").unwrap();
        let other_bucket = BucketName::try_from("subject-other").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let other_key = ObjectKey::try_from("path/to/other").unwrap();
        let encryption = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let other_encryption = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [13; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let same_nonce_other_wrapping_key = ObjectEncryption::SseS3(SseS3ObjectState::new(
            10,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let same_nonce_other_wrapped_dek = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [14; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let same_nonce_other_segment_nonce_prefix = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [15; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let same_wrap_nonce_other_instance = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [11; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_encryption_instance_id([20; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN]),
        );
        let checksum_enriched_encryption = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [11; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_encrypted_checksum_metadata([16; SSE_S3_CHECKSUM_NONCE_LEN], vec![17, 18, 19])
            .unwrap(),
        );
        let crossed_payload_codec = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [11; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let crossed_mode_encryption = ObjectEncryption::SseCustomer(SseCustomerObjectState::new(
            9,
            [1; SSE_C_VALIDATOR_SALT_LEN],
            [2; SSE_C_VALIDATOR_HMAC_LEN],
            [3; SSE_C_WRAP_SALT_LEN],
            [10; SSE_C_WRAP_NONCE_LEN],
            [11; SSE_C_WRAPPED_DEK_LEN],
            [12; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let subject = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let mut expected = vec![32];
        expected.extend_from_slice(b"argmin:object-encryption-subject");
        let mut historical_v1 = vec![32];
        historical_v1.extend_from_slice(b"argmin:object-encryption-subject");
        historical_v1.extend_from_slice(&[1, 1, 13, 2]);
        historical_v1.extend_from_slice(&[10; 12]);
        historical_v1.extend_from_slice(&17_u64.to_be_bytes());
        historical_v1.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1.extend_from_slice(b"subject-bucket");
        historical_v1.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1.extend_from_slice(b"path/to/object");
        historical_v1.extend_from_slice(&23_u64.to_be_bytes());
        assert_eq!(historical_v1[33], 1, "frozen V1 subject version changed");
        assert!(ObjectEncryptionSubject::validate_canonical_for_test(&historical_v1).is_err());

        expected.extend_from_slice(&[2, 1, 17, 2]);
        expected.extend_from_slice(&[10; 12]);
        expected.extend_from_slice(&[0; 4]);
        expected.extend_from_slice(&17_u64.to_be_bytes());
        expected.extend_from_slice(&14_u16.to_be_bytes());
        expected.extend_from_slice(b"subject-bucket");
        expected.extend_from_slice(&14_u16.to_be_bytes());
        expected.extend_from_slice(b"path/to/object");
        expected.extend_from_slice(&23_u64.to_be_bytes());
        assert_eq!(subject.canonical_bytes(), expected);
        assert_eq!(
            ObjectEncryptionSubject::validate_canonical_for_test(&expected)
                .unwrap()
                .as_ref(),
            expected
        );
        assert!(subject.matches_encryption(&encryption));
        assert!(subject.matches_encryption(&checksum_enriched_encryption));
        assert!(!subject.matches_encryption(&crossed_payload_codec));
        assert!(!subject.matches_encryption(&other_encryption));
        assert!(!subject.matches_encryption(&same_nonce_other_wrapping_key));
        assert!(!subject.matches_encryption(&same_nonce_other_wrapped_dek));
        assert!(!subject.matches_encryption(&same_nonce_other_segment_nonce_prefix));
        assert!(!subject.matches_encryption(&same_wrap_nonce_other_instance));
        assert!(!subject.matches_encryption(&crossed_mode_encryption));
        assert_ne!(
            subject.canonical_bytes(),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &crossed_payload_codec,
            )
            .canonical_bytes(),
        );

        for crossed in [
            ObjectEncryptionSubject::standard_object(
                18,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &other_bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &other_key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(24).unwrap(),
                &encryption,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &other_encryption,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &same_nonce_other_wrapping_key,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &same_nonce_other_wrapped_dek,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &same_nonce_other_segment_nonce_prefix,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &same_wrap_nonce_other_instance,
            ),
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &crossed_mode_encryption,
            ),
        ] {
            assert_ne!(crossed, subject);
        }
    }

    #[test]
    fn object_encryption_purpose_subjects_have_exact_separated_encodings_and_binding() {
        let bucket = BucketName::try_from("purpose-bucket").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let encryption = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [11; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let same_instance_other_wrapped_dek = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [13; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let crossed_key = ObjectKey::try_from("path/to/crossed").unwrap();
        let object = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let crossed_object = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &crossed_key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let same_instance_crossed_binding = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &same_instance_other_wrapped_dek,
        );

        let mut expected_object = vec![32];
        expected_object.extend_from_slice(b"argmin:object-encryption-subject");
        let mut historical_v1_object = vec![32];
        historical_v1_object.extend_from_slice(b"argmin:object-encryption-subject");
        historical_v1_object.extend_from_slice(&[1, 2, 13, 2]);
        historical_v1_object.extend_from_slice(&[10; 12]);
        historical_v1_object.extend_from_slice(&17_u64.to_be_bytes());
        historical_v1_object.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1_object.extend_from_slice(b"purpose-bucket");
        historical_v1_object.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1_object.extend_from_slice(b"path/to/object");
        historical_v1_object.extend_from_slice(&23_u64.to_be_bytes());
        assert!(
            ObjectEncryptionSubject::validate_canonical_for_test(&historical_v1_object).is_err()
        );

        expected_object.extend_from_slice(&[2, 2, 17, 2]);
        expected_object.extend_from_slice(&[10; 12]);
        expected_object.extend_from_slice(&[0; 4]);
        expected_object.extend_from_slice(&17_u64.to_be_bytes());
        expected_object.extend_from_slice(&14_u16.to_be_bytes());
        expected_object.extend_from_slice(b"purpose-bucket");
        expected_object.extend_from_slice(&14_u16.to_be_bytes());
        expected_object.extend_from_slice(b"path/to/object");
        expected_object.extend_from_slice(&23_u64.to_be_bytes());

        let wrap = object.dek_wrap_subject();
        let checksum = object.checksum_subject();
        let mut expected_wrap = expected_object.clone();
        // Immutable V1 evidence: purpose-subject version 1, DEK-wrap tag 1.
        // Keep these literal bytes independent of the production constants so
        // changing either discriminator requires an explicit format decision.
        expected_wrap.extend_from_slice(&[1, 1]);
        let mut expected_checksum = expected_object;
        // Immutable V1 evidence: purpose-subject version 1,
        // checksum-metadata tag 2.
        expected_checksum.extend_from_slice(&[1, 2]);
        let mut historical_v1_wrap = historical_v1_object.clone();
        historical_v1_wrap.extend_from_slice(&[1, 1]);
        let mut historical_v1_checksum = historical_v1_object;
        historical_v1_checksum.extend_from_slice(&[1, 2]);
        assert_eq!(wrap.aead_context(), expected_wrap);
        assert_eq!(checksum.aead_context(), expected_checksum);
        assert_ne!(wrap.aead_context(), historical_v1_wrap);
        assert_ne!(checksum.aead_context(), historical_v1_checksum);
        assert_ne!(wrap.aead_context(), checksum.aead_context());
        assert!(wrap.matches_encryption(&encryption));
        assert!(checksum.matches_encryption(&encryption));
        assert!(!wrap.matches_encryption(&same_instance_other_wrapped_dek));
        assert!(!checksum.matches_encryption(&same_instance_other_wrapped_dek));
        assert_ne!(
            wrap.aead_context(),
            crossed_object.dek_wrap_subject().aead_context()
        );

        // Wrapped-DEK bytes cannot be included in the AAD used to create
        // those same bytes. The opaque subject nevertheless retains the
        // complete immutable binding and rejects a same-instance
        // substitution before the encryption boundary consumes its context.
        assert_eq!(
            wrap.aead_context(),
            same_instance_crossed_binding
                .dek_wrap_subject()
                .aead_context()
        );

        let segment = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            3,
            29,
        );
        assert_eq!(segment.dek_wrap_subject().aead_context(), expected_wrap);
        assert_eq!(segment.checksum_subject().aead_context(), expected_checksum);
    }

    #[test]
    fn cobblestone_segment_profile_has_exact_argmin_context_vector() {
        let bucket = BucketName::try_from("context-bucket").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let encryption = ObjectEncryption::SseS3(
            SseS3ObjectState::new(
                9,
                [10; SSE_S3_WRAP_NONCE_LEN],
                [11; SSE_S3_WRAPPED_DEK_LEN],
                [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_payload_codec(SegmentPayloadCodec::Cobblestone256SegmentV1),
        );
        let object = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let subject = SegmentEncryptionSubject::standard_object(object, 3, 29);
        let plaintext = b"argmin segment context vector";
        let key = argmin_crypto::cobblestone::Cobblestone256Key::new(&[0x42; 32]).unwrap();
        let mut historical_v1_context = vec![32];
        historical_v1_context.extend_from_slice(b"argmin:object-encryption-subject");
        historical_v1_context.extend_from_slice(&[1, 2, 13, 2]);
        historical_v1_context.extend_from_slice(&[10; 12]);
        historical_v1_context.extend_from_slice(&17_u64.to_be_bytes());
        historical_v1_context.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1_context.extend_from_slice(b"context-bucket");
        historical_v1_context.extend_from_slice(&14_u16.to_be_bytes());
        historical_v1_context.extend_from_slice(b"path/to/object");
        historical_v1_context.extend_from_slice(&23_u64.to_be_bytes());
        historical_v1_context.extend_from_slice(&[1, 1]);
        historical_v1_context.extend_from_slice(&3_u32.to_be_bytes());
        historical_v1_context.extend_from_slice(&29_u64.to_be_bytes());
        let historical_v1_ciphertext = [
            36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
            36, 36, 171, 176, 171, 249, 64, 227, 60, 208, 170, 99, 22, 105, 185, 161, 189, 177,
            193, 53, 101, 36, 112, 163, 238, 48, 85, 101, 81, 240, 226, 212, 44, 21, 214, 12, 182,
            223, 158, 239, 64, 196, 84, 14, 197, 228, 22, 243, 140, 188, 226, 32, 75, 114, 49, 38,
            248, 129, 88, 24, 185, 181, 34, 32, 170, 170, 181, 193, 183, 147, 247, 188, 155, 23,
            12, 69, 234, 108, 25,
        ];
        assert_eq!(
            argmin_crypto::cobblestone::open(
                argmin_crypto::cobblestone::Cobblestone256Key::new(&[0x42; 32]).unwrap(),
                &historical_v1_context,
                &historical_v1_ciphertext,
            )
            .unwrap(),
            plaintext,
        );
        assert!(argmin_crypto::cobblestone::open(
            argmin_crypto::cobblestone::Cobblestone256Key::new(&[0x42; 32]).unwrap(),
            subject.canonical_bytes(),
            &historical_v1_ciphertext,
        )
        .is_err());
        let ciphertext = argmin_crypto::cobblestone::seal(
            &key,
            [0x24; argmin_crypto::cobblestone::COBBLESTONE_SALT_LEN],
            subject.canonical_bytes(),
            plaintext,
        )
        .unwrap();

        assert_eq!(
            ciphertext,
            [
                36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
                36, 36, 36, 238, 121, 27, 208, 160, 71, 77, 208, 212, 232, 76, 174, 19, 175, 104,
                42, 125, 226, 142, 52, 36, 9, 221, 183, 172, 226, 188, 94, 111, 7, 42, 84, 235,
                101, 76, 242, 114, 202, 7, 218, 227, 157, 223, 11, 196, 60, 63, 223, 117, 245, 114,
                232, 59, 167, 38, 226, 12, 153, 184, 208, 194, 37, 240, 216, 199, 167, 67, 149,
                190, 245, 130, 212, 29, 67, 67, 62, 203,
            ]
        );
        assert_eq!(
            argmin_crypto::cobblestone::open(
                argmin_crypto::cobblestone::Cobblestone256Key::new(&[0x42; 32]).unwrap(),
                subject.canonical_bytes(),
                &ciphertext,
            )
            .unwrap(),
            plaintext,
        );

        let crossed = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &ObjectKey::try_from("path/to/crossed").unwrap(),
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            3,
            29,
        );
        assert!(argmin_crypto::cobblestone::open(
            argmin_crypto::cobblestone::Cobblestone256Key::new(&[0x42; 32]).unwrap(),
            crossed.canonical_bytes(),
            &ciphertext,
        )
        .is_err());
    }

    #[test]
    fn object_encryption_subject_rejects_noncanonical_encodings() {
        let encryption = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let subject = ObjectEncryptionSubject::standard_object(
            17,
            &BucketName::try_from("subject-bucket").unwrap(),
            &ObjectKey::try_from("path/to/object").unwrap(),
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let canonical = subject.canonical_bytes();
        let domain_end = 1 + usize::from(canonical[0]);

        let mut unsupported_version = canonical.to_vec();
        unsupported_version[domain_end] = 1;
        let mut unsupported_profile = canonical.to_vec();
        unsupported_profile[domain_end + 1] = 3;
        let mut invalid_instance_len = canonical.to_vec();
        invalid_instance_len[domain_end + 2] = 12;
        let mut invalid_incarnation = canonical.to_vec();
        invalid_incarnation[domain_end + 3 + OBJECT_ENCRYPTION_INSTANCE_ID_LEN
            ..domain_end + 3 + OBJECT_ENCRYPTION_INSTANCE_ID_LEN + 8]
            .fill(0);
        let mut invalid_bucket_len = canonical.to_vec();
        let bucket_len_offset = domain_end + 3 + OBJECT_ENCRYPTION_INSTANCE_ID_LEN + 8;
        invalid_bucket_len[bucket_len_offset..bucket_len_offset + 2]
            .copy_from_slice(&u16::MAX.to_be_bytes());
        let mut invalid_generation = canonical.to_vec();
        let generation_offset = invalid_generation.len() - 8;
        invalid_generation[generation_offset..].fill(0);
        let mut trailing = canonical.to_vec();
        trailing.push(0);

        for malformed in [
            unsupported_version,
            unsupported_profile,
            invalid_instance_len,
            invalid_incarnation,
            invalid_bucket_len,
            invalid_generation,
            trailing,
        ] {
            assert!(ObjectEncryptionSubject::validate_canonical_for_test(&malformed).is_err());
        }
    }

    #[test]
    fn standard_object_segment_subject_extends_exact_object_and_geometry() {
        let bucket = BucketName::try_from("segment-subject-bucket").unwrap();
        let key = ObjectKey::try_from("path/to/segment").unwrap();
        let encryption = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let object = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let mut expected = object.canonical_bytes().to_vec();
        expected.extend_from_slice(&[SEGMENT_ENCRYPTION_SUBJECT_VERSION]);
        expected.extend_from_slice(&[STANDARD_OBJECT_SEGMENT_PROVENANCE_TAG]);
        expected.extend_from_slice(&3_u32.to_be_bytes());
        expected.extend_from_slice(&65_537_u64.to_be_bytes());
        let subject = SegmentEncryptionSubject::standard_object(object, 3, 65_537);

        assert_eq!(subject.canonical_bytes(), expected);
        assert!(subject.matches_standard_object_segment(&encryption, 3, 65_537));
        assert!(subject.matches_encryption_and_geometry(&encryption, 3, 65_537));
        assert!(!subject.matches_standard_object_segment(&encryption, 2, 65_537));
        assert!(!subject.matches_encryption_and_geometry(&encryption, 2, 65_537));
        assert!(!subject.matches_standard_object_segment(&encryption, 3, 65_536));
        assert!(!subject.matches_encryption_and_geometry(&encryption, 3, 65_536));

        let same_nonce_other_wrapped_dek = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [14; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        assert!(!subject.matches_standard_object_segment(&same_nonce_other_wrapped_dek, 3, 65_537,));

        let multipart_subject = SegmentEncryptionSubject::multipart_part(
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            &UploadId::try_from("01".repeat(UPLOAD_ID_LEN / 2)).unwrap(),
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        );
        assert!(!multipart_subject.matches_standard_object_segment(&encryption, 3, 65_537));
        assert!(multipart_subject.matches_encryption_and_geometry(&encryption, 3, 65_537));

        for crossed in [
            SegmentEncryptionSubject::standard_object(
                ObjectEncryptionSubject::standard_object(
                    17,
                    &bucket,
                    &key,
                    GenerationId::new(23).unwrap(),
                    &encryption,
                ),
                2,
                65_537,
            ),
            SegmentEncryptionSubject::standard_object(
                ObjectEncryptionSubject::standard_object(
                    17,
                    &bucket,
                    &key,
                    GenerationId::new(23).unwrap(),
                    &encryption,
                ),
                3,
                65_536,
            ),
        ] {
            assert_ne!(crossed, subject);
        }
    }

    #[test]
    fn multipart_part_segment_subject_extends_exact_upload_part_generation_and_geometry() {
        let bucket = BucketName::try_from("multipart-subject-bucket").unwrap();
        let key = ObjectKey::try_from("path/to/multipart").unwrap();
        let upload_id = UploadId::try_from("01".repeat(UPLOAD_ID_LEN / 2)).unwrap();
        let other_upload_id = UploadId::try_from("fe".repeat(UPLOAD_ID_LEN / 2)).unwrap();
        let encryption = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        let object = ObjectEncryptionSubject::standard_object(
            17,
            &bucket,
            &key,
            GenerationId::new(23).unwrap(),
            &encryption,
        );
        let mut expected = object.canonical_bytes().to_vec();
        expected.push(SEGMENT_ENCRYPTION_SUBJECT_VERSION);
        expected.push(MULTIPART_PART_SEGMENT_PROVENANCE_TAG);
        expected.extend_from_slice(
            &u16::try_from(UPLOAD_ID_LEN)
                .expect("upload ID length fits u16")
                .to_be_bytes(),
        );
        expected.extend_from_slice(upload_id.as_str().as_bytes());
        expected.extend_from_slice(&7_u32.to_be_bytes());
        expected.extend_from_slice(&29_u64.to_be_bytes());
        expected.extend_from_slice(&3_u32.to_be_bytes());
        expected.extend_from_slice(&65_537_u64.to_be_bytes());
        let subject = SegmentEncryptionSubject::multipart_part(
            object,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        );

        assert_eq!(subject.canonical_bytes(), expected);
        assert!(subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        ));
        assert!(!subject.matches_multipart_part_segment(
            &encryption,
            &other_upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        ));
        assert!(!subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            8,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        ));
        assert!(!subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            GenerationId::new(30).unwrap(),
            3,
            65_537,
        ));
        assert!(!subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            4,
            65_537,
        ));
        assert!(!subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_536,
        ));
        let same_nonce_other_wrapped_dek = ObjectEncryption::SseS3(SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [14; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        ));
        assert!(!subject.matches_multipart_part_segment(
            &same_nonce_other_wrapped_dek,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        ));

        let standard_subject = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                17,
                &bucket,
                &key,
                GenerationId::new(23).unwrap(),
                &encryption,
            ),
            3,
            65_537,
        );
        assert!(!standard_subject.matches_multipart_part_segment(
            &encryption,
            &upload_id,
            7,
            GenerationId::new(29).unwrap(),
            3,
            65_537,
        ));
    }

    #[test]
    fn object_encryption_checksum_metadata_is_bounded_at_construction() {
        let customer = SseCustomerObjectState::new(
            7,
            [1; SSE_C_VALIDATOR_SALT_LEN],
            [2; SSE_C_VALIDATOR_HMAC_LEN],
            [3; SSE_C_WRAP_SALT_LEN],
            [4; SSE_C_WRAP_NONCE_LEN],
            [5; SSE_C_WRAPPED_DEK_LEN],
            [6; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
        );
        let managed = SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        );
        let maximum = vec![0; usize::from(u16::MAX)];
        assert!(customer
            .with_encrypted_checksum_metadata([7; SSE_C_CHECKSUM_NONCE_LEN], maximum.clone())
            .is_ok());
        assert!(managed
            .with_encrypted_checksum_metadata([13; SSE_S3_CHECKSUM_NONCE_LEN], maximum)
            .is_ok());

        let overlong = vec![0; usize::from(u16::MAX) + 1];
        assert_eq!(
            customer
                .with_encrypted_checksum_metadata([7; SSE_C_CHECKSUM_NONCE_LEN], overlong.clone(),)
                .unwrap_err(),
            ObjectEncryptionStateError::EncryptedChecksumMetadataTooLong
        );
        assert_eq!(
            managed
                .with_encrypted_checksum_metadata([13; SSE_S3_CHECKSUM_NONCE_LEN], overlong)
                .unwrap_err(),
            ObjectEncryptionStateError::EncryptedChecksumMetadataTooLong
        );
    }

    #[test]
    fn object_encryption_empty_checksum_metadata_requires_zero_nonce() {
        let customer = SseCustomerObjectState::new(
            7,
            [1; SSE_C_VALIDATOR_SALT_LEN],
            [2; SSE_C_VALIDATOR_HMAC_LEN],
            [3; SSE_C_WRAP_SALT_LEN],
            [4; SSE_C_WRAP_NONCE_LEN],
            [5; SSE_C_WRAPPED_DEK_LEN],
            [6; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
        );
        assert_eq!(
            customer
                .with_encrypted_checksum_metadata([1; SSE_C_CHECKSUM_NONCE_LEN], Vec::new())
                .unwrap_err(),
            ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce
        );
        let mut encoded_customer = customer.encode();
        let customer_nonce_start =
            SseCustomerObjectState::FIXED_ENCODED_LEN - 2 - SSE_C_CHECKSUM_NONCE_LEN;
        encoded_customer[customer_nonce_start] = 1;
        assert_eq!(
            SseCustomerObjectState::decode(&encoded_customer).unwrap_err(),
            ObjectEncryptionDecodeError::NoncanonicalSseCustomerEmptyChecksumNonce
        );

        let managed = SseS3ObjectState::new(
            9,
            [10; SSE_S3_WRAP_NONCE_LEN],
            [11; SSE_S3_WRAPPED_DEK_LEN],
            [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        );
        assert_eq!(
            managed
                .with_encrypted_checksum_metadata([1; SSE_S3_CHECKSUM_NONCE_LEN], Vec::new())
                .unwrap_err(),
            ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce
        );
        let mut encoded_managed = managed.encode();
        let managed_nonce_start =
            SseS3ObjectState::FIXED_ENCODED_LEN - 2 - SSE_S3_CHECKSUM_NONCE_LEN;
        encoded_managed[managed_nonce_start] = 1;
        assert_eq!(
            SseS3ObjectState::decode(&encoded_managed).unwrap_err(),
            ObjectEncryptionDecodeError::NoncanonicalSseS3EmptyChecksumNonce
        );
    }

    #[test]
    fn object_encryption_decode_reports_typed_errors() {
        assert_eq!(ObjectEncryptionType::from_u8(0), None);
        assert_eq!(
            ObjectEncryptionType::from_u8(1),
            Some(ObjectEncryptionType::SseCustomer)
        );
        assert_eq!(
            ObjectEncryptionType::from_u8(2),
            Some(ObjectEncryptionType::SseS3)
        );
        assert_eq!(ObjectEncryptionType::from_u8(3), None);
        assert_eq!(ObjectEncryptionType::from_u8(u8::MAX), None);

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

        let state = SseS3ObjectState::new(
            7,
            [1; SSE_S3_WRAP_NONCE_LEN],
            [2; SSE_S3_WRAPPED_DEK_LEN],
            [3; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        );
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

        let customer_state = SseCustomerObjectState::new(
            17,
            [1; SSE_C_VALIDATOR_SALT_LEN],
            [2; SSE_C_VALIDATOR_HMAC_LEN],
            [3; SSE_C_WRAP_SALT_LEN],
            [4; SSE_C_WRAP_NONCE_LEN],
            [5; SSE_C_WRAPPED_DEK_LEN],
            [6; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
        );
        let mut encoded = customer_state.encode();
        encoded[0] = 99;
        assert_eq!(
            SseCustomerObjectState::decode(&encoded).unwrap_err(),
            ObjectEncryptionDecodeError::UnsupportedSseCustomerStateVersion { version: 99 }
        );

        let mut encoded = customer_state.encode();
        let checksum_len_offset = SseCustomerObjectState::FIXED_ENCODED_LEN - 2;
        encoded[checksum_len_offset..checksum_len_offset + 2].copy_from_slice(&1u16.to_be_bytes());
        assert_eq!(
            SseCustomerObjectState::decode(&encoded).unwrap_err(),
            ObjectEncryptionDecodeError::InvalidSseCustomerChecksumMetadataLength {
                declared: 1,
                remaining: 0,
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
        let tags = SerializedTagSet::new(
            "<Tagging><TagSet><Tag><Key>secret</Key><Value>tag</Value></Tag></TagSet></Tagging>"
                .to_string(),
        );
        let metadata_debug = format!("{metadata:?}");
        let system_debug = format!("{system_metadata:?}");
        let tags_debug = format!("{tags:?}");
        assert!(metadata_debug.contains("len"));
        assert!(system_debug.contains("len"));
        assert!(tags_debug.contains("tag_count"));
        assert!(!metadata_debug.contains("top-secret-metadata"));
        assert!(!system_debug.contains("checksum-secret"));
        assert!(!tags_debug.contains("secret-tag"));

        let encryption = ObjectEncryption::SseCustomer(
            SseCustomerObjectState::new(
                42,
                [1; SSE_C_VALIDATOR_SALT_LEN],
                [2; SSE_C_VALIDATOR_HMAC_LEN],
                [3; SSE_C_WRAP_SALT_LEN],
                [4; SSE_C_WRAP_NONCE_LEN],
                [5; SSE_C_WRAPPED_DEK_LEN],
                [6; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
            )
            .with_encrypted_checksum_metadata([7; SSE_C_CHECKSUM_NONCE_LEN], vec![8, 9, 10])
            .unwrap(),
        );
        let debug = format!("{encryption:?}");
        assert!(debug.contains("<redacted:sse_customer_state>"));
        assert!(!debug.contains("wrapped_dek"));
        assert!(!debug.contains("validator_hmac"));
    }

    #[test]
    fn stored_object_tags_accept_only_the_exact_current_validated_representation() {
        let tags = s3_types::TagSet::from_pairs(
            vec![("key".to_string(), "value".to_string())],
            s3_types::MAX_OBJECT_TAGS,
        )
        .unwrap();
        let stored = SerializedTagSet::from_tag_set(tags.clone()).unwrap();
        assert_eq!(stored.tag_set(), &tags);
        assert_eq!(
            SerializedTagSet::from_current_storage(stored.as_str().to_string()).unwrap(),
            stored
        );
        assert!(matches!(
            SerializedTagSet::from_current_storage(
                "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
                    .to_string()
            ),
            Err(s3_types::StoredTagSetParseError::Frame(
                s3_types::StoredTextFrameParseError::UnknownMagic
            ))
        ));

        let too_many = s3_types::TagSet::from_pairs(
            (0..=s3_types::MAX_OBJECT_TAGS)
                .map(|index| (format!("key-{index}"), "value".to_string()))
                .collect(),
            s3_types::MAX_OBJECT_TAGS + 1,
        )
        .unwrap();
        assert!(matches!(
            SerializedTagSet::from_tag_set(too_many),
            Err(s3_types::TagSetValidationError::TooMany {
                actual: 11,
                maximum: s3_types::MAX_OBJECT_TAGS,
            })
        ));
    }

    #[test]
    fn stored_bucket_tags_accept_only_the_exact_current_validated_representation() {
        let tags = s3_types::TagSet::from_pairs(
            vec![("key".to_string(), "value".to_string())],
            s3_types::MAX_BUCKET_TAGS,
        )
        .unwrap();
        let stored = SerializedBucketTagSet::from_tag_set(tags.clone()).unwrap();
        assert_eq!(stored.tag_set(), &tags);
        assert_eq!(
            stored.as_str(),
            "ARGMIN-TAGSET/1\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
        );
        assert_eq!(
            SerializedBucketTagSet::from_current_storage(stored.as_str().to_string()).unwrap(),
            stored
        );
        assert!(matches!(
            SerializedBucketTagSet::from_current_storage(
                "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
                    .to_string()
            ),
            Err(s3_types::StoredTagSetParseError::Frame(
                s3_types::StoredTextFrameParseError::UnknownMagic
            ))
        ));

        let too_many = s3_types::TagSet::from_pairs(
            (0..=s3_types::MAX_BUCKET_TAGS)
                .map(|index| (format!("key-{index}"), "value".to_string()))
                .collect(),
            s3_types::MAX_BUCKET_TAGS + 1,
        )
        .unwrap();
        assert!(matches!(
            SerializedBucketTagSet::from_tag_set(too_many),
            Err(s3_types::TagSetValidationError::TooMany {
                actual: 51,
                maximum: s3_types::MAX_BUCKET_TAGS,
            })
        ));
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

        let snapshot = BucketSnapshot::from_authoritative_load(
            info,
            BucketSnapshotRequest {
                policy: true,
                tags: BucketSnapshotTagsRequest::Always,
                lifecycle: true,
                cors: false,
            },
            LoadedBucketSubresource::Loaded("secret-policy".to_string()),
            LoadedBucketSubresource::Loaded(SerializedBucketTagSet::new(
                "<Tagging><TagSet><Tag><Key>secret</Key><Value>tags</Value></Tag></TagSet></Tagging>".to_string(),
            )),
            LoadedBucketSubresource::Loaded(
                "<LifecycleConfiguration>secret</LifecycleConfiguration>".to_string(),
            ),
            LoadedBucketSubresource::NotRequested,
        );
        let fast_debug = format!("{:?}", BucketFastPathInfo::from(&snapshot));
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
    fn upload_id_test_helpers_produce_logical_valid_and_overlong_values() {
        let first = UploadId::for_test("first");
        let second = UploadId::for_test("second");
        assert_ne!(first, second);
        assert!(matches!(
            UploadId::try_from(UploadId::overlong_for_test()),
            Err(UploadIdError::InvalidLength { .. })
        ));
    }

    #[test]
    fn multipart_upload_id_key_binds_issued_id_to_bucket_and_key() {
        let signing_key = MultipartUploadIdKey::from_bytes([0x5a; 32]);
        let bucket = BucketName::try_from("bucket-one").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let upload_id = signing_key.issue(&bucket, &key, "primary").unwrap();

        assert_eq!(
            MultipartUploadIdKey::listing_position(&upload_id),
            Some((0, 0))
        );
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

        let ordered_upload_id = signing_key.with_listing_position(&bucket, &key, &upload_id, 7, 42);
        assert_eq!(
            MultipartUploadIdKey::listing_position(&ordered_upload_id),
            Some((7, 42))
        );
        assert!(MultipartUploadIdKey::has_same_issuance_identity(
            &upload_id,
            &ordered_upload_id
        ));
        assert!(signing_key.authenticates(&bucket, &key, &ordered_upload_id));
        assert!(signing_key.was_issued_for_principal(&ordered_upload_id, "primary"));
        let mut mutated_sequence = ordered_upload_id.as_str().as_bytes().to_vec();
        mutated_sequence[31] = b'3';
        let mutated_sequence =
            UploadId::try_from(String::from_utf8(mutated_sequence).unwrap()).unwrap();
        assert_ne!(
            MultipartUploadIdKey::listing_position(&mutated_sequence),
            Some((7, 42))
        );
        assert!(!signing_key.authenticates(&bucket, &key, &mutated_sequence));
    }

    #[test]
    fn multipart_upload_id_authority_exposes_only_logical_id_checks() {
        let signing_key = MultipartUploadIdKey::from_bytes([1; MULTIPART_UPLOAD_ID_KEY_LEN]);
        let authority = MultipartUploadIdAuthority::for_test();
        let bucket = BucketName::try_from("bucket-one").unwrap();
        let key = ObjectKey::try_from("path/to/object").unwrap();
        let upload_id = signing_key.issue(&bucket, &key, "primary").unwrap();

        assert!(authority.authenticates(&bucket, &key, &upload_id));
        assert!(authority.was_issued_for_principal(&upload_id, "primary"));
        assert!(!authority.was_issued_for_principal(&upload_id, "alternate"));
        assert_eq!(
            format!("{authority:?}"),
            "MultipartUploadIdAuthority(<opaque>)"
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
    fn bucket_delete_diagnostic_rendering_is_storage_owned_and_exact() {
        let bucket = BucketName::try_from("debug-attempt-bucket").unwrap();
        let snapshot = BucketDeleteDebugSnapshot {
            bucket: bucket.clone(),
            pg_id: 3,
            cluster_epoch: ClusterEpoch::new(9).unwrap(),
            operation_epoch: ClusterEpoch::new(7).unwrap(),
            route_map_valid_until_ms: Some(123_456),
            bucket_pg_primary_node_id: 42,
            bucket_row: Some(BucketDeleteDebugBucketRow {
                state: BucketState::Deleting,
                bucket_execution_generation: 12,
                bucket_incarnation_generation: 34,
            }),
            durable_write_drain: Some(BucketDeleteDebugDrain {
                drain_id: "delete-drain-1".to_string(),
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                bucket_execution_generation: 11,
                created_at: 1_000,
                lease_deadline: 2_000,
            }),
            pending_metadata_command: Some(BucketDeleteDebugPendingCommand {
                kind: "mark_bucket_deleting",
                target_bucket: bucket.clone(),
                matches_bucket: true,
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                pg_id: 3,
                log_index: 88,
            }),
            finalize_claim: Some(BucketDeleteDebugFinalizeClaim {
                bucket: bucket.clone(),
                matches_bucket: true,
                bucket_incarnation_generation: 34,
                claim_id: "finalize-claim-1".to_string(),
                cluster_epoch: ClusterEpoch::new(8).unwrap(),
                pg_id: 3,
                claimed_at: 1_010,
                lease_deadline: Some(2_020),
                attempt_count: 2,
                last_error: Some("retry\nlater".to_string()),
            }),
            object_version_samples: vec![
                BucketDeleteDebugObjectVersionSample {
                    object_pg_id: 6,
                    kind: BucketDeleteDebugObjectVersionKind::Live,
                    key: ObjectKey::try_from("live-key\none").unwrap(),
                    version_id: VersionId::Versioned(std::num::NonZeroU64::new(77).unwrap()),
                    generation_id: Some(GenerationId::new(123).unwrap()),
                    size: Some(456),
                    layout: Some(ObjectLayout::MultipartManifest {
                        parts_count: std::num::NonZeroU32::new(3).unwrap(),
                    }),
                    last_modified: 3_030,
                    became_noncurrent_at: Some(4_040),
                },
                BucketDeleteDebugObjectVersionSample {
                    object_pg_id: 7,
                    kind: BucketDeleteDebugObjectVersionKind::DeleteMarker,
                    key: ObjectKey::try_from("marker-key").unwrap(),
                    version_id: VersionId::Null,
                    generation_id: None,
                    size: None,
                    layout: None,
                    last_modified: 5_050,
                    became_noncurrent_at: None,
                },
            ],
            object_version_sample_errors: vec![BucketDeleteDebugObjectVersionSampleError {
                object_pg_id: 8,
                detail: "object route\nexpired".to_string(),
            }],
            payload_reclaim_roots: vec![BucketDeleteDebugPayloadReclaimRoot {
                object_pg_id: 4,
                key: ObjectKey::try_from("root-key\none").unwrap(),
                generation_id: GenerationId::new(99).unwrap(),
                reclaim_kind: Some(ObjectPayloadReclaimKind::ObjectSegments),
                reclaim_created_at: Some(9_090),
                reclaim_item_count: Some(2),
            }],
            payload_reclaim_root_errors: vec![BucketDeleteDebugPayloadReclaimRootError {
                object_pg_id: 5,
                detail: "route\nexpired".to_string(),
            }],
            payload_reclaim_claims: vec![BucketDeleteDebugPayloadReclaimClaim {
                object_pg_id: 9,
                bucket: bucket.clone(),
                matches_bucket: true,
                bucket_incarnation_generation: 34,
                key: ObjectKey::try_from("claim-key\none").unwrap(),
                generation_id: GenerationId::new(101).unwrap(),
                reclaim_kind: ObjectPayloadReclaimKind::Multipart,
                claim_id: "payload-claim-1".to_string(),
                cluster_epoch: ClusterEpoch::new(10).unwrap(),
                claimed_at: 6_060,
                lease_deadline: Some(7_070),
                attempt_count: 3,
                last_error: Some("payload\nbusy".to_string()),
            }],
            payload_reclaim_claim_errors: vec![BucketDeleteDebugPayloadReclaimClaimError {
                object_pg_id: 10,
                detail: "claim route\nexpired".to_string(),
            }],
            attempt_outcome: Some(BucketDeleteAttemptOutcomeRecord {
                bucket,
                drain_id: "delete-drain-1".to_string(),
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                bucket_execution_generation: 11,
                outcome: BucketDeleteAttemptOutcomeKind::Retryable,
                phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
                detail: "line1\nline2\t\x1b[31m".to_string(),
                post_reservation_next_object_pg_id: Some(17),
                stream_cleanup_next_object_pg_id: Some(18),
                stream_cleanup_next_session_id_marker: Some(
                    SessionId::try_from("ab".repeat(16)).unwrap(),
                ),
                stream_cleanup_aborted_uploads: true,
                final_visibility_next_object_pg_id: Some(19),
                finalizer_next_object_pg_id: Some(19),
                updated_at: 12_345,
            }),
        };

        let diagnostic = BucketDeleteDiagnostic::from_snapshot(&snapshot).into_text();
        assert_eq!(
            diagnostic,
            concat!(
                "bucket=\"debug-attempt-bucket\" pg_id=3 cluster_epoch=9 operation_epoch=7 route_map_valid_until_ms=123456 bucket_pg_primary_node_id=42\n",
                "bucket_row=present state=deleting bucket_execution_generation=12 bucket_incarnation_generation=34\n",
                "durable_write_drain=present drain_id=\"delete-drain-1\" cluster_epoch=7 bucket_execution_generation=11 created_at=1000 lease_deadline=2000\n",
                "pending_metadata_command=present kind=mark_bucket_deleting target_bucket=\"debug-attempt-bucket\" matches_bucket=true cluster_epoch=7 pg_id=3 log_index=88\n",
                "finalize_claim=present bucket=\"debug-attempt-bucket\" matches_bucket=true bucket_incarnation_generation=34 claim_id=\"finalize-claim-1\" cluster_epoch=8 pg_id=3 claimed_at=1010 lease_deadline=2020 attempt_count=2 last_error=\"retry\\nlater\"\n",
                "object_version_samples_count=2\n",
                "object_version_sample index=0 object_pg_id=6 kind=live key=\"live-key\\none\" version_id=77 generation_id=123 size=456 layout=multipart_manifest:3 last_modified=3030 became_noncurrent_at=4040\n",
                "object_version_sample index=1 object_pg_id=7 kind=delete_marker key=\"marker-key\" version_id=null generation_id=none size=none layout=none last_modified=5050 became_noncurrent_at=none\n",
                "object_version_sample_errors_count=1\n",
                "object_version_sample_error index=0 object_pg_id=8 detail=\"object route\\nexpired\"\n",
                "payload_reclaim_roots_count=1\n",
                "payload_reclaim_root index=0 object_pg_id=4 key=\"root-key\\none\" generation_id=99 reclaim_kind=object_segments reclaim_created_at=9090 reclaim_item_count=2\n",
                "payload_reclaim_root_errors_count=1\n",
                "payload_reclaim_root_error index=0 object_pg_id=5 detail=\"route\\nexpired\"\n",
                "payload_reclaim_claims_count=1\n",
                "payload_reclaim_claim index=0 object_pg_id=9 bucket=\"debug-attempt-bucket\" matches_bucket=true bucket_incarnation_generation=34 key=\"claim-key\\none\" generation_id=101 reclaim_kind=multipart claim_id=\"payload-claim-1\" cluster_epoch=10 claimed_at=6060 lease_deadline=7070 attempt_count=3 last_error=\"payload\\nbusy\"\n",
                "payload_reclaim_claim_errors_count=1\n",
                "payload_reclaim_claim_error index=0 object_pg_id=10 detail=\"claim route\\nexpired\"\n",
                "attempt_outcome=present present=1 drain_id=\"delete-drain-1\" cluster_epoch=7 bucket_execution_generation=11 outcome=retryable phase=post_reservation_object_drain post_reservation_next_object_pg_id=17 finalizer_next_object_pg_id=19 updated_at=12345 detail=\"line1\\nline2\\t\\u{1b}[31m\"\n",
            )
        );
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
