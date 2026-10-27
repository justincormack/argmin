// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::ops::Range;

use argmin_crypto::aead::Aes256GcmKey;
use argmin_crypto::cobblestone::{
    self, Cobblestone256Key, Cobblestone256MessageLayout, Cobblestone256Opener,
    Cobblestone256SelectedRange, Cobblestone256StreamOpener, COBBLESTONE_HEADER_LEN,
    COBBLESTONE_SALT_LEN,
};
use argmin_crypto::hmac::Sha256Key;
#[cfg(test)]
use storage::SSE_S3_CHECKSUM_NONCE_LEN;
use storage::{
    ChecksumEncryptionSubject, DekWrapEncryptionSubject, ObjectEncryption,
    ObjectEncryptionKeyBinding, ObjectEncryptionStateError, ObjectEncryptionSubject,
    SegmentEncryptionSubject, SegmentPayloadCodec, SseCustomerObjectState, SseS3ObjectState,
    OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN, SSE_C_CHECKSUM_NONCE_LEN,
    SSE_C_SEGMENT_NONCE_PREFIX_LEN, SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_C_VALIDATOR_HMAC_LEN,
    SSE_C_VALIDATOR_SALT_LEN, SSE_C_WRAPPED_DEK_LEN, SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN,
    SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAP_NONCE_LEN,
};

use crate::error::ServerError;
use crate::system_metadata::ObjectChecksumMetadata;

pub const SSE_CUSTOMER_ALGORITHM: &str = "AES256";
pub const SSE_C_CUSTOMER_KEY_LEN: usize = 32;
pub const SSE_C_DEK_LEN: usize = 32;
const AES_256_GCM_TAG_LEN: usize = 16;
pub const SSE_C_SEGMENT_TAG_LEN: usize = AES_256_GCM_TAG_LEN;
const SSE_C_WRAP_AAD: &[u8] = b"argmin:sse-c:wrap:v1";
const SSE_C_SEGMENT_AAD: &[u8] = b"argmin:sse-c:segment:v1";
const SSE_C_CHECKSUM_AAD: &[u8] = b"argmin:sse-c:checksum:v1";
const SSE_C_HKDF_INFO: &[u8] = b"argmin:sse-c:kek:v1";
const CHECKSUM_METADATA_VERSION: u8 = 1;
const CHECKSUM_METADATA_HEADER_LEN: usize = 5;
const CHECKSUM_METADATA_MAX_VALUE_LEN: usize =
    65_535 - CHECKSUM_METADATA_HEADER_LEN - AES_256_GCM_TAG_LEN;
// Keep the persisted SSE-S3 wire format stable even though the internal
// provider boundary is now modelled as generic managed encryption.
const MANAGED_WRAP_AAD: &[u8] = b"argmin:sse-s3:wrap:v1";
const MANAGED_SEGMENT_AAD: &[u8] = b"argmin:sse-s3:segment:v1";
const MANAGED_CHECKSUM_AAD: &[u8] = b"argmin:sse-s3:checksum:v1";
const MANAGED_ENCRYPTION_LABEL: &str = "managed encryption";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum ChecksumMetadataCodecError {
    #[error("checksum metadata value length {actual} exceeds maximum {maximum}")]
    ValueTooLong { actual: usize, maximum: usize },
    #[error("truncated checksum metadata: length {actual}, minimum {minimum}")]
    Truncated { actual: usize, minimum: usize },
    #[error("unsupported checksum metadata version {version}")]
    UnsupportedVersion { version: u8 },
    #[error("invalid checksum algorithm tag {wire_tag}")]
    InvalidAlgorithmTag { wire_tag: u8 },
    #[error("invalid checksum type tag {wire_tag}")]
    InvalidChecksumTypeTag { wire_tag: u8 },
    #[error("invalid checksum metadata length {actual}; declared value length {declared}")]
    InvalidLength { declared: usize, actual: usize },
    #[error("checksum metadata value is not valid UTF-8")]
    InvalidUtf8,
}

fn checksum_metadata_codec_error(_error: ChecksumMetadataCodecError) -> ServerError {
    ServerError::InternalError {
        reason: "stored encrypted checksum metadata is invalid".to_string(),
    }
}

struct AeadDescriptor<'a> {
    aad: &'a [u8],
    label: &'a str,
}

struct SegmentSealParameters<'a> {
    payload_codec: SegmentPayloadCodec,
    segment_nonce_prefix: &'a [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    payload_aead_context: &'a [u8],
    segment_index: u32,
    descriptor: AeadDescriptor<'a>,
}

pub(crate) struct AuthorizedSegmentRead<'a> {
    subject: &'a SegmentEncryptionSubject,
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    plaintext_len: usize,
}

impl<'a> AuthorizedSegmentRead<'a> {
    pub(crate) fn new(
        subject: &'a SegmentEncryptionSubject,
        segment_scope: SseCustomerSegmentScope,
        segment_index: u32,
        plaintext_len: usize,
    ) -> Self {
        Self {
            subject,
            segment_scope,
            segment_index,
            plaintext_len,
        }
    }
}

/// Prepared key and immutable geometry for one authenticated Cobblestone
/// segment range.
///
/// Construction validates the storage-minted segment subject and unwraps the
/// exact object DEK before any partial shard IO occurs. The caller may expose
/// only the plaintext returned by [`Self::authenticate`].
pub(crate) struct AuthorizedCobblestoneSegmentRangeRead {
    key: Cobblestone256Key,
    payload_aead_context: Vec<u8>,
    selection: Cobblestone256SelectedRange,
}

/// Incremental, subject-bound opener for one complete Cobblestone segment.
///
/// The caller obtains the next exact stored-ciphertext window from this value,
/// reads it through storage's sequential physical-integrity capability, and
/// returns ownership to [`Self::authenticate_next_window`]. No plaintext from
/// a failing record is returned. The final window is not sufficient to
/// complete a response until the storage capability has also validated every
/// physical systematic-shard byte.
pub(crate) struct AuthorizedCobblestoneSegmentStreamRead {
    opener: Cobblestone256StreamOpener,
    layout: Cobblestone256MessageLayout,
    header_authenticated: bool,
    next_chunk: u64,
    plaintext_len: usize,
    authenticated_plaintext_len: usize,
    label: &'static str,
}

impl AuthorizedCobblestoneSegmentStreamRead {
    /// Select the next complete-record ciphertext window.
    ///
    /// `max_plaintext_bytes` is a soft bound: at least one record is always
    /// selected, and an immediately following empty terminal record is kept in
    /// the same window so its authentication completes before the preceding
    /// plaintext can be exposed.
    #[must_use]
    pub(crate) fn next_ciphertext_window(&self, max_plaintext_bytes: usize) -> Option<Range<u64>> {
        if !self.header_authenticated {
            return Some(0..COBBLESTONE_HEADER_LEN as u64);
        }
        if self.next_chunk >= self.layout.chunk_count() {
            return None;
        }
        let first = self
            .layout
            .ciphertext_range_for_chunk(self.next_chunk)
            .ok()?;
        let mut end = first.end;
        let first_plaintext = self
            .layout
            .plaintext_range_for_chunk(self.next_chunk)
            .ok()?;
        let mut selected_plaintext =
            usize::try_from(first_plaintext.end.checked_sub(first_plaintext.start)?).ok()?;
        let mut chunk = self.next_chunk + 1;
        while chunk < self.layout.chunk_count() {
            let plaintext = self.layout.plaintext_range_for_chunk(chunk).ok()?;
            let include_empty_terminal = plaintext.is_empty();
            let plaintext_len =
                usize::try_from(plaintext.end.checked_sub(plaintext.start)?).ok()?;
            let next_plaintext = selected_plaintext.checked_add(plaintext_len)?;
            if !include_empty_terminal && next_plaintext > max_plaintext_bytes.max(1) {
                break;
            }
            end = self.layout.ciphertext_range_for_chunk(chunk).ok()?.end;
            selected_plaintext = next_plaintext;
            chunk += 1;
            if include_empty_terminal {
                break;
            }
        }
        Some(first.start..end)
    }

    pub(crate) fn authenticate_next_window(
        &mut self,
        ciphertext: Vec<u8>,
    ) -> Result<Vec<u8>, ServerError> {
        let header = !self.header_authenticated;
        let opened_records = std::cell::Cell::new(0u64);
        let plaintext = self
            .opener
            .open_next_window_owned_with_observers(
                ciphertext,
                || {
                    observability::record_payload_read_metrics(
                        observability::PayloadReadMetricDelta {
                            aead_records: 1,
                            ..observability::PayloadReadMetricDelta::default()
                        },
                    );
                },
                |plaintext_len| {
                    opened_records.set(opened_records.get().saturating_add(1));
                    observability::record_payload_read_metrics(
                        observability::PayloadReadMetricDelta {
                            decrypted_bytes: u64::try_from(plaintext_len).unwrap_or(u64::MAX),
                            ..observability::PayloadReadMetricDelta::default()
                        },
                    );
                },
            )
            .map_err(|_| ServerError::InternalError {
                reason: format!("failed to decrypt {} Cobblestone segment", self.label),
            })?;
        if header {
            self.header_authenticated = true;
            debug_assert!(plaintext.is_empty());
            debug_assert_eq!(opened_records.get(), 0);
        } else {
            self.next_chunk = self
                .next_chunk
                .checked_add(opened_records.get())
                .ok_or_else(|| ServerError::InternalError {
                    reason: format!("{} Cobblestone record count overflowed", self.label),
                })?;
            self.authenticated_plaintext_len = self
                .authenticated_plaintext_len
                .checked_add(plaintext.len())
                .ok_or_else(|| ServerError::InternalError {
                    reason: format!(
                        "decrypted {} Cobblestone segment length overflowed",
                        self.label
                    ),
                })?;
        }
        Ok(plaintext)
    }

    #[must_use]
    pub(crate) fn input_complete(&self) -> bool {
        self.header_authenticated && self.next_chunk == self.layout.chunk_count()
    }

    pub(crate) fn finish(&mut self) -> Result<(), ServerError> {
        if !self.input_complete() || self.authenticated_plaintext_len != self.plaintext_len {
            return Err(ServerError::InternalError {
                reason: format!(
                    "decrypted {} Cobblestone segment length {} did not match expected {}",
                    self.label, self.authenticated_plaintext_len, self.plaintext_len
                ),
            });
        }
        self.opener
            .finish()
            .map_err(|_| ServerError::InternalError {
                reason: format!("failed to finish {} Cobblestone segment", self.label),
            })
    }
}

impl AuthorizedCobblestoneSegmentRangeRead {
    #[must_use]
    pub(crate) fn header_range(&self) -> Range<u64> {
        self.selection.header_range()
    }

    #[must_use]
    pub(crate) fn ciphertext_range(&self) -> Range<u64> {
        self.selection.ciphertext_range()
    }

    /// Authenticate every selected record in private scratch and return only
    /// the requested plaintext subrange. Authentication failure deliberately
    /// carries no partial plaintext so the caller can retry through the full
    /// witness-backed read path.
    pub(crate) fn authenticate(
        self,
        header: &[u8],
        selected_ciphertext: &[u8],
    ) -> Result<Vec<u8>, ()> {
        let opener =
            Cobblestone256Opener::from_header(&self.key, &self.payload_aead_context, header)
                .map_err(|_| ())?;
        opener
            .open_selected_range_with_observers(
                &self.selection,
                selected_ciphertext,
                || {
                    observability::record_payload_read_metrics(
                        observability::PayloadReadMetricDelta {
                            aead_records: 1,
                            ..observability::PayloadReadMetricDelta::default()
                        },
                    );
                },
                |plaintext_len| {
                    observability::record_payload_read_metrics(
                        observability::PayloadReadMetricDelta {
                            decrypted_bytes: u64::try_from(plaintext_len).unwrap_or(u64::MAX),
                            ..observability::PayloadReadMetricDelta::default()
                        },
                    );
                },
            )
            .map_err(|_| ())
    }
}

pub(crate) fn object_encryption_state_error(error: ObjectEncryptionStateError) -> ServerError {
    match error {
        ObjectEncryptionStateError::EncryptedChecksumMetadataTooLong => {
            ServerError::InternalError {
                reason: "encrypted checksum metadata exceeds the durable storage limit".to_string(),
            }
        }
        ObjectEncryptionStateError::NoncanonicalEmptyChecksumNonce => ServerError::InternalError {
            reason: "encrypted checksum metadata state is invalid".to_string(),
        },
        ObjectEncryptionStateError::ObjectEncryptionSubjectMismatch => ServerError::InternalError {
            reason: "object encryption subject does not match provisional encryption state"
                .to_string(),
        },
        ObjectEncryptionStateError::ObjectEncryptionAlreadySubjectBound => {
            ServerError::InternalError {
                reason: "object encryption state is already bound to its committed subject"
                    .to_string(),
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn require_terminal_subjects(
    encryption: &ObjectEncryption,
    wrap_subject: &DekWrapEncryptionSubject<'_>,
    checksum_subject: &ChecksumEncryptionSubject<'_>,
    label: &str,
) -> Result<(), ServerError> {
    if encryption.key_binding() != ObjectEncryptionKeyBinding::ProvisionalV1
        || !wrap_subject.matches_encryption(encryption)
        || !checksum_subject.matches_encryption(encryption)
    {
        return Err(ServerError::InternalError {
            reason: format!("{label} terminal encryption subject does not match provisional state"),
        });
    }
    Ok(())
}

#[cfg(test)]
fn require_committed_subjects(
    encryption: &ObjectEncryption,
    wrap_subject: &DekWrapEncryptionSubject<'_>,
    checksum_subject: &ChecksumEncryptionSubject<'_>,
    label: &str,
) -> Result<(), ServerError> {
    if encryption.key_binding() != ObjectEncryptionKeyBinding::ObjectSubjectV2
        || !wrap_subject.matches_encryption(encryption)
        || !checksum_subject.matches_encryption(encryption)
    {
        return Err(ServerError::InternalError {
            reason: format!("{label} committed encryption subject does not match stored state"),
        });
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
pub struct SseCustomerRequest {
    customer_key: [u8; SSE_C_CUSTOMER_KEY_LEN],
    customer_key_md5_b64: String,
}

impl SseCustomerRequest {
    #[must_use]
    pub fn new(customer_key: [u8; SSE_C_CUSTOMER_KEY_LEN], customer_key_md5_b64: String) -> Self {
        Self {
            customer_key,
            customer_key_md5_b64,
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> &str {
        SSE_CUSTOMER_ALGORITHM
    }

    #[must_use]
    pub fn customer_key(&self) -> &[u8; SSE_C_CUSTOMER_KEY_LEN] {
        &self.customer_key
    }

    #[must_use]
    pub fn response_headers(&self) -> SseCustomerResponseHeaders {
        SseCustomerResponseHeaders {
            key_md5_b64: self.customer_key_md5_b64.clone(),
        }
    }
}

impl fmt::Debug for SseCustomerRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseCustomerRequest")
            .field("algorithm", &SSE_CUSTOMER_ALGORITHM)
            .field(
                "customer_key_md5_b64",
                &observability::redacted("sse_customer_key_md5"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SseCustomerResponseHeaders {
    pub key_md5_b64: String,
}

impl fmt::Debug for SseCustomerResponseHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseCustomerResponseHeaders")
            .field(
                "key_md5_b64",
                &observability::redacted("sse_customer_key_md5"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SseCustomerValidatorConfig {
    pub key_id: u32,
    validator_key: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SseCustomerValidatorConfigError {
    #[error("invalid base64 in ARGMIN_SSE_C_VALIDATOR_KEY")]
    InvalidBase64,
    #[error("ARGMIN_SSE_C_VALIDATOR_KEY must decode to exactly 32 bytes")]
    InvalidLength,
}

impl SseCustomerValidatorConfig {
    pub fn from_base64(
        key_id: u32,
        encoded: &str,
    ) -> Result<Self, SseCustomerValidatorConfigError> {
        use base64::Engine;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| SseCustomerValidatorConfigError::InvalidBase64)?;
        let validator_key = decoded
            .try_into()
            .map_err(|_| SseCustomerValidatorConfigError::InvalidLength)?;
        Ok(Self {
            key_id,
            validator_key,
        })
    }

    fn hmac_key(&self) -> Sha256Key {
        Sha256Key::new(&self.validator_key)
    }
}

impl fmt::Debug for SseCustomerValidatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseCustomerValidatorConfig")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ManagedWrappingKeyConfig {
    pub key_id: u32,
    wrapping_key: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ManagedWrappingKeyConfigError {
    #[error("invalid base64 in managed wrapping key")]
    InvalidBase64,
    #[error("managed wrapping key must decode to exactly 32 bytes")]
    InvalidLength,
}

impl ManagedWrappingKeyConfig {
    pub fn from_base64(key_id: u32, encoded: &str) -> Result<Self, ManagedWrappingKeyConfigError> {
        use base64::Engine;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| ManagedWrappingKeyConfigError::InvalidBase64)?;
        let wrapping_key = decoded
            .try_into()
            .map_err(|_| ManagedWrappingKeyConfigError::InvalidLength)?;
        Ok(Self {
            key_id,
            wrapping_key,
        })
    }

    fn wrapping_key(&self) -> &[u8; 32] {
        &self.wrapping_key
    }
}

impl fmt::Debug for ManagedWrappingKeyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedWrappingKeyConfig")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

pub trait ManagedKeyProvider {
    fn active_key(&self) -> &ManagedWrappingKeyConfig;
    fn lookup_key(&self, key_id: u32) -> Option<&ManagedWrappingKeyConfig>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct StaticManagedKeyProvider {
    active_key_id: u32,
    keys: Vec<ManagedWrappingKeyConfig>,
}

impl StaticManagedKeyProvider {
    #[must_use]
    pub fn single(key: ManagedWrappingKeyConfig) -> Self {
        Self {
            active_key_id: key.key_id,
            keys: vec![key],
        }
    }

    pub fn new(
        active_key_id: u32,
        keys: Vec<ManagedWrappingKeyConfig>,
    ) -> Result<Self, ServerError> {
        if keys.is_empty() {
            return Err(ServerError::InternalError {
                reason: "managed key provider requires at least one wrapping key".to_string(),
            });
        }
        if !keys.iter().any(|key| key.key_id == active_key_id) {
            return Err(ServerError::InternalError {
                reason: format!("managed key provider missing active wrapping key {active_key_id}"),
            });
        }
        Ok(Self {
            active_key_id,
            keys,
        })
    }
}

impl ManagedKeyProvider for StaticManagedKeyProvider {
    fn active_key(&self) -> &ManagedWrappingKeyConfig {
        self.lookup_key(self.active_key_id)
            .expect("active wrapping key id validated at construction time")
    }

    fn lookup_key(&self, key_id: u32) -> Option<&ManagedWrappingKeyConfig> {
        self.keys.iter().find(|key| key.key_id == key_id)
    }
}

impl fmt::Debug for StaticManagedKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key_ids: Vec<u32> = self.keys.iter().map(|key| key.key_id).collect();
        f.debug_struct("StaticManagedKeyProvider")
            .field("active_key_id", &self.active_key_id)
            .field("key_ids", &key_ids)
            .finish()
    }
}

#[derive(Clone)]
pub struct SseCustomerWriteContext {
    request: SseCustomerRequest,
    encryption: ObjectEncryption,
    dek: [u8; SSE_C_DEK_LEN],
    segment_scope: SseCustomerSegmentScope,
}

impl SseCustomerWriteContext {
    #[must_use]
    pub fn request(&self) -> &SseCustomerRequest {
        &self.request
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.encryption
    }

    pub(crate) fn encrypt_authorized_segment(
        &self,
        subject: &SegmentEncryptionSubject,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseCustomer(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "SSE-C write context missing encryption state".to_string(),
            });
        };
        let logical_plaintext_len =
            u64::try_from(plaintext.len()).map_err(|_| ServerError::InternalError {
                reason: "SSE-C segment length exceeds its encryption subject".to_string(),
            })?;
        if !subject.matches_encryption_and_geometry(
            &self.encryption,
            segment_index,
            logical_plaintext_len,
        ) {
            return Err(ServerError::InternalError {
                reason: "SSE-C segment does not match its storage-minted encryption subject"
                    .to_string(),
            });
        }
        encrypt_segment_with_dek(
            &self.dek,
            state,
            self.segment_scope,
            subject,
            segment_index,
            plaintext,
        )
    }

    #[cfg(test)]
    fn encrypt_segment(
        &self,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseCustomer(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "SSE-C write context missing encryption state".to_string(),
            });
        };
        encrypt_legacy_segment_with_dek_and_prefix(
            &self.dek,
            state.segment_nonce_prefix(),
            self.segment_scope,
            segment_index,
            plaintext,
            AeadDescriptor {
                aad: SSE_C_SEGMENT_AAD,
                label: "SSE-C",
            },
        )
    }

    pub fn seal_checksum_metadata(
        &self,
        checksum: Option<&ObjectChecksumMetadata>,
    ) -> Result<ObjectEncryption, ServerError> {
        let ObjectEncryption::SseCustomer(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "SSE-C write context missing encryption state".to_string(),
            });
        };
        let (checksum_nonce, encrypted_checksum_metadata) =
            encrypt_checksum_with_dek(&self.dek, checksum, SSE_C_CHECKSUM_AAD, "SSE-C")?;
        Ok(ObjectEncryption::SseCustomer(
            state
                .with_encrypted_checksum_metadata(checksum_nonce, encrypted_checksum_metadata)
                .map_err(object_encryption_state_error)?,
        ))
    }

    /// Performs the one-way terminal transition from provisional SSE-C state
    /// to DEK and checksum ciphertext bound to the committed object subject.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn seal_committed_object_encryption(
        &self,
        subject: &ObjectEncryptionSubject,
        checksum: Option<&ObjectChecksumMetadata>,
    ) -> Result<ObjectEncryption, ServerError> {
        let ObjectEncryption::SseCustomer(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "SSE-C write context missing encryption state".to_string(),
            });
        };
        let wrap_subject = subject.dek_wrap_subject();
        let checksum_subject = subject.checksum_subject();
        require_terminal_subjects(&self.encryption, &wrap_subject, &checksum_subject, "SSE-C")?;
        let mut wrap_nonce = [0u8; SSE_C_WRAP_NONCE_LEN];
        argmin_crypto::random::fill(&mut wrap_nonce).map_err(|_| ServerError::InternalError {
            reason: "failed to generate SSE-C committed wrap nonce".to_string(),
        })?;
        let kek = derive_wrap_key(self.request.customer_key(), state.wrap_salt())?;
        let wrapped_dek = wrap_managed_dek(
            &kek,
            &wrap_nonce,
            &self.dek,
            wrap_subject.aead_context(),
            "SSE-C",
        )?;
        let (checksum_nonce, encrypted_checksum_metadata) = encrypt_checksum_with_dek(
            &self.dek,
            checksum,
            checksum_subject.aead_context(),
            "SSE-C",
        )?;
        self.encryption
            .bind_committed_object_ciphertexts(
                subject,
                wrap_nonce,
                wrapped_dek,
                checksum_nonce,
                encrypted_checksum_metadata,
            )
            .map_err(object_encryption_state_error)
    }
}

impl fmt::Debug for SseCustomerWriteContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseCustomerWriteContext")
            .field("request", &self.request)
            .field("encryption", &"sse_customer")
            .finish()
    }
}

#[derive(Clone)]
pub struct ManagedEncryptionWriteContext {
    encryption: ObjectEncryption,
    dek: [u8; SSE_C_DEK_LEN],
    #[cfg_attr(not(test), allow(dead_code))]
    wrapping_key: [u8; SSE_C_DEK_LEN],
    segment_scope: SseCustomerSegmentScope,
}

impl ManagedEncryptionWriteContext {
    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.encryption
    }

    pub(crate) fn encrypt_authorized_segment(
        &self,
        subject: &SegmentEncryptionSubject,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseS3(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "managed write context missing encryption state".to_string(),
            });
        };
        let logical_plaintext_len =
            u64::try_from(plaintext.len()).map_err(|_| ServerError::InternalError {
                reason: "managed segment length exceeds its encryption subject".to_string(),
            })?;
        if !subject.matches_encryption_and_geometry(
            &self.encryption,
            segment_index,
            logical_plaintext_len,
        ) {
            return Err(ServerError::InternalError {
                reason: "managed segment does not match its storage-minted encryption subject"
                    .to_string(),
            });
        }
        encrypt_segment_with_dek_and_prefix(
            &self.dek,
            plaintext,
            SegmentSealParameters {
                payload_codec: state.payload_codec(),
                segment_nonce_prefix: state.segment_nonce_prefix(),
                segment_scope: self.segment_scope,
                payload_aead_context: subject.payload_aead_context(),
                segment_index,
                descriptor: AeadDescriptor {
                    aad: MANAGED_SEGMENT_AAD,
                    label: MANAGED_ENCRYPTION_LABEL,
                },
            },
        )
    }

    #[cfg(test)]
    fn encrypt_segment(
        &self,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseS3(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "managed write context missing encryption state".to_string(),
            });
        };
        encrypt_legacy_segment_with_dek_and_prefix(
            &self.dek,
            state.segment_nonce_prefix(),
            self.segment_scope,
            segment_index,
            plaintext,
            AeadDescriptor {
                aad: MANAGED_SEGMENT_AAD,
                label: MANAGED_ENCRYPTION_LABEL,
            },
        )
    }

    pub fn seal_checksum_metadata(
        &self,
        checksum: Option<&ObjectChecksumMetadata>,
    ) -> Result<ObjectEncryption, ServerError> {
        let ObjectEncryption::SseS3(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "managed write context missing encryption state".to_string(),
            });
        };
        let (checksum_nonce, encrypted_checksum_metadata) = encrypt_checksum_with_dek(
            &self.dek,
            checksum,
            MANAGED_CHECKSUM_AAD,
            MANAGED_ENCRYPTION_LABEL,
        )?;
        Ok(ObjectEncryption::SseS3(
            state
                .with_encrypted_checksum_metadata(checksum_nonce, encrypted_checksum_metadata)
                .map_err(object_encryption_state_error)?,
        ))
    }

    /// Performs the one-way terminal transition from provisional managed
    /// state to DEK and checksum ciphertext bound to the committed object
    /// subject.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn seal_committed_object_encryption(
        &self,
        subject: &ObjectEncryptionSubject,
        checksum: Option<&ObjectChecksumMetadata>,
    ) -> Result<ObjectEncryption, ServerError> {
        let wrap_subject = subject.dek_wrap_subject();
        let checksum_subject = subject.checksum_subject();
        require_terminal_subjects(
            &self.encryption,
            &wrap_subject,
            &checksum_subject,
            MANAGED_ENCRYPTION_LABEL,
        )?;
        let mut wrap_nonce = [0u8; SSE_S3_WRAP_NONCE_LEN];
        argmin_crypto::random::fill(&mut wrap_nonce).map_err(|_| ServerError::InternalError {
            reason: "failed to generate managed committed wrap nonce".to_string(),
        })?;
        let wrapped_dek = wrap_managed_dek(
            &self.wrapping_key,
            &wrap_nonce,
            &self.dek,
            wrap_subject.aead_context(),
            MANAGED_ENCRYPTION_LABEL,
        )?;
        let (checksum_nonce, encrypted_checksum_metadata) = encrypt_checksum_with_dek(
            &self.dek,
            checksum,
            checksum_subject.aead_context(),
            MANAGED_ENCRYPTION_LABEL,
        )?;
        self.encryption
            .bind_committed_object_ciphertexts(
                subject,
                wrap_nonce,
                wrapped_dek,
                checksum_nonce,
                encrypted_checksum_metadata,
            )
            .map_err(object_encryption_state_error)
    }
}

impl fmt::Debug for ManagedEncryptionWriteContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedEncryptionWriteContext")
            .field("encryption", &"managed")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SseCustomerSegmentScope(u16);

impl SseCustomerSegmentScope {
    #[must_use]
    pub(crate) fn object() -> Self {
        Self(0)
    }

    pub(crate) fn multipart_part(part_number: u32) -> Result<Self, ServerError> {
        if part_number == 0 {
            return Err(ServerError::InternalError {
                reason: "multipart SSE-C nonce scope requires a positive part number".to_string(),
            });
        }
        let part_number = u16::try_from(part_number).map_err(|_| ServerError::InternalError {
            reason: format!(
                "multipart SSE-C part number {part_number} does not fit in the nonce scope"
            ),
        })?;
        Ok(Self(part_number))
    }

    fn encode(self) -> [u8; SSE_C_SEGMENT_NONCE_SCOPE_LEN] {
        self.0.to_be_bytes()
    }
}

pub fn prepare_sse_customer_write(
    validator: &SseCustomerValidatorConfig,
    request: &SseCustomerRequest,
) -> Result<SseCustomerWriteContext, ServerError> {
    prepare_sse_customer_write_with_payload_codec(
        validator,
        request,
        SegmentPayloadCodec::SegmentAes256GcmV1,
    )
}

fn prepare_sse_customer_write_with_payload_codec(
    validator: &SseCustomerValidatorConfig,
    request: &SseCustomerRequest,
    payload_codec: SegmentPayloadCodec,
) -> Result<SseCustomerWriteContext, ServerError> {
    let mut validator_salt = [0u8; SSE_C_VALIDATOR_SALT_LEN];
    argmin_crypto::random::fill(&mut validator_salt).map_err(|_| ServerError::InternalError {
        reason: "failed to generate SSE-C validator salt".to_string(),
    })?;

    let validator_hmac = compute_validator_hmac(validator, &validator_salt, request.customer_key());

    let mut wrap_salt = [0u8; SSE_C_WRAP_SALT_LEN];
    argmin_crypto::random::fill(&mut wrap_salt).map_err(|_| ServerError::InternalError {
        reason: "failed to generate SSE-C wrap salt".to_string(),
    })?;

    let mut wrap_nonce = [0u8; SSE_C_WRAP_NONCE_LEN];
    argmin_crypto::random::fill(&mut wrap_nonce).map_err(|_| ServerError::InternalError {
        reason: "failed to generate SSE-C wrap nonce".to_string(),
    })?;

    let mut encryption_instance_id = [0u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN];
    argmin_crypto::random::fill(&mut encryption_instance_id).map_err(|_| {
        ServerError::InternalError {
            reason: "failed to generate SSE-C encryption instance identity".to_string(),
        }
    })?;

    let mut dek = [0u8; SSE_C_DEK_LEN];
    argmin_crypto::random::fill(&mut dek).map_err(|_| ServerError::InternalError {
        reason: "failed to generate SSE-C object DEK".to_string(),
    })?;

    let kek = derive_wrap_key(request.customer_key(), &wrap_salt)?;
    let wrapped_dek = wrap_managed_dek(&kek, &wrap_nonce, &dek, SSE_C_WRAP_AAD, "SSE-C")?;

    let mut segment_nonce_prefix = [0u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN];
    argmin_crypto::random::fill(&mut segment_nonce_prefix).map_err(|_| {
        ServerError::InternalError {
            reason: "failed to generate SSE-C segment nonce prefix".to_string(),
        }
    })?;

    Ok(SseCustomerWriteContext {
        request: request.clone(),
        encryption: ObjectEncryption::SseCustomer(
            SseCustomerObjectState::new_with_encryption_instance_id(
                encryption_instance_id,
                validator.key_id,
                validator_salt,
                validator_hmac,
                wrap_salt,
                wrap_nonce,
                wrapped_dek,
                segment_nonce_prefix,
            )
            .with_payload_codec(payload_codec),
        ),
        dek,
        segment_scope: SseCustomerSegmentScope::object(),
    })
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_prepare_sse_customer_write_with_payload_codec(
    validator: &SseCustomerValidatorConfig,
    request: &SseCustomerRequest,
    payload_codec: SegmentPayloadCodec,
) -> Result<SseCustomerWriteContext, ServerError> {
    prepare_sse_customer_write_with_payload_codec(validator, request, payload_codec)
}

pub(crate) fn resume_sse_customer_write(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    segment_scope: SseCustomerSegmentScope,
) -> Result<SseCustomerWriteContext, ServerError> {
    if state.key_binding() != ObjectEncryptionKeyBinding::ProvisionalV1 {
        return Err(ServerError::InternalError {
            reason: "committed SSE-C encryption state cannot resume a provisional write"
                .to_string(),
        });
    }
    validate_sse_customer_write(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        SSE_C_WRAP_AAD,
        "SSE-C",
    )?;
    Ok(SseCustomerWriteContext {
        request: request.clone(),
        encryption: ObjectEncryption::SseCustomer(state.clone()),
        dek,
        segment_scope,
    })
}

pub fn validate_sse_customer_read(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
) -> Result<SseCustomerResponseHeaders, ServerError> {
    if state.validator_key_id() != validator.key_id {
        return Err(sse_customer_validator_key_unavailable());
    }
    let actual = compute_validator_hmac(validator, state.validator_salt(), request.customer_key());
    if !auth::constant_time_eq(state.validator_hmac(), &actual) {
        return Err(ServerError::AccessDenied);
    }
    Ok(request.response_headers())
}

pub fn prepare_managed_encryption_write(
    provider: &impl ManagedKeyProvider,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    prepare_managed_encryption_write_with_payload_codec(
        provider,
        SegmentPayloadCodec::SegmentAes256GcmV1,
    )
}

fn prepare_managed_encryption_write_with_payload_codec(
    provider: &impl ManagedKeyProvider,
    payload_codec: SegmentPayloadCodec,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    let wrapping_key = provider.active_key();

    let mut wrap_nonce = [0u8; SSE_S3_WRAP_NONCE_LEN];
    argmin_crypto::random::fill(&mut wrap_nonce).map_err(|_| ServerError::InternalError {
        reason: "failed to generate managed wrap nonce".to_string(),
    })?;

    let mut encryption_instance_id = [0u8; OBJECT_ENCRYPTION_INSTANCE_RANDOM_LEN];
    argmin_crypto::random::fill(&mut encryption_instance_id).map_err(|_| {
        ServerError::InternalError {
            reason: "failed to generate managed encryption instance identity".to_string(),
        }
    })?;

    let mut dek = [0u8; SSE_C_DEK_LEN];
    argmin_crypto::random::fill(&mut dek).map_err(|_| ServerError::InternalError {
        reason: "failed to generate managed object DEK".to_string(),
    })?;

    let wrapped_dek = wrap_managed_dek(
        wrapping_key.wrapping_key(),
        &wrap_nonce,
        &dek,
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;

    let mut segment_nonce_prefix = [0u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN];
    argmin_crypto::random::fill(&mut segment_nonce_prefix).map_err(|_| {
        ServerError::InternalError {
            reason: "failed to generate managed segment nonce prefix".to_string(),
        }
    })?;

    Ok(ManagedEncryptionWriteContext {
        encryption: ObjectEncryption::SseS3(
            SseS3ObjectState::new_with_encryption_instance_id(
                encryption_instance_id,
                wrapping_key.key_id,
                wrap_nonce,
                wrapped_dek,
                segment_nonce_prefix,
            )
            .with_payload_codec(payload_codec),
        ),
        dek,
        wrapping_key: *wrapping_key.wrapping_key(),
        segment_scope: SseCustomerSegmentScope::object(),
    })
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_prepare_managed_encryption_write_with_payload_codec(
    provider: &impl ManagedKeyProvider,
    payload_codec: SegmentPayloadCodec,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    prepare_managed_encryption_write_with_payload_codec(provider, payload_codec)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resume_managed_encryption_write(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    segment_scope: SseCustomerSegmentScope,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    if state.key_binding() != ObjectEncryptionKeyBinding::ProvisionalV1 {
        return Err(ServerError::InternalError {
            reason: "committed managed encryption state cannot resume a provisional write"
                .to_string(),
        });
    }
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    Ok(ManagedEncryptionWriteContext {
        encryption: ObjectEncryption::SseS3(state.clone()),
        dek,
        wrapping_key: *wrapping_key.wrapping_key(),
        segment_scope,
    })
}

pub(crate) fn decrypt_authorized_managed_encryption_segment_in_place(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    read: AuthorizedSegmentRead<'_>,
    ciphertext: &mut Vec<u8>,
) -> Result<(), ServerError> {
    let encryption = ObjectEncryption::SseS3(state.clone());
    if !read.subject.matches_encryption_and_geometry(
        &encryption,
        read.segment_index,
        read.plaintext_len as u64,
    ) {
        return Err(ServerError::InternalError {
            reason: "managed segment does not match its storage-minted read subject".to_string(),
        });
    }
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => MANAGED_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    decrypt_segment_with_dek_and_prefix_in_place(
        &dek,
        state.payload_codec(),
        state.segment_nonce_prefix(),
        read,
        ciphertext,
        AeadDescriptor {
            aad: MANAGED_SEGMENT_AAD,
            label: MANAGED_ENCRYPTION_LABEL,
        },
    )
}

pub(crate) fn prepare_authorized_managed_encryption_segment_range(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    read: AuthorizedSegmentRead<'_>,
    plaintext_range: Range<u64>,
) -> Result<AuthorizedCobblestoneSegmentRangeRead, ServerError> {
    let encryption = ObjectEncryption::SseS3(state.clone());
    if state.payload_codec() != SegmentPayloadCodec::Cobblestone256SegmentV1
        || !read.subject.matches_encryption_and_geometry(
            &encryption,
            read.segment_index,
            read.plaintext_len as u64,
        )
    {
        return Err(ServerError::InternalError {
            reason: "managed Cobblestone range does not match its storage-minted read subject"
                .to_string(),
        });
    }
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => MANAGED_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    prepare_authorized_cobblestone_segment_range(&dek, read, plaintext_range)
}

pub(crate) fn prepare_authorized_managed_encryption_segment_stream(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    read: AuthorizedSegmentRead<'_>,
) -> Result<AuthorizedCobblestoneSegmentStreamRead, ServerError> {
    let encryption = ObjectEncryption::SseS3(state.clone());
    if state.payload_codec() != SegmentPayloadCodec::Cobblestone256SegmentV1
        || !read.subject.matches_encryption_and_geometry(
            &encryption,
            read.segment_index,
            read.plaintext_len as u64,
        )
    {
        return Err(ServerError::InternalError {
            reason: "managed Cobblestone stream does not match its storage-minted read subject"
                .to_string(),
        });
    }
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => MANAGED_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    prepare_authorized_cobblestone_segment_stream(&dek, read, MANAGED_ENCRYPTION_LABEL)
}

#[cfg(test)]
fn decrypt_managed_encryption_segment_in_place(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &mut Vec<u8>,
    plaintext_len: usize,
) -> Result<(), ServerError> {
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    decrypt_legacy_segment_with_dek_and_prefix_in_place(
        &dek,
        state.segment_nonce_prefix(),
        segment_scope,
        segment_index,
        ciphertext,
        plaintext_len,
        AeadDescriptor {
            aad: MANAGED_SEGMENT_AAD,
            label: MANAGED_ENCRYPTION_LABEL,
        },
    )
}

pub fn decrypt_managed_encryption_checksum(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    decrypt_managed_encryption_checksum_with_subject(provider, state, None)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decrypt_authorized_managed_encryption_checksum(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    subject: &ObjectEncryptionSubject,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    decrypt_managed_encryption_checksum_with_subject(provider, state, Some(subject))
}

#[cfg(test)]
pub(crate) fn test_replace_authorized_managed_checksum_metadata_version(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    subject: &ObjectEncryptionSubject,
    checksum: &ObjectChecksumMetadata,
    version: u8,
) -> Result<ObjectEncryption, ServerError> {
    let encryption = ObjectEncryption::SseS3(state.clone());
    let wrap_subject = subject.dek_wrap_subject();
    let checksum_subject = subject.checksum_subject();
    require_committed_subjects(
        &encryption,
        &wrap_subject,
        &checksum_subject,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_subject.aead_context(),
        MANAGED_ENCRYPTION_LABEL,
    )?;
    let (checksum_nonce, encrypted_checksum_metadata) = test_encrypt_checksum_metadata_version(
        &dek,
        checksum,
        version,
        checksum_subject.aead_context(),
        MANAGED_ENCRYPTION_LABEL,
    )?;
    Ok(ObjectEncryption::SseS3(
        state
            .with_encrypted_checksum_metadata(checksum_nonce, encrypted_checksum_metadata)
            .map_err(object_encryption_state_error)?,
    ))
}

fn decrypt_managed_encryption_checksum_with_subject(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    subject: Option<&ObjectEncryptionSubject>,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    if state.encrypted_checksum_metadata().is_empty() {
        return Ok(None);
    }
    let encryption = ObjectEncryption::SseS3(state.clone());
    let subjects = subject.map(|subject| (subject.dek_wrap_subject(), subject.checksum_subject()));
    if subjects.as_ref().is_some_and(|(wrap, checksum)| {
        !wrap.matches_encryption(&encryption) || !checksum.matches_encryption(&encryption)
    }) {
        return Err(ServerError::InternalError {
            reason: "managed checksum subject does not match stored encryption".to_string(),
        });
    }
    let (wrap_aad, checksum_aad) = match (state.key_binding(), subjects.as_ref()) {
        (ObjectEncryptionKeyBinding::ProvisionalV1, _) => (MANAGED_WRAP_AAD, MANAGED_CHECKSUM_AAD),
        (ObjectEncryptionKeyBinding::ObjectSubjectV2, Some((wrap, checksum))) => {
            (wrap.aead_context(), checksum.aead_context())
        }
        (ObjectEncryptionKeyBinding::ObjectSubjectV2, None) => {
            return Err(ServerError::InternalError {
                reason: "object-bound managed checksum requires its storage-minted subject"
                    .to_string(),
            });
        }
    };
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    decrypt_checksum_with_dek(
        &dek,
        state.checksum_nonce(),
        state.encrypted_checksum_metadata(),
        checksum_aad,
        MANAGED_ENCRYPTION_LABEL,
    )
}

fn validate_sse_customer_write(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
) -> Result<SseCustomerResponseHeaders, ServerError> {
    if state.validator_key_id() != validator.key_id {
        return Err(sse_customer_validator_key_unavailable());
    }
    let actual = compute_validator_hmac(validator, state.validator_salt(), request.customer_key());
    if !auth::constant_time_eq(state.validator_hmac(), &actual) {
        return Err(ServerError::InvalidRequest {
            reason: "The provided encryption parameters did not match the ones used originally."
                .to_string(),
        });
    }
    Ok(request.response_headers())
}

pub(crate) fn validate_sse_customer_completion_replay(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
) -> Result<(), ServerError> {
    validate_sse_customer_write(validator, state, request)?;
    Ok(())
}

fn sse_customer_validator_key_unavailable() -> ServerError {
    ServerError::InternalError {
        reason: "SSE-C validator key for object is not available".to_string(),
    }
}

pub(crate) fn decrypt_authorized_sse_customer_segment_in_place(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    read: AuthorizedSegmentRead<'_>,
    ciphertext: &mut Vec<u8>,
) -> Result<(), ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let encryption = ObjectEncryption::SseCustomer(state.clone());
    if !read.subject.matches_encryption_and_geometry(
        &encryption,
        read.segment_index,
        read.plaintext_len as u64,
    ) {
        return Err(ServerError::InternalError {
            reason: "SSE-C segment does not match its storage-minted read subject".to_string(),
        });
    }
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => SSE_C_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        "SSE-C",
    )?;
    decrypt_segment_with_dek_and_prefix_in_place(
        &dek,
        state.payload_codec(),
        state.segment_nonce_prefix(),
        read,
        ciphertext,
        AeadDescriptor {
            aad: SSE_C_SEGMENT_AAD,
            label: "SSE-C",
        },
    )
}

pub(crate) fn prepare_authorized_sse_customer_segment_range(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    read: AuthorizedSegmentRead<'_>,
    plaintext_range: Range<u64>,
) -> Result<AuthorizedCobblestoneSegmentRangeRead, ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let encryption = ObjectEncryption::SseCustomer(state.clone());
    if state.payload_codec() != SegmentPayloadCodec::Cobblestone256SegmentV1
        || !read.subject.matches_encryption_and_geometry(
            &encryption,
            read.segment_index,
            read.plaintext_len as u64,
        )
    {
        return Err(ServerError::InternalError {
            reason: "SSE-C Cobblestone range does not match its storage-minted read subject"
                .to_string(),
        });
    }
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => SSE_C_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        "SSE-C",
    )?;
    prepare_authorized_cobblestone_segment_range(&dek, read, plaintext_range)
}

pub(crate) fn prepare_authorized_sse_customer_segment_stream(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    read: AuthorizedSegmentRead<'_>,
) -> Result<AuthorizedCobblestoneSegmentStreamRead, ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let encryption = ObjectEncryption::SseCustomer(state.clone());
    if state.payload_codec() != SegmentPayloadCodec::Cobblestone256SegmentV1
        || !read.subject.matches_encryption_and_geometry(
            &encryption,
            read.segment_index,
            read.plaintext_len as u64,
        )
    {
        return Err(ServerError::InternalError {
            reason: "SSE-C Cobblestone stream does not match its storage-minted read subject"
                .to_string(),
        });
    }
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let wrap_subject = read.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => SSE_C_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        "SSE-C",
    )?;
    prepare_authorized_cobblestone_segment_stream(&dek, read, "SSE-C")
}

fn prepare_authorized_cobblestone_segment_stream(
    dek: &[u8; SSE_C_DEK_LEN],
    read: AuthorizedSegmentRead<'_>,
    label: &'static str,
) -> Result<AuthorizedCobblestoneSegmentStreamRead, ServerError> {
    let failure = || ServerError::InternalError {
        reason: format!("invalid {label} Cobblestone segment geometry"),
    };
    let layout = Cobblestone256MessageLayout::from_plaintext_len(read.plaintext_len as u64)
        .map_err(|_| failure())?;
    let key = Cobblestone256Key::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {label} Cobblestone segment opening key"),
    })?;
    let opener = Cobblestone256StreamOpener::new(
        key,
        read.subject.payload_aead_context(),
        layout.ciphertext_len(),
    )
    .map_err(|_| failure())?;
    Ok(AuthorizedCobblestoneSegmentStreamRead {
        opener,
        layout,
        header_authenticated: false,
        next_chunk: 0,
        plaintext_len: read.plaintext_len,
        authenticated_plaintext_len: 0,
        label,
    })
}

fn prepare_authorized_cobblestone_segment_range(
    dek: &[u8; SSE_C_DEK_LEN],
    read: AuthorizedSegmentRead<'_>,
    plaintext_range: Range<u64>,
) -> Result<AuthorizedCobblestoneSegmentRangeRead, ServerError> {
    let failure = || ServerError::InternalError {
        reason: "invalid Cobblestone segment range geometry".to_string(),
    };
    let layout = Cobblestone256MessageLayout::from_plaintext_len(read.plaintext_len as u64)
        .map_err(|_| failure())?;
    let selection = layout
        .select_plaintext_range(plaintext_range)
        .map_err(|_| failure())?;
    let key = Cobblestone256Key::new(dek).map_err(|_| ServerError::InternalError {
        reason: "failed to create Cobblestone segment range opening key".to_string(),
    })?;
    Ok(AuthorizedCobblestoneSegmentRangeRead {
        key,
        payload_aead_context: read.subject.payload_aead_context().to_vec(),
        selection,
    })
}

#[cfg(test)]
fn decrypt_sse_customer_segment_in_place(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &mut Vec<u8>,
    plaintext_len: usize,
) -> Result<(), ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        SSE_C_WRAP_AAD,
        "SSE-C",
    )?;
    decrypt_legacy_segment_with_dek_and_prefix_in_place(
        &dek,
        state.segment_nonce_prefix(),
        segment_scope,
        segment_index,
        ciphertext,
        plaintext_len,
        AeadDescriptor {
            aad: SSE_C_SEGMENT_AAD,
            label: "SSE-C",
        },
    )
}

pub fn decrypt_sse_customer_checksum(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    decrypt_sse_customer_checksum_with_subject(validator, state, request, None)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decrypt_authorized_sse_customer_checksum(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    subject: &ObjectEncryptionSubject,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    decrypt_sse_customer_checksum_with_subject(validator, state, request, Some(subject))
}

#[cfg(test)]
pub(crate) fn test_replace_authorized_sse_customer_checksum_metadata_version(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    subject: &ObjectEncryptionSubject,
    checksum: &ObjectChecksumMetadata,
    version: u8,
) -> Result<ObjectEncryption, ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let encryption = ObjectEncryption::SseCustomer(state.clone());
    let wrap_subject = subject.dek_wrap_subject();
    let checksum_subject = subject.checksum_subject();
    require_committed_subjects(&encryption, &wrap_subject, &checksum_subject, "SSE-C")?;
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_subject.aead_context(),
        "SSE-C",
    )?;
    let (checksum_nonce, encrypted_checksum_metadata) = test_encrypt_checksum_metadata_version(
        &dek,
        checksum,
        version,
        checksum_subject.aead_context(),
        "SSE-C",
    )?;
    Ok(ObjectEncryption::SseCustomer(
        state
            .with_encrypted_checksum_metadata(checksum_nonce, encrypted_checksum_metadata)
            .map_err(object_encryption_state_error)?,
    ))
}

fn decrypt_sse_customer_checksum_with_subject(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    subject: Option<&ObjectEncryptionSubject>,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    if state.encrypted_checksum_metadata().is_empty() {
        return Ok(None);
    }
    validate_sse_customer_read(validator, state, request)?;
    let encryption = ObjectEncryption::SseCustomer(state.clone());
    let subjects = subject.map(|subject| (subject.dek_wrap_subject(), subject.checksum_subject()));
    if subjects.as_ref().is_some_and(|(wrap, checksum)| {
        !wrap.matches_encryption(&encryption) || !checksum.matches_encryption(&encryption)
    }) {
        return Err(ServerError::InternalError {
            reason: "SSE-C checksum subject does not match stored encryption".to_string(),
        });
    }
    let (wrap_aad, checksum_aad) = match (state.key_binding(), subjects.as_ref()) {
        (ObjectEncryptionKeyBinding::ProvisionalV1, _) => (SSE_C_WRAP_AAD, SSE_C_CHECKSUM_AAD),
        (ObjectEncryptionKeyBinding::ObjectSubjectV2, Some((wrap, checksum))) => {
            (wrap.aead_context(), checksum.aead_context())
        }
        (ObjectEncryptionKeyBinding::ObjectSubjectV2, None) => {
            return Err(ServerError::InternalError {
                reason: "object-bound SSE-C checksum requires its storage-minted subject"
                    .to_string(),
            });
        }
    };
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        "SSE-C",
    )?;
    decrypt_checksum_with_dek(
        &dek,
        state.checksum_nonce(),
        state.encrypted_checksum_metadata(),
        checksum_aad,
        "SSE-C",
    )
}

fn compute_validator_hmac(
    validator: &SseCustomerValidatorConfig,
    validator_salt: &[u8; SSE_C_VALIDATOR_SALT_LEN],
    customer_key: &[u8; SSE_C_CUSTOMER_KEY_LEN],
) -> [u8; SSE_C_VALIDATOR_HMAC_LEN] {
    let key = validator.hmac_key();
    let mut msg = [0u8; SSE_C_VALIDATOR_SALT_LEN + SSE_C_CUSTOMER_KEY_LEN];
    msg[..SSE_C_VALIDATOR_SALT_LEN].copy_from_slice(validator_salt);
    msg[SSE_C_VALIDATOR_SALT_LEN..].copy_from_slice(customer_key);
    key.sign(&msg)
}

fn derive_wrap_key(
    customer_key: &[u8; SSE_C_CUSTOMER_KEY_LEN],
    wrap_salt: &[u8; SSE_C_WRAP_SALT_LEN],
) -> Result<[u8; 32], ServerError> {
    argmin_crypto::hkdf::sha256(wrap_salt, customer_key, SSE_C_HKDF_INFO).map_err(|_| {
        ServerError::InternalError {
            reason: "failed to derive SSE-C wrapping key".to_string(),
        }
    })
}

fn wrap_managed_dek(
    kek: &[u8; 32],
    wrap_nonce: &[u8; SSE_C_WRAP_NONCE_LEN],
    dek: &[u8; SSE_C_DEK_LEN],
    wrap_aad: &[u8],
    label: &str,
) -> Result<[u8; SSE_C_WRAPPED_DEK_LEN], ServerError> {
    let sealing_key = Aes256GcmKey::new(kek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {label} wrapping key"),
    })?;
    let mut buf = dek.to_vec();
    sealing_key
        .seal_in_place_append_tag(*wrap_nonce, wrap_aad, &mut buf)
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to wrap {label} DEK"),
        })?;
    buf.try_into().map_err(|_| ServerError::InternalError {
        reason: format!("wrapped {label} DEK length mismatch"),
    })
}

fn unwrap_managed_dek(
    kek: &[u8; 32],
    wrap_nonce: &[u8; SSE_C_WRAP_NONCE_LEN],
    wrapped_dek: &[u8; SSE_C_WRAPPED_DEK_LEN],
    wrap_aad: &[u8],
    label: &str,
) -> Result<[u8; SSE_C_DEK_LEN], ServerError> {
    let opening_key = Aes256GcmKey::new(kek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {label} unwrap key"),
    })?;
    let mut buf = wrapped_dek.to_vec();
    let plaintext = opening_key
        .open_in_place(*wrap_nonce, wrap_aad, &mut buf)
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to unwrap {label} DEK"),
        })?;
    plaintext
        .try_into()
        .map_err(|_| ServerError::InternalError {
            reason: format!("unwrapped {label} DEK length mismatch"),
        })
}

fn encrypt_segment_with_dek(
    dek: &[u8; SSE_C_DEK_LEN],
    state: &SseCustomerObjectState,
    segment_scope: SseCustomerSegmentScope,
    subject: &SegmentEncryptionSubject,
    segment_index: u32,
    plaintext: &[u8],
) -> Result<Vec<u8>, ServerError> {
    encrypt_segment_with_dek_and_prefix(
        dek,
        plaintext,
        SegmentSealParameters {
            payload_codec: state.payload_codec(),
            segment_nonce_prefix: state.segment_nonce_prefix(),
            segment_scope,
            payload_aead_context: subject.payload_aead_context(),
            segment_index,
            descriptor: AeadDescriptor {
                aad: SSE_C_SEGMENT_AAD,
                label: "SSE-C",
            },
        },
    )
}

fn encrypt_segment_with_dek_and_prefix(
    dek: &[u8; SSE_C_DEK_LEN],
    plaintext: &[u8],
    parameters: SegmentSealParameters<'_>,
) -> Result<Vec<u8>, ServerError> {
    match parameters.payload_codec {
        SegmentPayloadCodec::SegmentAes256GcmV1 => encrypt_legacy_segment_with_dek_and_prefix(
            dek,
            parameters.segment_nonce_prefix,
            parameters.segment_scope,
            parameters.segment_index,
            plaintext,
            parameters.descriptor,
        ),
        SegmentPayloadCodec::Cobblestone256SegmentV1 => {
            let key = Cobblestone256Key::new(dek).map_err(|_| ServerError::InternalError {
                reason: format!(
                    "failed to create {} Cobblestone segment sealing key",
                    parameters.descriptor.label
                ),
            })?;
            let mut salt = [0_u8; COBBLESTONE_SALT_LEN];
            argmin_crypto::random::fill(&mut salt).map_err(|_| ServerError::InternalError {
                reason: format!(
                    "failed to generate {} Cobblestone segment salt",
                    parameters.descriptor.label
                ),
            })?;
            cobblestone::seal(&key, salt, parameters.payload_aead_context, plaintext).map_err(
                |_| ServerError::InternalError {
                    reason: format!(
                        "failed to encrypt {} Cobblestone segment",
                        parameters.descriptor.label
                    ),
                },
            )
        }
    }
}

fn encrypt_legacy_segment_with_dek_and_prefix(
    dek: &[u8; SSE_C_DEK_LEN],
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    plaintext: &[u8],
    descriptor: AeadDescriptor<'_>,
) -> Result<Vec<u8>, ServerError> {
    let sealing_key = Aes256GcmKey::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {} segment sealing key", descriptor.label),
    })?;
    let mut buf = plaintext.to_vec();
    sealing_key
        .seal_in_place_append_tag(
            segment_nonce(segment_nonce_prefix, segment_scope, segment_index),
            descriptor.aad,
            &mut buf,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to encrypt {} segment", descriptor.label),
        })?;
    Ok(buf)
}

fn decrypt_segment_with_dek_and_prefix_in_place(
    dek: &[u8; SSE_C_DEK_LEN],
    payload_codec: SegmentPayloadCodec,
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    read: AuthorizedSegmentRead<'_>,
    ciphertext: &mut Vec<u8>,
    descriptor: AeadDescriptor<'_>,
) -> Result<(), ServerError> {
    match payload_codec {
        SegmentPayloadCodec::SegmentAes256GcmV1 => {
            decrypt_legacy_segment_with_dek_and_prefix_in_place(
                dek,
                segment_nonce_prefix,
                read.segment_scope,
                read.segment_index,
                ciphertext,
                read.plaintext_len,
                descriptor,
            )
        }
        SegmentPayloadCodec::Cobblestone256SegmentV1 => decrypt_cobblestone_segment_in_place(
            dek,
            read.subject.payload_aead_context(),
            ciphertext,
            read.plaintext_len,
            descriptor,
        ),
    }
}

#[cfg(test)]
pub(crate) struct CrossedSegmentTestInput<'a> {
    pub(crate) source_codec: SegmentPayloadCodec,
    pub(crate) subject: &'a SegmentEncryptionSubject,
    pub(crate) segment_scope: SseCustomerSegmentScope,
    pub(crate) segment_index: u32,
    pub(crate) plaintext: &'a [u8],
}

#[cfg(test)]
pub(crate) fn test_encrypt_crossed_managed_segment(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    input: CrossedSegmentTestInput<'_>,
) -> Result<Vec<u8>, ServerError> {
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id())
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let wrap_subject = input.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => MANAGED_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    encrypt_segment_with_dek_and_prefix(
        &dek,
        input.plaintext,
        SegmentSealParameters {
            payload_codec: input.source_codec,
            segment_nonce_prefix: state.segment_nonce_prefix(),
            segment_scope: input.segment_scope,
            payload_aead_context: input.subject.payload_aead_context(),
            segment_index: input.segment_index,
            descriptor: AeadDescriptor {
                aad: MANAGED_SEGMENT_AAD,
                label: MANAGED_ENCRYPTION_LABEL,
            },
        },
    )
}

#[cfg(test)]
pub(crate) fn test_encrypt_crossed_sse_customer_segment(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    input: CrossedSegmentTestInput<'_>,
) -> Result<Vec<u8>, ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), state.wrap_salt())?;
    let wrap_subject = input.subject.dek_wrap_subject();
    let wrap_aad = match state.key_binding() {
        ObjectEncryptionKeyBinding::ProvisionalV1 => SSE_C_WRAP_AAD,
        ObjectEncryptionKeyBinding::ObjectSubjectV2 => wrap_subject.aead_context(),
    };
    let dek = unwrap_managed_dek(
        &kek,
        state.wrap_nonce(),
        state.wrapped_dek(),
        wrap_aad,
        "SSE-C",
    )?;
    encrypt_segment_with_dek_and_prefix(
        &dek,
        input.plaintext,
        SegmentSealParameters {
            payload_codec: input.source_codec,
            segment_nonce_prefix: state.segment_nonce_prefix(),
            segment_scope: input.segment_scope,
            payload_aead_context: input.subject.payload_aead_context(),
            segment_index: input.segment_index,
            descriptor: AeadDescriptor {
                aad: SSE_C_SEGMENT_AAD,
                label: "SSE-C",
            },
        },
    )
}

fn decrypt_legacy_segment_with_dek_and_prefix_in_place(
    dek: &[u8; SSE_C_DEK_LEN],
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &mut Vec<u8>,
    plaintext_len: usize,
    descriptor: AeadDescriptor<'_>,
) -> Result<(), ServerError> {
    let opening_key = Aes256GcmKey::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {} segment opening key", descriptor.label),
    })?;
    observability::record_payload_read_metrics(observability::PayloadReadMetricDelta {
        aead_records: 1,
        ..observability::PayloadReadMetricDelta::default()
    });
    let actual_plaintext_len = opening_key
        .open_in_place(
            segment_nonce(segment_nonce_prefix, segment_scope, segment_index),
            descriptor.aad,
            ciphertext,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to decrypt {} segment", descriptor.label),
        })?
        .len();
    if actual_plaintext_len != plaintext_len {
        return Err(ServerError::InternalError {
            reason: format!(
                "decrypted {} segment length {} did not match expected {}",
                descriptor.label, actual_plaintext_len, plaintext_len
            ),
        });
    }
    ciphertext.truncate(plaintext_len);
    Ok(())
}

fn decrypt_cobblestone_segment_in_place(
    dek: &[u8; SSE_C_DEK_LEN],
    payload_aead_context: &[u8],
    ciphertext: &mut Vec<u8>,
    plaintext_len: usize,
    descriptor: AeadDescriptor<'_>,
) -> Result<(), ServerError> {
    let failure = || ServerError::InternalError {
        reason: format!("failed to decrypt {} Cobblestone segment", descriptor.label),
    };
    let layout = Cobblestone256MessageLayout::from_ciphertext_len(ciphertext.len() as u64)
        .map_err(|_| failure())?;
    if layout.plaintext_len() != plaintext_len as u64 {
        return Err(ServerError::InternalError {
            reason: format!(
                "decrypted {} Cobblestone segment length {} did not match expected {}",
                descriptor.label,
                layout.plaintext_len(),
                plaintext_len
            ),
        });
    }
    let key = Cobblestone256Key::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!(
            "failed to create {} Cobblestone segment opening key",
            descriptor.label
        ),
    })?;
    let mut opener =
        Cobblestone256StreamOpener::new(key, payload_aead_context, ciphertext.len() as u64)
            .map_err(|_| failure())?;
    let mut plaintext = Vec::with_capacity(plaintext_len);
    opener
        .update(&ciphertext[..COBBLESTONE_HEADER_LEN], &mut plaintext)
        .map_err(|_| failure())?;
    for chunk_index in 0..layout.chunk_count() {
        let range = layout
            .ciphertext_range_for_chunk(chunk_index)
            .map_err(|_| failure())?;
        let start = usize::try_from(range.start).map_err(|_| failure())?;
        let end = usize::try_from(range.end).map_err(|_| failure())?;
        observability::record_payload_read_metrics(observability::PayloadReadMetricDelta {
            aead_records: 1,
            ..observability::PayloadReadMetricDelta::default()
        });
        opener
            .update(&ciphertext[start..end], &mut plaintext)
            .map_err(|_| failure())?;
    }
    opener.finish().map_err(|_| failure())?;
    if plaintext.len() != plaintext_len {
        return Err(ServerError::InternalError {
            reason: format!(
                "decrypted {} Cobblestone segment length {} did not match expected {}",
                descriptor.label,
                plaintext.len(),
                plaintext_len
            ),
        });
    }
    *ciphertext = plaintext;
    Ok(())
}

fn segment_nonce(
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..SSE_C_SEGMENT_NONCE_PREFIX_LEN].copy_from_slice(segment_nonce_prefix);
    let scope_start = SSE_C_SEGMENT_NONCE_PREFIX_LEN;
    let scope_end = scope_start + SSE_C_SEGMENT_NONCE_SCOPE_LEN;
    nonce[scope_start..scope_end].copy_from_slice(&segment_scope.encode());
    nonce[scope_end..].copy_from_slice(&segment_index.to_be_bytes());
    nonce
}

fn encode_checksum_metadata(
    checksum: &ObjectChecksumMetadata,
) -> Result<Vec<u8>, ChecksumMetadataCodecError> {
    let value = checksum.value().as_bytes();
    if value.len() > CHECKSUM_METADATA_MAX_VALUE_LEN {
        return Err(ChecksumMetadataCodecError::ValueTooLong {
            actual: value.len(),
            maximum: CHECKSUM_METADATA_MAX_VALUE_LEN,
        });
    }
    let value_len = u16::try_from(value.len())
        .expect("checksum metadata value length is bounded below u16::MAX");
    let mut out = Vec::with_capacity(CHECKSUM_METADATA_HEADER_LEN + value.len());
    out.push(CHECKSUM_METADATA_VERSION);
    out.push(checksum.algorithm().wire_tag());
    out.push(checksum::ChecksumType::optional_wire_tag(
        checksum.checksum_type(),
    ));
    out.extend_from_slice(&value_len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(out)
}

fn decode_checksum_metadata(
    data: &[u8],
) -> Result<ObjectChecksumMetadata, ChecksumMetadataCodecError> {
    if data.len() < CHECKSUM_METADATA_HEADER_LEN {
        return Err(ChecksumMetadataCodecError::Truncated {
            actual: data.len(),
            minimum: CHECKSUM_METADATA_HEADER_LEN,
        });
    }
    if data[0] != CHECKSUM_METADATA_VERSION {
        return Err(ChecksumMetadataCodecError::UnsupportedVersion { version: data[0] });
    }
    let algorithm = checksum::ChecksumAlgorithm::from_wire_tag(data[1])
        .ok_or(ChecksumMetadataCodecError::InvalidAlgorithmTag { wire_tag: data[1] })?;
    let checksum_type = checksum::ChecksumType::from_optional_wire_tag(data[2])
        .ok_or(ChecksumMetadataCodecError::InvalidChecksumTypeTag { wire_tag: data[2] })?;
    let value_len = usize::from(u16::from_be_bytes([data[3], data[4]]));
    if data.len() != CHECKSUM_METADATA_HEADER_LEN + value_len {
        return Err(ChecksumMetadataCodecError::InvalidLength {
            declared: value_len,
            actual: data.len(),
        });
    }
    let value = std::str::from_utf8(&data[CHECKSUM_METADATA_HEADER_LEN..])
        .map_err(|_| ChecksumMetadataCodecError::InvalidUtf8)?;
    Ok(ObjectChecksumMetadata::new(
        algorithm,
        checksum_type,
        value.to_string(),
    ))
}

fn encrypt_checksum_with_dek(
    dek: &[u8; SSE_C_DEK_LEN],
    checksum: Option<&ObjectChecksumMetadata>,
    aad: &[u8],
    label: &str,
) -> Result<([u8; SSE_C_CHECKSUM_NONCE_LEN], Vec<u8>), ServerError> {
    let Some(checksum) = checksum else {
        return Ok(([0u8; SSE_C_CHECKSUM_NONCE_LEN], Vec::new()));
    };
    let mut nonce = [0u8; SSE_C_CHECKSUM_NONCE_LEN];
    argmin_crypto::random::fill(&mut nonce).map_err(|_| ServerError::InternalError {
        reason: format!("failed to generate {label} checksum nonce"),
    })?;
    let plaintext = encode_checksum_metadata(checksum).map_err(checksum_metadata_codec_error)?;
    let ciphertext = seal_checksum_plaintext_with_dek(dek, nonce, plaintext, aad, label)?;
    Ok((nonce, ciphertext))
}

fn seal_checksum_plaintext_with_dek(
    dek: &[u8; SSE_C_DEK_LEN],
    nonce: [u8; SSE_C_CHECKSUM_NONCE_LEN],
    mut plaintext: Vec<u8>,
    aad: &[u8],
    label: &str,
) -> Result<Vec<u8>, ServerError> {
    let sealing_key = Aes256GcmKey::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {label} checksum sealing key"),
    })?;
    sealing_key
        .seal_in_place_append_tag(nonce, aad, &mut plaintext)
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to encrypt {label} checksum metadata"),
        })?;
    Ok(plaintext)
}

#[cfg(test)]
fn test_encrypt_checksum_metadata_version(
    dek: &[u8; SSE_C_DEK_LEN],
    checksum: &ObjectChecksumMetadata,
    version: u8,
    aad: &[u8],
    label: &str,
) -> Result<([u8; SSE_C_CHECKSUM_NONCE_LEN], Vec<u8>), ServerError> {
    assert_ne!(version, CHECKSUM_METADATA_VERSION);
    let nonce = [version.wrapping_add(1); SSE_C_CHECKSUM_NONCE_LEN];
    let mut buf = encode_checksum_metadata(checksum).map_err(checksum_metadata_codec_error)?;
    buf[0] = version;
    let ciphertext = seal_checksum_plaintext_with_dek(dek, nonce, buf, aad, label)?;
    Ok((nonce, ciphertext))
}

fn decrypt_checksum_with_dek(
    dek: &[u8; SSE_C_DEK_LEN],
    checksum_nonce: &[u8; SSE_C_CHECKSUM_NONCE_LEN],
    encrypted_checksum_metadata: &[u8],
    aad: &[u8],
    label: &str,
) -> Result<Option<ObjectChecksumMetadata>, ServerError> {
    if encrypted_checksum_metadata.is_empty() {
        return Ok(None);
    }
    let opening_key = Aes256GcmKey::new(dek).map_err(|_| ServerError::InternalError {
        reason: format!("failed to create {label} checksum opening key"),
    })?;
    let mut buf = encrypted_checksum_metadata.to_vec();
    let plaintext = opening_key
        .open_in_place(*checksum_nonce, aad, &mut buf)
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to decrypt {label} checksum metadata"),
        })?;
    Ok(Some(
        decode_checksum_metadata(plaintext).map_err(checksum_metadata_codec_error)?,
    ))
}

#[cfg(test)]
struct SseCustomerChecksumProfileFixture {
    validator: SseCustomerValidatorConfig,
    request: SseCustomerRequest,
    validator_salt: [u8; SSE_C_VALIDATOR_SALT_LEN],
    validator_hmac: [u8; SSE_C_VALIDATOR_HMAC_LEN],
    wrap_salt: [u8; SSE_C_WRAP_SALT_LEN],
    wrap_nonce: [u8; SSE_C_WRAP_NONCE_LEN],
    dek: [u8; SSE_C_DEK_LEN],
    kek: [u8; SSE_C_DEK_LEN],
    wrapped_dek: [u8; SSE_C_WRAPPED_DEK_LEN],
    segment_nonce_prefix: [u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_plaintext: &'static [u8],
    segment_ciphertext: Vec<u8>,
    checksum_nonce: [u8; SSE_C_CHECKSUM_NONCE_LEN],
    checksum: ObjectChecksumMetadata,
    checksum_plaintext: Vec<u8>,
    checksum_ciphertext: Vec<u8>,
    encryption: ObjectEncryption,
}

#[cfg(test)]
fn sse_customer_checksum_profile_fixture() -> SseCustomerChecksumProfileFixture {
    let customer_key = [0x11; SSE_C_CUSTOMER_KEY_LEN];
    let validator = SseCustomerValidatorConfig {
        key_id: 7,
        validator_key: [0x22; 32],
    };
    let request = SseCustomerRequest::new(customer_key, "fixed-vector-md5".to_string());
    let validator_salt = [0x33; SSE_C_VALIDATOR_SALT_LEN];
    let validator_hmac = compute_validator_hmac(&validator, &validator_salt, &customer_key);
    let wrap_salt = [0x44; SSE_C_WRAP_SALT_LEN];
    let wrap_nonce = [0x55; SSE_C_WRAP_NONCE_LEN];
    let dek = [0x66; SSE_C_DEK_LEN];
    let kek = derive_wrap_key(&customer_key, &wrap_salt).unwrap();
    let wrapped_dek = wrap_managed_dek(&kek, &wrap_nonce, &dek, SSE_C_WRAP_AAD, "SSE-C").unwrap();
    let segment_nonce_prefix = [0x77; SSE_C_SEGMENT_NONCE_PREFIX_LEN];
    let segment_plaintext = b"compat-sse-c";
    let segment_ciphertext = encrypt_legacy_segment_with_dek_and_prefix(
        &dek,
        &segment_nonce_prefix,
        SseCustomerSegmentScope::object(),
        7,
        segment_plaintext,
        AeadDescriptor {
            aad: SSE_C_SEGMENT_AAD,
            label: "SSE-C",
        },
    )
    .unwrap();
    let checksum_nonce = [0x88; SSE_C_CHECKSUM_NONCE_LEN];
    let checksum = ObjectChecksumMetadata::new(
        checksum::ChecksumAlgorithm::Sha256,
        Some(checksum::ChecksumType::FullObject),
        "0123456789abcdef".to_string(),
    );
    let checksum_plaintext = encode_checksum_metadata(&checksum).unwrap();
    let checksum_ciphertext = seal_checksum_plaintext_with_dek(
        &dek,
        checksum_nonce,
        checksum_plaintext.clone(),
        SSE_C_CHECKSUM_AAD,
        "SSE-C",
    )
    .unwrap();
    let encryption = ObjectEncryption::SseCustomer(
        SseCustomerObjectState::new(
            validator.key_id,
            validator_salt,
            validator_hmac,
            wrap_salt,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
        )
        .with_encrypted_checksum_metadata(checksum_nonce, checksum_ciphertext.clone())
        .unwrap(),
    );

    SseCustomerChecksumProfileFixture {
        validator,
        request,
        validator_salt,
        validator_hmac,
        wrap_salt,
        wrap_nonce,
        dek,
        kek,
        wrapped_dek,
        segment_nonce_prefix,
        segment_plaintext,
        segment_ciphertext,
        checksum_nonce,
        checksum,
        checksum_plaintext,
        checksum_ciphertext,
        encryption,
    }
}

#[cfg(test)]
pub(crate) fn test_sse_customer_checksum_profile_fixture() -> (
    SseCustomerValidatorConfig,
    SseCustomerRequest,
    ObjectEncryption,
    ObjectChecksumMetadata,
) {
    let fixture = sse_customer_checksum_profile_fixture();
    (
        fixture.validator,
        fixture.request,
        fixture.encryption,
        fixture.checksum,
    )
}

#[cfg(test)]
struct SseS3ChecksumProfileFixture {
    wrapping_key_id: u32,
    provider: StaticManagedKeyProvider,
    dek: [u8; SSE_C_DEK_LEN],
    wrap_nonce: [u8; SSE_S3_WRAP_NONCE_LEN],
    wrapped_dek: [u8; storage::SSE_S3_WRAPPED_DEK_LEN],
    segment_nonce_prefix: [u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    segment_plaintext: &'static [u8],
    segment_ciphertext: Vec<u8>,
    checksum_nonce: [u8; SSE_S3_CHECKSUM_NONCE_LEN],
    checksum: ObjectChecksumMetadata,
    checksum_plaintext: Vec<u8>,
    checksum_ciphertext: Vec<u8>,
    encryption: ObjectEncryption,
}

#[cfg(test)]
fn sse_s3_checksum_profile_fixture() -> SseS3ChecksumProfileFixture {
    let wrapping_key_id = 7;
    let wrapping_key = [0x11u8; 32];
    let provider = StaticManagedKeyProvider::single(ManagedWrappingKeyConfig {
        key_id: wrapping_key_id,
        wrapping_key,
    });
    let dek = [0x22u8; SSE_C_DEK_LEN];
    let wrap_nonce = [0x33u8; SSE_S3_WRAP_NONCE_LEN];
    let wrapped_dek = wrap_managed_dek(
        &wrapping_key,
        &wrap_nonce,
        &dek,
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )
    .unwrap();
    let segment_nonce_prefix = [0x44u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN];
    let segment_plaintext = b"compat-sse-s3";
    let segment_ciphertext = encrypt_legacy_segment_with_dek_and_prefix(
        &dek,
        &segment_nonce_prefix,
        SseCustomerSegmentScope::object(),
        7,
        segment_plaintext,
        AeadDescriptor {
            aad: MANAGED_SEGMENT_AAD,
            label: MANAGED_ENCRYPTION_LABEL,
        },
    )
    .unwrap();
    let checksum_nonce = [0x55u8; SSE_S3_CHECKSUM_NONCE_LEN];
    let checksum = ObjectChecksumMetadata::new(
        checksum::ChecksumAlgorithm::Sha256,
        Some(checksum::ChecksumType::FullObject),
        "0123456789abcdef".to_string(),
    );
    let checksum_plaintext = encode_checksum_metadata(&checksum).unwrap();
    let checksum_ciphertext = seal_checksum_plaintext_with_dek(
        &dek,
        checksum_nonce,
        checksum_plaintext.clone(),
        MANAGED_CHECKSUM_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )
    .unwrap();
    let encryption = ObjectEncryption::SseS3(
        SseS3ObjectState::new(
            wrapping_key_id,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
        )
        .with_encrypted_checksum_metadata(checksum_nonce, checksum_ciphertext.clone())
        .unwrap(),
    );

    SseS3ChecksumProfileFixture {
        wrapping_key_id,
        provider,
        dek,
        wrap_nonce,
        wrapped_dek,
        segment_nonce_prefix,
        segment_plaintext,
        segment_ciphertext,
        checksum_nonce,
        checksum,
        checksum_plaintext,
        checksum_ciphertext,
        encryption,
    }
}

#[cfg(test)]
pub(crate) fn test_sse_s3_checksum_profile_inputs(
) -> (StaticManagedKeyProvider, ObjectChecksumMetadata) {
    let fixture = sse_s3_checksum_profile_fixture();
    (fixture.provider, fixture.checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_metadata::ObjectChecksumMetadata;
    use checksum::{ChecksumAlgorithm, ChecksumType};

    fn request() -> SseCustomerRequest {
        SseCustomerRequest::new([7u8; SSE_C_CUSTOMER_KEY_LEN], "dummy-md5".to_string())
    }

    fn validator() -> SseCustomerValidatorConfig {
        SseCustomerValidatorConfig {
            key_id: 1,
            validator_key: [9u8; 32],
        }
    }

    fn missing_validator() -> SseCustomerValidatorConfig {
        SseCustomerValidatorConfig {
            key_id: 2,
            validator_key: [9u8; 32],
        }
    }

    fn managed_key_provider() -> StaticManagedKeyProvider {
        StaticManagedKeyProvider::single(ManagedWrappingKeyConfig {
            key_id: 7,
            wrapping_key: [11u8; 32],
        })
    }

    fn object_subject(
        encryption: &ObjectEncryption,
        key: &str,
    ) -> storage::ObjectEncryptionSubject {
        storage::test_support::object_encryption_subject_fixture(
            &storage::ObjectKey::try_from(key).unwrap(),
            encryption,
        )
    }

    fn segment_subject(
        encryption: &ObjectEncryption,
        key: &str,
        logical_plaintext_len: usize,
    ) -> storage::SegmentEncryptionSubject {
        storage::test_support::standard_segment_encryption_subject_fixture(
            &storage::ObjectKey::try_from(key).unwrap(),
            encryption,
            u64::try_from(logical_plaintext_len).unwrap(),
        )
    }

    fn terminal_checksum() -> ObjectChecksumMetadata {
        ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::FullObject),
            "AQIDBAUGBwg=".to_string(),
        )
    }

    #[test]
    fn sse_customer_terminal_binding_rewraps_and_opens_only_under_exact_object_subject() {
        let validator = validator();
        let request = request();
        let write = prepare_sse_customer_write_with_payload_codec(
            &validator,
            &request,
            SegmentPayloadCodec::Cobblestone256SegmentV1,
        )
        .unwrap();
        let provisional = write.encryption().clone();
        let provisional_subject = object_subject(&provisional, "object");
        let plaintext = b"payload encrypted before terminal SSE-C rewrap";
        let provisional_segment_subject = segment_subject(&provisional, "object", plaintext.len());
        let ciphertext = write
            .encrypt_authorized_segment(&provisional_segment_subject, 0, plaintext)
            .unwrap();
        let checksum = terminal_checksum();
        let committed = write
            .seal_committed_object_encryption(&provisional_subject, Some(&checksum))
            .unwrap();
        assert_eq!(
            committed.key_binding(),
            ObjectEncryptionKeyBinding::ObjectSubjectV2
        );
        let ObjectEncryption::SseCustomer(committed_state) = &committed else {
            unreachable!()
        };
        let committed_subject = object_subject(&committed, "object");
        assert_eq!(
            decrypt_authorized_sse_customer_checksum(
                &validator,
                committed_state,
                &request,
                &committed_subject,
            )
            .unwrap(),
            Some(checksum)
        );
        let committed_segment_subject = segment_subject(&committed, "object", plaintext.len());
        let mut opened_payload = ciphertext.clone();
        decrypt_authorized_sse_customer_segment_in_place(
            &validator,
            committed_state,
            &request,
            AuthorizedSegmentRead::new(
                &committed_segment_subject,
                SseCustomerSegmentScope::object(),
                0,
                plaintext.len(),
            ),
            &mut opened_payload,
        )
        .unwrap();
        assert_eq!(opened_payload, plaintext);
        assert!(decrypt_sse_customer_checksum(&validator, committed_state, &request).is_err());
        let crossed_subject = object_subject(&committed, "crossed-object");
        assert!(decrypt_authorized_sse_customer_checksum(
            &validator,
            committed_state,
            &request,
            &crossed_subject,
        )
        .is_err());
        let crossed_segment_subject =
            segment_subject(&committed, "crossed-object", plaintext.len());
        let mut crossed_payload = ciphertext;
        assert!(decrypt_authorized_sse_customer_segment_in_place(
            &validator,
            committed_state,
            &request,
            AuthorizedSegmentRead::new(
                &crossed_segment_subject,
                SseCustomerSegmentScope::object(),
                0,
                plaintext.len(),
            ),
            &mut crossed_payload,
        )
        .is_err());
    }

    #[test]
    fn managed_terminal_binding_rewraps_and_opens_only_under_exact_object_subject() {
        let provider = managed_key_provider();
        let write = prepare_managed_encryption_write_with_payload_codec(
            &provider,
            SegmentPayloadCodec::Cobblestone256SegmentV1,
        )
        .unwrap();
        let provisional = write.encryption().clone();
        let provisional_subject = object_subject(&provisional, "object");
        let plaintext = b"payload encrypted before terminal SSE-S3 rewrap";
        let provisional_segment_subject = segment_subject(&provisional, "object", plaintext.len());
        let ciphertext = write
            .encrypt_authorized_segment(&provisional_segment_subject, 0, plaintext)
            .unwrap();
        let checksum = terminal_checksum();
        let committed = write
            .seal_committed_object_encryption(&provisional_subject, Some(&checksum))
            .unwrap();
        assert_eq!(
            committed.key_binding(),
            ObjectEncryptionKeyBinding::ObjectSubjectV2
        );
        let ObjectEncryption::SseS3(committed_state) = &committed else {
            unreachable!()
        };
        let committed_subject = object_subject(&committed, "object");
        assert_eq!(
            decrypt_authorized_managed_encryption_checksum(
                &provider,
                committed_state,
                &committed_subject,
            )
            .unwrap(),
            Some(checksum)
        );
        let committed_segment_subject = segment_subject(&committed, "object", plaintext.len());
        let mut opened_payload = ciphertext.clone();
        decrypt_authorized_managed_encryption_segment_in_place(
            &provider,
            committed_state,
            AuthorizedSegmentRead::new(
                &committed_segment_subject,
                SseCustomerSegmentScope::object(),
                0,
                plaintext.len(),
            ),
            &mut opened_payload,
        )
        .unwrap();
        assert_eq!(opened_payload, plaintext);
        assert!(decrypt_managed_encryption_checksum(&provider, committed_state).is_err());
        let crossed_subject = object_subject(&committed, "crossed-object");
        assert!(decrypt_authorized_managed_encryption_checksum(
            &provider,
            committed_state,
            &crossed_subject,
        )
        .is_err());
        let crossed_segment_subject =
            segment_subject(&committed, "crossed-object", plaintext.len());
        let mut crossed_payload = ciphertext;
        assert!(decrypt_authorized_managed_encryption_segment_in_place(
            &provider,
            committed_state,
            AuthorizedSegmentRead::new(
                &crossed_segment_subject,
                SseCustomerSegmentScope::object(),
                0,
                plaintext.len(),
            ),
            &mut crossed_payload,
        )
        .is_err());
    }

    #[test]
    fn cobblestone_segment_profile_dispatch_authenticates_exact_context() {
        let dek = [0x41; SSE_C_DEK_LEN];
        let nonce_prefix = [0x52; SSE_C_SEGMENT_NONCE_PREFIX_LEN];
        let context = b"storage-minted-segment-subject";

        for plaintext_len in [
            0,
            1,
            cobblestone::COBBLESTONE_CHUNK_LEN - 1,
            cobblestone::COBBLESTONE_CHUNK_LEN,
            cobblestone::COBBLESTONE_CHUNK_LEN + 1,
        ] {
            let plaintext = vec![0x63; plaintext_len];
            let ciphertext = encrypt_segment_with_dek_and_prefix(
                &dek,
                &plaintext,
                SegmentSealParameters {
                    payload_codec: SegmentPayloadCodec::Cobblestone256SegmentV1,
                    segment_nonce_prefix: &nonce_prefix,
                    segment_scope: SseCustomerSegmentScope::object(),
                    payload_aead_context: context,
                    segment_index: 7,
                    descriptor: AeadDescriptor {
                        aad: MANAGED_SEGMENT_AAD,
                        label: MANAGED_ENCRYPTION_LABEL,
                    },
                },
            )
            .unwrap();
            assert_eq!(
                ciphertext.len() as u64,
                SegmentPayloadCodec::Cobblestone256SegmentV1
                    .stored_size(plaintext_len as u64)
                    .unwrap()
            );
            assert_eq!(
                cobblestone::open(Cobblestone256Key::new(&dek).unwrap(), context, &ciphertext)
                    .unwrap(),
                plaintext
            );
            let mut dispatched = ciphertext.clone();
            decrypt_cobblestone_segment_in_place(
                &dek,
                context,
                &mut dispatched,
                plaintext_len,
                AeadDescriptor {
                    aad: MANAGED_SEGMENT_AAD,
                    label: MANAGED_ENCRYPTION_LABEL,
                },
            )
            .unwrap();
            assert_eq!(dispatched, plaintext);

            let mut crossed = ciphertext.clone();
            assert!(decrypt_cobblestone_segment_in_place(
                &dek,
                b"crossed-storage-subject",
                &mut crossed,
                plaintext_len,
                AeadDescriptor {
                    aad: MANAGED_SEGMENT_AAD,
                    label: MANAGED_ENCRYPTION_LABEL,
                },
            )
            .is_err());
            assert_eq!(crossed, ciphertext);
            assert!(cobblestone::open(
                Cobblestone256Key::new(&dek).unwrap(),
                b"crossed-storage-subject",
                &ciphertext,
            )
            .is_err());
        }
    }

    #[test]
    fn sse_config_from_base64_reports_typed_errors() {
        assert_eq!(
            SseCustomerValidatorConfig::from_base64(1, "not base64").unwrap_err(),
            SseCustomerValidatorConfigError::InvalidBase64
        );
        assert_eq!(
            SseCustomerValidatorConfig::from_base64(1, "AQID").unwrap_err(),
            SseCustomerValidatorConfigError::InvalidLength
        );
        assert_eq!(
            ManagedWrappingKeyConfig::from_base64(1, "not base64").unwrap_err(),
            ManagedWrappingKeyConfigError::InvalidBase64
        );
        assert_eq!(
            ManagedWrappingKeyConfig::from_base64(1, "AQID").unwrap_err(),
            ManagedWrappingKeyConfigError::InvalidLength
        );
    }

    #[test]
    fn sse_c_round_trip() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let mut ciphertext = ctx.encrypt_segment(3, b"hello world").unwrap();
        let allocation = ciphertext.as_ptr();
        let capacity = ciphertext.capacity();
        decrypt_sse_customer_segment_in_place(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::object(),
            3,
            &mut ciphertext,
            11,
        )
        .unwrap();
        assert_eq!(ciphertext, b"hello world");
        assert_eq!(ciphertext.as_ptr(), allocation);
        assert_eq!(ciphertext.capacity(), capacity);
    }

    #[test]
    fn sse_c_rejects_wrong_key() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let mut ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let wrong = SseCustomerRequest::new([1u8; SSE_C_CUSTOMER_KEY_LEN], "wrong".to_string());
        let err = decrypt_sse_customer_segment_in_place(
            &validator,
            state,
            &wrong,
            SseCustomerSegmentScope::object(),
            0,
            &mut ciphertext,
            3,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn sse_c_missing_validator_key_is_internal_error() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let mut ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let err = decrypt_sse_customer_segment_in_place(
            &missing_validator(),
            state,
            &req,
            SseCustomerSegmentScope::object(),
            0,
            &mut ciphertext,
            3,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InternalError { .. }));
    }

    #[test]
    fn sse_c_multipart_nonce_scope_is_part_specific() {
        let req = request();
        let validator = validator();
        let base_ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = base_ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let part1_ctx = resume_sse_customer_write(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(1).unwrap(),
        )
        .unwrap();
        let part2_ctx = resume_sse_customer_write(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
        )
        .unwrap();

        let mut part1_ciphertext = part1_ctx.encrypt_segment(0, b"same-data").unwrap();
        let part2_ciphertext = part2_ctx.encrypt_segment(0, b"same-data").unwrap();
        assert_ne!(part1_ciphertext, part2_ciphertext);
        let mut wrong_scope_ciphertext = part1_ciphertext.clone();

        decrypt_sse_customer_segment_in_place(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(1).unwrap(),
            0,
            &mut part1_ciphertext,
            9,
        )
        .unwrap();
        assert_eq!(part1_ciphertext, b"same-data");

        let err = decrypt_sse_customer_segment_in_place(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
            0,
            &mut wrong_scope_ciphertext,
            9,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InternalError { .. }));
    }

    #[test]
    fn sse_c_resume_write_rejects_wrong_key_with_invalid_request() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let wrong = SseCustomerRequest::new([1u8; SSE_C_CUSTOMER_KEY_LEN], "wrong".to_string());
        let err = resume_sse_customer_write(
            &validator,
            state,
            &wrong,
            SseCustomerSegmentScope::multipart_part(1).unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn sse_c_resume_write_missing_validator_key_is_internal_error() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let err = resume_sse_customer_write(
            &missing_validator(),
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(1).unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InternalError { .. }));
    }

    #[test]
    fn sse_c_checksum_metadata_round_trip() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let checksum = ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Sha256,
            Some(ChecksumType::FullObject),
            "deadbeef".to_string(),
        );
        let ObjectEncryption::SseCustomer(state) =
            ctx.seal_checksum_metadata(Some(&checksum)).unwrap()
        else {
            panic!("expected SSE-C object state");
        };
        assert!(!state.encrypted_checksum_metadata().is_empty());
        let decrypted = decrypt_sse_customer_checksum(&validator, &state, &req)
            .unwrap()
            .expect("expected checksum metadata");
        assert_eq!(decrypted, checksum);
    }

    #[test]
    fn current_sse_c_checksum_profile_matches_frozen_vector_and_requires_outer_version_bump() {
        fn assert_authentication_failure(
            result: Result<Option<ObjectChecksumMetadata>, ServerError>,
        ) {
            let error = result.unwrap_err();
            assert_eq!(
                error.to_string(),
                "internal error: failed to decrypt SSE-C checksum metadata"
            );
        }

        assert_eq!(SSE_C_HKDF_INFO, b"argmin:sse-c:kek:v1");
        assert_eq!(SSE_C_WRAP_AAD, b"argmin:sse-c:wrap:v1");
        assert_eq!(SSE_C_CHECKSUM_AAD, b"argmin:sse-c:checksum:v1");

        let fixture = sse_customer_checksum_profile_fixture();
        assert_eq!(
            fixture.validator_hmac,
            [
                0xea, 0x57, 0x82, 0x44, 0x46, 0x83, 0x57, 0xae, 0x9b, 0x38, 0x93, 0x4c, 0xa6, 0xb9,
                0xf3, 0x7b, 0x57, 0xd2, 0xab, 0x78, 0x5f, 0x8a, 0xde, 0x3e, 0xb4, 0xa4, 0xe6, 0x79,
                0x17, 0x88, 0xa2, 0x7a,
            ]
        );

        assert_eq!(
            fixture.kek,
            [
                0xd6, 0x84, 0x1a, 0xad, 0x4f, 0x9d, 0x55, 0x9a, 0xf8, 0x73, 0xb8, 0x87, 0xf5, 0x55,
                0x64, 0x9f, 0x7b, 0x61, 0x90, 0xa8, 0x4b, 0x42, 0xd9, 0xf5, 0x82, 0x7c, 0xf2, 0x30,
                0x6b, 0x81, 0x86, 0xc6,
            ]
        );

        assert_eq!(
            fixture.wrapped_dek,
            [
                0x11, 0x20, 0x6a, 0xf3, 0x3d, 0x69, 0xa9, 0x44, 0x20, 0x7d, 0xd6, 0xe2, 0x0c, 0xf4,
                0x2d, 0x58, 0x1c, 0xe7, 0x25, 0x63, 0x04, 0xc4, 0x39, 0x68, 0x1d, 0xbb, 0x6d, 0x1f,
                0xcd, 0x6a, 0xa9, 0x14, 0x72, 0x1b, 0xc8, 0xfb, 0x23, 0xc0, 0x29, 0x72, 0x36, 0x93,
                0xe0, 0x62, 0x56, 0x70, 0xd0, 0x6b,
            ]
        );
        assert_eq!(
            fixture.segment_ciphertext,
            vec![
                0x42, 0x67, 0x70, 0xfb, 0x3a, 0x5e, 0x07, 0xe1, 0x95, 0xf7, 0xf1, 0x96, 0x49, 0x3c,
                0x59, 0x52, 0xdc, 0x87, 0x2e, 0xbb, 0xd4, 0x9b, 0xe4, 0x01, 0xdd, 0x1b, 0x99, 0x44,
            ]
        );
        let mut decrypted_segment = fixture.segment_ciphertext.clone();
        decrypt_legacy_segment_with_dek_and_prefix_in_place(
            &fixture.dek,
            &fixture.segment_nonce_prefix,
            SseCustomerSegmentScope::object(),
            7,
            &mut decrypted_segment,
            fixture.segment_plaintext.len(),
            AeadDescriptor {
                aad: SSE_C_SEGMENT_AAD,
                label: "SSE-C",
            },
        )
        .unwrap();
        assert_eq!(decrypted_segment, fixture.segment_plaintext);

        let mut crossed_segment = fixture.segment_ciphertext.clone();
        assert!(decrypt_legacy_segment_with_dek_and_prefix_in_place(
            &[0x67; SSE_C_DEK_LEN],
            &fixture.segment_nonce_prefix,
            SseCustomerSegmentScope::object(),
            7,
            &mut crossed_segment,
            fixture.segment_plaintext.len(),
            AeadDescriptor {
                aad: SSE_C_SEGMENT_AAD,
                label: "SSE-C",
            },
        )
        .is_err());

        assert_eq!(
            fixture.checksum_plaintext,
            [
                0x01, 0x03, 0x01, 0x00, 0x10, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
                0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
            ]
        );
        assert_eq!(
            fixture.checksum_ciphertext,
            [
                0xc6, 0xc3, 0x32, 0x20, 0xb4, 0xad, 0x22, 0xb8, 0xae, 0xd3, 0x02, 0x28, 0x72, 0x6a,
                0xb2, 0xd2, 0xde, 0xe6, 0x88, 0x5a, 0x87, 0x5f, 0xd0, 0x44, 0x47, 0x51, 0x69, 0xee,
                0xb0, 0x46, 0x82, 0x6e, 0xf5, 0xba, 0xe8, 0x17, 0xc2,
            ]
        );

        let make_state = |nonce, ciphertext| {
            SseCustomerObjectState::new(
                fixture.validator.key_id,
                fixture.validator_salt,
                fixture.validator_hmac,
                fixture.wrap_salt,
                fixture.wrap_nonce,
                fixture.wrapped_dek,
                fixture.segment_nonce_prefix,
            )
            .with_encrypted_checksum_metadata(nonce, ciphertext)
            .unwrap()
        };
        let ObjectEncryption::SseCustomer(state) = &fixture.encryption else {
            panic!("expected SSE-C fixture state");
        };
        assert_eq!(
            decrypt_sse_customer_checksum(&fixture.validator, state, &fixture.request).unwrap(),
            Some(fixture.checksum.clone())
        );

        let mut wrong_nonce = fixture.checksum_nonce;
        wrong_nonce[0] ^= 1;
        let wrong_nonce_state = make_state(wrong_nonce, fixture.checksum_ciphertext.clone());
        assert_authentication_failure(decrypt_sse_customer_checksum(
            &fixture.validator,
            &wrong_nonce_state,
            &fixture.request,
        ));
        assert_authentication_failure(decrypt_checksum_with_dek(
            &fixture.dek,
            &fixture.checksum_nonce,
            &fixture.checksum_ciphertext,
            b"argmin:sse-c:checksum:v2",
            "SSE-C",
        ));

        let mut corrupted_ciphertext = fixture.checksum_ciphertext.clone();
        corrupted_ciphertext[0] ^= 1;
        let corrupted_ciphertext_state = make_state(fixture.checksum_nonce, corrupted_ciphertext);
        assert_authentication_failure(decrypt_sse_customer_checksum(
            &fixture.validator,
            &corrupted_ciphertext_state,
            &fixture.request,
        ));

        let mut corrupted_tag = fixture.checksum_ciphertext.clone();
        let tag_byte = corrupted_tag.last_mut().unwrap();
        *tag_byte ^= 1;
        let corrupted_tag_state = make_state(fixture.checksum_nonce, corrupted_tag);
        assert_authentication_failure(decrypt_sse_customer_checksum(
            &fixture.validator,
            &corrupted_tag_state,
            &fixture.request,
        ));
    }

    #[test]
    fn checksum_metadata_plaintext_v1_corpus_is_exact() {
        let algorithm_cases = [
            (ChecksumAlgorithm::Crc32, 0),
            (ChecksumAlgorithm::Crc32c, 1),
            (ChecksumAlgorithm::Sha1, 2),
            (ChecksumAlgorithm::Sha256, 3),
            (ChecksumAlgorithm::Crc64nvme, 4),
            (ChecksumAlgorithm::Md5, 5),
            (ChecksumAlgorithm::XxHash64, 6),
            (ChecksumAlgorithm::XxHash3, 7),
            (ChecksumAlgorithm::XxHash128, 8),
            (ChecksumAlgorithm::Sha512, 9),
        ];
        for (algorithm, wire_tag) in algorithm_cases {
            let checksum = ObjectChecksumMetadata::new(algorithm, None, "v".to_string());
            let expected = [1, wire_tag, 255, 0, 1, b'v'];
            assert_eq!(encode_checksum_metadata(&checksum).unwrap(), expected);
            assert_eq!(decode_checksum_metadata(&expected).unwrap(), checksum);
        }

        for (checksum_type, wire_tag) in
            [(ChecksumType::Composite, 0), (ChecksumType::FullObject, 1)]
        {
            let checksum = ObjectChecksumMetadata::new(
                ChecksumAlgorithm::Sha256,
                Some(checksum_type),
                String::new(),
            );
            let expected = [1, 3, wire_tag, 0, 0];
            assert_eq!(encode_checksum_metadata(&checksum).unwrap(), expected);
            assert_eq!(decode_checksum_metadata(&expected).unwrap(), checksum);
        }

        let multibyte =
            ObjectChecksumMetadata::new(ChecksumAlgorithm::Sha256, None, "é".to_string());
        let expected = [1, 3, 255, 0, 2, 0xc3, 0xa9];
        assert_eq!(encode_checksum_metadata(&multibyte).unwrap(), expected);
        assert_eq!(decode_checksum_metadata(&expected).unwrap(), multibyte);
    }

    #[test]
    fn checksum_metadata_plaintext_v1_rejects_each_malformed_class() {
        assert_eq!(
            decode_checksum_metadata(&[]).unwrap_err(),
            ChecksumMetadataCodecError::Truncated {
                actual: 0,
                minimum: CHECKSUM_METADATA_HEADER_LEN,
            }
        );
        assert_eq!(
            decode_checksum_metadata(&[1, 0, 255, 0]).unwrap_err(),
            ChecksumMetadataCodecError::Truncated {
                actual: 4,
                minimum: CHECKSUM_METADATA_HEADER_LEN,
            }
        );
        for version in [0, CHECKSUM_METADATA_VERSION + 1] {
            assert_eq!(
                decode_checksum_metadata(&[version, 255, 2, 0, 0]).unwrap_err(),
                ChecksumMetadataCodecError::UnsupportedVersion { version }
            );
        }
        assert_eq!(
            decode_checksum_metadata(&[1, 10, 255, 0, 0]).unwrap_err(),
            ChecksumMetadataCodecError::InvalidAlgorithmTag { wire_tag: 10 }
        );
        assert_eq!(
            decode_checksum_metadata(&[1, 0, 2, 0, 0]).unwrap_err(),
            ChecksumMetadataCodecError::InvalidChecksumTypeTag { wire_tag: 2 }
        );
        for malformed in [&[1, 0, 255, 0, 1][..], &[1, 0, 255, 0, 0, b'x'][..]] {
            assert!(matches!(
                decode_checksum_metadata(malformed),
                Err(ChecksumMetadataCodecError::InvalidLength { .. })
            ));
        }
        assert_eq!(
            decode_checksum_metadata(&[1, 0, 255, 0, 1, 0xff]).unwrap_err(),
            ChecksumMetadataCodecError::InvalidUtf8
        );

        let public_error =
            checksum_metadata_codec_error(ChecksumMetadataCodecError::InvalidAlgorithmTag {
                wire_tag: 137,
            });
        assert_eq!(
            public_error.to_string(),
            "internal error: stored encrypted checksum metadata is invalid"
        );
        assert!(!public_error.to_string().contains("137"));
    }

    #[test]
    fn checksum_metadata_plaintext_v1_enforces_outer_ciphertext_limit() {
        let maximum = ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Sha256,
            None,
            "x".repeat(CHECKSUM_METADATA_MAX_VALUE_LEN),
        );
        let encoded = encode_checksum_metadata(&maximum).unwrap();
        assert_eq!(encoded.len() + AES_256_GCM_TAG_LEN, usize::from(u16::MAX));
        assert_eq!(decode_checksum_metadata(&encoded).unwrap(), maximum);

        let overlong = ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Sha256,
            None,
            "x".repeat(CHECKSUM_METADATA_MAX_VALUE_LEN + 1),
        );
        assert_eq!(
            encode_checksum_metadata(&overlong).unwrap_err(),
            ChecksumMetadataCodecError::ValueTooLong {
                actual: CHECKSUM_METADATA_MAX_VALUE_LEN + 1,
                maximum: CHECKSUM_METADATA_MAX_VALUE_LEN,
            }
        );
    }

    #[test]
    fn sse_s3_round_trip() {
        let provider = managed_key_provider();
        let ctx = prepare_managed_encryption_write(&provider).unwrap();
        let ObjectEncryption::SseS3(state) = ctx.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let mut ciphertext = ctx.encrypt_segment(4, b"hello sse-s3").unwrap();
        let allocation = ciphertext.as_ptr();
        let capacity = ciphertext.capacity();
        decrypt_managed_encryption_segment_in_place(
            &provider,
            state,
            SseCustomerSegmentScope::object(),
            4,
            &mut ciphertext,
            12,
        )
        .unwrap();
        assert_eq!(ciphertext, b"hello sse-s3");
        assert_eq!(ciphertext.as_ptr(), allocation);
        assert_eq!(ciphertext.capacity(), capacity);
    }

    #[test]
    fn sse_s3_resume_write_round_trip() {
        let provider = managed_key_provider();
        let initial = prepare_managed_encryption_write(&provider).unwrap();
        let ObjectEncryption::SseS3(state) = initial.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let resumed = resume_managed_encryption_write(
            &provider,
            state,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
        )
        .unwrap();
        let mut ciphertext = resumed.encrypt_segment(1, b"part-data").unwrap();
        decrypt_managed_encryption_segment_in_place(
            &provider,
            state,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
            1,
            &mut ciphertext,
            9,
        )
        .unwrap();
        assert_eq!(ciphertext, b"part-data");
    }

    #[test]
    fn sse_s3_checksum_metadata_round_trip() {
        let provider = managed_key_provider();
        let ctx = prepare_managed_encryption_write(&provider).unwrap();
        let checksum = ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Sha256,
            Some(ChecksumType::FullObject),
            "beadfeed".to_string(),
        );
        let ObjectEncryption::SseS3(state) = ctx.seal_checksum_metadata(Some(&checksum)).unwrap()
        else {
            panic!("expected SSE-S3 object state");
        };
        assert!(!state.encrypted_checksum_metadata().is_empty());
        let decrypted = decrypt_managed_encryption_checksum(&provider, &state)
            .unwrap()
            .expect("expected checksum metadata");
        assert_eq!(decrypted, checksum);
    }

    #[test]
    fn current_sse_s3_checksum_profile_matches_frozen_vector_and_requires_outer_version_bump() {
        fn assert_authentication_failure(
            result: Result<Option<ObjectChecksumMetadata>, ServerError>,
        ) {
            let error = result.unwrap_err();
            assert_eq!(
                error.to_string(),
                "internal error: failed to decrypt managed encryption checksum metadata"
            );
        }

        assert_eq!(MANAGED_WRAP_AAD, b"argmin:sse-s3:wrap:v1");
        assert_eq!(MANAGED_SEGMENT_AAD, b"argmin:sse-s3:segment:v1");
        assert_eq!(MANAGED_CHECKSUM_AAD, b"argmin:sse-s3:checksum:v1");

        let fixture = sse_s3_checksum_profile_fixture();
        assert_eq!(
            fixture.wrapped_dek,
            [
                0xa6, 0xa1, 0x70, 0x64, 0xe8, 0xb0, 0x57, 0x0e, 0x90, 0x9a, 0xc9, 0x4f, 0x85, 0x09,
                0xc1, 0x0a, 0x24, 0xea, 0x2d, 0x16, 0xa8, 0xfb, 0xa7, 0x8e, 0xda, 0x68, 0x2b, 0xa7,
                0x81, 0xac, 0xa8, 0x08, 0x26, 0x08, 0xbe, 0xf0, 0x5b, 0x07, 0x42, 0x11, 0x8e, 0xd7,
                0x91, 0xa6, 0x05, 0x0c, 0x86, 0x37,
            ]
        );
        assert_eq!(
            fixture.segment_ciphertext,
            vec![
                0xf0, 0x05, 0xef, 0x0d, 0x52, 0xfe, 0x04, 0x8e, 0xae, 0x22, 0x79, 0xa0, 0x1a, 0x23,
                0x8c, 0x13, 0xfe, 0x8f, 0x3a, 0x1d, 0xe6, 0xef, 0x57, 0xff, 0xf6, 0x5e, 0x4d, 0xd8,
                0x7d,
            ]
        );
        let mut decrypted_segment = fixture.segment_ciphertext.clone();
        decrypt_legacy_segment_with_dek_and_prefix_in_place(
            &fixture.dek,
            &fixture.segment_nonce_prefix,
            SseCustomerSegmentScope::object(),
            7,
            &mut decrypted_segment,
            fixture.segment_plaintext.len(),
            AeadDescriptor {
                aad: MANAGED_SEGMENT_AAD,
                label: MANAGED_ENCRYPTION_LABEL,
            },
        )
        .unwrap();
        assert_eq!(decrypted_segment, fixture.segment_plaintext);

        assert_eq!(
            fixture.checksum_plaintext,
            [
                0x01, 0x03, 0x01, 0x00, 0x10, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
                0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
            ]
        );
        assert_eq!(
            fixture.checksum_ciphertext,
            vec![
                0x58, 0x11, 0xd6, 0xb2, 0xc9, 0xd6, 0xd4, 0xca, 0xd8, 0xf1, 0x56, 0xcf, 0xc3, 0xa8,
                0x61, 0x77, 0x29, 0x03, 0xef, 0x16, 0x9a, 0xfe, 0x87, 0xaa, 0x1d, 0x39, 0x2d, 0xda,
                0x7e, 0xf5, 0xc7, 0x5a, 0x63, 0x50, 0x28, 0x67, 0x41,
            ]
        );
        let ObjectEncryption::SseS3(state) = &fixture.encryption else {
            panic!("expected SSE-S3 fixture state");
        };
        assert_eq!(
            decrypt_managed_encryption_checksum(&fixture.provider, state).unwrap(),
            Some(fixture.checksum.clone())
        );

        let make_state = |nonce, ciphertext| {
            SseS3ObjectState::new(
                fixture.wrapping_key_id,
                fixture.wrap_nonce,
                fixture.wrapped_dek,
                fixture.segment_nonce_prefix,
            )
            .with_encrypted_checksum_metadata(nonce, ciphertext)
            .unwrap()
        };
        let mut wrong_nonce = fixture.checksum_nonce;
        wrong_nonce[0] ^= 1;
        assert_authentication_failure(decrypt_managed_encryption_checksum(
            &fixture.provider,
            &make_state(wrong_nonce, fixture.checksum_ciphertext.clone()),
        ));
        assert_authentication_failure(decrypt_checksum_with_dek(
            &fixture.dek,
            &fixture.checksum_nonce,
            &fixture.checksum_ciphertext,
            b"argmin:sse-s3:checksum:v2",
            MANAGED_ENCRYPTION_LABEL,
        ));

        let mut corrupted_ciphertext = fixture.checksum_ciphertext.clone();
        corrupted_ciphertext[0] ^= 1;
        assert_authentication_failure(decrypt_managed_encryption_checksum(
            &fixture.provider,
            &make_state(fixture.checksum_nonce, corrupted_ciphertext),
        ));

        let mut corrupted_tag = fixture.checksum_ciphertext.clone();
        *corrupted_tag.last_mut().unwrap() ^= 1;
        assert_authentication_failure(decrypt_managed_encryption_checksum(
            &fixture.provider,
            &make_state(fixture.checksum_nonce, corrupted_tag),
        ));
    }

    #[test]
    fn sse_s3_missing_wrapping_key_fails() {
        let provider = managed_key_provider();
        let ctx = prepare_managed_encryption_write(&provider).unwrap();
        let ObjectEncryption::SseS3(state) = ctx.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let mut ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let missing_provider = StaticManagedKeyProvider::single(ManagedWrappingKeyConfig {
            key_id: 8,
            wrapping_key: [12u8; 32],
        });
        let err = decrypt_managed_encryption_segment_in_place(
            &missing_provider,
            state,
            SseCustomerSegmentScope::object(),
            0,
            &mut ciphertext,
            3,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InternalError { .. }));
    }

    #[test]
    fn segment_aead_metric_counts_only_actual_open_attempts() {
        let request = request();
        let validator = validator();
        let sse_c_context = prepare_sse_customer_write(&validator, &request).unwrap();
        let ObjectEncryption::SseCustomer(sse_c_state) = sse_c_context.encryption() else {
            panic!("expected SSE-C object state");
        };
        let sse_c_ciphertext = sse_c_context.encrypt_segment(0, b"abc").unwrap();
        let wrong_request =
            SseCustomerRequest::new([1u8; SSE_C_CUSTOMER_KEY_LEN], "wrong".to_string());

        let managed_provider = managed_key_provider();
        let managed_context = prepare_managed_encryption_write(&managed_provider).unwrap();
        let ObjectEncryption::SseS3(managed_state) = managed_context.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let missing_provider = StaticManagedKeyProvider::single(ManagedWrappingKeyConfig {
            key_id: 8,
            wrapping_key: [12u8; 32],
        });

        let before = observability::metrics_snapshot();
        let mut wrong_key_ciphertext = sse_c_ciphertext.clone();
        assert!(matches!(
            decrypt_sse_customer_segment_in_place(
                &validator,
                sse_c_state,
                &wrong_request,
                SseCustomerSegmentScope::object(),
                0,
                &mut wrong_key_ciphertext,
                3,
            ),
            Err(ServerError::AccessDenied)
        ));
        let mut missing_managed_key_ciphertext =
            managed_context.encrypt_segment(0, b"abc").unwrap();
        assert!(matches!(
            decrypt_managed_encryption_segment_in_place(
                &missing_provider,
                managed_state,
                SseCustomerSegmentScope::object(),
                0,
                &mut missing_managed_key_ciphertext,
                3,
            ),
            Err(ServerError::InternalError { .. })
        ));
        let after_prerequisite_failures = observability::metrics_snapshot();
        assert_eq!(
            after_prerequisite_failures.payload_read_aead_records_total,
            before.payload_read_aead_records_total
        );

        let mut corrupted_ciphertext = sse_c_ciphertext;
        *corrupted_ciphertext.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decrypt_sse_customer_segment_in_place(
                &validator,
                sse_c_state,
                &request,
                SseCustomerSegmentScope::object(),
                0,
                &mut corrupted_ciphertext,
                3,
            ),
            Err(ServerError::InternalError { .. })
        ));
        let after_authentication_failure = observability::metrics_snapshot();
        assert_eq!(
            after_authentication_failure.payload_read_aead_records_total,
            before.payload_read_aead_records_total + 1
        );
    }

    #[test]
    fn debug_redacts_sse_customer_material() {
        let request = SseCustomerRequest::new([7u8; SSE_C_CUSTOMER_KEY_LEN], "md5-value".into());
        let request_debug = format!("{request:?}");
        assert!(request_debug.contains("AES256"));
        assert!(request_debug.contains("<redacted:sse_customer_key_md5>"));
        assert!(!request_debug.contains("md5-value"));

        let headers = request.response_headers();
        let headers_debug = format!("{headers:?}");
        assert!(headers_debug.contains("<redacted:sse_customer_key_md5>"));
        assert!(!headers_debug.contains("md5-value"));

        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &request).unwrap();
        let ctx_debug = format!("{ctx:?}");
        assert!(ctx_debug.contains("SseCustomerWriteContext"));
        assert!(ctx_debug.contains("sse_customer"));
        assert!(ctx_debug.contains("<redacted:sse_customer_key_md5>"));
        assert!(!ctx_debug.contains("wrapped_dek"));

        let provider = managed_key_provider();
        let managed = prepare_managed_encryption_write(&provider).unwrap();
        let managed_debug = format!("{managed:?}");
        assert!(managed_debug.contains("ManagedEncryptionWriteContext"));
        assert!(managed_debug.contains("managed"));
        assert!(!managed_debug.contains("wrapped_dek"));
    }
}
