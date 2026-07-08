use std::fmt;

use ring::{aead, hmac, rand::SecureRandom};
use storage::{
    ObjectEncryption, SseCustomerObjectState, SseS3ObjectState, SSE_C_CHECKSUM_NONCE_LEN,
    SSE_C_SEGMENT_NONCE_PREFIX_LEN, SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_C_VALIDATOR_HMAC_LEN,
    SSE_C_VALIDATOR_SALT_LEN, SSE_C_WRAPPED_DEK_LEN, SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN,
    SSE_S3_CHECKSUM_NONCE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAP_NONCE_LEN,
};

use crate::error::ServerError;
use crate::system_metadata::ObjectChecksumMetadata;

pub const SSE_CUSTOMER_ALGORITHM: &str = "AES256";
pub const SSE_C_CUSTOMER_KEY_LEN: usize = 32;
pub const SSE_C_DEK_LEN: usize = 32;
pub const SSE_C_SEGMENT_TAG_LEN: usize = 16;
const SSE_C_WRAP_AAD: &[u8] = b"argmin:sse-c:wrap:v1";
const SSE_C_SEGMENT_AAD: &[u8] = b"argmin:sse-c:segment:v1";
const SSE_C_CHECKSUM_AAD: &[u8] = b"argmin:sse-c:checksum:v1";
const SSE_C_HKDF_INFO: &[u8] = b"argmin:sse-c:kek:v1";
const SSE_C_CHECKSUM_METADATA_VERSION: u8 = 1;
// Keep the persisted SSE-S3 wire format stable even though the internal
// provider boundary is now modelled as generic managed encryption.
const MANAGED_WRAP_AAD: &[u8] = b"argmin:sse-s3:wrap:v1";
const MANAGED_SEGMENT_AAD: &[u8] = b"argmin:sse-s3:segment:v1";
const MANAGED_CHECKSUM_AAD: &[u8] = b"argmin:sse-s3:checksum:v1";
const MANAGED_ENCRYPTION_LABEL: &str = "managed encryption";

struct AeadDescriptor<'a> {
    aad: &'a [u8],
    label: &'a str,
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

    fn hmac_key(&self) -> hmac::Key {
        hmac::Key::new(hmac::HMAC_SHA256, &self.validator_key)
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

    pub fn encrypt_segment(
        &self,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseCustomer(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "SSE-C write context missing encryption state".to_string(),
            });
        };
        encrypt_segment_with_dek(
            &self.dek,
            state,
            self.segment_scope,
            segment_index,
            plaintext,
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
        Ok(ObjectEncryption::SseCustomer(SseCustomerObjectState {
            validator_key_id: state.validator_key_id,
            validator_salt: state.validator_salt,
            validator_hmac: state.validator_hmac,
            wrap_salt: state.wrap_salt,
            wrap_nonce: state.wrap_nonce,
            wrapped_dek: state.wrapped_dek,
            segment_nonce_prefix: state.segment_nonce_prefix,
            checksum_nonce,
            encrypted_checksum_metadata,
        }))
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
    segment_scope: SseCustomerSegmentScope,
}

impl ManagedEncryptionWriteContext {
    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.encryption
    }

    pub fn encrypt_segment(
        &self,
        segment_index: u32,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let ObjectEncryption::SseS3(state) = &self.encryption else {
            return Err(ServerError::InternalError {
                reason: "managed write context missing encryption state".to_string(),
            });
        };
        encrypt_segment_with_dek_and_prefix(
            &self.dek,
            &state.segment_nonce_prefix,
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
        Ok(ObjectEncryption::SseS3(SseS3ObjectState {
            wrapping_key_id: state.wrapping_key_id,
            wrap_nonce: state.wrap_nonce,
            wrapped_dek: state.wrapped_dek,
            segment_nonce_prefix: state.segment_nonce_prefix,
            checksum_nonce,
            encrypted_checksum_metadata,
        }))
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
    let rng = ring::rand::SystemRandom::new();

    let mut validator_salt = [0u8; SSE_C_VALIDATOR_SALT_LEN];
    rng.fill(&mut validator_salt)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate SSE-C validator salt".to_string(),
        })?;

    let validator_hmac = compute_validator_hmac(validator, &validator_salt, request.customer_key());

    let mut wrap_salt = [0u8; SSE_C_WRAP_SALT_LEN];
    rng.fill(&mut wrap_salt)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate SSE-C wrap salt".to_string(),
        })?;

    let mut wrap_nonce = [0u8; SSE_C_WRAP_NONCE_LEN];
    rng.fill(&mut wrap_nonce)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate SSE-C wrap nonce".to_string(),
        })?;

    let mut dek = [0u8; SSE_C_DEK_LEN];
    rng.fill(&mut dek).map_err(|_| ServerError::InternalError {
        reason: "failed to generate SSE-C object DEK".to_string(),
    })?;

    let kek = derive_wrap_key(request.customer_key(), &wrap_salt)?;
    let wrapped_dek = wrap_managed_dek(&kek, &wrap_nonce, &dek, SSE_C_WRAP_AAD, "SSE-C")?;

    let mut segment_nonce_prefix = [0u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN];
    rng.fill(&mut segment_nonce_prefix)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate SSE-C segment nonce prefix".to_string(),
        })?;

    Ok(SseCustomerWriteContext {
        request: request.clone(),
        encryption: ObjectEncryption::SseCustomer(SseCustomerObjectState {
            validator_key_id: validator.key_id,
            validator_salt,
            validator_hmac,
            wrap_salt,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0u8; SSE_C_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: Vec::new(),
        }),
        dek,
        segment_scope: SseCustomerSegmentScope::object(),
    })
}

pub(crate) fn resume_sse_customer_write(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    segment_scope: SseCustomerSegmentScope,
) -> Result<SseCustomerWriteContext, ServerError> {
    validate_sse_customer_write(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), &state.wrap_salt)?;
    let dek = unwrap_managed_dek(
        &kek,
        &state.wrap_nonce,
        &state.wrapped_dek,
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
    if state.validator_key_id != validator.key_id {
        return Err(sse_customer_validator_key_unavailable());
    }
    let actual = compute_validator_hmac(validator, &state.validator_salt, request.customer_key());
    if !auth::constant_time_eq(&state.validator_hmac, &actual) {
        return Err(ServerError::AccessDenied);
    }
    Ok(request.response_headers())
}

pub fn prepare_managed_encryption_write(
    provider: &impl ManagedKeyProvider,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    let rng = ring::rand::SystemRandom::new();
    let wrapping_key = provider.active_key();

    let mut wrap_nonce = [0u8; SSE_S3_WRAP_NONCE_LEN];
    rng.fill(&mut wrap_nonce)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate managed wrap nonce".to_string(),
        })?;

    let mut dek = [0u8; SSE_C_DEK_LEN];
    rng.fill(&mut dek).map_err(|_| ServerError::InternalError {
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
    rng.fill(&mut segment_nonce_prefix)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate managed segment nonce prefix".to_string(),
        })?;

    Ok(ManagedEncryptionWriteContext {
        encryption: ObjectEncryption::SseS3(SseS3ObjectState {
            wrapping_key_id: wrapping_key.key_id,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
            checksum_nonce: [0u8; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: Vec::new(),
        }),
        dek,
        segment_scope: SseCustomerSegmentScope::object(),
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resume_managed_encryption_write(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    segment_scope: SseCustomerSegmentScope,
) -> Result<ManagedEncryptionWriteContext, ServerError> {
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id)
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        &state.wrap_nonce,
        &state.wrapped_dek,
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    Ok(ManagedEncryptionWriteContext {
        encryption: ObjectEncryption::SseS3(state.clone()),
        dek,
        segment_scope,
    })
}

pub(crate) fn decrypt_managed_encryption_segment(
    provider: &impl ManagedKeyProvider,
    state: &SseS3ObjectState,
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &[u8],
    plaintext_len: usize,
) -> Result<Vec<u8>, ServerError> {
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id)
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        &state.wrap_nonce,
        &state.wrapped_dek,
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    decrypt_segment_with_dek_and_prefix(
        &dek,
        &state.segment_nonce_prefix,
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
    if state.encrypted_checksum_metadata.is_empty() {
        return Ok(None);
    }
    let wrapping_key =
        provider
            .lookup_key(state.wrapping_key_id)
            .ok_or(ServerError::InternalError {
                reason: "managed wrapping key for object is not available".to_string(),
            })?;
    let dek = unwrap_managed_dek(
        wrapping_key.wrapping_key(),
        &state.wrap_nonce,
        &state.wrapped_dek,
        MANAGED_WRAP_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )?;
    decrypt_checksum_with_dek(
        &dek,
        &state.checksum_nonce,
        &state.encrypted_checksum_metadata,
        MANAGED_CHECKSUM_AAD,
        MANAGED_ENCRYPTION_LABEL,
    )
}

fn validate_sse_customer_write(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
) -> Result<SseCustomerResponseHeaders, ServerError> {
    if state.validator_key_id != validator.key_id {
        return Err(sse_customer_validator_key_unavailable());
    }
    let actual = compute_validator_hmac(validator, &state.validator_salt, request.customer_key());
    if !auth::constant_time_eq(&state.validator_hmac, &actual) {
        return Err(ServerError::InvalidRequest {
            reason: "The provided encryption parameters did not match the ones used originally."
                .to_string(),
        });
    }
    Ok(request.response_headers())
}

fn sse_customer_validator_key_unavailable() -> ServerError {
    ServerError::InternalError {
        reason: "SSE-C validator key for object is not available".to_string(),
    }
}

pub(crate) fn decrypt_sse_customer_segment(
    validator: &SseCustomerValidatorConfig,
    state: &SseCustomerObjectState,
    request: &SseCustomerRequest,
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &[u8],
    plaintext_len: usize,
) -> Result<Vec<u8>, ServerError> {
    validate_sse_customer_read(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), &state.wrap_salt)?;
    let dek = unwrap_managed_dek(
        &kek,
        &state.wrap_nonce,
        &state.wrapped_dek,
        SSE_C_WRAP_AAD,
        "SSE-C",
    )?;
    decrypt_segment_with_dek_and_prefix(
        &dek,
        &state.segment_nonce_prefix,
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
    if state.encrypted_checksum_metadata.is_empty() {
        return Ok(None);
    }
    validate_sse_customer_read(validator, state, request)?;
    let kek = derive_wrap_key(request.customer_key(), &state.wrap_salt)?;
    let dek = unwrap_managed_dek(
        &kek,
        &state.wrap_nonce,
        &state.wrapped_dek,
        SSE_C_WRAP_AAD,
        "SSE-C",
    )?;
    decrypt_checksum_with_dek(
        &dek,
        &state.checksum_nonce,
        &state.encrypted_checksum_metadata,
        SSE_C_CHECKSUM_AAD,
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
    let tag = hmac::sign(&key, &msg);
    tag.as_ref()
        .try_into()
        .expect("HMAC-SHA256 output length should be 32 bytes")
}

fn derive_wrap_key(
    customer_key: &[u8; SSE_C_CUSTOMER_KEY_LEN],
    wrap_salt: &[u8; SSE_C_WRAP_SALT_LEN],
) -> Result<[u8; 32], ServerError> {
    let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, wrap_salt);
    let prk = salt.extract(customer_key);
    let okm =
        prk.expand(&[SSE_C_HKDF_INFO], Aes256GcmLen)
            .map_err(|_| ServerError::InternalError {
                reason: "failed to derive SSE-C wrapping key".to_string(),
            })?;
    let mut out = [0u8; 32];
    okm.fill(&mut out).map_err(|_| ServerError::InternalError {
        reason: "failed to fill SSE-C wrapping key".to_string(),
    })?;
    Ok(out)
}

fn wrap_managed_dek(
    kek: &[u8; 32],
    wrap_nonce: &[u8; SSE_C_WRAP_NONCE_LEN],
    dek: &[u8; SSE_C_DEK_LEN],
    wrap_aad: &[u8],
    label: &str,
) -> Result<[u8; SSE_C_WRAPPED_DEK_LEN], ServerError> {
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, kek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {label} wrapping key"),
        })?;
    let sealing_key = aead::LessSafeKey::new(unbound);
    let mut buf = dek.to_vec();
    sealing_key
        .seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(*wrap_nonce),
            aead::Aad::from(wrap_aad),
            &mut buf,
        )
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
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, kek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {label} unwrap key"),
        })?;
    let opening_key = aead::LessSafeKey::new(unbound);
    let mut buf = wrapped_dek.to_vec();
    let plaintext = opening_key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(*wrap_nonce),
            aead::Aad::from(wrap_aad),
            &mut buf,
        )
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
    segment_index: u32,
    plaintext: &[u8],
) -> Result<Vec<u8>, ServerError> {
    encrypt_segment_with_dek_and_prefix(
        dek,
        &state.segment_nonce_prefix,
        segment_scope,
        segment_index,
        plaintext,
        AeadDescriptor {
            aad: SSE_C_SEGMENT_AAD,
            label: "SSE-C",
        },
    )
}

fn encrypt_segment_with_dek_and_prefix(
    dek: &[u8; SSE_C_DEK_LEN],
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    plaintext: &[u8],
    descriptor: AeadDescriptor<'_>,
) -> Result<Vec<u8>, ServerError> {
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, dek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {} segment sealing key", descriptor.label),
        })?;
    let sealing_key = aead::LessSafeKey::new(unbound);
    let mut buf = plaintext.to_vec();
    sealing_key
        .seal_in_place_append_tag(
            segment_nonce(segment_nonce_prefix, segment_scope, segment_index),
            aead::Aad::from(descriptor.aad),
            &mut buf,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to encrypt {} segment", descriptor.label),
        })?;
    Ok(buf)
}

fn decrypt_segment_with_dek_and_prefix(
    dek: &[u8; SSE_C_DEK_LEN],
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
    ciphertext: &[u8],
    plaintext_len: usize,
    descriptor: AeadDescriptor<'_>,
) -> Result<Vec<u8>, ServerError> {
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, dek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {} segment opening key", descriptor.label),
        })?;
    let opening_key = aead::LessSafeKey::new(unbound);
    let mut buf = ciphertext.to_vec();
    let plaintext = opening_key
        .open_in_place(
            segment_nonce(segment_nonce_prefix, segment_scope, segment_index),
            aead::Aad::from(descriptor.aad),
            &mut buf,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to decrypt {} segment", descriptor.label),
        })?;
    if plaintext.len() != plaintext_len {
        return Err(ServerError::InternalError {
            reason: format!(
                "decrypted {} segment length {} did not match expected {}",
                descriptor.label,
                plaintext.len(),
                plaintext_len
            ),
        });
    }
    Ok(plaintext.to_vec())
}

fn segment_nonce(
    segment_nonce_prefix: &[u8; SSE_C_SEGMENT_NONCE_PREFIX_LEN],
    segment_scope: SseCustomerSegmentScope,
    segment_index: u32,
) -> aead::Nonce {
    let mut nonce = [0u8; 12];
    nonce[..SSE_C_SEGMENT_NONCE_PREFIX_LEN].copy_from_slice(segment_nonce_prefix);
    let scope_start = SSE_C_SEGMENT_NONCE_PREFIX_LEN;
    let scope_end = scope_start + SSE_C_SEGMENT_NONCE_SCOPE_LEN;
    nonce[scope_start..scope_end].copy_from_slice(&segment_scope.encode());
    nonce[scope_end..].copy_from_slice(&segment_index.to_be_bytes());
    aead::Nonce::assume_unique_for_key(nonce)
}

fn encode_checksum_metadata(checksum: &ObjectChecksumMetadata) -> Result<Vec<u8>, ServerError> {
    let value = checksum.value().as_bytes();
    let value_len = u16::try_from(value.len()).map_err(|_| ServerError::InternalError {
        reason: "SSE-C checksum metadata value too long".to_string(),
    })?;
    let mut out = Vec::with_capacity(1 + 1 + 1 + 2 + value.len());
    out.push(SSE_C_CHECKSUM_METADATA_VERSION);
    out.push(checksum.algorithm() as u8);
    out.push(checksum.checksum_type().map_or(u8::MAX, |v| v as u8));
    out.extend_from_slice(&value_len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(out)
}

fn decode_checksum_metadata(data: &[u8]) -> Result<ObjectChecksumMetadata, ServerError> {
    if data.len() < 5 {
        return Err(ServerError::InternalError {
            reason: "SSE-C checksum metadata blob too short".to_string(),
        });
    }
    if data[0] != SSE_C_CHECKSUM_METADATA_VERSION {
        return Err(ServerError::InternalError {
            reason: format!("unsupported SSE-C checksum metadata version {}", data[0]),
        });
    }
    let algorithm = checksum::ChecksumAlgorithm::from_u8(data[1]).ok_or_else(|| {
        ServerError::InternalError {
            reason: format!("invalid stored checksum algorithm {}", data[1]),
        }
    })?;
    let checksum_type = if data[2] == u8::MAX {
        None
    } else {
        Some(checksum::ChecksumType::from_u8(data[2]).ok_or_else(|| {
            ServerError::InternalError {
                reason: format!("invalid stored checksum type {}", data[2]),
            }
        })?)
    };
    let value_len = u16::from_be_bytes([data[3], data[4]]) as usize;
    if data.len() != 5 + value_len {
        return Err(ServerError::InternalError {
            reason: "invalid stored checksum metadata length".to_string(),
        });
    }
    let value = std::str::from_utf8(&data[5..]).map_err(|_| ServerError::InternalError {
        reason: "stored checksum metadata is not valid UTF-8".to_string(),
    })?;
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
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, dek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {label} checksum sealing key"),
        })?;
    let sealing_key = aead::LessSafeKey::new(unbound);
    let rng = ring::rand::SystemRandom::new();
    let mut nonce = [0u8; SSE_C_CHECKSUM_NONCE_LEN];
    rng.fill(&mut nonce)
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to generate {label} checksum nonce"),
        })?;
    let mut buf = encode_checksum_metadata(checksum)?;
    sealing_key
        .seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            &mut buf,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to encrypt {label} checksum metadata"),
        })?;
    Ok((nonce, buf))
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
    let unbound =
        aead::UnboundKey::new(&aead::AES_256_GCM, dek).map_err(|_| ServerError::InternalError {
            reason: format!("failed to create {label} checksum opening key"),
        })?;
    let opening_key = aead::LessSafeKey::new(unbound);
    let mut buf = encrypted_checksum_metadata.to_vec();
    let plaintext = opening_key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(*checksum_nonce),
            aead::Aad::from(aad),
            &mut buf,
        )
        .map_err(|_| ServerError::InternalError {
            reason: format!("failed to decrypt {label} checksum metadata"),
        })?;
    Ok(Some(decode_checksum_metadata(plaintext)?))
}

struct Aes256GcmLen;

impl ring::hkdf::KeyType for Aes256GcmLen {
    fn len(&self) -> usize {
        32
    }
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
        let ciphertext = ctx.encrypt_segment(3, b"hello world").unwrap();
        let plaintext = decrypt_sse_customer_segment(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::object(),
            3,
            &ciphertext,
            11,
        )
        .unwrap();
        assert_eq!(plaintext, b"hello world");
    }

    #[test]
    fn sse_c_rejects_wrong_key() {
        let req = request();
        let validator = validator();
        let ctx = prepare_sse_customer_write(&validator, &req).unwrap();
        let ObjectEncryption::SseCustomer(state) = ctx.encryption() else {
            panic!("expected SSE-C object state");
        };
        let ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let wrong = SseCustomerRequest::new([1u8; SSE_C_CUSTOMER_KEY_LEN], "wrong".to_string());
        let err = decrypt_sse_customer_segment(
            &validator,
            state,
            &wrong,
            SseCustomerSegmentScope::object(),
            0,
            &ciphertext,
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
        let ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let err = decrypt_sse_customer_segment(
            &missing_validator(),
            state,
            &req,
            SseCustomerSegmentScope::object(),
            0,
            &ciphertext,
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

        let part1_ciphertext = part1_ctx.encrypt_segment(0, b"same-data").unwrap();
        let part2_ciphertext = part2_ctx.encrypt_segment(0, b"same-data").unwrap();
        assert_ne!(part1_ciphertext, part2_ciphertext);

        let part1_plaintext = decrypt_sse_customer_segment(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(1).unwrap(),
            0,
            &part1_ciphertext,
            9,
        )
        .unwrap();
        assert_eq!(part1_plaintext, b"same-data");

        let err = decrypt_sse_customer_segment(
            &validator,
            state,
            &req,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
            0,
            &part1_ciphertext,
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
        assert!(!state.encrypted_checksum_metadata.is_empty());
        let decrypted = decrypt_sse_customer_checksum(&validator, &state, &req)
            .unwrap()
            .expect("expected checksum metadata");
        assert_eq!(decrypted, checksum);
    }

    #[test]
    fn sse_s3_round_trip() {
        let provider = managed_key_provider();
        let ctx = prepare_managed_encryption_write(&provider).unwrap();
        let ObjectEncryption::SseS3(state) = ctx.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let ciphertext = ctx.encrypt_segment(4, b"hello sse-s3").unwrap();
        let plaintext = decrypt_managed_encryption_segment(
            &provider,
            state,
            SseCustomerSegmentScope::object(),
            4,
            &ciphertext,
            12,
        )
        .unwrap();
        assert_eq!(plaintext, b"hello sse-s3");
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
        let ciphertext = resumed.encrypt_segment(1, b"part-data").unwrap();
        let plaintext = decrypt_managed_encryption_segment(
            &provider,
            state,
            SseCustomerSegmentScope::multipart_part(2).unwrap(),
            1,
            &ciphertext,
            9,
        )
        .unwrap();
        assert_eq!(plaintext, b"part-data");
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
        assert!(!state.encrypted_checksum_metadata.is_empty());
        let decrypted = decrypt_managed_encryption_checksum(&provider, &state)
            .unwrap()
            .expect("expected checksum metadata");
        assert_eq!(decrypted, checksum);
    }

    #[test]
    fn sse_s3_wire_format_vectors_stay_stable() {
        let wrapping_key = [0x11u8; 32];
        let dek = [0x22u8; SSE_C_DEK_LEN];
        let wrap_nonce = [0x33u8; SSE_S3_WRAP_NONCE_LEN];
        let segment_prefix = [0x44u8; SSE_S3_SEGMENT_NONCE_PREFIX_LEN];
        let checksum_nonce = [0x55u8; SSE_S3_CHECKSUM_NONCE_LEN];
        let plaintext = b"compat-sse-s3";
        let checksum = ObjectChecksumMetadata::new(
            ChecksumAlgorithm::Sha256,
            Some(ChecksumType::FullObject),
            "0123456789abcdef".to_string(),
        );

        let wrapped_dek = wrap_managed_dek(
            &wrapping_key,
            &wrap_nonce,
            &dek,
            MANAGED_WRAP_AAD,
            MANAGED_ENCRYPTION_LABEL,
        )
        .unwrap();
        assert_eq!(
            wrapped_dek,
            [
                0xa6, 0xa1, 0x70, 0x64, 0xe8, 0xb0, 0x57, 0x0e, 0x90, 0x9a, 0xc9, 0x4f, 0x85, 0x09,
                0xc1, 0x0a, 0x24, 0xea, 0x2d, 0x16, 0xa8, 0xfb, 0xa7, 0x8e, 0xda, 0x68, 0x2b, 0xa7,
                0x81, 0xac, 0xa8, 0x08, 0x26, 0x08, 0xbe, 0xf0, 0x5b, 0x07, 0x42, 0x11, 0x8e, 0xd7,
                0x91, 0xa6, 0x05, 0x0c, 0x86, 0x37,
            ]
        );
        let segment_ciphertext = encrypt_segment_with_dek_and_prefix(
            &dek,
            &segment_prefix,
            SseCustomerSegmentScope::object(),
            7,
            plaintext,
            AeadDescriptor {
                aad: MANAGED_SEGMENT_AAD,
                label: MANAGED_ENCRYPTION_LABEL,
            },
        )
        .unwrap();
        assert_eq!(
            segment_ciphertext,
            vec![
                0xf0, 0x05, 0xef, 0x0d, 0x52, 0xfe, 0x04, 0x8e, 0xae, 0x22, 0x79, 0xa0, 0x1a, 0x23,
                0x8c, 0x13, 0xfe, 0x8f, 0x3a, 0x1d, 0xe6, 0xef, 0x57, 0xff, 0xf6, 0x5e, 0x4d, 0xd8,
                0x7d,
            ]
        );
        let decrypted_segment = decrypt_segment_with_dek_and_prefix(
            &dek,
            &segment_prefix,
            SseCustomerSegmentScope::object(),
            7,
            &segment_ciphertext,
            plaintext.len(),
            AeadDescriptor {
                aad: MANAGED_SEGMENT_AAD,
                label: MANAGED_ENCRYPTION_LABEL,
            },
        )
        .unwrap();
        assert_eq!(decrypted_segment, plaintext);

        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &dek).unwrap();
        let sealing_key = aead::LessSafeKey::new(unbound);
        let mut checksum_ciphertext = encode_checksum_metadata(&checksum).unwrap();
        sealing_key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(checksum_nonce),
                aead::Aad::from(MANAGED_CHECKSUM_AAD),
                &mut checksum_ciphertext,
            )
            .unwrap();
        assert_eq!(
            checksum_ciphertext,
            vec![
                0x58, 0x11, 0xd6, 0xb2, 0xc9, 0xd6, 0xd4, 0xca, 0xd8, 0xf1, 0x56, 0xcf, 0xc3, 0xa8,
                0x61, 0x77, 0x29, 0x03, 0xef, 0x16, 0x9a, 0xfe, 0x87, 0xaa, 0x1d, 0x39, 0x2d, 0xda,
                0x7e, 0xf5, 0xc7, 0x5a, 0x63, 0x50, 0x28, 0x67, 0x41,
            ]
        );
        let decrypted_checksum = decrypt_checksum_with_dek(
            &dek,
            &checksum_nonce,
            &checksum_ciphertext,
            MANAGED_CHECKSUM_AAD,
            MANAGED_ENCRYPTION_LABEL,
        )
        .unwrap()
        .expect("expected checksum metadata");
        assert_eq!(decrypted_checksum, checksum);
    }

    #[test]
    fn sse_s3_missing_wrapping_key_fails() {
        let provider = managed_key_provider();
        let ctx = prepare_managed_encryption_write(&provider).unwrap();
        let ObjectEncryption::SseS3(state) = ctx.encryption() else {
            panic!("expected SSE-S3 object state");
        };
        let ciphertext = ctx.encrypt_segment(0, b"abc").unwrap();
        let missing_provider = StaticManagedKeyProvider::single(ManagedWrappingKeyConfig {
            key_id: 8,
            wrapping_key: [12u8; 32],
        });
        let err = decrypt_managed_encryption_segment(
            &missing_provider,
            state,
            SseCustomerSegmentScope::object(),
            0,
            &ciphertext,
            3,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InternalError { .. }));
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
