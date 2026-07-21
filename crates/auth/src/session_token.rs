//! Stateless temporary-credential session-token sealing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::{aead, digest, rand};

use crate::credential::{
    valid_session_access_key_id, valid_session_secret_access_key, SESSION_ACCESS_KEY_ID_LEN,
    SESSION_SECRET_ACCESS_KEY_LEN,
};
use crate::identity::{
    ROLE_NAME_MAX_LEN, ROLE_SESSION_NAME_MAX_LEN, SOURCE_IDENTITY_MAX_LEN, STABLE_ROLE_ID_LEN,
};
use crate::{
    AwsAccountId, DecodedSessionCredential, RoleName, RoleSessionName, SecretKey, SessionLifetime,
    SourceIdentity, StableRoleId,
};

/// Prefix identifying Argmin's first session-token envelope version.
pub const SESSION_TOKEN_V1_PREFIX: &str = "ARGST1.";
/// Defensive upper bound accepted before allocating a decoded token frame.
pub const MAX_ENCODED_SESSION_TOKEN_LEN: usize = 21_853;
/// Defensive upper bound for a decoded token frame.
pub const MAX_DECODED_SESSION_TOKEN_FRAME_LEN: usize = 16_384;

const KEY_LEN: usize = 32;
const KEY_ID_LEN: usize = 16;
const CREDENTIAL_DOMAIN_LEN: usize = 16;
const NONCE_PREFIX_LEN: usize = 4;
const NONCE_COUNTER_LEN: usize = 8;
const NONCE_LEN: usize = NONCE_PREFIX_LEN + NONCE_COUNTER_LEN;
const TAG_LEN: usize = 16;
const AWS_ACCOUNT_ID_LEN: usize = 12;
const I64_PAIR_LEN: usize = 16;
const U16_LEN: usize = 2;
const SOURCE_IDENTITY_PRESENCE_LEN: usize = 1;
const ASSOCIATED_DATA_LABEL: &[u8] = b"argmin:sts:session-token:v1";

const MAX_V1_PLAINTEXT_LEN: usize = SESSION_ACCESS_KEY_ID_LEN
    + SESSION_SECRET_ACCESS_KEY_LEN
    + I64_PAIR_LEN
    + AWS_ACCOUNT_ID_LEN
    + STABLE_ROLE_ID_LEN
    + U16_LEN
    + ROLE_NAME_MAX_LEN
    + U16_LEN
    + ROLE_SESSION_NAME_MAX_LEN
    + SOURCE_IDENTITY_PRESENCE_LEN
    + U16_LEN
    + SOURCE_IDENTITY_MAX_LEN;
const MAX_ISSUED_V1_FRAME_LEN: usize = KEY_ID_LEN + NONCE_LEN + MAX_V1_PLAINTEXT_LEN + TAG_LEN;

const fn unpadded_base64_len(byte_len: usize) -> usize {
    let complete_groups = byte_len / 3;
    let remainder_len = match byte_len % 3 {
        0 => 0,
        1 => 2,
        2 => 3,
        _ => unreachable!(),
    };
    complete_groups * 4 + remainder_len
}

/// Maximum external length of a token this implementation can issue.
pub const MAX_ISSUED_V1_TOKEN_LEN: usize =
    SESSION_TOKEN_V1_PREFIX.len() + unpadded_base64_len(MAX_ISSUED_V1_FRAME_LEN);

type KeyId = [u8; KEY_ID_LEN];
type CredentialDomain = [u8; CREDENTIAL_DOMAIN_LEN];

/// Failure to initialize a process-local session-token key ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionTokenKeyRingInitError {
    #[error("secure random generation failed")]
    EntropyUnavailable,
    #[error("invalid session-token key material")]
    InvalidKeyMaterial,
}

/// Failure while sealing a session credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionTokenSealError {
    #[error("invalid session credential")]
    InvalidCredential,
    #[error("session-token key ring unavailable")]
    KeyRingUnavailable,
    #[error("session-token nonce space exhausted")]
    NonceExhausted,
    #[error("session-token issuance invariant violated")]
    IssuanceInvariant,
}

/// Failure while opening a sealed session token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionTokenOpenError {
    #[error("invalid session token")]
    InvalidToken,
    #[error("session-token key ring unavailable")]
    KeyRingUnavailable,
}

/// Public, non-secret status for the bounded process-local key ring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionTokenKeyRingStatus {
    credential_domain: CredentialDomain,
    active_key_id: KeyId,
    validation_only_key_ids: Vec<KeyId>,
}

impl SessionTokenKeyRingStatus {
    #[must_use]
    pub const fn credential_domain(&self) -> &CredentialDomain {
        &self.credential_domain
    }

    #[must_use]
    pub const fn active_key_id(&self) -> &KeyId {
        &self.active_key_id
    }

    #[must_use]
    pub fn validation_only_key_ids(&self) -> &[KeyId] {
        &self.validation_only_key_ids
    }
}

struct SessionTokenKey {
    key: aead::LessSafeKey,
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    next_nonce_counter: AtomicU64,
}

impl SessionTokenKey {
    fn new(
        key_material: &[u8; KEY_LEN],
        nonce_prefix: [u8; NONCE_PREFIX_LEN],
    ) -> Result<Self, SessionTokenKeyRingInitError> {
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, key_material)
            .map_err(|_| SessionTokenKeyRingInitError::InvalidKeyMaterial)?;
        Ok(Self {
            key: aead::LessSafeKey::new(key),
            nonce_prefix,
            next_nonce_counter: AtomicU64::new(0),
        })
    }

    fn next_nonce(&self) -> Result<[u8; NONCE_LEN], SessionTokenSealError> {
        let counter = self
            .next_nonce_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| SessionTokenSealError::NonceExhausted)?;
        let mut nonce = [0_u8; NONCE_LEN];
        nonce[..NONCE_PREFIX_LEN].copy_from_slice(&self.nonce_prefix);
        nonce[NONCE_PREFIX_LEN..].copy_from_slice(&counter.to_be_bytes());
        Ok(nonce)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct KeyMaterialFingerprint([u8; 32]);

impl KeyMaterialFingerprint {
    fn from_key_material(key_material: &[u8; KEY_LEN]) -> Self {
        Self(
            digest::digest(&digest::SHA256, key_material)
                .as_ref()
                .try_into()
                .expect("SHA-256 output is 32 bytes"),
        )
    }
}

struct ActiveSessionTokenKey {
    key_id: KeyId,
    material_fingerprint: KeyMaterialFingerprint,
    key: Arc<SessionTokenKey>,
}

enum RetiredSessionTokenKey {
    ValidationOnly {
        material_fingerprint: KeyMaterialFingerprint,
        key: Arc<SessionTokenKey>,
    },
    Removed {
        material_fingerprint: KeyMaterialFingerprint,
    },
}

impl RetiredSessionTokenKey {
    const fn material_fingerprint(&self) -> KeyMaterialFingerprint {
        match self {
            Self::ValidationOnly {
                material_fingerprint,
                ..
            }
            | Self::Removed {
                material_fingerprint,
            } => *material_fingerprint,
        }
    }
}

struct SessionTokenKeyRingState {
    active: ActiveSessionTokenKey,
    retired: HashMap<KeyId, RetiredSessionTokenKey>,
}

/// Shared process-local session-token sealing and validation keys.
///
/// Clones of the owning [`crate::IdentityProvider`] share this value. The
/// lifecycle API can only create a new active key, retire the former active key
/// for validation, and remove a validation-only key. There is no operation
/// that can reactivate an old key with a reset nonce allocator.
pub struct SessionTokenKeyRing {
    credential_domain: CredentialDomain,
    state: RwLock<SessionTokenKeyRingState>,
}

impl SessionTokenKeyRing {
    /// Generate the initial process-local key, key ID, nonce prefix, and
    /// credential domain together.
    pub fn new_process_local() -> Result<Self, SessionTokenKeyRingInitError> {
        let mut generated = [0_u8; CREDENTIAL_DOMAIN_LEN + KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN];
        rand::SecureRandom::fill(&rand::SystemRandom::new(), &mut generated)
            .map_err(|_| SessionTokenKeyRingInitError::EntropyUnavailable)?;
        let mut offset = 0;
        let credential_domain =
            take_array_from_generated::<CREDENTIAL_DOMAIN_LEN>(&generated, &mut offset);
        let key_id = take_array_from_generated::<KEY_ID_LEN>(&generated, &mut offset);
        let nonce_prefix = take_array_from_generated::<NONCE_PREFIX_LEN>(&generated, &mut offset);
        let key_material = take_array_from_generated::<KEY_LEN>(&generated, &mut offset);
        Self::from_initial_parts(credential_domain, key_id, nonce_prefix, key_material, 0)
    }

    fn from_initial_parts(
        credential_domain: CredentialDomain,
        key_id: KeyId,
        nonce_prefix: [u8; NONCE_PREFIX_LEN],
        key_material: [u8; KEY_LEN],
        next_nonce_counter: u64,
    ) -> Result<Self, SessionTokenKeyRingInitError> {
        let key = SessionTokenKey::new(&key_material, nonce_prefix)?;
        key.next_nonce_counter
            .store(next_nonce_counter, Ordering::Relaxed);
        Ok(Self {
            credential_domain,
            state: RwLock::new(SessionTokenKeyRingState {
                active: ActiveSessionTokenKey {
                    key_id,
                    material_fingerprint: KeyMaterialFingerprint::from_key_material(&key_material),
                    key: Arc::new(key),
                },
                retired: HashMap::new(),
            }),
        })
    }

    /// Return the non-secret credential-domain and key lifecycle status.
    pub fn status(&self) -> Result<SessionTokenKeyRingStatus, SessionTokenSealError> {
        let state = self
            .state
            .read()
            .map_err(|_| SessionTokenSealError::KeyRingUnavailable)?;
        let mut validation_only_key_ids: Vec<_> = state
            .retired
            .iter()
            .filter_map(|(key_id, key)| {
                matches!(key, RetiredSessionTokenKey::ValidationOnly { .. }).then_some(*key_id)
            })
            .collect();
        validation_only_key_ids.sort_unstable();
        Ok(SessionTokenKeyRingStatus {
            credential_domain: self.credential_domain,
            active_key_id: state.active.key_id,
            validation_only_key_ids,
        })
    }

    fn seal_v1(
        &self,
        credential: &DecodedSessionCredential,
    ) -> Result<String, SessionTokenSealError> {
        let plaintext = encode_v1_plaintext(credential)?;
        if plaintext.len() > MAX_V1_PLAINTEXT_LEN {
            return Err(SessionTokenSealError::IssuanceInvariant);
        }

        // Nonce allocation is issuance's linearization point. Clone the
        // immutable key and release the ring lock before cryptography.
        let (key_id, key, nonce) = {
            let state = self
                .state
                .read()
                .map_err(|_| SessionTokenSealError::KeyRingUnavailable)?;
            let key_id = state.active.key_id;
            let key = Arc::clone(&state.active.key);
            let nonce = key.next_nonce()?;
            (key_id, key, nonce)
        };
        let associated_data = associated_data(&self.credential_domain, &key_id, &nonce);
        let mut ciphertext = plaintext;
        key.key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(associated_data.as_slice()),
                &mut ciphertext,
            )
            .map_err(|_| SessionTokenSealError::IssuanceInvariant)?;

        let mut frame = Vec::with_capacity(KEY_ID_LEN + NONCE_LEN + ciphertext.len());
        frame.extend_from_slice(&key_id);
        frame.extend_from_slice(&nonce);
        frame.extend_from_slice(&ciphertext);
        if frame.len() > MAX_ISSUED_V1_FRAME_LEN {
            return Err(SessionTokenSealError::IssuanceInvariant);
        }
        let token = format!("{SESSION_TOKEN_V1_PREFIX}{}", URL_SAFE_NO_PAD.encode(frame));
        if token.len() > MAX_ISSUED_V1_TOKEN_LEN {
            return Err(SessionTokenSealError::IssuanceInvariant);
        }
        Ok(token)
    }

    fn open_v1(&self, token: &str) -> Result<OpenedSessionTokenV1, SessionTokenOpenError> {
        if token.len() > MAX_ENCODED_SESSION_TOKEN_LEN
            || !token.starts_with(SESSION_TOKEN_V1_PREFIX)
        {
            return Err(SessionTokenOpenError::InvalidToken);
        }
        let encoded = &token[SESSION_TOKEN_V1_PREFIX.len()..];
        if encoded.is_empty() || !encoded.is_ascii() {
            return Err(SessionTokenOpenError::InvalidToken);
        }
        let mut frame = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| SessionTokenOpenError::InvalidToken)?;
        if frame.len() > MAX_DECODED_SESSION_TOKEN_FRAME_LEN
            || URL_SAFE_NO_PAD.encode(&frame) != encoded
            || frame.len() < KEY_ID_LEN + NONCE_LEN + TAG_LEN
        {
            return Err(SessionTokenOpenError::InvalidToken);
        }

        let key_id: KeyId = frame[..KEY_ID_LEN]
            .try_into()
            .map_err(|_| SessionTokenOpenError::InvalidToken)?;
        let nonce: [u8; NONCE_LEN] = frame[KEY_ID_LEN..KEY_ID_LEN + NONCE_LEN]
            .try_into()
            .map_err(|_| SessionTokenOpenError::InvalidToken)?;
        let ciphertext_offset = KEY_ID_LEN + NONCE_LEN;
        let associated_data = associated_data(&self.credential_domain, &key_id, &nonce);

        let key = {
            let state = self
                .state
                .read()
                .map_err(|_| SessionTokenOpenError::KeyRingUnavailable)?;
            if state.active.key_id == key_id {
                Arc::clone(&state.active.key)
            } else {
                match state.retired.get(&key_id) {
                    Some(RetiredSessionTokenKey::ValidationOnly { key, .. }) => Arc::clone(key),
                    Some(RetiredSessionTokenKey::Removed { .. }) | None => {
                        return Err(SessionTokenOpenError::InvalidToken);
                    }
                }
            }
        };
        let plaintext = key
            .key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(associated_data.as_slice()),
                &mut frame[ciphertext_offset..],
            )
            .map_err(|_| SessionTokenOpenError::InvalidToken)?;
        decode_v1_plaintext(plaintext)
    }

    // Phase 1 defines and verifies the irreversible transition internally.
    // A provider-facing rotation operation waits for the configured key-ring
    // work, so this transition has no production caller yet.
    #[cfg_attr(not(test), allow(dead_code))]
    fn rotate_process_local(
        &self,
        generated: [u8; KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN],
    ) -> Result<KeyId, SessionTokenSealError> {
        let mut offset = 0;
        let key_id = take_array_from_generated::<KEY_ID_LEN>(&generated, &mut offset);
        let nonce_prefix = take_array_from_generated::<NONCE_PREFIX_LEN>(&generated, &mut offset);
        let key_material = take_array_from_generated::<KEY_LEN>(&generated, &mut offset);
        let key = SessionTokenKey::new(&key_material, nonce_prefix)
            .map_err(|_| SessionTokenSealError::IssuanceInvariant)?;
        let material_fingerprint = KeyMaterialFingerprint::from_key_material(&key_material);
        let mut state = self
            .state
            .write()
            .map_err(|_| SessionTokenSealError::KeyRingUnavailable)?;
        if state.active.key_id == key_id
            || state.retired.contains_key(&key_id)
            || state.active.material_fingerprint == material_fingerprint
            || state
                .retired
                .values()
                .any(|key| key.material_fingerprint() == material_fingerprint)
        {
            return Err(SessionTokenSealError::IssuanceInvariant);
        }
        let previous_active = std::mem::replace(
            &mut state.active,
            ActiveSessionTokenKey {
                key_id,
                material_fingerprint,
                key: Arc::new(key),
            },
        );
        let replaced = state.retired.insert(
            previous_active.key_id,
            RetiredSessionTokenKey::ValidationOnly {
                material_fingerprint: previous_active.material_fingerprint,
                key: previous_active.key,
            },
        );
        debug_assert!(replaced.is_none());
        Ok(key_id)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn remove_validation_only_key(&self, key_id: &KeyId) -> Result<bool, SessionTokenSealError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| SessionTokenSealError::KeyRingUnavailable)?;
        if key_id == &state.active.key_id {
            return Ok(false);
        }
        let Some(retired) = state.retired.get_mut(key_id) else {
            return Ok(false);
        };
        match retired {
            RetiredSessionTokenKey::ValidationOnly {
                material_fingerprint,
                ..
            } => {
                *retired = RetiredSessionTokenKey::Removed {
                    material_fingerprint: *material_fingerprint,
                };
                Ok(true)
            }
            RetiredSessionTokenKey::Removed { .. } => Ok(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn poison_state_for_test(&self) {
        let _guard = self.state.write().unwrap();
        panic!("poison session-token key-ring state for test");
    }
}

impl std::fmt::Debug for SessionTokenKeyRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status() {
            Ok(status) => f
                .debug_struct("SessionTokenKeyRing")
                .field("status", &status)
                .finish(),
            Err(_) => f
                .debug_struct("SessionTokenKeyRing")
                .field("status", &"unavailable")
                .finish(),
        }
    }
}

fn take_array_from_generated<const N: usize>(generated: &[u8], offset: &mut usize) -> [u8; N] {
    let end = *offset + N;
    let value = generated[*offset..end]
        .try_into()
        .expect("fixed generated material layout");
    *offset = end;
    value
}

fn associated_data(
    credential_domain: &CredentialDomain,
    key_id: &KeyId,
    nonce: &[u8; NONCE_LEN],
) -> Vec<u8> {
    let mut value = Vec::with_capacity(
        ASSOCIATED_DATA_LABEL.len()
            + CREDENTIAL_DOMAIN_LEN
            + SESSION_TOKEN_V1_PREFIX.len()
            + KEY_ID_LEN
            + NONCE_LEN,
    );
    value.extend_from_slice(ASSOCIATED_DATA_LABEL);
    value.extend_from_slice(credential_domain);
    value.extend_from_slice(SESSION_TOKEN_V1_PREFIX.as_bytes());
    value.extend_from_slice(key_id);
    value.extend_from_slice(nonce);
    value
}

fn push_len_prefixed(value: &str, output: &mut Vec<u8>) -> Result<(), SessionTokenSealError> {
    let len = u16::try_from(value.len()).map_err(|_| SessionTokenSealError::IssuanceInvariant)?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_v1_plaintext(
    credential: &DecodedSessionCredential,
) -> Result<Vec<u8>, SessionTokenSealError> {
    if !valid_session_access_key_id(credential.access_key_id())
        || !valid_session_secret_access_key(credential.secret_key().as_str())
    {
        return Err(SessionTokenSealError::InvalidCredential);
    }
    let session = credential.session();
    let role = session.role();
    let lifetime = session.lifetime();
    let mut plaintext = Vec::with_capacity(MAX_V1_PLAINTEXT_LEN);
    plaintext.extend_from_slice(credential.access_key_id().as_bytes());
    plaintext.extend_from_slice(credential.secret_key().as_str().as_bytes());
    plaintext.extend_from_slice(&lifetime.issued_at_epoch_secs().to_be_bytes());
    plaintext.extend_from_slice(&lifetime.expires_at_epoch_secs().to_be_bytes());
    plaintext.extend_from_slice(role.account_id().as_str().as_bytes());
    plaintext.extend_from_slice(role.stable_id().as_str().as_bytes());
    push_len_prefixed(role.name().as_str(), &mut plaintext)?;
    push_len_prefixed(session.session_name().as_str(), &mut plaintext)?;
    match session.source_identity() {
        None => plaintext.push(0),
        Some(source_identity) => {
            plaintext.push(1);
            push_len_prefixed(source_identity.as_str(), &mut plaintext)?;
        }
    }
    Ok(plaintext)
}

pub(crate) struct OpenedSessionTokenV1 {
    access_key_id: String,
    secret_key: SecretKey,
    lifetime: SessionLifetime,
    account_id: AwsAccountId,
    stable_role_id: StableRoleId,
    role_name: RoleName,
    session_name: RoleSessionName,
    source_identity: Option<SourceIdentity>,
}

struct PlaintextReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PlaintextReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SessionTokenOpenError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(SessionTokenOpenError::InvalidToken)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(SessionTokenOpenError::InvalidToken)?;
        self.offset = end;
        Ok(value)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], SessionTokenOpenError> {
        self.take(N)?
            .try_into()
            .map_err(|_| SessionTokenOpenError::InvalidToken)
    }

    fn take_u16(&mut self) -> Result<usize, SessionTokenOpenError> {
        Ok(usize::from(u16::from_be_bytes(self.take_array()?)))
    }

    fn take_string(&mut self, len: usize) -> Result<String, SessionTokenOpenError> {
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| SessionTokenOpenError::InvalidToken)
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn decode_v1_plaintext(plaintext: &[u8]) -> Result<OpenedSessionTokenV1, SessionTokenOpenError> {
    if plaintext.len() > MAX_V1_PLAINTEXT_LEN {
        return Err(SessionTokenOpenError::InvalidToken);
    }
    let mut reader = PlaintextReader::new(plaintext);
    let access_key_id = reader.take_string(SESSION_ACCESS_KEY_ID_LEN)?;
    if !valid_session_access_key_id(&access_key_id) {
        return Err(SessionTokenOpenError::InvalidToken);
    }
    let secret_key = reader.take_string(SESSION_SECRET_ACCESS_KEY_LEN)?;
    if !valid_session_secret_access_key(&secret_key) {
        return Err(SessionTokenOpenError::InvalidToken);
    }
    let issued_at_epoch_secs = i64::from_be_bytes(reader.take_array()?);
    let expires_at_epoch_secs = i64::from_be_bytes(reader.take_array()?);
    let lifetime = SessionLifetime::new(issued_at_epoch_secs, expires_at_epoch_secs)
        .map_err(|_| SessionTokenOpenError::InvalidToken)?;
    let account_id = AwsAccountId::new(reader.take_string(AWS_ACCOUNT_ID_LEN)?)
        .map_err(|_| SessionTokenOpenError::InvalidToken)?;
    let stable_role_id = StableRoleId::new(reader.take_string(STABLE_ROLE_ID_LEN)?)
        .map_err(|_| SessionTokenOpenError::InvalidToken)?;
    let role_name_len = reader.take_u16()?;
    let role_name = RoleName::new(reader.take_string(role_name_len)?)
        .map_err(|_| SessionTokenOpenError::InvalidToken)?;
    let session_name_len = reader.take_u16()?;
    let session_name = RoleSessionName::new(reader.take_string(session_name_len)?)
        .map_err(|_| SessionTokenOpenError::InvalidToken)?;
    let source_identity = match reader.take(1)?[0] {
        0 => None,
        1 => {
            let source_identity_len = reader.take_u16()?;
            Some(
                SourceIdentity::new(reader.take_string(source_identity_len)?)
                    .map_err(|_| SessionTokenOpenError::InvalidToken)?,
            )
        }
        _ => return Err(SessionTokenOpenError::InvalidToken),
    };
    if !reader.is_finished() {
        return Err(SessionTokenOpenError::InvalidToken);
    }
    Ok(OpenedSessionTokenV1 {
        access_key_id,
        secret_key: SecretKey::new(secret_key),
        lifetime,
        account_id,
        stable_role_id,
        role_name,
        session_name,
        source_identity,
    })
}

pub(crate) fn seal_v1(
    key_ring: &SessionTokenKeyRing,
    credential: &DecodedSessionCredential,
) -> Result<String, SessionTokenSealError> {
    key_ring.seal_v1(credential)
}

pub(crate) fn open_v1(
    key_ring: &SessionTokenKeyRing,
    token: &str,
) -> Result<OpenedSessionTokenV1, SessionTokenOpenError> {
    key_ring.open_v1(token)
}

pub(crate) fn opened_access_key_id(opened: &OpenedSessionTokenV1) -> &str {
    &opened.access_key_id
}

pub(crate) fn opened_secret_key(opened: &OpenedSessionTokenV1) -> &SecretKey {
    &opened.secret_key
}

pub(crate) fn opened_lifetime(opened: &OpenedSessionTokenV1) -> SessionLifetime {
    opened.lifetime
}

pub(crate) fn opened_account_id(opened: &OpenedSessionTokenV1) -> &AwsAccountId {
    &opened.account_id
}

pub(crate) fn opened_stable_role_id(opened: &OpenedSessionTokenV1) -> &StableRoleId {
    &opened.stable_role_id
}

pub(crate) fn opened_role_name(opened: &OpenedSessionTokenV1) -> &RoleName {
    &opened.role_name
}

pub(crate) fn opened_session_name(opened: &OpenedSessionTokenV1) -> &RoleSessionName {
    &opened.session_name
}

pub(crate) fn opened_source_identity(opened: &OpenedSessionTokenV1) -> Option<&SourceIdentity> {
    opened.source_identity.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AwsAccountId, CredentialStore, GeneratedSessionCredentialMaterial, IamPath,
        IamRoleIdentity, IdentityProvider, IdentityProviderBackend, IdentityProviderError,
        LiveRoleIdentity, ResolvedRoleIdentity, RoleIdentityStore,
        SessionCredentialAuthenticationError, StoredCredential,
    };

    const DOMAIN: CredentialDomain = [0x11; CREDENTIAL_DOMAIN_LEN];
    const KEY_ID: KeyId = [0x22; KEY_ID_LEN];
    const NONCE_PREFIX: [u8; NONCE_PREFIX_LEN] = [0x33; NONCE_PREFIX_LEN];
    const KEY_MATERIAL: [u8; KEY_LEN] = [0x44; KEY_LEN];

    fn key_ring() -> SessionTokenKeyRing {
        SessionTokenKeyRing::from_initial_parts(DOMAIN, KEY_ID, NONCE_PREFIX, KEY_MATERIAL, 0)
            .unwrap()
    }

    fn rotation_material(
        key_id: KeyId,
        nonce_prefix: [u8; NONCE_PREFIX_LEN],
        key_material: [u8; KEY_LEN],
    ) -> [u8; KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN] {
        let mut generated = [0_u8; KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN];
        generated[..KEY_ID_LEN].copy_from_slice(&key_id);
        generated[KEY_ID_LEN..KEY_ID_LEN + NONCE_PREFIX_LEN].copy_from_slice(&nonce_prefix);
        generated[KEY_ID_LEN + NONCE_PREFIX_LEN..].copy_from_slice(&key_material);
        generated
    }

    fn assert_invalid(result: Result<OpenedSessionTokenV1, SessionTokenOpenError>) {
        assert!(matches!(result, Err(SessionTokenOpenError::InvalidToken)));
    }

    fn live_role(name: &str) -> LiveRoleIdentity {
        let role = IamRoleIdentity::new(
            AwsAccountId::new("123456789012").unwrap(),
            StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            RoleName::new(name).unwrap(),
            IamPath::new("/test/").unwrap(),
        );
        LiveRoleIdentity::new(
            s3_types::AccountIdentity::new(
                "123456789012",
                s3_types::CanonicalUserId::from_principal("123456789012"),
                "test account",
            ),
            role,
        )
        .unwrap()
    }

    fn resolved_role(name: &str) -> (IdentityProvider, ResolvedRoleIdentity) {
        let stable_id = StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let mut roles = RoleIdentityStore::new();
        roles.add(live_role(name)).unwrap();
        let provider =
            IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles).unwrap();
        let role = provider
            .lookup_live_role_identity(&stable_id)
            .unwrap()
            .unwrap();
        (provider, role)
    }

    enum MutableRoleState {
        Present(Arc<LiveRoleIdentity>),
        Missing,
        Failure(IdentityProviderError),
    }

    struct MutableRoleProvider {
        role: Arc<RwLock<MutableRoleState>>,
    }

    impl IdentityProviderBackend for MutableRoleProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
            Ok(None)
        }

        fn lookup_live_role_identity(
            &self,
            stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<LiveRoleIdentity>>, IdentityProviderError> {
            let role = self
                .role
                .read()
                .map_err(|_| IdentityProviderError::Unavailable)?;
            match &*role {
                MutableRoleState::Present(role) if role.role().stable_id() == stable_role_id => {
                    Ok(Some(Arc::clone(role)))
                }
                MutableRoleState::Present(_) | MutableRoleState::Missing => Ok(None),
                MutableRoleState::Failure(error) => Err(*error),
            }
        }

        fn lookup_role_authorization(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<crate::RoleAuthorizationRecord>>, IdentityProviderError> {
            Ok(None)
        }

        fn lookup_configured_principal_authorization(
            &self,
            _key: &crate::ConfiguredPrincipalAuthorizationKey,
        ) -> Result<Option<Arc<crate::ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError>
        {
            Ok(None)
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
            Ok(None)
        }
    }

    fn credential(
        issuer: &ResolvedRoleIdentity,
        role_name: &str,
        session_name: &str,
        source_identity: Option<&str>,
    ) -> DecodedSessionCredential {
        assert_eq!(issuer.role().name().as_str(), role_name);
        DecodedSessionCredential::version1(
            "ARGS0123456789ABCDEFGHIJ".to_string(),
            SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
            issuer,
            RoleSessionName::new(session_name).unwrap(),
            SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
            source_identity.map(|value| SourceIdentity::new(value).unwrap()),
        )
        .unwrap()
    }

    fn generated_material() -> GeneratedSessionCredentialMaterial {
        GeneratedSessionCredentialMaterial::from_parts_for_test(
            "ARGS0123456789ABCDEFGHIJ".to_string(),
            SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
        )
    }

    #[test]
    fn issuance_bound_is_derived_from_actual_ascii_identity_bounds() {
        assert_eq!(MAX_V1_PLAINTEXT_LEN, 507);
        assert_eq!(MAX_ISSUED_V1_FRAME_LEN, 551);
        assert_eq!(MAX_ISSUED_V1_TOKEN_LEN, 742);

        let (_provider, issuer) = resolved_role(&"r".repeat(ROLE_NAME_MAX_LEN));
        let credential = credential(
            &issuer,
            &"r".repeat(ROLE_NAME_MAX_LEN),
            &"s".repeat(ROLE_SESSION_NAME_MAX_LEN),
            Some(&"i".repeat(SOURCE_IDENTITY_MAX_LEN)),
        );
        let token = key_ring().seal_v1(&credential).unwrap();
        assert_eq!(token.len(), MAX_ISSUED_V1_TOKEN_LEN);
    }

    #[test]
    fn seal_and_open_round_trip_without_issued_session_state() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", Some("source-user"));
        let key_ring = key_ring();

        let first = key_ring.seal_v1(&credential).unwrap();
        let second = key_ring.seal_v1(&credential).unwrap();
        assert_ne!(first, second);
        for token in [first, second] {
            let opened = key_ring.open_v1(&token).unwrap();
            assert_eq!(opened.access_key_id, credential.access_key_id());
            assert_eq!(opened.secret_key.as_str(), credential.secret_key().as_str());
            assert_eq!(opened.stable_role_id, *issuer.role().stable_id());
            assert_eq!(opened.role_name, *issuer.role().name());
            assert_eq!(opened.session_name.as_str(), "test-session");
            assert_eq!(
                opened.source_identity.as_ref().map(SourceIdentity::as_str),
                Some("source-user")
            );
        }
        assert!(key_ring
            .status()
            .unwrap()
            .validation_only_key_ids()
            .is_empty());
    }

    #[test]
    fn provider_clones_share_sealing_keys_and_resolve_authoritative_identity() {
        let (provider, issuer) = resolved_role("test-role");
        let clone = provider.clone();
        let token = provider
            .seal_session_credential_v1(
                generated_material(),
                &issuer,
                RoleSessionName::new("test-session").unwrap(),
                SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                Some(SourceIdentity::new("source-user").unwrap()),
            )
            .unwrap();

        let opened = clone
            .authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            )
            .unwrap();
        let opened = opened.session().unwrap();
        assert_eq!(opened.access_key_id(), "ARGS0123456789ABCDEFGHIJ");
        assert_eq!(
            opened.identity().account().account_id(),
            issuer.account().account_id()
        );
        assert_eq!(opened.session().role(), issuer.role());
        assert_eq!(opened.session().session_name().as_str(), "test-session");
        assert_eq!(
            opened
                .session()
                .source_identity()
                .map(SourceIdentity::as_str),
            Some("source-user")
        );

        let (other_provider, _) = resolved_role("test-role");
        assert_invalid_provider(other_provider.authenticate_session_credential(
            "ARGS0123456789ABCDEFGHIJ",
            Some(&token),
            1_700_003_599,
        ));
    }

    #[test]
    fn provider_authentication_enforces_binding_expiry_and_stable_issuer_liveness_in_order() {
        let role_state = Arc::new(RwLock::new(MutableRoleState::Present(Arc::new(live_role(
            "test-role",
        )))));
        let provider = IdentityProvider::new(MutableRoleProvider {
            role: Arc::clone(&role_state),
        })
        .unwrap();
        let stable_role_id = StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let issuer = provider
            .lookup_live_role_identity(&stable_role_id)
            .unwrap()
            .unwrap();
        let token = provider
            .seal_session_credential_v1(
                generated_material(),
                &issuer,
                RoleSessionName::new("test-session").unwrap(),
                SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                None,
            )
            .unwrap();

        let valid = provider
            .authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            )
            .unwrap();
        assert_eq!(
            valid.session().unwrap().session().role().stable_id(),
            &stable_role_id
        );

        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS1123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_600,
            ),
            Err(SessionCredentialAuthenticationError::InvalidCredential)
        ));
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_600,
            ),
            Err(SessionCredentialAuthenticationError::ExpiredToken)
        ));

        *role_state.write().unwrap() = MutableRoleState::Missing;
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            ),
            Err(SessionCredentialAuthenticationError::InvalidCredential)
        ));
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_600,
            ),
            Err(SessionCredentialAuthenticationError::ExpiredToken)
        ));

        *role_state.write().unwrap() =
            MutableRoleState::Failure(IdentityProviderError::Unavailable);
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS1123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            ),
            Err(SessionCredentialAuthenticationError::InvalidCredential)
        ));
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            ),
            Err(SessionCredentialAuthenticationError::IdentityProvider(
                IdentityProviderError::Unavailable
            ))
        ));
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_600,
            ),
            Err(SessionCredentialAuthenticationError::ExpiredToken)
        ));

        *role_state.write().unwrap() =
            MutableRoleState::Present(Arc::new(live_role("replacement-role")));
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS0123456789ABCDEFGHIJ",
                Some(&token),
                1_700_003_599,
            ),
            Err(SessionCredentialAuthenticationError::IdentityProvider(
                IdentityProviderError::InvalidRecord
            ))
        ));
    }

    fn assert_invalid_provider(
        result: Result<crate::AuthenticatedCredential, SessionCredentialAuthenticationError>,
    ) {
        assert!(matches!(
            result,
            Err(SessionCredentialAuthenticationError::InvalidToken)
        ));
    }

    #[test]
    fn provider_authentication_classifies_missing_empty_and_malformed_tokens_without_leaking_input()
    {
        let (provider, _) = resolved_role("test-role");
        let access_key_id = "ARGS0123456789ABCDEFGHIJ";

        for token in [None, Some("")] {
            assert!(matches!(
                provider.authenticate_session_credential(access_key_id, token, 1_700_000_000),
                Err(SessionCredentialAuthenticationError::InvalidCredential)
            ));
        }

        let malformed = "ARGST1.not-a-canonical-token";
        let error = provider
            .authenticate_session_credential(access_key_id, Some(malformed), 1_700_000_000)
            .unwrap_err();
        assert_eq!(error, SessionCredentialAuthenticationError::InvalidToken);
        assert!(matches!(
            provider.authenticate_session_credential(
                "ARGS1123456789ABCDEFGHIJ",
                Some(malformed),
                1_700_000_000,
            ),
            Err(SessionCredentialAuthenticationError::InvalidToken)
        ));
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains(access_key_id));
        assert!(!rendered.contains(malformed));
    }

    #[test]
    fn concurrent_issuance_uses_unique_counter_nonces() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = Arc::new(credential(&issuer, "test-role", "test-session", None));
        let key_ring = Arc::new(key_ring());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let key_ring = Arc::clone(&key_ring);
                let credential = Arc::clone(&credential);
                std::thread::spawn(move || {
                    (0..128)
                        .map(|_| key_ring.seal_v1(&credential).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut tokens: Vec<_> = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect();
        tokens.sort_unstable();
        tokens.dedup();
        assert_eq!(tokens.len(), 8 * 128);
    }

    #[test]
    fn nonce_exhaustion_fails_closed() {
        let key_ring = SessionTokenKeyRing::from_initial_parts(
            DOMAIN,
            KEY_ID,
            NONCE_PREFIX,
            KEY_MATERIAL,
            u64::MAX,
        )
        .unwrap();
        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", None);
        assert_eq!(
            key_ring.seal_v1(&credential),
            Err(SessionTokenSealError::NonceExhausted)
        );
    }

    #[test]
    fn tampering_wrong_domain_and_unknown_key_are_indistinguishable() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", None);
        let key_ring = key_ring();
        let token = key_ring.seal_v1(&credential).unwrap();

        let mut tampered = token.clone().into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).unwrap();
        assert_invalid(key_ring.open_v1(&tampered));

        let other_domain = SessionTokenKeyRing::from_initial_parts(
            [0x55; CREDENTIAL_DOMAIN_LEN],
            KEY_ID,
            NONCE_PREFIX,
            KEY_MATERIAL,
            0,
        )
        .unwrap();
        assert_invalid(other_domain.open_v1(&token));

        let mut frame = URL_SAFE_NO_PAD
            .decode(&token[SESSION_TOKEN_V1_PREFIX.len()..])
            .unwrap();
        frame[..KEY_ID_LEN].fill(0x99);
        let unknown_key = format!("{SESSION_TOKEN_V1_PREFIX}{}", URL_SAFE_NO_PAD.encode(frame));
        assert_invalid(key_ring.open_v1(&unknown_key));
    }

    #[test]
    fn decoder_rejects_noncanonical_truncated_oversized_and_trailing_shapes() {
        let key_ring = key_ring();
        for invalid in [
            "",
            "ARGST2.AA",
            "ARGST1.",
            "ARGST1.====",
            "ARGST1.A",
            "ARGST1.AA==",
        ] {
            assert_invalid(key_ring.open_v1(invalid));
        }
        let oversized = format!(
            "{SESSION_TOKEN_V1_PREFIX}{}",
            "A".repeat(MAX_ENCODED_SESSION_TOKEN_LEN)
        );
        assert!(oversized.len() > MAX_ENCODED_SESSION_TOKEN_LEN);
        assert_invalid(key_ring.open_v1(&oversized));
    }

    #[test]
    fn plaintext_decoder_rejects_every_truncation_and_invalid_field_shape() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", Some("source-user"));
        let plaintext = encode_v1_plaintext(&credential).unwrap();
        assert!(decode_v1_plaintext(&plaintext).is_ok());

        for cut in 0..plaintext.len() {
            assert!(matches!(
                decode_v1_plaintext(&plaintext[..cut]),
                Err(SessionTokenOpenError::InvalidToken)
            ));
        }

        let mut invalid_access_key = plaintext.clone();
        invalid_access_key[0] = b'X';
        assert!(matches!(
            decode_v1_plaintext(&invalid_access_key),
            Err(SessionTokenOpenError::InvalidToken)
        ));

        let mut invalid_secret_key = plaintext.clone();
        invalid_secret_key[SESSION_ACCESS_KEY_ID_LEN] = b'+';
        assert!(matches!(
            decode_v1_plaintext(&invalid_secret_key),
            Err(SessionTokenOpenError::InvalidToken)
        ));

        let issued_at_offset = SESSION_ACCESS_KEY_ID_LEN + SESSION_SECRET_ACCESS_KEY_LEN;
        let mut impossible_lifetime = plaintext.clone();
        impossible_lifetime[issued_at_offset..issued_at_offset + 8]
            .copy_from_slice(&(-1_i64).to_be_bytes());
        assert!(matches!(
            decode_v1_plaintext(&impossible_lifetime),
            Err(SessionTokenOpenError::InvalidToken)
        ));

        let role_name_len_offset =
            issued_at_offset + I64_PAIR_LEN + AWS_ACCOUNT_ID_LEN + STABLE_ROLE_ID_LEN;
        let role_name_offset = role_name_len_offset + U16_LEN;
        let mut invalid_utf8 = plaintext.clone();
        invalid_utf8[role_name_offset] = 0xff;
        assert!(matches!(
            decode_v1_plaintext(&invalid_utf8),
            Err(SessionTokenOpenError::InvalidToken)
        ));

        let role_name_len = usize::from(u16::from_be_bytes(
            plaintext[role_name_len_offset..role_name_offset]
                .try_into()
                .unwrap(),
        ));
        let session_name_len_offset = role_name_offset + role_name_len;
        let session_name_offset = session_name_len_offset + U16_LEN;
        let session_name_len = usize::from(u16::from_be_bytes(
            plaintext[session_name_len_offset..session_name_offset]
                .try_into()
                .unwrap(),
        ));
        let source_presence_offset = session_name_offset + session_name_len;
        let mut impossible_presence = plaintext.clone();
        impossible_presence[source_presence_offset] = 2;
        assert!(matches!(
            decode_v1_plaintext(&impossible_presence),
            Err(SessionTokenOpenError::InvalidToken)
        ));

        let mut trailing = plaintext;
        trailing.push(0);
        assert!(matches!(
            decode_v1_plaintext(&trailing),
            Err(SessionTokenOpenError::InvalidToken)
        ));
    }

    #[test]
    fn rotation_retains_old_validation_key_and_removal_revokes_it() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", None);
        let key_ring = key_ring();
        let old_token = key_ring.seal_v1(&credential).unwrap();
        let generated = [0x66; KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN];
        let new_key_id = key_ring.rotate_process_local(generated).unwrap();
        let new_token = key_ring.seal_v1(&credential).unwrap();

        assert_ne!(new_key_id, KEY_ID);
        assert!(key_ring.open_v1(&old_token).is_ok());
        assert!(key_ring.open_v1(&new_token).is_ok());
        let status = key_ring.status().unwrap();
        assert_eq!(status.active_key_id(), &new_key_id);
        assert_eq!(status.validation_only_key_ids(), &[KEY_ID]);

        assert!(!key_ring.remove_validation_only_key(&new_key_id).unwrap());
        assert!(key_ring.remove_validation_only_key(&KEY_ID).unwrap());
        assert_invalid(key_ring.open_v1(&old_token));
        assert!(key_ring.open_v1(&new_token).is_ok());
    }

    #[test]
    fn key_ids_and_material_cannot_be_reused_before_or_after_removal() {
        const NEW_KEY_ID: KeyId = [0x55; KEY_ID_LEN];
        const NEW_KEY_MATERIAL: [u8; KEY_LEN] = [0x77; KEY_LEN];
        let key_ring = key_ring();
        key_ring
            .rotate_process_local(rotation_material(
                NEW_KEY_ID,
                [0x66; NONCE_PREFIX_LEN],
                NEW_KEY_MATERIAL,
            ))
            .unwrap();

        for generated in [
            rotation_material(KEY_ID, [0x88; NONCE_PREFIX_LEN], [0x99; KEY_LEN]),
            rotation_material([0xaa; KEY_ID_LEN], [0xbb; NONCE_PREFIX_LEN], KEY_MATERIAL),
            rotation_material(
                [0xcc; KEY_ID_LEN],
                [0xdd; NONCE_PREFIX_LEN],
                NEW_KEY_MATERIAL,
            ),
        ] {
            assert_eq!(
                key_ring.rotate_process_local(generated),
                Err(SessionTokenSealError::IssuanceInvariant)
            );
        }

        assert!(key_ring.remove_validation_only_key(&KEY_ID).unwrap());
        assert!(!key_ring.remove_validation_only_key(&KEY_ID).unwrap());

        for generated in [
            rotation_material(KEY_ID, [0xee; NONCE_PREFIX_LEN], [0xff; KEY_LEN]),
            rotation_material([0x12; KEY_ID_LEN], [0x23; NONCE_PREFIX_LEN], KEY_MATERIAL),
        ] {
            assert_eq!(
                key_ring.rotate_process_local(generated),
                Err(SessionTokenSealError::IssuanceInvariant)
            );
        }

        let status = key_ring.status().unwrap();
        assert_eq!(status.active_key_id(), &NEW_KEY_ID);
        assert!(status.validation_only_key_ids().is_empty());
    }

    #[test]
    fn concurrent_rotation_and_issuance_keeps_every_retained_token_openable() {
        let (_provider, issuer) = resolved_role("test-role");
        let credential = Arc::new(credential(&issuer, "test-role", "test-session", None));
        let key_ring = Arc::new(key_ring());
        let issuing_ring = Arc::clone(&key_ring);
        let issuing_credential = Arc::clone(&credential);
        let issuer_thread = std::thread::spawn(move || {
            (0..1_024)
                .map(|_| issuing_ring.seal_v1(&issuing_credential).unwrap())
                .collect::<Vec<_>>()
        });
        let rotating_ring = Arc::clone(&key_ring);
        let rotation_thread = std::thread::spawn(move || {
            for generation in 0_u8..16 {
                let mut generated = [generation; KEY_ID_LEN + NONCE_PREFIX_LEN + KEY_LEN];
                generated[0] = 0x80 | generation;
                rotating_ring.rotate_process_local(generated).unwrap();
            }
        });
        let tokens = issuer_thread.join().unwrap();
        rotation_thread.join().unwrap();

        assert_eq!(tokens.len(), 1_024);
        for token in tokens {
            assert!(key_ring.open_v1(&token).is_ok());
        }
    }

    #[test]
    fn debug_output_never_contains_key_or_token_material() {
        let key_ring = key_ring();
        let debug = format!("{key_ring:?}");
        assert!(!debug.contains(&"44".repeat(KEY_LEN)));
        assert!(!debug.contains("nonce_prefix"));

        let (_provider, issuer) = resolved_role("test-role");
        let credential = credential(&issuer, "test-role", "test-session", None);
        let token = key_ring.seal_v1(&credential).unwrap();
        let error = match key_ring.open_v1(&format!("{token}A")) {
            Ok(_) => panic!("trailing token data must fail"),
            Err(error) => error,
        };
        assert_eq!(format!("{error:?}"), "InvalidToken");
        assert!(!format!("{error:?}").contains(&token));
    }
}
