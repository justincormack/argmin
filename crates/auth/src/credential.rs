/// Credential storage for SigV4 authentication.
use std::collections::HashMap;
use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

use crate::{
    AssumedRoleSessionIdentity, AuthenticatedIdentity, ConfiguredPrincipalIdentity, IdentityError,
    ResolvedRoleIdentity, RoleSessionName, SessionAuthorizationContext, SessionLifetime,
    SourceIdentity,
};

/// Prefix reserved for Argmin-issued temporary access keys.
pub const SESSION_ACCESS_KEY_ID_PREFIX: &str = "ARGS";
/// Exact length of an Argmin-issued temporary access key ID.
pub const SESSION_ACCESS_KEY_ID_LEN: usize = 24;
/// Exact length of an Argmin-issued temporary secret access key.
pub const SESSION_SECRET_ACCESS_KEY_LEN: usize = 40;
const SESSION_ACCESS_KEY_SUFFIX_LEN: usize =
    SESSION_ACCESS_KEY_ID_LEN - SESSION_ACCESS_KEY_ID_PREFIX.len();
const SESSION_ACCESS_KEY_ALPHABET: &[u8; 36] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const SESSION_SECRET_RANDOM_LEN: usize = 30;

/// Failure to generate unpredictable temporary credential material.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionCredentialGenerationError {
    #[error("secure random generation failed")]
    EntropyUnavailable,
}

/// Newly generated temporary access-key material ready to seal into a token.
pub struct GeneratedSessionCredentialMaterial {
    access_key_id: String,
    secret_key: SecretKey,
}

impl GeneratedSessionCredentialMaterial {
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    #[must_use]
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }

    #[must_use]
    pub fn into_parts(self) -> (String, SecretKey) {
        (self.access_key_id, self.secret_key)
    }

    #[cfg(test)]
    pub(crate) fn from_parts_for_test(access_key_id: String, secret_key: SecretKey) -> Self {
        assert!(valid_session_access_key_id(&access_key_id));
        assert!(valid_session_secret_access_key(secret_key.as_str()));
        Self {
            access_key_id,
            secret_key,
        }
    }
}

impl std::fmt::Debug for GeneratedSessionCredentialMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedSessionCredentialMaterial")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field("secret_key", &self.secret_key)
            .finish()
    }
}

trait SessionCredentialRandomSource {
    fn fill(&self, output: &mut [u8]) -> Result<(), SessionCredentialGenerationError>;
}

struct SystemRandom;

impl SessionCredentialRandomSource for SystemRandom {
    fn fill(&self, output: &mut [u8]) -> Result<(), SessionCredentialGenerationError> {
        argmin_crypto::random::fill(output)
            .map_err(|_| SessionCredentialGenerationError::EntropyUnavailable)
    }
}

/// Generate an Argmin-namespaced session access key and a 240-bit secret key.
pub fn generate_session_credential_material(
) -> Result<GeneratedSessionCredentialMaterial, SessionCredentialGenerationError> {
    generate_session_credential_material_with(&SystemRandom)
}

fn generate_session_credential_material_with(
    random: &dyn SessionCredentialRandomSource,
) -> Result<GeneratedSessionCredentialMaterial, SessionCredentialGenerationError> {
    // Rejection sampling removes the bias that byte modulo 36 would introduce.
    const UNBIASED_BYTE_CEILING: u8 = (u8::MAX / SESSION_ACCESS_KEY_ALPHABET.len() as u8)
        * SESSION_ACCESS_KEY_ALPHABET.len() as u8;
    let mut suffix = [0_u8; SESSION_ACCESS_KEY_SUFFIX_LEN];
    let mut suffix_len = 0;
    while suffix_len < suffix.len() {
        let mut candidates = [0_u8; 32];
        random.fill(&mut candidates)?;
        for candidate in candidates {
            if candidate >= UNBIASED_BYTE_CEILING {
                continue;
            }
            suffix[suffix_len] = SESSION_ACCESS_KEY_ALPHABET
                [usize::from(candidate) % SESSION_ACCESS_KEY_ALPHABET.len()];
            suffix_len += 1;
            if suffix_len == suffix.len() {
                break;
            }
        }
    }
    let mut access_key_id = String::with_capacity(SESSION_ACCESS_KEY_ID_LEN);
    access_key_id.push_str(SESSION_ACCESS_KEY_ID_PREFIX);
    access_key_id.push_str(std::str::from_utf8(&suffix).expect("ASCII access-key alphabet"));

    let mut secret_bytes = [0_u8; SESSION_SECRET_RANDOM_LEN];
    random.fill(&mut secret_bytes)?;
    let secret_key = URL_SAFE_NO_PAD.encode(secret_bytes);
    debug_assert_eq!(secret_key.len(), SESSION_SECRET_ACCESS_KEY_LEN);

    Ok(GeneratedSessionCredentialMaterial {
        access_key_id,
        secret_key: SecretKey::new(secret_key),
    })
}

/// Rejected long-lived credential-store mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("the temporary access key namespace is reserved")]
    ReservedSessionAccessKeyNamespace,
}

/// Whether an access key ID belongs to the namespace reserved for sessions.
#[must_use]
pub fn is_reserved_session_access_key_id(value: &str) -> bool {
    value.starts_with(SESSION_ACCESS_KEY_ID_PREFIX)
}

/// Coarse-grained authorization scope attached to an authenticated credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthorizationProfile {
    /// No implicit owner-account administrative permissions beyond principal/ACL/policy checks.
    #[default]
    Standard,
    /// Broad owner-account administrative permissions for the credential's own account.
    OwnerAccountAdmin,
}

/// A secret access key. Debug deliberately redacts the value to avoid leaking secrets.
/// Field is private — use [`SecretKey::as_str()`] to access the value.
#[derive(Clone)]
pub struct SecretKey(String);

impl SecretKey {
    /// Create a new secret key.
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Access the secret key value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::redacted("secret_key"), f)
    }
}

/// Configured long-lived credential stored by the identity provider.
///
/// Temporary session credentials are decoded from sealed tokens into a
/// separate type and are never inserted into this store.
pub struct StoredCredential {
    access_key_id: String,
    secret_key: SecretKey,
    identity: AuthenticatedIdentity,
    authorization_profile: AuthorizationProfile,
    expires_at_epoch_secs: Option<u64>,
    enabled: bool,
}

impl StoredCredential {
    /// Construct a configured long-lived credential.
    #[must_use]
    pub fn configured(
        access_key_id: String,
        secret_key: SecretKey,
        account: s3_types::AccountIdentity,
        principal: ConfiguredPrincipalIdentity,
        authorization_profile: AuthorizationProfile,
        expires_at_epoch_secs: Option<u64>,
        enabled: bool,
    ) -> Self {
        Self {
            access_key_id,
            secret_key,
            identity: AuthenticatedIdentity::configured(account, principal),
            authorization_profile,
            expires_at_epoch_secs,
            enabled,
        }
    }

    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    #[must_use]
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }

    #[must_use]
    pub fn identity(&self) -> &AuthenticatedIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn authorization_profile(&self) -> AuthorizationProfile {
        self.authorization_profile
    }

    #[must_use]
    pub const fn expires_at_epoch_secs(&self) -> Option<u64> {
        self.expires_at_epoch_secs
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl std::fmt::Debug for StoredCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCredential")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field("secret_key", &self.secret_key)
            .field("identity", &self.identity)
            .field("authorization_profile", &self.authorization_profile)
            .field("expires_at_epoch_secs", &self.expires_at_epoch_secs)
            .field("enabled", &self.enabled)
            .finish()
    }
}

/// A rejected decoded session-credential field or identity composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionCredentialError {
    #[error("invalid session access key ID")]
    InvalidAccessKeyId,
    #[error("invalid session secret access key")]
    InvalidSecretAccessKey,
    #[error(transparent)]
    Identity(#[from] IdentityError),
}

/// Temporary credential decoded from and authenticated by a sealed token.
///
/// This type is constructed only after stable issuer-role liveness has been
/// resolved. It cannot be inserted into [`CredentialStore`], whose insertion
/// API accepts only [`StoredCredential`].
#[derive(Clone)]
pub struct DecodedSessionCredential {
    access_key_id: String,
    secret_key: SecretKey,
    session: Arc<AssumedRoleSessionIdentity>,
    identity: AuthenticatedIdentity,
}

impl DecodedSessionCredential {
    pub(crate) fn version1(
        access_key_id: String,
        secret_key: SecretKey,
        issuer: &ResolvedRoleIdentity,
        session_name: RoleSessionName,
        lifetime: SessionLifetime,
        source_identity: Option<SourceIdentity>,
    ) -> Result<Self, SessionCredentialError> {
        if !valid_session_access_key_id(&access_key_id) {
            return Err(SessionCredentialError::InvalidAccessKeyId);
        }
        if !valid_session_secret_access_key(secret_key.as_str()) {
            return Err(SessionCredentialError::InvalidSecretAccessKey);
        }
        let session = AssumedRoleSessionIdentity::new(
            issuer.role().clone(),
            session_name,
            lifetime,
            source_identity,
        );
        let session = Arc::new(session);
        let identity = AuthenticatedIdentity::assumed_role_session_shared(
            issuer.account().clone(),
            Arc::clone(&session),
        )?;
        Ok(Self {
            access_key_id,
            secret_key,
            session,
            identity,
        })
    }

    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    #[must_use]
    pub fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }

    #[must_use]
    pub fn identity(&self) -> &AuthenticatedIdentity {
        &self.identity
    }

    #[must_use]
    pub fn authorization_context(&self) -> SessionAuthorizationContext {
        self.session.authorization_context()
    }

    #[must_use]
    pub fn session(&self) -> &AssumedRoleSessionIdentity {
        &self.session
    }
}

impl std::fmt::Debug for DecodedSessionCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedSessionCredential")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field("secret_key", &self.secret_key)
            .field("identity", &self.identity)
            .field("authorization_context", &self.authorization_context())
            .finish()
    }
}

/// Credential material selected by the shared authentication pipeline.
#[derive(Clone, Debug)]
pub enum AuthenticatedCredential {
    LongLived(Arc<StoredCredential>),
    Session(Arc<DecodedSessionCredential>),
}

impl AuthenticatedCredential {
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        match self {
            Self::LongLived(credential) => credential.access_key_id(),
            Self::Session(credential) => credential.access_key_id(),
        }
    }

    #[must_use]
    pub fn secret_key(&self) -> &SecretKey {
        match self {
            Self::LongLived(credential) => credential.secret_key(),
            Self::Session(credential) => credential.secret_key(),
        }
    }

    #[must_use]
    pub fn identity(&self) -> &AuthenticatedIdentity {
        match self {
            Self::LongLived(credential) => credential.identity(),
            Self::Session(credential) => credential.identity(),
        }
    }

    #[must_use]
    pub fn long_lived(&self) -> Option<&StoredCredential> {
        match self {
            Self::LongLived(credential) => Some(credential),
            Self::Session(_) => None,
        }
    }

    #[must_use]
    pub fn session(&self) -> Option<&DecodedSessionCredential> {
        match self {
            Self::LongLived(_) => None,
            Self::Session(credential) => Some(credential),
        }
    }
}

pub(crate) fn valid_session_access_key_id(value: &str) -> bool {
    value.len() == SESSION_ACCESS_KEY_ID_LEN
        && value.starts_with(SESSION_ACCESS_KEY_ID_PREFIX)
        && value[SESSION_ACCESS_KEY_ID_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

pub(crate) fn valid_session_secret_access_key(value: &str) -> bool {
    value.len() == SESSION_SECRET_ACCESS_KEY_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Parsed credential scope from the Authorization header.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialScope {
    pub access_key_id: String,
    pub date: String, // YYYYMMDD
    pub region: String,
    pub service: String, // "s3"
}

impl std::fmt::Debug for CredentialScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialScope")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field("date", &observability::escaped(&self.date))
            .field("region", &observability::escaped(&self.region))
            .field("service", &observability::escaped(&self.service))
            .finish()
    }
}

/// Borrowed credential scope parsed from a request wire format.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CredentialScopeRef<'a> {
    pub access_key_id: &'a str,
    pub date: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

impl std::fmt::Debug for CredentialScopeRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialScopeRef")
            .field("access_key_id", &observability::escaped(self.access_key_id))
            .field("date", &observability::escaped(self.date))
            .field("region", &observability::escaped(self.region))
            .field("service", &observability::escaped(self.service))
            .finish()
    }
}

impl From<CredentialScopeRef<'_>> for CredentialScope {
    fn from(value: CredentialScopeRef<'_>) -> Self {
        Self {
            access_key_id: value.access_key_id.to_string(),
            date: value.date.to_string(),
            region: value.region.to_string(),
            service: value.service.to_string(),
        }
    }
}

pub(crate) fn parse_credential_scope_ref(value: &str) -> Option<CredentialScopeRef<'_>> {
    if value.is_empty() || value.len() > crate::MAX_CREDENTIAL_LEN {
        return None;
    }

    let mut parts = value.split('/');
    let access_key_id = parts.next()?;
    let date = parts.next()?;
    let region = parts.next()?;
    let service = parts.next()?;
    let terminator = parts.next()?;
    if terminator != "aws4_request" || parts.next().is_some() {
        return None;
    }
    if access_key_id.is_empty() || access_key_id.len() > crate::MAX_ACCESS_KEY_ID_LEN {
        return None;
    }
    crate::canonical::parse_amz_date_stamp(date)?;

    Some(CredentialScopeRef {
        access_key_id,
        date,
        region,
        service,
    })
}

/// Bootstrap collection of long-lived credentials for an in-memory identity provider.
///
/// Runtime frontend workers share an [`crate::IdentityProvider`] rather than
/// cloning this collection.
pub struct CredentialStore {
    keys: HashMap<String, Arc<StoredCredential>>,
}

impl CredentialStore {
    /// Create an empty credential store.
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Add a basic static credential pair.
    ///
    /// Principal defaults to the access key ID, with no expiry.
    pub fn add(
        &mut self,
        access_key_id: String,
        secret_key: SecretKey,
    ) -> Result<(), CredentialStoreError> {
        let account = s3_types::AccountIdentity::from_principal(access_key_id.clone());
        let principal = ConfiguredPrincipalIdentity::new(access_key_id.clone());
        self.add_record(StoredCredential::configured(
            access_key_id,
            secret_key,
            account,
            principal,
            AuthorizationProfile::Standard,
            None,
            true,
        ))
    }

    /// Add a full credential record.
    pub fn add_record(&mut self, record: StoredCredential) -> Result<(), CredentialStoreError> {
        if is_reserved_session_access_key_id(record.access_key_id()) {
            return Err(CredentialStoreError::ReservedSessionAccessKeyNamespace);
        }
        self.keys
            .insert(record.access_key_id().to_string(), Arc::new(record));
        Ok(())
    }

    /// Look up a full credential record by access key ID.
    pub fn get_record(&self, access_key_id: &str) -> Option<&StoredCredential> {
        self.keys.get(access_key_id).map(Arc::as_ref)
    }

    pub(crate) fn get_record_arc(&self, access_key_id: &str) -> Option<Arc<StoredCredential>> {
        self.keys.get(access_key_id).cloned()
    }

    /// Look up an account by canonical user ID.
    pub fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Option<&s3_types::AccountIdentity> {
        self.keys
            .values()
            .find(|record| record.identity().account().canonical_user_id() == canonical_user_id)
            .map(|record| record.identity().account())
    }
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::Mutex;

    use super::*;

    struct FixedRandom {
        bytes: Mutex<VecDeque<u8>>,
    }

    impl FixedRandom {
        fn new(bytes: impl IntoIterator<Item = u8>) -> Self {
            Self {
                bytes: Mutex::new(bytes.into_iter().collect()),
            }
        }
    }

    impl SessionCredentialRandomSource for FixedRandom {
        fn fill(&self, output: &mut [u8]) -> Result<(), SessionCredentialGenerationError> {
            let mut bytes = self.bytes.lock().unwrap();
            for output_byte in output {
                *output_byte = bytes
                    .pop_front()
                    .ok_or(SessionCredentialGenerationError::EntropyUnavailable)?;
            }
            Ok(())
        }
    }

    struct FailingRandom;

    impl SessionCredentialRandomSource for FailingRandom {
        fn fill(&self, _output: &mut [u8]) -> Result<(), SessionCredentialGenerationError> {
            Err(SessionCredentialGenerationError::EntropyUnavailable)
        }
    }

    fn session_credential() -> Result<DecodedSessionCredential, SessionCredentialError> {
        let role = crate::IamRoleIdentity::new(
            crate::AwsAccountId::new("123456789012").unwrap(),
            crate::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            crate::RoleName::new("test-role").unwrap(),
            crate::IamPath::new("/test/").unwrap(),
        );
        let live_role = crate::LiveRoleIdentity::new(
            s3_types::AccountIdentity::new(
                "123456789012",
                s3_types::CanonicalUserId::from_principal("authoritative account record"),
                "authoritative account",
            ),
            role,
        )
        .unwrap();
        let mut roles = crate::RoleIdentityStore::new();
        roles.add(live_role).unwrap();
        let provider =
            crate::IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles).unwrap();
        let issuer = provider
            .lookup_live_role_identity(
                &crate::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            )
            .unwrap()
            .unwrap();
        DecodedSessionCredential::version1(
            "ARGS0123456789ABCDEFGHIJ".to_string(),
            SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
            &issuer,
            crate::RoleSessionName::new("test-session").unwrap(),
            crate::SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
            None,
        )
    }

    #[test]
    fn get_missing_key() {
        let store = CredentialStore::new();
        assert!(store.get_record("nonexistent").is_none());
    }

    #[test]
    fn session_credential_generation_uses_unbiased_argmin_shapes() {
        let candidates = (252_u8..=255)
            .cycle()
            .take(12)
            .chain(0_u8..20)
            .chain(0_u8..30);
        let material =
            generate_session_credential_material_with(&FixedRandom::new(candidates)).unwrap();
        assert_eq!(material.access_key_id(), "ARGSABCDEFGHIJKLMNOPQRST");
        assert_eq!(material.access_key_id().len(), SESSION_ACCESS_KEY_ID_LEN);
        assert_eq!(
            material.secret_key().as_str(),
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwd"
        );
        assert_eq!(
            material.secret_key().as_str().len(),
            SESSION_SECRET_ACCESS_KEY_LEN
        );
        assert!(valid_session_access_key_id(material.access_key_id()));
        assert!(valid_session_secret_access_key(
            material.secret_key().as_str()
        ));
        let debug = format!("{material:?}");
        assert!(!debug.contains(material.secret_key().as_str()));
        assert!(debug.contains("<redacted:secret_key>"));
    }

    #[test]
    fn session_credential_generation_fails_closed_when_entropy_is_unavailable() {
        assert!(matches!(
            generate_session_credential_material_with(&FailingRandom),
            Err(SessionCredentialGenerationError::EntropyUnavailable)
        ));
    }

    #[test]
    fn generated_session_access_keys_are_unique_and_never_use_aws_namespaces() {
        let mut access_key_ids = HashSet::new();
        for _ in 0..256 {
            let material = generate_session_credential_material().unwrap();
            assert!(material.access_key_id().starts_with("ARGS"));
            assert!(!material.access_key_id().starts_with("ASIA"));
            assert!(!material.access_key_id().starts_with("AKIA"));
            assert!(access_key_ids.insert(material.access_key_id().to_string()));
        }
    }

    #[test]
    fn add_then_get() {
        let mut store = CredentialStore::new();
        store
            .add("AKID".into(), SecretKey::new("secret123".into()))
            .unwrap();
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.secret_key().as_str(), "secret123");
        assert_eq!(
            record
                .identity()
                .configured_principal()
                .unwrap()
                .principal(),
            "AKID"
        );
        assert!(record.is_enabled());
    }

    #[test]
    fn overwrite_key() {
        let mut store = CredentialStore::new();
        store
            .add("AKID".into(), SecretKey::new("first".into()))
            .unwrap();
        store
            .add("AKID".into(), SecretKey::new("second".into()))
            .unwrap();
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.secret_key().as_str(), "second");
    }

    #[test]
    fn long_lived_store_rejects_reserved_session_namespace() {
        let mut store = CredentialStore::new();
        assert_eq!(
            store
                .add(
                    "ARGS-configured-key".to_string(),
                    SecretKey::new("secret".to_string())
                )
                .unwrap_err(),
            CredentialStoreError::ReservedSessionAccessKeyNamespace
        );
        assert!(store.get_record("ARGS-configured-key").is_none());
    }

    #[test]
    fn add_record_round_trip() {
        let mut store = CredentialStore::new();
        store
            .add_record(StoredCredential::configured(
                "AKID".into(),
                SecretKey::new("secret".into()),
                s3_types::AccountIdentity::new(
                    "user-123",
                    s3_types::CanonicalUserId::from_principal("user-123"),
                    "User 123",
                ),
                ConfiguredPrincipalIdentity::new("user-123"),
                AuthorizationProfile::Standard,
                Some(1234),
                true,
            ))
            .unwrap();
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.identity().account().principal(), "user-123");
        assert_eq!(record.identity().account().display_name(), "User 123");
        assert_eq!(record.expires_at_epoch_secs(), Some(1234));
    }

    #[test]
    fn configured_record_keeps_account_and_request_principal_separate() {
        let account_id = "123456789012";
        let record = StoredCredential::configured(
            "AKID".into(),
            SecretKey::new("secret".into()),
            s3_types::AccountIdentity::new(
                account_id,
                s3_types::CanonicalUserId::from_principal(account_id),
                "Test account",
            ),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
            AuthorizationProfile::Standard,
            None,
            true,
        );

        assert_eq!(record.identity().account().principal(), account_id);
        assert_eq!(
            record
                .identity()
                .configured_principal()
                .unwrap()
                .principal(),
            "arn:aws:iam::123456789012:user/test"
        );
        assert!(record.identity().session_principal_arn().is_none());
        assert!(record.identity().role_principal_arn().is_none());
    }

    #[test]
    fn decoded_session_credential_requires_session_identity_and_lifetime() {
        let credential = session_credential().unwrap();
        assert_eq!(credential.access_key_id(), "ARGS0123456789ABCDEFGHIJ");
        assert_eq!(
            credential.authorization_context(),
            SessionAuthorizationContext::Version1NoSessionPolicy
        );
        assert_eq!(
            credential.session().lifetime(),
            crate::SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap()
        );
        assert_eq!(
            credential
                .identity()
                .session_principal_arn()
                .unwrap()
                .as_str(),
            "arn:aws:sts::123456789012:assumed-role/test-role/test-session"
        );
        assert_eq!(
            credential.identity().account().display_name(),
            "authoritative account"
        );
        assert_eq!(
            credential.identity().account().canonical_user_id().as_str(),
            s3_types::CanonicalUserId::from_principal("authoritative account record").as_str()
        );
    }

    #[test]
    fn decoded_session_credential_validates_argmin_key_shapes() {
        let valid = session_credential().unwrap();
        let live_role = crate::LiveRoleIdentity::new(
            valid.identity().account().clone(),
            valid.session().role().clone(),
        )
        .unwrap();
        let mut roles = crate::RoleIdentityStore::new();
        roles.add(live_role).unwrap();
        let provider =
            crate::IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles).unwrap();
        let issuer = provider
            .lookup_live_role_identity(valid.session().role().stable_id())
            .unwrap()
            .unwrap();

        for access_key_id in [
            "ASIA0123456789ABCDEFGHIJ",
            "ARGS0123456789ABCDEFGHI",
            "ARGS0123456789ABCDEFGH!J",
            "ARGS0123456789abcdefghij",
        ] {
            assert_eq!(
                DecodedSessionCredential::version1(
                    access_key_id.to_string(),
                    SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
                    &issuer,
                    crate::RoleSessionName::new("test-session").unwrap(),
                    crate::SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                    None,
                )
                .unwrap_err(),
                SessionCredentialError::InvalidAccessKeyId
            );
        }

        assert_eq!(
            DecodedSessionCredential::version1(
                "ARGS0123456789ABCDEFGHIJ".to_string(),
                SecretKey::new("not-a-forty-character-session-secret".to_string()),
                &issuer,
                crate::RoleSessionName::new("test-session").unwrap(),
                crate::SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                None,
            )
            .unwrap_err(),
            SessionCredentialError::InvalidSecretAccessKey
        );
    }

    #[test]
    fn authenticated_credential_preserves_credential_kind() {
        let session = Arc::new(session_credential().unwrap());
        let session_credential = AuthenticatedCredential::Session(Arc::clone(&session));
        assert!(session_credential.long_lived().is_none());
        assert!(Arc::ptr_eq(
            &session,
            match &session_credential {
                AuthenticatedCredential::Session(credential) => credential,
                AuthenticatedCredential::LongLived(_) => unreachable!(),
            }
        ));

        let long_lived = Arc::new(StoredCredential::configured(
            "AKID".to_string(),
            SecretKey::new("long-lived-secret".to_string()),
            s3_types::AccountIdentity::from_principal("configured"),
            ConfiguredPrincipalIdentity::new("configured"),
            AuthorizationProfile::Standard,
            None,
            true,
        ));
        let long_lived_credential = AuthenticatedCredential::LongLived(Arc::clone(&long_lived));
        assert!(long_lived_credential.session().is_none());
        assert_eq!(long_lived_credential.access_key_id(), "AKID");
        assert_eq!(long_lived_credential.identity(), long_lived.identity());
    }

    #[test]
    fn decoded_session_debug_redacts_secret() {
        let credential = session_credential().unwrap();
        let debug = format!("{credential:?}");
        assert!(debug.contains("<redacted:secret_key>"));
        assert!(!debug.contains("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN"));
    }

    #[test]
    fn default_is_empty() {
        let store = CredentialStore::default();
        assert!(store.get_record("anything").is_none());
    }

    #[test]
    fn parse_credential_scope_ref_round_trip() {
        let scope = parse_credential_scope_ref("AKID/20250101/us-east-1/s3/aws4_request").unwrap();
        assert_eq!(
            scope,
            CredentialScopeRef {
                access_key_id: "AKID",
                date: "20250101",
                region: "us-east-1",
                service: "s3",
            }
        );
        assert_eq!(
            CredentialScope::from(scope),
            CredentialScope {
                access_key_id: "AKID".to_string(),
                date: "20250101".to_string(),
                region: "us-east-1".to_string(),
                service: "s3".to_string(),
            }
        );
    }

    #[test]
    fn parse_credential_scope_ref_rejects_bad_suffix() {
        assert!(parse_credential_scope_ref("AKID/20250101/us-east-1/s3/not-aws4").is_none());
    }

    #[test]
    fn parse_credential_scope_ref_rejects_extra_segments() {
        assert!(
            parse_credential_scope_ref("AKID/20250101/us-east-1/s3/aws4_request/extra").is_none()
        );
    }

    #[test]
    fn parse_credential_scope_ref_rejects_empty_access_key() {
        assert!(parse_credential_scope_ref("/20250101/us-east-1/s3/aws4_request").is_none());
    }

    #[test]
    fn parse_credential_scope_ref_rejects_invalid_date_stamp() {
        assert!(parse_credential_scope_ref("AKID/2025010X/us-east-1/s3/aws4_request").is_none());
    }

    #[test]
    fn debug_redacts_credential_secrets_and_escapes_text() {
        let secret = SecretKey::new("super-secret".into());
        assert_eq!(format!("{secret:?}"), "<redacted:secret_key>");

        let record = StoredCredential::configured(
            "AK\r\nID".into(),
            SecretKey::new("super-secret".into()),
            s3_types::AccountIdentity::from_principal("user-123"),
            ConfiguredPrincipalIdentity::new("user-123"),
            AuthorizationProfile::Standard,
            Some(1234),
            true,
        );
        let debug = format!("{record:?}");
        assert!(debug.contains(r#""AK\r\nID""#));
        assert!(debug.contains("<redacted:secret_key>"));
        assert!(!debug.contains("super-secret"));

        let scope = CredentialScope {
            access_key_id: "AK\nID".into(),
            date: "20250101".into(),
            region: "us-\reast-1".into(),
            service: "s3".into(),
        };
        let scope_debug = format!("{scope:?}");
        assert!(scope_debug.contains(r#""AK\nID""#));
        assert!(scope_debug.contains(r#""us-\reast-1""#));
        assert!(!scope_debug.contains('\n'));
        assert!(!scope_debug.contains('\r'));
    }
}
