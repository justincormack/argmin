//! Shared identity-provider abstraction.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{
    is_reserved_session_access_key_id, AuthenticatedCredential, AuthenticatedIdentity,
    AuthorizationRecordStore, AwsAccountId, ConfiguredPrincipalAuthorizationKey,
    ConfiguredPrincipalAuthorizationRecord, CredentialStore, DecodedSessionCredential,
    GeneratedSessionCredentialMaterial, IdentityPolicyRequest, LiveRoleIdentity, PolicyEvaluation,
    PrincipalIdentity, RoleAuthorizationRecord, RoleSessionName, SessionAuthorizationContext,
    SessionLifetime, SessionPolicyRestriction, SessionTokenKeyRingInitError,
    SessionTokenKeyRingStatus, SessionTokenOpenError, SessionTokenSealError, SourceIdentity,
    StableRoleId, StoredCredential,
};

/// Conflicting live-role identity in process-local bootstrap state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RoleIdentityStoreError {
    #[error("duplicate stable role ID")]
    DuplicateStableRoleId,
    #[error("duplicate live IAM role ARN")]
    DuplicateRoleArn,
}

/// Bootstrap collection of live immutable role incarnations.
///
/// Mutable trust and permission-policy state deliberately does not belong in
/// this Phase 1 liveness index.
pub struct RoleIdentityStore {
    roles: HashMap<StableRoleId, Arc<LiveRoleIdentity>>,
}

impl RoleIdentityStore {
    /// Create an empty live-role identity store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            roles: HashMap::new(),
        }
    }

    /// Add one live role incarnation.
    pub fn add(&mut self, identity: LiveRoleIdentity) -> Result<(), RoleIdentityStoreError> {
        if self.roles.contains_key(identity.role().stable_id()) {
            return Err(RoleIdentityStoreError::DuplicateStableRoleId);
        }
        if self
            .roles
            .values()
            .any(|existing| existing.role().arn() == identity.role().arn())
        {
            return Err(RoleIdentityStoreError::DuplicateRoleArn);
        }
        self.roles
            .insert(identity.role().stable_id().clone(), Arc::new(identity));
        Ok(())
    }

    fn get(&self, stable_role_id: &StableRoleId) -> Option<Arc<LiveRoleIdentity>> {
        self.roles.get(stable_role_id).cloned()
    }
}

impl Default for RoleIdentityStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Failure to read authoritative identity state.
///
/// Absence is represented by `Ok(None)` on lookup methods. It must not be
/// collapsed into this failure or vice versa.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdentityProviderError {
    #[error("identity provider unavailable")]
    Unavailable,
    #[error("identity provider returned an invalid identity record")]
    InvalidRecord,
}

impl IdentityProviderError {
    /// Stable redacted label for server-side diagnostics.
    #[must_use]
    pub const fn diagnostic_cause_label(&self) -> &'static str {
        match self {
            Self::Unavailable => "identity_provider_unavailable",
            Self::InvalidRecord => "identity_provider_invalid_record",
        }
    }
}

/// Failure while authenticating an Argmin-issued temporary credential.
///
/// This shared result deliberately does not contain the presented access key
/// or session token. Protocol adapters retain the inputs they need for
/// AWS-compatible error rendering and map these typed decisions at their own
/// service boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionCredentialAuthenticationError {
    #[error("invalid session credential")]
    InvalidCredential,
    #[error("invalid session token")]
    InvalidToken,
    #[error("session credential expired")]
    ExpiredToken,
    #[error("session-token key ring unavailable")]
    KeyRingUnavailable,
    #[error(transparent)]
    IdentityProvider(#[from] IdentityProviderError),
}

/// Live role/account record whose lookup key was verified by the shared
/// identity-provider boundary.
///
/// Its constructor is deliberately private. Session credential construction
/// accepts this type rather than a caller-constructed role or account.
#[derive(Clone, Debug)]
pub struct ResolvedRoleIdentity(Arc<LiveRoleIdentity>);

impl ResolvedRoleIdentity {
    #[must_use]
    pub fn identity(&self) -> &LiveRoleIdentity {
        &self.0
    }

    #[must_use]
    pub fn account(&self) -> &s3_types::AccountIdentity {
        self.0.account()
    }

    #[must_use]
    pub fn role(&self) -> &crate::IamRoleIdentity {
        self.0.role()
    }
}

/// Current role authorization record whose lookup binding was checked by the
/// provider boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRoleAuthorization(Arc<RoleAuthorizationRecord>);

impl ResolvedRoleAuthorization {
    #[must_use]
    pub fn record(&self) -> &RoleAuthorizationRecord {
        &self.0
    }
}

/// Current configured-principal authorization record whose lookup binding was
/// checked by the provider boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedConfiguredPrincipalAuthorization(Arc<ConfiguredPrincipalAuthorizationRecord>);

impl ResolvedConfiguredPrincipalAuthorization {
    #[must_use]
    pub fn record(&self) -> &ConfiguredPrincipalAuthorizationRecord {
        &self.0
    }
}

/// Current mutable authorization state bound to one authenticated identity.
///
/// The role-session variant retains the versioned authorization context that
/// was authenticated by the sealed token. Configured principals may have no
/// IAM policy record while the legacy bootstrap profile remains in use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedPrincipalAuthorization {
    Configured(Option<ResolvedConfiguredPrincipalAuthorization>),
    RoleSession {
        role: ResolvedRoleAuthorization,
        session_context: SessionAuthorizationContext,
    },
}

impl ResolvedPrincipalAuthorization {
    /// Return whether this authorization state belongs to the complete
    /// authenticated identity presented at the consuming boundary.
    #[must_use]
    pub fn matches_authenticated_identity(&self, identity: &AuthenticatedIdentity) -> bool {
        match (self, identity.kind()) {
            (Self::Configured(authorization), PrincipalIdentity::Configured { principal, .. }) => {
                authorization.as_ref().is_none_or(|authorization| {
                    authorization.record().account() == identity.account()
                        && authorization.record().key().principal() == principal
                })
            }
            (
                Self::RoleSession {
                    role,
                    session_context,
                },
                PrincipalIdentity::AssumedRoleSession(session),
            ) => {
                role.record().identity().account() == identity.account()
                    && role.record().identity().role() == session.role()
                    && *session_context == session.authorization_context()
            }
            (Self::Configured(_), PrincipalIdentity::AssumedRoleSession(_))
            | (Self::RoleSession { .. }, PrincipalIdentity::Configured { .. }) => false,
        }
    }

    #[must_use]
    pub fn evaluate_permissions(&self, request: &IdentityPolicyRequest<'_>) -> PolicyEvaluation {
        match self {
            Self::Configured(Some(principal)) => {
                principal.record().evaluate_identity_permissions(request)
            }
            Self::Configured(None) => PolicyEvaluation::NoMatch,
            Self::RoleSession {
                role,
                session_context: SessionAuthorizationContext::Version1NoSessionPolicy,
            } => role
                .record()
                .evaluate_session_permissions(request, SessionPolicyRestriction::Absent),
        }
    }
}

/// Read capabilities required from an identity provider.
///
/// Lookup results are owned so implementations can release locks before
/// callers perform canonicalization, cryptography, policy evaluation, storage
/// work, or response rendering.
pub trait IdentityProviderBackend: Send + Sync + 'static {
    /// Resolve a configured long-lived credential by access key ID.
    fn lookup_long_lived_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError>;

    /// Resolve a live immutable role incarnation by stable role ID.
    fn lookup_live_role_identity(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Result<Option<Arc<LiveRoleIdentity>>, IdentityProviderError>;

    /// Resolve current mutable authorization state for a role incarnation.
    fn lookup_role_authorization(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError>;

    /// Resolve current mutable authorization state for a role ARN.
    fn lookup_role_authorization_by_arn(
        &self,
        role_arn: &crate::IamRoleArn,
    ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError>;

    /// Resolve current identity-policy state for a configured principal.
    fn lookup_configured_principal_authorization(
        &self,
        key: &ConfiguredPrincipalAuthorizationKey,
    ) -> Result<Option<Arc<ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError>;

    /// Resolve the account associated with a canonical user ID.
    fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError>;
}

/// Clonable handle to authoritative identity state shared by frontend workers.
#[derive(Clone)]
pub struct IdentityProvider {
    backend: Arc<dyn IdentityProviderBackend>,
    session_token_key_ring: Arc<crate::session_token::SessionTokenKeyRing>,
}

impl IdentityProvider {
    /// Wrap an identity-provider backend in a shared handle.
    pub fn new(
        backend: impl IdentityProviderBackend,
    ) -> Result<Self, SessionTokenKeyRingInitError> {
        Ok(Self {
            backend: Arc::new(backend),
            session_token_key_ring: Arc::new(
                crate::session_token::SessionTokenKeyRing::new_process_local()?,
            ),
        })
    }

    /// Build the initial process-local in-memory provider.
    pub fn in_memory(credentials: CredentialStore) -> Result<Self, SessionTokenKeyRingInitError> {
        Self::in_memory_with_authorization(
            credentials,
            RoleIdentityStore::new(),
            AuthorizationRecordStore::new(),
        )
    }

    /// Build the process-local provider with configured credentials and roles.
    pub fn in_memory_with_roles(
        credentials: CredentialStore,
        roles: RoleIdentityStore,
    ) -> Result<Self, SessionTokenKeyRingInitError> {
        Self::in_memory_with_authorization(credentials, roles, AuthorizationRecordStore::new())
    }

    /// Build the process-local provider with configured credentials, immutable
    /// role liveness, and independently mutable authorization state.
    pub fn in_memory_with_authorization(
        credentials: CredentialStore,
        roles: RoleIdentityStore,
        authorization: AuthorizationRecordStore,
    ) -> Result<Self, SessionTokenKeyRingInitError> {
        Self::new(InMemoryIdentityProvider {
            state: RwLock::new(InMemoryIdentityState {
                credentials,
                roles,
                authorization,
            }),
        })
    }

    /// Resolve a configured long-lived credential by access key ID.
    pub fn lookup_long_lived_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
        if is_reserved_session_access_key_id(access_key_id) {
            return Ok(None);
        }
        let record = self.backend.lookup_long_lived_credential(access_key_id)?;
        if record.as_ref().is_some_and(|record| {
            record.access_key_id() != access_key_id
                || is_reserved_session_access_key_id(record.access_key_id())
        }) {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(record)
    }

    /// Resolve a live immutable role incarnation by stable role ID.
    pub fn lookup_live_role_identity(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Result<Option<ResolvedRoleIdentity>, IdentityProviderError> {
        let identity = self.backend.lookup_live_role_identity(stable_role_id)?;
        if identity
            .as_ref()
            .is_some_and(|identity| identity.role().stable_id() != stable_role_id)
        {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(identity.map(ResolvedRoleIdentity))
    }

    /// Resolve current mutable authorization state for a role incarnation.
    pub fn lookup_role_authorization(
        &self,
        expected_identity: &LiveRoleIdentity,
    ) -> Result<Option<ResolvedRoleAuthorization>, IdentityProviderError> {
        let record = self
            .backend
            .lookup_role_authorization(expected_identity.role().stable_id())?;
        if record
            .as_ref()
            .is_some_and(|record| record.identity().as_ref() != expected_identity)
        {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(record.map(ResolvedRoleAuthorization))
    }

    /// Resolve current mutable authorization state by its live IAM role ARN.
    pub fn lookup_role_authorization_by_arn(
        &self,
        role_arn: &crate::IamRoleArn,
    ) -> Result<Option<ResolvedRoleAuthorization>, IdentityProviderError> {
        let record = self.backend.lookup_role_authorization_by_arn(role_arn)?;
        if record
            .as_ref()
            .is_some_and(|record| record.identity().role().arn() != role_arn)
        {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(record.map(ResolvedRoleAuthorization))
    }

    /// Resolve the live immutable identity for an already authorized role.
    ///
    /// Both records must describe the same complete role incarnation. A
    /// backend that reuses a stable ID for different role metadata is invalid
    /// and must never influence credential issuance.
    pub fn resolve_authorized_role_identity(
        &self,
        authorization: &ResolvedRoleAuthorization,
    ) -> Result<Option<ResolvedRoleIdentity>, IdentityProviderError> {
        let identity =
            self.lookup_live_role_identity(authorization.record().identity().role().stable_id())?;
        if identity.as_ref().is_some_and(|identity| {
            identity.identity() != authorization.record().identity().as_ref()
        }) {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(identity)
    }

    /// Resolve current identity-policy state for a configured principal.
    pub fn lookup_configured_principal_authorization(
        &self,
        key: &ConfiguredPrincipalAuthorizationKey,
        expected_account: &s3_types::AccountIdentity,
    ) -> Result<Option<ResolvedConfiguredPrincipalAuthorization>, IdentityProviderError> {
        if expected_account.account_id() != Some(key.account_id().as_str()) {
            return Err(IdentityProviderError::InvalidRecord);
        }
        let record = self
            .backend
            .lookup_configured_principal_authorization(key)?;
        if record
            .as_ref()
            .is_some_and(|record| record.key() != key || record.account() != expected_account)
        {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(record.map(ResolvedConfiguredPrincipalAuthorization))
    }

    /// Resolve current mutable authorization state after authentication.
    ///
    /// A role-session identity must have a matching authorization record for
    /// the same immutable role incarnation. Configured principals may still
    /// use the legacy bootstrap profile without an IAM policy record.
    pub fn resolve_principal_authorization(
        &self,
        identity: &AuthenticatedIdentity,
    ) -> Result<ResolvedPrincipalAuthorization, IdentityProviderError> {
        match identity.kind() {
            PrincipalIdentity::Configured { principal, .. } => {
                let Some(account_id) = identity.account().account_id() else {
                    return Ok(ResolvedPrincipalAuthorization::Configured(None));
                };
                let key = ConfiguredPrincipalAuthorizationKey::new(
                    AwsAccountId::new(account_id.to_string())
                        .map_err(|_| IdentityProviderError::InvalidRecord)?,
                    principal.clone(),
                );
                self.lookup_configured_principal_authorization(&key, identity.account())
                    .map(ResolvedPrincipalAuthorization::Configured)
            }
            PrincipalIdentity::AssumedRoleSession(session) => {
                let expected_identity =
                    LiveRoleIdentity::new(identity.account().clone(), session.role().clone())
                        .map_err(|_| IdentityProviderError::InvalidRecord)?;
                let role = self
                    .lookup_role_authorization(&expected_identity)?
                    .ok_or(IdentityProviderError::InvalidRecord)?;
                Ok(ResolvedPrincipalAuthorization::RoleSession {
                    role,
                    session_context: session.authorization_context(),
                })
            }
        }
    }

    /// Resolve the account associated with a canonical user ID.
    pub fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
        let account = self
            .backend
            .find_account_by_canonical_user_id(canonical_user_id)?;
        if account
            .as_ref()
            .is_some_and(|account| account.canonical_user_id() != canonical_user_id)
        {
            return Err(IdentityProviderError::InvalidRecord);
        }
        Ok(account)
    }

    /// Seal one temporary credential using a provider-resolved live role and
    /// the shared process-local key ring.
    ///
    /// The provider owns selection of the current session-token format.
    pub fn seal_session_credential(
        &self,
        material: GeneratedSessionCredentialMaterial,
        issuer: &ResolvedRoleIdentity,
        session_name: RoleSessionName,
        lifetime: SessionLifetime,
        source_identity: Option<SourceIdentity>,
    ) -> Result<String, SessionTokenSealError> {
        let (access_key_id, secret_key) = material.into_parts();
        let credential = DecodedSessionCredential::version1(
            access_key_id,
            secret_key,
            issuer,
            session_name,
            lifetime,
            source_identity,
        )
        .map_err(|_| SessionTokenSealError::InvalidCredential)?;
        crate::session_token::seal_v1(&self.session_token_key_ring, &credential)
    }

    /// Authenticate one temporary credential after a protocol adapter has
    /// selected and structurally collapsed its mode-specific token input.
    ///
    /// Token opening and constant-time access-key binding precede expiry.
    /// Expiry precedes the authoritative stable-role liveness lookup, matching
    /// the AWS ordering pinned independently for each initial SigV4 mode.
    pub fn authenticate_session_credential(
        &self,
        access_key_id: &str,
        token: Option<&str>,
        now_epoch_secs: u64,
    ) -> Result<AuthenticatedCredential, SessionCredentialAuthenticationError> {
        let token = token
            .filter(|token| !token.is_empty())
            .ok_or(SessionCredentialAuthenticationError::InvalidCredential)?;
        let opened = crate::session_token::open_v1(&self.session_token_key_ring, token).map_err(
            |error| match error {
                SessionTokenOpenError::InvalidToken => {
                    SessionCredentialAuthenticationError::InvalidToken
                }
                SessionTokenOpenError::KeyRingUnavailable => {
                    SessionCredentialAuthenticationError::KeyRingUnavailable
                }
            },
        )?;
        if !crate::constant_time_eq(
            access_key_id.as_bytes(),
            crate::session_token::opened_access_key_id(&opened).as_bytes(),
        ) {
            return Err(SessionCredentialAuthenticationError::InvalidCredential);
        }
        let lifetime = crate::session_token::opened_lifetime(&opened);
        if i64::try_from(now_epoch_secs).map_or(true, |now| now >= lifetime.expires_at_epoch_secs())
        {
            return Err(SessionCredentialAuthenticationError::ExpiredToken);
        }
        let issuer = self
            .lookup_live_role_identity(crate::session_token::opened_stable_role_id(&opened))?
            .ok_or(SessionCredentialAuthenticationError::InvalidCredential)?;
        if issuer.role().account_id() != crate::session_token::opened_account_id(&opened)
            || issuer.role().name() != crate::session_token::opened_role_name(&opened)
        {
            return Err(IdentityProviderError::InvalidRecord.into());
        }
        let credential = DecodedSessionCredential::version1(
            crate::session_token::opened_access_key_id(&opened).to_string(),
            crate::session_token::opened_secret_key(&opened).clone(),
            &issuer,
            crate::session_token::opened_session_name(&opened).clone(),
            crate::session_token::opened_lifetime(&opened),
            crate::session_token::opened_source_identity(&opened).cloned(),
        )
        .map_err(|_| IdentityProviderError::InvalidRecord)?;
        Ok(AuthenticatedCredential::Session(Arc::new(credential)))
    }

    /// Return non-secret status for the shared session-token key ring.
    pub fn session_token_key_ring_status(
        &self,
    ) -> Result<SessionTokenKeyRingStatus, SessionTokenSealError> {
        self.session_token_key_ring.status()
    }
}

impl std::fmt::Debug for IdentityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityProvider").finish_non_exhaustive()
    }
}

struct InMemoryIdentityProvider {
    state: RwLock<InMemoryIdentityState>,
}

struct InMemoryIdentityState {
    credentials: CredentialStore,
    roles: RoleIdentityStore,
    authorization: AuthorizationRecordStore,
}

impl IdentityProviderBackend for InMemoryIdentityProvider {
    fn lookup_long_lived_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state.credentials.get_record_arc(access_key_id))
    }

    fn lookup_live_role_identity(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Result<Option<Arc<LiveRoleIdentity>>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state.roles.get(stable_role_id))
    }

    fn lookup_role_authorization(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state.authorization.role(stable_role_id))
    }

    fn lookup_role_authorization_by_arn(
        &self,
        role_arn: &crate::IamRoleArn,
    ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state.authorization.role_by_arn(role_arn))
    }

    fn lookup_configured_principal_authorization(
        &self,
        key: &ConfiguredPrincipalAuthorizationKey,
    ) -> Result<Option<Arc<ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state.authorization.configured_principal(key))
    }

    fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
        let state = self
            .state
            .read()
            .map_err(|_| IdentityProviderError::Unavailable)?;
        Ok(state
            .credentials
            .find_account_by_canonical_user_id(canonical_user_id)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthorizationProfile, AwsAccountId, ConfiguredPrincipalIdentity, IamPath, IamRoleIdentity,
        PolicyEffect, PolicyVersion, RoleMaximumSessionDuration, RoleName, RoleRecordTimestamps,
        RoleTrustPolicy, RoleTrustPolicyStatement, RoleTrustPrincipal, SecretKey,
    };

    thread_local! {
        static SUPPRESS_EXPECTED_PANIC_OUTPUT: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    static EXPECTED_PANIC_HOOK: std::sync::Once = std::sync::Once::new();

    struct SuppressExpectedPanicOutput {
        previous_suppressed: bool,
    }

    impl SuppressExpectedPanicOutput {
        fn new() -> Self {
            EXPECTED_PANIC_HOOK.call_once(|| {
                let previous_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(move |panic_info| {
                    if SUPPRESS_EXPECTED_PANIC_OUTPUT.with(std::cell::Cell::get) {
                        return;
                    }
                    previous_hook(panic_info);
                }));
            });
            let previous_suppressed = SUPPRESS_EXPECTED_PANIC_OUTPUT.with(|suppressed| {
                let previous = suppressed.get();
                suppressed.set(true);
                previous
            });
            Self {
                previous_suppressed,
            }
        }
    }

    impl Drop for SuppressExpectedPanicOutput {
        fn drop(&mut self) {
            SUPPRESS_EXPECTED_PANIC_OUTPUT
                .with(|suppressed| suppressed.set(self.previous_suppressed));
        }
    }

    fn role(stable_id: &str, name: &str) -> LiveRoleIdentity {
        let role = IamRoleIdentity::new(
            AwsAccountId::new("123456789012").unwrap(),
            StableRoleId::new(stable_id).unwrap(),
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

    fn trust_policy() -> Arc<RoleTrustPolicy> {
        Arc::new(
            RoleTrustPolicy::new(
                Some(PolicyVersion::V2012_10_17),
                vec![RoleTrustPolicyStatement::new(
                    PolicyEffect::Allow,
                    vec![RoleTrustPrincipal::new("arn:aws:iam::123456789012:root").unwrap()],
                )
                .unwrap()],
            )
            .unwrap(),
        )
    }

    struct FixedIdentityProvider {
        credential: Option<Arc<StoredCredential>>,
        role: Option<Arc<LiveRoleIdentity>>,
        account: Option<s3_types::AccountIdentity>,
        credential_lookups: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl IdentityProviderBackend for FixedIdentityProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
            self.credential_lookups
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self.credential.clone())
        }

        fn lookup_live_role_identity(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<LiveRoleIdentity>>, IdentityProviderError> {
            Ok(self.role.clone())
        }

        fn lookup_role_authorization(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            Ok(None)
        }

        fn lookup_role_authorization_by_arn(
            &self,
            _role_arn: &crate::IamRoleArn,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            Ok(None)
        }

        fn lookup_configured_principal_authorization(
            &self,
            _key: &ConfiguredPrincipalAuthorizationKey,
        ) -> Result<Option<Arc<ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError>
        {
            Ok(None)
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
            Ok(self.account.clone())
        }
    }

    fn stored_credential(access_key_id: &str) -> Arc<StoredCredential> {
        Arc::new(StoredCredential::configured(
            access_key_id.to_string(),
            SecretKey::new("secret".to_string()),
            s3_types::AccountIdentity::from_principal("configured"),
            ConfiguredPrincipalIdentity::new("configured"),
            AuthorizationProfile::Standard,
            None,
            true,
        ))
    }

    #[test]
    fn cloned_handles_share_one_backend() {
        let provider = IdentityProvider::in_memory(CredentialStore::new()).unwrap();
        let clone = provider.clone();
        assert!(Arc::ptr_eq(&provider.backend, &clone.backend));
        assert!(Arc::ptr_eq(
            &provider.session_token_key_ring,
            &clone.session_token_key_ring
        ));
    }

    #[test]
    fn in_memory_lookup_returns_owned_record() {
        let mut credentials = CredentialStore::new();
        credentials
            .add("AKID".to_string(), SecretKey::new("secret".to_string()))
            .unwrap();
        let provider = IdentityProvider::in_memory(credentials).unwrap();

        let record = provider
            .lookup_long_lived_credential("AKID")
            .unwrap()
            .unwrap();
        assert_eq!(record.access_key_id(), "AKID");
        assert_eq!(record.secret_key().as_str(), "secret");
        assert!(provider
            .lookup_long_lived_credential("missing")
            .unwrap()
            .is_none());
    }

    #[test]
    fn provider_boundary_never_resolves_reserved_long_lived_keys() {
        let credential_lookups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: Some(stored_credential("ARGS0123456789ABCDEFGHIJ")),
            role: None,
            account: None,
            credential_lookups: Arc::clone(&credential_lookups),
        })
        .unwrap();

        assert!(provider
            .lookup_long_lived_credential("ARGS0123456789ABCDEFGHIJ")
            .unwrap()
            .is_none());
        assert_eq!(
            credential_lookups.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn provider_boundary_rejects_misbound_long_lived_record() {
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: Some(stored_credential("OTHER")),
            role: None,
            account: None,
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
        .unwrap();

        assert!(matches!(
            provider.lookup_long_lived_credential("AKID"),
            Err(IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn provider_boundary_rejects_reserved_record_from_custom_backend() {
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: Some(stored_credential("ARGS0123456789ABCDEFGHIJ")),
            role: None,
            account: None,
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
        .unwrap();

        assert!(matches!(
            provider.lookup_long_lived_credential("AKID"),
            Err(IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn provider_boundary_validates_canonical_user_account_binding() {
        let requested = s3_types::CanonicalUserId::from_principal("requested owner");
        let returned_canonical_id = s3_types::CanonicalUserId::from_principal("other owner");
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: None,
            role: None,
            account: Some(s3_types::AccountIdentity::new(
                "123456789012",
                returned_canonical_id,
                "other account",
            )),
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
        .unwrap();

        assert!(matches!(
            provider.find_account_by_canonical_user_id(&requested),
            Err(IdentityProviderError::InvalidRecord)
        ));

        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: None,
            role: None,
            account: Some(s3_types::AccountIdentity::new(
                "123456789012",
                requested.clone(),
                "requested account",
            )),
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
        .unwrap();
        assert_eq!(
            provider
                .find_account_by_canonical_user_id(&requested)
                .unwrap()
                .unwrap()
                .display_name(),
            "requested account"
        );
    }

    #[test]
    fn role_identity_store_rejects_duplicate_stable_id_and_live_arn() {
        let mut roles = RoleIdentityStore::new();
        roles
            .add(role("ARGR0123456789ABCDEFGHIJ", "first"))
            .unwrap();

        assert_eq!(
            roles
                .add(role("ARGR0123456789ABCDEFGHIJ", "second"))
                .unwrap_err(),
            RoleIdentityStoreError::DuplicateStableRoleId
        );
        assert_eq!(
            roles
                .add(role("ARGRKLMNOPQRST0123456789", "first"))
                .unwrap_err(),
            RoleIdentityStoreError::DuplicateRoleArn
        );
    }

    #[test]
    fn in_memory_role_lookup_returns_live_immutable_incarnation() {
        let stable_id = StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let mut roles = RoleIdentityStore::new();
        roles.add(role(stable_id.as_str(), "test-role")).unwrap();
        let provider =
            IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles).unwrap();

        let stored = provider
            .lookup_live_role_identity(&stable_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.role().stable_id(), &stable_id);
        assert_eq!(stored.role().name().as_str(), "test-role");
        assert!(provider
            .lookup_live_role_identity(&StableRoleId::new("ARGRKLMNOPQRST0123456789").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn in_memory_authorization_lookup_is_separate_and_fully_bound() {
        let identity = role("ARGR0123456789ABCDEFGHIJ", "test-role");
        let expected_identity = identity.clone();
        let expected_role = identity.role().clone();
        let expected_account = identity.account().clone();
        let configured_key = ConfiguredPrincipalAuthorizationKey::new(
            AwsAccountId::new("123456789012").unwrap(),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
        );
        let mut roles = RoleIdentityStore::new();
        roles.add(identity.clone()).unwrap();
        let mut authorization = AuthorizationRecordStore::new();
        authorization
            .add_role(
                RoleAuthorizationRecord::new(
                    Arc::new(identity),
                    RoleRecordTimestamps::new(1_700_000_000, 1_700_000_000).unwrap(),
                    RoleMaximumSessionDuration::new(3_600).unwrap(),
                    trust_policy(),
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap();
        authorization
            .add_configured_principal(
                ConfiguredPrincipalAuthorizationRecord::new(
                    configured_key.clone(),
                    expected_account.clone(),
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap();
        let provider = IdentityProvider::in_memory_with_authorization(
            CredentialStore::new(),
            roles,
            authorization,
        )
        .unwrap();

        let resolved = provider
            .lookup_role_authorization(&expected_identity)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.record().identity().role(), &expected_role);
        assert_eq!(
            resolved.record().maximum_session_duration().seconds(),
            3_600
        );
        let resolved_by_arn = provider
            .lookup_role_authorization_by_arn(expected_role.arn())
            .unwrap()
            .unwrap();
        assert_eq!(resolved_by_arn.record().identity().role(), &expected_role);
        assert!(provider
            .lookup_role_authorization_by_arn(
                &crate::IamRoleArn::new("arn:aws:iam::123456789012:role/missing").unwrap(),
            )
            .unwrap()
            .is_none());
        let configured = provider
            .lookup_configured_principal_authorization(&configured_key, &expected_account)
            .unwrap()
            .unwrap();
        assert_eq!(configured.record().key(), &configured_key);
    }

    struct AuthorizationFailureProvider {
        role: Arc<LiveRoleIdentity>,
        authorization_lookups: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl IdentityProviderBackend for AuthorizationFailureProvider {
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
            Ok((self.role.role().stable_id() == stable_role_id).then(|| Arc::clone(&self.role)))
        }

        fn lookup_role_authorization(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            self.authorization_lookups
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(IdentityProviderError::Unavailable)
        }

        fn lookup_role_authorization_by_arn(
            &self,
            _role_arn: &crate::IamRoleArn,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            Err(IdentityProviderError::Unavailable)
        }

        fn lookup_configured_principal_authorization(
            &self,
            _key: &ConfiguredPrincipalAuthorizationKey,
        ) -> Result<Option<Arc<ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError>
        {
            Err(IdentityProviderError::Unavailable)
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
            Ok(None)
        }
    }

    #[test]
    fn authorization_provider_failure_is_not_read_during_session_authentication() {
        let live_role = Arc::new(role("ARGR0123456789ABCDEFGHIJ", "test-role"));
        let authorization_lookups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = IdentityProvider::new(AuthorizationFailureProvider {
            role: Arc::clone(&live_role),
            authorization_lookups: Arc::clone(&authorization_lookups),
        })
        .unwrap();
        let issuer = provider
            .lookup_live_role_identity(live_role.role().stable_id())
            .unwrap()
            .unwrap();
        let access_key_id = "ARGS0123456789ABCDEFGHIJ";
        let token = provider
            .seal_session_credential(
                GeneratedSessionCredentialMaterial::from_parts_for_test(
                    access_key_id.to_string(),
                    SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
                ),
                &issuer,
                RoleSessionName::new("test-session").unwrap(),
                SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                None,
            )
            .unwrap();

        let authenticated = provider
            .authenticate_session_credential(access_key_id, Some(&token), 1_700_000_001)
            .unwrap();
        assert_eq!(
            authorization_lookups.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(matches!(
            provider.resolve_principal_authorization(authenticated.identity()),
            Err(IdentityProviderError::Unavailable)
        ));
        assert_eq!(
            authorization_lookups.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    struct FixedAuthorizationProvider {
        live_role: Arc<LiveRoleIdentity>,
        role: Arc<RoleAuthorizationRecord>,
        principal: Arc<ConfiguredPrincipalAuthorizationRecord>,
    }

    impl IdentityProviderBackend for FixedAuthorizationProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
            Ok(None)
        }

        fn lookup_live_role_identity(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<LiveRoleIdentity>>, IdentityProviderError> {
            Ok(Some(Arc::clone(&self.live_role)))
        }

        fn lookup_role_authorization(
            &self,
            _stable_role_id: &StableRoleId,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            Ok(Some(Arc::clone(&self.role)))
        }

        fn lookup_role_authorization_by_arn(
            &self,
            _role_arn: &crate::IamRoleArn,
        ) -> Result<Option<Arc<RoleAuthorizationRecord>>, IdentityProviderError> {
            Ok(Some(Arc::clone(&self.role)))
        }

        fn lookup_configured_principal_authorization(
            &self,
            _key: &ConfiguredPrincipalAuthorizationKey,
        ) -> Result<Option<Arc<ConfiguredPrincipalAuthorizationRecord>>, IdentityProviderError>
        {
            Ok(Some(Arc::clone(&self.principal)))
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
            Ok(None)
        }
    }

    #[test]
    fn provider_boundary_rejects_misbound_authorization_records() {
        let stored_role = Arc::new(role("ARGR0123456789ABCDEFGHIJ", "stored-role"));
        let role_record = Arc::new(
            RoleAuthorizationRecord::new(
                Arc::clone(&stored_role),
                RoleRecordTimestamps::new(1, 1).unwrap(),
                RoleMaximumSessionDuration::new(3_600).unwrap(),
                trust_policy(),
                Vec::new(),
            )
            .unwrap(),
        );
        let account = stored_role.account().clone();
        let stored_principal_key = ConfiguredPrincipalAuthorizationKey::new(
            AwsAccountId::new("123456789012").unwrap(),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/stored"),
        );
        let principal_record = Arc::new(
            ConfiguredPrincipalAuthorizationRecord::new(
                stored_principal_key.clone(),
                account,
                Vec::new(),
            )
            .unwrap(),
        );
        let provider = IdentityProvider::new(FixedAuthorizationProvider {
            live_role: Arc::clone(&stored_role),
            role: role_record,
            principal: principal_record,
        })
        .unwrap();
        let different_account = s3_types::AccountIdentity::new(
            "123456789012",
            s3_types::CanonicalUserId::from_principal("different-canonical-user"),
            "different display identity",
        );
        let expected_role =
            LiveRoleIdentity::new(different_account.clone(), stored_role.role().clone()).unwrap();

        assert!(matches!(
            provider.lookup_role_authorization(&expected_role),
            Err(IdentityProviderError::InvalidRecord)
        ));
        assert!(matches!(
            provider.lookup_configured_principal_authorization(
                &stored_principal_key,
                &different_account,
            ),
            Err(IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn provider_boundary_rejects_authorized_role_bound_to_different_live_identity() {
        let authorized_role = Arc::new(role("ARGR0123456789ABCDEFGHIJ", "authorized-role"));
        let live_role = Arc::new(role("ARGR0123456789ABCDEFGHIJ", "different-live-role"));
        let role_record = Arc::new(
            RoleAuthorizationRecord::new(
                Arc::clone(&authorized_role),
                RoleRecordTimestamps::new(1, 1).unwrap(),
                RoleMaximumSessionDuration::new(3_600).unwrap(),
                trust_policy(),
                Vec::new(),
            )
            .unwrap(),
        );
        let principal_record = Arc::new(
            ConfiguredPrincipalAuthorizationRecord::new(
                ConfiguredPrincipalAuthorizationKey::new(
                    AwsAccountId::new("123456789012").unwrap(),
                    ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/stored"),
                ),
                authorized_role.account().clone(),
                Vec::new(),
            )
            .unwrap(),
        );
        let provider = IdentityProvider::new(FixedAuthorizationProvider {
            live_role,
            role: role_record,
            principal: principal_record,
        })
        .unwrap();
        let authorization = provider
            .lookup_role_authorization_by_arn(authorized_role.role().arn())
            .unwrap()
            .unwrap();

        assert!(matches!(
            provider.resolve_authorized_role_identity(&authorization),
            Err(IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn provider_boundary_rejects_misbound_live_role_record() {
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: None,
            role: Some(Arc::new(role("ARGR0123456789ABCDEFGHIJ", "test-role"))),
            account: None,
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
        .unwrap();

        assert!(matches!(
            provider
                .lookup_live_role_identity(&StableRoleId::new("ARGRKLMNOPQRST0123456789").unwrap()),
            Err(IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn poisoned_in_memory_provider_is_failure_not_absence() {
        let backend = Arc::new(InMemoryIdentityProvider {
            state: RwLock::new(InMemoryIdentityState {
                credentials: CredentialStore::new(),
                roles: RoleIdentityStore::new(),
                authorization: AuthorizationRecordStore::new(),
            }),
        });
        let poison_target = Arc::clone(&backend);
        let _ = std::thread::spawn(move || {
            let _panic_guard = SuppressExpectedPanicOutput::new();
            let _guard = poison_target.state.write().unwrap();
            panic!("poison identity state for test");
        })
        .join();
        let provider = IdentityProvider {
            backend,
            session_token_key_ring: Arc::new(
                crate::session_token::SessionTokenKeyRing::new_process_local().unwrap(),
            ),
        };

        assert!(matches!(
            provider.lookup_long_lived_credential("missing"),
            Err(IdentityProviderError::Unavailable)
        ));
        assert!(matches!(
            provider
                .lookup_live_role_identity(&StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap()),
            Err(IdentityProviderError::Unavailable)
        ));
        let expected_role = role("ARGR0123456789ABCDEFGHIJ", "test-role");
        assert!(matches!(
            provider.lookup_role_authorization(&expected_role),
            Err(IdentityProviderError::Unavailable)
        ));
        let configured_key = ConfiguredPrincipalAuthorizationKey::new(
            AwsAccountId::new("123456789012").unwrap(),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
        );
        assert!(matches!(
            provider.lookup_configured_principal_authorization(
                &configured_key,
                expected_role.account(),
            ),
            Err(IdentityProviderError::Unavailable)
        ));
    }

    #[test]
    fn poisoned_session_token_key_ring_is_typed_unavailability_during_authentication() {
        let stable_role_id = StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let mut roles = RoleIdentityStore::new();
        roles
            .add(role(stable_role_id.as_str(), "test-role"))
            .unwrap();
        let provider =
            IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles).unwrap();
        let issuer = provider
            .lookup_live_role_identity(&stable_role_id)
            .unwrap()
            .unwrap();
        let access_key_id = "ARGS0123456789ABCDEFGHIJ";
        let token = provider
            .seal_session_credential(
                GeneratedSessionCredentialMaterial::from_parts_for_test(
                    access_key_id.to_string(),
                    SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string()),
                ),
                &issuer,
                RoleSessionName::new("test-session").unwrap(),
                SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
                None,
            )
            .unwrap();
        let poison_target = Arc::clone(&provider.session_token_key_ring);
        let _ = std::thread::spawn(move || {
            let _panic_guard = SuppressExpectedPanicOutput::new();
            poison_target.poison_state_for_test();
        })
        .join();

        assert_eq!(
            provider.session_token_key_ring_status(),
            Err(SessionTokenSealError::KeyRingUnavailable)
        );
        assert!(matches!(
            provider.authenticate_session_credential(access_key_id, Some(&token), 1_700_003_599,),
            Err(SessionCredentialAuthenticationError::KeyRingUnavailable)
        ));
    }
}
