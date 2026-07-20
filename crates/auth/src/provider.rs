//! Shared identity-provider abstraction.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{
    is_reserved_session_access_key_id, CredentialStore, LiveRoleIdentity, StableRoleId,
    StoredCredential,
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

/// Live role/account record whose lookup key was verified by the shared
/// identity-provider boundary.
///
/// Its constructor is deliberately private. Session credential construction
/// accepts this type rather than a caller-constructed role or account.
#[derive(Clone, Debug)]
pub struct ResolvedRoleIdentity(Arc<LiveRoleIdentity>);

impl ResolvedRoleIdentity {
    #[must_use]
    pub fn account(&self) -> &s3_types::AccountIdentity {
        self.0.account()
    }

    #[must_use]
    pub fn role(&self) -> &crate::IamRoleIdentity {
        self.0.role()
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
}

impl IdentityProvider {
    /// Wrap an identity-provider backend in a shared handle.
    #[must_use]
    pub fn new(backend: impl IdentityProviderBackend) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Build the initial process-local in-memory provider.
    #[must_use]
    pub fn in_memory(credentials: CredentialStore) -> Self {
        Self::in_memory_with_roles(credentials, RoleIdentityStore::new())
    }

    /// Build the process-local provider with configured credentials and roles.
    #[must_use]
    pub fn in_memory_with_roles(credentials: CredentialStore, roles: RoleIdentityStore) -> Self {
        Self::new(InMemoryIdentityProvider {
            state: RwLock::new(InMemoryIdentityState { credentials, roles }),
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
        RoleName, SecretKey,
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
        let provider = IdentityProvider::in_memory(CredentialStore::new());
        let clone = provider.clone();
        assert!(Arc::ptr_eq(&provider.backend, &clone.backend));
    }

    #[test]
    fn in_memory_lookup_returns_owned_record() {
        let mut credentials = CredentialStore::new();
        credentials
            .add("AKID".to_string(), SecretKey::new("secret".to_string()))
            .unwrap();
        let provider = IdentityProvider::in_memory(credentials);

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
        });

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
        });

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
        });

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
        });

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
        });
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
        let provider = IdentityProvider::in_memory_with_roles(CredentialStore::new(), roles);

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
    fn provider_boundary_rejects_misbound_live_role_record() {
        let provider = IdentityProvider::new(FixedIdentityProvider {
            credential: None,
            role: Some(Arc::new(role("ARGR0123456789ABCDEFGHIJ", "test-role"))),
            account: None,
            credential_lookups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });

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
            }),
        });
        let poison_target = Arc::clone(&backend);
        let _ = std::thread::spawn(move || {
            let _panic_guard = SuppressExpectedPanicOutput::new();
            let _guard = poison_target.state.write().unwrap();
            panic!("poison identity state for test");
        })
        .join();
        let provider = IdentityProvider { backend };

        assert!(matches!(
            provider.lookup_long_lived_credential("missing"),
            Err(IdentityProviderError::Unavailable)
        ));
        assert!(matches!(
            provider
                .lookup_live_role_identity(&StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap()),
            Err(IdentityProviderError::Unavailable)
        ));
    }
}
