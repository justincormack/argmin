//! Shared identity-provider abstraction.

use std::sync::{Arc, RwLock};

use crate::{CredentialStore, StoredCredential};

/// Failure to read authoritative identity state.
///
/// Absence is represented by `Ok(None)` on lookup methods. It must not be
/// collapsed into this failure or vice versa.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdentityProviderError {
    #[error("identity provider unavailable")]
    Unavailable,
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
        Self::new(InMemoryIdentityProvider {
            state: RwLock::new(credentials),
        })
    }

    /// Resolve a configured long-lived credential by access key ID.
    pub fn lookup_long_lived_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<Arc<StoredCredential>>, IdentityProviderError> {
        self.backend.lookup_long_lived_credential(access_key_id)
    }

    /// Resolve the account associated with a canonical user ID.
    pub fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Result<Option<s3_types::AccountIdentity>, IdentityProviderError> {
        self.backend
            .find_account_by_canonical_user_id(canonical_user_id)
    }
}

impl std::fmt::Debug for IdentityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityProvider").finish_non_exhaustive()
    }
}

struct InMemoryIdentityProvider {
    state: RwLock<CredentialStore>,
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
        Ok(state.get_record_arc(access_key_id))
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
            .find_account_by_canonical_user_id(canonical_user_id)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecretKey;

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

    #[test]
    fn cloned_handles_share_one_backend() {
        let provider = IdentityProvider::in_memory(CredentialStore::new());
        let clone = provider.clone();
        assert!(Arc::ptr_eq(&provider.backend, &clone.backend));
    }

    #[test]
    fn in_memory_lookup_returns_owned_record() {
        let mut credentials = CredentialStore::new();
        credentials.add("AKID".to_string(), SecretKey::new("secret".to_string()));
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
    fn poisoned_in_memory_provider_is_failure_not_absence() {
        let backend = Arc::new(InMemoryIdentityProvider {
            state: RwLock::new(CredentialStore::new()),
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
    }
}
