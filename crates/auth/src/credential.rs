/// Credential storage for SigV4 authentication.
use std::collections::HashMap;

/// A secret access key. Deliberately does not implement Debug to avoid leaking secrets.
pub struct SecretKey(pub String);

/// Parsed credential scope from the Authorization header.
#[derive(Debug, Clone)]
pub struct CredentialScope {
    pub access_key_id: String,
    pub date: String, // YYYYMMDD
    pub region: String,
    pub service: String, // "s3"
}

/// Static in-memory credential store. Maps access_key_id to secret_key.
pub struct CredentialStore {
    keys: HashMap<String, SecretKey>,
}

impl CredentialStore {
    /// Create an empty credential store.
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Add a credential pair.
    pub fn add(&mut self, access_key_id: String, secret_key: SecretKey) {
        self.keys.insert(access_key_id, secret_key);
    }

    /// Look up a secret key by access key ID.
    pub fn get(&self, access_key_id: &str) -> Option<&SecretKey> {
        self.keys.get(access_key_id)
    }
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_missing_key() {
        let store = CredentialStore::new();
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn add_then_get() {
        let mut store = CredentialStore::new();
        store.add("AKID".into(), SecretKey("secret123".into()));
        let key = store.get("AKID").unwrap();
        assert_eq!(key.0, "secret123");
    }

    #[test]
    fn overwrite_key() {
        let mut store = CredentialStore::new();
        store.add("AKID".into(), SecretKey("first".into()));
        store.add("AKID".into(), SecretKey("second".into()));
        let key = store.get("AKID").unwrap();
        assert_eq!(key.0, "second");
    }

    #[test]
    fn default_is_empty() {
        let store = CredentialStore::default();
        assert!(store.get("anything").is_none());
    }
}
