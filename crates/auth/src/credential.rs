/// Credential storage for SigV4 authentication.
use std::collections::HashMap;

/// A secret access key. Deliberately does not implement Debug to avoid leaking secrets.
pub struct SecretKey(pub String);

/// Full credential record used by authentication.
pub struct CredentialRecord {
    pub access_key_id: String,
    pub secret_key: SecretKey,
    pub principal: String,
    pub session_token: Option<String>,
    pub expires_at_epoch_secs: Option<u64>,
    pub enabled: bool,
}

/// Parsed credential scope from the Authorization header.
#[derive(Debug, Clone)]
pub struct CredentialScope {
    pub access_key_id: String,
    pub date: String, // YYYYMMDD
    pub region: String,
    pub service: String, // "s3"
}

/// Static in-memory credential store. Maps access_key_id to credential record.
pub struct CredentialStore {
    keys: HashMap<String, CredentialRecord>,
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
    /// Principal defaults to the access key ID, with no session token or expiry.
    pub fn add(&mut self, access_key_id: String, secret_key: SecretKey) {
        let principal = access_key_id.clone();
        self.add_record(CredentialRecord {
            access_key_id,
            secret_key,
            principal,
            session_token: None,
            expires_at_epoch_secs: None,
            enabled: true,
        });
    }

    /// Add a full credential record.
    pub fn add_record(&mut self, record: CredentialRecord) {
        self.keys.insert(record.access_key_id.clone(), record);
    }

    /// Look up a secret key by access key ID.
    pub fn get(&self, access_key_id: &str) -> Option<&SecretKey> {
        self.keys.get(access_key_id).map(|r| &r.secret_key)
    }

    /// Look up a full credential record by access key ID.
    pub fn get_record(&self, access_key_id: &str) -> Option<&CredentialRecord> {
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
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.principal, "AKID");
        assert!(record.enabled);
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
    fn add_record_round_trip() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKID".into(),
            secret_key: SecretKey("secret".into()),
            principal: "user-123".into(),
            session_token: Some("token".into()),
            expires_at_epoch_secs: Some(1234),
            enabled: true,
        });
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.principal, "user-123");
        assert_eq!(record.session_token.as_deref(), Some("token"));
        assert_eq!(record.expires_at_epoch_secs, Some(1234));
    }

    #[test]
    fn default_is_empty() {
        let store = CredentialStore::default();
        assert!(store.get("anything").is_none());
    }
}
