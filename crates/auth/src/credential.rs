/// Credential storage for SigV4 authentication.
use std::collections::HashMap;

use s3_types::AccountIdentity;

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

/// Full credential record used by authentication.
pub struct CredentialRecord {
    pub access_key_id: String,
    pub secret_key: SecretKey,
    pub account: AccountIdentity,
    pub authorization_profile: AuthorizationProfile,
    pub expires_at_epoch_secs: Option<u64>,
    pub enabled: bool,
}

impl std::fmt::Debug for CredentialRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialRecord")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field("secret_key", &self.secret_key)
            .field("account", &self.account)
            .field("authorization_profile", &self.authorization_profile)
            .field("expires_at_epoch_secs", &self.expires_at_epoch_secs)
            .field("enabled", &self.enabled)
            .finish()
    }
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
    /// Principal defaults to the access key ID, with no expiry.
    pub fn add(&mut self, access_key_id: String, secret_key: SecretKey) {
        let account = AccountIdentity::from_principal(access_key_id.clone());
        self.add_record(CredentialRecord {
            access_key_id,
            secret_key,
            account,
            authorization_profile: AuthorizationProfile::Standard,
            expires_at_epoch_secs: None,
            enabled: true,
        });
    }

    /// Add a full credential record.
    pub fn add_record(&mut self, record: CredentialRecord) {
        self.keys.insert(record.access_key_id.clone(), record);
    }

    /// Look up a full credential record by access key ID.
    pub fn get_record(&self, access_key_id: &str) -> Option<&CredentialRecord> {
        self.keys.get(access_key_id)
    }

    /// Look up an account by canonical user ID.
    pub fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Option<&AccountIdentity> {
        self.keys
            .values()
            .find(|record| record.account.canonical_user_id() == canonical_user_id)
            .map(|record| &record.account)
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
        assert!(store.get_record("nonexistent").is_none());
    }

    #[test]
    fn add_then_get() {
        let mut store = CredentialStore::new();
        store.add("AKID".into(), SecretKey::new("secret123".into()));
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.secret_key.as_str(), "secret123");
        assert_eq!(record.account.principal(), "AKID");
        assert!(record.enabled);
    }

    #[test]
    fn overwrite_key() {
        let mut store = CredentialStore::new();
        store.add("AKID".into(), SecretKey::new("first".into()));
        store.add("AKID".into(), SecretKey::new("second".into()));
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.secret_key.as_str(), "second");
    }

    #[test]
    fn add_record_round_trip() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKID".into(),
            secret_key: SecretKey::new("secret".into()),
            account: AccountIdentity::new(
                "user-123",
                s3_types::CanonicalUserId::from_principal("user-123"),
                "User 123",
            ),
            authorization_profile: AuthorizationProfile::Standard,
            expires_at_epoch_secs: Some(1234),
            enabled: true,
        });
        let record = store.get_record("AKID").unwrap();
        assert_eq!(record.account.principal(), "user-123");
        assert_eq!(record.account.display_name(), "User 123");
        assert_eq!(record.expires_at_epoch_secs, Some(1234));
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

        let record = CredentialRecord {
            access_key_id: "AK\r\nID".into(),
            secret_key: SecretKey::new("super-secret".into()),
            account: AccountIdentity::from_principal("user-123"),
            authorization_profile: AuthorizationProfile::Standard,
            expires_at_epoch_secs: Some(1234),
            enabled: true,
        };
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
