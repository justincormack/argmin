//! Shared S3/domain value types used across storage and server layers.

use std::fmt::Write;
use std::num::NonZeroU64;

/// Maximum supported principal string length stored in metadata.
pub const MAX_PRINCIPAL_LEN: usize = 256;

/// S3 canonical user IDs are 64 lowercase hex characters.
pub const CANONICAL_USER_ID_LEN: usize = 64;

/// Bucket versioning state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketVersioningState {
    Disabled = 0,
    Enabled = 1,
    Suspended = 2,
}

impl BucketVersioningState {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Disabled),
            1 => Some(Self::Enabled),
            2 => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// Object version identifier.
///
/// `Null` represents the single unversioned copy (`version_id=0` in storage).
/// `Versioned` represents an explicit version (`version_id>=1` in storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionId {
    /// The null version (unversioned bucket).
    Null,
    /// An explicit version in a versioning-enabled bucket.
    Versioned(NonZeroU64),
}

impl VersionId {
    /// Convert from the raw `u64` stored in the database.
    #[must_use]
    pub fn from_u64(v: u64) -> Self {
        match NonZeroU64::new(v) {
            Some(nz) => Self::Versioned(nz),
            None => Self::Null,
        }
    }

    /// Convert to the raw `u64` for database storage.
    #[must_use]
    pub fn to_u64(self) -> u64 {
        match self {
            Self::Null => 0,
            Self::Versioned(v) => v.get(),
        }
    }

    /// Returns true if this is the null (unversioned) version.
    #[must_use]
    pub fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this is an explicit versioned ID.
    #[must_use]
    pub fn is_versioned(self) -> bool {
        matches!(self, Self::Versioned(_))
    }
}

impl std::fmt::Display for VersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::Versioned(v) => write!(f, "{v}"),
        }
    }
}

/// Canonical S3 owner ID used in XML owner fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CanonicalUserId(String);

impl CanonicalUserId {
    /// Deterministically derive a canonical user id from a stable principal.
    #[must_use]
    pub fn from_principal(principal: &str) -> Self {
        let digest = ring::digest::digest(&ring::digest::SHA256, principal.as_bytes());
        let mut out = String::with_capacity(CANONICAL_USER_ID_LEN);
        for byte in digest.as_ref() {
            let _ = write!(out, "{byte:02x}");
        }
        Self(out)
    }

    /// Validate and construct from a stored canonical ID string.
    #[must_use]
    pub fn new(id: &str) -> Option<Self> {
        if id.len() != CANONICAL_USER_ID_LEN || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        Some(Self(id.to_ascii_lowercase()))
    }

    /// Borrow the underlying canonical ID string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the underlying string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for CanonicalUserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Durable authenticated account identity shared across auth and server layers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountIdentity {
    principal: String,
    canonical_user_id: CanonicalUserId,
    display_name: String,
}

impl AccountIdentity {
    /// Construct an account identity from explicit durable fields.
    #[must_use]
    pub fn new(
        principal: impl Into<String>,
        canonical_user_id: CanonicalUserId,
        display_name: impl Into<String>,
    ) -> Self {
        Self {
            principal: principal.into(),
            canonical_user_id,
            display_name: display_name.into(),
        }
    }

    /// Construct an account identity by deriving a canonical ID from the principal.
    #[must_use]
    pub fn from_principal(principal: impl Into<String>) -> Self {
        let principal = principal.into();
        let canonical_user_id = CanonicalUserId::from_principal(&principal);
        Self {
            display_name: principal.clone(),
            principal,
            canonical_user_id,
        }
    }

    /// Stable principal name for authorization and XML owner display fields.
    #[must_use]
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Stable canonical owner ID for XML owner identity fields.
    #[must_use]
    pub fn canonical_user_id(&self) -> &CanonicalUserId {
        &self.canonical_user_id
    }

    /// Display name used in XML surfaces that expose account identity.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Stored ACL permission used on bucket and object ACL surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclPermission {
    Read,
    Write,
    ReadAcp,
    WriteAcp,
    FullControl,
}

impl AclPermission {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::ReadAcp => "READ_ACP",
            Self::WriteAcp => "WRITE_ACP",
            Self::FullControl => "FULL_CONTROL",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "READ" => Some(Self::Read),
            "WRITE" => Some(Self::Write),
            "READ_ACP" => Some(Self::ReadAcp),
            "WRITE_ACP" => Some(Self::WriteAcp),
            "FULL_CONTROL" => Some(Self::FullControl),
            _ => None,
        }
    }

    #[must_use]
    pub fn implies(self, requested: Self) -> bool {
        matches!(self, Self::FullControl) || self == requested
    }
}

/// Supported ACL grantees for the currently modeled ACL surface.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclGrantee {
    CanonicalUser(CanonicalUserId),
    AllUsers,
}

impl AclGrantee {
    const ALL_USERS_TOKEN: &'static str = "all_users";

    #[must_use]
    pub const fn all_users_uri() -> &'static str {
        "http://acs.amazonaws.com/groups/global/AllUsers"
    }

    #[must_use]
    pub fn parse_group_uri(uri: &str) -> Option<Self> {
        if uri == Self::all_users_uri() {
            Some(Self::AllUsers)
        } else {
            None
        }
    }

    fn serialize_tag(&self) -> (&'static str, &str) {
        match self {
            Self::CanonicalUser(id) => ("cu", id.as_str()),
            Self::AllUsers => ("group", Self::ALL_USERS_TOKEN),
        }
    }
}

/// A single ACL grant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AclGrant {
    grantee: AclGrantee,
    permission: AclPermission,
}

impl AclGrant {
    #[must_use]
    pub fn new(grantee: AclGrantee, permission: AclPermission) -> Self {
        Self {
            grantee,
            permission,
        }
    }

    #[must_use]
    pub fn grantee(&self) -> &AclGrantee {
        &self.grantee
    }

    #[must_use]
    pub const fn permission(&self) -> AclPermission {
        self.permission
    }
}

/// Canonical stored ACL grants for a bucket or object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct AclGrants(Vec<AclGrant>);

impl AclGrants {
    #[must_use]
    pub fn new(mut grants: Vec<AclGrant>) -> Self {
        grants.sort();
        grants.dedup();
        Self(grants)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AclGrant> {
        self.0.iter()
    }

    #[must_use]
    pub fn allows_canonical_user(
        &self,
        canonical_user_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        self.0.iter().any(|grant| {
            matches!(grant.grantee(), AclGrantee::CanonicalUser(id) if id == canonical_user_id)
                && grant.permission().implies(permission)
        })
    }

    #[must_use]
    pub fn allows_all_users(&self, permission: AclPermission) -> bool {
        self.0.iter().any(|grant| {
            matches!(grant.grantee(), AclGrantee::AllUsers)
                && grant.permission().implies(permission)
        })
    }

    #[must_use]
    pub fn serialized(&self) -> String {
        let mut out = String::new();
        for grant in &self.0 {
            let (kind, value) = grant.grantee().serialize_tag();
            let _ = writeln!(out, "{kind}:{value}:{}", grant.permission().as_str());
        }
        out
    }

    pub fn parse(serialized: &str) -> Result<Self, String> {
        if serialized.is_empty() {
            return Ok(Self::default());
        }

        let mut grants = Vec::new();
        for (idx, line) in serialized.lines().enumerate() {
            let mut parts = line.splitn(3, ':');
            let kind = parts
                .next()
                .ok_or_else(|| format!("missing ACL grant kind on line {}", idx + 1))?;
            let value = parts
                .next()
                .ok_or_else(|| format!("missing ACL grant value on line {}", idx + 1))?;
            let permission_raw = parts
                .next()
                .ok_or_else(|| format!("missing ACL grant permission on line {}", idx + 1))?;

            let grantee = match kind {
                "cu" => {
                    AclGrantee::CanonicalUser(CanonicalUserId::new(value).ok_or_else(|| {
                        format!("invalid canonical user id in ACL grant on line {}", idx + 1)
                    })?)
                }
                "group" if value == AclGrantee::ALL_USERS_TOKEN => AclGrantee::AllUsers,
                _ => return Err(format!("invalid ACL grantee on line {}", idx + 1)),
            };
            let permission = AclPermission::parse(permission_raw).ok_or_else(|| {
                format!("invalid ACL permission in ACL grant on line {}", idx + 1)
            })?;
            grants.push(AclGrant::new(grantee, permission));
        }

        Ok(Self::new(grants))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccountIdentity, AclGrant, AclGrantee, AclGrants, AclPermission, BucketVersioningState,
        CanonicalUserId, VersionId, CANONICAL_USER_ID_LEN,
    };

    #[test]
    fn bucket_versioning_state_from_u8_round_trip() {
        assert_eq!(
            BucketVersioningState::from_u8(0),
            Some(BucketVersioningState::Disabled)
        );
        assert_eq!(
            BucketVersioningState::from_u8(1),
            Some(BucketVersioningState::Enabled)
        );
        assert_eq!(
            BucketVersioningState::from_u8(2),
            Some(BucketVersioningState::Suspended)
        );
        assert_eq!(BucketVersioningState::from_u8(3), None);
    }

    #[test]
    fn version_id_round_trip() {
        assert_eq!(VersionId::from_u64(0), VersionId::Null);
        assert_eq!(VersionId::Null.to_u64(), 0);

        let versioned = VersionId::from_u64(42);
        assert!(versioned.is_versioned());
        assert_eq!(versioned.to_u64(), 42);
        assert_eq!(versioned.to_string(), "42");
    }

    #[test]
    fn version_id_null_helpers() {
        assert!(VersionId::Null.is_null());
        assert!(!VersionId::Null.is_versioned());
        assert_eq!(VersionId::Null.to_string(), "null");
    }

    #[test]
    fn canonical_user_id_from_principal_is_deterministic() {
        let a = CanonicalUserId::from_principal("owner-a");
        let b = CanonicalUserId::from_principal("owner-a");
        let c = CanonicalUserId::from_principal("owner-b");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.as_str().len(), CANONICAL_USER_ID_LEN);
    }

    #[test]
    fn canonical_user_id_new_validates_shape() {
        let good = "a".repeat(CANONICAL_USER_ID_LEN);
        assert_eq!(CanonicalUserId::new(&good).unwrap().as_str(), good.as_str());
        assert!(CanonicalUserId::new("short").is_none());
        assert!(CanonicalUserId::new(&"z".repeat(CANONICAL_USER_ID_LEN)).is_none());
    }

    #[test]
    fn account_identity_from_principal_derives_canonical_id() {
        let account = AccountIdentity::from_principal("owner-a");
        assert_eq!(account.principal(), "owner-a");
        assert_eq!(account.display_name(), "owner-a");
        assert_eq!(
            account.canonical_user_id(),
            &CanonicalUserId::from_principal("owner-a")
        );
    }

    #[test]
    fn account_identity_new_uses_explicit_fields() {
        let canonical = CanonicalUserId::from_principal("owner-a");
        let account = AccountIdentity::new("owner-a", canonical.clone(), "Owner A");
        assert_eq!(account.principal(), "owner-a");
        assert_eq!(account.display_name(), "Owner A");
        assert_eq!(account.canonical_user_id(), &canonical);
    }

    #[test]
    fn acl_grants_round_trip_and_normalize() {
        let alt = CanonicalUserId::from_principal("alt");
        let grants = AclGrants::new(vec![
            AclGrant::new(AclGrantee::AllUsers, AclPermission::Read),
            AclGrant::new(
                AclGrantee::CanonicalUser(alt.clone()),
                AclPermission::FullControl,
            ),
            AclGrant::new(AclGrantee::AllUsers, AclPermission::Read),
        ]);
        let serialized = grants.serialized();
        let parsed = AclGrants::parse(&serialized).unwrap();
        assert_eq!(parsed, grants);
        assert!(parsed.allows_all_users(AclPermission::Read));
        assert!(parsed.allows_canonical_user(&alt, AclPermission::WriteAcp));
    }

    #[test]
    fn acl_permission_parse_and_implication() {
        assert_eq!(AclPermission::parse("READ"), Some(AclPermission::Read));
        assert_eq!(AclPermission::parse("bogus"), None);
        assert!(AclPermission::FullControl.implies(AclPermission::ReadAcp));
        assert!(!AclPermission::Read.implies(AclPermission::Write));
    }
}
