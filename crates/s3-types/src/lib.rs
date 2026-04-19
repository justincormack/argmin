//! Shared S3/domain value types used across storage and server layers.

use std::fmt::Write;
use std::num::{NonZeroU32, NonZeroU64};

pub mod lifecycle;

pub use lifecycle::*;

/// Maximum supported principal string length stored in metadata.
pub const MAX_PRINCIPAL_LEN: usize = 256;
pub const WEBSITE_REDIRECT_LOCATION_HEADER_NAME: &str = "x-amz-website-redirect-location";

/// AWS account IDs are 12 decimal digits.
pub const AWS_ACCOUNT_ID_LEN: usize = 12;

/// S3 canonical user IDs are 64 lowercase hex characters.
pub const CANONICAL_USER_ID_LEN: usize = 64;
/// AWS's special canonical user ID for anonymous public-write uploads.
pub const ANONYMOUS_UPLOAD_CANONICAL_USER_ID: &str = "65a011a29cdf8ec533ec3d1ccaae921c";

const LEGACY_SIGV2_REGIONS: &[&str] = &[
    "us-east-1",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "sa-east-1",
];

/// Returns whether this region uses the legacy `us-east-1` CreateBucket behavior
/// where a repeated create by the owner remains idempotent and the
/// `LocationConstraint` is omitted.
#[must_use]
pub fn is_legacy_create_bucket_region(region: &str) -> bool {
    region == "us-east-1"
}

/// Returns the response/body representation for bucket location constraint.
///
/// `None` is the legacy `us-east-1` null/empty location.
#[must_use]
pub fn bucket_location_constraint(region: &str) -> Option<&str> {
    match region {
        "us-east-1" => None,
        "eu-west-1" => Some("EU"),
        other => Some(other),
    }
}

/// Returns whether the region still supports the legacy S3 SigV2 auth scheme.
///
/// This covers the standard-partition regions launched before 2013. Newer
/// regions require SigV4.
#[must_use]
pub fn supports_legacy_sigv2(region: &str) -> bool {
    LEGACY_SIGV2_REGIONS.contains(&region)
}

/// Returns whether the region requires SigV4 and rejects SigV2.
#[must_use]
pub fn requires_sigv4(region: &str) -> bool {
    !supports_legacy_sigv2(region)
}

/// Returns whether a string is a valid 12-digit AWS account ID.
#[must_use]
pub fn is_valid_aws_account_id(account_id: &str) -> bool {
    account_id.len() == AWS_ACCOUNT_ID_LEN && account_id.bytes().all(|b| b.is_ascii_digit())
}

/// Extract an AWS account ID from a principal string when present.
///
/// Accepted forms:
/// - a bare 12-digit account ID
/// - IAM ARNs such as `arn:aws:iam::123456789012:user/name`
#[must_use]
pub fn aws_account_id_from_principal(principal: &str) -> Option<&str> {
    if is_valid_aws_account_id(principal) {
        return Some(principal);
    }

    let rest = principal.strip_prefix("arn:")?;
    let mut parts = rest.splitn(6, ':');
    let _partition = parts.next()?;
    let service = parts.next()?;
    if service != "iam" {
        return None;
    }
    let _region = parts.next()?;
    let account_id = parts.next()?;
    let _resource = parts.next()?;
    is_valid_aws_account_id(account_id).then_some(account_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoredHeaderValueValidationError {
    InvalidHeaderBytes,
}

fn validate_stored_header_value(value: &str) -> Result<(), StoredHeaderValueValidationError> {
    if value.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(StoredHeaderValueValidationError::InvalidHeaderBytes);
    }
    Ok(())
}

macro_rules! bounded_system_metadata_value {
    ($name:ident, $error:ident, $header_name:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
        pub enum $error {
            #[error("{field_name} value contains invalid header bytes", field_name = $header_name)]
            InvalidHeaderBytes,
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, $error> {
                let value = value.into();
                match validate_stored_header_value(&value) {
                    Ok(()) => Ok(Self(value)),
                    Err(StoredHeaderValueValidationError::InvalidHeaderBytes) => {
                        Err($error::InvalidHeaderBytes)
                    }
                }
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

bounded_system_metadata_value!(ContentType, ContentTypeError, "content-type");
bounded_system_metadata_value!(ContentEncoding, ContentEncodingError, "content-encoding");
bounded_system_metadata_value!(CacheControl, CacheControlError, "cache-control");
bounded_system_metadata_value!(
    ContentDisposition,
    ContentDispositionError,
    "content-disposition"
);
bounded_system_metadata_value!(ContentLanguage, ContentLanguageError, "content-language");
bounded_system_metadata_value!(Expires, ExpiresError, "expires");

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WebsiteRedirectLocationError {
    #[error("website redirect location must not be empty")]
    Empty,
    #[error("website redirect location contains invalid header bytes")]
    InvalidHeaderBytes,
    #[error("website redirect location must have a prefix of 'http://' or 'https://' or '/'")]
    InvalidPrefix,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WebsiteRedirectLocation(String);

impl WebsiteRedirectLocation {
    pub fn new(value: impl Into<String>) -> Result<Self, WebsiteRedirectLocationError> {
        let value = value.into();
        if value.is_empty() {
            return Err(WebsiteRedirectLocationError::Empty);
        }
        if value.bytes().any(|b| b < 0x20 || b == 0x7f) {
            return Err(WebsiteRedirectLocationError::InvalidHeaderBytes);
        }
        if !value.starts_with('/')
            && !value.starts_with("http://")
            && !value.starts_with("https://")
        {
            return Err(WebsiteRedirectLocationError::InvalidPrefix);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for WebsiteRedirectLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for WebsiteRedirectLocation {
    type Error = WebsiteRedirectLocationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for WebsiteRedirectLocation {
    type Error = WebsiteRedirectLocationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Bucket namespace requested by `CreateBucket`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketNamespace {
    Global,
    AccountRegional,
}

impl BucketNamespace {
    #[must_use]
    pub const fn as_header_value(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::AccountRegional => "account-regional",
        }
    }
}

/// Parsed `-<account-id>-<region>-an` suffix for account-regional bucket names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountRegionalBucketName<'a> {
    account_id: &'a str,
    region: &'a str,
}

impl<'a> AccountRegionalBucketName<'a> {
    #[must_use]
    pub const fn account_id(self) -> &'a str {
        self.account_id
    }

    #[must_use]
    pub const fn region(self) -> &'a str {
        self.region
    }
}

/// Parse the AWS account-regional bucket-name suffix if present.
///
/// This recognizes names that end in `-<12-digit-account-id>-<region>-an`.
#[must_use]
pub fn parse_account_regional_bucket_name(name: &str) -> Option<AccountRegionalBucketName<'_>> {
    let stem = name.strip_suffix("-an")?;
    let bytes = stem.as_bytes();
    if bytes.len() <= AWS_ACCOUNT_ID_LEN + 2 {
        return None;
    }

    for idx in (1..bytes.len()).rev() {
        if bytes[idx] != b'-' {
            continue;
        }
        let account_start = idx + 1;
        let account_end = account_start + AWS_ACCOUNT_ID_LEN;
        if account_end >= bytes.len() || bytes[account_end] != b'-' {
            continue;
        }
        let account_id = &stem[account_start..account_end];
        if !is_valid_aws_account_id(account_id) {
            continue;
        }
        let region = &stem[account_end + 1..];
        if region.is_empty() {
            continue;
        }
        let prefix = &stem[..idx];
        if prefix.is_empty() {
            continue;
        }
        return Some(AccountRegionalBucketName { account_id, region });
    }

    None
}

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

/// Object Lock retention mode.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLockMode {
    Governance = 0,
    Compliance = 1,
}

impl ObjectLockMode {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Governance),
            1 => Some(Self::Compliance),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Governance => "GOVERNANCE",
            Self::Compliance => "COMPLIANCE",
        }
    }
}

/// Object Lock legal hold status.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegalHoldStatus {
    Off = 0,
    On = 1,
}

impl LegalHoldStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Off),
            1 => Some(Self::On),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::On => "ON",
        }
    }
}

/// Stored tri-state legal hold metadata for object versions.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StoredLegalHoldStatus {
    #[default]
    NotSet = 0,
    Off = 1,
    On = 2,
}

impl StoredLegalHoldStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::NotSet),
            1 => Some(Self::Off),
            2 => Some(Self::On),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_legal_hold_status(self) -> Option<LegalHoldStatus> {
        match self {
            Self::NotSet => None,
            Self::Off => Some(LegalHoldStatus::Off),
            Self::On => Some(LegalHoldStatus::On),
        }
    }

    #[must_use]
    pub const fn from_legal_hold_status(status: Option<LegalHoldStatus>) -> Self {
        match status {
            None => Self::NotSet,
            Some(LegalHoldStatus::Off) => Self::Off,
            Some(LegalHoldStatus::On) => Self::On,
        }
    }
}

/// Bucket default retention period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPeriod {
    Days(NonZeroU32),
    Years(NonZeroU32),
}

impl RetentionPeriod {
    #[must_use]
    pub fn days(days: u32) -> Option<Self> {
        NonZeroU32::new(days).map(Self::Days)
    }

    #[must_use]
    pub fn years(years: u32) -> Option<Self> {
        NonZeroU32::new(years).map(Self::Years)
    }
}

/// Default retention rule for an Object Lock-enabled bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLockDefaultRetention {
    pub mode: ObjectLockMode,
    pub period: RetentionPeriod,
}

/// Bucket-level Object Lock configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketObjectLockConfig {
    pub enabled: bool,
    pub default_retention: Option<ObjectLockDefaultRetention>,
}

/// Per-version retention metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRetention {
    /// Absolute retain-until date as a Unix timestamp in seconds.
    pub retain_until_unix_seconds: u64,
    pub mode: ObjectLockMode,
}

/// First-class per-version Object Lock state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObjectLockState {
    pub retention: Option<ObjectRetention>,
    pub legal_hold: StoredLegalHoldStatus,
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
    const AWS_EXEC_READ_CANONICAL_ID: &'static str =
        "6aa5a366c34c1cbe25dc49211496e913e0351eb0e8c37aa3477e40942ec6b97c";

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

    /// Validate and construct from a stored canonical ID string.
    ///
    /// Stored ownership/ACL state may legitimately contain AWS's special
    /// anonymous-upload owner ID in addition to normal canonical user IDs.
    #[must_use]
    pub fn parse_stored(id: &str) -> Option<Self> {
        Self::new(id).or_else(|| {
            id.eq_ignore_ascii_case(ANONYMOUS_UPLOAD_CANONICAL_USER_ID)
                .then(|| Self(ANONYMOUS_UPLOAD_CANONICAL_USER_ID.to_string()))
        })
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

    /// Canonical user id AWS uses for the `aws-exec-read` canned ACL.
    #[must_use]
    pub fn aws_exec_read() -> Self {
        Self::new(Self::AWS_EXEC_READ_CANONICAL_ID)
            .expect("aws-exec-read canonical user id must stay valid")
    }

    /// Canonical user ID AWS uses as the shared owner for anonymous uploads.
    #[must_use]
    pub fn anonymous_upload() -> Self {
        Self(ANONYMOUS_UPLOAD_CANONICAL_USER_ID.to_string())
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

    /// AWS account ID when the principal is represented as one.
    #[must_use]
    pub fn account_id(&self) -> Option<&str> {
        aws_account_id_from_principal(&self.principal)
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
    AuthenticatedUsers,
}

impl AclGrantee {
    const ALL_USERS_TOKEN: &'static str = "all_users";
    const AUTHENTICATED_USERS_TOKEN: &'static str = "authenticated_users";

    #[must_use]
    pub const fn all_users_uri() -> &'static str {
        "http://acs.amazonaws.com/groups/global/AllUsers"
    }

    #[must_use]
    pub const fn authenticated_users_uri() -> &'static str {
        "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
    }

    #[must_use]
    pub const fn group_uri(&self) -> Option<&'static str> {
        match self {
            Self::CanonicalUser(_) => None,
            Self::AllUsers => Some(Self::all_users_uri()),
            Self::AuthenticatedUsers => Some(Self::authenticated_users_uri()),
        }
    }

    #[must_use]
    pub fn parse_group_uri(uri: &str) -> Option<Self> {
        match uri {
            uri if uri == Self::all_users_uri() => Some(Self::AllUsers),
            uri if uri == Self::authenticated_users_uri() => Some(Self::AuthenticatedUsers),
            _ => None,
        }
    }

    fn serialize_tag(&self) -> (&'static str, &str) {
        match self {
            Self::CanonicalUser(id) => ("cu", id.as_str()),
            Self::AllUsers => ("group", Self::ALL_USERS_TOKEN),
            Self::AuthenticatedUsers => ("group", Self::AUTHENTICATED_USERS_TOKEN),
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
    pub fn allows_authenticated_users(&self, permission: AclPermission) -> bool {
        self.0.iter().any(|grant| {
            matches!(grant.grantee(), AclGrantee::AuthenticatedUsers)
                && grant.permission().implies(permission)
        })
    }

    #[must_use]
    pub fn allows_public_groups(&self, permission: AclPermission) -> bool {
        self.allows_all_users(permission) || self.allows_authenticated_users(permission)
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
                    AclGrantee::CanonicalUser(CanonicalUserId::parse_stored(value).ok_or_else(
                        || format!("invalid canonical user id in ACL grant on line {}", idx + 1),
                    )?)
                }
                "group" if value == AclGrantee::ALL_USERS_TOKEN => AclGrantee::AllUsers,
                "group" if value == AclGrantee::AUTHENTICATED_USERS_TOKEN => {
                    AclGrantee::AuthenticatedUsers
                }
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
        aws_account_id_from_principal, bucket_location_constraint, is_legacy_create_bucket_region,
        is_valid_aws_account_id, parse_account_regional_bucket_name, requires_sigv4,
        supports_legacy_sigv2, AccountIdentity, AclGrant, AclGrantee, AclGrants, AclPermission,
        BucketObjectLockConfig, BucketVersioningState, CacheControl, CanonicalUserId,
        ContentEncoding, ContentType, Expires, LegalHoldStatus, ObjectLockDefaultRetention,
        ObjectLockMode, ObjectLockState, ObjectRetention, RetentionPeriod, StoredLegalHoldStatus,
        VersionId, WebsiteRedirectLocation, WebsiteRedirectLocationError,
        ANONYMOUS_UPLOAD_CANONICAL_USER_ID, CANONICAL_USER_ID_LEN,
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
    fn object_lock_mode_from_u8_round_trip() {
        assert_eq!(ObjectLockMode::from_u8(0), Some(ObjectLockMode::Governance));
        assert_eq!(ObjectLockMode::from_u8(1), Some(ObjectLockMode::Compliance));
        assert_eq!(ObjectLockMode::from_u8(2), None);
        assert_eq!(ObjectLockMode::Governance.as_str(), "GOVERNANCE");
        assert_eq!(ObjectLockMode::Compliance.as_str(), "COMPLIANCE");
    }

    #[test]
    fn legal_hold_status_round_trip() {
        assert_eq!(LegalHoldStatus::from_u8(0), Some(LegalHoldStatus::Off));
        assert_eq!(LegalHoldStatus::from_u8(1), Some(LegalHoldStatus::On));
        assert_eq!(LegalHoldStatus::from_u8(2), None);
        assert_eq!(LegalHoldStatus::Off.as_str(), "OFF");
        assert_eq!(LegalHoldStatus::On.as_str(), "ON");
    }

    #[test]
    fn stored_legal_hold_status_round_trip() {
        assert_eq!(
            StoredLegalHoldStatus::from_u8(0),
            Some(StoredLegalHoldStatus::NotSet)
        );
        assert_eq!(
            StoredLegalHoldStatus::from_u8(1),
            Some(StoredLegalHoldStatus::Off)
        );
        assert_eq!(
            StoredLegalHoldStatus::from_u8(2),
            Some(StoredLegalHoldStatus::On)
        );
        assert_eq!(StoredLegalHoldStatus::from_u8(3), None);
        assert_eq!(
            StoredLegalHoldStatus::from_legal_hold_status(None),
            StoredLegalHoldStatus::NotSet
        );
        assert_eq!(
            StoredLegalHoldStatus::from_legal_hold_status(Some(LegalHoldStatus::Off)),
            StoredLegalHoldStatus::Off
        );
        assert_eq!(
            StoredLegalHoldStatus::from_legal_hold_status(Some(LegalHoldStatus::On)),
            StoredLegalHoldStatus::On
        );
        assert_eq!(StoredLegalHoldStatus::NotSet.as_legal_hold_status(), None);
        assert_eq!(
            StoredLegalHoldStatus::Off.as_legal_hold_status(),
            Some(LegalHoldStatus::Off)
        );
        assert_eq!(
            StoredLegalHoldStatus::On.as_legal_hold_status(),
            Some(LegalHoldStatus::On)
        );
    }

    #[test]
    fn retention_period_rejects_zero() {
        assert_eq!(RetentionPeriod::days(0), None);
        assert_eq!(RetentionPeriod::years(0), None);
        assert!(matches!(
            RetentionPeriod::days(1),
            Some(RetentionPeriod::Days(_))
        ));
        assert!(matches!(
            RetentionPeriod::years(1),
            Some(RetentionPeriod::Years(_))
        ));
    }

    #[test]
    fn object_lock_state_defaults_to_unset() {
        assert_eq!(
            ObjectLockState::default(),
            ObjectLockState {
                retention: None,
                legal_hold: StoredLegalHoldStatus::NotSet,
            }
        );
        assert!(!BucketObjectLockConfig::default().enabled);
        assert_eq!(BucketObjectLockConfig::default().default_retention, None);
    }

    #[test]
    fn object_lock_types_compare_by_value() {
        let retention = ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: 1_893_456_000,
        };
        let default_retention = ObjectLockDefaultRetention {
            mode: ObjectLockMode::Compliance,
            period: RetentionPeriod::years(1).unwrap(),
        };

        assert_eq!(
            retention,
            ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: 1_893_456_000,
            }
        );
        assert_eq!(
            BucketObjectLockConfig {
                enabled: true,
                default_retention: Some(default_retention),
            },
            BucketObjectLockConfig {
                enabled: true,
                default_retention: Some(default_retention),
            }
        );
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
        assert!(CanonicalUserId::new(ANONYMOUS_UPLOAD_CANONICAL_USER_ID).is_none());
        assert!(CanonicalUserId::new("short").is_none());
        assert!(CanonicalUserId::new(&"z".repeat(CANONICAL_USER_ID_LEN)).is_none());
    }

    #[test]
    fn canonical_user_id_parse_stored_accepts_aws_anonymous_special_case_only() {
        let good = "a".repeat(CANONICAL_USER_ID_LEN);
        assert_eq!(
            CanonicalUserId::parse_stored(&good).unwrap().as_str(),
            good.as_str()
        );
        assert_eq!(
            CanonicalUserId::parse_stored(ANONYMOUS_UPLOAD_CANONICAL_USER_ID)
                .unwrap()
                .as_str(),
            ANONYMOUS_UPLOAD_CANONICAL_USER_ID
        );
        assert!(CanonicalUserId::parse_stored(&"a".repeat(32)).is_none());
        assert!(CanonicalUserId::parse_stored("short").is_none());
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
        assert_eq!(account.account_id(), None);
    }

    #[test]
    fn account_identity_account_id_detects_12_digit_ids() {
        let account = AccountIdentity::new(
            "111122223333",
            CanonicalUserId::from_principal("111122223333"),
            "owner",
        );
        assert_eq!(account.account_id(), Some("111122223333"));
        assert!(is_valid_aws_account_id("111122223333"));
        assert!(!is_valid_aws_account_id("owner-a"));
    }

    #[test]
    fn account_identity_account_id_extracts_iam_arn_account_id() {
        let account = AccountIdentity::new(
            "arn:aws:iam::111122223333:user/reader",
            CanonicalUserId::from_principal("reader"),
            "reader",
        );
        assert_eq!(account.account_id(), Some("111122223333"));
    }

    #[test]
    fn content_type_accepts_empty_and_regular_values() {
        assert_eq!(ContentType::new("").unwrap().as_str(), "");
        assert_eq!(
            ContentType::new("text/plain").unwrap().as_str(),
            "text/plain"
        );
    }

    #[test]
    fn content_type_accepts_large_value_without_boundary_budget_limit() {
        let value = "t".repeat(4096);
        assert_eq!(ContentType::new(value.clone()).unwrap().as_str(), value);
    }

    #[test]
    fn stored_http_header_values_reject_invalid_header_bytes() {
        assert!(ContentEncoding::new("gzip\nbr").is_err());
        assert!(CacheControl::new("max-age=60\x7f").is_err());
        assert!(Expires::new("Wed, 21 Oct 2015 07:28:00 GMT\n").is_err());
    }

    #[test]
    fn website_redirect_location_accepts_valid_value() {
        let value = WebsiteRedirectLocation::new("/docs/index.html").unwrap();
        assert_eq!(value.as_str(), "/docs/index.html");
    }

    #[test]
    fn website_redirect_location_rejects_empty() {
        assert!(matches!(
            WebsiteRedirectLocation::new(""),
            Err(WebsiteRedirectLocationError::Empty)
        ));
    }

    #[test]
    fn website_redirect_location_rejects_invalid_header_bytes() {
        assert!(matches!(
            WebsiteRedirectLocation::new("https://example.com/\nnext"),
            Err(WebsiteRedirectLocationError::InvalidHeaderBytes)
        ));
    }

    #[test]
    fn website_redirect_location_rejects_missing_supported_prefix() {
        assert!(matches!(
            WebsiteRedirectLocation::new("docs/index.html"),
            Err(WebsiteRedirectLocationError::InvalidPrefix)
        ));
        assert!(matches!(
            WebsiteRedirectLocation::new("ftp://example.com/out"),
            Err(WebsiteRedirectLocationError::InvalidPrefix)
        ));
    }

    #[test]
    fn website_redirect_location_accepts_large_value_without_boundary_budget_limit() {
        let value = format!("/{}", "r".repeat(4095));
        assert_eq!(
            WebsiteRedirectLocation::new(value.clone())
                .unwrap()
                .as_str(),
            value
        );
    }

    #[test]
    fn aws_account_id_from_principal_accepts_iam_arn_and_bare_id() {
        assert_eq!(
            aws_account_id_from_principal("arn:aws:iam::111122223333:root"),
            Some("111122223333")
        );
        assert_eq!(
            aws_account_id_from_principal("arn:aws:iam::111122223333:user/example"),
            Some("111122223333")
        );
        assert_eq!(
            aws_account_id_from_principal("111122223333"),
            Some("111122223333")
        );
        assert_eq!(aws_account_id_from_principal("arn:aws:s3:::bucket"), None);
        assert_eq!(aws_account_id_from_principal("owner-a"), None);
    }

    #[test]
    fn parse_account_regional_bucket_name_extracts_suffix() {
        let parsed =
            parse_account_regional_bucket_name("example-111122223333-us-west-2-an").unwrap();
        assert_eq!(parsed.account_id(), "111122223333");
        assert_eq!(parsed.region(), "us-west-2");
    }

    #[test]
    fn parse_account_regional_bucket_name_uses_last_matching_suffix() {
        let parsed = parse_account_regional_bucket_name(
            "prefix-111122223333-middle-444455556666-us-east-1-an",
        )
        .unwrap();
        assert_eq!(parsed.account_id(), "444455556666");
        assert_eq!(parsed.region(), "us-east-1");
    }

    #[test]
    fn parse_account_regional_bucket_name_rejects_non_matching_shapes() {
        assert!(parse_account_regional_bucket_name("plain-bucket").is_none());
        assert!(parse_account_regional_bucket_name("bucket-111122223333-an").is_none());
        assert!(parse_account_regional_bucket_name("111122223333-us-east-1-an").is_none());
        assert!(parse_account_regional_bucket_name("bucket-abcdef-us-east-1-an").is_none());
    }

    #[test]
    fn acl_grants_round_trip_and_normalize() {
        let alt = CanonicalUserId::from_principal("alt");
        let grants = AclGrants::new(vec![
            AclGrant::new(AclGrantee::AllUsers, AclPermission::Read),
            AclGrant::new(AclGrantee::AuthenticatedUsers, AclPermission::ReadAcp),
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
        assert!(parsed
            .iter()
            .any(|grant| grant.grantee() == &AclGrantee::AuthenticatedUsers
                && grant.permission() == AclPermission::ReadAcp));
    }

    #[test]
    fn acl_permission_parse_and_implication() {
        assert_eq!(AclPermission::parse("READ"), Some(AclPermission::Read));
        assert_eq!(AclPermission::parse("bogus"), None);
        assert!(AclPermission::FullControl.implies(AclPermission::ReadAcp));
        assert!(!AclPermission::Read.implies(AclPermission::Write));
    }

    #[test]
    fn legacy_create_bucket_region_helpers_match_aws_behavior() {
        assert!(is_legacy_create_bucket_region("us-east-1"));
        assert!(!is_legacy_create_bucket_region("us-west-2"));
        assert_eq!(bucket_location_constraint("us-east-1"), None);
        assert_eq!(bucket_location_constraint("eu-west-1"), Some("EU"));
        assert_eq!(bucket_location_constraint("us-west-2"), Some("us-west-2"));
    }

    #[test]
    fn legacy_sigv2_region_helpers_match_standard_partition_split() {
        assert!(supports_legacy_sigv2("us-east-1"));
        assert!(supports_legacy_sigv2("us-west-2"));
        assert!(supports_legacy_sigv2("ap-southeast-2"));
        assert!(!supports_legacy_sigv2("eu-central-1"));
        assert!(!supports_legacy_sigv2("ap-south-1"));
        assert!(requires_sigv4("eu-central-1"));
        assert!(!requires_sigv4("us-west-2"));
    }
}
