use s3_types::{is_valid_aws_account_id, AccountIdentity};
use std::sync::Arc;

const STABLE_ROLE_ID_PREFIX: &str = "ARGR";
const GENERATED_ID_SUFFIX_LEN: usize = 20;
const STABLE_ROLE_ID_LEN: usize = STABLE_ROLE_ID_PREFIX.len() + GENERATED_ID_SUFFIX_LEN;
const ROLE_NAME_MAX_LEN: usize = 64;
const IAM_PATH_MAX_LEN: usize = 512;
const ROLE_SESSION_NAME_MIN_LEN: usize = 2;
const ROLE_SESSION_NAME_MAX_LEN: usize = 64;
const SOURCE_IDENTITY_MIN_LEN: usize = 2;
const SOURCE_IDENTITY_MAX_LEN: usize = 256;

/// A rejected structured identity field or inconsistent identity composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("invalid account ID")]
    InvalidAccountId,
    #[error("invalid stable role ID")]
    InvalidStableRoleId,
    #[error("invalid IAM role name")]
    InvalidRoleName,
    #[error("invalid IAM path")]
    InvalidIamPath,
    #[error("invalid role session name")]
    InvalidRoleSessionName,
    #[error("invalid source identity")]
    InvalidSourceIdentity,
    #[error("session expiry must be later than its issue time")]
    InvalidSessionLifetime,
    #[error("the account identity and principal belong to different accounts")]
    AccountMismatch,
}

macro_rules! string_identity {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
            }
        }
    };
}

string_identity!(AwsAccountId);
string_identity!(StableRoleId);
string_identity!(RoleName);
string_identity!(IamPath);
string_identity!(RoleSessionName);
string_identity!(SourceIdentity);
string_identity!(IamRoleArn);
string_identity!(AssumedRoleSessionArn);
string_identity!(AssumedRoleId);

impl AwsAccountId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if !is_valid_aws_account_id(&value) {
            return Err(IdentityError::InvalidAccountId);
        }
        Ok(Self(value))
    }
}

impl StableRoleId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        let valid = value.len() == STABLE_ROLE_ID_LEN
            && value.starts_with(STABLE_ROLE_ID_PREFIX)
            && value[STABLE_ROLE_ID_PREFIX.len()..]
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
        if !valid {
            return Err(IdentityError::InvalidStableRoleId);
        }
        Ok(Self(value))
    }
}

fn is_iam_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'+' | b'=' | b',' | b'.' | b'@' | b'-')
}

impl RoleName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > ROLE_NAME_MAX_LEN
            || !value.bytes().all(is_iam_name_byte)
        {
            return Err(IdentityError::InvalidRoleName);
        }
        Ok(Self(value))
    }
}

impl IamPath {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        let valid = value == "/"
            || ((3..=IAM_PATH_MAX_LEN).contains(&value.len())
                && value.starts_with('/')
                && value.ends_with('/')
                && value[1..value.len() - 1]
                    .bytes()
                    .all(|byte| (0x21..=0x7e).contains(&byte)));
        if !valid {
            return Err(IdentityError::InvalidIamPath);
        }
        Ok(Self(value))
    }
}

impl RoleSessionName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if !(ROLE_SESSION_NAME_MIN_LEN..=ROLE_SESSION_NAME_MAX_LEN).contains(&value.len())
            || !value.bytes().all(is_iam_name_byte)
        {
            return Err(IdentityError::InvalidRoleSessionName);
        }
        Ok(Self(value))
    }
}

impl SourceIdentity {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if !(SOURCE_IDENTITY_MIN_LEN..=SOURCE_IDENTITY_MAX_LEN).contains(&value.chars().count())
            || !value.bytes().all(is_iam_name_byte)
        {
            return Err(IdentityError::InvalidSourceIdentity);
        }
        Ok(Self(value))
    }
}

/// Immutable identity of one incarnation of an IAM role.
///
/// The stable ID, rather than the ARN, is the issuer-liveness key. Recreating a
/// role with the same path and name therefore cannot revive an old session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IamRoleIdentity {
    account_id: AwsAccountId,
    stable_id: StableRoleId,
    name: RoleName,
    path: IamPath,
    arn: IamRoleArn,
}

impl IamRoleIdentity {
    #[must_use]
    pub fn new(
        account_id: AwsAccountId,
        stable_id: StableRoleId,
        name: RoleName,
        path: IamPath,
    ) -> Self {
        let arn = IamRoleArn(format!(
            "arn:aws:iam::{}:role{}{}",
            account_id.as_str(),
            path.as_str(),
            name.as_str()
        ));
        Self {
            account_id,
            stable_id,
            name,
            path,
            arn,
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &AwsAccountId {
        &self.account_id
    }

    #[must_use]
    pub fn stable_id(&self) -> &StableRoleId {
        &self.stable_id
    }

    #[must_use]
    pub fn name(&self) -> &RoleName {
        &self.name
    }

    #[must_use]
    pub fn path(&self) -> &IamPath {
        &self.path
    }

    #[must_use]
    pub fn arn(&self) -> &IamRoleArn {
        &self.arn
    }
}

/// Authoritative live role incarnation and its S3 account identity.
///
/// Identity-provider backends construct this record from their own state. The
/// account is stored alongside the IAM role so session authentication never
/// accepts caller-supplied canonical-user or display-name fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveRoleIdentity {
    account: AccountIdentity,
    role: IamRoleIdentity,
}

impl LiveRoleIdentity {
    pub fn new(account: AccountIdentity, role: IamRoleIdentity) -> Result<Self, IdentityError> {
        if account.account_id() != Some(role.account_id().as_str()) {
            return Err(IdentityError::AccountMismatch);
        }
        Ok(Self { account, role })
    }

    #[must_use]
    pub fn account(&self) -> &AccountIdentity {
        &self.account
    }

    #[must_use]
    pub fn role(&self) -> &IamRoleIdentity {
        &self.role
    }
}

/// Validated issue and expiry times for a temporary session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionLifetime {
    issued_at_epoch_secs: i64,
    expires_at_epoch_secs: i64,
}

impl SessionLifetime {
    pub fn new(
        issued_at_epoch_secs: i64,
        expires_at_epoch_secs: i64,
    ) -> Result<Self, IdentityError> {
        if issued_at_epoch_secs < 0 || expires_at_epoch_secs <= issued_at_epoch_secs {
            return Err(IdentityError::InvalidSessionLifetime);
        }
        Ok(Self {
            issued_at_epoch_secs,
            expires_at_epoch_secs,
        })
    }

    #[must_use]
    pub const fn issued_at_epoch_secs(self) -> i64 {
        self.issued_at_epoch_secs
    }

    #[must_use]
    pub const fn expires_at_epoch_secs(self) -> i64 {
        self.expires_at_epoch_secs
    }
}

/// Authenticated identity carried by one assumed-role session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssumedRoleSessionIdentity {
    role: IamRoleIdentity,
    session_name: RoleSessionName,
    lifetime: SessionLifetime,
    source_identity: Option<SourceIdentity>,
    session_arn: AssumedRoleSessionArn,
    assumed_role_id: AssumedRoleId,
}

impl AssumedRoleSessionIdentity {
    #[must_use]
    pub fn new(
        role: IamRoleIdentity,
        session_name: RoleSessionName,
        lifetime: SessionLifetime,
        source_identity: Option<SourceIdentity>,
    ) -> Self {
        let session_arn = AssumedRoleSessionArn(format!(
            "arn:aws:sts::{}:assumed-role/{}/{}",
            role.account_id().as_str(),
            role.name().as_str(),
            session_name.as_str()
        ));
        let assumed_role_id = AssumedRoleId(format!(
            "{}:{}",
            role.stable_id().as_str(),
            session_name.as_str()
        ));
        Self {
            role,
            session_name,
            lifetime,
            source_identity,
            session_arn,
            assumed_role_id,
        }
    }

    #[must_use]
    pub fn role(&self) -> &IamRoleIdentity {
        &self.role
    }

    #[must_use]
    pub fn session_name(&self) -> &RoleSessionName {
        &self.session_name
    }

    #[must_use]
    pub const fn lifetime(&self) -> SessionLifetime {
        self.lifetime
    }

    #[must_use]
    pub fn source_identity(&self) -> Option<&SourceIdentity> {
        self.source_identity.as_ref()
    }

    #[must_use]
    pub fn session_arn(&self) -> &AssumedRoleSessionArn {
        &self.session_arn
    }

    #[must_use]
    pub fn assumed_role_id(&self) -> &AssumedRoleId {
        &self.assumed_role_id
    }
}

/// Existing configured principal identity.
///
/// Configuration historically permits non-ARN principals, so this wrapper is
/// deliberately lossless. New IAM identity kinds use their stricter types.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ConfiguredPrincipalIdentity(String);

impl ConfiguredPrincipalIdentity {
    #[must_use]
    pub fn new(principal: impl Into<String>) -> Self {
        Self(principal.into())
    }

    #[must_use]
    pub fn principal(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ConfiguredPrincipalIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ConfiguredPrincipalIdentity")
            .field(&observability::escaped(&self.0))
            .finish()
    }
}

/// Typed authenticated request-principal kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrincipalIdentity {
    Configured(ConfiguredPrincipalIdentity),
    AssumedRoleSession(Arc<AssumedRoleSessionIdentity>),
}

/// An authenticated principal composed with its stable S3 account identity.
///
/// Anonymous requests are represented by the absence of this value rather than
/// by an optional principal inside it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthenticatedIdentity {
    account: AccountIdentity,
    principal: PrincipalIdentity,
}

impl AuthenticatedIdentity {
    #[must_use]
    pub fn configured(account: AccountIdentity, principal: ConfiguredPrincipalIdentity) -> Self {
        Self {
            account,
            principal: PrincipalIdentity::Configured(principal),
        }
    }

    pub fn assumed_role_session(
        account: AccountIdentity,
        session: AssumedRoleSessionIdentity,
    ) -> Result<Self, IdentityError> {
        Self::assumed_role_session_shared(account, Arc::new(session))
    }

    pub(crate) fn assumed_role_session_shared(
        account: AccountIdentity,
        session: Arc<AssumedRoleSessionIdentity>,
    ) -> Result<Self, IdentityError> {
        if account.account_id() != Some(session.role().account_id().as_str()) {
            return Err(IdentityError::AccountMismatch);
        }
        Ok(Self {
            account,
            principal: PrincipalIdentity::AssumedRoleSession(session),
        })
    }

    #[must_use]
    pub fn account(&self) -> &AccountIdentity {
        &self.account
    }

    #[must_use]
    pub fn kind(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Existing configured principal, when this identity came from configuration.
    #[must_use]
    pub fn configured_principal(&self) -> Option<&ConfiguredPrincipalIdentity> {
        match &self.principal {
            PrincipalIdentity::Configured(principal) => Some(principal),
            PrincipalIdentity::AssumedRoleSession(_) => None,
        }
    }

    /// IAM role ARN used as `aws:PrincipalArn` for an assumed-role request.
    ///
    /// This deliberately does not reinterpret an arbitrary configured
    /// principal as an ARN.
    #[must_use]
    pub fn role_principal_arn(&self) -> Option<&IamRoleArn> {
        match &self.principal {
            PrincipalIdentity::Configured(_) => None,
            PrincipalIdentity::AssumedRoleSession(session) => Some(session.role().arn()),
        }
    }

    /// Assumed-role session principal ARN, when this is a role session.
    #[must_use]
    pub fn session_principal_arn(&self) -> Option<&AssumedRoleSessionArn> {
        match &self.principal {
            PrincipalIdentity::Configured(_) => None,
            PrincipalIdentity::AssumedRoleSession(session) => Some(session.session_arn()),
        }
    }

    /// Stable role-session value for the `aws:userid` global condition key.
    ///
    /// Configured principals do not yet carry a stable IAM user ID.
    #[must_use]
    pub fn aws_userid(&self) -> Option<&AssumedRoleId> {
        match &self.principal {
            PrincipalIdentity::Configured(_) => None,
            PrincipalIdentity::AssumedRoleSession(session) => Some(session.assumed_role_id()),
        }
    }

    #[must_use]
    pub fn role_session(&self) -> Option<&AssumedRoleSessionIdentity> {
        match &self.principal {
            PrincipalIdentity::Configured(_) => None,
            PrincipalIdentity::AssumedRoleSession(session) => Some(session),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3_types::CanonicalUserId;

    fn role(path: &str) -> IamRoleIdentity {
        IamRoleIdentity::new(
            AwsAccountId::new("123456789012").unwrap(),
            StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            RoleName::new("test-role").unwrap(),
            IamPath::new(path).unwrap(),
        )
    }

    fn account(account_id: &str) -> AccountIdentity {
        AccountIdentity::new(
            account_id,
            CanonicalUserId::from_principal(account_id),
            "test account",
        )
    }

    #[test]
    fn stable_role_id_uses_argmin_namespace_and_exact_shape() {
        assert!(StableRoleId::new("ARGR0123456789ABCDEFGHIJ").is_ok());
        for invalid in [
            "AROA0123456789ABCDEFGHIJ",
            "ARGR0123456789ABCDEFGHI",
            "ARGR0123456789ABCDEFGHIJK",
            "ARGR0123456789abcdefGHIJ",
            "ARGR0123456789ABCDE-GHIJ",
        ] {
            assert_eq!(
                StableRoleId::new(invalid),
                Err(IdentityError::InvalidStableRoleId)
            );
        }
    }

    #[test]
    fn role_identity_derives_path_bearing_arn() {
        let path_role = role("/team/nested/");
        assert_eq!(
            path_role.arn().as_str(),
            "arn:aws:iam::123456789012:role/team/nested/test-role"
        );
        assert_eq!(
            role("/").arn().as_str(),
            "arn:aws:iam::123456789012:role/test-role"
        );
    }

    #[test]
    fn live_role_identity_requires_authoritative_account_match() {
        assert_eq!(
            LiveRoleIdentity::new(account("210987654321"), role("/")).unwrap_err(),
            IdentityError::AccountMismatch
        );

        let identity = LiveRoleIdentity::new(account("123456789012"), role("/")).unwrap();
        assert_eq!(identity.account().account_id(), Some("123456789012"));
        assert_eq!(
            identity.role().stable_id().as_str(),
            "ARGR0123456789ABCDEFGHIJ"
        );
    }

    #[test]
    fn session_identity_keeps_role_and_session_principals_distinct() {
        let session = AssumedRoleSessionIdentity::new(
            role("/team/nested/"),
            RoleSessionName::new("test-session").unwrap(),
            SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
            Some(SourceIdentity::new("source-user").unwrap()),
        );
        let identity =
            AuthenticatedIdentity::assumed_role_session(account("123456789012"), session).unwrap();

        assert!(identity.configured_principal().is_none());
        assert_eq!(
            identity.session_principal_arn().unwrap().as_str(),
            "arn:aws:sts::123456789012:assumed-role/test-role/test-session"
        );
        assert_eq!(
            identity.role_principal_arn().unwrap().as_str(),
            "arn:aws:iam::123456789012:role/team/nested/test-role"
        );
        assert_eq!(
            identity.aws_userid().unwrap().as_str(),
            "ARGR0123456789ABCDEFGHIJ:test-session"
        );
        assert_eq!(
            identity
                .role_session()
                .unwrap()
                .source_identity()
                .unwrap()
                .as_str(),
            "source-user"
        );
    }

    #[test]
    fn authenticated_identity_requires_matching_account() {
        let session = AssumedRoleSessionIdentity::new(
            role("/"),
            RoleSessionName::new("test-session").unwrap(),
            SessionLifetime::new(10, 20).unwrap(),
            None,
        );
        assert_eq!(
            AuthenticatedIdentity::assumed_role_session(account("999999999999"), session),
            Err(IdentityError::AccountMismatch)
        );
    }

    #[test]
    fn session_lifetime_rejects_empty_or_reversed_ranges() {
        assert_eq!(
            SessionLifetime::new(10, 10),
            Err(IdentityError::InvalidSessionLifetime)
        );
        assert_eq!(
            SessionLifetime::new(10, 9),
            Err(IdentityError::InvalidSessionLifetime)
        );
        assert_eq!(
            SessionLifetime::new(-1, 10),
            Err(IdentityError::InvalidSessionLifetime)
        );
    }

    #[test]
    fn role_and_session_name_validation_matches_pinned_shapes() {
        assert!(RoleName::new("azAZ09_+=,.@-").is_ok());
        assert!(RoleName::new("a".repeat(ROLE_NAME_MAX_LEN)).is_ok());
        assert_eq!(RoleName::new(""), Err(IdentityError::InvalidRoleName));
        assert_eq!(
            RoleName::new("a".repeat(ROLE_NAME_MAX_LEN + 1)),
            Err(IdentityError::InvalidRoleName)
        );
        assert_eq!(
            RoleName::new("bad/name"),
            Err(IdentityError::InvalidRoleName)
        );

        assert!(RoleSessionName::new("azAZ09_+=,.@-").is_ok());
        assert!(RoleSessionName::new("a".repeat(ROLE_SESSION_NAME_MIN_LEN)).is_ok());
        assert!(RoleSessionName::new("a".repeat(ROLE_SESSION_NAME_MAX_LEN)).is_ok());
        assert_eq!(
            RoleSessionName::new("a"),
            Err(IdentityError::InvalidRoleSessionName)
        );
        assert_eq!(
            RoleSessionName::new("a".repeat(ROLE_SESSION_NAME_MAX_LEN + 1)),
            Err(IdentityError::InvalidRoleSessionName)
        );
        assert_eq!(
            RoleSessionName::new("bad/name"),
            Err(IdentityError::InvalidRoleSessionName)
        );
    }

    #[test]
    fn source_identity_uses_scalar_length_and_pinned_pattern() {
        assert!(SourceIdentity::new("azAZ09_+=,.@-").is_ok());
        assert!(SourceIdentity::new("a".repeat(SOURCE_IDENTITY_MIN_LEN)).is_ok());
        assert!(SourceIdentity::new("a".repeat(SOURCE_IDENTITY_MAX_LEN)).is_ok());
        assert_eq!(
            SourceIdentity::new("a"),
            Err(IdentityError::InvalidSourceIdentity)
        );
        assert_eq!(
            SourceIdentity::new("a".repeat(SOURCE_IDENTITY_MAX_LEN + 1)),
            Err(IdentityError::InvalidSourceIdentity)
        );
        assert_eq!(
            SourceIdentity::new("éé"),
            Err(IdentityError::InvalidSourceIdentity)
        );
        assert_eq!(
            SourceIdentity::new("bad value"),
            Err(IdentityError::InvalidSourceIdentity)
        );
    }

    #[test]
    fn iam_path_validation_preserves_canonical_role_arn_shape() {
        assert!(IamPath::new("/").is_ok());
        assert!(IamPath::new("/team/nested/").is_ok());
        assert!(IamPath::new(format!("/{}", "a".repeat(IAM_PATH_MAX_LEN - 2)) + "/").is_ok());
        for invalid in [
            "",
            "//",
            "team/",
            "/team",
            "/white space/",
            "/control\u{7f}/",
        ] {
            assert_eq!(IamPath::new(invalid), Err(IdentityError::InvalidIamPath));
        }
        assert_eq!(
            IamPath::new(format!("/{}", "a".repeat(IAM_PATH_MAX_LEN - 1)) + "/"),
            Err(IdentityError::InvalidIamPath)
        );
    }

    #[test]
    fn configured_principal_is_lossless_and_separate_from_account() {
        let identity = AuthenticatedIdentity::configured(
            account("123456789012"),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
        );
        assert_eq!(
            identity.configured_principal().unwrap().principal(),
            "arn:aws:iam::123456789012:user/test"
        );
        assert!(identity.role_principal_arn().is_none());
        assert_eq!(identity.account().principal(), "123456789012");
        assert!(identity.session_principal_arn().is_none());
        assert!(identity.aws_userid().is_none());
    }
}
