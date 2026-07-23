#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_string_new,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::redundant_closure_for_method_calls,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unreadable_literal
)]

pub mod authorization;
pub mod bucket_policy;
pub mod canonical;
pub mod credential;
mod encoding;
pub mod error;
pub mod iam_policy;
pub mod identity;
mod policy;
pub mod post;
pub mod provider;
pub mod request;
mod session_token;
pub mod sigv4;

pub use s3_types::AccountIdentity;

use subtle::ConstantTimeEq;

pub(crate) const MAX_ACCESS_KEY_ID_LEN: usize = 128;
pub(crate) const MAX_AUTHORIZATION_HEADER_LEN: usize = 8192;
pub(crate) const MAX_PRESIGNED_QUERY_LEN: usize = 16384;
pub(crate) const MAX_CREDENTIAL_LEN: usize = 2048;
pub(crate) const MAX_SIGNED_HEADERS_LEN: usize = 2048;
pub(crate) const MAX_SIGNED_HEADER_COUNT: usize = 128;
pub(crate) const SIGNATURE_HEX_LEN: usize = 64;
pub const SIGV4_CLOCK_SKEW_SECS: u64 = 15 * 60;

/// Constant-time byte slice comparison to prevent timing attacks.
///
/// Returns `true` if both slices have equal length and contents.
/// Runs in time proportional to the length of the slices, regardless
/// of where (or whether) they differ.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

pub(crate) fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub use authorization::{
    AuthorizationEvaluationError, AuthorizationRecordError, AuthorizationRecordStore,
    ConfiguredPrincipalAuthorizationKey, ConfiguredPrincipalAuthorizationRecord,
    RoleAuthorizationRecord, RoleMaximumSessionDuration, RoleRecordTimestamps,
    SessionPolicyRestriction,
};
pub use bucket_policy::{
    parse_bucket_policy, BucketPolicy, BucketPolicyError, PolicyAction, PolicyConditionClause,
    PolicyEffect, PolicyEvaluation, PolicyPrincipal, PolicyRequest, PolicyStatement, PolicyTag,
    PolicyVersion,
};
pub use canonical::parse_amz_date;
pub use credential::{
    generate_session_credential_material, is_reserved_session_access_key_id,
    AuthenticatedCredential, AuthorizationProfile, CredentialScope, CredentialStore,
    CredentialStoreError, DecodedSessionCredential, GeneratedSessionCredentialMaterial, SecretKey,
    SessionCredentialError, SessionCredentialGenerationError, StoredCredential,
    SESSION_ACCESS_KEY_ID_LEN, SESSION_ACCESS_KEY_ID_PREFIX, SESSION_SECRET_ACCESS_KEY_LEN,
};
pub use error::{AuthError, SignatureMismatchDiagnostics};
pub use iam_policy::{
    IamActionPattern, IamPolicyError, IamResourcePattern, IdentityPolicy, IdentityPolicyRequest,
    IdentityPolicyStatement, InlineIdentityPolicy, InlinePolicyName, RoleTrustPolicy,
    RoleTrustPolicyStatement, RoleTrustPrincipal, S3IdentityPolicyResource, SessionPolicy,
};
pub use identity::{
    AssumedRoleId, AssumedRoleSessionArn, AssumedRoleSessionIdentity, AuthenticatedIdentity,
    AwsAccountId, ConfiguredPrincipalIdentity, IamPath, IamRoleArn, IamRoleIdentity, IamUserArn,
    IdentityError, LiveRoleIdentity, PrincipalIdentity, RoleName, RoleSessionName,
    RoleSessionNameError, SessionAuthorizationContext, SessionLifetime, SourceIdentity,
    StableRoleId,
};
pub use post::{
    authenticate_post_sigv4, prepare_post_policy, validate_post_policy,
    validate_prepared_post_policy_size, ExpectedCredentialScope, PostPolicyConditionExpression,
    PostPolicyError, PostSigV4Request, PreparedPostPolicy,
};
pub use provider::{
    IdentityProvider, IdentityProviderBackend, IdentityProviderError,
    ResolvedConfiguredPrincipalAuthorization, ResolvedPrincipalAuthorization,
    ResolvedRoleAuthorization, ResolvedRoleIdentity, RoleIdentityStore, RoleIdentityStoreError,
    SessionCredentialAuthenticationError,
};
pub use request::{
    authenticate_request, AuthContext, AuthMode, ConfiguredOrAnonymousAuth, ExpectedSigningRegion,
    HeaderSource, SigningService, StreamingSigningContext, UnsupportedAuthorizationIdentity,
};
pub use session_token::{
    SessionTokenKeyRingInitError, SessionTokenKeyRingStatus, SessionTokenOpenError,
    SessionTokenSealError, MAX_DECODED_SESSION_TOKEN_FRAME_LEN, MAX_ENCODED_SESSION_TOKEN_LEN,
    MAX_ISSUED_V1_TOKEN_LEN, SESSION_TOKEN_V1_PREFIX,
};
pub use sigv4::{parse_auth_header, SigV4Auth};

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_checks_length_and_contents() {
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"same", b"diff"));
        assert!(!constant_time_eq(b"same", b"same-longer"));
    }
}
