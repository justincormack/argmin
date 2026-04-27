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

pub mod bucket_policy;
pub mod canonical;
pub mod credential;
pub mod error;
pub mod post;
pub mod request;
pub mod sigv4;

pub use s3_types::AccountIdentity;

use subtle::ConstantTimeEq;

pub(crate) const MAX_ACCESS_KEY_ID_LEN: usize = 128;
pub(crate) const MAX_SESSION_TOKEN_LEN: usize = 4096;
pub(crate) const MAX_AUTHORIZATION_HEADER_LEN: usize = 8192;
pub(crate) const MAX_PRESIGNED_QUERY_LEN: usize = 16384;
pub(crate) const MAX_CREDENTIAL_LEN: usize = 2048;
pub(crate) const MAX_SIGNED_HEADERS_LEN: usize = 2048;
pub(crate) const MAX_SIGNED_HEADER_COUNT: usize = 128;
pub(crate) const SIGNATURE_HEX_LEN: usize = 64;

/// Constant-time byte slice comparison to prevent timing attacks.
///
/// Returns `true` if both slices have equal length and contents.
/// Runs in time proportional to the length of the slices, regardless
/// of where (or whether) they differ.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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

pub use bucket_policy::{
    parse_bucket_policy, BucketPolicy, BucketPolicyError, PolicyAction, PolicyConditionClause,
    PolicyEffect, PolicyEvaluation, PolicyPrincipal, PolicyRequest, PolicyStatement, PolicyTag,
    PolicyVersion,
};
pub use canonical::parse_amz_date;
pub use credential::{
    AuthorizationProfile, CredentialRecord, CredentialScope, CredentialStore, SecretKey,
};
pub use error::AuthError;
pub use post::{
    authenticate_post_sigv4, prepare_post_policy, validate_post_policy,
    validate_prepared_post_policy_size, PostPolicyError, PreparedPostPolicy,
};
pub use request::{
    authenticate_request, AuthContext, AuthMode, HeaderSource, StreamingSigningContext,
};
pub use sigv4::{parse_auth_header, verify_request, SigV4Auth};
