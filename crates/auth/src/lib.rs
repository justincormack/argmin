pub mod canonical;
pub mod credential;
pub mod error;
pub mod post;
pub mod request;
pub mod sigv4;

/// Constant-time byte slice comparison to prevent timing attacks.
///
/// Returns `true` if both slices have equal length and contents.
/// Runs in time proportional to the length of the slices, regardless
/// of where (or whether) they differ.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    // XOR each byte pair; accumulate into `diff`. If any byte differs,
    // diff will be non-zero, but the loop always runs to completion.
    let diff = a
        .iter()
        .zip(b.iter())
        .fold(0u8, |acc, (&x, &y)| acc | (x ^ y));
    diff == 0
}

pub use canonical::parse_amz_date;
pub use credential::{CredentialRecord, CredentialScope, CredentialStore, SecretKey};
pub use error::AuthError;
pub use post::{authenticate_post, authenticate_post_sigv4, validate_post_policy, PostPolicyError};
pub use request::{authenticate_request, AuthContext, AuthMode, StreamingSigningContext};
pub use sigv4::{parse_auth_header, verify_request, SigV4Auth};
