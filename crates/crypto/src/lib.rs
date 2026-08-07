//! Backend-neutral cryptographic primitives used by Argmin.
//!
//! Production crates should use this crate rather than depending on a concrete
//! cryptographic implementation. Ring is the default provider; the `openssl`
//! feature selects dynamically linked OpenSSL for every primitive.

#[cfg(not(any(feature = "ring", feature = "openssl")))]
compile_error!("argmin-crypto requires either the `ring` or `openssl` feature");

pub mod aead;
pub mod digest;
pub mod hkdf;
pub mod hmac;
pub mod random;
pub mod sha256;

/// Return the cryptography provider selected for this build.
///
/// OpenSSL takes precedence when both features are enabled so that workspace
/// all-feature checks exercise the alternative provider.
#[must_use]
pub const fn provider_name() -> &'static str {
    #[cfg(feature = "openssl")]
    {
        "openssl"
    }
    #[cfg(all(feature = "ring", not(feature = "openssl")))]
    {
        "ring"
    }
}

/// Failure reported by a cryptographic backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CryptoError;

impl std::fmt::Display for CryptoError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("cryptographic operation failed")
    }
}

impl std::error::Error for CryptoError {}
