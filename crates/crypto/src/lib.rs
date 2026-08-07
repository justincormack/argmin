//! Backend-neutral cryptographic primitives used by Argmin.
//!
//! Production crates should use this crate rather than depending on a concrete
//! cryptographic implementation. TLS provider selection remains in the
//! separate `tls-provider` crate because the local primitive and TLS backends
//! are intentionally independent.

pub mod aead;
pub mod digest;
pub mod hkdf;
pub mod hmac;
pub mod random;
pub mod sha256;

/// Failure reported by a cryptographic backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CryptoError;

impl std::fmt::Display for CryptoError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("cryptographic operation failed")
    }
}

impl std::error::Error for CryptoError {}
