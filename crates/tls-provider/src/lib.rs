//! Compile-time rustls cryptography-provider selection.

use std::fmt;
use std::sync::Arc;

use rustls::crypto::CryptoProvider;

/// Construct the cryptography provider selected for this build.
#[must_use]
pub fn build_provider() -> CryptoProvider {
    #[cfg(feature = "openssl")]
    {
        rustls_openssl::default_provider()
    }
    #[cfg(not(feature = "openssl"))]
    {
        rustls::crypto::ring::default_provider()
    }
}

/// Construct the selected provider behind the shared pointer rustls builders
/// require.
#[must_use]
pub fn configured_provider() -> Arc<CryptoProvider> {
    Arc::new(build_provider())
}

/// Install the provider selected for this build as rustls' process default.
pub fn install_default() -> Result<(), TlsCryptoProviderInstallError> {
    build_provider()
        .install_default()
        .map_err(|_| TlsCryptoProviderInstallError)
}

/// Return the stable name of the provider selected for this build.
#[must_use]
pub const fn provider_name() -> &'static str {
    #[cfg(feature = "openssl")]
    {
        "openssl"
    }
    #[cfg(not(feature = "openssl"))]
    {
        "ring"
    }
}

/// Failure to install the selected provider because one was already installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TlsCryptoProviderInstallError;

impl fmt::Display for TlsCryptoProviderInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a rustls cryptography provider is already installed")
    }
}

impl std::error::Error for TlsCryptoProviderInstallError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_provider_can_be_built() {
        assert!(!build_provider().cipher_suites.is_empty());
    }

    #[cfg(not(feature = "openssl"))]
    #[test]
    fn default_build_selects_ring() {
        assert_eq!(provider_name(), "ring");
    }

    #[cfg(feature = "openssl")]
    #[test]
    fn openssl_build_selects_openssl() {
        assert_eq!(provider_name(), "openssl");
    }
}
