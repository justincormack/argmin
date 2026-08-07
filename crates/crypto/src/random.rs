//! Cryptographically secure random bytes.

use ring::rand::SecureRandom as _;

use crate::CryptoError;

/// Fill `output` from the operating system's secure random source.
pub fn fill(output: &mut [u8]) -> Result<(), CryptoError> {
    ring::rand::SystemRandom::new()
        .fill(output)
        .map_err(|_| CryptoError)
}
