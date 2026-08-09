// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Cryptographically secure random bytes.

#[cfg(all(feature = "ring", not(feature = "openssl")))]
use ring::rand::SecureRandom as _;

use crate::CryptoError;

/// Fill `output` from the operating system's secure random source.
pub fn fill(output: &mut [u8]) -> Result<(), CryptoError> {
    #[cfg(feature = "openssl")]
    {
        openssl::rand::rand_bytes(output).map_err(|_| CryptoError)
    }
    #[cfg(all(feature = "ring", not(feature = "openssl")))]
    {
        ring::rand::SystemRandom::new()
            .fill(output)
            .map_err(|_| CryptoError)
    }
}
