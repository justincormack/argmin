// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! HKDF key derivation.

use crate::CryptoError;

const SHA256_LEN: usize = 32;
const SHA512_LEN: usize = 64;

/// Derive `N` bytes using HKDF-SHA256 and one info value.
pub fn sha256<const N: usize>(
    salt: &[u8],
    input_key_material: &[u8],
    info: &[u8],
) -> Result<[u8; N], CryptoError> {
    if N > 255 * SHA256_LEN {
        return Err(CryptoError);
    }

    let mut result = [0; N];
    if N == 0 {
        return Ok(result);
    }

    #[cfg(feature = "openssl")]
    {
        use openssl::pkey::Id;
        use openssl::pkey_ctx::PkeyCtx;

        let mut context = PkeyCtx::new_id(Id::HKDF).map_err(|_| CryptoError)?;
        context.derive_init().map_err(|_| CryptoError)?;
        context
            .set_hkdf_md(openssl::md::Md::sha256())
            .map_err(|_| CryptoError)?;
        context
            .set_hkdf_key(input_key_material)
            .map_err(|_| CryptoError)?;
        context.set_hkdf_salt(salt).map_err(|_| CryptoError)?;
        context.add_hkdf_info(info).map_err(|_| CryptoError)?;
        let written = context.derive(Some(&mut result)).map_err(|_| CryptoError)?;
        if written != N {
            return Err(CryptoError);
        }
    }

    #[cfg(all(feature = "ring", not(feature = "openssl")))]
    {
        struct OutputLength(usize);

        impl ring::hkdf::KeyType for OutputLength {
            fn len(&self) -> usize {
                self.0
            }
        }

        let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, salt);
        let pseudorandom_key = salt.extract(input_key_material);
        let info = [info];
        pseudorandom_key
            .expand(&info, OutputLength(N))
            .map_err(|_| CryptoError)?
            .fill(&mut result)
            .map_err(|_| CryptoError)?;
    }
    Ok(result)
}

/// Derive `N` bytes using HKDF-Expand-SHA512 and one info value.
///
/// Unlike [`sha256`], this accepts an already uniformly random pseudorandom
/// key and deliberately does not perform HKDF-Extract.
pub fn expand_sha512<const N: usize>(
    pseudorandom_key: &[u8],
    info: &[u8],
) -> Result<[u8; N], CryptoError> {
    if N > 255 * SHA512_LEN {
        return Err(CryptoError);
    }

    let mut result = [0; N];
    if N == 0 {
        return Ok(result);
    }

    #[cfg(feature = "openssl")]
    {
        use openssl::md::Md;
        use openssl::pkey::Id;
        use openssl::pkey_ctx::{HkdfMode, PkeyCtx};

        let mut context = PkeyCtx::new_id(Id::HKDF).map_err(|_| CryptoError)?;
        context.derive_init().map_err(|_| CryptoError)?;
        context
            .set_hkdf_mode(HkdfMode::EXPAND_ONLY)
            .map_err(|_| CryptoError)?;
        context.set_hkdf_md(Md::sha512()).map_err(|_| CryptoError)?;
        context
            .set_hkdf_key(pseudorandom_key)
            .map_err(|_| CryptoError)?;
        context.add_hkdf_info(info).map_err(|_| CryptoError)?;
        let written = context.derive(Some(&mut result)).map_err(|_| CryptoError)?;
        if written != N {
            return Err(CryptoError);
        }
    }

    #[cfg(all(feature = "ring", not(feature = "openssl")))]
    {
        struct OutputLength(usize);

        impl ring::hkdf::KeyType for OutputLength {
            fn len(&self) -> usize {
                self.0
            }
        }

        let pseudorandom_key =
            ring::hkdf::Prk::new_less_safe(ring::hkdf::HKDF_SHA512, pseudorandom_key);
        let info = [info];
        pseudorandom_key
            .expand(&info, OutputLength(N))
            .map_err(|_| CryptoError)?
            .fill(&mut result)
            .map_err(|_| CryptoError)?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_5869_case_one() {
        let output = sha256::<42>(
            &[
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
            ],
            &[0x0b; 22],
            &[0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9],
        )
        .unwrap();
        assert_eq!(
            output,
            [
                0x3c, 0xb2, 0x5f, 0x25, 0xfa, 0xac, 0xd5, 0x7a, 0x90, 0x43, 0x4f, 0x64, 0xd0, 0x36,
                0x2f, 0x2a, 0x2d, 0x2d, 0x0a, 0x90, 0xcf, 0x1a, 0x5a, 0x4c, 0x5d, 0xb0, 0x2d, 0x56,
                0xec, 0xc4, 0xc5, 0xbf, 0x34, 0x00, 0x72, 0x08, 0xd5, 0xb8, 0x87, 0x18, 0x58, 0x65,
            ]
        );
    }

    #[test]
    fn sha512_expand_only_vector() {
        // Keeping the PRK explicit pins expand-only rather than
        // extract-and-expand semantics.
        let pseudorandom_key = [0x0b; 64];
        let info = [0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9];
        let output = expand_sha512::<42>(&pseudorandom_key, &info).unwrap();
        assert_eq!(
            output,
            [
                0xaf, 0x07, 0xe9, 0xff, 0x7d, 0x30, 0x0e, 0xea, 0xa3, 0x25, 0x85, 0xf2, 0x31, 0xcc,
                0xe6, 0x8b, 0xf7, 0x82, 0xbf, 0xce, 0x25, 0xf4, 0x5c, 0xd2, 0xc4, 0x85, 0x74, 0x44,
                0x76, 0x08, 0x93, 0x81, 0x3d, 0x94, 0xf9, 0x3f, 0x96, 0x6a, 0xf1, 0xc2, 0x45, 0xf9,
            ]
        );
    }
}
