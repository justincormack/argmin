//! HKDF key derivation.

use crate::CryptoError;

struct OutputLength<const N: usize>;

impl<const N: usize> ring::hkdf::KeyType for OutputLength<N> {
    fn len(&self) -> usize {
        N
    }
}

/// Derive `N` bytes using HKDF-SHA256 and one info value.
pub fn sha256<const N: usize>(
    salt: &[u8],
    input_key_material: &[u8],
    info: &[u8],
) -> Result<[u8; N], CryptoError> {
    let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, salt);
    let pseudorandom_key = salt.extract(input_key_material);
    let info = [info];
    let output = pseudorandom_key
        .expand(&info, OutputLength::<N>)
        .map_err(|_| CryptoError)?;
    let mut result = [0; N];
    output.fill(&mut result).map_err(|_| CryptoError)?;
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
}
