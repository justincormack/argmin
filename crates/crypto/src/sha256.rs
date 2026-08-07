//! SHA-256 hashing.

const DIGEST_LEN: usize = 32;

/// Incremental SHA-256 hasher.
#[cfg(feature = "openssl")]
pub struct Sha256(openssl::hash::Hasher);

/// Incremental SHA-256 hasher.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha256(ring::digest::Context);

impl Sha256 {
    #[must_use]
    pub fn new() -> Self {
        #[cfg(feature = "openssl")]
        {
            Self(
                openssl::hash::Hasher::new(openssl::hash::MessageDigest::sha256())
                    .expect("OpenSSL SHA-256 context creation failed"),
            )
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            Self(ring::digest::Context::new(&ring::digest::SHA256))
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        #[cfg(feature = "openssl")]
        self.0.update(data).expect("OpenSSL SHA-256 update failed");
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; DIGEST_LEN] {
        #[cfg(feature = "openssl")]
        let digest = {
            let mut hasher = self.0;
            hasher
                .finish()
                .expect("OpenSSL SHA-256 finalization failed")
        };
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        let digest = self.0.finish();
        digest
            .as_ref()
            .try_into()
            .expect("SHA-256 produces a 32-byte digest")
    }
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn digest(data: &[u8]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize()
}

/// Return the cryptography provider selected for this build.
#[must_use]
pub const fn backend_name() -> &'static str {
    crate::provider_name()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_vectors() {
        assert_eq!(
            digest(b""),
            [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55,
            ]
        );
        assert_eq!(
            digest(b"abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }

    #[test]
    fn incremental_boundaries_match_oneshot() {
        let data = (0_u32..4097)
            .map(|index| (index.wrapping_mul(29) ^ (index >> 3)) as u8)
            .collect::<Vec<_>>();
        let expected = digest(&data);
        for chunk_size in [1, 3, 7, 31, 64, 65, 255, 1024, data.len()] {
            let mut hasher = Sha256::new();
            for chunk in data.chunks(chunk_size) {
                hasher.update(chunk);
            }
            assert_eq!(hasher.finalize(), expected, "chunk size {chunk_size}");
        }
    }
}
