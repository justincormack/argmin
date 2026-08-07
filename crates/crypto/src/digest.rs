//! Cryptographic message digests other than the target-optimized SHA-256 path.

/// Incremental SHA-1 hasher for legacy S3 checksum compatibility.
pub struct Sha1(ring::digest::Context);

impl Sha1 {
    #[must_use]
    pub fn new() -> Self {
        Self(ring::digest::Context::new(
            &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
        ))
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 20] {
        self.0
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA-1 produces a 20-byte digest")
    }
}

impl Default for Sha1 {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental SHA-512 hasher.
pub struct Sha512(ring::digest::Context);

impl Sha512 {
    #[must_use]
    pub fn new() -> Self {
        Self(ring::digest::Context::new(&ring::digest::SHA512))
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 64] {
        self.0
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA-512 produces a 64-byte digest")
    }
}

impl Default for Sha512 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_vectors() {
        assert_eq!(
            Sha1::new().finalize(),
            [
                0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60,
                0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
            ]
        );
        assert_eq!(
            Sha512::new().finalize(),
            [
                0xcf, 0x83, 0xe1, 0x35, 0x7e, 0xef, 0xb8, 0xbd, 0xf1, 0x54, 0x28, 0x50, 0xd6, 0x6d,
                0x80, 0x07, 0xd6, 0x20, 0xe4, 0x05, 0x0b, 0x57, 0x15, 0xdc, 0x83, 0xf4, 0xa9, 0x21,
                0xd3, 0x6c, 0xe9, 0xce, 0x47, 0xd0, 0xd1, 0x3c, 0x5d, 0x85, 0xf2, 0xb0, 0xff, 0x83,
                0x18, 0xd2, 0x87, 0x7e, 0xec, 0x2f, 0x63, 0xb9, 0x31, 0xbd, 0x47, 0x41, 0x7a, 0x81,
                0xa5, 0x38, 0x32, 0x7a, 0xf9, 0x27, 0xda, 0x3e,
            ]
        );
    }
}
