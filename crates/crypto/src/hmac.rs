//! HMAC primitives.

/// A reusable HMAC-SHA256 key.
pub struct Sha256Key(ring::hmac::Key);

impl Sha256Key {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        Self(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key))
    }

    #[must_use]
    pub fn sign(&self, data: &[u8]) -> [u8; 32] {
        ring::hmac::sign(&self.0, data)
            .as_ref()
            .try_into()
            .expect("HMAC-SHA256 produces a 32-byte tag")
    }

    #[must_use]
    pub fn verify(&self, data: &[u8], tag: &[u8]) -> bool {
        ring::hmac::verify(&self.0, data, tag).is_ok()
    }

    #[must_use]
    pub fn context(&self) -> Sha256Context {
        Sha256Context(ring::hmac::Context::with_key(&self.0))
    }
}

/// Incremental HMAC-SHA256 computation.
pub struct Sha256Context(ring::hmac::Context);

impl Sha256Context {
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        self.0
            .sign()
            .as_ref()
            .try_into()
            .expect("HMAC-SHA256 produces a 32-byte tag")
    }
}

#[must_use]
pub fn sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    Sha256Key::new(key).sign(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_4231_vector() {
        let key = [0x0b; 20];
        let expected = [
            0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b,
            0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c,
            0x2e, 0x32, 0xcf, 0xf7,
        ];
        let hmac_key = Sha256Key::new(&key);
        assert_eq!(hmac_key.sign(b"Hi There"), expected);
        assert!(hmac_key.verify(b"Hi There", &expected));
        assert!(!hmac_key.verify(b"Hi there", &expected));
    }
}
