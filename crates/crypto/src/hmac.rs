//! HMAC primitives.

/// A reusable HMAC-SHA256 key.
#[cfg(feature = "openssl")]
pub struct Sha256Key(openssl::pkey::PKey<openssl::pkey::Private>);

/// A reusable HMAC-SHA256 key.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha256Key(ring::hmac::Key);

impl Sha256Key {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        #[cfg(feature = "openssl")]
        {
            // EVP_PKEY rejects a zero-length HMAC key. HMAC pads every key
            // shorter than the SHA-256 block size with zeroes, so a single
            // zero byte is exactly equivalent to the empty key.
            let openssl_key = if key.is_empty() { &[0][..] } else { key };
            Self(openssl::pkey::PKey::hmac(openssl_key).expect("OpenSSL HMAC key creation failed"))
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            Self(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key))
        }
    }

    #[must_use]
    pub fn sign(&self, data: &[u8]) -> [u8; 32] {
        #[cfg(feature = "openssl")]
        {
            let mut context = self.context();
            context.update(data);
            context.finalize()
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            ring::hmac::sign(&self.0, data)
                .as_ref()
                .try_into()
                .expect("HMAC-SHA256 produces a 32-byte tag")
        }
    }

    #[must_use]
    pub fn verify(&self, data: &[u8], tag: &[u8]) -> bool {
        #[cfg(feature = "openssl")]
        {
            tag.len() == 32 && openssl::memcmp::eq(&self.sign(data), tag)
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            ring::hmac::verify(&self.0, data, tag).is_ok()
        }
    }

    #[must_use]
    pub fn context(&self) -> Sha256Context {
        #[cfg(feature = "openssl")]
        {
            let mut context =
                openssl::md_ctx::MdCtx::new().expect("OpenSSL HMAC-SHA256 context creation failed");
            context
                .digest_sign_init(Some(openssl::md::Md::sha256()), &self.0)
                .expect("OpenSSL HMAC-SHA256 initialization failed");
            Sha256Context(context)
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            Sha256Context(ring::hmac::Context::with_key(&self.0))
        }
    }
}

/// Incremental HMAC-SHA256 computation.
#[cfg(feature = "openssl")]
pub struct Sha256Context(openssl::md_ctx::MdCtx);

/// Incremental HMAC-SHA256 computation.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha256Context(ring::hmac::Context);

impl Sha256Context {
    pub fn update(&mut self, data: &[u8]) {
        #[cfg(feature = "openssl")]
        self.0
            .digest_sign_update(data)
            .expect("OpenSSL HMAC-SHA256 update failed");
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        #[cfg(feature = "openssl")]
        {
            let mut context = self.0;
            let mut output = [0; 32];
            let written = context
                .digest_sign_final(Some(&mut output))
                .expect("OpenSSL HMAC-SHA256 finalization failed");
            assert_eq!(written, output.len(), "OpenSSL returned a truncated HMAC");
            output
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            self.0
                .sign()
                .as_ref()
                .try_into()
                .expect("HMAC-SHA256 produces a 32-byte tag")
        }
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
