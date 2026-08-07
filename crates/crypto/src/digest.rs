//! Incremental message digests.

#[cfg(feature = "openssl")]
fn openssl_hasher(digest: openssl::hash::MessageDigest) -> openssl::hash::Hasher {
    openssl::hash::Hasher::new(digest).expect("OpenSSL digest context creation failed")
}

#[cfg(feature = "openssl")]
macro_rules! openssl_digest {
    ($name:ident, $algorithm:ident, $length:literal, $description:literal) => {
        #[doc = $description]
        pub struct $name(openssl::hash::Hasher);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(openssl_hasher(openssl::hash::MessageDigest::$algorithm()))
            }

            pub fn update(&mut self, data: &[u8]) {
                self.0.update(data).expect("OpenSSL digest update failed");
            }

            #[must_use]
            pub fn finalize(mut self) -> [u8; $length] {
                self.0
                    .finish()
                    .expect("OpenSSL digest finalization failed")
                    .as_ref()
                    .try_into()
                    .expect("digest output length matches its algorithm")
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

#[cfg(feature = "openssl")]
openssl_digest!(Md5, md5, 16, "Incremental MD5 hasher for S3 compatibility.");
#[cfg(feature = "openssl")]
openssl_digest!(
    Sha1,
    sha1,
    20,
    "Incremental SHA-1 hasher for legacy S3 checksum compatibility."
);
#[cfg(feature = "openssl")]
openssl_digest!(Sha512, sha512, 64, "Incremental SHA-512 hasher.");

/// Incremental MD5 hasher for S3 compatibility.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Md5(md5_legacy::Md5);

#[cfg(all(feature = "ring", not(feature = "openssl")))]
impl Md5 {
    #[must_use]
    pub fn new() -> Self {
        use md5_legacy::Digest as _;
        Self(md5_legacy::Md5::new())
    }

    pub fn update(&mut self, data: &[u8]) {
        use md5_legacy::Digest as _;
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 16] {
        use md5_legacy::Digest as _;
        self.0.finalize().into()
    }
}

#[cfg(all(feature = "ring", not(feature = "openssl")))]
impl Default for Md5 {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental SHA-1 hasher for legacy S3 checksum compatibility.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha1(ring::digest::Context);

#[cfg(all(feature = "ring", not(feature = "openssl")))]
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

#[cfg(all(feature = "ring", not(feature = "openssl")))]
impl Default for Sha1 {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental SHA-512 hasher.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha512(ring::digest::Context);

#[cfg(all(feature = "ring", not(feature = "openssl")))]
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

#[cfg(all(feature = "ring", not(feature = "openssl")))]
impl Default for Sha512 {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn md5(data: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_vectors() {
        assert_eq!(
            Md5::new().finalize(),
            [
                0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
                0x42, 0x7e,
            ]
        );
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
