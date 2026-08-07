//! HMAC primitives.

#[cfg(feature = "openssl")]
mod openssl_hmac {
    use core::ffi::c_void;
    use std::ptr::{self, NonNull};
    use std::sync::OnceLock;

    const OSSL_PARAM_UTF8_STRING: u32 = 4;
    const OSSL_PARAM_UNMODIFIED: usize = usize::MAX;

    struct Algorithm(NonNull<openssl_sys::EVP_MAC>);

    // SAFETY: EVP_MAC_fetch returns an immutable, reference-counted algorithm
    // descriptor. OpenSSL permits it to be shared when separate operation
    // contexts are used by each thread.
    unsafe impl Send for Algorithm {}
    // SAFETY: see the Send implementation; no operation mutates the fetched
    // algorithm descriptor.
    unsafe impl Sync for Algorithm {}

    impl Drop for Algorithm {
        fn drop(&mut self) {
            // SAFETY: self owns the reference returned by EVP_MAC_fetch.
            unsafe { openssl_sys::EVP_MAC_free(self.0.as_ptr()) };
        }
    }

    fn algorithm() -> &'static Algorithm {
        static HMAC: OnceLock<Algorithm> = OnceLock::new();
        HMAC.get_or_init(|| {
            openssl::init();
            // SAFETY: both names are static NUL-terminated strings and null
            // selects the default OpenSSL library context and property query.
            let algorithm = unsafe {
                openssl_sys::EVP_MAC_fetch(ptr::null_mut(), c"HMAC".as_ptr(), ptr::null())
            };
            Algorithm(NonNull::new(algorithm).unwrap_or_else(|| {
                panic!(
                    "OpenSSL EVP_MAC HMAC fetch failed: {}",
                    openssl::error::ErrorStack::get()
                )
            }))
        })
    }

    fn digest_parameter() -> openssl_sys::OSSL_PARAM {
        let digest_name = c"SHA256";
        openssl_sys::OSSL_PARAM {
            key: c"digest".as_ptr(),
            data_type: OSSL_PARAM_UTF8_STRING,
            // OpenSSL's input parameter ABI uses a mutable pointer even
            // though providers only read a set parameter's value.
            data: digest_name.as_ptr().cast_mut().cast::<c_void>(),
            data_size: digest_name.to_bytes().len(),
            return_size: OSSL_PARAM_UNMODIFIED,
        }
    }

    pub(super) struct Context(NonNull<openssl_sys::EVP_MAC_CTX>);

    impl Context {
        pub(super) fn new(key: &[u8]) -> Self {
            // SAFETY: algorithm points to a process-lifetime EVP_MAC fetched
            // from OpenSSL. A distinct context is created for this operation.
            let context = unsafe { openssl_sys::EVP_MAC_CTX_new(algorithm().0.as_ptr()) };
            let context = Self(NonNull::new(context).unwrap_or_else(|| {
                panic!(
                    "OpenSSL EVP_MAC context creation failed: {}",
                    openssl::error::ErrorStack::get()
                )
            }));

            let parameters = [
                digest_parameter(),
                // SAFETY: this constructs the required terminating parameter.
                unsafe { openssl_sys::OSSL_PARAM_construct_end() },
            ];
            // EVP_MAC accepts an empty HMAC key, but keep a readable pointer
            // for providers which inspect it even when the length is zero.
            let empty_key = [0_u8];
            let key_pointer = if key.is_empty() {
                empty_key.as_ptr()
            } else {
                key.as_ptr()
            };
            // SAFETY: context is valid, key_pointer is readable for key.len()
            // bytes, and parameters remains alive for the duration of init.
            let result = unsafe {
                openssl_sys::EVP_MAC_init(
                    context.0.as_ptr(),
                    key_pointer,
                    key.len(),
                    parameters.as_ptr(),
                )
            };
            if result != 1 {
                let errors = openssl::error::ErrorStack::get();
                drop(context);
                panic!("OpenSSL EVP_MAC HMAC initialization failed: {errors}");
            }
            context
        }

        pub(super) fn duplicate(&self) -> Self {
            // SAFETY: self owns a fully initialized HMAC context. Duplication
            // creates an independent context with the same pre-message state.
            let context = unsafe { openssl_sys::EVP_MAC_CTX_dup(self.0.as_ptr()) };
            Self(NonNull::new(context).unwrap_or_else(|| {
                panic!(
                    "OpenSSL EVP_MAC HMAC context duplication failed: {}",
                    openssl::error::ErrorStack::get()
                )
            }))
        }

        pub(super) fn update(&mut self, data: &[u8]) {
            // SAFETY: the context is initialized and data is readable for its
            // complete length.
            let result =
                unsafe { openssl_sys::EVP_MAC_update(self.0.as_ptr(), data.as_ptr(), data.len()) };
            if result != 1 {
                panic!(
                    "OpenSSL EVP_MAC HMAC update failed: {}",
                    openssl::error::ErrorStack::get()
                );
            }
        }

        pub(super) fn finalize(self) -> [u8; 32] {
            let mut output = [0_u8; 32];
            let mut written = 0_usize;
            // SAFETY: the context is initialized, output is writable for its
            // complete length, and written points to initialized storage.
            let result = unsafe {
                openssl_sys::EVP_MAC_final(
                    self.0.as_ptr(),
                    output.as_mut_ptr(),
                    &raw mut written,
                    output.len(),
                )
            };
            if result != 1 {
                panic!(
                    "OpenSSL EVP_MAC HMAC finalization failed: {}",
                    openssl::error::ErrorStack::get()
                );
            }
            assert_eq!(written, output.len(), "OpenSSL returned a truncated HMAC");
            output
        }
    }

    impl Drop for Context {
        fn drop(&mut self) {
            // SAFETY: self exclusively owns this EVP_MAC_CTX.
            unsafe { openssl_sys::EVP_MAC_CTX_free(self.0.as_ptr()) };
        }
    }
}

/// A reusable HMAC-SHA256 key.
#[cfg(feature = "openssl")]
pub struct Sha256Key(openssl_hmac::Context);

/// A reusable HMAC-SHA256 key.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha256Key(ring::hmac::Key);

impl Sha256Key {
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        #[cfg(feature = "openssl")]
        {
            Self(openssl_hmac::Context::new(key))
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
            Sha256Context(self.0.duplicate())
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            Sha256Context(ring::hmac::Context::with_key(&self.0))
        }
    }
}

/// Incremental HMAC-SHA256 computation.
#[cfg(feature = "openssl")]
pub struct Sha256Context(openssl_hmac::Context);

/// Incremental HMAC-SHA256 computation.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Sha256Context(ring::hmac::Context);

impl Sha256Context {
    pub fn update(&mut self, data: &[u8]) {
        #[cfg(feature = "openssl")]
        self.0.update(data);
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        self.0.update(data);
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        #[cfg(feature = "openssl")]
        return self.0.finalize();
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
    #[cfg(feature = "openssl")]
    {
        let mut context = openssl_hmac::Context::new(key);
        context.update(data);
        context.finalize()
    }
    #[cfg(all(feature = "ring", not(feature = "openssl")))]
    {
        Sha256Key::new(key).sign(data)
    }
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

    #[test]
    fn empty_key_and_message_oneshot_matches_reusable_key() {
        let reusable = Sha256Key::new(b"");
        assert_eq!(sha256(b"", b""), reusable.sign(b""));
    }
}
