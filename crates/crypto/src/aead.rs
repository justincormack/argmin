// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated encryption primitives.

use crate::CryptoError;

pub const AES_256_GCM_KEY_LEN: usize = 32;
pub const AES_GCM_NONCE_LEN: usize = 12;
pub const AES_GCM_TAG_LEN: usize = 16;

/// An AES-256-GCM key.
#[cfg(feature = "openssl")]
pub struct Aes256GcmKey([u8; AES_256_GCM_KEY_LEN]);

/// An AES-256-GCM key.
#[cfg(all(feature = "ring", not(feature = "openssl")))]
pub struct Aes256GcmKey(ring::aead::LessSafeKey);

impl Aes256GcmKey {
    pub fn new(key_material: &[u8; AES_256_GCM_KEY_LEN]) -> Result<Self, CryptoError> {
        #[cfg(feature = "openssl")]
        {
            Ok(Self(*key_material))
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            let key = ring::aead::UnboundKey::new(&ring::aead::AES_256_GCM, key_material)
                .map_err(|_| CryptoError)?;
            Ok(Self(ring::aead::LessSafeKey::new(key)))
        }
    }

    /// Encrypt in place and append the authentication tag.
    ///
    /// `nonce` must never be used for another message with this key.
    pub fn seal_in_place_append_tag(
        &self,
        nonce: [u8; AES_GCM_NONCE_LEN],
        associated_data: &[u8],
        plaintext_and_tag: &mut Vec<u8>,
    ) -> Result<(), CryptoError> {
        #[cfg(feature = "openssl")]
        {
            use openssl::cipher::Cipher;
            use openssl::cipher_ctx::CipherCtx;

            let plaintext_len = plaintext_and_tag.len();
            let mut context = CipherCtx::new().map_err(|_| CryptoError)?;
            context
                .encrypt_init(Some(Cipher::aes_256_gcm()), Some(&self.0), Some(&nonce))
                .map_err(|_| CryptoError)?;
            context
                .cipher_update(associated_data, None)
                .map_err(|_| CryptoError)?;
            let written = context
                .cipher_update_inplace(plaintext_and_tag, plaintext_len)
                .map_err(|_| CryptoError)?;
            if written != plaintext_len {
                return Err(CryptoError);
            }
            let finalized = context.cipher_final(&mut []).map_err(|_| CryptoError)?;
            if finalized != 0 {
                return Err(CryptoError);
            }
            let mut tag = [0; AES_GCM_TAG_LEN];
            context.tag(&mut tag).map_err(|_| CryptoError)?;
            plaintext_and_tag.extend_from_slice(&tag);
            Ok(())
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            self.0
                .seal_in_place_append_tag(
                    ring::aead::Nonce::assume_unique_for_key(nonce),
                    ring::aead::Aad::from(associated_data),
                    plaintext_and_tag,
                )
                .map_err(|_| CryptoError)
        }
    }

    pub fn open_in_place<'a>(
        &self,
        nonce: [u8; AES_GCM_NONCE_LEN],
        associated_data: &[u8],
        ciphertext_and_tag: &'a mut [u8],
    ) -> Result<&'a mut [u8], CryptoError> {
        #[cfg(feature = "openssl")]
        {
            use openssl::cipher::Cipher;
            use openssl::cipher_ctx::CipherCtx;

            let ciphertext_len = ciphertext_and_tag
                .len()
                .checked_sub(AES_GCM_TAG_LEN)
                .ok_or(CryptoError)?;
            let (ciphertext, tag) = ciphertext_and_tag.split_at_mut(ciphertext_len);
            let mut context = CipherCtx::new().map_err(|_| CryptoError)?;
            context
                .decrypt_init(Some(Cipher::aes_256_gcm()), Some(&self.0), Some(&nonce))
                .map_err(|_| CryptoError)?;
            context
                .cipher_update(associated_data, None)
                .map_err(|_| CryptoError)?;
            context.set_tag(tag).map_err(|_| CryptoError)?;
            let written = context
                .cipher_update_inplace(ciphertext, ciphertext_len)
                .map_err(|_| CryptoError)?;
            if written != ciphertext_len {
                return Err(CryptoError);
            }
            let finalized = context.cipher_final(&mut []).map_err(|_| CryptoError)?;
            if finalized != 0 {
                return Err(CryptoError);
            }
            Ok(ciphertext)
        }
        #[cfg(all(feature = "ring", not(feature = "openssl")))]
        {
            self.0
                .open_in_place(
                    ring::aead::Nonce::assume_unique_for_key(nonce),
                    ring::aead::Aad::from(associated_data),
                    ciphertext_and_tag,
                )
                .map_err(|_| CryptoError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nist_zero_vector() {
        let key = Aes256GcmKey::new(&[0; AES_256_GCM_KEY_LEN]).unwrap();
        let nonce = [0; AES_GCM_NONCE_LEN];
        let mut ciphertext = vec![0; 16];
        key.seal_in_place_append_tag(nonce, &[], &mut ciphertext)
            .unwrap();
        assert_eq!(
            ciphertext,
            [
                0xce, 0xa7, 0x40, 0x3d, 0x4d, 0x60, 0x6b, 0x6e, 0x07, 0x4e, 0xc5, 0xd3, 0xba, 0xf3,
                0x9d, 0x18, 0xd0, 0xd1, 0xc8, 0xa7, 0x99, 0x99, 0x6b, 0xf0, 0x26, 0x5b, 0x98, 0xb5,
                0xd4, 0x8a, 0xb9, 0x19,
            ]
        );
    }

    #[test]
    fn round_trip_and_authentication() {
        let key = Aes256GcmKey::new(&[0x42; AES_256_GCM_KEY_LEN]).unwrap();
        let nonce = [0x24; AES_GCM_NONCE_LEN];
        let mut ciphertext = b"plaintext".to_vec();
        key.seal_in_place_append_tag(nonce, b"associated data", &mut ciphertext)
            .unwrap();
        assert_eq!(ciphertext.len(), b"plaintext".len() + AES_GCM_TAG_LEN);
        assert_eq!(
            key.open_in_place(nonce, b"associated data", &mut ciphertext)
                .unwrap(),
            b"plaintext"
        );

        let different_nonce = [0x25; AES_GCM_NONCE_LEN];
        let mut ciphertext = b"plaintext".to_vec();
        key.seal_in_place_append_tag(different_nonce, b"associated data", &mut ciphertext)
            .unwrap();
        assert!(key
            .open_in_place(different_nonce, b"wrong", &mut ciphertext)
            .is_err());
    }
}
