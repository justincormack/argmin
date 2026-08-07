#![cfg(feature = "openssl")]

use md5_legacy::Digest as _;
use proptest::prelude::*;

use argmin_crypto::aead::{Aes256GcmKey, AES_256_GCM_KEY_LEN, AES_GCM_NONCE_LEN};
use argmin_crypto::digest::{Md5, Sha1, Sha512};
use argmin_crypto::hmac::{self, Sha256Key};
use argmin_crypto::sha256::Sha256;

struct HkdfLength(usize);

impl ring::hkdf::KeyType for HkdfLength {
    fn len(&self) -> usize {
        self.0
    }
}

fn ring_hkdf(salt: &[u8], key_material: &[u8], info: &[u8], output: &mut [u8]) {
    let salt = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, salt);
    let key = salt.extract(key_material);
    let info = [info];
    key.expand(&info, HkdfLength(output.len()))
        .unwrap()
        .fill(output)
        .unwrap();
}

fn update_in_chunks(mut update: impl FnMut(&[u8]), data: &[u8], chunk_size: usize) {
    if data.is_empty() {
        update(data);
        return;
    }
    for chunk in data.chunks(chunk_size.max(1)) {
        update(chunk);
    }
}

proptest! {
    #[test]
    fn message_digests_match_ring_oracles(
        data in proptest::collection::vec(any::<u8>(), 0..32_768),
        chunk_size in 1usize..1024,
    ) {
        prop_assert_eq!(argmin_crypto::provider_name(), "openssl");

        let mut md5 = Md5::new();
        update_in_chunks(|chunk| md5.update(chunk), &data, chunk_size);
        let expected_md5: [u8; 16] = md5_legacy::Md5::digest(&data).into();
        prop_assert_eq!(md5.finalize(), expected_md5);

        let mut sha1 = Sha1::new();
        update_in_chunks(|chunk| sha1.update(chunk), &data, chunk_size);
        let expected_sha1: [u8; 20] = ring::digest::digest(
            &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            &data,
        )
        .as_ref()
        .try_into()
        .unwrap();
        prop_assert_eq!(sha1.finalize(), expected_sha1);

        let mut sha256 = Sha256::new();
        update_in_chunks(|chunk| sha256.update(chunk), &data, chunk_size);
        let expected_sha256: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &data)
            .as_ref()
            .try_into()
            .unwrap();
        prop_assert_eq!(sha256.finalize(), expected_sha256);

        let mut sha512 = Sha512::new();
        update_in_chunks(|chunk| sha512.update(chunk), &data, chunk_size);
        let expected_sha512: [u8; 64] = ring::digest::digest(&ring::digest::SHA512, &data)
            .as_ref()
            .try_into()
            .unwrap();
        prop_assert_eq!(sha512.finalize(), expected_sha512);
    }

    #[test]
    fn hmac_sha256_matches_ring_with_chunk_boundaries(
        key in proptest::collection::vec(any::<u8>(), 0..256),
        data in proptest::collection::vec(any::<u8>(), 0..32_768),
        chunk_size in 1usize..1024,
    ) {
        let key_under_test = Sha256Key::new(&key);
        let expected_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &key);
        let expected = ring::hmac::sign(&expected_key, &data);
        let expected_bytes: [u8; 32] = expected.as_ref().try_into().unwrap();
        prop_assert_eq!(hmac::sha256(&key, &data), expected_bytes);
        prop_assert_eq!(key_under_test.sign(&data), expected_bytes);

        let mut context = key_under_test.context();
        update_in_chunks(|chunk| context.update(chunk), &data, chunk_size);
        let actual = context.finalize();
        prop_assert_eq!(actual, expected_bytes);
        prop_assert!(key_under_test.verify(&data, expected.as_ref()));
    }

    #[test]
    fn aes256_gcm_interoperates_in_both_directions(
        key in any::<[u8; AES_256_GCM_KEY_LEN]>(),
        nonce in any::<[u8; AES_GCM_NONCE_LEN]>(),
        aad in proptest::collection::vec(any::<u8>(), 0..256),
        plaintext in proptest::collection::vec(any::<u8>(), 0..16_384),
    ) {
        let openssl_key = Aes256GcmKey::new(&key).unwrap();
        let ring_key = ring::aead::LessSafeKey::new(
            ring::aead::UnboundKey::new(&ring::aead::AES_256_GCM, &key).unwrap(),
        );

        let mut openssl_ciphertext = plaintext.clone();
        openssl_key
            .seal_in_place_append_tag(nonce, &aad, &mut openssl_ciphertext)
            .unwrap();
        let opened = ring_key
            .open_in_place(
                ring::aead::Nonce::assume_unique_for_key(nonce),
                ring::aead::Aad::from(&aad),
                &mut openssl_ciphertext,
            )
            .unwrap();
        prop_assert_eq!(opened, plaintext.as_slice());

        let mut ring_ciphertext = plaintext.clone();
        ring_key
            .seal_in_place_append_tag(
                ring::aead::Nonce::assume_unique_for_key(nonce),
                ring::aead::Aad::from(&aad),
                &mut ring_ciphertext,
            )
            .unwrap();
        let opened = openssl_key.open_in_place(nonce, &aad, &mut ring_ciphertext).unwrap();
        prop_assert_eq!(opened, plaintext.as_slice());
    }

    #[test]
    fn hkdf_sha256_matches_ring_for_arbitrary_inputs(
        salt in proptest::collection::vec(any::<u8>(), 0..256),
        input_key_material in proptest::collection::vec(any::<u8>(), 0..1024),
        info in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let actual = argmin_crypto::hkdf::sha256::<42>(&salt, &input_key_material, &info).unwrap();
        let mut expected = [0; 42];
        ring_hkdf(&salt, &input_key_material, &info, &mut expected);
        prop_assert_eq!(actual, expected);
    }
}

#[test]
fn hkdf_sha256_matches_ring_across_output_boundaries() {
    let salt = b"provider interop salt";
    let input_key_material = b"provider interop input key material";
    let info = b"provider interop info";

    macro_rules! compare_length {
        ($length:literal) => {{
            let actual =
                argmin_crypto::hkdf::sha256::<$length>(salt, input_key_material, info).unwrap();
            let mut expected = [0; $length];
            ring_hkdf(salt, input_key_material, info, &mut expected);
            assert_eq!(actual, expected);
        }};
    }

    compare_length!(0);
    compare_length!(1);
    compare_length!(31);
    compare_length!(32);
    compare_length!(33);
    compare_length!(255);
}
