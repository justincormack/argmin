// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Read as _;

use argmin_crypto::cobblestone::{
    self, Cobblestone256Key, Cobblestone256MessageLayout, Cobblestone256Opener,
    Cobblestone256StreamOpener, COBBLESTONE_HEADER_LEN, COBBLESTONE_SALT_LEN,
};
use argmin_crypto::digest::Sha512;
use argmin_crypto::sha256::Sha256;
use serde_json::Value;

const VECTORS: &[u8] = include_bytes!("../testdata/c2sp_chunked_encryption_aes_256_gcm_test.json");
const VECTORS_SHA256: &str = "0f8e35568d59649fb55fb0e5602dc678dc8910d6822beef4945aee0b5994ef05";

fn decode_hex(value: &Value, name: &str) -> Vec<u8> {
    hex::decode(
        value
            .as_str()
            .unwrap_or_else(|| panic!("{name} must be a hexadecimal string")),
    )
    .unwrap_or_else(|error| panic!("{name} must contain valid hexadecimal: {error}"))
}

fn decompress_ciphertext(value: &Value) -> Vec<u8> {
    let compressed = decode_hex(value, "ct");
    let mut decoder = flate2::read::ZlibDecoder::new(compressed.as_slice());
    let mut ciphertext = Vec::new();
    decoder
        .read_to_end(&mut ciphertext)
        .expect("Wycheproof ciphertext must be valid zlib");
    ciphertext
}

fn sha512(value: &[u8]) -> [u8; 64] {
    let mut digest = Sha512::new();
    digest.update(value);
    digest.finalize()
}

fn vectors() -> Value {
    let mut digest = Sha256::new();
    digest.update(VECTORS);
    let digest = digest.finalize();
    assert_eq!(hex::encode(digest), VECTORS_SHA256);
    serde_json::from_slice(VECTORS).expect("pinned Wycheproof fixture must parse")
}

fn all_tests(document: &Value) -> impl Iterator<Item = &Value> {
    document["testGroups"]
        .as_array()
        .expect("testGroups must be an array")
        .iter()
        .flat_map(|group| {
            assert_eq!(group["aead"], "AEAD_AES_256_GCM");
            assert_eq!(group["sha"], "SHA-512");
            group["tests"].as_array().expect("tests must be an array")
        })
}

#[test]
fn pinned_wycheproof_cobblestone_256_vectors() {
    let document = vectors();
    assert_eq!(document["algorithm"], "Cobblestone-256");
    assert_eq!(document["numberOfTests"], 35);

    let mut observed = 0_u64;
    for test in all_tests(&document) {
        observed += 1;
        let test_id = test["tcId"].as_u64().expect("tcId must be an integer");
        let input_key = decode_hex(&test["key"], "key");
        let context = decode_hex(&test["ctx"], "ctx");
        let ciphertext = decompress_ciphertext(&test["ct"]);
        let expected_result = test["result"].as_str().expect("result must be a string");
        let flags = test["flags"].as_array().expect("flags must be an array");

        let key = Cobblestone256Key::new(&input_key);
        if flags.iter().any(|flag| flag == "InvalidKeySize") {
            assert!(key.is_err(), "tcId {test_id} accepted an invalid key size");
            continue;
        }
        let opened = cobblestone::open(key.unwrap(), &context, &ciphertext);
        if expected_result == "invalid" {
            assert!(
                opened.is_err(),
                "tcId {test_id} accepted invalid ciphertext"
            );
            continue;
        }
        assert_eq!(
            expected_result, "valid",
            "tcId {test_id} has unknown result"
        );
        let plaintext = opened.unwrap_or_else(|_| panic!("tcId {test_id} failed to open"));
        assert_eq!(
            plaintext.len() as u64,
            test["msgLength"]
                .as_u64()
                .expect("valid vector must have msgLength"),
            "tcId {test_id} plaintext length"
        );
        assert_eq!(
            hex::encode(sha512(&plaintext)),
            test["msgSha512"]
                .as_str()
                .expect("valid vector must have msgSha512"),
            "tcId {test_id} plaintext digest"
        );

        let salt: [u8; COBBLESTONE_SALT_LEN] = ciphertext[..COBBLESTONE_SALT_LEN]
            .try_into()
            .expect("valid vector must contain a salt");
        let resealed = cobblestone::seal(
            &Cobblestone256Key::new(&input_key).unwrap(),
            salt,
            &context,
            &plaintext,
        )
        .unwrap_or_else(|_| panic!("tcId {test_id} failed to reseal"));
        assert_eq!(resealed, ciphertext, "tcId {test_id} persisted bytes");

        let layout = Cobblestone256MessageLayout::from_ciphertext_len(ciphertext.len() as u64)
            .unwrap_or_else(|_| panic!("tcId {test_id} has invalid layout"));
        let opener = Cobblestone256Opener::from_header(
            &Cobblestone256Key::new(&input_key).unwrap(),
            &context,
            &ciphertext[..COBBLESTONE_HEADER_LEN],
        )
        .unwrap_or_else(|_| panic!("tcId {test_id} has invalid commitment"));
        let mut selected = Vec::with_capacity(plaintext.len());
        for chunk_index in 0..layout.chunk_count() {
            let range = layout
                .ciphertext_range_for_chunk(chunk_index)
                .unwrap_or_else(|_| panic!("tcId {test_id} chunk {chunk_index} range"));
            let mut chunk = ciphertext[range.start as usize..range.end as usize].to_vec();
            selected.extend_from_slice(
                opener
                    .open_chunk(layout, chunk_index, &mut chunk)
                    .unwrap_or_else(|_| panic!("tcId {test_id} chunk {chunk_index} failed")),
            );
        }
        assert_eq!(selected, plaintext, "tcId {test_id} selected chunks");
    }
    assert_eq!(observed, document["numberOfTests"].as_u64().unwrap());
}

#[test]
fn wycheproof_streaming_failures_expose_only_authenticated_prefixes() {
    let document = vectors();
    for test in all_tests(&document) {
        let test_id = test["tcId"].as_u64().expect("tcId must be an integer");
        let input_key = decode_hex(&test["key"], "key");
        let context = decode_hex(&test["ctx"], "ctx");
        let ciphertext = decompress_ciphertext(&test["ct"]);
        let flags = test["flags"].as_array().expect("flags must be an array");
        if flags.iter().any(|flag| flag == "InvalidKeySize") {
            continue;
        }

        let mut opener = match Cobblestone256StreamOpener::new(
            Cobblestone256Key::new(&input_key).unwrap(),
            &context,
            ciphertext.len() as u64,
        ) {
            Ok(opener) => opener,
            Err(_) => {
                assert_eq!(test["result"], "invalid", "tcId {test_id}");
                continue;
            }
        };
        let mut plaintext = Vec::new();
        let mut failure = false;
        for input in ciphertext.chunks(997) {
            if opener.update(input, &mut plaintext).is_err() {
                failure = true;
                break;
            }
        }
        if !failure {
            failure = opener.finish().is_err();
        }

        if test["result"] == "valid" {
            assert!(!failure, "tcId {test_id} streaming open failed");
            assert_eq!(plaintext.len() as u64, test["msgLength"].as_u64().unwrap());
            assert_eq!(
                hex::encode(sha512(&plaintext)),
                test["msgSha512"].as_str().unwrap(),
                "tcId {test_id} streaming plaintext"
            );
        } else {
            assert!(failure, "tcId {test_id} streaming open succeeded");
            let maximum_prefix = test["msgLength"].as_u64().unwrap_or(0);
            assert!(
                plaintext.len() as u64 <= maximum_prefix,
                "tcId {test_id} released bytes beyond its authenticated prefix"
            );
            if plaintext.len() as u64 == maximum_prefix && maximum_prefix != 0 {
                assert_eq!(
                    hex::encode(sha512(&plaintext)),
                    test["msgSha512"].as_str().unwrap(),
                    "tcId {test_id} authenticated prefix"
                );
            }
        }
    }
}
