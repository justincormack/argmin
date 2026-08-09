// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use auth::{CredentialStore, IdentityProvider, SecretKey};

pub fn split_input(data: &[u8], parts: usize) -> Vec<&[u8]> {
    let mut slices = Vec::with_capacity(parts);
    let chunk_len = data.len().div_ceil(parts.max(1));
    for index in 0..parts {
        let start = usize::min(index * chunk_len, data.len());
        let end = usize::min(start + chunk_len, data.len());
        slices.push(&data[start..end]);
    }
    slices
}

pub fn lossy(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

pub fn chunk_by_controls<'a>(data: &'a [u8], controls: &[u8]) -> Vec<&'a [u8]> {
    if data.is_empty() {
        return vec![data];
    }

    let mut chunks = Vec::new();
    let mut offset = 0;

    for &control in controls {
        if offset >= data.len() {
            break;
        }

        let remaining = data.len() - offset;
        let len = 1 + usize::from(control) % remaining;
        chunks.push(&data[offset..offset + len]);
        offset += len;
    }

    if offset < data.len() {
        chunks.push(&data[offset..]);
    }

    if chunks.is_empty() {
        chunks.push(data);
    }

    chunks
}

pub fn seeded_store() -> IdentityProvider {
    let mut store = CredentialStore::new();
    store.add(
        "testAccessKey123".to_string(),
        SecretKey::new("testSecretKey1234567890".to_string()),
    )
    .expect("fuzz credential store contains one unique key");
    IdentityProvider::in_memory(store).expect("build fuzz identity provider")
}
