use auth::{CredentialStore, SecretKey};

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

pub fn seeded_store() -> CredentialStore {
    let mut store = CredentialStore::new();
    store.add(
        "testAccessKey123".to_string(),
        SecretKey::new("testSecretKey1234567890".to_string()),
    );
    store
}
