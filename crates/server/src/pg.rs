/// PG derivation: maps (bucket, key) to a placement group ID.

/// Derive the PG ID for a given bucket and key.
///
/// pg_id = rapidhash(bucket + "/" + key) % pg_count
pub fn derive_pg(bucket: &str, key: &str, pg_count: u32) -> u32 {
    let full_key = format!("{}/{}", bucket, key);
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % pg_count as u64) as u32
}

/// Compute the 16-byte object key hash used in ShardKey construction.
///
/// Uses SHA-256 truncated to 16 bytes for deterministic, well-distributed hashing.
pub fn object_key_hash(bucket: &str, key: &str) -> [u8; 16] {
    use ring::digest;
    let full_key = format!("{}/{}", bucket, key);
    let hash = digest::digest(&digest::SHA256, full_key.as_bytes());
    let mut result = [0u8; 16];
    result.copy_from_slice(&hash.as_ref()[..16]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_pg_deterministic() {
        let a = derive_pg("mybucket", "mykey", 16);
        let b = derive_pg("mybucket", "mykey", 16);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_pg_within_range() {
        for pg_count in [1, 4, 16, 256] {
            for i in 0..100 {
                let key = format!("key-{}", i);
                let pg = derive_pg("bucket", &key, pg_count);
                assert!(pg < pg_count);
            }
        }
    }

    #[test]
    fn derive_pg_distributes() {
        let pg_count = 16u32;
        let mut counts = vec![0u32; pg_count as usize];
        for i in 0..1000 {
            let key = format!("object-{}", i);
            let pg = derive_pg("test-bucket", &key, pg_count);
            counts[pg as usize] += 1;
        }
        // Each PG should get at least some objects (rough check)
        for count in &counts {
            assert!(*count > 0, "at least one PG got no objects: {:?}", counts);
        }
    }

    #[test]
    fn object_key_hash_deterministic() {
        let a = object_key_hash("bucket", "key");
        let b = object_key_hash("bucket", "key");
        assert_eq!(a, b);
    }

    #[test]
    fn object_key_hash_different_keys() {
        let a = object_key_hash("bucket", "key1");
        let b = object_key_hash("bucket", "key2");
        assert_ne!(a, b);
    }

    #[test]
    fn object_key_hash_length() {
        let hash = object_key_hash("bucket", "key");
        assert_eq!(hash.len(), 16);
    }
}
