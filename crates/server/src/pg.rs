/// PG derivation: maps (bucket, key) to a placement group ID.
/// Derive the PG ID for a given bucket and key.
///
/// pg_id = rapidhash(bucket + "/" + key) % pg_count
pub fn derive_pg(bucket: &str, key: &str, pg_count: u32) -> u32 {
    let full_key = format!("{}/{}", bucket, key);
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % pg_count as u64) as u32
}

/// Derive the PG ID for shard data placement.
///
/// Includes the version_id so different versions of the same key
/// can have their shards distributed across different PGs.
///
/// pg_id = rapidhash(bucket + "/" + key + "/" + version_id) % pg_count
pub fn derive_pg_shards(bucket: &str, key: &str, version_id: u64, pg_count: u32) -> u32 {
    let full_key = format!("{}/{}/{}", bucket, key, version_id);
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

/// Compute the 16-byte part key hash for multipart upload shard keys.
///
/// `part_okh = SHA-256("mpu/" + upload_id + "/" + part_number + "/" + generation)[:16]`
pub fn part_key_hash(upload_id: &str, part_number: u32, generation: u32) -> [u8; 16] {
    use ring::digest;
    let input = format!("mpu/{upload_id}/{part_number}/{generation}");
    let hash = digest::digest(&digest::SHA256, input.as_bytes());
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
    fn derive_pg_shards_deterministic() {
        let a = derive_pg_shards("mybucket", "mykey", 1, 16);
        let b = derive_pg_shards("mybucket", "mykey", 1, 16);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_pg_shards_within_range() {
        for pg_count in [1, 4, 16, 256] {
            for i in 0..100 {
                let key = format!("key-{}", i);
                let pg = derive_pg_shards("bucket", &key, i, pg_count);
                assert!(pg < pg_count);
            }
        }
    }

    #[test]
    fn derive_pg_shards_different_versions_may_differ() {
        // Different versions of the same key can map to different PGs
        let pg_count = 256;
        let pg_v0 = derive_pg_shards("bucket", "key", 0, pg_count);
        let pg_v1 = derive_pg_shards("bucket", "key", 1, pg_count);
        let pg_v2 = derive_pg_shards("bucket", "key", 2, pg_count);
        // At least some should differ with 256 PGs
        assert!(
            pg_v0 != pg_v1 || pg_v1 != pg_v2,
            "all versions mapped to same PG"
        );
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
