/// PG derivation: maps keys to placement group IDs.
///
/// `PgTopology` is the coordinator-facing API and should be preferred over
/// direct modulo/count arithmetic so fanout and routing use the same PG set.

#[derive(Debug, Clone)]
pub struct PgTopology {
    pg_ids: Box<[u32]>,
}

impl PgTopology {
    /// Build canonical topology from raw PG IDs.
    ///
    /// IDs are sorted and de-duplicated so hash routing is deterministic.
    pub fn new(pg_ids: &[u32]) -> Result<Self, &'static str> {
        if pg_ids.is_empty() {
            return Err("pg topology cannot be empty");
        }
        let mut canonical = pg_ids.to_vec();
        canonical.sort_unstable();
        canonical.dedup();
        Ok(Self {
            pg_ids: canonical.into_boxed_slice(),
        })
    }

    /// Number of PGs in this topology.
    pub fn pg_count(&self) -> u32 {
        self.pg_ids.len() as u32
    }

    /// Derive the PG ID for a given bucket and key.
    pub fn object_pg(&self, bucket: &str, key: &str) -> u32 {
        let full_key = format!("{bucket}/{key}");
        let hash = rapidhash::rapidhash(full_key.as_bytes());
        pick_pg(&self.pg_ids, hash)
    }

    /// Derive the PG ID for bucket metadata placement.
    pub fn bucket_pg(&self, bucket: &str) -> u32 {
        let full_key = format!("bucket/{bucket}");
        let hash = rapidhash::rapidhash(full_key.as_bytes());
        pick_pg(&self.pg_ids, hash)
    }

    /// Derive the shard PG ID.
    pub fn shard_pg(&self, bucket: &str, key: &str, version_id: u64) -> u32 {
        let full_key = format!("{bucket}/{key}/{version_id}");
        let hash = rapidhash::rapidhash(full_key.as_bytes());
        pick_pg(&self.pg_ids, hash)
    }

    /// Execute a closure once for every PG in topology order.
    pub fn for_each_pg<E>(&self, mut f: impl FnMut(u32) -> Result<(), E>) -> Result<(), E> {
        for &pg_id in &*self.pg_ids {
            f(pg_id)?;
        }
        Ok(())
    }
}

fn pick_pg(pg_ids: &[u32], hash: u64) -> u32 {
    let idx = (hash % pg_ids.len() as u64) as usize;
    pg_ids[idx]
}

/// Derive the PG ID for a given bucket and key.
///
/// pg_id = rapidhash(bucket + "/" + key) % pg_count
#[cfg(test)]
pub(crate) fn derive_pg(bucket: &str, key: &str, pg_count: u32) -> u32 {
    let full_key = format!("{bucket}/{key}");
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % u64::from(pg_count)) as u32
}

/// Derive the PG ID for bucket metadata placement.
///
/// pg_id = rapidhash("bucket/" + bucket_name) % pg_count
#[cfg(test)]
pub(crate) fn derive_bucket_pg(bucket: &str, pg_count: u32) -> u32 {
    let full_key = format!("bucket/{bucket}");
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % u64::from(pg_count)) as u32
}

/// Derive the PG ID for shard data placement.
///
/// Includes the version_id so different versions of the same key
/// can have their shards distributed across different PGs.
///
/// pg_id = rapidhash(bucket + "/" + key + "/" + version_id) % pg_count
#[cfg(test)]
pub(crate) fn derive_pg_shards(bucket: &str, key: &str, version_id: u64, pg_count: u32) -> u32 {
    let full_key = format!("{bucket}/{key}/{version_id}");
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % u64::from(pg_count)) as u32
}

/// Compute the 16-byte object key hash used in ShardKey construction.
///
/// Uses SHA-256 truncated to 16 bytes for deterministic, well-distributed hashing.
pub fn object_key_hash(bucket: &str, key: &str) -> [u8; 16] {
    use ring::digest;
    let full_key = format!("{bucket}/{key}");
    let hash = digest::digest(&digest::SHA256, full_key.as_bytes());
    let mut result = [0u8; 16];
    result.copy_from_slice(&hash.as_ref()[..16]);
    result
}

/// Compute the 16-byte chunk key hash for streaming upload shard keys.
///
/// `chunk_okh = SHA-256("chunk/" + session_id + "/" + chunk_index)[:16]`
pub fn chunk_key_hash(session_id: &str, chunk_index: u32) -> [u8; 16] {
    use ring::digest;
    let input = format!("chunk/{session_id}/{chunk_index}");
    let hash = digest::digest(&digest::SHA256, input.as_bytes());
    let mut result = [0u8; 16];
    result.copy_from_slice(&hash.as_ref()[..16]);
    result
}

/// Compute the 16-byte segment key hash for committed object segment shard keys.
///
/// `segment_okh = SHA-256("segment/" + bucket + "/" + key + "/" + generation_id + "/" +
/// segment_index)[:16]`
pub fn segment_key_hash(
    bucket: &str,
    key: &str,
    generation_id: storage::GenerationId,
    segment_index: u32,
) -> [u8; 16] {
    use ring::digest;
    let input = format!(
        "segment/{bucket}/{key}/{}/{segment_index}",
        generation_id.get()
    );
    let hash = digest::digest(&digest::SHA256, input.as_bytes());
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
    fn topology_rejects_empty() {
        assert!(PgTopology::new(&[]).is_err());
    }

    #[test]
    fn topology_canonicalizes_ids() {
        let topo = PgTopology::new(&[8, 2, 8, 1]).unwrap();
        assert_eq!(topo.pg_ids.as_ref(), &[1, 2, 8]);
    }

    #[test]
    fn topology_maps_to_actual_pg_ids() {
        let topo = PgTopology::new(&[1, 2, 8]).unwrap();
        for i in 0..100 {
            let key = format!("k-{i}");
            let pg = topo.object_pg("bucket", &key);
            assert!(matches!(pg, 1 | 2 | 8));
        }
    }

    #[test]
    fn derive_bucket_pg_deterministic() {
        let a = derive_bucket_pg("mybucket", 16);
        let b = derive_bucket_pg("mybucket", 16);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_bucket_pg_within_range() {
        for pg_count in [1, 4, 16, 256] {
            for i in 0..100 {
                let bucket = format!("bucket-{i}");
                let pg = derive_bucket_pg(&bucket, pg_count);
                assert!(pg < pg_count);
            }
        }
    }

    #[test]
    fn derive_bucket_pg_distributes() {
        let pg_count = 16u32;
        let mut counts = vec![0u32; pg_count as usize];
        for i in 0..1000 {
            let bucket = format!("bucket-{i}");
            let pg = derive_bucket_pg(&bucket, pg_count);
            counts[pg as usize] += 1;
        }
        for count in &counts {
            assert!(*count > 0, "at least one PG got no buckets: {counts:?}");
        }
    }

    #[test]
    fn derive_pg_within_range() {
        for pg_count in [1, 4, 16, 256] {
            for i in 0..100 {
                let key = format!("key-{i}");
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
            let key = format!("object-{i}");
            let pg = derive_pg("test-bucket", &key, pg_count);
            counts[pg as usize] += 1;
        }
        // Each PG should get at least some objects (rough check)
        for count in &counts {
            assert!(*count > 0, "at least one PG got no objects: {counts:?}");
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
                let key = format!("key-{i}");
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
    fn chunk_key_hash_deterministic() {
        let a = chunk_key_hash("session-abc", 0);
        let b = chunk_key_hash("session-abc", 0);
        assert_eq!(a, b);
    }

    #[test]
    fn chunk_key_hash_different_indices() {
        let a = chunk_key_hash("session-abc", 0);
        let b = chunk_key_hash("session-abc", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_key_hash_different_sessions() {
        let a = chunk_key_hash("session-abc", 0);
        let b = chunk_key_hash("session-def", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_key_hash_length() {
        let hash = chunk_key_hash("session", 42);
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn segment_key_hash_deterministic() {
        let generation_id = storage::GenerationId::new(7).unwrap();
        let a = segment_key_hash("bucket", "key", generation_id, 0);
        let b = segment_key_hash("bucket", "key", generation_id, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn segment_key_hash_different_generations() {
        let a = segment_key_hash("bucket", "key", storage::GenerationId::new(7).unwrap(), 0);
        let b = segment_key_hash("bucket", "key", storage::GenerationId::new(8).unwrap(), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn segment_key_hash_different_indices() {
        let generation_id = storage::GenerationId::new(7).unwrap();
        let a = segment_key_hash("bucket", "key", generation_id, 0);
        let b = segment_key_hash("bucket", "key", generation_id, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn segment_key_hash_length() {
        let generation_id = storage::GenerationId::new(7).unwrap();
        let hash = segment_key_hash("bucket", "key", generation_id, 42);
        assert_eq!(hash.len(), 16);
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
