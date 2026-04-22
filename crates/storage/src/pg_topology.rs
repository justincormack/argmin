use rapidhash::v3::{rapidhash_v3_micro_inline, RapidSecrets};

use crate::types::{BucketName, ObjectKey};

const RAPIDHASH_SECRETS: RapidSecrets = RapidSecrets::seed(0);
const PG_HASH_STACK_LIMIT: usize = 63 + 1 + 1024 + 1 + 20;

#[inline]
fn hash_bytes(data: &[u8]) -> u64 {
    rapidhash_v3_micro_inline::<true, false>(data, &RAPIDHASH_SECRETS)
}

#[inline]
fn hash_parts(parts: &[&[u8]]) -> u64 {
    let total_len = parts.iter().map(|part| part.len()).sum::<usize>();
    assert!(
        total_len <= PG_HASH_STACK_LIMIT,
        "PG hash input exceeded validated bound: {total_len} > {PG_HASH_STACK_LIMIT}"
    );

    let mut data = [0u8; PG_HASH_STACK_LIMIT];
    let mut offset = 0;
    for part in parts {
        let end = offset + part.len();
        data[offset..end].copy_from_slice(part);
        offset = end;
    }
    hash_bytes(&data[..total_len])
}

#[inline]
fn decimal_u64_bytes(value: u64, out: &mut [u8; 20]) -> &[u8] {
    let mut value = value;
    let mut idx = out.len();
    loop {
        idx -= 1;
        out[idx] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &out[idx..];
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgTopology {
    pg_ids: Box<[u32]>,
}

impl PgTopology {
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

    pub fn pg_count(&self) -> u32 {
        self.pg_ids.len() as u32
    }

    pub fn object_pg(&self, bucket: &str, key: &str) -> u32 {
        let hash = hash_parts(&[bucket.as_bytes(), b"/", key.as_bytes()]);
        pick_pg(&self.pg_ids, hash)
    }

    pub fn object_pg_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_pg(bucket.as_str(), key.as_str())
    }

    pub fn bucket_pg(&self, bucket: &str) -> u32 {
        let hash = hash_parts(&[b"bucket/", bucket.as_bytes()]);
        pick_pg(&self.pg_ids, hash)
    }

    pub fn bucket_pg_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_pg(bucket.as_str())
    }

    pub fn shard_pg(&self, bucket: &str, key: &str, version_id: u64) -> u32 {
        let mut version_buf = [0u8; 20];
        let version_bytes = decimal_u64_bytes(version_id, &mut version_buf);
        let hash = hash_parts(&[bucket.as_bytes(), b"/", key.as_bytes(), b"/", version_bytes]);
        pick_pg(&self.pg_ids, hash)
    }

    pub fn shard_pg_for(&self, bucket: &BucketName, key: &ObjectKey, version_id: u64) -> u32 {
        self.shard_pg(bucket.as_str(), key.as_str(), version_id)
    }

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

#[cfg(test)]
mod tests {
    use super::PgTopology;
    use crate::{BucketName, ObjectKey};

    #[test]
    fn topology_rejects_empty() {
        assert!(PgTopology::new(&[]).is_err());
    }

    #[test]
    fn topology_canonicalizes_ids() {
        let topo = PgTopology::new(&[8, 2, 8, 1]).unwrap();
        assert_eq!(topo.pg_count(), 3);
    }

    #[test]
    fn bucket_and_object_pg_are_stable() {
        let topo = PgTopology::new(&[1, 2, 8]).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        assert_eq!(topo.bucket_pg_for(&bucket), topo.bucket_pg("bucket"));
        assert_eq!(
            topo.object_pg_for(&bucket, &key),
            topo.object_pg("bucket", "key")
        );
    }
}
