// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use rapidhash::v3::{rapidhash_v3_micro_inline, RapidSecrets};

use crate::types::{BucketName, GenerationId, ObjectKey, PgId};

const RAPIDHASH_SECRETS: RapidSecrets = RapidSecrets::seed(0);
const PG_HASH_STACK_LIMIT: usize = 32 + 63 + 1 + 1024 + 1 + 20 + 1 + 10;
pub const DEFAULT_OBJECT_DATA_PG_SET_WIDTH: usize = 4;
pub const DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE: u32 = 16;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PgTopologyError {
    #[error("pg topology cannot be empty")]
    Empty,
}

impl PgTopology {
    pub fn new(pg_ids: &[u32]) -> Result<Self, PgTopologyError> {
        if pg_ids.is_empty() {
            return Err(PgTopologyError::Empty);
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

    /// Derive the object metadata PG for `(bucket, key)`.
    ///
    /// This PG owns object namespace metadata. It is not necessarily the data PG
    /// that stores payload shards for the object's segments.
    pub fn object_pg(&self, bucket: &str, key: &str) -> u32 {
        let hash = hash_parts(&[bucket.as_bytes(), b"/", key.as_bytes()]);
        pick_pg(&self.pg_ids, hash)
    }

    pub fn object_pg_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_pg(bucket.as_str(), key.as_str())
    }

    /// Derive the bucket metadata PG for `bucket`.
    ///
    /// Bucket rows and bucket subresources remain PG-sharded. They do not move
    /// back to a global service in the multihost transition.
    pub fn bucket_pg(&self, bucket: &str) -> u32 {
        self.pg_for_bucket_route_hash(Self::bucket_route_hash(bucket))
    }

    pub fn bucket_pg_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_pg(bucket.as_str())
    }

    pub(crate) fn bucket_route_hash(bucket: &str) -> u64 {
        hash_parts(&[b"bucket/", bucket.as_bytes()])
    }

    pub(crate) fn pg_for_bucket_route_hash(&self, hash: u64) -> u32 {
        pick_pg(&self.pg_ids, hash)
    }

    #[cfg(test)]
    pub(crate) fn object_data_pg_set_width(&self) -> usize {
        DEFAULT_OBJECT_DATA_PG_SET_WIDTH.min(self.pg_ids.len())
    }

    pub(crate) fn object_generation_data_pg_set(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Vec<PgId> {
        self.object_generation_data_pg_set_with_width(
            bucket,
            key,
            generation_id,
            NonZeroUsize::new(DEFAULT_OBJECT_DATA_PG_SET_WIDTH)
                .expect("default data PG set width must be nonzero"),
        )
    }

    fn object_generation_data_pg_set_with_width(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        width: NonZeroUsize,
    ) -> Vec<PgId> {
        let width = width.get().min(self.pg_ids.len());
        let mut ranked: Vec<(u64, u32)> = self
            .pg_ids
            .iter()
            .map(|&pg_id| {
                (
                    object_generation_pg_score(bucket, key, generation_id, pg_id),
                    pg_id,
                )
            })
            .collect();
        ranked.sort_unstable_by(|(left_score, left_pg), (right_score, right_pg)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_pg.cmp(right_pg))
        });
        ranked
            .into_iter()
            .take(width)
            .map(|(_, pg_id)| PgId::new(pg_id))
            .collect()
    }

    pub(crate) fn object_generation_segment_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
    ) -> PgId {
        self.object_generation_segment_data_pg_with_width(
            bucket,
            key,
            generation_id,
            segment_index,
            NonZeroUsize::new(DEFAULT_OBJECT_DATA_PG_SET_WIDTH)
                .expect("default data PG set width must be nonzero"),
        )
    }

    fn object_generation_segment_data_pg_with_width(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
        width: NonZeroUsize,
    ) -> PgId {
        let set = self.object_generation_data_pg_set_with_width(bucket, key, generation_id, width);
        let band_index = segment_index / DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE;
        set[band_index as usize % set.len()]
    }

    pub(crate) fn object_generation_multipart_part_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        part_number: u32,
    ) -> PgId {
        let part_band_index = u64::from(part_number.saturating_sub(1));
        self.object_generation_band_data_pg(bucket, key, generation_id, part_band_index)
    }

    pub(crate) fn object_generation_multipart_part_segment_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        part_number: u32,
        segment_index: u32,
    ) -> PgId {
        let part_band_index = u64::from(part_number.saturating_sub(1));
        let segment_band_index =
            u64::from(segment_index / DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE);
        self.object_generation_band_data_pg(
            bucket,
            key,
            generation_id,
            part_band_index + segment_band_index,
        )
    }

    fn object_generation_band_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        band_index: u64,
    ) -> PgId {
        let set = self.object_generation_data_pg_set(bucket, key, generation_id);
        set[(band_index % set.len() as u64) as usize]
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

fn object_generation_pg_score(
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
    pg_id: u32,
) -> u64 {
    let mut generation_buf = [0u8; 20];
    let generation_bytes = decimal_u64_bytes(generation_id.get(), &mut generation_buf);
    let mut pg_buf = [0u8; 20];
    let pg_bytes = decimal_u64_bytes(u64::from(pg_id), &mut pg_buf);
    hash_parts(&[
        b"object-generation-data-pg/",
        bucket.as_str().as_bytes(),
        b"/",
        key.as_str().as_bytes(),
        b"/",
        generation_bytes,
        b"/",
        pg_bytes,
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    use super::{
        hash_bytes, hash_parts, PgTopology, PgTopologyError,
        DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE, DEFAULT_OBJECT_DATA_PG_SET_WIDTH,
    };
    use crate::{BucketName, GenerationId, ObjectKey};

    #[test]
    fn topology_rejects_empty() {
        assert_eq!(PgTopology::new(&[]).unwrap_err(), PgTopologyError::Empty);
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

    #[test]
    fn bucket_and_object_pg_stay_within_configured_ids() {
        let pg_ids: Vec<u32> = (0..16).collect();
        let topo = PgTopology::new(&pg_ids).unwrap();

        for i in 0..100 {
            let bucket = format!("bucket-{i}");
            let key = format!("key-{i}");
            assert!(pg_ids.contains(&topo.bucket_pg(&bucket)));
            assert!(pg_ids.contains(&topo.object_pg(&bucket, &key)));
        }
    }

    #[test]
    fn bucket_and_object_pg_distribute_over_configured_ids() {
        let pg_ids: Vec<u32> = (0..16).collect();
        let topo = PgTopology::new(&pg_ids).unwrap();
        let mut bucket_counts = vec![0u32; pg_ids.len()];
        let mut object_counts = vec![0u32; pg_ids.len()];

        for i in 0..1000 {
            let bucket = format!("bucket-{i}");
            let key = format!("object-{i}");
            bucket_counts[topo.bucket_pg(&bucket) as usize] += 1;
            object_counts[topo.object_pg("test-bucket", &key) as usize] += 1;
        }

        for count in &bucket_counts {
            assert!(
                *count > 0,
                "at least one PG got no buckets: {bucket_counts:?}"
            );
        }
        for count in &object_counts {
            assert!(
                *count > 0,
                "at least one PG got no objects: {object_counts:?}"
            );
        }
    }

    #[test]
    fn hash_parts_matches_bulk_hash_for_object_pg() {
        let bucket = "bucket";
        let key = "key";
        let expected = hash_bytes(format!("{bucket}/{key}").as_bytes());
        let actual = hash_parts(&[bucket.as_bytes(), b"/", key.as_bytes()]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_parts_matches_bulk_hash_for_bucket_pg() {
        let bucket = "bucket";
        let expected = hash_bytes(format!("bucket/{bucket}").as_bytes());
        let actual = hash_parts(&[b"bucket/", bucket.as_bytes()]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn typed_placement_inputs_match_string_placement() {
        let topo = PgTopology::new(&[1, 2, 8]).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();

        assert_eq!(topo.bucket_pg_for(&bucket), topo.bucket_pg(bucket.as_str()));
        assert_eq!(
            topo.object_pg_for(&bucket, &key),
            topo.object_pg(bucket.as_str(), key.as_str())
        );
    }

    #[test]
    fn object_generation_data_pg_set_is_deterministic_and_bounded() {
        let pg_ids: Vec<u32> = (0..16).collect();
        let topo = PgTopology::new(&pg_ids).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let generation_id = GenerationId::new(7).unwrap();

        let first = topo.object_generation_data_pg_set(&bucket, &key, generation_id);
        let second = topo.object_generation_data_pg_set(&bucket, &key, generation_id);

        assert_eq!(first, second);
        assert_eq!(first.len(), DEFAULT_OBJECT_DATA_PG_SET_WIDTH);
        assert_eq!(
            first
                .iter()
                .map(|pg_id| pg_id.get())
                .collect::<BTreeSet<_>>()
                .len(),
            first.len()
        );
        assert!(first.iter().all(|pg_id| pg_ids.contains(&pg_id.get())));
    }

    #[test]
    fn object_generation_data_pg_set_width_clamps_to_topology_size() {
        let topo = PgTopology::new(&[2, 4]).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let generation_id = GenerationId::new(1).unwrap();

        assert_eq!(topo.object_data_pg_set_width(), 2);
        assert_eq!(
            topo.object_generation_data_pg_set(&bucket, &key, generation_id)
                .len(),
            2
        );
    }

    #[test]
    fn object_generation_segment_data_pg_uses_bands_over_bounded_set() {
        let topo = PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let generation_id = GenerationId::new(9).unwrap();
        let width = NonZeroUsize::new(3).unwrap();
        let set =
            topo.object_generation_data_pg_set_with_width(&bucket, &key, generation_id, width);

        assert_eq!(
            topo.object_generation_segment_data_pg_with_width(
                &bucket,
                &key,
                generation_id,
                0,
                width
            ),
            set[0]
        );
        assert_eq!(
            topo.object_generation_segment_data_pg_with_width(
                &bucket,
                &key,
                generation_id,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE - 1,
                width
            ),
            set[0]
        );
        assert_eq!(
            topo.object_generation_segment_data_pg_with_width(
                &bucket,
                &key,
                generation_id,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE,
                width
            ),
            set[1]
        );
        assert_eq!(
            topo.object_generation_segment_data_pg_with_width(
                &bucket,
                &key,
                generation_id,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE * 3,
                width
            ),
            set[0]
        );
    }

    #[test]
    fn object_generation_multipart_part_segment_data_pg_uses_part_and_segment_bands() {
        let topo = PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let generation_id = GenerationId::new(11).unwrap();
        let set = topo.object_generation_data_pg_set(&bucket, &key, generation_id);

        assert_eq!(
            topo.object_generation_multipart_part_data_pg(&bucket, &key, generation_id, 1),
            set[0]
        );
        assert_eq!(
            topo.object_generation_multipart_part_data_pg(&bucket, &key, generation_id, 2),
            set[1]
        );
        assert_eq!(
            topo.object_generation_multipart_part_segment_data_pg(
                &bucket,
                &key,
                generation_id,
                1,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE - 1,
            ),
            set[0]
        );
        assert_eq!(
            topo.object_generation_multipart_part_segment_data_pg(
                &bucket,
                &key,
                generation_id,
                1,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE,
            ),
            set[1]
        );
        assert_eq!(
            topo.object_generation_multipart_part_segment_data_pg(
                &bucket,
                &key,
                generation_id,
                2,
                DEFAULT_OBJECT_DATA_PG_SEGMENT_BAND_SIZE,
            ),
            set[2]
        );
    }
}
