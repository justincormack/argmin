// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use crate::{BucketName, GenerationId, ObjectKey, PgTopology};

/// A logical key-selection result for topology UATs.
///
/// Storage owns the placement prediction. The UAT receives only the candidate
/// object key it should exercise and the selected data-PG identifier needed to
/// drive the external topology transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestObjectDataPgSelection {
    object_key: ObjectKey,
    data_pg_id: u32,
}

impl TestObjectDataPgSelection {
    #[must_use]
    pub fn into_key_and_data_pg(self) -> (ObjectKey, u32) {
        (self.object_key, self.data_pg_id)
    }
}

/// Storage-owned physical-placement selection for local topology UATs.
///
/// The production `PgTopology` API deliberately does not expose payload
/// data-PG prediction. This feature-gated facility expresses only the two
/// semantic placement relations required by the backfill and migration UATs.
pub trait PgTopologyPlacementTestSupport {
    fn test_find_object_key_with_distinct_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key_prefix: &str,
        target_data_pg: Option<u32>,
        excluded_metadata_pgs: &BTreeSet<u32>,
    ) -> Option<TestObjectDataPgSelection>;

    fn test_find_object_key_on_metadata_pg_with_distinct_data_pg(
        &self,
        bucket: &BucketName,
        key_prefix: &str,
        target_metadata_pg: u32,
        target_data_pg: Option<u32>,
    ) -> Option<TestObjectDataPgSelection>;
}

impl PgTopologyPlacementTestSupport for PgTopology {
    fn test_find_object_key_with_distinct_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key_prefix: &str,
        target_data_pg: Option<u32>,
        excluded_metadata_pgs: &BTreeSet<u32>,
    ) -> Option<TestObjectDataPgSelection> {
        let bucket_pg = self.bucket_pg_for(bucket);
        if excluded_metadata_pgs.contains(&bucket_pg) {
            return None;
        }
        let pg_count = self.pg_count();
        if target_data_pg.is_some_and(|target| !topology_contains_pg(self, target)) {
            return None;
        }
        let eligible_metadata_pg_count = pg_count.saturating_sub(
            excluded_metadata_pgs
                .iter()
                .filter(|pg_id| **pg_id < pg_count)
                .count() as u32,
        );
        if eligible_metadata_pg_count == 0 {
            return None;
        }
        let search_limit = distinct_data_pg_key_search_limit(
            pg_count,
            eligible_metadata_pg_count,
            target_data_pg.is_some(),
        );

        find_object_key(
            self,
            bucket,
            key_prefix,
            search_limit,
            |object_pg, data_pg| {
                data_pg != bucket_pg
                    && data_pg != object_pg
                    && !excluded_metadata_pgs.contains(&object_pg)
                    && target_data_pg.is_none_or(|target| data_pg == target)
            },
        )
    }

    fn test_find_object_key_on_metadata_pg_with_distinct_data_pg(
        &self,
        bucket: &BucketName,
        key_prefix: &str,
        target_metadata_pg: u32,
        target_data_pg: Option<u32>,
    ) -> Option<TestObjectDataPgSelection> {
        if !topology_contains_pg(self, target_metadata_pg)
            || target_data_pg.is_some_and(|target| {
                target == target_metadata_pg || !topology_contains_pg(self, target)
            })
        {
            return None;
        }
        let search_limit =
            distinct_data_pg_key_search_limit(self.pg_count(), 1, target_data_pg.is_some());
        find_object_key(
            self,
            bucket,
            key_prefix,
            search_limit,
            |object_pg, data_pg| {
                object_pg == target_metadata_pg
                    && data_pg != target_metadata_pg
                    && target_data_pg.is_none_or(|target| data_pg == target)
            },
        )
    }
}

fn topology_contains_pg(topology: &PgTopology, candidate: u32) -> bool {
    let mut contains = false;
    topology
        .for_each_pg(|pg_id| {
            contains |= pg_id == candidate;
            Ok::<(), std::convert::Infallible>(())
        })
        .expect("infallible topology iteration must succeed");
    contains
}

fn find_object_key(
    topology: &PgTopology,
    bucket: &BucketName,
    key_prefix: &str,
    search_limit: u32,
    mut accepts: impl FnMut(u32, u32) -> bool,
) -> Option<TestObjectDataPgSelection> {
    for suffix in 0..search_limit {
        let object_key = ObjectKey::try_from(format!("{key_prefix}-{suffix:04}")).ok()?;
        let object_pg = topology.object_pg_for(bucket, &object_key);
        let data_pg = topology
            .object_generation_segment_data_pg(bucket, &object_key, GenerationId::MIN, 0)
            .get();
        if accepts(object_pg, data_pg) {
            return Some(TestObjectDataPgSelection {
                object_key,
                data_pg_id: data_pg,
            });
        }
    }
    None
}

fn distinct_data_pg_key_search_limit(
    pg_count: u32,
    eligible_metadata_pg_count: u32,
    targets_exact_data_pg: bool,
) -> u32 {
    const MIN_SEARCH_LIMIT: u64 = 10_000;
    const EXPECTED_MATCH_SAFETY_FACTOR: u64 = 128;

    if !targets_exact_data_pg {
        return MIN_SEARCH_LIMIT as u32;
    }
    let estimated_attempts_per_match = u64::from(pg_count)
        .saturating_mul(u64::from(pg_count))
        .div_ceil(u64::from(eligible_metadata_pg_count));
    MIN_SEARCH_LIMIT
        .max(estimated_attempts_per_match.saturating_mul(EXPECTED_MATCH_SAFETY_FACTOR))
        .min(u64::from(u32::MAX)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_data_pg_search_scales_beyond_the_old_fixed_bound() {
        let topology = PgTopology::new(&(0..216).collect::<Vec<_>>()).unwrap();
        let excluded_metadata_pgs = (0..200).collect::<BTreeSet<_>>();
        let bucket =
            BucketName::try_from("argmin-s3-976110-18cf77b41c5bca93-14".to_string()).unwrap();
        let selection = topology
            .test_find_object_key_with_distinct_metadata_and_data_pgs(
                &bucket,
                "uat-route-change-new-object-61",
                Some(60),
                &excluded_metadata_pgs,
            )
            .expect("scaled search should find the retained soak target");
        let (key, data_pg) = selection.into_key_and_data_pg();

        assert_eq!(data_pg, 60);
        assert!(key.as_str().starts_with("uat-route-change-new-object-61-"));
        let suffix = key
            .as_str()
            .rsplit_once('-')
            .and_then(|(_, suffix)| suffix.parse::<u32>().ok())
            .expect("generated key should end in a numeric suffix");
        assert!(
            suffix >= 10_000,
            "regression must exceed the old fixed bound"
        );
        assert!(distinct_data_pg_key_search_limit(216, 16, true) > 10_000);
    }

    #[test]
    fn metadata_pg_relation_selection_keeps_data_pg_distinct() {
        let topology = PgTopology::new(&(0..32).collect::<Vec<_>>()).unwrap();
        let bucket = BucketName::try_from("placement-test-bucket".to_string()).unwrap();
        let selection = topology
            .test_find_object_key_on_metadata_pg_with_distinct_data_pg(
                &bucket,
                "placement-test-object",
                7,
                None,
            )
            .expect("test topology should contain the requested relation");
        let (key, data_pg) = selection.into_key_and_data_pg();

        assert_eq!(topology.object_pg_for(&bucket, &key), 7);
        assert_ne!(data_pg, 7);
    }

    #[test]
    fn metadata_and_data_pg_relation_selection_targets_both_pgs() {
        let topology = PgTopology::new(&(0..32).collect::<Vec<_>>()).unwrap();
        let bucket = BucketName::try_from("exact-placement-test-bucket".to_string()).unwrap();
        let selection = topology
            .test_find_object_key_on_metadata_pg_with_distinct_data_pg(
                &bucket,
                "exact-placement-test-object",
                7,
                Some(11),
            )
            .expect("test topology should contain the requested exact relation");
        let (key, data_pg) = selection.into_key_and_data_pg();

        assert_eq!(topology.object_pg_for(&bucket, &key), 7);
        assert_eq!(data_pg, 11);
        assert!(topology
            .test_find_object_key_on_metadata_pg_with_distinct_data_pg(
                &bucket,
                "invalid-exact-placement-test-object",
                7,
                Some(7),
            )
            .is_none());
    }

    #[test]
    fn exact_data_pg_selection_validates_sparse_topology_membership() {
        let topology = PgTopology::new(&[2, 8, 20]).unwrap();
        let bucket = BucketName::try_from("sparse-placement-test".to_string()).unwrap();

        assert!(topology
            .test_find_object_key_with_distinct_metadata_and_data_pgs(
                &bucket,
                "present-target",
                Some(8),
                &BTreeSet::new(),
            )
            .is_some());
        assert!(topology
            .test_find_object_key_with_distinct_metadata_and_data_pgs(
                &bucket,
                "missing-target",
                Some(1),
                &BTreeSet::new(),
            )
            .is_none());
    }
}
