use storage::{BucketName, ObjectKey, PgTopology};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommittedObjectPlacement {
    pub generation_id: u64,
    pub data_pg_id: u32,
}

pub fn parse_committed_object_placement(body: &str) -> Result<CommittedObjectPlacement, String> {
    let generation_id = body
        .lines()
        .find_map(|line| line.strip_prefix("generation_id="))
        .ok_or_else(|| "missing generation_id".to_string())?
        .parse::<u64>()
        .map_err(|error| format!("invalid generation_id: {error}"))?;
    let segment_count = body
        .lines()
        .find_map(|line| line.strip_prefix("segment_count="))
        .ok_or_else(|| "missing segment_count".to_string())?
        .parse::<usize>()
        .map_err(|error| format!("invalid segment_count: {error}"))?;
    if segment_count != 1 {
        return Err(format!(
            "UAT placement requires one standard segment, got {segment_count}"
        ));
    }
    let segment = body
        .lines()
        .find(|line| line.starts_with("segment_index=0 "))
        .ok_or_else(|| "missing segment_index=0".to_string())?;
    let data_pg_id = segment
        .split_ascii_whitespace()
        .find_map(|field| field.strip_prefix("data_pg_id="))
        .ok_or_else(|| "missing segment-0 data_pg_id".to_string())?
        .parse::<u32>()
        .map_err(|error| format!("invalid data_pg_id: {error}"))?;
    Ok(CommittedObjectPlacement {
        generation_id,
        data_pg_id,
    })
}

pub fn committed_data_pg_satisfies_request(
    topology: &PgTopology,
    bucket: &str,
    key: &str,
    data_pg_id: u32,
    target_data_pg: Option<u32>,
) -> bool {
    if target_data_pg.is_some_and(|target| target != data_pg_id) {
        return false;
    }
    let bucket = BucketName::try_from(bucket.to_string()).expect("UAT bucket must be valid");
    let key = ObjectKey::try_from(key.to_string()).expect("UAT key must be valid");
    data_pg_id != topology.bucket_pg_for(&bucket)
        && data_pg_id != topology.object_pg_for(&bucket, &key)
}

pub fn distinct_data_pg_bucket_search_limit(pg_count: u32, eligible_metadata_pg_count: u32) -> u32 {
    const MIN_SEARCH_LIMIT: u64 = 100;
    const EXPECTED_ELIGIBLE_BUCKET_SAFETY_FACTOR: u64 = 128;

    MIN_SEARCH_LIMIT
        .max(
            u64::from(pg_count)
                .div_ceil(u64::from(eligible_metadata_pg_count))
                .saturating_mul(EXPECTED_ELIGIBLE_BUCKET_SAFETY_FACTOR),
        )
        .min(u64::from(u32::MAX)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_placement_parser_requires_one_standard_segment() {
        let parsed = parse_committed_object_placement(
            "generation_id=2\nsegment_count=1\nsegment_index=0 data_pg_id=12 placement_cluster_epoch=7\n",
        )
        .unwrap();
        assert_eq!(
            parsed,
            CommittedObjectPlacement {
                generation_id: 2,
                data_pg_id: 12,
            }
        );
        assert!(parse_committed_object_placement(
            "generation_id=2\nsegment_count=2\nsegment_index=0 data_pg_id=12 placement_cluster_epoch=7\n"
        )
        .is_err());
    }

    #[test]
    fn committed_placement_rejects_a_target_mismatch() {
        let topology = PgTopology::new(&(0..32).collect::<Vec<_>>()).unwrap();
        let bucket = "uat-placement-target-check";
        let key = "object";
        let bucket_name = BucketName::try_from(bucket.to_string()).unwrap();
        let object_key = ObjectKey::try_from(key.to_string()).unwrap();
        let selected_data_pg = (0..topology.pg_count())
            .find(|candidate| {
                *candidate != topology.bucket_pg_for(&bucket_name)
                    && *candidate != topology.object_pg_for(&bucket_name, &object_key)
            })
            .expect("test topology should contain a distinct committed data PG");
        let different_data_pg = (selected_data_pg + 1) % topology.pg_count();

        assert!(committed_data_pg_satisfies_request(
            &topology,
            bucket,
            key,
            selected_data_pg,
            Some(selected_data_pg),
        ));
        assert!(!committed_data_pg_satisfies_request(
            &topology,
            bucket,
            key,
            different_data_pg,
            Some(selected_data_pg),
        ));
    }

    #[test]
    fn distinct_data_pg_bucket_search_scales_for_large_excluded_metadata_set() {
        assert_eq!(distinct_data_pg_bucket_search_limit(216, 16), 1_792);
    }
}
