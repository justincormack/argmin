use super::*;
use storage::test_support::StorageClusterTopologyTestSupport as _;

pub(crate) fn find_keys_on_distinct_object_metadata_pgs(
    coord: &Coordinator,
    bucket: &str,
    prefixes: &[&str],
) -> Vec<String> {
    coord
        .storage_node()
        .test_find_object_keys_on_distinct_metadata_pgs(&trusted_bucket_name(bucket), prefixes)
        .unwrap_or_else(|| {
            panic!(
                "failed to find {} keys on distinct object metadata PGs",
                prefixes.len()
            )
        })
        .into_iter()
        .map(ObjectKey::into_string)
        .collect()
}

pub(crate) fn find_key_with_object_pg_ne_bucket_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    coord
        .storage_node()
        .test_find_object_key_on_metadata_pg_distinct_from_bucket(
            &trusted_bucket_name(bucket),
            prefix,
        )
        .unwrap_or_else(|| {
            panic!("failed to find a key with object metadata PG distinct from bucket metadata PG")
        })
        .into_string()
}

pub(crate) fn find_key_with_object_pg_eq_bucket_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    coord
        .storage_node()
        .test_find_object_key_on_same_metadata_pg_as_bucket(&trusted_bucket_name(bucket), prefix)
        .unwrap_or_else(|| panic!("failed to find a key sharing the bucket metadata PG"))
        .into_string()
}

pub(crate) fn find_fresh_key_with_object_pg_gt_data_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    coord
        .storage_node()
        .test_find_fresh_object_key_with_metadata_pg_after_data_pg(
            &trusted_bucket_name(bucket),
            prefix,
        )
        .unwrap_or_else(|| {
            panic!("failed to find a key with object metadata PG ordered after its data PG")
        })
        .into_string()
}

pub(crate) fn assert_object_maps_object_pg_gt_data_pg(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) {
    assert!(
        coord
            .storage_node()
            .test_current_object_has_metadata_pg_after_data_pg(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
            )
            .unwrap(),
        "expected test object {bucket}/{key} to preserve the selected object/data cross-PG ordering"
    );
}

pub(crate) fn stream_put_session_has_cross_pg_segments(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    session_id: &SessionId,
) -> bool {
    coord
        .storage_node()
        .test_stream_put_session_crosses_metadata_and_data_pgs(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
        )
        .unwrap()
}
