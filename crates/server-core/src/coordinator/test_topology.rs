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

pub(crate) fn find_key_on_same_object_metadata_pg_as(
    coord: &Coordinator,
    bucket: &str,
    reference: &str,
    prefix: &str,
) -> String {
    coord
        .storage_node()
        .test_find_object_key_on_same_metadata_pg_as(
            &trusted_bucket_name(bucket),
            &trusted_object_key(reference),
            prefix,
        )
        .unwrap_or_else(|| {
            panic!(
                "failed to find a key with prefix {prefix:?} on the same object metadata PG as {reference:?}"
            )
        })
        .into_string()
}

pub(crate) fn find_keys_on_same_object_metadata_pg(
    coord: &Coordinator,
    bucket: &str,
    prefixes: &[&str],
) -> Vec<String> {
    coord
        .storage_node()
        .test_find_object_keys_on_same_metadata_pg(&trusted_bucket_name(bucket), prefixes)
        .unwrap_or_else(|| {
            panic!(
                "failed to find {} keys on one object metadata PG",
                prefixes.len()
            )
        })
        .into_iter()
        .map(ObjectKey::into_string)
        .collect()
}

pub(crate) fn find_key_groups_in_object_metadata_scan_order(
    coord: &Coordinator,
    bucket: &str,
    group_prefixes: &[&str],
    keys_per_group: usize,
) -> Vec<Vec<String>> {
    assert!(keys_per_group > 0, "placement groups must not be empty");
    let first_prefixes = group_prefixes
        .iter()
        .map(|prefix| format!("{prefix}0/"))
        .collect::<Vec<_>>();
    let first_prefix_refs = first_prefixes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let representatives = coord
        .storage_node()
        .test_find_object_keys_on_metadata_pgs_in_scan_order(
            &trusted_bucket_name(bucket),
            &first_prefix_refs,
        )
        .unwrap_or_else(|| {
            panic!(
                "failed to place {} key groups in object metadata scan order",
                group_prefixes.len()
            )
        })
        .into_iter()
        .map(ObjectKey::into_string)
        .collect::<Vec<_>>();

    representatives
        .into_iter()
        .zip(group_prefixes)
        .map(|(representative, prefix)| {
            let mut keys = Vec::with_capacity(keys_per_group);
            keys.push(representative.clone());
            for index in 1..keys_per_group {
                keys.push(find_key_on_same_object_metadata_pg_as(
                    coord,
                    bucket,
                    &representative,
                    &format!("{prefix}{index}/"),
                ));
            }
            keys
        })
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
