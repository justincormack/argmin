use super::*;

pub(crate) fn bucket_pg_id(coord: &Coordinator, bucket: &str) -> u32 {
    coord
        .storage_node
        .test_bucket_pg_id_for(&trusted_bucket_name(bucket))
}

pub(crate) fn object_pg_id(coord: &Coordinator, bucket: &str, key: &str) -> u32 {
    coord
        .storage_node
        .test_object_pg_id_for(&trusted_bucket_name(bucket), &trusted_object_key(key))
}

pub(crate) fn shard_pg_id(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    generation_id: GenerationId,
) -> u32 {
    coord.storage_node.test_shard_pg_id_for(
        &trusted_bucket_name(bucket),
        &trusted_object_key(key),
        generation_id,
    )
}

pub(crate) fn object_pgs_differ(
    coord: &Coordinator,
    bucket: &str,
    key_a: &str,
    key_b: &str,
) -> bool {
    object_pg_id(coord, bucket, key_a) != object_pg_id(coord, bucket, key_b)
}

pub(crate) fn find_key_with_object_pg_distinct_from(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
    excluded_pg_ids: &[u32],
) -> String {
    for index in 0..10_000 {
        let key = format!("{prefix}-{index:04}");
        let pg_id = object_pg_id(coord, bucket, &key);
        if !excluded_pg_ids.contains(&pg_id) {
            return key;
        }
    }
    panic!("failed to find key for prefix {prefix}");
}

pub(crate) fn find_key_with_object_pg_ne_bucket_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    let bucket_pg_id = bucket_pg_id(coord, bucket);
    for suffix in 0..1024 {
        let key = format!("{prefix}-{suffix}");
        if object_pg_id(coord, bucket, &key) != bucket_pg_id {
            return key;
        }
    }
    panic!("failed to find a key with object_pg_id != bucket_pg_id");
}

pub(crate) fn find_key_with_object_pg_eq_bucket_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    let bucket_pg_id = bucket_pg_id(coord, bucket);
    for suffix in 0..1024 {
        let key = format!("{prefix}-{suffix}");
        if object_pg_id(coord, bucket, &key) == bucket_pg_id {
            return key;
        }
    }
    panic!("failed to find a key with object_pg_id == bucket_pg_id");
}

pub(crate) fn find_fresh_key_with_meta_pg_gt_shard_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    for suffix in 0..1024 {
        let key = format!("{prefix}-{suffix}");
        if object_pg_id(coord, bucket, &key) > shard_pg_id(coord, bucket, &key, GenerationId::MIN) {
            return key;
        }
    }
    panic!("failed to find a key with meta_pg_id > shard_pg_id");
}

pub(crate) fn assert_object_maps_meta_pg_gt_shard_pg(coord: &Coordinator, bucket: &str, key: &str) {
    let meta_pg_id = object_pg_id(coord, bucket, key);
    let generation_id = match coord
        .storage_node
        .test_get_object_meta(&trusted_bucket_name(bucket), &trusted_object_key(key))
        .unwrap()
    {
        StoredObject::Live(record) => record.generation_id,
        StoredObject::DeleteMarker(other) => {
            panic!("expected live object for {bucket}/{key}, got {other:?}")
        }
    };
    let shard_pg_id = shard_pg_id(coord, bucket, key, generation_id);
    assert!(
        meta_pg_id > shard_pg_id,
        "expected test object {bucket}/{key} to map to old read slow path: meta_pg_id={meta_pg_id} shard_pg_id={shard_pg_id}"
    );
}

pub(crate) fn stream_put_session_has_cross_pg_segments(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    session_id: &SessionId,
) -> bool {
    let meta_pg_id = object_pg_id(coord, bucket, key);
    let first_vid_pg = coord.shard_pg_id_raw(&format!("segment/{}", session_id.as_str()), "0", 1);
    let second_vid_pg = coord.shard_pg_id_raw(&format!("segment/{}", session_id.as_str()), "0", 2);
    first_vid_pg != meta_pg_id || second_vid_pg != meta_pg_id
}
