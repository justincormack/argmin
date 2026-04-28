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

pub(crate) fn object_data_pg_id(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    generation_id: GenerationId,
) -> u32 {
    coord.storage_node.test_data_pg_id_for(
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

pub(crate) fn find_fresh_key_with_object_pg_gt_data_pg(
    coord: &Coordinator,
    bucket: &str,
    prefix: &str,
) -> String {
    for suffix in 0..1024 {
        let key = format!("{prefix}-{suffix}");
        if object_pg_id(coord, bucket, &key)
            > object_data_pg_id(coord, bucket, &key, GenerationId::MIN)
        {
            return key;
        }
    }
    panic!("failed to find a key with object_pg_id > data_pg_id");
}

pub(crate) fn assert_object_maps_object_pg_gt_data_pg(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) {
    let object_pg_id = object_pg_id(coord, bucket, key);
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
    let data_pg_id = object_data_pg_id(coord, bucket, key, generation_id);
    assert!(
        object_pg_id > data_pg_id,
        "expected test object {bucket}/{key} to map to object/data cross-PG ordering: object_pg_id={object_pg_id} data_pg_id={data_pg_id}"
    );
}

pub(crate) fn stream_put_session_has_cross_pg_segments(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    session_id: &SessionId,
) -> bool {
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let meta_pg_id = coord
        .storage_node
        .test_object_pg_id_for(&bucket_name, &object_key);
    let generation_id = coord
        .storage_node
        .test_object_generation_reservation_for(&bucket_name, &object_key, session_id)
        .unwrap();
    let data_pg_id =
        coord
            .storage_node
            .test_data_pg_id_for(&bucket_name, &object_key, generation_id);
    data_pg_id != meta_pg_id
}

pub(crate) fn multipart_part_data_pg_id(
    coord: &Coordinator,
    bucket: &BucketName,
    key: &ObjectKey,
    object_generation_id: GenerationId,
    part_number: u32,
) -> u32 {
    coord.storage_node.test_multipart_part_data_pg_id_for(
        bucket,
        key,
        object_generation_id,
        part_number,
    )
}
