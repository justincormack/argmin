use super::*;
use crate::{SegmentStoredBytesRequest, StorageClusterRuntimeMapHandle};

#[test]
fn retained_object_payload_read_rejects_a_crossed_segment_descriptor() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"retained payload",
    );

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_object_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let outcome = route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let segment = outcome
        .snapshot()
        .object_segments
        .first()
        .expect("test object should have one segment")
        .clone();
    let live = outcome.snapshot().stored.as_live().unwrap();
    let request = SegmentStoredBytesRequest {
        data_pg_id: segment.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        stored_size: segment.size as usize + live.encryption.segment_ciphertext_extra_len(),
        segment_crc64: segment.segment_crc64,
        ec: EcShape {
            k: segment.ec_k,
            m: segment.ec_m,
        },
    };
    let (_, snapshot, leased_snapshot) = outcome.into_parts();
    assert!(Arc::ptr_eq(&snapshot, &leased_snapshot.snapshot));
    let retained = route
        .retain_object_payload_read(leased_snapshot)
        .unwrap()
        .expect("live object should retain payload authority");
    let mut bytes = Vec::new();
    retained
        .read_segment_payload_stored_bytes_into(
            segment.placement_cluster_epoch,
            request,
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"retained payload");

    let mut crossed = request;
    crossed.segment_okh[0] ^= 0xff;
    let error = retained
        .read_segment_payload_stored_bytes_into(
            segment.placement_cluster_epoch,
            crossed,
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn retained_object_payload_handoff_rejects_another_version_of_the_same_key() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [1; 16],
        [1; 16],
        b"older",
    );
    let newer = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [2; 16],
        [2; 16],
        b"newer",
    );
    assert_ne!(older.version_id, newer.version_id);

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let older_route = admission
        .active_object_read_route(
            &bucket,
            &key,
            Some(older.version_id),
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let (_, _snapshot, older_handoff) = older_route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap()
        .into_parts();
    let newer_route = admission
        .active_object_read_route(
            &bucket,
            &key,
            Some(newer.version_id),
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let error = match newer_route.retain_object_payload_read(older_handoff) {
        Ok(_) => panic!("another version's payload handoff must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn object_read_snapshot_fails_closed_while_metadata_pg_is_peering() {
    let tmp = test_util::tempdir();
    let mut map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(0))
        .unwrap()
        .state = PgState::Peering;
    let cluster = current_cluster(&map);

    let err = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |_stored| Ok::<_, ()>(()),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn object_read_snapshot_retries_when_object_changes_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let outcome = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                call_count.set(call_count.get() + 1);
                let generation_id = stored
                    .as_live()
                    .expect("test object should be live")
                    .generation_id;
                if !mutated.replace(true) {
                    assert_eq!(generation_id, original.generation_id);
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [2; 16],
                        [2; 16],
                        b"second",
                    );
                }
                Ok::<_, ()>(generation_id)
            },
        )
        .unwrap()
        .unwrap();

    let snapshot_generation = outcome
        .snapshot
        .stored
        .as_live()
        .expect("snapshot should contain the replacement live object")
        .generation_id;
    assert_eq!(call_count.get(), 2);
    assert_ne!(snapshot_generation, original.generation_id);
    assert_eq!(outcome.value, snapshot_generation);
}

#[test]
fn leased_object_read_snapshot_retries_when_subject_changes_before_exact_snapshot() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let outcome = cluster
        .load_leased_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |stored| {
                call_count.set(call_count.get() + 1);
                let generation_id = stored
                    .as_live()
                    .expect("test object should be live")
                    .generation_id;
                if !mutated.replace(true) {
                    assert_eq!(generation_id, original.generation_id);
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [2; 16],
                        [2; 16],
                        b"second",
                    );
                }
                Ok::<_, ()>(generation_id)
            },
        )
        .unwrap()
        .unwrap();

    let snapshot_generation = outcome
        .snapshot()
        .stored
        .as_live()
        .expect("snapshot should contain the replacement live object")
        .generation_id;
    assert_eq!(call_count.get(), 2);
    assert_ne!(snapshot_generation, original.generation_id);
    let (value, _snapshot, _leased_snapshot) = outcome.into_parts();
    assert_eq!(value, snapshot_generation);
}

#[test]
fn leased_object_read_snapshot_blocks_reclaim_until_handoff() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );

    let outcome = cluster
        .load_leased_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();
    let (_, _snapshot, leased_snapshot) = outcome.into_parts();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [2; 16],
        [2; 16],
        b"second",
    );
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, original.generation_id)
        .unwrap());
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, original.generation_id)
            .unwrap(),
        "broad snapshot lease must block reclaim before the read-handle lease handoff"
    );

    drop(leased_snapshot);
    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, original.generation_id)
            .unwrap(),
        "reclaim should proceed after the broad snapshot lease is released"
    );
}

#[test]
fn object_read_snapshot_retries_when_object_disappears_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let deleted = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let err = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                call_count.set(call_count.get() + 1);
                assert!(stored.as_live().is_some());
                if !deleted.replace(true) {
                    let object_pg = map
                        .node(NodeId::new(0))
                        .unwrap()
                        .storage_node()
                        .get_pg(cluster.object_metadata_pg_id(&bucket, &key))
                        .unwrap();
                    crate::PgMetadataStore::delete_object_meta(&*object_pg, &bucket, &key).unwrap();
                }
                Ok::<_, ()>(())
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(crate::MetadataError::ObjectNotFound)
    ));
    assert_eq!(call_count.get(), 1);
}

#[test]
fn object_read_snapshot_stale_retry_uses_time_budget() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    const STALE_SNAPSHOTS: u8 = 24;
    let call_count = std::cell::Cell::new(0_u8);

    let outcome = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                let attempt = call_count.get();
                call_count.set(attempt + 1);
                assert!(stored.as_live().is_some());
                if attempt < STALE_SNAPSHOTS {
                    let mutation_seed = attempt + 2;
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [mutation_seed; 16],
                        [mutation_seed; 16],
                        &[mutation_seed],
                    );
                }
                Ok::<_, ()>(())
            },
        )
        .unwrap()
        .unwrap();

    assert!(outcome.snapshot.stored.as_live().is_some());
    assert_eq!(call_count.get(), STALE_SNAPSHOTS + 1);
}

#[test]
fn object_tag_read_retries_when_tags_change_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let written = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"tagged",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let tags = cluster
            .get_object_tags_if(&bucket, &key, None, |stored| {
                call_count.set(call_count.get() + 1);
                let live = stored.as_live().expect("test object should be live");
                assert_eq!(live.generation_id, written.generation_id);
                if !mutated.replace(true) {
                    cluster
                        .put_object_tags_if(
                            &bucket,
                            &key,
                            None,
                            "<Tagging><TagSet><Tag><Key>state</Key><Value>new</Value></Tag></TagSet></Tagging>",
                            |stored| {
                                Ok::<_, ()>(
                                    stored.as_live().expect("test object should be live").version_id,
                                )
                            },
                        )
                        .unwrap()
                        .unwrap();
                }
                Ok::<_, ()>(live.version_id)
            })
            .unwrap()
            .unwrap();

    assert_eq!(call_count.get(), 2);
    let expected = crate::tests::object_tags(
        "<Tagging><TagSet><Tag><Key>state</Key><Value>new</Value></Tag></TagSet></Tagging>",
    );
    assert_eq!(
        tags.as_ref().map(crate::SerializedTagSet::tag_set),
        Some(expected.tag_set())
    );
}
