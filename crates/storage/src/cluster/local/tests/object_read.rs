use super::*;

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
fn object_read_snapshot_stale_retry_loop_is_bounded() {
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
    let call_count = std::cell::Cell::new(0_u8);

    let err = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                let attempt = call_count.get();
                call_count.set(attempt + 1);
                assert!(stored.as_live().is_some());
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
                Ok::<_, ()>(())
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io {
            context: "load object read snapshot stale retry limit exceeded",
            ..
        })
    ));
    assert_eq!(call_count.get(), 16);
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
    assert_eq!(
        tags.as_deref(),
        Some("<Tagging><TagSet><Tag><Key>state</Key><Value>new</Value></Tag></TagSet></Tagging>")
    );
}
