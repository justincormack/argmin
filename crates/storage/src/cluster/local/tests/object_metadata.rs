use super::*;

#[test]
fn object_metadata_pending_install_race_drains_winner_and_retries() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "pending-install-race-");
    let first_key = key_for_object_pg(topology, &bucket, 2, "first-object-");
    let second_key = key_for_object_pg(topology, &bucket, 2, "second-object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    let first_committed = write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &first_key,
        [0x81; 16],
        b"first",
    );
    write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &second_key,
        [0x82; 16],
        b"second",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = second_key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let second_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>winner</Value></Tag></TagSet></Tagging>"
            .to_string();
    let hook_second_tags = second_tags.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "test-put-object-metadata-race",
        Some(second_key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &hook_bucket, &hook_key).unwrap();
            let live = stored.as_live().expect("test object is live").clone();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::PutObjectMetadata(Box::new(
                    PutObjectMetadataCommand::from_live_object_and_mutation(
                        live,
                        PutObjectMetadataMutation::PutTags(hook_second_tags.clone()),
                        hook_proof.clone(),
                    ),
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let first_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>retry</Value></Tag></TagSet></Tagging>";
    let tagged_version = first_cluster
        .put_object_tags_if(&bucket, &first_key, None, first_tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert_eq!(tagged_version, first_committed.version_id);
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for (key, tags) in [
        (&first_key, first_tags),
        (&second_key, second_tags.as_str()),
    ] {
        for node_id in node_ids {
            let pg = first_map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(2)
                .unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, key).unwrap();
            let live = stored.as_live().expect("test object is live");
            assert_eq!(
                live.tags.as_ref().map(crate::SerializedTagSet::as_str),
                Some(tags)
            );
        }
    }
}

#[test]
fn object_metadata_pending_install_race_reruns_precondition_action() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "pending-install-precondition-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &key,
        [0x83; 16],
        b"object",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let winner_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>winner</Value></Tag></TagSet></Tagging>"
            .to_string();
    let hook_winner_tags = winner_tags.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "test-put-object-metadata-race",
        Some(key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &hook_bucket, &hook_key).unwrap();
            let live = stored.as_live().expect("test object is live").clone();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::PutObjectMetadata(Box::new(
                    PutObjectMetadataCommand::from_live_object_and_mutation(
                        live,
                        PutObjectMetadataMutation::PutTags(hook_winner_tags.clone()),
                        hook_proof.clone(),
                    ),
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let loser_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>loser</Value></Tag></TagSet></Tagging>";
    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .put_object_tags_if(&bucket, &key, None, loser_tags, move |stored| {
            calls_for_action.fetch_add(1, Ordering::SeqCst);
            if stored
                .as_live()
                .expect("test object is live")
                .tags
                .is_some()
            {
                Err("tags already present")
            } else {
                Ok(stored.version_id())
            }
        })
        .unwrap();
    assert_eq!(result, Err("tags already present"));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "request action must be rerun after slot contention changes object state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("test object is live");
        assert_eq!(
            live.tags.as_ref().map(crate::SerializedTagSet::as_str),
            Some(winner_tags.as_str())
        );
    }
    assert_bucket_write_reservations_released(&first_map, &bucket);
}
