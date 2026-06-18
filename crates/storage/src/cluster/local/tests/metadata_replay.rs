use super::*;

#[test]
fn opens_distinct_local_node_stores_with_static_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();

    assert_eq!(map.epoch(), ClusterEpoch::INITIAL);
    assert_eq!(map.metadata_primary_node_id(), NodeId::new(0));
    assert_eq!(map.node_count(), 3);
    assert_eq!(map.node_ids().collect::<Vec<_>>(), node_ids);
    assert_eq!(map.pg_routes().count(), 4);
    let route = map.pg_route(PgId::new(2)).unwrap();
    assert_eq!(route.cluster_epoch(), ClusterEpoch::INITIAL);
    assert_eq!(route.pg_id(), PgId::new(2));
    assert_eq!(route.primary_node_id(), NodeId::new(0));
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.acting_set(), node_ids);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap();
        assert_eq!(node.node_id(), node_id);
        assert!(node
            .data_dir()
            .ends_with(format!("node-{:04}", node_id.as_u32())));
        assert!(node.data_dir().join("pg-0000").is_dir());
        assert!(node.data_dir().join("pg-0003").is_dir());
    }
    assert_ne!(
        map.node(NodeId::new(0)).unwrap().data_dir(),
        map.node(NodeId::new(1)).unwrap().data_dir()
    );
}

#[test]
fn opens_frontend_topology_only_map_without_pg_stores() {
    let node_ids = [NodeId::new(0), NodeId::new(1)];
    let ec_shape = EcShape { k: 1, m: 1 };
    let epoch = ClusterEpoch::new(9).unwrap();
    let map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        NodeId::new(1),
        node_ids,
        &[0, 3],
        ec_shape,
        epoch,
    )
    .unwrap();

    assert_eq!(map.epoch(), epoch);
    assert_eq!(map.metadata_primary_node_id(), NodeId::new(1));
    assert_eq!(map.node_count(), 2);
    assert_eq!(map.node_ids().collect::<Vec<_>>(), node_ids);
    let route = map.pg_route(PgId::new(3)).unwrap();
    assert_eq!(route.cluster_epoch(), epoch);
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), node_ids);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap();
        assert_eq!(node.data_dir(), Path::new(""));
        assert_eq!(node.storage_node().pg_ids(), &[0, 3]);
        assert!(matches!(
            node.storage_node().get_pg(0),
            Err(StoreError::PgNotFound { pg_id: 0 })
        ));
    }
}

#[test]
fn opens_frontend_topology_with_supplied_pg_routes() {
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let epoch = ClusterEpoch::new(7).unwrap();
    let ec_shape = EcShape { k: 2, m: 1 };
    let acting_set = Arc::<[NodeId]>::from(vec![NodeId::new(2), NodeId::new(1)]);
    let routes = vec![
        LocalPgRoute::active(epoch, PgId::new(0), NodeId::new(2), Arc::clone(&acting_set)),
        LocalPgRoute::active(epoch, PgId::new(1), NodeId::new(2), Arc::clone(&acting_set)),
    ];

    let map = LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(0),
        node_ids,
        &[0, 1],
        ec_shape,
        epoch,
        routes,
    )
    .unwrap();

    assert_eq!(map.epoch(), epoch);
    assert_eq!(map.metadata_primary_node_id(), NodeId::new(0));
    assert_eq!(map.pg_routes().count(), 2);
    let route = map.pg_route(PgId::new(1)).unwrap();
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(route.cluster_epoch(), epoch);
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(map.route_map_valid_until_ms(), None);
    assert!(map.is_route_map_valid_at(u64::MAX));
    map.require_route_map_valid_at(u64::MAX).unwrap();
}

#[test]
fn supplied_pg_routes_can_carry_runtime_validity_deadline() {
    let node_ids = [NodeId::new(0), NodeId::new(1)];
    let epoch = ClusterEpoch::new(7).unwrap();
    let ec_shape = EcShape { k: 1, m: 1 };
    let acting_set = Arc::<[NodeId]>::from(vec![NodeId::new(1)]);
    let route = LocalPgRoute::active(epoch, PgId::new(0), NodeId::new(1), acting_set);

    let map = LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
        NodeId::new(0),
        node_ids,
        &[0],
        ec_shape,
        epoch,
        vec![route],
        Some(1_500),
    )
    .unwrap();

    assert_eq!(map.route_map_valid_until_ms(), Some(1_500));
    assert!(map.is_route_map_valid_at(1_499));
    map.require_route_map_valid_at(1_499).unwrap();
    assert!(!map.is_route_map_valid_at(1_500));
    assert!(matches!(
        map.require_route_map_valid_at(1_500),
        Err(StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: 1_500,
            now_ms: 1_500,
        }) if cluster_epoch == epoch
    ));
}

#[test]
fn expired_route_maps_reject_metadata_routing() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, BucketName::new("bucket").unwrap());
    map.route_map_valid_until_ms = Some(crate::clock::current_time_millis().saturating_add(60_000));

    map.metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    map.metadata_pg_acting_nodes(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    map.validate_metadata_command_for_replica(NodeId::new(0), NodeId::new(1), pg_id, &command)
        .unwrap();

    let expired_at = crate::clock::current_time_millis();
    map.route_map_valid_until_ms = Some(expired_at);
    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if valid_until_ms == expired_at && now_ms >= expired_at
    ));

    let err = map
        .metadata_pg_acting_nodes(ClusterEpoch::INITIAL, pg_id)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if valid_until_ms == expired_at && now_ms >= expired_at
    ));

    let err = map
        .validate_metadata_command_for_replica(NodeId::new(0), NodeId::new(1), pg_id, &command)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if valid_until_ms == expired_at && now_ms >= expired_at
    ));
}

#[test]
fn supplied_pg_routes_must_match_configured_epoch_and_pg_set() {
    let node_ids = [NodeId::new(0), NodeId::new(1)];
    let epoch = ClusterEpoch::new(7).unwrap();
    let stale_epoch = ClusterEpoch::new(6).unwrap();
    let ec_shape = EcShape { k: 1, m: 1 };
    let acting_set = Arc::<[NodeId]>::from(vec![NodeId::new(0), NodeId::new(1)]);

    let stale_route = LocalPgRoute::active(
        stale_epoch,
        PgId::new(0),
        NodeId::new(0),
        Arc::clone(&acting_set),
    );
    assert!(matches!(
        LocalClusterMap::open_frontend_topology_only_with_pg_routes(
            NodeId::new(0),
            node_ids,
            &[0],
            ec_shape,
            epoch,
            vec![stale_route],
        ),
        Err(ClusterBuildError::RouteClusterEpochMismatch {
            pg_id: 0,
            route_epoch,
            cluster_epoch,
        }) if route_epoch == stale_epoch && cluster_epoch == epoch
    ));

    let only_route =
        LocalPgRoute::active(epoch, PgId::new(0), NodeId::new(0), Arc::clone(&acting_set));
    assert!(matches!(
        LocalClusterMap::open_frontend_topology_only_with_pg_routes(
            NodeId::new(0),
            node_ids,
            &[0, 1],
            ec_shape,
            epoch,
            vec![only_route],
        ),
        Err(ClusterBuildError::MissingPgRoute { pg_id: 1 })
    ));
}

#[test]
fn metadata_pg_primary_node_routes_by_pg_primary_and_fails_closed() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();

    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.primary_node_id = NodeId::new(2);
    }
    assert_eq!(
        map.metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap()
            .node_id(),
        NodeId::new(2)
    );

    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.cluster_epoch = ClusterEpoch::new(2).unwrap();
    }
    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataRoute {
            pg_id: 1,
            route_epoch,
            current_epoch,
        } if route_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.cluster_epoch = ClusterEpoch::INITIAL;
    }

    let err = map
        .metadata_pg_primary_node(ClusterEpoch::new(2).unwrap(), PgId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataOperation {
            pg_id: 1,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));

    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.state = PgState::Peering;
    }
    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch,
            state: PgState::Peering,
        } if cluster_epoch == ClusterEpoch::INITIAL
    ));

    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.state = PgState::Active;
        route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
        route.primary_node_id = NodeId::new(2);
    }
    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id: 2,
            pg_id: 1,
            cluster_epoch,
        } if cluster_epoch == ClusterEpoch::INITIAL
    ));

    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.acting_set = Arc::from([NodeId::new(99)]);
        route.primary_node_id = NodeId::new(99);
    }
    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::NodeNotFound {
            node_id: 99,
            pg_id: 1,
            cluster_epoch,
        } if cluster_epoch == ClusterEpoch::INITIAL
    ));

    let err = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(9))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::ClusterPgNotFound {
            pg_id: 9,
            cluster_epoch,
        } if cluster_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn metadata_routes_reject_all_non_active_pg_states() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "non-active-route-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        map.pg_routes.get_mut(&PgId::new(1)).unwrap().state = state;

        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .metadata_pg_acting_nodes(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .validate_metadata_command_for_replica(
                NodeId::new(0),
                NodeId::new(1),
                PgId::new(1),
                &command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .validate_metadata_command_abandon_for_replica(
                NodeId::new(0),
                NodeId::new(1),
                PgId::new(1),
                &command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));
    }
}

#[test]
fn metadata_write_fails_closed_when_required_replica_is_missing() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "strict-metadata-replica-");
    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.primary_node_id = NodeId::new(1);
        route.acting_set = Arc::from([NodeId::new(0), NodeId::new(99), NodeId::new(1)]);
    }

    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let err = cluster
        .create_bucket_with_config_and_load_info(&config)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::NodeNotFound {
            node_id: 99,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
        })
    ));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "failed strict write should keep the command pending for replica recovery"
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }

    drop(cluster);
    {
        let route = Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&PgId::new(1))
            .unwrap();
        route.acting_set = Arc::from(node_ids);
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    cluster
        .create_bucket_with_config_and_load_info(&config)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
}

#[test]
fn create_bucket_rehydrates_durable_pending_slot_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "rehydrate-create-");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let pg_id = PgId::new(1);
    let command = {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let command = CreateBucketCommand::from_config(
            &config,
            123,
            primary_pg
                .next_bucket_execution_generation_candidate()
                .unwrap(),
        )
        .unwrap();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(command),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        command
    };
    drop(map);

    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert_eq!(
            primary_pg
                .pending_metadata_command_envelope(0, ClusterEpoch::INITIAL)
                .unwrap()
                .unwrap(),
            command
        );
    }

    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    cluster
        .create_bucket_with_config_and_load_info(&config)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_acl_rehydration_preserves_completed_multipart_sequence_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "rehydrate-acl-sequence-");
    let pg_id = PgId::new(1);
    let acl_grants = crate::AclGrants::default();
    {
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        assert_eq!(
            cluster
                .test_reserve_completed_multipart_upload_order(&bucket)
                .unwrap(),
            1
        );
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        assert_eq!(current.completed_multipart_upload_sequence, 1);
        let command_id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(
                primary_pg
                    .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                    .unwrap()
                    + 1,
            )
            .unwrap(),
        );
        let target_generation = primary_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                current.with_execution_generation(target_generation),
                acl_grants.clone(),
                true,
                false,
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
    }
    drop(map);

    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let info = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap();
    assert!(info.public_read);
    assert!(!info.public_write);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let row = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(row.completed_multipart_upload_sequence, 1);
        assert!(row.public_read);
        assert!(!row.public_write);
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn object_generation_reservation_rehydrates_durable_pending_slot_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "rehydrate-object-reserve-");
    let key = key_for_object_pg(topology, &bucket, 2, "rehydrate-object-key-");
    let pg_id = PgId::new(2);
    let reservation_id = crate::SessionId::try_from("72".repeat(16)).unwrap();
    let command = {
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                crate::GenerationId::MIN,
                123,
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        command
    };
    drop(map);

    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            primary_pg
                .pending_metadata_command_envelope(0, ClusterEpoch::INITIAL)
                .unwrap()
                .unwrap(),
            command
        );
    }

    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    assert_eq!(
        cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap(),
        crate::GenerationId::MIN
    );

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn stream_segment_vid_allocation_survives_reopen_without_runtime_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-vid-bucket-");
    let key = key_for_object_pg(topology, &bucket, 2, "stream-vid-key-");
    let session_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
    {
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let (_target, first) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: 16,
                    segment_crc64: 1,
                    segment_okh: [0x73; 16],
                },
            )
            .unwrap();
        assert_eq!(first.segment_vid, crate::GenerationId::MIN);
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap()
            .node_id();
        assert_stream_next_segment_vid(&map, primary, 2, &session_id, 2);
    }
    drop(map);

    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let (_target, second) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: 32,
                segment_crc64: 2,
                segment_okh: [0x74; 16],
            },
        )
        .unwrap();
    assert_eq!(second.segment_vid, crate::GenerationId::new(2).unwrap());
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .node_id();
    assert_stream_next_segment_vid(&map, primary, 2, &session_id, 3);
}

#[test]
fn stream_segment_vid_allocation_is_visible_to_already_open_handle() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let first_map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let second_map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-vid-open-bucket-");
    let key = key_for_object_pg(topology, &bucket, 2, "stream-vid-open-key-");
    let session_id = crate::SessionId::try_from("74".repeat(16)).unwrap();
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();

    create_test_bucket(&first_cluster, &bucket);
    first_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let (_target, first) = first_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: 16,
                segment_crc64: 1,
                segment_okh: [0x74; 16],
            },
        )
        .unwrap();
    let (_target, second) = second_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: 32,
                segment_crc64: 2,
                segment_okh: [0x75; 16],
            },
        )
        .unwrap();

    assert_eq!(first.segment_vid, crate::GenerationId::MIN);
    assert_eq!(second.segment_vid, crate::GenerationId::new(2).unwrap());
    for map in [&first_map, &second_map] {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap()
            .node_id();
        assert_stream_next_segment_vid(map, primary, 2, &session_id, 3);
    }
}

#[test]
fn metadata_command_replica_acceptance_rejects_invalid_route_context() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "replica-acceptance-");
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
    {
        let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
        route.primary_node_id = NodeId::new(1);
    }

    let err = map
        .validate_metadata_command_for_replica(
            NodeId::new(0),
            NodeId::new(2),
            PgId::new(1),
            &command,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandFromNonPrimary {
            node_id: 2,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            origin_node_id: 0,
            primary_node_id: 1,
        }
    ));

    let stale_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(2).unwrap(),
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        command.payload().clone(),
    );
    let err = map
        .validate_metadata_command_for_replica(
            NodeId::new(1),
            NodeId::new(2),
            PgId::new(1),
            &stale_command,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataCommand {
            node_id: 2,
            pg_id: 1,
            command_epoch,
            current_epoch: ClusterEpoch::INITIAL,
        } if command_epoch == ClusterEpoch::new(2).unwrap()
    ));

    let err = map
        .validate_metadata_command_for_replica(
            NodeId::new(1),
            NodeId::new(2),
            PgId::new(0),
            &command,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandWrongPg {
            node_id: 2,
            command_pg_id: 1,
            target_pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));

    let err = map
        .validate_metadata_command_for_replica(
            NodeId::new(1),
            NodeId::new(99),
            PgId::new(1),
            &command,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id: 99,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
}

#[test]
fn metadata_command_apply_validates_origin_and_duplicate_conflict_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "accepted-command-");
    let conflict_bucket = bucket_for_pg(topology, 1, "conflicting-command-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket.clone());

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &first_command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandFromNonPrimary {
            node_id: 1,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            origin_node_id: 0,
            primary_node_id: 1,
        })
    ));
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &first_bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
        .unwrap();
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
        .unwrap();
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert_eq!(info.name, first_bucket);
    }

    let conflicting_command =
        create_bucket_metadata_command(PgId::new(1), 1, conflict_bucket.clone());
    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &conflicting_command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
        })
    ));
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &conflict_bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }
}

#[test]
fn metadata_command_apply_accepts_unseen_lower_index_after_higher_index() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let earlier_bucket = bucket_for_pg(topology, 1, "earlier-command-");
    let later_bucket = bucket_for_pg(topology, 1, "later-command-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let earlier_command = create_bucket_metadata_command(PgId::new(1), 1, earlier_bucket.clone());
    let later_command = create_bucket_metadata_command(PgId::new(1), 2, later_bucket.clone());

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &later_command)
        .unwrap();
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        let later = crate::PgMetadataStore::head_bucket(&*pg, &later_bucket).unwrap();
        assert_eq!(later.name, later_bucket);
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &earlier_command)
        .unwrap();

    let first_hash = metadata_command_log_hash(
        ClusterEpoch::INITIAL,
        PgId::new(1),
        MetadataCommandLogIndex::new(1).unwrap(),
        0,
        earlier_command.checksum_crc64(),
    );
    let second_hash = metadata_command_log_hash(
        ClusterEpoch::INITIAL,
        PgId::new(1),
        MetadataCommandLogIndex::new(2).unwrap(),
        first_hash,
        later_command.checksum_crc64(),
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_hash, second_hash);
        let earlier = crate::PgMetadataStore::head_bucket(&*pg, &earlier_bucket).unwrap();
        assert_eq!(earlier.name, earlier_bucket);
        let later = crate::PgMetadataStore::head_bucket(&*pg, &later_bucket).unwrap();
        assert_eq!(later.name, later_bucket);
    }
}

#[test]
fn metadata_command_log_state_survives_local_cluster_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "durable-command-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        bucket
    };

    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        let info = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
}

#[test]
fn local_cluster_reopen_preserves_mixed_storage_cluster_history() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let (bucket, key, object_pg, committed, before_reopen) = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
        let (bucket, key, object_pg, data_pg) = {
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "replay-harness-");
            let key = key_for_object_pg(topology, &bucket, 2, "replay-object-");
            let data_pg = topology
                .object_generation_segment_data_pg(&bucket, &key, crate::GenerationId::MIN, 0)
                .get();
            let object_pg = topology.object_pg_for(&bucket, &key);
            (bucket, key, object_pg, data_pg)
        };
        assert_eq!(object_pg, 2);
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, object_pg, NodeId::new(2));
        set_route_primary(&mut map, data_pg, NodeId::new(0));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        put_test_lifecycle(&cluster, &bucket);
        let committed = write_committed_direct_segment_for_with_versioning(
            &cluster,
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            [91; 16],
            [92; 16],
            b"phase 7.4 replay harness object",
        );
        let tags =
            "<Tagging><TagSet><Tag><Key>phase</Key><Value>7.4</Value></Tag></TagSet></Tagging>";
        let tagged_version = cluster
            .put_object_tags_if(&bucket, &key, None, tags, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap()
            .unwrap();
        assert_eq!(tagged_version, committed.version_id);
        let before_reopen = collect_metadata_replay_snapshot(&map, &node_ids, &pg_ids);
        drop(cluster);
        drop(map);
        (bucket, key, object_pg, committed, before_reopen)
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    set_route_primary(&mut reopened, object_pg, NodeId::new(2));
    let after_reopen = collect_metadata_replay_snapshot(&reopened, &node_ids, &pg_ids);
    assert_eq!(after_reopen, before_reopen);

    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, committed.version_id)
                .unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(
            live.tags.as_ref().map(|tags| tags.as_str()),
            Some(
                "<Tagging><TagSet><Tag><Key>phase</Key><Value>7.4</Value></Tag></TagSet></Tagging>"
            )
        );
        assert_eq!(live.size, committed.payload.len() as u64);
    }
}

#[test]
fn local_cluster_reopen_rejects_missing_applied_command_log_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "missing-log-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute("DELETE FROM metadata_command_log WHERE log_index = 1", [])
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::MetadataCommandLogConflict {
                pg_id: 1,
                log_index: 1,
                ..
            }
        }
    ));
}

#[test]
fn local_cluster_reopen_rejects_pending_slot_on_non_primary_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "non-primary-pending-slot-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        let non_primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        non_primary_pg
            .try_insert_pending_metadata_command_slot(1, &command, Some(&bucket))
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 1,
            source: StoreError::MetadataCommandPendingOnNonPrimary {
                node_id: 1,
                primary_node_id: 0,
                pg_id: 1,
                ..
            }
        }
    ));
}

#[test]
fn local_cluster_reopen_cleans_terminal_primary_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "terminal-primary-pending-slot-");
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
            .unwrap();
        force_insert_pending_metadata_command_for_test(&map, PgId::new(1), &bucket, &command);
        bucket
    };

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    assert_clean_metadata_command_stream(&reopened, &[1]);
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn local_cluster_reopen_rejects_nonprimary_applied_mismatched_primary_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "nonprimary-primary-pending-");
        let divergent_bucket = bucket_for_pg(topology, 1, "nonprimary-divergent-applied-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        let divergent_command = create_bucket_metadata_command(PgId::new(1), 1, divergent_bucket);
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        primary_pg
            .try_insert_pending_metadata_command_slot(
                NodeId::new(0).as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();
        let nonprimary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        nonprimary_pg
            .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &divergent_command)
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 1,
                source: StoreError::MetadataCommandReplicaStateDiverged {
                    pg_id: 1,
                    reference_node_id: 0,
                    applied_log_index: 1,
                    reference_applied_log_index: 0,
                    ..
                }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn local_cluster_reopen_converges_inflight_primary_pending_before_snapshots() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "inflight-converge-before-read-");
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        let pg_id = PgId::new(1);
        let acl_grants = crate::AclGrants::default();
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let current =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        let log_index = MetadataCommandLogIndex::new(
            primary_pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1,
        )
        .unwrap();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
                acl_grants,
                true,
                false,
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(
                NodeId::new(0).as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
                .unwrap();
        }

        let stale_primary =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        assert!(
            !stale_primary.public_read,
            "test setup should leave the primary materialized row stale"
        );
        bucket
    };

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    let snapshot = cluster
        .load_bucket_snapshot(&bucket, crate::BucketSnapshotRequest::default())
        .unwrap();
    assert!(
        snapshot.bucket.public_read,
        "reopen must converge the primary pending slot before snapshots can serve the bucket"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let row = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert!(row.public_read);
        assert!(!row.public_write);
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn local_cluster_reopen_converges_primary_terminal_pending_before_slot_cleanup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "primary-terminal-pending-reopen-");
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        let pg_id = PgId::new(1);
        let acl_grants = crate::AclGrants::default();
        let command = {
            let primary_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            let current =
                crate::traits::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket)
                    .unwrap();
            let log_index = MetadataCommandLogIndex::new(
                primary_pg
                    .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                    .unwrap()
                    + 1,
            )
            .unwrap();
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
                MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                    current.with_execution_generation(
                        primary_pg
                            .next_bucket_execution_generation_candidate()
                            .unwrap(),
                    ),
                    acl_grants,
                    true,
                    false,
                )),
            );
            primary_pg
                .try_insert_pending_metadata_command_slot(
                    NodeId::new(0).as_u32(),
                    &command,
                    Some(&bucket),
                )
                .unwrap();
            command
        };
        for node_id in [NodeId::new(1), NodeId::new(0)] {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
                .unwrap();
        }
        let stale_replica_pg = map
            .node(NodeId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let stale_replica =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*stale_replica_pg, &bucket)
                .unwrap();
        assert!(
            !stale_replica.public_read,
            "test setup should leave one non-primary replica unadvanced"
        );
        bucket
    };

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    assert_clean_metadata_command_stream(&reopened, &[1]);
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let row = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert!(row.public_read);
        assert!(!row.public_write);
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn local_cluster_reopen_rejects_inflight_matching_log_with_divergent_advanced_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "inflight-divergent-digest-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        primary_pg
            .try_insert_pending_metadata_command_slot(
                NodeId::new(0).as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
                .unwrap();
        }
        let divergent_pg = map
            .node(NodeId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        divergent_pg
            .connection()
            .execute(
                "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                rusqlite::params![bucket.as_str()],
            )
            .unwrap();
        divergent_pg
            .refresh_metadata_command_state_digest()
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 2,
                source: StoreError::MetadataCommandReplicaStateDiverged {
                    pg_id: 1,
                    reference_node_id: 1,
                    applied_log_index: 1,
                    reference_applied_log_index: 1,
                    ..
                }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn local_cluster_reopen_recovers_abandoned_log_tail_without_nonprimary_pending_slots() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "abandoned-tail-reopen-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            if node_id == NodeId::new(0) {
                pg.try_insert_pending_metadata_command_slot(
                    node_id.as_u32(),
                    &command,
                    Some(&bucket),
                )
                .unwrap();
            }
            pg.connection()
                    .execute(
                        "INSERT INTO metadata_command_log \
                         (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                         VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
                        rusqlite::params![
                            ClusterEpoch::INITIAL.get() as i64,
                            1_i64,
                            1_i64,
                            command.abandoned_log_checksum_crc64() as i64,
                            command.abandoned_log_bytes(),
                        ],
                    )
                    .unwrap();
        }
        bucket
    };

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    assert_clean_metadata_command_stream(&reopened, &[1]);
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
        assert!(pg
            .pending_metadata_command_slot(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn local_cluster_reopen_rejects_reordered_applied_command_log_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "reordered-log-first-");
        let second_bucket = bucket_for_pg(topology, 1, "reordered-log-second-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket);
        let second_command = create_bucket_metadata_command(PgId::new(1), 2, second_bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE metadata_command_log \
                     SET command_checksum = ?1, command_bytes = ?2 \
                     WHERE log_index = 1",
                rusqlite::params![
                    second_command.checksum_crc64() as i64,
                    second_command.command_bytes(),
                ],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::MetadataCommandLogConflict {
                pg_id: 1,
                log_index: 1,
                ..
            }
        }
    ));
}

#[test]
fn local_cluster_reopen_rejects_corrupt_applied_command_log_hash() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "corrupt-log-hash-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE metadata_command_log SET previous_log_hash = ?1 WHERE log_index = 1",
                rusqlite::params![123_i64],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::MetadataCommandLogHashMismatch {
                pg_id: 1,
                log_index: 1,
                ..
            }
        }
    ));
}

#[test]
fn local_cluster_reopen_rejects_materialized_state_digest_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "corrupt-state-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                rusqlite::params![bucket.as_str()],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
        }
    ));
}

#[test]
fn local_cluster_reopen_rejects_replica_materialized_state_digest_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "corrupt-replica-state-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        let replica_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        replica_pg
            .connection()
            .execute(
                "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                rusqlite::params![bucket.as_str()],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 1,
                source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn local_cluster_reopen_rejects_missing_replica_state_for_nonempty_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "missing-replica-state-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "DELETE FROM metadata_command_replica_state WHERE singleton = 0",
                [],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::MetadataCommandReplicaStateMissing { pg_id: 1 }
        }
    ));
}

#[test]
fn local_cluster_reopen_rejects_replica_state_disagreement() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "replica-agree-first-");
        let second_bucket = bucket_for_pg(topology, 1, "replica-agree-second-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket);
        let second_command = create_bucket_metadata_command(PgId::new(1), 2, second_bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();
        let stale_state = {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            (
                node_zero_pg.metadata_command_replica_state().unwrap(),
                node_zero_pg
                    .connection()
                    .query_row(
                        "SELECT next_bucket_execution_generation \
                             FROM pg_counters WHERE singleton = 0",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
            )
        };
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
            .unwrap();
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute("DELETE FROM metadata_command_log WHERE log_index = 2", [])
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "DELETE FROM buckets WHERE name = ?1",
                rusqlite::params![second_bucket.as_str()],
            )
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE pg_counters \
                     SET next_bucket_execution_generation = ?1 \
                     WHERE singleton = 0",
                rusqlite::params![stale_state.1],
            )
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE metadata_command_replica_state \
                     SET cluster_epoch = ?1, applied_log_index = ?2, \
                         applied_log_hash = ?3, state_digest = ?4 \
                     WHERE singleton = 0",
                rusqlite::params![
                    stale_state.0.cluster_epoch.get() as i64,
                    stale_state.0.applied_log_index as i64,
                    stale_state.0.applied_log_hash as i64,
                    stale_state.0.state_digest as i64,
                ],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 1,
                source: StoreError::MetadataCommandReplicaStateDiverged {
                    pg_id: 1,
                    reference_node_id: 0,
                    applied_log_index: 2,
                    reference_applied_log_index: 1,
                    ..
                }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn local_cluster_reopen_rejects_same_state_with_different_history() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "same-state-history-");
        let alternate_bucket = bucket_for_pg(topology, 1, "alternate-history-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();

        let alternate_command = create_bucket_metadata_command(PgId::new(1), 1, alternate_bucket);
        let alternate_hash = metadata_command_log_hash(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
            0,
            alternate_command.checksum_crc64(),
        );
        let replica_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        replica_pg
            .connection()
            .execute(
                "UPDATE metadata_command_log \
                     SET command_checksum = ?1, command_bytes = ?2, \
                         previous_log_hash = ?3, log_hash = ?4 \
                     WHERE log_index = 1",
                rusqlite::params![
                    alternate_command.checksum_crc64() as i64,
                    alternate_command.command_bytes(),
                    0_i64,
                    alternate_hash as i64,
                ],
            )
            .unwrap();
        replica_pg
            .connection()
            .execute(
                "UPDATE metadata_command_replica_state \
                     SET applied_log_hash = ?1 \
                     WHERE singleton = 0",
                rusqlite::params![alternate_hash as i64],
            )
            .unwrap();
    }

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 1,
                source: StoreError::MetadataCommandReplicaStateDiverged {
                    pg_id: 1,
                    reference_node_id: 0,
                    applied_log_index: 1,
                    reference_applied_log_index: 1,
                    ..
                }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn metadata_command_log_index_allocator_seeds_from_reopened_log() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let (first_bucket, second_bucket) = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "reopen-index-first-");
        let second_bucket = bucket_for_pg(topology, 1, "reopen-index-second-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &first_bucket);
        (first_bucket, second_bucket)
    };

    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
        let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn metadata_command_log_index_allocator_reads_durable_log_from_already_open_handle() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "open-index-first-");
    let second_bucket = bucket_for_pg(topology, 1, "open-index-second-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();

    create_test_bucket(&first_cluster, &first_bucket);
    create_test_bucket(&second_cluster, &second_bucket);

    for node_id in node_ids {
        let pg = second_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(
                state.applied_log_index, 2,
                "already-open second handle must allocate after the durable log entry from the first handle"
            );
        let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn metadata_command_log_index_allocator_drains_unresolved_durable_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pending_bucket = bucket_for_pg(topology, 1, "durable-pending-first-");
    let blocked_bucket = bucket_for_pg(topology, 1, "durable-pending-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let pending = create_bucket_metadata_command(pg_id, 1, pending_bucket.clone());
    {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        primary_pg
            .try_insert_pending_metadata_command_slot(
                NodeId::new(1).as_u32(),
                &pending,
                Some(&pending_bucket),
            )
            .unwrap();
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: blocked_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            2
        );
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            2
        );
        crate::PgMetadataStore::head_bucket(&*pg, &pending_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &blocked_bucket).unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(primary_pg
        .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
        .unwrap()
        .is_none());
}
