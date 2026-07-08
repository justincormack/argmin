use super::*;
use crate::BucketAclSummary;

#[test]
fn composite_object_listings_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let key_a = key_for_object_pg(topology, &bucket, 1, "dir/a/file-");
    let key_b = key_for_object_pg(topology, &bucket, 2, "dir/b/file-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_b, [52; 16], b"payload-b");
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_a, [53; 16], b"payload-a");

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let bridge_pg_a = bridge_node.get_pg(1).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_a, &bucket, &key_a).unwrap();
    drop(bridge_pg_a);
    let bridge_pg_b = bridge_node.get_pg(2).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_b, &bucket, &key_b).unwrap();
    drop(bridge_pg_b);
    assert!(bridge_node.test_get_object_meta(&bucket, &key_a).is_err());
    assert!(bridge_node.test_get_object_meta(&bucket, &key_b).is_err());

    let all_objects = cluster.list_all_objects_for_bucket(&bucket).unwrap();
    assert_eq!(
        all_objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed = cluster
        .list_objects_for_bucket(&bucket, None, None, None, 100, 100)
        .unwrap();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let prefix = crate::ObjectKey::try_from("dir/".to_string()).unwrap();
    let delimited = cluster
        .list_objects_for_bucket(&bucket, Some(&prefix), Some("/"), None, 100, 100)
        .unwrap();
    assert!(delimited.objects.is_empty());
    assert_eq!(
        delimited
            .common_prefixes
            .iter()
            .map(crate::ObjectKey::as_str)
            .collect::<Vec<_>>(),
        vec!["dir/a/", "dir/b/"]
    );

    let all_versions = cluster
        .list_all_object_versions_for_bucket(&bucket)
        .unwrap();
    assert_eq!(
        all_versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed_versions = cluster
        .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap();
    assert_eq!(
        listed_versions
            .versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );
}

#[test]
fn composite_bucket_listings_fail_closed_while_any_metadata_pg_is_peering() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &crate::ObjectKey::try_from("key".to_string()).unwrap(),
        [54; 16],
        b"payload",
    );

    drop(cluster);
    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(1))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(map).unwrap();

    let err = cluster
        .list_objects_for_bucket(&bucket, None, None, None, 100, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));

    let err = cluster
        .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));

    let err = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, None, None, 100, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn composite_bucket_listing_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "routed-bucket-a-"),
            bucket_for_pg(topology, 2, "routed-bucket-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let owner = crate::CanonicalUserId::from_principal("owner");
    seed_bucket_record(&map, NodeId::new(1), 1, &bucket_a, &owner);
    seed_bucket_record(&map, NodeId::new(2), 2, &bucket_b, &owner);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node.test_head_bucket_raw(&bucket_a).is_err());
    assert!(bridge_node.test_head_bucket_raw(&bucket_b).is_err());

    let buckets = cluster.list_buckets_for_owner(owner.as_str()).unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&bucket_a, &bucket_b]
    );
}

#[test]
fn create_bucket_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-create-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let created = match created {
        crate::BucketCreateAttemptOutcome::Created(info) => info,
        crate::BucketCreateAttemptOutcome::Exists(_) => {
            panic!("fresh bucket unexpectedly existed")
        }
    };

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, created.name);
        assert_eq!(info.owner_principal, created.owner_principal);
        assert_eq!(info.owner_canonical_id, created.owner_canonical_id);
        assert_eq!(info.created_at, created.created_at);
        assert_eq!(info.versioning, created.versioning);
        assert_eq!(info.acl_grants, created.acl_grants);
        assert_eq!(info.public_read, created.public_read);
        assert_eq!(info.public_write, created.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            created.bucket_execution_generation
        );
    }
    assert_bucket_execution_counter_on_acting_nodes(
        &map,
        &node_ids,
        1,
        created.bucket_execution_generation,
    );

    let exists = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info) if info.name == bucket
    ));
}

#[test]
fn create_bucket_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    let create_config = || crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: true,
        public_write: false,
        versioning: crate::BucketVersioningState::Enabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let err = cluster
        .create_bucket_with_config_and_load_info(&create_config())
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial create bucket should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(
                info.created_at, partial_info.created_at,
                "primary-first apply should create bucket on node {node_id:?} before the replica failure"
            );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err(),
            "node {node_id:?} should not have the partially applied bucket"
        );
    }

    let retried = cluster
        .create_bucket_with_config_and_load_info(&create_config())
        .unwrap();
    assert!(matches!(
        retried,
        crate::BucketCreateAttemptOutcome::Created(info)
            if info.name == bucket
                && info.created_at == partial_info.created_at
                && info.bucket_execution_generation
                    == partial_info.bucket_execution_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, partial_info.name);
        assert_eq!(info.owner_principal, partial_info.owner_principal);
        assert_eq!(info.owner_canonical_id, partial_info.owner_canonical_id);
        assert_eq!(info.created_at, partial_info.created_at);
        assert_eq!(info.state, partial_info.state);
        assert_eq!(info.versioning, partial_info.versioning);
        assert_eq!(info.object_lock, partial_info.object_lock);
        assert_eq!(info.acl_grants, partial_info.acl_grants);
        assert_eq!(info.public_read, partial_info.public_read);
        assert_eq!(info.public_write, partial_info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict_on_first_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-first-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(0)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(0)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(0).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_drains_different_bucket_pending_command_on_same_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "partial-create-first-");
    let second_bucket = bucket_for_pg(topology, 1, "partial-create-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let first_bucket_for_hook = first_bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == first_bucket_for_hook
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected partial create bucket failure",
                        source: std::io::Error::other("injected partial create bucket failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: first_bucket.as_str(),
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
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected partial create bucket failure",
                ..
            })
        ),
        "expected injected partial create failure, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_some());
    drop(hook_guard);

    let second = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: second_bucket.as_str(),
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
    assert!(matches!(
        second,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == second_bucket
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &second_bucket).is_none());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let first = crate::PgMetadataStore::head_bucket_raw(&*pg, &first_bucket).unwrap();
        let second = crate::PgMetadataStore::head_bucket_raw(&*pg, &second_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn put_bucket_versioning_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-versioning-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, updated.versioning);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_versioning_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-versioning-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial bucket versioning should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Enabled,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Disabled,
            "node {node_id:?} should not have the partially applied versioning update"
        );
    }

    let retried = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(retried.versioning, crate::BucketVersioningState::Enabled);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn same_bucket_pending_metadata_command_drains_before_later_acl() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "pending-stream-acl-block-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed versioning command should remain pending");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::PutBucketVersioning(versioning)
            if versioning.bucket.name == bucket
                && versioning.bucket.versioning == crate::BucketVersioningState::Enabled
    ));
    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );

    let acl_grants = crate::AclGrants::default();
    let acl_updated = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(
        acl_updated.versioning,
        crate::BucketVersioningState::Enabled
    );
    assert!(acl_updated.public_read);
    assert!(!acl_updated.public_write);
    assert!(
            acl_updated.bucket_execution_generation > partial_info.bucket_execution_generation,
            "later ACL command must reserve a newer execution generation after draining the pending versioning command"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            acl_updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_update_cleans_terminal_pending_slot_before_new_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "terminal-pending-next-op-");
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "test setup should leave a terminal durable pending slot"
    );

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > 1);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "later bucket operation should clean the terminal slot before publishing its command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn put_bucket_acl_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-acl-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };
    let acl_grants = crate::AclGrants::default();

    let updated = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(updated.acl_grants, acl_grants);
    assert!(updated.public_read);
    assert!(!updated.public_write);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert_eq!(info.public_read, updated.public_read);
        assert_eq!(info.public_write, updated.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_acl_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.public_read && !info.public_write,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(!info.public_read);
        assert!(
            !info.public_write,
            "node {node_id:?} should not have the partially applied ACL update"
        );
    }

    let retried = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(retried.public_read);
    assert!(!retried.public_write);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_drains_pending_completed_multipart_sequence_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-drains-mpu-sequence-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            crate::ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
            AdvanceCompletedMultipartUploadSequenceCommand {
                bucket: bucket.clone(),
                completion_order: 7,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let apply_count = Arc::new(AtomicUsize::new(0));
    let hook_bucket = bucket.clone();
    let apply_count_hook = Arc::clone(&apply_count);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                    if advance.bucket == hook_bucket
                        && apply_count_hook.fetch_add(1, Ordering::SeqCst) == 1 =>
                {
                    return Err(StoreError::Io {
                        context: "injected completed multipart sequence apply failure",
                        source: std::io::Error::other(
                            "injected completed multipart sequence apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected completed multipart sequence apply failure",
                ..
            })
        ),
        "expected injected sequence apply failure, got {err:?}"
    );
    drop(hook_guard);

    let acl_grants = crate::AclGrants::default();
    let updated = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(updated.public_read);
    assert!(!updated.public_write);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(info.completed_multipart_upload_sequence, 7);
    }
}

#[test]
fn existing_create_bucket_preserves_pending_acl_command_for_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-create-exists-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending_before = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed ACL command should remain pending");
    assert!(matches!(
        pending_before.payload(),
        MetadataCommandPayload::PutBucketAcl(acl)
            if acl.bucket.name == bucket && acl.bucket.public_read && !acl.bucket.public_write
    ));
    let partial_info = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    let failed_replica_info = {
        let primary = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(!failed_replica_info.public_read);
    assert!(!failed_replica_info.public_write);

    let attacker_owner = crate::CanonicalUserId::from_principal("attacker");
    let exists = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "attacker",
            owner_canonical_id: &attacker_owner,
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
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info)
            if info.owner_principal == "owner"
                && info.owner_canonical_id
                    == crate::CanonicalUserId::from_principal("owner")
    ));

    assert!(
            pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
            "existing CreateBucket should drain and apply the pending ACL command before returning Exists"
        );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_retry_rejects_same_acl_with_mismatched_post_image() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-post-image-conflict-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let current = {
        let node = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap()
    };
    let mut update = PutBucketAclCommand::from_bucket(
        current.with_execution_generation(77),
        acl_grants.clone(),
        BucketAclSummary {
            public_read: true,
            public_write: false,
        },
    );
    update.bucket.bucket_policy_public = true;
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            PgId::new(1),
            MetadataCommandLogIndex::new(77).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(update),
    );
    insert_pending_metadata_command_for_test(&map, PgId::new(1), &bucket, &command);

    let err = cluster
        .put_bucket_acl_and_load_info(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention {
                context: "conflicting pending put bucket acl command",
            })
        ),
        "expected conflicting pending ACL command, got {err:?}"
    );
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert!(!info.public_read);
    assert!(!info.bucket_policy_public);
}

#[test]
fn bucket_property_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-property-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;
    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;

    let object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: Some(crate::ObjectLockDefaultRetention {
            mode: crate::ObjectLockMode::Governance,
            period: crate::RetentionPeriod::days(3).unwrap(),
        }),
    };
    let updated = cluster
        .put_bucket_object_lock_and_load_info(&bucket, object_lock)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, object_lock);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let encryption = crate::BucketEncryptionConfig {
        default_encryption: Some(crate::ManagedEncryptionAlgorithm::Aes256),
        sse_c_blocked: false,
    };
    let updated = cluster
        .put_bucket_encryption_and_load_info(&bucket, encryption)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.encryption, encryption.effective());
        assert_eq!(
            crate::PgMetadataStore::get_bucket_encryption(&*pg, &bucket).unwrap(),
            encryption
        );
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_public_access_block_and_load_info(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let ownership_controls = crate::BucketOwnershipControls {
        object_ownership: crate::BucketObjectOwnership::BucketOwnerPreferred,
    };
    let updated = cluster
        .put_bucket_ownership_controls_and_load_info(&bucket, ownership_controls)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, Some(ownership_controls));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_ownership_controls_and_load_info(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .put_bucket_abac_enabled_and_load_info(&bucket, true)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.bucket_abac_enabled);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_property_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-property-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: true,
    };
    let expected_mutation = BucketPropertyMutation::PublicAccessBlock(Some(public_access_block));
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketProperty(property)
                    if property.bucket.name == hook_bucket
                        && property.effect == expected_mutation.effect()
                        && property.bucket.public_access_block == Some(public_access_block)
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(partial_info.public_access_block, Some(public_access_block));
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            Some(public_access_block),
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            None,
            "node {node_id:?} should not have the partially applied property update"
        );
    }

    let retried = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert_eq!(retried.public_access_block, Some(public_access_block));
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn invalid_bucket_property_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-property-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let invalid_object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: None,
    };
    let err = cluster
        .put_bucket_object_lock_and_load_info(&bucket, invalid_object_lock)
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
            context: "put bucket object lock",
            source: rusqlite::Error::SqliteFailure(_, Some(message)),
        }) if message == "bucket object lock requires enabled versioning" => {}
        other => panic!("expected object-lock storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, crate::BucketObjectLockConfig::default());
        assert_eq!(info.bucket_execution_generation, initial_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert_eq!(updated.public_access_block, Some(public_access_block));
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_subresource_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-subresource-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let policy_body = r#"{"Statement":[]}"#;
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(true));
        assert!(info.bucket_policy_present);
        assert!(info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: tags_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Tagging)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let lifecycle_body = "<LifecycleConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: lifecycle_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, lifecycle_body);
        assert_eq!(stored.generation, Some(1));
        assert!(info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let cors_body = "<CORSConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: cors_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, cors_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Cors)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Policy)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Lifecycle)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_subresource_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-subresource-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let policy_body = r#"{"Statement":[]}"#;
    let expected_mutation = BucketSubresourceMutation::Put {
        kind: crate::BucketSubresourceKind::Policy,
        body: policy_body.to_owned(),
        aux: crate::BucketSubresourceAux::policy(false),
    };
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketSubresource(subresource)
                    if subresource.name == hook_bucket
                        && subresource.mutation == expected_mutation
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.bucket_policy_present);
    assert!(!partial_info.bucket_policy_public);
    assert_eq!(partial_info.bucket_policy_generation, 1);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.bucket_policy_present,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            !info.bucket_policy_present,
            "node {node_id:?} should not have the partially applied policy"
        );
    }

    let retried = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
    assert!(retried.bucket_policy_present);
    assert!(!retried.bucket_policy_public);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(false));
        assert!(info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn invalid_bucket_subresource_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-subresource-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let err = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
            context: "put bucket subresource",
            source: rusqlite::Error::InvalidParameterName(message),
        }) if message.contains("Tagging does not support aux") => {}
        other => panic!("expected subresource storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.bucket_execution_generation, initial_generation);
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: tags_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags_body);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}
