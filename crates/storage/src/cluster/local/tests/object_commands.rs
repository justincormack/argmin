// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::test_support::{
    MetadataCommandApplyTestKind, StorageClusterMetadataCommandTestSupport,
    StorageClusterObjectTestSupport,
};

#[test]
fn opaque_metadata_command_state_and_hook_are_domain_and_subject_bound() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let bucket = crate::BucketName::try_from("metadata-evidence".to_string()).unwrap();
    let target_key = crate::ObjectKey::try_from("target".to_string()).unwrap();
    let crossed_key = crate::ObjectKey::try_from("crossed".to_string()).unwrap();
    ensure_test_bucket(&cluster, &bucket);

    let target_before = cluster
        .test_capture_object_metadata_command_state(&bucket, &target_key)
        .unwrap();
    let crossed_before = cluster
        .test_capture_object_metadata_command_state(&bucket, &crossed_key)
        .unwrap();
    assert!(!target_before.is_same_position_as(&crossed_before));
    assert!(!target_before.advanced_exactly_by(&crossed_before, 0));
    let observed_commits = Arc::new(AtomicUsize::new(0));
    let observed_commits_for_hook = Arc::clone(&observed_commits);
    let _guard = cluster.test_install_before_object_metadata_command_primary_apply_hook(
        &bucket,
        &target_key,
        Arc::new(move |kind| {
            if kind == MetadataCommandApplyTestKind::CommitDirectPutObject {
                observed_commits_for_hook.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }),
    );

    write_committed_direct_segment_for(&cluster, &bucket, &crossed_key, b"crossed");
    assert_eq!(observed_commits.load(Ordering::SeqCst), 0);
    let target_after_crossed_commit = cluster
        .test_capture_object_metadata_command_state(&bucket, &target_key)
        .unwrap();
    assert!(target_after_crossed_commit.advanced_exactly_by(&target_before, 2));

    write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &target_key,
        [42; 16],
        b"target",
    );
    assert_eq!(observed_commits.load(Ordering::SeqCst), 1);
    let target_after = cluster
        .test_capture_object_metadata_command_state(&bucket, &target_key)
        .unwrap();
    assert!(target_after.advanced_exactly_by(&target_after_crossed_commit, 2));
    assert!(target_after.advanced_exactly_by(&target_before, 4));
    assert!(target_after.log_hash_changed_since(&target_before));
    assert!(target_after.state_digest_changed_since(&target_before));

    let foreign_tmp = test_util::tempdir();
    let foreign_map = Arc::new(
        LocalClusterMap::open(foreign_tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let foreign = crate::StorageCluster::from_static_local_map(foreign_map).unwrap();
    ensure_test_bucket(&foreign, &bucket);
    let foreign_state = foreign
        .test_capture_object_metadata_command_state(&bucket, &target_key)
        .unwrap();
    assert!(foreign_state.is_same_position_as(&foreign_state.clone()));
    assert!(!target_before.is_same_position_as(&foreign_state));
    assert!(!format!("{target_after:?}").contains(bucket.as_str()));
    assert!(!format!("{target_after:?}").contains(target_key.as_str()));
}

#[test]
fn object_delete_metadata_command_applies_to_all_acting_object_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete me");

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "delete command should publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn non_current_epoch_object_delete_fails_closed_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let map = Arc::new(local_map);
    let current_cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&current_cluster, &bucket, &key, b"stale delete");
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale object delete should fail closed at the metadata-primary boundary, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale object delete must not append an object-PG command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(stored.generation_id, committed.generation_id);
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "stale object delete must not publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn control_plane_peering_object_delete_old_primary_fails_closed_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&source_map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &source_cluster,
        &bucket,
        &key,
        b"control-plane peering stale delete",
    );
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary object delete should fail closed after control-plane Peering transition, got {err:?}"
    );

    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof::current(
            state.applied_log_index,
            state.applied_log_hash,
            state.state_digest,
        );
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary object delete must not append an object-PG command on node {node_id:?}"
        );
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            stored.generation_id, committed.generation_id,
            "old-primary object delete must preserve the live object on node {node_id:?}"
        );
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "old-primary object delete must not publish reclaim metadata on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary object delete must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary object delete must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary object delete must leave no bucket write reservation on node {node_id:?}"
        );
    }
}

#[test]
fn object_delete_after_reservation_stale_failure_releases_bucket_write_reservation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete after reservation");
    let before_object_pg_proof = cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let bucket_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    let current_epoch = ClusterEpoch::INITIAL;
    let next_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let hook_seen_reservation = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_seen_reservation_for_closure = Arc::clone(&hook_seen_reservation);
    let _hook_guard =
        cluster.test_install_after_object_metadata_reservation_acquired_hook(Arc::new(move || {
            let mut reservation_count = 0usize;
            for node_id in node_ids {
                let pg = hook_map
                    .node(node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(bucket_pg)
                    .unwrap();
                reservation_count +=
                    crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &hook_bucket)
                        .unwrap()
                        .len();
            }
            assert!(
                reservation_count > 0,
                "hook must run after DELETE acquired a bucket-write reservation"
            );
            hook_seen_reservation_for_closure.store(true, Ordering::SeqCst);
            Err(crate::ObjectPgActionError::Store(
                StoreError::StaleMetadataOperation {
                    pg_id: object_pg,
                    operation_epoch: current_epoch,
                    current_epoch: next_epoch,
                },
            ))
        }));

    let err = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == current_epoch
                && observed_current_epoch == next_epoch
        ),
        "expected injected stale operation after reservation acquisition, got {err:?}"
    );
    assert!(
        hook_seen_reservation.load(Ordering::SeqCst),
        "DELETE stale failure test did not reach the after-reservation hook"
    );

    let after_object_pg_proof = cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "after-reservation stale DELETE must not append an object-PG command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(stored.generation_id, committed.generation_id);
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "after-reservation stale DELETE must not publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_metadata_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial delete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected object delete metadata command apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .expect("published object delete must hand trailing convergence to recovery")
        .expect("object delete precondition should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object delete command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    {
        let failed_replica = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            committed.generation_id
        );
    }

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(stored.is_none());
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        matches!(outcome.deleted, crate::DeletedCurrentObject::Missing)
            || matches!(
                outcome.deleted,
                crate::DeletedCurrentObject::Live {
                    generation_id,
                    ..
                } if generation_id == committed.generation_id
            )
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_retries_replica_transport_failure_after_primary_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"transport retry delete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_replica_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_replica_once_hook = Arc::clone(&fail_replica_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_replica_once_hook.swap(false, Ordering::SeqCst)
            ) {
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "injected delete replica connection interruption after primary apply",
                    ),
                });
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    drop(hook_guard);
    assert!(
        !fail_replica_once.load(Ordering::SeqCst),
        "object delete must exercise the post-primary transport retry"
    );
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_exact_pending_retry_converges_partial_exact_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let _committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"exact delete retry");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let fail_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected exact pending delete apply failure",
                        source: std::io::Error::other(
                            "injected exact pending delete apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        published.deleted,
        crate::DeletedCurrentObject::Live { .. }
    ));
    drop(fail_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some());

    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let apply_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual exact pending delete command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(
                    stored.is_none(),
                    "primary-first retry should drain the pending delete before observing a fresh missing object"
                );
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
    drop(apply_guard);
    assert!(applied_by_hook.load(Ordering::SeqCst));
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Missing
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_committed_response_loss_retry_returns_missing_without_rerunning_live_delete() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete response loss");

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard =
        cluster.test_install_after_object_metadata_command_publish_hook(Arc::new(|| {
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "injected object delete response loss".to_string(),
            })
        }));

    let first_err = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected object delete response loss"
        ),
        "expected injected post-commit object delete response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(
                stored.is_none(),
                "committed delete retry must not observe the deleted object as live"
            );
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Missing
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
}

#[test]
fn object_delete_metadata_command_partial_apply_reopens_and_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete reopen");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected object delete reopen apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .expect("published object delete must hand trailing convergence to recovery")
        .expect("object delete precondition should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object delete command must remain durable before reopen"
    );
    {
        let node_id = NodeId::new(2);
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            committed.generation_id
        );
    }
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*primary_pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
            .expect("reopen local map with in-flight object delete command"),
    );
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial object delete command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "open-time delete convergence should publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn multipart_completion_barrier_drains_same_pg_object_command_with_cleanup_hooks() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "mpu-order-drains-object-");
        let key = key_for_object_pg(topology, &bucket, 1, "same-pg-key-");
        (bucket, key, 1)
    };
    set_route_primary(&mut map, pg_id, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"same pg object command cleanup",
    );
    assert!(cluster.try_take_reclaim_work().is_none());

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected same-pg object delete apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .expect("published object delete must hand trailing convergence to recovery")
        .expect("object delete precondition should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(pg_id), &bucket).is_some());
    assert!(cluster.try_take_reclaim_work().is_none());

    let barrier_sequence = cluster
        .test_establish_multipart_completion_barrier(&bucket)
        .unwrap();
    assert_eq!(barrier_sequence, 1);
    assert!(pending_metadata_command_for_test(&map, PgId::new(pg_id), &bucket).is_none());
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.multipart_completion_barrier_sequence, barrier_sequence);
    }
}

#[test]
fn multipart_completion_barrier_drains_other_bucket_sequence_without_stealing_order() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket_a = bucket_for_pg(topology, 1, "mpu-order-other-a-");
    let bucket_b = bucket_for_pg(topology, 1, "mpu-order-other-b-");
    assert_ne!(bucket_a, bucket_b);
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);

    assert_eq!(
        cluster
            .test_establish_multipart_completion_barrier(&bucket_b)
            .unwrap(),
        1
    );

    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        cluster.next_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket_a.clone(),
                barrier_sequence: 1,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket_a, &command);

    assert_eq!(
        cluster
            .test_establish_multipart_completion_barrier(&bucket_b)
            .unwrap(),
        2
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket_b).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_a_info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket_a).unwrap();
        let bucket_b_info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket_b).unwrap();
        assert_eq!(bucket_a_info.multipart_completion_barrier_sequence, 1);
        assert_eq!(bucket_b_info.multipart_completion_barrier_sequence, 2);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn object_metadata_update_commands_apply_to_all_acting_object_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"object metadata");
    let tags = "<Tagging><TagSet><Tag><Key>tier</Key><Value>hot</Value></Tag></TagSet></Tagging>";
    let expected_tags = crate::tests::object_tags(tags);
    let retention = crate::ObjectRetention {
        mode: crate::ObjectLockMode::Governance,
        retain_until_unix_seconds: 123_456,
    };
    let acl_grants = crate::AclGrants::default();

    let tagged_version = cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert_eq!(tagged_version, crate::VersionId::Null);
    cluster
        .put_object_retention_if(&bucket, &key, None, retention, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    cluster
        .put_object_legal_hold_if(
            &bucket,
            &key,
            None,
            crate::StoredLegalHoldStatus::On,
            |stored| Ok::<_, ()>(stored.version_id()),
        )
        .unwrap()
        .unwrap();
    let acl_version = cluster
        .put_object_acl_if(&bucket, &key, None, |stored| {
            Ok::<_, ()>((stored.version_id(), acl_grants.clone(), true))
        })
        .unwrap()
        .unwrap();
    assert_eq!(acl_version, crate::VersionId::Null);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null)
                .unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(
            live.tags.as_ref().map(crate::SerializedTagSet::tag_set),
            Some(expected_tags.tag_set())
        );
        assert_eq!(live.object_lock.retention, Some(retention));
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert_eq!(live.acl_grants, acl_grants);
        assert!(live.public_read);
    }

    cluster
        .delete_object_tags_if(&bucket, &key, None, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn non_current_epoch_object_metadata_update_fails_closed_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let map = Arc::new(local_map);
    let current_cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&current_cluster, &bucket, &key, b"stale metadata");
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();
    let tags =
        "<Tagging><TagSet><Tag><Key>stale</Key><Value>ignored</Value></Tag></TagSet></Tagging>";

    let err = stale_cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale object metadata update should fail closed at the metadata-primary boundary, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale object metadata update must not append an object-PG command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(stored.generation_id, committed.generation_id);
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None,
            "stale object metadata update must not publish tags on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn control_plane_peering_object_metadata_update_old_primary_fails_closed_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&source_map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &source_cluster,
        &bucket,
        &key,
        b"control-plane peering stale metadata",
    );
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let tags =
        "<Tagging><TagSet><Tag><Key>stale</Key><Value>ignored</Value></Tag></TagSet></Tagging>";
    let err = old_primary_cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary object metadata update should fail closed after control-plane Peering transition, got {err:?}"
    );

    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof::current(
            state.applied_log_index,
            state.applied_log_hash,
            state.state_digest,
        );
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary object metadata update must not append an object-PG command on node {node_id:?}"
        );
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            stored.generation_id, committed.generation_id,
            "old-primary object metadata update must preserve the live object on node {node_id:?}"
        );
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None,
            "old-primary object metadata update must not publish tags on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary object metadata update must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary object metadata update must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary object metadata update must leave no bucket write reservation on node {node_id:?}"
        );
    }
}

#[test]
fn object_metadata_update_retry_converges_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial object metadata");
    let tags = "<Tagging><TagSet><Tag><Key>retry</Key><Value>yes</Value></Tag></TagSet></Tagging>";
    let expected_tags = crate::tests::object_tags(tags);
    fn require_tags_absent(stored: &crate::StoredObject) -> Result<crate::VersionId, &'static str> {
        if stored.as_live().unwrap().tags.is_some() {
            Err("tags already visible before pending command convergence")
        } else {
            Ok(stored.version_id())
        }
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected object metadata command apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .put_object_tags_if(&bucket, &key, None, tags, require_tags_absent)
        .expect("published metadata update must hand trailing convergence to recovery")
        .expect("object metadata precondition should succeed");
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object metadata command must remain pending"
    );
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_ref()
                .map(crate::SerializedTagSet::tag_set),
            Some(expected_tags.tag_set())
        );
    }

    cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_ref()
                .map(crate::SerializedTagSet::tag_set),
            Some(expected_tags.tag_set())
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_metadata_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"object metadata reopen");
    let tags =
        "<Tagging><TagSet><Tag><Key>retry</Key><Value>reopen</Value></Tag></TagSet></Tagging>";
    let expected_tags = crate::tests::object_tags(tags);

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected object metadata reopen apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .expect("published metadata update must hand trailing convergence to recovery")
        .expect("object metadata precondition should succeed");
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object metadata command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let reopened_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let reopened_map = Arc::new(reopened_map);
    assert!(
        pending_metadata_command_for_test(&reopened_map, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial object metadata command"
    );
    for node_id in node_ids {
        let node = reopened_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_ref()
                .map(crate::SerializedTagSet::tag_set),
            Some(expected_tags.tag_set())
        );
    }
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened_map, &bucket);
}

#[test]
fn object_metadata_retry_rejects_same_mutation_with_mismatched_post_image() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata mismatch");
    let tags = "<Tagging><TagSet><Tag><Key>retry</Key><Value>no</Value></Tag></TagSet></Tagging>";

    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    let mut mismatched_post_image = stored.clone();
    mismatched_post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
    mismatched_post_image.public_read = !stored.public_read;
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
            bucket_write_reservation: proof,
            object: mismatched_post_image,
        })),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let err = cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "conflicting pending command for object metadata update",
            })
        ),
        "expected conflicting post-image error, got {err:?}"
    );
    let pg = primary.get_pg(object_pg).unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
    assert_eq!(stored.as_live().unwrap().tags, None);
    drop(pg);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_some());
}

#[test]
fn object_metadata_command_rejects_non_metadata_post_image_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata apply mismatch");
    let tags = "<Tagging><TagSet><Tag><Key>apply</Key><Value>no</Value></Tag></TagSet></Tagging>";
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let mut post_image = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
    post_image.size += 1;

    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
            bucket_write_reservation: proof,
            object: post_image,
        })),
    );
    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::InvariantViolation {
                context: "put object metadata command preimage mismatch",
                ..
            })
        ),
        "expected preimage mismatch, got {err:?}"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().tags, None);
    }
}

#[test]
fn lifecycle_current_expiration_delete_command_applies_to_all_acting_object_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"expired current");

    let callback_error = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Err::<bool, _>("policy callback failed")
            },
        )
        .expect("storage publication should succeed")
        .unwrap_err();
    assert_eq!(callback_error, "policy callback failed");
    assert_bucket_write_reservations_released(&map, &bucket);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("current object should expire");
    assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
}

#[test]
fn semantic_lifecycle_aging_rejects_current_live_and_delete_marker_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [91; 16],
        [92; 16],
        b"current lifecycle version",
    );

    let current_before = cluster
        .test_get_object_version(&bucket, &key, current.version_id)
        .unwrap();
    let current_proof_before = cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let current_err = crate::StorageCluster::test_age_noncurrent_live_object(
        &cluster,
        &bucket,
        &key,
        current.version_id,
        1,
    )
    .expect_err("a current live version must not be made artificially noncurrent");
    assert!(matches!(
        current_err,
        crate::ObjectPgActionError::Store(StoreError::IntegrityError {
            expected: 1,
            actual: 0,
        })
    ));
    assert_eq!(
        cluster
            .test_get_object_version(&bucket, &key, current.version_id)
            .unwrap(),
        current_before
    );
    assert_eq!(
        cluster
            .test_object_pg_metadata_proof(&bucket, &key)
            .unwrap(),
        current_proof_before,
        "rejected current-version aging must not change durable metadata"
    );

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();
    let marker_before = cluster
        .test_get_object_version(&bucket, &key, marker.version_id)
        .unwrap();
    let marker_proof_before = cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let marker_err = crate::StorageCluster::test_age_noncurrent_live_object(
        &cluster,
        &bucket,
        &key,
        marker.version_id,
        1,
    )
    .expect_err("a delete marker must not be mutated by lifecycle aging");
    assert!(matches!(
        marker_err,
        crate::ObjectPgActionError::Store(StoreError::IntegrityError {
            expected: 1,
            actual: 0,
        })
    ));
    assert_eq!(
        cluster
            .test_get_object_version(&bucket, &key, marker.version_id)
            .unwrap(),
        marker_before
    );
    assert_eq!(
        cluster
            .test_object_pg_metadata_proof(&bucket, &key)
            .unwrap(),
        marker_proof_before,
        "rejected delete-marker aging must not change durable metadata"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
}

#[test]
fn logical_delete_marker_owner_observation_rejects_live_and_binds_owner_fields() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let (bucket, key, _, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let live = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [93; 16],
        [94; 16],
        b"owner observation live version",
    );
    let live_owner = crate::OwnerIdentity::from_principal("owner");
    let marker_owner = crate::OwnerIdentity::new(
        "marker-owner",
        s3_types::CanonicalUserId::from_principal("marker-canonical-source"),
    );
    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            marker_owner.clone(),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let matching_live_owner_err =
        crate::test_support::delete_marker_version_has_owner_raw_for_owner_test(
            &cluster,
            &bucket,
            &key,
            live.version_id,
            &live_owner,
        )
        .expect_err("a live version must not pass through the delete-marker observation");
    assert!(matches!(
        matching_live_owner_err,
        crate::ObjectPgActionError::InvalidRequest { reason }
            if reason == "selected object version is not a delete marker"
    ));
    assert!(cluster
        .test_delete_marker_version_has_owner(&bucket, &key, marker.version_id, &marker_owner)
        .unwrap());
    let crossed_live_version_err =
        crate::test_support::delete_marker_version_has_owner_raw_for_owner_test(
            &cluster,
            &bucket,
            &key,
            live.version_id,
            &marker_owner,
        )
        .expect_err("a crossed live version must not be treated as a delete marker");
    assert!(matches!(
        crossed_live_version_err,
        crate::ObjectPgActionError::InvalidRequest { reason }
            if reason == "selected object version is not a delete marker"
    ));
    assert!(!cluster
        .test_delete_marker_version_has_owner(&bucket, &key, marker.version_id, &live_owner)
        .unwrap());

    let wrong_principal =
        crate::OwnerIdentity::new("other-marker-owner", marker_owner.canonical_id.clone());
    assert!(!cluster
        .test_delete_marker_version_has_owner(&bucket, &key, marker.version_id, &wrong_principal,)
        .unwrap());
    let wrong_canonical = crate::OwnerIdentity::new(
        marker_owner.principal.clone(),
        s3_types::CanonicalUserId::from_principal("other-marker-canonical-source"),
    );
    assert!(!cluster
        .test_delete_marker_version_has_owner(&bucket, &key, marker.version_id, &wrong_canonical,)
        .unwrap());
}

#[test]
fn lifecycle_current_expiration_stops_after_bucket_recreate_before_proof() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let old_bucket_incarnation = cluster
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old lifecycle object");

    let hook_ran = Arc::new(AtomicBool::new(false));
    let fresh_generation_id = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let old_generation_id = old_committed.generation_id;
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let fresh_generation_id_for_hook = Arc::clone(&fresh_generation_id);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(bucket.clone()),
            before_lifecycle_bucket_write_proof_acquire: Some(Arc::new(move || {
                if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                hook_cluster
                    .delete_current_object_if(&hook_bucket, &hook_key, |_| Ok::<(), ()>(()))
                    .unwrap()
                    .expect("old live object should be deleted before bucket recreate");
                hook_cluster
                    .reclaim_object_payload_if_unleased(&hook_bucket, &hook_key, old_generation_id)
                    .expect("test should reclaim the old payload");
                hook_cluster
                    .test_begin_bucket_delete_if_current(&hook_bucket)
                    .expect("test should begin old bucket delete");
                assert_eq!(
                    hook_cluster
                        .try_finalize_bucket_delete(&hook_bucket)
                        .expect("test should finalize old bucket delete"),
                    crate::BucketDeleteFinalizeOutcome::Finalized
                );
                create_test_bucket(&hook_cluster, &hook_bucket);
                let fresh = write_committed_direct_segment_for_with_versioning(
                    &hook_cluster,
                    &hook_bucket,
                    &hook_key,
                    crate::BucketVersioningState::Disabled,
                    [0xc1; 16],
                    [0xc2; 16],
                    b"fresh recreated object",
                );
                *fresh_generation_id_for_hook.lock().unwrap() = Some(fresh.generation_id);
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            old_committed.version_id,
            old_bucket_incarnation,
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap()
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        outcome.is_none(),
        "stale lifecycle context must not delete the recreated bucket's object"
    );
    let new_bucket = cluster.head_bucket_info(&bucket).unwrap();
    assert!(
        new_bucket.bucket_incarnation_generation > old_bucket_incarnation,
        "test setup should recreate the bucket incarnation"
    );
    assert!(
        !new_bucket.bucket_lifecycle_present,
        "recreated bucket should not inherit the old lifecycle config"
    );
    let current = cluster.test_get_object_meta(&bucket, &key).unwrap();
    let live = current.as_live().expect("fresh object should remain live");
    let fresh_generation_id =
        (*fresh_generation_id.lock().unwrap()).expect("hook should write a fresh recreated object");
    assert_eq!(live.version_id, crate::VersionId::Null);
    assert_eq!(live.generation_id, fresh_generation_id);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_current_expiration_stops_after_bucket_recreate_before_context_load() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let old_bucket_incarnation = current_bucket_incarnation(&cluster, &bucket);
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old lifecycle object");

    let hook_ran = Arc::new(AtomicBool::new(false));
    let selector_ran = Arc::new(AtomicBool::new(false));
    let fresh_generation_id = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let old_generation_id = old_committed.generation_id;
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let fresh_generation_id_for_hook = Arc::clone(&fresh_generation_id);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(bucket.clone()),
            before_lifecycle_context_load: Some(Arc::new(move || {
                if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                hook_cluster
                    .delete_current_object_if(&hook_bucket, &hook_key, |_| Ok::<(), ()>(()))
                    .unwrap()
                    .expect("old live object should be deleted before bucket recreate");
                hook_cluster
                    .reclaim_object_payload_if_unleased(&hook_bucket, &hook_key, old_generation_id)
                    .expect("test should reclaim the old payload");
                hook_cluster
                    .test_begin_bucket_delete_if_current(&hook_bucket)
                    .expect("test should begin old bucket delete");
                assert_eq!(
                    hook_cluster
                        .try_finalize_bucket_delete(&hook_bucket)
                        .expect("test should finalize old bucket delete"),
                    crate::BucketDeleteFinalizeOutcome::Finalized
                );
                create_test_bucket(&hook_cluster, &hook_bucket);
                put_test_lifecycle(&hook_cluster, &hook_bucket);
                let fresh = write_committed_direct_segment_for_with_versioning(
                    &hook_cluster,
                    &hook_bucket,
                    &hook_key,
                    crate::BucketVersioningState::Disabled,
                    [0xd1; 16],
                    [0xd2; 16],
                    b"fresh recreated object",
                );
                *fresh_generation_id_for_hook.lock().unwrap() = Some(fresh.generation_id);
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let selector_ran_for_closure = Arc::clone(&selector_ran);
    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            old_committed.version_id,
            old_bucket_incarnation,
            move |_, _| {
                selector_ran_for_closure.store(true, Ordering::SeqCst);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        !selector_ran.load(Ordering::SeqCst),
        "old lifecycle claim must not evaluate the recreated bucket's lifecycle"
    );
    assert!(
        outcome.is_none(),
        "old lifecycle claim must not delete the recreated bucket's object"
    );
    let new_bucket = cluster.head_bucket_info(&bucket).unwrap();
    assert!(
        new_bucket.bucket_incarnation_generation > old_bucket_incarnation,
        "test setup should recreate the bucket incarnation"
    );
    assert!(
        new_bucket.bucket_lifecycle_present,
        "recreated bucket intentionally has lifecycle to prove the incarnation fence"
    );
    let current = cluster.test_get_object_meta(&bucket, &key).unwrap();
    let live = current.as_live().expect("fresh object should remain live");
    let fresh_generation_id =
        (*fresh_generation_id.lock().unwrap()).expect("hook should write a fresh recreated object");
    assert_eq!(live.version_id, crate::VersionId::Null);
    assert_eq!(live.generation_id, fresh_generation_id);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_suspended_current_expiration_replaces_null_live_on_all_acting_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    put_test_lifecycle(&cluster, &bucket);
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"suspended current");
    assert_eq!(committed.version_id, crate::VersionId::Null);

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("suspended null live object should expire");
    assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null)
                .unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
        assert!(
            crate::PgMetadataStore::get_object_segments(
                &*pg,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap()
            .is_empty(),
            "null live segment rows should be removed on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn suspended_delete_replaces_null_live_on_all_acting_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"suspended current");
    assert_eq!(committed.version_id, crate::VersionId::Null);

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            |stored| {
                let live = stored
                    .and_then(crate::StoredObject::as_live)
                    .expect("current null object must be live");
                assert_eq!(live.generation_id, committed.generation_id);
                Ok::<_, ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(marker.version_id, crate::VersionId::Null);

    let repeated_marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            |stored| {
                let Some(crate::StoredObject::DeleteMarker(marker)) = stored else {
                    panic!("current null object must be a delete marker");
                };
                assert_eq!(marker.version_id, crate::VersionId::Null);
                Ok::<_, ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(repeated_marker.version_id, crate::VersionId::Null);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null)
                .unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
        assert!(
            crate::PgMetadataStore::get_object_segments(
                &*pg,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap()
            .is_empty(),
            "null live segment rows should be removed on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_enabled_current_expiration_reserves_delete_marker_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0x77; 16],
        [0x78; 16],
        b"enabled lifecycle current",
    );
    assert_eq!(committed.version_id, crate::VersionId::from_u64(1));

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("enabled current live object should expire");
    assert_eq!(outcome.reclaim_generation_id, None);
    assert!(cluster.try_take_reclaim_work().is_none());

    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let crate::StoredObject::DeleteMarker(marker) = current else {
            panic!("expected current delete marker on node {node_id:?}, got {current:?}");
        };
        assert_eq!(marker.version_id, crate::VersionId::from_u64(2));

        let stored_live =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, committed.version_id)
                .unwrap();
        let live = stored_live.as_live().unwrap();
        assert_eq!(live.generation_id, committed.generation_id);
        assert!(live.became_noncurrent_at.is_some());
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "enabled current expiration should not reclaim the preserved live version"
        );
    }
}

#[test]
fn lifecycle_noncurrent_and_delete_marker_expiration_use_object_commands() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [11; 16],
        [51; 16],
        b"older version",
    );
    let middle = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [13; 16],
        [53; 16],
        b"middle version",
    );
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [12; 16],
        [52; 16],
        b"current version",
    );

    let mut reclaimed = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                Ok::<_, ()>(
                    [older.version_id, middle.version_id]
                        .into_iter()
                        .filter(|version_id| {
                            versions
                                .iter()
                                .any(|stored| stored.version_id() == *version_id)
                        })
                        .collect(),
                )
            },
        )
        .unwrap()
        .unwrap();
    let next_reclaimed = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                Ok::<_, ()>(
                    [older.version_id, middle.version_id]
                        .into_iter()
                        .filter(|version_id| {
                            versions
                                .iter()
                                .any(|stored| stored.version_id() == *version_id)
                        })
                        .collect(),
                )
            },
        )
        .unwrap()
        .unwrap();
    reclaimed.extend(next_reclaimed);
    assert_eq!(reclaimed.len(), 2);
    assert!(reclaimed.contains(&older.generation_id));
    assert!(reclaimed.contains(&middle.generation_id));

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let deleted_marker = cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert!(versions.iter().any(|stored| {
                    stored.version_id() == marker.version_id
                        && matches!(stored, crate::StoredObject::DeleteMarker(_))
                }));
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap();
    assert!(deleted_marker);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, middle.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            middle.generation_id
        )
        .unwrap());
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, marker.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.version_id(), current.version_id);
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_noncurrent_pending_install_race_reruns_selector() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa1; 16],
        [0xb1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa2; 16],
        [0xb2; 16],
        b"current",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let selector_calls = Arc::new(AtomicUsize::new(0));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_version_id = older.version_id;
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
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
            let stored = crate::PgMetadataStore::get_object_version(
                &*pg,
                &hook_bucket,
                &hook_key,
                hook_version_id,
            )
            .unwrap();
            let live = stored.as_live().expect("older object is live").clone();
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
                        PutObjectMetadataMutation::PutLegalHold(crate::StoredLegalHoldStatus::On),
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

    let calls_for_selector = Arc::clone(&selector_calls);
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                calls_for_selector.fetch_add(1, Ordering::SeqCst);
                let older_live = versions
                    .iter()
                    .find(|stored| stored.version_id() == older.version_id)
                    .and_then(crate::StoredObject::as_live)
                    .expect("older version should be listed");
                if older_live.object_lock.legal_hold == crate::StoredLegalHoldStatus::On {
                    Ok::<_, ()>(HashSet::new())
                } else {
                    Ok(HashSet::from([older.version_id]))
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        2,
        "lifecycle selector must be rerun after slot contention changes object lock state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        let live = stored.as_live().expect("older object remains live");
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
}

#[test]
fn lifecycle_noncurrent_command_id_race_drains_winner_and_reruns_selector() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xc1; 16],
        [0xd1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xc2; 16],
        [0xd2; 16],
        b"current",
    );

    let slot_installed = Arc::new(AtomicBool::new(false));
    let selector_calls = Arc::new(AtomicUsize::new(0));
    let install_map = Arc::clone(&second_map);
    let install_bucket = bucket.clone();
    let install_key = key.clone();
    let install_version_id = older.version_id;
    let install_once = Arc::clone(&slot_installed);
    let calls_for_selector = Arc::clone(&selector_calls);
    let install_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                calls_for_selector.fetch_add(1, Ordering::SeqCst);
                let older_live = versions
                    .iter()
                    .find(|stored| stored.version_id() == install_version_id)
                    .and_then(crate::StoredObject::as_live)
                    .expect("older version should be listed");
                if older_live.object_lock.legal_hold == crate::StoredLegalHoldStatus::On {
                    return Ok::<_, ()>(HashSet::new());
                }
                if !install_once.swap(true, Ordering::SeqCst) {
                    let pg_id = PgId::new(2);
                    let primary = install_map
                        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                        .unwrap();
                    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                    let stored = crate::PgMetadataStore::get_object_version(
                        &*pg,
                        &install_bucket,
                        &install_key,
                        install_version_id,
                    )
                    .unwrap();
                    let live = stored.as_live().expect("older object is live").clone();
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
                                PutObjectMetadataMutation::PutLegalHold(
                                    crate::StoredLegalHoldStatus::On,
                                ),
                                install_proof.clone(),
                            ),
                        )),
                    );
                    pg.try_insert_pending_metadata_command_slot(
                        primary.node_id().as_u32(),
                        &command,
                        Some(&install_bucket),
                    )
                    .unwrap();
                }
                Ok(HashSet::from([install_version_id]))
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert!(slot_installed.load(Ordering::SeqCst));
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        2,
        "lifecycle selector must rerun after command-id contention"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        let live = stored.as_live().expect("older object remains live");
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
}

#[test]
fn lifecycle_noncurrent_version_list_change_defers_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-version-list-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xe1; 16],
        [0xf1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xe2; 16],
        [0xf2; 16],
        b"current",
    );

    let selector_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_selector = Arc::clone(&selector_calls);
    let race_bucket = bucket.clone();
    let race_key = key.clone();
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                let call = calls_for_selector.fetch_add(1, Ordering::SeqCst);
                assert!(versions
                    .iter()
                    .any(|stored| stored.version_id() == older.version_id));
                if call == 0 {
                    write_committed_direct_segment_for_with_versioning(
                        &second_cluster,
                        &race_bucket,
                        &race_key,
                        crate::BucketVersioningState::Enabled,
                        [0xe3; 16],
                        [0xf3; 16],
                        b"racing current",
                    );
                    Ok::<_, ()>(HashSet::from([older.version_id]))
                } else {
                    Ok(HashSet::new())
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        1,
        "version-list drift must defer lifecycle work to a later sweep"
    );

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        assert!(stored.as_live().is_some());
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn lifecycle_expired_marker_version_list_change_defers_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-marker-list-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let marker = first_cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let selector_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_selector = Arc::clone(&selector_calls);
    let race_bucket = bucket.clone();
    let race_key = key.clone();
    let deleted = first_cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                let call = calls_for_selector.fetch_add(1, Ordering::SeqCst);
                assert!(versions.iter().any(|stored| {
                    stored.version_id() == marker.version_id
                        && matches!(stored, crate::StoredObject::DeleteMarker(_))
                }));
                if call == 0 {
                    assert_eq!(versions.len(), 1);
                    write_committed_direct_segment_for_with_versioning(
                        &second_cluster,
                        &race_bucket,
                        &race_key,
                        crate::BucketVersioningState::Enabled,
                        [0xe4; 16],
                        [0xf4; 16],
                        b"racing live",
                    );
                    Ok::<_, ()>(true)
                } else {
                    panic!("version-list drift should defer lifecycle work without retrying")
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(!deleted);
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        1,
        "delete-marker version-list drift must defer lifecycle work to a later sweep"
    );

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, marker.version_id)
                .unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn insert_delete_marker_metadata_command_applies_to_all_acting_object_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let owner = crate::OwnerIdentity::from_principal("owner");

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            owner.clone(),
            |stored| {
                assert!(stored.is_none());
                Ok::<(), ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(marker.version_id, crate::VersionId::from_u64(1));

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        match stored {
            crate::StoredObject::DeleteMarker(record) => {
                assert_eq!(record.version_id, marker.version_id);
                assert_eq!(record.owner, owner);
            }
            other => panic!("expected delete marker on node {node_id:?}, got {other:?}"),
        }
    }
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        marker.version_id.to_u64() + 1,
    );
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn insert_delete_marker_retries_local_command_contention_before_publication() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let attempts = Arc::new(AtomicUsize::new(0));
    let hook_attempts = Arc::clone(&attempts);
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::InsertDeleteMarker(_)
            ) && hook_attempts.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected local delete-marker publication contention",
                });
            }
            Ok(())
        }));

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            |stored| {
                assert!(stored.is_none());
                Ok::<(), ()>(())
            },
        )
        .expect("local command contention should be retried")
        .expect("delete-marker condition should succeed");

    assert_eq!(marker.version_id, crate::VersionId::Null);
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[derive(Clone, Copy)]
enum WitnessedCommandExpiredAuthority {
    Reservation,
    Route,
}

fn assert_insert_delete_marker_witness_survives_expired_authority(
    expired_authority: WitnessedCommandExpiredAuthority,
) {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let delayed_witness = Arc::new(AtomicBool::new(false));
    let delayed_witness_hook = Arc::clone(&delayed_witness);
    let witnessed_command = Arc::new(Mutex::new(None));
    let witnessed_command_for_hook = Arc::clone(&witnessed_command);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook = cluster.test_install_after_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id == NodeId::new(0)
                && matches!(
                    command.payload(),
                    MetadataCommandPayload::InsertDeleteMarker(marker)
                        if marker.bucket == hook_bucket && marker.key == hook_key
                )
                && !delayed_witness_hook.swap(true, Ordering::SeqCst)
            {
                std::thread::sleep(Duration::from_millis(1_050));
                *witnessed_command_for_hook
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(command.clone());
                match expired_authority {
                    WitnessedCommandExpiredAuthority::Reservation => {
                        let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload()
                        else {
                            unreachable!(
                                "matched delete-marker command must remain a delete marker"
                            )
                        };
                        let bucket_pg_id = PgId::new(
                            hook_map
                                .node(NodeId::new(0))
                                .unwrap()
                                .storage_node()
                                .pg_topology()
                                .bucket_pg_for(&hook_bucket),
                        );
                        let bucket_primary = hook_map
                            .metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
                            .unwrap();
                        let bucket_pg = bucket_primary
                            .storage_node()
                            .get_pg(bucket_pg_id.get())
                            .unwrap();
                        bucket_pg
                            .test_set_bucket_write_reservation_lease_deadline(
                                &marker.bucket_write_reservation.reservation_id,
                                crate::clock::current_time_millis().saturating_sub(1),
                            )
                            .unwrap();
                    }
                    WitnessedCommandExpiredAuthority::Route => hook_map
                        .test_store_route_map_validity(
                            crate::RouteMapValidity::until_ms(0).unwrap(),
                        ),
                }
            }
            Ok(())
        },
    ));

    let result = cluster.insert_current_delete_marker_if(
        &bucket,
        &key,
        crate::BucketVersioningState::Suspended,
        crate::OwnerIdentity::from_principal("owner"),
        |stored| {
            assert!(stored.is_none());
            Ok::<(), ()>(())
        },
    );
    drop(hook);

    assert!(delayed_witness.load(Ordering::SeqCst));
    let witnessed_command = witnessed_command
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .expect("test must capture the exact witnessed command");
    match expired_authority {
        WitnessedCommandExpiredAuthority::Reservation => {
            let marker = result
                .expect("expired reservation must not replace witnessed convergence")
                .expect("delete-marker condition should succeed");
            assert_eq!(marker.version_id, crate::VersionId::Null);
            assert!(
                pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none()
            );
            assert_clean_metadata_command_stream(&map, &[object_pg]);
            assert_bucket_write_reservations_released(&map, &bucket);
        }
        WitnessedCommandExpiredAuthority::Route => {
            assert!(matches!(
                result,
                Err(crate::ObjectPgActionError::Store(
                    StoreError::MetadataCommandIrrevocableConvergencePending { .. }
                ))
            ));
            let primary_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert_eq!(
                primary_pg
                    .pending_metadata_command_envelope(
                        NodeId::new(1).as_u32(),
                        ClusterEpoch::INITIAL,
                    )
                    .unwrap(),
                Some(witnessed_command.clone()),
                "expired routing authority must retain the exact witnessed command"
            );
            let witness_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert!(
                witness_pg
                    .applied_metadata_command_log_entry_hashes(
                        NodeId::new(0).as_u32(),
                        &witnessed_command,
                    )
                    .unwrap()
                    .is_some(),
                "the exact witnessed row must remain durable after route expiry"
            );
        }
    }
}

#[test]
fn insert_delete_marker_retries_witnessed_command_after_reservation_expires() {
    assert_insert_delete_marker_witness_survives_expired_authority(
        WitnessedCommandExpiredAuthority::Reservation,
    );
}

#[test]
fn insert_delete_marker_retains_witnessed_command_after_route_expires() {
    assert_insert_delete_marker_witness_survives_expired_authority(
        WitnessedCommandExpiredAuthority::Route,
    );
}

#[test]
fn insert_delete_marker_abandons_persistent_local_contention_at_retry_deadline() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let attempts = Arc::new(AtomicUsize::new(0));
    let hook_attempts = Arc::clone(&attempts);
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::InsertDeleteMarker(_)
            ) {
                hook_attempts.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::MetadataCommandContention {
                    context: "injected persistent local delete-marker publication contention",
                });
            }
            Ok(())
        }));

    let error = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<(), ()>(()),
        )
        .expect_err("persistent local contention must exhaust the bounded retry budget");

    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert!(attempts.load(Ordering::SeqCst) > 1);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn suspended_delete_marker_reinspects_replacement_after_transported_contention() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Suspended,
        [0xa1; 16],
        [0xa2; 16],
        b"original conditional delete subject",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_for_hook = Arc::clone(&fail_once);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _apply_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket && marker.key == hook_key
            ) && fail_once_for_hook.swap(false, Ordering::SeqCst)
            {
                return Err(StoreError::StorageRpc {
                    node_id: 0,
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandContention,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "injected transported conditional delete contention",
                    ),
                });
            }
            Ok(())
        }));
    let expire_bucket = bucket.clone();
    let expire_key = key.clone();
    let _budget_hook = cluster.test_install_object_metadata_command_definitive_retry_hook(
        Arc::new(move |command, source| {
            matches!(
                command.payload(),
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == expire_bucket && marker.key == expire_key
            ) && crate::cluster::request_ops::metadata_command_apply_error_is_contention(source)
        }),
    );

    let replacement_generation = Arc::new(Mutex::new(None));
    let replacement_generation_for_hook = Arc::clone(&replacement_generation);
    let replacement_cluster = cluster.clone();
    let replacement_bucket = bucket.clone();
    let replacement_key = key.clone();
    let replace_once = Arc::new(AtomicBool::new(true));
    let replace_once_for_hook = Arc::clone(&replace_once);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _abandoned_hook =
        cluster.test_install_object_metadata_command_abandoned_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket && marker.key == hook_key
            ) && replace_once_for_hook.swap(false, Ordering::SeqCst)
            {
                let replacement = write_committed_direct_segment_for_with_versioning(
                    &replacement_cluster,
                    &replacement_bucket,
                    &replacement_key,
                    crate::BucketVersioningState::Suspended,
                    [0xb1; 16],
                    [0xb2; 16],
                    b"different concurrent replacement",
                );
                *replacement_generation_for_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(replacement.generation_id);
            }
        }));

    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_closure = Arc::clone(&action_calls);
    let replacement_generation_for_closure = Arc::clone(&replacement_generation);
    let result = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            move |stored| {
                action_calls_for_closure.fetch_add(1, Ordering::SeqCst);
                let generation_id = stored
                    .and_then(crate::StoredObject::as_live)
                    .map(|live| live.generation_id);
                if generation_id == Some(original.generation_id) {
                    Ok(())
                } else {
                    let expected = replacement_generation_for_closure
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .expect("replacement generation must be recorded");
                    assert_eq!(generation_id, Some(expected));
                    Err("conditional request conflict")
                }
            },
        )
        .expect("safe unpublished contention must be reinspected");

    assert!(matches!(result, Err("conditional request conflict")));
    assert_eq!(action_calls.load(Ordering::SeqCst), 2);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(!replace_once.load(Ordering::SeqCst));
    let replacement_generation = replacement_generation
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .expect("replacement generation must be recorded");
    let pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
    assert_eq!(
        stored.as_live().map(|live| live.generation_id),
        Some(replacement_generation)
    );
    drop(pg);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn insert_delete_marker_abandons_fresh_reissue_before_returning_contention() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(object_pg);
    let stale_log_index = map.test_next_metadata_command_log_index(pg_id);
    let occupant_bucket = bucket_for_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        object_pg,
        "marker-fresh-reissue-occupant-",
    );
    let occupant = create_bucket_metadata_command(pg_id, stale_log_index.get(), occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &occupant)
        .unwrap();

    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let write_sequence = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .next_object_write_sequence(bucket.as_str(), key.as_str())
        .unwrap();
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, stale_log_index),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence,
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale);

    let replacement_log_index = stale_log_index.get() + 1;
    let observed_replacement = Arc::new(Mutex::new(None));
    let observed_replacement_for_hook = Arc::clone(&observed_replacement);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let budget_guard = cluster.test_install_pending_object_metadata_partial_conflict_hook(
        Arc::new(move |command| {
            if command.id().log_index().get() == replacement_log_index
                && matches!(
                    command.payload(),
                    MetadataCommandPayload::InsertDeleteMarker(marker)
                        if marker.bucket == hook_bucket && marker.key == hook_key
                )
            {
                *observed_replacement_for_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(command.clone());
                true
            } else {
                false
            }
        }),
    );

    let error = cluster
        .test_apply_new_object_metadata_command_for_bucket(pg_id, &bucket, &stale)
        .expect_err("fresh replacement must be abandoned before contention is returned");
    drop(budget_guard);

    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
    ));
    let replacement = observed_replacement
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .expect("production reissue must create and expose the replacement command");
    assert_eq!(replacement.id().log_index().get(), replacement_log_index);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(pg
            .metadata_command_abandoned(node_id.as_u32(), &replacement)
            .unwrap());
    }
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn insert_delete_marker_reissue_pg_lock_timeout_is_not_retryable_while_source_is_pending() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(object_pg);
    let stale_log_index = map.test_next_metadata_command_log_index(pg_id);
    let occupant_bucket = bucket_for_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        object_pg,
        "marker-reissue-timeout-occupant-",
    );
    let occupant = create_bucket_metadata_command(pg_id, stale_log_index.get(), occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &occupant)
        .unwrap();

    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let write_sequence = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .next_object_write_sequence(bucket.as_str(), key.as_str())
        .unwrap();
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, stale_log_index),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence,
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale);

    let pg_lock = map.runtime_state().metadata_command_pg_lock(pg_id);
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        start_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("reissue hook must start the PG-lock holder");
        let _pg_guard = pg_lock.lock().unwrap_or_else(|error| error.into_inner());
        locked_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("test must release the reissue PG lock");
    });
    let hook_command_id = stale.id();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_reissue = Arc::clone(&hook_once);
    let locked_rx = Arc::new(Mutex::new(locked_rx));
    let locked_rx_for_reissue = Arc::clone(&locked_rx);
    let _hook = cluster.test_install_before_object_metadata_command_reissue_hook(Arc::new(
        move |command| {
            if command.id() == hook_command_id
                && hook_once_for_reissue.swap(false, Ordering::SeqCst)
            {
                start_tx.send(()).unwrap();
                locked_rx_for_reissue
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv_timeout(Duration::from_secs(5))
                    .expect("PG-lock holder must acquire the lock before reissue");
            }
        },
    ));
    let error = cluster
        .test_apply_new_object_metadata_command_for_bucket_with_budget(
            pg_id,
            &bucket,
            &stale,
            Duration::from_millis(25),
        )
        .expect_err("a pre-reissue deadline must retain non-retryable source uncertainty");
    release_tx.send(()).unwrap();
    holder.join().expect("PG-lock holder must not panic");

    assert!(!hook_once.load(Ordering::SeqCst));
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(
            StoreError::MetadataCommandIrrevocableConvergencePending {
                pg_id: error_pg_id,
                log_index,
                ..
            }
        ) if error_pg_id == object_pg && log_index == stale_log_index.get()
    ));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(stale.clone())
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(!pg
            .metadata_command_abandoned(node_id.as_u32(), &stale)
            .unwrap());
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn delete_current_object_retries_local_command_contention_before_publication() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete after contention");

    let attempts = Arc::new(AtomicUsize::new(0));
    let hook_attempts = Arc::clone(&attempts);
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectVersion(_)
            ) && hook_attempts.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected local current-delete publication contention",
                });
            }
            Ok(())
        }));

    let deleted = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(stored.is_some());
            Ok::<(), ()>(())
        })
        .expect("local command contention should be retried")
        .expect("delete condition should succeed");

    assert!(matches!(
        deleted.deleted,
        crate::DeletedCurrentObject::Live { .. }
    ));
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn insert_delete_marker_partial_apply_reopens_and_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let owner = crate::OwnerIdentity::from_principal("owner");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket
                        && marker.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected delete marker reopen apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            owner.clone(),
            |_| Ok::<(), ()>(()),
        )
        .expect("published delete marker must hand trailing convergence to recovery")
        .expect("delete marker precondition should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial delete-marker command must remain durable before reopen"
    );
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*primary_pg, &bucket, &key).unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
    }
    {
        let node_id = NodeId::new(2);
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
            .expect("reopen local map with in-flight delete-marker command"),
    );
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial delete-marker command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        match stored {
            crate::StoredObject::DeleteMarker(record) => {
                assert_eq!(record.version_id, crate::VersionId::from_u64(1));
                assert_eq!(record.owner, owner);
            }
            other => panic!("expected delete marker on node {node_id:?}, got {other:?}"),
        }
    }
    assert_object_version_counter_on_acting_nodes(
        &reopened, &node_ids, object_pg, &bucket, &key, 2,
    );
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn witnessed_delete_marker_reopens_after_reservation_expires() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let open_map = || {
        LocalClusterMap::open_with_configs(
            NodeId::new(1),
            node_ids.map(|node_id| {
                LocalNodeStoreConfig::new(
                    node_id,
                    tmp.path().join(format!("node-{:04}", node_id.as_u32())),
                )
            }),
            &pg_ids,
            ec_shape,
        )
    };
    let map = Arc::new(open_map().unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    map.node(NodeId::new(0))
        .unwrap()
        .metadata_command_client()
        .apply_metadata_command_and_record(pg_id, &command)
        .unwrap();
    let bucket_pg_id = PgId::new(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(&bucket),
    );
    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    bucket_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap()
        .test_set_bucket_write_reservation_lease_deadline(
            &proof.reservation_id,
            crate::clock::current_time_millis().saturating_sub(1),
        )
        .unwrap();
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "owner loss must leave the exact witness-only command pending"
    );
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        open_map().expect("reopen must converge a witnessed command with expired reservation"),
    );
    assert!(pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Ok(crate::StoredObject::DeleteMarker(_))
        ));
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn publication_started_delete_marker_reopens_after_reservation_expires() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let open_map = || {
        LocalClusterMap::open_with_configs(
            NodeId::new(1),
            node_ids.map(|node_id| {
                LocalNodeStoreConfig::new(
                    node_id,
                    tmp.path().join(format!("node-{:04}", node_id.as_u32())),
                )
            }),
            &pg_ids,
            ec_shape,
        )
    };
    let map = Arc::new(open_map().unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    primary_pg
        .try_insert_pending_metadata_command_slot(NodeId::new(1).as_u32(), &command, Some(&bucket))
        .unwrap();
    primary_pg
        .mark_pending_metadata_command_publication_started(NodeId::new(1).as_u32(), &command)
        .unwrap();
    drop(primary_pg);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(pg
            .applied_metadata_command_log_entry_hashes(node_id.as_u32(), &command)
            .unwrap()
            .is_none());
    }
    let bucket_pg_id = PgId::new(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(&bucket),
    );
    map.metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap()
        .test_set_bucket_write_reservation_lease_deadline(
            &proof.reservation_id,
            crate::clock::current_time_millis().saturating_sub(1),
        )
        .unwrap();
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(open_map().expect("reopen must converge a publication-started command"));
    assert!(pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Ok(crate::StoredObject::DeleteMarker(_))
        ));
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn publication_started_fully_applied_delete_marker_reopens_and_cleans_terminal_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let open_map = || {
        LocalClusterMap::open_with_configs(
            NodeId::new(1),
            node_ids.map(|node_id| {
                LocalNodeStoreConfig::new(
                    node_id,
                    tmp.path().join(format!("node-{:04}", node_id.as_u32())),
                )
            }),
            &pg_ids,
            ec_shape,
        )
    };
    let map = Arc::new(open_map().unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    primary_pg
        .try_insert_pending_metadata_command_slot(NodeId::new(1).as_u32(), &command, Some(&bucket))
        .unwrap();
    primary_pg
        .mark_pending_metadata_command_publication_started(NodeId::new(1).as_u32(), &command)
        .unwrap();
    drop(primary_pg);

    for node_id in node_ids {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_some());
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        open_map().expect("reopen must clean a fully applied publication-started command"),
    );
    assert!(pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Ok(crate::StoredObject::DeleteMarker(_))
        ));
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn publication_started_recovery_wait_timeout_is_irrevocable() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    primary_pg
        .try_insert_pending_metadata_command_slot(NodeId::new(1).as_u32(), &command, Some(&bucket))
        .unwrap();
    primary_pg
        .mark_pending_metadata_command_publication_started(NodeId::new(1).as_u32(), &command)
        .unwrap();
    drop(primary_pg);

    let MetadataCommandRecoveryAdmission::Leader(owner_guard) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("test must establish the recovery owner")
    };
    let timeout_command = command.clone();
    let timeout_hook =
        cluster.test_install_pending_command_recovery_timeout_hook(Arc::new(move |command| {
            command == &timeout_command
        }));
    let waiter_cluster = Arc::clone(&cluster);
    let waiter_command = command.clone();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || {
        result_tx
            .send(
                waiter_cluster
                    .drain_pending_metadata_command_with_recovery_gate(pg_id, &waiter_command),
            )
            .unwrap();
    });
    let result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("recovery waiter must classify marker-only publication");
    waiter.join().unwrap();
    drop(timeout_hook);
    assert!(matches!(
        result,
        Err(crate::ObjectPgActionError::Store(
            StoreError::MetadataCommandIrrevocableConvergencePending { .. }
        ))
    ));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_some());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(pg
            .applied_metadata_command_log_entry_hashes(node_id.as_u32(), &command)
            .unwrap()
            .is_none());
    }
    drop(owner_guard);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_recovery_gate(pg_id, &command)
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn witnessed_delete_marker_pending_drain_ignores_expired_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let pg_lock = map.runtime_state().metadata_command_pg_lock(pg_id);
    let pg_guard = pg_lock.lock().unwrap_or_else(|error| error.into_inner());
    let (attempt_started_tx, attempt_started_rx) = std::sync::mpsc::sync_channel(1);
    let command_id = command.id();
    let notify_once = Arc::new(AtomicBool::new(true));
    let notify_once_for_hook = Arc::clone(&notify_once);
    let _hook = cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(
        move |attempted_command| {
            if attempted_command.id() == command_id
                && notify_once_for_hook.swap(false, Ordering::SeqCst)
            {
                attempt_started_tx.send(()).unwrap();
            }
            Ok(())
        },
    ));
    let drain_cluster = cluster.clone();
    let drain_command = command.clone();
    let drain = std::thread::spawn(move || {
        drain_cluster.drain_pending_metadata_command_with_recovery_gate(pg_id, &drain_command)
    });
    attempt_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("pending drainer must reach command apply while the PG lock is held");

    map.node(NodeId::new(0))
        .unwrap()
        .metadata_command_client()
        .apply_metadata_command_and_record(pg_id, &command)
        .unwrap();
    let bucket_pg_id = PgId::new(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(&bucket),
    );
    map.metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap()
        .test_set_bucket_write_reservation_lease_deadline(
            &proof.reservation_id,
            crate::clock::current_time_millis().saturating_sub(1),
        )
        .unwrap();
    drop(pg_guard);

    assert_eq!(
        drain
            .join()
            .expect("pending command drainer thread must not panic")
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Ok(crate::StoredObject::DeleteMarker(_))
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn open_convergence_validates_reservation_before_first_apply_only() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(
        LocalClusterMap::open_with_configs(
            NodeId::new(1),
            node_ids.map(|node_id| {
                LocalNodeStoreConfig::new(
                    node_id,
                    tmp.path().join(format!("node-{:04}", node_id.as_u32())),
                )
            }),
            &pg_ids,
            ec_shape,
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: primary_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let command_id = command.id();
    let reservation_id = proof.reservation_id.clone();
    let hook_bucket = bucket.clone();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let _hook = test_install_open_metadata_command_after_apply_hook(Arc::new(
        move |nodes, pg_routes, _node_id, applied_command| {
            if applied_command.id() != command_id || hook_ran_for_hook.swap(true, Ordering::SeqCst)
            {
                return;
            }
            let topology = nodes
                .values()
                .next()
                .expect("reopened local map must contain a node")
                .storage_node()
                .pg_topology();
            let bucket_pg_id = PgId::new(topology.bucket_pg_for(&hook_bucket));
            let bucket_primary = pg_routes
                .get(&bucket_pg_id)
                .expect("bucket PG route must exist")
                .primary_node_id();
            nodes
                .get(&bucket_primary)
                .expect("bucket primary must exist")
                .storage_node()
                .get_pg(bucket_pg_id.get())
                .unwrap()
                .test_set_bucket_write_reservation_lease_deadline(
                    &reservation_id,
                    crate::clock::current_time_millis().saturating_sub(1),
                )
                .unwrap();
        },
    ));
    converge_in_flight_metadata_command_on_open(
        &map.nodes,
        &map.pg_routes,
        pg_id,
        NodeId::new(1),
        ClusterEpoch::INITIAL,
        &command,
        false,
    )
    .expect("open recovery must not revalidate after creating its first witness");
    assert!(hook_ran.load(Ordering::SeqCst));
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Ok(crate::StoredObject::DeleteMarker(_))
        ));
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn suspended_delete_marker_reissue_conflict_classifies_replacement_identity_after_publication() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let owner = crate::OwnerIdentity::from_principal("owner");

    let pg_id = PgId::new(object_pg);
    let stale_log_index = map.test_next_metadata_command_log_index(pg_id);
    let occupant_bucket = bucket_for_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        object_pg,
        "marker-reissue-occupant-",
    );
    let occupant = create_bucket_metadata_command(pg_id, stale_log_index.get(), occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &occupant)
        .unwrap();

    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let write_sequence = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .next_object_write_sequence(bucket.as_str(), key.as_str())
        .unwrap();
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, stale_log_index),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner,
            write_sequence,
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale);
    let replacement_log_index = stale_log_index.get() + 1;

    let helper_ran = Arc::new(AtomicBool::new(false));
    let helper_ran_for_action = Arc::clone(&helper_ran);
    let helper_cluster = cluster.clone();
    let helper_stale = stale.clone();
    let helper_map = Arc::clone(&map);
    let tail_command = Arc::new(Mutex::new(None));
    let tail_command_for_action = Arc::clone(&tail_command);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let expire_once = Arc::new(AtomicBool::new(true));
    let expire_once_for_hook = Arc::clone(&expire_once);
    let budget_guard = cluster.test_install_pending_object_metadata_partial_conflict_hook(
        Arc::new(move |command| {
            if command.id().log_index().get() == replacement_log_index
                && matches!(
                    command.payload(),
                    MetadataCommandPayload::InsertDeleteMarker(marker)
                        if marker.bucket == hook_bucket && marker.key == hook_key
                )
                && expire_once_for_hook.swap(false, Ordering::SeqCst)
            {
                return true;
            }
            false
        }),
    );
    let error = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            crate::OwnerIdentity::from_principal("owner"),
            move |_| {
                if !helper_ran_for_action.swap(true, Ordering::SeqCst) {
                    let replacement = helper_cluster
                        .test_reissue_pending_metadata_command(pg_id, &helper_stale)
                        .unwrap()
                        .expect("concurrent helper must replace the stale command");
                    assert_eq!(replacement.id().log_index().get(), replacement_log_index);
                    let witness_node_id = NodeId::new(0);
                    let witness_pg = helper_map
                        .node(witness_node_id)
                        .unwrap()
                        .storage_node()
                        .get_pg(pg_id.get())
                        .unwrap();
                    witness_pg
                        .apply_metadata_command_and_record(witness_node_id.as_u32(), &replacement)
                        .unwrap();
                    let tail_bucket = bucket_for_pg(
                        helper_map
                            .node(witness_node_id)
                            .unwrap()
                            .storage_node()
                            .pg_topology(),
                        pg_id.get(),
                        "marker-reissue-tail-",
                    );
                    let tail = create_bucket_metadata_command(
                        pg_id,
                        replacement_log_index + 1,
                        tail_bucket,
                    );
                    witness_pg
                        .apply_metadata_command_and_record(witness_node_id.as_u32(), &tail)
                        .unwrap();
                    *tail_command_for_action
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = Some(tail);
                }
                Ok::<(), ()>(())
            },
        )
        .unwrap_err();
    drop(budget_guard);

    assert!(helper_ran.load(Ordering::SeqCst));
    assert!(!expire_once.load(Ordering::SeqCst));
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(
            StoreError::MetadataCommandIrrevocableConvergencePending {
                pg_id: error_pg_id,
                log_index,
                ..
            }
        ) if error_pg_id == object_pg && log_index == replacement_log_index
    ));
    let pending = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("published replacement must remain recoverable");
    assert_eq!(pending.id().log_index().get(), replacement_log_index);

    let tail = tail_command
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .expect("helper must retain its later command");
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &pending)
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &tail)
            .unwrap();
    }

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn suspended_delete_marker_exact_retry_rechecks_after_abandonment() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    let owner = crate::OwnerIdentity::from_principal("owner");
    cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            owner.clone(),
            |_| Ok::<(), ()>(()),
        )
        .unwrap()
        .unwrap();

    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let write_sequence = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .next_object_write_sequence(bucket.as_str(), key.as_str())
        .unwrap();
    let pending = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: crate::VersionId::Null,
            owner: owner.clone(),
            write_sequence,
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending);
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_closure = Arc::clone(&action_calls);
    let replacement_generation = Arc::new(Mutex::new(None));
    let replacement_generation_for_closure = Arc::clone(&replacement_generation);
    let replacement_cluster = cluster.clone();
    let replacement_bucket = bucket.clone();
    let replacement_key = key.clone();
    let pending_for_closure = pending.clone();
    let retried = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            owner,
            move |stored| {
                let call = action_calls_for_closure.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    assert!(matches!(stored, Some(crate::StoredObject::DeleteMarker(_))));
                    replacement_cluster
                        .record_abandoned_metadata_command_to_acting_set(&pending_for_closure)
                        .unwrap();
                    replacement_cluster
                        .remove_pending_metadata_command_for_bucket(
                            pg_id,
                            &replacement_bucket,
                            &pending_for_closure,
                        )
                        .unwrap();
                    let replacement = write_committed_direct_segment_for_with_versioning(
                        &replacement_cluster,
                        &replacement_bucket,
                        &replacement_key,
                        crate::BucketVersioningState::Suspended,
                        [0xd1; 16],
                        [0xd2; 16],
                        b"concurrent suspended replacement",
                    );
                    *replacement_generation_for_closure.lock().unwrap() =
                        Some(replacement.generation_id);
                } else {
                    let expected = replacement_generation_for_closure
                        .lock()
                        .unwrap()
                        .expect("replacement generation must be recorded");
                    assert!(matches!(
                        stored,
                        Some(crate::StoredObject::Live(live))
                            if live.generation_id == expected
                    ));
                }
                Ok::<(), ()>(())
            },
        )
        .expect("abandoned matching command must restart from a fresh snapshot")
        .expect("delete-marker condition should succeed after reinspection");

    assert_eq!(retried.version_id, crate::VersionId::Null);
    assert_eq!(action_calls.load(Ordering::SeqCst), 2);
    assert!(replacement_generation.lock().unwrap().is_some());
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}
