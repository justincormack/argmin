// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::DeleteFinalizedBucketCommand;
use crate::test_support::StorageClusterLifecycleTestSupport as _;
use crate::{BucketAclSummary, BucketIdentityGenerations, BucketState};

#[test]
fn bucket_delete_finalize_admission_bounds_distinct_roots_and_allows_exact_retries() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let runtime = cluster.local_map.runtime_state();
    let first = BucketDeleteFinalizeRoot {
        bucket: crate::tests::bucket_name("delete-admission-first"),
        bucket_incarnation_generation: 1,
    };
    let second = BucketDeleteFinalizeRoot {
        bucket: crate::tests::bucket_name("delete-admission-second"),
        bucket_incarnation_generation: 1,
    };

    let first_admission = runtime
        .try_admit_bucket_delete_finalize(first.clone(), 1)
        .expect("the first distinct delete should reserve capacity");
    let duplicate_admission = runtime
        .try_admit_bucket_delete_finalize(first.clone(), 1)
        .expect("an exact retry must not be rejected by its own reservation");
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second.clone(), 1)
            .is_none(),
        "a distinct delete must see backpressure at the capacity boundary"
    );
    drop(first_admission);
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second.clone(), 1)
            .is_none(),
        "one failed exact retry must not release another retry's shared reservation"
    );
    drop(duplicate_admission);
    let released_admission = runtime
        .try_admit_bucket_delete_finalize(second.clone(), 1)
        .expect("capacity must release after every failed exact retry exits");
    drop(released_admission);

    let first_admission = runtime
        .try_admit_bucket_delete_finalize(first.clone(), 1)
        .expect("the first root should reserve capacity again");
    first_admission.commit();
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second.clone(), 1)
            .is_none(),
        "committing admission must retain capacity until finalization finishes"
    );
    let delayed_exact_retry = runtime
        .try_admit_bucket_delete_finalize(first.clone(), 1)
        .expect("an exact retry must share outstanding capacity");
    runtime.finish_bucket_delete_finalize_work(&first);
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second.clone(), 1)
            .is_none(),
        "an in-flight exact retry must retain capacity after finalization finishes"
    );
    delayed_exact_retry.commit();
    assert_eq!(
        runtime.test_bucket_delete_finalize_outstanding_depth(),
        1,
        "a delayed exact retry commit must restore one outstanding root"
    );
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second.clone(), 1)
            .is_none(),
        "a delayed exact retry commit must not exceed the capacity bound"
    );
    runtime.finish_bucket_delete_finalize_work(&first);
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(second, 1)
            .is_some(),
        "terminal finalization must release admission capacity"
    );
}

#[test]
fn recovered_bucket_delete_begin_counts_toward_finalize_admission_capacity() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let runtime = cluster.local_map.runtime_state();
    let bucket = crate::tests::bucket_name("delete-recovered-begin-capacity");
    let finalize_root = BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 7,
    };

    assert!(
        runtime.enqueue_bucket_delete_begin(crate::BucketDeleteBeginRoot {
            bucket,
            bucket_execution_generation: 11,
            bucket_incarnation_generation: 7,
        })
    );
    assert_eq!(
        runtime.test_bucket_delete_finalize_outstanding_depth(),
        1,
        "a recovered durable begin must consume finalizer capacity"
    );
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(
                BucketDeleteFinalizeRoot {
                    bucket: crate::tests::bucket_name("delete-recovered-begin-blocked"),
                    bucket_incarnation_generation: 1,
                },
                1,
            )
            .is_none(),
        "new deletes must see backpressure from recovered begin work"
    );
    runtime.finish_bucket_delete_finalize_work(&finalize_root);
}

#[test]
fn finishing_stale_begin_preserves_newer_begin_for_same_bucket_incarnation() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let runtime = cluster.local_map.runtime_state();
    let bucket = crate::tests::bucket_name("delete-begin-same-incarnation");
    let stale = crate::BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: 10,
        bucket_incarnation_generation: 20,
    };
    let current = crate::BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: 11,
        bucket_incarnation_generation: 20,
    };
    let finalize_root = current.finalize_root();

    assert!(runtime.enqueue_bucket_delete_begin(stale.clone()));
    assert!(runtime.enqueue_bucket_delete_begin(current.clone()));
    assert_eq!(
        runtime.try_take_reclaim_work(),
        Some(ReclaimWorkItem::BucketDeleteBegin(stale.clone()))
    );

    runtime.finish_bucket_delete_begin_work(&stale);

    assert_eq!(
        runtime.test_bucket_delete_finalize_outstanding_depth(),
        1,
        "retiring stale C1 must retain the shared capacity slot for C2"
    );
    assert_eq!(
        runtime.try_take_reclaim_work(),
        Some(ReclaimWorkItem::BucketDeleteBegin(current.clone())),
        "retiring stale C1 must not erase same-incarnation C2"
    );

    runtime.promote_bucket_delete_begin_to_finalize(&current);
    assert_eq!(
        runtime.try_take_reclaim_work(),
        Some(ReclaimWorkItem::BucketDelete(finalize_root.clone()))
    );
    assert_eq!(runtime.test_bucket_delete_finalize_outstanding_depth(), 1);
    runtime.finish_bucket_delete_finalize_work(&finalize_root);
    assert_eq!(runtime.test_bucket_delete_finalize_outstanding_depth(), 0);
}

#[test]
fn bucket_delete_retained_attempt_keeps_finalize_admission_capacity() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let bucket = crate::tests::bucket_name("delete-retained-admission");
    create_test_bucket(&cluster, &bucket);
    let info = cluster.test_head_bucket_raw(&bucket).unwrap();
    let identity = BucketIdentityGenerations {
        bucket_execution_generation: info.bucket_execution_generation,
        bucket_incarnation_generation: info.bucket_incarnation_generation,
    };
    let _hook =
        cluster.test_install_after_bucket_delete_final_visibility_proven_hook(Arc::new(|| {
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 0,
                now_ms: 1,
            })
        }));
    let handle =
        crate::StorageClusterRouteHandle::from_static_cluster(Arc::clone(&cluster)).unwrap();
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_bucket_route(&bucket).unwrap();

    let error = route
        .begin_bucket_delete_with_finalize_capacity_for_test(identity, 1)
        .unwrap_err();
    assert_eq!(error.kind(), &crate::BucketWriteDrainFailureKind::SlowDown);
    let runtime = cluster.local_map.runtime_state();
    assert_eq!(
        runtime.test_bucket_delete_finalize_outstanding_depth(),
        1,
        "a durable retained delete attempt must continue consuming capacity"
    );
    assert!(
        runtime
            .try_admit_bucket_delete_finalize(
                BucketDeleteFinalizeRoot {
                    bucket: crate::tests::bucket_name("delete-retained-admission-blocked"),
                    bucket_incarnation_generation: 1,
                },
                1,
            )
            .is_none(),
        "a retained durable attempt must apply backpressure to another root"
    );
}

#[test]
fn bucket_delete_backpressure_precedes_durable_delete_state() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let bucket = crate::tests::bucket_name("delete-backpressure-target");
    create_test_bucket(&cluster, &bucket);
    let info = cluster.test_head_bucket_raw(&bucket).unwrap();
    let identity = BucketIdentityGenerations {
        bucket_execution_generation: info.bucket_execution_generation,
        bucket_incarnation_generation: info.bucket_incarnation_generation,
    };
    let runtime = cluster.local_map.runtime_state();
    let blocker = runtime
        .try_admit_bucket_delete_finalize(
            BucketDeleteFinalizeRoot {
                bucket: crate::tests::bucket_name("delete-backpressure-blocker"),
                bucket_incarnation_generation: 1,
            },
            1,
        )
        .unwrap();
    let handle =
        crate::StorageClusterRouteHandle::from_static_cluster(Arc::clone(&cluster)).unwrap();
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_bucket_route(&bucket).unwrap();

    let error = route
        .begin_bucket_delete_with_finalize_capacity_for_test(identity, 1)
        .unwrap_err();
    assert_eq!(error.kind(), &crate::BucketWriteDrainFailureKind::SlowDown);
    assert_eq!(
        cluster.test_head_bucket_raw(&bucket).unwrap().state,
        BucketState::Active,
        "backpressure must reject before installing the durable delete drain or mark"
    );

    drop(blocker);
    route
        .begin_bucket_delete_with_finalize_capacity_for_test(identity, 1)
        .unwrap();
    assert_eq!(
        cluster.test_head_bucket_raw(&bucket).unwrap().state,
        BucketState::Deleting
    );
    assert_eq!(cluster.test_bucket_delete_finalize_outstanding_depth(), 1);
}

#[test]
fn bucket_delete_progress_observation_binds_the_exact_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let target = crate::tests::bucket_name("delete-progress-target");
    let canary = crate::tests::bucket_name("delete-progress-canary");
    create_test_bucket(&cluster, &target);
    create_test_bucket(&cluster, &canary);

    cluster
        .test_begin_durable_bucket_delete_drain(&target)
        .unwrap();

    assert_eq!(
        cluster
            .test_observe_bucket_delete_progress(&target)
            .unwrap(),
        crate::TestBucketDeleteProgress {
            bucket_state: Some(crate::BucketState::Active),
            has_durable_write_drain: true,
            has_pending_metadata_command: false,
        }
    );
    assert_eq!(
        cluster
            .test_observe_bucket_delete_progress(&canary)
            .unwrap(),
        crate::TestBucketDeleteProgress {
            bucket_state: Some(crate::BucketState::Active),
            has_durable_write_drain: false,
            has_pending_metadata_command: false,
        }
    );
}

#[test]
fn bucket_delete_subject_binds_incarnation_and_exactly_once_transition() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let target = crate::tests::bucket_name("delete-subject-target");
    let canary = crate::tests::bucket_name("delete-subject-canary");
    create_test_bucket(&cluster, &target);
    create_test_bucket(&cluster, &canary);
    let canary_subject = cluster
        .test_capture_bucket_delete_begin_subject(&canary)
        .unwrap();
    let initial_generation = cluster
        .test_head_bucket_raw(&target)
        .unwrap()
        .bucket_execution_generation;
    assert!(!cluster
        .test_bucket_execution_generation_is_newer_than(&target, initial_generation)
        .unwrap());
    cluster
        .put_bucket_versioning_and_load_info_raw(&target, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert!(cluster
        .test_bucket_execution_generation_is_newer_than(&target, initial_generation)
        .unwrap());

    let target_subject = cluster
        .test_capture_bucket_delete_begin_subject(&target)
        .unwrap();

    cluster.test_begin_current_bucket_delete(&target).unwrap();

    assert!(cluster
        .test_current_bucket_delete_marked_once(&target_subject)
        .unwrap());
    assert!(!cluster
        .test_current_bucket_delete_marked_once(&canary_subject)
        .unwrap());
    assert_eq!(
        cluster.test_bucket_presence(&target).unwrap(),
        crate::test_support::TestBucketPresence::Deleting
    );
    assert_eq!(
        cluster.test_bucket_presence(&canary).unwrap(),
        crate::test_support::TestBucketPresence::Active
    );

    assert_eq!(
        cluster.try_finalize_bucket_delete(&target).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    create_test_bucket(&cluster, &target);
    assert!(cluster
        .test_current_bucket_is_distinct_active_incarnation(&target_subject)
        .unwrap());
    assert!(!cluster
        .test_current_bucket_is_distinct_active_incarnation(&canary_subject)
        .unwrap());
}

#[test]
fn seeded_bucket_delete_subject_uses_the_acquired_drain_generation() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(map).unwrap());
    let bucket = crate::tests::bucket_name("seeded-delete-subject");
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let cluster_for_hook = Arc::clone(&cluster);
    let bucket_for_hook = bucket.clone();
    let hook = crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
        target: Some(bucket.clone()),
        before_begin_bucket_delete_drain: Some(Arc::new(move || {
            if !hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                cluster_for_hook
                    .put_bucket_versioning_and_load_info_raw(
                        &bucket_for_hook,
                        crate::BucketVersioningState::Enabled,
                    )
                    .unwrap();
            }
        })),
        ..crate::node::BucketScopedTestHooks::default()
    });

    let subject = cluster
        .test_seed_bucket_delete_attempt(
            &bucket,
            crate::TestBucketDeleteAttemptOutcomeKind::Retryable,
            crate::TestBucketDeleteAttemptPhase::ReservationWait,
            "seeded after generation change".to_string(),
        )
        .unwrap();
    drop(hook);
    assert!(hook_ran.load(Ordering::SeqCst));
    let subject_debug = format!("{subject:?}");
    assert!(subject_debug.contains(bucket.as_str()));
    assert!(!subject_debug.contains("execution_generation"));
    assert!(!subject_debug.contains("incarnation_generation"));

    cluster.test_begin_current_bucket_delete(&bucket).unwrap();
    assert!(cluster
        .test_current_bucket_delete_marked_once(&subject)
        .unwrap());
}

fn apply_delete_finalized_bucket_command_to_pg(
    pg: &crate::PgStore,
    node_id: NodeId,
    pg_id: PgId,
    log_index: MetadataCommandLogIndex,
    bucket: &BucketName,
) {
    let deleting = crate::PgMetadataStore::head_bucket_record_raw(pg, bucket).unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            deleting.bucket_execution_generation,
            deleting.bucket_incarnation_generation,
        )),
    );
    pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
        .unwrap();
}

fn delete_bucket_row_for_divergence_test(pg: &crate::PgStore, bucket: &BucketName) {
    pg.test_delete_bucket_row(bucket).unwrap();
    pg.refresh_metadata_command_state_digest().unwrap();
}

#[test]
fn finalized_bucket_delete_clears_pending_versioning_command_for_recreate() {
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
        bucket_for_pg(topology, 1, "partial-versioning-delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
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

    let published = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(published.versioning, crate::BucketVersioningState::Enabled);
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "failed versioning command should remain pending before delete"
    );
    let old_partial_generation = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_execution_generation
    };

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "finalized delete must clear stale pending commands for the old bucket incarnation"
    );

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
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
    let recreated_generation = match recreated {
        crate::BucketCreateAttemptOutcome::Created(info) => info.bucket_execution_generation(),
        other => panic!("expected recreated bucket, got {other:?}"),
    };
    assert!(recreated_generation > old_partial_generation);

    let updated = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > recreated_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn finalized_bucket_delete_removes_replicated_create_rows() {
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
        bucket_for_pg(topology, 1, "delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let created_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let pre_delete_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(pre_delete_generation > created_generation);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    let deleting_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(deleting_generation > pre_delete_generation);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.bucket_execution_generation, deleting_generation);
    }
    assert_bucket_execution_counter_on_acting_nodes(&map, &node_ids, 1, deleting_generation);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err());
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
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
        recreated,
        crate::BucketCreateAttemptOutcome::Created(info)
            if info.name() == &bucket
                && info.bucket_execution_generation() > pre_delete_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .name,
            bucket
        );
    }
}

#[test]
fn finalized_bucket_delete_after_reopen_does_not_need_begin_waiter() {
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
        bucket_for_pg(topology, 1, "delete-finalize-reopen-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    {
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .unwrap();
    }

    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    assert_clean_metadata_command_stream(&map, &[1]);
    drop(cluster);
    drop(map);

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();

    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "finalization must not require the process that began DeleteBucket"
    );
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::NotFound,
        "finalized delete should be idempotent after row removal"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn bucket_delete_finalizer_resumes_bounded_pg_scan_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = vec![8, 0, 16, 4, 12, 2, 14, 6, 10, 1, 15, 3, 13, 5, 11, 7, 9];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
        for pg_id in &pg_ids {
            set_route_primary(&mut map, *pg_id, NodeId::new(1));
        }
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "bounded-finalizer-reopen-")
        };
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .unwrap();

        assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Continue
        );
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let progress = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
            .unwrap()
            .expect("first finalizer batch should persist progress");
        assert_eq!(progress.finalizer_next_object_pg_id, Some(8));
        bucket
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    for pg_id in &pg_ids {
        set_route_primary(&mut reopened, *pg_id, NodeId::new(1));
    }
    let reopened = Arc::new(reopened);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Continue
    );
    {
        let bucket_pg = reopened
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let progress = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
            .unwrap()
            .expect("reopened finalizer should advance from persisted progress");
        assert_eq!(progress.finalizer_next_object_pg_id, Some(16));
    }
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(0).unwrap());
    let error = cluster
        .try_finalize_bucket_delete_internal(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "route expiry should defer finalization without resetting progress, got {error:?}"
    );
    {
        let bucket_pg = reopened
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let progress = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
            .unwrap()
            .expect("route failure should retain finalizer progress");
        assert_eq!(progress.finalizer_next_object_pg_id, Some(16));
    }
    cluster.test_store_route_map_validity(
        RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_add(60_000))
            .unwrap(),
    );
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
}

#[test]
fn durable_bucket_finalize_scan_recovers_lost_local_queue_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "delete-finalize-scan-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .unwrap();
        bucket
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();

    let scan = reopened_cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 1,
        "startup scan should rediscover the deleting bucket without an in-memory hint"
    );
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket
    ));
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn missing_bucket_finalize_scenario_rejects_existing_deleting_bucket() {
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
        bucket_for_pg(topology, 1, "missing-finalize-reject-existing-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let deleting = cluster.test_head_bucket_raw(&bucket).unwrap();
    assert_eq!(deleting.state, crate::BucketState::Deleting);
    assert_eq!(
        deleting.bucket_incarnation_generation, 1,
        "the adversarial bucket must have the generation previously forged by the helper"
    );
    let outstanding_before = cluster.test_bucket_delete_finalize_outstanding_depth();

    let error = cluster
        .test_enqueue_missing_bucket_delete_finalize(&bucket)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotFinalizedForDelete {
            state: crate::BucketState::Deleting,
        })
    ));
    assert_eq!(
        cluster.test_bucket_delete_finalize_outstanding_depth(),
        outstanding_before,
        "rejected synthetic work must not alter finalize queue ownership"
    );
    assert_eq!(cluster.try_take_reclaim_work(), None);
}

#[test]
fn durable_reclaim_discovery_scans_bounded_pg_batches() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = vec![8, 0, 16, 4, 12, 2, 14, 6, 10, 1, 15, 3, 13, 5, 11, 7, 9];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
        for pg_id in &pg_ids {
            set_route_primary(&mut map, *pg_id, NodeId::new(1));
        }
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 16, "bounded-durable-discovery-")
        };
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .unwrap();
        bucket
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    for pg_id in &pg_ids {
        set_route_primary(&mut reopened, *pg_id, NodeId::new(1));
    }
    let reopened = Arc::new(reopened);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    let excluded_payload = HashSet::new();
    let excluded_begin = HashSet::new();
    let excluded_finalize = HashSet::new();

    let first = cluster.enqueue_durable_reclaim_work_batch_excluding(
        None,
        8,
        &excluded_payload,
        &excluded_begin,
        &excluded_finalize,
    );
    assert_eq!(first.outcome, crate::DurableReclaimScanOutcome::Complete);
    assert_eq!(first.scanned_pgs, 8);
    assert_eq!(first.next_pg_id, Some(8));
    assert!(!first.retry_pass_required);
    assert!(cluster.try_take_reclaim_work().is_none());

    let second = cluster.enqueue_durable_reclaim_work_batch_excluding(
        first.next_pg_id,
        8,
        &excluded_payload,
        &excluded_begin,
        &excluded_finalize,
    );
    assert_eq!(second.outcome, crate::DurableReclaimScanOutcome::Complete);
    assert_eq!(second.scanned_pgs, 8);
    assert_eq!(second.next_pg_id, Some(16));
    assert!(!second.retry_pass_required);
    assert!(cluster.try_take_reclaim_work().is_none());

    let third = cluster.enqueue_durable_reclaim_work_batch_excluding(
        second.next_pg_id,
        8,
        &excluded_payload,
        &excluded_begin,
        &excluded_finalize,
    );
    assert_eq!(third.outcome, crate::DurableReclaimScanOutcome::Complete);
    assert_eq!(third.scanned_pgs, 1);
    assert_eq!(third.next_pg_id, None);
    assert!(!third.retry_pass_required);
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(root)) if root.bucket == bucket
    ));
}

#[test]
fn durable_bucket_finalize_scan_skips_excluded_bucket() {
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
        bucket_for_pg(topology, 1, "delete-finalize-excluded-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let excluded = std::collections::HashSet::from([bucket.clone()]);
    let scan = cluster.enqueue_durable_bucket_delete_finalize_roots_excluding(&excluded);
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 0);
    assert_eq!(cluster.try_take_reclaim_work(), None);

    let scan = cluster
        .enqueue_durable_bucket_delete_finalize_roots_excluding(&std::collections::HashSet::new());
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 1);
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket
    ));
}

#[test]
fn durable_bucket_finalize_scan_prioritizes_expired_claimed_bucket() {
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
            bucket_for_pg(topology, 1, "delete-finalize-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-b-"),
        )
    };
    assert!(
        bucket_a < bucket_b,
        "test bucket names should exercise an earlier unclaimed bucket"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);
    cluster
        .test_begin_bucket_delete_if_current(&bucket_b)
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting_b =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket_b).unwrap();
    crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket_b,
        deleting_b.bucket_incarnation_generation,
        "held-finalizer-claim-b",
        "external-worker",
        ClusterEpoch::INITIAL,
        10,
        Some(20),
        10,
    )
    .unwrap()
    .expect("later bucket should be claimable");
    drop(primary_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket_a)
        .unwrap();

    let scan = crate::clock::with_time_override(21, || {
        cluster.enqueue_durable_bucket_delete_finalize_roots()
    });
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 2,
        "scan should enqueue the expired claimed bucket and the earlier deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket_b
    ));
    assert_eq!(
        crate::clock::with_time_override(21, || { cluster.try_finalize_bucket_delete(&bucket_b) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "expired stale claim work should be recoverable from the durable scan"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket_a
    ));
    assert_eq!(
        crate::clock::with_time_override(22, || { cluster.try_finalize_bucket_delete(&bucket_a) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_continues_after_unavailable_pg() {
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
        bucket_for_pg(topology, 1, "delete-finalize-scan-later-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    drop(cluster);

    let mut map = Arc::try_unwrap(map).expect("test should hold the only map reference");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = PgState::Peering;
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let scan = cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(
        scan.errors, 1,
        "unavailable PG should be reported in scan stats"
    );
    assert_eq!(
        scan.queued, 1,
        "scan should continue and enqueue the later healthy deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket
    ));
}

#[test]
fn bucket_finalize_durable_claim_blocks_second_worker_until_released() {
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
        bucket_for_pg(topology, 1, "delete-finalize-claim-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    let claimed_at = crate::clock::current_time_millis();
    let held = crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        "held-finalizer-claim",
        "external-worker",
        ClusterEpoch::INITIAL,
        claimed_at,
        claimed_at.checked_add(60_000),
        claimed_at,
    )
    .unwrap()
    .expect("test should be able to hold the finalizer claim");
    drop(primary_pg);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "a non-expired durable finalizer claim should block a second worker"
    );
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );

    crate::PgMetadataStore::release_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        &held.claim_id,
        &held.owner_token,
        held.cluster_epoch,
    )
    .unwrap();
    drop(primary_pg);
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
}

#[test]
fn stale_bucket_finalize_claim_for_deleted_generation_does_not_block_recreated_bucket() {
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
        bucket_for_pg(topology, 1, "delete-finalize-stale-claim-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let old_deleting = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket)
        .expect("old bucket should be deleting");
    let _stale_claim = crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        old_deleting.bucket_incarnation_generation,
        "stale-finalizer-claim",
        "worker-that-lost-response",
        ClusterEpoch::INITIAL,
        10,
        Some(70_000),
        10,
    )
    .unwrap()
    .expect("old delete generation should be claimable");
    drop(primary_pg);

    let pg_id = PgId::new(1);
    let delete_log_index = map.test_next_metadata_command_log_index(pg_id);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        apply_delete_finalized_bucket_command_to_pg(&pg, node_id, pg_id, delete_log_index, &bucket);
    }

    create_test_bucket(&cluster, &bucket);
    let recreated = cluster.test_head_bucket_raw(&bucket).unwrap();
    assert!(
        recreated.bucket_incarnation_generation > old_deleting.bucket_incarnation_generation,
        "recreated bucket must have a distinct incarnation"
    );
    let stale_root = crate::BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: old_deleting.bucket_incarnation_generation,
    };
    assert_eq!(
        cluster
            .try_finalize_bucket_delete_root(&stale_root)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::StaleIncarnation,
        "old finalizer work must not target the recreated bucket"
    );
    assert_eq!(
        cluster.test_head_bucket_raw(&bucket).unwrap().state,
        crate::BucketState::Active
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "a live stale finalizer claim for a deleted generation must not block recreated bucket finalization"
    );
}

#[test]
fn finalized_bucket_delete_releases_finalizer_claim_for_next_same_pg_bucket() {
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
            bucket_for_pg(topology, 1, "delete-finalize-release-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-release-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);

    cluster
        .test_begin_bucket_delete_if_current(&bucket_a)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket_a).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket_b)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket_b).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "a terminal bucket finalizer must release its exact claim before later same-PG work"
    );
}

#[test]
fn finalized_bucket_delete_waits_for_reclaim_then_finalizes_after_worker_progress() {
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
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"finalize reclaim");
    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();

    let delete_outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        delete_outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "object delete should leave payload reclaim metadata"
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "finalization must wait while reclaim metadata remains"
    );

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "worker progress should clear the reclaim root after the read lease releases"
    );
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1, object_pg]);
}

#[test]
fn bucket_finalizer_does_not_adopt_a_live_worker_payload_reclaim_claim() {
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
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"single-flight reclaim");
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let hook_invocations = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let hook_invocations_for_hook = Arc::clone(&hook_invocations);
    let _hook = cluster.test_install_after_reclaim_claim_acquired_hook(Arc::new(move || {
        let invocation = hook_invocations_for_hook.fetch_add(1, Ordering::SeqCst);
        if invocation == 0 {
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(Duration::from_secs(5))
                .expect("test must release the live reclaim worker");
        }
        Ok(())
    }));

    let worker_cluster = Arc::clone(&cluster);
    let worker_bucket = bucket.clone();
    let worker_key = key.clone();
    let worker = thread::spawn(move || {
        worker_cluster.reclaim_object_payload_if_unleased_with_outcome(
            &worker_bucket,
            &worker_key,
            committed.generation_id,
        )
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker must acquire the durable reclaim claim");

    let nested_outcome = cluster.try_finalize_bucket_delete(&bucket);
    release_tx.send(()).unwrap();
    let worker_outcome = worker.join().unwrap();

    assert_eq!(
        nested_outcome.unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "the finalizer must defer while the queued worker owns the exact reclaim execution"
    );
    assert_eq!(
        hook_invocations.load(Ordering::SeqCst),
        1,
        "the nested finalizer must not reacquire the live worker's durable claim"
    );
    assert_eq!(
        worker_outcome.unwrap(),
        crate::cluster::ObjectPayloadReclaimAttempt::Completed
    );
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1, object_pg]);
}

#[test]
fn finalized_bucket_delete_ignores_volatile_read_lease_without_reclaim_root() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-phantom-lease-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let phantom_key = crate::ObjectKey::try_from("phantom-read".to_string()).unwrap();
    let phantom_lease = cluster
        .acquire_object_payload_lease(&bucket, &phantom_key, crate::GenerationId::MIN)
        .unwrap();
    assert_eq!(cluster.bucket_object_payload_lease_count(&bucket), 1);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "volatile read handles without durable reclaim roots must not wedge bucket finalization"
    );
    drop(phantom_lease);
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn finalized_bucket_delete_preserves_unrelated_same_pg_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (deleting_bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-pending-target-"),
            bucket_for_pg(topology, 1, "delete-pending-survivor-"),
        )
    };
    assert_ne!(deleting_bucket, pending_bucket);
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &deleting_bucket);
    create_test_bucket(&cluster, &pending_bucket);
    cluster
        .test_begin_bucket_delete_if_current(&deleting_bucket)
        .unwrap();

    let pg_id = PgId::new(1);
    let pending_log_index = map.test_next_metadata_command_log_index(pg_id);
    let pending_command = {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &pending_bucket).unwrap();
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, pending_log_index),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &pending_command);

    assert_eq!(
        cluster
            .try_finalize_bucket_delete(&deleting_bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &pending_bucket).is_none(),
        "bucket finalization should drain same-PG pending work rather than dropping it"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(
            &*map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &pending_bucket,
        )
        .unwrap()
        .versioning,
        crate::BucketVersioningState::Enabled
    );
}

#[test]
fn begin_bucket_delete_drain_blocks_bucket_control_slot_before_command_id() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-command-id-race-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_identity = crate::cluster::BucketIdentityGenerations::from_bucket_info(
        &cluster.head_bucket_info(&bucket).unwrap(),
    );

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return false;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let log_index = hook_map.test_next_metadata_command_log_index(pg_id);
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let current =
                crate::PgMetadataStore::head_bucket_record_raw(&*pg, &hook_bucket).unwrap();
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
                MetadataCommandPayload::PutBucketVersioning(
                    PutBucketVersioningCommand::from_bucket(
                        current.with_execution_generation(
                            pg.next_bucket_execution_generation_candidate().unwrap(),
                        ),
                        crate::BucketVersioningState::Enabled,
                    ),
                ),
            );
            assert!(
                !pg.try_insert_bucket_control_pending_metadata_command_slot(
                    primary.node_id().as_u32(),
                    &command,
                    &hook_bucket,
                )
                .unwrap(),
                "live delete drain must prevent the bucket-control command from winning the slot"
            );
            false
        }));

    cluster
        .begin_bucket_delete_if_current(&bucket, bucket_identity)
        .unwrap();
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
            .unwrap()
            .expect("successful DeleteBucket begin should record an attempt outcome");
        assert_eq!(
            outcome.outcome,
            crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
        );
        assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    }

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should attempt a bucket-control install before MarkBucketDeleting id allocation"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked bucket-control command must not leave a pending slot"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.versioning, crate::BucketVersioningState::Disabled);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_mark_deleting_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-partial-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_identity = crate::cluster::BucketIdentityGenerations::from_bucket_info(
        &cluster.head_bucket_info(&bucket).unwrap(),
    );

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual mark deleting command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    cluster
        .begin_bucket_delete_if_current(&bucket, bucket_identity)
        .unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "bucket delete should converge the exact witnessed command and clear its pending slot"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissues_stale_duplicate_mark_deleting_index() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-stale-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-stale-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command)
            .unwrap();
    }
    force_insert_pending_metadata_command_for_test(
        &map,
        pg_id,
        &delete_bucket,
        &stale_delete_command,
    );

    cluster
        .test_begin_bucket_delete_if_current(&delete_bucket)
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &delete_bucket).is_none(),
        "stale duplicate-index mark command should be reissued and cleared"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_transient_abandonment_observation_after_publication_started() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let bucket = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, pg_id.get(), "delete-abandonment-observation-")
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let current = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    primary_pg
        .mark_pending_metadata_command_publication_started(primary.node_id().as_u32(), &command)
        .unwrap();
    drop(primary_pg);

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_bucket = bucket.clone();
    let _hook = cluster.test_install_before_metadata_command_abandoned_log_inspection_hook(
        Arc::new(move |command| {
            if command.bucket_name() == &hook_bucket
                && hook_calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(StoreError::StorageRpc {
                    node_id: 2,
                    operation: "injected abandonment observation",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                    detail: crate::StorageNodeFailureDetail::new(
                        "transient response loss after publication started",
                    ),
                }
                .into());
            }
            Ok(())
        }),
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    assert!(hook_calls.load(Ordering::SeqCst) >= 2);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Deleting
        );
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissue_waits_for_post_primary_replica_apply_window() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-window-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-window-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &delete_bucket, &stale_delete_command);

    let primary_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_primary = Arc::new((Mutex::new(false), Condvar::new()));
    let occupant_map = Arc::clone(&map);
    let occupant_command_for_thread = occupant_command.clone();
    let primary_gate_for_thread = Arc::clone(&primary_gate);
    let release_primary_for_thread = Arc::clone(&release_primary);
    let occupant_thread = std::thread::spawn(move || {
        let pg_lock = occupant_map.runtime_state().metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock();
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = occupant_map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command_for_thread)
                .unwrap();
        }
        {
            let (lock, cv) = &*primary_gate_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        {
            let (lock, cv) = &*release_primary_for_thread;
            let _guard = cv
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                    !*released
                })
                .unwrap();
        }
        let pg = occupant_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(0, &occupant_command_for_thread)
            .unwrap();
    });

    {
        let (lock, cv) = &*primary_gate;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |at_primary| {
                !*at_primary
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "occupant command should pause in the post-primary replica apply window"
        );
    }

    let reissue_cluster = cluster.clone();
    let stale_for_thread = stale_delete_command.clone();
    let reissue_thread = std::thread::spawn(move || {
        reissue_cluster
            .test_reissue_pending_metadata_command(pg_id, &stale_for_thread)
            .unwrap()
    });

    {
        let (lock, cv) = &*release_primary;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    occupant_thread.join().unwrap();
    let replacement = reissue_thread
        .join()
        .unwrap()
        .expect("stale delete command should be reissued after in-flight apply finishes");
    assert_eq!(
        replacement.id().log_index().get(),
        stale_command_id.log_index().get() + 1
    );
    assert_eq!(replacement.payload(), stale_delete_command.payload());

    cluster
        .test_begin_bucket_delete_if_current(&delete_bucket)
        .unwrap();

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_object_pg_drain_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-object-pg-conflict-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let object_pg_id = PgId::new(2);
    let reservation_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(object_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, object_pg_id, &bucket, &command);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::ReserveObjectGeneration(reservation)
                    if reservation.bucket == hook_bucket && reservation.key == hook_key =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual object PG command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary object replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket).is_none(),
        "bucket delete should finish object-PG drain instead of surfacing a retryable conflict"
    );
    for node_id in node_ids {
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*object_pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id.get()]);
}

#[test]
fn begin_bucket_delete_skips_unrelated_all_pg_drain_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-unrelated-bucket-drain-"),
            bucket_for_pg(topology, 2, "delete-pending-bucket-drain-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &pending_bucket);

    let pending_pg_id = PgId::new(2);
    let command_id = cluster
        .next_bucket_metadata_command_id(pending_pg_id)
        .unwrap();
    let pending_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pending_pg_id)
        .unwrap();
    let pending_pg = pending_primary
        .storage_node()
        .get_pg(pending_pg_id.get())
        .unwrap();
    let current =
        crate::PgMetadataStore::head_bucket_record_raw(&*pending_pg, &pending_bucket).unwrap();
    let pending_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(
                pending_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(pending_pg);
    insert_pending_metadata_command_for_test(
        &map,
        pending_pg_id,
        &pending_bucket,
        &pending_command,
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pending_pg_id, &pending_bucket).is_some(),
        "bucket delete should not drain unrelated bucket-PG work found during all-PG scan"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let bucket_pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let pending_pg = node.get_pg(pending_pg_id.get()).unwrap();
        let pending_info =
            crate::PgMetadataStore::head_bucket_raw(&*pending_pg, &pending_bucket).unwrap();
        assert_eq!(pending_info.state, crate::BucketState::Active);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn begin_bucket_delete_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-divergent-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketVersioning(
                            PutBucketVersioningCommand::from_bucket(
                                current.with_execution_generation(
                                    node_pg
                                        .next_bucket_execution_generation_candidate()
                                        .unwrap(),
                                ),
                                crate::BucketVersioningState::Enabled,
                            ),
                        ),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "divergent same-index command log state must fail closed, got {err:?}"
    );
}

#[test]
fn begin_bucket_delete_partial_mark_deleting_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-mark-reopen-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let command = {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let now = crate::clock::current_time_millis();
        let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*primary_pg,
            &bucket,
            "delete-mark-reopen-drain",
            "delete-mark-reopen-owner",
            crate::ClusterEpoch::INITIAL,
            now,
            now.saturating_add(30_000),
        )
        .unwrap();
        crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
            &*primary_pg,
            &crate::BucketDeleteAttemptOutcomeRecord {
                bucket: bucket.clone(),
                drain_id: drain.drain_id.clone(),
                cluster_epoch: drain.cluster_epoch,
                bucket_execution_generation: drain.bucket_execution_generation,
                outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
                phase: crate::BucketDeleteAttemptPhase::FinalVisibilityProven,
                detail: "final visibility proven before partial mark".to_string(),
                post_reservation_next_object_pg_id: None,
                stream_cleanup_next_object_pg_id: None,
                stream_cleanup_next_session_id_marker: None,
                stream_cleanup_aborted_uploads: false,
                final_visibility_next_object_pg_id: None,
                finalizer_next_object_pg_id: None,
                updated_at: now,
            },
        )
        .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        command
    };
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
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "partial MarkBucketDeleting must leave the primary pending slot durable"
    );
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert!(
        pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none(),
        "open-time convergence should clear terminal MarkBucketDeleting pending slot"
    );
    let primary_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
            .unwrap()
            .is_some(),
        "terminal delete drain should remain durable after reopen convergence"
    );
    drop(primary_pg);
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn bucket_update_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "bucket-update-divergent-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                            current.with_execution_generation(
                                node_pg
                                    .next_bucket_execution_generation_candidate()
                                    .unwrap(),
                            ),
                            crate::AclGrants::default(),
                            BucketAclSummary {
                                public_read: false,
                                public_write: false,
                            },
                        )),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "ordinary bucket-PG finish conflicts must fail closed, got {err:?}"
    );
}

#[test]
fn finalized_bucket_delete_fails_closed_on_active_replica() {
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
        bucket_for_pg(topology, 1, "delete-diverged-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let primary_node = map.node(NodeId::new(1)).unwrap().storage_node();
    let primary_pg = primary_node.get_pg(1).unwrap();
    delete_bucket_row_for_divergence_test(&primary_pg, &bucket);
    drop(primary_pg);

    let other_deleted_node = map.node(NodeId::new(2)).unwrap().storage_node();
    let other_deleted_pg = other_deleted_node.get_pg(1).unwrap();
    delete_bucket_row_for_divergence_test(&other_deleted_pg, &bucket);
    drop(other_deleted_pg);

    let divergent_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let divergent_pg = divergent_node.get_pg(1).unwrap();
    delete_bucket_row_for_divergence_test(&divergent_pg, &bucket);
    crate::PgMetadataStore::create_bucket(
        &*divergent_pg,
        &bucket,
        "owner",
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    divergent_pg
        .refresh_metadata_command_state_digest()
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
    drop(divergent_pg);

    let err = cluster
        .try_finalize_bucket_delete_internal(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketNotFinalizedForDelete {
                    state: crate::BucketState::Active
                }
            )
        ),
        "expected active replica to fail finalized delete, got {err:?}"
    );

    let divergent_pg = divergent_node.get_pg(1).unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
}

#[test]
fn bucket_snapshot_fails_closed_while_bucket_pg_is_peering() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "snapshot-peering-")
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(1))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();

    let err = cluster
        .load_bucket_snapshot_internal(&bucket, crate::BucketSnapshotRequest::default())
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn composite_multipart_and_lifecycle_scans_fan_out_to_routed_pg_primaries() {
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
    let upload_bucket = crate::BucketName::try_from("multipart-fanout-bucket".to_string()).unwrap();
    let lifecycle_bucket = bucket_for_pg(topology, 1, "lifecycle-bucket-");
    let aborting_bucket = bucket_for_pg(topology, 1, "aborting-bucket-");
    let key_a = key_for_object_pg(topology, &upload_bucket, 1, "uploads/a-");
    let key_b = key_for_object_pg(topology, &upload_bucket, 2, "uploads/b-");
    let aborting_key = key_for_object_pg(topology, &aborting_bucket, 2, "abort-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &lifecycle_bucket);
    create_test_bucket(&cluster, &aborting_bucket);
    cluster
        .put_bucket_subresource_and_load_info_raw(
            &lifecycle_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();

    let upload_a = upload_id_from_label("routedUploadA");
    let upload_b = upload_id_from_label("routedUploadB");
    let aborting_upload = upload_id_from_label("abortingUpload");
    seed_multipart_upload_record(
        &map,
        NodeId::new(1),
        1,
        &upload_bucket,
        &key_a,
        &upload_a,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &upload_bucket,
        &key_b,
        &upload_b,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &aborting_bucket,
        &aborting_key,
        &aborting_upload,
        crate::UploadState::Aborting,
    );

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node
        .test_list_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap()
        .is_empty());
    let all_uploads = cluster
        .list_all_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap();
    assert_eq!(
        all_uploads
            .iter()
            .map(|upload| (upload.key(), upload.upload_id()))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let mut listed_uploads = cluster
        .list_multipart_uploads_for_bucket(&upload_bucket, None, None, None, None, 100)
        .unwrap()
        .uploads;
    listed_uploads.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        listed_uploads
            .iter()
            .map(|upload| (&upload.key, &upload.upload_id))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let sweep = cluster.list_lifecycle_sweep_buckets().unwrap();
    assert_eq!(
        sweep
            .lifecycle_buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&lifecycle_bucket]
    );
    assert_eq!(sweep.aborting_buckets, vec![aborting_bucket.clone()]);

    let roots = cluster.list_lifecycle_sweep_roots(0).unwrap();
    assert_eq!(
        roots
            .iter()
            .map(|root| (&root.bucket, root.source))
            .collect::<Vec<_>>(),
        vec![
            (
                &lifecycle_bucket,
                crate::LifecycleSweepRootSource::LifecycleConfig
            ),
            (
                &aborting_bucket,
                crate::LifecycleSweepRootSource::AbortingMultipartUpload,
            ),
        ]
    );
}

#[test]
fn bucket_control_plane_pending_install_waits_behind_durable_delete_drain() {
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
        bucket_for_pg(topology, 1, "control-plane-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(1);
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let versioning_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let versioning_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*bucket_pg, &bucket)
            .unwrap()
            .with_execution_generation(
                bucket_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            );
        MetadataCommandEnvelope::new(
            versioning_command_id,
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current,
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &versioning_command)
            .unwrap(),
        "versioning command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked bucket control-plane command must not leave a pending slot"
    );

    let lifecycle_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let lifecycle_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            lifecycle_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::PutLifecycle("<LifecycleConfiguration/>".to_string()),
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &lifecycle_command)
            .unwrap(),
        "lifecycle command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked lifecycle command must not leave a pending slot"
    );

    let cors_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let cors_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            cors_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::PutCors("<CORSConfiguration/>".to_string()),
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &cors_command)
            .unwrap(),
        "CORS command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked CORS command must not leave a pending slot"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
    let versioned = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(versioned.versioning, crate::BucketVersioningState::Enabled);
    let lifecycle = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(lifecycle.bucket_lifecycle_present);
    cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
    let cors = crate::PgMetadataStore::get_bucket_subresource(
        &*bucket_pg,
        &bucket,
        crate::BucketSubresourceKind::Cors,
    )
    .unwrap()
    .expect("CORS subresource should be installed after the drain clears");
    assert_eq!(cors.body, "<CORSConfiguration/>");
}

#[test]
fn begin_bucket_delete_waits_for_durable_reservation_and_post_drains_visible_write() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-durable-reservation-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let payload = b"visible";
    let generation_reservation_id = crate::SessionId::try_from("72".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &generation_reservation_id)
        .unwrap();
    let segment_okh = [71; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = crate::CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id,
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag_crc64: checksum::crc64::checksum(payload),
        ec: written.ec,
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: checksum::crc64::checksum(payload),
        segment_okh,
        segment_vid: generation_id,
        data_pg_id: written.data_pg_id,
        bucket_write_reservation: bucket_write_proof,
    };

    let delete_started = Arc::new((Mutex::new(false), Condvar::new()));
    let delete_waiting = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_bucket = bucket.clone();
    let delete_waiting_for_hook = Arc::clone(&delete_waiting);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                let (lock, cv) = &*delete_waiting_for_hook;
                *lock.lock().unwrap() = true;
                cv.notify_all();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let delete_cluster = Arc::clone(&cluster);
    let delete_bucket = bucket.clone();
    let delete_started_for_thread = Arc::clone(&delete_started);
    let delete_thread = std::thread::spawn(move || {
        {
            let (lock, cv) = &*delete_started_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        delete_cluster.test_begin_bucket_delete_if_current(&delete_bucket)
    });

    {
        let (lock, cv) = &*delete_started;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |started| {
                !*started
            })
            .unwrap()
            .0;
        assert!(*guard, "delete thread should start");
    }
    {
        let (lock, cv) = &*delete_waiting;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |waiting| {
                !*waiting
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "DeleteBucket should wait for the durable writer reservation before emptiness"
        );
    }

    {
        let pg_id = PgId::new(2);
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .register_payload_shard_acks(written.data_pg_id, &shard_batch)
            .unwrap();
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        let command = cluster
            .prepare_commit_direct_put_object_command(
                pg_id,
                &pg,
                &commit_req,
                crate::VersionId::Null,
                commit_req.bucket_write_reservation.clone(),
            )
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    }

    let err = delete_thread.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "post-reservation drain/check should see the committed object, got {err:?}"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "failed DeleteBucket should clear its temporary durable drain"
    );
}

#[test]
fn begin_bucket_delete_records_reservation_wait_blocker_and_adopts_after_release() {
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
        bucket_for_pg(topology, 1, "delete-orphan-reservation-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "test-orphaned-write",
            Some("orphaned-key"),
        )
        .unwrap();

    let started = std::time::Instant::now();
    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "DeleteBucket should not wait indefinitely for an orphaned durable reservation"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
        ),
        "blocked durable reservation should make DeleteBucket retryable, got {err:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_some(),
        "retryable reservation-wait failure should preserve the durable drain"
    );
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .len(),
        1,
        "DeleteBucket must not silently drop another operation's durable reservation"
    );
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("reservation-wait blocker should be recorded durably");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::Retryable
    );
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::ReservationWait
    );
    assert!(
        outcome.detail.contains("reservation wait timeout"),
        "unexpected reservation-wait detail: {}",
        outcome.detail
    );
    assert!(
        outcome.detail.contains("reservations=1")
            && outcome
                .detail
                .contains("first_operation_kind=test-orphaned-write")
            && outcome
                .detail
                .contains("first_target_context=Some(\"orphaned-key\")"),
        "reservation-wait detail should identify the blocking reservation: {}",
        outcome.detail
    );
    drop(bucket_pg);

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("adopted retry should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
}

#[test]
fn begin_bucket_delete_preserves_drain_when_object_pg_enters_peering() {
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
        bucket_for_pg(topology, 1, "delete-peering-preserve-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let peering_pg_id = PgId::new(2);
    let _exact_drain_hook_guard = cluster.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |_has_progress, _next_object_pg_id| {
            Err(StoreError::PgNotActive {
                pg_id: peering_pg_id.get(),
                cluster_epoch: ClusterEpoch::INITIAL,
                state: crate::PgState::Peering,
            })
        }),
    );

    let error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::BucketWriteDrainError::Store(StoreError::PgNotActive {
            pg_id: 2,
            state: crate::PgState::Peering,
            ..
        })
    ));

    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_some(),
        "a transient Peering failure must preserve the durable delete drain for adoption"
    );
    drop(bucket_pg);
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(root)) if root.bucket == bucket
    ));
}

#[test]
fn begin_bucket_delete_reaps_expired_durable_write_reservation() {
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
        bucket_for_pg(topology, 1, "delete-expired-reservation-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::acquire_durable_bucket_write_reservation(
        &*bucket_pg,
        crate::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "expired-reservation",
            owner_token: "expired-owner",
            cluster_epoch: ClusterEpoch::INITIAL,
            operation_kind: "test-expired-write",
            created_at: 1,
            lease_deadline: 2,
            target_context: Some("expired-key"),
        },
    )
    .unwrap();
    drop(bucket_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("expired durable reservation should be reaped during DeleteBucket");
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "expired durable write reservation should be removed"
    );
    assert!(matches!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    ));
}

#[test]
fn begin_bucket_delete_adopts_live_same_generation_delete_drain() {
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
        bucket_for_pg(topology, 1, "delete-active-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
        &*bucket_pg,
        &bucket,
        "held-delete-drain",
        "other-delete-owner",
        crate::ClusterEpoch::INITIAL,
        crate::clock::current_time_millis(),
        crate::clock::current_time_millis().saturating_add(30_000),
    )
    .unwrap();
    drop(bucket_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        drain.lease_deadline > crate::clock::current_time_millis(),
        "test must seed a live delete drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some(drain.drain_id.as_str()),
        "DeleteBucket should adopt the live same-generation delete drain"
    );
    assert!(matches!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    ));
}

#[test]
fn begin_bucket_delete_adopts_active_delete_drain_after_reopen() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-reopen-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let clock = crate::clock::test_time_override_guard(1_000);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let original_deadline = drain.record.lease_deadline;
    clock.set(12_000);
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    let advanced_during_proof = Arc::new(AtomicBool::new(false));
    let advanced_during_proof_for_hook = Arc::clone(&advanced_during_proof);
    let clock_for_hook = clock.control();
    let _progress_hook_guard = reopened_cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                clock_for_hook.set(17_000);
                advanced_during_proof_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));
    reopened_cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("DeleteBucket should adopt and complete an active pre-mark drain after reopen");
    assert!(
        advanced_during_proof.load(Ordering::SeqCst),
        "test must advance logical time during the adopted DeleteBucket proof"
    );

    let bucket_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some(drain.record.drain_id.as_str()),
        "DeleteBucket adoption should preserve the original drain identity"
    );
    let renewed_deadline = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .map(|record| record.lease_deadline)
        .expect("adopted delete drain should remain leased until finalization");
    assert!(
        renewed_deadline > original_deadline,
        "adopted delete drain should renew before long proof phases; original={original_deadline} renewed={renewed_deadline}"
    );
    assert!(
        renewed_deadline > crate::clock::current_time_millis(),
        "adopted delete drain should still be live after the proof reaches MarkBucketDeleting"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting,
        "adopted pre-mark delete drain should reach terminal deleting state"
    );
}

#[test]
fn durable_scan_skips_live_and_queues_expired_delete_begin_drain_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-scan-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_info = cluster.test_head_bucket_raw(&bucket).unwrap();
    let live_drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    let expected_root = crate::BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: bucket_info.bucket_execution_generation,
        bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
    };

    let scan =
        reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&HashSet::new());
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 0);
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);
    reopened_cluster
        .clear_durable_bucket_delete_drain(&live_drain)
        .unwrap();

    let expired_drain = {
        let bucket_pg = reopened
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*bucket_pg,
            &bucket,
            "expired-delete-begin-scan-drain",
            "expired-delete-begin-scan-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            now.saturating_sub(1),
        )
        .unwrap()
    };

    let scan =
        reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&HashSet::new());
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 1);
    assert_eq!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(
            expected_root.clone()
        ))
    );

    let excluded = HashSet::from([expected_root]);
    let scan = reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&excluded);
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 0);
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);

    reopened_cluster
        .clear_durable_bucket_delete_drain(&crate::cluster::DurableBucketWriteDrain {
            pg_id: 1,
            record: expired_drain,
        })
        .unwrap();
}

#[test]
fn durable_scan_paginates_past_excluded_delete_begin_drains() {
    const PAGE_LIMIT: usize = 16;

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let buckets = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let mut buckets = Vec::new();
        for index in 0..=PAGE_LIMIT {
            buckets.push(bucket_for_pg(
                topology,
                1,
                &format!("delete-begin-page-{index:02}-"),
            ));
        }
        buckets.sort();
        buckets
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let mut drains = Vec::new();
    let mut roots = Vec::new();
    let now = crate::clock::current_time_millis();
    for (index, bucket) in buckets.iter().enumerate() {
        create_test_bucket(&cluster, bucket);
        let bucket_info = cluster.test_head_bucket_raw(bucket).unwrap();
        let bucket_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*bucket_pg,
            bucket,
            &format!("expired-delete-begin-page-drain-{index}"),
            "expired-delete-begin-page-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            now.saturating_sub(1),
        )
        .unwrap();
        drains.push(drain);
        roots.push(crate::BucketDeleteBeginRoot {
            bucket: bucket.clone(),
            bucket_execution_generation: bucket_info.bucket_execution_generation,
            bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
        });
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    let excluded = roots
        .iter()
        .take(PAGE_LIMIT)
        .cloned()
        .collect::<HashSet<_>>();

    let scan = reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&excluded);
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 1);
    assert_eq!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(
            roots[PAGE_LIMIT].clone()
        ))
    );
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);

    for drain in drains {
        reopened_cluster
            .clear_durable_bucket_delete_drain(&crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: drain,
            })
            .unwrap();
    }
}

#[test]
fn begin_bucket_delete_records_final_visibility_phase_before_mark_command() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-final-visibility-phase-"),
            topology.pg_count(),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            hook_ran_for_closure.store(true, Ordering::SeqCst);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &hook_bucket)
                .unwrap()
                .expect("DeleteBucket should record final visibility progress before mark command");
            assert_eq!(
                outcome.outcome,
                crate::BucketDeleteAttemptOutcomeKind::Retryable
            );
            assert_eq!(
                outcome.phase,
                crate::BucketDeleteAttemptPhase::FinalVisibilityProven
            );
            assert_eq!(
                outcome.post_reservation_next_object_pg_id,
                Some(pg_count),
                "final visibility progress should restore the terminal post-reservation frontier after stream cleanup"
            );
            false
        }));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should observe the attempt before MarkBucketDeleting id allocation"
    );

    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
        .unwrap()
        .expect("successful DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
}

#[test]
fn begin_bucket_delete_deadline_after_final_visibility_does_not_install_mark_command() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-final-visibility-deadline-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            hook_calls_for_hook.fetch_add(1, Ordering::SeqCst);
            true
        }));

    let error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                context: "bucket delete mark-deleting admission budget exhausted"
            })
        ),
        "deadline after final visibility should return retryable contention, got {error:?}"
    );
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "test must expire the shared budget at the mark-command admission boundary"
    );

    let pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "an expired delete must not install MarkBucketDeleting"
    );
    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Active);
    let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
        .unwrap()
        .expect("deadline exhaustion should preserve the durable delete drain");
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
        .unwrap()
        .expect("deadline exhaustion should preserve resumable progress");
    assert_eq!(outcome.drain_id, drain.drain_id);
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityProven
    );
    drop(pg);
    drop(hook_guard);

    let visibility_reran = Arc::new(AtomicBool::new(false));
    let visibility_reran_for_hook = Arc::clone(&visibility_reran);
    let _visibility_hook =
        cluster.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_reran_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }));
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("retry should resume the proven visibility frontier and mark the bucket deleting");
    assert!(
        !visibility_reran.load(Ordering::SeqCst),
        "retry must adopt the final-visibility proof rather than repeating the scan"
    );
    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
}

#[test]
fn begin_bucket_delete_response_loss_after_mark_install_preserves_drain_and_exact_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-mark-install-response-loss-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let installed_command = Arc::new(Mutex::new(None));
    let installed_command_for_hook = Arc::clone(&installed_command);
    let hook_bucket = bucket.clone();
    let hook_guard = cluster.test_install_after_bucket_delete_pending_install_response_loss_hook(
        Arc::new(move |command| {
            let MetadataCommandPayload::MarkBucketDeleting(mark) = command.payload() else {
                return false;
            };
            if mark.bucket_name() != &hook_bucket {
                return false;
            }
            *installed_command_for_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(command.clone());
            true
        }),
    );

    let error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    let installed_command = installed_command
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .expect("test must lose the response after the exact mark command is installed");
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(
                StoreError::MetadataCommandOutcomeUnconfirmed {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index,
                }
            ) if log_index == installed_command.id().log_index().get()
        ),
        "post-install response loss should remain typed uncertainty, got {error:?}"
    );

    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("response loss must retain the committed pending mark command");
    assert_eq!(pending.id(), installed_command.id());
    assert_eq!(pending.command_bytes(), installed_command.command_bytes());
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .expect("install uncertainty must preserve the durable delete fence");
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("install uncertainty must preserve resumable delete progress");
    assert_eq!(outcome.drain_id, drain.drain_id);
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    drop(bucket_pg);
    drop(hook_guard);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("retry should converge the exact retained mark command");
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "converged retry must clear the exact pending mark command"
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Deleting
        );
    }
}

#[test]
fn concurrent_bucket_delete_adopters_preserve_drain_on_mark_validation_failure() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-mark-validation-failure-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let installed_command = Arc::new(Mutex::new(None));
    let installed_command_for_hook = Arc::clone(&installed_command);
    let hook_bucket = bucket.clone();
    let install_hook = cluster.test_install_after_bucket_delete_pending_install_response_loss_hook(
        Arc::new(move |command| {
            let MetadataCommandPayload::MarkBucketDeleting(mark) = command.payload() else {
                return false;
            };
            if mark.bucket_name() != &hook_bucket {
                return false;
            }
            *installed_command_for_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(command.clone());
            true
        }),
    );
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect_err("the setup must lose the response after mark installation");
    drop(install_hook);
    let installed_command = installed_command
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .expect("the setup must retain the exact installed mark command");

    let validation_calls = Arc::new(AtomicUsize::new(0));
    let validation_calls_for_hook = Arc::clone(&validation_calls);
    let expected_command = installed_command.clone();
    let validation_hook = cluster.test_install_before_bucket_delete_adopted_mark_validation_hook(
        Arc::new(move |command| {
            if command == &expected_command {
                validation_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id: 1,
                    pg_id: command.id().pg_id().get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                });
            }
            Ok(())
        }),
    );
    let start = Arc::new(std::sync::Barrier::new(3));
    let mut adopters = Vec::new();
    for _ in 0..2 {
        let adopter_cluster = Arc::clone(&cluster);
        let adopter_bucket = bucket.clone();
        let adopter_start = Arc::clone(&start);
        adopters.push(std::thread::spawn(move || {
            adopter_start.wait();
            adopter_cluster.test_begin_bucket_delete_if_current(&adopter_bucket)
        }));
    }
    start.wait();
    for adopter in adopters {
        let error = adopter
            .join()
            .expect("delete-drain adopter must not panic")
            .expect_err("injected mark validation failure must propagate");
        assert!(
            matches!(
                error,
                crate::BucketWriteDrainError::Store(
                    StoreError::MetadataCommandLogConflict {
                        pg_id: 1,
                        cluster_epoch: ClusterEpoch::INITIAL,
                        log_index,
                        ..
                    }
                ) if log_index == installed_command.id().log_index().get()
            ),
            "adopted mark validation failure regressed to {error:?}"
        );
    }
    assert!(validation_calls.load(Ordering::SeqCst) >= 2);
    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("validation failures must retain the exact pending mark command");
    assert_eq!(pending.id(), installed_command.id());
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(1).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
            .unwrap()
            .is_some(),
        "concurrent adopters must not clear the command-protected drain"
    );
    drop(primary_pg);
    drop(validation_hook);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("retry should converge the retained mark command");
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
}

#[test]
fn begin_bucket_delete_marker_only_convergence_expiry_preserves_drain_and_exact_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-mark-marker-only-expiry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let installed_command = Arc::new(Mutex::new(None));
    let installed_command_for_hook = Arc::clone(&installed_command);
    let hook_bucket = bucket.clone();
    let install_hook = cluster.test_install_after_bucket_delete_pending_install_response_loss_hook(
        Arc::new(move |command| {
            let MetadataCommandPayload::MarkBucketDeleting(mark) = command.payload() else {
                return false;
            };
            if mark.bucket_name() != &hook_bucket {
                return false;
            }
            *installed_command_for_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(command.clone());
            true
        }),
    );

    let install_error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(matches!(
        install_error,
        crate::BucketWriteDrainError::Store(StoreError::MetadataCommandOutcomeUnconfirmed { .. })
    ));
    drop(install_hook);
    let installed_command = installed_command
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .expect("test must retain the exact installed mark command");
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(1).unwrap();
    primary_pg
        .mark_pending_metadata_command_publication_started(
            primary.node_id().as_u32(),
            &installed_command,
        )
        .unwrap();
    drop(primary_pg);

    let observation_calls = Arc::new(AtomicUsize::new(0));
    let observation_calls_for_hook = Arc::clone(&observation_calls);
    let expected_command = installed_command.clone();
    let observation_hook = cluster
        .test_install_before_metadata_command_abandoned_log_inspection_hook(Arc::new(
            move |command| {
                if command == &expected_command {
                    observation_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    return Err(StoreError::StorageRpc {
                        node_id: 2,
                        operation: "injected marker-only abandonment observation",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                        detail: crate::StorageNodeFailureDetail::new(
                            "persistent response loss after publication started",
                        ),
                    }
                    .into());
                }
                Ok(())
            },
        ));

    let error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect_err("marker-only confirmation expiry must remain typed convergence");
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(
                StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index,
                }
            ) if log_index == installed_command.id().log_index().get()
        ),
        "marker-only convergence regressed to {error:?}"
    );
    assert!(observation_calls.load(Ordering::SeqCst) > 0);
    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("marker-only convergence must retain the exact pending mark command");
    assert_eq!(pending.id(), installed_command.id());
    assert_eq!(pending.command_bytes(), installed_command.command_bytes());
    let primary_pg = primary.storage_node().get_pg(1).unwrap();
    let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
        .unwrap()
        .expect("irrevocable mark convergence must preserve the delete fence");
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*primary_pg, &bucket)
        .unwrap()
        .expect("irrevocable mark convergence must preserve resumable progress");
    assert_eq!(outcome.drain_id, drain.drain_id);
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    drop(primary_pg);
    drop(observation_hook);

    let recovery_outcome = cluster
        .drain_pending_metadata_command_with_authorized_recovery_route(
            PgId::new(1),
            &installed_command,
            &cluster,
        )
        .expect("authorized recovery should converge the marker-only pending mark command");
    assert!(recovery_outcome.is_logically_applied());
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "converged retry must clear the exact pending mark command"
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Deleting
        );
    }
}

#[test]
fn begin_bucket_delete_adopts_final_visibility_phase_without_repeating_post_reservation_scan() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-final-visibility-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::FinalVisibilityCheck,
            detail: "resume from final visibility".to_string(),
            post_reservation_next_object_pg_id: Some(0),
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Err(StoreError::Io {
                    context: "unexpected post-reservation scan during final-visibility adoption",
                    source: std::io::Error::other(format!(
                        "unexpected next_object_pg_id={next_object_pg_id}"
                    )),
                })
            },
        ));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        !post_reservation_scan_ran.load(Ordering::SeqCst),
        "final-visibility adoption should not repeat the post-reservation scan"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_resumes_final_visibility_from_durable_pg_cursor() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..16).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-final-visibility-cursor-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let stage = Arc::new(AtomicUsize::new(0));
    let stage_for_hook = Arc::clone(&stage);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_final_visibility_progress_hook(Arc::new(
            move |next_object_pg_id| {
                match stage_for_hook.load(Ordering::SeqCst) {
                    0 if next_object_pg_id == 8 => {
                        stage_for_hook.store(1, Ordering::SeqCst);
                        return Err(StoreError::RouteMapExpired {
                            cluster_epoch: ClusterEpoch::INITIAL,
                            valid_until_ms: 0,
                            now_ms: 1,
                        });
                    }
                    1 => {
                        assert!(
                            next_object_pg_id > 8,
                            "final visibility retry revisited a completed PG frontier: {next_object_pg_id}"
                        );
                        stage_for_hook.store(2, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

    let first_error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            first_error,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first visibility pass should stop after persisting its cursor, got {first_error:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("failed visibility pass should retain durable progress");
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityCheck
    );
    assert_eq!(outcome.final_visibility_next_object_pg_id, Some(8));
    drop(bucket_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        stage.load(Ordering::SeqCst),
        2,
        "retry should continue strictly after the persisted visibility frontier"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn begin_bucket_delete_adopts_final_visibility_proven_without_repeating_visibility_check() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-final-visibility-proven-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::FinalVisibilityProven,
            detail: "resume after final visibility proof".to_string(),
            post_reservation_next_object_pg_id: Some(0),
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let visibility_check_ran = Arc::new(AtomicBool::new(false));
    let visibility_check_ran_for_hook = Arc::clone(&visibility_check_ran);
    let _visibility_hook_guard =
        cluster.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_check_ran_for_hook.store(true, Ordering::SeqCst);
            Err(StoreError::Io {
                context: "unexpected final visibility scan during proven adoption",
                source: std::io::Error::other("final visibility should already be proven"),
            })
        }));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        !visibility_check_ran.load(Ordering::SeqCst),
        "final-visibility-proven adoption should not repeat the visibility scan"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_renews_drain_after_final_visibility_proof_before_retryable_exit() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-final-visibility-renew-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let clock = crate::clock::test_time_override_guard(1_000);
    let advanced_during_visibility = Arc::new(AtomicBool::new(false));
    let advanced_during_visibility_for_hook = Arc::clone(&advanced_during_visibility);
    let clock_for_visibility_hook = clock.control();
    let visibility_hook_guard =
        cluster.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            clock_for_visibility_hook.set(12_000);
            advanced_during_visibility_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }));
    let proven_hook_guard =
        cluster.test_install_after_bucket_delete_final_visibility_proven_hook(Arc::new(|| {
            Err(StoreError::MetadataCommandContention {
                context: "injected retryable error after final visibility proof",
            })
        }));

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                context: "injected retryable error after final visibility proof"
            })
        ),
        "expected injected retryable error, got {err:?}"
    );
    assert!(
        advanced_during_visibility.load(Ordering::SeqCst),
        "test must advance logical time during final visibility"
    );
    drop(proven_hook_guard);
    drop(visibility_hook_guard);

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let preserved_drain = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .expect("retryable final visibility proof should preserve the delete drain");
    let preserved_deadline = preserved_drain.lease_deadline;
    assert!(
        preserved_deadline > 20_000,
        "final visibility proof should renew the drain for a later retry; deadline={preserved_deadline}"
    );
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("retryable final visibility proof should record an attempt outcome");
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityProven
    );
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
    drop(bucket_pg);

    clock.set(20_000);
    let visibility_reran = Arc::new(AtomicBool::new(false));
    let visibility_reran_for_hook = Arc::clone(&visibility_reran);
    let _visibility_retry_hook_guard = cluster
        .test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_reran_for_hook.store(true, Ordering::SeqCst);
            Err(StoreError::Io {
                context: "unexpected final visibility rerun after preserved proof",
                source: std::io::Error::other("final visibility should already be proven"),
            })
        }));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("retry should adopt the preserved final-visibility proof");
    assert!(
        !visibility_reran.load(Ordering::SeqCst),
        "retry should not rerun final visibility while the preserved drain is live"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let final_outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        final_outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(final_outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_preserves_drain_after_post_proof_transport_interruption() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-transport-interruption-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _hook_guard =
        cluster.test_install_after_bucket_delete_final_visibility_proven_hook(Arc::new(|| {
            Err(StoreError::Io {
                context: "injected storage RPC response loss",
                source: std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected response loss",
                ),
            })
        }));

    let error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(StoreError::Io { ref source, .. })
                if source.kind() == std::io::ErrorKind::ConnectionReset
        ),
        "expected the injected transport interruption, got {error:?}"
    );

    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
        .unwrap()
        .expect("an ambiguous transport failure must preserve the durable delete drain");
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
        .unwrap()
        .expect("an ambiguous transport failure must preserve resumable progress");
    assert_eq!(outcome.drain_id, drain.drain_id);
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityProven
    );
}

#[test]
fn adopted_bucket_delete_does_not_touch_drain_through_expired_runtime_map() {
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
        bucket_for_pg(topology, 1, "delete-expired-adoption-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_info = cluster.test_head_bucket_raw(&bucket).unwrap();
    let original = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain.record,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire a delete drain")
        }
    };
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(0).unwrap());

    let root = crate::BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: bucket_info.bucket_execution_generation,
        bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
    };
    let error = cluster.continue_adopted_bucket_delete(&root).unwrap_err();
    assert!(
        matches!(
            error,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "expired background route must fail before drain adoption, got {error:?}"
    );

    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("expired background route must leave the drain intact"),
        original,
        "expired background recovery must not heartbeat, replace, or clear the drain"
    );
}

#[test]
fn begin_bucket_delete_route_expiry_after_final_visibility_preserves_for_fenced_retry() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-route-expiry-after-proof-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = cluster.test_head_bucket_raw(&bucket).unwrap();

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let route_expiry_hook_guard = cluster
        .test_install_after_bucket_delete_final_visibility_proven_hook(Arc::new(move || {
            hook_ran_for_hook.store(true, Ordering::SeqCst);
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 0,
                now_ms: 1,
            })
        }));

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "expected injected route expiry after final visibility proof, got {err:?}"
    );
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "route-expiry hook should run after final visibility is proven"
    );

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let preserved_drain = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .expect("route expiry after final visibility should preserve the delete drain");
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("route expiry after final visibility should record attempt progress");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::Retryable
    );
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityProven
    );
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
    drop(bucket_pg);

    let resume_root = match cluster.try_take_reclaim_work() {
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(root)) => root,
        other => {
            panic!("route-expired DeleteBucket begin should queue fenced resume, got {other:?}")
        }
    };
    assert_eq!(resume_root.bucket, bucket);
    assert_eq!(
        resume_root.bucket_execution_generation,
        initial_bucket.bucket_execution_generation
    );
    assert_eq!(
        resume_root.bucket_incarnation_generation,
        initial_bucket.bucket_incarnation_generation
    );

    drop(route_expiry_hook_guard);
    cluster
        .begin_bucket_delete_if_current(
            &resume_root.bucket,
            crate::cluster::BucketIdentityGenerations {
                bucket_execution_generation: resume_root.bucket_execution_generation,
                bucket_incarnation_generation: resume_root.bucket_incarnation_generation,
            },
        )
        .expect("fenced retry should adopt the preserved route-expired attempt");

    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let final_outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful fenced retry should record terminal outcome");
    assert_eq!(
        final_outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(
        final_outcome.phase,
        crate::BucketDeleteAttemptPhase::MarkDeleting
    );
    assert_eq!(final_outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_post_publication_route_loss_returns_success_and_retains_recovery_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
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
        bucket_for_pg(topology, 1, "delete-committed-response-loss-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_identity = crate::cluster::BucketIdentityGenerations::from_bucket_info(
        &cluster.head_bucket_info(&bucket).unwrap(),
    );

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_bucket = bucket.clone();
    let _hook_guard = cluster.test_install_after_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(2)
                        && hook_calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0 =>
                {
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .begin_bucket_delete_if_current(&bucket, bucket_identity)
        .expect("post-publication route loss must not replace the committed outcome");
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "first attempt should inject exactly once after publishing MarkBucketDeleting"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "post-publication recovery handoff must retain the exact pending command"
    );

    cluster
        .begin_bucket_delete_if_current(&bucket, bucket_identity)
        .expect("retry should observe the committed bucket delete");
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        2,
        "committed retry must replay the exact retained MarkBucketDeleting command"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "committed retry must clear the terminal MarkBucketDeleting pending command"
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn begin_bucket_delete_treats_retryable_error_after_concurrent_mark_deleting_as_success() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-concurrent-mark-success-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let concurrent_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_bucket = bucket.clone();
    let _proven_hook_guard = cluster.test_install_after_bucket_delete_final_visibility_proven_hook(
        Arc::new(move || {
            if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            concurrent_cluster
                .test_begin_bucket_delete_if_current(&hook_bucket)
                .expect("concurrent begin should mark the same bucket incarnation deleting");
            Err(StoreError::MetadataCommandContention {
                context: "injected retryable error after concurrent mark deleting",
            })
        }),
    );

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("retryable error after same-incarnation MarkBucketDeleting should be success");
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should simulate a concurrent MarkBucketDeleting before retryable exit"
    );
    assert_eq!(
        cluster.try_take_reclaim_work(),
        None,
        "stale retryable exit should not enqueue begin work after observing Deleting"
    );

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful concurrent mark should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
}

#[test]
fn begin_bucket_delete_does_not_suppress_route_error_after_concurrent_mark_deleting() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-route-error-preserved-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let concurrent_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_bucket = bucket.clone();
    let _proven_hook_guard = cluster.test_install_after_bucket_delete_final_visibility_proven_hook(
        Arc::new(move || {
            if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            concurrent_cluster
                .test_begin_bucket_delete_if_current(&hook_bucket)
                .expect("concurrent begin should mark the same bucket incarnation deleting");
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            })
        }),
    );

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "route errors from the pinned map must not be suppressed by a same-client deleting recheck; got {err:?}"
    );
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should simulate a concurrent MarkBucketDeleting before route error"
    );

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
}

#[test]
fn begin_bucket_delete_adopts_stream_cleanup_phase_and_revalidates_after_reservations() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-stream-cleanup-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::StreamCleanup,
            detail: "resume from stream cleanup".to_string(),
            post_reservation_next_object_pg_id: None,
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "stream-cleanup adoption must still revalidate the post-reservation object-PG drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_resumes_stream_cleanup_from_durable_pg_cursor() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..16).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-stream-cleanup-cursor-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let stage = Arc::new(AtomicUsize::new(0));
    let stage_for_hook = Arc::clone(&stage);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_stream_cleanup_progress_hook(Arc::new(
            move |phase, next_object_pg_id| {
                if phase != crate::TestBucketDeleteAttemptPhase::StreamCleanup {
                    return Ok(());
                }
                match stage_for_hook.load(Ordering::SeqCst) {
                    0 if next_object_pg_id == 8 => {
                        stage_for_hook.store(1, Ordering::SeqCst);
                        return Err(StoreError::RouteMapExpired {
                            cluster_epoch: ClusterEpoch::INITIAL,
                            valid_until_ms: 0,
                            now_ms: 1,
                        });
                    }
                    1 => {
                        assert!(
                            next_object_pg_id > 8,
                            "stream cleanup retry revisited a completed PG frontier: {next_object_pg_id}"
                        );
                        stage_for_hook.store(2, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

    let first_error = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            first_error,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first stream cleanup pass should stop after persisting its cursor, got {first_error:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("failed stream cleanup should retain durable progress");
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::StreamCleanup
    );
    assert_eq!(outcome.stream_cleanup_next_object_pg_id, Some(8));
    drop(bucket_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert_eq!(
        stage.load(Ordering::SeqCst),
        2,
        "retry should continue strictly after the persisted stream-cleanup frontier"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn begin_bucket_delete_records_reservation_wait_phase_after_stream_cleanup() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-reservation-wait-record-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let _hook_guard =
        cluster.test_install_after_bucket_delete_reservation_wait_ready_hook(Arc::new(move || {
            hook_ran_for_hook.store(true, Ordering::SeqCst);
            let bucket_pg = hook_map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let outcome =
                crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &hook_bucket)
                    .unwrap()
                    .expect("DeleteBucket should record reservation-wait progress");
            assert_eq!(
                outcome.outcome,
                crate::BucketDeleteAttemptOutcomeKind::Retryable
            );
            assert_eq!(
                outcome.phase,
                crate::BucketDeleteAttemptPhase::ReservationWait
            );
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 0,
                now_ms: 1,
            })
        }));

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "hook should fail after recording reservation-wait cursor, got {err:?}"
    );
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should observe the durable reservation-wait cursor"
    );
}

#[test]
fn active_delete_attempt_authorization_snapshot_requires_drained_bucket_writes() {
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
        bucket_for_pg(topology, 1, "delete-active-auth-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(&bucket, "held-policy-write", None)
        .unwrap();
    cluster
        .test_seed_bucket_delete_attempt_outcome(
            &bucket,
            crate::TestBucketDeleteAttemptOutcomeKind::Retryable,
            crate::TestBucketDeleteAttemptPhase::ReservationWait,
            "seeded reservation-wait attempt".to_string(),
            None,
        )
        .unwrap();

    let snapshot = cluster
        .load_active_bucket_delete_attempt_authorization_snapshot(
            &bucket,
            crate::BucketSnapshotRequest::default(),
        )
        .unwrap();
    assert!(
        snapshot.is_none(),
        "active DeleteBucket raw auth must wait until pre-drain bucket writes are gone"
    );

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    let snapshot = cluster
        .load_active_bucket_delete_attempt_authorization_snapshot(
            &bucket,
            crate::BucketSnapshotRequest::default(),
        )
        .unwrap()
        .expect("drained active DeleteBucket attempt should expose a stable auth snapshot");
    assert_eq!(snapshot.bucket.state, crate::BucketState::Active);
}

#[test]
fn begin_bucket_delete_adopts_reservation_wait_phase_without_repeating_initial_scan() {
    let _serial = lock_bucket_scoped_hook_test();
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
        bucket_for_pg(topology, 1, "delete-reservation-wait-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::ReservationWait,
            detail: "resume from reservation wait".to_string(),
            post_reservation_next_object_pg_id: None,
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let initial_scan_ran = Arc::new(AtomicBool::new(false));
    let initial_scan_ran_for_hook = Arc::clone(&initial_scan_ran);
    let _exact_drain_hook_guard = cluster.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            if !has_progress {
                initial_scan_ran_for_hook.store(true, Ordering::SeqCst);
                return Err(StoreError::Io {
                    context:
                        "unexpected initial exact-bucket drain during reservation-wait adoption",
                    source: std::io::Error::other(format!("next_object_pg_id={next_object_pg_id}")),
                });
            }
            Ok(())
        }),
    );
    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        !initial_scan_ran.load(Ordering::SeqCst),
        "reservation-wait adoption must skip the initial pre-cleanup exact-bucket drain"
    );
    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "reservation-wait adoption must still validate object PGs after reservations drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_adopts_post_reservation_phase_after_budget_exhaustion() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..32).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-post-reservation-budget-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let stage = Arc::new(AtomicUsize::new(0));
    let post_reservation_retry_seen = Arc::new(AtomicBool::new(false));
    let stage_for_hook = Arc::clone(&stage);
    let post_reservation_retry_seen_for_hook = Arc::clone(&post_reservation_retry_seen);
    let _progress_hook_guard = cluster.test_install_after_bucket_delete_exact_drain_progress_hook(
        Arc::new(move |phase, next_object_pg_id| {
            match (stage_for_hook.load(Ordering::SeqCst), phase) {
                (0, crate::TestBucketDeleteAttemptPhase::PostReservationObjectDrain)
                    if next_object_pg_id > 0 =>
                {
                    stage_for_hook.store(1, Ordering::SeqCst);
                    return Err(StoreError::MetadataCommandContention {
                        context: "bucket delete exact-bucket drain budget exhausted",
                    });
                }
                (1, crate::TestBucketDeleteAttemptPhase::Initial) => {
                    return Err(StoreError::Io {
                        context: "unexpected initial exact-bucket drain after post-reservation budget exhaustion",
                        source: std::io::Error::other(format!(
                            "next_object_pg_id={next_object_pg_id}"
                        )),
                    });
                }
                (1, crate::TestBucketDeleteAttemptPhase::PostReservationObjectDrain) => {
                    post_reservation_retry_seen_for_hook.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(())
        }),
    );

    let first_err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                context: "bucket delete exact-bucket drain budget exhausted"
            })
        ),
        "first DeleteBucket should preserve the post-reservation budget failure, got {first_err:?}"
    );

    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first_outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("budget failure should record a retryable attempt outcome");
    assert_eq!(
        first_outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::Retryable
    );
    assert_eq!(
        first_outcome.phase,
        crate::BucketDeleteAttemptPhase::PostReservationObjectDrain
    );
    assert!(
        first_outcome
            .post_reservation_next_object_pg_id
            .is_some_and(|next| next > 0),
        "post-reservation progress should be retained, got {first_outcome:?}"
    );
    drop(bucket_pg);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    assert!(
        post_reservation_retry_seen.load(Ordering::SeqCst),
        "retry should resume at post-reservation object drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
}

#[test]
fn begin_bucket_delete_adopted_attempt_clears_drain_on_bucket_not_empty() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-adopt-not-empty-");
        let key = key_for_object_pg(topology, &bucket, 2, "object-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &key,
        [0x5d; 16],
        b"visible object after preserved delete attempt",
    );

    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::ReservationWait,
            detail: "resume from reservation wait before terminal not-empty".to_string(),
            post_reservation_next_object_pg_id: None,
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "adopted DeleteBucket attempt should return terminal BucketNotEmpty, got {err:?}"
    );

    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "terminal BucketNotEmpty after adoption must clear the preserved delete drain"
    );
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(
        info.state,
        crate::BucketState::Active,
        "terminal BucketNotEmpty must leave the bucket active"
    );
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("terminal adopted attempt should record the not-empty outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::NotEmpty
    );
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityCheck
    );
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn post_reservation_exact_bucket_frontier_is_identity_fenced_and_resettable() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-progress-frontier-"),
            topology.pg_count(),
        )
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();

    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: "stale-delete-drain".to_string(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::PostReservationObjectDrain,
            detail: "stale progress must not be trusted".to_string(),
            post_reservation_next_object_pg_id: Some(pg_count),
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        None,
        "frontier from a different drain identity must not be trusted"
    );

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, pg_count)
        .unwrap();
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(pg_count),
        "empty post-reservation scan should advance the durable frontier"
    );

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, 0)
        .unwrap();
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(0),
        "pre-cleanup reset must be persisted before cleanup can proceed"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
}

#[test]
fn post_reservation_exact_bucket_frontier_resumes_after_recorded_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-progress-resume-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_key = key_for_object_pg(topology, &bucket, 2, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count > 2,
        "test requires at least three metadata PGs to prove frontier resume"
    );

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("81".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(2);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("82".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, 2)
        .unwrap();
    cluster
        .test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation(
            &bucket, &drain,
        )
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_some(),
        "post-reservation resume must not revisit object PGs below the stored frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "post-reservation resume must drain object PGs at or above the stored frontier"
    );
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(pg_count),
        "completed resumed scan should advance the durable frontier to the end"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
}

#[test]
fn post_reservation_exact_bucket_frontier_waits_for_published_replica_convergence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-published-frontier-");
        let key = key_for_object_pg(topology, &bucket, 2, "pending-");
        (bucket, key)
    };
    let object_pg_id = PgId::new(2);
    set_route_primary(&mut map, object_pg_id.get(), NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("83".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(object_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, object_pg_id, &bucket, &command);
    for node_id in [NodeId::new(0), NodeId::new(2)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN,
            "witness and primary must contain exact publication evidence"
        );
    }
    let trailing_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg_id.get())
        .unwrap();
    assert!(crate::PgMetadataStore::get_object_generation_reservation(
        &*trailing_pg,
        &bucket,
        &key,
        &reservation_id,
    )
    .is_err());
    drop(trailing_pg);

    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let error = cluster
        .test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation_with_max_attempts(
            &bucket,
            &drain,
            Some(1),
        )
        .expect_err("published command must not advance the exact-bucket drain frontier");
    assert!(matches!(
        error,
        crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        None,
        "incomplete replica convergence must not advance durable drain progress"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket),
        Some(command),
        "published command must remain owned by recovery until every replica converges"
    );
    assert_eq!(
        cluster.test_bucket_presence(&bucket).unwrap(),
        crate::test_support::TestBucketPresence::Active
    );
}

#[test]
fn post_reservation_exact_bucket_frontier_waits_for_terminal_slot_cleanup() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key, pg_count) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-cleanup-frontier-");
        let key = key_for_object_pg(topology, &bucket, 2, "pending-");
        (bucket, key, topology.pg_count())
    };
    let object_pg_id = PgId::new(2);
    set_route_primary(&mut map, object_pg_id.get(), NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("84".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(object_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, object_pg_id, &bucket, &command);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN,
            "every actor must contain the exact applied command before cleanup is deferred"
        );
    }

    let checksum = command.checksum_crc64();
    let cleanup_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_cleanup_attempts = Arc::clone(&cleanup_attempts);
    let cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |candidate| {
            if candidate.checksum_crc64() != checksum {
                return false;
            }
            hook_cleanup_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }),
    );
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let error = cluster
        .test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation_with_max_attempts(
            &bucket,
            &drain,
            Some(8),
        )
        .expect_err("deferred terminal cleanup must not advance the exact-bucket drain frontier");
    assert!(matches!(
        error,
        crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert!(
        cleanup_attempts.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the bounded drain must reach deferred pending-slot removal"
    );
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        None,
        "deferred pending-slot removal must not advance durable drain progress"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket),
        Some(command.clone()),
        "fully applied command must remain pending until terminal cleanup succeeds"
    );

    drop(cleanup_hook);
    cluster
        .test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation(
            &bucket, &drain,
        )
        .unwrap();
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(pg_count),
        "successful terminal cleanup should allow the exact-bucket frontier to complete"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket),
        None,
        "successful terminal cleanup must remove the pending command"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
}

#[test]
fn begin_bucket_delete_adopts_completed_post_reservation_frontier_without_rescan() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = vec![0, 2, 5, 31];
    let terminal_post_reservation_next_object_pg_id = 32;
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 2, "delete-terminal-progress-adopt-")
    };
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap()
    };
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::PostReservationObjectDrain,
            detail: "terminal post-reservation scan already completed".to_string(),
            post_reservation_next_object_pg_id: Some(terminal_post_reservation_next_object_pg_id),
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let saw_terminal_resume = Arc::new(AtomicBool::new(false));
    let saw_terminal_resume_for_hook = Arc::clone(&saw_terminal_resume);
    let _exact_drain_hook_guard = cluster.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            assert!(
                has_progress,
                "adopted post-reservation attempt should use stored progress"
            );
            assert_eq!(
                next_object_pg_id, terminal_post_reservation_next_object_pg_id,
                "terminal post-reservation cursor must be one past the highest PG id, not the PG count"
            );
            saw_terminal_resume_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }),
    );

    cluster
        .begin_bucket_delete_if_current(
            &bucket,
            crate::cluster::BucketIdentityGenerations {
                bucket_execution_generation: initial_bucket.bucket_execution_generation,
                bucket_incarnation_generation: initial_bucket.bucket_incarnation_generation,
            },
        )
        .unwrap();

    assert!(
        saw_terminal_resume.load(Ordering::SeqCst),
        "DeleteBucket retry should observe the terminal post-reservation frontier"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
}

#[test]
fn begin_bucket_delete_adopts_preserved_post_reservation_frontier() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..32).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-progress-adopt-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_pg = topology.pg_count() - 1;
        let later_key = key_for_object_pg(topology, &bucket, later_pg, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count >= 32,
        "test requires several exact-bucket drain chunks"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap()
    };
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "test-held-write",
            Some(lower_key.as_str()),
        )
        .unwrap();

    let delete_waiting = Arc::new((Mutex::new(false), Condvar::new()));
    let release_delete_wait = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_bucket = bucket.clone();
    let delete_waiting_for_hook = Arc::clone(&delete_waiting);
    let release_delete_wait_for_hook = Arc::clone(&release_delete_wait);
    let _bucket_hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                let (lock, cv) = &*delete_waiting_for_hook;
                *lock.lock().unwrap() = true;
                cv.notify_all();

                let (release_lock, release_cv) = &*release_delete_wait_for_hook;
                let _release_guard = release_cv
                    .wait_while(release_lock.lock().unwrap(), |released| !*released)
                    .unwrap();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let fail_after_first_frontier = Arc::new(AtomicBool::new(true));
    let fail_after_first_frontier_for_hook = Arc::clone(&fail_after_first_frontier);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                if next_object_pg_id < pg_count
                    && fail_after_first_frontier_for_hook.swap(false, Ordering::SeqCst)
                {
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                Ok(())
            },
        ));

    let delete_cluster = Arc::clone(&cluster);
    let delete_bucket = bucket.clone();
    let delete_thread = std::thread::spawn(move || {
        delete_cluster.test_begin_bucket_delete_if_current(&delete_bucket)
    });

    {
        let (lock, cv) = &*delete_waiting;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |waiting| {
                !*waiting
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "DeleteBucket should reach reservation wait before test installs pending work"
        );
    }

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("83".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(pg_count - 1);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("84".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    {
        let (lock, cv) = &*release_delete_wait;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    let err = delete_thread.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first DeleteBucket should preserve the attempt on injected route expiry, got {err:?}"
    );
    assert!(
        !fail_after_first_frontier.load(Ordering::SeqCst),
        "test hook should fail after the first persisted post-reservation frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_none(),
        "first DeleteBucket attempt should drain object PGs below the persisted frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_some(),
        "first DeleteBucket attempt should leave later object PGs for adoption/resume"
    );
    let preserved_drain = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .expect("retryable failure should preserve the active delete drain")
    };
    assert_eq!(
        preserved_drain.bucket_execution_generation,
        initial_bucket.bucket_execution_generation
    );
    let preserved_frontier = cluster
        .test_bucket_delete_post_reservation_next_object_pg_id(
            &crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: preserved_drain.clone(),
            },
        )
        .unwrap()
        .expect("retryable failure should persist post-reservation progress");
    assert!(
        preserved_frontier > 0 && preserved_frontier < pg_count,
        "frontier should identify a partial post-reservation scan, got {preserved_frontier}"
    );
    let resume_root = match cluster.try_take_reclaim_work() {
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(root)) => root,
        other => panic!(
            "retryable preserved DeleteBucket begin should queue background resume work, got {other:?}"
        ),
    };
    assert_eq!(resume_root.bucket, bucket);
    assert_eq!(
        resume_root.bucket_execution_generation,
        initial_bucket.bucket_execution_generation
    );
    assert_eq!(
        resume_root.bucket_incarnation_generation,
        initial_bucket.bucket_incarnation_generation
    );

    cluster
        .begin_bucket_delete_if_current(
            &resume_root.bucket,
            crate::cluster::BucketIdentityGenerations {
                bucket_execution_generation: resume_root.bucket_execution_generation,
                bucket_incarnation_generation: resume_root.bucket_incarnation_generation,
            },
        )
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "adopted DeleteBucket attempt should resume and drain later object PGs"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted attempt should record final outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_adopts_preserved_initial_frontier_then_resets_before_stream_cleanup() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..32).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-initial-progress-adopt-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_pg = topology.pg_count() - 1;
        let later_key = key_for_object_pg(topology, &bucket, later_pg, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count >= 32,
        "test requires several exact-bucket drain chunks"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap()
    };

    let hook_stage = Arc::new(AtomicUsize::new(0));
    let first_frontier = Arc::new(AtomicUsize::new(0));
    let reset_seen = Arc::new(AtomicBool::new(false));
    let hook_stage_for_hook = Arc::clone(&hook_stage);
    let first_frontier_for_hook = Arc::clone(&first_frontier);
    let reset_seen_for_hook = Arc::clone(&reset_seen);
    let _progress_hook_guard = cluster.test_install_after_bucket_delete_exact_drain_progress_hook(
        Arc::new(move |phase, next_object_pg_id| {
            match (hook_stage_for_hook.load(Ordering::SeqCst), phase) {
                (0, crate::TestBucketDeleteAttemptPhase::Initial)
                    if next_object_pg_id > 0 && next_object_pg_id < pg_count =>
                {
                    first_frontier_for_hook.store(next_object_pg_id as usize, Ordering::SeqCst);
                    hook_stage_for_hook.store(1, Ordering::SeqCst);
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                (1, crate::TestBucketDeleteAttemptPhase::Initial)
                    if next_object_pg_id == pg_count =>
                {
                    hook_stage_for_hook.store(2, Ordering::SeqCst);
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                (_, crate::TestBucketDeleteAttemptPhase::StreamCleanup)
                    if next_object_pg_id == 0 =>
                {
                    reset_seen_for_hook.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(())
        }),
    );

    let first_err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first DeleteBucket should preserve the attempt on injected route expiry, got {first_err:?}"
    );
    let preserved_drain = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .expect("retryable failure should preserve the active delete drain")
    };
    let first_frontier = first_frontier.load(Ordering::SeqCst) as u32;
    assert!(
        first_frontier > 0 && first_frontier < pg_count,
        "first attempt should persist a partial initial frontier, got {first_frontier}"
    );
    let initial_outcome = cluster
        .test_bucket_delete_post_reservation_next_object_pg_id(
            &crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: preserved_drain.clone(),
            },
        )
        .unwrap();
    assert_eq!(
        initial_outcome,
        Some(first_frontier),
        "first retryable failure should persist the initial exact-bucket frontier"
    );

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("87".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(pg_count - 1);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("88".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    let second_err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            second_err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "second DeleteBucket should fail after completing the resumed initial scan, got {second_err:?}"
    );
    assert_eq!(
        hook_stage.load(Ordering::SeqCst),
        2,
        "second attempt should reach the completed initial frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_some(),
        "resumed initial scan must not revisit object PGs below the stored frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "resumed initial scan must drain object PGs at or above the stored frontier"
    );

    cluster
        .begin_bucket_delete_if_current(
            &bucket,
            crate::cluster::BucketIdentityGenerations {
                bucket_execution_generation: initial_bucket.bucket_execution_generation,
                bucket_incarnation_generation: initial_bucket.bucket_incarnation_generation,
            },
        )
        .unwrap();
    assert!(
        reset_seen.load(Ordering::SeqCst),
        "successful retry should reset the exact-bucket frontier before stream cleanup"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_none(),
        "post-reset post-reservation scan should drain lower object PG work"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted attempt should record final outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(
        outcome.post_reservation_next_object_pg_id,
        Some(pg_count),
        "terminal post-reservation frontier should be restored after stream cleanup completes"
    );
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_drains_pending_delete_marker_before_emptiness_decision() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-marker-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    let pg_id = PgId::new(2);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let marker_version = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: marker_version,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: object_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the drained delete marker as bucket data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    marker_version,
                ),
                Ok(crate::StoredObject::DeleteMarker(_))
            ),
            "DeleteBucket should converge the pending delete marker on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .expect("BucketNotEmpty should leave the bucket active");
        assert_eq!(bucket_info.state, crate::BucketState::Active);
    }
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "BucketNotEmpty rollback should clear the durable delete drain"
    );
    drop(bucket_pg);
    assert_clean_metadata_command_stream(&map, &[1, 2]);
}

#[test]
fn begin_bucket_delete_drains_pending_specific_version_delete_that_empties_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "specific-delete-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "version-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [91; 16],
        [92; 16],
        b"delete the only version",
    );

    let pg_id = PgId::new(object_pg_id);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let stored = crate::PgMetadataStore::get_object_version(
        &*object_pg,
        &bucket,
        &key,
        committed.version_id,
    )
    .unwrap();
    let live = stored.as_live().unwrap();
    let payload = crate::StorageCluster::snapshot_live_object_payload_reclaim_command(
        &object_pg,
        &bucket,
        &key,
        live,
        crate::clock::current_time_millis(),
    )
    .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                crate::metadata_command::DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: committed.version_id,
            mode: crate::metadata_command::DeleteObjectVersionMode::Specific,
            target: DeleteObjectVersionTarget::Live {
                generation_id: live.generation_id,
                layout: live.layout,
                payload,
            },
        })),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    committed.version_id,
                ),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "DeleteBucket should converge the pending specific-version delete on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(bucket_info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_current_expiry_marker() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-current-");
        let key = key_for_object_pg(topology, &bucket, 2, "current-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0x93; 16],
        [0x94; 16],
        b"lifecycle current before delete",
    );

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
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "injected lifecycle current expiry apply failure",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected lifecycle current expiry apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .expire_current_object_if_due_raw(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .expect("published lifecycle expiry should hand trailing convergence to recovery")
        .expect("lifecycle current-expiry predicate should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle current expiry command should remain pending"
    );

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the lifecycle delete marker as visible data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("DeleteBucket should leave the lifecycle marker visible");
        assert!(
            matches!(current, crate::StoredObject::DeleteMarker(_)),
            "expected current delete marker on node {node_id:?}, got {current:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_noncurrent_expiry() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-noncurrent-");
        let key = key_for_object_pg(topology, &bucket, 2, "noncurrent-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa1; 16],
        [0xa2; 16],
        b"older lifecycle version",
    );
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa3; 16],
        [0xa4; 16],
        b"current lifecycle version",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let older_version = older.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == older_version
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "injected lifecycle noncurrent expiry apply failure",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected lifecycle noncurrent expiry apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .delete_noncurrent_live_versions_if_due_raw(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(HashSet::from([older.version_id])),
        )
        .expect("published lifecycle expiry should hand trailing convergence to recovery")
        .expect("lifecycle noncurrent-expiry predicate should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle noncurrent expiry command should remain pending"
    );

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the current version after draining noncurrent expiry, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                older.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("current version should remain visible");
        assert_eq!(visible.version_id(), current.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_expired_delete_marker_cleanup() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-marker-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let live = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xb1; 16],
        [0xb2; 16],
        b"live behind marker",
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

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let marker_version = marker.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == marker_version
                        && matches!(
                            delete.target,
                            DeleteObjectVersionTarget::DeleteMarker { .. }
                        )
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "injected lifecycle delete-marker cleanup apply failure",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected lifecycle delete-marker cleanup apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .delete_expired_delete_marker_if_due_raw(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .expect("published lifecycle cleanup should hand trailing convergence to recovery")
        .expect("lifecycle delete-marker predicate should succeed");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle delete-marker cleanup command should remain pending"
    );

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the revealed live version after draining marker cleanup, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                marker.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("live version should be revealed after marker cleanup");
        assert_eq!(visible.version_id(), live.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn stale_delete_drain_identity_cannot_clear_recreated_bucket_drain() {
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
        bucket_for_pg(topology, 1, "stale-delete-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let old_drain = {
        let node = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg_id = 1;
        let pg = node.get_pg(pg_id).unwrap();
        let record = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("DeleteBucket begin should leave a terminal durable drain");
        crate::cluster::DurableBucketWriteDrain { pg_id, record }
    };
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    create_test_bucket(&cluster, &bucket);

    let new_drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("recreated active bucket should acquire a fresh delete drain")
        }
    };
    assert_ne!(
        old_drain.record.bucket_execution_generation, new_drain.record.bucket_execution_generation,
        "delete/recreate must produce a distinct bucket incarnation"
    );

    let err = cluster
        .clear_durable_bucket_delete_drain(&old_drain)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketWriteDrainNotFound { .. }
            )
        ),
        "stale drain cleanup should not match the recreated bucket, got {err:?}"
    );
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("fresh drain should remain installed");
        assert_eq!(current.drain_id, new_drain.record.drain_id);
        assert_eq!(
            current.bucket_execution_generation,
            new_drain.record.bucket_execution_generation
        );
    }

    cluster
        .clear_durable_bucket_delete_drain(&new_drain)
        .unwrap();
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Active);
}

#[test]
fn begin_bucket_delete_recovers_expired_durable_drain_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "expired-delete-drain-")
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old_drain_id = "expired-delete-drain-before-reopen";
    {
        let pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            old_drain_id,
            "dead-delete-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            now.saturating_sub(1),
        )
        .unwrap();
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened)).unwrap();
    reopened_cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    {
        let pg = reopened
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(current.state, crate::BucketState::Deleting);
        let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect(
                "DeleteBucket should leave a terminal drain after recovering the expired drain",
            );
        assert_ne!(
            drain.drain_id, old_drain_id,
            "expired pre-reopen drain must be rolled back by exact identity"
        );
    }
    assert!(
        matches!(
            reopened_cluster
                .begin_durable_bucket_delete_drain(&bucket)
                .unwrap(),
            crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting
        ),
        "recovered terminal delete should be idempotent after reopen"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn pending_delete_finalized_bucket_recovery_uses_expired_active_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "pending-finalized-recovery-")
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();

    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let deleting = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    assert_eq!(deleting.state, crate::BucketState::Deleting);
    let command_log_index = primary_pg
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index
        .checked_add(1)
        .and_then(MetadataCommandLogIndex::new)
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, command_log_index),
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            deleting.bucket_execution_generation,
            deleting.bucket_incarnation_generation,
        )),
    );
    primary_pg
        .try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    drop(primary_pg);

    map.expire_route_map_lease_at(
        RouteMapValidity::until_ms(0).unwrap(),
        crate::clock::monotonic_time_millis(),
    );
    assert!(
        matches!(
            cluster.pending_metadata_command_for_bucket(pg_id, &bucket),
            Err(StoreError::RouteMapExpired { .. })
        ),
        "normal pending-command reads must still reject an expired route map"
    );

    let drained = cluster
        .drain_pending_metadata_commands_for_current_map()
        .unwrap();
    assert_eq!(drained, 1);

    let primary = map
        .metadata_pg_primary_node_for_metadata_command_recovery(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    assert!(
        primary_pg
            .pending_metadata_command_envelope(primary.node_id().as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none(),
        "recovery should clear the exact pending finalizer slot"
    );
    drop(primary_pg);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ),
            "DeleteFinalizedBucket should be applied on node {node_id:?}"
        );
    }
}
