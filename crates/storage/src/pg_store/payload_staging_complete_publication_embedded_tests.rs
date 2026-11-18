// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    LocalClusterMap, LocalNodeStoreConfig, LocalPgRoute, StorageCluster, StorageClusterRouteHandle,
};
use std::sync::Arc;

pub(super) fn embedded(
    dir: &test_util::TempDir,
    case: usize,
    state: PgState,
    validity: RouteMapValidity,
) -> (Arc<LocalClusterMap>, StorageClusterRouteHandle) {
    let config = config(case);
    let mut map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        config.node_id,
        [0, 1, 2, 3, 4]
            .map(|id| LocalNodeStoreConfig::new(NodeId::new(id), dir.path().join(id.to_string()))),
        &config.pg_ids,
        config.default_ec_shape,
        config.cluster_epoch,
        config.pg_routes.iter().map(|route| {
            LocalPgRoute::from(&crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch,
                PgId::new(route.pg_id),
                route.primary_node_id,
                route.acting_set.clone(),
                state,
            ))
        }),
    )
    .unwrap();
    map.test_set_route_map_validity(validity);
    let map = Arc::new(map);
    let cluster =
        StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), config.cluster_epoch)
            .unwrap();
    let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
    (map, handle)
}

#[test]
fn staging_complete_publication_embedded_contexts_exact_retry_and_apply() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (map, handle) = embedded(&dir, case, PgState::Active, RouteMapValidity::Forever);
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        for _ in 0..2 {
            section
                .start_complete_publication(&request, deadline())
                .unwrap();
            assert!(started(&node.get_pg(pg_id.get()).unwrap()));
            assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
        }
        apply(&node.get_pg(pg_id.get()).unwrap(), &context);
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
        section
            .start_complete_publication(&request, deadline())
            .unwrap();
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
}

#[test]
fn staging_complete_publication_embedded_rejects_crossed_primary_epoch_and_peering() {
    let dir = test_util::tempdir();
    let (map, handle) = embedded(&dir, 0, PgState::Active, RouteMapValidity::Forever);
    let admission = handle.admit_current_route().unwrap();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    for (node, epoch) in [(2, 5), (4, 4), (4, 6)] {
        assert!(admission
            .embedded_publication_route(NodeId::new(node), pg_id, ClusterEpoch::new(epoch).unwrap())
            .is_err());
    }
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(NodeId::new(2), &topology())
        .unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
    assert!(section
        .start_complete_publication(&request, deadline())
        .is_err());
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    assert_eq!(
        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
        writes
    );
    let dir = test_util::tempdir();
    let (_, handle) = embedded(&dir, 0, PgState::Peering, RouteMapValidity::Forever);
    let admission = handle.admit_current_route().unwrap();
    assert!(matches!(
        admission.embedded_publication_route(NodeId::new(4), pg_id, ClusterEpoch::new(5).unwrap()),
        Err(StoreError::PgNotActive { .. })
    ));
}

#[test]
fn staging_complete_publication_embedded_binds_exact_store_and_command() {
    let dir = test_util::tempdir();
    let (map, handle) = embedded(&dir, 0, PgState::Active, RouteMapValidity::Forever);
    let foreign_dir = test_util::tempdir();
    let (foreign, _) = embedded(&foreign_dir, 0, PgState::Active, RouteMapValidity::Forever);
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let foreign_node = foreign.node(NodeId::new(4)).unwrap().test_node();
    // Same complete command/PG/epoch, different physical store: position alone
    // must not make an authority reusable across runtime domains.
    let foreign_pg = foreign_node.get_pg(pg_id.get()).unwrap();
    let _ = super::context(&foreign_pg, 0);
    install(&foreign_pg, &context);
    let foreign_before = snapshot(&foreign_pg);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    section
        .test_with_publication_authority(&request, deadline(), |pg, authority| {
            let before = snapshot(pg);
            assert!(matches!(
                authority.require_effect(&foreign_pg, &request),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            let wrong = pending
                .publication_request(NodeId::new(2), &topology())
                .unwrap();
            assert!(matches!(
                authority.require_effect(pg, &wrong),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
                context.id(),
                crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
                    crate::metadata_command::ReserveObjectVersionCommand::new(
                        fixture(StagingTarget::DirectPut).subject.bucket,
                        fixture(StagingTarget::DirectPut).subject.key,
                        crate::VersionId::from_u64(99),
                    ),
                ),
            );
            let crossed =
                ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
            let crossed = super::pending(&crossed);
            let crossed = crossed
                .publication_request(NodeId::new(4), &topology())
                .unwrap();
            assert_eq!(crossed.id(), request.id());
            assert_ne!(crossed.command_sha256(), request.command_sha256());
            assert!(matches!(
                authority.require_effect(pg, &crossed),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            assert_eq!(snapshot(pg), before);
            request.start_embedded(pg, authority)
        })
        .unwrap();
    assert_eq!(snapshot(&foreign_pg), foreign_before);
}

#[test]
fn staging_complete_publication_embedded_uses_shared_serialization_and_work_cutoff() {
    let dir = test_util::tempdir();
    let (map, handle) = embedded(&dir, 0, PgState::Active, RouteMapValidity::Forever);
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
        .unwrap();
    let shared_lock = map.runtime_state().metadata_command_pg_lock(pg_id);
    let guard = shared_lock.lock();
    assert!(matches!(
        route.lock_until(Instant::now() + Duration::from_millis(10)),
        Err(StoreError::OperationDeadlineExceeded { .. })
    ));
    drop(guard);
    let section = route.lock_until(deadline()).unwrap();
    assert!(
        shared_lock.try_lock().is_none(),
        "must hold the real shared lock"
    );
    let cutoff = Instant::now() + Duration::from_secs(1);
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let error = section
        .test_with_publication_authority(&request, cutoff, |pg, authority| {
            request.test_start_embedded_with_hook(pg, authority, |stage| {
                if stage == "before-marker" {
                    assert!(Instant::now() < cutoff);
                    while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
                        std::thread::sleep(remaining);
                    }
                }
                Ok(())
            })
        })
        .unwrap_err();
    assert!(matches!(
        error.into_store_error().unwrap(),
        StoreError::OperationDeadlineExceeded { .. }
    ));
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    assert!(
        section
            .start_complete_publication(&request, deadline())
            .is_err(),
        "later retry cannot renew holder"
    );
    drop(section);
    assert!(shared_lock.try_lock().is_some());
}

#[test]
fn staging_complete_publication_embedded_fence_survives_same_epoch_renewal() {
    for failure_stage in [
        "before-trial",
        "after-trial",
        "before-marker",
        "before-commit",
    ] {
        let clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let (map, handle) = embedded(
            &dir,
            0,
            PgState::Active,
            RouteMapValidity::until_ms(20_000).unwrap(),
        );
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let result =
            section.test_with_publication_authority(&request, deadline(), |pg, authority| {
                request.test_start_embedded_with_hook(pg, authority, |stage| {
                    if stage == failure_stage {
                        map.test_store_route_map_validity(
                            RouteMapValidity::until_ms(40_000).unwrap(),
                        );
                        clock.set(21_000);
                        map.require_route_map_valid_now().unwrap();
                    }
                    Ok(())
                })
            });
        assert!(matches!(
            result.unwrap_err().into_store_error().unwrap(),
            StoreError::RouteMapExpired { .. }
        ));
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}
