// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::{StorageClusterRouteHandle, StorageClusterRuntimeMapRefreshError};

fn embedded(root: &Path) -> Arc<StorageCluster> {
    let map = LocalClusterMap::open(
        root,
        &[NodeId::new(0), NodeId::new(1)],
        &[0, 1],
        EcShape { k: 1, m: 1 },
    )
    .unwrap();
    cluster(map)
}

fn cluster(mut map: LocalClusterMap) -> Arc<StorageCluster> {
    map.test_set_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let epoch = map.epoch;
    StorageCluster::test_from_local_map_with_epoch(Arc::new(map), epoch).unwrap()
}

fn identity(cluster: &StorageCluster, node: u32, pg: u32) -> Option<[u8; 16]> {
    cluster.local_map.nodes[&NodeId::new(node)]
        .runtime
        .test_pg_allocation_lifetime(pg)
}

fn changed(
    cluster: &StorageCluster,
    epoch: ClusterEpoch,
    state: PgState,
    primary: NodeId,
) -> Arc<StorageCluster> {
    let routes = cluster.local_map.pg_routes().map(|route| {
        PgRouteSnapshot::reconstructed(
            epoch,
            route.pg_id(),
            if route.pg_id().get() == 0 {
                primary
            } else {
                route.primary_node_id()
            },
            route.acting_set().to_vec(),
            if route.pg_id().get() == 0 {
                state
            } else {
                route.state()
            },
        )
    });
    let map = cluster
        .local_map
        .test_clone_with_pg_routes(epoch, routes, [])
        .unwrap();
    self::cluster(map)
}

#[test]
fn embedded_allocation_lifetime_publication_changes_only_affected_issuers() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let root = test_util::tempdir();
    let initial = embedded(root.path());
    let handle = StorageClusterRouteHandle::from_authorized_cluster(initial.clone());
    let before = [identity(&initial, 0, 0), identity(&initial, 1, 0)];
    let unaffected = [identity(&initial, 0, 1), identity(&initial, 1, 1)];
    let candidate = changed(
        &initial,
        ClusterEpoch::INITIAL,
        PgState::Peering,
        NodeId::new(0),
    );
    // Candidate construction shares the stores but must not invalidate them.
    assert_eq!([identity(&initial, 0, 0), identity(&initial, 1, 0)], before);
    handle.install(candidate.clone()).unwrap();
    let after = [identity(&initial, 0, 0), identity(&initial, 1, 0)];
    for (old, new) in before.into_iter().zip(after) {
        assert!(old.is_some() && new.is_some());
        assert_ne!(old, new);
    }
    assert_eq!(
        [identity(&initial, 0, 1), identity(&initial, 1, 1)],
        unaffected
    );
    let renewed = candidate
        .test_clone_with_dynamic_route_map_validity(RouteMapValidity::until_ms(20_000).unwrap())
        .unwrap();
    handle.install(renewed).unwrap();
    assert_eq!([identity(&initial, 0, 0), identity(&initial, 1, 0)], after);
    let active = changed(
        &candidate,
        ClusterEpoch::INITIAL,
        PgState::Active,
        NodeId::new(0),
    );
    handle.install(active.clone()).unwrap();
    assert_ne!(identity(&active, 0, 0), before[0]);
    assert_ne!(identity(&active, 0, 0), after[0]);
    let before_primary = identity(&active, 0, 0);
    let moved = changed(
        &active,
        ClusterEpoch::INITIAL,
        PgState::Active,
        NodeId::new(1),
    );
    handle.install(moved.clone()).unwrap();
    assert_ne!(identity(&active, 0, 0), before_primary);
    assert_eq!(identity(&active, 0, 1), unaffected[0]);
    let next = changed(
        &moved,
        ClusterEpoch::new(2).unwrap(),
        PgState::Active,
        NodeId::new(0),
    );
    handle.install(next).unwrap();
    assert_ne!(identity(&initial, 0, 1), unaffected[0]);
}

#[test]
fn embedded_allocation_lifetime_replacement_covers_removed_and_foreign_runtimes() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let root = test_util::tempdir();
    let initial = embedded(root.path());
    let handle = StorageClusterRouteHandle::from_authorized_cluster(initial.clone());
    let old = identity(&initial, 0, 0);
    // A genuinely different node runtime, not another SQLite opener on the same files.
    let foreign_root = test_util::tempdir();
    let candidate = embedded(foreign_root.path());
    let candidate_before = identity(&candidate, 0, 0);
    handle.install(candidate.clone()).unwrap();
    assert_ne!(identity(&initial, 0, 0), old);
    assert_ne!(identity(&candidate, 0, 0), candidate_before);
    let removed_before = identity(&candidate, 1, 0);
    let mut removed = candidate
        .local_map
        .test_clone_with_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    removed.nodes.remove(&NodeId::new(1));
    removed.default_ec_shape = EcShape { k: 1, m: 0 };
    for route in removed.pg_routes.values_mut() {
        route.acting_set = Arc::from([NodeId::new(0)]);
    }
    handle.install(cluster(removed)).unwrap();
    assert_ne!(identity(&candidate, 1, 0), removed_before);
}

#[test]
fn embedded_allocation_lifetime_remote_frontend_preserves_shadow_runtimes() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let root = test_util::tempdir();
    let mut map = LocalClusterMap::open(
        root.path(),
        &[NodeId::new(0), NodeId::new(1)],
        &[0, 1],
        EcShape { k: 1, m: 1 },
    )
    .unwrap();
    // Frontends may carry local shadow state. Both transports must leave its
    // lifetime alone: the installed storage server owns route replacement.
    map.nodes
        .get_mut(&NodeId::new(0))
        .unwrap()
        .route_execution_endpoint =
        LocalRouteExecutionEndpoint::RpcUnix(root.path().join("node.sock"));
    map.nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .route_execution_endpoint = LocalRouteExecutionEndpoint::RpcTcp("127.0.0.1:12345".into());
    let initial = cluster(map);
    let handle = StorageClusterRouteHandle::from_authorized_cluster(initial.clone());
    let before = [identity(&initial, 0, 0), identity(&initial, 1, 0)];
    let candidate = changed(
        &initial,
        ClusterEpoch::new(2).unwrap(),
        PgState::Active,
        NodeId::new(1),
    );
    handle.install(candidate).unwrap();
    assert_eq!([identity(&initial, 0, 0), identity(&initial, 1, 0)], before);
}

#[test]
fn embedded_allocation_lifetime_waits_for_admission_and_rejects_stale_candidate() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let root = test_util::tempdir();
    let initial = embedded(root.path());
    let handle = StorageClusterRouteHandle::from_authorized_cluster(initial.clone());
    let admission = handle.admit_current_route().unwrap();
    let before = identity(&initial, 0, 0);
    let candidate = changed(
        &initial,
        ClusterEpoch::INITIAL,
        PgState::Peering,
        NodeId::new(0),
    );
    let installer_handle = handle.clone();
    let installer_candidate = candidate.clone();
    let installer = thread::spawn(move || {
        let _clock = crate::clock::test_time_override_guard(1_000);
        installer_handle.install(installer_candidate).unwrap();
    });
    handle.route_admission.wait_until_publication_is_pending();
    assert_eq!(identity(&initial, 0, 0), before);
    drop(admission);
    installer.join().unwrap();
    let published = identity(&initial, 0, 0);
    assert_ne!(published, before);
    assert!(!handle
        .test_install_if_current(&initial, initial.clone())
        .unwrap());
    assert_eq!(identity(&initial, 0, 0), published);
    let expired = candidate
        .test_clone_with_dynamic_route_map_validity(RouteMapValidity::until_ms(900).unwrap())
        .unwrap();
    assert!(matches!(
        handle.install(expired),
        Err(StorageClusterRuntimeMapRefreshError::ExpiredRouteMapValidity { .. })
    ));
    assert_eq!(identity(&initial, 0, 0), published);
}

#[test]
fn embedded_allocation_lifetime_failed_publication_retires_and_retry_recovers() {
    let clock = crate::clock::test_time_override_guard(1_000);
    let root = test_util::tempdir();
    let initial = embedded(root.path());
    let handle = StorageClusterRouteHandle::from_authorized_cluster(initial.clone());
    let before = identity(&initial, 0, 0);
    let candidate = changed(
        &initial,
        ClusterEpoch::INITIAL,
        PgState::Peering,
        NodeId::new(0),
    );
    let publication = handle.route_admission.begin_publication();
    let result =
        handle.install_during_publication_with_lifetime_hook(&publication, None, candidate, || {
            clock.set(11_000)
        });
    drop(publication);
    assert!(matches!(
        result,
        Err(StorageClusterRuntimeMapRefreshError::ExpiredRouteMapValidity { .. })
    ));
    assert!(Arc::ptr_eq(&handle.current(), &initial));
    assert_eq!(identity(&initial, 0, 0), None);
    assert!(handle
        .allocation_lifetime_replacement_pending
        .load(Ordering::SeqCst));
    // The CP may abandon the failed topology and only renew the old map.
    let renewed = initial
        .test_clone_with_dynamic_route_map_validity(RouteMapValidity::until_ms(20_000).unwrap())
        .unwrap();
    let authority = renewed.route_authority.dynamic_proof().unwrap();
    let status = crate::control_plane::ControlPlaneRuntimeMapStatus::test_with_lease_renewal(
        renewed.cluster_epoch(),
        authority.content_digest,
        RouteMapValidity::until_ms(20_000).unwrap(),
        authority.freshness_proof,
    );
    assert!(handle
        .renew_from_runtime_map_status(status, 11_000, 11_000)
        .unwrap()
        .is_none());
    handle.install(renewed.clone()).unwrap();
    let recovered = identity(&initial, 0, 0);
    assert!(recovered.is_some());
    assert_ne!(recovered, before);
    assert!(!handle
        .allocation_lifetime_replacement_pending
        .load(Ordering::SeqCst));
    handle.install(renewed).unwrap();
    assert_eq!(identity(&initial, 0, 0), recovered);
}
