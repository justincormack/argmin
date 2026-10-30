// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed-server lifetime tracking, not yet durable window/write fencing.

use super::*;

fn identity(server: &StorageNodeServer, pg_id: u32) -> Option<[u8; 16]> {
    server
        .test_storage_node()
        .get_pg(pg_id)
        .unwrap()
        .test_allocation_lifetime_identity()
}

fn two_pg_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
    let mut config = bounded_runtime_refresh_config(test_config(tmp));
    config.pg_ids.push(1);
    config.pg_routes.push(test_route(1));
    private_socket_dir(config.socket_path.parent().unwrap());
    config
}

#[test]
fn storage_node_allocation_lifetime_tracks_route_changes_not_renewal_or_history() {
    let tmp = test_util::tempdir();
    let mut config = two_pg_config(&tmp);
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let original = identity(&server, 0).unwrap();
    let other = identity(&server, 1).unwrap();
    let checkpoint = server
        .test_storage_node()
        .get_pg(0)
        .unwrap()
        .metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
        .unwrap();
    config.route_map_validity =
        RouteMapValidity::until_ms(config.route_map_valid_until_ms().unwrap() + 1_000).unwrap();
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    assert_eq!(identity(&server, 0), Some(original));
    assert_eq!(identity(&server, 1), Some(other));

    // Active -> Peering -> Active in one epoch is sufficient to lose allocation
    // continuity, even when the node ID and every durable PG row stay unchanged.
    config.pg_routes[0].state = PgState::Peering;
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    let peering = identity(&server, 0).unwrap();
    assert_ne!(peering, original);
    assert_eq!(identity(&server, 1), Some(other));
    config.pg_routes[0].state = PgState::Active;
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    let returned = identity(&server, 0).unwrap();
    assert_ne!(returned, original);
    assert_ne!(returned, peering);
    assert_eq!(identity(&server, 1), Some(other));

    // Move the primary away and back across actual epoch transitions. The same
    // still-open process must never recover its original allocation identity.
    // First isolate acting-set and primary changes from epoch/state changes.
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    let expanded = identity(&server, 0).unwrap();
    assert_ne!(expanded, returned);
    assert_eq!(identity(&server, 1), Some(other));
    config.pg_routes[0].primary_node_id = NodeId::new(8);
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    assert_ne!(identity(&server, 0).unwrap(), expanded);
    assert_eq!(identity(&server, 1), Some(other));
    for (epoch, primary) in [(2, 7), (3, 8), (4, 7)] {
        let before = identity(&server, 0).unwrap();
        config.historical_pg_routes.extend(config.pg_routes.clone());
        config.cluster_epoch = ClusterEpoch::new(epoch).unwrap();
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
        }
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        config.pg_routes[0].primary_node_id = NodeId::new(primary);
        server
            .install_control_plane_runtime_config(config.clone())
            .unwrap();
        assert_ne!(identity(&server, 0).unwrap(), before);
        assert_ne!(identity(&server, 0).unwrap(), original);
    }
    let before = [identity(&server, 0), identity(&server, 1)];
    config.historical_pg_routes.clear();
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    assert_eq!([identity(&server, 0), identity(&server, 1)], before);
    // An exact repeated install is not another allocation lifetime.
    server.install_control_plane_runtime_config(config).unwrap();
    assert_eq!([identity(&server, 0), identity(&server, 1)], before);
    assert_eq!(
        server
            .test_storage_node()
            .get_pg(0)
            .unwrap()
            .metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap(),
        checkpoint,
        "route-driven issuer replacement must not mutate the PG's durable state"
    );
}

#[test]
fn storage_node_allocation_lifetime_failed_publication_stays_retired_until_retry() {
    for retry_as_renewal in [false, true] {
        let tmp = test_util::tempdir();
        let config = two_pg_config(&tmp);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let original = identity(&server, 0).unwrap();
        let other = identity(&server, 1).unwrap();
        let mut next = config.clone();
        next.pg_routes[0].state = PgState::Peering;
        let staging_path =
            control_plane_runtime_config_path(&config.data_dir).with_extension("tmp");
        // Staging succeeds, then publication's rename fails. This reaches lifetime
        // replacement, unlike failure before the route-publication barrier.
        *server.runtime_config_stage_test_hook.lock().unwrap() = Some(Arc::new(move || {
            fs::remove_file(&staging_path).unwrap();
        }));
        let error = server
            .install_control_plane_runtime_config(next.clone())
            .unwrap_err();
        assert!(
            matches!(error, StorageNodeServerError::RuntimeConfigWrite { source, .. }
        if source.kind() == io::ErrorKind::NotFound)
        );
        assert_eq!(identity(&server, 0), None);
        assert_eq!(identity(&server, 1), Some(other));
        assert_storage_node_process_config_eq(&server.config_snapshot(), &config);
        *server.runtime_config_stage_test_hook.lock().unwrap() = None;
        if retry_as_renewal {
            // The control plane may abandon the candidate and extend the still
            // installed route instead. Do not strand a retired PG in the fast path.
            next = config;
            next.route_map_validity =
                RouteMapValidity::until_ms(next.route_map_valid_until_ms().unwrap() + 1_000)
                    .unwrap();
        }
        server
            .install_control_plane_runtime_config(next.clone())
            .unwrap();
        assert_ne!(identity(&server, 0).unwrap(), original);
        assert_eq!(identity(&server, 1), Some(other));
        assert_storage_node_process_config_eq(&server.config_snapshot(), &next);
        let recovered = identity(&server, 0).unwrap();
        next.route_map_validity =
            RouteMapValidity::until_ms(next.route_map_valid_until_ms().unwrap() + 1_000).unwrap();
        server.install_control_plane_runtime_config(next).unwrap();
        assert_eq!(
            identity(&server, 0),
            Some(recovered),
            "successful retry must clear the pending reset"
        );
    }
}

#[test]
fn storage_node_allocation_lifetime_waits_for_admitted_frames_before_replacement() {
    let tmp = test_util::tempdir();
    let config = two_pg_config(&tmp);
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let original = identity(&server, 0).unwrap();
    let admitted = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    let mut next = config;
    next.pg_routes[0].state = PgState::Peering;
    let installing = Arc::clone(&server);
    let installer = thread::spawn(move || installing.install_control_plane_runtime_config(next));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server
            .route_admission
            .inner
            .state
            .lock()
            .unwrap()
            .transition
            == StorageNodeRouteTransitionState::Draining
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "publication never entered draining"
        );
        thread::yield_now();
    }
    assert_eq!(identity(&server, 0), Some(original));
    assert_eq!(server.config_snapshot().pg_routes[0].state, PgState::Active);
    drop(admitted);
    installer.join().unwrap().unwrap();
    assert_ne!(identity(&server, 0).unwrap(), original);
    assert_eq!(
        server.config_snapshot().pg_routes[0].state,
        PgState::Peering
    );
}
