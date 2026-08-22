// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn administrative_unavailable_fence_survives_heartbeat_and_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let healthy_epoch = authority
        .heartbeat(heartbeat(2, authority.snapshot().cluster_epoch(), 100), 100)
        .unwrap()
        .cluster_epoch();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let unavailable = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(unavailable.membership(), NodeMembershipState::Active);
    assert_eq!(
        unavailable.availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(!unavailable.administratively_available());
    assert_eq!(
        unavailable.observed_availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(authority.snapshot().cluster_epoch() > healthy_epoch);

    let unavailable_epoch = authority.snapshot().cluster_epoch();
    let fenced = authority
        .heartbeat(heartbeat(2, unavailable_epoch, 500), 500)
        .unwrap();
    assert_eq!(fenced.cluster_epoch(), unavailable_epoch);
    assert!(!fenced.serving());
    let fenced_node = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!fenced_node.administratively_available());
    assert_eq!(
        fenced_node.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let fenced_lease_deadline_ms = fenced_node.lease_deadline_ms();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let still_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), unavailable_epoch);
    assert_eq!(
        still_fenced.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    assert_eq!(still_fenced.lease_deadline_ms(), fenced_lease_deadline_ms);

    let mut authority = reopen_file_authority(&store);
    let restarted = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!restarted.administratively_available());
    assert_eq!(restarted.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(
        restarted.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let restart_epoch = authority.snapshot().cluster_epoch();
    assert!(!authority
        .heartbeat(heartbeat(2, restart_epoch, 600), 600)
        .unwrap()
        .serving());
    let expiry = authority.expire_heartbeat_leases(700).unwrap();
    assert_eq!(expiry.cluster_epoch(), restart_epoch);
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(2)]);
    assert!(expiry.peering_pgs().is_empty());
    let expired_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!expired_fenced.administratively_available());
    assert_eq!(
        expired_fenced.observed_availability(),
        NodeAvailabilityState::Unavailable
    );

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    let enabled_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority
        .heartbeat(heartbeat(2, enabled_epoch, 700), 700)
        .unwrap();
    assert!(!recovering.serving());
    let serving = authority
        .heartbeat(heartbeat(2, recovering.cluster_epoch(), 800), 800)
        .unwrap();
    assert!(serving.serving());
    let enabled = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(enabled.membership(), NodeMembershipState::Active);
    assert!(enabled.administratively_available());
    assert_eq!(
        enabled.observed_availability(),
        NodeAvailabilityState::Healthy
    );
}

#[test]
fn deterministic_primary_uses_first_healthy_serving_acting_set_member() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Unavailable)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Out)
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    let acting_set = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(7), &acting_set, 2_000),
        Some(NodeId::new(3))
    );
}

#[test]
fn expired_heartbeat_lease_marks_node_unavailable_and_bumps_epoch_once() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let node_one = heartbeat_until_serving(&mut authority, 1, 1_000);
    let node_two = heartbeat_until_serving(&mut authority, 2, 1_000);
    assert!(node_one.serving());
    assert!(node_two.serving());
    let node_one_current = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    assert!(node_one_current.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_002,),
        Some(NodeId::new(1))
    );
    authority
        .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            9,
            PgState::Peering,
            1_003 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(9),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_011);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_051),
            1_051,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(9), 1_051),
        Some(NodeId::new(1))
    );

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(1_152).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(9)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(expiry.snapshot().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        expiry.snapshot().pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_112,),
        None
    );
    assert_eq!(authority.serving_pg_primary(PgId::new(9), 1_112), None);

    let durable_after_expiry = std::fs::read(&store_path).unwrap();
    let repeated = authority.expire_heartbeat_leases(9_999).unwrap();
    assert_eq!(repeated.expired_nodes(), &[]);
    assert_eq!(repeated.peering_pgs(), &[]);
    assert_eq!(repeated.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        durable_after_expiry,
        "an expiry scan with no lease transition must not rewrite state"
    );

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(persisted.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        persisted.pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        persisted.node(NodeId::new(2)).unwrap().availability(),
        NodeAvailabilityState::Unavailable
    );
}

#[test]
fn heartbeat_after_expiry_must_observe_new_epoch_before_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 3, 1_000);
    assert!(serving.serving());

    let expiry = authority.expire_heartbeat_leases(1_101).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(3)]);

    let stale_after_expiry = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, serving.cluster_epoch(), 1_200),
            1_200,
        )
        .unwrap();
    assert!(!stale_after_expiry.serving());
    assert_eq!(stale_after_expiry.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(3))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, stale_after_expiry.cluster_epoch(), 1_300),
            1_300,
        )
        .unwrap();
    assert!(recovered.cluster_epoch() > stale_after_expiry.cluster_epoch());
    assert!(!recovered.serving());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, recovered.cluster_epoch(), 1_400),
            1_400,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(3)], 1_400),
        Some(NodeId::new(3))
    );
}

#[test]
fn recovered_earlier_primary_forces_active_pg_back_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 100).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Peering, 1_000);
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, 1_050);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_060,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Active, 1_070);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_080),
            1_080,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), 1_080),
        Some(NodeId::new(1))
    );

    let node_one_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expiry = authority
        .expire_heartbeat_leases(node_one_deadline)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(13)]);

    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Peering,
        node_one_deadline + 1,
    );
    let successor_fence_ms = node_one_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, successor_fence_ms);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            successor_fence_ms,
        )
        .unwrap();
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Active,
        successor_fence_ms + 1,
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 1),
        Some(NodeId::new(2))
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                successor_fence_ms + 2,
            ),
            successor_fence_ms + 2,
        )
        .unwrap();
    assert!(!recovered.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 2),
        None
    );

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                recovered.cluster_epoch(),
                successor_fence_ms + 3,
            ),
            successor_fence_ms + 3,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            successor_fence_ms + 4,
        ),
        Err(ControlPlaneError::PgNotActive { pg_id: 13, .. })
    ));
}

#[test]
fn stale_runtime_map_fails_closed_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 2_002);

    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let stale_frontend_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &active_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(
        stale_frontend_cluster.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert!(stale_frontend_cluster
        .require_route_map_valid_at(valid_until_ms - 1)
        .is_ok());

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(valid_until_ms).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(17)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        stale_frontend_cluster.require_route_map_valid_at(valid_until_ms),
        Err(crate::StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: expired_at,
            now_ms,
        }) if cluster_epoch == active_map.cluster_epoch()
            && expired_at == valid_until_ms
            && now_ms == valid_until_ms
    ));
    assert_eq!(
        authority
            .snapshot()
            .runtime_map(valid_until_ms)
            .unwrap()
            .pg_routes()[0]
            .state(),
        PgState::Peering
    );
}

#[test]
fn stale_primary_authorization_cannot_validate_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Active, 2_002);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active.cluster_epoch(),
            2_003,
        )
        .unwrap();
    assert!(authority
        .validate_pg_operation_authorization(&authorization, 2_004)
        .is_ok());

    let lease_deadline_ms = authorization.primary().lease_deadline_ms();
    let expiry = authority
        .expire_heartbeat_leases(lease_deadline_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(
        authority.snapshot().pg(PgId::new(18)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, lease_deadline_ms),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active.cluster_epoch()
            && current_epoch == expiry.cluster_epoch()
    ));
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            expiry.cluster_epoch(),
            lease_deadline_ms,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, expiry.cluster_epoch(), lease_deadline_ms + 1),
            lease_deadline_ms + 1,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > expiry.cluster_epoch());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, lease_deadline_ms + 2),
            lease_deadline_ms + 2,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            recovery_epoch,
            lease_deadline_ms + 3,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 18,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Peering,
        lease_deadline_ms + 4,
    );
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline_ms + 5,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Active,
        lease_deadline_ms + 6,
    );
    let fresh_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            lease_deadline_ms + 7,
        )
        .unwrap();
    assert_eq!(
        fresh_authorization.cluster_epoch(),
        active_again.cluster_epoch()
    );
    authority
        .validate_pg_operation_authorization(&fresh_authorization, lease_deadline_ms + 8)
        .unwrap();
}

#[test]
fn storage_node_refresh_after_epoch_transition_cannot_keep_stale_active_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let active_valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(active_map.pg_routes()[0].state(), PgState::Active);

    let expiry = authority
        .expire_heartbeat_leases(active_valid_until_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(19)).unwrap().state(),
        PgState::Peering
    );

    let mut stale_active_heartbeat =
        heartbeat_from_record(&authority, 1, active_epoch, active_valid_until_ms + 1);
    stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(stale_active_heartbeat, active_valid_until_ms + 1)
        .unwrap();

    assert!(!refresh.lease().serving());
    assert_eq!(refresh.lease().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        refresh.runtime_map().cluster_epoch(),
        expiry.cluster_epoch()
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Peering
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].primary_lease_deadline_ms(),
        None
    );
    let record = authority.snapshot().node(NodeId::new(1)).unwrap();
    assert_eq!(record.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(record.last_observed_epoch(), Some(active_epoch));
}

#[test]
fn temporary_availability_loss_reactivates_same_primary_before_old_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_003);
    let active_epoch = active.cluster_epoch();
    let active_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_004,
        )
        .unwrap();

    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Suspect)
        .unwrap();
    let suspect_epoch = authority.snapshot().cluster_epoch();
    assert!(suspect_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&active_authorization, 2_005),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == suspect_epoch
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            suspect_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let node_two = authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, suspect_epoch, 2_007),
            2_007,
        )
        .unwrap();
    assert!(node_two.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            suspect_epoch,
            2_008,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 25,
            state: PgState::Peering,
            ..
        })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, suspect_epoch, 2_009),
            2_009,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > suspect_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(25)).unwrap().state(),
        PgState::Peering
    );

    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, 2_010),
            2_010,
        )
        .unwrap()
        .serving());
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_011);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_012);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert_eq!(
        pg.previous_primary_node_incarnation(),
        Some(node_incarnation(&authority, 1))
    );
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_013);
    assert_eq!(
        authority.complete_ready_pg_peerings(2_013).unwrap(),
        vec![PgId::new(25)],
        "the unchanged primary process must not wait out its own old lease"
    );
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_014);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_015,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn recovering_preferred_replica_does_not_displace_live_previous_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Active, 2_002);

    let recovery = heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_003);
    assert!(
        !recovery.serving(),
        "recovering replica must first observe its availability epoch"
    );
    let recovery_epoch = recovery.cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(26)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_006);

    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_004);
    heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_005);
    assert_eq!(authority.snapshot().cluster_epoch(), recovery_epoch);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(26),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_006,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 26,
            node_id: 2,
        })
    ));
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(2_006)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(
        ready[0].primary,
        NodeId::new(1),
        "the exact previous primary must retain priority while its old lease is live"
    );
    assert_eq!(
        authority.complete_ready_pg_peerings(2_006).unwrap(),
        vec![PgId::new(26)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(26))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(1))
    );
}

#[test]
fn acting_set_reorder_moves_primary_after_previous_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_003);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Active, 2_004);

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    let reordered_epoch = authority.snapshot().cluster_epoch();
    let previous_lease_deadline = authority
        .snapshot()
        .pg(PgId::new(27))
        .unwrap()
        .previous_primary_lease_deadline_ms()
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false)
    );

    drop(authority);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > reordered_epoch);
    let pg = authority.snapshot().pg(PgId::new(27)).unwrap();
    assert_eq!(
        pg.previous_primary_lease_deadline_ms(),
        Some(previous_lease_deadline)
    );
    assert_eq!(
        pg.previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false),
        "acting-set transition provenance must survive authority restart"
    );
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_005);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_006);
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline - 1)
        .unwrap()
        .is_empty());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 27,
            node_id: 1,
        })
    ));
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 27, .. })
    ));

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline)
        .unwrap()
        .is_empty());
    heartbeat_with_pg_observation(
        &mut authority,
        1,
        27,
        PgState::Peering,
        previous_lease_deadline,
    );
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        previous_lease_deadline + 1,
    );
    let successor_fence_ms = previous_lease_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, successor_fence_ms);
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        successor_fence_ms + 1,
    );
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(successor_fence_ms + 1)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].primary, NodeId::new(2));
    assert_eq!(
        authority
            .complete_ready_pg_peerings(successor_fence_ms + 1)
            .unwrap(),
        vec![PgId::new(27)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(2))
    );
}

#[test]
fn acting_set_change_fences_old_primary_token_until_new_peering_completes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(77, 0xabcddcba, 0x12344321);
    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Active,
        active_proof,
        false,
        2_004,
    );
    let old_epoch = active.cluster_epoch();
    let old_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            old_epoch,
            2_005,
        )
        .unwrap();
    authority
        .validate_pg_operation_authorization(&old_authorization, 2_006)
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > old_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(20)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(20))
            .unwrap()
            .peering_metadata_proof_floor(),
        Some(active_proof)
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&old_authorization, 2_007),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == old_epoch && current_epoch == peering_epoch
    ));
    for node_id in [1, 2] {
        let now_ms = 2_008 + u64::from(node_id);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, node_id, peering_epoch, now_ms),
                now_ms,
            )
            .unwrap();
    }
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            peering_epoch,
            2_011,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            peering_epoch,
            2_012,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));

    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 2_013);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 2_013).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_014,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive {
            pg_id: 20,
            lease_deadline_ms: 2_103,
            ..
        })
    ));
    let mut ready_but_fenced = heartbeat_from_record(&authority, 2, peering_epoch, 2_015);
    ready_but_fenced.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(ready_but_fenced, 2_015).unwrap();
    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(2_016)
        .unwrap()
        .is_empty());
    let mut fence_bridge = heartbeat_from_record(&authority, 2, peering_epoch, 2_103);
    fence_bridge.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(fence_bridge, 2_103).unwrap();
    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_103);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 3_103).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_103,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 20,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == active_proof && actual == PgMetadataProof::empty()
    ));

    let mut node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_104);
    node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(node_two_peering, 3_104).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_104,
        )
        .unwrap();
    let mut new_active_heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_105);
    new_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Active,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    let new_active = authority.heartbeat(new_active_heartbeat, 3_105).unwrap();
    let new_epoch = new_active.cluster_epoch();
    assert!(new_epoch > peering_epoch);

    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, new_epoch, 3_106),
            3_106,
        )
        .unwrap();
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            new_epoch,
            3_107,
        ),
        Err(ControlPlaneError::NodeNotPgPrimary {
            pg_id: 20,
            node_id: 1,
            primary_node_id: 2,
            ..
        })
    ));

    let new_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            new_epoch,
            3_108,
        )
        .unwrap();
    assert_eq!(new_authorization.primary_node_id(), NodeId::new(2));
    authority
        .validate_pg_operation_authorization(&new_authorization, 2_109)
        .unwrap();
}

#[test]
fn active_metadata_pg_acting_set_change_requires_authoritative_overlap() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(40), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(40),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(40), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 40 })
    ));
    let pg = authority.snapshot().pg(PgId::new(40)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(pg.active_metadata_proof(), Some(active_proof));
}

#[test]
fn active_metadata_migration_waits_for_source_after_unrelated_epoch_change() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let target_pg_id = PgId::new(40);
    let unrelated_pg_id = PgId::new(41);
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            target_pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(unrelated_pg_id, vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(target_pg_id)
        .is_none());
    let before = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 40,
            cluster_epoch,
        }) if cluster_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_003,
    );

    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let target = authority.snapshot().pg(target_pg_id).unwrap();
    assert_eq!(target.state(), PgState::Peering);
    assert_eq!(target.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(target.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn active_metadata_overlap_migration_does_not_relax_non_primary_imported_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_floor = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            45,
            PgState::Peering,
            imported_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(45),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(45)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        45,
        PgState::Active,
        imported_floor,
        false,
        2_020,
    );
    authority
        .set_pg_acting_set(PgId::new(47), vec![NodeId::new(1)])
        .unwrap();

    let epoch_local_progress = PgMetadataProof::current(
        imported_floor.applied_log_index,
        imported_floor.applied_log_hash + 1,
        imported_floor.state_digest + 1,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        45,
        PgState::Active,
        epoch_local_progress,
        false,
        2_021,
    );
    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(45), vec![NodeId::new(2), NodeId::new(3)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 45 })
    ));
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        45,
        PgState::Active,
        epoch_local_progress,
        false,
        2_022,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
        assert_eq!(pg.active_metadata_proof(), Some(epoch_local_progress));
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(epoch_local_progress)
    );

    let imported_high_index_floor = PgMetadataProof::current(20, 30, 40);
    let primary_destination_progress = PgMetadataProof::current(2, 31, 41);
    authority
        .set_pg_acting_set(PgId::new(46), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            46,
            PgState::Peering,
            imported_high_index_floor,
            false,
            3_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(46),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(46)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        46,
        PgState::Active,
        imported_high_index_floor,
        false,
        3_020,
    );
    authority
        .set_pg_acting_set(PgId::new(48), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        46,
        PgState::Active,
        primary_destination_progress,
        false,
        3_021,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
        assert_eq!(
            pg.active_metadata_proof(),
            Some(primary_destination_progress)
        );
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(
            PgId::new(46),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(primary_destination_progress)
    );
}

#[test]
fn imported_active_primary_restart_preserves_epoch_local_peering_floor() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof::current(42, 100, 200);
    authority
        .set_pg_acting_set(PgId::new(49), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(49)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 1,
        imported_proof.state_digest + 1,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    restarting_primary.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(epoch_local_proof));

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 49, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn imported_active_restart_without_initial_observation_accepts_later_epoch_local_peering_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof::current(42, 100, 200);
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(50)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 1,
        imported_proof.state_digest + 1,
    );
    let active_proof_epoch = authority
        .snapshot()
        .pg(PgId::new(50))
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();
    let active_route_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_route_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(
        pg.peering_metadata_proof_floor_epoch(),
        Some(active_proof_epoch)
    );
    assert!(pg.peering_metadata_proof_floor_imported());

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(50),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 50, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn peering_metadata_pg_acting_set_change_preserves_floor_and_requires_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(41),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(41), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 41 })
    ));
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn pg_transition_graph_survives_file_reopen_and_continues() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(61);
    let initial_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_000, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), None);

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_010, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_011,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        initial_proof,
        false,
        (2_012, 5),
    );

    let expected_overlap_floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch();
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor_epoch(),
        expected_overlap_floor_epoch
    );
    for (node_id, now_ms) in [(1, 2_020), (2, 2_021)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    assert_eq!(
        authority.complete_ready_pg_peerings(2_022).unwrap(),
        vec![pg_id]
    );
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_metadata_proof(), Some(initial_proof));
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    for (node_id, now_ms) in [(1, 2_030), (2, 2_031)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    authority.complete_ready_pg_peerings(2_032).unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    let source_primary = active_pg.active_primary().unwrap();
    let source_proof = active_pg.active_metadata_proof().unwrap();
    let imported_proof = PgMetadataProof::current(
        source_proof.applied_log_index + 1,
        source_proof.applied_log_hash + 100,
        source_proof.state_digest + 100,
    );
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(3)], transfer)
        .unwrap();
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.acting_set(), &[NodeId::new(3)]);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_node_id(),
        Some(source_primary)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor_epoch(),
        Some(active_epoch)
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (2_040, 5),
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (3_035, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_035,
        )
        .unwrap();
    let imported_active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(imported_active_pg.state(), PgState::Active);
    assert_eq!(
        imported_active_pg.active_metadata_proof(),
        Some(imported_proof)
    );
    assert!(imported_active_pg.active_metadata_transfer_imported());
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_import_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_import_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_import_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(restarted_import_pg.peering_metadata_proof_floor_imported());
    assert_eq!(restarted_import_pg.peering_metadata_transfer(), None);
    heartbeat_with_pg_proof(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_050,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_051,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn metadata_transfer_allows_explicit_non_overlap_pg_migration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        (2_000, 3),
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        (2_002, 1),
    );
    let active_epoch = authority.snapshot().cluster_epoch();

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(42), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));

    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        PgMetadataProof::current(
            active_proof.applied_log_index,
            active_proof.applied_log_hash + 1,
            active_proof.state_digest,
        ),
        PgMetadataProof::current(
            active_proof.applied_log_index,
            active_proof.applied_log_hash + 10,
            active_proof.state_digest,
        ),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let future_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let future_transfer = PgMetadataTransferProof::new(future_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            future_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochInFuture { pg_id: 42, .. })
    ));

    let stale_epoch = ClusterEpoch::new(active_epoch.get() - 1).unwrap();
    let stale_epoch_transfer = PgMetadataTransferProof::new(stale_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_epoch_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochStale { pg_id: 42, .. })
    ));

    let imported_proof = PgMetadataProof::current(
        active_proof.applied_log_index,
        active_proof.applied_log_hash + 100,
        active_proof.state_digest,
    );
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let snapshot_without_transfer = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(1)],
            transfer,
            next_epoch(active_epoch).unwrap(),
        ),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));
    assert_eq!(authority.snapshot(), &snapshot_without_transfer);
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
    assert!(!pg.metadata_transfer_fenced());
    let mismatched_same_acting_set = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        PgMetadataProof::current(
            imported_proof.applied_log_index,
            imported_proof.applied_log_hash + 1,
            imported_proof.state_digest,
        ),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            mismatched_same_acting_set,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofMismatch { pg_id: 42, .. })
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    let mismatched_destination_epoch = next_epoch(peering_epoch).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            mismatched_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 42,
            expected_destination_epoch,
            actual_destination_epoch,
        }) if expected_destination_epoch == mismatched_destination_epoch
            && actual_destination_epoch == peering_epoch
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            peering_epoch,
        )
        .unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    let transfer_epoch = authority.snapshot().cluster_epoch();
    assert_eq!(
        authority
            .fence_pg_for_metadata_transfer(PgId::new(42))
            .unwrap()
            .cluster_epoch(),
        transfer_epoch
    );
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert!(!pg.metadata_transfer_fenced());

    let restarted = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-first-restart.state"),
    );
    let restarted_pg = restarted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), Some(transfer));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        3_003,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_003,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            ..
        }) if cluster_epoch == peering_epoch
    ));

    let stale_source_above_imported = PgMetadataProof::current(
        imported_proof.applied_log_index + 10,
        imported_proof.applied_log_hash + 10,
        imported_proof.state_digest + 10,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        stale_source_above_imported,
        false,
        3_004,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_004,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            expected,
            actual,
            ..
        }) if cluster_epoch == peering_epoch
            && expected == imported_proof
            && actual == stale_source_above_imported
    ));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        3_005,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_006,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(2)));
    assert_eq!(active_pg.active_metadata_proof(), Some(imported_proof));
    assert!(active_pg.active_metadata_transfer_imported());
    assert_eq!(active_pg.peering_metadata_proof_floor(), None);
    assert_eq!(active_pg.peering_metadata_transfer(), None);

    let restarted_active = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-active-restart.state"),
    );
    let restarted_active_pg = restarted_active.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(restarted_active_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_active_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_active_pg.peering_metadata_transfer(), None);
    assert!(!restarted_active_pg.metadata_transfer_fenced());
    assert!(!restarted_active_pg.active_metadata_transfer_imported());

    let epoch_local_source_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 200,
        imported_proof.state_digest + 1,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Active,
        imported_proof,
        false,
        2_007,
    );
    let repeated_source_epoch = authority.snapshot().cluster_epoch();
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(fenced_pg.state(), PgState::Peering);
    assert!(fenced_pg.metadata_transfer_fence_source_imported);
    assert_eq!(
        fenced_pg.metadata_transfer_fence_epoch(),
        Some(fenced_epoch)
    );
    assert_eq!(
        fenced_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    let repeated_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        repeated_source_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(
            epoch_local_source_proof.applied_log_index,
            epoch_local_source_proof.applied_log_hash + 100,
            epoch_local_source_proof.state_digest,
        ),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(1)],
            repeated_transfer,
        )
        .unwrap();
    let repeated_transfer_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(repeated_transfer_pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        repeated_transfer_pg.peering_metadata_transfer(),
        Some(repeated_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_retry_without_stored_deadline_uses_max_source_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Active,
        active_proof,
        false,
        2_020,
    );

    authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();
    let record = authority
        .snapshot
        .pgs
        .get_mut(&PgId::new(50))
        .expect("test PG should exist");
    assert!(record.metadata_transfer_fenced);
    record.metadata_transfer_fence_source_lease_deadline_ms = None;
    persist_manually_modified_test_snapshot(&mut authority);

    let retry = authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();

    assert_eq!(retry.source_primary_lease_deadline_ms(), Some(2_120));
}

#[test]
fn fencing_pending_recovery_peering_preserves_imported_source_provenance() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(52);
    let source_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        source_proof,
        false,
        2_002,
    );

    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let imported_proof = PgMetadataProof::current(3, 20, 21);
    let first_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        authority.snapshot().cluster_epoch(),
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], first_transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        2_010,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_200,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_201,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_transfer_imported());

    let local_progress = PgMetadataProof::current(2, 30, 31);
    let pending = test_pending_metadata_command(active_epoch);
    authority
        .set_pg_acting_set(PgId::new(54), vec![NodeId::new(1)])
        .unwrap();
    let report_epoch = authority.snapshot().cluster_epoch();
    let mut pending_heartbeat = heartbeat_from_record(&authority, 2, report_epoch, 3_220);
    pending_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            active_epoch,
            pg_id,
        )]);
    pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Active,
        metadata_proof: local_progress,
        pending_metadata_command: Some(pending),
    }];
    authority.heartbeat(pending_heartbeat, 3_220).unwrap();
    let recovery_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(recovering.state(), PgState::Peering);
    assert_eq!(
        recovering.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(recovering.peering_metadata_proof_floor_imported());

    let mut cleared_heartbeat = heartbeat_from_record(&authority, 2, recovery_epoch, 3_221);
    cleared_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Peering,
        metadata_proof: local_progress,
        pending_metadata_command: None,
    }];
    authority.heartbeat(cleared_heartbeat, 3_221).unwrap();
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fenced = authority.snapshot().pg(pg_id).unwrap();
    assert!(fenced.metadata_transfer_fence_source_imported);

    let stale_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let stale_second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof::current(
            2,
            stale_destination_epoch.get(),
            local_progress.state_digest,
        ),
    );
    authority
        .set_pg_acting_set(PgId::new(53), vec![NodeId::new(1)])
        .unwrap();
    let snapshot_after_unrelated_advance = authority.snapshot().clone();
    let actual_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            stale_second_transfer,
            stale_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 52,
            expected_destination_epoch,
            actual_destination_epoch: actual,
        }) if expected_destination_epoch == stale_destination_epoch
            && actual == actual_destination_epoch
    ));
    assert_eq!(authority.snapshot(), &snapshot_after_unrelated_advance);

    let second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof::current(
            2,
            actual_destination_epoch.get(),
            local_progress.state_digest,
        ),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            second_transfer,
            actual_destination_epoch,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().cluster_epoch(),
        actual_destination_epoch
    );
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        transferred.peering_metadata_transfer(),
        Some(second_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_rejects_epoch_local_source_proof_without_imported_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_activation_floor = PgMetadataProof::current(9, 10, 11);
    let epoch_local_source_proof = PgMetadataProof::current(2, 12, 13);
    for (idx, pg_id) in [42, 43].into_iter().enumerate() {
        let base_ms = 1_990 + (idx as u64 * 100);
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Peering,
            imported_activation_floor,
            false,
            base_ms + 10,
        );
        authority
            .complete_pg_peering(
                PgId::new(pg_id),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                base_ms + 20,
            )
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Active,
            imported_activation_floor,
            false,
            base_ms + 30,
        );
    }

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        imported_activation_floor,
        false,
        2_180,
    );
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    assert!(
        !authority
            .snapshot()
            .pg(PgId::new(42))
            .unwrap()
            .metadata_transfer_fence_source_imported
    );
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(3, 14, 15),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            fenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    authority
        .set_pg_state(PgId::new(43), PgState::Peering)
        .unwrap();
    let unfenced_epoch = authority.snapshot().cluster_epoch();
    let unfenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        unfenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(3, 16, 17),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(43),
            vec![NodeId::new(2)],
            unfenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 43, .. })
    ));
}

#[test]
fn fenced_metadata_transfer_accepts_later_prefence_epoch_local_source_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(42);
    let floor = PgMetadataProof::current(3, 9_745, 14_796);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        floor,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_002,
    );
    let floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    assert!(source_epoch > floor_epoch);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_003,
    );
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fence_epoch = authority.snapshot().cluster_epoch();
    assert!(fence_epoch > source_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );

    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    let post_fence_epoch = authority.snapshot().cluster_epoch();
    assert!(post_fence_epoch > fence_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);

    let source_proof = PgMetadataProof::current(2, 71_284, 19_648);
    let imported_proof = PgMetadataProof::current(
        source_proof.applied_log_index,
        82_951,
        source_proof.state_digest,
    );
    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        floor_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let at_fence_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fence_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            at_fence_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], transfer)
        .unwrap();
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.state(), PgState::Peering);
    assert_eq!(transferred.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transferred.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    let reopened = reopen_file_authority(&store);
    assert_eq!(
        reopened
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .peering_metadata_transfer(),
        Some(transfer)
    );
}

#[test]
fn fenced_metadata_transfer_accepts_prefence_source_epoch_with_floor_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let source_proof = PgMetadataProof::current(3, 9_474, 15_725);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        source_proof,
        false,
        2_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();

    authority
        .fence_pg_for_metadata_transfer(PgId::new(44))
        .unwrap();
    assert!(source_epoch < authority.snapshot().cluster_epoch());
    let imported_proof = PgMetadataProof::current(3, 49_281, source_proof.state_digest);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(44), vec![NodeId::new(2)], transfer)
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(44)).unwrap();
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
}

#[test]
fn complete_pg_peering_requires_every_acting_node_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_001).serving());
    authority
        .set_pg_acting_set(PgId::new(39), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    assert!(heartbeat_until_serving(&mut authority, 2, 1_999).serving());
    heartbeat_with_pg_observation(&mut authority, 1, 39, PgState::Peering, 2_000);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 39,
            node_id: 2,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 2, 39, PgState::Peering, 2_002);
    authority
        .complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_003,
        )
        .unwrap();
}

#[test]
fn acting_set_change_discards_stale_peering_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(38), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let first_peering_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_observation(&mut authority, 1, 38, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 38, PgState::Peering, 2_001);

    authority
        .set_pg_acting_set(
            PgId::new(38),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let changed_epoch = authority.snapshot().cluster_epoch();
    assert!(changed_epoch > first_peering_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == changed_epoch
    ));
    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, changed_epoch, 2_003),
            2_003,
        )
        .unwrap()
        .serving());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 38,
            cluster_epoch,
            ..
        }) if cluster_epoch == changed_epoch
    ));

    for (node_id, now_ms) in [(1, 2_005), (2, 2_006), (3, 2_007)] {
        heartbeat_with_pg_observation(&mut authority, node_id, 38, PgState::Peering, now_ms);
    }
    authority
        .complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_008,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn membership_change_to_joining_forces_active_pg_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_003,
        )
        .unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Joining)
        .unwrap();
    let joining_epoch = authority.snapshot().cluster_epoch();
    assert!(joining_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(23)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 2_004),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == joining_epoch
    ));

    let joining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, joining_epoch, 2_005),
            2_005,
        )
        .unwrap();
    assert!(!joining_lease.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            joining_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == joining_epoch
    ));
}

#[test]
fn membership_change_to_draining_forces_repeering_before_service() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Draining)
        .unwrap();
    let draining_epoch = authority.snapshot().cluster_epoch();
    assert!(draining_epoch > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(24)).unwrap().state(),
        PgState::Peering
    );
    let draining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, draining_epoch, 2_003),
            2_003,
        )
        .unwrap();
    assert!(
        draining_lease.serving(),
        "draining nodes can still serve after observing the new map"
    );
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            draining_epoch,
            2_004,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 24,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_005);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_006,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_007);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_008,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn failed_expiry_persist_does_not_expose_uncommitted_epoch_or_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(12), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 12, 1_000).serving());
    let committed = authority.snapshot().clone();
    assert_eq!(
        committed.node(NodeId::new(12)).unwrap().availability(),
        NodeAvailabilityState::Healthy
    );

    let failing_store = FailingStore::new(committed.clone());
    let mut restarted = SingleAuthorityControlPlane::open(failing_store).unwrap();
    assert!(restarted
        .heartbeat(
            heartbeat_from_record(&restarted, 12, restarted.snapshot().cluster_epoch(), 1_001,),
            1_001
        )
        .unwrap()
        .serving());
    let visible_before_failure = restarted.snapshot().clone();
    restarted.store.fail_saves();
    assert!(matches!(
        restarted.expire_heartbeat_leases(1_101),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "test save failure"
    ));
    assert_eq!(restarted.snapshot(), &visible_before_failure);
    assert_eq!(
        restarted.deterministic_pg_primary(PgId::new(1), &[NodeId::new(12)], 1_001),
        Some(NodeId::new(12))
    );
}

#[test]
fn heartbeat_rejects_unknown_removed_and_zero_duration_nodes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, ClusterEpoch::INITIAL, 1), 1),
        Err(ControlPlaneError::UnknownNode { node_id: 9 })
    ));

    authority
        .set_node_membership(NodeId::new(9), NodeMembershipState::Removed)
        .unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, authority.snapshot().cluster_epoch(), 2), 2),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 9, .. })
    ));

    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();
    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = 0;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::InvalidLeaseDuration)
    ));
}

#[test]
fn heartbeat_rejects_overlong_lease_duration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();

    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::LeaseDurationTooLong {
            requested_ms,
            max_ms,
        }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
            && max_ms == MAX_HEARTBEAT_LEASE_MS
    ));
}

#[test]
fn primary_selection_requires_unexpired_authority_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(42), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 42, 1_000);
    assert!(serving.serving());
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms() - 1,
        ),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms(),
        ),
        None
    );

    authority
        .set_pg_acting_set(PgId::new(21), vec![NodeId::new(42)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Peering, 1_010);
    authority
        .complete_pg_peering(
            PgId::new(21),
            NodeId::new(42),
            node_incarnation(&authority, 42),
            1_020,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Active, 1_030);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms() - 1),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms()),
        None
    );
}

#[test]
fn removed_nodes_cannot_rejoin_or_be_marked_healthy() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Removed)
        .unwrap();

    assert!(matches!(
        authority.set_node_membership(NodeId::new(11), NodeMembershipState::Active),
        Err(ControlPlaneError::RemovedNodeCannotRejoin { node_id: 11 })
    ));
    assert!(matches!(
        authority.mark_node_availability(NodeId::new(11), NodeAvailabilityState::Healthy),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 11, .. })
    ));
}
