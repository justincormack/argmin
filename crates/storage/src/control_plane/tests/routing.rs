// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::Condvar;

#[test]
fn complete_pg_peering_command_replays_with_committed_completion_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 8, PgState::Peering, 1_010);

    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::CompletePgPeering {
            pg_id: PgId::new(8),
            primary: NodeId::new(1),
            node_incarnation: node_incarnation(&authority, 1),
            complete_at_ms: 1_011,
        })
        .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::CompletePgPeering
    );
    let pg = applied.snapshot().pg(PgId::new(8)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(
        applied.snapshot().cluster_epoch(),
        ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap()
    );
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(1_011));
}

#[test]
fn active_pg_primary_comes_from_authoritative_acting_set() {
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
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);

    assert!(matches!(
        authority.set_pg_state(PgId::new(8), PgState::Active),
        Err(ControlPlaneError::ActivePgRequiresPeeringComplete { pg_id: 8 })
    ));
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            8,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(8),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_050,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 8,
            node_id: 2
        })
    ));
    assert!(matches!(
        authority.complete_pg_peering(PgId::new(8), NodeId::new(99), 99, 2_050),
        Err(ControlPlaneError::PgPrimaryNotInActingSet {
            pg_id: 8,
            node_id: 99
        })
    ));
    authority
        .complete_pg_peering(
            PgId::new(8),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 2_050), None);
    heartbeat_with_pg_observation(&mut authority, 1, 8, PgState::Active, 3_001);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
            3_002,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(8), 3_002),
        Some(NodeId::new(1))
    );
}

#[test]
fn heartbeat_refresh_completes_ready_peering_for_storage_node_before_frontend_export() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(peering_heartbeat, 2_000)
        .unwrap();

    let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(
        refresh.lease().cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert!(
        !refresh.lease().serving(),
        "peering completion bumps the epoch before the node observes it"
    );
    let storage_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(22))
        .unwrap();
    assert_eq!(storage_route.state(), PgState::Active);
    assert!(matches!(
        authority.snapshot().runtime_map(2_001),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 22, .. })
    ));

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_002);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(active_heartbeat, 2_002)
        .unwrap();
    assert!(refresh.lease().serving());
    let frontend_map = authority.snapshot().runtime_map(2_003).unwrap();
    assert_eq!(
        frontend_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(22))
            .unwrap()
            .state(),
        PgState::Active
    );
}

#[test]
fn storage_node_refresh_hands_active_route_to_primary_after_non_primary_completes_peering() {
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
        .set_pg_acting_set(PgId::new(77), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let proof = PgMetadataProof::current(42, 0xabc, 0xdef);

    for node_id in [1, 2] {
        let now_ms = 2_000 + u64::from(node_id);
        let mut heartbeat = heartbeat_from_record(&authority, node_id, peering_epoch, now_ms);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(77),
            state: PgState::Peering,
            metadata_proof: proof,
            pending_metadata_command: None,
        }];
        if node_id == 1 {
            authority.heartbeat(heartbeat, now_ms).unwrap();
        } else {
            let non_primary_handoff = authority.refresh_node_heartbeat(heartbeat, now_ms).unwrap();
            let route = non_primary_handoff
                .runtime_map()
                .pg_routes()
                .iter()
                .find(|route| route.pg_id() == PgId::new(77))
                .unwrap();
            assert_eq!(route.state(), PgState::Active);
            assert_eq!(route.primary_node_id(), NodeId::new(1));
            assert_eq!(route.primary_lease_deadline_ms(), None);
        }
    }
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(active_epoch > peering_epoch);
    let active_pg = authority.snapshot().pg(PgId::new(77)).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert!(matches!(
        authority.snapshot().runtime_map(2_003),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 77, .. })
    ));

    let stale_primary_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_004);
    let primary_handoff = authority
        .refresh_node_heartbeat(stale_primary_heartbeat, 2_004)
        .unwrap();
    assert!(
        !primary_handoff.lease().serving(),
        "the primary still has to observe the new epoch before serving"
    );
    let route = primary_handoff
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(77))
        .unwrap();
    assert_eq!(route.cluster_epoch(), active_epoch);
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.primary_node_id(), NodeId::new(1));

    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_005);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(77),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    let active_refresh = authority
        .refresh_node_heartbeat(active_heartbeat, 2_005)
        .unwrap();
    assert!(active_refresh.lease().serving());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(77), 2_006),
        Some(NodeId::new(1))
    );
    assert!(authority.snapshot().runtime_map(2_006).is_ok());
}

#[test]
fn storage_node_refresh_recovers_when_another_active_primary_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    for (pg_id, node_id, now_ms) in [(80, 1, 2_000), (81, 2, 2_100)] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(node_id)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, node_id, pg_id, PgState::Peering, now_ms);
        authority
            .complete_pg_peering(
                PgId::new(pg_id),
                NodeId::new(node_id),
                node_incarnation(&authority, node_id),
                now_ms + 1,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, node_id, pg_id, PgState::Active, now_ms + 2);
    }

    let active_epoch = authority.snapshot().cluster_epoch();
    for (pg_id, node_id, now_ms) in [(80, 1, 3_000), (81, 2, 3_001)] {
        let mut heartbeat = heartbeat_from_record(&authority, node_id, active_epoch, now_ms);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(pg_id),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap();
    }

    let mut node_2_heartbeat = heartbeat_from_record(&authority, 2, active_epoch, 3_200);
    node_2_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(81),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let node_2_refresh = authority
        .refresh_node_heartbeat(node_2_heartbeat, 3_200)
        .expect("one expired primary must not prevent another node from refreshing");
    let unavailable_route = node_2_refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(80))
        .unwrap();
    assert_eq!(unavailable_route.state(), PgState::Active);
    assert_eq!(unavailable_route.primary_node_id(), NodeId::new(1));
    assert_eq!(unavailable_route.primary_lease_deadline_ms(), None);

    let mut node_1_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 3_201);
    node_1_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(80),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let node_1_refresh = authority
        .refresh_node_heartbeat(node_1_heartbeat, 3_201)
        .expect("the expired primary must be able to renew after receiving the current map");
    assert!(node_1_refresh.lease().serving());
    assert!(authority.snapshot().runtime_map(3_202).is_ok());
}

#[test]
fn storage_node_refresh_does_not_block_non_actor_on_pending_active_handoff() {
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
        .set_pg_acting_set(PgId::new(70), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 70, PgState::Peering, 1_010);
    authority
        .complete_pg_peering(
            PgId::new(70),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_011,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 70, PgState::Active, 1_012);
    authority
        .set_pg_acting_set(PgId::new(71), vec![NodeId::new(2)])
        .unwrap();
    let mut next_snapshot = authority.snapshot().clone();
    {
        let pg = next_snapshot.pgs.get_mut(&PgId::new(71)).unwrap();
        pg.state = PgState::Active;
        pg.active_primary = Some(NodeId::new(2));
        pg.active_metadata_proof = Some(PgMetadataProof::empty());
        pg.active_metadata_proof_epoch = Some(authority.snapshot().cluster_epoch());
    }
    next_snapshot.bump_epoch().unwrap();
    authority.commit_snapshot(next_snapshot).unwrap();

    let active_epoch = authority.snapshot().cluster_epoch();
    let pg_71 = authority.snapshot().pg(PgId::new(71)).unwrap();
    assert_eq!(pg_71.state(), PgState::Active);
    assert_eq!(pg_71.active_primary(), Some(NodeId::new(2)));
    assert!(authority.snapshot().runtime_map(1_013).is_err());

    let mut unrelated_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 1_014);
    unrelated_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(70),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(unrelated_heartbeat, 1_014)
        .unwrap();
    let unrelated_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(70))
        .unwrap();
    assert_eq!(unrelated_route.state(), PgState::Active);
    let pending_handoff_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(71))
        .unwrap();
    assert_eq!(pending_handoff_route.state(), PgState::Active);
    assert_eq!(pending_handoff_route.primary_node_id(), NodeId::new(2));
    assert_eq!(pending_handoff_route.primary_lease_deadline_ms(), None);
}

#[test]
fn active_pg_primary_is_bound_by_peering_completion() {
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
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            20,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(20))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(1))
    );

    heartbeat_with_pg_observation(&mut authority, 2, 20, PgState::Active, 3_001);
    assert_eq!(authority.serving_pg_primary(PgId::new(20), 3_001), None);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_002,
        ),
        Err(ControlPlaneError::PgNotPeering {
            pg_id: 20,
            state: PgState::Active,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 20, PgState::Active, 3_003);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(20), 3_003),
        Some(NodeId::new(1))
    );
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_004,
        )
        .unwrap();
}

#[test]
fn active_pg_route_requires_bound_primary_and_active_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(21), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(21),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();

    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(21), 2_002),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 21, .. })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Active, 2_003);
    let route = authority
        .snapshot()
        .active_pg_route(PgId::new(21), 2_004)
        .unwrap();
    assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
    assert_eq!(route.pg_id(), PgId::new(21));
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), &[NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(
        route.primary_lease_deadline_ms(),
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
    );
    assert_eq!(
        authority.snapshot().active_pg_routes(2_004).unwrap(),
        vec![route.clone()]
    );

    let local_route = crate::cluster::LocalPgRoute::from(&route);
    assert_eq!(local_route.cluster_epoch(), route.cluster_epoch());
    assert_eq!(local_route.pg_id(), route.pg_id());
    assert_eq!(local_route.primary_node_id(), route.primary_node_id());
    assert_eq!(local_route.acting_set(), route.acting_set());
    assert_eq!(local_route.state(), route.state());

    let storage_node_route = crate::storage_node_server::StorageNodePgRoute::from(&route);
    assert_eq!(storage_node_route.cluster_epoch, route.cluster_epoch());
    assert_eq!(storage_node_route.pg_id, route.pg_id().get());
    assert_eq!(storage_node_route.primary_node_id, route.primary_node_id());
    assert_eq!(storage_node_route.acting_set, route.acting_set());
    assert_eq!(storage_node_route.state, route.state());
}

#[test]
fn active_primary_service_requires_current_observation_metadata_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let accepted_proof = PgMetadataProof::current(42, 0xabc, 0xdef);
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(22), 2_030)
        .is_ok());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(22), 2_030),
        Some(NodeId::new(1))
    );
    assert!(authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_030,
        )
        .is_ok());

    let progressed_proof = PgMetadataProof::current(43, 0xabd, 0xdf0);
    let mut progressed_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_031);
    progressed_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(progressed_heartbeat, 2_031).unwrap();

    assert_eq!(
        authority.serving_pg_primary(PgId::new(22), 2_031),
        Some(NodeId::new(1))
    );
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(22), 2_031)
        .is_ok());
    assert!(authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_031, NodeId::new(1), ClusterEpoch::INITIAL)
        .is_ok());
    assert!(authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_031,
        )
        .is_ok());

    let mismatched_proof = PgMetadataProof::current(43, 0xabe, 0xdf1);
    authority
        .snapshot
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .pg_observations
        .get_mut(&PgId::new(22))
        .unwrap()
        .metadata_proof = mismatched_proof;

    assert_eq!(authority.serving_pg_primary(PgId::new(22), 2_032), None);
    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(22), 2_032),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
    assert!(matches!(
        authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(2_032, NodeId::new(1), ClusterEpoch::INITIAL),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_032,
        ),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
}

#[test]
fn active_primary_heartbeat_does_not_promote_digest_only_cleanup_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let accepted_proof = PgMetadataProof::current(42, 0xabc, 0xdef);
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    let accepted_proof_epoch = authority
        .snapshot()
        .pg(PgId::new(22))
        .unwrap()
        .active_metadata_proof_epoch();

    let cleanup_proof = PgMetadataProof::current(
        accepted_proof.applied_log_index,
        accepted_proof.applied_log_hash,
        0xdf0,
    );
    let mut cleanup_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_030);
    cleanup_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: cleanup_proof,
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(cleanup_heartbeat, 2_030),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == accepted_proof && actual == cleanup_proof
    ));

    let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), accepted_proof_epoch);
    assert!(authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_031, NodeId::new(1), ClusterEpoch::INITIAL)
        .is_ok());
}

#[test]
fn non_primary_active_observation_cannot_satisfy_primary_active_proof() {
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
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let accepted_proof = PgMetadataProof::current(42, 0xabc, 0xdef);
    for node_id in [1, 2] {
        let mut peering_heartbeat = heartbeat_from_record(
            &authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            2_000 + u64::from(node_id),
        );
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(23),
            state: PgState::Peering,
            metadata_proof: accepted_proof,
            pending_metadata_command: None,
        }];
        authority
            .heartbeat(peering_heartbeat, 2_000 + u64::from(node_id))
            .unwrap();
    }
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let progressed_proof = PgMetadataProof::current(43, 0xabd, 0xdf0);
    let mut non_primary_active =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_020);
    non_primary_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(23),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(non_primary_active, 2_020).unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(23), 2_030), None);
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(23), 2_030)
        .is_err());
    let non_primary_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_030, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();
    let route = non_primary_runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(23))
        .unwrap();
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.primary_lease_deadline_ms(), None);
}

#[test]
fn non_primary_active_observation_may_lag_active_primary_metadata_proof() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(path.clone());
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(&mut authority, node_id, 24, PgState::Peering, 2_000);
    }
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let accepted_proof = PgMetadataProof::current(2, 0xabc, 0xdef);
    let stale_proof = PgMetadataProof::empty();
    let mut next_snapshot = authority.snapshot().clone();
    {
        let pg = next_snapshot.pgs.get_mut(&PgId::new(24)).unwrap();
        pg.active_metadata_proof = Some(accepted_proof);
    }
    let active_epoch = next_snapshot.cluster_epoch();
    for node_id in [1, 2] {
        let node = next_snapshot.nodes.get_mut(&NodeId::new(node_id)).unwrap();
        node.last_observed_epoch = Some(active_epoch);
        node.last_heartbeat_ms = Some(2_011);
        node.lease_deadline_ms = Some(3_011);
        node.pg_observations.insert(
            PgId::new(24),
            NodePgObservationRecord {
                pg_id: PgId::new(24),
                state: PgState::Active,
                observed_epoch: active_epoch,
                observed_at_ms: 2_011,
                metadata_proof: if node_id == 1 {
                    accepted_proof
                } else {
                    stale_proof
                },
                pending_metadata_command: None,
            },
        );
    }
    authority.commit_snapshot(next_snapshot).unwrap();

    let mut stale_replica_heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_020);
    stale_replica_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(24),
        state: PgState::Active,
        metadata_proof: stale_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_replica_heartbeat, 2_020).unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(24), 2_021),
        Some(NodeId::new(1))
    );
    assert!(authority.snapshot().runtime_map(2_021).is_ok());
}

#[test]
fn active_pg_route_fails_closed_for_peering_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(22), 1_001),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 22,
            state: PgState::Peering,
            ..
        })
    ));
    assert!(authority
        .snapshot()
        .active_pg_routes(1_001)
        .unwrap()
        .is_empty());
}

#[test]
fn pg_route_exports_peering_pg_for_fail_closed_runtime_install() {
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
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let route = authority.snapshot().pg_route(PgId::new(24), 1_001).unwrap();
    assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
    assert_eq!(route.pg_id(), PgId::new(24));
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    assert_eq!(
        authority.snapshot().pg_routes(1_001).unwrap(),
        vec![route.clone()]
    );

    let local_route = crate::cluster::LocalPgRoute::from(&route);
    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2)],
        &[24],
        crate::EcShape { k: 1, m: 1 },
        authority.snapshot().cluster_epoch(),
        vec![local_route],
    )
    .unwrap();
    assert!(matches!(
        local_map.metadata_pg_primary_node(authority.snapshot().cluster_epoch(), PgId::new(24)),
        Err(crate::StoreError::PgNotActive {
            pg_id: 24,
            state: PgState::Peering,
            ..
        })
    ));
}

#[test]
fn pg_route_certifies_only_exact_committed_peering_metadata_proof() {
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
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let committed_proof = PgMetadataProof::current(7, 0xabc, 0xdef);
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            24,
            PgState::Peering,
            committed_proof,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        24,
        PgState::Active,
        committed_proof,
        false,
        2_011,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Active,
        committed_proof,
        false,
        2_012,
    );

    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        committed_proof,
        false,
        2_020,
    );
    let route = authority.snapshot().pg_route(PgId::new(24), 2_021).unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(
        route.metadata_read_route(),
        Some(PgMetadataReadRoute::new(NodeId::new(2), committed_proof))
    );
    let local_route = crate::cluster::LocalPgRoute::from(&route);
    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2)],
        &[24],
        crate::EcShape { k: 1, m: 1 },
        authority.snapshot().cluster_epoch(),
        vec![local_route],
    )
    .unwrap();
    assert_eq!(
        local_map
            .metadata_pg_read_node(authority.snapshot().cluster_epoch(), PgId::new(24))
            .unwrap()
            .node_id(),
        NodeId::new(2)
    );
    assert!(local_map
        .metadata_pg_primary_node(authority.snapshot().cluster_epoch(), PgId::new(24))
        .is_err());

    let ahead_proof = PgMetadataProof::current(
        committed_proof.applied_log_index + 1,
        committed_proof.applied_log_hash + 1,
        committed_proof.state_digest + 1,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        ahead_proof,
        false,
        2_022,
    );
    assert_eq!(
        authority
            .snapshot()
            .pg_route(PgId::new(24), 2_023)
            .unwrap()
            .metadata_read_route(),
        None,
        "uncertified progress beyond the committed floor must not become read authority"
    );

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        2_024,
    );
    assert_eq!(
        authority
            .snapshot()
            .pg_route(PgId::new(24), 2_025)
            .unwrap()
            .metadata_read_route(),
        None,
        "a replica below the committed floor must not be certified"
    );

    let mut pending_snapshot = authority.snapshot().clone();
    let pending_epoch = pending_snapshot.cluster_epoch();
    let pending_observation = pending_snapshot
        .nodes
        .get_mut(&NodeId::new(2))
        .unwrap()
        .pg_observations
        .get_mut(&PgId::new(24))
        .unwrap();
    pending_observation.metadata_proof = committed_proof;
    pending_observation.pending_metadata_command =
        Some(test_pending_metadata_command(pending_epoch));
    assert_eq!(
        peering_metadata_read_route_for_snapshot(
            &pending_snapshot,
            pending_snapshot.pg(PgId::new(24)).unwrap(),
            2_027,
        ),
        None,
        "a pending command makes the replica's visible state ambiguous"
    );
}

#[test]
fn peering_metadata_read_certification_requires_exact_expected_proof() {
    let floor_proof = PgMetadataProof::current(7, 0xabc, 0xdef);
    let floor = PeeringMetadataProofFloor {
        proof: floor_proof,
        epoch: Some(ClusterEpoch::new(4).unwrap()),
        imported: false,
    };
    let ahead_proof = PgMetadataProof::current(8, 0x123, 0x456);
    assert!(peering_metadata_proof_is_read_certified(
        floor,
        None,
        floor_proof
    ));
    assert!(!peering_metadata_proof_is_read_certified(
        floor,
        None,
        ahead_proof
    ));

    let imported_proof = PgMetadataProof::current(9, 0x789, 0xabc);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        ClusterEpoch::new(3).unwrap(),
        floor_proof,
        imported_proof,
    );
    assert!(peering_metadata_proof_is_read_certified(
        floor,
        Some(transfer),
        imported_proof
    ));
    assert!(!peering_metadata_proof_is_read_certified(
        floor,
        Some(transfer),
        floor_proof
    ));
}

#[test]
fn runtime_map_exports_pg_routes_with_routed_node_endpoints() {
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
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    assert_eq!(
        runtime_map.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(1_001 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    let route = &runtime_map.pg_routes()[0];
    assert_eq!(route.pg_id(), PgId::new(25));
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(
        runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(1), NodeId::new(2)]
    );
    assert_eq!(runtime_map.nodes()[0].endpoint(), "node-1.sock");
    assert_eq!(runtime_map.nodes()[1].endpoint(), "node-2.sock");
    assert_eq!(
        runtime_map.nodes()[0].node_incarnation(),
        node_incarnation(&authority, 1)
    );
}

#[test]
fn pg_runtime_map_snapshot_uses_bounded_non_serving_validity() {
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
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let runtime_map =
        ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(&authority, PgId::new(25), 1_234)
            .unwrap();

    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(1_234 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
    assert_eq!(runtime_map.pg_routes()[0].primary_lease_deadline_ms(), None);
}

#[test]
fn serving_pg_runtime_map_ignores_unrelated_unserved_pg() {
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
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2)])
        .unwrap();
    authority
        .submit_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(2),
                node_incarnation: node_incarnation(&authority, 2),
                endpoint: "node-2.sock".to_owned(),
                observed_epoch: authority.snapshot().cluster_epoch(),
                requested_lease_duration_ms: 1_000,
                cluster_map_history_route_references: Default::default(),
                pg_observations: vec![NodePgHeartbeatObservation {
                    pg_id: PgId::new(26),
                    state: PgState::Peering,
                    metadata_proof: PgMetadataProof::empty(),
                    pending_metadata_command: None,
                }],
            },
            1_030,
        )
        .unwrap();
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            1_050,
        )
        .unwrap();

    assert!(authority.runtime_map_snapshot(1_051).is_err());
    let scoped = authority
        .serving_pg_runtime_map_snapshot(PgId::new(25), 1_051)
        .unwrap();

    assert_eq!(scoped.pg_routes().len(), 1);
    assert_eq!(scoped.pg_routes()[0].pg_id(), PgId::new(25));
    assert_eq!(scoped.pg_routes()[0].state(), PgState::Peering);
    assert!(scoped.freshness_proof().is_serving_authority_read());
    assert_eq!(
        scoped.valid_until_ms(),
        Some(1_051 + MAX_HEARTBEAT_LEASE_MS)
    );
}

#[test]
fn runtime_map_exports_retained_historical_pg_routes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 2_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(2_001).unwrap();
    assert_eq!(
        runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(1), NodeId::new(2)]
    );
    let historical = runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(26), source_epoch)
        .unwrap();
    assert_eq!(historical.cluster_epoch(), source_epoch);
    assert_eq!(historical.pg_id(), PgId::new(26));
    assert_eq!(historical.primary_node_id(), NodeId::new(1));
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);
    assert_eq!(historical.primary_lease_deadline_ms(), None);

    let current = runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(26), runtime_map.cluster_epoch())
        .unwrap();
    assert_eq!(current.acting_set(), &[NodeId::new(2)]);
    assert_eq!(current.primary_lease_deadline_ms(), None);
}

#[test]
fn storage_node_refresh_filters_history_but_preserves_transfer_source_route() {
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
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(3)])
        .unwrap();
    let unrelated_history_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();

    let source_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        30,
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(30),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        30,
        PgState::Active,
        source_proof,
        false,
        2_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        PgMetadataProof::current(10, 11, 12),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(30), vec![NodeId::new(2)], transfer)
        .unwrap();

    let full_runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    assert!(full_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(31), unrelated_history_epoch)
        .is_ok());
    assert!(full_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .is_ok());

    let filtered_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_003, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(filtered_runtime_map.historical_pg_routes().len(), 1);
    let source_route = filtered_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(source_route.primary_node_id(), NodeId::new(1));
    assert!(matches!(
        filtered_runtime_map
            .reconstructed_pg_route_at_epoch(PgId::new(31), unrelated_history_epoch,),
        Err(ControlPlaneError::UnknownClusterMapEpoch { .. })
    ));
}

#[test]
fn storage_node_refresh_filters_unrelated_history_around_exact_old_route() {
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
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(30),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for raw_pg_id in 100..120 {
        authority
            .set_pg_acting_set(PgId::new(raw_pg_id), vec![NodeId::new(2)])
            .unwrap();
    }
    for round in 0..20 {
        let node_id = if round % 2 == 0 {
            NodeId::new(3)
        } else {
            NodeId::new(2)
        };
        for raw_pg_id in 100..120 {
            authority
                .set_pg_acting_set(PgId::new(raw_pg_id), vec![node_id])
                .unwrap();
        }
    }

    let filtered_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), ClusterEpoch::INITIAL)
        .unwrap();
    assert!(filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .all(|route| route.acting_set().contains(&NodeId::new(1))));
    assert!(filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| route.pg_id() == PgId::new(30) && route.cluster_epoch() == protected_epoch));
    assert!(!filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| (100..120).contains(&route.pg_id().get())));

    let mut encoded = Vec::new();
    write_runtime_map_snapshot(&mut encoded, &filtered_runtime_map).unwrap();
    assert!(
        encoded.len() < CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN / 8,
        "storage-node refresh retained too much unrelated route history: {} bytes",
        encoded.len()
    );
}

#[test]
fn storage_node_refresh_distributes_remote_exact_route_to_historical_actor() {
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
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(2)])
        .unwrap();
    let observed_epoch = authority.snapshot().cluster_epoch();

    let mut metadata_owner_heartbeat = heartbeat_from_record(&authority, 2, observed_epoch, 2_000);
    metadata_owner_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            source_epoch,
            PgId::new(30),
        )]);
    authority
        .heartbeat(metadata_owner_heartbeat, 2_000)
        .unwrap();

    let source_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_001, NodeId::new(1), observed_epoch)
        .unwrap();
    let historical = source_refresh
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);

    let unrelated_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_001, NodeId::new(3), observed_epoch)
        .unwrap();
    assert!(unrelated_refresh
        .historical_pg_routes()
        .iter()
        .all(|route| route.pg_id() != PgId::new(30) || route.cluster_epoch() != source_epoch));
}

#[test]
fn storage_node_refresh_history_uses_observed_epoch_for_running_node() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let protected_references = history_route_references([PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        protected_epoch,
        PgId::new(30),
    )]);
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for node_id in 10..18 {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let current_epoch = authority.snapshot().cluster_epoch();
    let observed_epoch = ClusterEpoch::new(current_epoch.get() - 2).unwrap();

    let bootstrap_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(bootstrap_refresh.historical_pg_routes().len(), 1);
    assert!(bootstrap_refresh
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));

    let running_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), observed_epoch)
        .unwrap();
    assert_eq!(running_refresh.historical_pg_routes().len(), 1);
    assert!(running_refresh
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));
}

#[test]
fn storage_node_restart_refresh_uses_control_plane_last_observed_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let protected_references = history_route_references([PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        protected_epoch,
        PgId::new(30),
    )]);
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for node_id in 10..18 {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let observed_epoch = authority.snapshot().cluster_epoch();
    let mut observed_heartbeat = heartbeat_from_record(&authority, 1, observed_epoch, 2_000);
    observed_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(observed_heartbeat, 2_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .and_then(NodeControlRecord::last_observed_epoch),
        Some(observed_epoch)
    );

    let mut restart_heartbeat = heartbeat_from_record(&authority, 1, ClusterEpoch::INITIAL, 2_100);
    restart_heartbeat.cluster_map_history_route_references = protected_references;
    authority
        .heartbeat(restart_heartbeat.clone(), 2_100)
        .expect("lost restart heartbeat response should still apply");
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .and_then(NodeControlRecord::last_observed_epoch),
        Some(observed_epoch),
        "stale restart heartbeat must not regress stored observed epoch"
    );
    let refresh = authority
        .refresh_node_heartbeat(restart_heartbeat, 2_200)
        .unwrap();
    let (_lease, runtime_map) = refresh.into_parts();
    assert!(runtime_map
        .historical_pg_routes()
        .iter()
        .all(|route| route.cluster_epoch() >= observed_epoch
            || route.cluster_epoch() == protected_epoch));
    assert!(runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));
}

#[test]
fn storage_node_refresh_includes_refreshing_node_without_assigned_pg() {
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
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();

    let runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();

    assert!(runtime_map
        .nodes()
        .iter()
        .any(|node| node.node_id() == NodeId::new(2)));
    assert!(runtime_map
        .pg_routes()
        .iter()
        .all(|route| !route.acting_set().contains(&NodeId::new(2))));
}

#[test]
fn runtime_map_refresh_preserves_historical_pg_routes_for_storage_cluster() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 2_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(2)])
        .unwrap();
    let moved_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    let intervening_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 1 },
    )
    .unwrap();

    let historical = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);
    let moved = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), moved_epoch)
        .unwrap();
    assert_eq!(moved.acting_set(), &[NodeId::new(2)]);
    let intervening = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), intervening_epoch)
        .unwrap();
    assert_eq!(intervening.acting_set(), &[NodeId::new(2)]);
    let current = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), runtime_map.cluster_epoch())
        .unwrap();
    assert_eq!(current.acting_set(), &[NodeId::new(2)]);

    for epoch in runtime_map.historical_cluster_epochs() {
        for pg_id in [PgId::new(30), PgId::new(31)] {
            let expected = runtime_map.reconstructed_pg_route_at_epoch(pg_id, *epoch);
            let actual = cluster.reconstructed_pg_route_at_epoch(pg_id, *epoch);
            match expected {
                Ok(expected) => {
                    let actual = actual.expect("local map should retain the runtime-map route");
                    assert!(pg_route_configuration_eq(&actual, &expected));
                }
                Err(ControlPlaneError::UnknownPg { .. }) => assert!(actual.is_err()),
                Err(error) => panic!("runtime map rejected retained epoch {epoch}: {error}"),
            }
        }
    }
}

#[test]
fn runtime_map_valid_until_is_minimum_active_primary_lease_deadline() {
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
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(28), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_002);
    heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Peering, 3_000);
    authority
        .complete_pg_peering(
            PgId::new(28),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Active, 3_002);
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 3_003);

    let node_1_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let node_2_deadline = authority
        .snapshot()
        .node(NodeId::new(2))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    assert!(node_2_deadline < node_1_deadline);

    let runtime_map = authority.snapshot().runtime_map(3_004).unwrap();
    assert_eq!(runtime_map.valid_until_ms(), Some(node_2_deadline));
    assert_eq!(
        runtime_map
            .pg_routes()
            .iter()
            .filter(|route| route.state() == PgState::Active)
            .map(PgRouteSnapshot::primary_lease_deadline_ms)
            .collect::<Vec<_>>(),
        vec![Some(node_1_deadline), Some(node_2_deadline)]
    );
}

#[test]
fn runtime_map_content_certificate_reuses_static_content_with_fresh_lease_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(127), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(127),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Active, 2_002);

    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let certificate = RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
        authority.snapshot(),
        &runtime_map,
    );
    let mut renewed_snapshot = authority.snapshot().clone();
    renewed_snapshot
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .lease_deadline_ms = Some(runtime_map.valid_until_ms().unwrap() + 1_000);
    let renewed_runtime_map = renewed_snapshot.runtime_map(2_003).unwrap();
    assert_eq!(
        renewed_runtime_map.content_digest(),
        runtime_map.content_digest()
    );

    let cached_status = ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
        &renewed_snapshot,
        2_003,
        *renewed_runtime_map.freshness_proof(),
        certificate,
    )
    .unwrap()
    .expect("lease-only changes should preserve certificate reuse");
    assert_eq!(
        cached_status,
        ControlPlaneRuntimeMapStatus::from_runtime_map(&renewed_runtime_map)
    );
    assert_eq!(
        cached_status.lease_renewal().unwrap().validity(),
        renewed_runtime_map.validity()
    );
}

#[test]
fn runtime_map_content_certificate_rejects_same_epoch_route_change() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(127), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(127),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Active, 2_002);

    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let certificate = RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
        authority.snapshot(),
        &runtime_map,
    );
    let mut changed_snapshot = authority.snapshot().clone();
    let changed_pg = changed_snapshot.pgs.get_mut(&PgId::new(127)).unwrap();
    changed_pg.state = PgState::Peering;
    changed_pg.active_primary = None;
    let changed_runtime_map = changed_snapshot.runtime_map(2_003).unwrap();
    assert_eq!(
        changed_runtime_map.cluster_epoch(),
        runtime_map.cluster_epoch()
    );
    assert_eq!(
        changed_runtime_map.pg_routes().len(),
        runtime_map.pg_routes().len()
    );
    assert_ne!(
        changed_runtime_map.content_digest(),
        runtime_map.content_digest()
    );

    assert!(
        ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
            &changed_snapshot,
            2_003,
            *changed_runtime_map.freshness_proof(),
            certificate,
        )
        .unwrap()
        .is_none(),
        "same-epoch route changes must force a full runtime-map refresh"
    );

    *authority.runtime_map_content_certificate.lock().unwrap() = Some(certificate);
    authority.snapshot = changed_snapshot;
    let rebuilt_status = authority.runtime_map_status(2_003).unwrap();
    assert_eq!(
        rebuilt_status
            .lease_renewal()
            .expect("single-authority status should retain a bounded validity proof")
            .content_digest(),
        changed_runtime_map.content_digest(),
        "the status source must rebuild rather than renew stale same-epoch content"
    );
}

#[test]
fn single_authority_runtime_map_content_certificate_invalidates_on_commit() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();

    let initial_status = authority.runtime_map_status(1_000).unwrap();
    let initial_certificate = authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .expect("status should cache the content certificate");
    assert_eq!(initial_status.cluster_epoch(), ClusterEpoch::INITIAL);

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .is_none());
    let updated_status = authority.runtime_map_status(1_001).unwrap();
    let updated_certificate = authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .expect("status should rebuild the invalidated certificate");
    assert_ne!(
        updated_status.cluster_epoch(),
        initial_status.cluster_epoch()
    );
    assert_ne!(updated_certificate, initial_certificate);
}

#[test]
fn runtime_map_builds_frontend_topology_with_validity_bound() {
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
        .set_pg_acting_set(PgId::new(29), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 1, 29, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(29),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Active, 2_003);
    let runtime_map = authority.snapshot().runtime_map(2_004).unwrap();
    let valid_until_ms = runtime_map
        .valid_until_ms()
        .expect("active runtime map should have a validity deadline");

    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 1 },
    )
    .unwrap();

    assert_eq!(local_map.epoch(), runtime_map.cluster_epoch());
    assert_eq!(local_map.route_map_valid_until_ms(), Some(valid_until_ms));
    assert!(local_map.is_route_map_valid_at(valid_until_ms - 1));
    assert!(!local_map.is_route_map_valid_at(valid_until_ms));
    let route = local_map.pg_route(PgId::new(29)).unwrap();
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Active);
}

#[test]
fn runtime_map_builds_storage_cluster_with_validity_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);
    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let valid_until_ms = runtime_map.valid_until_ms().unwrap();

    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();

    assert_eq!(cluster.cluster_epoch(), runtime_map.cluster_epoch());
    assert_eq!(cluster.operation_epoch(), runtime_map.cluster_epoch());
    assert_eq!(cluster.route_map_valid_until_ms(), Some(valid_until_ms));
    cluster
        .require_route_map_valid_at(valid_until_ms - 1)
        .unwrap();
    assert!(matches!(
        cluster.require_route_map_valid_at(valid_until_ms),
        Err(crate::StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: expired_at,
            now_ms,
        }) if cluster_epoch == runtime_map.cluster_epoch()
            && expired_at == valid_until_ms
            && now_ms == valid_until_ms
    ));
    let route = cluster.local_pg_route(PgId::new(31)).unwrap();
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.state(), PgState::Active);
}

#[test]
fn storage_cluster_refreshes_from_control_plane_runtime_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    assert_eq!(
        cluster.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        cluster.route_map_valid_until_ms(),
        Some(2_001 + MAX_HEARTBEAT_LEASE_MS)
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(
        refreshed.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        refreshed.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Active
    );
    assert_eq!(
        refreshed.route_map_valid_until_ms(),
        authority
            .snapshot()
            .runtime_map(2_004)
            .unwrap()
            .valid_until_ms()
    );
    assert!(refreshed.route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_refresh_preserves_process_local_reclaim_queue() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("refresh-queue-bucket").unwrap();
    let root = crate::BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 1,
    };
    cluster.enqueue_bucket_delete_finalize(root.clone());
    assert_eq!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(root.clone()))
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    let recreated_root = crate::BucketDeleteFinalizeRoot {
        bucket,
        bucket_incarnation_generation: 2,
    };
    refreshed.enqueue_bucket_delete_finalize(recreated_root.clone());
    cluster.finish_bucket_delete_finalize_work(&root);
    assert_eq!(
        refreshed.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(recreated_root.clone()))
    );
    refreshed.finish_bucket_delete_finalize_work(&recreated_root);
    assert!(refreshed.try_take_reclaim_work().is_none());
}

#[test]
fn storage_cluster_refresh_preserves_process_local_shard_repair_queue() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let repair = placed_segment_shard_repair_work_item_for_runtime_refresh(1);
    assert!(cluster.test_enqueue_placed_segment_shard_repair(repair));

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(
        refreshed.try_take_placed_segment_shard_repair_work(),
        Some(repair)
    );
    assert!(refreshed
        .try_take_placed_segment_shard_repair_work()
        .is_none());
}

#[test]
fn storage_cluster_refresh_preserves_metadata_runtime_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let pg_id = PgId::new(31);
    let lock_ptr = cluster.test_metadata_command_pg_lock_ptr(pg_id);
    let command = metadata_command_for_runtime_refresh(3);
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 0);
    let guard = cluster.test_begin_metadata_command_recovery_leader(pg_id, &command);
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 1);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(refreshed.test_metadata_command_pg_lock_ptr(pg_id), lock_ptr);
    assert_eq!(refreshed.test_metadata_command_recovery_flight_count(), 1);
    drop(guard);
    assert_eq!(refreshed.test_metadata_command_recovery_flight_count(), 0);
}

#[test]
fn storage_cluster_unix_refresh_preserves_process_local_runtime_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let registry_key = cluster.process_local_registry_key();
    let bucket = crate::BucketName::try_from("unix-refresh-queue-bucket").unwrap();
    let root = crate::BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 1,
    };
    cluster.enqueue_bucket_delete_finalize(root.clone());
    let repair = placed_segment_shard_repair_work_item_for_runtime_refresh(2);
    assert!(cluster.test_enqueue_placed_segment_shard_repair(repair));

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
            &authority,
            2_004,
            crate::cluster::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        )
        .unwrap();
    assert_eq!(refreshed.process_local_registry_key(), registry_key);
    assert_eq!(
        refreshed.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(root.clone()))
    );
    refreshed.finish_bucket_delete_finalize_work(&root);
    assert_eq!(
        refreshed.try_take_placed_segment_shard_repair_work(),
        Some(repair)
    );
    assert!(refreshed.try_take_reclaim_work().is_none());
    assert!(refreshed
        .try_take_placed_segment_shard_repair_work()
        .is_none());
}

#[test]
fn storage_cluster_route_handle_refresh_installs_current_map() {
    let _clock = crate::clock::test_time_override_guard(1_050);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(31))
            .unwrap()
            .state(),
        PgState::Peering
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = handle
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(Arc::as_ptr(&handle.current()), Arc::as_ptr(&refreshed));
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(31))
            .unwrap()
            .state(),
        PgState::Active
    );
    assert!(handle.current().route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_route_handle_rejects_epoch_downgrade() {
    let _clock = crate::clock::test_time_override_guard(2_001);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let older_map = authority.snapshot().runtime_map(2_001).unwrap();
    let older_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &older_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&older_cluster));

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 3_000).serving());
    let newer_map = authority.snapshot().runtime_map(3_001).unwrap();
    let newer_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &newer_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    assert!(newer_cluster.cluster_epoch() > older_cluster.cluster_epoch());
    handle.install(Arc::clone(&newer_cluster)).unwrap();

    assert!(matches!(
        handle.install(older_cluster),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::EpochDowngrade {
            current,
            candidate,
        }) if current == newer_cluster.cluster_epoch() && candidate == older_map.cluster_epoch()
    ));
    assert_eq!(
        handle.current().cluster_epoch(),
        newer_cluster.cluster_epoch()
    );
}

#[test]
fn storage_cluster_constructor_rejects_same_epoch_unbounded_dynamic_authority() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);

    let current_map = authority.snapshot().runtime_map(2_003).unwrap();
    let current_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &current_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let mut unbounded_map = current_map.clone();
    unbounded_map.validity = RouteMapValidity::Forever;
    assert!(matches!(
        crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &unbounded_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::DynamicRouteAuthorityUnboundedValidity { epoch })
            if epoch == current_map.cluster_epoch()
    ));
    assert!(current_cluster.route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_constructor_rejects_later_epoch_unbounded_dynamic_authority() {
    let current_map = runtime_map_test_snapshot_with_active_route();
    let mut unbounded_map = current_map.clone();
    unbounded_map.cluster_epoch = ClusterEpoch::new(current_map.cluster_epoch().get() + 1)
        .expect("test epoch should not overflow");
    unbounded_map.validity = RouteMapValidity::Forever;
    for route in &mut unbounded_map.pg_routes {
        route.cluster_epoch = unbounded_map.cluster_epoch;
    }
    let expected_epoch = ClusterEpoch::new(current_map.cluster_epoch().get() + 1).unwrap();
    assert!(matches!(
        crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &unbounded_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::DynamicRouteAuthorityUnboundedValidity { epoch })
            if epoch == expected_epoch
    ));
}

#[test]
fn storage_cluster_route_handle_accepts_same_epoch_shorter_bounded_validity() {
    crate::clock::with_time_override(12_000, || {
        let mut current_map = runtime_map_test_snapshot_with_active_route();
        current_map.validity = RouteMapValidity::until_ms(14_000).unwrap();
        current_map.pg_routes[0].primary_lease_deadline_ms = Some(14_000);
        let current_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &current_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(current_cluster);
        let mut shorter_map = current_map.clone();
        shorter_map.validity = RouteMapValidity::until_ms(13_500).unwrap();
        let shorter_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &shorter_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();

        handle.install(shorter_cluster).unwrap();
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(13_500));
    });
}

#[test]
fn storage_cluster_route_handle_extends_all_pinned_same_epoch_validity() {
    let _clock = crate::clock::test_time_override_guard(500);
    let route = PgRouteSnapshot::reconstructed(
        ClusterEpoch::INITIAL,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(1_000).unwrap(),
        )
        .unwrap();
    let pinned_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(local_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    pinned_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(1_000).unwrap());
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned_cluster));
    let candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(2_000).unwrap(),
        )
        .unwrap();
    let candidate_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(candidate_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    candidate_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(2_000).unwrap());

    handle.install(Arc::clone(&candidate_cluster)).unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(2_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(2_000));
    assert_eq!(
        Arc::as_ptr(&handle.current()),
        Arc::as_ptr(&candidate_cluster)
    );

    let second_candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(3_000).unwrap(),
        )
        .unwrap();
    let second_candidate_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(second_candidate_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    second_candidate_cluster
        .test_store_route_map_validity(RouteMapValidity::until_ms(3_000).unwrap());

    handle
        .install(Arc::clone(&second_candidate_cluster))
        .unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(3_000));
    assert_eq!(candidate_cluster.route_map_valid_until_ms(), Some(3_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(3_000));
    assert_eq!(
        Arc::as_ptr(&handle.current()),
        Arc::as_ptr(&second_candidate_cluster)
    );
}

#[test]
fn storage_cluster_route_handle_does_not_extend_pinned_previous_epoch_validity() {
    let _clock = crate::clock::test_time_override_guard(500);
    let initial_route = PgRouteSnapshot::reconstructed(
        ClusterEpoch::INITIAL,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&initial_route)],
            RouteMapValidity::until_ms(1_000).unwrap(),
        )
        .unwrap();
    let pinned_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(local_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    pinned_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(1_000).unwrap());
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned_cluster));
    let next_epoch = ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 1).unwrap();
    let next_route = PgRouteSnapshot::reconstructed(
        next_epoch,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            next_epoch,
            [crate::cluster::LocalPgRoute::from(&next_route)],
            RouteMapValidity::until_ms(3_000).unwrap(),
        )
        .unwrap();
    let candidate_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::new(candidate_map), next_epoch)
            .unwrap();
    candidate_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(3_000).unwrap());

    handle.install(candidate_cluster).unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(1_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(3_000));
}

#[test]
fn storage_cluster_route_handle_rejects_static_to_dynamic_same_epoch() {
    let _clock = crate::clock::test_time_override_guard(1_050);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);
    let active_map = authority.snapshot().runtime_map(2_004).unwrap();
    let active_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &active_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let unbounded_local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            active_map.cluster_epoch(),
            active_map
                .pg_routes()
                .iter()
                .map(crate::cluster::LocalPgRoute::from),
        )
        .unwrap();
    let unbounded_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(unbounded_local_map),
        active_map.cluster_epoch(),
    )
    .unwrap();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&unbounded_cluster));
    assert_eq!(
        active_cluster.cluster_epoch(),
        unbounded_cluster.cluster_epoch()
    );
    assert_eq!(unbounded_cluster.route_map_valid_until_ms(), None);
    assert!(active_cluster.route_map_valid_until_ms().is_some());

    assert!(matches!(
        handle.install(active_cluster),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh)
    ));
    assert!(Arc::ptr_eq(&handle.current(), &unbounded_cluster));
    assert_eq!(unbounded_cluster.route_map_valid_until_ms(), None);
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_installs_current_map() {
    let base_now_ms = crate::clock::current_time_millis();
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, base_now_ms).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        31,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        (base_now_ms + 2, 10_000),
    );

    let peering_map = authority.snapshot().runtime_map(base_now_ms + 3).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            base_now_ms + 4,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        31,
        PgState::Active,
        PgMetadataProof::empty(),
        false,
        (base_now_ms + 5, 10_000),
    );
    let expected_runtime_map = authority.snapshot().runtime_map(base_now_ms + 6).unwrap();
    let now = Arc::new(AtomicU64::new(base_now_ms + 6));
    let loop_now = Arc::clone(&now);
    let mut refresh_loop = handle
        .clone()
        .spawn_control_plane_refresh_loop(authority, Duration::from_millis(5), move || {
            loop_now.fetch_add(1, Ordering::SeqCst)
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if refresh_loop.status().successes > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "frontend runtime-map refresh loop did not install a map: {:?}",
            refresh_loop.status()
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    let current = handle.current();
    assert_eq!(
        current.cluster_epoch(),
        expected_runtime_map.cluster_epoch()
    );
    assert_eq!(
        current.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Active
    );
    assert_eq!(
        current.route_map_valid_until_ms(),
        expected_runtime_map.valid_until_ms()
    );
    assert_eq!(refresh_loop.status().failures, 0);
    assert_eq!(
        refresh_loop.status().last_success,
        Some(crate::StorageClusterRuntimeMapRefreshLoopSuccess {
            cluster_epoch: expected_runtime_map.cluster_epoch(),
            route_map_validity: expected_runtime_map.validity(),
        })
    );

    refresh_loop.stop();
    let attempts_after_stop = refresh_loop.status().attempts;
    std::thread::sleep(Duration::from_millis(15));
    assert_eq!(refresh_loop.status().attempts, attempts_after_stop);
}

#[test]
fn storage_cluster_runtime_map_refresh_continues_while_recovery_is_blocked() {
    struct BlockingRecoveryRuntimeMapSource {
        runtime_map: ClusterRuntimeMapSnapshot,
        recovery_gate: Arc<(Mutex<(bool, bool)>, Condvar)>,
    }

    impl BlockingRecoveryRuntimeMapSource {
        fn renewed_runtime_map(&self, authority_now_ms: u64) -> ClusterRuntimeMapSnapshot {
            let mut runtime_map = self.runtime_map.clone();
            runtime_map.validity =
                RouteMapValidity::until_ms(authority_now_ms.saturating_add(1_250)).unwrap();
            runtime_map.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
                issued_at_ms: authority_now_ms,
            };
            runtime_map
        }
    }

    impl ControlPlaneRuntimeMapSource for BlockingRecoveryRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            Ok(self.renewed_runtime_map(authority_now_ms))
        }

        fn pending_metadata_command_recoveries(
            &self,
            _authority_now_ms: u64,
        ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
            let (lock, changed) = &*self.recovery_gate;
            let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.0 = true;
            changed.notify_all();
            while !state.1 {
                let (next, timeout) = changed
                    .wait_timeout(state, Duration::from_secs(2))
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state = next;
                if timeout.timed_out() {
                    break;
                }
            }
            Ok(PendingMetadataCommandRecoveryListing::new(
                Vec::new(),
                Vec::new(),
            ))
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            let runtime_map = self.renewed_runtime_map(authority_now_ms);
            runtime_map
                .pg_routes()
                .iter()
                .any(|route| route.pg_id() == pg_id)
                .then_some(runtime_map)
                .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
        }
    }

    let base_now_ms = crate::clock::current_time_millis();
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, base_now_ms).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, base_now_ms + 1);
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            base_now_ms + 2,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        31,
        PgState::Active,
        PgMetadataProof::empty(),
        false,
        (base_now_ms + 3, 10_000),
    );
    let mut initial_map = authority.snapshot().runtime_map(base_now_ms + 4).unwrap();
    initial_map.validity = RouteMapValidity::until_ms(base_now_ms + 1_250).unwrap();
    initial_map.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: authority.snapshot().authority_incarnation(),
        issued_at_ms: base_now_ms,
    };
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &initial_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let recovery_gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let source = BlockingRecoveryRuntimeMapSource {
        runtime_map: initial_map,
        recovery_gate: Arc::clone(&recovery_gate),
    };
    let mut refresh_loop = handle
        .clone()
        .spawn_control_plane_refresh_loop(
            source,
            Duration::from_millis(10),
            crate::clock::current_time_millis,
        )
        .unwrap();

    {
        let (lock, changed) = &*recovery_gate;
        let state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (state, timeout) = changed
            .wait_timeout_while(state, Duration::from_secs(1), |state| !state.0)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.0 && !timeout.timed_out(),
            "recovery worker did not block"
        );
    }
    std::thread::sleep(Duration::from_millis(500));

    assert!(
        refresh_loop.status().successes >= 10,
        "route renewal did not continue during blocked recovery: {:?}",
        refresh_loop.status()
    );
    handle
        .admit_current_route()
        .expect("blocked pending-command recovery must not expire request routes");

    {
        let (lock, changed) = &*recovery_gate;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.1 = true;
        changed.notify_all();
    }
    refresh_loop.stop();
}

#[test]
fn storage_cluster_runtime_map_workers_resample_time_for_each_operation() {
    struct TimestampRecordingRuntimeMapSource {
        snapshot: ClusterControlSnapshot,
        discovery_now_ms: Arc<AtomicU64>,
        recovery_now_ms: Arc<Mutex<Vec<u64>>>,
        refresh_now_ms: Arc<AtomicU64>,
    }

    impl ControlPlaneRuntimeMapSource for TimestampRecordingRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            let _ = self.refresh_now_ms.compare_exchange(
                u64::MAX,
                authority_now_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.snapshot.runtime_map(authority_now_ms)
        }

        fn pending_metadata_command_recoveries(
            &self,
            authority_now_ms: u64,
        ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
            let _ = self.discovery_now_ms.compare_exchange(
                u64::MAX,
                authority_now_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            Ok(PendingMetadataCommandRecoveryListing::new(
                [31, 32]
                    .into_iter()
                    .map(|pg_id| {
                        PendingMetadataCommandRecoveryTask::new(
                            PgId::new(pg_id),
                            PendingMetadataCommandRecovery::new(
                                NodeId::new(1),
                                PendingMetadataCommandObservation::new(
                                    ClusterEpoch::INITIAL,
                                    NonZeroU64::MIN,
                                    u64::from(pg_id),
                                ),
                            ),
                        )
                    })
                    .collect(),
                Vec::new(),
            ))
        }

        fn pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.recovery_now_ms
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(authority_now_ms);
            Err(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.snapshot
                .serving_runtime_map_for_pg_with_freshness_proof(
                    pg_id,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                )
        }
    }

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    let initial_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &initial_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let discovery_now_ms = Arc::new(AtomicU64::new(u64::MAX));
    let recovery_now_ms = Arc::new(Mutex::new(Vec::new()));
    let refresh_now_ms = Arc::new(AtomicU64::new(u64::MAX));
    let source = TimestampRecordingRuntimeMapSource {
        snapshot: authority.snapshot().clone(),
        discovery_now_ms: Arc::clone(&discovery_now_ms),
        recovery_now_ms: Arc::clone(&recovery_now_ms),
        refresh_now_ms: Arc::clone(&refresh_now_ms),
    };
    let clock = Arc::new(AtomicU64::new(2_001));
    let loop_clock = Arc::clone(&clock);
    let mut refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_secs(1), move || {
            loop_clock.fetch_add(6_000, Ordering::SeqCst)
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while refresh_loop.status().successes == 0 {
        assert!(
            Instant::now() < deadline,
            "refresh loop did not complete: {:?}",
            refresh_loop.status()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    refresh_loop.stop();

    let discovery_now_ms = discovery_now_ms.load(Ordering::SeqCst);
    let recovery_now_ms = recovery_now_ms
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let refresh_now_ms = refresh_now_ms.load(Ordering::SeqCst);
    assert_ne!(discovery_now_ms, u64::MAX);
    assert_ne!(refresh_now_ms, u64::MAX);
    assert_eq!(recovery_now_ms.len(), 2);
    assert_ne!(discovery_now_ms, refresh_now_ms);
    assert!(
        recovery_now_ms.windows(2).all(|times| times[0] < times[1]),
        "recovery operations did not resample time: {recovery_now_ms:?}"
    );
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_retains_transient_failure_classification() {
    struct FailOnceRuntimeMapSource {
        snapshot: ClusterControlSnapshot,
        failures_remaining: AtomicU64,
    }

    impl ControlPlaneRuntimeMapSource for FailOnceRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            if self
                .failures_remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ControlPlaneError::io(
                    "sentinel runtime-map read",
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "sentinel-bucket/sentinel-object/sentinel-upload-id",
                    ),
                ));
            }
            self.snapshot.runtime_map(authority_now_ms)
        }

        fn pending_metadata_command_recoveries(
            &self,
            _authority_now_ms: u64,
        ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
            Ok(PendingMetadataCommandRecoveryListing::new(
                Vec::new(),
                Vec::new(),
            ))
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.snapshot
                .serving_runtime_map_for_pg_with_freshness_proof(
                    pg_id,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                )
        }
    }

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 1_001);
    let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let source = FailOnceRuntimeMapSource {
        snapshot: authority.snapshot().clone(),
        failures_remaining: AtomicU64::new(1),
    };
    let mut refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_millis(5), || 1_002)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let status = refresh_loop.status();
        if status.failures == 1 && status.successes > 0 {
            assert_eq!(status.last_error, None);
            assert_eq!(
                status.last_failure,
                Some(crate::StorageClusterRuntimeMapRefreshLoopFailure {
                    attempt: 1,
                    kind: "control_plane_io_timeout",
                })
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "refresh loop did not fail then recover: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    refresh_loop.stop();
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_rejects_zero_interval() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 1_001);
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);

    assert!(matches!(
        handle.spawn_control_plane_refresh_loop(authority, Duration::ZERO, || 1_000),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::RefreshLoopZeroInterval)
    ));
}

#[test]
fn runtime_node_routes_build_unix_storage_client_configs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let node = &runtime_map.nodes()[0];

    let default_config =
        crate::cluster::LocalUnixStorageNodeClientConfig::from_runtime_node_route(node);
    assert_eq!(default_config.node_id(), NodeId::new(1));
    assert_eq!(
        default_config.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(
        default_config.rpc_admission_limit(),
        crate::cluster::LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT
    );

    let configured =
        crate::cluster::LocalUnixStorageNodeClientConfig::with_rpc_admission_settings_from_runtime_node_route(
            node,
            crate::cluster::LocalUnixStorageNodeClientAdmissionSettings {
                rpc_admission_limit: 17,
                rpc_admission_wait_timeout: std::time::Duration::from_millis(200),
                rpc_control_admission_wait_timeout: std::time::Duration::from_millis(300),
            },
        );
    assert_eq!(configured.node_id(), NodeId::new(1));
    assert_eq!(
        configured.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(configured.rpc_admission_limit(), 17);
    assert_eq!(
        configured.rpc_admission_wait_timeout(),
        std::time::Duration::from_millis(200)
    );
    assert_eq!(
        configured.rpc_control_admission_wait_timeout(),
        std::time::Duration::from_millis(300)
    );

    let settings = crate::cluster::LocalUnixStorageNodeClientAdmissionSettings {
        rpc_admission_limit: 23,
        rpc_admission_wait_timeout: std::time::Duration::from_millis(400),
        rpc_control_admission_wait_timeout: std::time::Duration::from_millis(500),
    };
    let [ref refreshed_config] =
        crate::StorageCluster::unix_storage_node_client_configs_from_runtime_map(
            &runtime_map,
            settings,
        )
        .try_into()
        .unwrap();
    assert_eq!(refreshed_config.node_id(), NodeId::new(1));
    assert_eq!(
        refreshed_config.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(refreshed_config.rpc_admission_limit(), 23);
    assert_eq!(
        refreshed_config.rpc_admission_wait_timeout(),
        std::time::Duration::from_millis(400)
    );
    assert_eq!(
        refreshed_config.rpc_control_admission_wait_timeout(),
        std::time::Duration::from_millis(500)
    );
}

#[test]
fn runtime_map_installs_unix_storage_clients_from_absolute_endpoints() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(32), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    let cluster = crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();

    assert_eq!(cluster.local_node_count(), 1);
    assert_eq!(
        cluster.local_node_ids().collect::<Vec<_>>(),
        vec![NodeId::new(1)]
    );
    assert_eq!(
        cluster.local_pg_route(PgId::new(32)).unwrap().state(),
        PgState::Peering
    );
}

#[test]
fn runtime_map_builds_storage_node_process_config_for_node_routes() {
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
        .set_pg_acting_set(PgId::new(34), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(35), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Peering, 1_001);
    heartbeat_with_pg_observation(&mut authority, 2, 34, PgState::Peering, 1_002);
    authority
        .complete_pg_peering(
            PgId::new(34),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_003,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Active, 1_004);
    let runtime_map = authority.snapshot().runtime_map(1_005).unwrap();
    let valid_until_ms = runtime_map.valid_until_ms().unwrap();

    let node_1_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
        NodeId::new(1),
        tmp.path().join("node-1"),
        crate::EcShape { k: 1, m: 0 },
        &runtime_map,
    )
    .unwrap();
    assert_eq!(node_1_config.node_id, NodeId::new(1));
    assert_eq!(node_1_config.cluster_epoch, runtime_map.cluster_epoch());
    assert_eq!(
        node_1_config.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert_eq!(
        node_1_config.socket_path,
        std::path::PathBuf::from("node-1.sock")
    );
    assert_eq!(node_1_config.pg_ids, vec![34, 35]);
    assert_eq!(node_1_config.pg_routes.len(), 2);
    assert_eq!(node_1_config.pg_routes[0].pg_id, 34);
    assert_eq!(node_1_config.pg_routes[0].state, PgState::Active);
    assert_eq!(
        node_1_config.pg_routes[0].acting_set,
        vec![NodeId::new(1), NodeId::new(2)]
    );
    assert_eq!(node_1_config.pg_routes[1].pg_id, 35);
    assert_eq!(node_1_config.pg_routes[1].state, PgState::Peering);
    assert_eq!(node_1_config.pg_routes[1].acting_set, vec![NodeId::new(2)]);

    let node_2_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
        NodeId::new(2),
        tmp.path().join("node-2"),
        crate::EcShape { k: 1, m: 0 },
        &runtime_map,
    )
    .unwrap();
    assert_eq!(node_2_config.pg_ids, vec![34, 35]);
    assert_eq!(
        node_2_config.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert_eq!(
        node_2_config
            .pg_routes
            .iter()
            .map(|route| route.pg_id)
            .collect::<Vec<_>>(),
        vec![34, 35]
    );
}

#[test]
fn runtime_map_storage_node_config_rejects_absent_node() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(36), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    assert!(matches!(
        crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
            NodeId::new(2),
            tmp.path().join("node-2"),
            crate::EcShape { k: 1, m: 0 },
            &runtime_map,
        ),
        Err(crate::storage_node_server::StorageNodeServerError::RuntimeMapNodeNotFound {
            node_id: 2,
            cluster_epoch,
        }) if cluster_epoch == runtime_map.cluster_epoch()
    ));
}

#[test]
fn runtime_map_unix_storage_clients_reject_relative_endpoints() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(33), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    assert!(matches!(
        crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
            NodeId::new(1),
            &runtime_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::RemoteStorageNodeClientSocketPathNotAbsolute { path })
            if path.as_path() == std::path::Path::new("node-1.sock")
    ));
}

#[test]
fn runtime_map_requires_endpoints_for_routed_nodes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();

    assert!(matches!(
        authority.snapshot().runtime_map(1_000),
        Err(ControlPlaneError::NodeEndpointMissing { node_id: 1, .. })
    ));
}
