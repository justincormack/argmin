use super::*;

#[test]
fn active_pg_route_fails_closed_after_primary_lease_expiry() {
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
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
    let lease_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();

    assert!(matches!(
        authority
            .snapshot()
            .active_pg_route(PgId::new(23), lease_deadline),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 23, .. })
    ));
    let reconstructed = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(23), authority.snapshot().cluster_epoch())
        .unwrap();
    assert_eq!(reconstructed.state(), PgState::Active);
    assert_eq!(reconstructed.primary_node_id(), NodeId::new(1));
    assert_eq!(reconstructed.acting_set(), &[NodeId::new(1)]);
    assert_eq!(reconstructed.primary_lease_deadline_ms(), None);
}

#[test]
fn complete_pg_peering_requires_unexpired_primary_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(11), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, 1_050);
    let lease_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(11),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));
    assert_eq!(
        authority.snapshot().pg(PgId::new(11)).unwrap().state(),
        PgState::Peering
    );

    heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, lease_deadline + 1);
    authority
        .complete_pg_peering(
            PgId::new(11),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline + 2,
        )
        .unwrap();
}

#[test]
fn complete_pg_peering_requires_current_peering_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(12), vec![NodeId::new(1)])
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 12,
            node_id: 1,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Active, 2_020);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_030,
        ),
        Err(ControlPlaneError::PgPeeringObservationNotPeering {
            pg_id: 12,
            node_id: 1,
            state: PgState::Active,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Peering, 2_040);
    authority
        .complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
}

#[test]
fn complete_pg_peering_only_activates_from_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_state(PgId::new(15), PgState::Degraded)
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(15),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgNotPeering {
            pg_id: 15,
            state: PgState::Degraded,
            ..
        })
    ));
}

#[test]
fn active_pg_service_requires_primary_active_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_010), None);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020),
            2_020,
        )
        .unwrap();
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_021,
        ),
        Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 16,
            node_id: 1,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_030);
    assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_030), None);
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_040,
        ),
        Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 16,
            node_id: 1,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Active, 2_050);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(16), 2_050),
        Some(NodeId::new(1))
    );
    authority
        .authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_060,
        )
        .unwrap();
}

#[test]
fn node_service_authorization_requires_current_epoch_incarnation_and_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let record = authority.snapshot().node(NodeId::new(1)).unwrap();
    let node_incarnation = record.node_incarnation();
    let lease_deadline_ms = record.lease_deadline_ms().unwrap();

    let authorized = authority
        .authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            serving.cluster_epoch(),
            150,
        )
        .unwrap();
    assert_eq!(authorized.node_id(), NodeId::new(1));
    assert_eq!(authorized.node_incarnation(), node_incarnation);
    assert_eq!(authorized.cluster_epoch(), serving.cluster_epoch());
    assert_eq!(
        authorized.authority_incarnation(),
        authority.snapshot().authority_incarnation()
    );
    assert_eq!(authorized.lease_deadline_ms(), lease_deadline_ms);
    authority
        .validate_node_service_authorization(&authorized, 151)
        .unwrap();

    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation + 1,
            serving.cluster_epoch(),
            150,
        ),
        Err(ControlPlaneError::NodeIncarnationMismatch { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            ClusterEpoch::INITIAL,
            150,
        ),
        Err(ControlPlaneError::StaleNodeObservedEpoch { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            serving.cluster_epoch(),
            lease_deadline_ms,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.validate_node_service_authorization(&authorized, lease_deadline_ms),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let mut shorter_lease =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 152);
    shorter_lease.requested_lease_duration_ms = 5;
    let refreshed = authority.heartbeat(shorter_lease, 152).unwrap();
    assert_eq!(
        refreshed.lease_deadline_ms(),
        authorized.lease_deadline_ms()
    );
    assert!(authorized.lease_deadline_ms() > 157);
    authority
        .validate_node_service_authorization(&authorized, 157)
        .unwrap();
}

#[test]
fn node_service_authorization_cannot_validate_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let active_epoch = serving.cluster_epoch();
    let node_incarnation = node_incarnation(&authority, 1);
    let authorization = authority
        .authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 150)
        .unwrap();
    authority
        .validate_node_service_authorization(&authorization, 151)
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(37), vec![NodeId::new(1)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > active_epoch);

    assert!(matches!(
        authority.validate_node_service_authorization(&authorization, 152),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == peering_epoch
    ));
    assert!(matches!(
        authority.authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 153),
        Err(ControlPlaneError::StaleNodeObservedEpoch {
            node_id: 1,
            observed_epoch,
            current_epoch,
        }) if observed_epoch == active_epoch && current_epoch == peering_epoch
    ));

    let current = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, peering_epoch, 154),
            154,
        )
        .unwrap();
    assert!(current.serving());
    authority
        .authorize_node_service(NodeId::new(1), node_incarnation, peering_epoch, 155)
        .unwrap();
}

#[test]
fn node_service_authorization_cannot_validate_after_authority_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let active_epoch = serving.cluster_epoch();
    let active_authority_incarnation = serving.authority_incarnation();
    let node_incarnation = node_incarnation(&authority, 1);
    let authorization = authority
        .authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 150)
        .unwrap();
    authority
        .validate_node_service_authorization(&authorization, 151)
        .unwrap();

    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().cluster_epoch() > active_epoch);
    assert!(restarted.snapshot().authority_incarnation() > active_authority_incarnation);
    assert!(matches!(
        restarted.validate_node_service_authorization(&authorization, 152),
        Err(ControlPlaneError::StaleAuthorityIncarnation {
            authority_incarnation,
            current_authority_incarnation,
        }) if authority_incarnation == active_authority_incarnation
            && current_authority_incarnation == restarted.snapshot().authority_incarnation()
    ));

    let restart_epoch = restarted.snapshot().cluster_epoch();
    let current = restarted
        .heartbeat(
            heartbeat_from_record(&restarted, 1, restart_epoch, 153),
            153,
        )
        .unwrap();
    assert!(current.serving());
    restarted
        .authorize_node_service(NodeId::new(1), node_incarnation, restart_epoch, 154)
        .unwrap();
}

#[test]
fn pg_primary_authorization_fails_closed_for_peering_and_wrong_primary() {
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
        .set_pg_acting_set(PgId::new(10), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            10,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    let node_one_incarnation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .node_incarnation();
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(1),
            node_one_incarnation,
            authority.snapshot().cluster_epoch(),
            2_050,
        ),
        Err(ControlPlaneError::PgNotActive { pg_id: 10, .. })
    ));

    authority
        .complete_pg_peering(
            PgId::new(10),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 10, PgState::Active, 3_001);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
            3_002,
        )
        .unwrap();
    let node_one_incarnation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .node_incarnation();
    let node_two_incarnation = authority
        .snapshot()
        .node(NodeId::new(2))
        .unwrap()
        .node_incarnation();
    let authorized = authority
        .authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(1),
            node_one_incarnation,
            authority.snapshot().cluster_epoch(),
            3_050,
        )
        .unwrap();
    assert_eq!(authorized.pg_id(), PgId::new(10));
    assert_eq!(authorized.primary_node_id(), NodeId::new(1));
    assert_eq!(
        authorized.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        authorized.authority_incarnation(),
        authority.snapshot().authority_incarnation()
    );
    assert_eq!(
        authorized.lease_deadline_ms(),
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap()
    );

    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(2),
            node_two_incarnation,
            authority.snapshot().cluster_epoch(),
            3_050,
        ),
        Err(ControlPlaneError::NodeNotPgPrimary {
            pg_id: 10,
            node_id: 2,
            primary_node_id: 1,
            ..
        })
    ));
}

#[test]
fn pg_operation_authorization_requires_active_primary_for_all_operation_classes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(14), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(14),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Active, 3_000);

    let operations = [
        PgServiceOperation::MetadataRead,
        PgServiceOperation::MetadataList,
        PgServiceOperation::MetadataWrite,
        PgServiceOperation::PayloadRead,
        PgServiceOperation::PayloadWrite,
    ];
    for operation in operations {
        let authorization = authority
            .authorize_pg_operation(
                operation,
                PgId::new(14),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                3_050,
            )
            .unwrap();
        assert_eq!(authorization.operation(), operation);
        assert_eq!(authorization.pg_id(), PgId::new(14));
        assert_eq!(authorization.primary_node_id(), NodeId::new(1));
        assert_eq!(
            authorization.primary_node_incarnation(),
            node_incarnation(&authority, 1)
        );
        assert_eq!(
            authorization.cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        authority
            .validate_pg_operation_authorization(&authorization, 3_060)
            .unwrap();
        authority
            .validate_pg_operation_authorization_for(&authorization, operation, 3_060)
            .unwrap();
        let wrong_operation = match operation {
            PgServiceOperation::MetadataWrite => PgServiceOperation::MetadataRead,
            _ => PgServiceOperation::MetadataWrite,
        };
        assert!(matches!(
            authority.validate_pg_operation_authorization_for(
                &authorization,
                wrong_operation,
                3_060,
            ),
            Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                expected,
                actual,
            }) if expected == wrong_operation && actual == operation
        ));
    }

    for state in [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ] {
        authority.set_pg_state(PgId::new(14), state).unwrap();
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 4_000),
                4_000,
            )
            .unwrap();
        for operation in operations {
            assert!(matches!(
                authority.authorize_pg_operation(
                    operation,
                    PgId::new(14),
                    NodeId::new(1),
                    node_incarnation(&authority, 1),
                    authority.snapshot().cluster_epoch(),
                    4_050,
                ),
                Err(ControlPlaneError::PgNotActive {
                    pg_id: 14,
                    state: err_state,
                    ..
                }) if err_state == state
            ));
        }
    }
}

#[test]
fn pg_operation_authorization_validation_fences_stale_tokens() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
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
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_000);

    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            3_050,
        )
        .unwrap();
    authority
        .validate_pg_operation_authorization(&authorization, 3_060)
        .unwrap();

    let mut shorter_lease =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 3_061);
    shorter_lease.requested_lease_duration_ms = 5;
    shorter_lease.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(17),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refreshed = authority.heartbeat(shorter_lease, 3_061).unwrap();
    assert_eq!(
        refreshed.lease_deadline_ms(),
        authorization.lease_deadline_ms()
    );
    assert!(authorization.lease_deadline_ms() > 3_066);
    authority
        .validate_pg_operation_authorization(&authorization, 3_066)
        .unwrap();

    assert!(matches!(
        authority
            .validate_pg_operation_authorization(&authorization, authorization.lease_deadline_ms()),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_070);
    authority
        .set_pg_state(PgId::new(17), PgState::Peering)
        .unwrap();
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 3_060),
        Err(ControlPlaneError::StaleAuthorizationEpoch { .. })
    ));

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(matches!(
        restarted.validate_pg_operation_authorization(&authorization, 3_060),
        Err(ControlPlaneError::StaleAuthorityIncarnation { .. })
    ));
}

#[test]
fn stale_observed_epoch_heartbeat_returns_map_without_becoming_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(6), NodeMembershipState::Active)
        .unwrap();
    let stale_epoch = ClusterEpoch::INITIAL;
    let lease = authority
        .heartbeat(heartbeat(6, stale_epoch, 100), 100)
        .unwrap();
    assert!(!lease.serving());
    assert_eq!(lease.snapshot().cluster_epoch(), lease.cluster_epoch());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(6)], 100),
        None
    );

    let caught_up = authority
        .heartbeat(heartbeat(6, lease.cluster_epoch(), 200), 200)
        .unwrap();
    assert!(!caught_up.serving());
    let final_lease = authority
        .heartbeat(heartbeat(6, caught_up.cluster_epoch(), 300), 300)
        .unwrap();
    assert!(final_lease.serving());
}

#[test]
fn heartbeat_records_current_epoch_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
        .unwrap();

    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(15),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        },
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();

    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(15))
        .unwrap();
    assert_eq!(observation.pg_id(), PgId::new(15));
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(
        observation.observed_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(observation.observed_at_ms(), 2_000);
    assert_eq!(
        observation.metadata_proof(),
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        }
    );
    let persisted = store.load().unwrap().unwrap();
    let persisted_observation = persisted
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(15))
        .unwrap();
    assert_eq!(persisted_observation.state(), PgState::Peering);
    assert_eq!(
        persisted_observation.metadata_proof(),
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        }
    );
}

#[test]
fn complete_pg_peering_requires_matching_metadata_proofs() {
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
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let matching_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let different_proof = PgMetadataProof {
        applied_log_index: 41,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for (node_id, metadata_proof) in [(1, matching_proof), (2, different_proof)] {
        let mut heartbeat = heartbeat_from_record(
            &authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            2_000 + u64::from(node_id),
        );
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Peering,
            metadata_proof,
            pending_metadata_command: None,
        }];
        authority
            .heartbeat(heartbeat, 2_000 + u64::from(node_id))
            .unwrap();
    }
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 19,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == matching_proof && actual == different_proof
    ));

    let mut heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_060);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Peering,
        metadata_proof: matching_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_060).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(active_pg.active_metadata_proof(), Some(matching_proof));
    let persisted = store.load().unwrap().unwrap();
    let persisted_pg = persisted.pg(PgId::new(19)).unwrap();
    assert_eq!(persisted_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(persisted_pg.active_metadata_proof(), Some(matching_proof));
}

#[test]
fn complete_pg_peering_accepts_converged_later_epoch_log_with_unchanged_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(35);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let active_floor = PgMetadataProof {
        applied_log_index: 90,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            active_floor,
            false,
            2_012 + u64::from(node_id),
        );
    }

    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)])
        .unwrap();
    let later_epoch_proof = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 0x123,
        state_digest: active_floor.state_digest,
    };
    for node_id in [1, 2, 3] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            later_epoch_proof,
            false,
            2_020 + u64::from(node_id),
        );
    }
    assert_eq!(
        authority.complete_ready_pg_peerings(2_030).unwrap(),
        vec![pg_id],
        "the converged reset proof must be discovered and completed automatically"
    );

    let active = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(active.state(), PgState::Active);
    assert_eq!(active.active_metadata_proof(), Some(later_epoch_proof));
}

#[test]
fn complete_pg_peering_rejects_same_log_divergent_replica_digest() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(32);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let node_one_pg = PgStore::open(&tmp.path().join("node-1-pg"), pg_id.get()).unwrap();
    let node_two_pg = PgStore::open(&tmp.path().join("node-2-pg"), pg_id.get()).unwrap();
    let bucket = bucket_name("divergent-replica-source");
    let create = logged_create_bucket_command(pg_id, 1, &bucket);
    node_one_pg
        .apply_metadata_command_and_record(1, &create)
        .unwrap();
    node_two_pg
        .apply_metadata_command_and_record(2, &create)
        .unwrap();
    let primary_proof = pg_metadata_proof_from_store(&node_one_pg);
    assert_eq!(primary_proof, pg_metadata_proof_from_store(&node_two_pg));

    node_two_pg
        .put_bucket_versioning(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    node_two_pg.refresh_metadata_command_state_digest().unwrap();
    let divergent_proof = pg_metadata_proof_from_store(&node_two_pg);
    assert_eq!(
        divergent_proof.applied_log_index,
        primary_proof.applied_log_index
    );
    assert_eq!(
        divergent_proof.applied_log_hash,
        primary_proof.applied_log_hash
    );
    assert_ne!(divergent_proof.state_digest, primary_proof.state_digest);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        primary_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        divergent_proof,
        false,
        2_001,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 32,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == primary_proof && actual == divergent_proof
    ));
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );
}

#[test]
fn finalized_bucket_cleanup_proof_progress_requires_logged_command() {
    let tmp = test_util::tempdir();
    let pg_id = PgId::new(33);
    let store = PgStore::open(tmp.path(), pg_id.get()).unwrap();
    let bucket = bucket_name("finalized-cleanup-proof");
    let create = logged_create_bucket_command(pg_id, 1, &bucket);
    store.apply_metadata_command_and_record(1, &create).unwrap();
    let mark = logged_mark_bucket_deleting_command(&store, 2, &bucket);
    store.apply_metadata_command_and_record(1, &mark).unwrap();
    let cleanup_floor = pg_metadata_proof_from_store(&store);

    let digest_only_cleanup = PgMetadataProof {
        applied_log_index: cleanup_floor.applied_log_index,
        applied_log_hash: cleanup_floor.applied_log_hash,
        state_digest: cleanup_floor.state_digest.wrapping_add(1),
    };
    let cleanup_floor_epoch = ClusterEpoch::new(7).unwrap();
    let cleanup_observed_epoch = ClusterEpoch::new(8).unwrap();
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        cleanup_floor,
        digest_only_cleanup,
        Some(MetadataProofProgressProvenance {
            floor_epoch: cleanup_floor_epoch,
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        cleanup_observed_epoch,
    ));

    let delete = logged_delete_finalized_bucket_command(&store, 3, &bucket);
    store.apply_metadata_command_and_record(1, &delete).unwrap();
    let logged_cleanup = pg_metadata_proof_from_store(&store);
    assert!(
        logged_cleanup.applied_log_index > cleanup_floor.applied_log_index,
        "finalized cleanup must advance the command-log index"
    );
    assert_ne!(
        logged_cleanup.applied_log_hash, cleanup_floor.applied_log_hash,
        "finalized cleanup must advance the command-log hash"
    );
    assert!(metadata_proof_satisfies_active_primary_observation_floor(
        cleanup_floor,
        logged_cleanup,
        Some(MetadataProofProgressProvenance {
            floor_epoch: cleanup_floor_epoch,
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        cleanup_observed_epoch,
    ));
    assert!(matches!(
        store.head_bucket_record_raw(&bucket),
        Err(crate::MetadataError::BucketNotFound { .. })
    ));
}

#[test]
fn pg_peering_reconstruction_fails_closed_until_serving_replicas_converge() {
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
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let reconstructed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        31,
        PgState::Peering,
        reconstructed_proof,
        false,
        2_001,
    );

    let lagging_proof = PgMetadataProof {
        applied_log_index: 41,
        applied_log_hash: 0xaaa,
        state_digest: 0xddd,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        lagging_proof,
        false,
        2_002,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == lagging_proof
    ));

    let same_index_hash_fork = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabd,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        same_index_hash_fork,
        false,
        2_020,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_030,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == same_index_hash_fork
    ));

    let same_index_state_fork = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdf0,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        same_index_state_fork,
        false,
        2_040,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == same_index_state_fork
    ));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        reconstructed_proof,
        false,
        2_060,
    );
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(31)).unwrap();
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(active_pg.active_metadata_proof(), Some(reconstructed_proof));
}

#[test]
fn complete_pg_peering_rejects_pending_metadata_command_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(29), vec![NodeId::new(1)])
        .unwrap();

    let proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(&mut authority, 1, 29, PgState::Peering, proof, false, 2_000);
    authority.complete_ready_pg_peerings(2_010).unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_epoch)),
    }];
    authority.heartbeat(heartbeat, 2_020).unwrap();
    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_030);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Peering,
        metadata_proof: proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_epoch)),
    }];
    authority.heartbeat(heartbeat, 2_030).unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(29),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 29,
            node_id: 1,
            ..
        })
    ));

    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_060);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Peering,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_060).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(29),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
}

#[test]
fn active_heartbeat_accepts_metadata_progress_after_peering() {
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

    let accepted_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let peering_epoch = authority.snapshot().cluster_epoch();
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch < active_epoch);
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));

    let mut equal_active = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    equal_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(equal_active, 2_020).unwrap();
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(
        pg.active_metadata_proof_epoch(),
        Some(peering_epoch),
        "equal-proof heartbeat must not restamp proof provenance"
    );
    let pre_progress_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();

    let progressed_proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 0x1234,
        state_digest: 0x5678,
    };
    let mut progressed_active = heartbeat_from_record(&authority, 1, active_epoch, 2_030);
    progressed_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    let lease = authority.heartbeat(progressed_active, 2_030).unwrap();
    assert_eq!(lease.lease_deadline_ms(), 2_130);
    assert!(lease.lease_deadline_ms() > pre_progress_deadline);
    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(19))
        .unwrap();
    assert_eq!(observation.state(), PgState::Active);
    assert_eq!(observation.metadata_proof(), progressed_proof);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(19), 2_031),
        Some(NodeId::new(1))
    );
    let expiry_at_old_deadline = authority
        .expire_heartbeat_leases(pre_progress_deadline + 1)
        .unwrap();
    assert_eq!(expiry_at_old_deadline.expired_nodes(), &[]);
    assert_eq!(expiry_at_old_deadline.peering_pgs(), &[]);
    assert_eq!(
        authority.snapshot().pg(PgId::new(19)).unwrap().state(),
        PgState::Active
    );
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_stamps_active_proofs_with_peering_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    for pg_id in [21, 22] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }

    let peering_epoch = authority.snapshot().cluster_epoch();
    let proof_21 = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let proof_22 = PgMetadataProof {
        applied_log_index: 99,
        applied_log_hash: 0xaabb,
        state_digest: 0xccdd,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![
        NodePgHeartbeatObservation {
            pg_id: PgId::new(21),
            state: PgState::Peering,
            metadata_proof: proof_21,
            pending_metadata_command: None,
        },
        NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Peering,
            metadata_proof: proof_22,
            pending_metadata_command: None,
        },
    ];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let mut completed = authority.complete_ready_pg_peerings(2_010).unwrap();
    completed.sort_by_key(|pg_id| pg_id.get());
    assert_eq!(completed, vec![PgId::new(21), PgId::new(22)]);
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch < active_epoch);
    for (pg_id, proof) in [(21, proof_21), (22, proof_22)] {
        let pg = authority.snapshot().pg(PgId::new(pg_id)).unwrap();
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(pg.active_metadata_proof(), Some(proof));
        assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));
    }

    let progressed_proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 0x1234,
        state_digest: 0x5678,
    };
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(21),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    let pg = authority.snapshot().pg(PgId::new(21)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_unobserved_metadata_proof() {
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

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(23),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let forged_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(23),
                    primary: NodeId::new(1),
                    node_incarnation: node_incarnation(&authority, 1),
                    active_metadata_proof: forged_proof,
                    active_metadata_proof_epoch: peering_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 23,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == observed_proof && actual == forged_proof
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_wrong_metadata_proof_epoch() {
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

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(24),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let forged_epoch = ClusterEpoch::new(peering_epoch.get() + 1).unwrap();
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(24),
                    primary: NodeId::new(1),
                    node_incarnation: node_incarnation(&authority, 1),
                    active_metadata_proof: observed_proof,
                    active_metadata_proof_epoch: forged_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofEpochMismatch {
            pg_id: 24,
            expected,
            actual,
        }) if expected == peering_epoch && actual == forged_epoch
    ));
}

#[test]
fn complete_ready_pg_peerings_command_replays_with_committed_ready_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(25),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let command = ControlPlaneCommand::CompleteReadyPgPeerings {
        ready_at_ms: 2_010,
        ready: vec![ReadyPgPeeringCompletion {
            pg_id: PgId::new(25),
            primary: NodeId::new(1),
            node_incarnation: node_incarnation(&authority, 1),
            active_metadata_proof: observed_proof,
            active_metadata_proof_epoch: peering_epoch,
        }],
    };
    let applied = authority
        .snapshot()
        .apply_control_plane_command(command.clone())
        .unwrap();
    let pg = applied.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(pg.active_metadata_proof(), Some(observed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(2_010));

    authority.snapshot = applied.into_snapshot();
    persist_manually_modified_test_snapshot(&mut authority);
    let active_epoch = authority.snapshot().cluster_epoch();
    let progressed_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xbc,
        state_digest: 0xef,
    };
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_010);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(25),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_010).unwrap();
    let replayed = authority
        .snapshot()
        .apply_control_plane_command(command)
        .unwrap();
    assert!(
        !replayed.changed(),
        "exact active CompleteReadyPgPeerings replay should be a no-op"
    );
    let pg = replayed.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_stale_node_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(51), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(51),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(2_010)
        .unwrap();
    assert_eq!(ready.len(), 1);
    let mut restarted = heartbeat_from_record(&authority, 1, peering_epoch, 2_011);
    restarted.node_incarnation += 1;
    authority.heartbeat(restarted, 2_011).unwrap();

    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_012,
                ready,
            },
        ),
        Err(ControlPlaneError::NodeIncarnationMismatch {
            node_id: 1,
            sender_incarnation,
            current_incarnation,
        }) if sender_incarnation + 1 == current_incarnation
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 2_000).serving());

    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::CompleteReadyPgPeerings {
            ready_at_ms: 1_999,
            ready: Vec::new(),
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 1_999,
            max_committed_timestamp_ms: 2_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn complete_ready_pg_peerings_command_rejects_non_deterministic_primary() {
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
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        let mut peering_heartbeat =
            heartbeat_from_record(&authority, node_id, peering_epoch, 2_000);
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(26),
            state: PgState::Peering,
            metadata_proof: observed_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    }

    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(26),
                    primary: NodeId::new(2),
                    node_incarnation: node_incarnation(&authority, 2),
                    active_metadata_proof: observed_proof,
                    active_metadata_proof_epoch: peering_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 26,
            node_id: 2
        })
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_duplicate_pg_completion() {
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
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let active_proof = authority
        .snapshot()
        .pg(PgId::new(27))
        .unwrap()
        .active_metadata_proof()
        .unwrap();
    authority
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(27),
            vec![NodeId::new(2)],
            PgMetadataTransferProof::new(authority.snapshot().cluster_epoch(), active_proof),
        )
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        27,
        PgState::Peering,
        active_proof,
        false,
        2_020,
    );

    let completion = ReadyPgPeeringCompletion {
        pg_id: PgId::new(27),
        primary: NodeId::new(2),
        node_incarnation: node_incarnation(&authority, 2),
        active_metadata_proof: active_proof,
        active_metadata_proof_epoch: peering_epoch,
    };
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_030,
                ready: vec![completion, completion],
            },
        ),
        Err(ControlPlaneError::DuplicateReadyPgPeeringCompletion { pg_id: 27 })
    ));
}

#[test]
fn active_primary_heartbeat_pending_command_fences_pg_for_recovery() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(32), vec![NodeId::new(1)])
        .unwrap();

    let active_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        32,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(32),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let pending = test_pending_metadata_command(active_epoch);

    let mut pending_active = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    pending_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(32),
        state: PgState::Active,
        metadata_proof: active_proof,
        pending_metadata_command: Some(pending),
    }];
    let before_invalid = authority.snapshot().clone();
    let mut invalid_pending = pending_active.clone();
    invalid_pending.pg_observations[0].pending_metadata_command = Some(
        test_pending_metadata_command(ClusterEpoch::new(active_epoch.get() + 1).unwrap()),
    );
    assert!(matches!(
        authority.refresh_node_heartbeat(invalid_pending, 2_020),
        Err(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })
            if cluster_epoch == ClusterEpoch::new(active_epoch.get() + 1).unwrap()
    ));
    assert_eq!(authority.snapshot(), &before_invalid);

    let refresh = authority
        .refresh_node_heartbeat(pending_active, 2_020)
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > active_epoch);
    assert!(!refresh.lease().serving());
    let route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(32))
        .unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(
        route.pending_metadata_command_recovery(),
        Some(PendingMetadataCommandRecovery::new(NodeId::new(1), pending))
    );
    authority = reopen_file_authority(&store);
    let pg = authority.snapshot().pg(PgId::new(32)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(32))
        .is_none());
    assert!(authority
        .snapshot()
        .pending_metadata_command_recoveries()
        .tasks()
        .is_empty());

    let restart_epoch = authority.snapshot().cluster_epoch();
    let mut reconstructed = heartbeat_from_record(&authority, 1, restart_epoch, 2_030);
    reconstructed.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(32),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: Some(pending),
    }];
    let reconstructed = authority
        .refresh_node_heartbeat(reconstructed, 2_030)
        .unwrap();
    assert_eq!(reconstructed.runtime_map().cluster_epoch(), restart_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pending_metadata_command_recoveries()
            .tasks(),
        &[PendingMetadataCommandRecoveryTask::new(
            PgId::new(32),
            PendingMetadataCommandRecovery::new(NodeId::new(1), pending),
        )]
    );
    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(32))
        .unwrap();
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(observation.observed_epoch(), restart_epoch);
    assert_eq!(observation.pending_metadata_command(), Some(pending));
    assert_eq!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(32), active_epoch)
            .unwrap()
            .state(),
        PgState::Active
    );
    assert_eq!(store.load().unwrap().unwrap(), *authority.snapshot());
}

#[test]
fn peering_heartbeat_rejects_pending_command_without_historical_active_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(33), vec![NodeId::new(1)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let mut invalid = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    invalid.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(33),
        state: PgState::Peering,
        metadata_proof: heartbeat_model_proof(3),
        pending_metadata_command: Some(test_pending_metadata_command(peering_epoch)),
    }];
    let before = authority.snapshot().clone();

    assert!(matches!(
        authority.refresh_node_heartbeat(invalid, 2_000),
        Err(
            ControlPlaneError::PgPeeringPendingMetadataCommandReporterNotHistoricalPrimary {
                pg_id: 33,
                node_id: 1,
                pending_epoch,
                historical_state: PgState::Peering,
                ..
            }
        ) if pending_epoch == peering_epoch
    ));
    assert_eq!(authority.snapshot(), &before);
    assert_eq!(store.load().unwrap().unwrap(), before);
}

#[test]
fn stale_heartbeat_does_not_mutate_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();

    let mut stale = heartbeat_from_record(
        &authority,
        1,
        ClusterEpoch::new(current_epoch.get() - 1).unwrap(),
        2_000,
    );
    stale.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(16),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let response = authority.heartbeat(stale, 2_000).unwrap();
    assert!(!response.serving());
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(16))
        .is_none());
}

#[test]
fn heartbeat_rejects_invalid_pg_observations() {
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
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();

    let mut duplicate =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    duplicate.pg_observations = vec![
        NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        },
        NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        },
    ];
    assert!(matches!(
        authority.heartbeat(duplicate, 2_000),
        Err(ControlPlaneError::DuplicatePgObservation {
            node_id: 1,
            pg_id: 17
        })
    ));

    let mut unknown =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_001);
    unknown.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(99),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(unknown, 2_001),
        Err(ControlPlaneError::UnknownPg { pg_id: 99 })
    ));

    let mut wrong_node =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_002);
    wrong_node.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(17),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(wrong_node, 2_002),
        Err(ControlPlaneError::PgObservationNotInActingSet {
            node_id: 2,
            pg_id: 17
        })
    ));
}

#[test]
fn epoch_change_drops_reconstructible_pg_observations_from_history() {
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
    let observation_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(18),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_some());

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    let history = authority
        .snapshot()
        .cluster_map_at_epoch(observation_epoch)
        .unwrap();
    assert!(history.nodes().contains(&NodeId::new(1)));
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(18), observation_epoch)
        .is_ok());
}

#[test]
fn restart_epoch_bump_drops_reconstructible_pg_observations_from_history() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    let observation_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
    let metadata_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(18),
        state: PgState::Peering,
        metadata_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_some());

    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert!(restarted.snapshot().cluster_epoch() > observation_epoch);
    assert!(restarted
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    assert!(restarted
        .snapshot()
        .cluster_map_at_epoch(observation_epoch)
        .unwrap()
        .nodes()
        .contains(&NodeId::new(1)));
    let persisted_text = std::fs::read_to_string(store.path()).unwrap();
    assert!(!persisted_text.contains("history_node_pg="));
    assert!(persisted_text
        .lines()
        .any(|line| { line.starts_with("history_node=") && line.split(',').count() == 2 }));
    assert!(persisted_text
        .lines()
        .any(|line| { line.starts_with("history_pg_absent=") && line.split(',').count() == 2 }));

    let persisted = store.load().unwrap().unwrap();
    assert!(persisted
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    SingleAuthorityControlPlane::open(store).unwrap();
}

#[test]
fn authority_restart_moves_active_pg_back_to_peering_before_service() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();
    let active_metadata_proof = PgMetadataProof {
        applied_log_index: 11,
        applied_log_hash: 12,
        state_digest: 13,
    };
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Peering,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_002);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Active,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let active = authority.heartbeat(active_heartbeat, 2_002).unwrap();
    let active_epoch = active.cluster_epoch();
    assert_eq!(
        authority.snapshot().pg(PgId::new(26)).unwrap().state(),
        PgState::Active
    );

    let mut restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let restart_epoch = restarted.snapshot().cluster_epoch();
    assert!(restart_epoch > active_epoch);
    let restarted_pg = restarted.snapshot().pg(PgId::new(26)).unwrap();
    assert_eq!(restarted_pg.state(), PgState::Peering);
    assert_eq!(restarted_pg.active_primary(), None);
    assert_eq!(restarted_pg.active_metadata_proof(), None);
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(active_metadata_proof)
    );
    let historical_pg = restarted
        .snapshot()
        .cluster_map_at_epoch(active_epoch)
        .unwrap()
        .pgs()
        .iter()
        .find(|record| record.pg_id() == PgId::new(26))
        .unwrap();
    assert_eq!(historical_pg.state(), PgState::Active);
    assert_eq!(historical_pg.active_primary, Some(NodeId::new(1)));
    let historical_route = restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(26), active_epoch)
        .unwrap();
    assert_eq!(historical_route.cluster_epoch(), active_epoch);
    assert_eq!(historical_route.state(), PgState::Active);
    assert_eq!(historical_route.primary_node_id(), NodeId::new(1));
    assert_eq!(historical_route.acting_set(), &[NodeId::new(1)]);
    assert_eq!(historical_route.primary_lease_deadline_ms(), None);

    let mut stale_active_heartbeat = heartbeat_from_record(&restarted, 1, restart_epoch, 2_003);
    stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Active,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let refresh = restarted
        .refresh_node_heartbeat(stale_active_heartbeat, 2_003)
        .unwrap();
    assert!(refresh.lease().serving());
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Peering
    );
    assert!(matches!(
        restarted.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&restarted, 1),
            restart_epoch,
            2_004,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 26,
            state: PgState::Peering,
            ..
        })
    ));

    let mut current_peering_heartbeat = heartbeat_from_record(&restarted, 1, restart_epoch, 2_005);
    current_peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Peering,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let refresh = restarted
        .refresh_node_heartbeat(current_peering_heartbeat, 2_005)
        .unwrap();
    assert!(
        !refresh.lease().serving(),
        "same-process peering completion bumps the epoch before the node observes it"
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Active
    );
    assert_eq!(
        restarted.snapshot().pg(PgId::new(26)).unwrap().state(),
        PgState::Active,
        "the unchanged primary process need not wait out its own old lease"
    );
}

#[test]
fn stale_observed_epoch_heartbeat_updates_liveness_without_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(7), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 7, 100);
    assert!(serving.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 100),
        Some(NodeId::new(7))
    );
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(7)])
        .unwrap();
    let route_epoch = authority.snapshot().cluster_epoch();

    let before = authority.snapshot().node(NodeId::new(7)).unwrap().clone();
    let stale_epoch = ClusterEpoch::new(route_epoch.get() - 1).unwrap();
    let mut stale = heartbeat(7, stale_epoch, 200);
    stale.node_incarnation = before.node_incarnation() + 1;
    stale.endpoint = "stale-node-7.sock".to_owned();
    let stale_response = authority.refresh_node_heartbeat(stale, 200).unwrap();
    let stale_lease = stale_response.lease();
    assert!(!stale_lease.serving());
    assert!(stale_lease.cluster_epoch() > serving.cluster_epoch());
    assert_eq!(
        stale_response.runtime_map().cluster_epoch(),
        stale_lease.cluster_epoch()
    );
    let stale_route = stale_response
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(1))
        .unwrap();
    assert_eq!(stale_route.state(), PgState::Peering);
    assert_eq!(stale_route.primary_node_id(), NodeId::new(7));
    assert_eq!(stale_route.primary_lease_deadline_ms(), None);

    let after = authority.snapshot().node(NodeId::new(7)).unwrap();
    assert_eq!(after.node_incarnation(), before.node_incarnation() + 1);
    assert_eq!(after.endpoint(), "stale-node-7.sock");
    assert_eq!(after.availability(), NodeAvailabilityState::Healthy);
    assert_eq!(after.last_observed_epoch(), Some(stale_epoch));
    assert_eq!(after.last_heartbeat_ms(), Some(200));
    assert_eq!(after.lease_deadline_ms(), Some(300));
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 200),
        None
    );
}

#[test]
fn future_observed_epoch_heartbeat_rejected_without_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();

    let proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    heartbeat_with_pg_proof(&mut authority, 1, 1, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(1),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    assert!(authority
        .heartbeat(active_heartbeat, 2_020)
        .unwrap()
        .serving());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(1), 2_021),
        Some(NodeId::new(1))
    );

    let before = authority.snapshot().clone();
    let before_node = before.node(NodeId::new(1)).unwrap();
    let future_epoch = ClusterEpoch::new(before.cluster_epoch().get() + 100).unwrap();
    let mut future = heartbeat_from_record(&authority, 1, future_epoch, 2_030);
    future.node_incarnation = before_node.node_incarnation() + 1;
    future.endpoint = "future-node-1.sock".to_owned();
    future.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];

    assert!(matches!(
        authority.refresh_node_heartbeat(future, 2_030),
        Err(ControlPlaneError::FutureNodeObservedEpoch {
            node_id: 1,
            observed_epoch,
            current_epoch,
        }) if observed_epoch == future_epoch && current_epoch == before.cluster_epoch()
    ));
    assert_eq!(authority.snapshot(), &before);
    assert_eq!(store.load().unwrap().unwrap(), before);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(1), 2_031),
        Some(NodeId::new(1))
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        max_shrink_iters: 256,
    ..ProptestConfig::default()
    })]

    #[test]
    fn prop_record_node_heartbeat_command_matches_single_authority(
        ops in proptest::collection::vec(
            control_plane_heartbeat_command_boundary_op_strategy(),
            1..48,
        ),
    ) {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000 + u64::from(node_id)).serving());
        }
        authority
            .set_pg_acting_set(heartbeat_model_pg_id(), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        let initial_proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_000,
        );
        heartbeat_with_pg_proof(
            &mut authority,
            2,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_001,
        );
        authority.complete_ready_pg_peerings(2_002).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        let mut active_primary_heartbeat =
            heartbeat_from_record(&authority, 1, active_epoch, 2_003);
        active_primary_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: heartbeat_model_pg_id(),
            state: PgState::Active,
            metadata_proof: initial_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(active_primary_heartbeat, 2_003).unwrap();

        let mut replayed = authority.snapshot().clone();
        let mut now_ms = 3_000_u64;

        for op in ops {
            now_ms = now_ms.saturating_add(10);
            let heartbeat = match op {
                ControlPlaneHeartbeatCommandBoundaryOp::Current {
                    node_slot,
                    observation_kind,
                    floor_kind,
                    bump_incarnation,
                    change_endpoint,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let mut heartbeat = heartbeat_from_snapshot(
                        &replayed,
                        node_id,
                        replayed.cluster_epoch(),
                        now_ms,
                    );
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-current-node-{node_id}-{now_ms}.sock");
                    }
                    heartbeat.pg_observations =
                        heartbeat_model_observation(&replayed, node_id, observation_kind);
                    heartbeat.cluster_map_history_route_references =
                        heartbeat_model_history_references(&replayed, floor_kind);
                    heartbeat
                }
                ControlPlaneHeartbeatCommandBoundaryOp::Stale {
                    node_slot,
                    stale_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let current_raw_epoch = replayed.cluster_epoch().get();
                    let stale_raw_epoch = current_raw_epoch
                        .saturating_sub(u64::from(stale_delta))
                        .max(ClusterEpoch::INITIAL.get());
                    let stale_epoch = ClusterEpoch::new(stale_raw_epoch).unwrap();
                    let mut heartbeat =
                        heartbeat_from_snapshot(&replayed, node_id, stale_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-stale-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&replayed, node_id, 3);
                    }
                    heartbeat
                }
                ControlPlaneHeartbeatCommandBoundaryOp::Future {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let future_epoch = ClusterEpoch::new(
                        replayed.cluster_epoch().get() + u64::from(future_delta),
                    )
                    .unwrap();
                    let mut heartbeat =
                        heartbeat_from_snapshot(&replayed, node_id, future_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-future-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&replayed, node_id, 4);
                    }
                    heartbeat
                }
            };

            let lease_deadline_ms = now_ms + heartbeat.requested_lease_duration_ms;
            let before_replayed = replayed.clone();
            let before_authority = authority.snapshot().clone();
            let replay_result = replayed.apply_control_plane_command(
                ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: heartbeat.clone(),
                    heartbeat_at_ms: now_ms,
                    lease_deadline_ms,
                    lease_horizon_authority: None,
                },
            );
            let authority_result = authority.heartbeat(heartbeat, now_ms);

            match (replay_result, authority_result) {
                (Ok(applied), Ok(_lease)) => {
                    replayed = applied.into_snapshot();
                    prop_assert_eq!(
                        &replayed,
                        authority.snapshot(),
                        "direct heartbeat command replay must match single-authority heartbeat"
                    );
                }
                (Err(_), Err(_)) => {
                    prop_assert_eq!(
                        &replayed,
                        &before_replayed,
                        "rejected direct heartbeat command must not mutate replay snapshot"
                    );
                    prop_assert_eq!(
                        authority.snapshot(),
                        &before_authority,
                        "rejected single-authority heartbeat must not mutate snapshot"
                    );
                }
                (Ok(_), Err(error)) => {
                    prop_assert!(
                        false,
                        "direct heartbeat command accepted but single-authority rejected: {error:?}"
                    );
                }
                (Err(error), Ok(_)) => {
                    prop_assert!(
                        false,
                        "single-authority heartbeat accepted but direct command rejected: {error:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn prop_control_plane_epoch_heartbeat_model_preserves_invariants(
        ops in proptest::collection::vec(control_plane_heartbeat_model_op_strategy(), 1..48),
    ) {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000 + u64::from(node_id)).serving());
        }
        authority
            .set_pg_acting_set(heartbeat_model_pg_id(), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        let initial_proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_000,
        );
        heartbeat_with_pg_proof(
            &mut authority,
            2,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_001,
        );
        authority.complete_ready_pg_peerings(2_002).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        let mut active_primary_heartbeat =
            heartbeat_from_record(&authority, 1, active_epoch, 2_003);
        active_primary_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: heartbeat_model_pg_id(),
            state: PgState::Active,
            metadata_proof: initial_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(active_primary_heartbeat, 2_003).unwrap();

        let mut now_ms = 3_000_u64;
        assert_control_plane_heartbeat_model_invariants(&authority, &store, now_ms)?;

        for op in ops {
            now_ms = now_ms.saturating_add(10);
            match op {
                ControlPlaneHeartbeatModelOp::CurrentHeartbeat {
                    node_slot,
                    observation_kind,
                    floor_kind,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let before = authority.snapshot().clone();
                    let mut heartbeat = heartbeat_from_record(
                        &authority,
                        node_id,
                        authority.snapshot().cluster_epoch(),
                        now_ms,
                    );
                    heartbeat.pg_observations = heartbeat_model_observation(
                        authority.snapshot(),
                        node_id,
                        observation_kind,
                    );
                    heartbeat.cluster_map_history_route_references =
                        heartbeat_model_history_references(authority.snapshot(), floor_kind);
                    if authority.heartbeat(heartbeat, now_ms).is_err() {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(
                            store.load().unwrap().unwrap(),
                            before,
                            "rejected current heartbeat must not mutate durable state"
                        );
                    }
                }
                ControlPlaneHeartbeatModelOp::StaleHeartbeat {
                    node_slot,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let current_epoch = authority.snapshot().cluster_epoch();
                    let stale_epoch = ClusterEpoch::new(current_epoch.get().saturating_sub(1))
                        .unwrap_or(ClusterEpoch::INITIAL);
                    let mut heartbeat =
                        heartbeat_from_record(&authority, node_id, stale_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint = format!("stale-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(authority.snapshot(), node_id, 3);
                    }
                    let before = authority.snapshot().clone();
                    let before_epoch = authority.snapshot().cluster_epoch();
                    let Ok(lease) = authority.heartbeat(heartbeat, now_ms) else {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(store.load().unwrap().unwrap(), before);
                        continue;
                    };
                    prop_assert!(!lease.serving());
                    prop_assert!(lease.cluster_epoch() >= before_epoch);
                    prop_assert!(
                        authority
                            .snapshot()
                            .node(NodeId::new(node_id))
                            .unwrap()
                            .pg_observation(heartbeat_model_pg_id())
                            .is_none(),
                        "stale heartbeats must not install current PG observations"
                    );
                }
                ControlPlaneHeartbeatModelOp::FutureHeartbeat {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let before = authority.snapshot().clone();
                    let future_epoch = ClusterEpoch::new(
                        before.cluster_epoch().get() + u64::from(future_delta),
                    )
                    .unwrap();
                    let mut heartbeat =
                        heartbeat_from_record(&authority, node_id, future_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint = format!("future-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&before, node_id, 4);
                    }
                    let error = authority.refresh_node_heartbeat(heartbeat, now_ms).unwrap_err();
                    let is_fail_closed_error = matches!(
                        error,
                        ControlPlaneError::FutureNodeObservedEpoch { .. }
                            | ControlPlaneError::CommittedTimestampTooFarAhead { .. }
                    );
                    prop_assert!(is_fail_closed_error);
                    prop_assert_eq!(authority.snapshot(), &before);
                    prop_assert_eq!(
                        store.load().unwrap().unwrap(),
                        before,
                        "future heartbeats must not mutate durable state"
                    );
                }
                ControlPlaneHeartbeatModelOp::SetActingSet { shape } => {
                    let before = authority.snapshot().clone();
                    if authority
                        .set_pg_acting_set(
                            heartbeat_model_pg_id(),
                            heartbeat_model_acting_set(shape),
                        )
                        .is_err()
                    {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(
                            store.load().unwrap().unwrap(),
                            before,
                            "rejected acting-set changes must not mutate durable state"
                        );
                    }
                }
                ControlPlaneHeartbeatModelOp::CompleteReadyPeerings => {
                    authority.complete_ready_pg_peerings(now_ms).unwrap();
                }
                ControlPlaneHeartbeatModelOp::ExpireLeases { advance_ms } => {
                    now_ms = now_ms.saturating_add(u64::from(advance_ms));
                    authority.expire_heartbeat_leases(now_ms).unwrap();
                }
                ControlPlaneHeartbeatModelOp::RestartAuthority => {
                    authority = reopen_file_authority(&store);
                }
            }

            assert_control_plane_heartbeat_model_invariants(&authority, &store, now_ms)?;
        }
    }
}
