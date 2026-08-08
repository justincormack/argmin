#[test]
fn control_plane_openraft_single_node_initialize_uses_bootstrap_membership() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-bootstrap-membership-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store.clone(), "test-cluster");
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        assert!(authority.is_initialized().await.unwrap());

        let bootstrap_log_id = raft_log_id(0, 1, 0);
        let status = authority.status().await.unwrap();
        assert_eq!(status.effective_membership_log_id(), Some(bootstrap_log_id));
        assert_eq!(status.effective_voters(), &BTreeSet::from([1]));

        let entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..1)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, bootstrap_log_id);
        assert!(matches!(entries[0].payload, EntryPayload::Membership(_)));

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_triggered_single_node_client_write_applies_and_rejects() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-client-write-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "single-node initialization leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "single-node authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let bootstrap = authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        let bootstrap_log_id = bootstrap.log_id();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        let bootstrap_epoch = authority.status().await.unwrap().current_cluster_epoch();

        let heartbeat = authority
            .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "node-1".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 345,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 12_000,
                lease_deadline_ms: 12_345,
                lease_horizon_authority: Some(LeaseHorizonAuthorityBinding::new(1, Some(1))),
            })
            .await
            .unwrap();
        assert_eq!(heartbeat.log_id().index(), bootstrap_log_id.index() + 1);
        assert!(matches!(
            heartbeat.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::RecordNodeHeartbeat
            )
        ));

        let rejected = authority
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(99),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .unwrap();
        assert_eq!(rejected.log_id().index(), heartbeat.log_id().index() + 1);
        let rejected_log_id = rejected.log_id();
        assert!(matches!(
            rejected.outcome(),
            ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                node_id
            }) if *node_id == 99
        ));
        authority
            .wait_for_applied_log_id(
                bootstrap_log_id,
                Duration::from_secs(1),
                "already-applied earlier entry remains reflected",
            )
            .await
            .unwrap();

        let (applied_snapshot, (applied_log_id, _applied_membership)) = authority
            .raft()
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot().clone();
                let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                Box::pin(async move { (snapshot, applied_state) })
            })
            .await
            .unwrap();
        assert!(applied_snapshot.node(NodeId::new(1)).is_some());
        assert!(applied_snapshot.node(NodeId::new(99)).is_none());
        assert_eq!(applied_log_id, Some(rejected_log_id));

        let status = authority.status().await.unwrap();
        assert_eq!(status.node_id(), 1);
        assert_eq!(status.current_leader(), Some(1));
        assert_eq!(status.server_state(), ServerState::Leader);
        assert!(status.local_leader());
        assert!(status.effective_voter());
        assert!(!status.effective_learner());
        assert!(status.applied_voter());
        assert!(!status.applied_learner());
        assert_eq!(
            status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert!(status.linearized_authority_serving());
        let persisted_vote = status
            .persisted_vote()
            .expect("single-node leader should persist a vote");
        assert!(persisted_vote.committed);
        assert_eq!(persisted_vote.leader_id.node_id, 1);
        assert_eq!(
            status.current_term(),
            Some(rejected_log_id.committed_leader_id().term)
        );
        assert_eq!(
            persisted_vote.leader_id.term,
            rejected_log_id.committed_leader_id().term
        );
        assert_eq!(status.last_log_id(), Some(rejected_log_id));
        assert_eq!(status.last_log_index(), Some(rejected_log_id.index()));
        assert_eq!(status.last_purged_log_id(), None);
        assert_eq!(status.last_purged_index(), None);
        assert_eq!(status.committed(), Some(rejected_log_id));
        assert_eq!(status.committed_index(), Some(rejected_log_id.index()));
        assert_eq!(status.applied(), Some(rejected_log_id));
        assert_eq!(status.applied_index(), Some(rejected_log_id.index()));
        assert_eq!(status.current_snapshot(), None);
        assert_eq!(status.current_snapshot_index(), None);
        assert_eq!(status.durable_last_vote(), Some(persisted_vote));
        assert_eq!(status.durable_last_log_id(), Some(rejected_log_id));
        assert_eq!(status.durable_last_purged_log_id(), None);
        assert_eq!(status.durable_committed(), Some(rejected_log_id));
        assert_eq!(status.durable_applied(), Some(rejected_log_id));
        assert_eq!(status.durable_timestamp_high_water_ms(), Some(12_000));
        assert_eq!(status.committed_to_applied_index_gap(), Some(0));
        assert_eq!(status.last_log_to_committed_index_gap(), Some(0));
        assert!(status.applied_caught_up_to_committed());
        assert!(status.committed_caught_up_to_last_log());
        assert_eq!(status.effective_voters(), &BTreeSet::from([1]));
        assert_eq!(status.applied_voters(), &BTreeSet::from([1]));
        assert_eq!(status.storage_node_lease_deadline_count(), 1);
        assert_eq!(
            status.earliest_storage_node_lease_deadline_ms(),
            Some(12_345)
        );
        assert_eq!(status.latest_storage_node_lease_deadline_ms(), Some(12_345));
        assert_eq!(
            status.metadata_transfer_fence_source_lease_deadline_count(),
            0
        );
        assert_eq!(
            status.earliest_metadata_transfer_fence_source_lease_deadline_ms(),
            None
        );
        assert_eq!(
            status.latest_metadata_transfer_fence_source_lease_deadline_ms(),
            None
        );

        let command_metrics_before = authority.durability_metric_snapshots().command;
        let update_guard = authority.volatile_heartbeat_update_gate.lock().await;
        let mut queued_submission = Box::pin(authority.submit_control_plane_command(
            ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(100),
                availability: NodeAvailabilityState::Healthy,
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), queued_submission.as_mut())
                .await
                .is_err(),
            "command submission should wait behind the authority update gate"
        );
        drop(update_guard);
        let queued = queued_submission.await.unwrap();
        assert!(matches!(
            queued.outcome(),
            ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                node_id
            }) if *node_id == 100
        ));
        let command_metrics_after = authority.durability_metric_snapshots().command;
        assert_eq!(
            command_metrics_after.submit_total,
            command_metrics_before.submit_total + 1
        );
        assert_eq!(
            command_metrics_after.submit_error_total, command_metrics_before.submit_error_total,
            "a deterministic state-machine rejection is still a successful Raft submission"
        );
        assert!(
            command_metrics_after.queue_wait_us_total > command_metrics_before.queue_wait_us_total,
            "queued command should record authority gate wait time"
        );
        assert!(
            command_metrics_after.operation_us_total > command_metrics_before.operation_us_total,
            "queued command should record guarded submission time"
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_read_index_runtime_map_uses_applied_tip() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-read-index-runtime-map-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let authority = ControlPlaneRaftAuthority::new_with_log_store(
            raft,
            log_store,
            "control-plane-raft-read-index-runtime-map-test",
        );
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "single-node read-index leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "single-node read-index authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let write = authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0), PgId::new(1)],
            })
            .await
            .unwrap();
        assert!(matches!(
            write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let runtime_map = authority
            .linearized_runtime_map_snapshot(44_000)
            .await
            .unwrap();
        let applied_log_id = authority
            .status()
            .await
            .unwrap()
            .applied()
            .expect("read-index should have an applied tip");
        let expected_control_plane_read_index = control_plane_log_id_from_raft(applied_log_id)
            .expect("read-index should be non-bootstrap");

        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(expected_control_plane_read_index)
        );
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(44_000));
        assert!(runtime_map.freshness_proof().is_serving_authority_read());
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(1)));

        let compact_status = authority
            .linearized_runtime_map_status(44_001)
            .await
            .unwrap();
        assert_eq!(compact_status.cluster_epoch(), runtime_map.cluster_epoch());
        assert_eq!(compact_status.pg_routes(), runtime_map.pg_routes().len());
        assert_eq!(
            compact_status
                .lease_renewal()
                .expect("read-index status should renew the runtime-map lease")
                .content_digest(),
            runtime_map.content_digest()
        );
        assert_eq!(
            authority
                .runtime_map_content_certificate
                .lock()
                .unwrap()
                .as_ref()
                .map(|(log_id, _certificate)| *log_id),
            Some(applied_log_id)
        );
        let repeated_status = authority
            .linearized_runtime_map_status(44_002)
            .await
            .unwrap();
        assert_eq!(
            repeated_status
                .lease_renewal()
                .expect("repeated read-index status should use cached content")
                .content_digest(),
            runtime_map.content_digest()
        );

        let current_epoch = runtime_map.cluster_epoch();
        let heartbeat = authority
            .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "node-1".to_string(),
                    observed_epoch: current_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(1),
                        state: PgState::Peering,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                heartbeat_at_ms: 44_100,
                lease_deadline_ms: 45_100,
                lease_horizon_authority: Some(LeaseHorizonAuthorityBinding::new(1, Some(1))),
            })
            .await
            .unwrap();
        assert!(
            matches!(
                heartbeat.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::RecordNodeHeartbeat
                )
            ),
            "unexpected scoped-read heartbeat outcome: {:?}",
            heartbeat.outcome()
        );
        let current_epoch = authority
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .cluster_epoch();
        let heartbeat = authority
            .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "node-1".to_string(),
                    observed_epoch: current_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(1),
                        state: PgState::Peering,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                heartbeat_at_ms: 44_101,
                lease_deadline_ms: 45_101,
                lease_horizon_authority: Some(LeaseHorizonAuthorityBinding::new(1, Some(1))),
            })
            .await
            .unwrap();
        assert!(matches!(
            heartbeat.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::RecordNodeHeartbeat
            )
        ));
        let completion = authority
            .submit_control_plane_command(ControlPlaneCommand::CompletePgPeering {
                pg_id: PgId::new(1),
                primary: NodeId::new(1),
                node_incarnation: 1,
                complete_at_ms: 44_102,
            })
            .await
            .unwrap();
        assert!(
            matches!(
                completion.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::CompletePgPeering
                )
            ),
            "unexpected scoped-read completion outcome: {:?}",
            completion.outcome()
        );
        assert!(authority
            .linearized_runtime_map_snapshot(44_103)
            .await
            .is_err());

        let scoped = authority
            .linearized_serving_pg_runtime_map_snapshot(PgId::new(0), 44_103)
            .await
            .unwrap();
        assert_eq!(scoped.pg_routes().len(), 1);
        assert_eq!(scoped.pg_routes()[0].pg_id(), PgId::new(0));
        assert_eq!(scoped.pg_routes()[0].state(), PgState::Peering);
        assert!(scoped.freshness_proof().is_serving_authority_read());

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_linearized_authority_handle_submits_reads_and_reports_status() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            301,
            test_raft_config("control-plane-raft-linearized-authority-trait-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let authority = Arc::new(ControlPlaneRaftAuthority::new_with_log_store(
            raft,
            log_store,
            "control-plane-raft-linearized-authority-trait-test",
        ));
        authority
            .initialize_membership(BTreeMap::from([(301, BasicNode::new("node-301"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "linearized authority trait leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "linearized authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let handle = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority));
        let linearized_authority = handle.as_linearized_authority();
        let write = linearized_authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(301), "node-301".to_string())],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let runtime_map = linearized_authority
            .linearized_runtime_map_snapshot(66_000)
            .await
            .unwrap();
        let status = linearized_authority.status().await.unwrap();
        assert_eq!(
            status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert_eq!(status.current_leader(), Some(301));
        assert_eq!(status.applied(), Some(write.log_id()));
        let expected_read_index = control_plane_log_id_from_raft(
            status
                .applied()
                .expect("trait read should have an applied tip"),
        )
        .expect("trait read should be non-bootstrap");

        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(expected_read_index)
        );
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(66_000));
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(301)));

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_explicit_handles_manage_membership_and_leadership() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(2);
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-admin-authority-handle-test",
            401,
            402,
        )
        .await;
        let authority1 = Arc::new(authority1);
        let authority2 = Arc::new(authority2);
        let linearized1 = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority1));
        let admin1 = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::clone(&authority1));
        let admin2 = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::clone(&authority2));
        let status2 = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority2));
        let bootstrap1 = ControlPlaneRaftAuthorityBootstrapHandle::new(Arc::clone(&authority1));
        let lifecycle1 = ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority1));
        let lifecycle2 = ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority2));
        let command_sink = linearized1.as_linearized_authority();
        let leader_admin = admin1.as_leader_routed_admin();

        assert!(bootstrap1.is_initialized().await.unwrap());
        let bootstrap = expect_bounded_control_plane_raft(
            command_sink.submit_control_plane_command(
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(401), "node-401".to_string()),
                        (NodeId::new(402), "node-402".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                },
            ),
            operation_timeout,
            "explicit handles bootstrap command",
        )
        .await;
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        expect_bounded_control_plane_raft(
            lifecycle2.wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "explicit handles follower applied bootstrap",
            ),
            operation_timeout,
            "explicit handles wait for follower bootstrap",
        )
        .await;

        expect_bounded_control_plane_raft(
            leader_admin.transfer_leadership_to(402),
            operation_timeout,
            "explicit handles transfer leadership",
        )
        .await;
        expect_bounded_control_plane_raft(
            lifecycle2.wait_for_current_leader(
                402,
                Duration::from_secs(1),
                "explicit handles observed transferred leader",
            ),
            operation_timeout,
            "explicit handles wait for transferred leader",
        )
        .await;
        wait_for_authority_status_matching(
            &authority2,
            Duration::from_secs(1),
            "explicit handles transferred leader serving before proposal lease expiry",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        assert!(
            authority2
                .status()
                .await
                .unwrap()
                .linearized_authority_serving(),
            "serving status remains true after alpha.33's proposal lease expires"
        );
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));
        let post_expiry_write = expect_bounded_control_plane_raft(
            authority2.submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(402),
                availability: NodeAvailabilityState::Unavailable,
            }),
            operation_timeout,
            "explicit handles write after proposal lease expiry",
        )
        .await;
        assert!(matches!(
            post_expiry_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert_eq!(
            authority2.proposal_lease_retry_count_for_test(),
            1,
            "ordinary proposal should safely retry after its confirmed lease expires before dispatch"
        );

        let before_expired_dispatch = authority2.status().await.unwrap().last_log_id();
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            1_100,
        ));
        let expired_dispatch_error = expect_bounded_control_plane_raft_error(
            authority2.submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(401),
                availability: NodeAvailabilityState::Unavailable,
            }),
            operation_timeout,
            "explicit handles reject proposal after overall reconfirmation deadline",
        )
        .await;
        assert!(matches!(
            expired_dispatch_error,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                ..
            }
        ));
        assert_eq!(
            authority2.status().await.unwrap().last_log_id(),
            before_expired_dispatch,
            "an expired pre-dispatch budget must not append the command"
        );

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));
        let learner_log_id = expect_bounded_control_plane_raft(
            admin2.add_learner(403, BasicNode::new("node-403"), false),
            operation_timeout,
            "explicit handles add learner",
        )
        .await;
        assert_eq!(
            authority2.proposal_lease_retry_count_for_test(),
            1,
            "learner proposal should safely retry after its confirmed lease expires before dispatch"
        );
        expect_bounded_control_plane_raft(
            lifecycle2.wait_for_applied_log_id(
                learner_log_id,
                Duration::from_secs(1),
                "explicit handles applied learner addition",
            ),
            operation_timeout,
            "explicit handles wait for learner addition",
        )
        .await;

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));

        let membership_log_id = expect_bounded_control_plane_raft(
            admin2.replace_voters(BTreeSet::from([402]), false),
            operation_timeout,
            "explicit handles replace voters",
        )
        .await;
        assert_eq!(
            authority2.proposal_lease_retry_count_for_test(),
            1,
            "membership proposal should safely retry after its confirmed lease expires before dispatch"
        );
        expect_bounded_control_plane_raft(
            lifecycle2.wait_for_applied_log_id(
                membership_log_id,
                Duration::from_secs(1),
                "explicit handles applied voter replacement",
            ),
            operation_timeout,
            "explicit handles wait for voter replacement",
        )
        .await;

        let status = status2.status().await.unwrap();
        assert_eq!(status.current_leader(), Some(402));
        assert_eq!(
            status.effective_membership_log_id(),
            Some(membership_log_id)
        );
        assert_eq!(status.effective_voters(), &BTreeSet::from([402]));
        assert_eq!(
            status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );

        expect_bounded_control_plane_raft(
            lifecycle1.shutdown(),
            operation_timeout,
            "explicit handles shutdown removed voter",
        )
        .await;
        expect_bounded_control_plane_raft(
            lifecycle2.shutdown(),
            operation_timeout,
            "explicit handles shutdown surviving voter",
        )
        .await;
    });
}

#[test]
fn control_plane_openraft_partial_membership_transition_is_not_retried() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(2);
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-partial-membership-no-retry-test",
            451,
            452,
        )
        .await;
        let before_log_id = authority1
            .status()
            .await
            .unwrap()
            .last_log_id()
            .expect("initialized authority should have a log tip");

        let apply_gate = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let apply_gate_for_state_machine = Arc::clone(&apply_gate);
        authority1
            .raft()
            .with_state_machine(move |state_machine| {
                state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
                    apply: Some(apply_gate_for_state_machine),
                    ..ControlPlaneRaftStateMachineTestHooks::default()
                });
                Box::pin(async {})
            })
            .await
            .unwrap();

        let apply_gate_watchdog = Arc::clone(&apply_gate);
        let release_worker = thread::spawn(move || {
            apply_gate_watchdog.wait_until_entered(Duration::from_secs(1));
            thread::sleep(Duration::from_millis(350));
            apply_gate_watchdog.release();
        });
        let error = expect_bounded_control_plane_raft_error(
            authority1.replace_voters(BTreeSet::from([451]), false),
            operation_timeout,
            "partial membership transition rejects final entry after lease expiry",
        )
        .await;
        release_worker.join().unwrap();
        assert!(matches!(
            error,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                ..
            }
        ));
        assert_eq!(authority1.proposal_lease_retry_count_for_test(), 0);
        assert_eq!(
            authority1.proposal_changed_tip_rejection_count_for_test(),
            1,
            "the final-entry lease rejection should fail closed because the joint entry advanced the log"
        );

        let (last_log_id, effective_membership_log_id, joint_config) = authority1
            .raft()
            .with_raft_state(|state| {
                let effective = state.membership_state.effective();
                (
                    state.log_ids.last().copied(),
                    *effective.log_id(),
                    effective.membership().get_joint_config().to_vec(),
                )
            })
            .await
            .unwrap();
        let last_log_id = last_log_id.expect("joint membership entry should remain in the log");
        assert_eq!(last_log_id.index(), before_log_id.index() + 1);
        assert_eq!(effective_membership_log_id, Some(last_log_id));
        assert_eq!(joint_config.len(), 2);
        assert!(joint_config.contains(&BTreeSet::from([451, 452])));
        assert!(joint_config.contains(&BTreeSet::from([451])));

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            authority1.status().await.unwrap().last_log_id(),
            Some(last_log_id),
            "the wrapper must not submit a final or repeated membership entry after returning the ambiguous result"
        );

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_authority_directories_route_by_node_id() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(5);
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-authority-capability-directory-test",
            411,
            412,
        )
        .await;
        let authority1 = Arc::new(authority1);
        let authority2 = Arc::new(authority2);
        let directory = InMemoryAuthorityCapabilityDirectory::default();
        directory.register(411, Arc::clone(&authority1));
        directory.register(412, Arc::clone(&authority2));
        let bootstrap_directory =
            ControlPlaneRaftAuthorityBootstrapDirectoryHandle::new(Arc::new(directory.clone()));
        let linearized_directory =
            ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(Arc::new(directory.clone()));
        let admin_directory =
            ControlPlaneRaftLeaderRoutedAdminDirectoryHandle::new(Arc::new(directory.clone()));
        let node_lifecycle_directory =
            ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle::new(Arc::new(directory.clone()));
        let status_list_handle =
            ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(directory.clone()));
        let observer_status = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));

        let missing_bootstrap = bootstrap_directory.authority_bootstrap_for_node(499).await;
        assert!(matches!(
            missing_bootstrap,
            Err(ControlPlaneError::RpcRemote { diagnostic: message })
                if message.contains("no bootstrap node 499")
        ));
        let missing_node_lifecycle = node_lifecycle_directory
            .authority_node_lifecycle_for_node(499)
            .await;
        assert!(matches!(
            missing_node_lifecycle,
            Err(ControlPlaneError::RpcRemote { diagnostic: message })
                if message.contains("no node-lifecycle node 499")
        ));
        let missing_linearized = linearized_directory
            .linearized_authority_for_node(499)
            .await;
        assert!(matches!(
            missing_linearized,
            Err(ControlPlaneError::RpcRemote { diagnostic: message })
                if message.contains("no linearized node 499")
        ));
        let missing_admin = admin_directory.leader_routed_admin_for_node(499).await;
        assert!(matches!(
            missing_admin,
            Err(ControlPlaneError::RpcRemote { diagnostic: message })
                if message.contains("no leader-routed admin node 499")
        ));

        let leader_bootstrap = expect_bounded_control_plane_raft(
            bootstrap_directory.authority_bootstrap_for_node(411),
            operation_timeout,
            "authority bootstrap directory lookup leader",
        )
        .await;
        assert!(
            expect_bounded_control_plane_raft(
                leader_bootstrap.is_initialized(),
                operation_timeout,
                "authority bootstrap directory is initialized check",
            )
            .await
        );
        let leader_node_lifecycle = expect_bounded_control_plane_raft(
            node_lifecycle_directory.authority_node_lifecycle_for_node(411),
            operation_timeout,
            "authority node-lifecycle directory lookup leader",
        )
        .await;
        let follower_node_lifecycle = expect_bounded_control_plane_raft(
            node_lifecycle_directory.authority_node_lifecycle_for_node(412),
            operation_timeout,
            "authority node-lifecycle directory lookup follower",
        )
        .await;
        let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
            observer_status,
            status_list_handle.clone(),
            linearized_directory.clone(),
        );
        let routed_admin = ControlPlaneRaftLeaderRoutedAdminRoutingHandle::new(
            status_list_handle.clone(),
            admin_directory.clone(),
        );
        let routed_admin = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::new(routed_admin));
        let leader_routed_admin = routed_admin.as_leader_routed_admin();
        let routed_linearized_handle =
            ControlPlaneRaftAuthorityHandle::new(Arc::new(routed_client.clone()));
        let routed_linearized_authority = routed_linearized_handle.as_linearized_authority();
        wait_for_authority_status_matching(
            &authority1,
            operation_timeout,
            "authority capability directory leader serving before bootstrap command",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let bootstrap = expect_bounded_control_plane_raft(
            routed_client.submit_control_plane_command(
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(411), "node-411".to_string()),
                        (NodeId::new(412), "node-412".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                },
            ),
            operation_timeout,
            "authority capability directory bootstrap command",
        )
        .await;
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        expect_bounded_control_plane_raft(
            follower_node_lifecycle.wait_for_applied_log_id(
                bootstrap.log_id(),
                operation_timeout,
                "authority capability directory follower applied bootstrap",
            ),
            operation_timeout,
            "authority capability directory follower wait",
        )
        .await;

        expect_bounded_control_plane_raft(
            leader_routed_admin.transfer_leadership_to(412),
            operation_timeout,
            "authority capability directory routed transfer leadership",
        )
        .await;
        expect_bounded_control_plane_raft(
            follower_node_lifecycle.wait_for_current_leader(
                412,
                operation_timeout,
                "authority capability directory observed transferred leader",
            ),
            operation_timeout,
            "authority capability directory wait for transferred leader",
        )
        .await;
        expect_bounded_control_plane_raft(
            leader_node_lifecycle.wait_for_current_leader(
                412,
                operation_timeout,
                "authority capability directory observer saw transferred leader",
            ),
            operation_timeout,
            "authority capability directory wait for observer transfer view",
        )
        .await;
        let transferred_statuses = expect_bounded_control_plane_raft(
            async {
                loop {
                    let statuses = status_list_handle.authority_statuses().await?;
                    if statuses.get(&412).is_some_and(|status| {
                        status.current_leader() == Some(412)
                            && status.local_leader()
                            && status.linearized_authority_serving()
                    }) {
                        return Ok(statuses);
                    }
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                }
            },
            operation_timeout,
            "authority status-list directory statuses after transfer",
        )
        .await;
        let status_list = status_list_handle.as_status_list();
        let listed_statuses = expect_bounded_control_plane_raft(
            status_list.authority_statuses(),
            operation_timeout,
            "authority status-list handle statuses after transfer",
        )
        .await;
        assert_eq!(
            listed_statuses.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([411, 412])
        );
        assert_eq!(
            transferred_statuses
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([411, 412])
        );
        let old_leader_status = transferred_statuses
            .get(&411)
            .expect("directory should report old leader");
        assert_eq!(old_leader_status.current_leader(), Some(412));
        assert!(!old_leader_status.local_leader());
        let new_leader_status = transferred_statuses
            .get(&412)
            .expect("directory should report new leader");
        assert_eq!(new_leader_status.current_leader(), Some(412));
        assert!(new_leader_status.local_leader());
        assert!(new_leader_status.linearized_authority_serving());
        let linearized_directory_authority = expect_bounded_control_plane_raft(
            linearized_directory.linearized_authority_for_node(412),
            operation_timeout,
            "linearized authority directory lookup transferred leader",
        )
        .await;
        let linearized_directory_status = expect_bounded_control_plane_raft(
            async {
                loop {
                    let status = linearized_directory_authority.status().await?;
                    if status.node_id() == 412 && status.linearized_authority_serving() {
                        return Ok(status);
                    }
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                }
            },
            operation_timeout,
            "linearized authority directory transferred leader status",
        )
        .await;
        assert_eq!(linearized_directory_status.node_id(), 412);
        assert!(linearized_directory_status.linearized_authority_serving());
        let routed_client_serving_authority = expect_bounded_control_plane_raft(
            async {
                loop {
                    match routed_client.current_serving_linearized_authority().await {
                        Ok(authority) => return Ok(authority),
                        Err(ControlPlaneError::RpcRemote {
                            diagnostic: message,
                        }) if message.contains("no serving raft authority") => {
                            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            },
            operation_timeout,
            "authority routing handle current serving linearized authority",
        )
        .await;
        let routed_client_serving_status = expect_bounded_control_plane_raft(
            async {
                loop {
                    let status = routed_client_serving_authority.status().await?;
                    if status.linearized_authority_serving() {
                        return Ok(status);
                    }
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                }
            },
            operation_timeout,
            "authority routing handle current serving routed authority status",
        )
        .await;
        assert_eq!(routed_client_serving_status.node_id(), 412);
        assert!(routed_client_serving_status.linearized_authority_serving());
        let routed_write = expect_bounded_control_plane_raft(
            routed_linearized_authority.submit_control_plane_command(
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(411),
                    availability: NodeAvailabilityState::Unavailable,
                },
            ),
            operation_timeout,
            "authority capability directory routed command after transfer",
        )
        .await;
        assert!(matches!(
            routed_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        let routed_status = expect_bounded_control_plane_raft(
            routed_linearized_authority.status(),
            operation_timeout,
            "authority capability directory routed status after transfer",
        )
        .await;
        assert_eq!(routed_status.node_id(), 412);
        assert_eq!(routed_status.current_leader(), Some(412));
        assert!(routed_status.local_leader());
        assert!(routed_status.linearized_authority_serving());
        let observer_status = expect_bounded_control_plane_raft(
            routed_client.observer_status(),
            operation_timeout,
            "authority capability directory observer status after transfer",
        )
        .await;
        assert_eq!(observer_status.node_id(), 411);
        assert_eq!(observer_status.current_leader(), Some(412));
        assert!(!observer_status.local_leader());
        let runtime_map = expect_bounded_control_plane_raft(
            retry_transient_openraft_read_index_quorum_failure(|| {
                routed_linearized_authority.linearized_runtime_map_snapshot(91_000)
            }),
            operation_timeout,
            "authority capability directory routed runtime map read",
        )
        .await;
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(91_000));
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(411)));
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(412)));
        let direct_runtime_map = expect_bounded_control_plane_raft(
            retry_transient_openraft_read_index_quorum_failure(|| {
                routed_client_serving_authority.linearized_runtime_map_snapshot(91_001)
            }),
            operation_timeout,
            "authority capability directory current serving routed runtime map read",
        )
        .await;
        assert_eq!(
            direct_runtime_map.freshness_proof().issued_at_ms(),
            Some(91_001)
        );
        let routed_membership_log_id = expect_bounded_control_plane_raft(
            leader_routed_admin.replace_voters(BTreeSet::from([412]), false),
            operation_timeout,
            "authority capability directory routed voter replacement",
        )
        .await;
        expect_bounded_control_plane_raft(
            follower_node_lifecycle.wait_for_applied_log_id(
                routed_membership_log_id,
                operation_timeout,
                "authority capability directory routed voter replacement applied",
            ),
            operation_timeout,
            "authority capability directory wait for routed voter replacement",
        )
        .await;
        let routed_membership_status = expect_bounded_control_plane_raft(
            routed_client.status(),
            operation_timeout,
            "authority capability directory routed status after voter replacement",
        )
        .await;
        assert_eq!(routed_membership_status.node_id(), 412);
        assert_eq!(
            routed_membership_status.effective_membership_log_id(),
            Some(routed_membership_log_id)
        );
        assert_eq!(
            routed_membership_status.effective_voters(),
            &BTreeSet::from([412])
        );
        assert!(routed_membership_status.local_leader());
        assert!(routed_membership_status.linearized_authority_serving());

        expect_bounded_control_plane_raft(
            leader_node_lifecycle.shutdown(),
            operation_timeout,
            "authority capability directory shutdown old leader",
        )
        .await;
        expect_bounded_control_plane_raft(
            follower_node_lifecycle.shutdown(),
            operation_timeout,
            "authority capability directory shutdown transferred leader",
        )
        .await;
    });
}

#[test]
fn control_plane_openraft_authority_capability_directory_rejects_multiple_serving_authorities() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(2);
        let network = InMemoryRaftNetworkFactory::default();
        let log_store1 = ControlPlaneRaftLogStore::empty();
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            421,
            test_raft_config("control-plane-raft-directory-multiple-serving-test-421"),
            network.clone(),
            log_store1.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let log_store2 = ControlPlaneRaftLogStore::empty();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            422,
            test_raft_config("control-plane-raft-directory-multiple-serving-test-422"),
            network,
            log_store2.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority1 = Arc::new(ControlPlaneRaftAuthority::new_with_log_store(
            raft1,
            log_store1,
            "control-plane-raft-directory-multiple-serving-test-421",
        ));
        let authority2 = Arc::new(ControlPlaneRaftAuthority::new_with_log_store(
            raft2,
            log_store2,
            "control-plane-raft-directory-multiple-serving-test-422",
        ));
        authority1
            .initialize_membership(BTreeMap::from([(421, BasicNode::new("node-421"))]))
            .await
            .unwrap();
        authority2
            .initialize_membership(BTreeMap::from([(422, BasicNode::new("node-422"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "directory first independent leader").await;
        wait_for_local_leader(authority2.raft(), "directory second independent leader").await;
        wait_for_authority_status_matching(
            &authority1,
            operation_timeout,
            "directory first independent leader serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        wait_for_authority_status_matching(
            &authority2,
            operation_timeout,
            "directory second independent leader serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let directory = InMemoryAuthorityCapabilityDirectory::default();
        directory.register(421, Arc::clone(&authority1));
        directory.register(422, Arc::clone(&authority2));
        let linearized_directory =
            ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(Arc::new(directory.clone()));
        let status_list =
            ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(directory.clone()));
        let observer_status = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));
        let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
            observer_status,
            status_list,
            linearized_directory,
        );

        let err = expect_bounded_control_plane_raft_error(
            routed_client.current_serving_linearized_authority(),
            operation_timeout,
            "routing handle rejects multiple serving authorities",
        )
        .await;
        assert!(matches!(
            err,
            ControlPlaneError::RpcRemote { diagnostic: message }
                if message.contains("multiple serving raft authorities")
        ));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_routing_rejects_mismatched_linearized_directory_handle() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(2);
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-routing-mismatched-directory-test",
            451,
            452,
        )
        .await;
        let authority1 = Arc::new(authority1);
        let authority2 = Arc::new(authority2);
        wait_for_authority_status_matching(
            &authority1,
            operation_timeout,
            "mismatched directory selected leader serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let status_directory = InMemoryAuthorityCapabilityDirectory::default();
        status_directory.register(451, Arc::clone(&authority1));
        status_directory.register(452, Arc::clone(&authority2));
        let status_list =
            ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(status_directory));
        let observer_status = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));
        let mismatched_directory = FixedLinearizedAuthorityDirectory {
            authority: ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority2)),
        };
        let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
            observer_status,
            status_list,
            ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(Arc::new(mismatched_directory)),
        );

        let err = expect_bounded_control_plane_raft_error(
            routed_client.current_serving_linearized_authority(),
            operation_timeout,
            "routing handle rejects mismatched linearized directory handle",
        )
        .await;
        assert!(matches!(
            err,
            ControlPlaneError::RpcRemote { diagnostic: message }
                if message.contains(
                    "linearized authority directory returned node 452 for selected serving node 451"
                )
        ));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_read_index_requires_leader() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-read-index-non-leader-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let authority = ControlPlaneRaftAuthority::new_with_log_store(
            raft,
            log_store,
            "control-plane-raft-read-index-non-leader-test",
        );
        authority
            .initialize_membership(BTreeMap::from([
                (1, BasicNode::new("node-1")),
                (2, BasicNode::new("node-2")),
            ]))
            .await
            .unwrap();

        let err = authority
            .raft()
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .unwrap_err();
        let forward_to_leader = err.forward_to_leader().expect("read should need leader");
        assert_eq!(forward_to_leader.leader_id, None);
        assert_eq!(forward_to_leader.leader_node, None);

        let err = authority
            .linearized_runtime_map_snapshot(44_000)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                ..
            }
        ));

        let err = authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ControlPlaneError::AuthorityNotServing));

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restarted_stale_leader_rejects_command_without_quorum() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store: _,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities(
            "control-plane-raft-stale-restarted-leader-test",
            91,
            92,
            93,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(91), "node-91".to_string()),
                    (NodeId::new(92), "node-92".to_string()),
                    (NodeId::new(93), "node-93".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "second voter applied bootstrap before replacement election",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before replacement election",
            )
            .await
            .unwrap();

        let restart_artifact =
            capture_openraft_restart_artifact(&leader_log_store, &authority1).await;
        authority1.transfer_leadership_to(92).await.unwrap();
        authority2
            .wait_for_current_leader(
                92,
                Duration::from_secs(1),
                "replacement voter observed transferred leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority2,
            Duration::from_secs(1),
            "replacement voter became serving after transfer",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        authority1.shutdown().await.unwrap();
        network.unregister(91);

        let replacement_status = authority2.status().await.unwrap();
        assert_eq!(replacement_status.current_leader(), Some(92));

        let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
        let restored_log_store_for_status = restored_log_store.clone();
        let restarted_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            91,
            config,
            UnreachableRaftNetworkFactory,
            restored_log_store,
            restored_state_machine,
        )
        .await
        .unwrap();
        let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
            restarted_raft,
            restored_log_store_for_status,
            "test-cluster",
        );
        restarted_authority
            .wait_for_current_leader(
                91,
                Duration::from_secs(1),
                "stale restarted leader restored its local leadership view",
            )
            .await
            .unwrap();
        let stale_status = restarted_authority.status().await.unwrap();
        assert_eq!(stale_status.current_leader(), Some(91));
        assert!(stale_status.linearized_authority_serving());
        let before = restarted_authority
            .current_control_plane_snapshot()
            .await
            .unwrap();

        let confirmation_error = expect_bounded_control_plane_raft_error(
            restarted_authority.confirmed_linearized_authority_status(),
            Duration::from_secs(2),
            "stale restarted leader bounds quorum authority confirmation",
        )
        .await;
        assert!(matches!(
            &confirmation_error,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                ..
            }
        ));

        let error = expect_bounded_control_plane_raft_error(
            restarted_authority.submit_control_plane_command(
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(92),
                    availability: NodeAvailabilityState::Unavailable,
                },
            ),
            Duration::from_secs(2),
            "stale restarted leader rejects command without quorum authority",
        )
        .await;
        assert!(
            matches!(
                &error,
                ControlPlaneError::OpenRaftOperation {
                    kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                    ..
                }
            ),
            "stale restarted leader returned unexpected error: {error:?}"
        );
        assert_eq!(
            restarted_authority
                .current_control_plane_snapshot()
                .await
                .unwrap(),
            before
        );

        restarted_authority.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        authority3.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_two_node_client_write_replicates_to_follower() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-two-node-replication-test",
            101,
            102,
        )
        .await;

        let follower_error = tokio::time::timeout(
            Duration::from_secs(1),
            authority2.submit_control_plane_command(
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(102), "node-102".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ),
        )
        .await
        .expect("follower command submission should fail without waiting for Raft")
        .unwrap_err();
        assert!(matches!(
            follower_error,
            ControlPlaneError::AuthorityNotServing
        ));

        let write = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(101), "node-101".to_string()),
                    (NodeId::new(102), "node-102".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        authority2
            .wait_for_applied_log_id(
                write.log_id(),
                Duration::from_secs(1),
                "two-node follower applied client write",
            )
            .await
            .unwrap();
        let follower_state = authority2
            .raft()
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let node_ids = state_machine
                    .inner()
                    .snapshot()
                    .nodes()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>();
                Box::pin(async move { (last_applied, node_ids) })
            })
            .await
            .unwrap();
        assert_eq!(follower_state.0, Some(write.log_id()));
        assert_eq!(follower_state.1, vec![NodeId::new(101), NodeId::new(102)]);

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_snapshot_purge_requires_serving_authority() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-snapshot-purge-serving-test",
            111,
            112,
        )
        .await;

        let write = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(111), "node-111".to_string()),
                    (NodeId::new(112), "node-112".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                write.log_id(),
                Duration::from_secs(1),
                "snapshot-purge follower applied bootstrap before rejection",
            )
            .await
            .unwrap();

        let follower_error = authority2.trigger_snapshot_applied().await.unwrap_err();
        assert!(matches!(
            follower_error,
            ControlPlaneError::RpcRemote { diagnostic: message }
                if message.contains("requires the current serving authority")
                    && message.contains("NotLocalLeader")
        ));

        let snapshot_log_id = authority1
            .trigger_snapshot_applied()
            .await
            .unwrap()
            .expect("serving authority snapshot should have an applied log id");
        assert_eq!(snapshot_log_id.index(), write.log_id().index());
        authority1
            .purge_log_through_snapshot(snapshot_log_id)
            .await
            .unwrap();

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_manual_snapshot_does_not_purge_before_explicit_trigger() {
    ControlPlaneRaftTypeConfig::run(async {
        let node_id = 121;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            "control-plane-raft-manual-snapshot-no-implicit-purge-test",
            node_id,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(node_id)
            .await
            .unwrap();
        authority
            .wait_for_current_leader(
                node_id,
                Duration::from_secs(1),
                "manual snapshot no-implicit-purge leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            Duration::from_secs(1),
            "manual snapshot no-implicit-purge serving state",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        for _ in 0..1_300 {
            let rejected = authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(999),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
                .unwrap();
            assert!(matches!(
                rejected.outcome(),
                ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                    node_id: 999
                })
            ));
        }
        let snapshot_log_id = authority
            .trigger_snapshot_applied()
            .await
            .unwrap()
            .expect("rejected entries should advance the applied snapshot position");
        assert!(snapshot_log_id.index() > 1_000);
        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            authority.status().await.unwrap().last_purged_log_id(),
            None,
            "manual snapshot completion must not trigger OpenRaft policy purge"
        );

        authority
            .purge_log_through_snapshot(snapshot_log_id)
            .await
            .unwrap();
        assert_eq!(
            authority.status().await.unwrap().last_purged_log_id(),
            Some(snapshot_log_id)
        );
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_snapshot_purge_waits_for_durable_publication() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("raft.state");
        let wal_path = tmp.path().join("raft.wal");
        let node_id = 122;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
            "control-plane-raft-purge-durable-publication-test",
            node_id,
            &artifact_path,
            &wal_path,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(node_id)
            .await
            .unwrap();
        authority
            .wait_for_current_leader(
                node_id,
                Duration::from_secs(1),
                "durable-publication purge leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            Duration::from_secs(1),
            "durable-publication purge serving state",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let snapshot_log_id = authority
            .trigger_snapshot_applied()
            .await
            .unwrap()
            .expect("membership should establish a snapshot log id");
        let gate = TestWalFileSyncGate::install_durable_publication(wal_path);
        let operation_completed = Arc::new(AtomicBool::new(false));
        let watchdog = gate.pending_operation_watchdog(Arc::clone(&operation_completed));

        authority
            .purge_log_through_snapshot(snapshot_log_id)
            .await
            .unwrap();
        operation_completed.store(true, Ordering::SeqCst);
        assert!(
            watchdog.join().expect("purge watchdog should not panic"),
            "snapshot purge returned after accepted publication but before durable publication"
        );
        let status = authority.status().await.unwrap();
        assert_eq!(status.last_purged_log_id(), Some(snapshot_log_id));
        assert_eq!(status.durable_last_purged_log_id(), Some(snapshot_log_id));

        drop(gate);
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_snapshot_purge_reports_post_sync_poison() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("raft.state");
        let wal_path = tmp.path().join("raft.wal");
        let node_id = 123;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
            "control-plane-raft-purge-post-sync-poison-test",
            node_id,
            &artifact_path,
            &wal_path,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(node_id)
            .await
            .unwrap();
        authority
            .wait_for_current_leader(
                node_id,
                Duration::from_secs(1),
                "post-sync poison purge leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            Duration::from_secs(1),
            "post-sync poison purge serving state",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let snapshot_log_id = authority
            .trigger_snapshot_applied()
            .await
            .unwrap()
            .expect("membership should establish a snapshot log id");
        *CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_PARENT_SYNC
            .lock()
            .expect("test WAL parent-sync fault lock should not be poisoned") = Some(wal_path);
        let error = authority
            .purge_log_through_snapshot(snapshot_log_id)
            .await
            .expect_err("post-sync purge poison must override the durable watermark");
        assert!(
            matches!(error, ControlPlaneError::RpcRemote { diagnostic: ref message }
                    if message.contains("snapshot purge WAL durability failed")
                        && message.contains("WAL append failed after file sync")),
            "unexpected post-sync purge error: {error:?}"
        );
        let status = authority
            .log_store
            .as_ref()
            .unwrap()
            .status_snapshot()
            .unwrap();
        assert_eq!(
            status.durable_last_purged_log_id,
            Some(snapshot_log_id),
            "the regression requires poison and the target durable watermark together"
        );
        assert!(status.durability.wal_poisoned.is_some());

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_two_node_rejected_command_replicates_without_mutation() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-two-node-rejected-command-test",
            401,
            402,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(401), "node-401".to_string()),
                    (NodeId::new(402), "node-402".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let rejected = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(499),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .unwrap();
        assert_eq!(rejected.log_id().index(), bootstrap.log_id().index() + 1);
        assert!(matches!(
            rejected.outcome(),
            ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                node_id
            }) if *node_id == 499
        ));

        authority2
            .wait_for_applied_log_id(
                rejected.log_id(),
                Duration::from_secs(1),
                "two-node follower applied rejected command",
            )
            .await
            .unwrap();
        let follower_state = authority2
            .raft()
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let snapshot = state_machine.inner().snapshot().clone();
                Box::pin(async move { (last_applied, snapshot) })
            })
            .await
            .unwrap();
        assert_eq!(follower_state.0, Some(rejected.log_id()));
        assert!(follower_state.1.node(NodeId::new(401)).is_some());
        assert!(follower_state.1.node(NodeId::new(402)).is_some());
        assert!(follower_state.1.node(NodeId::new(499)).is_none());

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_leader_transfer_fences_old_leader() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2) =
            initialized_two_node_authorities("control-plane-raft-leader-transfer-test", 701, 702)
                .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(701), "node-701".to_string()),
                    (NodeId::new(702), "node-702".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "new leader candidate applied bootstrap before transfer",
            )
            .await
            .unwrap();

        authority1.transfer_leadership_to(702).await.unwrap();
        authority1
            .wait_for_current_leader(
                702,
                Duration::from_secs(1),
                "old leader observed transferred leader",
            )
            .await
            .unwrap();
        authority2
            .wait_for_current_leader(
                702,
                Duration::from_secs(1),
                "new leader observed transferred leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority2,
            Duration::from_secs(1),
            "transferred leader committed its current-term entry",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let old_leader_read_err = authority1
            .linearized_runtime_map_snapshot(77_000)
            .await
            .unwrap_err();
        assert!(matches!(
            old_leader_read_err,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                ..
            }
        ));

        let old_leader_err = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(701),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap_err();
        assert!(old_leader_err.is_control_plane_leader_routing_rejection());
        let old_leader_replace_voters_err = authority1
            .replace_voters(BTreeSet::from([701]), false)
            .await
            .unwrap_err();
        assert!(old_leader_replace_voters_err.is_control_plane_leader_routing_rejection());
        let old_leader_add_learner_err = authority1
            .add_learner(703, BasicNode::new("node-703"), false)
            .await
            .unwrap_err();
        assert!(old_leader_add_learner_err.is_control_plane_leader_routing_rejection());

        let follow_up = authority2
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(702),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            follow_up.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert!(follow_up.log_id().index() > bootstrap.log_id().index());

        authority1
            .wait_for_applied_log_id(
                follow_up.log_id(),
                Duration::from_secs(1),
                "old leader follower applied post-transfer command",
            )
            .await
            .unwrap();
        let old_status = authority1.status().await.unwrap();
        let new_status = authority2.status().await.unwrap();
        assert_eq!(old_status.current_leader(), Some(702));
        assert_eq!(new_status.current_leader(), Some(702));
        assert_eq!(old_status.server_state(), ServerState::Follower);
        assert_eq!(new_status.server_state(), ServerState::Leader);
        assert!(!old_status.local_leader());
        assert!(old_status.effective_voter());
        assert_eq!(
            old_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
        );
        assert!(!old_status.linearized_authority_serving());
        assert!(new_status.local_leader());
        assert!(new_status.effective_voter());
        assert_eq!(
            new_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert!(new_status.linearized_authority_serving());
        assert_eq!(old_status.applied(), Some(follow_up.log_id()));
        assert_eq!(new_status.applied(), Some(follow_up.log_id()));

        let runtime_map = authority2
            .linearized_runtime_map_snapshot(78_000)
            .await
            .unwrap();
        let expected_read_index = control_plane_log_id_from_raft(follow_up.log_id())
            .expect("post-transfer command log id should be non-bootstrap");
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(expected_read_index)
        );
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(78_000));
        assert!(runtime_map.freshness_proof().is_serving_authority_read());

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_two_node_membership_change_updates_state_machine() {
    ControlPlaneRaftTypeConfig::run(async {
        let operation_timeout = Duration::from_secs(2);
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-two-node-membership-change-test",
            501,
            502,
        )
        .await;

        let bootstrap = expect_bounded_control_plane_raft(
            authority1.submit_control_plane_command(
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(501), "node-501".to_string()),
                        (NodeId::new(502), "node-502".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                },
            ),
            operation_timeout,
            "two-node membership test bootstrap command",
        )
        .await;
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "removed voter candidate applied bootstrap before serving",
            )
            .await
            .unwrap();

        expect_bounded_control_plane_raft(
            authority1.transfer_leadership_to(502),
            operation_timeout,
            "two-node membership test transfer leadership to removed voter",
        )
        .await;
        authority1
            .wait_for_current_leader(
                502,
                Duration::from_secs(1),
                "old leader observed removed voter leadership",
            )
            .await
            .unwrap();
        authority2
            .wait_for_current_leader(
                502,
                Duration::from_secs(1),
                "removed voter became serving leader before removal",
            )
            .await
            .unwrap();

        let pre_removal_runtime_map = expect_bounded_control_plane_raft(
            authority2.linearized_runtime_map_snapshot(50_000),
            operation_timeout,
            "two-node membership test pre-removal read-index runtime map",
        )
        .await;
        assert!(pre_removal_runtime_map
            .freshness_proof()
            .is_serving_authority_read());
        assert_eq!(
            pre_removal_runtime_map.freshness_proof().issued_at_ms(),
            Some(50_000)
        );
        let pre_removal_write = expect_bounded_control_plane_raft(
            authority2.submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(502),
                availability: NodeAvailabilityState::Unavailable,
            }),
            operation_timeout,
            "two-node membership test pre-removal write through removed voter",
        )
        .await;
        assert!(matches!(
            pre_removal_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));

        expect_bounded_control_plane_raft(
            authority2.transfer_leadership_to(501),
            operation_timeout,
            "two-node membership test transfer leadership back before removal",
        )
        .await;
        authority1
            .wait_for_current_leader(
                501,
                Duration::from_secs(1),
                "surviving voter became leader before membership removal",
            )
            .await
            .unwrap();
        authority2
            .wait_for_current_leader(
                501,
                Duration::from_secs(1),
                "removed voter observed surviving leader before removal",
            )
            .await
            .unwrap();

        let membership_log_id = expect_bounded_control_plane_raft(
            authority1.replace_voters(BTreeSet::from([501]), false),
            operation_timeout,
            "two-node membership test remove previously serving voter from membership",
        )
        .await;
        authority1
            .wait_for_applied_log_id(
                membership_log_id,
                Duration::from_secs(1),
                "two-node leader applied membership change",
            )
            .await
            .unwrap();
        authority1
            .wait_for_current_leader(
                501,
                Duration::from_secs(1),
                "remaining voter stayed leader after membership removal",
            )
            .await
            .unwrap();
        let status = expect_bounded_control_plane_raft(
            authority1.status(),
            operation_timeout,
            "two-node membership test status after membership removal",
        )
        .await;
        assert_eq!(status.current_leader(), Some(501));
        assert_eq!(
            status.effective_membership_log_id(),
            Some(membership_log_id)
        );
        assert_eq!(status.effective_voters(), &BTreeSet::from([501]));
        assert_eq!(
            status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert_eq!(status.applied_membership_log_id(), Some(membership_log_id));
        assert_eq!(status.applied_voters(), &BTreeSet::from([501]));

        let removed_read_err = expect_bounded_control_plane_raft_error(
            authority2.linearized_runtime_map_snapshot(50_100),
            operation_timeout,
            "two-node membership test removed voter read-index runtime map",
        )
        .await;
        assert!(matches!(
            removed_read_err,
            ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                ..
            }
        ));
        let removed_write_err = expect_bounded_control_plane_raft_error(
            authority2.submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(502),
                availability: NodeAvailabilityState::Unavailable,
            }),
            operation_timeout,
            "two-node membership test removed voter write",
        )
        .await;
        assert!(removed_write_err.is_control_plane_leader_routing_rejection());

        let follow_up = expect_bounded_control_plane_raft(
            authority1.submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(501),
                availability: NodeAvailabilityState::Unavailable,
            }),
            operation_timeout,
            "two-node membership test follow-up write through surviving voter",
        )
        .await;
        assert!(matches!(
            follow_up.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert!(follow_up.log_id().index() > membership_log_id.index());
        let follow_up_status = expect_bounded_control_plane_raft(
            authority1.status(),
            operation_timeout,
            "two-node membership test follow-up status",
        )
        .await;
        assert_eq!(follow_up_status.applied(), Some(follow_up.log_id()));
        assert_eq!(
            follow_up_status.effective_membership_log_id(),
            Some(membership_log_id)
        );
        assert_eq!(follow_up_status.effective_voters(), &BTreeSet::from([501]));
        assert_eq!(
            follow_up_status.applied_membership_log_id(),
            Some(membership_log_id)
        );
        assert_eq!(follow_up_status.applied_voters(), &BTreeSet::from([501]));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_adds_learner_then_promotes_to_voter() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2, authority3) = initialized_three_node_cluster_with_two_voters(
            "control-plane-raft-add-learner-promote-test",
            601,
            602,
            603,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(601), "node-601".to_string()),
                    (NodeId::new(602), "node-602".to_string()),
                    (NodeId::new(603), "node-603".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let learner_log_id = authority1
            .add_learner(603, BasicNode::new("node-603"), false)
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                learner_log_id,
                Duration::from_secs(1),
                "new learner applied learner membership",
            )
            .await
            .unwrap();
        let learner_status = authority3.status().await.unwrap();
        assert_eq!(learner_status.applied(), Some(learner_log_id));
        assert_eq!(
            learner_status.effective_membership_log_id(),
            Some(learner_log_id)
        );
        assert_eq!(
            learner_status.effective_voters(),
            &BTreeSet::from([601, 602])
        );
        assert_eq!(learner_status.effective_learners(), &BTreeSet::from([603]));
        assert!(!learner_status.effective_voter());
        assert!(learner_status.effective_learner());
        assert_eq!(
            learner_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
        );
        assert!(!learner_status.linearized_authority_serving());
        assert_eq!(
            learner_status.applied_membership_log_id(),
            Some(learner_log_id)
        );
        assert_eq!(learner_status.applied_voters(), &BTreeSet::from([601, 602]));
        assert_eq!(learner_status.applied_learners(), &BTreeSet::from([603]));
        assert!(!learner_status.applied_voter());
        assert!(learner_status.applied_learner());

        let promote_log_id = authority1
            .replace_voters(BTreeSet::from([601, 602, 603]), true)
            .await
            .unwrap();
        authority1
            .wait_for_applied_log_id(
                promote_log_id,
                Duration::from_secs(1),
                "leader applied learner promotion",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                promote_log_id,
                Duration::from_secs(1),
                "promoted learner applied voter membership",
            )
            .await
            .unwrap();

        let leader_status = authority1.status().await.unwrap();
        assert_eq!(
            leader_status.effective_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            leader_status.effective_voters(),
            &BTreeSet::from([601, 602, 603])
        );
        assert_eq!(leader_status.effective_learners(), &BTreeSet::new());
        assert!(leader_status.local_leader());
        assert!(leader_status.effective_voter());
        assert!(!leader_status.effective_learner());
        assert_eq!(
            leader_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert!(leader_status.linearized_authority_serving());
        assert_eq!(
            leader_status.applied_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            leader_status.applied_voters(),
            &BTreeSet::from([601, 602, 603])
        );
        assert_eq!(leader_status.applied_learners(), &BTreeSet::new());
        assert!(leader_status.applied_voter());
        assert!(!leader_status.applied_learner());

        let promoted_status = authority3.status().await.unwrap();
        assert_eq!(promoted_status.applied(), Some(promote_log_id));
        assert_eq!(
            promoted_status.effective_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            promoted_status.effective_voters(),
            &BTreeSet::from([601, 602, 603])
        );
        assert_eq!(promoted_status.effective_learners(), &BTreeSet::new());
        assert!(promoted_status.effective_voter());
        assert!(!promoted_status.effective_learner());
        assert_eq!(
            promoted_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
        );
        assert!(!promoted_status.linearized_authority_serving());
        assert_eq!(
            promoted_status.applied_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            promoted_status.applied_voters(),
            &BTreeSet::from([601, 602, 603])
        );
        assert_eq!(promoted_status.applied_learners(), &BTreeSet::new());
        assert!(promoted_status.applied_voter());
        assert!(!promoted_status.applied_learner());

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        authority3.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_promoted_voter_restart_preserves_membership() {
    ControlPlaneRaftTypeConfig::run(async {
        let network = InMemoryRaftNetworkFactory::default();
        let config = test_raft_config("control-plane-raft-promoted-voter-restart-test");
        let log_store1 = ControlPlaneRaftLogStore::empty();
        let log_store2 = ControlPlaneRaftLogStore::empty();
        let log_store3 = ControlPlaneRaftLogStore::empty();
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            621,
            config.clone(),
            network.clone(),
            log_store1.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            622,
            config.clone(),
            network.clone(),
            log_store2.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            623,
            config.clone(),
            network.clone(),
            log_store3.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(621, raft1.clone());
        network.register(622, raft2.clone());
        network.register(623, raft3.clone());
        let authority1 = ControlPlaneRaftAuthority::new_with_log_store(
            raft1,
            log_store1,
            "control-plane-raft-promoted-voter-restart-test",
        );
        let authority2 = ControlPlaneRaftAuthority::new_with_log_store(
            raft2,
            log_store2,
            "control-plane-raft-promoted-voter-restart-test",
        );
        let authority3 = ControlPlaneRaftAuthority::new_with_log_store(
            raft3,
            log_store3.clone(),
            "test-cluster",
        );

        authority1
            .initialize_membership(BTreeMap::from([
                (621, BasicNode::new("node-621")),
                (622, BasicNode::new("node-622")),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(
            authority1.raft(),
            "two-voter cluster initialized before promoted-voter restart",
        )
        .await;
        wait_for_authority_status_matching(
            &authority1,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "two-voter leader applies initialization before promoted-voter restart",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(621), "node-621".to_string()),
                    (NodeId::new(622), "node-622".to_string()),
                    (NodeId::new(623), "node-623".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let learner_log_id = authority1
            .add_learner(623, BasicNode::new("node-623"), false)
            .await
            .unwrap();
        authority3
            .wait_for_applied_index_at_least(
                learner_log_id.index(),
                Duration::from_secs(1),
                "restart candidate applied learner membership",
            )
            .await
            .unwrap();

        let promote_log_id = authority1
            .replace_voters(BTreeSet::from([621, 622, 623]), true)
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                promote_log_id,
                Duration::from_secs(1),
                "restart candidate applied voter promotion",
            )
            .await
            .unwrap();
        let pre_restart_status = authority3.status().await.unwrap();
        assert_eq!(pre_restart_status.applied(), Some(promote_log_id));
        assert_eq!(
            pre_restart_status.effective_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            pre_restart_status.effective_voters(),
            &BTreeSet::from([621, 622, 623])
        );
        assert_eq!(
            pre_restart_status.applied_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            pre_restart_status.applied_voters(),
            &BTreeSet::from([621, 622, 623])
        );

        let restart_artifact = capture_openraft_restart_artifact(&log_store3, &authority3).await;
        authority3.shutdown().await.unwrap();
        network.unregister(623);

        let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
        let restored_log_store_for_status = restored_log_store.clone();
        let restarted_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            623,
            config,
            network.clone(),
            restored_log_store,
            restored_state_machine,
        )
        .await
        .unwrap();
        network.register(623, restarted_raft.clone());
        let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
            restarted_raft,
            restored_log_store_for_status,
            "test-cluster",
        );

        let restarted_status = restarted_authority.status().await.unwrap();
        assert_eq!(restarted_status.applied(), Some(promote_log_id));
        assert_eq!(
            restarted_status.effective_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            restarted_status.effective_voters(),
            &BTreeSet::from([621, 622, 623])
        );
        assert_eq!(
            restarted_status.applied_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            restarted_status.applied_voters(),
            &BTreeSet::from([621, 622, 623])
        );

        let post_restart_write = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(623),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            post_restart_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert!(post_restart_write.log_id().index() > promote_log_id.index());
        restarted_authority
            .wait_for_applied_index_at_least(
                post_restart_write.log_id().index(),
                Duration::from_secs(1),
                "restarted promoted voter applied post-restart command",
            )
            .await
            .unwrap();
        let caught_up_status = restarted_authority.status().await.unwrap();
        assert_eq!(
            caught_up_status.applied(),
            Some(post_restart_write.log_id())
        );
        assert_eq!(
            caught_up_status.effective_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            caught_up_status.effective_voters(),
            &BTreeSet::from([621, 622, 623])
        );
        assert_eq!(
            caught_up_status.applied_membership_log_id(),
            Some(promote_log_id)
        );
        assert_eq!(
            caught_up_status.applied_voters(),
            &BTreeSet::from([621, 622, 623])
        );
        let node623_availability = restarted_authority
            .raft()
            .with_state_machine(|state_machine| {
                let availability = state_machine
                    .inner()
                    .snapshot()
                    .node(NodeId::new(623))
                    .map(|node| node.availability());
                Box::pin(async move { availability })
            })
            .await
            .unwrap();
        assert_eq!(
            node623_availability,
            Some(NodeAvailabilityState::Unavailable)
        );

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        restarted_authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_two_node_read_index_runtime_map_uses_quorum_applied_tip() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-two-node-read-index-runtime-map-test",
            201,
            202,
        )
        .await;

        let write = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(201), "node-201".to_string()),
                    (NodeId::new(202), "node-202".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                write.log_id(),
                Duration::from_secs(1),
                "two-node follower applied before read-index",
            )
            .await
            .unwrap();

        let runtime_map = authority1
            .linearized_runtime_map_snapshot(55_000)
            .await
            .unwrap();
        let applied_log_id = authority1
            .status()
            .await
            .unwrap()
            .applied()
            .expect("read-index should have an applied tip");
        assert!(applied_log_id.index() >= write.log_id().index());
        let expected_read_index = control_plane_log_id_from_raft(applied_log_id)
            .expect("read-index should be non-bootstrap");

        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(expected_read_index)
        );
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(55_000));
        assert!(runtime_map.freshness_proof().is_serving_authority_read());
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(201)));
        assert!(runtime_map
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(202)));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restarted_follower_catches_up_committed_prefix() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store: _,
            third_log_store,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities_with_config(
            experimental_raft_config(
                "control-plane-raft-follower-restart-catch-up-test",
                ExperimentalRaftTimerMode::Manual,
            )
            .unwrap(),
            801,
            802,
            803,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(801), "node-801".to_string()),
                    (NodeId::new(802), "node-802".to_string()),
                    (NodeId::new(803), "node-803".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before restart",
            )
            .await
            .unwrap();

        let restart_artifact =
            capture_openraft_restart_artifact(&third_log_store, &authority3).await;
        authority3.shutdown().await.unwrap();
        network.unregister(803);

        let mut offline_write = None;
        for update in 0..=ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES {
            let write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(802),
                    availability: if update % 2 == 0 {
                        NodeAvailabilityState::Unavailable
                    } else {
                        NodeAvailabilityState::Healthy
                    },
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            offline_write = Some(write);
        }
        let offline_write = offline_write.expect("offline suffix contains commands");
        assert!(
            offline_write.log_id().index() - bootstrap.log_id().index()
                > ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES as u64
        );
        authority2
            .wait_for_applied_index_at_least(
                offline_write.log_id().index(),
                Duration::from_secs(3),
                "second voter applied command committed while third voter was down",
            )
            .await
            .unwrap();

        let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
        let restored_log_store_for_status = restored_log_store.clone();
        let restarted_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            803,
            config,
            network.clone(),
            restored_log_store,
            restored_state_machine,
        )
        .await
        .unwrap();
        network.register(803, restarted_raft.clone());
        let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
            restarted_raft,
            restored_log_store_for_status,
            "test-cluster",
        );

        let catch_up_trigger = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(803),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            catch_up_trigger.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert!(catch_up_trigger.log_id().index() > offline_write.log_id().index());

        restarted_authority
            .wait_for_applied_index_at_least(
                catch_up_trigger.log_id().index(),
                Duration::from_secs(3),
                "restarted third voter caught up missing committed prefix",
            )
            .await
            .unwrap();
        let restarted_status = restarted_authority.status().await.unwrap();
        assert!(
            network.max_append_entries_seen()
                <= usize::try_from(CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES).unwrap(),
            "restart catch-up exceeded the configured replication batch"
        );
        assert_eq!(restarted_status.current_leader(), Some(801));
        assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
        assert_eq!(
            restarted_status.effective_voters(),
            &BTreeSet::from([801, 802, 803])
        );

        let restarted_state = restarted_authority
            .raft()
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot();
                let node_ids = snapshot
                    .nodes()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>();
                let node802_availability = snapshot
                    .node(NodeId::new(802))
                    .map(|node| node.availability());
                let node802_administratively_available = snapshot
                    .node(NodeId::new(802))
                    .map(|node| node.administratively_available());
                let node803_availability = snapshot
                    .node(NodeId::new(803))
                    .map(|node| node.availability());
                let node803_administratively_available = snapshot
                    .node(NodeId::new(803))
                    .map(|node| node.administratively_available());
                Box::pin(async move {
                    (
                        node_ids,
                        node802_availability,
                        node802_administratively_available,
                        node803_availability,
                        node803_administratively_available,
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(
            restarted_state.0,
            vec![NodeId::new(801), NodeId::new(802), NodeId::new(803)]
        );
        assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
        assert_eq!(restarted_state.2, Some(false));
        assert_eq!(restarted_state.3, Some(NodeAvailabilityState::Unavailable));
        assert_eq!(restarted_state.4, Some(false));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        restarted_authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restarted_follower_catches_up_from_leader_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities_with_config(
            test_raft_config_with_log_reversion(
                "control-plane-raft-follower-snapshot-catch-up-test",
                Some(true),
            ),
            1101,
            1102,
            1103,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1101), "node-1101".to_string()),
                    (NodeId::new(1102), "node-1102".to_string()),
                    (NodeId::new(1103), "node-1103".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before snapshot catch-up restart",
            )
            .await
            .unwrap();

        let restart_artifact =
            capture_openraft_restart_artifact(&third_log_store, &authority3).await;

        let offline_write = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1102),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            offline_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        authority2
            .wait_for_applied_index_at_least(
                offline_write.log_id().index(),
                Duration::from_secs(1),
                "second voter applied command before leader snapshot purge",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_index_at_least(
                offline_write.log_id().index(),
                Duration::from_secs(1),
                "third voter applied command before leader snapshot purge",
            )
            .await
            .unwrap();

        let mut snapshot_progress = authority1.raft().watch_snapshot_progress();
        authority1.raft().trigger().snapshot().await.unwrap();
        snapshot_progress
            .wait_until_ge(&Some(offline_write.log_id()))
            .await
            .unwrap();
        let leader_snapshot = authority1.raft().get_snapshot().await.unwrap().unwrap();
        assert_eq!(
            leader_snapshot.meta.last_log_id,
            Some(offline_write.log_id())
        );

        authority1
            .raft()
            .trigger()
            .purge_log(offline_write.log_id().index())
            .await
            .unwrap();
        wait_for_log_purged_to(
            &leader_log_store,
            offline_write.log_id(),
            "leader purged log prefix covered by snapshot",
        )
        .await;
        let leader_status = authority1.status().await.unwrap();
        let leader_vote = leader_status
            .persisted_vote()
            .expect("leader should report its persisted vote");
        assert!(leader_vote.committed);
        assert_eq!(leader_vote.leader_id.node_id, 1101);
        assert_eq!(
            leader_status.current_term(),
            Some(offline_write.log_id().committed_leader_id().term)
        );
        assert_eq!(
            leader_status.last_purged_log_id(),
            Some(offline_write.log_id())
        );

        authority3.shutdown().await.unwrap();
        network.unregister(1103);

        let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
        let restored_log_store_for_status = restored_log_store.clone();
        let restarted_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1103,
            config,
            network.clone(),
            restored_log_store,
            restored_state_machine,
        )
        .await
        .unwrap();
        network.register(1103, restarted_raft.clone());
        let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
            restarted_raft,
            restored_log_store_for_status,
            "test-cluster",
        );
        authority1
            .raft()
            .trigger()
            .allow_next_revert(&1103, true)
            .await
            .unwrap()
            .unwrap();

        let catch_up_trigger = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1103),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            catch_up_trigger.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));

        restarted_authority
            .wait_for_applied_index_at_least(
                catch_up_trigger.log_id().index(),
                Duration::from_secs(1),
                "restarted third voter caught up through leader snapshot",
            )
            .await
            .unwrap();
        let restarted_status = restarted_authority.status().await.unwrap();
        assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
        let restarted_vote = restarted_status
            .persisted_vote()
            .expect("restarted follower should retain its persisted vote");
        assert_eq!(restarted_vote.leader_id.node_id, 1101);
        assert_eq!(
            restarted_status.current_term(),
            Some(catch_up_trigger.log_id().committed_leader_id().term)
        );
        assert_eq!(
            restarted_status.last_purged_log_id(),
            Some(offline_write.log_id())
        );
        assert_eq!(
            restarted_status.current_snapshot(),
            Some(offline_write.log_id())
        );

        let restarted_state = restarted_authority
            .raft()
            .with_state_machine(|state_machine| {
                let snapshot_log_id = state_machine
                    .current_snapshot()
                    .and_then(|snapshot| snapshot.meta.last_log_id);
                let snapshot = state_machine.inner().snapshot();
                let node1102_availability = snapshot
                    .node(NodeId::new(1102))
                    .map(|node| node.availability());
                let node1103_availability = snapshot
                    .node(NodeId::new(1103))
                    .map(|node| node.availability());
                Box::pin(async move {
                    (
                        snapshot_log_id,
                        node1102_availability,
                        node1103_availability,
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(restarted_state.0, Some(offline_write.log_id()));
        assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
        assert_eq!(restarted_state.2, Some(NodeAvailabilityState::Unavailable));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        restarted_authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_fresh_follower_catches_up_from_leader_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store: _,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities_with_config(
            test_raft_config_with_log_reversion(
                "control-plane-raft-fresh-follower-snapshot-catch-up-test",
                Some(true),
            ),
            1201,
            1202,
            1203,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1201), "node-1201".to_string()),
                    (NodeId::new(1202), "node-1202".to_string()),
                    (NodeId::new(1203), "node-1203".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before losing local state",
            )
            .await
            .unwrap();

        let offline_write = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1202),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            offline_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        authority2
            .wait_for_applied_index_at_least(
                offline_write.log_id().index(),
                Duration::from_secs(1),
                "second voter applied command before leader snapshot purge",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_index_at_least(
                offline_write.log_id().index(),
                Duration::from_secs(1),
                "third voter applied command before losing local state",
            )
            .await
            .unwrap();

        let mut snapshot_progress = authority1.raft().watch_snapshot_progress();
        authority1.raft().trigger().snapshot().await.unwrap();
        snapshot_progress
            .wait_until_ge(&Some(offline_write.log_id()))
            .await
            .unwrap();
        let leader_snapshot = authority1.raft().get_snapshot().await.unwrap().unwrap();
        assert_eq!(
            leader_snapshot.meta.last_log_id,
            Some(offline_write.log_id())
        );

        authority1
            .raft()
            .trigger()
            .purge_log(offline_write.log_id().index())
            .await
            .unwrap();
        wait_for_log_purged_to(
            &leader_log_store,
            offline_write.log_id(),
            "leader purged log prefix before fresh follower rejoin",
        )
        .await;

        authority3.shutdown().await.unwrap();
        network.unregister(1203);

        let fresh_log_store = ControlPlaneRaftLogStore::empty();
        let fresh_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1203,
            config,
            network.clone(),
            fresh_log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(1203, fresh_raft.clone());
        let fresh_authority = ControlPlaneRaftAuthority::new_with_log_store(
            fresh_raft,
            fresh_log_store.clone(),
            "test-cluster",
        );
        authority1
            .raft()
            .trigger()
            .allow_next_revert(&1203, true)
            .await
            .unwrap()
            .unwrap();

        let catch_up_trigger = authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1203),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            catch_up_trigger.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));

        fresh_authority
            .wait_for_applied_index_at_least(
                catch_up_trigger.log_id().index(),
                Duration::from_secs(1),
                "fresh third voter caught up through leader snapshot",
            )
            .await
            .unwrap();
        let fresh_status = fresh_authority.status().await.unwrap();
        assert_eq!(fresh_status.applied(), Some(catch_up_trigger.log_id()));
        assert_eq!(
            fresh_status.last_purged_log_id(),
            Some(offline_write.log_id())
        );
        assert_eq!(
            fresh_status.current_snapshot(),
            Some(offline_write.log_id())
        );
        assert_eq!(
            RaftLogStorage::read_committed(&mut fresh_log_store.clone())
                .await
                .unwrap(),
            Some(catch_up_trigger.log_id())
        );

        let fresh_state = fresh_authority
            .raft()
            .with_state_machine(|state_machine| {
                let snapshot_log_id = state_machine
                    .current_snapshot()
                    .and_then(|snapshot| snapshot.meta.last_log_id);
                let snapshot = state_machine.inner().snapshot();
                let node1202_availability = snapshot
                    .node(NodeId::new(1202))
                    .map(|node| node.availability());
                let node1203_availability = snapshot
                    .node(NodeId::new(1203))
                    .map(|node| node.availability());
                Box::pin(async move {
                    (
                        snapshot_log_id,
                        node1202_availability,
                        node1203_availability,
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(fresh_state.0, Some(offline_write.log_id()));
        assert_eq!(fresh_state.1, Some(NodeAvailabilityState::Unavailable));
        assert_eq!(fresh_state.2, Some(NodeAvailabilityState::Unavailable));

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        fresh_authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restarted_leader_resumes_writes_and_reads() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store: _,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities(
            "control-plane-raft-leader-restart-resume-test",
            901,
            902,
            903,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(901), "node-901".to_string()),
                    (NodeId::new(902), "node-902".to_string()),
                    (NodeId::new(903), "node-903".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "second voter applied bootstrap before leader restart",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before leader restart",
            )
            .await
            .unwrap();

        let restart_artifact =
            capture_openraft_restart_artifact(&leader_log_store, &authority1).await;
        authority1.shutdown().await.unwrap();
        network.unregister(901);

        let follower_write_err = authority2
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(902),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap_err();
        assert!(follower_write_err.is_control_plane_leader_routing_rejection());

        let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
        let restored_log_store_for_status = restored_log_store.clone();
        let restarted_raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            901,
            config,
            network.clone(),
            restored_log_store,
            restored_state_machine,
        )
        .await
        .unwrap();
        network.register(901, restarted_raft.clone());
        let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
            restarted_raft,
            restored_log_store_for_status,
            "test-cluster",
        );
        restarted_authority
            .wait_for_current_leader(
                901,
                Duration::from_secs(1),
                "restarted leader recovered current leadership",
            )
            .await
            .unwrap();

        let resumed_write = restarted_authority
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(903),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            resumed_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert!(resumed_write.log_id().index() > bootstrap.log_id().index());
        authority2
            .wait_for_applied_index_at_least(
                resumed_write.log_id().index(),
                Duration::from_secs(1),
                "second voter applied restarted leader write",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_index_at_least(
                resumed_write.log_id().index(),
                Duration::from_secs(1),
                "third voter applied restarted leader write",
            )
            .await
            .unwrap();

        let runtime_map = restarted_authority
            .linearized_runtime_map_snapshot(90_000)
            .await
            .unwrap();
        let expected_read_index = control_plane_log_id_from_raft(resumed_write.log_id())
            .expect("restarted leader command log id should be non-bootstrap");
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(expected_read_index)
        );
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(90_000));
        assert_eq!(
            runtime_map
                .nodes()
                .iter()
                .map(|node| node.node_id())
                .collect::<Vec<_>>(),
            vec![NodeId::new(901), NodeId::new(902), NodeId::new(903)]
        );
        let restarted_status = restarted_authority.status().await.unwrap();
        assert_eq!(restarted_status.server_state(), ServerState::Leader);
        assert!(restarted_status.local_leader());
        assert!(restarted_status.effective_voter());
        assert!(!restarted_status.effective_learner());
        assert!(restarted_status.applied_voter());
        assert!(!restarted_status.applied_learner());
        assert_eq!(
            restarted_status.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
        assert!(restarted_status.linearized_authority_serving());
        assert_eq!(restarted_status.applied(), Some(resumed_write.log_id()));
        assert_eq!(
            restarted_status.authority_incarnation(),
            runtime_map.freshness_proof().authority_incarnation()
        );
        assert_eq!(
            restarted_status.current_cluster_epoch(),
            runtime_map.cluster_epoch()
        );
        assert_eq!(restarted_status.oldest_storage_history_floor_epoch(), None);
        assert!(
            restarted_status.retained_history_count() > 0,
            "restarted leader should retain historical route state after epoch changes"
        );
        assert!(
            restarted_status.oldest_retained_history_epoch()
                <= restarted_status.newest_retained_history_epoch()
        );
        assert!(restarted_status
            .newest_retained_history_epoch()
            .is_some_and(|epoch| epoch < restarted_status.current_cluster_epoch()));
        assert_eq!(restarted_status.storage_node_count(), 3);
        assert_eq!(restarted_status.joining_storage_node_count(), 0);
        assert_eq!(restarted_status.active_storage_node_count(), 3);
        assert_eq!(restarted_status.draining_storage_node_count(), 0);
        assert_eq!(restarted_status.out_storage_node_count(), 0);
        assert_eq!(restarted_status.removed_storage_node_count(), 0);
        assert_eq!(restarted_status.healthy_storage_node_count(), 0);
        assert_eq!(restarted_status.suspect_storage_node_count(), 2);
        assert_eq!(restarted_status.unavailable_storage_node_count(), 1);
        assert_eq!(restarted_status.pg_count(), 1);
        assert_eq!(restarted_status.active_pg_count(), 0);
        assert_eq!(restarted_status.peering_pg_count(), 1);
        assert_eq!(restarted_status.degraded_pg_count(), 0);
        assert_eq!(restarted_status.backfilling_pg_count(), 0);
        assert_eq!(restarted_status.inconsistent_pg_count(), 0);
        assert_eq!(restarted_status.active_primary_pg_count(), 0);
        assert_eq!(restarted_status.peering_metadata_transfer_pg_count(), 0);
        assert_eq!(restarted_status.metadata_transfer_fenced_pg_count(), 0);
        assert_eq!(restarted_status.storage_node_lease_deadline_count(), 0);
        assert_eq!(
            restarted_status.earliest_storage_node_lease_deadline_ms(),
            None
        );
        assert_eq!(
            restarted_status.latest_storage_node_lease_deadline_ms(),
            None
        );
        assert_eq!(
            restarted_status.metadata_transfer_fence_source_lease_deadline_count(),
            0
        );
        assert_eq!(
            restarted_status.earliest_metadata_transfer_fence_source_lease_deadline_ms(),
            None
        );
        assert_eq!(
            restarted_status.latest_metadata_transfer_fence_source_lease_deadline_ms(),
            None
        );

        let restarted_node903_availability = restarted_authority
            .raft()
            .with_state_machine(|state_machine| {
                let availability = state_machine
                    .inner()
                    .snapshot()
                    .node(NodeId::new(903))
                    .map(|node| node.availability());
                Box::pin(async move { availability })
            })
            .await
            .unwrap();
        assert_eq!(
            restarted_node903_availability,
            Some(NodeAvailabilityState::Unavailable)
        );

        restarted_authority.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        authority3.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_transferred_leader_continues_after_old_leader_loss() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network,
            config: _,
            leader_log_store: _,
            third_log_store: _,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities(
            "control-plane-raft-post-transfer-old-leader-loss-test",
            1001,
            1002,
            1003,
        )
        .await;

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1001), "node-1001".to_string()),
                    (NodeId::new(1002), "node-1002".to_string()),
                    (NodeId::new(1003), "node-1003".to_string()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        assert!(matches!(
            bootstrap.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "second voter applied bootstrap before leader loss",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "third voter applied bootstrap before leader loss",
            )
            .await
            .unwrap();

        authority1.transfer_leadership_to(1002).await.unwrap();
        authority2
            .wait_for_current_leader(
                1002,
                Duration::from_secs(1),
                "second voter accepted leadership before old leader loss",
            )
            .await
            .unwrap();
        authority3
            .wait_for_current_leader(
                1002,
                Duration::from_secs(1),
                "third voter learned transferred leader before old leader loss",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority2,
            Duration::from_secs(1),
            "transferred leader became serving before old leader loss",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        authority1.shutdown().await.unwrap();
        network.unregister(1001);

        let failover_write = authority2
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1001),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        assert!(matches!(
            failover_write.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert_eq!(failover_write.log_id().committed_leader_id().node_id, 1002);
        assert!(
            failover_write.log_id().committed_leader_id().term
                > bootstrap.log_id().committed_leader_id().term
        );
        authority3
            .wait_for_applied_index_at_least(
                failover_write.log_id().index(),
                Duration::from_secs(1),
                "third voter applied failover leader write",
            )
            .await
            .unwrap();

        let follower_state = authority3
            .raft()
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot();
                let node_ids = snapshot
                    .nodes()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>();
                let node1001_availability = snapshot
                    .node(NodeId::new(1001))
                    .map(|node| node.availability());
                Box::pin(async move { (node_ids, node1001_availability) })
            })
            .await
            .unwrap();
        assert_eq!(
            follower_state.0,
            vec![NodeId::new(1001), NodeId::new(1002), NodeId::new(1003)]
        );
        assert_eq!(follower_state.1, Some(NodeAvailabilityState::Unavailable));

        authority2.shutdown().await.unwrap();
        authority3.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restart_replays_committed_entries() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                single_node_bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                single_node_membership_entry(3, 1, 2),
                blank_entry(3, 1, 3),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut log_store, &vote)
            .await
            .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();

        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .unwrap();
        let (mut restored_log_store, restored_state_machine) = artifact.restore().unwrap();
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );

        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-restart-replay-test"),
            UnreachableRaftNetworkFactory,
            restored_log_store.clone(),
            restored_state_machine,
        )
        .await
        .unwrap();

        assert!(raft.is_initialized().await.unwrap());
        let raft_state = raft
            .with_raft_state(|state| {
                (
                    state.log_ids.last().cloned(),
                    state.local_committed().cloned(),
                    *state.membership_state.effective().log_id(),
                )
            })
            .await
            .unwrap();
        assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 3)));
        assert_eq!(raft_state.1, Some(raft_log_id(3, 1, 3)));
        assert_eq!(raft_state.2, Some(raft_log_id(3, 1, 2)));
        let applied_state = raft
            .with_state_machine(|state_machine| {
                let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                Box::pin(async move { applied_state })
            })
            .await
            .unwrap();
        assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 3)));
        assert_eq!(applied_state.1.log_id(), &Some(raft_log_id(3, 1, 2)));
        assert_eq!(
            RaftLogStorage::read_committed(&mut restored_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 3))
        );

        raft.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restart_replays_rejected_committed_entry() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_membership_entry(1),
                normal_entry(
                    3,
                    1,
                    1,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                normal_entry(
                    3,
                    1,
                    2,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                ),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut log_store, &vote)
            .await
            .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();

        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .unwrap();
        let (_, restored_state_machine) = artifact.restore().unwrap();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-restart-rejection-test"),
            UnreachableRaftNetworkFactory,
            log_store,
            restored_state_machine,
        )
        .await
        .unwrap();

        let applied_state = raft
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let inner_last_applied = state_machine.inner().last_applied();
                let node_ids = state_machine
                    .inner()
                    .snapshot()
                    .nodes()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>();
                Box::pin(async move { (last_applied, inner_last_applied, node_ids) })
            })
            .await
            .unwrap();
        assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
        assert_eq!(applied_state.1, Some(ControlPlaneLogId::new(3, 2).unwrap()));
        assert_eq!(applied_state.2, vec![NodeId::new(1)]);

        raft.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_restart_restores_current_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_membership_entry(1),
                normal_entry(
                    3,
                    1,
                    1,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                blank_entry(3, 1, 2),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut log_store, &vote)
            .await
            .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut log_store, raft_log_id(3, 1, 2))
            .await
            .unwrap();

        let mut snapshot_source = ControlPlaneRaftStateMachine::empty();
        snapshot_source
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        snapshot_source
            .apply_entry(normal_entry(
                3,
                1,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        snapshot_source.apply_entry(blank_entry(3, 1, 2)).unwrap();
        let snapshot = snapshot_source.build_snapshot().unwrap();

        let state_machine = ControlPlaneRaftStateMachine::empty();
        publish_control_plane_raft_snapshot(&state_machine.current_snapshot, snapshot)
            .finish_on_current_thread()
            .unwrap();

        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-current-snapshot-recovery-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();

        let raft_state = raft
            .with_raft_state(|state| {
                (
                    state.local_committed().cloned(),
                    *state.membership_state.effective().log_id(),
                )
            })
            .await
            .unwrap();
        assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 2)));
        assert_eq!(raft_state.1, Some(raft_log_id(0, 1, 0)));
        let applied_state = raft
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let node_ids = state_machine
                    .inner()
                    .snapshot()
                    .nodes()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>();
                Box::pin(async move { (last_applied, node_ids) })
            })
            .await
            .unwrap();
        assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
        assert_eq!(applied_state.1, vec![NodeId::new(1)]);

        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");
        let status = authority.status().await.unwrap();
        assert_eq!(status.last_purged_index(), Some(2));
        assert_eq!(status.committed_index(), Some(2));
        assert_eq!(status.applied_index(), Some(2));
        assert_eq!(status.current_snapshot_index(), Some(2));
        assert_eq!(status.committed_to_applied_index_gap(), Some(0));
        assert!(status.applied_caught_up_to_committed());

        authority.shutdown().await.unwrap();
    });
}
