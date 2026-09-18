// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

struct LowPriorityGateRelease(Arc<ControlPlaneRaftLowPriorityTestGate>);

impl Drop for LowPriorityGateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

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
fn post_dispatch_evidence_uncertainty_crosses_rpc_and_defers_the_durable_outbox() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority, follower) = initialized_two_node_authorities(
            "control-plane-raft-evidence-uncertainty-rpc-test",
            501,
            502,
        )
        .await;
        let authority = Arc::new(authority);
        let follower = Arc::new(follower);
        let mut host = crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost::new_for_test(
            tokio::runtime::Handle::current(),
            Arc::clone(&authority),
            false,
        )
        .unwrap();

        let pg_id = PgId::new(19);
        let storage_nodes = (501..=504)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    format!("unix:///catalogue/storage-{node_id}.sock"),
                )
            })
            .collect::<Vec<_>>();
        let source_acting_set = vec![NodeId::new(504), NodeId::new(502), NodeId::new(503)];
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                2,
                [0x71; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![501, 502],
                &storage_nodes,
                &[(pg_id, source_acting_set.clone())],
                crate::control_plane::test_certified_storage_placement_policy(
                    (501..=504).map(NodeId::new),
                    3,
                    1,
                ),
            )
            .unwrap();
        host.submit_command_for_test(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: storage_nodes.clone(),
            pg_acting_sets: vec![(pg_id, source_acting_set)],
            topology,
        })
        .unwrap();

        let heartbeat =
            |host: &mut crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost,
             node_id: u32,
             lease_ms: u64,
             state: Option<PgState>,
             heartbeat_at_ms: u64| {
                for _ in 0..4 {
                    let observed_epoch =
                        host.current_snapshot_for_test().unwrap().cluster_epoch();
                    let endpoint = storage_nodes
                        .iter()
                        .find(|(candidate, _)| candidate.as_u32() == node_id)
                        .unwrap()
                        .1
                        .clone();
                    let refresh = match host.refresh_node_heartbeat(
                        NodeHeartbeat {
                            node_id: NodeId::new(node_id),
                            node_incarnation: 4,
                            endpoint,
                            observed_epoch,
                            requested_lease_duration_ms: lease_ms,
                            cluster_map_history_route_scan_generation:
                                std::num::NonZeroU64::new(1).unwrap(),
                            cluster_map_history_route_references: Default::default(),
                            pg_observations: state
                                .map(|state| {
                                    vec![crate::control_plane::NodePgHeartbeatObservation {
                                        pg_id,
                                        state,
                                        metadata_proof: PgMetadataProof::empty(),
                                        pending_metadata_command: None,
                                    }]
                                })
                                .unwrap_or_default(),
                        },
                        heartbeat_at_ms,
                    ) {
                        Ok(refresh) => refresh,
                        Err(ControlPlaneError::PgPrimaryObservationNotActive { .. })
                            if state == Some(PgState::Active) =>
                        {
                            continue;
                        }
                        Err(error) => panic!(
                            "storage node {node_id} {state:?} heartbeat failed: {error:?}"
                        ),
                    };
                    if refresh.lease().serving() {
                        return;
                    }
                }
                panic!("storage node {node_id} did not receive a serving lease");
            };

        for node_id in 501..=504 {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                None,
                1_000 + u64::from(node_id),
            );
        }
        for (offset, node_id) in [504, 502, 503].into_iter().enumerate() {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                Some(PgState::Peering),
                2_000 + u64::try_from(offset).unwrap(),
            );
        }
        for (offset, node_id) in [504, 502, 503].into_iter().enumerate() {
            heartbeat(
                &mut host,
                node_id,
                if node_id == 504 { 100 } else { 10_000 },
                Some(PgState::Active),
                3_000 + u64::try_from(offset).unwrap(),
            );
        }
        let failed_deadline = host
            .current_snapshot_for_test()
            .unwrap()
            .node(NodeId::new(504))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        host.expire_heartbeat_leases(failed_deadline).unwrap();
        for node_id in [501, 502, 503] {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                None,
                failed_deadline + u64::from(node_id),
            );
        }
        let proof_at_ms = host
            .current_snapshot_for_test()
            .unwrap()
            .max_committed_timestamp_ms()
            .unwrap()
            + 1;
        for node_id in [502, 503] {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                Some(PgState::Peering),
                proof_at_ms + u64::from(node_id),
            );
        }
        let begin_at_ms = host
            .current_snapshot_for_test()
            .unwrap()
            .unavailable_node_observation(NodeId::new(504))
            .unwrap()
            .observed_at_ms()
            + 1;
        let mut cursor = crate::control_plane::UnavailablePgReconciliationCursor::start();
        let begun = host
            .poll_unavailable_pg_reconciliation_batch(&mut cursor, begin_at_ms)
            .unwrap();
        assert!(begun.rejected.is_empty());
        assert_eq!(begun.work.len(), 1);

        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("staging-evidence-uncertainty.sock");
        let store = Arc::new(
            crate::pg_store::MetadataTransferStagingStore::open(
                tmp.path(),
                crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                    NodeId::new(501),
                    4,
                    "unix:///catalogue/storage-501.sock".to_owned(),
                )
                .unwrap(),
                crate::pg_store::MetadataTransferStagingLimits::new(
                    8,
                    1024 * 1024,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let binding = crate::control_plane::UnavailablePgTransitionMutationBinding::new(
            pg_id,
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap()
                .transition_epoch(),
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap()
                .source_epoch(),
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap()
                .source_acting_set()
                .to_vec(),
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap()
                .destination_acting_set()
                .to_vec(),
        );
        let artifact =
            crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
                &binding,
                ClusterEpoch::new(13).unwrap(),
            );
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &binding,
            checksum::sha256::digest(&artifact),
            u64::try_from(artifact.len()).unwrap(),
            crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap();
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, &artifact).unwrap();
        let retained_page = store.next_evidence_page().unwrap().unwrap();
        let authorization =
            crate::control_plane_command::UnavailablePgStagingIntentAuthorizationRequest {
                unavailable_transition: binding.clone(),
                staging_generation: intent.staging_generation(),
                artifact_target_epoch: ClusterEpoch::new(13).unwrap(),
                artifact_digest: intent.artifact_digest(),
                artifact_length: intent.artifact_length(),
                artifact_format_version: intent.artifact_format_version(),
            };
        <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::authorize_unavailable_pg_staging_intents_batch(
            &mut host,
            std::slice::from_ref(&authorization),
        )
        .unwrap();

        let credential_input =
            crate::control_plane::ControlPlaneStorageNodeAuthCredentialInput {
                node_id: NodeId::new(501),
                credential_id: "storage-node-501".to_owned(),
                credential_version: 1,
                secret: b"storage-node-501-secret".to_vec(),
            };
        let verifier = crate::control_plane::ControlPlaneUnixAuthVerifier::new(
            "raft-evidence-uncertainty-cluster",
            vec![
                crate::control_plane::ControlPlaneStorageNodeAuthCredential::new(
                    credential_input.clone(),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let listener = crate::control_plane::ControlPlaneRpcServerListener::unix(
            UnixListener::bind(&socket_path).unwrap(),
            4,
            crate::control_plane::CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(2),
        )
        .unwrap();
        let confirmation_host = host.clone();
        let policy = crate::control_plane::ControlPlaneRpcServerPolicy::new(
            crate::control_plane::ControlPlaneRpcServerRole::Ordinary,
            4,
            crate::control_plane::CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        )
        .unwrap()
        .with_auth_verifier(Arc::new(verifier))
        .with_authority_confirmation(Arc::new(move || {
            confirmation_host
                .block_on_for_test(
                    confirmation_host
                        .authority_for_test()
                        .confirmed_linearized_authority_status(),
                )
                .map(|_| ())
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_server = Arc::clone(&stop);
        let server = std::thread::spawn(move || {
            listener
                .serve_shared_until_stop_for_test(
                    Arc::new(Mutex::new(host)),
                    policy,
                    2_000,
                    &stop_for_server,
                )
                .unwrap();
        });

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        authority.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));
        let certification_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_low_priority_during_retry_certification_for_test(Arc::clone(
            &certification_gate,
        ));
        let certification_release = LowPriorityGateRelease(Arc::clone(&certification_gate));

        let control_plane = crate::ControlPlaneStorageNodeClient::with_socket_paths(
            [socket_path.clone()],
            Some("raft-evidence-uncertainty-cluster"),
            501,
            4,
            vec![credential_input],
            Some(("storage-node-501".to_owned(), 1)),
        )
        .unwrap();
        let (fatal_tx, fatal_rx) = std::sync::mpsc::sync_channel(1);
        let mut outbox = crate::StorageNodeMetadataTransferStagingOutbox::spawn(
            Arc::clone(&store),
            control_plane,
            || 2_000,
            move |error| fatal_tx.send(error).unwrap(),
        )
        .unwrap();
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            certification_gate.wait_for_arrival(),
        )
        .await
        .expect("outbox evidence did not enter Raft retry certification");

        let ordinary_authority = Arc::clone(&authority);
        let ordinary = ControlPlaneRaftTypeConfig::spawn(async move {
            ordinary_authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(501),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), ordinary)
            .await
            .expect("ordinary command did not proceed after evidence yielded")
            .expect("ordinary command task failed")
            .unwrap();

        let status_deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let status = outbox.status();
            if status.publication_failures != 0 {
                assert!(
                    !status.failed,
                    "Raft uncertainty became fatal after RPC: {status:?}"
                );
                assert!(status
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("outcome is unconfirmed")));
                break;
            }
            assert!(
                std::time::Instant::now() < status_deadline,
                "outbox did not observe Raft uncertainty"
            );
            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
        }
        assert!(matches!(
            fatal_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(store.next_evidence_page().unwrap().unwrap(), retained_page);

        certification_release.0.release();
        drop(certification_release);
        outbox.stop();
        stop.store(true, Ordering::Release);
        drop(UnixStream::connect(&socket_path).unwrap());
        server.join().unwrap();
        authority.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    });
}

#[test]
fn dispatched_low_priority_proposal_releases_heartbeat_gate_until_completion() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority, follower, network) = initialized_two_node_authorities_with_network(
            "control-plane-raft-dispatched-evidence-heartbeat-test",
            511,
            512,
        )
        .await;
        let authority = Arc::new(authority);
        let follower = Arc::new(follower);
        authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(511), "node-511".to_owned()),
                    (NodeId::new(512), "node-512".to_owned()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();

        let status = authority.status().await.unwrap();
        let authority_binding = LeaseHorizonAuthorityBinding::new(
            1,
            Some(
                status
                    .current_term()
                    .expect("initialized authority should have a serving term"),
            ),
        );
        let observed_epoch = status.current_cluster_epoch();
        authority
            .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(511),
                    node_incarnation: 1,
                    endpoint: "node-511".to_owned(),
                    observed_epoch,
                    requested_lease_duration_ms: 2_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 70_000,
                lease_deadline_ms: 72_000,
                lease_horizon_authority: Some(authority_binding),
            })
            .await
            .unwrap();

        let existing_overlay_deadline_ms = 72_250;
        let existing_overlay = authority
            .try_apply_volatile_heartbeat(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(511),
                    node_incarnation: 1,
                    endpoint: "node-511".to_owned(),
                    observed_epoch,
                    requested_lease_duration_ms: 2_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 70_250,
                lease_deadline_ms: existing_overlay_deadline_ms,
                lease_horizon_authority: Some(authority_binding),
            })
            .await
            .unwrap()
            .expect("covered heartbeat should establish a volatile overlay before dispatch");
        assert_eq!(
            existing_overlay
                .node(NodeId::new(511))
                .unwrap()
                .lease_deadline_ms(),
            Some(existing_overlay_deadline_ms)
        );
        authority
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(512),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .expect("ordinary command should promote the existing volatile overlay");
        assert_eq!(
            authority
                .current_control_plane_snapshot()
                .await
                .unwrap()
                .node(NodeId::new(511))
                .unwrap()
                .lease_deadline_ms(),
            Some(existing_overlay_deadline_ms)
        );

        let before_dispatch_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_low_priority_before_dispatch_for_test(Arc::clone(&before_dispatch_gate));
        let before_dispatch_release =
            LowPriorityGateRelease(Arc::clone(&before_dispatch_gate));
        let dispatch_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_low_priority_after_dispatch_for_test(Arc::clone(&dispatch_gate));
        let dispatch_release = LowPriorityGateRelease(Arc::clone(&dispatch_gate));
        let command = ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(512),
            availability: NodeAvailabilityState::Unavailable,
        };
        let submitting_authority = Arc::clone(&authority);
        let submitted_command = command.clone();
        let submission = ControlPlaneRaftTypeConfig::spawn(async move {
            submitting_authority
                .submit_low_priority_control_plane_command(submitted_command)
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            before_dispatch_gate.wait_for_arrival(),
        )
        .await
        .expect("low-priority proposal did not finish pre-dispatch certification");
        network.unregister(512);
        before_dispatch_release.0.release();
        drop(before_dispatch_release);
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            dispatch_gate.wait_for_arrival(),
        )
        .await
        .expect("low-priority proposal was not accepted by RaftCore");

        let renewed_deadline_ms = 72_500;
        let renewed = ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(1),
            authority.try_apply_volatile_heartbeat(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(511),
                    node_incarnation: 1,
                    endpoint: "node-511".to_owned(),
                    observed_epoch,
                    requested_lease_duration_ms: 2_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 70_500,
                lease_deadline_ms: renewed_deadline_ms,
                lease_horizon_authority: Some(authority_binding),
            }),
        )
        .await
        .expect("heartbeat remained blocked behind the dispatched evidence proposal")
        .unwrap()
        .expect("covered heartbeat should renew through the volatile overlay");
        assert_eq!(
            renewed
                .node(NodeId::new(511))
                .unwrap()
                .lease_deadline_ms(),
            Some(renewed_deadline_ms)
        );

        let ordinary_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_ordinary_durable_after_update_gate_for_test(Arc::clone(&ordinary_gate));
        let ordinary_release = LowPriorityGateRelease(Arc::clone(&ordinary_gate));
        let ordinary_authority = Arc::clone(&authority);
        let ordinary = ControlPlaneRaftTypeConfig::spawn(async move {
            ordinary_authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(511),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), async {
            while authority
                .evidence_submission_admission
                .ordinary_waiters
                .load(Ordering::Acquire)
                == 0
            {
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("ordinary durable command did not queue behind accepted evidence");

        network.register(512, follower.raft().clone());
        dispatch_release.0.release();
        drop(dispatch_release);
        let first = ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), submission)
            .await
            .expect("low-priority proposal did not complete after quorum returned")
            .expect("low-priority proposal task failed")
            .unwrap();
        assert!(matches!(
            first.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            ordinary_gate.wait_for_arrival(),
        )
        .await
        .expect("ordinary durable command did not run after evidence completion");
        ordinary_release.0.release();
        drop(ordinary_release);
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), ordinary)
            .await
            .expect("ordinary durable command did not complete after evidence")
            .expect("ordinary durable command task failed")
            .unwrap();
        let after_first = authority.current_control_plane_snapshot().await.unwrap();
        assert_eq!(
            after_first
                .node(NodeId::new(512))
                .unwrap()
                .observed_availability(),
            NodeAvailabilityState::Unavailable,
            "successor durable proposal must retain the accepted evidence mutation"
        );
        assert_eq!(
            after_first
                .node(NodeId::new(511))
                .unwrap()
                .lease_deadline_ms(),
            Some(renewed_deadline_ms),
            "proposal completion must rebase the renewal that arrived while it was pending"
        );

        let replay = authority
            .submit_low_priority_control_plane_command(command)
            .await
            .unwrap();
        assert!(matches!(
            replay.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::MarkNodeAvailability
            )
        ));
        assert_eq!(
            authority.current_control_plane_snapshot().await.unwrap(),
            after_first,
            "exact low-priority replay must not mutate control-plane state twice"
        );

        authority.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    });
}

#[test]
fn low_priority_dispatch_yields_to_waiter_before_raft_core_acceptance() {
    ControlPlaneRaftTypeConfig::run(async {
        let (authority, follower) = initialized_two_node_authorities(
            "control-plane-raft-evidence-preaccept-priority-test",
            521,
            522,
        )
        .await;
        let authority = Arc::new(authority);
        let follower = Arc::new(follower);
        authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(521), "node-521".to_owned()),
                    (NodeId::new(522), "node-522".to_owned()),
                ],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        let before_log_id = authority.status().await.unwrap().last_log_id();
        let before_target_availability = authority
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .node(NodeId::new(522))
            .unwrap()
            .observed_availability();

        let before_dispatch_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_low_priority_before_dispatch_for_test(Arc::clone(&before_dispatch_gate));
        let before_dispatch_release =
            LowPriorityGateRelease(Arc::clone(&before_dispatch_gate));
        let evidence_authority = Arc::clone(&authority);
        let evidence = ControlPlaneRaftTypeConfig::spawn(async move {
            evidence_authority
                .submit_low_priority_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(522),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                )
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            before_dispatch_gate.wait_for_arrival(),
        )
        .await
        .expect("evidence did not pause immediately before RaftCore dispatch");

        let ordinary_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_ordinary_durable_after_update_gate_for_test(Arc::clone(&ordinary_gate));
        let ordinary_release = LowPriorityGateRelease(Arc::clone(&ordinary_gate));
        let ordinary_authority = Arc::clone(&authority);
        let ordinary = ControlPlaneRaftTypeConfig::spawn(async move {
            ordinary_authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(521),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), async {
            while authority
                .evidence_submission_admission
                .ordinary_waiters
                .load(Ordering::Acquire)
                == 0
            {
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("ordinary command did not register before evidence dispatch");

        before_dispatch_release.0.release();
        drop(before_dispatch_release);
        let evidence_error = ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), evidence)
            .await
            .expect("evidence did not yield before RaftCore acceptance")
            .expect("evidence task failed")
            .unwrap_err();
        assert!(matches!(
            evidence_error,
            ControlPlaneError::StagingEvidencePublicationDeferred
        ));
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            ordinary_gate.wait_for_arrival(),
        )
        .await
        .expect("ordinary command did not acquire admission after evidence yielded");
        assert_eq!(
            authority.status().await.unwrap().last_log_id(),
            before_log_id,
            "yielded evidence must not append before the ordinary successor dispatches"
        );

        ordinary_release.0.release();
        drop(ordinary_release);
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), ordinary)
            .await
            .expect("ordinary successor did not complete")
            .expect("ordinary successor task failed")
            .unwrap();
        assert_eq!(
            authority
                .current_control_plane_snapshot()
                .await
                .unwrap()
                .node(NodeId::new(522))
                .unwrap()
                .observed_availability(),
            before_target_availability,
            "yielded evidence must leave the target state unchanged"
        );

        authority.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    });
}

#[test]
fn low_priority_evidence_yields_when_membership_queues_after_gate_acquisition() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let state_machine = ControlPlaneRaftStateMachine::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-evidence-membership-priority-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();
        let authority = Arc::new(ControlPlaneRaftAuthority::new_with_log_store(
            raft,
            log_store,
            "test-cluster",
        ));
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "evidence priority leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "evidence priority authority serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority.set_low_priority_after_update_gate_for_test(Arc::clone(&gate));
        let release = LowPriorityGateRelease(Arc::clone(&gate));
        let evidence_authority = Arc::clone(&authority);
        let evidence = ControlPlaneRaftTypeConfig::spawn(async move {
            evidence_authority
                .submit_low_priority_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                )
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), gate.wait_for_arrival())
            .await
            .expect("evidence did not acquire the update gate");

        let membership_authority = Arc::clone(&authority);
        let membership = ControlPlaneRaftTypeConfig::spawn(async move {
            membership_authority
                .replace_voters(BTreeSet::from([1]), true)
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), async {
            while authority
                .evidence_submission_admission
                .ordinary_waiters
                .load(Ordering::Acquire)
                == 0
            {
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("membership mutation did not register ordinary priority");
        release.0.release();
        drop(release);

        let evidence_error = ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), evidence)
            .await
            .expect("evidence did not yield to membership")
            .expect("evidence task failed")
            .unwrap_err();
        assert!(matches!(
            evidence_error,
            ControlPlaneError::StagingEvidencePublicationDeferred
        ));
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), membership)
            .await
            .expect("membership did not proceed after evidence yielded")
            .expect("membership task failed")
            .unwrap();

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
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
            "serving status remains true after OpenRaft's proposal lease expires"
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

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));
        let certification_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority2.set_low_priority_during_retry_certification_for_test(Arc::clone(
            &certification_gate,
        ));
        let certification_release = LowPriorityGateRelease(Arc::clone(&certification_gate));
        let certifying_evidence_authority = Arc::clone(&authority2);
        let certifying_evidence = ControlPlaneRaftTypeConfig::spawn(async move {
            certifying_evidence_authority
                .submit_low_priority_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(401),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                )
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(2),
            certification_gate.wait_for_arrival(),
        )
        .await
        .expect("evidence did not enter post-dispatch retry certification");

        let certification_status = authority2.status().await.unwrap();
        let certification_heartbeat_binding = LeaseHorizonAuthorityBinding::new(
            1,
            Some(
                certification_status
                    .current_term()
                    .expect("serving transferred authority should have a term"),
            ),
        );
        let certification_heartbeat_authority = Arc::clone(&authority2);
        let certification_heartbeat = ControlPlaneRaftTypeConfig::spawn(async move {
            certification_heartbeat_authority
                .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: NodeHeartbeat {
                        node_id: NodeId::new(402),
                        node_incarnation: 1,
                        endpoint: "node-402".to_owned(),
                        observed_epoch: certification_status.current_cluster_epoch(),
                        requested_lease_duration_ms: 2_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms: 49_000,
                    lease_deadline_ms: 51_000,
                    lease_horizon_authority: Some(certification_heartbeat_binding),
                })
                .await
        });
        let certification_error = ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(1),
            certifying_evidence,
        )
        .await
        .expect("evidence certification did not yield to heartbeat")
        .expect("certifying evidence task failed")
        .unwrap_err();
        assert!(matches!(
            certification_error,
            ControlPlaneError::StagingEvidencePublicationOutcomeUnconfirmed { .. }
        ));
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), certification_heartbeat)
            .await
            .expect("heartbeat did not proceed after evidence certification yielded")
            .expect("certification heartbeat task failed")
            .unwrap();
        certification_release.0.release();
        drop(certification_release);

        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(350)).await;
        authority2.pause_next_proposal_after_lease_confirmation_for_test(Duration::from_millis(
            350,
        ));
        let retry_gate = Arc::new(ControlPlaneRaftLowPriorityTestGate::new());
        authority2
            .set_low_priority_after_proven_unappended_retry_for_test(Arc::clone(&retry_gate));
        let retry_release = LowPriorityGateRelease(Arc::clone(&retry_gate));
        let retry_evidence_authority = Arc::clone(&authority2);
        let retrying_evidence = ControlPlaneRaftTypeConfig::spawn(async move {
            retry_evidence_authority
                .submit_low_priority_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(401),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                )
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), retry_gate.wait_for_arrival())
            .await
            .expect("evidence did not reach the proven-unappended retry boundary");

        let status = authority2.status().await.unwrap();
        let heartbeat_authority_binding = LeaseHorizonAuthorityBinding::new(
            1,
            Some(
                status
                    .current_term()
                    .expect("serving transferred authority should have a term"),
            ),
        );
        let heartbeat_authority = Arc::clone(&authority2);
        let heartbeat = ControlPlaneRaftTypeConfig::spawn(async move {
            heartbeat_authority
                .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: NodeHeartbeat {
                        node_id: NodeId::new(402),
                        node_incarnation: 1,
                        endpoint: "node-402".to_owned(),
                        observed_epoch: status.current_cluster_epoch(),
                        requested_lease_duration_ms: 2_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms: 50_000,
                    lease_deadline_ms: 52_000,
                    lease_horizon_authority: Some(heartbeat_authority_binding),
                })
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(1), async {
            while authority2
                .evidence_submission_admission
                .ordinary_waiters
                .load(Ordering::Acquire)
                == 0
            {
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("heartbeat renewal did not register ordinary priority");
        retry_release.0.release();
        drop(retry_release);

        let retry_error = ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(1),
            retrying_evidence,
        )
        .await
        .expect("evidence retry did not yield to heartbeat")
        .expect("retrying evidence task failed")
        .unwrap_err();
        assert!(matches!(
            retry_error,
            ControlPlaneError::StagingEvidencePublicationDeferred
        ));
        ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(2), heartbeat)
            .await
            .expect("heartbeat did not proceed after evidence retry yielded")
            .expect("heartbeat task failed")
            .unwrap();

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
fn control_plane_openraft_runtime_map_reads_route_followers_and_capture_rebased_volatile_lease() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (authority1, authority2) = initialized_two_node_authorities(
            "control-plane-raft-runtime-map-leader-read-test",
            711,
            712,
        )
        .await;
        let authority1 = Arc::new(authority1);

        let bootstrap = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(711), "node-711".to_string())],
                pg_ids: vec![PgId::new(0)],
            })
            .await
            .unwrap();
        authority2
            .wait_for_applied_log_id(
                bootstrap.log_id(),
                Duration::from_secs(1),
                "runtime-map follower applied bootstrap before read",
            )
            .await
            .unwrap();

        let durable_generation = authority1
            .raft()
            .with_state_machine(|state_machine| {
                let generation = state_machine.inner().snapshot_generation();
                Box::pin(async move { generation })
            })
            .await
            .unwrap();
        let durable_retirement_hook =
            Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        authority1.set_linearized_snapshot_retirement_hook_for_test(Arc::clone(
            &durable_retirement_hook,
        ));
        let durable_selection = authority1
            .linearized_control_plane_snapshot_selection()
            .await
            .unwrap();
        let durable_retirement_entered = Arc::new(AtomicBool::new(false));
        let durable_timer_completed = Arc::new(AtomicBool::new(false));
        let durable_retirement_watchdog = state_machine_retirement_progress_watchdog(
            Arc::clone(&durable_retirement_hook),
            Arc::clone(&durable_retirement_entered),
            Arc::clone(&durable_timer_completed),
        );
        let durable_timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
            durable_retirement_entered,
            durable_timer_completed,
        ));
        assert!(
            Arc::ptr_eq(durable_selection.snapshot.arc(), &durable_generation),
            "durable runtime-map selection must retain the immutable state-machine generation"
        );
        drop(durable_generation);
        authority1
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(711),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .unwrap();
        drop(durable_selection);
        durable_timer.await.unwrap();
        assert!(
            durable_retirement_watchdog.join().unwrap(),
            "single-worker executor must progress while durable generation destruction is blocked"
        );

        let leader_map = authority1
            .linearized_runtime_map_snapshot(79_000)
            .await
            .unwrap();
        assert_eq!(leader_map.pg_routes().len(), 1);
        assert_eq!(leader_map.pg_routes()[0].pg_id(), PgId::new(0));

        let follower_error = authority2
            .linearized_runtime_map_snapshot(79_001)
            .await
            .unwrap_err();
        assert!(
            follower_error.is_control_plane_leader_routing_rejection(),
            "unexpected follower runtime-map error: {follower_error:?}"
        );
        let follower_status_error = authority2
            .linearized_runtime_map_status(79_002)
            .await
            .unwrap_err();
        assert!(
            follower_status_error.is_control_plane_leader_routing_rejection(),
            "unexpected follower runtime-map status error: {follower_status_error:?}"
        );

        let authority_term = authority1
            .status()
            .await
            .unwrap()
            .current_term()
            .expect("serving runtime-map authority should have a term");
        let lease_horizon_authority =
            LeaseHorizonAuthorityBinding::new(1, Some(authority_term));
        let mut observed_epoch = leader_map.cluster_epoch();
        for heartbeat_at_ms in [79_010, 79_011] {
            let heartbeat = authority1
                .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: NodeHeartbeat {
                        node_id: NodeId::new(711),
                        node_incarnation: 1,
                        endpoint: "node-711".to_string(),
                        observed_epoch,
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id: PgId::new(0),
                            state: PgState::Peering,
                            metadata_proof: PgMetadataProof::empty(),
                            pending_metadata_command: None,
                        }],
                    },
                    heartbeat_at_ms,
                    lease_deadline_ms: heartbeat_at_ms + 1_000,
                    lease_horizon_authority: Some(lease_horizon_authority),
                })
                .await
                .unwrap();
            assert!(matches!(
                heartbeat.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::RecordNodeHeartbeat
                )
            ));
            observed_epoch = authority1
                .current_control_plane_snapshot()
                .await
                .unwrap()
                .cluster_epoch();
        }
        let completion = authority1
            .submit_control_plane_command(ControlPlaneCommand::CompletePgPeering {
                pg_id: PgId::new(0),
                primary: NodeId::new(711),
                node_incarnation: 1,
                complete_at_ms: 79_012,
            })
            .await
            .unwrap();
        assert!(matches!(
            completion.outcome(),
            ControlPlaneRaftCommandOutcome::Applied(
                ControlPlaneCommandResponse::CompletePgPeering
            )
        ));
        observed_epoch = authority1
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .cluster_epoch();
        authority1
            .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(711),
                    node_incarnation: 1,
                    endpoint: "node-711".to_string(),
                    observed_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                heartbeat_at_ms: 79_013,
                lease_deadline_ms: 80_013,
                lease_horizon_authority: Some(lease_horizon_authority),
            })
            .await
            .unwrap();
        observed_epoch = authority1
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .cluster_epoch();
        let volatile_lease_deadline_ms = 80_500;
        let volatile_snapshot = authority1
            .try_apply_volatile_heartbeat(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(711),
                    node_incarnation: 1,
                    endpoint: "node-711".to_string(),
                    observed_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                heartbeat_at_ms: 79_500,
                lease_deadline_ms: volatile_lease_deadline_ms,
                lease_horizon_authority: Some(lease_horizon_authority),
            })
            .await
            .unwrap()
            .expect("covered heartbeat should publish a volatile lease overlay");
        assert_eq!(
            volatile_snapshot
                .node(NodeId::new(711))
                .unwrap()
                .lease_deadline_ms(),
            Some(volatile_lease_deadline_ms)
        );
        let overlay_generation = authority1
            .volatile_heartbeat_overlay_generation(authority_term, authority1.status().await.unwrap().applied().unwrap())
            .unwrap()
            .expect("covered heartbeat should retain an overlay generation");
        let overlay_retirement_hook =
            Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        authority1.set_linearized_snapshot_retirement_hook_for_test(Arc::clone(
            &overlay_retirement_hook,
        ));
        let overlay_selection = authority1
            .linearized_control_plane_snapshot_selection()
            .await
            .unwrap();
        let overlay_retirement_entered = Arc::new(AtomicBool::new(false));
        let overlay_timer_completed = Arc::new(AtomicBool::new(false));
        let overlay_retirement_watchdog = state_machine_retirement_progress_watchdog(
            Arc::clone(&overlay_retirement_hook),
            Arc::clone(&overlay_retirement_entered),
            Arc::clone(&overlay_timer_completed),
        );
        let overlay_timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
            overlay_retirement_entered,
            overlay_timer_completed,
        ));
        assert!(
            Arc::ptr_eq(
                overlay_selection.snapshot.arc(),
                overlay_generation.arc()
            ),
            "overlay runtime-map selection must retain the immutable overlay generation"
        );
        drop(overlay_generation);
        authority1
            .publish_rebased_volatile_heartbeat_overlay(
                authority_term,
                authority1.status().await.unwrap().applied().unwrap(),
                volatile_snapshot.clone(),
            )
            .await
            .unwrap();
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            async {
                while Arc::strong_count(overlay_selection.snapshot.arc()) != 1 {
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(1)).await;
                }
            },
        )
        .await
        .expect("replaced overlay owner should retire on the blocking lane");
        drop(overlay_selection);
        overlay_timer.await.unwrap();
        assert!(
            overlay_retirement_watchdog.join().unwrap(),
            "single-worker executor must progress while overlay generation destruction is blocked"
        );

        let reads_before_advance = authority1.linearized_runtime_map_read_index_count_for_test();
        let update_guard = authority1.volatile_heartbeat_update_gate.lock().await;
        let read_gate = Arc::new(tokio::sync::Barrier::new(2));
        authority1.set_linearized_read_after_snapshot_gate_for_test(Arc::clone(&read_gate));
        let reader_authority = Arc::clone(&authority1);
        let reader = tokio::spawn(async move {
            reader_authority
                .linearized_runtime_map_snapshot(79_003)
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            read_gate.wait(),
        )
        .await
        .expect("runtime-map reader should reach the post-ReadIndex gate");

        let advanced = authority1
            .raft()
            .client_write(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(711),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .unwrap();
        authority1
            .publish_rebased_volatile_heartbeat_overlay(
                authority_term,
                advanced.log_id,
                volatile_snapshot,
            )
            .await
            .unwrap();
        drop(update_guard);

        let advanced_map = ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            reader,
        )
        .await
        .expect("runtime-map reader should complete after publication is released")
        .expect("runtime-map reader task should join")
        .expect("runtime-map reader should capture the rebased volatile lease");
        assert_eq!(
            authority1.linearized_runtime_map_read_index_count_for_test(),
            reads_before_advance + 1,
            "one ReadIndex followed by a current state-machine capture must make progress"
        );
        assert_eq!(
            advanced_map.freshness_proof().read_index(),
            Some(
                control_plane_log_id_from_raft(advanced.log_id)
                    .expect("advanced runtime-map read tip should be non-bootstrap")
            ),
            "runtime-map read must capture the applied tip reached after ReadIndex"
        );
        assert_eq!(
            advanced_map.valid_until_ms(),
            Some(volatile_lease_deadline_ms),
            "full runtime-map read must select the volatile lease rebased to the captured tip"
        );
        let reads_before_status = authority1.linearized_runtime_map_read_index_count_for_test();
        let advanced_status = authority1
            .linearized_runtime_map_status(79_504)
            .await
            .unwrap();
        assert_eq!(
            authority1.linearized_runtime_map_read_index_count_for_test(),
            reads_before_status + 1,
            "compact status must also complete with one ReadIndex"
        );
        assert_eq!(advanced_status.pg_routes(), 1);
        assert_eq!(advanced_status.active_serving_pg_routes(), 1);
        assert_eq!(
            advanced_status
                .lease_renewal()
                .expect("volatile active route should renew the compact runtime-map lease")
                .validity()
                .valid_until_ms(),
            Some(volatile_lease_deadline_ms),
            "compact status must derive its serving lease from the rebased volatile overlay"
        );
        assert_eq!(
            advanced_status
                .lease_renewal()
                .expect("volatile active route should carry a content renewal")
                .content_digest(),
            advanced_map.content_digest(),
            "full and compact reads must describe the same rebased volatile map"
        );

        let raft_state_capture_gate = Arc::new(tokio::sync::Barrier::new(2));
        let generation_capture_gate = Arc::new(tokio::sync::Barrier::new(2));
        let rejected_retirement_hook =
            Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        authority1.set_linearized_read_after_raft_state_capture_gate_for_test(Arc::clone(
            &raft_state_capture_gate,
        ));
        authority1.set_linearized_read_after_generation_capture_gate_for_test(Arc::clone(
            &generation_capture_gate,
        ));
        authority1.set_linearized_snapshot_retirement_hook_for_test(Arc::clone(
            &rejected_retirement_hook,
        ));
        let rejected_retirement_entered = Arc::new(AtomicBool::new(false));
        let rejected_timer_completed = Arc::new(AtomicBool::new(false));
        let rejected_retirement_watchdog = state_machine_retirement_progress_watchdog(
            Arc::clone(&rejected_retirement_hook),
            Arc::clone(&rejected_retirement_entered),
            Arc::clone(&rejected_timer_completed),
        );
        let rejected_timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
            rejected_retirement_entered,
            rejected_timer_completed,
        ));
        let rejected_reader_authority = Arc::clone(&authority1);
        let rejected_reader = tokio::spawn(async move {
            rejected_reader_authority
                .linearized_control_plane_snapshot_selection()
                .await
        });
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            raft_state_capture_gate.wait(),
        )
        .await
        .expect("reader should pause after capturing Raft readiness state");
        authority1
            .raft()
            .client_write(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(711),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            raft_state_capture_gate.wait(),
        )
        .await
        .expect("reader should resume to capture the advanced generation");
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            generation_capture_gate.wait(),
        )
        .await
        .expect("reader should retain the advanced generation before readiness checks");
        authority1
            .raft()
            .client_write(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(711),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .unwrap();
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            generation_capture_gate.wait(),
        )
        .await
        .expect("reader should resume after its generation is replaced");
        let rejected_result = ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            rejected_reader,
        )
        .await
        .expect("readiness-rejected reader should finish boundedly")
        .expect("readiness-rejected reader task should join");
        let rejected_error = match rejected_result {
            Err(error) => error,
            Ok(_) => panic!("mismatched Raft-state and applied captures must fail readiness"),
        };
        assert!(matches!(
            rejected_error,
            ControlPlaneError::AuthorityNotServing
        ));
        rejected_timer.await.unwrap();
        assert!(
            rejected_retirement_watchdog.join().unwrap(),
            "single-worker executor must progress while an early-error generation is destroyed"
        );

        let channel_retirement_hook =
            Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let response_ready_notify = Arc::new(tokio::sync::Notify::new());
        authority1.set_linearized_snapshot_retirement_hook_for_test(Arc::clone(
            &channel_retirement_hook,
        ));
        authority1.set_linearized_state_machine_response_ready_notify_for_test(Arc::clone(
            &response_ready_notify,
        ));
        let channel_retirement_entered = Arc::new(AtomicBool::new(false));
        let channel_timer_completed = Arc::new(AtomicBool::new(false));
        let channel_retirement_watchdog = state_machine_retirement_progress_watchdog(
            Arc::clone(&channel_retirement_hook),
            Arc::clone(&channel_retirement_entered),
            Arc::clone(&channel_timer_completed),
        );
        let channel_timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
            channel_retirement_entered,
            channel_timer_completed,
        ));
        let mut unpolled_receiver =
            Box::pin(authority1.retained_state_machine_snapshot_generation());
        assert!(
            futures_util::poll!(unpolled_receiver.as_mut()).is_pending(),
            "state-machine generation request must suspend before its response is delivered"
        );
        ControlPlaneRaftTypeConfig::timeout(
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            response_ready_notify.notified(),
        )
        .await
        .expect("state-machine worker should send the retained generation response");
        authority1
            .raft()
            .client_write(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(711),
                availability: NodeAvailabilityState::Unavailable,
            })
            .await
            .unwrap();
        drop(unpolled_receiver);
        channel_timer.await.unwrap();
        assert!(
            channel_retirement_watchdog.join().unwrap(),
            "single-worker executor must progress when a sent generation response is cancelled"
        );

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
fn control_plane_openraft_plural_staging_and_install_replicate_and_replay_after_leader_transfer() {
    ControlPlaneRaftTypeConfig::run(async {
        let ThreeVoterAuthorityFixture {
            network: _,
            config: _,
            leader_log_store: _,
            third_log_store: _,
            authority1,
            authority2,
            authority3,
        } = initialized_three_node_voter_authorities(
            "control-plane-raft-plural-staging-install-test",
            1201,
            1202,
            1203,
        )
        .await;
        let authority1 = Arc::new(authority1);
        let authority2 = Arc::new(authority2);
        let authority3 = Arc::new(authority3);
        let mut leader = crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost::new_for_test(
            tokio::runtime::Handle::current(),
            Arc::clone(&authority1),
            false,
        )
        .unwrap();

        let pg_ids = [PgId::new(70), PgId::new(71), PgId::new(72)];
        let storage_nodes = (1..=4)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    format!("unix:///raft-batch-storage-{node_id}.sock"),
                )
            })
            .collect::<Vec<_>>();
        let source_acting_set = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let pg_acting_sets = pg_ids
            .iter()
            .copied()
            .map(|pg_id| (pg_id, source_acting_set.clone()))
            .collect::<Vec<_>>();
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                3,
                [0x6a; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1201, 1202, 1203],
                &storage_nodes,
                &pg_acting_sets,
                crate::control_plane::test_certified_storage_placement_policy(
                    (1..=4).map(NodeId::new),
                    3,
                    1,
                ),
            )
            .unwrap();
        leader
            .submit_command_for_test(
                ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                    nodes: storage_nodes.clone(),
                    pg_acting_sets,
                    topology,
                },
            )
            .unwrap();

        let heartbeat =
            |host: &mut crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost,
             node_id: u32,
             lease_ms: u64,
             state: Option<PgState>,
                heartbeat_at_ms: u64| {
                for _ in 0..4 {
                    let observed_snapshot = host.current_snapshot_for_test().unwrap();
                    let observed_epoch = observed_snapshot.cluster_epoch();
                    let observed_pg_ids = pg_ids
                        .iter()
                        .copied()
                        .filter(|pg_id| {
                            observed_snapshot
                                .pg(*pg_id)
                                .is_some_and(|pg| pg.acting_set().contains(&NodeId::new(node_id)))
                                || observed_snapshot
                                    .unavailable_pg_placement_transition(*pg_id)
                                    .is_some_and(|transition| {
                                        transition.destination_epoch().is_some()
                                            && transition
                                                .destination_acting_set()
                                                .contains(&NodeId::new(node_id))
                                    })
                        })
                        .collect::<Vec<_>>();
                    let refresh = match host.refresh_node_heartbeat(
                        NodeHeartbeat {
                            node_id: NodeId::new(node_id),
                            node_incarnation: 1,
                            endpoint: storage_nodes
                                [usize::try_from(node_id.checked_sub(1).unwrap()).unwrap()]
                            .1
                            .clone(),
                            observed_epoch,
                            requested_lease_duration_ms: lease_ms,
                            cluster_map_history_route_scan_generation:
                                std::num::NonZeroU64::new(1).unwrap(),
                            cluster_map_history_route_references: Default::default(),
                            pg_observations: state
                                .into_iter()
                                .flat_map(|state| {
                                    observed_pg_ids.iter().copied().map(move |pg_id| {
                                        NodePgHeartbeatObservation {
                                            pg_id,
                                            state,
                                            metadata_proof: PgMetadataProof::empty(),
                                            pending_metadata_command: None,
                                        }
                                    })
                                })
                                .collect(),
                        },
                        heartbeat_at_ms,
                    ) {
                        Ok(refresh) => refresh,
                        Err(ControlPlaneError::PgPrimaryObservationNotActive { .. })
                            if state == Some(PgState::Active) =>
                        {
                            continue;
                        }
                        Err(error) => panic!(
                            "storage node {node_id} {state:?} heartbeat failed: {error:?}"
                        ),
                    };
                    if refresh.lease().serving() {
                        return;
                    }
                }
                panic!("storage node {node_id} did not receive a serving lease");
            };

        for node_id in 1..=4 {
            heartbeat(
                &mut leader,
                node_id,
                10_000,
                None,
                1_000 + u64::from(node_id),
            );
        }
        for node_id in 1..=3 {
            heartbeat(
                &mut leader,
                node_id,
                10_000,
                Some(PgState::Peering),
                2_000 + u64::from(node_id),
            );
        }
        for node_id in 1..=3 {
            heartbeat(
                &mut leader,
                node_id,
                if node_id == 1 { 100 } else { 10_000 },
                Some(PgState::Active),
                3_000 + u64::from(node_id),
            );
        }
        let failed_deadline = leader
            .current_snapshot_for_test()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        leader.expire_heartbeat_leases(failed_deadline).unwrap();
        for node_id in 2..=4 {
            heartbeat(
                &mut leader,
                node_id,
                10_000,
                None,
                failed_deadline + u64::from(node_id),
            );
        }
        let proof_at_ms = leader
            .current_snapshot_for_test()
            .unwrap()
            .max_committed_timestamp_ms()
            .unwrap()
            + 1;
        for node_id in [2, 3] {
            heartbeat(
                &mut leader,
                node_id,
                10_000,
                Some(PgState::Peering),
                proof_at_ms + u64::from(node_id),
            );
        }
        let begin_at_ms = leader
            .current_snapshot_for_test()
            .unwrap()
            .unavailable_node_observation(NodeId::new(1))
            .unwrap()
            .observed_at_ms()
            + 1;
        let mut cursor = crate::control_plane::UnavailablePgReconciliationCursor::start();
        let begun = leader
            .poll_unavailable_pg_reconciliation_batch(&mut cursor, begin_at_ms)
            .unwrap();
        assert!(begun.rejected.is_empty());
        assert_eq!(
            begun
                .work
                .iter()
                .map(crate::control_plane::UnavailablePgReconciliationWork::pg_id)
                .collect::<Vec<_>>(),
            pg_ids
        );

        let begun_snapshot = leader.current_snapshot_for_test().unwrap();
        let authorizations = pg_ids
            .into_iter()
            .enumerate()
            .map(|(index, pg_id)| {
                let transition = begun_snapshot
                    .unavailable_pg_placement_transition(pg_id)
                    .unwrap();
                crate::control_plane_command::UnavailablePgStagingIntentAuthorizationRequest {
                    unavailable_transition:
                        crate::control_plane::UnavailablePgTransitionMutationBinding::new(
                            transition.pg_id(),
                            transition.transition_epoch(),
                            transition.source_epoch(),
                            transition.source_acting_set().to_vec(),
                            transition.destination_acting_set().to_vec(),
                    ),
                    staging_generation: transition.transition_epoch().get(),
                    artifact_target_epoch: ClusterEpoch::new(
                        begun_snapshot.cluster_epoch().get() + 1,
                    )
                    .unwrap(),
                    artifact_digest: [0x80 + u8::try_from(index).unwrap(); 32],
                    artifact_length: 8_192 + u64::try_from(index).unwrap(),
                    artifact_format_version:
                        crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
                }
            })
            .collect::<Vec<_>>();
        let authorization_epoch = begun_snapshot.cluster_epoch();
        let authorized = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::authorize_unavailable_pg_staging_intents_batch(
            &mut leader,
            &authorizations,
        )
        .unwrap();
        assert_eq!(authorized.cluster_epoch(), authorization_epoch);
        let authorization_applied = authority1.status().await.unwrap().applied().unwrap();
        authority2
            .wait_for_applied_log_id(
                authorization_applied,
                Duration::from_secs(1),
                "second voter applied plural staging authorization",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                authorization_applied,
                Duration::from_secs(1),
                "third voter applied plural staging authorization",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            let snapshot = authority
                .durable_state_machine_snapshot_for_test()
                .await
                .unwrap();
            assert_eq!(snapshot, authorized);
        }

        let transfer = PgMetadataTransferProof::new(
            authorizations[0].unavailable_transition.source_epoch(),
            PgMetadataProof::empty(),
        );
        let destination_nodes = authorized
            .unavailable_pg_placement_transition(pg_ids[0])
            .unwrap()
            .destination_acting_set()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut evidence_digests = BTreeMap::new();
        let mut evidence_actors = Vec::new();
        let mut evidence_tips = BTreeMap::new();
        for node_id in destination_nodes {
            let node = authorized.node(node_id).unwrap();
            let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                node_id,
                node.node_incarnation(),
                node.endpoint().to_owned(),
            )
            .unwrap();
            evidence_actors.push(actor.clone());
            let mut previous_receipt = None;
            for authorization in &authorizations[..2] {
                let intent =
                    crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
                        &authorization.unavailable_transition,
                        authorization.artifact_digest,
                        authorization.artifact_length,
                        authorization.artifact_format_version,
                    )
                    .unwrap();
                let page =
                    crate::pg_store::metadata_transfer_staging_publication_evidence_page_for_test(
                        actor.clone(),
                        &intent,
                        transfer,
                        previous_receipt.as_ref(),
                    );
                evidence_digests.insert(
                    (authorization.unavailable_transition.pg_id(), node_id),
                    checksum::sha256::digest(page.entries()[0].evidence()),
                );
                let apply_receipt = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::apply_metadata_transfer_staging_evidence_page(
                    &mut leader,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap();
                previous_receipt = Some(
                    crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap(),
                );
            }
            evidence_tips.insert(node_id, previous_receipt.unwrap());
        }

        let checkpoint_epoch = leader
            .current_snapshot_for_test()
            .unwrap()
            .cluster_epoch();
        let mut maintenance_cursor =
            crate::control_plane::MetadataTransferStagingMaintenanceCursor::start();
        for _ in 0..64 {
            leader
                .maintain_metadata_transfer_staging_evidence_once(&mut maintenance_cursor)
                .unwrap();
            if crate::control_plane::format_snapshot(
                &leader.current_snapshot_for_test().unwrap(),
            )
                .matches("metadata_transfer_staging_evidence_checkpoint=")
                .count()
                == evidence_actors.len()
            {
                break;
            }
        }
        let checkpointed = leader.current_snapshot_for_test().unwrap();
        assert_eq!(checkpointed.cluster_epoch(), checkpoint_epoch);
        assert_eq!(
            crate::control_plane::format_snapshot(&checkpointed)
                .matches("metadata_transfer_staging_evidence_checkpoint=")
                .count(),
            evidence_actors.len()
        );
        let checkpoint_applied = authority1.status().await.unwrap().applied().unwrap();
        authority2
            .wait_for_applied_log_id(
                checkpoint_applied,
                Duration::from_secs(1),
                "second voter applied staging evidence checkpoints",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                checkpoint_applied,
                Duration::from_secs(1),
                "third voter applied staging evidence checkpoints",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            assert_eq!(
                authority
                    .durable_state_machine_snapshot_for_test()
                    .await
                    .unwrap(),
                checkpointed
            );
        }

        let install_source = leader.current_snapshot_for_test().unwrap();
        let destination_epoch = ClusterEpoch::new(install_source.cluster_epoch().get() + 1).unwrap();
        let install_requests = authorizations[..2]
            .iter()
            .map(|authorization| {
                let transition = install_source
                    .unavailable_pg_placement_transition(
                        authorization.unavailable_transition.pg_id(),
                    )
                    .unwrap();
                let mut publications = transition
                    .destination_acting_set()
                    .iter()
                    .copied()
                    .map(|node_id| {
                        let node = install_source.node(node_id).unwrap();
                        crate::control_plane_command::UnavailablePgStagingPublicationBinding {
                            node_id,
                            node_incarnation: node.node_incarnation(),
                            endpoint: node.endpoint().to_owned(),
                            evidence_digest: evidence_digests
                                [&(authorization.unavailable_transition.pg_id(), node_id)],
                        }
                    })
                    .collect::<Vec<_>>();
                publications.sort_by_key(|publication| publication.node_id);
                crate::control_plane_command::UnavailablePgTransitionInstallRequest {
                    unavailable_transition: authorization.unavailable_transition.clone(),
                    transfer,
                    expected_destination_epoch: destination_epoch,
                    publications,
                }
            })
            .collect::<Vec<_>>();
        let installed = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::install_unavailable_pg_placement_transitions_batch(
            &mut leader,
            &install_requests,
            destination_epoch,
        )
        .unwrap();
        assert_eq!(installed.cluster_epoch(), destination_epoch);
        for pg_id in &pg_ids[..2] {
            let transition = installed
                .unavailable_pg_placement_transition(*pg_id)
                .unwrap();
            assert_eq!(transition.destination_epoch(), Some(destination_epoch));
        }
        let install_applied = authority1.status().await.unwrap().applied().unwrap();
        authority2
            .wait_for_applied_log_id(
                install_applied,
                Duration::from_secs(1),
                "second voter applied plural destination installation",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                install_applied,
                Duration::from_secs(1),
                "third voter applied plural destination installation",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            assert_eq!(
                authority
                    .durable_state_machine_snapshot_for_test()
                    .await
                    .unwrap(),
                installed
            );
        }

        let readiness_at_ms = installed
            .unavailable_pg_placement_transitions()
            .map(|transition| transition.grace_cutoff_ms())
            .max()
            .unwrap()
            .max(installed.max_committed_timestamp_ms().unwrap())
            + crate::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS
            + 1;
        let destination_nodes = installed
            .unavailable_pg_placement_transition(pg_ids[0])
            .unwrap()
            .destination_acting_set()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for (offset, node_id) in destination_nodes.iter().copied().enumerate() {
            heartbeat(
                &mut leader,
                node_id.as_u32(),
                10_000,
                Some(PgState::Peering),
                readiness_at_ms + u64::try_from(offset).unwrap(),
            );
        }
        let ready_snapshot = leader.current_snapshot_for_test().unwrap();
        let work = pg_ids[..2]
            .iter()
            .map(|pg_id| {
                crate::control_plane::UnavailablePgReconciliationWork::from_transition(
                    ready_snapshot
                        .unavailable_pg_placement_transition(*pg_id)
                        .unwrap(),
                    crate::control_plane::UnavailablePgReconciliationStage::PayloadReadiness,
                )
            })
            .collect::<Vec<_>>();
        let completion = leader
            .complete_unavailable_pg_reconciliation_batch(
                &work,
                ready_snapshot.max_committed_timestamp_ms().unwrap() + 1,
            )
            .unwrap();
        assert!(
            completion.rejected.is_empty(),
            "unexpected completion rejection: {:?}",
            completion.rejected
        );
        assert_eq!(completion.completed.len(), 2);
        assert!(completion.rederive.is_empty());
        let completed = leader.current_snapshot_for_test().unwrap();
        for pg_id in &pg_ids[..2] {
            assert!(completed
                .unavailable_pg_placement_transition(*pg_id)
                .is_none());
        }

        let mut tombstone_digests = BTreeMap::new();
        for actor in &evidence_actors {
            let mut previous_receipt = evidence_tips[&actor.node_id()].clone();
            for authorization in &authorizations[..2] {
                let page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
                    actor.clone(),
                    &authorization.unavailable_transition,
                    authorization.artifact_digest,
                    authorization.artifact_length,
                    authorization.artifact_format_version,
                    crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                    Some(&previous_receipt),
                );
                tombstone_digests.insert(
                    (
                        authorization.unavailable_transition.pg_id(),
                        actor.node_id(),
                    ),
                    checksum::sha256::digest(page.entries()[0].evidence()),
                );
                let apply_receipt = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::apply_metadata_transfer_staging_evidence_page(
                    &mut leader,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap();
                previous_receipt =
                    crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap();
            }
            let trailing_authorization = &authorizations[2];
            let trailing_intent =
                crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
                    &trailing_authorization.unavailable_transition,
                    trailing_authorization.artifact_digest,
                    trailing_authorization.artifact_length,
                    trailing_authorization.artifact_format_version,
                )
                .unwrap();
            let trailing_page =
                crate::pg_store::metadata_transfer_staging_publication_evidence_page_for_test(
                    actor.clone(),
                    &trailing_intent,
                    transfer,
                    Some(&previous_receipt),
                );
            <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::apply_metadata_transfer_staging_evidence_page(
                &mut leader,
                trailing_page.operation_payload().to_vec(),
                trailing_page.page_digest(),
            ).unwrap();
        }
        for actor in &evidence_actors {
            <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::checkpoint_metadata_transfer_staging_evidence_pages(
                &mut leader,
                actor.node_id(),
                actor.node_incarnation(),
                2,
                4,
            )
            .unwrap();
        }
        let cleanups = authorizations[..2]
            .iter()
            .map(|authorization| {
                let transition = completed
                    .retained_unavailable_pg_placement_transitions()
                    .find(|transition| {
                        transition.pg_id() == authorization.unavailable_transition.pg_id()
                            && transition.transition_epoch()
                                == authorization.unavailable_transition.transition_epoch()
                    })
                    .unwrap();
                let mut tombstones = transition
                    .destination_acting_set()
                    .iter()
                    .copied()
                    .map(|node_id| {
                        let node = completed.node(node_id).unwrap();
                        crate::control_plane_command::MetadataTransferStagingTombstoneBinding {
                            node_id,
                            node_incarnation: node.node_incarnation(),
                            endpoint: node.endpoint().to_owned(),
                            evidence_digest: tombstone_digests
                                [&(authorization.unavailable_transition.pg_id(), node_id)],
                        }
                    })
                    .collect::<Vec<_>>();
                tombstones.sort_by_key(|tombstone| tombstone.node_id);
                crate::control_plane_command::FinalizeMetadataTransferStagingGenerationRequest {
                    unavailable_transition: authorization.unavailable_transition.clone(),
                    staging_generation: authorization.staging_generation,
                    disposition: crate::control_plane_command::MetadataTransferStagingCleanupDisposition::Completed,
                    tombstones,
                }
            })
            .collect::<Vec<_>>();
        let cleanup_epoch = leader
            .current_snapshot_for_test()
            .unwrap()
            .cluster_epoch();
        let mut finalized = None;
        for cleanup in &cleanups {
            finalized = Some(<crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::finalize_metadata_transfer_staging_generation(
                &mut leader,
                cleanup.clone(),
            ).unwrap());
        }
        let finalized = finalized.unwrap();
        assert_eq!(finalized.cluster_epoch(), cleanup_epoch);
        let cleanup_applied = authority1.status().await.unwrap().applied().unwrap();
        authority2
            .wait_for_applied_log_id(
                cleanup_applied,
                Duration::from_secs(1),
                "second voter applied staging cleanup",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                cleanup_applied,
                Duration::from_secs(1),
                "third voter applied staging cleanup",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            assert_eq!(
                authority
                    .durable_state_machine_snapshot_for_test()
                    .await
                    .unwrap(),
                finalized
            );
        }
        let mut collapsed = None;
        for actor in &evidence_actors {
            for (first_generation, last_generation) in [(1, 1), (2, 4)] {
                <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
                    &mut leader,
                    actor.node_id(),
                    actor.node_incarnation(),
                    first_generation,
                    last_generation,
                ).unwrap();
            }
            collapsed = Some(<crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
                &mut leader,
                actor.node_id(),
                actor.node_incarnation(),
                1,
                4,
            ).unwrap());
        }
        let collapsed = collapsed.unwrap();
        assert_eq!(collapsed.cluster_epoch(), cleanup_epoch);
        let collapse_applied = authority1.status().await.unwrap().applied().unwrap();
        authority2
            .wait_for_applied_log_id(
                collapse_applied,
                Duration::from_secs(1),
                "second voter applied staging checkpoint collapse",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                collapse_applied,
                Duration::from_secs(1),
                "third voter applied staging checkpoint collapse",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            assert_eq!(
                authority
                    .durable_state_machine_snapshot_for_test()
                    .await
                    .unwrap(),
                collapsed
            );
        }

        authority1.transfer_leadership_to(1202).await.unwrap();
        authority2
            .wait_for_current_leader(
                1202,
                Duration::from_secs(1),
                "second voter accepted leadership before plural replay",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority2,
            Duration::from_secs(1),
            "second voter became serving before plural replay",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let mut successor =
            crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost::new_for_test(
                tokio::runtime::Handle::current(),
                Arc::clone(&authority2),
                false,
            )
            .unwrap();
        for actor in &evidence_actors {
            let replayed_checkpoint = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::checkpoint_metadata_transfer_staging_evidence_pages(
                &mut successor,
                actor.node_id(),
                actor.node_incarnation(),
                1,
                1,
            )
            .unwrap();
            assert_eq!(replayed_checkpoint, collapsed);
            let replayed_collapse = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
                &mut successor,
                actor.node_id(),
                actor.node_incarnation(),
                1,
                1,
            ).unwrap();
            assert_eq!(replayed_collapse, collapsed);
            let replayed_second_collapse = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
                &mut successor,
                actor.node_id(),
                actor.node_incarnation(),
                2,
                4,
            ).unwrap();
            assert_eq!(replayed_second_collapse, collapsed);
            let replayed_coalescing = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
                &mut successor,
                actor.node_id(),
                actor.node_incarnation(),
                1,
                4,
            ).unwrap();
            assert_eq!(replayed_coalescing, collapsed);
        }
        let replayed_authorization = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::authorize_unavailable_pg_staging_intents_batch(
            &mut successor,
            &authorizations,
        )
        .unwrap();
        assert_eq!(replayed_authorization, collapsed);
        let replayed_install = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::install_unavailable_pg_placement_transitions_batch(
            &mut successor,
            &install_requests,
            destination_epoch,
        )
        .unwrap();
        assert_eq!(replayed_install, collapsed);
        for cleanup in cleanups {
            let replayed_cleanup = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::finalize_metadata_transfer_staging_generation(
                &mut successor,
                cleanup,
            )
            .unwrap();
            assert_eq!(replayed_cleanup, collapsed);
        }

        let replay_applied = authority2.status().await.unwrap().applied().unwrap();
        authority1
            .wait_for_applied_log_id(
                replay_applied,
                Duration::from_secs(1),
                "old leader applied exact plural replay",
            )
            .await
            .unwrap();
        authority3
            .wait_for_applied_log_id(
                replay_applied,
                Duration::from_secs(1),
                "third voter applied exact plural replay",
            )
            .await
            .unwrap();
        for authority in [&authority1, &authority2, &authority3] {
            assert_eq!(
                authority
                    .durable_state_machine_snapshot_for_test()
                    .await
                    .unwrap(),
                    collapsed
            );
        }

        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
        authority3.shutdown().await.unwrap();
    });
}

#[test]
fn authenticated_staging_evidence_preflight_suppresses_invalid_pages_and_exact_replays() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let cluster_id = "authenticated-staging-evidence-preflight";
        let authority_node_id = 1301;
        let artifact_path = tmp.path().join("authority.state");
        let wal_path = tmp.path().join("authority.wal");
        let authority = Arc::new(
            ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                cluster_id,
                authority_node_id,
                &artifact_path,
                &wal_path,
            )
            .await
            .unwrap(),
        );
        authority
            .initialize_single_node_membership(authority_node_id)
            .await
            .unwrap();
        authority
            .wait_for_current_leader(
                authority_node_id,
                Duration::from_secs(1),
                "staging-evidence preflight leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            Duration::from_secs(1),
            "staging-evidence preflight serving state",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let mut host = crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost::new_for_test(
            tokio::runtime::Handle::current(),
            Arc::clone(&authority),
            true,
        )
        .unwrap();

        let pg_id = PgId::new(70);
        let storage_nodes = (1..=4)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    format!("unix:///staging-evidence-preflight-{node_id}.sock"),
                )
            })
            .collect::<Vec<_>>();
        let source_acting_set = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                1,
                [0x6b; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![authority_node_id],
                &storage_nodes,
                &[(pg_id, source_acting_set.clone())],
                crate::control_plane::test_certified_storage_placement_policy(
                    (1..=4).map(NodeId::new),
                    3,
                    1,
                ),
            )
            .unwrap();
        host.submit_command_for_test(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: storage_nodes.clone(),
            pg_acting_sets: vec![(pg_id, source_acting_set)],
            topology,
        })
        .unwrap();

        let heartbeat =
            |host: &mut crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost,
             node_id: u32,
             lease_ms: u64,
             state: Option<PgState>,
             heartbeat_at_ms: u64| {
                for _ in 0..4 {
                    let observed_epoch =
                        host.current_snapshot_for_test().unwrap().cluster_epoch();
                    let refresh = match host.refresh_node_heartbeat(
                        crate::control_plane::NodeHeartbeat {
                            node_id: NodeId::new(node_id),
                            node_incarnation: 1,
                            endpoint: storage_nodes
                                [usize::try_from(node_id.checked_sub(1).unwrap()).unwrap()]
                            .1
                            .clone(),
                            observed_epoch,
                            requested_lease_duration_ms: lease_ms,
                            cluster_map_history_route_scan_generation:
                                std::num::NonZeroU64::new(1).unwrap(),
                            cluster_map_history_route_references: Default::default(),
                            pg_observations: state
                                .map(|state| {
                                    vec![crate::control_plane::NodePgHeartbeatObservation {
                                        pg_id,
                                        state,
                                        metadata_proof: PgMetadataProof::empty(),
                                        pending_metadata_command: None,
                                    }]
                                })
                                .unwrap_or_default(),
                        },
                        heartbeat_at_ms,
                    ) {
                        Ok(refresh) => refresh,
                        Err(ControlPlaneError::PgPrimaryObservationNotActive { .. })
                            if state == Some(PgState::Active) =>
                        {
                            continue;
                        }
                        Err(error) => panic!(
                            "storage node {node_id} {state:?} heartbeat failed: {error:?}"
                        ),
                    };
                    if refresh.lease().serving() {
                        return;
                    }
                }
                panic!("storage node {node_id} did not receive a serving lease");
            };

        for node_id in 1..=4 {
            heartbeat(&mut host, node_id, 10_000, None, 1_000 + u64::from(node_id));
        }
        for node_id in 1..=3 {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                Some(PgState::Peering),
                2_000 + u64::from(node_id),
            );
        }
        for node_id in 1..=3 {
            heartbeat(
                &mut host,
                node_id,
                if node_id == 1 { 100 } else { 10_000 },
                Some(PgState::Active),
                3_000 + u64::from(node_id),
            );
        }
        let failed_deadline = host
            .current_snapshot_for_test()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        host.expire_heartbeat_leases(failed_deadline).unwrap();
        for node_id in 2..=4 {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                None,
                failed_deadline + u64::from(node_id),
            );
        }
        let proof_at_ms = host
            .current_snapshot_for_test()
            .unwrap()
            .max_committed_timestamp_ms()
            .unwrap()
            + 1;
        for node_id in [2, 3] {
            heartbeat(
                &mut host,
                node_id,
                10_000,
                Some(PgState::Peering),
                proof_at_ms + u64::from(node_id),
            );
        }
        let begin_at_ms = host
            .current_snapshot_for_test()
            .unwrap()
            .unavailable_node_observation(NodeId::new(1))
            .unwrap()
            .observed_at_ms()
            + 1;
        let mut cursor = crate::control_plane::UnavailablePgReconciliationCursor::start();
        let begun = host
            .poll_unavailable_pg_reconciliation_batch(&mut cursor, begin_at_ms)
            .unwrap();
        assert!(begun.rejected.is_empty());
        assert_eq!(begun.work.len(), 1);

        let begun_snapshot = host.current_snapshot_for_test().unwrap();
        let transition = begun_snapshot
            .unavailable_pg_placement_transition(pg_id)
            .unwrap();
        let authorization =
            crate::control_plane_command::UnavailablePgStagingIntentAuthorizationRequest {
                unavailable_transition:
                    crate::control_plane::UnavailablePgTransitionMutationBinding::new(
                        transition.pg_id(),
                        transition.transition_epoch(),
                        transition.source_epoch(),
                        transition.source_acting_set().to_vec(),
                        transition.destination_acting_set().to_vec(),
                ),
                staging_generation: transition.transition_epoch().get(),
                artifact_target_epoch: ClusterEpoch::new(
                    begun_snapshot.cluster_epoch().get() + 1,
                )
                .unwrap(),
                artifact_digest: [0x80; 32],
                artifact_length: 8_192,
                artifact_format_version:
                    crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            };
        let authorized = <crate::control_plane_raft_host::ControlPlaneRaftAuthorityHost as crate::control_plane::ControlPlaneAdmin>::authorize_unavailable_pg_staging_intents_batch(
            &mut host,
            std::slice::from_ref(&authorization),
        )
        .unwrap();
        let actor_node_id = authorization.unavailable_transition.destination_acting_set()[0];
        let actor_node = authorized.node(actor_node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            actor_node_id,
            actor_node.node_incarnation(),
            actor_node.endpoint().to_owned(),
        )
        .unwrap();
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &authorization.unavailable_transition,
            authorization.artifact_digest,
            authorization.artifact_length,
            authorization.artifact_format_version,
        )
        .unwrap();
        let transfer = PgMetadataTransferProof::new(
            intent.source_epoch(),
            PgMetadataProof::empty(),
        );
        let valid_page =
            crate::pg_store::metadata_transfer_staging_publication_evidence_page_for_test(
                actor.clone(),
                &intent,
                transfer,
                None,
            );
        let invalid_page =
            crate::pg_store::metadata_transfer_staging_evidence_page_with_duplicate_member_for_test(
                actor.clone(),
                &intent,
                crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                None,
            );
        let invalid_target_page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
            actor.clone(),
            &intent,
            intent.transition_epoch(),
            transfer,
            None,
        );
        let invalid_source_page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
            actor.clone(),
            &intent,
            ClusterEpoch::new(intent.transition_epoch().get().checked_add(1).unwrap()).unwrap(),
            PgMetadataTransferProof::new(
                intent.transition_epoch(),
                PgMetadataProof::empty(),
            ),
            None,
        );
        let credential_input = crate::control_plane::ControlPlaneStorageNodeAuthCredentialInput {
            node_id: actor_node_id,
            credential_id: "staging-evidence-node".to_owned(),
            credential_version: 1,
            secret: b"staging-evidence-secret".to_vec(),
        };
        let credential = crate::control_plane::ControlPlaneStorageNodeAuthCredential::new(
            credential_input.clone(),
        )
        .unwrap();
        let scoped_credential = credential
            .scoped_for_cluster_and_incarnation(cluster_id, actor.node_incarnation())
            .unwrap();
        let verifier = crate::control_plane::ControlPlaneUnixAuthVerifier::new(
            cluster_id,
            vec![credential],
        )
        .unwrap();
        let socket_path = tmp.path().join("staging-evidence.sock");
        let listener = crate::control_plane::ControlPlaneRpcServerListener::unix(
            UnixListener::bind(&socket_path).unwrap(),
            4,
            crate::control_plane::CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(2),
        )
        .unwrap();
        let confirmation_host = host.clone();
        let policy = crate::control_plane::ControlPlaneRpcServerPolicy::new(
            crate::control_plane::ControlPlaneRpcServerRole::Ordinary,
            4,
            crate::control_plane::CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        )
        .unwrap()
        .with_auth_verifier(Arc::new(verifier))
        .with_authority_confirmation(Arc::new(move || {
            confirmation_host
                .block_on_for_test(
                    confirmation_host
                        .authority_for_test()
                        .confirmed_linearized_authority_status(),
                )
                .map(|_| ())
        }));
        let server = std::thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(
                    Arc::new(Mutex::new(host)),
                    policy,
                    vec![
                        20_000;
                        3 + crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT + 1 + 2
                    ],
                    |_| {},
                )
                .unwrap();
        });
        let client = crate::control_plane::AuthenticatedUnixControlPlaneClient::new(
            crate::control_plane::UnixControlPlaneClient::new(&socket_path),
            scoped_credential,
        );

        let baseline_log_id = authority.status().await.unwrap().last_log_id().unwrap();
        let baseline_metrics = authority.durability_metric_snapshots_for_test();
        for invalid_page in [&invalid_page, &invalid_target_page, &invalid_source_page] {
            let error = client
                .publish_metadata_transfer_staging_evidence_page(invalid_page, 20_000)
                .expect_err("semantically invalid evidence must be rejected before Raft");
            assert!(
                matches!(error, ControlPlaneError::RpcRemote { .. }),
                "unexpected semantic rejection: {error:?}"
            );
        }
        assert_eq!(
            authority.status().await.unwrap().last_log_id(),
            Some(baseline_log_id)
        );
        assert_eq!(
            authority.durability_metric_snapshots_for_test(),
            baseline_metrics,
            "preflight rejection must not submit, append, or checkpoint"
        );

        let first_receipt = client
            .publish_metadata_transfer_staging_evidence_page(&valid_page, 20_000)
            .unwrap();
        assert!(first_receipt.is_for_page(&valid_page));
        let mut previous_receipt = first_receipt.clone();
        for offset in 1..crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
            let target_epoch = ClusterEpoch::new(
                intent
                    .transition_epoch()
                    .get()
                    .checked_add(u64::try_from(offset).unwrap() + 1)
                    .unwrap(),
            )
            .unwrap();
            let page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
                actor.clone(),
                &intent,
                target_epoch,
                PgMetadataTransferProof::new(
                    intent.source_epoch(),
                    PgMetadataProof::empty(),
                ),
                Some(&previous_receipt),
            );
            previous_receipt = client
                .publish_metadata_transfer_staging_evidence_page(&page, 20_000)
                .unwrap();
            assert!(previous_receipt.is_for_page(&page));
        }
        let capped_log_id = authority.status().await.unwrap().last_log_id().unwrap();
        assert_eq!(
            capped_log_id.index(),
            baseline_log_id.index()
                + u64::try_from(crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT).unwrap()
        );
        let capped_metrics = authority.durability_metric_snapshots_for_test();
        assert_eq!(
            capped_metrics.command.submit_total,
            baseline_metrics.command.submit_total
                + u64::try_from(crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT).unwrap()
        );
        assert_eq!(
            capped_metrics.checkpoint.store_total,
            baseline_metrics.checkpoint.store_total
                + u64::try_from(crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT).unwrap()
        );

        let excess_target_epoch = ClusterEpoch::new(
            intent
                .transition_epoch()
                .get()
                .checked_add(
                    u64::try_from(crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT).unwrap()
                        + 1,
                )
                .unwrap(),
        )
        .unwrap();
        let excess_page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
            actor,
            &intent,
            excess_target_epoch,
            PgMetadataTransferProof::new(intent.source_epoch(), PgMetadataProof::empty()),
            Some(&previous_receipt),
        );
        let excess_error = client
            .publish_metadata_transfer_staging_evidence_page(&excess_page, 20_000)
            .expect_err("the first publication above the per-intent limit must be rejected");
        assert!(
            matches!(excess_error, ControlPlaneError::RpcRemote { .. }),
            "unexpected excess-publication rejection: {excess_error:?}"
        );
        assert_eq!(
            authority.status().await.unwrap().last_log_id(),
            Some(capped_log_id)
        );
        assert_eq!(
            authority.durability_metric_snapshots_for_test(),
            capped_metrics,
            "excess evidence must not submit, append, or checkpoint"
        );
        let capped_snapshot = authority
            .durable_state_machine_snapshot_for_test()
            .await
            .unwrap();
        assert!(matches!(
            capped_snapshot.apply_control_plane_command(
                ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                    operation_payload: excess_page.operation_payload().to_vec(),
                    page_digest: excess_page.page_digest(),
                }
            ),
            Err(ControlPlaneError::CommandDecode { message })
                if message.contains("per-intent publication-target limit")
        ));

        for _ in 0..2 {
            assert_eq!(
                client
                    .publish_metadata_transfer_staging_evidence_page(&valid_page, 20_000)
                    .unwrap(),
                first_receipt
            );
        }
        server.join().unwrap();
        assert_eq!(
            authority.status().await.unwrap().last_log_id(),
            Some(capped_log_id)
        );
        assert_eq!(
            authority.durability_metric_snapshots_for_test(),
            capped_metrics,
            "exact replay must return its receipt without submitting or checkpointing"
        );

        authority.shutdown().await.unwrap();
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
