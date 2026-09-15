// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[test]
fn control_plane_raft_peer_rpc_append_entries_request_frame_round_trips() {
    let request = AppendEntriesRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 7),
        prev_log_id: Some(raft_log_id(3, 7, 4)),
        entries: vec![normal_entry(
            3,
            7,
            5,
            ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(11),
                membership: NodeMembershipState::Active,
            },
        )],
        leader_commit: Some(raft_log_id(3, 7, 5)),
    };
    let encoded = ControlPlaneRaftPeerRpcRequest::AppendEntries(request.clone())
        .encode_frame()
        .unwrap();
    let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();

    let ControlPlaneRaftPeerRpcRequest::AppendEntries(decoded) = decoded else {
        panic!("decoded wrong peer RPC request variant");
    };
    assert_eq!(decoded.vote, request.vote);
    assert_eq!(decoded.prev_log_id, request.prev_log_id);
    assert_eq!(decoded.entries, request.entries);
    assert_eq!(decoded.leader_commit, request.leader_commit);
}

#[test]
fn control_plane_raft_peer_rpc_rejects_noncurrent_nested_command_versions() {
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(11),
        membership: NodeMembershipState::Active,
    };
    let request = AppendEntriesRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 7),
        prev_log_id: Some(raft_log_id(3, 7, 4)),
        entries: vec![normal_entry(3, 7, 5, command.clone())],
        leader_commit: Some(raft_log_id(3, 7, 5)),
    };
    let encoded = ControlPlaneRaftPeerRpcRequest::AppendEntries(request)
        .encode_frame()
        .unwrap();
    let current_command = encode_control_plane_command(&command).unwrap();
    let command_offsets = encoded
        .windows(current_command.len())
        .enumerate()
        .filter_map(|(offset, candidate)| {
            (candidate == current_command.as_slice()).then_some(offset)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        command_offsets.len(),
        1,
        "current command must occur exactly once in the peer RPC fixture"
    );
    ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();

    let mut unsupported_frames = Vec::new();
    for version in [
        14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 29,
    ] {
        let unsupported_command =
            crate::control_plane_command::encode_control_plane_command_with_version_for_test(
                &command, version,
            )
            .unwrap();
        assert_eq!(unsupported_command.len(), current_command.len());
        let mut unsupported_frame = encoded.clone();
        let command_start = command_offsets[0];
        unsupported_frame
            [command_start..command_start + unsupported_command.len()]
            .copy_from_slice(&unsupported_command);
        refresh_raft_peer_frame_checksum(&mut unsupported_frame);

        assert!(matches!(
            ControlPlaneRaftPeerRpcRequest::decode_frame(&unsupported_frame),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!("unsupported control-plane command version {version}")
        ));
        unsupported_frames.push((version, unsupported_frame));
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let authority = runtime
        .block_on(
            ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "nested-command-version-peer-dispatch",
                1,
            ),
        )
        .unwrap();
    let before = runtime.block_on(authority.status()).unwrap();
    for (version, unsupported_frame) in unsupported_frames {
        assert!(matches!(
            runtime.block_on(handle_control_plane_raft_peer_rpc_frame_with_identity(
                authority.raft(),
                &unsupported_frame,
                None,
            )),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!("unsupported control-plane command version {version}")
        ));
    }
    assert_eq!(runtime.block_on(authority.status()).unwrap(), before);
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_rpc_vote_request_frames_round_trip() {
    let request = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
        last_log_id: Some(raft_log_id(3, 7, 8)),
        leadership_transfer: true,
    };
    let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request.clone())
        .encode_frame()
        .unwrap();
    let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();
    let ControlPlaneRaftPeerRpcRequest::Vote(decoded) = decoded else {
        panic!("decoded wrong vote peer RPC request variant");
    };
    assert_eq!(decoded, request);

    let encoded = ControlPlaneRaftPeerRpcRequest::PreVote(request.clone())
        .encode_frame()
        .unwrap();
    let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();
    let ControlPlaneRaftPeerRpcRequest::PreVote(decoded) = decoded else {
        panic!("decoded wrong pre-vote peer RPC request variant");
    };
    assert_eq!(decoded, request);
}

#[test]
fn control_plane_raft_peer_auth_policy_wraps_and_verifies_request_frame() {
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let request = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: Some(raft_log_id(3, 1, 8)),
        leadership_transfer: false,
    };
    let raw_frame = ControlPlaneRaftPeerRpcRequest::Vote(request.clone())
        .encode_frame_for_peer(&identity)
        .unwrap();
    let signed = test_peer_auth_policy(1)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            raw_frame.clone(),
        )
        .unwrap();

    let verified = test_peer_auth_policy(2)
        .verify_peer_frame(
            &signed,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .unwrap();
    assert_eq!(verified, raw_frame);
    let ControlPlaneRaftPeerRpcRequest::Vote(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&verified, &identity).unwrap()
    else {
        panic!("decoded wrong authenticated peer request variant");
    };
    assert_eq!(decoded, request);
}

#[test]
fn control_plane_raft_peer_auth_policy_wraps_and_verifies_response_frames() {
    let request_identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let response_identity = reverse_raft_peer_frame_identity(&request_identity);

    let samples: Vec<(ControlPlaneAuthOperation, Vec<u8>)> = vec![
        (
            ControlPlaneAuthOperation::RaftAppendEntries,
            ControlPlaneRaftPeerRpcResponse::AppendEntries(AppendEntriesResponse::HigherVote(
                Vote::<ControlPlaneRaftLeaderId>::new(5, 2),
            ))
            .encode_frame_for_peer(&response_identity)
            .unwrap(),
        ),
        (
            ControlPlaneAuthOperation::RaftVote,
            ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 2),
                vote_granted: true,
                last_log_id: Some(raft_log_id(5, 2, 11)),
            })
            .encode_frame_for_peer(&response_identity)
            .unwrap(),
        ),
        (
            ControlPlaneAuthOperation::RaftPreVote,
            ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(5, 2),
                vote_granted: false,
                last_log_id: None,
            })
            .encode_frame_for_peer(&response_identity)
            .unwrap(),
        ),
        (
            ControlPlaneAuthOperation::RaftTransferLeader,
            ControlPlaneRaftPeerRpcResponse::TransferLeader(Ok(()))
                .encode_frame_for_peer(&response_identity)
                .unwrap(),
        ),
        (
            ControlPlaneAuthOperation::RaftSnapshot,
            ControlPlaneRaftPeerSnapshotResponse {
                response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new_committed(
                    5, 2,
                )),
            }
            .encode_frame_for_peer(&response_identity)
            .unwrap(),
        ),
    ];

    for (operation, raw_frame) in samples {
        let signed = test_peer_auth_policy(2)
            .sign_peer_frame(&response_identity, operation, raw_frame.clone())
            .unwrap();
        assert_eq!(
            test_peer_auth_policy(1)
                .verify_peer_frame(&signed, &response_identity, operation, 4096)
                .unwrap(),
            raw_frame
        );
    }
}

#[test]
fn control_plane_raft_peer_auth_policy_rejects_missing_wrong_or_tampered_auth() {
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let raw_frame = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: Some(raft_log_id(3, 1, 8)),
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();
    let signed = test_peer_auth_policy(1)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            raw_frame.clone(),
        )
        .unwrap();
    let verifier = test_peer_auth_policy(2);

    assert!(verifier
        .verify_peer_frame(
            &raw_frame,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .is_err());
    assert!(verifier
        .verify_peer_frame(
            &signed,
            &identity,
            ControlPlaneAuthOperation::RaftPreVote,
            4096,
        )
        .is_err());

    let envelope = ControlPlaneAuthEnvelope::decode_frame(&signed, 4096).unwrap();
    let mut tampered_payload = envelope.payload().to_vec();
    let last = tampered_payload
        .last_mut()
        .expect("sample peer frame is non-empty");
    *last ^= 0x01;
    let tampered =
        ControlPlaneAuthEnvelope::new(crate::control_plane_auth::ControlPlaneAuthEnvelopeInput {
            header: envelope.header().clone(),
            payload: tampered_payload,
            authenticator: envelope.authenticator().to_vec(),
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(verifier
        .verify_peer_frame(
            &tampered,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .is_err());
}

#[test]
fn control_plane_raft_peer_auth_policy_rejects_envelope_payload_binding_mismatch() {
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let append_payload = ControlPlaneRaftPeerRpcRequest::AppendEntries(AppendEntriesRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();

    assert!(test_peer_auth_policy(1)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            append_payload.clone(),
        )
        .is_err());

    let mismatched_envelope = test_peer_scoped_credential(1)
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 2,
            }),
            operation: ControlPlaneAuthOperation::RaftVote,
            issued_at_ms: None,
            expires_at_ms: None,
            sequence: None,
            nonce: Vec::new(),
            payload: append_payload,
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(test_peer_auth_policy(2)
        .verify_peer_frame(
            &mismatched_envelope,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .is_err());

    let wrong_target_identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 1);
    let wrong_target_payload = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: Some(raft_log_id(3, 1, 8)),
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&wrong_target_identity)
    .unwrap();
    let wrong_target_envelope = test_peer_scoped_credential(1)
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 2,
            }),
            operation: ControlPlaneAuthOperation::RaftVote,
            issued_at_ms: None,
            expires_at_ms: None,
            sequence: None,
            nonce: Vec::new(),
            payload: wrong_target_payload,
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(test_peer_auth_policy(2)
        .verify_peer_frame(
            &wrong_target_envelope,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .is_err());

    let response_identity = reverse_raft_peer_frame_identity(&identity);
    let append_response_payload =
        ControlPlaneRaftPeerRpcResponse::AppendEntries(AppendEntriesResponse::HigherVote(Vote::<
            ControlPlaneRaftLeaderId,
        >::new(
            4, 1
        )))
        .encode_frame_for_peer(&response_identity)
        .unwrap();
    let mismatched_response_envelope = test_peer_scoped_credential(2)
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 1,
            }),
            operation: ControlPlaneAuthOperation::RaftVote,
            issued_at_ms: None,
            expires_at_ms: None,
            sequence: None,
            nonce: Vec::new(),
            payload: append_response_payload,
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(test_peer_auth_policy(1)
        .verify_peer_frame(
            &mismatched_response_envelope,
            &response_identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .is_err());
}

#[test]
fn control_plane_raft_peer_auth_policy_bounds_transfer_leader_freshness() {
    let clock = crate::clock::test_time_override_guard(10_000);
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let raw_frame = ControlPlaneRaftPeerRpcRequest::TransferLeader(TransferLeaderRequest::new(
        Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        2,
        Some(raft_log_id(4, 1, 8)),
    ))
    .encode_frame_for_peer(&identity)
    .unwrap();
    let signed = test_peer_auth_policy(1)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftTransferLeader,
            raw_frame.clone(),
        )
        .unwrap();
    let envelope = ControlPlaneAuthEnvelope::decode_frame(&signed, 4096).unwrap();
    assert_eq!(envelope.header().issued_at_ms(), Some(10_000));
    assert_eq!(
        envelope.header().expires_at_ms(),
        Some(10_000 + CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS)
    );

    let verifier = test_peer_auth_policy(2);
    assert_eq!(
        verifier
            .verify_peer_frame(
                &signed,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .unwrap(),
        raw_frame
    );

    clock.set(10_000 - CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS);
    assert_eq!(
        verifier
            .verify_peer_frame(
                &signed,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .unwrap(),
        raw_frame,
        "in-budget cross-host clock skew must be accepted"
    );

    clock.set(10_000 - CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS - 1);
    assert!(
        verifier
            .verify_peer_frame(
                &signed,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .is_err(),
        "transfer-leader auth beyond the clock-skew budget must fail"
    );

    clock.set(10_000 + CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS);
    assert!(
        verifier
            .verify_peer_frame(
                &signed,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .is_err(),
        "expired transfer-leader auth must fail"
    );

    clock.set(10_000);
    let missing_window = test_peer_scoped_credential(1)
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 2,
            }),
            operation: ControlPlaneAuthOperation::RaftTransferLeader,
            issued_at_ms: Some(10_000),
            expires_at_ms: None,
            sequence: None,
            nonce: Vec::new(),
            payload: raw_frame.clone(),
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(
        verifier
            .verify_peer_frame(
                &missing_window,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .is_err(),
        "transfer-leader auth must include a complete freshness window"
    );

    let overlong_window = test_peer_scoped_credential(1)
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 2,
            }),
            operation: ControlPlaneAuthOperation::RaftTransferLeader,
            issued_at_ms: Some(10_000),
            expires_at_ms: Some(10_000 + CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS + 1),
            sequence: None,
            nonce: Vec::new(),
            payload: raw_frame,
        })
        .unwrap()
        .encode_frame()
        .unwrap();
    assert!(
        verifier
            .verify_peer_frame(
                &overlong_window,
                &identity,
                ControlPlaneAuthOperation::RaftTransferLeader,
                4096,
            )
            .is_err(),
        "transfer-leader auth freshness window must be bounded"
    );

    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 2);
    assert_eq!(metrics.rejected_total(), 4);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::RaftTransferLeader),
        2
    );
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::RaftTransferLeader),
        4
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure),
        4
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::StaleCredential),
        0
    );
}

#[test]
fn control_plane_raft_peer_auth_status_snapshot_exposes_redacted_counters() {
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
    let raw_frame = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: Some(raft_log_id(3, 1, 8)),
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();
    let signed = test_peer_auth_policy(1)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            raw_frame.clone(),
        )
        .unwrap();
    let verifier = test_peer_auth_policy(2);
    verifier
        .verify_peer_frame(
            &signed,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .unwrap();
    verifier
        .verify_peer_frame(
            &raw_frame,
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            4096,
        )
        .expect_err("unauthenticated peer frame should be rejected");

    let status = verifier.status_snapshot();
    assert!(status.required());
    assert_eq!(
        status.local_principal(),
        Some(&ControlPlaneAuthPrincipal::RaftPeer { node_id: 2 })
    );
    assert_eq!(status.credential_id(), Some("raft-peer-2"));
    assert_eq!(status.credential_version(), Some(1));
    assert_eq!(status.metrics().accepted_total(), 1);
    assert_eq!(status.metrics().rejected_total(), 1);
    assert_eq!(
        status
            .metrics()
            .accepted_for_operation(ControlPlaneAuthOperation::RaftVote),
        1
    );
    assert_eq!(
        status
            .metrics()
            .rejected_for_reason(ControlPlaneAuthRejectionReason::Malformed),
        1
    );

    let debug = format!("{status:?}");
    assert!(debug.contains("raft-peer-2"));
    assert!(!debug.contains("test-raft-peer-secret-2"));
    assert!(!debug.contains("authenticator"));
    assert!(!debug.contains("payload"));

    let unauthenticated = ControlPlaneRaftPeerTransportPolicy::new(
        "control-plane-raft-peer-transport-test",
        BTreeMap::from([(1, BasicNode::new("node-1"))]),
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .auth_status_snapshot();
    assert!(!unauthenticated.required());
    assert_eq!(unauthenticated.credential_id(), None);
    assert_eq!(unauthenticated.metrics().accepted_total(), 0);
}

#[test]
fn control_plane_raft_peer_rpc_response_frames_round_trip() {
    let append = AppendEntriesResponse::HigherVote(Vote::<ControlPlaneRaftLeaderId>::new(5, 9));
    let encoded = ControlPlaneRaftPeerRpcResponse::AppendEntries(append.clone())
        .encode_frame()
        .unwrap();
    let decoded = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded).unwrap();
    assert_eq!(
        decoded,
        ControlPlaneRaftPeerRpcResponse::AppendEntries(append)
    );

    let vote = VoteResponse {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 9),
        vote_granted: true,
        last_log_id: Some(raft_log_id(5, 9, 12)),
    };
    let encoded = ControlPlaneRaftPeerRpcResponse::Vote(vote.clone())
        .encode_frame()
        .unwrap();
    let decoded = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded).unwrap();
    assert_eq!(decoded, ControlPlaneRaftPeerRpcResponse::Vote(vote));
}

#[test]
fn control_plane_raft_peer_rpc_transfer_leader_frames_round_trip() {
    let request = TransferLeaderRequest::new(
        Vote::<ControlPlaneRaftLeaderId>::new_committed(6, 1),
        2,
        Some(raft_log_id(6, 1, 14)),
    );
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-transfer-leader-frame", 1, 2);
    let encoded = ControlPlaneRaftPeerRpcRequest::TransferLeader(request.clone())
        .encode_frame_for_peer(&identity)
        .unwrap();
    let decoded =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&encoded, &identity).unwrap();
    let ControlPlaneRaftPeerRpcRequest::TransferLeader(decoded) = decoded else {
        panic!("decoded wrong transfer_leader request variant");
    };
    assert_eq!(decoded, request);

    let success = ControlPlaneRaftPeerRpcResponse::TransferLeader(Ok(()));
    let encoded = success.encode_frame_for_peer(&identity).unwrap();
    assert_eq!(
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
        success
    );

    let vote_changed =
        ControlPlaneRaftPeerRpcResponse::TransferLeader(Err(TransferLeaderError::VoteChanged {
            expected: Vote::<ControlPlaneRaftLeaderId>::new_committed(6, 1),
            actual: Vote::<ControlPlaneRaftLeaderId>::new(7, 2),
        }));
    let encoded = vote_changed.encode_frame_for_peer(&identity).unwrap();
    assert_eq!(
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
        vote_changed
    );

    let log_not_flushed =
        ControlPlaneRaftPeerRpcResponse::TransferLeader(Err(TransferLeaderError::LogNotFlushed {
            expected: Some(raft_log_id(6, 1, 14)),
            actual: Some(raft_log_id(6, 2, 12)),
        }));
    let encoded = log_not_flushed.encode_frame_for_peer(&identity).unwrap();
    assert_eq!(
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
        log_not_flushed
    );
}

#[test]
fn control_plane_raft_peer_snapshot_frames_round_trip() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let snapshot = state_machine.build_snapshot().unwrap();
    assert!(!snapshot.snapshot.get_ref().is_empty());

    let request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
        snapshot: snapshot.clone(),
    };
    let encoded = request.encode_frame().unwrap();
    let decoded =
        ControlPlaneRaftPeerSnapshotRequest::decode_frame(&encoded, usize::MAX, usize::MAX)
            .unwrap();
    assert_eq!(decoded.vote, request.vote);
    assert_eq!(decoded.snapshot.meta, request.snapshot.meta);
    assert_eq!(
        decoded.snapshot.snapshot.get_ref(),
        request.snapshot.snapshot.get_ref()
    );

    let response = ControlPlaneRaftPeerSnapshotResponse {
        response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new_committed(2, 1)),
    };
    let encoded = response.encode_frame().unwrap();
    let decoded = ControlPlaneRaftPeerSnapshotResponse::decode_frame(&encoded).unwrap();
    assert_eq!(decoded, response);
}

fn current_peer_snapshot_payload() -> Vec<u8> {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    state_machine
        .build_snapshot()
        .unwrap()
        .snapshot
        .into_inner()
}

fn assert_peer_server_rejects_snapshot_before_publication(
    cluster_name: &str,
    snapshot_payload: Vec<u8>,
    assert_error: impl Fn(&ControlPlaneError),
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name,
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let checkpoint = Arc::new(RecordingPeerServerCheckpoint::default());
    let durability = authority
        .bind_peer_server_durability(checkpoint.clone())
        .unwrap();
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096)
        .unwrap()
        .with_durability(durability);

    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let mut snapshot = state_machine.build_snapshot().unwrap();
    snapshot.snapshot = ControlPlaneRaftSnapshotData::new(snapshot_payload);
    let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
    let request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
        snapshot,
    }
    .encode_frame_for_peer(&identity)
    .unwrap();
    let admission_error = ControlPlaneRaftPeerSnapshotRequest::decode_frame_with_identity(
        &request,
        usize::MAX,
        usize::MAX,
        Some(&identity),
    )
    .unwrap_err();
    assert_error(&admission_error);

    let mut transport_request = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport_request, &request).unwrap();
    let mut stream =
        RecordingPeerServerStream::new(transport_request, Arc::clone(&checkpoint.events));
    let before = runtime.block_on(authority.status()).unwrap();

    let error = handle_control_plane_raft_peer_server_request(
        runtime.handle(),
        &authority,
        &mut stream,
        &policy,
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .unwrap_err();
    let ControlPlaneRaftPeerServerWorkerError::PeerRpc(error) = error else {
        panic!("invalid nested snapshot returned checkpoint error")
    };
    assert_error(&error);
    assert_eq!(runtime.block_on(authority.status()).unwrap(), before);
    assert!(checkpoint.events.lock().unwrap().is_empty());
    assert!(stream.response.is_empty());
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_rejects_noncurrent_nested_snapshot_versions_before_publication() {
    let current = current_peer_snapshot_payload();
    for version in [0, 2] {
        let unsupported =
            crate::control_plane_command::reseal_control_plane_snapshot_version_for_test(
                &current, version,
            )
            .unwrap();
        assert_peer_server_rejects_snapshot_before_publication(
            &format!("nested-snapshot-version-{version}"),
            unsupported,
            |error| {
                assert!(matches!(
                    error,
                    ControlPlaneError::SnapshotDecode { message }
                        if message == &format!(
                            "unsupported control-plane snapshot version {version}"
                        )
                ));
            },
        );
    }
}

#[test]
fn control_plane_raft_peer_server_rejects_noncurrent_nested_state_versions_before_publication() {
    let current = current_peer_snapshot_payload();
    for version in [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41] {
        let unsupported =
            crate::control_plane_command::reseal_control_plane_snapshot_state_version_for_test(
                &current, version,
            )
            .unwrap();
        assert_peer_server_rejects_snapshot_before_publication(
            &format!("nested-state-version-{version}"),
            unsupported,
            |error| {
                assert!(matches!(
                    error,
                    ControlPlaneError::Parse { message, .. }
                        if message == &format!(
                            "unsupported control-plane state version {version}"
                        )
                ));
            },
        );
    }
}

#[test]
fn control_plane_raft_peer_server_rejects_invalid_nested_snapshot_state_before_publication() {
    let invalid = ClusterControlSnapshot::test_invalid_active_without_metadata_proof_epoch(
        PgId::new(31),
    );
    let payload = crate::control_plane_command::encode_control_plane_snapshot(&invalid).unwrap();
    assert_peer_server_rejects_snapshot_before_publication(
        "invalid-nested-snapshot-state",
        payload,
        |error| {
            assert!(matches!(
                error,
                ControlPlaneError::SnapshotInvariantViolation { context, message }
                    if *context == "attempted to install invalid replicated control-plane snapshot"
                        && message.contains("has no metadata proof epoch")
            ));
        },
    );
}

#[test]
fn control_plane_raft_peer_snapshot_frames_fail_closed_across_direction() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let snapshot = state_machine.build_snapshot().unwrap();

    let request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
        snapshot,
    };
    let encoded_request = request.encode_frame().unwrap();
    let err = ControlPlaneRaftPeerSnapshotResponse::decode_frame(&encoded_request).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("does not match expected kind")
    ));

    let response = ControlPlaneRaftPeerSnapshotResponse {
        response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new(3, 1)),
    };
    let encoded_response = response.encode_frame().unwrap();
    let err = ControlPlaneRaftPeerSnapshotRequest::decode_frame(
        &encoded_response,
        usize::MAX,
        usize::MAX,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("does not match expected kind")
    ));
}

#[test]
fn control_plane_raft_peer_snapshot_request_decode_enforces_size_limits() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let snapshot = state_machine.build_snapshot().unwrap();
    let payload_len = snapshot.snapshot.get_ref().len();
    assert!(payload_len > 0);

    let request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
        snapshot,
    };
    let encoded = request.encode_frame().unwrap();
    let err =
        ControlPlaneRaftPeerSnapshotRequest::decode_frame(&encoded, encoded.len() - 1, usize::MAX)
            .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("peer snapshot request frame size")
                && message.contains("exceeds limit")
    ));

    let err =
        ControlPlaneRaftPeerSnapshotRequest::decode_frame(&encoded, usize::MAX, payload_len - 1)
            .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("raft peer snapshot payload length")
                && message.contains("exceeds limit")
    ));
}

#[test]
fn control_plane_raft_peer_rpc_frames_fail_closed_across_direction() {
    let request = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
        last_log_id: None,
        leadership_transfer: false,
    };
    let encoded_request = ControlPlaneRaftPeerRpcRequest::Vote(request)
        .encode_frame()
        .unwrap();
    let err = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded_request).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("does not match expected kind")
    ));

    let response = VoteResponse {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
        vote_granted: false,
        last_log_id: None,
    };
    let encoded_response = ControlPlaneRaftPeerRpcResponse::Vote(response)
        .encode_frame()
        .unwrap();
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded_response).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("does not match expected kind")
    ));
}

#[test]
fn control_plane_raft_peer_rpc_frame_decode_fails_closed() {
    let request = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
        last_log_id: Some(raft_log_id(3, 7, 8)),
        leadership_transfer: false,
    };
    let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request)
        .encode_frame()
        .unwrap();

    assert!(matches!(
        raft_peer_rpc_frame_reader_classified(&encoded[..4]),
        Err(ControlPlaneRaftPeerRpcFrameDecodeError::Format(
            ControlPlaneRaftPeerRpcFrameFormatError::Truncated
        ))
    ));
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded[..4]).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("truncated control-plane OpenRaft peer RPC frame")
    ));

    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 1;
    refresh_raft_peer_frame_checksum(&mut bad_magic);
    assert!(matches!(
        raft_peer_rpc_frame_reader_classified(&bad_magic),
        Err(ControlPlaneRaftPeerRpcFrameDecodeError::Format(
            ControlPlaneRaftPeerRpcFrameFormatError::UnknownMagic
        ))
    ));
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&bad_magic).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("invalid control-plane OpenRaft peer RPC frame magic")
    ));

    for version in [1_u16, 2, 4] {
        let mut unsupported_version = encoded.clone();
        unsupported_version[CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len()
            ..CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_peer_frame_checksum(&mut unsupported_version);
        assert!(matches!(
            raft_peer_rpc_frame_reader_classified(&unsupported_version),
            Err(ControlPlaneRaftPeerRpcFrameDecodeError::Format(
                ControlPlaneRaftPeerRpcFrameFormatError::UnsupportedVersion(actual)
            )) if actual == version
        ));
        let err =
            ControlPlaneRaftPeerRpcRequest::decode_frame(&unsupported_version).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message == format!("unsupported control-plane OpenRaft peer RPC frame version {version}")
        ));
    }

    let mut bad_checksum = encoded.clone();
    bad_checksum[CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2] ^= 1;
    assert!(matches!(
        raft_peer_rpc_frame_reader_classified(&bad_checksum),
        Err(ControlPlaneRaftPeerRpcFrameDecodeError::Invalid(
            ControlPlaneError::CommandDecode { .. }
        ))
    ));
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&bad_checksum).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("control-plane OpenRaft peer RPC frame checksum mismatch")
    ));

    let mut trailing = encoded.clone();
    let checksum_start = trailing.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
    trailing.insert(checksum_start, 0);
    refresh_raft_peer_frame_checksum(&mut trailing);
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&trailing).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("control-plane OpenRaft peer RPC frame has 1 trailing bytes")
    ));

    let mut unknown_tag = Vec::new();
    unknown_tag.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
    write_raft_u16(&mut unknown_tag, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
    write_raft_u8(
        &mut unknown_tag,
        ControlPlaneRaftPeerRpcFrameTag::Request.as_u8(),
    );
    write_raft_u8(&mut unknown_tag, 0);
    write_raft_u8(&mut unknown_tag, 99);
    append_raft_artifact_checksum(&mut unknown_tag);
    let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&unknown_tag).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("unknown control-plane OpenRaft peer RPC request tag 99")
    ));
}

fn append_raft_peer_rpc_catalogue_frame(aggregate: &mut Vec<u8>, sample: u8, frame: &[u8]) {
    write_raft_u8(aggregate, sample);
    write_raft_u32(aggregate, u32::try_from(frame.len()).unwrap());
    aggregate.extend_from_slice(frame);
}

fn assert_raft_peer_rpc_optional_fields_are_complete(
    observations: Vec<(ControlPlaneRaftPeerRpcOptionalField, RaftWireOptionTag)>,
) {
    let mut coverage = BTreeMap::<
        ControlPlaneRaftPeerRpcOptionalField,
        BTreeSet<RaftWireOptionTag>,
    >::new();
    for (field, arm) in observations {
        coverage.entry(field).or_default().insert(arm);
    }
    assert_eq!(
        coverage.keys().copied().collect::<BTreeSet<_>>(),
        ControlPlaneRaftPeerRpcOptionalField::ALL
            .iter()
            .copied()
            .collect()
    );
    let expected = RaftWireOptionTag::ALL.into_iter().collect::<BTreeSet<_>>();
    for &field in ControlPlaneRaftPeerRpcOptionalField::ALL {
        assert_eq!(
            coverage.get(&field),
            Some(&expected),
            "peer RPC optional field {field:?} does not cover both arms"
        );
    }
}

fn assert_raft_peer_rpc_wire_registries_are_complete() {
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftPeerRpcFrameTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftPeerRpcFrameTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftPeerRpcRequestTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftPeerRpcRequestTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftPeerRpcResponseTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftPeerRpcResponseTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftAppendEntriesResponseTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftAppendEntriesResponseTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftTransferLeaderResponseTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftTransferLeaderResponseTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| RaftWireBoolean::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        RaftWireBoolean::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| RaftWireOptionTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        RaftWireOptionTag::ALL
    );
    assert_eq!(
        (0..=u8::MAX)
            .filter_map(|tag| ControlPlaneRaftEntryPayloadTag::from_u8(tag).ok())
            .collect::<Vec<_>>(),
        ControlPlaneRaftEntryPayloadTag::ALL
    );
}

#[test]
fn control_plane_raft_peer_rpc_v2_catalogues_remain_exact_and_rejected() {
    for (encoded, expected_len, expected_digest) in [
        (
            include_str!("peer_rpc_v2_state_v28_command_v15_aggregate.hex"),
            2_452,
            "621a9fb29dac5c5044ec398707c03f3578ec15fbb7e34d1e52fe2e0bd98dc304",
        ),
        (
            include_str!("peer_rpc_v2_state_v29_command_v16_aggregate.hex"),
            2_452,
            "08ce6de520700664880c7d26f21ed20f73f93be3bbf0114ff6852e173730311f",
        ),
    ] {
        assert_raft_peer_rpc_v2_catalogue_is_exact_and_rejected(
            encoded,
            expected_len,
            expected_digest,
        );
    }
}

fn assert_raft_peer_rpc_v2_catalogue_is_exact_and_rejected(
    encoded: &str,
    expected_len: usize,
    expected_digest: &str,
) {
    let aggregate = raft_test_decode_hex(encoded);
    assert_eq!(
        (
            aggregate.len(),
            raft_test_hex(&checksum::sha256::digest(&aggregate))
        ),
        (expected_len, expected_digest.to_owned())
    );

    let mut offset = 0;
    let mut expected_sample = 1_u8;
    while offset < aggregate.len() {
        assert_eq!(aggregate[offset], expected_sample);
        offset += 1;
        let frame_len = u32::from_be_bytes(
            aggregate[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let frame = &aggregate[offset..offset + frame_len];
        offset += frame_len;
        assert!(matches!(
            raft_peer_rpc_frame_reader_classified(frame),
            Err(ControlPlaneRaftPeerRpcFrameDecodeError::Format(
                ControlPlaneRaftPeerRpcFrameFormatError::UnsupportedVersion(2)
            ))
        ));
        expected_sample = expected_sample.checked_add(1).unwrap();
    }
    assert_eq!(expected_sample, 21);
}

#[test]
fn control_plane_raft_peer_rpc_v3_catalogue_is_exact() {
    assert_eq!(CONTROL_PLANE_RAFT_PEER_RPC_VERSION, 3);
    assert_raft_peer_rpc_wire_registries_are_complete();
    let optional_capture = ControlPlaneRaftPeerRpcOptionCapture::begin();
    let identity_without_topology =
        ControlPlaneRaftPeerFrameIdentity::new("peer-catalogue", 11, 12);
    let identity_with_topology = identity_without_topology
        .clone()
        .with_topology(13, "topology-digest");
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(17),
        membership: NodeMembershipState::Active,
    };
    let mut aggregate = Vec::new();
    let mut sample = 0_u8;
    let mut frame_tags = BTreeSet::new();
    let mut identity_branches = BTreeSet::new();
    let mut request_tags = BTreeSet::new();
    let mut response_tags = BTreeSet::new();
    let mut append_response_tags = BTreeSet::new();
    let mut transfer_response_tags = BTreeSet::new();
    let mut boolean_values = BTreeSet::new();
    let mut entry_payload_tags = BTreeSet::new();
    let mut append = |
        frame_tag: ControlPlaneRaftPeerRpcFrameTag,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
        frame: Vec<u8>,
    | {
        sample = sample.checked_add(1).unwrap();
        frame_tags.insert(frame_tag);
        identity_branches.insert(ControlPlaneRaftPeerFrameIdentityBranch::from_identity(
            identity,
        ));
        append_raft_peer_rpc_catalogue_frame(&mut aggregate, sample, &frame);
    };

    let append_request: AppendEntriesRequest<ControlPlaneRaftTypeConfig> = AppendEntriesRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(21, 11),
        prev_log_id: Some(raft_log_id(20, 11, 30)),
        entries: vec![
            blank_entry(21, 11, 31),
            membership_entry(21, 11, 32),
            normal_entry(21, 11, 33, command),
        ],
        leader_commit: None,
    };
    boolean_values.insert(RaftWireBoolean::from_bool(append_request.vote.committed));
    entry_payload_tags.extend(
        append_request
            .entries
            .iter()
            .map(control_plane_raft_entry_payload_tag),
    );
    let request = ControlPlaneRaftPeerRpcRequest::AppendEntries(append_request.clone());
    request_tags.insert(request.wire_tag());
    let frame = request
        .encode_frame_for_peer(&identity_with_topology)
        .unwrap();
    let ControlPlaneRaftPeerRpcRequest::AppendEntries(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&frame, &identity_with_topology)
            .unwrap()
    else {
        panic!("catalogue append request decoded to another request kind")
    };
    assert_eq!(decoded.vote, append_request.vote);
    assert_eq!(decoded.prev_log_id, append_request.prev_log_id);
    assert_eq!(decoded.entries, append_request.entries);
    assert_eq!(decoded.leader_commit, append_request.leader_commit);
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        Some(&identity_with_topology),
        frame,
    );

    let append_request_without_previous: AppendEntriesRequest<ControlPlaneRaftTypeConfig> =
        AppendEntriesRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(21, 11),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: Some(raft_log_id(21, 11, 33)),
        };
    boolean_values.insert(RaftWireBoolean::from_bool(
        append_request_without_previous.vote.committed,
    ));
    let request =
        ControlPlaneRaftPeerRpcRequest::AppendEntries(append_request_without_previous.clone());
    request_tags.insert(request.wire_tag());
    let frame = request.encode_frame().unwrap();
    let ControlPlaneRaftPeerRpcRequest::AppendEntries(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame(&frame).unwrap()
    else {
        panic!("catalogue append request decoded to another request kind")
    };
    assert_eq!(decoded.vote, append_request_without_previous.vote);
    assert_eq!(
        decoded.prev_log_id,
        append_request_without_previous.prev_log_id
    );
    assert_eq!(decoded.entries, append_request_without_previous.entries);
    assert_eq!(
        decoded.leader_commit,
        append_request_without_previous.leader_commit
    );
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        None,
        frame,
    );

    let vote_request: VoteRequest<ControlPlaneRaftTypeConfig> = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(22, 11),
        last_log_id: None,
        leadership_transfer: false,
    };
    boolean_values.insert(RaftWireBoolean::from_bool(vote_request.vote.committed));
    boolean_values.insert(RaftWireBoolean::from_bool(
        vote_request.leadership_transfer,
    ));
    let request = ControlPlaneRaftPeerRpcRequest::Vote(vote_request.clone());
    request_tags.insert(request.wire_tag());
    let frame = request
        .encode_frame_for_peer(&identity_without_topology)
        .unwrap();
    let ControlPlaneRaftPeerRpcRequest::Vote(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&frame, &identity_without_topology)
            .unwrap()
    else {
        panic!("catalogue vote request decoded to another request kind")
    };
    assert_eq!(decoded, vote_request);
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        Some(&identity_without_topology),
        frame,
    );

    let pre_vote_request: VoteRequest<ControlPlaneRaftTypeConfig> = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(23, 11),
        last_log_id: Some(raft_log_id(22, 11, 34)),
        leadership_transfer: true,
    };
    boolean_values.insert(RaftWireBoolean::from_bool(pre_vote_request.vote.committed));
    boolean_values.insert(RaftWireBoolean::from_bool(
        pre_vote_request.leadership_transfer,
    ));
    let request = ControlPlaneRaftPeerRpcRequest::PreVote(pre_vote_request.clone());
    request_tags.insert(request.wire_tag());
    let frame = request.encode_frame().unwrap();
    let ControlPlaneRaftPeerRpcRequest::PreVote(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame(&frame).unwrap()
    else {
        panic!("catalogue pre-vote request decoded to another request kind")
    };
    assert_eq!(decoded, pre_vote_request);
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        None,
        frame,
    );

    let transfer_request: TransferLeaderRequest<ControlPlaneRaftTypeConfig> =
        TransferLeaderRequest::new(
        Vote::<ControlPlaneRaftLeaderId>::new_committed(24, 11),
        12,
        None,
    );
    boolean_values.insert(RaftWireBoolean::from_bool(
        transfer_request.from_leader().committed,
    ));
    let request = ControlPlaneRaftPeerRpcRequest::TransferLeader(transfer_request.clone());
    request_tags.insert(request.wire_tag());
    let frame = request
        .encode_frame_for_peer(&identity_with_topology)
        .unwrap();
    let ControlPlaneRaftPeerRpcRequest::TransferLeader(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&frame, &identity_with_topology)
            .unwrap()
    else {
        panic!("catalogue transfer-leader request decoded to another request kind")
    };
    assert_eq!(decoded, transfer_request);
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        Some(&identity_with_topology),
        frame,
    );

    let transfer_request_with_log: TransferLeaderRequest<ControlPlaneRaftTypeConfig> =
        TransferLeaderRequest::new(
            Vote::<ControlPlaneRaftLeaderId>::new(24, 11),
            12,
            Some(raft_log_id(24, 11, 34)),
        );
    boolean_values.insert(RaftWireBoolean::from_bool(
        transfer_request_with_log.from_leader().committed,
    ));
    let request =
        ControlPlaneRaftPeerRpcRequest::TransferLeader(transfer_request_with_log.clone());
    request_tags.insert(request.wire_tag());
    let frame = request.encode_frame().unwrap();
    let ControlPlaneRaftPeerRpcRequest::TransferLeader(decoded) =
        ControlPlaneRaftPeerRpcRequest::decode_frame(&frame).unwrap()
    else {
        panic!("catalogue transfer-leader request decoded to another request kind")
    };
    assert_eq!(decoded, transfer_request_with_log);
    append(
        ControlPlaneRaftPeerRpcFrameTag::Request,
        None,
        frame,
    );

    let append_responses = [
        AppendEntriesResponse::Success,
        AppendEntriesResponse::PartialSuccess(None),
        AppendEntriesResponse::PartialSuccess(Some(raft_log_id(25, 12, 35))),
        AppendEntriesResponse::Conflict,
        AppendEntriesResponse::HigherVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(
            26, 12,
        )),
    ];
    for response in append_responses {
        append_response_tags.insert(control_plane_raft_append_entries_response_tag(&response));
        match &response {
            AppendEntriesResponse::PartialSuccess(_) => {}
            AppendEntriesResponse::HigherVote(vote) => {
                boolean_values.insert(RaftWireBoolean::from_bool(vote.committed));
            }
            AppendEntriesResponse::Success | AppendEntriesResponse::Conflict => {}
        }
        let expected = ControlPlaneRaftPeerRpcResponse::AppendEntries(response);
        response_tags.insert(expected.wire_tag());
        let frame = expected
            .encode_frame_for_peer(&identity_without_topology)
            .unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                &frame,
                &identity_without_topology
            )
            .unwrap(),
            expected
        );
        append(
            ControlPlaneRaftPeerRpcFrameTag::Response,
            Some(&identity_without_topology),
            frame,
        );
    }

    let vote_responses: [VoteResponse<ControlPlaneRaftTypeConfig>; 2] = [
        VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(27, 12),
            vote_granted: false,
            last_log_id: None,
        },
        VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(28, 12),
            vote_granted: true,
            last_log_id: Some(raft_log_id(28, 12, 36)),
        },
    ];
    for response in vote_responses {
        boolean_values.insert(RaftWireBoolean::from_bool(response.vote.committed));
        boolean_values.insert(RaftWireBoolean::from_bool(response.vote_granted));
        let expected = ControlPlaneRaftPeerRpcResponse::Vote(response);
        response_tags.insert(expected.wire_tag());
        let frame = expected.encode_frame().unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame(&frame).unwrap(),
            expected
        );
        append(
            ControlPlaneRaftPeerRpcFrameTag::Response,
            None,
            frame,
        );
    }

    let transfer_responses = [
        Ok(()),
        Err(TransferLeaderError::VoteChanged {
            expected: Vote::<ControlPlaneRaftLeaderId>::new_committed(29, 11),
            actual: Vote::<ControlPlaneRaftLeaderId>::new(30, 12),
        }),
        Err(TransferLeaderError::LogNotFlushed {
            expected: None,
            actual: Some(raft_log_id(30, 12, 37)),
        }),
        Err(TransferLeaderError::LogNotFlushed {
            expected: Some(raft_log_id(30, 12, 37)),
            actual: None,
        }),
    ];
    for response in transfer_responses {
        transfer_response_tags.insert(control_plane_raft_transfer_leader_response_tag(&response));
        match &response {
            Ok(()) => {}
            Err(TransferLeaderError::VoteChanged { expected, actual }) => {
                boolean_values.insert(RaftWireBoolean::from_bool(expected.committed));
                boolean_values.insert(RaftWireBoolean::from_bool(actual.committed));
            }
            Err(TransferLeaderError::LogNotFlushed { .. }) => {}
        }
        let expected = ControlPlaneRaftPeerRpcResponse::TransferLeader(response);
        response_tags.insert(expected.wire_tag());
        let frame = expected
            .encode_frame_for_peer(&identity_with_topology)
            .unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                &frame,
                &identity_with_topology
            )
            .unwrap(),
            expected
        );
        append(
            ControlPlaneRaftPeerRpcFrameTag::Response,
            Some(&identity_with_topology),
            frame,
        );
    }

    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(11))
        .unwrap();
    let snapshot_request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(31, 11),
        snapshot: state_machine.build_snapshot().unwrap(),
    };
    boolean_values.insert(RaftWireBoolean::from_bool(
        snapshot_request.vote.committed,
    ));
    let frame = snapshot_request
        .encode_frame_for_peer(&identity_with_topology)
        .unwrap();
    let decoded = ControlPlaneRaftPeerSnapshotRequest::decode_frame_with_identity(
        &frame,
        usize::MAX,
        usize::MAX,
        Some(&identity_with_topology),
    )
    .unwrap();
    assert_eq!(decoded.vote, snapshot_request.vote);
    assert_eq!(decoded.snapshot.meta, snapshot_request.snapshot.meta);
    assert_eq!(
        decoded.snapshot.snapshot.get_ref(),
        snapshot_request.snapshot.snapshot.get_ref()
    );
    append(
        ControlPlaneRaftPeerRpcFrameTag::SnapshotRequest,
        Some(&identity_with_topology),
        frame,
    );

    let empty_snapshot_request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(31, 11),
        snapshot: ControlPlaneRaftStateMachine::empty()
            .build_snapshot()
            .unwrap(),
    };
    boolean_values.insert(RaftWireBoolean::from_bool(
        empty_snapshot_request.vote.committed,
    ));
    let frame = empty_snapshot_request.encode_frame().unwrap();
    let decoded = ControlPlaneRaftPeerSnapshotRequest::decode_frame(
        &frame,
        usize::MAX,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(decoded.vote, empty_snapshot_request.vote);
    assert_eq!(decoded.snapshot.meta, empty_snapshot_request.snapshot.meta);
    assert_eq!(
        decoded.snapshot.snapshot.get_ref(),
        empty_snapshot_request.snapshot.snapshot.get_ref()
    );
    append(
        ControlPlaneRaftPeerRpcFrameTag::SnapshotRequest,
        None,
        frame,
    );

    let snapshot_response = ControlPlaneRaftPeerSnapshotResponse {
        response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new(32, 12)),
    };
    boolean_values.insert(RaftWireBoolean::from_bool(
        snapshot_response.response.vote.committed,
    ));
    let frame = snapshot_response.encode_frame().unwrap();
    assert_eq!(
        ControlPlaneRaftPeerSnapshotResponse::decode_frame(&frame).unwrap(),
        snapshot_response
    );
    append(
        ControlPlaneRaftPeerRpcFrameTag::SnapshotResponse,
        None,
        frame,
    );

    assert_eq!(
        frame_tags,
        ControlPlaneRaftPeerRpcFrameTag::ALL.into_iter().collect()
    );
    assert_eq!(
        identity_branches,
        ControlPlaneRaftPeerFrameIdentityBranch::ALL
            .into_iter()
            .collect()
    );
    assert_eq!(
        request_tags,
        ControlPlaneRaftPeerRpcRequestTag::ALL.into_iter().collect()
    );
    assert_eq!(
        response_tags,
        ControlPlaneRaftPeerRpcResponseTag::ALL
            .into_iter()
            .collect()
    );
    assert_eq!(
        append_response_tags,
        ControlPlaneRaftAppendEntriesResponseTag::ALL
            .into_iter()
            .collect()
    );
    assert_eq!(
        transfer_response_tags,
        ControlPlaneRaftTransferLeaderResponseTag::ALL
            .into_iter()
            .collect()
    );
    assert_eq!(
        boolean_values,
        RaftWireBoolean::ALL.into_iter().collect()
    );
    assert_raft_peer_rpc_optional_fields_are_complete(optional_capture.finish());
    assert_eq!(
        entry_payload_tags,
        ControlPlaneRaftEntryPayloadTag::ALL.into_iter().collect()
    );

    assert_eq!(sample, 20);
    assert_eq!(
        (
            aggregate.len(),
            raft_test_hex(&checksum::sha256::digest(&aggregate))
        ),
        (
            2_402,
            "44e4684b26b0164e7a3b351b0f24a0699f98785289f6bab8a0e86c25745eabc4".to_owned()
        )
    );
}

#[test]
fn historical_state_v39_command_v27_peer_rpc_v3_catalogue_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_peer_v3_state_v39_command_v27.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_402,
            "53044898a6c6e50bd3a4bd8608461878e2b5e96dabb3e4c3ec0bbb339cc5be89".to_owned()
        )
    );

    let mut offset = 0;
    let mut expected_sample = 1_u8;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        assert_eq!(AGGREGATE[offset], expected_sample);
        offset += 1;
        let frame_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let frame = &AGGREGATE[offset..offset + frame_len];
        offset += frame_len;
        let tag_offset = CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + std::mem::size_of::<u16>();
        let result = match frame[tag_offset] {
            1 => ControlPlaneRaftPeerRpcRequest::decode_frame(frame).map(|_| ()),
            2 => ControlPlaneRaftPeerRpcResponse::decode_frame(frame).map(|_| ()),
            3 => ControlPlaneRaftPeerSnapshotRequest::decode_frame(frame, usize::MAX, usize::MAX)
                .map(|_| ()),
            4 => ControlPlaneRaftPeerSnapshotResponse::decode_frame(frame).map(|_| ()),
            tag => panic!("unexpected historical peer frame tag {tag}"),
        };
        match result {
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 27" =>
            {
                command_rejections += 1;
            }
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 39" =>
            {
                state_rejections += 1;
            }
            Ok(()) => {}
            Err(error) => panic!("unexpected historical peer frame rejection: {error:?}"),
        }
        expected_sample = expected_sample.checked_add(1).unwrap();
    }
    assert_eq!(expected_sample, 21);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v38_command_v26_peer_rpc_v3_catalogue_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_peer_v3_state_v38_command_v26.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_402,
            "31e136e10fb30bcac3eb8ded11357a12fcf57a8b88ed1ce1d799c0647eba8674".to_owned()
        )
    );

    let mut offset = 0;
    let mut expected_sample = 1_u8;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        assert_eq!(AGGREGATE[offset], expected_sample);
        offset += 1;
        let frame_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let frame = &AGGREGATE[offset..offset + frame_len];
        offset += frame_len;
        let tag_offset = CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + std::mem::size_of::<u16>();
        let result = match frame[tag_offset] {
            1 => ControlPlaneRaftPeerRpcRequest::decode_frame(frame).map(|_| ()),
            2 => ControlPlaneRaftPeerRpcResponse::decode_frame(frame).map(|_| ()),
            3 => ControlPlaneRaftPeerSnapshotRequest::decode_frame(
                frame,
                usize::MAX,
                usize::MAX,
            )
            .map(|_| ()),
            4 => ControlPlaneRaftPeerSnapshotResponse::decode_frame(frame).map(|_| ()),
            tag => panic!("unexpected historical peer frame tag {tag}"),
        };
        match result {
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 26" =>
            {
                command_rejections += 1;
            }
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 38" =>
            {
                state_rejections += 1;
            }
            Ok(()) => {}
            Err(error) => panic!("unexpected historical peer frame rejection: {error:?}"),
        }
        expected_sample = expected_sample.checked_add(1).unwrap();
    }
    assert_eq!(expected_sample, 21);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v37_command_v25_peer_rpc_v3_catalogue_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_peer_v3_state_v37_command_v25.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_402,
            "d69f2444f84b0b483f478268d38a7fbc14d480f746ae308dfb33a891094aa7c4".to_owned()
        )
    );

    let mut offset = 0;
    let mut expected_sample = 1_u8;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        assert_eq!(AGGREGATE[offset], expected_sample);
        offset += 1;
        let frame_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let frame = &AGGREGATE[offset..offset + frame_len];
        offset += frame_len;
        let tag_offset = CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + std::mem::size_of::<u16>();
        let result = match frame[tag_offset] {
            1 => ControlPlaneRaftPeerRpcRequest::decode_frame(frame).map(|_| ()),
            2 => ControlPlaneRaftPeerRpcResponse::decode_frame(frame).map(|_| ()),
            3 => ControlPlaneRaftPeerSnapshotRequest::decode_frame(
                frame,
                usize::MAX,
                usize::MAX,
            )
            .map(|_| ()),
            4 => ControlPlaneRaftPeerSnapshotResponse::decode_frame(frame).map(|_| ()),
            tag => panic!("unexpected historical peer frame tag {tag}"),
        };
        match result {
            Ok(()) => {}
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 25" =>
            {
                command_rejections += 1;
            }
            Err(ControlPlaneError::Parse { message, .. })
                if message == "unsupported control-plane state version 37" =>
            {
                state_rejections += 1;
            }
            Err(error) => panic!("unexpected historical peer frame rejection: {error:?}"),
        }
        expected_sample = expected_sample.checked_add(1).unwrap();
    }
    assert_eq!(expected_sample, 21);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn control_plane_raft_peer_rpc_frame_identity_fails_closed() {
    let request = VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: None,
        leadership_transfer: false,
    };
    let expected = ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 2)
        .with_topology(7, "topology-a");
    let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request)
        .encode_frame_for_peer(&expected)
        .unwrap();

    let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
        &encoded,
        &ControlPlaneRaftPeerFrameIdentity::new("wrong-cluster", 1, 2),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("cluster identity mismatch")
    ));

    let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
        &encoded,
        &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 2)
            .with_topology(8, "topology-a"),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("topology identity mismatch")
    ));

    let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
        &encoded,
        &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 2)
            .with_topology(7, "topology-b"),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("topology identity mismatch")
    ));

    let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
        &encoded,
        &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 9, 2)
            .with_topology(7, "topology-a"),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("source identity mismatch")
    ));

    let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
        &encoded,
        &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 9)
            .with_topology(7, "topology-a"),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("target identity mismatch")
    ));

    let missing_identity = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        vote_granted: false,
        last_log_id: None,
    })
    .encode_frame()
    .unwrap();
    let err = ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
        &missing_identity,
        &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 2, 1),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("missing peer identity")
    ));
}

#[test]
fn control_plane_raft_peer_transport_frame_round_trips() {
    let identity =
        ControlPlaneRaftPeerFrameIdentity::new("control-plane-peer-transport-frame", 1, 2);
    let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: Some(raft_log_id(3, 1, 7)),
        leadership_transfer: false,
    });
    let frame = request.encode_frame_for_peer(&identity).unwrap();
    let mut transport = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport, &frame).unwrap();
    assert_eq!(
        transport.get(..4),
        Some(u32::try_from(frame.len()).unwrap().to_be_bytes().as_slice())
    );
    assert_eq!(&transport[4..], frame);

    let mut cursor = Cursor::new(transport);
    let decoded_frame =
        read_control_plane_raft_peer_transport_frame(&mut cursor, frame.len()).unwrap();
    assert_eq!(decoded_frame, frame);
    let decoded =
        ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&decoded_frame, &identity).unwrap();
    assert!(matches!(decoded, ControlPlaneRaftPeerRpcRequest::Vote(_)));
}

#[test]
fn control_plane_raft_peer_transport_record_layout_and_format_failures_are_exact() {
    let mut encoded = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut encoded, b"peer").unwrap();
    assert_eq!(encoded, b"\0\0\0\x04peer");
    assert!(matches!(
        write_control_plane_raft_peer_transport_frame(&mut Vec::new(), b""),
        Err(ControlPlaneError::RpcProtocol { diagnostic })
            if diagnostic.as_str() == "control-plane OpenRaft peer transport frame is empty"
    ));

    let cases = [
        (
            Vec::new(),
            4,
            ControlPlaneRaftPeerTransportFrameFormatError::TruncatedHeader,
            0,
        ),
        (
            vec![0, 0, 0],
            4,
            ControlPlaneRaftPeerTransportFrameFormatError::TruncatedHeader,
            0,
        ),
        (
            vec![0, 0, 0, 0],
            4,
            ControlPlaneRaftPeerTransportFrameFormatError::EmptyFrame,
            0,
        ),
        (
            vec![0, 0, 0, 5],
            4,
            ControlPlaneRaftPeerTransportFrameFormatError::FrameTooLarge {
                frame_len: 5,
                max_frame_bytes: 4,
            },
            0,
        ),
        (
            vec![0, 0, 0, 4, b'p', b'e', b'e'],
            4,
            ControlPlaneRaftPeerTransportFrameFormatError::TruncatedPayload { frame_len: 4 },
            1,
        ),
    ];

    for (bytes, max_frame_bytes, expected, expected_reservations) in cases {
        let reservations = AtomicUsize::new(0);
        let error = read_control_plane_raft_peer_transport_frame_classified(
            &mut Cursor::new(bytes),
            max_frame_bytes,
            |_| {
                reservations.fetch_add(1, Ordering::AcqRel);
                Ok(())
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ControlPlaneRaftPeerTransportFrameReadError::Format(actual)
                if actual == expected
        ));
        assert_eq!(
            reservations.load(Ordering::Acquire),
            expected_reservations
        );
    }
}

#[test]
fn control_plane_raft_peer_frame_exchange_debug_redacts_frame_bytes() {
    let secret = b"signed-command-and-authenticator";
    let exchange = ControlPlaneRaftPeerFrameExchange {
        target: 2,
        endpoint: "tcp://raft-2.example:7401".to_string(),
        request_frame: secret.to_vec(),
        max_frame_bytes: 1024,
        connect_timeout: Duration::from_secs(1),
        deadline: Instant::now() + Duration::from_secs(1),
        context_prefix: "",
    };

    let debug = format!("{exchange:?}");
    assert!(debug.contains(&format!("request_frame_len: {}", secret.len())));
    assert!(!debug.contains("signed-command"));
    assert!(!debug.contains("authenticator"));
}

#[test]
fn control_plane_raft_tls_peer_transport_exchanges_complete_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let connection =
            rustls::ServerConnection::new(raft_peer_test_tls_server_config(true)).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let request = read_control_plane_raft_peer_transport_frame(&mut stream, 1024).unwrap();
        assert_eq!(request, b"request-frame");
        write_control_plane_raft_peer_transport_frame(&mut stream, b"response-frame").unwrap();
    });
    let endpoint = raft_peer_test_tls_endpoint(port);
    let transport = raft_peer_test_configured_transport(2, endpoint.clone());

    let response =
        ControlPlaneRaftTypeConfig::run(transport.exchange(ControlPlaneRaftPeerFrameExchange {
            target: 2,
            endpoint: endpoint.advertised_endpoint().to_owned(),
            request_frame: b"request-frame".to_vec(),
            max_frame_bytes: 1024,
            connect_timeout: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_secs(1),
            context_prefix: "",
        }))
        .unwrap();

    assert_eq!(response, b"response-frame");
    server.join().unwrap();
}

#[test]
fn control_plane_raft_tls_peer_transport_requires_alpn() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut connection =
            rustls::ServerConnection::new(raft_peer_test_tls_server_config(false)).unwrap();
        while connection.is_handshaking() {
            connection.complete_io(&mut stream).unwrap();
        }
        assert_eq!(connection.alpn_protocol(), None);
    });
    let endpoint = raft_peer_test_tls_endpoint(port);
    let transport = raft_peer_test_configured_transport(2, endpoint.clone());

    let error =
        ControlPlaneRaftTypeConfig::run(transport.exchange(ControlPlaneRaftPeerFrameExchange {
            target: 2,
            endpoint: endpoint.advertised_endpoint().to_owned(),
            request_frame: b"request-frame".to_vec(),
            max_frame_bytes: 1024,
            connect_timeout: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_secs(1),
            context_prefix: "",
        }))
        .unwrap_err();

    assert!(format!("{error:?}").contains("required protocol profile"));
    assert!(matches!(
        raft_peer_transport_rpc_error("TLS/TCP", 2, error),
        RPCError::Network(_)
    ));
    server.join().unwrap();
}

#[test]
fn control_plane_raft_tls_peer_transport_bounds_handshake_absolutely() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_millis(100));
    });
    let endpoint = raft_peer_test_tls_endpoint(port);
    let transport = raft_peer_test_configured_transport(2, endpoint.clone());

    let error =
        ControlPlaneRaftTypeConfig::run(transport.exchange(ControlPlaneRaftPeerFrameExchange {
            target: 2,
            endpoint: endpoint.advertised_endpoint().to_owned(),
            request_frame: b"request-frame".to_vec(),
            max_frame_bytes: 1024,
            connect_timeout: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_millis(20),
            context_prefix: "",
        }))
        .unwrap_err();

    assert!(matches!(
        raft_peer_transport_rpc_error("TLS/TCP", 2, error),
        RPCError::Unreachable(_)
    ));
    server.join().unwrap();
}

#[test]
fn control_plane_raft_tls_peer_transport_bounds_trickled_response_absolutely() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let connection =
            rustls::ServerConnection::new(raft_peer_test_tls_server_config(true)).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        read_control_plane_raft_peer_transport_frame(&mut stream, 1024).unwrap();
        let mut response = Vec::new();
        write_raft_u32(&mut response, 8);
        response.extend_from_slice(b"response");
        for byte in response {
            if stream
                .write_all(std::slice::from_ref(&byte))
                .and_then(|()| stream.flush())
                .is_err()
            {
                break;
            }
            thread::sleep(Duration::from_millis(30));
        }
    });
    let endpoint = raft_peer_test_tls_endpoint(port);
    let transport = raft_peer_test_configured_transport(2, endpoint.clone());

    let error =
        ControlPlaneRaftTypeConfig::run(transport.exchange(ControlPlaneRaftPeerFrameExchange {
            target: 2,
            endpoint: endpoint.advertised_endpoint().to_owned(),
            request_frame: b"request-frame".to_vec(),
            max_frame_bytes: 1024,
            connect_timeout: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_millis(80),
            context_prefix: "",
        }))
        .unwrap_err();

    assert!(matches!(
        raft_peer_transport_rpc_error("TLS/TCP", 2, error),
        RPCError::Unreachable(_)
    ));
    server.join().unwrap();
}

#[test]
fn control_plane_raft_tls_peer_connect_failure_is_unreachable() {
    // TCP port zero is reserved and is never assigned to a listening socket.
    // Releasing an ephemeral listener here would race every other listener in
    // this test binary and could connect to an unrelated TLS peer instead.
    let endpoint = raft_peer_test_tls_endpoint(0);
    let transport = raft_peer_test_configured_transport(2, endpoint.clone());

    let error =
        ControlPlaneRaftTypeConfig::run(transport.exchange(ControlPlaneRaftPeerFrameExchange {
            target: 2,
            endpoint: endpoint.advertised_endpoint().to_owned(),
            request_frame: b"request-frame".to_vec(),
            max_frame_bytes: 1024,
            connect_timeout: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_secs(1),
            context_prefix: "",
        }))
        .unwrap_err();

    assert!(matches!(
        raft_peer_transport_rpc_error("TLS/TCP", 2, error),
        RPCError::Unreachable(_)
    ));
}

#[test]
fn control_plane_raft_peer_network_config_requires_exact_endpoint_binding() {
    let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        "endpoint-binding",
        [(1, "node-1".to_owned()), (2, "node-2".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let missing = ControlPlaneRaftPeerNetworkConfig::with_peer_endpoints(
        Duration::from_secs(1),
        [(1, ControlPlaneRaftPeerClientEndpoint::unix("node-1"))],
    )
    .unwrap();
    assert!(missing.validate_policy(&policy).is_err());

    let mismatch = ControlPlaneRaftPeerNetworkConfig::with_peer_endpoints(
        Duration::from_secs(1),
        [
            (1, ControlPlaneRaftPeerClientEndpoint::unix("node-1")),
            (2, ControlPlaneRaftPeerClientEndpoint::unix("wrong-node-2")),
        ],
    )
    .unwrap();
    assert!(mismatch.validate_policy(&policy).is_err());
}

#[derive(Default)]
struct RecordingPeerServerCheckpoint {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ControlPlaneRaftPeerServerCheckpoint for RecordingPeerServerCheckpoint {
    fn checkpoint_before_snapshot_response(
        &self,
        _authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        self.events.lock().unwrap().push("checkpoint");
        Ok(())
    }
}

struct NoopPeerServerCheckpoint;

impl ControlPlaneRaftPeerServerCheckpoint for NoopPeerServerCheckpoint {
    fn checkpoint_before_snapshot_response(
        &self,
        _authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        Ok(())
    }
}

struct FailingPeerServerCheckpoint;

impl ControlPlaneRaftPeerServerCheckpoint for FailingPeerServerCheckpoint {
    fn checkpoint_before_snapshot_response(
        &self,
        _authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        Err(ControlPlaneError::durability_failure(
            "injected peer-server checkpoint failure",
        ))
    }
}

struct RecordingPeerServerStream {
    request: Cursor<Vec<u8>>,
    response: Vec<u8>,
    events: Arc<Mutex<Vec<&'static str>>>,
    write_started: bool,
    fail_response_write: bool,
    response_deadline: Option<Instant>,
    poison_after_request_read: Option<ControlPlaneRaftDurabilityPublication>,
}

impl RecordingPeerServerStream {
    fn new(request: Vec<u8>, events: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            request: Cursor::new(request),
            response: Vec::new(),
            events,
            write_started: false,
            fail_response_write: false,
            response_deadline: None,
            poison_after_request_read: None,
        }
    }

    fn with_failed_response_write(mut self) -> Self {
        self.fail_response_write = true;
        self
    }

    fn with_ingress_deadline(mut self, deadline: Instant) -> Self {
        self.response_deadline = Some(deadline);
        self
    }

    fn with_poison_after_request_read(
        mut self,
        publication: ControlPlaneRaftDurabilityPublication,
    ) -> Self {
        self.poison_after_request_read = Some(publication);
        self
    }
}

impl Read for RecordingPeerServerStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.request.read(buffer)?;
        if self.request.position() == self.request.get_ref().len() as u64 {
            if let Some(publication) = self.poison_after_request_read.take() {
                publication.poison("injected poison after request admission");
            }
        }
        Ok(read)
    }
}

impl Write for RecordingPeerServerStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.fail_response_write {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected peer response write failure",
            ));
        }
        if self
            .response_deadline
            .is_none_or(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "recording peer response deadline expired",
            ));
        }
        if !self.write_started {
            self.write_started = true;
            self.events.lock().unwrap().push("write");
        }
        self.response.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ControlPlaneRaftPeerServerStream for RecordingPeerServerStream {
    fn begin_response(&mut self, timeout: Duration) {
        self.events.lock().unwrap().push("begin_response");
        self.response_deadline = Some(Instant::now() + timeout);
    }

    fn finish_response(&mut self) -> io::Result<()> {
        self.events.lock().unwrap().push("finish");
        Ok(())
    }
}

#[test]
fn control_plane_raft_peer_server_snapshots_checkpoint_before_acknowledgement() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                    "peer-server-checkpoint-order",
                    1,
                ),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        "peer-server-checkpoint-order",
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let checkpoint = Arc::new(RecordingPeerServerCheckpoint::default());
    let durability = authority
        .bind_peer_server_durability(checkpoint.clone())
        .unwrap();
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096)
        .unwrap()
        .with_durability(durability);
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let request = ControlPlaneRaftPeerSnapshotRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
        snapshot: state_machine.build_snapshot().unwrap(),
    }
    .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(
        "peer-server-checkpoint-order",
        1,
        1,
    ))
    .unwrap();
    let mut transport_request = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport_request, &request).unwrap();
    let mut stream =
        RecordingPeerServerStream::new(transport_request, Arc::clone(&checkpoint.events));

    handle_control_plane_raft_peer_server_request(
        runtime.handle(),
        &authority,
        &mut stream,
        &policy,
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .unwrap();

    assert_eq!(
        *checkpoint.events.lock().unwrap(),
        ["checkpoint", "begin_response", "write", "finish"]
    );
    assert!(!stream.response.is_empty());
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_poison_after_validation_prevents_dispatch() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster_name = "peer-server-poison-after-validation";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let before = runtime.block_on(authority.status()).unwrap();
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name,
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096)
        .unwrap()
        .with_durability(
            authority
                .bind_peer_server_durability(Arc::new(NoopPeerServerCheckpoint))
                .unwrap(),
        );
    let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1))
    .unwrap();
    let mut transport_request = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport_request, &request).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut stream = RecordingPeerServerStream::new(transport_request, events)
        .with_poison_after_request_read(authority.durability_publication().unwrap());

    let result = handle_control_plane_raft_peer_server_request(
        runtime.handle(),
        &authority,
        &mut stream,
        &policy,
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );

    assert!(matches!(
        result,
        Err(ControlPlaneRaftPeerServerWorkerError::PeerRpc(_))
    ));
    let after = runtime.block_on(authority.status()).unwrap();
    assert_eq!(after.persisted_vote(), before.persisted_vote());
    assert_eq!(after.current_term(), before.current_term());
    assert!(stream.response.is_empty());
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_rejects_resigned_auth_versions_before_dispatch() {
    for auth_version in [0_u16, 1, 3] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let cluster_name = "control-plane-raft-peer-transport-test";
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                        cluster_name,
                        1,
                    ),
                )
                .unwrap(),
        );
        let before = runtime.block_on(authority.status()).unwrap();
        let server_auth = test_peer_auth_policy(1);
        let policy = ControlPlaneRaftPeerServerPolicy::new(
            1,
            test_peer_transport_policy().with_auth_policy(server_auth.clone()),
            4096,
        )
        .unwrap()
        .with_durability(
            authority
                .bind_peer_server_durability(Arc::new(NoopPeerServerCheckpoint))
                .unwrap(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 2, 1);
        let raw_request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 2),
            last_log_id: None,
            leadership_transfer: false,
        })
        .encode_frame_for_peer(&identity)
        .unwrap();
        let signed = test_peer_auth_policy(2)
            .sign_peer_frame_with_auth_version_for_test(
                &identity,
                ControlPlaneAuthOperation::RaftVote,
                raw_request,
                auth_version,
            )
            .unwrap();
        let mut transport_request = Vec::new();
        write_control_plane_raft_peer_transport_frame(&mut transport_request, &signed).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut stream = RecordingPeerServerStream::new(transport_request, Arc::clone(&events));

        let error = handle_control_plane_raft_peer_server_request(
            runtime.handle(),
            &authority,
            &mut stream,
            &policy,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap_err();
        let ControlPlaneRaftPeerServerWorkerError::PeerRpc(error) = error else {
            panic!("auth-envelope version rejection returned checkpoint error")
        };

        assert!(
            matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
                if message.as_str() == format!("control-plane auth envelope: unsupported control-plane auth version {auth_version}")),
            "unexpected auth v{auth_version} error: {error}"
        );
        assert_eq!(runtime.block_on(authority.status()).unwrap(), before);
        assert!(events.lock().unwrap().is_empty());
        assert!(stream.response.is_empty());
        let metrics = server_auth.metrics_snapshot();
        assert_eq!(metrics.accepted_total(), 0);
        assert_eq!(metrics.rejected_total(), 1);
        assert_eq!(metrics.rejected_without_operation_total(), 1);
        assert_eq!(
            metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::UnsupportedVersion),
            1
        );
        runtime.block_on(authority.shutdown()).unwrap();
    }
}

#[test]
fn control_plane_raft_peer_server_rejects_authenticated_peer_rpc_versions_before_dispatch() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster_name = "control-plane-raft-peer-transport-test";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let before = runtime.block_on(authority.status()).unwrap();
    let server_auth = test_peer_auth_policy(1);
    let checkpoint = Arc::new(RecordingPeerServerCheckpoint::default());
    let policy = ControlPlaneRaftPeerServerPolicy::new(
        1,
        test_peer_transport_policy().with_auth_policy(server_auth.clone()),
        4096,
    )
    .unwrap()
    .with_durability(
        authority
            .bind_peer_server_durability(checkpoint.clone())
            .unwrap(),
    );
    let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 2, 1);
    let current = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(41, 2),
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();

    for (index, version) in [1_u16, 2, 4].into_iter().enumerate() {
        let mut unsupported = current.clone();
        unsupported[CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len()
            ..CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_peer_frame_checksum(&mut unsupported);
        let signed = test_peer_scoped_credential(2)
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: 1,
                }),
                operation: ControlPlaneAuthOperation::RaftVote,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
                payload: unsupported,
            })
            .unwrap()
            .encode_frame()
            .unwrap();
        let mut transport_request = Vec::new();
        write_control_plane_raft_peer_transport_frame(&mut transport_request, &signed).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut stream = RecordingPeerServerStream::new(transport_request, Arc::clone(&events));

        let error = handle_control_plane_raft_peer_server_request(
            runtime.handle(),
            &authority,
            &mut stream,
            &policy,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap_err();
        let ControlPlaneRaftPeerServerWorkerError::PeerRpc(error) = error else {
            panic!("peer RPC v{version}: version rejection returned checkpoint error")
        };
        assert!(
            matches!(error, ControlPlaneError::CommandDecode { ref message }
                if message == &format!("unsupported control-plane OpenRaft peer RPC frame version {version}")),
            "peer RPC v{version}: unexpected error: {error}"
        );
        assert_eq!(
            runtime.block_on(authority.status()).unwrap(),
            before,
            "peer RPC v{version}: unsupported frame reached OpenRaft dispatch"
        );
        assert!(events.lock().unwrap().is_empty());
        assert!(
            checkpoint.events.lock().unwrap().is_empty(),
            "peer RPC v{version}: unsupported frame reached checkpoint publication"
        );
        assert!(stream.response.is_empty());
        let metrics = server_auth.metrics_snapshot();
        assert_eq!(metrics.accepted_total(), 0);
        assert_eq!(metrics.rejected_total(), u64::try_from(index + 1).unwrap());
        assert_eq!(
            metrics.rejected_for_operation(ControlPlaneAuthOperation::RaftVote),
            u64::try_from(index + 1).unwrap()
        );
        assert_eq!(
            metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Malformed),
            u64::try_from(index + 1).unwrap()
        );
    }
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_rejects_malformed_transport_before_auth_or_dispatch() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster_name = "control-plane-raft-peer-transport-test";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let before = runtime.block_on(authority.status()).unwrap();
    let server_auth = test_peer_auth_policy(1);
    let max_frame_bytes = 4096usize;
    let checkpoint = Arc::new(RecordingPeerServerCheckpoint::default());
    let policy = ControlPlaneRaftPeerServerPolicy::new(
        1,
        test_peer_transport_policy().with_auth_policy(server_auth.clone()),
        max_frame_bytes,
    )
    .unwrap()
    .with_durability(
        authority
            .bind_peer_server_durability(checkpoint.clone())
            .unwrap(),
    );
    let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 2, 1);
    let raw_request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 2),
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();
    let signed = test_peer_auth_policy(2)
        .sign_peer_frame(
            &identity,
            ControlPlaneAuthOperation::RaftVote,
            raw_request,
        )
        .unwrap();
    let mut truncated_payload = u32::try_from(signed.len()).unwrap().to_be_bytes().to_vec();
    truncated_payload.extend_from_slice(&signed[..signed.len() - 1]);

    let malformed = [
        ("missing header", Vec::new()),
        ("truncated header", vec![0, 0, 0]),
        ("empty frame", vec![0, 0, 0, 0]),
        (
            "oversized frame",
            u32::try_from(max_frame_bytes + 1)
                .unwrap()
                .to_be_bytes()
                .to_vec(),
        ),
        ("truncated payload", truncated_payload),
    ];

    for (case, transport_request) in malformed {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut stream = RecordingPeerServerStream::new(transport_request, Arc::clone(&events));
        let error = handle_control_plane_raft_peer_server_request(
            runtime.handle(),
            &authority,
            &mut stream,
            &policy,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap_err();
        let ControlPlaneRaftPeerServerWorkerError::PeerRpc(error) = error else {
            panic!("{case}: malformed transport returned checkpoint error")
        };
        match case {
            "missing header" | "truncated header" | "truncated payload" => assert!(
                matches!(error, ControlPlaneError::Io { diagnostic: ref source }
                    if source.kind() == io::ErrorKind::UnexpectedEof),
                "{case}: unexpected error: {error}"
            ),
            "empty frame" => assert!(
                matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
                    if message.as_str() == "control-plane OpenRaft peer transport frame is empty"),
                "{case}: unexpected error: {error}"
            ),
            "oversized frame" => assert!(
                matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
                    if message.contains("peer transport frame size 4097 bytes exceeds limit 4096")),
                "{case}: unexpected error: {error}"
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            runtime.block_on(authority.status()).unwrap(),
            before,
            "{case}: malformed transport reached OpenRaft dispatch"
        );
        assert!(events.lock().unwrap().is_empty());
        assert!(
            checkpoint.events.lock().unwrap().is_empty(),
            "{case}: malformed transport reached checkpoint publication"
        );
        assert!(stream.response.is_empty());
    }
    let metrics = server_auth.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 0);
    assert_eq!(metrics.rejected_without_operation_total(), 0);
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_storage_owned_publication_publishes_once() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let authority = runtime
        .block_on(
            ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "peer-server-publication-once",
                1,
            ),
        )
        .unwrap();
    let durability = authority
        .bind_peer_server_durability(Arc::new(NoopPeerServerCheckpoint))
        .unwrap();
    let calls = AtomicUsize::new(0);
    let mut publish = || {
        calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    };
    publish_control_plane_raft_peer_server_response(&authority, Some(&durability), &mut publish)
        .unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_checkpoint_failure_poisons_bound_authority() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let authority = runtime
        .block_on(
            ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "peer-server-checkpoint-failure-poison",
                1,
            ),
        )
        .unwrap();
    let durability = authority
        .bind_peer_server_durability(Arc::new(FailingPeerServerCheckpoint))
        .unwrap();

    assert!(durability
        .checkpoint_before_snapshot_response(&authority)
        .is_err());
    assert!(
        authority.durability_publication().unwrap().is_poisoned(),
        "storage must poison the issuing authority when checkpoint work fails"
    );
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_response_deadline_starts_after_publication() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster_name = "peer-server-response-deadline";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name,
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let ingress_timeout = Duration::from_millis(250);
    let response_timeout = Duration::from_millis(100);
    let durability = authority
        .bind_peer_server_durability(Arc::new(NoopPeerServerCheckpoint))
        .unwrap();
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096)
        .unwrap()
        .with_durability(durability.clone());
    let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
    let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();
    let mut transport_request = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport_request, &request).unwrap();
    let ingress_deadline = Instant::now() + ingress_timeout;
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut stream = RecordingPeerServerStream::new(transport_request, events)
        .with_ingress_deadline(ingress_deadline);

    let publication = durability.publication.clone();
    let publication_gate = Arc::clone(&publication.gate);
    let gate_locked = Arc::new((Mutex::new(false), Condvar::new()));
    let gate_locked_worker = Arc::clone(&gate_locked);
    let hold_publication = thread::spawn(move || {
        let _guard = publication_gate.0.lock().unwrap();
        let (locked, wake) = &*gate_locked_worker;
        *locked.lock().unwrap() = true;
        wake.notify_one();
        thread::sleep(ingress_timeout + Duration::from_millis(50));
    });
    let (locked, wake) = &*gate_locked;
    let mut locked = locked.lock().unwrap();
    while !*locked {
        locked = wake.wait(locked).unwrap();
    }
    drop(locked);

    handle_control_plane_raft_peer_server_request(
        runtime.handle(),
        &authority,
        &mut stream,
        &policy,
        ingress_deadline,
        response_timeout,
    )
    .unwrap();

    hold_publication.join().unwrap();

    assert!(!stream.response.is_empty());
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_response_failure_preserves_durable_mutation() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let directory = test_util::tempdir();
    let artifact_path = directory.path().join("raft.state");
    let wal_path = directory.path().join("raft.wal");
    let cluster_name = "peer-server-response-failure-durability";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                    cluster_name,
                    1,
                    &artifact_path,
                    &wal_path,
                ),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name,
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096).unwrap();
    let expected_vote = Vote::<ControlPlaneRaftLeaderId>::new(4, 1);
    let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: expected_vote,
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1))
    .unwrap();
    let mut transport_request = Vec::new();
    write_control_plane_raft_peer_transport_frame(&mut transport_request, &request).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut stream =
        RecordingPeerServerStream::new(transport_request, events).with_failed_response_write();

    let error = handle_control_plane_raft_peer_server_request(
        runtime.handle(),
        &authority,
        &mut stream,
        &policy,
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .expect_err("injected response write should fail after dispatch");

    assert!(matches!(
        error,
        ControlPlaneRaftPeerServerWorkerError::PeerRpc(ControlPlaneError::Io { diagnostic: source })
            if source.kind() == io::ErrorKind::BrokenPipe
    ));
    assert_eq!(
        runtime
            .block_on(authority.status())
            .unwrap()
            .persisted_vote(),
        Some(expected_vote),
        "response loss must not roll back the fsynced peer mutation"
    );
    let offsets = authority.durable_wal_monitor_snapshot().unwrap().offsets();
    assert!(
        offsets.clean_len() > offsets.base_offset(),
        "the durable mutation must remain visible to checkpoint scheduling"
    );
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_peer_server_pre_auth_budget_is_held_until_frame_read_finishes() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                    "peer-server-pre-auth-budget",
                    1,
                ),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        "peer-server-pre-auth-budget",
        [(1, "node-1".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    );
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 64).unwrap();
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("peer.sock");
    let raw_listener = UnixListener::bind(&socket_path).unwrap();
    raw_listener.set_nonblocking(true).unwrap();
    let listener = Arc::new(
        ControlPlaneRaftPeerServerListener::unix(
            "peer-server-pre-auth-budget",
            raw_listener,
            1,
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let accept_listener = Arc::clone(&listener);
    let accept_authority = Arc::clone(&authority);
    let accept_policy = policy.clone();
    let runtime_handle = runtime.handle().clone();
    let accept = thread::spawn(move || {
        accept_listener
            .accept_one(&runtime_handle, accept_authority, &accept_policy)
            .unwrap();
    });
    let mut client = UnixStream::connect(&socket_path).unwrap();
    client.write_all(&32_u32.to_be_bytes()).unwrap();

    for _ in 0..100 {
        if policy.reserved_pre_auth_bytes() == 32 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(policy.reserved_pre_auth_bytes(), 32);
    drop(client);
    accept.join().unwrap();
    for _ in 0..100 {
        if policy.reserved_pre_auth_bytes() == 0 && listener.active_workers() == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(policy.reserved_pre_auth_bytes(), 0);
    assert_eq!(listener.active_workers(), 0);
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn control_plane_raft_tls_peer_server_authenticates_dispatches_and_requires_alpn() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster_name = "control-plane-raft-peer-transport-test";
    let authority = Arc::new(
        runtime
            .block_on(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(cluster_name, 1),
            )
            .unwrap(),
    );
    let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name,
        [(1, "node-1".to_owned()), (2, "node-2".to_owned())],
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .with_auth_policy(test_peer_auth_policy(1));
    let policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096).unwrap();
    let client_auth = test_peer_auth_policy(2);
    let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 2, 1);
    let raw_request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(2, 2),
        last_log_id: None,
        leadership_transfer: false,
    })
    .encode_frame_for_peer(&identity)
    .unwrap();
    let request = client_auth
        .sign_peer_frame(&identity, ControlPlaneAuthOperation::RaftVote, raw_request)
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let listener = Arc::new(
        ControlPlaneRaftPeerServerListener::tls_tcp(
            "peer-server-tls",
            listener,
            raft_peer_test_tls_certified_key(),
            1,
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let ControlPlaneRaftPeerServerListenerKind::TlsTcp {
        tls_server_config, ..
    } = &listener.kind
    else {
        panic!("TLS constructor returned a Unix listener");
    };
    assert_eq!(
        tls_server_config.alpn_protocols,
        [CONTROL_PLANE_RAFT_TLS_ALPN]
    );
    let accept_listener = Arc::clone(&listener);
    let accept_authority = Arc::clone(&authority);
    let accept_policy = policy.clone();
    let runtime_handle = runtime.handle().clone();
    let accept = thread::spawn(move || {
        accept_listener
            .accept_one(&runtime_handle, accept_authority, &accept_policy)
            .unwrap();
    });

    let mut client_config =
        rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(raft_peer_test_tls_roots())
            .with_no_client_auth();
    client_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
    let connection = rustls::ClientConnection::new(
        Arc::new(client_config.clone()),
        ServerName::try_from("localhost").unwrap().to_owned(),
    )
    .unwrap();
    let stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut client = rustls::StreamOwned::new(connection, stream);
    write_control_plane_raft_peer_transport_frame(&mut client, &request).unwrap();
    let response = read_control_plane_raft_peer_transport_frame(
        &mut client,
        ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
    )
    .unwrap();
    let response_identity = reverse_raft_peer_frame_identity(&identity);
    let response = client_auth
        .verify_peer_frame(
            &response,
            &response_identity,
            ControlPlaneAuthOperation::RaftVote,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .unwrap();
    assert!(matches!(
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&response, &response_identity)
            .unwrap(),
        ControlPlaneRaftPeerRpcResponse::Vote(_)
    ));
    accept.join().unwrap();
    for _ in 0..100 {
        if listener.active_workers() == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(listener.active_workers(), 0);
    assert_eq!(
        policy
            .peer_policy
            .auth_policy()
            .unwrap()
            .metrics_snapshot()
            .accepted_total(),
        1
    );

    client_config.alpn_protocols.clear();
    let no_alpn_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let no_alpn_address = no_alpn_listener.local_addr().unwrap();
    let no_alpn_listener = Arc::new(
        ControlPlaneRaftPeerServerListener::tls_tcp(
            "peer-server-no-alpn",
            no_alpn_listener,
            raft_peer_test_tls_certified_key(),
            1,
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let accept_listener = Arc::clone(&no_alpn_listener);
    let accept_authority = Arc::clone(&authority);
    let accept_policy = policy.clone();
    let runtime_handle = runtime.handle().clone();
    let accept = thread::spawn(move || {
        accept_listener
            .accept_one(&runtime_handle, accept_authority, &accept_policy)
            .unwrap();
    });
    let connection = rustls::ClientConnection::new(
        Arc::new(client_config),
        ServerName::try_from("localhost").unwrap().to_owned(),
    )
    .unwrap();
    let stream = TcpStream::connect(no_alpn_address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut client = rustls::StreamOwned::new(connection, stream);
    let _ = write_control_plane_raft_peer_transport_frame(&mut client, &request);
    assert!(read_control_plane_raft_peer_transport_frame(
        &mut client,
        ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
    )
    .is_err());
    accept.join().unwrap();
    for _ in 0..100 {
        if no_alpn_listener.active_workers() == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(no_alpn_listener.active_workers(), 0);
    assert_eq!(
        policy
            .peer_policy
            .auth_policy()
            .unwrap()
            .metrics_snapshot()
            .accepted_total(),
        1
    );
    runtime.block_on(authority.shutdown()).unwrap();
}

#[test]
fn raft_peer_current_and_adjacent_alpn_profiles_are_negotiated_exactly() {
    let client_config = raft_peer_test_tls_endpoint(1).tls_client_config_for_test();
    let listener = ControlPlaneRaftPeerServerListener::tls_tcp(
        "peer-alpn-profile",
        TcpListener::bind("127.0.0.1:0").unwrap(),
        raft_peer_test_tls_certified_key(),
        1,
        Duration::from_secs(1),
    )
    .unwrap();
    let ControlPlaneRaftPeerServerListenerKind::TlsTcp {
        tls_server_config, ..
    } = listener.kind
    else {
        panic!("TLS/TCP constructor returned a Unix listener")
    };

    crate::internal_tls_protocol::assert_current_and_adjacent_profile_negotiation(
        InternalTlsProtocol::RaftPeer,
        client_config,
        tls_server_config,
    );
}

#[test]
fn control_plane_raft_peer_policy_rejects_fresh_topology_mismatch() {
    let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        "cluster-a",
        [(1, "node-1".to_string()), (2, "node-2".to_string())],
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .with_topology_identity(7, "topology-a");

    let error = policy
        .validate_incoming_frame_identity(
            &ControlPlaneRaftPeerFrameIdentity::new("cluster-a", 1, 2)
                .with_topology(8, "topology-b"),
            2,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneRaftPeerTransportRejection::TopologyMismatch { .. }
    ));
}

#[test]
fn control_plane_raft_peer_transport_frame_rejects_oversized_prefix_before_payload_read() {
    let max_frame_bytes = 8usize;
    let mut transport = Vec::new();
    write_raft_u32(&mut transport, u32::try_from(max_frame_bytes + 1).unwrap());
    let mut cursor = Cursor::new(transport);

    let err =
        read_control_plane_raft_peer_transport_frame(&mut cursor, max_frame_bytes).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("peer transport frame size 9 bytes exceeds limit 8")
    ));
}

#[test]
fn control_plane_raft_peer_transport_rejects_endpoint_mismatch() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut network = test_policy_network_client(2, &BasicNode::new("wrong-node-2")).await;
        let err = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
                    last_log_id: None,
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_millis(10)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Network(error)
                if error.to_string().contains("endpoint mismatch")
                    && error.to_string().contains("expected node-2")
                    && error.to_string().contains("got wrong-node-2")
        ));
    });
}

#[test]
fn control_plane_raft_peer_transport_rejects_unconfigured_target() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut network = test_policy_network_client(3, &BasicNode::new("node-3")).await;
        let err = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
                    last_log_id: None,
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_millis(10)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Unreachable(error)
                if error.to_string().contains("no configured target node 3")
        ));
    });
}

#[test]
fn control_plane_raft_peer_transport_rejects_oversized_append_entries_batch() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
        let err = network
            .append_entries(
                AppendEntriesRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                    prev_log_id: None,
                    entries: vec![blank_entry(1, 1, 1), blank_entry(1, 1, 2)],
                    leader_commit: None,
                },
                RPCOption::new(Duration::from_millis(10)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Network(error)
                if error.to_string().contains("2 entries exceeds limit 1")
        ));
    });
}

#[test]
fn control_plane_raft_peer_transport_rejects_oversized_append_entries_payload() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
        let err = network
            .append_entries(
                AppendEntriesRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                    prev_log_id: None,
                    entries: vec![normal_entry(
                        1,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "x".repeat(1024))],
                            pg_ids: vec![],
                        },
                    )],
                    leader_commit: None,
                },
                RPCOption::new(Duration::from_millis(10)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Network(error)
                if error.to_string().contains("encoded entries payload")
                    && error.to_string().contains("exceeds limit 128")
        ));
    });
}

#[test]
fn control_plane_raft_peer_transport_rejects_oversized_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let snapshot = state_machine.build_snapshot().unwrap();
        assert!(!snapshot.snapshot.get_ref().is_empty());

        let err = network
            .full_snapshot(
                Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                snapshot,
                std::future::pending::<ReplicationClosed>(),
                RPCOption::new(Duration::from_millis(10)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StreamingError::Network(error)
                if error.to_string().contains("bytes exceeds limit 0")
        ));
    });
}

#[test]
fn control_plane_raft_peer_network_routes_frames_through_configured_transport() {
    ControlPlaneRaftTypeConfig::run(async {
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let node = BasicNode::new("tcp://peer-2.internal:7402");
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-configured-transport-test",
            BTreeMap::from([(1, BasicNode::new("local")), (2, node.clone())]),
            limits,
        )
        .with_timeouts(Duration::from_millis(123), Duration::from_secs(1));
        let transport = Arc::new(TestPeerFrameTransport::new());
        let mut factory = ControlPlaneRaftPeerNetworkFactory::new_with_transport(
            1,
            policy,
            Duration::from_secs(1),
            transport.clone(),
        );
        let mut network = factory.new_client(2, &node).await;

        let response = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                    last_log_id: Some(raft_log_id(6, 1, 10)),
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();

        assert!(response.vote_granted);
        assert_eq!(response.vote, Vote::<ControlPlaneRaftLeaderId>::new(7, 1));
        assert_eq!(response.last_log_id, Some(raft_log_id(6, 1, 10)));
        assert_eq!(
            *transport.observations.lock().unwrap(),
            vec![(
                2,
                "tcp://peer-2.internal:7402".to_string(),
                4096,
                Duration::from_millis(123),
            )]
        );
    });
}

#[test]
fn control_plane_raft_unix_peer_network_vote_round_trips_framed_identity() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("vote-round-trip");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let expected_request_identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-unix-peer-test", 1, 2);
        let server_identity = expected_request_identity.clone();
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            let request_frame =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            let request = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
                &request_frame,
                &server_identity,
            )
            .unwrap();
            let ControlPlaneRaftPeerRpcRequest::Vote(request) = request else {
                panic!("decoded wrong Unix peer request variant");
            };
            assert_eq!(request.vote, Vote::<ControlPlaneRaftLeaderId>::new(7, 1));
            assert_eq!(request.last_log_id, Some(raft_log_id(6, 1, 10)));
            assert!(!request.leadership_transfer);

            let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                vote_granted: true,
                last_log_id: Some(raft_log_id(7, 2, 11)),
            });
            let response_frame = response
                .encode_frame_for_peer(&reverse_raft_peer_frame_identity(&server_identity))
                .unwrap();
            write_control_plane_raft_peer_transport_frame(&mut stream, &response_frame).unwrap();
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-unix-peer-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        );
        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
        let mut network = factory.new_client(2, &node).await;
        let response = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                    last_log_id: Some(raft_log_id(6, 1, 10)),
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                vote_granted: true,
                last_log_id: Some(raft_log_id(7, 2, 11)),
            }
        );
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_raft_unix_peer_network_vote_round_trips_authenticated_response() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("vote-auth-round-trip");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let request_identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
        let server_identity = request_identity.clone();
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            let request_frame =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            let request_frame = test_peer_auth_policy(2)
                .verify_peer_frame(
                    &request_frame,
                    &server_identity,
                    ControlPlaneAuthOperation::RaftVote,
                    limits.max_frame_bytes,
                )
                .unwrap();
            let request = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
                &request_frame,
                &server_identity,
            )
            .unwrap();
            let ControlPlaneRaftPeerRpcRequest::Vote(request) = request else {
                panic!("decoded wrong Unix peer request variant");
            };
            assert_eq!(request.vote, Vote::<ControlPlaneRaftLeaderId>::new(7, 1));

            let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                vote_granted: true,
                last_log_id: Some(raft_log_id(7, 2, 11)),
            });
            let response_identity = reverse_raft_peer_frame_identity(&server_identity);
            let response_frame = response.encode_frame_for_peer(&response_identity).unwrap();
            let signed_response = test_peer_auth_policy(2)
                .sign_peer_frame(
                    &response_identity,
                    ControlPlaneAuthOperation::RaftVote,
                    response_frame,
                )
                .unwrap();
            write_control_plane_raft_peer_transport_frame(&mut stream, &signed_response).unwrap();
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-peer-transport-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        )
        .with_auth_policy(test_peer_auth_policy(1));
        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
        let mut network = factory.new_client(2, &node).await;
        let response = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                    last_log_id: Some(raft_log_id(6, 1, 10)),
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                vote_granted: true,
                last_log_id: Some(raft_log_id(7, 2, 11)),
            }
        );
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_raft_unix_peer_network_rejects_unauthenticated_response_when_auth_required() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("vote-unauth-response");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let request_identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-transport-test", 1, 2);
        let server_identity = request_identity.clone();
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            let request_frame =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            test_peer_auth_policy(2)
                .verify_peer_frame(
                    &request_frame,
                    &server_identity,
                    ControlPlaneAuthOperation::RaftVote,
                    limits.max_frame_bytes,
                )
                .unwrap();

            let response_identity = reverse_raft_peer_frame_identity(&server_identity);
            let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                vote_granted: true,
                last_log_id: None,
            });
            let response_frame = response.encode_frame_for_peer(&response_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut stream, &response_frame).unwrap();
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-peer-transport-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        )
        .with_auth_policy(test_peer_auth_policy(1));
        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
        let mut network = factory.new_client(2, &node).await;
        let err = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                    last_log_id: None,
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Network(error)
                if error
                    .to_string()
                    .contains("auth response decode peer frame failed")
        ));
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_openraft_unix_peer_two_node_client_write_replicates_to_follower() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let (_node1_socket_dir, node1_socket) =
            raft_unix_socket_path("two-node-replication-node-1");
        let (_node2_socket_dir, node2_socket) =
            raft_unix_socket_path("two-node-replication-node-2");
        let cluster_name = "control-plane-raft-unix-peer-two-node-replication-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, node1_socket.display().to_string()),
                (2, node2_socket.display().to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let authority1 = Arc::new(
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
                cluster_name,
                1,
                &tmp.path().join("node-1.state"),
                policy.clone(),
                Duration::from_secs(1),
            )
            .await
            .unwrap(),
        );
        let authority2 = Arc::new(
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
                cluster_name,
                2,
                &tmp.path().join("node-2.state"),
                policy.clone(),
                Duration::from_secs(1),
            )
            .await
            .unwrap(),
        );
        let listener1 =
            TestUnixPeerListener::spawn(node1_socket, Arc::clone(&authority1), 1, policy.clone());
        let listener2 =
            TestUnixPeerListener::spawn(node2_socket, Arc::clone(&authority2), 2, policy.clone());

        authority1
            .initialize_membership(policy.peers())
            .await
            .unwrap();
        authority1
            .wait_for_current_leader(
                1,
                Duration::from_secs(1),
                "Unix-peer two-node initialized leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority1,
            Duration::from_secs(1),
            "Unix-peer two-node leader applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let write = authority1
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1), "node-1".to_string()),
                    (NodeId::new(2), "node-2".to_string()),
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
                "Unix-peer two-node follower applied client write",
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
        assert_eq!(follower_state.1, vec![NodeId::new(1), NodeId::new(2)]);

        drop(listener1);
        drop(listener2);
        authority1.shutdown().await.unwrap();
        authority2.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_unix_peer_network_read_eof_is_unreachable() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("vote-eof");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            drop(stream);
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-unix-peer-eof-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        );
        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
        let mut network = factory.new_client(2, &node).await;
        let err = network
            .vote(
                VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                    last_log_id: None,
                    leadership_transfer: false,
                },
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RPCError::Unreachable(error)
                if error.to_string().contains("read transport")
        ));
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_raft_unix_peer_network_stalled_peer_does_not_block_runtime() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("vote-stalled");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            thread::sleep(Duration::from_millis(150));
            drop(stream);
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-unix-peer-stalled-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        );
        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_millis(50));
        let mut network = factory.new_client(2, &node).await;
        let vote = network.vote(
            VoteRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                last_log_id: None,
                leadership_transfer: false,
            },
            RPCOption::new(Duration::from_millis(50)),
        );
        let runtime_tick = ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10));
        futures_util::pin_mut!(vote);
        futures_util::pin_mut!(runtime_tick);

        match futures_util::future::select(runtime_tick, vote).await {
            futures_util::future::Either::Left(((), vote)) => {
                let err = vote.await.unwrap_err();
                assert!(matches!(
                    err,
                    RPCError::Unreachable(error)
                        if error.to_string().contains("read transport")
                ));
            }
            futures_util::future::Either::Right((result, _)) => {
                panic!("stalled peer RPC completed before runtime tick: {result:?}");
            }
        }

        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_raft_unix_peer_snapshot_read_eof_is_unreachable() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) = raft_unix_socket_path("snapshot-eof");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 8192,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 8192,
        };
        let server_path = socket_path.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ =
                read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)
                    .unwrap();
            drop(stream);
            let _ = std::fs::remove_file(server_path);
        });

        let node = BasicNode::new(socket_path.display().to_string());
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-unix-peer-snapshot-eof-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
            limits,
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let snapshot = state_machine.build_snapshot().unwrap();

        let mut factory =
            ControlPlaneRaftPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
        let mut network = factory.new_client(2, &node).await;
        let err = network
            .full_snapshot(
                Vote::<ControlPlaneRaftLeaderId>::new_committed(7, 1),
                snapshot,
                std::future::pending::<ReplicationClosed>(),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StreamingError::Unreachable(error)
                if error.to_string().contains("full_snapshot read transport")
        ));
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    });
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_dispatches_vote() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_001;
        let target_node_id = 7_002;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            "control-plane-raft-unix-peer-handler-vote-test",
            target_node_id,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(target_node_id)
            .await
            .unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
            "control-plane-raft-unix-handler",
            source_node_id,
            target_node_id,
        );
        let client_identity = request_identity.clone();
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                last_log_id: None,
                leadership_transfer: false,
            });
            let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
            let response_frame = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                limits.max_frame_bytes,
            )
            .unwrap();
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                &response_frame,
                &reverse_raft_peer_frame_identity(&client_identity),
            )
            .unwrap()
        });

        handle_control_plane_raft_peer_unix_stream(
            authority.raft(),
            &mut server_stream,
            ControlPlaneRaftPeerFrameKind::OrdinaryRpc,
            limits,
            &request_identity,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let response = client.join().unwrap();
        assert!(matches!(response, ControlPlaneRaftPeerRpcResponse::Vote(_)));
    });
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_dispatches_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_011;
        let target_node_id = 7_012;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            "control-plane-raft-unix-peer-handler-snapshot-test",
            target_node_id,
        )
        .await
        .unwrap();
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        let snapshot = state_machine.build_snapshot().unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 8192,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 8192,
        };
        let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
            "control-plane-raft-unix-handler",
            source_node_id,
            target_node_id,
        );
        let client_identity = request_identity.clone();
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerSnapshotRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id),
                snapshot,
            };
            let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
            let response_frame = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                limits.max_frame_bytes,
            )
            .unwrap();
            ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
                &response_frame,
                &reverse_raft_peer_frame_identity(&client_identity),
            )
            .unwrap()
        });

        handle_control_plane_raft_peer_unix_stream(
            authority.raft(),
            &mut server_stream,
            ControlPlaneRaftPeerFrameKind::Snapshot,
            limits,
            &request_identity,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let response = client.join().unwrap();
        assert_eq!(
            response.response.vote,
            Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id)
        );
    });
}

#[test]
fn control_plane_raft_peer_request_frame_kind_rejects_response_frames() {
    let identity = ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-kind-detect", 1, 2);
    let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
        vote: Vote::<ControlPlaneRaftLeaderId>::new(3, 2),
        vote_granted: true,
        last_log_id: None,
    });
    let encoded_response = response.encode_frame_for_peer(&identity).unwrap();
    let err = decode_control_plane_raft_peer_request_frame_kind(&encoded_response).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("response frame cannot be handled as a request")
    ));

    let snapshot_response = ControlPlaneRaftPeerSnapshotResponse {
        response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new(3, 2)),
    };
    let encoded_snapshot_response = snapshot_response.encode_frame_for_peer(&identity).unwrap();
    let err =
        decode_control_plane_raft_peer_request_frame_kind(&encoded_snapshot_response).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::CommandDecode { message }
            if message.contains("snapshot response frame cannot be handled as a request")
    ));
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_auto_dispatches_vote() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_021;
        let target_node_id = 7_022;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            "control-plane-raft-unix-peer-auto-handler-vote-test",
            target_node_id,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(target_node_id)
            .await
            .unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
            "control-plane-raft-unix-auto-handler",
            source_node_id,
            target_node_id,
        );
        let client_identity = request_identity.clone();
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                last_log_id: None,
                leadership_transfer: false,
            });
            let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
            let response_frame = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                limits.max_frame_bytes,
            )
            .unwrap();
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                &response_frame,
                &reverse_raft_peer_frame_identity(&client_identity),
            )
            .unwrap()
        });

        handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
            authority.raft(),
            &mut server_stream,
            limits,
            &request_identity,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let response = client.join().unwrap();
        assert!(matches!(response, ControlPlaneRaftPeerRpcResponse::Vote(_)));
    });
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_accepts_configured_source() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_025;
        let target_node_id = 7_026;
        let cluster_name = "control-plane-raft-unix-configured-handler";
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            cluster_name,
            target_node_id,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(target_node_id)
            .await
            .unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (source_node_id, "source.sock".to_string()),
                (target_node_id, "target.sock".to_string()),
            ],
            limits,
        );
        let request_identity =
            ControlPlaneRaftPeerFrameIdentity::new(cluster_name, source_node_id, target_node_id);
        let client_identity = request_identity.clone();
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                last_log_id: None,
                leadership_transfer: false,
            });
            let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
            let response_frame = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                limits.max_frame_bytes,
            )
            .unwrap();
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                &response_frame,
                &reverse_raft_peer_frame_identity(&client_identity),
            )
            .unwrap()
        });

        handle_control_plane_raft_peer_unix_stream_from_configured_peer(
            authority.raft(),
            &mut server_stream,
            target_node_id,
            &policy,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let response = client.join().unwrap();
        assert!(matches!(response, ControlPlaneRaftPeerRpcResponse::Vote(_)));
    });
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_rejects_unconfigured_source() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_027;
        let target_node_id = 7_028;
        let cluster_name = "control-plane-raft-unix-configured-handler-reject";
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            cluster_name,
            target_node_id,
        )
        .await
        .unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 4096,
        };
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [(target_node_id, "target.sock".to_string())],
            limits,
        );
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                last_log_id: None,
                leadership_transfer: false,
            });
            let request_frame = request
                .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(
                    cluster_name,
                    source_node_id,
                    target_node_id,
                ))
                .unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
        });

        let err = handle_control_plane_raft_peer_unix_stream_from_configured_peer(
            authority.raft(),
            &mut server_stream,
            target_node_id,
            &policy,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            ControlPlaneError::RpcProtocol { diagnostic: message }
                if message.contains("no configured source node")
                    && message.contains(&source_node_id.to_string())
        ));
        client.join().unwrap();
    });
}

#[test]
fn control_plane_raft_peer_unix_stream_handler_auto_dispatches_snapshot() {
    ControlPlaneRaftTypeConfig::run(async {
        let source_node_id = 7_031;
        let target_node_id = 7_032;
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
            "control-plane-raft-unix-peer-auto-handler-snapshot-test",
            target_node_id,
        )
        .await
        .unwrap();
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        let snapshot = state_machine.build_snapshot().unwrap();
        let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 8192,
            max_append_entries: 8,
            max_append_entries_bytes: 4096,
            max_snapshot_bytes: 8192,
        };
        let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
            "control-plane-raft-unix-auto-handler",
            source_node_id,
            target_node_id,
        );
        let client_identity = request_identity.clone();
        let client = thread::spawn(move || {
            let request = ControlPlaneRaftPeerSnapshotRequest {
                vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id),
                snapshot,
            };
            let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                .unwrap();
            let response_frame = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                limits.max_frame_bytes,
            )
            .unwrap();
            ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
                &response_frame,
                &reverse_raft_peer_frame_identity(&client_identity),
            )
            .unwrap()
        });

        handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
            authority.raft(),
            &mut server_stream,
            limits,
            &request_identity,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let response = client.join().unwrap();
        assert_eq!(
            response.response.vote,
            Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id)
        );
    });
}

fn test_raft_config(cluster_name: &'static str) -> Arc<Config> {
    test_raft_config_with_log_reversion(cluster_name, None)
}

#[test]
fn experimental_raft_replication_batch_fits_peer_transport_limit() {
    let config = experimental_raft_config(
        "control-plane-raft-batch-limit-test",
        ExperimentalRaftTimerMode::Automatic,
    )
    .unwrap();

    assert_eq!(
        config.snapshot_policy,
        SnapshotPolicy::Never,
        "only the application-coordinated snapshot/purge path may compact the restart log"
    );
    assert_eq!(
        config.max_in_snapshot_log_to_keep,
        u64::MAX,
        "manual snapshot completion must not schedule implicit log purge"
    );
    assert_eq!(
        config.max_payload_entries,
        CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES
    );
    assert!(
        config.max_payload_entries
            <= ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES as u64
    );
    assert!(
        CONTROL_PLANE_RAFT_APPEND_ENTRIES_COUNT_BYTES
            + CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
                * usize::try_from(config.max_payload_entries).unwrap()
            <= ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES
    );
}

#[test]
fn experimental_raft_replication_rejects_undersized_peer_policy() {
    let policy = ControlPlaneRaftPeerTransportPolicy::new(
        "control-plane-raft-undersized-policy-test",
        BTreeMap::from([(1, BasicNode::new("node-1"))]),
        ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
            max_append_entries: usize::try_from(CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES).unwrap()
                - 1,
            max_append_entries_bytes:
                ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES,
            max_snapshot_bytes: ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_SNAPSHOT_BYTES,
        },
    );

    let error = policy.validate_replication_compatibility().unwrap_err();
    assert!(error
        .to_string()
        .contains("below the configured replication batch size"));
}

#[test]
fn control_plane_command_replication_rejects_oversized_entry() {
    let command = ControlPlaneCommand::BootstrapInitialClusterMap {
        nodes: vec![(
            NodeId::new(1),
            "x".repeat(CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES),
        )],
        pg_ids: vec![PgId::new(0)],
    };

    let error = validate_control_plane_command_replication_size_detailed(&command).unwrap_err();
    assert!(matches!(error, ControlPlaneError::RpcProtocol { .. }));
    assert!(error.retained_diagnostic_contains("exceeding the replication-safe per-entry limit"));
    assert_eq!(
        validate_control_plane_command_replication_size(&command)
            .unwrap_err()
            .to_string(),
        "control-plane command is not accepted by the current replication policy"
    );
}

#[test]
fn control_plane_command_replication_rejects_oversized_entry_before_log_mutation() {
    ControlPlaneRaftTypeConfig::run(async {
        let log_store = ControlPlaneRaftLogStore::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-command-size-boundary-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority = ControlPlaneRaftAuthority::new_with_log_store(
            raft,
            log_store,
            "control-plane-raft-command-size-boundary-test",
        );
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "command-size boundary leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "command-size boundary authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let before = authority.status().await.unwrap();
        let command = ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![(
                NodeId::new(1),
                "x".repeat(CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES),
            )],
            pg_ids: vec![PgId::new(0)],
        };

        let error = authority
            .submit_control_plane_command(command)
            .await
            .unwrap_err();
        assert!(matches!(error, ControlPlaneError::RpcProtocol { .. }));
        let after = authority.status().await.unwrap();
        assert_eq!(after.last_log_id(), before.last_log_id());
        assert_eq!(after.committed(), before.committed());
        assert_eq!(after.applied(), before.applied());

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn unix_peer_rpc_timeout_honors_openraft_soft_ttl() {
    let network = ControlPlaneRaftPeerNetwork {
        local_node_id: 1,
        target: 2,
        node: BasicNode::new("unused"),
        policy: Arc::new(ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-rpc-timeout-test",
            BTreeMap::new(),
            ControlPlaneRaftPeerTransportLimits::default(),
        )),
        rpc_timeout: Duration::from_secs(1),
        transport: Arc::new(ControlPlaneRaftUnixPeerFrameTransport),
    };

    let short_deadline = network
        .effective_rpc_deadline(&RPCOption::new(Duration::from_millis(400)))
        .unwrap();
    let short_budget = short_deadline.saturating_duration_since(Instant::now());
    assert!(short_budget <= Duration::from_millis(300));
    assert!(!short_budget.is_zero());

    let capped_deadline = network
        .effective_rpc_deadline(&RPCOption::new(Duration::from_secs(2)))
        .unwrap();
    let capped_budget = capped_deadline.saturating_duration_since(Instant::now());
    assert!(capped_budget <= Duration::from_secs(1));
    assert!(!capped_budget.is_zero());
}

#[test]
fn unix_peer_rpc_absolute_deadline_bounds_complete_response_read() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_socket_dir, socket_path) =
            raft_unix_socket_path("control-plane-raft-absolute-deadline-test");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(200));
        });
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-absolute-deadline-test",
            BTreeMap::from([
                (1, BasicNode::new("source")),
                (2, BasicNode::new(socket_path.to_string_lossy())),
            ]),
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let network = ControlPlaneRaftPeerNetwork {
            local_node_id: 1,
            target: 2,
            node: BasicNode::new(socket_path.to_string_lossy()),
            policy: Arc::new(policy),
            rpc_timeout: Duration::from_secs(1),
            transport: Arc::new(ControlPlaneRaftUnixPeerFrameTransport),
        };
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
            last_log_id: None,
            leadership_transfer: false,
        });
        let started = Instant::now();
        let error = network
            .send_rpc_frame(
                "vote",
                request,
                started.checked_add(Duration::from_millis(40)).unwrap(),
            )
            .await
            .unwrap_err();
        let elapsed = started.elapsed();

        assert!(elapsed < Duration::from_millis(150), "elapsed={elapsed:?}");
        assert!(matches!(
            error,
            RPCError::Unreachable(_) | RPCError::Network(_)
        ));
        server.join().unwrap();
    });
}
