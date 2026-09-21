// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[test]
fn control_plane_raft_log_store_rejects_append_holes() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();

        let err = RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 1)], IOFlushed::noop())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expected 0"));
        assert_eq!(
            RaftLogStorage::get_log_state(&mut store)
                .await
                .unwrap()
                .last_log_id,
            None
        );

        RaftLogStorage::append(
            &mut store,
            vec![bootstrap_membership_entry(1)],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let err = RaftLogStorage::append(
            &mut store,
            vec![blank_entry(3, 1, 1), blank_entry(3, 1, 3)],
            IOFlushed::noop(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("log hole at index 2"));
        let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
            .await
            .unwrap();
        assert!(entries.is_empty());
        let entries = RaftLogReader::try_get_log_entries(&mut store, 0..5)
            .await
            .unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            vec![raft_log_id(0, 1, 0)]
        );
    });
}

#[test]
fn control_plane_raft_log_store_rejects_append_after_max_index() {
    ControlPlaneRaftTypeConfig::run(async {
        let artifact = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, u64::MAX)),
            last_purged_log_id: Some(raft_log_id(3, 1, u64::MAX)),
            ..Default::default()
        };
        let mut store =
            ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact).unwrap();

        let err = RaftLogStorage::append(
            &mut store,
            vec![blank_entry(3, 1, u64::MAX)],
            IOFlushed::noop(),
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("cannot append after u64::MAX OpenRaft log index"));
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(
            log_state.last_purged_log_id,
            Some(raft_log_id(3, 1, u64::MAX))
        );
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, u64::MAX)));
    });
}

#[test]
fn control_plane_raft_log_store_purges_and_truncates_without_holes() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
                blank_entry(3, 1, 3),
                blank_entry(3, 1, 4),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();

        let err = RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no committed restart gate"));

        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

        let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
            .await
            .unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
        );

        RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 3)))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));

        RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));

        RaftLogStorage::truncate_after(&mut store, None)
            .await
            .unwrap_err();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
    });
}

#[test]
fn control_plane_raft_log_store_restores_restart_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::append(
            &mut store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
                blank_entry(3, 1, 3),
                blank_entry(3, 1, 4),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
            .await
            .unwrap();

        let artifact = store.export_restart_artifact().unwrap();
        let mut restored =
            ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact).unwrap();

        assert_eq!(
            RaftLogReader::read_vote(&mut restored).await.unwrap(),
            Some(vote)
        );
        assert_eq!(
            RaftLogStorage::read_committed(&mut restored).await.unwrap(),
            Some(raft_log_id(3, 1, 2))
        );
        let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

        let entries = RaftLogReader::try_get_log_entries(&mut restored, 0..5)
            .await
            .unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
        );

        RaftLogStorage::append(&mut restored, vec![blank_entry(3, 1, 5)], IOFlushed::noop())
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 5)));
    });
}

#[test]
fn control_plane_raft_combined_restart_restores_catchup_state() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
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
        state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();

        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .unwrap();
        let (mut restored_log_store, mut restored_state_machine) = artifact.restore().unwrap();

        assert_eq!(
            RaftLogStorage::read_committed(&mut restored_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 3))
        );
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 2))
        );
        restored_state_machine
            .apply_entry(blank_entry(3, 1, 3))
            .unwrap();
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 3))
        );
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_codec_round_trips() {
    ControlPlaneRaftTypeConfig::run(async {
        let bootstrap_membership = Membership::new(
            vec![BTreeSet::from([1])],
            BTreeMap::from([(1, BasicNode::new("raft-node-1"))]),
        )
        .unwrap();
        let bootstrap_entry = Entry {
            log_id: raft_log_id(0, 1, 0),
            payload: EntryPayload::Membership(bootstrap_membership),
        };
        let bootstrap_command = ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
            pg_ids: vec![PgId::new(1)],
        };
        let command_entry = normal_entry(3, 1, 1, bootstrap_command);

        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![bootstrap_entry.clone(), command_entry.clone()],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut log_store, &vote)
            .await
            .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_entry).unwrap();
        state_machine.apply_entry(command_entry).unwrap();
        let expected_snapshot = state_machine.inner().snapshot().clone();
        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .unwrap();

        let encoded = artifact.encode_durable_artifact().unwrap();
        let decoded = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&encoded)
            .expect("durable restart artifact should decode");
        assert_eq!(decoded.local_node_id, 1);
        let (mut restored_log_store, restored_state_machine) = decoded.restore().unwrap();

        assert_eq!(
            RaftLogReader::read_vote(&mut restored_log_store)
                .await
                .unwrap(),
            Some(vote)
        );
        assert_eq!(
            RaftLogStorage::read_committed(&mut restored_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 1))
        );
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );
        assert_eq!(
            restored_state_machine.inner().snapshot(),
            &expected_snapshot
        );
        let restored_entries = RaftLogReader::try_get_log_entries(&mut restored_log_store, 0..2)
            .await
            .unwrap();
        assert_eq!(
            restored_entries
                .iter()
                .map(|entry| entry.log_id)
                .collect::<Vec<_>>(),
            vec![raft_log_id(0, 1, 0), raft_log_id(3, 1, 1)]
        );
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_v4_aggregates_remain_exact_and_rejected() {
    for (encoded, expected_len, expected_digest) in [
        (
            include_str!("restart_artifact_v4_state_v28_command_v15_aggregate.hex"),
            2_241,
            "dd78c6257bcfbcd8d442c4bc3e80a4281993a6adeb009f5774cc98b84071ecf9",
        ),
        (
            include_str!("restart_artifact_v4_state_v29_command_v16_aggregate.hex"),
            2_245,
            "fe9a18f104034100ba82445ec8c2c302b4506e1fa2ff6119f68821ac9f762792",
        ),
    ] {
        assert_raft_restart_v4_aggregate_is_exact_and_rejected(
            encoded,
            expected_len,
            expected_digest,
        );
    }
}

fn assert_raft_restart_v4_aggregate_is_exact_and_rejected(
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
    let mut artifact_count = 0;
    while offset < aggregate.len() {
        let artifact_len = u32::from_be_bytes(
            aggregate[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &aggregate[offset..offset + artifact_len];
        offset += artifact_len;
        assert!(matches!(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
                artifact
            ),
            Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::UnsupportedVersion(4)
            ))
        ));
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
}

#[test]
fn control_plane_raft_durable_restart_artifact_v5_aggregate_is_exact_and_complete() {
    let empty = ControlPlaneRaftRestartArtifact {
        cluster_name: "restart-v5-empty".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };

    let mut empty_snapshot_state_machine = ControlPlaneRaftStateMachine::empty();
    empty_snapshot_state_machine.build_snapshot().unwrap();
    let empty_snapshot = ControlPlaneRaftRestartArtifact {
        cluster_name: "restart-v5-empty-snapshot".to_string(),
        local_node_id: 2,
        wal_replay_offset: 17,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: empty_snapshot_state_machine.export_restart_artifact(),
    };

    let bootstrap_membership = single_node_bootstrap_membership_entry(1);
    let bootstrap_command = normal_entry(
        3,
        1,
        1,
        ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![(NodeId::new(1), "/tmp/restart-v5-node-1.sock".to_string())],
            pg_ids: vec![PgId::new(7)],
        },
    );
    let blank = blank_entry(3, 1, 2);
    let mut populated_state_machine = ControlPlaneRaftStateMachine::empty();
    for entry in [
        bootstrap_membership.clone(),
        bootstrap_command.clone(),
        blank.clone(),
    ] {
        populated_state_machine.apply_entry(entry).unwrap();
    }
    populated_state_machine.build_snapshot().unwrap();
    let populated = ControlPlaneRaftRestartArtifact {
        cluster_name: "restart-v5-populated".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0x0102_0304_0506_0708,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            last_purged_log_id: None,
            entries: vec![bootstrap_membership, bootstrap_command, blank],
        },
        state_machine: populated_state_machine.export_restart_artifact(),
    };

    let bootstrap_membership = single_node_bootstrap_membership_entry(1);
    let mut purged_state_machine = ControlPlaneRaftStateMachine::empty();
    purged_state_machine
        .apply_entry(bootstrap_membership.clone())
        .unwrap();
    let purged = ControlPlaneRaftRestartArtifact {
        cluster_name: "restart-v5-purged".to_string(),
        local_node_id: 1,
        wal_replay_offset: 23,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new(4, 1)),
            committed: Some(bootstrap_membership.log_id),
            last_purged_log_id: Some(bootstrap_membership.log_id),
            entries: Vec::new(),
        },
        state_machine: purged_state_machine.export_restart_artifact(),
    };

    let artifacts = [empty, empty_snapshot, populated, purged];
    let option_capture = ControlPlaneRaftRestartOptionCapture::begin();
    let encoded = artifacts
        .iter()
        .map(|artifact| artifact.encode_durable_artifact().unwrap())
        .collect::<Vec<_>>();
    let observed_options = option_capture.finish().into_iter().collect::<BTreeSet<_>>();
    let expected_options = ControlPlaneRaftRestartOptionalField::ALL
        .iter()
        .copied()
        .flat_map(|field| {
            RaftWireOptionTag::ALL
                .into_iter()
                .map(move |arm| (field, arm))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(observed_options, expected_options);

    let observed_entry_payloads = artifacts
        .iter()
        .flat_map(|artifact| artifact.log_store.entries.iter())
        .map(control_plane_raft_entry_payload_tag)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        observed_entry_payloads,
        ControlPlaneRaftEntryPayloadTag::ALL
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    let observed_vote_booleans = artifacts
        .iter()
        .filter_map(|artifact| artifact.log_store.vote)
        .map(|vote| RaftWireBoolean::from_bool(vote.committed))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        observed_vote_booleans,
        RaftWireBoolean::ALL.into_iter().collect::<BTreeSet<_>>()
    );

    let mut aggregate = Vec::new();
    for bytes in &encoded {
        write_raft_u32(
            &mut aggregate,
            u32::try_from(bytes.len()).expect("restart fixture length should fit u32"),
        );
        aggregate.extend_from_slice(bytes);
        let decoded = ControlPlaneRaftRestartArtifact::decode_durable_artifact(bytes).unwrap();
        assert_eq!(decoded.encode_durable_artifact().unwrap(), *bytes);
    }
    assert_eq!(
        (
            aggregate.len(),
            raft_test_hex(&checksum::sha256::digest(&aggregate))
        ),
        (
            2200,
            "4c37739d9a8219c128c8841f01310184bd254ca1394252321ce56091696d5217"
                .to_string()
        )
    );
}

#[test]
fn historical_state_v43_command_v31_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v43_command_v31.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "d67aa11d97e31b59beb66ccdfd92ab21c518627cc6c3b090ae2e76562f64b0a7".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 31") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 43") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v42_command_v30_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v42_command_v30.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "5baf4c54d1ce6d31ad1a69937deade23f8b9efbc4bcbd1d09f8e2e6083b30380".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 30") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 42") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v41_command_v29_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v41_command_v29.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "c15df3148228ab0207428540380f205dc91193d7b864fae984131a8346a21d75".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 29") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 41") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v40_command_v28_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v40_command_v28.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "08e0ed85a6dd29121523384efdc612d825df838d632384eb8887a250a7c3fb4f".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 28") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 40") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v39_command_v27_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v39_command_v27.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "a82464c4e059837e3363b37a95d4f14acc7e4c1d8f018e406689b6098734e6ac".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 27") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 39") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v38_command_v26_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v38_command_v26.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "0baa2d75c98fcd134df188fed0bab6425f08a86d87d43692d86dbffe1829a45d".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 26") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 38") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn historical_state_v37_command_v25_restart_v5_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v37_command_v25.aggregate"
    );
    assert_eq!(
        (
            AGGREGATE.len(),
            raft_test_hex(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            2_196,
            "bb03ca7958a9206082281c2b0a5369eae025465ec527541159bf8b6d2a6d89c8".to_owned()
        )
    );

    let mut offset = 0;
    let mut artifact_count = 0;
    let mut command_rejections = 0;
    let mut state_rejections = 0;
    while offset < AGGREGATE.len() {
        let artifact_len = u32::from_be_bytes(
            AGGREGATE[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let artifact = &AGGREGATE[offset..offset + artifact_len];
        offset += artifact_len;
        let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(artifact).unwrap_err();
        let rendered = error.to_string();
        if rendered.contains("unsupported control-plane command version 25") {
            command_rejections += 1;
        } else if rendered.contains("unsupported control-plane state version 37") {
            state_rejections += 1;
        } else {
            panic!("unexpected historical restart artifact rejection: {error:?}");
        }
        artifact_count += 1;
    }
    assert_eq!(artifact_count, 4);
    assert!(command_rejections > 0);
    assert!(state_rejections > 0);
}

#[test]
fn control_plane_raft_restart_v5_rejects_noncurrent_nested_versions() {
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(7),
        membership: NodeMembershipState::Active,
    };
    let current_command = encode_control_plane_command(&command).unwrap();
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    let current_snapshot = state_machine.build_snapshot().unwrap().snapshot.into_inner();
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "restart-v5-nested-version-evidence".to_owned(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![
                bootstrap_membership_entry(1),
                normal_entry(3, 1, 1, command.clone()),
            ],
            ..Default::default()
        },
        state_machine: state_machine.export_restart_artifact(),
    };
    let encoded = artifact.encode_durable_artifact().unwrap();

    for version in [
        16, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36,
    ] {
        let previous =
            crate::control_plane_command::encode_control_plane_command_with_version_for_test(
                &command, version,
            )
            .unwrap();
        assert_eq!(current_command.len(), previous.len());
        let offsets = encoded
            .windows(current_command.len())
            .enumerate()
            .filter_map(|(offset, candidate)| {
                (candidate == current_command.as_slice()).then_some(offset)
            })
            .collect::<Vec<_>>();
        assert!(!offsets.is_empty(), "nested fixture must occur in restart v5");
        for offset in offsets {
            let mut unsupported = encoded.clone();
            unsupported[offset..offset + previous.len()].copy_from_slice(&previous);
            refresh_raft_restart_artifact_checksum(&mut unsupported);
            let error =
                ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unsupported).unwrap_err();
            assert!(
                error.to_string().contains(&format!(
                    "unsupported control-plane command version {version}"
                )),
                "unexpected nested restart error: {error:?}"
            );
        }
    }
    for version in [29, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48] {
        let previous =
            crate::control_plane_command::reseal_control_plane_snapshot_state_version_for_test(
                &current_snapshot,
                version,
            )
            .unwrap();
        assert_eq!(current_snapshot.len(), previous.len());
        let offsets = encoded
            .windows(current_snapshot.len())
            .enumerate()
            .filter_map(|(offset, candidate)| {
                (candidate == current_snapshot.as_slice()).then_some(offset)
            })
            .collect::<Vec<_>>();
        assert!(!offsets.is_empty(), "nested fixture must occur in restart v5");
        for offset in offsets {
            let mut unsupported = encoded.clone();
            unsupported[offset..offset + previous.len()].copy_from_slice(&previous);
            refresh_raft_restart_artifact_checksum(&mut unsupported);
            let error =
                ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unsupported).unwrap_err();
            assert!(
                error.to_string().contains(&format!(
                    "unsupported control-plane state version {version}"
                )),
                "unexpected nested restart error: {error:?}"
            );
        }
    }
}

#[test]
fn control_plane_raft_durable_restart_artifact_file_round_trips() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane").join("raft.state");
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };

    artifact.store_durable_artifact(&path).unwrap();
    assert!(!durable_artifact_tmp_path(&path).exists());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(durable_artifact_sentinel_path(&path))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
        .expect("stored durable restart artifact should load");
    let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
    ControlPlaneRaftTypeConfig::run(async {
        assert_eq!(
            RaftLogStorage::read_committed(&mut loaded_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 1))
        );
    });
    assert_eq!(
        loaded_state_machine.last_applied(),
        Some(raft_log_id(3, 1, 1))
    );
}

#[test]
fn control_plane_raft_durable_restart_artifact_file_ignores_stale_temp_file() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let tmp_path = durable_artifact_tmp_path(&path);
    let committed_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };
    let stale_temp_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 1)),
            committed: Some(raft_log_id(5, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(5, 1, 1),
                blank_entry(5, 1, 2),
            ],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(5, 1, 2),
    };

    committed_artifact.store_durable_artifact(&path).unwrap();
    std::fs::write(
        &tmp_path,
        stale_temp_artifact.encode_durable_artifact().unwrap(),
    )
    .unwrap();

    let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
        .expect("stable durable restart artifact should load despite stale temp file");
    let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
    ControlPlaneRaftTypeConfig::run(async {
        assert_eq!(
            RaftLogStorage::read_committed(&mut loaded_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 1))
        );
    });
    assert_eq!(
        loaded_state_machine.last_applied(),
        Some(raft_log_id(3, 1, 1))
    );
}

#[test]
fn control_plane_raft_durable_restart_artifact_file_preserves_existing_on_temp_failure() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let tmp_path = durable_artifact_tmp_path(&path);
    let committed_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };
    let replacement_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 1)),
            committed: Some(raft_log_id(5, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(5, 1, 1),
                blank_entry(5, 1, 2),
            ],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(5, 1, 2),
    };

    committed_artifact.store_durable_artifact(&path).unwrap();
    std::fs::create_dir(&tmp_path).unwrap();
    assert_error_contains(
        replacement_artifact.store_durable_artifact(&path),
        "create control-plane OpenRaft durable restart artifact temp file",
    );

    let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
        .expect("previous durable restart artifact should remain after temp-file failure");
    let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
    ControlPlaneRaftTypeConfig::run(async {
        assert_eq!(
            RaftLogStorage::read_committed(&mut loaded_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 1))
        );
    });
    assert_eq!(
        loaded_state_machine.last_applied(),
        Some(raft_log_id(3, 1, 1))
    );
}

#[test]
fn control_plane_raft_durable_restart_artifact_store_rejects_inconsistent_pair_before_overwrite() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let tmp_path = durable_artifact_tmp_path(&path);
    let committed_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };
    let torn_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 2),
    };

    committed_artifact.store_durable_artifact(&path).unwrap();
    assert_error_contains(
        torn_artifact.store_durable_artifact(&path),
        "validate control-plane OpenRaft durable restart artifact",
    );
    assert!(!tmp_path.exists());

    let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
        .expect("previous durable restart artifact should remain after pair validation failure");
    let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
    ControlPlaneRaftTypeConfig::run(async {
        assert_eq!(
            RaftLogStorage::read_committed(&mut loaded_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 1))
        );
    });
    assert_eq!(
        loaded_state_machine.last_applied(),
        Some(raft_log_id(3, 1, 1))
    );
}

#[test]
fn control_plane_raft_durable_restart_artifact_capture_rejects_inconsistent_pair() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();

        let err =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .unwrap_err();
        assert!(err.to_string().contains("after committed restart gate"));
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_capture_allows_log_ahead_of_state_machine() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();

        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .expect("log-ahead restart artifact should be replayable");
        assert_eq!(
            artifact.state_machine.last_applied,
            Some(raft_log_id(3, 1, 1))
        );
        assert_eq!(artifact.log_store.committed, Some(raft_log_id(3, 1, 2)));

        let (mut restored_log_store, mut restored_state_machine) = artifact.restore().unwrap();
        assert_eq!(
            RaftLogStorage::read_committed(&mut restored_log_store)
                .await
                .unwrap(),
            Some(raft_log_id(3, 1, 2))
        );
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );
        restored_state_machine
            .apply_entry(blank_entry(3, 1, 2))
            .unwrap();
        assert_eq!(
            restored_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 2))
        );
    });
}

#[test]
fn control_plane_raft_durable_authority_applies_committed_restart_suffix() {
    let tmp = test_util::tempdir();
    let artifact_path = tmp.path().join("raft.state");
    let cluster_name = "control-plane-raft-committed-restart-suffix";
    let expected = ControlPlaneRaftRestartArtifact::
            store_single_node_committed_ahead_bootstrap_artifact_for_test(
                &artifact_path,
                cluster_name,
                1,
                vec![(NodeId::new(1), "node-1".to_owned())],
                vec![PgId::new(0)],
            )
            .expect("committed-ahead restart artifact should store");

    ControlPlaneRaftTypeConfig::run(async {
        let authority = ControlPlaneRaftAuthority::new_single_node_durable(
            cluster_name,
            1,
            &artifact_path,
        )
        .await
        .expect("durable authority should restore committed-ahead state");
        authority
            .wait_for_applied_index_at_least(
                2,
                Duration::from_secs(1),
                "restored authority applies committed restart suffix",
            )
            .await
            .expect("restored authority should apply committed restart suffix");
        assert_eq!(
            authority.current_control_plane_snapshot().await.unwrap(),
            expected
        );
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_public_durable_authority_applies_wal_only_committed_command() {
    let tmp = test_util::tempdir();
    let artifact_path = tmp.path().join("raft.state");
    let cluster_name = "control-plane-raft-wal-only-committed-command";

    ControlPlaneRaftTypeConfig::run(async {
        let authority = ControlPlaneRaftAuthority::new_single_node_durable(
            cluster_name,
            1,
            &artifact_path,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(1)
            .await
            .unwrap();
        authority
            .wait_for_current_leader(1, Duration::from_secs(1), "WAL-only command setup")
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            Duration::from_secs(1),
            "WAL-only command authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let bootstrap_command = ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![(NodeId::new(1), "node-1".to_owned())],
            pg_ids: vec![PgId::new(0)],
        };
        let startup_retry_deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("WAL-only command startup retry deadline should fit");
        let bootstrap = loop {
                match authority
                    .submit_control_plane_command(bootstrap_command.clone())
                    .await
                {
                    Err(ControlPlaneError::AuthorityNotServing) => {
                        let remaining = startup_retry_deadline
                            .saturating_duration_since(Instant::now());
                        if !remaining.is_zero() {
                            ControlPlaneRaftTypeConfig::sleep(
                                Duration::from_millis(10).min(remaining),
                            )
                            .await;
                        }
                        if Instant::now() >= startup_retry_deadline {
                            let status = authority.status().await;
                            panic!(
                                "WAL-only command authority did not finish startup convergence; last status: {status:?}"
                            );
                        }
                    }
                    result => break result,
                }
            }
            .unwrap();
        authority.store_durable_restart_artifact().await.unwrap();
        authority.shutdown().await.unwrap();

        let artifact =
            ControlPlaneRaftRestartArtifact::load_durable_artifact(&artifact_path).unwrap();
        let checkpoint_last_log_id = artifact
            .log_store
            .entries
            .last()
            .map(|entry| entry.log_id)
            .expect("checkpoint should retain its bootstrap log tip");
        assert_eq!(checkpoint_last_log_id, bootstrap.log_id());
        assert_ne!(
            artifact
                .state_machine
                .inner
                .snapshot()
                .node(NodeId::new(1))
                .map(|node| node.availability()),
            Some(NodeAvailabilityState::Unavailable)
        );

        let command_term = artifact
            .log_store
            .vote
            .expect("checkpoint should retain a vote")
            .leader_id
            .term
            .checked_add(1_000)
            .unwrap();
        let command_log_id = LogId::new(
            LeaderId {
                term: command_term,
                node_id: 1,
            },
            checkpoint_last_log_id.index().checked_add(1).unwrap(),
        );
        let wal = test_raft_wal_file(durable_artifact_wal_path(&artifact_path), cluster_name, 1);
        wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
            ControlPlaneRaftLeaderId,
        >::new_committed(
            command_term, 1
        )))
        .unwrap();
        wal.append_record(&ControlPlaneRaftWalRecord::Append(vec![
            ControlPlaneRaftEntry {
                log_id: command_log_id,
                payload: EntryPayload::Normal(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1),
                    availability: NodeAvailabilityState::Unavailable,
                }),
            },
        ]))
        .unwrap();
        wal.append_record(&ControlPlaneRaftWalRecord::SaveCommitted(Some(
            command_log_id,
        )))
        .unwrap();

        let restarted = ControlPlaneRaftAuthority::new_single_node_durable(
            cluster_name,
            1,
            &artifact_path,
        )
        .await
        .expect("public durable authority should restore the derived WAL");
        restarted
            .wait_for_applied_log_id(
                command_log_id,
                Duration::from_secs(1),
                "public durable authority applies WAL-only committed command",
            )
            .await
            .unwrap();
        assert_eq!(
            restarted
                .current_control_plane_snapshot()
                .await
                .unwrap()
                .node(NodeId::new(1))
                .map(|node| node.availability()),
            Some(NodeAvailabilityState::Unavailable)
        );
        restarted.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_file_rejects_corruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };
    let mut encoded = artifact.encode_durable_artifact().unwrap();
    encoded[CONTROL_PLANE_RAFT_RESTART_MAGIC.len() + 2] ^= 1;
    std::fs::write(&path, encoded).unwrap();

    assert_error_contains(
        ControlPlaneRaftRestartArtifact::load_durable_artifact(&path),
        "checksum mismatch",
    );
}

#[test]
fn control_plane_raft_startup_rejects_semantically_inconsistent_artifact() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let sentinel_path = durable_artifact_sentinel_path(&path);
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 1)),
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(4, 1, 1),
    };
    std::fs::write(&path, artifact.encode_durable_artifact().unwrap()).unwrap();
    ControlPlaneRaftRestartSentinel {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
    }
    .store_durable_sentinel(&sentinel_path, None)
    .unwrap();

    assert_error_contains(
        restore_raft_durable_artifact("test-cluster", 1, &path, None, |_| Ok(())),
        "does not match log-store log id",
    );
}

#[test]
fn control_plane_raft_durable_restart_sentinel_round_trips_with_artifact() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let sentinel_path = durable_artifact_sentinel_path(&path);
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };

    artifact.store_durable_artifact(&path).unwrap();

    let sentinel = ControlPlaneRaftRestartSentinel::load_durable_sentinel(&sentinel_path)
        .expect("durable sentinel should be written with artifact");
    assert_eq!(
        sentinel,
        ControlPlaneRaftRestartSentinel {
            cluster_name: "test-cluster".to_string(),
            local_node_id: 1,
        }
    );
    let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
        .expect("stored durable restart artifact should load");
    assert_eq!(loaded.cluster_name, "test-cluster");
    assert_eq!(loaded.local_node_id, 1);
}

#[test]
fn control_plane_raft_durable_restart_sentinel_v1_bytes_are_stable() {
    let sentinel = ControlPlaneRaftRestartSentinel {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
    };
    let expected = [
        0x41, 0x52, 0x47, 0x4d, 0x49, 0x4e, 0x43, 0x50, 0x52, 0x41, 0x46, 0x54, 0x53, 0x45,
        0x45, 0x4e, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x63,
        0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x54, 0xc7, 0x3b, 0x23, 0x85, 0x5a, 0xd7, 0x89,
    ];

    assert_eq!(sentinel.encode_durable_sentinel().unwrap(), expected);
    assert_eq!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&expected),
        Ok(sentinel)
    );
}

#[test]
fn control_plane_raft_durable_restart_sentinel_rejects_malformed_frames_exactly() {
    let sentinel = ControlPlaneRaftRestartSentinel {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
    };
    let encoded = sentinel.encode_durable_sentinel().unwrap();

    for truncated_len in [
        0,
        CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len() - 1,
        CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len(),
        CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len() + 1,
    ] {
        assert_eq!(
            ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(
                &encoded[..truncated_len]
            ),
            Err(ControlPlaneRaftRestartSentinelFormatError::Truncated)
        );
    }

    let mut checksum_mismatch = encoded.clone();
    *checksum_mismatch.last_mut().unwrap() ^= 1;
    assert!(matches!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&checksum_mismatch),
        Err(ControlPlaneRaftRestartSentinelFormatError::ChecksumMismatch { .. })
    ));

    let mut malformed_magic = encoded.clone();
    malformed_magic[0] ^= 1;
    refresh_raft_wal_frame_checksum(&mut malformed_magic);
    assert_eq!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&malformed_magic),
        Err(ControlPlaneRaftRestartSentinelFormatError::UnknownMagic)
    );

    for version in [0, CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION + 1] {
        let mut unsupported = encoded.clone();
        let version_offset = CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len();
        unsupported[version_offset..version_offset + 2]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_wal_frame_checksum(&mut unsupported);
        assert_eq!(
            ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&unsupported),
            Err(ControlPlaneRaftRestartSentinelFormatError::UnsupportedVersion(version))
        );
    }

    let mut truncated_payload = encoded.clone();
    let cluster_name_len_offset = CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len() + 2;
    truncated_payload[cluster_name_len_offset..cluster_name_len_offset + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    refresh_raft_wal_frame_checksum(&mut truncated_payload);
    assert_eq!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&truncated_payload),
        Err(ControlPlaneRaftRestartSentinelFormatError::Truncated)
    );

    let mut invalid_cluster_name = encoded.clone();
    invalid_cluster_name[cluster_name_len_offset + 4] = 0xff;
    refresh_raft_wal_frame_checksum(&mut invalid_cluster_name);
    assert_eq!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&invalid_cluster_name),
        Err(ControlPlaneRaftRestartSentinelFormatError::InvalidClusterName)
    );

    let mut trailing = encoded.clone();
    trailing.insert(trailing.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN, 0xff);
    refresh_raft_wal_frame_checksum(&mut trailing);
    assert_eq!(
        ControlPlaneRaftRestartSentinel::decode_durable_sentinel_classified(&trailing),
        Err(ControlPlaneRaftRestartSentinelFormatError::TrailingBytes)
    );
}

#[test]
fn control_plane_openraft_durable_startup_rejects_unsupported_sentinel_without_mutation() {
    ControlPlaneRaftTypeConfig::run(async {
        let cluster_name = "control-plane-raft-unsupported-sentinel-startup-test";
        for version in [0, CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION + 1] {
            for artifact_present in [false, true] {
                let tmp = test_util::tempdir();
                let path = tmp.path().join("raft.state");
                let sentinel_path = durable_artifact_sentinel_path(&path);
                let wal_path = durable_artifact_wal_path(&path);
                let artifact = ControlPlaneRaftRestartArtifact {
                    cluster_name: cluster_name.to_string(),
                    local_node_id: 1,
                    wal_replay_offset: 0,
                    log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
                    state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
                };

                if artifact_present {
                    artifact.store_durable_artifact(&path).unwrap();
                    std::fs::write(&wal_path, b"WAL must not be inspected or replaced").unwrap();
                } else {
                    ControlPlaneRaftRestartSentinel::for_artifact(&artifact)
                        .store_durable_sentinel(&sentinel_path, None)
                        .unwrap();
                }

                let mut sentinel_bytes = std::fs::read(&sentinel_path).unwrap();
                let version_offset = CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len();
                sentinel_bytes[version_offset..version_offset + 2]
                    .copy_from_slice(&version.to_be_bytes());
                refresh_raft_wal_frame_checksum(&mut sentinel_bytes);
                std::fs::write(&sentinel_path, &sentinel_bytes).unwrap();
                let artifact_bytes = artifact_present.then(|| std::fs::read(&path).unwrap());
                let wal_bytes = artifact_present.then(|| std::fs::read(&wal_path).unwrap());

                assert_error_contains(
                    ControlPlaneRaftAuthority::new_single_node_durable(
                        cluster_name,
                        1,
                        &path,
                    )
                    .await,
                    &format!(
                        "unsupported control-plane OpenRaft durable restart sentinel version {version}"
                    ),
                );

                assert_eq!(std::fs::read(&sentinel_path).unwrap(), sentinel_bytes);
                if let Some(artifact_bytes) = artifact_bytes {
                    assert_eq!(std::fs::read(&path).unwrap(), artifact_bytes);
                    assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes.unwrap());
                } else {
                    assert!(!path.exists());
                    assert!(!wal_path.exists());
                }
            }
        }
    });
}

#[test]
fn control_plane_openraft_durable_startup_rejects_unsupported_wal_file_without_mutation() {
    ControlPlaneRaftTypeConfig::run(async {
        let cluster_name = "control-plane-raft-unsupported-wal-file-startup-test";
        for version in [
            CONTROL_PLANE_RAFT_WAL_FILE_VERSION - 1,
            CONTROL_PLANE_RAFT_WAL_FILE_VERSION + 1,
        ] {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let sentinel_path = durable_artifact_sentinel_path(&path);
            let wal_path = durable_artifact_wal_path(&path);
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                local_node_id: 1,
                wal_replay_offset: 0,
                log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
                state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
            };
            artifact.store_durable_artifact(&path).unwrap();

            let wal = test_raft_wal_file(&wal_path, cluster_name, 1);
            wal.append_record(&ControlPlaneRaftWalRecord::Append(vec![
                bootstrap_membership_entry(1),
            ]))
            .unwrap();
            OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap()
                .write_all(&[0, 0, 0])
                .unwrap();

            let mut wal_bytes = std::fs::read(&wal_path).unwrap();
            let version_offset = CONTROL_PLANE_RAFT_WAL_FILE_MAGIC.len();
            wal_bytes[version_offset..version_offset + std::mem::size_of::<u16>()]
                .copy_from_slice(&version.to_be_bytes());
            let header_checksum_start = CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len()
                - std::mem::size_of::<u64>();
            let header_checksum = checksum::crc64::checksum(&wal_bytes[..header_checksum_start]);
            wal_bytes[header_checksum_start..CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len()]
                .copy_from_slice(&header_checksum.to_be_bytes());
            std::fs::write(&wal_path, &wal_bytes).unwrap();

            let artifact_bytes = std::fs::read(&path).unwrap();
            let sentinel_bytes = std::fs::read(&sentinel_path).unwrap();
            assert!(!durable_artifact_tmp_path(&path).exists());

            assert_error_contains(
                ControlPlaneRaftAuthority::new_single_node_durable(
                    cluster_name,
                    1,
                    &path,
                )
                .await,
                &format!(
                    "unsupported control-plane OpenRaft WAL file header version {version}"
                ),
            );

            assert_eq!(std::fs::read(&path).unwrap(), artifact_bytes);
            assert_eq!(std::fs::read(&sentinel_path).unwrap(), sentinel_bytes);
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert!(!durable_artifact_tmp_path(&path).exists());
        }
    });
}

#[test]
fn control_plane_openraft_durable_startup_rejects_unsupported_wal_frame_without_replay() {
    ControlPlaneRaftTypeConfig::run(async {
        let cluster_name = "control-plane-raft-unsupported-wal-frame-startup-test";
        for version in [0, CONTROL_PLANE_RAFT_WAL_VERSION + 1] {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let sentinel_path = durable_artifact_sentinel_path(&path);
            let wal_path = durable_artifact_wal_path(&path);
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                local_node_id: 1,
                wal_replay_offset: 0,
                log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
                state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
            };
            artifact.store_durable_artifact(&path).unwrap();

            let wal = test_raft_wal_file(&wal_path, cluster_name, 1);
            wal.append_record(&ControlPlaneRaftWalRecord::Append(vec![
                bootstrap_membership_entry(1),
            ]))
            .unwrap();
            wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
                ControlPlaneRaftLeaderId,
            >::new_committed(
                3, 1,
            )))
            .unwrap();
            OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap()
                .write_all(&[0, 0, 0])
                .unwrap();

            let mut wal_bytes = std::fs::read(&wal_path).unwrap();
            let first_frame_end = wal_file_frame_end(&wal_bytes, 0);
            let second_frame_end = wal_file_frame_end(&wal_bytes, first_frame_end);
            let second_frame_start = first_frame_end + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN;
            let version_offset = second_frame_start + CONTROL_PLANE_RAFT_WAL_MAGIC.len();
            wal_bytes[version_offset..version_offset + std::mem::size_of::<u16>()]
                .copy_from_slice(&version.to_be_bytes());
            let checksum_start = second_frame_end - CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN;
            let checksum = checksum::crc64::checksum(
                &wal_bytes[second_frame_start..checksum_start],
            );
            wal_bytes[checksum_start..second_frame_end]
                .copy_from_slice(&checksum.to_be_bytes());
            std::fs::write(&wal_path, &wal_bytes).unwrap();

            let artifact_bytes = std::fs::read(&path).unwrap();
            let sentinel_bytes = std::fs::read(&sentinel_path).unwrap();
            reset_control_plane_raft_wal_replay_attempts();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_single_node_durable(
                    cluster_name,
                    1,
                    &path,
                )
                .await,
                &format!("unsupported control-plane OpenRaft WAL frame version {version}"),
            );

            assert_eq!(control_plane_raft_wal_replay_attempts(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), artifact_bytes);
            assert_eq!(std::fs::read(&sentinel_path).unwrap(), sentinel_bytes);
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert!(!durable_artifact_tmp_path(&path).exists());
        }
    });
}

#[test]
fn control_plane_openraft_durable_startup_rejects_unsupported_restart_artifact_without_replay() {
    ControlPlaneRaftTypeConfig::run(async {
        let cluster_name = "control-plane-raft-unsupported-restart-artifact-startup-test";
        for version in [3, 4, CONTROL_PLANE_RAFT_RESTART_VERSION + 1] {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let sentinel_path = durable_artifact_sentinel_path(&path);
            let wal_path = durable_artifact_wal_path(&path);
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                local_node_id: 1,
                wal_replay_offset: 0,
                log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
                state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
            };
            artifact.store_durable_artifact(&path).unwrap();

            let wal = test_raft_wal_file(&wal_path, cluster_name, 1);
            wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
                ControlPlaneRaftLeaderId,
            >::new_committed(
                3, 1,
            )))
            .unwrap();
            OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap()
                .write_all(&[0, 0, 0])
                .unwrap();

            let mut artifact_bytes = std::fs::read(&path).unwrap();
            let version_offset = CONTROL_PLANE_RAFT_RESTART_MAGIC.len();
            artifact_bytes[version_offset..version_offset + std::mem::size_of::<u16>()]
                .copy_from_slice(&version.to_be_bytes());
            refresh_raft_restart_artifact_checksum(&mut artifact_bytes);
            std::fs::write(&path, &artifact_bytes).unwrap();
            let sentinel_bytes = std::fs::read(&sentinel_path).unwrap();
            let wal_bytes = std::fs::read(&wal_path).unwrap();
            reset_control_plane_raft_wal_replay_attempts();
            reset_control_plane_raft_restore_attempts();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_single_node_durable(
                    cluster_name,
                    1,
                    &path,
                )
                .await,
                &format!(
                    "unsupported control-plane OpenRaft durable restart artifact version {version}"
                ),
            );

            assert_eq!(control_plane_raft_wal_replay_attempts(), 0);
            assert_eq!(control_plane_raft_restore_attempts(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), artifact_bytes);
            assert_eq!(std::fs::read(&sentinel_path).unwrap(), sentinel_bytes);
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert!(!durable_artifact_tmp_path(&path).exists());
        }
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_store_rejects_mismatched_sentinel() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.state");
    let sentinel_path = durable_artifact_sentinel_path(&path);
    ControlPlaneRaftRestartSentinel {
        cluster_name: "old-cluster".to_string(),
        local_node_id: 1,
    }
    .store_durable_sentinel(&sentinel_path, None)
    .unwrap();
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "new-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };

    let metrics_before = observability::control_plane_raft_checkpoint_metrics_snapshot();
    assert_error_contains(
        artifact.store_durable_artifact(&path),
        "durable restart sentinel belongs to cluster \"old-cluster\"",
    );
    let metrics_after = observability::control_plane_raft_checkpoint_metrics_snapshot();
    assert!(metrics_after.store_total > metrics_before.store_total);
    assert!(metrics_after.store_error_total > metrics_before.store_error_total);
    assert!(!path.exists());
}

#[test]
fn control_plane_openraft_durable_single_node_starts_empty_without_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("missing").join("raft.state");
        let authority = ControlPlaneRaftAuthority::new_single_node_durable(
            "control-plane-raft-durable-empty-start-test",
            1,
            &path,
        )
        .await
        .unwrap();

        assert!(!authority.is_initialized().await.unwrap());
        let status = authority.status().await.unwrap();
        assert_eq!(status.applied(), None);
        assert_eq!(status.committed(), None);
        assert_eq!(status.persisted_vote(), None);
        assert!(!durable_artifact_sentinel_path(&path).exists());
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_missing_artifact_with_sentinel() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        ControlPlaneRaftRestartSentinel {
            cluster_name: "control-plane-raft-missing-artifact-sentinel-test".to_string(),
            local_node_id: 1,
        }
        .store_durable_sentinel(&durable_artifact_sentinel_path(&path), None)
        .unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(
                "control-plane-raft-missing-artifact-sentinel-test",
                1,
                &path,
            )
            .await,
            "is missing but sentinel",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_artifact_without_sentinel() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-artifact-without-sentinel-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        std::fs::write(&path, artifact.encode_durable_artifact().unwrap()).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await,
            "is missing for existing artifact",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_wrong_sentinel_identity() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-wrong-sentinel-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();
        ControlPlaneRaftRestartSentinel {
            cluster_name: cluster_name.to_string(),
            local_node_id: 2,
        }
        .store_durable_sentinel(&durable_artifact_sentinel_path(&path), None)
        .unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await,
            "durable restart sentinel belongs to local OpenRaft node 2",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_restores_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-restore-test";

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
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(single_node_bootstrap_membership_entry(1))
            .unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();
        ControlPlaneRaftRestartArtifact::capture(cluster_name, 1, &log_store, &state_machine)
            .unwrap()
            .store_durable_artifact(&path)
            .unwrap();

        let authority =
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await
                .unwrap();
        assert!(authority.is_initialized().await.unwrap());
        authority
            .wait_for_applied_log_id(
                raft_log_id(3, 1, 3),
                Duration::from_secs(1),
                "durable single-node authority replayed committed suffix",
            )
            .await
            .unwrap();
        let status = authority.status().await.unwrap();
        assert_eq!(status.applied(), Some(raft_log_id(3, 1, 3)));
        assert_eq!(status.committed(), Some(raft_log_id(3, 1, 3)));
        assert_eq!(
            status.applied_membership_log_id(),
            Some(raft_log_id(3, 1, 2))
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_wrong_cluster_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "old-cluster".to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(
                "new-cluster",
                1,
                &path,
            )
            .await,
            "belongs to cluster \"old-cluster\", not configured cluster \"new-cluster\"",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_restores_current_snapshot_cache() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-current-snapshot-cache-test";
        let bootstrap_entry = single_node_bootstrap_membership_entry(1);
        let bootstrap_command = normal_entry(
            3,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                pg_ids: vec![PgId::new(1)],
            },
        );

        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![bootstrap_entry.clone(), bootstrap_command.clone()],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_entry).unwrap();
        state_machine.apply_entry(bootstrap_command).unwrap();
        let built_snapshot = state_machine.build_snapshot().unwrap();
        assert_eq!(built_snapshot.meta.last_log_id, Some(raft_log_id(3, 1, 1)));
        ControlPlaneRaftRestartArtifact::capture(cluster_name, 1, &log_store, &state_machine)
            .unwrap()
            .store_durable_artifact(&path)
            .unwrap();

        let authority =
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await
                .unwrap();

        let status = authority.status().await.unwrap();
        assert_eq!(status.applied(), Some(raft_log_id(3, 1, 1)));
        assert_eq!(status.committed(), Some(raft_log_id(3, 1, 1)));
        assert_eq!(status.current_snapshot(), Some(raft_log_id(3, 1, 1)));
        let restored_snapshot = authority.raft().get_snapshot().await.unwrap().unwrap();
        assert_eq!(
            restored_snapshot.meta.last_log_id,
            Some(raft_log_id(3, 1, 1))
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_durable_single_node_refreshes_cached_snapshot_after_purge() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-snapshot-suffix-replay-test";
        let bootstrap_entry = single_node_bootstrap_membership_entry(1);
        let bootstrap_command = normal_entry(
            3,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                pg_ids: vec![PgId::new(1)],
            },
        );
        let suffix_entry_2 = blank_entry(3, 1, 2);
        let suffix_entry_3 = blank_entry(3, 1, 3);

        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![
                bootstrap_entry.clone(),
                bootstrap_command.clone(),
                suffix_entry_2.clone(),
                suffix_entry_3.clone(),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut log_store, raft_log_id(3, 1, 1))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_entry).unwrap();
        state_machine.apply_entry(bootstrap_command).unwrap();
        let built_snapshot = state_machine.build_snapshot().unwrap();
        assert_eq!(built_snapshot.meta.last_log_id, Some(raft_log_id(3, 1, 1)));
        state_machine.apply_entry(suffix_entry_2).unwrap();
        state_machine.apply_entry(suffix_entry_3).unwrap();
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 1, 3)));

        ControlPlaneRaftRestartArtifact::capture(cluster_name, 1, &log_store, &state_machine)
            .unwrap()
            .store_durable_artifact(&path)
            .unwrap();

        let authority =
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await
                .unwrap();
        authority
            .wait_for_applied_log_id(
                raft_log_id(3, 1, 3),
                Duration::from_secs(1),
                "durable single-node authority restored refreshed snapshot",
            )
            .await
            .unwrap();

        let status = authority.status().await.unwrap();
        assert_eq!(status.last_purged_log_id(), Some(raft_log_id(3, 1, 1)));
        assert_eq!(status.current_snapshot(), Some(raft_log_id(3, 1, 3)));
        assert_eq!(status.committed(), Some(raft_log_id(3, 1, 3)));
        assert_eq!(status.applied(), Some(raft_log_id(3, 1, 3)));
        assert_eq!(status.committed_to_applied_index_gap(), Some(0));
        assert!(status.applied_caught_up_to_committed());

        let retained_entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..4)
            .await
            .unwrap();
        assert_eq!(
            retained_entries
                .iter()
                .map(|entry| entry.log_id)
                .collect::<Vec<_>>(),
            vec![raft_log_id(3, 1, 2), raft_log_id(3, 1, 3)]
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_multi_voter_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-multi-voter-start-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![
                    single_node_bootstrap_membership_entry(1),
                    membership_entry(3, 1, 1),
                ],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachineRestartArtifact {
                inner: replicated_state_machine_with_noops(3, 1),
                last_applied: Some(raft_log_id(3, 1, 1)),
                last_membership: StoredMembership::new(
                    Some(raft_log_id(3, 1, 1)),
                    test_membership(),
                ),
                current_snapshot: None,
            },
        };
        artifact.store_durable_artifact(&path).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await,
            "must be single-node membership for local node 1",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_unpositioned_multi_voter_membership() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-unpositioned-multi-voter-start-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![
                    single_node_bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                ],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachineRestartArtifact {
                inner: replicated_state_machine_with_noops(3, 1),
                last_applied: Some(raft_log_id(3, 1, 1)),
                last_membership: StoredMembership::new(None, test_membership()),
                current_snapshot: None,
            },
        };
        artifact.store_durable_artifact(&path).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 1, &path)
                .await,
            "without log id must be empty uninitialized membership",
        );
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_wrong_node_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-wrong-node-start-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![
                    single_node_bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        artifact.store_durable_artifact(&path).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(cluster_name, 2, &path)
                .await,
            "belongs to local OpenRaft node 1, not configured local node 2",
        );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_starts_empty_without_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("missing").join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-durable-empty-start-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );

        let authority = ControlPlaneRaftAuthority::new_unix_peer_durable(
            cluster_name,
            1,
            &path,
            policy,
            Duration::from_millis(50),
        )
        .await
        .unwrap();

        assert!(!authority.is_initialized().await.unwrap());
        let status = authority.status().await.unwrap();
        assert_eq!(status.node_id(), 1);
        assert_eq!(status.applied(), None);
        assert_eq!(status.committed(), None);
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_missing_artifact_with_sentinel() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-missing-artifact-sentinel-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        ControlPlaneRaftRestartSentinel {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
        }
        .store_durable_sentinel(&durable_artifact_sentinel_path(&path), None)
        .unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                1,
                &path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "is missing but sentinel",
        );
    });
}

#[test]
fn control_plane_openraft_static_restore_requires_certificate_before_raft_start() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let cluster_name = "control-plane-raft-static-certificate-restore-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_topology_identity(7, "ab".repeat(32));
        let bootstrap_nodes = vec![
            (NodeId::new(11), "node-11".to_string()),
            (NodeId::new(12), "node-12".to_string()),
        ];
        let bootstrap_pgs = vec![(crate::PgId::new(0), vec![NodeId::new(11)])];
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                7,
                [0xab; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1, 2],
                &bootstrap_nodes,
                &bootstrap_pgs,
                crate::control_plane::test_certified_storage_placement_policy(
                    [NodeId::new(11), NodeId::new(12)],
                    1,
                    1_000,
                ),
            )
            .unwrap();
        let policy = policy.with_initial_topology_certificate(topology.clone());
        let certified_bootstrap = ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: bootstrap_nodes,
            pg_acting_sets: bootstrap_pgs,
            topology,
        };

        let membership = policy_bootstrap_membership_entry(1, &policy);
        let bootstrap_entry = normal_entry(1, 1, 1, certified_bootstrap.clone());
        let acting_set_entry = normal_entry(
            1,
            1,
            2,
            ControlPlaneCommand::SetPgActingSet {
                pg_id: crate::PgId::new(0),
                acting_set: vec![NodeId::new(12)],
            },
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        for entry in [
            membership.clone(),
            bootstrap_entry.clone(),
            acting_set_entry.clone(),
        ] {
            state_machine.apply_entry(entry).unwrap();
        }
        let changed_path = tmp.path().join("changed.state");
        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(acting_set_entry.log_id),
                last_purged_log_id: None,
                entries: vec![membership.clone(), bootstrap_entry, acting_set_entry],
            },
            state_machine: state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(&changed_path)
        .unwrap();

        let authority = ControlPlaneRaftAuthority::new_unix_peer_durable(
            cluster_name,
            1,
            &changed_path,
            policy.clone(),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        assert_eq!(
            authority
                .current_control_plane_snapshot()
                .await
                .unwrap()
                .pg(crate::PgId::new(0))
                .unwrap()
                .acting_set(),
            &[NodeId::new(12)]
        );
        authority.shutdown().await.unwrap();

        let wrong_entry = normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                nodes: vec![
                    (NodeId::new(11), "wrong-node-11".to_string()),
                    (NodeId::new(12), "node-12".to_string()),
                ],
                pg_acting_sets: vec![(crate::PgId::new(0), vec![NodeId::new(11)])],
                topology:
                    crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                        7,
                        [0xab; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                        vec![1, 2],
                        &[
                            (NodeId::new(11), "wrong-node-11".to_string()),
                            (NodeId::new(12), "node-12".to_string()),
                        ],
                        &[(crate::PgId::new(0), vec![NodeId::new(11)])],
                        crate::control_plane::test_certified_storage_placement_policy(
                            [NodeId::new(11), NodeId::new(12)],
                            1,
                            1_000,
                        ),
                    )
                    .unwrap(),
            },
        );
        let mut wrong_state_machine = ControlPlaneRaftStateMachine::empty();
        wrong_state_machine.apply_entry(membership.clone()).unwrap();
        wrong_state_machine
            .apply_entry(wrong_entry.clone())
            .unwrap();
        let wrong_path = tmp.path().join("wrong-certificate.state");
        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(wrong_entry.log_id),
                last_purged_log_id: None,
                entries: vec![membership.clone(), wrong_entry],
            },
            state_machine: wrong_state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(&wrong_path)
        .unwrap();
        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                1,
                &wrong_path,
                policy.clone(),
                Duration::from_millis(50),
            )
            .await,
            "initial topology certificate does not match the configured certificate",
        );

        let missing_entry = normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(11), "node-11".to_string())],
                pg_ids: vec![crate::PgId::new(0)],
            },
        );
        let mut missing_state_machine = ControlPlaneRaftStateMachine::empty();
        missing_state_machine
            .apply_entry(membership.clone())
            .unwrap();
        missing_state_machine
            .apply_entry(missing_entry.clone())
            .unwrap();
        let missing_path = tmp.path().join("missing-certificate.state");
        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(missing_entry.log_id),
                last_purged_log_id: None,
                entries: vec![membership, missing_entry],
            },
            state_machine: missing_state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(&missing_path)
        .unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                1,
                &missing_path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "has no initial topology certificate",
        );
    });
}

#[test]
fn control_plane_openraft_static_pending_restore_accepts_only_expected_bootstrap() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("pending.state");
        let wal_path = tmp.path().join("pending.wal");
        let cluster_name = "control-plane-raft-static-pending-restore-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_topology_identity(7, "ab".repeat(32));
        let expected_nodes = vec![(NodeId::new(11), "node-11".to_string())];
        let expected_pgs = vec![(crate::PgId::new(0), vec![NodeId::new(11)])];
        let expected_topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                7,
                [0xab; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1, 2],
                &expected_nodes,
                &expected_pgs,
                crate::control_plane::test_certified_storage_placement_policy(
                    [NodeId::new(11)],
                    1,
                    1_000,
                ),
            )
            .unwrap();
        let policy = policy.with_initial_topology_certificate(expected_topology.clone());
        let expected = ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            topology: expected_topology,
            nodes: expected_nodes,
            pg_acting_sets: expected_pgs,
        };
        let membership = policy_bootstrap_membership_entry(1, &policy);
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(membership.clone()).unwrap();
        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(membership.log_id),
                last_purged_log_id: None,
                entries: vec![membership.clone()],
            },
            state_machine: state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(&path)
        .unwrap();

        let authority = ControlPlaneRaftAuthority::new_unix_peer_durable_with_wal_pending_static_initialization(
                cluster_name,
                1,
                &path,
                &wal_path,
                policy.clone(),
                expected.clone(),
                Duration::from_millis(50),
            )
            .await
            .unwrap();
        assert!(authority
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .initial_topology()
            .is_none());
        authority.shutdown().await.unwrap();

        let unexpected_entry = normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(11), "node-11".to_string())],
                pg_ids: vec![crate::PgId::new(0)],
            },
        );
        let mut unexpected_state_machine = ControlPlaneRaftStateMachine::empty();
        unexpected_state_machine
            .apply_entry(membership.clone())
            .unwrap();
        let unexpected_path = tmp.path().join("unexpected.state");
        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(membership.log_id),
                last_purged_log_id: None,
                entries: vec![membership, unexpected_entry],
            },
            state_machine: unexpected_state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(&unexpected_path)
        .unwrap();
        assert_error_contains(
                ControlPlaneRaftAuthority::new_unix_peer_durable_with_wal_pending_static_initialization(
                    cluster_name,
                    1,
                    &unexpected_path,
                    &tmp.path().join("unexpected.wal"),
                    policy,
                    expected,
                    Duration::from_millis(50),
                )
                .await,
                "normal entry other than the configured certified bootstrap",
            );

        let policy_without_topology = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        assert_error_contains(
                ControlPlaneRaftAuthority::new_unix_peer_durable_with_wal_pending_static_initialization(
                    cluster_name,
                    1,
                    &tmp.path().join("fresh.state"),
                    &tmp.path().join("fresh.wal"),
                    policy_without_topology,
                    ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                        nodes: vec![(NodeId::new(11), "node-11".to_string())],
                        pg_acting_sets: vec![(crate::PgId::new(0), vec![NodeId::new(11)])],
                        topology: crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                            7,
                            [0xab; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                            vec![1, 2],
                            &[(NodeId::new(11), "node-11".to_string())],
                            &[(crate::PgId::new(0), vec![NodeId::new(11)])],
                            crate::control_plane::test_certified_storage_placement_policy(
                                [NodeId::new(11)],
                                1,
                                1_000,
                            ),
                        )
                        .unwrap(),
                    },
                    Duration::from_millis(50),
                )
                .await,
                "pending static initialization requires a peer-policy topology identity",
            );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_wrong_local_node_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-durable-wrong-node-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                2,
                &path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "belongs to local OpenRaft node 1, not configured local node 2",
        );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_retained_membership_mismatch() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-durable-retained-membership-mismatch-test";
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![single_node_bootstrap_membership_entry(1)],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                1,
                &path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "retained log entry does not match configured peer map",
        );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_wal_membership_mismatch() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let wal_path = tmp.path().join("raft.wal");
        let cluster_name = "control-plane-raft-unix-peer-durable-wal-membership-mismatch-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![policy_bootstrap_membership_entry(1, &policy)],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();

        let wal = test_raft_wal_file(&wal_path, cluster_name, 1);
        wal.append_record(&ControlPlaneRaftWalRecord::Append(vec![
            single_node_membership_entry(3, 1, 1),
        ]))
        .expect("WAL-only membership suffix should persist");

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable_with_wal(
                cluster_name,
                1,
                &path,
                &wal_path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "retained log entry does not match configured peer map",
        );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_applied_membership_mismatch() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-durable-applied-membership-mismatch-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(single_node_bootstrap_membership_entry(1))
            .unwrap();
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![policy_bootstrap_membership_entry(1, &policy)],
                ..Default::default()
            },
            state_machine: state_machine.export_restart_artifact(),
        };
        artifact.store_durable_artifact(&path).unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_unix_peer_durable(
                cluster_name,
                1,
                &path,
                policy,
                Duration::from_millis(50),
            )
            .await,
            "state-machine membership does not match configured peer map",
        );
    });
}

#[test]
fn control_plane_openraft_unix_peer_durable_rejects_static_peer_reconfiguration() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("missing").join("raft.state");
        let cluster_name = "control-plane-raft-unix-peer-durable-static-reconfiguration-test";
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name,
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );

        let authority = ControlPlaneRaftAuthority::new_unix_peer_durable(
            cluster_name,
            1,
            &path,
            policy,
            Duration::from_millis(50),
        )
        .await
        .unwrap();

        assert_error_contains(
            authority.replace_voters(BTreeSet::from([1]), false).await,
            "change-membership is not supported for static configured peer policy",
        );
        assert_error_contains(
            authority
                .add_learner(3, BasicNode::new("/tmp/argmin-raft-node-3.sock"), false)
                .await,
            "add-learner is not supported for static configured peer policy",
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_openraft_durable_single_node_rejects_corrupt_artifact() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        std::fs::write(&path, b"not a durable artifact").unwrap();

        assert_error_contains(
            ControlPlaneRaftAuthority::new_single_node_durable(
                "control-plane-raft-durable-corrupt-start-test",
                1,
                &path,
            )
            .await,
            "checksum mismatch",
        );
    });
}

#[test]
fn control_plane_raft_durable_restart_artifact_codec_rejects_malformed_frames() {
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };
    assert!(matches!(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
            b"short"
        ),
        Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
            ControlPlaneRaftRestartArtifactFormatError::Truncated
        ))
    ));

    let encoded = artifact.encode_durable_artifact().unwrap();
    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 1;
    refresh_raft_restart_artifact_checksum(&mut bad_magic);
    assert!(matches!(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
            &bad_magic
        ),
        Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
            ControlPlaneRaftRestartArtifactFormatError::UnknownMagic
        ))
    ));

    for version in [3, 4, CONTROL_PLANE_RAFT_RESTART_VERSION + 1] {
        let mut unsupported_version = encoded.clone();
        let version_offset = CONTROL_PLANE_RAFT_RESTART_MAGIC.len();
        unsupported_version[version_offset..version_offset + std::mem::size_of::<u16>()]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_restart_artifact_checksum(&mut unsupported_version);
        assert!(matches!(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
                &unsupported_version
            ),
            Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::UnsupportedVersion(candidate)
            )) if candidate == version
        ));
    }

    let mut bad_checksum = encoded.clone();
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert!(matches!(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
            &bad_checksum
        ),
        Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
            ControlPlaneRaftRestartArtifactFormatError::ChecksumMismatch { .. }
        ))
    ));

    let mut truncated_payload = encoded.clone();
    truncated_payload.remove(
        truncated_payload.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN - 1,
    );
    refresh_raft_restart_artifact_checksum(&mut truncated_payload);
    assert!(matches!(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
            &truncated_payload
        ),
        Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
            ControlPlaneRaftRestartArtifactFormatError::Truncated
        ))
    ));

    let mut unknown_entry_tag = Vec::new();
    unknown_entry_tag.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
    write_raft_u16(&mut unknown_entry_tag, CONTROL_PLANE_RAFT_RESTART_VERSION);
    write_raft_string(&mut unknown_entry_tag, "test-cluster").unwrap();
    write_raft_u64(&mut unknown_entry_tag, 1);
    write_raft_u64(&mut unknown_entry_tag, 0);
    write_raft_restart_option_vote(
        &mut unknown_entry_tag,
        ControlPlaneRaftRestartOptionalField::LogStoreVote,
        None,
    );
    write_raft_option_log_id(&mut unknown_entry_tag, None);
    write_raft_option_log_id(&mut unknown_entry_tag, None);
    write_raft_u32(&mut unknown_entry_tag, 1);
    write_raft_log_id(&mut unknown_entry_tag, raft_log_id(0, 1, 0));
    write_raft_u8(&mut unknown_entry_tag, 99);
    append_raft_artifact_checksum(&mut unknown_entry_tag);
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unknown_entry_tag),
        "unknown control-plane OpenRaft durable entry payload tag 99",
    );

    let index_zero_blank = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![blank_entry(0, 1, 0)],
            ..Default::default()
        },
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    }
    .encode_durable_artifact()
    .unwrap();
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&index_zero_blank),
        "log index 0 entry must be bootstrap membership",
    );

    let non_bootstrap_index_zero_membership = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![membership_entry(1, 1, 0)],
            ..Default::default()
        },
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    }
    .encode_durable_artifact()
    .unwrap();
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(
            &non_bootstrap_index_zero_membership,
        ),
        "log index 0 entry must be bootstrap membership",
    );
}

#[test]
fn control_plane_raft_durable_restart_artifact_decode_validates_restart_pair() {
    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
                blank_entry(3, 1, 3),
            ],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
    };
    let encoded = artifact.encode_durable_artifact().unwrap();

    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&encoded),
        "after committed restart gate",
    );
}

#[test]
fn control_plane_raft_combined_restart_rejects_inconsistent_artifacts() {
    let log_committed_through_two = ControlPlaneRaftLogStoreRestartArtifact {
        vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        committed: Some(raft_log_id(3, 1, 2)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
            blank_entry(3, 1, 3),
        ],
        ..Default::default()
    };
    let applied_after_committed = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: log_committed_through_two.clone(),
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
    };
    let err = applied_after_committed.restore().unwrap_err();
    assert!(err.to_string().contains("after committed restart gate"));

    let missing_committed_gate = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };
    let err = missing_committed_gate.restore().unwrap_err();
    assert!(err.to_string().contains("no committed restart gate"));

    let applied_unknown_to_log = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
    };
    let err = applied_unknown_to_log.restore().unwrap_err();
    assert!(err
        .to_string()
        .contains("is not retained or purged in the log store"));

    let state_behind_purged_boundary = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            last_purged_log_id: Some(raft_log_id(3, 1, 2)),
            entries: Vec::new(),
        },
        state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
    };
    let err = state_behind_purged_boundary.restore().unwrap_err();
    assert!(err.to_string().contains("behind purged boundary"));

    let bootstrap_entry = single_node_bootstrap_membership_entry(1);
    let bootstrap_command = normal_entry(
        3,
        1,
        1,
        ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
            pg_ids: vec![PgId::new(1)],
        },
    );
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(bootstrap_entry.clone()).unwrap();
    state_machine
        .apply_entry(bootstrap_command.clone())
        .unwrap();
    state_machine.build_snapshot().unwrap();
    state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();
    let mut artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            last_purged_log_id: Some(raft_log_id(3, 1, 1)),
            entries: vec![normal_entry(
                3,
                1,
                2,
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1),
                    availability: NodeAvailabilityState::Healthy,
                },
            )],
        },
        state_machine: state_machine.export_restart_artifact(),
    };
    assert_eq!(
        artifact
            .state_machine
            .current_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.meta.last_log_id),
        Some(raft_log_id(3, 1, 1))
    );
    let err = artifact.clone().restore().unwrap_err();
    assert!(err
        .to_string()
        .contains("cached OpenRaft snapshot plus retained suffix"));
    artifact.log_store.entries = vec![blank_entry(3, 1, 2)];
    artifact.clone().restore().unwrap();
    artifact.log_store.entries = vec![blank_entry(3, 1, 2), membership_entry(3, 1, 3)];
    artifact.restore().unwrap();

    let mut membership_state_machine = ControlPlaneRaftStateMachine::empty();
    membership_state_machine
        .apply_entry(bootstrap_entry.clone())
        .unwrap();
    membership_state_machine
        .apply_entry(bootstrap_command.clone())
        .unwrap();
    membership_state_machine.build_snapshot().unwrap();
    membership_state_machine
        .apply_entry(membership_entry(3, 1, 2))
        .unwrap();
    let mut stale_snapshot_wrong_membership = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: 0,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            last_purged_log_id: Some(raft_log_id(3, 1, 1)),
            entries: vec![membership_entry(3, 1, 2)],
        },
        state_machine: membership_state_machine.export_restart_artifact(),
    };
    stale_snapshot_wrong_membership
        .state_machine
        .current_snapshot
        .as_mut()
        .unwrap()
        .meta
        .last_membership = StoredMembership::new(Some(raft_log_id(0, 1, 0)), test_membership());
    let err = stale_snapshot_wrong_membership.restore().unwrap_err();
    assert!(err
        .to_string()
        .contains("cached OpenRaft snapshot membership"));
}

#[test]
fn control_plane_raft_log_store_rejects_invalid_restart_artifacts() {
    let artifact_with_entry_at_purged_boundary = ControlPlaneRaftLogStoreRestartArtifact {
        last_purged_log_id: Some(raft_log_id(3, 1, 2)),
        entries: vec![blank_entry(3, 1, 2)],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(
        artifact_with_entry_at_purged_boundary,
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected 3"));

    let artifact_with_log_hole = ControlPlaneRaftLogStoreRestartArtifact {
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 3),
        ],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact_with_log_hole)
        .unwrap_err();
    assert!(err.to_string().contains("log hole at index 2"));

    let artifact_with_future_committed = ControlPlaneRaftLogStoreRestartArtifact {
        committed: Some(raft_log_id(3, 1, 3)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
        ],
        ..Default::default()
    };
    let err =
        ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact_with_future_committed)
            .unwrap_err();
    assert!(err.to_string().contains("current last log id"));

    let artifact_with_mismatched_committed = ControlPlaneRaftLogStoreRestartArtifact {
        committed: Some(raft_log_id(4, 1, 2)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
        ],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(
        artifact_with_mismatched_committed,
    )
    .unwrap_err();
    assert!(err.to_string().contains("mismatched log id"));

    let artifact_with_committed_before_purge = ControlPlaneRaftLogStoreRestartArtifact {
        committed: Some(raft_log_id(3, 1, 1)),
        last_purged_log_id: Some(raft_log_id(3, 1, 2)),
        entries: vec![blank_entry(3, 1, 3)],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(
        artifact_with_committed_before_purge,
    )
    .unwrap_err();
    assert!(err.to_string().contains("before purged boundary"));

    let artifact_with_missing_vote = ControlPlaneRaftLogStoreRestartArtifact {
        committed: Some(raft_log_id(3, 1, 2)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
        ],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact_with_missing_vote)
        .unwrap_err();
    assert!(err.to_string().contains("missing vote state"));

    let artifact_with_stale_vote = ControlPlaneRaftLogStoreRestartArtifact {
        vote: Some(Vote::<ControlPlaneRaftLeaderId>::new(2, 99)),
        committed: Some(raft_log_id(3, 1, 2)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
        ],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(artifact_with_stale_vote)
        .unwrap_err();
    assert!(err.to_string().contains("does not cover"));

    let artifact_with_same_term_lower_node_vote = ControlPlaneRaftLogStoreRestartArtifact {
        vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0)),
        committed: Some(raft_log_id(3, 1, 2)),
        entries: vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            blank_entry(3, 1, 2),
        ],
        ..Default::default()
    };
    let err = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(
        artifact_with_same_term_lower_node_vote,
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not cover"));
}
