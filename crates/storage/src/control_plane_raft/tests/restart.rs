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
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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

        let restarted = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
        restore_experimental_raft_durable_artifact("test-cluster", 1, &path, None, |_| Ok(())),
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
fn control_plane_raft_durable_restart_sentinel_rejects_unsupported_versions() {
    let sentinel = ControlPlaneRaftRestartSentinel {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
    };
    for version in [0, CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION + 1] {
        let mut encoded = sentinel.encode_durable_sentinel().unwrap();
        let version_offset = CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len();
        encoded[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        refresh_raft_wal_frame_checksum(&mut encoded);
        assert_error_contains(
            ControlPlaneRaftRestartSentinel::decode_durable_sentinel(&encoded),
            &format!(
                "unsupported control-plane OpenRaft durable restart sentinel version {version}"
            ),
        );
    }
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
        let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
        assert_eq!(
            control_plane_raft_snapshot_id(restored_snapshot.meta.last_log_id),
            "control-plane-T3-N1-I1"
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 1, &path)
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(cluster_name, 2, &path)
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

        let authority = ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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

        let authority = ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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

        let authority = ControlPlaneRaftAuthority::new_experimental_unix_peer_durable_with_wal_pending_static_initialization(
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
                ControlPlaneRaftAuthority::new_experimental_unix_peer_durable_with_wal_pending_static_initialization(
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
                ControlPlaneRaftAuthority::new_experimental_unix_peer_durable_with_wal_pending_static_initialization(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable_with_wal(
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
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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

        let authority = ControlPlaneRaftAuthority::new_experimental_unix_peer_durable(
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
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(
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
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(b"short"),
        Err(ControlPlaneError::CommandDecode { .. })
    ));

    let encoded = artifact.encode_durable_artifact().unwrap();
    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 1;
    bad_magic.truncate(bad_magic.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN);
    append_raft_artifact_checksum(&mut bad_magic);
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&bad_magic),
        "invalid control-plane OpenRaft durable restart artifact magic",
    );

    let mut unsupported_version = Vec::new();
    unsupported_version.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
    write_raft_u16(
        &mut unsupported_version,
        CONTROL_PLANE_RAFT_RESTART_VERSION + 1,
    );
    append_raft_artifact_checksum(&mut unsupported_version);
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unsupported_version),
        "unsupported control-plane OpenRaft durable restart artifact version",
    );

    let mut truncated = encoded.clone();
    truncated.pop();
    assert_error_contains(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&truncated),
        "checksum mismatch",
    );

    let mut unknown_entry_tag = Vec::new();
    unknown_entry_tag.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
    write_raft_u16(&mut unknown_entry_tag, CONTROL_PLANE_RAFT_RESTART_VERSION);
    write_raft_string(&mut unknown_entry_tag, "test-cluster").unwrap();
    write_raft_u64(&mut unknown_entry_tag, 1);
    write_raft_u64(&mut unknown_entry_tag, 0);
    write_raft_option_vote(&mut unknown_entry_tag, None);
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
