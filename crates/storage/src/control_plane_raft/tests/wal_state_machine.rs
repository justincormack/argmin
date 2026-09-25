// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[test]
fn control_plane_raft_wal_partial_write_failure_is_ambiguous() {
    let mut writer = PartialFailWriter {
        fail_after: 2,
        written: Vec::new(),
    };
    let err = write_control_plane_raft_wal_bytes(
        &mut writer,
        b"abcdef",
        "write test control-plane OpenRaft WAL bytes",
    )
    .expect_err("partial WAL write failure should be ambiguous");
    assert!(
        matches!(
            err,
            ControlPlaneRaftWalAppendError::AmbiguousRecordMayExist(_)
        ),
        "partial WAL write failure should poison through the ambiguous append path: {err:?}"
    );
    assert_eq!(writer.written, b"ab");
}

#[test]
fn control_plane_raft_wal_frame_codec_round_trips_records() {
    let records = vec![
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        ControlPlaneRaftWalRecord::Append(vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
        ]),
        ControlPlaneRaftWalRecord::SaveCommitted(Some(raft_log_id(3, 1, 1))),
        ControlPlaneRaftWalRecord::TruncateAfter(Some(raft_log_id(3, 1, 1))),
        ControlPlaneRaftWalRecord::Purge(raft_log_id(3, 1, 1)),
    ];

    for record in records {
        let frame = ControlPlaneRaftWalFrame::new("test-cluster", 1, record.clone());
        let encoded = frame.encode_frame().expect("WAL frame should encode");
        let decoded =
            ControlPlaneRaftWalFrame::decode_frame(&encoded).expect("WAL frame should decode");
        assert_eq!(decoded.cluster_name(), "test-cluster");
        assert_eq!(decoded.local_node_id(), 1);
        assert_eq!(decoded.record(), &record);
        decoded
            .validate_identity("test-cluster", 1)
            .expect("WAL frame identity should match");
    }
}

#[test]
fn control_plane_raft_wal_v2_full_file_layout_is_exact() {
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let command_entry = normal_entry(
        3,
        1,
        2,
        ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(1),
            availability: NodeAvailabilityState::Unavailable,
        },
    );
    let records = vec![
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        ControlPlaneRaftWalRecord::Append(vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
            command_entry,
        ]),
        ControlPlaneRaftWalRecord::SaveCommitted(None),
        ControlPlaneRaftWalRecord::SaveCommitted(Some(raft_log_id(3, 1, 1))),
        ControlPlaneRaftWalRecord::TruncateAfter(None),
        ControlPlaneRaftWalRecord::TruncateAfter(Some(raft_log_id(3, 1, 1))),
        ControlPlaneRaftWalRecord::Purge(raft_log_id(3, 1, 1)),
    ];
    assert_eq!(
        records
            .iter()
            .map(ControlPlaneRaftWalRecord::kind)
            .collect::<BTreeSet<_>>(),
        ControlPlaneRaftWalRecordKind::ALL
            .iter()
            .copied()
            .collect()
    );
    let append_entry_kinds = records
        .iter()
        .filter_map(|record| match record {
            ControlPlaneRaftWalRecord::Append(entries) => Some(entries),
            _ => None,
        })
        .flatten()
        .map(control_plane_raft_entry_payload_tag)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        append_entry_kinds,
        ControlPlaneRaftEntryPayloadTag::ALL.into_iter().collect()
    );
    let save_committed_arms = records
        .iter()
        .filter_map(|record| match record {
            ControlPlaneRaftWalRecord::SaveCommitted(log_id) => Some(log_id.is_some()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let truncate_after_arms = records
        .iter()
        .filter_map(|record| match record {
            ControlPlaneRaftWalRecord::TruncateAfter(log_id) => Some(log_id.is_some()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(save_committed_arms, BTreeSet::from([false, true]));
    assert_eq!(truncate_after_arms, BTreeSet::from([false, true]));
    for record in &records {
        wal.append_record(record).unwrap();
    }

    let bytes = fs::read(&wal_path).unwrap();
    let (base_offset, header_len) = wal.journal.decode_file_header(&bytes).unwrap();
    assert_eq!(base_offset, 0);
    assert_eq!(header_len, CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len());
    let decoded = wal.read_records_from(0).unwrap();
    assert_eq!(decoded.records, records);
    assert!(!decoded.truncated_tail);
    assert_eq!(
        decoded.clean_len,
        u64::try_from(bytes.len() - header_len).unwrap()
    );
    assert_eq!(
        (
            bytes.len(),
            raft_test_hex(&checksum::sha256::digest(&bytes))
        ),
        (
            714,
            "1ef19cd3fcbd35de83e6f83d24903b67069ab4ffec35a5a45207e254405ed715".to_owned()
        )
    );
}

#[test]
fn control_plane_raft_wal_v2_frame_v1_file_remains_exact_and_rejected() {
    let bytes = raft_test_decode_hex(include_str!("raft_wal_v2_frame_v1_current.hex"));
    assert_eq!(
        (bytes.len(), raft_test_hex(&checksum::sha256::digest(&bytes))),
        (
            714,
            "f3dfc773aee8b2304ed3215cec1108730f4eb6cf17ba6246f562fcbe8e16e836"
                .to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, &bytes).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected previous WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), bytes);
}

#[test]
fn historical_command_v31_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v31.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "a3dac3cff01f9104cabc3f6076477f4fc300ff67d62aa6c6bc747a347e05f2ea".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v30_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v30.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "a1f903b6b79fcd844a06a422073e1f9e4329c95385bec1b40c92bbe0e93bd07b".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v29_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v29.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "752e0c3fa2002932f49d8e37c18697465daa733a40d00079b5615551fc8083bf".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v28_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v28.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "853ed8b6e97d27ad78f06878fb6df7267fae8dbe1f625a594d345acbdd8a7ab6".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v27_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v27.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "19cef45591207bf005dd5fa4460bbee0c74b6750eabbaa7aec0dc020102c90ff".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v26_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v26.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "9f198c2bebdd07874801af7640d70a16643bfc2de729a0867284937313a7d041".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn historical_command_v25_wal_v2_file_remains_rejected_evidence() {
    const BYTES: &[u8] =
        include_bytes!("../../control_plane/testdata/raft_wal_v2_command_v25.bin");
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            714,
            "8b4d1fb08e16fde0c350a65d3f9e77952c80f4e1971c22739fbbad54ed0474c6".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, BYTES).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(0).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected historical WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), BYTES);
}

#[test]
fn control_plane_raft_wal_v2_rejects_noncurrent_nested_command_versions() {
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(7),
        membership: NodeMembershipState::Active,
    };
    let current = encode_control_plane_command(&command).unwrap();
    let frame = ControlPlaneRaftWalFrame::new(
        "nested-command-version-wal",
        1,
        ControlPlaneRaftWalRecord::Append(vec![normal_entry(3, 1, 1, command.clone())]),
    )
    .encode_frame()
    .unwrap();
    let offsets = frame
        .windows(current.len())
        .enumerate()
        .filter_map(|(offset, candidate)| (candidate == current.as_slice()).then_some(offset))
        .collect::<Vec<_>>();
    assert_eq!(offsets.len(), 1);
    for version in [
        16, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 38,
    ] {
        let previous =
            crate::control_plane_command::encode_control_plane_command_with_version_for_test(
                &command, version,
            )
            .unwrap();
        assert_eq!(previous.len(), current.len());
        let mut unsupported = frame.clone();
        let offset = offsets[0];
        unsupported[offset..offset + previous.len()].copy_from_slice(&previous);
        refresh_raft_wal_frame_checksum(&mut unsupported);

        let error = ControlPlaneRaftWalFrame::decode_frame(&unsupported).unwrap_err();
        assert!(matches!(
            error,
            ControlPlaneError::CommandDecode { message }
                if message == format!("unsupported control-plane command version {version}")
        ));
    }
}

#[test]
fn control_plane_raft_wal_file_header_failures_are_typed() {
    use crate::durable_journal::DurableJournalFileHeaderFormatError;

    let tmp = test_util::tempdir();
    let wal = test_raft_wal_file(tmp.path().join("raft.wal"), "test-cluster", 1);
    let header = wal.journal.encode_file_header(0x0102_0304_0506_0708);
    assert!(matches!(
        wal.journal.decode_file_header_classified(&header[..3]),
        Err(DurableJournalFileHeaderFormatError::Truncated)
    ));

    let mut bad_magic = header.clone();
    bad_magic[0] ^= 0xff;
    refresh_raft_wal_frame_checksum(&mut bad_magic);
    assert!(matches!(
        wal.journal.decode_file_header_classified(&bad_magic),
        Err(DurableJournalFileHeaderFormatError::UnknownMagic)
    ));

    let mut bad_checksum = header.clone();
    *bad_checksum.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        wal.journal.decode_file_header_classified(&bad_checksum),
        Err(DurableJournalFileHeaderFormatError::ChecksumMismatch { .. })
    ));

    for version in [
        CONTROL_PLANE_RAFT_WAL_FILE_VERSION - 1,
        CONTROL_PLANE_RAFT_WAL_FILE_VERSION + 1,
    ] {
        let mut unsupported = header.clone();
        let version_offset = CONTROL_PLANE_RAFT_WAL_FILE_MAGIC.len();
        unsupported[version_offset..version_offset + 2]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_wal_frame_checksum(&mut unsupported);
        assert_eq!(
            wal.journal
                .decode_file_header_classified(&unsupported)
                .unwrap_err(),
            DurableJournalFileHeaderFormatError::UnsupportedVersion(version)
        );
    }
}

#[test]
fn control_plane_raft_wal_frame_rejects_malformed_frames() {
    assert!(matches!(
        ControlPlaneRaftWalFrame::decode_frame_classified(b"short"),
        Err(ControlPlaneRaftWalFrameDecodeError::Format(
            ControlPlaneRaftWalFrameFormatError::Truncated
        ))
    ));

    let frame = ControlPlaneRaftWalFrame::new(
        "test-cluster",
        1,
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
    );
    let encoded = frame.encode_frame().unwrap();

    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 0xff;
    refresh_raft_wal_frame_checksum(&mut bad_magic);
    assert!(matches!(
        ControlPlaneRaftWalFrame::decode_frame_classified(&bad_magic),
        Err(ControlPlaneRaftWalFrameDecodeError::Format(
            ControlPlaneRaftWalFrameFormatError::UnknownMagic
        ))
    ));

    for version in [0, CONTROL_PLANE_RAFT_WAL_VERSION + 1] {
        let mut unsupported_version = encoded.clone();
        let version_offset = CONTROL_PLANE_RAFT_WAL_MAGIC.len();
        unsupported_version[version_offset..version_offset + std::mem::size_of::<u16>()]
            .copy_from_slice(&version.to_be_bytes());
        refresh_raft_wal_frame_checksum(&mut unsupported_version);
        assert!(matches!(
            ControlPlaneRaftWalFrame::decode_frame_classified(&unsupported_version),
            Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::UnsupportedVersion(candidate)
            )) if candidate == version
        ));
    }

    let mut bad_checksum = encoded.clone();
    let last = bad_checksum
        .last_mut()
        .expect("encoded WAL frame should include checksum");
    *last ^= 0xff;
    assert!(matches!(
        ControlPlaneRaftWalFrame::decode_frame_classified(&bad_checksum),
        Err(ControlPlaneRaftWalFrameDecodeError::Format(
            ControlPlaneRaftWalFrameFormatError::ChecksumMismatch { .. }
        ))
    ));

    let mut truncated_payload = encoded.clone();
    truncated_payload.remove(truncated_payload.len() - CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN - 1);
    refresh_raft_wal_frame_checksum(&mut truncated_payload);
    assert!(matches!(
        ControlPlaneRaftWalFrame::decode_frame_classified(&truncated_payload),
        Err(ControlPlaneRaftWalFrameDecodeError::Format(
            ControlPlaneRaftWalFrameFormatError::Truncated
        ))
    ));

    let mut maximum_cluster_name_length = encoded.clone();
    let cluster_name_length_offset =
        CONTROL_PLANE_RAFT_WAL_MAGIC.len() + std::mem::size_of::<u16>();
    maximum_cluster_name_length[cluster_name_length_offset
        ..cluster_name_length_offset + std::mem::size_of::<u32>()]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    refresh_raft_wal_frame_checksum(&mut maximum_cluster_name_length);
    assert!(matches!(
        ControlPlaneRaftWalFrame::decode_frame_classified(&maximum_cluster_name_length),
        Err(ControlPlaneRaftWalFrameDecodeError::Format(
            ControlPlaneRaftWalFrameFormatError::Truncated
        ))
    ));

    let mut trailing = encoded.clone();
    let checksum_start = trailing.len() - CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN;
    trailing.insert(checksum_start, 0);
    refresh_raft_wal_frame_checksum(&mut trailing);
    assert_error_contains(
        ControlPlaneRaftWalFrame::decode_frame(&trailing),
        "control-plane OpenRaft WAL frame has 1 trailing bytes",
    );

    let mut unknown_record = encoded;
    let record_tag_offset = CONTROL_PLANE_RAFT_WAL_MAGIC.len()
        + 2
        + 4
        + "test-cluster".len()
        + std::mem::size_of::<u64>();
    unknown_record[record_tag_offset] = 99;
    refresh_raft_wal_frame_checksum(&mut unknown_record);
    assert_error_contains(
        ControlPlaneRaftWalFrame::decode_frame(&unknown_record),
        "unknown control-plane OpenRaft WAL record tag",
    );
}

#[test]
fn control_plane_raft_wal_frame_validates_identity() {
    let frame = ControlPlaneRaftWalFrame::new(
        "test-cluster",
        1,
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
    );
    assert_error_contains(
        frame.validate_identity("other-cluster", 1),
        "control-plane OpenRaft WAL frame belongs to cluster",
    );
    assert_error_contains(
        frame.validate_identity("test-cluster", 2),
        "control-plane OpenRaft WAL frame belongs to local OpenRaft node",
    );
}

#[test]
fn control_plane_raft_wal_replay_matches_live_log_store_mutations() {
    ControlPlaneRaftTypeConfig::run(async {
        let records = vec![
            ControlPlaneRaftWalRecord::Append(vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
            ]),
            ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(
                3, 1,
            )),
            ControlPlaneRaftWalRecord::SaveCommitted(Some(raft_log_id(3, 1, 1))),
            ControlPlaneRaftWalRecord::Append(vec![blank_entry(3, 1, 2), blank_entry(3, 1, 3)]),
            ControlPlaneRaftWalRecord::SaveCommitted(Some(raft_log_id(3, 1, 3))),
            ControlPlaneRaftWalRecord::Purge(raft_log_id(3, 1, 2)),
            ControlPlaneRaftWalRecord::TruncateAfter(Some(raft_log_id(3, 1, 3))),
        ];

        let mut live = ControlPlaneRaftLogStore::empty();
        for record in &records {
            match record {
                ControlPlaneRaftWalRecord::SaveVote(vote) => {
                    RaftLogStorage::save_vote(&mut live, vote).await.unwrap();
                }
                ControlPlaneRaftWalRecord::Append(entries) => {
                    RaftLogStorage::append(&mut live, entries.clone(), IOFlushed::noop())
                        .await
                        .unwrap();
                }
                ControlPlaneRaftWalRecord::SaveCommitted(committed) => {
                    RaftLogStorage::save_committed(&mut live, *committed)
                        .await
                        .unwrap();
                }
                ControlPlaneRaftWalRecord::TruncateAfter(last_log_id) => {
                    RaftLogStorage::truncate_after(&mut live, *last_log_id)
                        .await
                        .unwrap();
                }
                ControlPlaneRaftWalRecord::Purge(log_id) => {
                    RaftLogStorage::purge(&mut live, *log_id).await.unwrap();
                }
            }
        }

        let replayed = ControlPlaneRaftLogStoreRestartArtifact::default()
            .replay_wal_records(&records)
            .expect("WAL replay should reconstruct log store");
        assert_eq!(replayed, live.export_restart_artifact().unwrap());
    });
}

#[test]
fn control_plane_raft_wal_replay_rejects_invalid_sequence() {
    let records = vec![ControlPlaneRaftWalRecord::Append(vec![blank_entry(
        3, 1, 1,
    )])];
    let err = ControlPlaneRaftLogStoreRestartArtifact::default()
        .replay_wal_records(&records)
        .expect_err("WAL replay should reject append holes");
    assert!(
        err.to_string()
            .contains("control-plane OpenRaft append starts at index 1, expected 0"),
        "unexpected WAL replay error: {err:?}"
    );
}

#[test]
fn control_plane_raft_wal_file_replays_records() {
    let tmp = test_util::tempdir();
    let wal = test_raft_wal_file(tmp.path().join("raft.wal"), "test-cluster", 1);
    let records = vec![
        ControlPlaneRaftWalRecord::Append(vec![
            bootstrap_membership_entry(1),
            blank_entry(3, 1, 1),
        ]),
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        ControlPlaneRaftWalRecord::SaveCommitted(Some(raft_log_id(3, 1, 1))),
    ];

    let metrics_before = observability::control_plane_raft_wal_metrics_snapshot();
    for record in &records {
        wal.append_record(record)
            .expect("WAL append should succeed");
    }
    let metrics_after = observability::control_plane_raft_wal_metrics_snapshot();
    let physical_record_bytes = fs::metadata(wal.path()).unwrap().len()
        - u64::try_from(CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len()).unwrap();
    assert!(
        metrics_after.append_total
            >= metrics_before
                .append_total
                .saturating_add(records.len() as u64)
    );
    assert!(
        metrics_after.frame_bytes_total
            >= metrics_before
                .frame_bytes_total
                .saturating_add(physical_record_bytes),
        "WAL frame-byte metrics should include every length-prefixed record"
    );
    assert!(
        metrics_after.file_sync_total
            >= metrics_before
                .file_sync_total
                .saturating_add(records.len() as u64)
    );
    assert!(
        metrics_after.directory_sync_total
            >= metrics_before
                .directory_sync_total
                .saturating_add(records.len() as u64)
    );

    let replayed = wal
        .replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
            replay_offset: 0,
        })
        .expect("WAL file replay should succeed");
    let expected = ControlPlaneRaftLogStoreRestartArtifact::default()
        .replay_wal_records(&records)
        .expect("in-memory WAL replay should succeed");
    assert_eq!(replayed, expected);
}

#[test]
fn control_plane_raft_restart_artifact_replays_wal_after_checkpoint_offset() {
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);

    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
        ControlPlaneRaftLeaderId,
    >::new_committed(
        3, 1
    )))
    .expect("pre-checkpoint WAL append should succeed");
    let checkpoint_offset = wal.clean_len().expect("WAL clean length should read");

    let artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: checkpoint_offset,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            ..Default::default()
        },
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };

    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
        ControlPlaneRaftLeaderId,
    >::new_committed(
        4, 1
    )))
    .expect("post-checkpoint WAL append should succeed");

    let (restored_log_store, restored_state_machine) = artifact
        .restore_with_wal_file(test_raft_wal_file(&wal_path, "test-cluster", 1))
        .expect("artifact plus post-checkpoint WAL should restore");
    assert_eq!(
        restored_log_store.persisted_vote().unwrap(),
        Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(4, 1))
    );
    assert_eq!(restored_state_machine.last_applied(), None);
}

#[test]
fn historical_state_v38_restart_compaction_artifact_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v38_compaction.bin"
    );
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            236,
            "bdba2f7fe0755c9dbb53d846a874b6037b7e2a829e9c6474e613218c0307d453".to_owned()
        )
    );
    let resealed = reseal_historical_restart_artifact_for_nested_evidence(BYTES);
    let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&resealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane state version 38"),
        "unexpected historical compaction artifact rejection: {error:?}"
    );
}

#[test]
fn historical_state_v40_restart_compaction_artifact_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v40_compaction.bin"
    );
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            236,
            "0f6decaba938357fe7eb70ea34c07f91cf3f219710f862034f44f80a04975175".to_owned()
        )
    );
    let resealed = reseal_historical_restart_artifact_for_nested_evidence(BYTES);
    let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&resealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane state version 40"),
        "unexpected historical compaction artifact rejection: {error:?}"
    );
}

#[test]
fn historical_state_v41_restart_compaction_artifact_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v41_compaction.bin"
    );
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            236,
            "be4df37cdc06eac3978a18a842e52135adf4c2bcb25e4fb5f701f261ecc4ba1c".to_owned()
        )
    );
    let resealed = reseal_historical_restart_artifact_for_nested_evidence(BYTES);
    let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&resealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane state version 41"),
        "unexpected historical compaction artifact rejection: {error:?}"
    );
}

#[test]
fn historical_state_v39_restart_compaction_artifact_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v39_compaction.bin"
    );
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            236,
            "0afd29ed98527f5a9a6c872f65d4315afed97c3a94e9f5640925db1adbd98b64".to_owned()
        )
    );
    let resealed = reseal_historical_restart_artifact_for_nested_evidence(BYTES);
    let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&resealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane state version 39"),
        "unexpected historical compaction artifact rejection: {error:?}"
    );
}

#[test]
fn historical_state_v37_restart_compaction_artifact_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_state_v37_compaction.bin"
    );
    assert_eq!(
        (BYTES.len(), raft_test_hex(&checksum::sha256::digest(BYTES))),
        (
            236,
            "2d47f6bad67257801f48529cea7ed3ea6cce4f3f9a4e6b5c1769a696ea1d9f8d".to_owned()
        )
    );
    let resealed = reseal_historical_restart_artifact_for_nested_evidence(BYTES);
    let error = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&resealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane state version 37"),
        "unexpected historical compaction artifact rejection: {error:?}"
    );
}

#[test]
fn previous_openraft_checkpoint_pair_remains_exact_and_rejected() {
    let artifact = raft_test_decode_hex(include_str!("restart_v5_checkpoint_current.hex"));
    assert_eq!(
        (
            artifact.len(),
            raft_test_hex(&checksum::sha256::digest(&artifact))
        ),
        (
            236,
            "85c3792be2c0d22886bd4774eb8ba55538e8951a229ce70e4a0473090a47a13d"
                .to_owned()
        )
    );
    assert!(matches!(
        ControlPlaneRaftRestartArtifact::decode_durable_artifact_before_restore_validation_classified(
            &artifact
        ),
        Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
            ControlPlaneRaftRestartArtifactFormatError::UnsupportedVersion(5)
        ))
    ));

    let wal_bytes = raft_test_decode_hex(include_str!("wal_v2_frame_v1_compacted_current.hex"));
    assert_eq!(
        (
            wal_bytes.len(),
            raft_test_hex(&checksum::sha256::digest(&wal_bytes))
        ),
        (
            112,
            "8b4fb9ff0d05a667fe24a461aaf2f731cdc9b9933ae81a288372bf4ada1b3d62"
                .to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    fs::write(&wal_path, &wal_bytes).unwrap();
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
    let error = wal.read_records_from(75).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported control-plane OpenRaft WAL frame version 1"),
        "unexpected previous compacted WAL rejection: {error:?}"
    );
    assert_eq!(fs::read(&wal_path).unwrap(), wal_bytes);
}

#[test]
fn control_plane_raft_wal_compaction_preserves_checkpoint_suffix() {
    const HISTORICAL_STATE_V43_CHECKPOINT: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_checkpoint_state_v43.bin"
    );
    assert_eq!(
        (
            HISTORICAL_STATE_V43_CHECKPOINT.len(),
            raft_test_hex(&checksum::sha256::digest(HISTORICAL_STATE_V43_CHECKPOINT))
        ),
        (
            236,
            "8ccaea1b423cdeb0876b048301d15e985af7f6aaf5ff9d55cfd106161cd9e37b".to_owned()
        )
    );
    let historical = reseal_historical_restart_artifact_for_nested_evidence(
        HISTORICAL_STATE_V43_CHECKPOINT,
    );
    let historical_error =
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&historical).unwrap_err();
    assert!(
        historical_error
            .to_string()
            .contains("unsupported control-plane state version 43"),
        "unexpected historical checkpoint rejection: {historical_error:?}"
    );

    const HISTORICAL_STATE_V42_CHECKPOINT: &[u8] = include_bytes!(
        "../../control_plane/testdata/raft_restart_v5_checkpoint_state_v42.bin"
    );
    assert_eq!(
        (
            HISTORICAL_STATE_V42_CHECKPOINT.len(),
            raft_test_hex(&checksum::sha256::digest(HISTORICAL_STATE_V42_CHECKPOINT))
        ),
        (
            236,
            "75a25ee0a0248e9759ff8f608570c762ac584f2609367593e544b4b915897f97".to_owned()
        )
    );
    let historical = reseal_historical_restart_artifact_for_nested_evidence(
        HISTORICAL_STATE_V42_CHECKPOINT,
    );
    let historical_error =
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&historical).unwrap_err();
    assert!(
        historical_error
            .to_string()
            .contains("unsupported control-plane state version 42"),
        "unexpected historical checkpoint rejection: {historical_error:?}"
    );

    let tmp = test_util::tempdir();
    let wal_path = tmp.path().join("raft.wal");
    let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);

    let checkpoint_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(checkpoint_vote))
        .expect("pre-checkpoint WAL append should succeed");
    let checkpoint_offset = wal.clean_len().expect("WAL clean length should read");
    let checkpoint_artifact = ControlPlaneRaftRestartArtifact {
        cluster_name: "test-cluster".to_string(),
        local_node_id: 1,
        wal_replay_offset: checkpoint_offset,
        log_store: ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(checkpoint_vote),
            ..Default::default()
        },
        state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
    };
    let checkpoint_artifact_bytes = checkpoint_artifact.encode_durable_artifact().unwrap();
    let decoded_checkpoint_artifact =
        ControlPlaneRaftRestartArtifact::decode_durable_artifact(&checkpoint_artifact_bytes)
            .unwrap();

    let suffix_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(4, 1);
    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(suffix_vote))
        .expect("post-checkpoint WAL append should succeed");
    let suffix_end = wal
        .clean_len()
        .expect("WAL suffix clean length should read");
    let pre_compaction_len = fs::metadata(&wal_path).unwrap().len();
    wal.compact_through(checkpoint_offset)
        .expect("WAL compaction should succeed");
    let post_compaction_len = fs::metadata(&wal_path).unwrap().len();
    assert!(
        post_compaction_len < pre_compaction_len,
        "WAL compaction should physically remove the checkpointed prefix"
    );
    let compacted_bytes = fs::read(&wal_path).unwrap();
    let (compacted_base, _) = wal
        .journal
        .decode_file_header(&compacted_bytes)
        .unwrap();
    assert_eq!(compacted_base, checkpoint_offset);
    assert_eq!(
        (
            checkpoint_offset,
            checkpoint_artifact_bytes.len(),
            raft_test_hex(&checksum::sha256::digest(&checkpoint_artifact_bytes)),
            compacted_bytes.len(),
            raft_test_hex(&checksum::sha256::digest(&compacted_bytes))
        ),
        (
            75,
            236,
            "99f82cbfd574d9751b328f6f3f6ca85b2890c4a75ddc9762abefdf9a95e773f4".to_owned(),
            112,
            "93e59ed1fe3d5152075c2974b5ca291b4942c958fefe2fc62782eff760c58f29".to_owned()
        )
    );
    assert_eq!(
        wal.clean_len()
            .expect("compacted WAL clean length should read"),
        suffix_end
    );

    let (restored_log_store, restored_state_machine) = decoded_checkpoint_artifact
        .restore_with_wal_file(test_raft_wal_file(&wal_path, "test-cluster", 1))
        .expect("checkpoint artifact should restore with compacted WAL suffix");
    assert_eq!(
        restored_log_store.persisted_vote().unwrap(),
        Some(suffix_vote)
    );
    assert_eq!(restored_state_machine.last_applied(), None);
}

#[test]
fn control_plane_raft_restart_artifact_capture_records_wal_replay_offset() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let mut log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal.clone(),
        )
        .expect("WAL-backed log store should restore");
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut log_store, &vote)
            .await
            .expect("WAL-backed vote should persist");

        let state_machine = ControlPlaneRaftStateMachine::empty();
        let artifact =
            ControlPlaneRaftRestartArtifact::capture("test-cluster", 1, &log_store, &state_machine)
                .expect("artifact capture should succeed");

        assert_eq!(
            artifact.wal_replay_offset,
            wal.clean_len().expect("WAL clean length should read")
        );
        assert_eq!(artifact.log_store.vote, Some(vote));
    });
}

#[test]
fn control_plane_raft_authority_checkpoint_compacts_wal_prefix() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("raft.state");
        let wal_path = durable_artifact_wal_path(&artifact_path);
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-wal-checkpoint-compaction-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster")
                .with_durable_artifact_path(&artifact_path);
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "WAL compaction checkpoint leadership").await;

        let pre_checkpoint_clean_len = wal.clean_len().expect("WAL clean length should read");
        assert!(
            pre_checkpoint_clean_len > 0,
            "initialized WAL-backed authority should have WAL bytes to compact"
        );
        let metrics_before = observability::control_plane_raft_checkpoint_metrics_snapshot();
        authority
            .store_durable_restart_artifact()
            .await
            .expect("durable checkpoint should store and compact WAL");
        let metrics_after = observability::control_plane_raft_checkpoint_metrics_snapshot();
        assert!(metrics_after.encode_total > metrics_before.encode_total);
        assert!(metrics_after.store_total > metrics_before.store_total);
        assert!(metrics_after.bytes_total > metrics_before.bytes_total);
        assert!(
            metrics_after.file_sync_total >= metrics_before.file_sync_total.saturating_add(2),
            "a first checkpoint should sync the sentinel and restart artifact files"
        );
        assert!(
            metrics_after.directory_sync_total
                >= metrics_before.directory_sync_total.saturating_add(2),
            "a first checkpoint should sync the directory after sentinel and artifact rename"
        );
        assert!(metrics_after.compaction_total > metrics_before.compaction_total);

        let artifact =
            ControlPlaneRaftRestartArtifact::load_durable_artifact(&artifact_path).unwrap();
        let status = authority.status().await.unwrap();
        let status_offsets = status
            .durable_wal_offsets()
            .expect("WAL-backed authority status should report durable offsets");
        assert_eq!(status_offsets.base_offset(), artifact.wal_replay_offset);
        assert!(
            status_offsets.clean_len() >= artifact.wal_replay_offset,
            "status WAL clean length {} should not precede checkpoint replay offset {}",
            status_offsets.clean_len(),
            artifact.wal_replay_offset
        );

        // A completed sync may become visible on disk immediately before the
        // durability lane publishes its cached offsets. Read the published
        // status first and prove that the physical WAL covers that prefix.
        let compacted_bytes = fs::read(&wal_path).unwrap();
        let (wal_base_offset, _) = wal
            .journal
            .decode_file_header(&compacted_bytes)
            .unwrap();
        assert_eq!(wal_base_offset, artifact.wal_replay_offset);
        let compacted_clean_len = wal
            .clean_len()
            .expect("compacted WAL clean length should read");
        assert!(
                compacted_clean_len >= artifact.wal_replay_offset,
                "compacted WAL clean length {compacted_clean_len} should not precede checkpoint replay offset {}",
                artifact.wal_replay_offset
            );
        assert!(
            compacted_clean_len >= status_offsets.clean_len(),
            "compacted WAL clean length {compacted_clean_len} should cover published status clean length {}",
            status_offsets.clean_len()
        );
        artifact
            .restore_with_wal_file(test_raft_wal_file(&wal_path, "test-cluster", 1))
            .expect("checkpoint artifact should restore after WAL compaction");

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_captured_checkpoint_preserves_post_capture_wal_suffix() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("raft.state");
        let wal_path = durable_artifact_wal_path(&artifact_path);
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-captured-checkpoint-suffix-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store.clone(), "test-cluster")
                .with_durable_artifact_path(&artifact_path);
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "captured checkpoint suffix leadership").await;

        let checkpoint = authority
            .capture_durable_restart_checkpoint()
            .await
            .expect("restart checkpoint should capture");
        let replay_offset = checkpoint.wal_replay_offset();
        authority.shutdown().await.unwrap();

        let suffix_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(7, 1);
        let mut suffix_store = log_store;
        RaftLogStorage::save_vote(&mut suffix_store, &suffix_vote)
            .await
            .expect("post-capture vote should append to the WAL suffix");
        let suffix_end = wal.clean_len().expect("suffix WAL length should read");
        assert!(suffix_end > replay_offset);

        authority
            .persist_durable_restart_checkpoint(checkpoint)
            .expect("captured checkpoint should persist and compact its WAL prefix");
        assert_eq!(
            authority
                .durable_wal_monitor_snapshot()
                .expect("compacted WAL monitor snapshot should read")
                .offsets(),
            ControlPlaneRaftWalOffsets {
                base_offset: replay_offset,
                clean_len: suffix_end,
            }
        );

        let artifact =
            ControlPlaneRaftRestartArtifact::load_durable_artifact(&artifact_path).unwrap();
        let (restored_log_store, _) = artifact
            .restore_with_wal_file(test_raft_wal_file(&wal_path, "test-cluster", 1))
            .expect("checkpoint plus post-capture WAL suffix should restore");
        assert_eq!(
            restored_log_store.persisted_vote().unwrap(),
            Some(suffix_vote)
        );
    });
}

#[test]
fn control_plane_raft_established_policy_reports_captured_apply_convergence() {
    let nodes = BTreeMap::from([(1, BasicNode::new("raft-node-1"))]);
    let membership = Membership::new(vec![BTreeSet::from([1])], nodes.clone()).unwrap();
    let membership_entry = ControlPlaneRaftEntry {
        log_id: raft_log_id(0, 1, 0),
        payload: EntryPayload::Membership(membership),
    };
    let applied_entry = blank_entry(1, 1, 1);
    let committed_entry = blank_entry(1, 1, 2);
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(membership_entry.clone()).unwrap();
    state_machine.apply_entry(applied_entry.clone()).unwrap();
    let mut checkpoint = ControlPlaneRaftCapturedRestartCheckpoint {
        artifact: ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(committed_entry.log_id),
                last_purged_log_id: None,
                entries: vec![membership_entry, applied_entry.clone(), committed_entry],
            },
            state_machine: state_machine.export_restart_artifact(),
        },
        authority_instance_id: ControlPlaneRaftAuthorityInstanceId::generate().unwrap(),
    };
    checkpoint.artifact.validate_restart_pair().unwrap();
    let policy = ControlPlaneRaftPeerTransportPolicy::new(
        "test-cluster",
        nodes,
        ControlPlaneRaftPeerTransportLimits::default(),
    );

    assert_eq!(
        checkpoint
            .established_peer_policy_convergence(&policy)
            .unwrap(),
        ControlPlaneRaftEstablishedPeerPolicyConvergence::AppliedStatePending {
            applied: Some(applied_entry.log_id),
            committed: Some(raft_log_id(1, 1, 2)),
        }
    );
    checkpoint.artifact.log_store.committed = Some(applied_entry.log_id);
    assert_eq!(
        checkpoint
            .established_peer_policy_convergence(&policy)
            .unwrap(),
        ControlPlaneRaftEstablishedPeerPolicyConvergence::Converged
    );
}

#[test]
fn control_plane_raft_established_static_policy_requires_matching_topology_certificate() {
    let nodes = BTreeMap::from([(1, BasicNode::default()), (2, BasicNode::default())]);
    let membership_entry = membership_entry(0, 1, 0);
    let bootstrap_nodes = vec![(NodeId::new(11), "node-11".to_string())];
    let bootstrap_pgs = vec![(crate::PgId::new(0), vec![NodeId::new(11)])];
    let topology = crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
        7,
        [0xab; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        vec![1, 2],
        &bootstrap_nodes,
        &bootstrap_pgs,
        crate::control_plane::test_certified_storage_placement_policy(
            [NodeId::new(11)],
            1,
            1_000,
        ),
    )
    .unwrap();
    let bootstrap_entry = normal_entry(
        1,
        1,
        1,
        ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: bootstrap_nodes,
            pg_acting_sets: bootstrap_pgs,
            topology: topology.clone(),
        },
    );
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(membership_entry.clone()).unwrap();
    state_machine.apply_entry(bootstrap_entry.clone()).unwrap();
    let checkpoint = ControlPlaneRaftCapturedRestartCheckpoint {
        artifact: ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            local_node_id: 1,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1)),
                committed: Some(bootstrap_entry.log_id),
                last_purged_log_id: None,
                entries: vec![membership_entry, bootstrap_entry],
            },
            state_machine: state_machine.export_restart_artifact(),
        },
        authority_instance_id: ControlPlaneRaftAuthorityInstanceId::generate().unwrap(),
    };
    let policy = ControlPlaneRaftPeerTransportPolicy::new(
        "test-cluster",
        nodes.clone(),
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .with_topology_identity(7, "ab".repeat(32))
    .with_initial_topology_certificate(topology);
    assert!(
        validate_captured_static_initial_topology(&ClusterControlSnapshot::empty(), &policy,)
            .unwrap_err()
            .to_string()
            .contains("has no initial topology certificate")
    );
    assert_eq!(
        checkpoint
            .established_peer_policy_convergence(&policy)
            .unwrap(),
        ControlPlaneRaftEstablishedPeerPolicyConvergence::Converged
    );

    let wrong_voters = ControlPlaneRaftPeerTransportPolicy::new(
        "test-cluster",
        BTreeMap::from([(1, BasicNode::default()), (3, BasicNode::default())]),
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .with_topology_identity(7, "ab".repeat(32));
    assert!(validate_captured_static_initial_topology(
        checkpoint.artifact.state_machine.inner.snapshot(),
        &wrong_voters,
    )
    .unwrap_err()
    .to_string()
    .contains("initial topology does not match"));

    let wrong_digest = ControlPlaneRaftPeerTransportPolicy::new(
        "test-cluster",
        nodes,
        ControlPlaneRaftPeerTransportLimits::default(),
    )
    .with_topology_identity(7, "cd".repeat(32));
    assert!(checkpoint
        .established_peer_policy_convergence(&wrong_digest)
        .unwrap_err()
        .to_string()
        .contains("initial topology does not match"));
}

fn static_initial_topology_for_submission_test(
    storage_endpoint: &str,
) -> StaticInitialControlPlaneTopology {
    let placement = crate::derive_static_initial_pg_placement(
        1,
        crate::StaticStoragePlacementParameters::new(
            1,
            0,
            crate::StaticStorageFailureDomain::None,
            0,
        ),
        &["host-1".to_owned()],
        &["disk-1".to_owned()],
        &[crate::StaticStoragePlacementNode::new(
            11, "host-1", "disk-1",
        )],
    )
    .unwrap();
    crate::derive_static_initial_control_plane_topology(
        7,
        &"ab".repeat(32),
        &[1],
        &[crate::StaticStorageNodeEndpoint::new(11, storage_endpoint)],
        placement,
    )
    .unwrap()
}

#[test]
fn raft_durability_publication_allows_concurrent_responses_before_exclusive_poison() {
    ControlPlaneRaftTypeConfig::run(async {
        let authority = ControlPlaneRaftAuthority::new_single_node_in_memory(
            "control-plane-raft-durability-publication-test",
            1,
        )
        .await
        .unwrap();
        let publication = authority.durability_publication().unwrap();
        let first_publication = publication.clone();
        let (first_started_tx, first_started_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let first = std::thread::spawn(move || {
            first_publication.publish(|| {
                first_started_tx
                    .send(())
                    .expect("first response should report publication start");
                release_first_rx
                    .recv()
                    .expect("first response should be released");
                Ok(())
            })
        });
        first_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first response should acquire a publication permit");

        let second_publication = publication.clone();
        let (second_finished_tx, second_finished_rx) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            let result = second_publication.publish(|| Ok(()));
            second_finished_tx
                .send(())
                .expect("second response should report completion");
            result
        });
        second_finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("an unrelated response must not wait for the first socket write");
        second
            .join()
            .expect("second response worker should exit")
            .expect("second response should publish");

        let poison_publication = publication.clone();
        let poisoner = std::thread::spawn(move || {
            poison_publication.poison("owner-local test durability failure")
        });
        let poison_wait_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let poison_requested = publication
                .gate
                .0
                .lock()
                .expect("response publication state should lock")
                .poison_requested;
            if poison_requested {
                break;
            }
            assert!(
                Instant::now() < poison_wait_deadline,
                "poison publication should become pending"
            );
            std::thread::yield_now();
        }
        assert!(!publication.is_poisoned());
        let error = publication
            .publish(|| Ok(()))
            .expect_err("a response arriving after poison was requested must be suppressed");
        assert!(matches!(error, ControlPlaneError::DurabilityFailure { .. }));

        release_first_tx
            .send(())
            .expect("first response should resume");
        first
            .join()
            .expect("first response worker should exit")
            .expect("first response should publish");
        poisoner
            .join()
            .expect("poison publication worker should exit");
        assert!(publication.is_poisoned());
        assert!(authority
            .durability_publication()
            .unwrap()
            .ensure_available()
            .is_err());
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn durable_uncertified_topology_establishment_publishes_before_resolution_and_restarts() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let artifact_path = directory.path().join("raft.state");
        let cluster_name = "control-plane-raft-durable-uncertified-topology-test";
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
            .wait_for_current_leader(
                1,
                Duration::from_secs(1),
                "durable uncertified topology test leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "durable uncertified topology authority becomes serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let topology = crate::derive_uncertified_initial_control_plane_topology(
            &[crate::StaticStorageNodeEndpoint::new(
                11,
                "/tmp/storage-node-11.sock",
            )],
            &[7],
        )
        .unwrap();

        let epoch = authority
            .establish_uncertified_initial_control_plane_topology(&topology)
            .await
            .unwrap()
            .expect("empty durable authority should establish the topology");
        assert!(artifact_path.is_file());
        assert_eq!(
            authority
                .establish_uncertified_initial_control_plane_topology(&topology)
                .await
                .unwrap(),
            None,
            "an established topology should be an idempotent no-op"
        );
        authority.shutdown().await.unwrap();

        let restarted = ControlPlaneRaftAuthority::new_single_node_durable(
            cluster_name,
            1,
            &artifact_path,
        )
        .await
        .unwrap();
        let snapshot = restarted.current_control_plane_snapshot().await.unwrap();
        assert_eq!(snapshot.cluster_epoch().get(), epoch);
        assert!(snapshot.node(NodeId::new(11)).is_some());
        assert!(snapshot.pg(PgId::new(7)).is_some());
        restarted.shutdown().await.unwrap();
    });
}

#[test]
fn durable_uncertified_topology_establishment_retries_on_follower_until_leader_publishes() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let (leader, follower) = initialized_two_node_checkpoint_authorities(
            "control-plane-raft-uncertified-leader-retry-test",
            1,
            2,
            &directory.path().join("leader.state"),
            &directory.path().join("follower.state"),
        )
        .await;
        let topology = crate::derive_uncertified_initial_control_plane_topology(
            &[
                crate::StaticStorageNodeEndpoint::new(1, "/tmp/storage-node-1.sock"),
                crate::StaticStorageNodeEndpoint::new(2, "/tmp/storage-node-2.sock"),
            ],
            &[7],
        )
        .unwrap();
        let follower_submit_errors_before = follower
            .durability_metric_snapshots()
            .command
            .submit_error_total;
        let follower_establishment =
            follower.establish_uncertified_initial_control_plane_topology(&topology);
        let leader_establishment = async {
            loop {
                let submit_errors = follower
                    .durability_metric_snapshots()
                    .command
                    .submit_error_total;
                if submit_errors > follower_submit_errors_before {
                    break;
                }
                tokio::task::yield_now().await;
            }
            leader
                .establish_uncertified_initial_control_plane_topology(&topology)
                .await
        };

        let (follower_result, leader_result) =
            futures_util::future::join(follower_establishment, leader_establishment).await;
        assert!(leader_result.unwrap().is_some());
        assert_eq!(
            follower_result.unwrap(),
            None,
            "the follower should retry its routing rejection and observe leader publication"
        );
        assert!(
            follower
                .durability_metric_snapshots()
                .command
                .submit_error_total
                > follower_submit_errors_before,
            "the fixture must observe a real follower submission rejection"
        );
        let follower_snapshot = follower.current_control_plane_snapshot().await.unwrap();
        assert!(follower_snapshot.node(NodeId::new(1)).is_some());
        assert!(follower_snapshot.node(NodeId::new(2)).is_some());
        assert!(follower_snapshot.pg(PgId::new(7)).is_some());

        leader.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    });
}

#[test]
fn follower_existing_topology_waits_for_its_own_checkpoint_when_leader_publication_fails() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let leader_path = directory.path().join("leader.state");
        let follower_path = directory.path().join("follower.state");
        let (leader, follower) = initialized_two_node_checkpoint_authorities(
            "control-plane-raft-uncertified-follower-checkpoint-test",
            1,
            2,
            &leader_path,
            &follower_path,
        )
        .await;
        fs::create_dir(&leader_path)
            .expect("leader checkpoint target should become an invalid directory");
        let topology = crate::derive_uncertified_initial_control_plane_topology(
            &[
                crate::StaticStorageNodeEndpoint::new(1, "/tmp/storage-node-1.sock"),
                crate::StaticStorageNodeEndpoint::new(2, "/tmp/storage-node-2.sock"),
            ],
            &[7],
        )
        .unwrap();
        let follower_checkpoint_total_before = follower
            .durability_metric_snapshots()
            .checkpoint
            .store_total;
        let follower_submit_errors_before = follower
            .durability_metric_snapshots()
            .command
            .submit_error_total;
        let follower_establishment =
            follower.establish_uncertified_initial_control_plane_topology(&topology);
        let leader_establishment = async {
            loop {
                if follower
                    .durability_metric_snapshots()
                    .command
                    .submit_error_total
                    > follower_submit_errors_before
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            leader
                .establish_uncertified_initial_control_plane_topology(&topology)
                .await
        };

        let (follower_result, leader_result) =
            futures_util::future::join(follower_establishment, leader_establishment).await;
        assert!(matches!(
            leader_result.expect_err("leader checkpoint publication must fail"),
            ControlPlaneError::DurabilityFailure { .. }
        ));
        assert_eq!(
            follower_result.unwrap(),
            None,
            "a follower may report observed topology only after its own checkpoint"
        );
        assert!(
            follower
                .durability_metric_snapshots()
                .checkpoint
                .store_total
                > follower_checkpoint_total_before,
            "existing-state observation must publish a follower-local checkpoint"
        );
        let artifact =
            ControlPlaneRaftRestartArtifact::load_durable_artifact(&follower_path).unwrap();
        let snapshot = artifact.state_machine.inner.snapshot();
        assert!(snapshot.node(NodeId::new(1)).is_some());
        assert!(snapshot.node(NodeId::new(2)).is_some());
        assert!(snapshot.pg(PgId::new(7)).is_some());

        leader.shutdown().await.unwrap();
        follower.shutdown().await.unwrap();
    });
}

#[test]
fn durable_uncertified_topology_checkpoint_failure_poisons_response_publication() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let artifact_path = directory.path().join("raft.state");
        let authority = ControlPlaneRaftAuthority::new_single_node_durable(
            "control-plane-raft-uncertified-checkpoint-failure-test",
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
            .wait_for_current_leader(
                1,
                Duration::from_secs(1),
                "uncertified checkpoint failure test leadership",
            )
            .await
            .unwrap();
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "uncertified checkpoint failure authority becomes serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        fs::create_dir(&artifact_path)
            .expect("checkpoint artifact path should become an invalid directory target");
        let topology = crate::derive_uncertified_initial_control_plane_topology(
            &[crate::StaticStorageNodeEndpoint::new(
                11,
                "/tmp/storage-node-11.sock",
            )],
            &[7],
        )
        .unwrap();

        let error = authority
            .establish_uncertified_initial_control_plane_topology(&topology)
            .await
            .expect_err("checkpoint failure must suppress the bootstrap result");
        assert!(matches!(error, ControlPlaneError::DurabilityFailure { .. }));
        let publication = authority.durability_publication().unwrap();
        assert!(publication.is_poisoned());
        let mut response_called = false;
        let mut response = || {
            response_called = true;
            Ok(())
        };
        let response_error =
            ControlPlaneRpcResponsePublication::publish(&publication, &mut response)
                .expect_err("a poisoned authority must suppress later response publication");
        assert!(matches!(
            response_error,
            ControlPlaneError::DurabilityFailure { .. }
        ));
        assert!(!response_called);
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn uncertified_topology_submission_owns_empty_state_and_concurrent_success_classification() {
    ControlPlaneRaftTypeConfig::run(async {
        let node_id = 1;
        let authority = ControlPlaneRaftAuthority::new_single_node_in_memory(
            "control-plane-raft-uncertified-topology-test",
            node_id,
        )
        .await
        .unwrap();
        authority
            .initialize_single_node_membership(node_id)
            .await
            .unwrap();
        wait_for_local_leader(
            authority.raft(),
            "uncertified topology submission test leadership",
        )
        .await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "uncertified topology submission authority becomes serving",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;
        let topology = crate::derive_uncertified_initial_control_plane_topology(
            &[crate::StaticStorageNodeEndpoint::new(
                11,
                "/tmp/storage-node-11.sock",
            )],
            &[7],
        )
        .unwrap();

        let empty_rejection = authority
            .resolve_uncertified_initial_control_plane_topology_submission(
                UncertifiedInitialControlPlaneTopologySubmission {
                    authority_instance_id: authority.authority_instance_id().unwrap(),
                    topology: topology.clone(),
                    submitted: SubmittedControlPlaneRaftCommand {
                        log_id: raft_log_id(1, node_id, 1),
                        outcome: ControlPlaneRaftCommandOutcome::Rejected(
                            ControlPlaneError::BootstrapRequiresEmptyState,
                        ),
                    },
                },
            )
            .await
            .expect_err("an empty authority must not accept a concurrent-success claim");
        assert!(matches!(
            empty_rejection,
            ControlPlaneError::BootstrapRequiresEmptyState
        ));

        let submitted = authority
            .prepare_uncertified_initial_control_plane_topology(&topology)
            .await
            .unwrap()
            .expect("empty authority should submit the owner-built bootstrap command");
        assert_eq!(
            format!("{submitted:?}"),
            "UncertifiedInitialControlPlaneTopologySubmission { diagnostic: \"<redacted>\", .. }"
        );
        let epoch = authority
            .resolve_uncertified_initial_control_plane_topology_submission(submitted)
            .await
            .unwrap()
            .expect("applied bootstrap should report its logical epoch");
        let snapshot = authority.current_control_plane_snapshot().await.unwrap();
        assert_eq!(epoch, snapshot.cluster_epoch().get());
        assert!(snapshot.node(NodeId::new(11)).is_some());
        assert!(snapshot.pg(PgId::new(7)).is_some());
        assert!(authority
            .prepare_uncertified_initial_control_plane_topology(&topology)
            .await
            .unwrap()
            .is_none());

        assert_eq!(
            authority
                .resolve_uncertified_initial_control_plane_topology_submission(
                    UncertifiedInitialControlPlaneTopologySubmission {
                        authority_instance_id: authority.authority_instance_id().unwrap(),
                        topology: topology.clone(),
                        submitted: SubmittedControlPlaneRaftCommand {
                            log_id: raft_log_id(1, node_id, 2),
                            outcome: ControlPlaneRaftCommandOutcome::Rejected(
                                ControlPlaneError::BootstrapRequiresEmptyState,
                            ),
                        },
                    },
                )
                .await
                .unwrap(),
            Some(epoch),
            "a rejection is concurrent success only after a fresh state observation"
        );
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn uncertified_topology_submission_rejects_a_different_raft_authority() {
    ControlPlaneRaftTypeConfig::run(async {
        let authority_a = ControlPlaneRaftAuthority::new_single_node_in_memory(
            "control-plane-raft-uncertified-authority-a",
            1,
        )
        .await
        .unwrap();
        let authority_b = ControlPlaneRaftAuthority::new_single_node_in_memory(
            "control-plane-raft-uncertified-authority-b",
            2,
        )
        .await
        .unwrap();
        for (authority, node_id, context) in [
            (&authority_a, 1, "uncertified authority A becomes serving"),
            (&authority_b, 2, "uncertified authority B becomes serving"),
        ] {
            authority
                .initialize_single_node_membership(node_id)
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), context).await;
            wait_for_authority_status_matching(
                authority,
                IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
                context,
                ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
            )
            .await;
        }
        assert!(
            authority_a.authority_instance_id().unwrap()
                != authority_b.authority_instance_id().unwrap()
        );

        let topology_a = crate::derive_uncertified_initial_control_plane_topology(
            &[crate::StaticStorageNodeEndpoint::new(
                11,
                "/tmp/storage-node-a.sock",
            )],
            &[7],
        )
        .unwrap();
        let topology_b = crate::derive_uncertified_initial_control_plane_topology(
            &[crate::StaticStorageNodeEndpoint::new(
                22,
                "/tmp/storage-node-b.sock",
            )],
            &[8],
        )
        .unwrap();

        let submission_b = authority_b
            .prepare_uncertified_initial_control_plane_topology(&topology_b)
            .await
            .unwrap()
            .unwrap();
        let epoch_b = authority_b
            .resolve_uncertified_initial_control_plane_topology_submission(submission_b)
            .await
            .unwrap()
            .unwrap();
        let submission_a = authority_a
            .prepare_uncertified_initial_control_plane_topology(&topology_a)
            .await
            .unwrap()
            .unwrap();

        let crossed = authority_b
            .resolve_uncertified_initial_control_plane_topology_submission(submission_a)
            .await
            .expect_err("authority B must reject authority A's opaque submission");
        assert!(crossed.retained_diagnostic_contains(
            "uncertified initial-topology submission belongs to another authority instance"
        ));
        let snapshot_b = authority_b.current_control_plane_snapshot().await.unwrap();
        assert_eq!(snapshot_b.cluster_epoch().get(), epoch_b);
        assert!(snapshot_b.node(NodeId::new(22)).is_some());
        assert!(snapshot_b.node(NodeId::new(11)).is_none());
        assert!(authority_a
            .current_control_plane_snapshot()
            .await
            .unwrap()
            .node(NodeId::new(11))
            .is_some());

        authority_a.shutdown().await.unwrap();
        authority_b.shutdown().await.unwrap();
    });
}

#[test]
fn static_topology_submission_rejects_crossed_authority_before_log_append() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let artifact_path = directory.path().join("static-authority.state");
        let configured = static_initial_topology_for_submission_test("node-11");
        let crossed = static_initial_topology_for_submission_test("crossed-node-11");
        let cluster_name = "control-plane-raft-static-submission-binding-test";
        let peer_policy = ControlPlaneRaftPeerTransportPolicy::new(
            cluster_name,
            BTreeMap::from([(1, BasicNode::new("raft-node-1"))]),
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_static_initial_topology(&configured);
        let log_store = ControlPlaneRaftLogStore::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config(cluster_name),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority = ControlPlaneRaftAuthority::new_with_log_store_and_static_peer_policy(
            raft,
            log_store,
            cluster_name,
            peer_policy,
        )
        .with_durable_artifact_path(&artifact_path);
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("raft-node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(
            authority.raft(),
            "static submission binding test leadership",
        )
        .await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "static submission binding authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let before = authority.status().await.unwrap();
        validate_static_initial_raft_membership(
            authority
                .static_peer_policy
                .as_ref()
                .expect("static authority should retain its peer policy"),
            &before,
        )
        .expect("applied membership should match the retained static policy");
        authority
            .wait_for_static_initial_membership()
            .await
            .expect("retained static membership should already be converged");

        let mut applied_pending = before.clone();
        applied_pending.applied = None;
        let error = validate_static_initial_raft_membership(
            authority.static_peer_policy.as_ref().unwrap(),
            &applied_pending,
        )
        .expect_err("unapplied committed state must not certify static membership");
        assert!(error.retained_diagnostic_contains("catches up to committed state"));

        let mut no_effective_membership = before.clone();
        no_effective_membership.effective_membership_log_id = None;
        let error = validate_static_initial_raft_membership(
            authority.static_peer_policy.as_ref().unwrap(),
            &no_effective_membership,
        )
        .expect_err("missing effective membership must not certify static membership");
        assert!(error.retained_diagnostic_contains("before effective Raft membership"));

        let mut no_applied_membership = before.clone();
        no_applied_membership.applied_membership_log_id = None;
        let error = validate_static_initial_raft_membership(
            authority.static_peer_policy.as_ref().unwrap(),
            &no_applied_membership,
        )
        .expect_err("missing applied membership must not certify static membership");
        assert!(error.retained_diagnostic_contains("before applied Raft membership"));

        let mut mismatched_membership = before.clone();
        mismatched_membership.effective_voters.insert(2);
        mismatched_membership.effective_learners.insert(3);
        mismatched_membership.applied_voters.insert(4);
        mismatched_membership.applied_learners.insert(5);
        mismatched_membership.applied_membership_log_id = Some(raft_log_id(2, 1, 99));
        let error = validate_static_initial_raft_membership(
            authority.static_peer_policy.as_ref().unwrap(),
            &mismatched_membership,
        )
        .expect_err("any membership identity mismatch must fail closed");
        assert!(error.retained_diagnostic_contains("membership does not match configured topology"));

        let error = authority
            .submit_static_initial_topology(&crossed)
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            ControlPlaneError::StaticTopologyFailure { .. }
        ));
        assert!(error.retained_diagnostic_contains(
            "static initial topology submission does not match the authority's configured topology"
        ));
        let after = authority.status().await.unwrap();
        assert_eq!(after.last_log_id(), before.last_log_id());
        assert_eq!(after.committed(), before.committed());
        assert_eq!(after.applied(), before.applied());
        let snapshot = authority.current_control_plane_snapshot().await.unwrap();
        assert!(snapshot.nodes().next().is_none());
        assert!(snapshot.pgs().next().is_none());
        assert!(snapshot.initial_topology().is_none());

        let error = authority
            .establish_static_initial_topology(&configured, false)
            .await
            .expect_err("published outer identity must forbid a missing topology");
        assert!(error.retained_diagnostic_contains(
            "established static control-plane state is missing its certified initial topology"
        ));
        let after_rejection = authority.status().await.unwrap();
        assert_eq!(after_rejection.last_log_id(), before.last_log_id());

        authority
            .establish_static_initial_topology(&configured, true)
            .await
            .expect("the bound authority should establish its retained topology");
        let established = authority.current_control_plane_snapshot().await.unwrap();
        assert!(configured.validate_snapshot(&established).unwrap());
        let publication = authority
            .publish_static_identity_restart_checkpoint()
            .await
            .expect("the authority should certify and publish its converged checkpoint")
            .expect("a converged checkpoint should return an opaque publication proof");
        assert_eq!(
            publication.authority_clock_binding(),
            authority.authority_clock_checkpoint_binding()
        );
        assert!(artifact_path.is_file());
        assert_eq!(
            format!("{publication:?}"),
            "ControlPlaneRaftStaticIdentityCheckpointPublication { .. }"
        );
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_checkpoint_refreshes_snapshot_after_state_machine_export() {
    ControlPlaneRaftTypeConfig::run(async {
        let bootstrap = single_node_bootstrap_membership_entry(1);
        let applied = blank_entry(3, 1, 1);
        let mut log_store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut log_store,
            vec![bootstrap.clone(), applied.clone()],
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
        RaftLogStorage::save_committed(&mut log_store, Some(applied.log_id))
            .await
            .unwrap();

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap).unwrap();
        let snapshot_log_id = state_machine
            .build_snapshot()
            .unwrap()
            .meta
            .last_log_id
            .expect("bootstrap snapshot should have a log id");
        state_machine.apply_entry(applied.clone()).unwrap();
        let applied_log_id = applied.log_id;

        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-checkpoint-refresh-isolation-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");
        assert!(applied_log_id.index() > snapshot_log_id.index());

        let state_machine = authority
            .capture_state_machine_restart_artifact()
            .await
            .unwrap();
        assert_eq!(state_machine.last_applied, Some(applied_log_id));
        assert_eq!(
            state_machine
                .current_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.meta.last_log_id),
            Some(snapshot_log_id),
            "state-machine export must finish before cached snapshot serialization"
        );
        authority
            .raft()
            .with_state_machine(|_| Box::pin(async {}))
            .await
            .expect("state-machine boundary should be available before snapshot refresh");

        let refreshed = state_machine.refresh_cached_snapshot().unwrap();
        assert_eq!(
            refreshed
                .current_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.meta.last_log_id),
            Some(applied_log_id),
            "detached checkpoint should refresh the cached snapshot to its applied tip"
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_wal_monitor_does_not_enter_state_machine() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let artifact_path = directory.path().join("raft.state");
        let wal_path = durable_artifact_wal_path(&artifact_path);
        let authority = Arc::new(
            ControlPlaneRaftAuthority::new_single_node_durable_with_wal(
                "wal-monitor-state-machine-isolation",
                1,
                &artifact_path,
                &wal_path,
            )
            .await
            .unwrap(),
        );
        let expected_offsets = authority.durable_wal_monitor_snapshot().unwrap().offsets();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let raft = authority.raft.clone();
        let holder = tokio::spawn(async move {
            raft.with_state_machine(move |_| {
                let _ = entered_tx.send(());
                Box::pin(async move {
                    let _ = release_rx.await;
                })
            })
            .await
            .unwrap();
        });
        entered_rx.await.unwrap();
        let monitor_authority = Arc::clone(&authority);
        let monitor = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || monitor_authority.durable_wal_monitor_snapshot()),
        )
        .await
        .expect("WAL monitor must not wait for the state-machine boundary")
        .unwrap()
        .unwrap();
        assert_eq!(monitor.offsets(), expected_offsets);
        assert_eq!(monitor.poisoned(), None);
        let _ = release_tx.send(());
        holder.await.unwrap();
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_captured_checkpoint_persists_outside_state_machine_boundary() {
    ControlPlaneRaftTypeConfig::run(async {
        let directory = test_util::tempdir();
        let artifact_path = directory.path().join("raft.state");
        let wal_path = durable_artifact_wal_path(&artifact_path);
        let authority = Arc::new(
            ControlPlaneRaftAuthority::new_single_node_durable_with_wal(
                "captured-checkpoint-state-machine-isolation",
                1,
                &artifact_path,
                &wal_path,
            )
            .await
            .unwrap(),
        );
        let checkpoint = authority
            .capture_durable_restart_checkpoint()
            .await
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let raft = authority.raft.clone();
        let holder = tokio::spawn(async move {
            raft.with_state_machine(move |_| {
                let _ = entered_tx.send(());
                Box::pin(async move {
                    let _ = release_rx.await;
                })
            })
            .await
            .unwrap();
        });
        entered_rx.await.unwrap();
        let persist_authority = Arc::clone(&authority);
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || {
                persist_authority.persist_durable_restart_checkpoint(checkpoint)
            }),
        )
        .await
        .expect("captured checkpoint persistence must not wait for the state machine")
        .unwrap()
        .unwrap();
        ControlPlaneRaftRestartArtifact::load_durable_artifact(&artifact_path).unwrap();
        let _ = release_tx.send(());
        holder.await.unwrap();
        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_checkpoint_rejects_stale_capture_before_publication() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("raft.state");
        let wal_path = durable_artifact_wal_path(&artifact_path);
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal,
        )
        .expect("WAL-backed log store should initialize");
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-stale-captured-checkpoint-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster")
                .with_durable_artifact_path(&artifact_path);
        authority
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        wait_for_local_leader(authority.raft(), "stale captured checkpoint leadership").await;
        wait_for_authority_status_matching(
            &authority,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "stale captured checkpoint authority applies initialization",
            ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
        )
        .await;

        let stale = authority
            .capture_durable_restart_checkpoint()
            .await
            .expect("older restart checkpoint should capture");
        let rejected = authority
            .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(99),
                availability: NodeAvailabilityState::Healthy,
            })
            .await
            .expect("deterministically rejected command should commit");
        assert!(matches!(
            rejected.outcome(),
            ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                node_id: 99
            })
        ));
        let current = authority
            .capture_durable_restart_checkpoint()
            .await
            .expect("newer restart checkpoint should capture");
        authority
            .persist_durable_restart_checkpoint(current)
            .expect("newer restart checkpoint should publish");
        let artifact_before_stale = fs::read(&artifact_path).unwrap();
        let offsets_before_stale = authority.durable_wal_monitor_snapshot().unwrap().offsets();

        let error = authority
            .persist_durable_restart_checkpoint(stale)
            .expect_err("older captured checkpoint must not replace a newer publication");
        assert!(
            error.retained_diagnostic_contains("precedes the last publication"),
            "unexpected stale-checkpoint error: {error}"
        );
        assert_eq!(fs::read(&artifact_path).unwrap(), artifact_before_stale);
        assert_eq!(
            authority.durable_wal_monitor_snapshot().unwrap().offsets(),
            offsets_before_stale
        );

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_checkpoint_rejects_capture_from_another_authority() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("foreign.state");
        fs::write(&artifact_path, b"unchanged").unwrap();
        let log_store = ControlPlaneRaftLogStore::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-foreign-captured-checkpoint-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster")
                .with_durable_artifact_path(&artifact_path);
        let checkpoint = authority
            .capture_durable_restart_checkpoint()
            .await
            .expect("restart checkpoint should capture");
        let foreign_authority = ControlPlaneRaftAuthority::new_with_log_store(
            authority.raft().clone(),
            authority
                .log_store
                .as_ref()
                .expect("test authority should retain its log store")
                .clone(),
            "test-cluster",
        )
        .with_durable_artifact_path(&artifact_path);

        let error = foreign_authority
            .persist_durable_restart_checkpoint(checkpoint)
            .expect_err("another authority instance must reject the captured checkpoint");
        assert!(
            error.retained_diagnostic_contains("another authority instance"),
            "unexpected foreign-checkpoint error: {error}"
        );
        assert_eq!(fs::read(&artifact_path).unwrap(), b"unchanged");

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_wal_file_missing_is_empty_replay() {
    let tmp = test_util::tempdir();
    let wal = test_raft_wal_file(tmp.path().join("missing.wal"), "test-cluster", 1);
    let base = ControlPlaneRaftLogStoreRestartArtifact {
        vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        entries: vec![bootstrap_membership_entry(1)],
        ..Default::default()
    };

    let replayed = wal
        .replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &base,
            replay_offset: 0,
        })
        .expect("missing WAL should replay as empty");
    assert_eq!(replayed, base);
}

#[test]
fn control_plane_raft_wal_file_rejects_identity_mismatch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.wal");
    let writer = test_raft_wal_file(&path, "other-cluster", 1);
    writer
        .append_record(&ControlPlaneRaftWalRecord::Append(vec![
            bootstrap_membership_entry(1),
        ]))
        .expect("WAL append should succeed");

    let reader = test_raft_wal_file(&path, "test-cluster", 1);
    assert_error_contains(
        reader.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
            replay_offset: 0,
        }),
        "control-plane OpenRaft WAL frame belongs to cluster",
    );
}

#[test]
fn control_plane_raft_wal_file_rejects_corrupt_middle_frame() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.wal");
    let wal = test_raft_wal_file(&path, "test-cluster", 1);
    for record in [
        ControlPlaneRaftWalRecord::Append(vec![bootstrap_membership_entry(1)]),
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
        ControlPlaneRaftWalRecord::Append(vec![blank_entry(3, 1, 1)]),
    ] {
        wal.append_record(&record)
            .expect("WAL append should succeed");
    }

    let mut bytes = fs::read(&path).unwrap();
    let second_start = wal_file_frame_end(&bytes, 0);
    let second_body = second_start + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN;
    bytes[second_body] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    assert_error_contains(
        wal.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
            replay_offset: 0,
        }),
        "control-plane OpenRaft WAL frame checksum mismatch",
    );
}

#[test]
fn control_plane_raft_wal_file_rejects_corrupt_first_and_middle_frame_lengths() {
    for target_frame in [0, 1] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("raft-{target_frame}.wal"));
        let wal = test_raft_wal_file(&path, "test-cluster", 1);
        for record in [
            ControlPlaneRaftWalRecord::Append(vec![bootstrap_membership_entry(1)]),
            ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(
                3, 1,
            )),
            ControlPlaneRaftWalRecord::Append(vec![blank_entry(3, 1, 1)]),
        ] {
            wal.append_record(&record)
                .expect("WAL append should succeed");
        }

        let mut bytes = fs::read(&path).unwrap();
        let first_start = CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len();
        let target_start = if target_frame == 0 {
            first_start
        } else {
            wal_file_frame_end(&bytes, first_start)
        };
        bytes[target_start] ^= 0x01;
        fs::write(&path, &bytes).unwrap();

        assert_error_contains(
            wal.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
                base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
                replay_offset: 0,
            }),
            "control-plane OpenRaft WAL frame length check mismatch",
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "length corruption must not be mistaken for a truncatable torn tail"
        );
    }
}

#[test]
fn control_plane_raft_wal_status_offsets_do_not_decode_frames() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&path, "test-cluster", 1);
        let mut log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");

        let (accepted, durable) =
            append_and_wait_for_durability(&mut log_store, vec![bootstrap_membership_entry(1)])
                .await;
        accepted.expect("bootstrap append should be accepted");
        durable.expect("bootstrap append should become durable");
        RaftLogStorage::save_vote(
            &mut log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .expect("vote should become durable");
        let (accepted, durable) =
            append_and_wait_for_durability(&mut log_store, vec![blank_entry(3, 1, 1)]).await;
        accepted.expect("blank entry should be accepted");
        durable.expect("blank entry should become durable");

        let mut bytes = fs::read(&path).unwrap();
        let second_start = wal_file_frame_end(&bytes, 0);
        let second_body = second_start + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN;
        bytes[second_body] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let status = log_store
            .status_snapshot()
            .expect("WAL status should report offsets without decoding frames")
            .durability;
        assert!(status.wal_backed);
        assert_eq!(
            status
                .wal_offsets
                .map(ControlPlaneRaftWalOffsets::base_offset),
            Some(0)
        );
        assert_eq!(
            status
                .wal_offsets
                .map(ControlPlaneRaftWalOffsets::clean_len),
            Some(
                u64::try_from(bytes.len() - CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len())
                    .expect("test WAL length should fit u64")
            )
        );
        assert_eq!(status.wal_poisoned, None);

        assert_error_contains(
            wal.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
                base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
                replay_offset: 0,
            }),
            "control-plane OpenRaft WAL frame checksum mismatch",
        );
    });
}

#[test]
fn control_plane_raft_wal_file_truncates_torn_final_frame() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("raft.wal");
    let wal = test_raft_wal_file(&path, "test-cluster", 1);
    let retained_record = ControlPlaneRaftWalRecord::Append(vec![bootstrap_membership_entry(1)]);
    let torn_record =
        ControlPlaneRaftWalRecord::SaveVote(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1));
    wal.append_record(&retained_record)
        .expect("WAL append should succeed");
    wal.append_record(&torn_record)
        .expect("WAL append should succeed");

    let bytes = fs::read(&path).unwrap();
    let clean_len = wal_file_frame_end(&bytes, 0);
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len((bytes.len() - 1) as u64).unwrap();
    drop(file);

    let replayed = wal
        .replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &ControlPlaneRaftLogStoreRestartArtifact::default(),
            replay_offset: 0,
        })
        .expect("WAL replay should discard torn final frame");
    let expected = ControlPlaneRaftLogStoreRestartArtifact::default()
        .replay_wal_records(&[retained_record])
        .expect("retained WAL prefix should replay");
    assert_eq!(replayed, expected);
    assert_eq!(fs::metadata(&path).unwrap().len(), clean_len as u64);
}

#[test]
fn control_plane_raft_wal_backed_log_store_persists_live_mutations_for_replay() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal = test_raft_wal_file(tmp.path().join("raft.wal"), "test-cluster", 1);
        let base = ControlPlaneRaftLogStoreRestartArtifact::default();
        let mut live = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            base.clone(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");

        RaftLogStorage::append(
            &mut live,
            vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(
            &mut live,
            &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut live, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();
        RaftLogStorage::append(&mut live, vec![blank_entry(3, 1, 2)], IOFlushed::noop())
            .await
            .unwrap();
        RaftLogStorage::save_committed(&mut live, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut live, raft_log_id(3, 1, 1))
            .await
            .unwrap();
        RaftLogStorage::truncate_after(&mut live, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        let replayed = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(base, wal)
            .expect("WAL-backed restart should replay live mutations")
            .export_restart_artifact()
            .unwrap();
        assert_eq!(replayed, live.export_restart_artifact().unwrap());
        let durability = live
            .status_snapshot()
            .expect("WAL-backed log store status should be observable")
            .durability;
        assert!(durability.wal_backed);
        assert_eq!(
            durability
                .wal_offsets
                .map(ControlPlaneRaftWalOffsets::base_offset),
            Some(0)
        );
        assert!(
                durability.wal_offsets.map(ControlPlaneRaftWalOffsets::clean_len).is_some_and(|len| len > 0),
                "WAL-backed log store should report a positive clean WAL length after mutations: {durability:?}"
            );
        assert_eq!(durability.wal_poisoned, None);
    });
}

#[test]
fn control_plane_raft_wal_backed_log_store_skips_idempotent_records() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal = test_raft_wal_file(tmp.path().join("raft.wal"), "test-cluster", 1);
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal,
        )
        .expect("WAL-backed log store should initialize");
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        let committed = raft_log_id(3, 1, 1);
        let purged = raft_log_id(0, 1, 0);

        RaftLogStorage::append(
            &mut store,
            vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(committed))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut store, purged).await.unwrap();

        let before_state = store.export_restart_artifact().unwrap();
        let before_offsets = store
            .status_snapshot()
            .unwrap()
            .durability
            .wal_offsets
            .expect("WAL-backed log store should report offsets");

        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(committed))
            .await
            .unwrap();
        RaftLogStorage::append(&mut store, Vec::new(), IOFlushed::noop())
            .await
            .unwrap();
        RaftLogStorage::truncate_after(&mut store, Some(committed))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut store, purged).await.unwrap();

        assert_eq!(store.export_restart_artifact().unwrap(), before_state);
        assert_eq!(
            store.status_snapshot().unwrap().durability.wal_offsets,
            Some(before_offsets),
            "idempotent storage operations must not grow the WAL"
        );
    });
}

#[test]
fn control_plane_raft_wal_backed_log_store_failure_publishes_only_accepted_mutation() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let parent = tmp.path().join("wal-parent");
        fs::create_dir(&parent).unwrap();
        let wal_path = parent.join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal,
        )
        .expect("missing WAL under existing directory should initialize");
        fs::remove_dir(&parent).unwrap();
        fs::write(&parent, b"not a directory").unwrap();

        let (accepted, durability) =
            append_and_wait_for_durability(&mut store, vec![bootstrap_membership_entry(1)]).await;
        accepted.expect("append should publish its readable view before WAL I/O");
        let err = durability.expect_err("WAL append failure should fail durability");
        assert!(
            err.to_string().contains("append OpenRaft WAL record"),
            "unexpected WAL append error: {err:?}"
        );
        let accepted = store.inner.lock().unwrap();
        assert_eq!(accepted.entries.len(), 1);
        assert!(accepted.poisoned.is_some());
        drop(accepted);
        let durable = store.durable.lock().unwrap();
        assert!(durable.inner.entries.is_empty());
        assert!(durable.inner.poisoned.is_some());
    });
}

#[test]
fn control_plane_raft_wal_backed_log_store_torn_header_poisons_after_acceptance() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal.clone(),
        )
        .expect("missing WAL should initialize");
        fs::write(&wal_path, &wal.journal.encode_file_header(0)[..3]).unwrap();

        let (accepted, durability) =
            append_and_wait_for_durability(&mut store, vec![bootstrap_membership_entry(1)]).await;
        accepted.expect("append should become readable before checking its WAL header");
        let err = durability.expect_err("torn WAL header should fail durability");
        assert!(
            err.to_string()
                .contains("truncated control-plane OpenRaft WAL file header"),
            "unexpected WAL append error: {err:?}"
        );
        let accepted = store.inner.lock().unwrap();
        assert_eq!(accepted.entries.len(), 1);
        assert!(accepted.poisoned.is_some());
        drop(accepted);
        assert!(store.durable.lock().unwrap().inner.entries.is_empty());
    });
}

#[test]
fn control_plane_raft_wal_append_is_readable_without_blocking_executor_on_file_sync() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("single-worker Tokio runtime should initialize")
        .block_on(async {
            let tmp = test_util::tempdir();
            let wal_path = tmp.path().join("raft.wal");
            let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
            let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
                ControlPlaneRaftLogStoreRestartArtifact::default(),
                wal,
            )
            .expect("WAL-backed log store should initialize");
            let gate = TestWalFileSyncGate::install(wal_path);
            let (flushed, mut durability) = ControlPlaneRaftTypeConfig::oneshot();

            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1)],
                IOFlushed::signal(flushed),
            )
            .await
            .expect("append should return after publishing the readable view");
            gate.wait_until_entered(Duration::from_secs(1));

            let log_state = RaftLogStorage::get_log_state(&mut store)
                .await
                .expect("accepted log state should remain readable during fsync");
            assert_eq!(log_state.last_log_id, Some(raft_log_id(0, 1, 0)));
            let status = store
                .status_snapshot()
                .expect("log-store status should remain readable during fsync");
            assert_eq!(status.durable_last_log_id, None);
            assert_eq!(status.durable_vote, None);
            assert!(store.durable.lock().unwrap().inner.entries.is_empty());
            assert!(matches!(
                durability.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));

            tokio::time::timeout(Duration::from_millis(100), async {
                tokio::time::sleep(Duration::from_millis(5)).await;
            })
            .await
            .expect("Tokio timer should progress while the WAL worker is blocked in fsync");
            assert!(matches!(
                durability.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));

            gate.release();
            tokio::time::timeout(Duration::from_secs(1), durability)
                .await
                .expect("flush callback should complete after releasing file sync")
                .expect("flush callback sender should remain available")
                .expect("WAL append should become durable");
            assert_eq!(store.export_restart_artifact().unwrap().entries.len(), 1);
        });
}

#[test]
fn control_plane_raft_durable_operation_survives_caller_cancellation_in_order() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("single-worker Tokio runtime should initialize")
        .block_on(async {
            let tmp = test_util::tempdir();
            let wal_path = tmp.path().join("raft.wal");
            let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
            let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
                ControlPlaneRaftLogStoreRestartArtifact::default(),
                wal,
            )
            .expect("WAL-backed log store should initialize");
            let gate = TestWalFileSyncGate::install(wal_path);
            let (flushed, durability) = ControlPlaneRaftTypeConfig::oneshot();
            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1)],
                IOFlushed::signal(flushed),
            )
            .await
            .expect("append should be accepted before file sync");
            gate.wait_until_entered(Duration::from_secs(1));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
            let mut vote_store = store.clone();
            let vote_task =
                tokio::spawn(
                    async move { RaftLogStorage::save_vote(&mut vote_store, &vote).await },
                );
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if store
                        .wal_metric_snapshot()
                        .is_some_and(|metrics| metrics.durability_queue_depth > 0)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("durable vote should enter the ordered queue");
            assert!(!vote_task.is_finished());
            tokio::time::timeout(Duration::from_millis(100), async {
                tokio::time::sleep(Duration::from_millis(5)).await;
            })
            .await
            .expect("Tokio timer should progress while the durable vote waits");

            vote_task.abort();
            assert!(vote_task.await.unwrap_err().is_cancelled());
            gate.release();
            durability
                .await
                .expect("append flush callback should remain connected")
                .expect("accepted append should become durable");

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if store.persisted_vote().unwrap() == Some(vote) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("submitted vote should complete after its caller is cancelled");
            let durable = store.export_restart_artifact().unwrap();
            assert_eq!(durable.entries.len(), 1);
            assert_eq!(durable.vote, Some(vote));
        });
}

#[test]
fn control_plane_raft_durability_queue_applies_bounded_backpressure() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("single-worker Tokio runtime should initialize")
        .block_on(async {
            let tmp = test_util::tempdir();
            let wal_path = tmp.path().join("raft.wal");
            let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
            let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
                ControlPlaneRaftLogStoreRestartArtifact::default(),
                wal,
            )
            .expect("WAL-backed log store should initialize");
            let gate = TestWalFileSyncGate::install(wal_path);
            let (flushed, durability) = ControlPlaneRaftTypeConfig::oneshot();
            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1)],
                IOFlushed::signal(flushed),
            )
            .await
            .expect("append should be accepted before file sync");
            gate.wait_until_entered(Duration::from_secs(1));

            let mut tasks = Vec::new();
            for term in 1..=CONTROL_PLANE_RAFT_DURABILITY_QUEUE_CAPACITY + 1 {
                let mut queued_store = store.clone();
                tasks.push(tokio::spawn(async move {
                    let vote =
                        Vote::<ControlPlaneRaftLeaderId>::new(u64::try_from(term).unwrap(), 1);
                    RaftLogStorage::save_vote(&mut queued_store, &vote).await
                }));
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if store.wal_metric_snapshot().is_some_and(|metrics| {
                        metrics.durability_queue_depth
                            > u64::try_from(CONTROL_PLANE_RAFT_DURABILITY_QUEUE_CAPACITY).unwrap()
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("one durable operation should wait outside the full bounded queue");
            let lane = store.durability_lane.as_ref().unwrap();
            assert_eq!(
                lane.sender.max_capacity(),
                CONTROL_PLANE_RAFT_DURABILITY_QUEUE_CAPACITY
            );
            assert_eq!(lane.sender.capacity(), 0);
            assert!(tasks.iter().any(|task| !task.is_finished()));

            gate.release();
            durability
                .await
                .expect("append flush callback should remain connected")
                .expect("accepted append should become durable");
            for task in tasks {
                task.await
                    .expect("durable operation task should remain available")
                    .expect("queued durable operation should complete");
            }
            assert_eq!(
                store.wal_metric_snapshot().unwrap().durability_queue_depth,
                0
            );
        });
}

#[test]
fn control_plane_raft_wal_compaction_waits_for_durable_publication() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            wal,
        )
        .expect("WAL-backed log store should initialize");
        let gate = TestWalFileSyncGate::install(wal_path);
        let (flushed, durability) = ControlPlaneRaftTypeConfig::oneshot();
        RaftLogStorage::append(
            &mut store,
            vec![bootstrap_membership_entry(1)],
            IOFlushed::signal(flushed),
        )
        .await
        .expect("append should be accepted before file sync");
        gate.wait_until_entered(Duration::from_secs(1));

        let compact_store = store.clone();
        let (compacted, compacted_rx) = std::sync::mpsc::channel();
        let compact_thread = thread::spawn(move || {
            let _ = compacted.send(compact_store.compact_wal_through(0));
        });
        assert!(matches!(
            compacted_rx.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));

        gate.release();
        durability
            .await
            .expect("append flush callback should remain connected")
            .expect("accepted append should become durable");
        compacted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compaction should continue after durable publication")
            .expect("compaction should succeed");
        compact_thread.join().unwrap();

        let durable = store.export_restart_artifact().unwrap();
        assert_eq!(durable.entries.len(), 1);
        let offsets = store.wal_monitor_snapshot().unwrap().unwrap().offsets();
        assert_eq!(offsets.base_offset(), 0);
        assert!(offsets.clean_len() > 0);
    });
}

#[test]
fn control_plane_raft_wal_backed_log_store_file_sync_error_poisons_without_publishing() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let base = ControlPlaneRaftLogStoreRestartArtifact::default();
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            base.clone(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");

        *CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_FILE_SYNC
            .lock()
            .expect("test WAL file-sync fault lock should not be poisoned") =
            Some(wal_path.clone());
        let metrics_before = observability::control_plane_raft_wal_metrics_snapshot();
        let (accepted, durability) =
            append_and_wait_for_durability(&mut store, vec![bootstrap_membership_entry(1)]).await;
        accepted.expect("append should become readable before WAL sync");
        let err = durability.expect_err("ambiguous WAL sync failure should fail durability");
        assert!(
            err.to_string()
                .contains("ambiguous WAL append after WAL write before file sync"),
            "unexpected WAL append error: {err:?}"
        );
        let metrics_after = observability::control_plane_raft_wal_metrics_snapshot();
        assert!(metrics_after.append_total > metrics_before.append_total);
        assert!(metrics_after.append_error_total > metrics_before.append_error_total);
        assert!(metrics_after.file_sync_total > metrics_before.file_sync_total);

        let err = store
            .export_restart_artifact()
            .expect_err("ambiguous WAL sync failure should poison the live log store");
        assert!(
            err.to_string()
                .contains("control-plane OpenRaft WAL-backed durable log store poisoned"),
            "unexpected poison error: {err:?}"
        );
        let inner = store
            .inner
            .lock()
            .expect("test should be able to inspect poisoned log store");
        assert_eq!(inner.vote, base.vote);
        assert_eq!(inner.committed, base.committed);
        assert_eq!(inner.last_purged_log_id, base.last_purged_log_id);
        assert_eq!(inner.entries.len(), 1);
        assert!(
            inner
                .poisoned
                .as_deref()
                .is_some_and(|reason| reason.contains("ambiguous WAL append")),
            "unexpected poison reason: {:?}",
            inner.poisoned
        );
        drop(inner);
        let durability = store
            .status_snapshot()
            .expect("poisoned WAL-backed log store status should remain observable")
            .durability;
        assert!(durability.wal_backed);
        assert_eq!(
            durability
                .wal_offsets
                .map(ControlPlaneRaftWalOffsets::base_offset),
            None
        );
        assert_eq!(
            durability
                .wal_offsets
                .map(ControlPlaneRaftWalOffsets::clean_len),
            None
        );
        assert!(
            durability
                .wal_poisoned
                .as_deref()
                .is_some_and(|reason| reason.contains("ambiguous WAL append")),
            "unexpected durability poison reason: {durability:?}"
        );
    });
}

#[test]
fn control_plane_raft_wal_backed_log_store_parent_sync_error_publishes_then_poisons() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let wal = test_raft_wal_file(&wal_path, "test-cluster", 1);
        let base = ControlPlaneRaftLogStoreRestartArtifact::default();
        let mut store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            base.clone(),
            wal.clone(),
        )
        .expect("WAL-backed log store should initialize");

        *CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_PARENT_SYNC
            .lock()
            .expect("test WAL parent-sync fault lock should not be poisoned") =
            Some(wal_path.clone());
        let metrics_before = observability::control_plane_raft_wal_metrics_snapshot();
        let (accepted, durability) =
            append_and_wait_for_durability(&mut store, vec![bootstrap_membership_entry(1)]).await;
        accepted.expect("append should become readable before WAL sync");
        let err = durability.expect_err("post-write WAL sync failure should fail durability");
        assert!(
            err.to_string()
                .contains("WAL append failed after file sync"),
            "unexpected WAL append error: {err:?}"
        );
        let metrics_after = observability::control_plane_raft_wal_metrics_snapshot();
        let physical_record_bytes = fs::metadata(&wal_path).unwrap().len()
            - u64::try_from(CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT.header_len()).unwrap();
        assert!(
            metrics_after.frame_bytes_total
                >= metrics_before
                    .frame_bytes_total
                    .saturating_add(physical_record_bytes),
            "file-synced WAL bytes should be counted even when parent sync fails"
        );

        let err = store
            .export_restart_artifact()
            .expect_err("post-file-sync failure should poison the live log store");
        assert!(
            err.to_string()
                .contains("control-plane OpenRaft WAL-backed durable log store poisoned"),
            "unexpected poison error: {err:?}"
        );
        let live_artifact = {
            let inner = store
                .inner
                .lock()
                .expect("test should be able to inspect poisoned log store");
            assert!(
                inner
                    .poisoned
                    .as_deref()
                    .is_some_and(|reason| reason.contains("WAL append failed after file sync")),
                "unexpected poison reason: {:?}",
                inner.poisoned
            );
            ControlPlaneRaftLogStoreRestartArtifact {
                vote: inner.vote,
                committed: inner.committed,
                last_purged_log_id: inner.last_purged_log_id,
                entries: inner.entries.values().cloned().collect(),
            }
        };
        assert_ne!(live_artifact, base);
        let replayed = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(base, wal)
            .expect("replayable WAL record should be restored after post-write error")
            .export_restart_artifact()
            .unwrap();
        assert_eq!(live_artifact, replayed);
    });
}

#[test]
fn control_plane_raft_status_reports_wal_durability() {
    ControlPlaneRaftTypeConfig::run(async {
        let tmp = test_util::tempdir();
        let wal_path = tmp.path().join("raft.wal");
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact_with_wal_file(
            ControlPlaneRaftLogStoreRestartArtifact::default(),
            test_raft_wal_file(&wal_path, "test-cluster", 1),
        )
        .expect("WAL-backed log store should initialize");
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            1,
            test_raft_config("control-plane-raft-wal-status-test"),
            UnreachableRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let authority =
            ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");

        let initial_status = authority.status().await.unwrap();
        assert!(initial_status.durable_wal_backed());
        assert_eq!(
            initial_status
                .durable_wal_offsets()
                .map(ControlPlaneRaftWalOffsets::base_offset),
            Some(0)
        );
        assert_eq!(
            initial_status
                .durable_wal_offsets()
                .map(ControlPlaneRaftWalOffsets::clean_len),
            Some(0)
        );
        assert_eq!(initial_status.durable_wal_poisoned(), None);
        assert_eq!(initial_status.durable_last_vote(), None);
        assert_eq!(initial_status.durable_last_log_id(), None);
        assert_eq!(initial_status.durable_last_purged_log_id(), None);
        assert_eq!(initial_status.durable_committed(), None);
        assert_eq!(initial_status.durable_applied(), None);
        assert_eq!(initial_status.durable_timestamp_high_water_ms(), None);

        let mut shared_log_store = authority
            .log_store
            .as_ref()
            .expect("authority should retain WAL-backed log store")
            .clone();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
        RaftLogStorage::save_vote(&mut shared_log_store, &vote)
            .await
            .unwrap();
        let bootstrap_log_id = raft_log_id(0, 1, 0);
        RaftLogStorage::append(
            &mut shared_log_store,
            vec![bootstrap_membership_entry(1)],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        RaftLogStorage::save_committed(&mut shared_log_store, Some(bootstrap_log_id))
            .await
            .unwrap();

        let status = authority.status().await.unwrap();
        assert!(status.durable_wal_backed());
        assert_eq!(
            status
                .durable_wal_offsets()
                .map(ControlPlaneRaftWalOffsets::base_offset),
            Some(0)
        );
        assert!(
                status.durable_wal_offsets().map(ControlPlaneRaftWalOffsets::clean_len).is_some_and(|len| len > 0),
                "WAL-backed authority status should report a positive clean WAL length after a durable mutation: {status:?}"
            );
        assert_eq!(status.durable_wal_poisoned(), None);
        assert_eq!(status.durable_last_vote(), Some(vote));
        assert_eq!(status.durable_last_log_id(), Some(bootstrap_log_id));
        assert_eq!(status.durable_last_purged_log_id(), None);
        assert_eq!(status.durable_committed(), Some(bootstrap_log_id));
        assert_eq!(status.durable_applied(), None);
        assert_eq!(status.durable_timestamp_high_water_ms(), None);

        *CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_FILE_SYNC
            .lock()
            .expect("test WAL file-sync fault lock should not be poisoned") =
            Some(wal_path.clone());
        RaftLogStorage::save_vote(
            &mut shared_log_store,
            &Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
        )
        .await
        .expect_err("ambiguous WAL sync failure should poison the log store");

        let poisoned_monitor = authority
            .durable_wal_monitor_snapshot()
            .expect("WAL monitor should remain available after WAL poison");
        assert!(
            poisoned_monitor
                .poisoned()
                .is_some_and(|reason| reason.contains("ambiguous WAL append")),
            "WAL monitor should expose the poison reason: {poisoned_monitor:?}"
        );
        assert!(
            poisoned_monitor.offsets().clean_len() > 0,
            "WAL monitor should retain O(1) offset diagnostics after poison"
        );

        let poisoned_status = authority
            .status()
            .await
            .expect("authority status should remain available after WAL poison");
        assert!(poisoned_status.durable_wal_backed());
        assert_eq!(
            poisoned_status
                .durable_wal_offsets()
                .map(ControlPlaneRaftWalOffsets::base_offset),
            None
        );
        assert_eq!(
            poisoned_status
                .durable_wal_offsets()
                .map(ControlPlaneRaftWalOffsets::clean_len),
            None
        );
        assert!(
            poisoned_status
                .durable_wal_poisoned()
                .is_some_and(|reason| reason.contains("ambiguous WAL append")),
            "poisoned authority status should expose WAL poison reason: {poisoned_status:?}"
        );
        assert_eq!(poisoned_status.durable_last_vote(), Some(vote));
        assert_eq!(
            poisoned_status.durable_last_log_id(),
            Some(bootstrap_log_id)
        );
        assert_eq!(poisoned_status.durable_committed(), Some(bootstrap_log_id));
        assert_eq!(poisoned_status.durable_applied(), None);

        authority.shutdown().await.unwrap();
    });
}

#[test]
fn control_plane_raft_durable_purge_coverage_is_monotonic_and_poison_first() {
    let target = raft_log_id(3, 1, 7);
    let mut status = ControlPlaneRaftLogStore::default()
        .status_snapshot()
        .unwrap();
    assert!(!control_plane_raft_durable_purge_covers(&status, target).unwrap());

    status.durable_last_purged_log_id = Some(raft_log_id(3, 1, 6));
    assert!(!control_plane_raft_durable_purge_covers(&status, target).unwrap());
    status.durable_last_purged_log_id = Some(target);
    assert!(control_plane_raft_durable_purge_covers(&status, target).unwrap());
    status.durable_last_purged_log_id = Some(raft_log_id(4, 1, 8));
    assert!(control_plane_raft_durable_purge_covers(&status, target).unwrap());

    status.durable_last_purged_log_id = Some(raft_log_id(4, 1, 7));
    assert!(matches!(
        control_plane_raft_durable_purge_covers(&status, target),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("conflicts with requested log id")
    ));

    status.durable_last_purged_log_id = Some(target);
    status.durability.wal_poisoned = Some("injected post-sync failure".to_string());
    assert!(matches!(
        control_plane_raft_durable_purge_covers(&status, target),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("injected post-sync failure")
    ));
}

#[test]
fn control_plane_raft_status_binds_durable_purge_watermark_to_durable_poison() {
    let target = raft_log_id(3, 1, 7);
    let store = ControlPlaneRaftLogStore::default();
    {
        let accepted = store.inner.lock().unwrap();
        assert_eq!(accepted.poisoned, None);
        assert_eq!(accepted.last_purged_log_id, None);
    }
    {
        let mut durable = store.durable.lock().unwrap();
        durable.inner.last_purged_log_id = Some(target);
        durable.inner.poisoned = Some("injected durable purge poison".to_string());
    }

    let status = store.status_snapshot().unwrap();
    assert_eq!(status.last_purged_log_id, None);
    assert_eq!(status.durable_last_purged_log_id, Some(target));
    assert_eq!(
        status.durability.wal_poisoned.as_deref(),
        Some("injected durable purge poison")
    );
    assert!(matches!(
        control_plane_raft_durable_purge_covers(&status, target),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("injected durable purge poison")
    ));
}

#[test]
fn control_plane_raft_log_id_round_trips_term_and_index() {
    let control_plane_log_id = ControlPlaneLogId::new(7, 42).unwrap();
    let raft_log_id = raft_log_id_from_control_plane(3, control_plane_log_id);

    assert_eq!(raft_log_id.committed_leader_id().term, 7);
    assert_eq!(raft_log_id.committed_leader_id().node_id, 3);
    assert_eq!(raft_log_id.index(), 42);
    assert_eq!(
        control_plane_log_id_from_raft(raft_log_id),
        Some(control_plane_log_id)
    );
}

#[test]
fn control_plane_raft_log_id_rejects_reserved_values() {
    let zero_term = LogId::new(
        LeaderId {
            term: 0,
            node_id: 1,
        },
        1,
    );
    let zero_index = LogId::new(
        LeaderId {
            term: 1,
            node_id: 1,
        },
        0,
    );

    assert_eq!(control_plane_log_id_from_raft(zero_term), None);
    assert_eq!(control_plane_log_id_from_raft(zero_index), None);
}

#[test]
fn control_plane_raft_node_id_conversion_is_bounded_by_storage_node_id() {
    let storage_id = NodeId::new(17);

    assert_eq!(raft_node_id_from_storage_node_id(storage_id), 17);
    assert_eq!(storage_node_id_from_raft_node_id(17), Some(storage_id));
    assert_eq!(
        storage_node_id_from_raft_node_id(u64::from(u32::MAX) + 1),
        None
    );
}

#[test]
fn control_plane_raft_state_machine_applies_blank_and_membership_entries() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();

    assert!(matches!(
        state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap(),
        ControlPlaneRaftApplyResponse::Blank
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 1)));

    assert!(matches!(
        state_machine
            .apply_entry(membership_entry(1, 1, 2))
            .unwrap(),
        ControlPlaneRaftApplyResponse::Membership
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
    assert_eq!(
        state_machine.last_membership().log_id(),
        &Some(raft_log_id(1, 1, 2))
    );
    assert_eq!(
        state_machine
            .inner()
            .last_applied()
            .map(|log_id| (log_id.term(), log_id.index())),
        Some((1, 2))
    );
}

#[test]
fn control_plane_raft_state_machine_applies_openraft_bootstrap_membership() {
    let mut empty_state_machine = ControlPlaneRaftStateMachine::empty();
    let empty_snapshot = empty_state_machine.build_snapshot().unwrap();
    assert_eq!(empty_snapshot.meta.last_log_id, None);

    let mut state_machine = ControlPlaneRaftStateMachine::empty();

    assert!(matches!(
        state_machine
            .apply_entry(bootstrap_membership_entry(7))
            .unwrap(),
        ControlPlaneRaftApplyResponse::Membership
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 7, 0)));
    assert_eq!(
        state_machine.last_membership().log_id(),
        &Some(raft_log_id(0, 7, 0))
    );
    assert_eq!(state_machine.inner().last_applied(), None);

    let snapshot = state_machine.build_snapshot().unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(0, 7, 0)));
    assert_eq!(
        snapshot.meta.last_membership.log_id(),
        &Some(raft_log_id(0, 7, 0))
    );

    let mut target = ControlPlaneRaftStateMachine::empty();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .unwrap();
    assert_eq!(target.last_applied(), Some(raft_log_id(0, 7, 0)));
    assert_eq!(target.inner().last_applied(), None);
    assert_eq!(
        target.last_membership().log_id(),
        &Some(raft_log_id(0, 7, 0))
    );
}

#[test]
fn control_plane_raft_state_machine_new_validates_restart_state() {
    let empty = ControlPlaneRaftStateMachine::new(
        ReplicatedControlPlaneStateMachine::empty(),
        None,
        StoredMembership::default(),
    )
    .unwrap();
    assert_eq!(empty.last_applied(), None);

    let bootstrap = ControlPlaneRaftStateMachine::new(
        ReplicatedControlPlaneStateMachine::empty(),
        Some(raft_log_id(0, 7, 0)),
        StoredMembership::new(Some(raft_log_id(0, 7, 0)), test_membership()),
    )
    .unwrap();
    assert_eq!(bootstrap.last_applied(), Some(raft_log_id(0, 7, 0)));
    assert_eq!(bootstrap.inner().last_applied(), None);

    let applied = ControlPlaneRaftStateMachine::new(
        replicated_state_machine_with_noops(2, 3),
        Some(raft_log_id(2, 7, 3)),
        StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership()),
    )
    .unwrap();
    assert_eq!(applied.last_applied(), Some(raft_log_id(2, 7, 3)));
    assert_eq!(
        applied
            .inner()
            .last_applied()
            .map(|log_id| (log_id.term(), log_id.index())),
        Some((2, 3))
    );
}

#[test]
fn control_plane_raft_state_machine_new_rejects_inconsistent_restart_state() {
    let err = ControlPlaneRaftStateMachine::new(
        replicated_state_machine_with_noops(2, 3),
        None,
        StoredMembership::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

    let err = ControlPlaneRaftStateMachine::new(
        ReplicatedControlPlaneStateMachine::empty(),
        Some(raft_log_id(2, 7, 3)),
        StoredMembership::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

    let err = ControlPlaneRaftStateMachine::new(
        replicated_state_machine_with_noops(2, 3),
        Some(raft_log_id(3, 7, 3)),
        StoredMembership::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

    let err = ControlPlaneRaftStateMachine::new(
        replicated_state_machine_with_noops(2, 3),
        Some(raft_log_id(2, 7, 3)),
        StoredMembership::new(Some(raft_log_id(2, 7, 4)), test_membership()),
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

}

#[test]
fn control_plane_raft_state_machine_restores_restart_artifact() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source
        .apply_entry(normal_entry(
            2,
            7,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    source.apply_entry(membership_entry(2, 7, 2)).unwrap();
    assert!(matches!(
        source
            .apply_entry(normal_entry(
                2,
                7,
                3,
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                },
            ))
            .unwrap(),
        ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
    ));

    let artifact = source.export_restart_artifact();
    let mut restored = ControlPlaneRaftStateMachine::from_restart_artifact(artifact).unwrap();

    assert_eq!(restored.last_applied(), Some(raft_log_id(2, 7, 3)));
    assert_eq!(
        restored.last_membership().log_id(),
        &Some(raft_log_id(2, 7, 2))
    );
    assert_eq!(restored.inner().snapshot(), source.inner().snapshot());
    assert!(restored.current_snapshot().is_none());

    restored.apply_entry(blank_entry(2, 7, 4)).unwrap();
    let runtime_map = restored
        .runtime_map_for_applied_read_index(raft_log_id(2, 7, 4), 12_345)
        .unwrap();
    assert_eq!(
        runtime_map.freshness_proof().read_index(),
        Some(ControlPlaneLogId::new(2, 4).unwrap())
    );
}

#[test]
fn control_plane_raft_state_machine_rejects_stale_lease_authority_terms() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(normal_entry(
            2,
            7,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    let before = state_machine.inner().snapshot().clone();

    let response = state_machine
        .apply_entry(normal_entry(
            2,
            7,
            2,
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "node-1".to_string(),
                    observed_epoch: before.cluster_epoch(),
                    requested_lease_duration_ms: 100,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 1_000,
                lease_deadline_ms: 1_100,
                lease_horizon_authority: Some(LeaseHorizonAuthorityBinding::new(7, Some(1))),
            },
        ))
        .unwrap();

    assert!(matches!(
        response,
        ControlPlaneRaftApplyResponse::Rejected(
            ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                authority_term: Some(1),
                committed_term: Some(2),
            }
        )
    ));
    assert_eq!(state_machine.inner().snapshot(), &before);
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(2, 7, 2)));
    assert_eq!(
        state_machine.inner().last_applied(),
        Some(ControlPlaneLogId::new(2, 2).unwrap())
    );

    let response = state_machine
        .apply_entry(normal_entry(
            2,
            7,
            3,
            ControlPlaneCommand::PromoteNodeHeartbeatLeases {
                authority: LeaseHorizonAuthorityBinding::new(7, Some(1)),
                promoted: vec![crate::control_plane_command::PromotedNodeHeartbeatLease {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    lease_deadline_ms: 1_100,
                }],
            },
        ))
        .unwrap();
    assert!(matches!(
        response,
        ControlPlaneRaftApplyResponse::Rejected(
            ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                authority_term: Some(1),
                committed_term: Some(2),
            }
        )
    ));
    assert_eq!(state_machine.inner().snapshot(), &before);
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(2, 7, 3)));
    assert_eq!(
        state_machine.inner().last_applied(),
        Some(ControlPlaneLogId::new(2, 3).unwrap())
    );
}

#[test]
fn control_plane_raft_state_machine_rejects_invalid_restart_artifacts() {
    let inner_with_applied = replicated_state_machine_with_noops(2, 3);
    let err = ControlPlaneRaftStateMachine::from_restart_artifact(
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: inner_with_applied.clone(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

    let err = ControlPlaneRaftStateMachine::from_restart_artifact(
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: ReplicatedControlPlaneStateMachine::empty(),
            last_applied: Some(raft_log_id(2, 7, 3)),
            last_membership: StoredMembership::default(),
            current_snapshot: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

    let err = ControlPlaneRaftStateMachine::from_restart_artifact(
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: inner_with_applied,
            last_applied: Some(raft_log_id(2, 7, 3)),
            last_membership: StoredMembership::new(Some(raft_log_id(2, 7, 4)), test_membership()),
            current_snapshot: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
}

#[test]
fn control_plane_raft_state_machine_rejects_nonzero_term_index_zero_membership() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();

    let err = state_machine
        .apply_entry(membership_entry(1, 7, 0))
        .unwrap_err();

    assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
    assert_eq!(state_machine.last_applied(), None);
    assert_eq!(state_machine.last_membership().log_id(), &None);
    assert_eq!(state_machine.inner().last_applied(), None);
}

#[test]
fn control_plane_raft_state_machine_rejects_out_of_order_apply() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();

    let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::ControlPlaneLogIndexMismatch {
            expected_index: 1,
            actual_index: 2,
        }
    ));
    assert_eq!(state_machine.last_applied(), None);

    state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap();
    let err = state_machine
        .apply_entry(bootstrap_membership_entry(1))
        .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::ControlPlaneLogIndexMismatch {
            expected_index: 1,
            actual_index: 0,
        }
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 1, 0)));

    state_machine.apply_entry(blank_entry(3, 2, 1)).unwrap();
    let err = state_machine
        .apply_entry(blank_entry(2, 99, 2))
        .unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::ControlPlaneLogTermRegression {
            previous_term: 3,
            actual_term: 2,
            index: 2,
        }
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));

    let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
    assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));
}

#[test]
fn control_plane_raft_state_machine_rejects_apply_after_max_index() {
    let inner = ReplicatedControlPlaneStateMachine::new(
        ClusterControlSnapshot::empty(),
        Some(ControlPlaneLogId::new(1, u64::MAX).unwrap()),
    )
    .unwrap();
    let mut state_machine = ControlPlaneRaftStateMachine::new(
        inner,
        Some(raft_log_id(1, 7, u64::MAX)),
        StoredMembership::default(),
    )
    .unwrap();

    let err = state_machine
        .apply_entry(blank_entry(1, 7, u64::MAX))
        .unwrap_err();

    assert!(matches!(
        err,
        ControlPlaneError::ControlPlaneLogIndexOverflow { index: u64::MAX }
    ));
    assert_eq!(
        state_machine.last_applied(),
        Some(raft_log_id(1, 7, u64::MAX))
    );
}

#[test]
fn control_plane_raft_state_machine_builds_and_installs_snapshot() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(blank_entry(2, 7, 1)).unwrap();
    let snapshot = source.build_snapshot().unwrap();

    assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));

    let mut target = ControlPlaneRaftStateMachine::empty();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .unwrap();

    assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
    assert_eq!(
        target
            .inner()
            .last_applied()
            .map(|log_id| (log_id.term(), log_id.index())),
        Some((2, 1))
    );
}

#[test]
fn control_plane_raft_snapshot_builder_returns_stable_snapshot_view() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(blank_entry(2, 7, 1)).unwrap();

    let mut builder = state_machine.create_snapshot_builder().unwrap();
    state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

    let snapshot = ControlPlaneRaftTypeConfig::run(builder.build_snapshot()).unwrap();

    assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(2, 7, 2)));
    assert_eq!(
        state_machine
            .current_snapshot()
            .map(|snapshot| snapshot.meta.last_log_id),
        Some(Some(raft_log_id(2, 7, 1)))
    );
}

#[test]
fn control_plane_raft_snapshot_install_rejects_same_position_different_leader() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(blank_entry(2, 7, 1)).unwrap();
    let snapshot = source.build_snapshot().unwrap();
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_payload = snapshot.snapshot.clone();

    let mut target = ControlPlaneRaftStateMachine::empty();
    target
        .install_snapshot(&snapshot_meta, snapshot_payload.clone())
        .unwrap();

    let mut bad_meta = snapshot_meta;
    bad_meta.last_log_id = Some(raft_log_id(2, 8, 1));
    let err = target
        .install_snapshot(&bad_meta, snapshot_payload)
        .unwrap_err();

    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
}

#[test]
fn control_plane_raft_snapshot_install_rejects_unapplied_membership_log_id() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(blank_entry(2, 7, 1)).unwrap();
    let snapshot = source.build_snapshot().unwrap();
    let mut bad_meta = snapshot.meta.clone();
    bad_meta.last_membership = StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership());

    let mut target = ControlPlaneRaftStateMachine::empty();
    let err = target
        .install_snapshot(&bad_meta, snapshot.snapshot)
        .unwrap_err();

    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    assert_eq!(target.last_applied(), None);
}

#[test]
fn control_plane_raft_snapshot_install_rejects_invalid_membership_log_id() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(blank_entry(2, 7, 1)).unwrap();
    let snapshot = source.build_snapshot().unwrap();
    let mut bad_meta = snapshot.meta.clone();
    bad_meta.last_membership = StoredMembership::new(Some(raft_log_id(0, 7, 1)), test_membership());

    let mut target = ControlPlaneRaftStateMachine::empty();
    let err = target
        .install_snapshot(&bad_meta, snapshot.snapshot)
        .unwrap_err();

    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    assert_eq!(target.last_applied(), None);
}

#[test]
fn control_plane_raft_snapshot_install_rejects_nonzero_term_index_zero_log_id() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(bootstrap_membership_entry(7)).unwrap();
    let snapshot = source.build_snapshot().unwrap();

    let mut bad_meta = snapshot.meta.clone();
    bad_meta.last_log_id = Some(raft_log_id(1, 7, 0));
    bad_meta.last_membership = StoredMembership::new(Some(raft_log_id(1, 7, 0)), test_membership());

    let mut target = ControlPlaneRaftStateMachine::empty();
    let err = target
        .install_snapshot(&bad_meta, snapshot.snapshot)
        .unwrap_err();

    assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    assert_eq!(target.last_applied(), None);
    assert_eq!(target.last_membership().log_id(), &None);
    assert_eq!(target.inner().last_applied(), None);
}

#[test]
fn control_plane_raft_state_machine_maps_normal_outcomes_to_application_responses() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();

    let applied = state_machine
        .apply_entry(normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    assert!(matches!(
        applied,
        ControlPlaneRaftApplyResponse::Applied(
            ControlPlaneCommandResponse::BootstrapInitialClusterMap
        )
    ));

    let rejected = state_machine
        .apply_entry(normal_entry(
            1,
            1,
            2,
            ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(99),
                availability: NodeAvailabilityState::Healthy,
            },
        ))
        .unwrap();
    assert!(matches!(
        rejected,
        ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
    ));
    assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
    assert_eq!(
        state_machine
            .inner()
            .last_applied()
            .map(|log_id| (log_id.term(), log_id.index())),
        Some((1, 2))
    );
}

#[test]
fn control_plane_raft_state_machine_runtime_map_read_index_uses_applied_log_id() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(normal_entry(
            2,
            7,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();

    let runtime_map = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
        .unwrap();

    assert_eq!(
        runtime_map.freshness_proof(),
        &RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
            read_index: ControlPlaneLogId::new(2, 1).unwrap(),
            issued_at_ms: 12_345,
        }
    );
    assert_eq!(
        runtime_map.freshness_proof().read_index(),
        Some(ControlPlaneLogId::new(2, 1).unwrap())
    );
    assert!(runtime_map.freshness_proof().is_serving_authority_read());
}

#[test]
fn control_plane_raft_state_machine_runtime_map_current_read_index_uses_tip() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine
        .apply_entry(normal_entry(
            2,
            7,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

    let stale_captured_read_index = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
        .unwrap_err();
    assert!(matches!(
        stale_captured_read_index,
        ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
    ));

    let runtime_map = state_machine
        .runtime_map_for_current_applied_read_index(12_346)
        .unwrap();
    assert_eq!(
        runtime_map.freshness_proof(),
        &RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
            read_index: ControlPlaneLogId::new(2, 2).unwrap(),
            issued_at_ms: 12_346,
        }
    );
}

#[test]
fn control_plane_raft_state_machine_runtime_map_rejects_unapplied_read_index() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(blank_entry(3, 7, 1)).unwrap();

    let future_index = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(3, 7, 2), 12_345)
        .unwrap_err();
    assert!(matches!(
        future_index,
        ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
    ));

    let lower_term_higher_index = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(2, 7, 2), 12_345)
        .unwrap_err();
    assert!(matches!(
        lower_term_higher_index,
        ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
    ));

    let same_position_different_leader = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(3, 8, 1), 12_345)
        .unwrap_err();
    assert!(matches!(
        same_position_different_leader,
        ControlPlaneError::CommandDecode { .. }
    ));

    let invalid_bootstrap_read_index = state_machine
        .runtime_map_for_applied_read_index(raft_log_id(0, 7, 0), 12_345)
        .unwrap_err();
    assert!(matches!(
        invalid_bootstrap_read_index,
        ControlPlaneError::CommandDecode { .. }
    ));
}

#[test]
fn control_plane_raft_state_machine_trait_apply_drains_entry_responder_stream() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    let entries = stream::iter(vec![
        Ok((blank_entry(1, 1, 1), None)),
        Ok((
            normal_entry(
                1,
                1,
                2,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ),
            None,
        )),
        Ok((
            normal_entry(
                1,
                1,
                3,
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                },
            ),
            None,
        )),
    ]);

    ControlPlaneRaftTypeConfig::run(RaftStateMachine::apply(&mut state_machine, entries)).unwrap();

    assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 3)));
    assert_eq!(
        state_machine
            .inner()
            .last_applied()
            .map(|log_id| (log_id.term(), log_id.index())),
        Some((1, 3))
    );
}

#[test]
fn control_plane_raft_state_machine_apply_does_not_block_single_worker_executor() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let timer_completed = Arc::new(AtomicBool::new(false));
        let watchdog = state_machine_executor_progress_watchdog(
            Arc::clone(&hook),
            Arc::clone(&timer_completed),
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
            apply: Some(hook),
            ..ControlPlaneRaftStateMachineTestHooks::default()
        });
        let operation = tokio::spawn(async move {
            let entries = stream::iter(vec![Ok((blank_entry(1, 1, 1), None))]);
            RaftStateMachine::apply(&mut state_machine, entries)
                .await
                .unwrap();
            state_machine
        });
        let timer = tokio::spawn(mark_executor_timer_progress(Arc::clone(&timer_completed)));

        let state_machine = operation.await.unwrap();
        timer.await.unwrap();
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 1)));
        assert!(
            watchdog.join().unwrap(),
            "single-worker executor timer must progress while command apply is blocked"
        );
    });
}

#[test]
fn control_plane_raft_apply_publication_retires_state_off_single_worker_executor() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
            let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
            let retirement_entered = Arc::new(AtomicBool::new(false));
            let timer_completed = Arc::new(AtomicBool::new(false));
            let watchdog = state_machine_retirement_progress_watchdog(
                Arc::clone(&hook),
                Arc::clone(&retirement_entered),
                Arc::clone(&timer_completed),
            );
            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
                retire_generation: Some(hook),
                ..ControlPlaneRaftStateMachineTestHooks::default()
            });
            let operation = tokio::spawn(async move {
                let entries = stream::iter(vec![Ok((
                    normal_entry(
                        1,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "node-1".to_string())],
                            pg_ids: vec![PgId::new(0)],
                        },
                    ),
                    None,
                ))]);
                RaftStateMachine::apply(&mut state_machine, entries)
                    .await
                    .unwrap();
                state_machine
            });
            let timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
                retirement_entered,
                Arc::clone(&timer_completed),
            ));

            let state_machine = operation.await.unwrap();
            timer.await.unwrap();
            assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 1)));
            assert!(
                watchdog.join().unwrap(),
                "single-worker executor timer must progress while the replaced state generation is awaiting destruction"
            );
        });
}

#[test]
fn control_plane_raft_snapshot_build_does_not_block_single_worker_executor() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let timer_completed = Arc::new(AtomicBool::new(false));
        let watchdog = state_machine_executor_progress_watchdog(
            Arc::clone(&hook),
            Arc::clone(&timer_completed),
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap();
        state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
            snapshot_build: Some(hook),
            ..ControlPlaneRaftStateMachineTestHooks::default()
        });
        let mut builder = RaftStateMachine::get_snapshot_builder(&mut state_machine).await;
        let operation = tokio::spawn(async move { builder.build_snapshot().await.unwrap() });
        let timer = tokio::spawn(mark_executor_timer_progress(Arc::clone(&timer_completed)));

        let snapshot = operation.await.unwrap();
        timer.await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(1, 1, 1)));
        assert!(
            watchdog.join().unwrap(),
            "single-worker executor timer must progress while snapshot build is blocked"
        );
        let current_snapshot = state_machine
            .current_snapshot()
            .expect("completed builder should publish the current snapshot");
        assert_eq!(
            current_snapshot.meta.last_log_id,
            Some(raft_log_id(1, 1, 1)),
        );
        assert!(
            Arc::ptr_eq(
                &snapshot.snapshot.payload,
                &current_snapshot.snapshot.payload,
            ),
            "current-snapshot reads must share rather than copy the payload",
        );
    });
}

#[test]
fn control_plane_raft_late_snapshot_builder_cannot_regress_current_snapshot() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap();
        state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
            snapshot_build: Some(Arc::clone(&hook)),
            ..ControlPlaneRaftStateMachineTestHooks::default()
        });
        let mut old_builder = RaftStateMachine::get_snapshot_builder(&mut state_machine).await;
        let old_build = tokio::spawn(async move { old_builder.build_snapshot().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !hook.entered() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("old snapshot builder should enter its blocking hook");

        state_machine.apply_entry(blank_entry(1, 1, 2)).unwrap();
        state_machine.set_test_hooks(ControlPlaneRaftStateMachineTestHooks::default());
        let newer = state_machine.build_snapshot().unwrap();
        hook.release();
        let older = old_build.await.unwrap();

        assert_eq!(older.meta.last_log_id, Some(raft_log_id(1, 1, 1)));
        assert_eq!(newer.meta.last_log_id, Some(raft_log_id(1, 1, 2)));
        assert_eq!(
            state_machine
                .current_snapshot()
                .and_then(|snapshot| snapshot.meta.last_log_id),
            Some(raft_log_id(1, 1, 2)),
            "a late older builder must not replace a newer current snapshot"
        );
    });
}

#[test]
fn control_plane_raft_snapshot_cache_rejects_conflicting_same_index_publication() {
    let mut state_machine = ControlPlaneRaftStateMachine::empty();
    state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap();
    let current = state_machine.build_snapshot().unwrap();
    let mut conflicting = current.clone();
    conflicting.meta.last_log_id = Some(raft_log_id(1, 2, 1));

    let error = publish_control_plane_raft_snapshot(&state_machine.current_snapshot, conflicting)
        .finish_on_current_thread()
        .unwrap_err();

    assert!(matches!(error, ControlPlaneError::SnapshotDecode { .. }));
    assert_eq!(
        state_machine
            .current_snapshot()
            .and_then(|snapshot| snapshot.meta.last_log_id),
        current.meta.last_log_id,
    );
}

#[test]
fn control_plane_raft_snapshot_install_does_not_block_single_worker_executor() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source.apply_entry(blank_entry(1, 1, 1)).unwrap();
    let snapshot = source.build_snapshot().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
        let timer_completed = Arc::new(AtomicBool::new(false));
        let watchdog = state_machine_executor_progress_watchdog(
            Arc::clone(&hook),
            Arc::clone(&timer_completed),
        );
        let mut target = ControlPlaneRaftStateMachine::empty();
        target.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
            snapshot_install: Some(hook),
            ..ControlPlaneRaftStateMachineTestHooks::default()
        });
        let operation = tokio::spawn(async move {
            RaftStateMachine::install_snapshot(&mut target, &snapshot.meta, snapshot.snapshot)
                .await
                .unwrap();
            target
        });
        let timer = tokio::spawn(mark_executor_timer_progress(Arc::clone(&timer_completed)));

        let target = operation.await.unwrap();
        timer.await.unwrap();
        assert_eq!(target.last_applied(), Some(raft_log_id(1, 1, 1)));
        assert!(
            watchdog.join().unwrap(),
            "single-worker executor timer must progress while snapshot install is blocked"
        );
    });
}

#[test]
fn control_plane_raft_snapshot_publication_retires_state_and_cache_off_executor() {
    let mut source = ControlPlaneRaftStateMachine::empty();
    source
        .apply_entry(normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "node-1".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    source.apply_entry(blank_entry(1, 1, 2)).unwrap();
    let snapshot = source.build_snapshot().unwrap();

    let mut target = ControlPlaneRaftStateMachine::empty();
    target
        .apply_entry(normal_entry(
            1,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(2), "node-2".to_string())],
                pg_ids: vec![PgId::new(0)],
            },
        ))
        .unwrap();
    drop(target.build_snapshot().unwrap());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
            let hook = Arc::new(ControlPlaneRaftStateMachineBlockingHook::default());
            let retirement_entered = Arc::new(AtomicBool::new(false));
            let timer_completed = Arc::new(AtomicBool::new(false));
            let watchdog = state_machine_retirement_progress_watchdog(
                Arc::clone(&hook),
                Arc::clone(&retirement_entered),
                Arc::clone(&timer_completed),
            );
            target.set_test_hooks(ControlPlaneRaftStateMachineTestHooks {
                retire_generation: Some(hook),
                ..ControlPlaneRaftStateMachineTestHooks::default()
            });
            let operation = tokio::spawn(async move {
                RaftStateMachine::install_snapshot(&mut target, &snapshot.meta, snapshot.snapshot)
                    .await
                    .unwrap();
                target
            });
            let timer = tokio::spawn(mark_executor_timer_progress_after_phase_entry(
                retirement_entered,
                Arc::clone(&timer_completed),
            ));

            let target = operation.await.unwrap();
            timer.await.unwrap();
            assert_eq!(target.last_applied(), Some(raft_log_id(1, 1, 2)));
            assert!(
                watchdog.join().unwrap(),
                "single-worker executor timer must progress while replaced state and cached snapshot generations await destruction"
            );
        });
}

#[test]
fn control_plane_raft_openraft_log_suite_compatible_cases_pass() {
    ControlPlaneRaftTypeConfig::run(async {
        async fn suite_pair() -> (ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine) {
            let (_, log_store, state_machine) =
                ControlPlaneOpenRaftSuiteBuilder.build().await.unwrap();
            (log_store, state_machine)
        }

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::last_membership_in_log_initial(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::get_membership_initial(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::get_membership_from_empty_log_and_sm(
            log_store,
            state_machine,
        )
        .await
        .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::get_initial_state_membership_from_empty_log_and_sm(
            log_store,
            state_machine,
        )
        .await
        .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::get_initial_state_membership_from_log_insm_is_smaller(
            log_store,
            state_machine,
        )
        .await
        .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::get_initial_state_without_init(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::initial_logs(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::save_vote(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::snapshot_meta(log_store, state_machine)
            .await
            .unwrap();

        let (log_store, state_machine) = suite_pair().await;
        ControlPlaneOpenRaftLogSuite::snapshot_meta_optional(log_store, state_machine)
            .await
            .unwrap();
    });
}

#[test]
fn control_plane_raft_openraft_log_suite_documents_index_zero_deviation() {
    ControlPlaneRaftTypeConfig::run(async {
        let (_, log_store, state_machine) = ControlPlaneOpenRaftSuiteBuilder.build().await.unwrap();
        let err = ControlPlaneOpenRaftLogSuite::get_log_state(log_store, state_machine)
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("log index 0 entry must be bootstrap membership"));
    });
}

#[test]
fn control_plane_raft_log_store_tracks_vote_committed_and_visible_entries() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);

        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        assert_eq!(
            RaftLogReader::read_vote(&mut store).await.unwrap(),
            Some(vote)
        );

        RaftLogStorage::append(
            &mut store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();

        let mut reader = RaftLogStorage::get_log_reader(&mut store).await;
        RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 3)], IOFlushed::noop())
            .await
            .unwrap();

        let entries = RaftLogReader::try_get_log_entries(&mut reader, 0..4)
            .await
            .unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
            vec![
                raft_log_id(0, 1, 0),
                raft_log_id(3, 1, 1),
                raft_log_id(3, 1, 2),
                raft_log_id(3, 1, 3),
            ]
        );

        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(raft_log_id(3, 1, 2))
        );

        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, None);
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));
    });
}

#[test]
fn control_plane_raft_log_store_rejects_vote_regression() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        let vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);

        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();

        let lower_term = Vote::<ControlPlaneRaftLeaderId>::new(2, 99);
        let err = RaftLogStorage::save_vote(&mut store, &lower_term)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regress"));
        assert_eq!(
            RaftLogReader::read_vote(&mut store).await.unwrap(),
            Some(vote)
        );

        let lower_node_same_term = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
        let err = RaftLogStorage::save_vote(&mut store, &lower_node_same_term)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regress"));

        let committed = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 2);
        RaftLogStorage::save_vote(&mut store, &committed)
            .await
            .unwrap();

        let uncommitted_same_leader = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);
        let err = RaftLogStorage::save_vote(&mut store, &uncommitted_same_leader)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regress"));
        assert_eq!(
            RaftLogReader::read_vote(&mut store).await.unwrap(),
            Some(committed)
        );

        let higher = Vote::<ControlPlaneRaftLeaderId>::new(4, 1);
        RaftLogStorage::save_vote(&mut store, &higher)
            .await
            .unwrap();
        assert_eq!(
            RaftLogReader::read_vote(&mut store).await.unwrap(),
            Some(higher)
        );
    });
}

#[test]
fn control_plane_raft_log_store_rejects_invalid_committed_watermarks() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();

        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("log is empty"));
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            None
        );

        RaftLogStorage::append(
            &mut store,
            vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            IOFlushed::noop(),
        )
        .await
        .unwrap();
        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing vote state"));

        let lower_node_same_term_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0);
        RaftLogStorage::save_vote(&mut store, &lower_node_same_term_vote)
            .await
            .unwrap();
        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not cover"));

        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regress"));

        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(4, 1, 2)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cannot change"));

        let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 3)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("current last log id"));

        let err = RaftLogStorage::save_committed(&mut store, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cannot clear"));

        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(raft_log_id(3, 1, 2))
        );
    });
}

#[test]
fn control_plane_raft_log_store_rejects_truncating_committed_entries() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut store,
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
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("committed log id"));

        let err = RaftLogStorage::truncate_after(&mut store, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("committed log id"));

        RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(raft_log_id(3, 1, 2))
        );
    });
}

#[test]
fn control_plane_raft_log_store_rejects_unknown_truncate_boundaries() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut store,
            vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
            IOFlushed::noop(),
        )
        .await
        .unwrap();

        let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("current last log id"));

        let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("mismatched log id"));

        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();
        RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 1))
            .await
            .unwrap();
        let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("mismatched purged log id"));

        RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 1)));
    });
}

#[test]
fn control_plane_raft_log_store_promotes_committed_gate_when_snapshot_purge_passes_it() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        RaftLogStorage::append(
            &mut store,
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
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
            .await
            .unwrap();

        RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 3))
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 3)));
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(raft_log_id(3, 1, 3))
        );
        let entries = RaftLogReader::try_get_log_entries(&mut store, 0..4)
            .await
            .unwrap();
        assert!(entries.is_empty());
    });
}

#[test]
fn control_plane_raft_log_store_allows_empty_snapshot_purge_to_establish_committed_gate() {
    ControlPlaneRaftTypeConfig::run(async {
        let mut store = ControlPlaneRaftLogStore::empty();
        let snapshot_log_id = raft_log_id(3, 1, 7);

        let err = RaftLogStorage::purge(&mut store, snapshot_log_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing vote state"));

        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
        RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
        RaftLogStorage::purge(&mut store, snapshot_log_id)
            .await
            .unwrap();
        let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(snapshot_log_id));
        assert_eq!(log_state.last_log_id, Some(snapshot_log_id));
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(snapshot_log_id)
        );

        let err = RaftLogStorage::purge(&mut store, raft_log_id(4, 1, 8))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not cover"));
        assert_eq!(
            RaftLogStorage::read_committed(&mut store).await.unwrap(),
            Some(snapshot_log_id)
        );
    });
}
