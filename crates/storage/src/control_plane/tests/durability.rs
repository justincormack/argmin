// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn authority_clock_durable_state_binding_matches_frozen_versioned_vectors() {
    const SINGLE_AUTHORITY_BINDING: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    const RAFT_BINDING: [u8; 32] = [
        0x91, 0xe1, 0xa6, 0x75, 0x3f, 0x6c, 0x53, 0xb3, 0xb4, 0x8f, 0x00, 0xc9, 0xfc, 0x59, 0x00,
        0xfd, 0x68, 0xf2, 0x5b, 0x4b, 0xe2, 0x6b, 0xda, 0xa7, 0x68, 0x61, 0x52, 0x19, 0x81, 0xec,
        0x4a, 0x5f,
    ];
    const BINDING_OFFSET: usize = 8 + 2;
    const BINDING_END: usize = BINDING_OFFSET + 32;

    assert_eq!(CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN, 32);
    assert_eq!(CONTROL_PLANE_STATE_IDENTITY_VERSION, 1);
    assert_eq!(SINGLE_AUTHORITY_INITIALIZED_VERSION, 1);
    assert_eq!(CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION, 2);
    assert_eq!(SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION, 2);
    assert_eq!(
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-clustér", 1),
        ControlPlaneAuthorityClockCheckpointBinding(RAFT_BINDING)
    );

    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding(SINGLE_AUTHORITY_BINDING);
    store_single_authority_clock_checkpoint_binding(&state_path, binding).unwrap();
    store_single_authority_initialized_binding(&state_path, binding).unwrap();
    let identity = std::fs::read(single_authority_identity_path(&state_path)).unwrap();
    let initialized = std::fs::read(single_authority_initialized_path(&state_path)).unwrap();
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(2), 3, 4).encode();
    let journal_record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: 5,
        resulting_chain_digest: 5,
        command: None,
    }
    .encode()
    .unwrap();

    for encoded in [&identity, &initialized, &checkpoint, &journal_record] {
        assert_eq!(
            &encoded[BINDING_OFFSET..BINDING_END],
            &SINGLE_AUTHORITY_BINDING
        );
    }
}

#[test]
fn single_authority_durable_identity_v1_layout_and_format_failures_are_exact() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]);
    let current = encode_single_authority_clock_checkpoint_binding(binding);
    assert_eq!(
        hex_encode(&current),
        "41524743504944000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1ff53237eea69307b9"
    );
    assert_eq!(
        decode_single_authority_clock_checkpoint_binding(&current).unwrap(),
        binding
    );

    for truncated_len in [0, 7, 8, 9, CONTROL_PLANE_STATE_IDENTITY_LEN - 1] {
        assert!(matches!(
            decode_single_authority_clock_checkpoint_binding(&current[..truncated_len]),
            Err(SingleAuthorityIdentityDecodeError::Format(
                SingleAuthorityIdentityFormatError::Truncated
            ))
        ));
    }

    let mut oversized = current.clone();
    oversized.push(0);
    assert!(matches!(
        decode_single_authority_clock_checkpoint_binding(&oversized),
        Err(SingleAuthorityIdentityDecodeError::Invalid(
            ControlPlaneError::AuthorityClockCheckpoint { message }
        )) if message.contains("length")
    ));

    let mut bad_checksum = current.clone();
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert!(matches!(
        decode_single_authority_clock_checkpoint_binding(&bad_checksum),
        Err(SingleAuthorityIdentityDecodeError::Invalid(
            ControlPlaneError::AuthorityClockCheckpoint { message }
        )) if message == "single-authority durable identity checksum mismatch"
    ));

    let mut bad_magic = current.clone();
    bad_magic[0] ^= 0xff;
    reseal_crc64_suffix(&mut bad_magic);
    assert!(matches!(
        decode_single_authority_clock_checkpoint_binding(&bad_magic),
        Err(SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::UnknownMagic
        ))
    ));

    for version in [0u16, 2] {
        let mut unsupported = current.clone();
        let version_offset = CONTROL_PLANE_STATE_IDENTITY_MAGIC.len();
        unsupported[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut unsupported);
        assert!(matches!(
            decode_single_authority_clock_checkpoint_binding(&unsupported),
            Err(SingleAuthorityIdentityDecodeError::Format(
                SingleAuthorityIdentityFormatError::UnsupportedVersion(actual)
            )) if actual == version
        ));
    }

    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    assert!(
        load_single_authority_clock_checkpoint_binding_classified(&state_path)
            .unwrap()
            .is_none()
    );
    let identity_path = single_authority_identity_path(&state_path);
    std::fs::write(&identity_path, &current[..9]).unwrap();
    assert!(matches!(
        load_single_authority_clock_checkpoint_binding_classified(&state_path),
        Err(SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::Truncated
        ))
    ));
}

#[test]
fn single_authority_initialization_marker_v1_layout_and_format_failures_are_exact() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]);
    let current = encode_single_authority_initialized_binding(binding);
    assert_eq!(
        hex_encode(&current),
        "4152474350494e490001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f5cc8caa13fc3b183"
    );
    assert_eq!(
        decode_single_authority_initialized_binding(&current).unwrap(),
        binding
    );

    for truncated_len in [0, 7, 8, 9, SINGLE_AUTHORITY_INITIALIZED_LEN - 1] {
        assert!(matches!(
            decode_single_authority_initialized_binding(&current[..truncated_len]),
            Err(SingleAuthorityInitializationMarkerDecodeError::Format(
                SingleAuthorityInitializationMarkerFormatError::Truncated
            ))
        ));
    }

    let mut oversized = current.clone();
    oversized.push(0);
    assert!(matches!(
        decode_single_authority_initialized_binding(&oversized),
        Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::InvalidLength
        ))
    ));

    let mut bad_checksum = current.clone();
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert!(matches!(
        decode_single_authority_initialized_binding(&bad_checksum),
        Err(SingleAuthorityInitializationMarkerDecodeError::Invalid(
            ControlPlaneError::CommandDecode { message }
        )) if message == "single-authority initialization marker checksum mismatch"
    ));

    let mut bad_magic = current.clone();
    bad_magic[0] ^= 0xff;
    reseal_crc64_suffix(&mut bad_magic);
    assert!(matches!(
        decode_single_authority_initialized_binding(&bad_magic),
        Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::UnknownMagic
        ))
    ));

    for version in [0u16, 2] {
        let mut unsupported = current.clone();
        let version_offset = SINGLE_AUTHORITY_INITIALIZED_MAGIC.len();
        unsupported[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut unsupported);
        assert!(matches!(
            decode_single_authority_initialized_binding(&unsupported),
            Err(SingleAuthorityInitializationMarkerDecodeError::Format(
                SingleAuthorityInitializationMarkerFormatError::UnsupportedVersion(actual)
            )) if actual == version
        ));
    }

    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    assert!(
        load_single_authority_initialized_binding_classified(&state_path)
            .unwrap()
            .is_none()
    );
    let marker_path = single_authority_initialized_path(&state_path);
    std::fs::write(&marker_path, &current[..9]).unwrap();
    assert!(matches!(
        load_single_authority_initialized_binding_classified(&state_path),
        Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::Truncated
        ))
    ));
    std::fs::write(&marker_path, &oversized).unwrap();
    assert!(matches!(
        load_single_authority_initialized_binding_classified(&state_path),
        Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::InvalidLength
        ))
    ));
    assert!(matches!(
        load_single_authority_initialized_binding(&state_path),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "single-authority initialization marker length does not match required fixed length"
    ));
}

#[test]
fn single_authority_initialization_marker_binding_mismatch_fails_before_journal_replay() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    drop(SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap());

    store_single_authority_initialized_binding(
        &state_path,
        ControlPlaneAuthorityClockCheckpointBinding([0x7b; 32]),
    )
    .unwrap();
    let journal_path = single_authority_journal_path(&state_path);
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .unwrap();
    journal.write_all(&[0xa3, 0xc1]).unwrap();
    journal.sync_all().unwrap();
    drop(journal);
    let snapshot_before = std::fs::read(&state_path).unwrap();
    let identity_before = std::fs::read(single_authority_identity_path(&state_path)).unwrap();
    let marker_before = std::fs::read(single_authority_initialized_path(&state_path)).unwrap();
    let journal_before = std::fs::read(&journal_path).unwrap();

    assert!(matches!(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "single-authority initialization marker identity does not match durable state"
    ));
    assert_eq!(std::fs::read(&state_path).unwrap(), snapshot_before);
    assert_eq!(
        std::fs::read(single_authority_identity_path(&state_path)).unwrap(),
        identity_before
    );
    assert_eq!(
        std::fs::read(single_authority_initialized_path(&state_path)).unwrap(),
        marker_before
    );
    assert_eq!(std::fs::read(&journal_path).unwrap(), journal_before);
}

#[test]
fn previous_v28_v15_journal_chain_vectors_remain_immutable_evidence() {
    const EMPTY_STATE_V28: &str = concat!(
        "version=28\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0xf8f1_78f8_a5db_b7ba;
    const FIRST_COMMAND_V15: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x0f, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x07, 0x02, 0x5d, 0xf8, 0xc2, 0xf9, 0x75, 0x37, 0x8d, 0x2c,
    ];
    const FIRST_CHAIN_DIGEST: u64 = 0x1096_0ae6_13f1_ca37;
    const SECOND_COMMAND_V15: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x0f, 0x00, 0x03, 0x00, 0x00, 0x00,
        0x09, 0x03, 0x6d, 0x6f, 0x57, 0x14, 0x95, 0x82, 0x61, 0xae,
    ];
    const SECOND_CHAIN_DIGEST: u64 = 0xbc55_3777_1bb4_1d01;
    const COMMAND_RECORD_V2: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x53, 0x4a, 0x52, 0x00, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
        0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0xf8, 0xf1, 0x78,
        0xf8, 0xa5, 0xdb, 0xb7, 0xba, 0x10, 0x96, 0x0a, 0xe6, 0x13, 0xf1, 0xca, 0x37, 0x02, 0x00,
        0x00, 0x00, 0x19, 0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x0f, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x07, 0x02, 0x5d, 0xf8, 0xc2, 0xf9, 0x75, 0x37, 0x8d, 0x2c, 0x05, 0x39,
        0x42, 0x43, 0x05, 0x5b, 0x53, 0x20,
    ];

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V28.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, FIRST_COMMAND_V15),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, SECOND_COMMAND_V15),
        SECOND_CHAIN_DIGEST
    );
    assert!(matches!(
        decode_control_plane_command(FIRST_COMMAND_V15),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 15"
    ));
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(COMMAND_RECORD_V2),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 15"
    ));
}

#[test]
fn previous_v29_v16_journal_chain_vectors_remain_immutable_evidence() {
    const EMPTY_STATE_V29: &str = concat!(
        "version=29\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0x2872_a5a1_e84d_8627;
    const FIRST_COMMAND_V16: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x10, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x07, 0x02, 0xf4, 0xd1, 0x3b, 0x45, 0x1c, 0x2e, 0xcb, 0x75,
    ];
    const FIRST_CHAIN_DIGEST: u64 = 0x7e8d_5c0d_38e3_eead;
    const SECOND_COMMAND_V16: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00, 0x00,
        0x09, 0x03, 0xc4, 0x46, 0xae, 0xa8, 0xfc, 0x9b, 0x27, 0xf7,
    ];
    const SECOND_CHAIN_DIGEST: u64 = 0xdc3f_ad96_43b7_50b0;
    const COMMAND_RECORD_V2: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x53, 0x4a, 0x52, 0x00, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
        0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x28, 0x72, 0xa5,
        0xa1, 0xe8, 0x4d, 0x86, 0x27, 0x7e, 0x8d, 0x5c, 0x0d, 0x38, 0xe3, 0xee, 0xad, 0x02, 0x00,
        0x00, 0x00, 0x19, 0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x10, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x07, 0x02, 0xf4, 0xd1, 0x3b, 0x45, 0x1c, 0x2e, 0xcb, 0x75, 0xd5, 0x32,
        0xeb, 0x88, 0x26, 0xf4, 0xf3, 0x00,
    ];

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V29.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, FIRST_COMMAND_V16),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, SECOND_COMMAND_V16),
        SECOND_CHAIN_DIGEST
    );
    assert!(matches!(
        decode_control_plane_command(FIRST_COMMAND_V16),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 16"
    ));
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(COMMAND_RECORD_V2),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 16"
    ));
}

#[test]
fn previous_v30_v17_journal_chain_vectors_remain_immutable_evidence() {
    const EMPTY_STATE_V30: &str = concat!(
        "version=30\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0x3475_5119_a7c3_a1ae;
    const FIRST_COMMAND_V17: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x11, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x07, 0x02, 0xd5, 0x38, 0x4d, 0x5b, 0x39, 0x08, 0xea, 0xd9,
    ];
    const FIRST_CHAIN_DIGEST: u64 = 0xcf1a_28f2_b781_9c42;
    const SECOND_COMMAND_V17: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x11, 0x00, 0x03, 0x00, 0x00, 0x00,
        0x09, 0x03, 0xe5, 0xaf, 0xd8, 0xb6, 0xd9, 0xbd, 0x06, 0x5b,
    ];
    const SECOND_CHAIN_DIGEST: u64 = 0xcf12_9677_7eda_9032;
    const COMMAND_RECORD_V2: &[u8] = &[
        0x41, 0x52, 0x47, 0x43, 0x50, 0x53, 0x4a, 0x52, 0x00, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
        0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x34, 0x75, 0x51,
        0x19, 0xa7, 0xc3, 0xa1, 0xae, 0xcf, 0x1a, 0x28, 0xf2, 0xb7, 0x81, 0x9c, 0x42, 0x02, 0x00,
        0x00, 0x00, 0x19, 0x41, 0x52, 0x47, 0x43, 0x50, 0x43, 0x4d, 0x44, 0x00, 0x11, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x07, 0x02, 0xd5, 0x38, 0x4d, 0x5b, 0x39, 0x08, 0xea, 0xd9, 0x8d, 0x97,
        0x8e, 0x43, 0xb4, 0x30, 0x4d, 0x0d,
    ];

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V30.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, FIRST_COMMAND_V17),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, SECOND_COMMAND_V17),
        SECOND_CHAIN_DIGEST
    );
    for command in [FIRST_COMMAND_V17, SECOND_COMMAND_V17] {
        assert!(matches!(
            decode_control_plane_command(command),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 17"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(COMMAND_RECORD_V2),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 17"
    ));
}

#[test]
fn previous_v31_v19_single_authority_journal_chain_remains_immutable_evidence() {
    const EMPTY_STATE_V31: &str = concat!(
        "version=31\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0xe4f6_8c40_ea55_9033;
    const FIRST_COMMAND_V19: &[u8] = &[
        65, 82, 71, 67, 80, 67, 77, 68, 0, 19, 0, 2, 0, 0, 0, 7, 2, 150, 234, 161, 103, 115, 68,
        169, 129,
    ];
    const FIRST_CHAIN_DIGEST: u64 = 0xf65f_9524_9d3f_56ee;
    const SECOND_COMMAND_V19: &[u8] = &[
        65, 82, 71, 67, 80, 67, 77, 68, 0, 19, 0, 3, 0, 0, 0, 9, 3, 166, 125, 52, 138, 147, 241,
        69, 3,
    ];
    const SECOND_CHAIN_DIGEST: u64 = 0x9239_1747_bf63_856e;
    const COMMAND_RECORD_V2: &[u8] = &[
        65, 82, 71, 67, 80, 83, 74, 82, 0, 2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 228, 246, 140, 64, 234, 85,
        144, 51, 246, 95, 149, 36, 157, 63, 86, 238, 2, 0, 0, 0, 25, 65, 82, 71, 67, 80, 67, 77,
        68, 0, 19, 0, 2, 0, 0, 0, 7, 2, 150, 234, 161, 103, 115, 68, 169, 129, 63, 91, 31, 210,
        248, 109, 4, 108,
    ];

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V31.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, FIRST_COMMAND_V19),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, SECOND_COMMAND_V19),
        SECOND_CHAIN_DIGEST
    );
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V31),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 31"
    ));
    for command in [FIRST_COMMAND_V19, SECOND_COMMAND_V19] {
        assert!(matches!(
            decode_control_plane_command(command),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 19"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(COMMAND_RECORD_V2),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 19"
    ));
}

#[test]
fn previous_v32_v20_single_authority_journal_chain_remains_immutable_evidence() {
    const EMPTY_STATE_V32: &str = concat!(
        "version=32\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0xa1ab_cdf8_6478_51ff;
    const FIRST_COMMAND_V20: &[u8] = &[
        65, 82, 71, 67, 80, 67, 77, 68, 0, 20, 0, 2, 0, 0, 0, 7, 2, 115, 116, 227, 61, 136, 182,
        77, 197,
    ];
    const FIRST_CHAIN_DIGEST: u64 = 0x9d27_5aea_18aa_abf2;
    const SECOND_COMMAND_V20: &[u8] = &[
        65, 82, 71, 67, 80, 67, 77, 68, 0, 20, 0, 3, 0, 0, 0, 9, 3, 67, 227, 118, 208, 104, 3, 161,
        71,
    ];
    const SECOND_CHAIN_DIGEST: u64 = 0xeff7_2072_5634_3796;
    const COMMAND_RECORD_V2: &[u8] = &[
        65, 82, 71, 67, 80, 83, 74, 82, 0, 2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 161, 171, 205, 248, 100,
        120, 81, 255, 157, 39, 90, 234, 24, 170, 171, 242, 2, 0, 0, 0, 25, 65, 82, 71, 67, 80, 67,
        77, 68, 0, 20, 0, 2, 0, 0, 0, 7, 2, 115, 116, 227, 61, 136, 182, 77, 197, 215, 61, 253, 26,
        185, 36, 132, 52,
    ];

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V32.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, FIRST_COMMAND_V20),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, SECOND_COMMAND_V20),
        SECOND_CHAIN_DIGEST
    );
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V32),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 32"
    ));
    for command in [FIRST_COMMAND_V20, SECOND_COMMAND_V20] {
        assert!(matches!(
            decode_control_plane_command(command),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 20"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(COMMAND_RECORD_V2),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 20"
    ));
}

#[test]
fn previous_v33_v21_single_authority_journal_chain_remains_immutable_evidence() {
    const EMPTY_STATE_V33: &str = concat!(
        "version=33\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0x7128_10a1_29ee_6062;
    const FIRST_CHAIN_DIGEST: u64 = 0x1276_1465_0423_8987;
    const SECOND_CHAIN_DIGEST: u64 = 0x5f3d_c239_8b95_617b;
    let first_command =
        hex_decode(0, "4152474350434d44001500020000000702529d9523ad906c69").unwrap();
    let second_command =
        hex_decode(0, "4152474350434d44001500030000000903620a00ce4d2580eb").unwrap();
    let command_record = hex_decode(
        0,
        "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f712810a129ee6062127614650423898702000000194152474350434d44001500020000000702529d9523ad906c69c71cbb50fc798244",
    )
    .unwrap();

    assert_eq!(
        checksum::crc64::checksum(EMPTY_STATE_V33.as_bytes()),
        SNAPSHOT_SEED
    );
    assert_eq!(
        single_authority_command_chain_digest(SNAPSHOT_SEED, &first_command),
        FIRST_CHAIN_DIGEST
    );
    assert_eq!(
        single_authority_command_chain_digest(FIRST_CHAIN_DIGEST, &second_command),
        SECOND_CHAIN_DIGEST
    );
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V33),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 33"
    ));
    for command in [&first_command, &second_command] {
        assert!(matches!(
            decode_control_plane_command(command),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 21"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&command_record),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 21"
    ));
}

#[test]
fn v34_v22_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V34: &str = concat!(
        "version=34\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d4400160002000000070230a60f01c2fa0e9d";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2b114e897823d26730ace8bc9690feaf02000000194152474350434d4400160002000000070230a60f01c2fa0e9dfecda3b57b9d51ed";

    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V34),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 34"
    ));
    assert!(matches!(
        decode_control_plane_command(&hex_decode(0, FIRST_COMMAND).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 22"
    ));
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 22"
    ));
}

#[test]
fn historical_v36_v24_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V36: &str = concat!(
        "version=36\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d44001800020000000702cf43ade76d88557e";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1fbecfd268bb98223630fc84080feee32b02000000194152474350434d44001800020000000702cf43ade76d88557e0edec5be94cdd840";

    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V36),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 36"
    ));
    assert!(matches!(
        decode_control_plane_command(&hex_decode(0, FIRST_COMMAND).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 24"
    ));
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 24"
    ));
}

#[test]
fn historical_v37_v25_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V37: &str = concat!(
        "version=37\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d44001900020000000702eeaadbf948ae74d2";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f6e4c0f31f60e13abbfadca871367c15e02000000194152474350434d44001900020000000702eeaadbf948ae74d21eff83f4d190de30";

    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V37),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 37"
    ));
    assert!(matches!(
        decode_control_plane_command(&hex_decode(0, FIRST_COMMAND).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 25"
    ));
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 25"
    ));
}

#[test]
fn historical_v39_v27_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V39: &str = concat!(
        "version=39\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d44001b00020000000702ad7837c502e2378a";
    const SECOND_COMMAND: &str = "4152474350434d44001b000300000009039defa228e257db08";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1fda3eb361559577a145938dcd59affc7502000000194152474350434d44001b00020000000702ad7837c502e2378aa669b4bd5638f839";
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V39),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 39"
    ));
    for command in [FIRST_COMMAND, SECOND_COMMAND] {
        assert!(matches!(
            decode_control_plane_command(&hex_decode(0, command).unwrap()),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 27"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 27"
    ));
}

#[test]
fn historical_v40_v28_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V40: &str = concat!(
        "version=40\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d44001c0002000000070248e6759ff910d3ce";
    const SECOND_COMMAND: &str = "4152474350434d44001c000300000009037871e07219a53f4c";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f82237dfc27e653eadd9414b2cf480f4802000000194152474350434d44001c0002000000070248e6759ff910d3ce08a46afa5d762af0";
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V40),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 40"
    ));
    for command in [FIRST_COMMAND, SECOND_COMMAND] {
        assert!(matches!(
            decode_control_plane_command(&hex_decode(0, command).unwrap()),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 28"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 28"
    ));
}

#[test]
fn current_v41_v29_single_authority_journal_chain_matches_frozen_v3_vectors() {
    const EMPTY_STATE_V41: &str = concat!(
        "version=41\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const SNAPSHOT_SEED: u64 = 0x52a0_a0a5_6a70_6277;

    let snapshot = ClusterControlSnapshot::empty();
    assert_eq!(format_snapshot(&snapshot), EMPTY_STATE_V41);
    assert_eq!(single_authority_snapshot_digest(&snapshot), SNAPSHOT_SEED);
    let first_command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(7),
        membership: NodeMembershipState::Active,
    };
    let encoded_first = encode_control_plane_command(&first_command).unwrap();
    let first_chain_digest = single_authority_command_chain_digest(SNAPSHOT_SEED, &encoded_first);
    let second_command = ControlPlaneCommand::MarkNodeAvailability {
        node_id: NodeId::new(9),
        availability: NodeAvailabilityState::Unavailable,
    };
    let encoded_second = encode_control_plane_command(&second_command).unwrap();
    let second_chain_digest =
        single_authority_command_chain_digest(first_chain_digest, &encoded_second);
    let binding = ControlPlaneAuthorityClockCheckpointBinding([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]);
    let record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: SNAPSHOT_SEED,
        resulting_chain_digest: first_chain_digest,
        command: Some(first_command),
    };
    let encoded_record = record.encode().unwrap();
    let decoded = SingleAuthorityJournalRecord::decode(&encoded_record).unwrap();
    assert_eq!(decoded.binding, binding);
    assert_eq!(decoded.previous_chain_digest, SNAPSHOT_SEED);
    assert_eq!(decoded.resulting_chain_digest, first_chain_digest);
    assert_eq!(decoded.command, record.command);
    assert_eq!(
        (
            hex_encode(&encoded_first),
            first_chain_digest,
            hex_encode(&encoded_second),
            second_chain_digest,
            hex_encode(&encoded_record),
        ),
        (
            "4152474350434d44001d00020000000702690f0381dc36f262".to_string(),
            5_964_272_503_114_247_485,
            "4152474350434d44001d000300000009035998966c3c831ee0".to_string(),
            275_149_680_868_117_224,
            "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f52a0a0a56a70627752c55a3dd3c12d3d02000000194152474350434d44001d00020000000702690f0381dc36f26218852cb0182b2c80".to_string(),
        )
    );
}

#[test]
fn historical_v38_v26_single_authority_journal_chain_remains_rejected_evidence() {
    const EMPTY_STATE_V38: &str = concat!(
        "version=38\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=-\n",
        "lease_grant_horizon=-\n",
    );
    const FIRST_COMMAND: &str = "4152474350434d44001a000200000007028c9141db27c41626";
    const SECOND_COMMAND: &str = "4152474350434d44001a00030000000903bc06d436c771faa4";
    const RECORD: &str = "4152474350534a520002000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0abd6e381803463ccac2c3424526de0002000000194152474350434d44001a000200000007028c9141db27c41626b648f2f71365fe49";
    assert!(matches!(
        parse_snapshot(EMPTY_STATE_V38),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 38"
    ));
    for command in [FIRST_COMMAND, SECOND_COMMAND] {
        assert!(matches!(
            decode_control_plane_command(&hex_decode(0, command).unwrap()),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported control-plane command version 26"
        ));
    }
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&hex_decode(0, RECORD).unwrap()),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 26"
    ));
}

#[test]
fn single_authority_journal_record_v2_format_failures_are_exact() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);
    let checkpoint = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: 7,
        resulting_chain_digest: 7,
        command: None,
    }
    .encode()
    .unwrap();
    assert_eq!(
        SingleAuthorityJournalRecord::decode_classified(&checkpoint)
            .unwrap()
            .binding,
        binding
    );

    for truncated_len in [
        0,
        SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len() - 1,
        SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len(),
        SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len() + 1,
        checkpoint.len() - 1,
    ] {
        assert!(matches!(
            SingleAuthorityJournalRecord::decode_classified(&checkpoint[..truncated_len]),
            Err(SingleAuthorityJournalRecordDecodeError::Format(
                SingleAuthorityJournalRecordFormatError::Truncated
            ))
        ));
    }

    let mut bad_checksum = checkpoint.clone();
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert!(matches!(
        SingleAuthorityJournalRecord::decode_classified(&bad_checksum),
        Err(SingleAuthorityJournalRecordDecodeError::Invalid(
            ControlPlaneError::CommandDecode { message }
        )) if message.contains("journal record checksum mismatch")
    ));

    let mut bad_magic = checkpoint.clone();
    bad_magic[0] ^= 0xff;
    reseal_crc64_suffix(&mut bad_magic);
    assert!(matches!(
        SingleAuthorityJournalRecord::decode_classified(&bad_magic),
        Err(SingleAuthorityJournalRecordDecodeError::Format(
            SingleAuthorityJournalRecordFormatError::UnknownMagic
        ))
    ));

    for version in [1u16, 3] {
        let mut unsupported = checkpoint.clone();
        let version_offset = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        unsupported[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut unsupported);
        assert!(matches!(
            SingleAuthorityJournalRecord::decode_classified(&unsupported),
            Err(SingleAuthorityJournalRecordDecodeError::Format(
                SingleAuthorityJournalRecordFormatError::UnsupportedVersion(actual)
            )) if actual == version
        ));
    }

    let invalid_kind = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: 7,
        resulting_chain_digest: 7,
        command: None,
    }
    .encode_parts(0xff, &[])
    .unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode_classified(&invalid_kind),
        Err(SingleAuthorityJournalRecordDecodeError::Invalid(
            ControlPlaneError::CommandDecode { message }
        )) if message == "invalid single-authority control-plane journal record kind 255"
    ));

    for invalid_kind_length in [
        SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: 7,
            resulting_chain_digest: 7,
            command: None,
        }
        .encode_parts(SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT, &[0])
        .unwrap(),
        SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: 7,
            resulting_chain_digest: 7,
            command: None,
        }
        .encode_parts(SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND, &[])
        .unwrap(),
    ] {
        assert!(matches!(
            SingleAuthorityJournalRecord::decode_classified(&invalid_kind_length),
            Err(SingleAuthorityJournalRecordDecodeError::Invalid(
                ControlPlaneError::CommandDecode { message }
            )) if message == "single-authority control-plane journal record kind has invalid command length"
        ));
    }

    let mut mismatched_length = checkpoint.clone();
    let command_length_offset = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
        + std::mem::size_of::<u16>()
        + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
        + 2 * std::mem::size_of::<u64>()
        + std::mem::size_of::<u8>();
    mismatched_length[command_length_offset..command_length_offset + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    reseal_crc64_suffix(&mut mismatched_length);
    assert!(matches!(
        SingleAuthorityJournalRecord::decode_classified(&mismatched_length),
        Err(SingleAuthorityJournalRecordDecodeError::Invalid(
            ControlPlaneError::CommandDecode { message }
        )) if message == "single-authority control-plane journal command length mismatch"
    ));

    let invalid_inner_command =
        SingleAuthorityJournalRecord::encode_command_bytes_for_test(binding, 7, b"invalid")
            .unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode_classified(&invalid_inner_command),
        Err(SingleAuthorityJournalRecordDecodeError::Invalid(
            ControlPlaneError::CommandDecode { message }
        )) if message == "truncated control-plane command payload"
    ));
}

#[test]
fn historical_v36_v24_single_authority_journal_file_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!("../testdata/single_authority_journal_v36_v24.bin");
    assert_eq!(
        (BYTES.len(), hex_encode(&checksum::sha256::digest(BYTES))),
        (
            209,
            "545afdca86e2ee56fe457b494a7c17ba6b3474d249c660550ef72828ff3efa19".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    std::fs::write(store.journal_path(), BYTES).unwrap();
    let frames = store.journal.read_frames_from(0).unwrap();
    assert_eq!(frames.frames.len(), 2);
    SingleAuthorityJournalRecord::decode(&frames.frames[0]).unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&frames.frames[1]),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 24"
    ));
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), BYTES);
}

#[test]
fn historical_v37_v25_single_authority_journal_file_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!("../testdata/single_authority_journal_v37_v25.bin");
    assert_eq!(
        (BYTES.len(), hex_encode(&checksum::sha256::digest(BYTES))),
        (
            209,
            "cff94f34f97b18d905df19e069cf85b2d5771426239daa5e7fa0b472277f1321".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    std::fs::write(store.journal_path(), BYTES).unwrap();
    let frames = store.journal.read_frames_from(0).unwrap();
    assert_eq!(frames.frames.len(), 2);
    SingleAuthorityJournalRecord::decode(&frames.frames[0]).unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&frames.frames[1]),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 25"
    ));
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), BYTES);
}

#[test]
fn historical_v38_v26_single_authority_journal_file_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!("../testdata/single_authority_journal_v38_v26.bin");
    assert_eq!(
        (BYTES.len(), hex_encode(&checksum::sha256::digest(BYTES))),
        (
            209,
            "affaa37a0c1c52f05be1941257e53e004d17e4b4de04972a438ddd9cf1da7096".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    std::fs::write(store.journal_path(), BYTES).unwrap();
    let frames = store.journal.read_frames_from(0).unwrap();
    assert_eq!(frames.frames.len(), 2);
    SingleAuthorityJournalRecord::decode(&frames.frames[0]).unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&frames.frames[1]),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 26"
    ));
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), BYTES);
}

#[test]
fn historical_v39_v27_single_authority_journal_file_remains_rejected_evidence() {
    const BYTES: &[u8] = include_bytes!("../testdata/single_authority_journal_v39_v27.bin");
    assert_eq!(
        (BYTES.len(), hex_encode(&checksum::sha256::digest(BYTES))),
        (
            209,
            "5fb3fc0bcdd3c13708fd5a5f73df50c3ea8edf9d2fcb35c49f27f0fd44ab3734".to_owned()
        )
    );
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    std::fs::write(store.journal_path(), BYTES).unwrap();
    let frames = store.journal.read_frames_from(0).unwrap();
    assert_eq!(frames.frames.len(), 2);
    SingleAuthorityJournalRecord::decode(&frames.frames[0]).unwrap();
    assert!(matches!(
        SingleAuthorityJournalRecord::decode(&frames.frames[1]),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "unsupported control-plane command version 27"
    ));
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), BYTES);
}

#[test]
fn single_authority_journal_v2_full_file_layout_is_exact() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x5a; 32]);
    let snapshot_digest = single_authority_snapshot_digest(&ClusterControlSnapshot::empty());
    let checkpoint = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: snapshot_digest,
        resulting_chain_digest: snapshot_digest,
        command: None,
    }
    .encode()
    .unwrap();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(7),
        membership: NodeMembershipState::Active,
    };
    let encoded_command = encode_control_plane_command(&command).unwrap();
    let command_record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: snapshot_digest,
        resulting_chain_digest: single_authority_command_chain_digest(
            snapshot_digest,
            &encoded_command,
        ),
        command: Some(command),
    }
    .encode()
    .unwrap();

    store.journal.append_frame(&checkpoint).unwrap();
    store.journal.append_frame(&command_record).unwrap();

    let bytes = std::fs::read(store.journal_path()).unwrap();
    let (base_offset, header_len) = store.journal.decode_file_header(&bytes).unwrap();
    assert_eq!(base_offset, 0);
    assert_eq!(
        header_len,
        SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u64>()
            + std::mem::size_of::<u64>()
    );
    let frames = store.journal.read_frames_from(0).unwrap();
    assert_eq!(frames.frames, vec![checkpoint, command_record]);
    assert!(!frames.truncated_tail);
    assert_eq!(frames.clean_len, (bytes.len() - header_len) as u64);
    assert_eq!(
        (bytes.len(), hex_encode(&checksum::sha256::digest(&bytes))),
        (
            209,
            "59505fe3c2c9f663d5a60e4547ed23c32bafcecb83cbddf75d6fc13cb05b9ce4".to_owned()
        )
    );
}

#[test]
fn single_authority_journal_file_header_failures_are_typed() {
    use crate::durable_journal::DurableJournalFileHeaderFormatError;

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let header = store.journal.encode_file_header(0x0102_0304_0506_0708);
    assert!(matches!(
        store.journal.decode_file_header_classified(&header[..3]),
        Err(DurableJournalFileHeaderFormatError::Truncated)
    ));

    let mut bad_magic = header.clone();
    bad_magic[0] ^= 0xff;
    reseal_crc64_suffix(&mut bad_magic);
    assert!(matches!(
        store.journal.decode_file_header_classified(&bad_magic),
        Err(DurableJournalFileHeaderFormatError::UnknownMagic)
    ));

    let mut bad_checksum = header.clone();
    *bad_checksum.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        store.journal.decode_file_header_classified(&bad_checksum),
        Err(DurableJournalFileHeaderFormatError::ChecksumMismatch { .. })
    ));

    for version in [
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION + 1,
    ] {
        let mut unsupported = header.clone();
        let version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
        unsupported[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut unsupported);
        assert_eq!(
            store
                .journal
                .decode_file_header_classified(&unsupported)
                .unwrap_err(),
            DurableJournalFileHeaderFormatError::UnsupportedVersion(version)
        );
    }
}

#[test]
fn single_authority_journal_frame_length_boundaries_are_owner_pinned() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let header = store.journal.encode_file_header(0);

    assert!(matches!(
        store.journal.append_frame(&[]),
        Err(DurableJournalAppendError::BeforeReplayableRecord(
            ControlPlaneError::CommandDecode { message }
        )) if message == "zero-length single-authority control-plane journal frame"
    ));

    let mut zero_length = header.clone();
    zero_length.extend_from_slice(&0u32.to_be_bytes());
    zero_length.extend_from_slice(&(!0u32).to_be_bytes());
    std::fs::write(store.journal_path(), zero_length).unwrap();
    assert!(matches!(
        store.journal.read_frames_from(0),
        Err(ControlPlaneError::CommandDecode { message })
            if message == "zero-length single-authority control-plane journal frame"
    ));

    let mut maximum_length = header;
    maximum_length.extend_from_slice(&u32::MAX.to_be_bytes());
    maximum_length.extend_from_slice(&(!u32::MAX).to_be_bytes());
    std::fs::write(store.journal_path(), maximum_length).unwrap();
    let decoded = store.journal.read_frames_from(0).unwrap();
    assert!(decoded.frames.is_empty());
    assert!(decoded.truncated_tail);
    assert_eq!(decoded.clean_len, 0);
}

#[test]
fn single_authority_journal_file_version_precedes_record_decoding_on_open() {
    for version in [
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION + 1,
    ] {
        let tmp = test_util::tempdir();
        let state_path = tmp.path().join("control-plane.state");
        drop(SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap());

        let journal_path = single_authority_journal_path(&state_path);
        let mut journal = std::fs::read(&journal_path).unwrap();
        let header_len = FileControlPlaneStore::new(&state_path)
            .journal
            .encode_file_header(0)
            .len();
        let frame_len = u32::from_be_bytes(
            journal[header_len..header_len + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let record_start = header_len + std::mem::size_of::<u32>() * 2;
        let record_end = record_start + frame_len;
        let record_version_offset = record_start + SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        journal[record_version_offset..record_version_offset + 2]
            .copy_from_slice(&0u16.to_be_bytes());
        reseal_crc64_suffix(&mut journal[record_start..record_end]);
        assert!(matches!(
            SingleAuthorityJournalRecord::decode(&journal[record_start..record_end]),
            Err(ControlPlaneError::CommandDecode { message })
                if message == "unsupported single-authority control-plane journal record version 0"
        ));

        let file_version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
        journal[file_version_offset..file_version_offset + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut journal[..header_len]);
        std::fs::write(&journal_path, &journal).unwrap();

        let snapshot_before = std::fs::read(&state_path).unwrap();
        let identity_before = std::fs::read(single_authority_identity_path(&state_path)).unwrap();
        let marker_before = std::fs::read(single_authority_initialized_path(&state_path)).unwrap();
        let journal_before = std::fs::read(&journal_path).unwrap();
        assert!(matches!(
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal file header version {version}"
                )
        ));
        assert_eq!(std::fs::read(&state_path).unwrap(), snapshot_before);
        assert_eq!(
            std::fs::read(single_authority_identity_path(&state_path)).unwrap(),
            identity_before
        );
        assert_eq!(
            std::fs::read(single_authority_initialized_path(&state_path)).unwrap(),
            marker_before
        );
        assert_eq!(std::fs::read(&journal_path).unwrap(), journal_before);
    }
}

#[test]
fn file_backed_authority_restarts_with_never_reused_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let first_lease = authority
        .heartbeat(
            heartbeat(1, authority.snapshot().cluster_epoch(), 1_000),
            1_000,
        )
        .unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().max_committed_timestamp_ms(),
        Some(1_000)
    );

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > first_lease.authority_incarnation());
    assert!(restarted.snapshot().cluster_epoch() > first_lease.cluster_epoch());
    assert_eq!(
        restarted.snapshot().max_committed_timestamp_ms(),
        Some(1_000)
    );
}

#[test]
fn file_backed_authority_restarts_empty_state_with_new_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > AuthorityIncarnation::INITIAL);
    assert!(restarted.snapshot().cluster_epoch() > ClusterEpoch::INITIAL);
    assert_eq!(restarted.snapshot().nodes().count(), 0);
}

#[test]
fn file_backed_authority_rejects_pre_v7_state() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=2\nauthority_incarnation=1\ncluster_epoch=1\nnode=1,active,1,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 2"
    ));
}

#[test]
fn file_backed_authority_rejects_previous_and_future_state_versions() {
    for version in [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-v{version}.state"));
        let current = format_snapshot(&canonical_snapshot_with_node());
        let unsupported = current.replacen("version=41\n", &format!("version={version}\n"), 1);
        std::fs::write(&path, &unsupported).unwrap();

        assert!(matches!(
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path.clone())),
            Err(ControlPlaneError::Parse { message, .. })
                if message == format!("unsupported control-plane state version {version}")
        ));
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            unsupported,
            "unsupported state v{version} was replaced during authority open"
        );
    }
}

#[test]
fn file_backed_authority_rejects_noncurrent_nested_command_versions_before_replay() {
    for version in [
        14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 30,
    ] {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(
            tmp.path()
                .join(format!("control-plane-command-v{version}.state")),
        );
        let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let checkpoint_before = std::fs::read(store.path()).unwrap();
        let binding = load_single_authority_clock_checkpoint_binding(store.path())
            .unwrap()
            .unwrap();
        let published_chain_digest = store
            .lock_durability()
            .unwrap()
            .published_chain_digest
            .unwrap();
        let command = ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        };
        let encoded_command =
            crate::control_plane_command::encode_control_plane_command_with_version_for_test(
                &command, version,
            )
            .unwrap();
        let record = SingleAuthorityJournalRecord::encode_command_bytes_for_test(
            binding,
            published_chain_digest,
            &encoded_command,
        )
        .unwrap();
        store.journal.append_frame(&record).unwrap();
        let journal_before = std::fs::read(store.journal_path()).unwrap();
        drop(authority);

        assert!(matches!(
            FileControlPlaneStore::new(store.path()).load(),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!("unsupported control-plane command version {version}")
        ));
        assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
        assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
    }
}

#[test]
fn file_backed_authority_rejects_current_state_missing_timestamp_high_water() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=41\nauthority_incarnation=1\ncluster_epoch=1\ninitial_topology=-\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing max committed timestamp"
    ));
}

#[test]
fn initial_216_pg_placement_uses_sparse_history_deltas() {
    const PG_COUNT: u32 = 216;

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    for pg_id in 0..PG_COUNT {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
    }

    let snapshot = authority.snapshot();
    assert_eq!(snapshot.pgs().count(), PG_COUNT as usize);
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.pgs().len())
            .sum::<usize>(),
        0,
        "introducing PGs must not copy every previously configured route"
    );
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.absent_pgs.len())
            .sum::<usize>(),
        PG_COUNT as usize
    );
    let runtime_map = snapshot.runtime_map(1_001).unwrap();
    assert!(
        runtime_map.historical_pg_routes().len()
            <= snapshot.cluster_map_history().len() + PG_COUNT as usize
    );
    let persisted = std::fs::read(store.path()).unwrap();
    assert!(
        persisted.len() < 128 * 1_024,
        "216-PG initial placement state unexpectedly grew to {} bytes",
        persisted.len()
    );
    assert_eq!(store.load().unwrap().as_ref(), Some(snapshot));
}

#[test]
fn sparse_runtime_map_round_trip_preserves_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let before_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();
    let after_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();

    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), before_introduction),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    assert_eq!(
        decoded
            .reconstructed_pg_route_at_epoch(PgId::new(2), after_introduction)
            .unwrap()
            .acting_set(),
        &[NodeId::new(1)]
    );
}

#[test]
fn sparse_runtime_map_round_trip_preserves_epoch_before_first_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let no_pg_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    assert!(runtime_map
        .historical_cluster_epochs()
        .contains(&no_pg_epoch));
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
}

#[test]
fn cluster_map_history_is_persisted_across_epoch_changes_and_pruned() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    let persisted = store.load().unwrap().unwrap();
    let initial_history = persisted.cluster_map_at_epoch(initial_epoch).unwrap();
    assert_eq!(
        initial_history.authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(initial_history.nodes().len(), 0);
    assert_eq!(initial_history.pgs().len(), 0);

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let pg_epoch = authority.snapshot().cluster_epoch();
    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let before_restart = restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(3), pg_epoch)
        .unwrap();
    assert_eq!(
        before_restart.acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );

    let mut authority = restarted;
    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT);
    assert!(history.first().unwrap().cluster_epoch() > initial_epoch);
    assert!(history.last().unwrap().cluster_epoch() < authority.snapshot().cluster_epoch());

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(
        persisted.cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    assert!(persisted.cluster_map_at_epoch(initial_epoch).is_none());
}

#[test]
fn cluster_map_history_pruning_preserves_metadata_transfer_route_epochs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let initial_epoch = ClusterEpoch::INITIAL;
    let source_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let destination_epoch = authority.snapshot().cluster_epoch();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 2);
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    let protected_source = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap();
    assert!(protected_source.pg(PgId::new(42)).is_some());
    assert!(protected_source.pg(PgId::new(43)).is_none());
    assert_eq!(protected_source.pgs().len(), 1);
    let protected_destination = authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .unwrap();
    assert!(protected_destination.pg(PgId::new(43)).is_none());
    let destination_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(42), destination_epoch)
        .unwrap();
    assert_eq!(
        destination_route.peering_metadata_transfer(),
        Some(transfer)
    );
    assert_eq!(
        destination_route.peering_metadata_transfer_destination_epoch(),
        Some(destination_epoch)
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(initial_epoch)
        .is_none());

    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(source_epoch).is_some());
    assert!(persisted.cluster_map_at_epoch(destination_epoch).is_some());
    assert_eq!(
        persisted
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        13_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            13_001,
        )
        .unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
    let persisted_after_completion = store.load().unwrap().unwrap();
    assert!(persisted_after_completion
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
}

#[test]
fn exact_old_transfer_route_preserves_and_clears_older_source_dependency_atomically() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 42, PgState::Active, proof, false, 11_002);
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        proof,
        PgMetadataProof::current(9, 12, 11),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let transfer_epoch = authority.snapshot().cluster_epoch();
    let source_record = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap()
        .clone();
    let transfer_record = ClusterMapHistoryRecord::from_snapshot(authority.snapshot());
    assert_eq!(
        transfer_record
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch,
        Some(source_epoch)
    );

    let current_epoch =
        ClusterEpoch::new(transfer_epoch.get() + CLUSTER_MAP_HISTORY_LIMIT as u64 + 2).unwrap();
    let mut history = vec![source_record, transfer_record];
    for raw_epoch in (transfer_epoch.get() + 1)..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: Vec::new(),
            pgs: Vec::new(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(transfer_epoch, PgId::new(42))].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert!(history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
    validate_metadata_transfer_route_references(
        &history,
        current_epoch,
        std::iter::empty::<(PgId, Option<ClusterEpoch>, Option<NodeId>)>(),
    )
    .unwrap();

    prune_cluster_map_history(
        &mut history,
        &ClusterMapHistoryProtection {
            exact_routes: BTreeSet::new(),
        },
        current_epoch,
    );

    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
}

#[test]
fn exact_old_routes_do_not_displace_recent_reverse_deltas() {
    let current_epoch = ClusterEpoch::new(1_000).unwrap();
    let ordinary_floor =
        ClusterEpoch::new(current_epoch.get() - CLUSTER_MAP_HISTORY_LIMIT as u64).unwrap();
    let old_epoch = ClusterEpoch::new(100).unwrap();
    let pg_id = PgId::new(42);
    let old_pg_id = PgId::new(7);
    let route = HistoricalPgRouteRecord {
        pg_id,
        state: PgState::Active,
        acting_set: vec![NodeId::new(1), NodeId::new(2)],
        active_primary: Some(NodeId::new(1)),
        peering_metadata_proof_floor: None,
        peering_metadata_proof_floor_epoch: None,
        peering_metadata_proof_floor_imported: false,
        peering_metadata_transfer: None,
        peering_metadata_transfer_source_route_epoch: None,
        peering_metadata_transfer_source_node_id: None,
    };
    let old_route = HistoricalPgRouteRecord {
        pg_id: old_pg_id,
        ..route.clone()
    };
    let mut history = vec![ClusterMapHistoryRecord {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        cluster_epoch: old_epoch,
        nodes: vec![NodeId::new(1), NodeId::new(2)],
        pgs: vec![old_route],
        absent_pgs: Vec::new(),
    }];
    for raw_epoch in ordinary_floor.get()..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            pgs: (raw_epoch == ordinary_floor.get())
                .then(|| route.clone())
                .into_iter()
                .collect(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(old_epoch, old_pg_id)].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 1);
    assert!(history
        .iter()
        .any(|record| { record.cluster_epoch() == ordinary_floor && record.pg(pg_id).is_some() }));
}

#[test]
fn cluster_map_history_pruning_preserves_only_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let protected_record = authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .unwrap();
    assert!(
        protected_record.pgs().is_empty(),
        "unchanged routes should not be copied into an exact epoch marker"
    );
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(1), protected_epoch)
        .is_ok());
    let current_epoch = authority.snapshot().cluster_epoch();
    let advanced_floor = ClusterEpoch::new(current_epoch.get() - 10).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let retained_before_floor_advance = authority.snapshot().cluster_map_history().len();
    assert_eq!(retained_before_floor_advance, CLUSTER_MAP_HISTORY_LIMIT + 1);
    let runtime_map_before_floor_advance = authority.snapshot().runtime_map(10_999).unwrap();
    assert_eq!(
        runtime_map_before_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_before_floor_advance
            .historical_pg_routes()
            .len(),
        2,
        "one exact route and one reconstruction baseline are sufficient"
    );
    let mut advanced_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    advanced_floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            advanced_floor,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(advanced_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    let mut confirmed_advanced_floor = heartbeat_from_record(&authority, 1, current_epoch, 11_001);
    confirmed_advanced_floor.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            advanced_floor,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(confirmed_advanced_floor, 11_001)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        authority.snapshot().cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    let runtime_map_after_floor_advance = authority.snapshot().runtime_map(11_000).unwrap();
    assert_eq!(
        runtime_map_after_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_after_floor_advance.historical_pg_routes().len(),
        2,
        "advancing the exact route must not restore per-epoch route markers"
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        persisted.snapshot().runtime_map(11_000).unwrap().nodes()[0]
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
}

#[test]
fn exact_old_route_retains_later_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_001);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    authority.heartbeat(exact_heartbeat, 1_001).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let introduction_boundary = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(introduction_boundary)
        .is_some_and(|record| record.absent_pgs.contains(&PgId::new(2))));
    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
}

#[test]
fn pending_abort_stream_command_retains_auxiliary_reservation_route_through_pruning_and_restart() {
    let tmp = test_util::tempdir();
    let control_store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(control_store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 20_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let topology = crate::PgTopology::new(&[1, 2]).unwrap();
    let bucket = (0..10_000)
        .find_map(|candidate| {
            let bucket = crate::BucketName::new(format!("released-route-{candidate}")).ok()?;
            (topology.bucket_pg_for(&bucket) == 2).then_some(bucket)
        })
        .expect("test must find a bucket routed to PG 2");
    let key = crate::ObjectKey::new("object").unwrap();
    let command_path = tmp.path().join("command-pg");
    let reservation_path = tmp.path().join("reservation-pg");
    let command_store = PgStore::open(&command_path, 1).unwrap();
    let reservation_store = PgStore::open(&reservation_path, 2).unwrap();
    PgMetadataStore::create_bucket(
        &reservation_store,
        &bucket,
        "owner",
        &s3_types::CanonicalUserId::from_principal("owner"),
        &s3_types::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
        &reservation_store,
        crate::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "released-route-reservation",
            owner_token: "released-route-owner",
            cluster_epoch: protected_epoch,
            operation_kind:
                crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            created_at: 1,
            lease_deadline: 10_000,
            target_context: Some(key.as_str()),
        },
    )
    .unwrap();
    let proof = BucketWriteReservationProof::from(&reservation);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            protected_epoch,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
            bucket: bucket.clone(),
            key,
            session_id: crate::SessionId::try_from("ab".repeat(16)).unwrap(),
            staged_segments: Vec::new(),
            stream_create_bucket_write_reservation: Some(proof.clone()),
        })),
    );
    command_store
        .try_insert_pending_metadata_command_slot(1, &command, Some(&bucket))
        .unwrap();
    command_store
        .record_metadata_command_abandoned(1, &command)
        .unwrap();
    let protected_references = command_store
        .cluster_map_history_route_references(&topology)
        .unwrap();
    let reservation_route = PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
        protected_epoch,
        PgId::new(2),
    );
    assert!(protected_references
        .iter()
        .any(|reference| reference == reservation_route));
    drop(command_store);
    drop(reservation_store);

    let mut heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 20_100);
    heartbeat.cluster_map_history_route_references = protected_references;
    assert!(authority.heartbeat(heartbeat, 20_100).unwrap().serving());
    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(
        authority.snapshot().cluster_epoch().get() - protected_epoch.get()
            > CLUSTER_MAP_HISTORY_LIMIT as u64
    );
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch)
        .is_ok());
    drop(authority);

    let persisted = SingleAuthorityControlPlane::open(control_store).unwrap();
    assert!(persisted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch)
        .is_ok());
    let command_store = PgStore::open(&command_path, 1).unwrap();
    assert!(command_store
        .cluster_map_history_route_references(&topology)
        .unwrap()
        .iter()
        .any(|reference| reference == reservation_route));
    let reservation_store = PgStore::open(&reservation_path, 2).unwrap();
    PgMetadataStore::release_metadata_command_bucket_write_reservation(&reservation_store, &proof)
        .unwrap();
    assert!(command_store
        .remove_pending_metadata_command_slot(1, &command)
        .unwrap());
    assert!(PgMetadataStore::durable_bucket_write_reservation(
        &reservation_store,
        &bucket,
        &reservation.reservation_id,
    )
    .unwrap()
    .is_none());
    assert!(command_store
        .cluster_map_history_route_references(&topology)
        .unwrap()
        .iter()
        .all(|reference| {
            reference.kind() != PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource
        }));
}

#[test]
fn heartbeat_persists_exact_cluster_map_history_route_references() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    let current_epoch = authority.snapshot().cluster_epoch();
    let references = PgClusterMapHistoryRouteReferences::try_from_iter([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            current_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            current_epoch,
            PgId::new(2),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            current_epoch,
            PgId::new(1),
        ),
    ])
    .unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    heartbeat.cluster_map_history_route_references = references.clone();
    authority.heartbeat(heartbeat, 10_000).unwrap();

    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
    let persisted_text = format_snapshot(&store.load().unwrap().unwrap());
    assert!(persisted_text.contains("node_history_route=1,live,"));
    assert!(persisted_text.contains("node_history_route=1,backfill-desired,"));
    let invalid_kind = persisted_text.replace(
        "node_history_route=1,live,",
        "node_history_route=1,unknown,",
    );
    assert!(matches!(
        parse_snapshot(&invalid_kind),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("invalid node history route reference kind")
    ));
    let live_line = persisted_text
        .lines()
        .find(|line| line.starts_with("node_history_route=1,live,"))
        .unwrap();
    let duplicate = persisted_text.replace(live_line, &format!("{live_line}\n{live_line}"));
    assert!(matches!(
        parse_snapshot(&duplicate),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("duplicate node history route reference")
    ));
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
}

#[test]
fn heartbeat_route_reference_handoff_survives_one_omission_and_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let protected_reference = PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
        protected_epoch,
        PgId::new(1),
    );
    let references = history_route_references([protected_reference]);

    let mut initial = heartbeat_from_record(&authority, 1, protected_epoch, 10_000);
    initial.cluster_map_history_route_references = references.clone();
    authority.heartbeat(initial, 10_000).unwrap();

    let omitted = heartbeat_from_record(&authority, 1, protected_epoch, 10_001);
    authority.heartbeat(omitted, 10_001).unwrap();
    let node = authority.snapshot().node(NodeId::new(1)).unwrap();
    assert!(node.cluster_map_history_route_references().is_empty());
    assert_eq!(
        node.retiring_cluster_map_history_route_references,
        references
    );

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(1), protected_epoch)
        .is_ok());
    drop(authority);

    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(1), protected_epoch)
        .is_ok());
    let current_epoch = restarted.snapshot().cluster_epoch();
    let mut restored = heartbeat_from_record(&restarted, 1, current_epoch, 20_000);
    restored.cluster_map_history_route_references = references.clone();
    restarted.heartbeat(restored, 20_000).unwrap();
    let node = restarted.snapshot().node(NodeId::new(1)).unwrap();
    assert_eq!(node.cluster_map_history_route_references(), &references);
    assert!(node
        .retiring_cluster_map_history_route_references
        .is_empty());
}

#[test]
fn heartbeat_rejects_future_or_missing_exact_history_route_before_persisting() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let before = authority.snapshot().clone();

    let mut future = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    future.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::new(current_epoch.get() + 1).unwrap(),
                PgId::new(1),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(future, 10_000),
        Err(ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
            route_epoch,
            pg_id: 1,
            validation_epoch,
            ..
        }) if route_epoch.get() == current_epoch.get() + 1
            && validation_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    let mut missing = heartbeat_from_record(&authority, 1, current_epoch, 10_001);
    missing.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::INITIAL,
                PgId::new(99),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(missing, 10_001),
        Err(
            ControlPlaneError::StorageClusterMapHistoryRouteNotRetained {
                route_epoch: ClusterEpoch::INITIAL,
                pg_id: 99,
                ..
            }
        )
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn cluster_map_history_pruning_preserves_reported_pending_command_epoch_exactly() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        1,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        10_020,
    );
    authority.complete_ready_pg_peerings(10_030).unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            protected_epoch,
            PgId::new(1),
        )]);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        )),
    }];
    authority.heartbeat(heartbeat, 10_100).unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(1))
            .unwrap()
            .pending_metadata_command(),
        Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        ))
    );

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        let now_ms = 20_000 + u64::from(node_id);
        let current_epoch = authority.snapshot().cluster_epoch();
        let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, now_ms);
        heartbeat.cluster_map_history_route_references =
            history_route_references([PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
                protected_epoch,
                PgId::new(1),
            )]);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(1),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: Some(PendingMetadataCommandObservation::new(
                protected_epoch,
                NonZeroU64::MIN,
                0x1234,
            )),
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(protected_epoch).is_some());
}

#[test]
fn cluster_map_history_pruning_preserves_exact_durable_backfill_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
}

#[test]
fn cluster_map_history_pruning_releases_cleared_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());

    let current_epoch = authority.snapshot().cluster_epoch();
    let clear_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    assert!(authority
        .heartbeat(clear_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    let confirm_clear_floor = heartbeat_from_record(&authority, 1, current_epoch, 11_001);
    assert!(authority
        .heartbeat(confirm_clear_floor, 11_001)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );

    for node_id in 100..(100 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );
}

#[test]
fn heartbeat_accepts_exact_route_without_unretained_intermediate_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof::current(9, 12, 11),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let destination_epoch = next_epoch(source_epoch).unwrap();
    let unretained_intermediate_epoch = next_epoch(destination_epoch).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(unretained_intermediate_epoch)
        .is_none());
    let current_epoch = authority.snapshot().cluster_epoch();
    let heartbeat_at_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap_or(20_000);
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, heartbeat_at_ms);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            source_epoch,
            PgId::new(42),
        )]);
    authority
        .heartbeat(exact_heartbeat, heartbeat_at_ms)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
}

#[test]
fn peering_acting_set_update_preserves_metadata_transfer_source_route_fields() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof::current(9, 12, 11),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        transfer.metadata_proof(),
        false,
        11_003,
    );

    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(2), NodeId::new(3)])
        .unwrap();

    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    let pg = persisted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
}

#[test]
fn control_plane_reload_rejects_transfer_marker_without_source_route_history() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof::current(9, 12, 11),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    assert!(store
        .load()
        .unwrap()
        .unwrap()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    store
        .checkpoint(Some(authority.snapshot()), authority.snapshot())
        .unwrap();

    let source_history_prefixes = [
        format!("history={},", source_epoch.get()),
        format!("history_node={},", source_epoch.get()),
        format!("history_pg={},", source_epoch.get()),
    ];
    let state = std::fs::read_to_string(&state_path).unwrap();
    let filtered = state
        .lines()
        .filter(|line| {
            !source_history_prefixes
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&state_path, filtered).unwrap();

    let err = store.load().unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::Parse { message, .. }
            if message.contains(
                "references missing metadata transfer source route epoch"
            )
    ));
}

#[test]
fn snapshot_reconstructs_pg_route_at_epoch_without_serving_authority() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let route_epoch = authority.snapshot().cluster_epoch();

    authority
        .set_node_membership(NodeId::new(4), NodeMembershipState::Active)
        .unwrap();
    let snapshot = authority.snapshot();
    let route = snapshot
        .reconstructed_pg_route_at_epoch(PgId::new(3), route_epoch)
        .unwrap();
    assert_eq!(route.cluster_epoch(), route_epoch);
    assert_eq!(route.pg_id(), PgId::new(3));
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    let missing_epoch = ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap();
    assert!(matches!(
        snapshot.reconstructed_pg_route_at_epoch(PgId::new(3), missing_epoch),
        Err(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })
            if cluster_epoch == missing_epoch
    ));
}

#[test]
fn file_backed_authority_rejects_duplicate_pg_acting_set_nodes() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=1\n",
            "pg=7,peering,1:1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set contains duplicate node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_nodes_absent_from_current_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "pg=7,active,1:99,1,1,1,2,5,3,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set references unknown node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,9,1,10,5,11,3,9,1,10,5,11,9,1,10,5,11,2,1,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "metadata transfer source epoch must not be newer than PG record epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_imported_provenance_without_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "pg=7,active,1,1,9,1,10,5,11,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,1,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);

    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active metadata transfer imported provenance requires an active metadata proof epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=5\nauthority_incarnation=1\ncluster_epoch=2\nhistory=1,1\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 5"
    ));
}

#[test]
fn file_backed_authority_rejects_current_or_future_history_epochs() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=2,1\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history epoch must be older than current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_nodes_absent_from_history_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1:2,-,-,-,-,-,-,-,0,-,-,-,-,-,-,-,-,-,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG 7 acting set references unknown node 2"
    ));
}

#[test]
fn file_backed_authority_rejects_reconstructible_observations_in_history() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_node_pg=2,1,7,peering,2,100,0,0,0,-,-,-\n",
            "history_pg=2,7,peering,1,-,-,-,-,-,-,-,0,-,-,-,-,-,-,-,-,-,-,-,-,-\n",
            "node=1,active,1,healthy,11,3,100,200,-,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    let error = FileControlPlaneStore::new(path).load().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Parse { message, .. }
            if message == "unknown control-plane state line"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=4\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1,-,9,1,10,5,11,2,1,3,9,1,10,5,11,9,1,10,5,11,2,1\n",
            "node=1,active,1,healthy,11,4,100,200,-,6e6f64652d312e736f636b\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "PG 7 metadata transfer source epoch is newer than route epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_incomplete_compact_history_routes() {
    let cases = [
        (
            "history_pg=2,7,active,1,-,-,-,-,-,-,-,0,-,-,-,-,-,-,-,-,-,-,-,-,-\n",
            "active PG 7 has no primary",
        ),
        (
            "history_pg=2,7,peering,1,-,9,1,10,5,11,2,1,2,9,1,10,5,11,9,1,10,5,11,-,-\n",
            "PG 7 has incomplete metadata transfer route state",
        ),
    ];
    for (index, (history_pg, expected)) in cases.into_iter().enumerate() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{index}.state"));
        let contents = format!(
            "version=41\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\nhistory=2,1\nhistory_node=2,1\n{history_pg}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_broken_compact_history_transfer_chains() {
    let cases = [
        (
            "self-reference",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,9,1,10,5,11,1,1,1,9,1,10,5,11,9,1,10,5,11,2,1\n",
            ),
            "PG 7 metadata transfer source route epoch is not older than route epoch",
        ),
        (
            "missing-source-epoch",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,9,1,10,5,11,1,1,1,9,1,10,5,11,9,1,10,5,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source route epoch 1",
        ),
        (
            "missing-source-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,9,1,10,5,11,1,1,1,9,1,10,5,11,9,1,10,5,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source PG at epoch 1",
        ),
        (
            "mismatched-source-primary",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_node=1,2\n",
                "history_pg=1,7,active,2,2,-,-,-,-,-,-,0,-,-,-,-,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_node=2,2\n",
                "history_pg=2,7,peering,1,-,9,1,10,5,11,1,1,1,9,1,10,5,11,9,1,10,5,11,1,1\n",
            ),
            "PG 7 metadata transfer source node 1 does not match source route primary 2 at epoch 1",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let contents = format!(
            "version=41\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_invalid_pg_introduction_history() {
    let current_node = "node=1,active,1,suspect,11,-,-,-,-,2f746d702f6e6f64652d312e736f636b\n";
    let current_pg = "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n";
    let cases = [
        (
            "duplicate",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history repeats a PG introduction boundary",
        ),
        (
            "route-before-introduction",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg=1,7,peering,1,-,-,-,-,-,-,-,0,-,-,-,-,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history PG route precedes its introduction boundary",
        ),
        (
            "missing-current-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
            ),
            "history absent PG is missing from current state",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let current_pg = if name == "missing-current-pg" {
            ""
        } else {
            current_pg
        };
        std::fs::write(
            &path,
            format!(
                "version=41\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}{current_node}{current_pg}"
            ),
        )
        .unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error for {name}: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_noncanonical_absent_pg_order() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=1,1\n",
            "history_pg_absent=1,8\n",
            "history_pg_absent=1,7\n",
            "node=1,active,1,suspect,11,-,-,-,-,2f746d702f6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
            "pg=8,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(path).load(),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history absent PG records must be strictly increasing"
    ));
}

#[test]
fn file_backed_authority_rejects_pg_observations_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=5\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,0,0,0\n",
            "pg=7,peering,1,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "unsupported control-plane state version 5"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_outside_acting_set() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "node=2,active,1,healthy,12,2,100,200,-,6e6f64652d322e736f636b\n",
            "node_pg=2,7,peering,2,100,0,1,0,5,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation references PG outside node acting set"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_wrong_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "node=1,active,1,healthy,11,3,100,200,-,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,1,0,5,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation epoch must match current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_mismatched_proof() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,1,10,5,12,-,-,-\n",
            "pg=7,active,1,1,9,1,10,5,11,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active node PG observation metadata proof is behind or diverges from PG active proof"
    ));
}

#[test]
fn file_backed_authority_accepts_active_pg_observation_after_metadata_progress() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let initial = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let snapshot = parse_snapshot(concat!(
            "version=41\n",
        "authority_incarnation=1\n",
        "cluster_epoch=2\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=100\nlease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
        "node_pg=1,7,active,2,100,10,1,20,5,30,-,-,-\n",
        "pg=7,active,1,1,9,1,10,5,11,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
    ))
    .unwrap();
    store
        .checkpoint(Some(initial.snapshot()), &snapshot)
        .unwrap();
    drop(initial);
    let authority = SingleAuthorityControlPlane::open(store).unwrap();
    let history = authority
        .snapshot()
        .cluster_map_at_epoch(ClusterEpoch::new(2).unwrap())
        .unwrap();
    assert!(history.nodes().contains(&NodeId::new(1)));
    let historical_pg = history.pg(PgId::new(7)).unwrap();
    assert_eq!(historical_pg.state(), PgState::Active);
    assert_eq!(historical_pg.active_primary, Some(NodeId::new(1)));
}

#[test]
fn control_plane_state_rejects_each_unsupported_metadata_proof_carrier() {
    let current = concat!(
            "version=41\n",
        "authority_incarnation=1\n",
        "cluster_epoch=2\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=100\nlease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
        "node_pg=1,7,active,2,100,10,1,20,5,30,-,-,-\n",
        "pg=7,active,1,1,9,1,10,5,11,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
    );
    parse_snapshot(current).unwrap();

    for (unsupported, expected) in [
        (
            current.replacen(",10,1,20,5,30,", ",10,2,20,5,30,", 1),
            "unsupported metadata-command log-hash encoding version 2",
        ),
        (
            current.replacen(",10,1,20,5,30,", ",10,1,20,6,30,", 1),
            "unsupported canonical-state digest encoding version 6",
        ),
    ] {
        assert!(matches!(
            parse_snapshot(&unsupported),
            Err(ControlPlaneError::Parse { message, .. }) if message == expected
        ));
    }
}

#[test]
fn file_backed_authority_replays_journal_without_per_command_checkpoint() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let digest_computations_before = single_authority_snapshot_digest_computations();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    assert_eq!(
        single_authority_snapshot_digest_computations(),
        digest_computations_before,
        "journal suffix commands must not format and digest the full snapshot"
    );
    assert_eq!(
        std::fs::read(store.path()).unwrap(),
        checkpoint_before,
        "ordinary durable commands must not rewrite the full checkpoint"
    );
    let offsets = store.journal.status_offsets().unwrap();
    assert!(offsets.clean_len > offsets.base_offset);
    let replayed = store.load().unwrap().unwrap();
    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );

    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    let compacted = store.journal.status_offsets().unwrap();
    let retained = store
        .journal
        .read_frames_from(compacted.base_offset)
        .unwrap();
    assert_eq!(retained.frames.len(), 1);
    assert!(
        SingleAuthorityJournalRecord::decode(&retained.frames[0])
            .unwrap()
            .command
            .is_none(),
        "checkpoint compaction must retain exactly one checkpoint anchor"
    );
}

#[test]
fn file_backed_authority_recovers_identity_only_initialization() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let initializing_store = FileControlPlaneStore::new(&path);
    initializing_store
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();

    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(authority.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn single_authority_durable_formats_reject_unsupported_versions() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);

    for version in [0, CONTROL_PLANE_STATE_IDENTITY_VERSION + 1] {
        store_single_authority_clock_checkpoint_binding(&path, binding).unwrap();
        let identity_path = single_authority_identity_path(&path);
        let mut bytes = std::fs::read(&identity_path).unwrap();
        bytes[CONTROL_PLANE_STATE_IDENTITY_MAGIC.len()
            ..CONTROL_PLANE_STATE_IDENTITY_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&identity_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_clock_checkpoint_binding(&path),
            Err(ControlPlaneError::AuthorityClockCheckpoint { message })
                if message == format!(
                    "unsupported single-authority durable identity version {version}"
                )
        ));
    }

    for version in [0, SINGLE_AUTHORITY_INITIALIZED_VERSION + 1] {
        store_single_authority_initialized_binding(&path, binding).unwrap();
        let initialized_path = single_authority_initialized_path(&path);
        let mut bytes = std::fs::read(&initialized_path).unwrap();
        bytes[SINGLE_AUTHORITY_INITIALIZED_MAGIC.len()
            ..SINGLE_AUTHORITY_INITIALIZED_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&initialized_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_initialized_binding(&path),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority initialization marker version {version}"
                )
        ));
    }

    let store = FileControlPlaneStore::new(&path);
    for version in [
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION + 1,
    ] {
        let mut header = store.journal.encode_file_header(0);
        let version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
        header[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut header);
        assert!(matches!(
            store.journal.decode_file_header(&header),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal file header version {version}"
                )
        ));
    }

    for version in [
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION + 1,
    ] {
        let mut record = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: 7,
            resulting_chain_digest: 7,
            command: None,
        }
        .encode()
        .unwrap();
        let version_offset = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        record[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut record);
        assert!(matches!(
            SingleAuthorityJournalRecord::decode(&record),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal record version {version}"
                )
        ));
    }
}

#[test]
fn bare_control_plane_state_path_uses_current_directory_for_durability() {
    assert_eq!(
        state_parent(Path::new("control-plane.state")),
        Path::new(".")
    );
    assert_eq!(
        state_parent(Path::new("./control-plane.state")),
        Path::new(".")
    );
}

#[test]
fn control_plane_state_directory_creation_syncs_each_new_component_parent() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut synced_parents = Vec::new();

    create_control_plane_directory_all_durable_with(&second, |parent| {
        synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        synced_parents,
        vec![
            state_parent(tmp.path()).to_path_buf(),
            tmp.path().to_path_buf(),
            first
        ]
    );
    assert!(second.is_dir());
}

#[test]
fn control_plane_state_directory_creation_reconfirms_failed_sync_on_retry() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut sync_attempts = 0;

    let error = create_control_plane_directory_all_durable_with(&second, |_| {
        sync_attempts += 1;
        if sync_attempts == 2 {
            Err(ControlPlaneError::io(
                "injected control-plane state parent sync",
                std::io::Error::other("injected parent sync failure"),
            ))
        } else {
            Ok(())
        }
    })
    .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "injected control-plane state parent sync"
    ));
    assert!(first.is_dir());
    assert!(
        !second.exists(),
        "the next directory component must not be created before its parent link is durable"
    );

    let mut retry_synced_parents = Vec::new();
    create_control_plane_directory_all_durable_with(&second, |parent| {
        retry_synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(retry_synced_parents, vec![tmp.path().to_path_buf(), first]);
    assert!(second.is_dir());
}

#[test]
fn file_backed_authority_checkpoints_at_command_and_byte_bounds() {
    for (name, command_limit, byte_limit, commands_before_checkpoint) in
        [("commands", 2, u64::MAX, 2), ("bytes", u64::MAX, 1, 1)]
    {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::with_checkpoint_limits(
            tmp.path().join(format!("control-plane-{name}.state")),
            command_limit,
            byte_limit,
        );
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let initial_checkpoint = std::fs::read(store.path()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if commands_before_checkpoint == 2 {
            assert_eq!(std::fs::read(store.path()).unwrap(), initial_checkpoint);
            authority
                .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
                .unwrap();
        }
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .expect("checkpoint threshold should be due")
            .persist()
            .unwrap();

        assert_ne!(
            std::fs::read(store.path()).unwrap(),
            initial_checkpoint,
            "{name} threshold must publish a compacted checkpoint"
        );
        let offsets = store.journal.status_offsets().unwrap();
        let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
        assert_eq!(retained.frames.len(), 1);
        assert!(
            SingleAuthorityJournalRecord::decode(&retained.frames[0])
                .unwrap()
                .command
                .is_none(),
            "{name} threshold compaction must retain one checkpoint anchor"
        );
        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert_eq!(
            restarted
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .membership(),
            NodeMembershipState::Active
        );
    }
}

#[test]
fn file_backed_authority_checkpoint_is_due_at_time_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_policy(
        tmp.path().join("control-plane.state"),
        u64::MAX,
        u64::MAX,
        Duration::ZERO,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("time threshold should be due")
        .persist()
        .unwrap();

    assert_ne!(std::fs::read(store.path()).unwrap(), checkpoint_before);
}

#[test]
fn captured_checkpoint_preparation_failure_latches_poison() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");
    let prepared_path = single_authority_snapshot_tmp_path(&path);
    std::fs::create_dir(&prepared_path).unwrap();

    assert!(matches!(
        checkpoint.persist(),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "create control-plane state"
    ));
    let error = authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));
}

#[test]
fn captured_checkpoint_rebases_commands_appended_during_persistence() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    checkpoint.persist().unwrap();

    let offsets = store.journal.status_offsets().unwrap();
    let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
    assert_eq!(retained.frames.len(), 2);
    assert!(SingleAuthorityJournalRecord::decode(&retained.frames[0])
        .unwrap()
        .command
        .is_none());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn checkpoint_failure_latches_poison_before_concurrent_command_can_append() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let previous_snapshot = authority.durable_snapshot.clone();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let mut next_snapshot = previous_snapshot
        .apply_control_plane_command(command.clone())
        .unwrap()
        .into_snapshot();
    next_snapshot.record_history_from(&previous_snapshot);
    drop(authority);

    store.pause_next_checkpoint_after_journal_replacement();
    store.fail_next_checkpoint_after_anchor();
    let checkpoint_worker = std::thread::spawn(move || checkpoint.persist());
    let replacement_reached = store.wait_for_checkpoint_journal_replacement(Duration::from_secs(2));

    store.arm_commit_before_durability_lock_signal();
    let concurrent_store = store.clone();
    let command_worker = std::thread::spawn(move || {
        concurrent_store.commit_command(&previous_snapshot, &command, &next_snapshot)
    });
    let command_reached_durability_lock =
        store.wait_for_commit_before_durability_lock(Duration::from_secs(2));
    store.release_checkpoint_after_journal_replacement();

    let checkpoint_error = checkpoint_worker.join().unwrap().unwrap_err();
    let command_error = command_worker.join().unwrap().unwrap_err();
    assert!(
        replacement_reached,
        "checkpoint should pause after durable journal replacement"
    );
    assert!(
        command_reached_durability_lock,
        "concurrent command should reach the durability lock while checkpoint publication is paused"
    );
    assert!(matches!(
        checkpoint_error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(matches!(
        command_error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert!(restarted.snapshot().node(NodeId::new(1)).is_some());
    assert!(
        restarted.snapshot().node(NodeId::new(2)).is_none(),
        "the waiting command must not append after replacement failure"
    );
}

#[test]
fn captured_checkpoint_preserves_conservative_suffix_age() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let captured_at = Instant::now();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(captured_at)
        .unwrap()
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    checkpoint.persist().unwrap();

    let durability = store.lock_durability().unwrap();
    assert_eq!(durability.commands_since_checkpoint, 1);
    assert_eq!(durability.first_uncheckpointed_at, Some(captured_at));
}

#[test]
fn checkpoint_capture_uses_tracked_offset_without_scanning_journal() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let expected_offset = store.lock_durability().unwrap().journal_clean_offset;
    std::fs::write(store.journal_path(), b"not a valid journal").unwrap();

    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .expect("capture must not read the journal")
        .expect("command threshold should be due");

    assert_eq!(checkpoint.capture.journal_offset, expected_offset);
}

#[test]
fn stale_captured_checkpoint_is_rejected_before_publication() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let stale = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let current = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    current.persist().unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = stale.persist().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint capture is stale")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .expect("stale checkpoint rejection must not poison the store");
}

#[test]
fn captured_checkpoint_is_bound_to_its_store_instance() {
    let tmp = test_util::tempdir();
    let first_store =
        FileControlPlaneStore::with_checkpoint_limits(tmp.path().join("first.state"), 1, u64::MAX);
    let mut first = SingleAuthorityControlPlane::open(first_store).unwrap();
    first
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = first
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let SingleAuthorityDurableCheckpoint {
        capture, snapshot, ..
    } = checkpoint;
    let second_store = FileControlPlaneStore::new(tmp.path().join("second.state"));
    SingleAuthorityControlPlane::open(second_store.clone()).unwrap();
    let checkpoint_before = std::fs::read(second_store.path()).unwrap();
    let journal_before = std::fs::read(second_store.journal_path()).unwrap();

    let error = second_store
        .persist_captured_checkpoint(capture, &snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("belongs to another store instance")
    ));
    assert_eq!(
        std::fs::read(second_store.path()).unwrap(),
        checkpoint_before
    );
    assert_eq!(
        std::fs::read(second_store.journal_path()).unwrap(),
        journal_before
    );
    second_store.ensure_healthy().unwrap();
}

#[test]
fn captured_checkpoint_persists_without_authority_mutex() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(store).unwrap(),
    ));
    let checkpoint = {
        let mut authority = authority.lock().unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .unwrap()
    };
    let authority_guard = authority.lock().unwrap();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        completed_tx.send(checkpoint.persist()).unwrap();
    });

    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint persistence must not wait for the authority mutex")
        .unwrap();
    drop(authority_guard);
    worker.join().unwrap();
}

#[test]
fn file_backed_authority_checkpoint_compaction_reports_physical_io() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let before = observability::control_plane_journal_metrics_snapshot();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();

    let after = observability::control_plane_journal_metrics_snapshot();
    assert!(after.compaction_total > before.compaction_total);
    assert!(after.compaction_us_total >= before.compaction_us_total);
    assert!(after.compaction_lock_wait_us_total >= before.compaction_lock_wait_us_total);
    assert!(after.compaction_bytes_total > before.compaction_bytes_total);
    assert!(after.compaction_bytes_last > 0);
    assert!(after.compaction_file_sync_total > before.compaction_file_sync_total);
    assert!(after.compaction_directory_sync_total > before.compaction_directory_sync_total);
}

#[test]
fn file_backed_authority_recovers_checkpoint_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_checkpoint_after_anchor();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let error = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_anchor_file_sync_before_directory_sync() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let previous = authority.snapshot().clone();
    let applied = previous
        .clone()
        .apply_control_plane_command(ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        })
        .unwrap();
    let mut next = applied.into_snapshot();
    next.record_history_from(&previous);
    store.fail_next_journal_directory_sync();

    let error = store.checkpoint(Some(&previous), &next).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "sync single-authority control-plane journal directory"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_recovers_initial_identity_creation_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_initial_checkpoint_after_identity();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "initialize single-authority control-plane checkpoint"
    ));
    assert!(single_authority_identity_path(&path).exists());
    assert!(!path.exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_initial_prepared_snapshot_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_prepared_snapshot_sync();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "anchor prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    assert!(!store.journal_path().exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_torn_first_journal_creation() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        store.fail_next_checkpoint_after_prepared_snapshot_sync();
        SingleAuthorityControlPlane::open(store.clone()).unwrap_err();
        let prepared = std::fs::read_to_string(single_authority_snapshot_tmp_path(&path)).unwrap();
        let snapshot_digest = checksum::crc64::checksum(prepared.as_bytes());
        let binding = load_single_authority_clock_checkpoint_binding(&path)
            .unwrap()
            .unwrap();
        let anchor = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: snapshot_digest,
            resulting_chain_digest: snapshot_digest,
            command: None,
        }
        .encode()
        .unwrap();
        store.journal.append_frame(&anchor).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        let restarted =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
        assert_eq!(restarted.snapshot().nodes().count(), 0);
        assert!(single_authority_initialized_path(&path).exists());
    }
}

#[test]
fn file_backed_authority_rejects_torn_established_first_journal_record() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        assert!(
            FileControlPlaneStore::new(&path).load().is_err(),
            "established {shape} journal must fail closed"
        );
    }
}

#[test]
fn file_backed_authority_recovers_initial_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_anchor();

    let error = SingleAuthorityControlPlane::open(store).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(!path.exists());
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_restart_bump_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let incarnation_before = authority.snapshot().authority_incarnation();
    drop(authority);

    let failing_store = FileControlPlaneStore::new(&path);
    failing_store.fail_next_checkpoint_after_anchor();
    let error = SingleAuthorityControlPlane::open(failing_store).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert!(
        restarted.snapshot().authority_incarnation() > incarnation_before,
        "restart must recover and advance beyond the prepared incarnation"
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_truncates_torn_journal_tail_after_replay() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let clean_len = store.journal.clean_len().unwrap();
    let physical_len_before = std::fs::metadata(store.journal_path()).unwrap().len();
    std::fs::OpenOptions::new()
        .append(true)
        .open(store.journal_path())
        .unwrap()
        .write_all(&[0, 0])
        .unwrap();

    let replayed = store.load().unwrap().unwrap();

    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );
    assert_eq!(store.journal.clean_len().unwrap(), clean_len);
    assert_eq!(
        std::fs::metadata(store.journal_path()).unwrap().len(),
        physical_len_before
    );
}

#[test]
fn file_backed_authority_rejects_missing_or_empty_journal_after_acknowledged_command() {
    for missing in [true, false] {
        let tmp = test_util::tempdir();
        let store =
            FileControlPlaneStore::new(tmp.path().join(format!("control-plane-{missing}.state")));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if missing {
            std::fs::remove_file(store.journal_path()).unwrap();
        } else {
            std::fs::File::create(store.journal_path()).unwrap();
        }

        let error = FileControlPlaneStore::new(store.path()).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::CommandDecode { ref message }
                    if message.contains("has no identity-bound checkpoint anchor")
            ),
            "unexpected recovery error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_missing_established_checkpoint_and_journal() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert!(single_authority_initialized_path(&path).exists());
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(store.journal_path()).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("without its control-plane checkpoint")
    ));
}

#[test]
fn file_backed_authority_rejects_foreign_identity_journal() {
    let tmp = test_util::tempdir();
    let first = FileControlPlaneStore::new(tmp.path().join("first.state"));
    let second = FileControlPlaneStore::new(tmp.path().join("second.state"));
    let mut first_authority = SingleAuthorityControlPlane::open(first.clone()).unwrap();
    SingleAuthorityControlPlane::open(second.clone()).unwrap();
    first_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    std::fs::copy(first.journal_path(), second.journal_path()).unwrap();

    assert!(matches!(
        second.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("journal identity does not match durable state")
    ));
}

#[test]
fn file_backed_authority_rejects_checksum_valid_discontinuous_command_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let binding = load_single_authority_clock_checkpoint_binding(store.path())
        .unwrap()
        .unwrap();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let encoded_command = encode_control_plane_command(&command).unwrap();
    let published_chain_digest = store
        .lock_durability()
        .unwrap()
        .published_chain_digest
        .unwrap();
    let wrong_previous_chain_digest = published_chain_digest ^ 1;
    let record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: wrong_previous_chain_digest,
        resulting_chain_digest: single_authority_command_chain_digest(
            wrong_previous_chain_digest,
            &encoded_command,
        ),
        command: Some(command),
    };
    store
        .journal
        .append_frame(&record.encode().unwrap())
        .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(store.path()).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_complete_interior_command_omission() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let offsets = store.journal.status_offsets().unwrap();
    let frames = store
        .journal
        .read_frames_from(offsets.base_offset)
        .unwrap()
        .frames;
    assert_eq!(frames.len(), 3);
    std::fs::remove_file(store.journal_path()).unwrap();
    store.journal.append_frame(&frames[0]).unwrap();
    store.journal.append_frame(&frames[2]).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_checkpoint_off_retained_journal_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_checkpoint = std::fs::read(store.path()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();
    std::fs::write(store.path(), initial_checkpoint).unwrap();

    assert!(matches!(
        store.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("has no identity-bound checkpoint anchor for the durable snapshot")
    ));
}

#[test]
fn file_backed_authority_rejects_stale_checkpoint_before_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let stale_snapshot = authority.snapshot().clone();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = store
        .checkpoint(Some(&stale_snapshot), &stale_snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint base does not match")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
}

#[test]
fn file_backed_authority_poisoned_by_ambiguous_journal_append_stops_serving() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_journal_file_sync();

    let error = authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability poisoned after ambiguous journal append")
    ));
    assert!(authority.snapshot().node(NodeId::new(1)).is_none());
    assert!(matches!(
        authority.runtime_map_snapshot(1),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_non_current_pending_command() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=41\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,-,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,1,10,5,11,1,1,1\n",
            "pg=7,active,1,1,9,1,10,5,11,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "active node PG observation has a non-current pending metadata command"
    ));
}
