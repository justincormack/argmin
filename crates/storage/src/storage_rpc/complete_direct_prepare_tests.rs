// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Framing evidence, not evidence of physical acknowledgements, encryption
//! custody, authoritative snapshots or installable commands.
use super::*;

fn snapshot_request() -> Vec<u8> {
    let descriptor = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ));
    snapshot::encode_request(&complete_staging_ack::Request {
        receiver: complete_settled_prefix::Request {
            node: NodeId::new(4),
            pg: PgId::new(4),
            epoch: ClusterEpoch::new(5).unwrap(),
            topology: [0xa5; 32],
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 10_000,
            },
        },
        provenance: complete_staging_ack::Provenance::Direct {
            descriptor: &descriptor,
            logical_size: 4,
        },
        receipts: (0..3)
            .map(|shard_index| complete_staging_ack::Receipt {
                shard_index,
                ack: WriteAck {
                    stored_size: 38,
                    crc64: 0x1234 + u64::from(shard_index),
                },
            })
            .collect(),
    })
    .unwrap()
    .bytes()
    .to_vec()
}

fn request(snapshot_request: &[u8]) -> Request<'_> {
    Request {
        snapshot_request,
        snapshot_sha256: [0xab; 32],
        segment_crc64: 123,
        provisional: crate::tests::metadata_only_test_encryption(),
        commit: StreamPutCommitInput {
            versioning: BucketVersioningState::Disabled,
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            etag_crc64: 24,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            object_lock: ObjectLockState::default(),
            encryption: crate::tests::committed_metadata_only_test_encryption(
                1,
                &crate::tests::bucket_name("bucket"),
                &crate::tests::object_key("key"),
                GenerationId::MIN,
            ),
        },
    }
}

fn prepared(command: &[u8]) -> command::BorrowedRequest<'_> {
    command::BorrowedRequest {
        node_id: NodeId::new(4),
        epoch: ClusterEpoch::new(5).unwrap(),
        pg_id: PgId::new(4),
        checksum: 0x12345678,
        command,
    }
}

#[test]
fn complete_direct_prepare_frozen_composite_and_borrowed_response() {
    let snapshot = snapshot_request();
    let encoded = encode_request(&request(&snapshot)).unwrap();
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/direct_prepare_payload_v1_request.hex"
    ));
    assert_eq!(encoded.bytes(), frozen);
    assert_eq!(frozen.len(), 963);
    let reply = encode_response(encoded.bytes(), prepared(b"opaque context")).unwrap();
    assert_eq!(
        reply,
        crate::tests::frozen_hex_fixture(include_str!(
            "testdata/direct_prepare_payload_v1_reply.hex"
        ))
    );
    assert_eq!(
        encode_request(&decode_request(encoded.bytes()).unwrap())
            .unwrap()
            .bytes(),
        encoded.bytes()
    );
    let decoded = encoded.decode_response(&reply).unwrap();
    assert_eq!(decoded.command, b"opaque context");
    assert_eq!(decoded.command.as_ptr(), reply[66..].as_ptr());
    assert_eq!(StorageRpcMessageKind::StagingDirectPrepare as u16, 183);
}

#[test]
fn complete_direct_prepare_binds_inputs_and_reply_route() {
    let snapshot = snapshot_request();
    let original = encode_request(&request(&snapshot)).unwrap();
    let reply = encode_response(original.bytes(), prepared(b"opaque context")).unwrap();
    let mut crossed_snapshot = snapshot.clone();
    // Valid changed absolute deadline in the nested settled-prefix carrier.
    crossed_snapshot[18] ^= 1;
    for field in 0..10 {
        let mut changed = request(&snapshot);
        match field {
            0 => changed.snapshot_request = &crossed_snapshot,
            1 => changed.snapshot_sha256[0] ^= 1,
            2 => changed.segment_crc64 ^= 1,
            3 => changed.commit.public_read = true,
            4 => changed.commit.owner = OwnerIdentity::from_principal("another-owner"),
            5 => changed.commit.etag_crc64 ^= 1,
            6 => changed.commit.metadata_blob = SerializedMetadataBlob::new(vec![1]),
            7 => changed.commit.system_metadata_blob = SerializedSystemMetadataBlob::new(vec![2]),
            8 => {
                changed.provisional = ObjectEncryption::SseS3(crate::SseS3ObjectState::new(
                    2, [8; 12], [9; 48], [10; 6],
                ))
            }
            9 => {
                let subject = crate::ObjectEncryptionSubject::standard_object(
                    1,
                    &crate::tests::bucket_name("bucket"),
                    &crate::tests::object_key("key"),
                    GenerationId::MIN,
                    &changed.provisional,
                );
                changed.commit.encryption = changed
                    .provisional
                    .bind_committed_object_ciphertexts(
                        &subject,
                        [0x61; 12],
                        [0x63; 48],
                        [0; 12],
                        Vec::new(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let changed = encode_request(&changed).unwrap();
        assert!(changed.decode_response(&reply).is_err(), "field {field}");
        assert!(original
            .decode_response(
                &encode_response(changed.bytes(), prepared(b"opaque context")).unwrap()
            )
            .is_err());
    }
    for field in 0..3 {
        let mut changed = prepared(b"opaque context");
        match field {
            0 => changed.node_id = NodeId::new(3),
            1 => changed.pg_id = PgId::new(3),
            2 => changed.epoch = ClusterEpoch::new(6).unwrap(),
            _ => unreachable!(),
        }
        assert!(encode_response(original.bytes(), changed).is_err());
        let mut crossed = reply.clone();
        crossed[[38, 50, 42][field]] ^= 1;
        assert!(original.decode_response(&crossed).is_err());
    }
}

#[test]
fn complete_direct_prepare_rejects_profile_version_and_malformed_framing() {
    let snapshot = snapshot_request();
    for field in 0..3 {
        let mut changed = request(&snapshot);
        match field {
            0 => changed.commit.encryption = changed.provisional.clone(),
            1 => changed.provisional = changed.commit.encryption.clone(),
            2 => changed.commit.versioning = BucketVersioningState::Enabled,
            _ => unreachable!(),
        }
        assert!(encode_request(&changed).is_err());
    }
    let request = encode_request(&request(&snapshot)).unwrap();
    let reply = encode_response(request.bytes(), prepared(b"opaque context")).unwrap();
    for end in 0..request.bytes().len() {
        assert!(
            decode_request(&request.bytes()[..end]).is_err(),
            "request {end}"
        );
    }
    for end in 0..reply.len() {
        assert!(
            request.decode_response(&reply[..end]).is_err(),
            "reply {end}"
        );
    }
    for offset in [0, 4] {
        let mut bytes = request.bytes().to_vec();
        bytes[offset] ^= 1;
        assert!(decode_request(&bytes).is_err());
        let mut bytes = reply.clone();
        bytes[offset] ^= 1;
        assert!(request.decode_response(&bytes).is_err());
    }
    let mut bytes = request.bytes().to_vec();
    bytes.push(0);
    assert!(decode_request(&bytes).is_err());
    let mut bytes = reply.clone();
    bytes.push(0);
    assert!(request.decode_response(&bytes).is_err());
    for offset in [6, 10 + snapshot.len() + 40] {
        let mut bytes = request.bytes().to_vec();
        bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
    }
}

#[test]
fn complete_direct_prepare_caps_include_request_only_fields_and_response_envelope() {
    let snapshot = snapshot_request();
    let request = encode_request(&request(&snapshot)).unwrap();
    let command = vec![0; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let reply = encode_response(request.bytes(), prepared(&command)).unwrap();
    assert_eq!(reply.len(), MAX_RESPONSE_BYTES);
    assert_eq!(
        request.decode_response(&reply).unwrap().command.len(),
        command.len()
    );
    for bytes in [
        vec![0; MAX_REQUEST_BYTES],
        encode_storage_rpc_success_response(&reply),
    ] {
        let frame =
            encode_storage_rpc_frame(17, StorageRpcMessageKind::StagingDirectPrepare, &bytes)
                .unwrap();
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, bytes);
    }
    // Direction-aware header admission rejects a request one byte above its cap.
    validate_storage_rpc_request_frame_payload_limit(&StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingDirectPrepare,
        payload: vec![0; MAX_REQUEST_BYTES],
    })
    .unwrap();
    let frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingDirectPrepare,
        payload: vec![0; MAX_REQUEST_BYTES + 1],
    };
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame),
        Err(StorageRpcFrameError::PayloadTooLarge { len, limit }) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    let mut oversized = reply;
    oversized.push(0);
    assert!(request.decode_response(&oversized).is_err());
}
