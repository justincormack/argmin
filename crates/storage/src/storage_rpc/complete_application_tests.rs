// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn complete_application_wire_frozen_v37_request_response_composites() {
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/complete_application_payload_v1_command_v14_context_v1.hex"
    ));
    assert_eq!(
        encode_request(decode_request(&request).unwrap()).unwrap(),
        request
    );
    let response = crate::tests::frozen_hex_fixture(include_str!("../pg_store/testdata/complete_application_confirmation_v1_command_v14_context_v1_state_v10.hex"));
    let expected = Confirmation {
        node_id: NodeId::new(4),
        id: MetadataCommandId::new(
            ClusterEpoch::new(5).unwrap(),
            PgId::new(4),
            MetadataCommandLogIndex::new(11).unwrap(),
        ),
        command_sha256: crate::tests::frozen_hex_fixture(
            "5c1438e559d8e1dd4a19e1f6808e6693645dc489063c10cbf0835a979a1115f8",
        )
        .try_into()
        .unwrap(),
        state: MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(5).unwrap(),
            applied_log_index: 11,
            applied_log_hash: MetadataCommandLogHash::for_test(17),
            state_digest: CanonicalStateDigest::for_test(19),
        },
    };
    assert_eq!(decode_confirmation(&response).unwrap(), expected);
    assert_eq!(encode_confirmation(expected), response);
    let kind = StorageRpcMessageKind::MetadataCommandApplyAndRecord;
    for (payload, literal) in [
        (request, include_str!("testdata/complete_application_rpc_v37_payload_v1_command_v14_context_v1_request.hex")),
        (encode_storage_rpc_success_response(&response), include_str!("testdata/complete_application_rpc_v37_payload_v1_command_v14_context_v1_state_v10_response.hex")),
    ] {
        let frozen = crate::tests::frozen_hex_fixture(literal);
        assert_eq!(encode_storage_rpc_frame(17, kind, &payload).unwrap(), frozen);
        let decoded = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(&frozen)).unwrap();
        assert_eq!(decoded.kind, kind); assert_eq!(decoded.request_id, 17); assert_eq!(decoded.payload, payload);
        for version in [36, 38] {
            let other = encode_storage_rpc_frame_with_version_for_test(17, kind, &payload, version);
            assert!(matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(other)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::UnsupportedVersion(_)))));
        }
    }
}

#[test]
fn complete_application_wire_strict_purpose_version_length_and_exact_cap() {
    use std::io::Cursor;
    let request = |command| BorrowedRequest {
        command: complete_applied::BorrowedRequest {
            node_id: NodeId::new(4),
            epoch: ClusterEpoch::new(5).unwrap(),
            pg_id: PgId::new(7),
            checksum: 19,
            command,
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
    };
    let bytes = encode_request(request(&[0xAA, 0xBB])).unwrap();
    let frozen = crate::tests::frozen_hex_fixture(
        "41504c59010050c300000000000004000000050000000000000007000000130000000000000002000000aabb",
    );
    assert_eq!(bytes, frozen);
    for length in 0..bytes.len() {
        assert!(decode_request(&bytes[..length]).is_err(), "{length}");
    }
    for offset in [0, 4] {
        let mut wrong = bytes.clone();
        wrong[offset] ^= 1;
        assert!(decode_request(&wrong).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let bytes = encode_request(request(&body)).unwrap();
    assert_eq!(bytes.len(), MAX_REQUEST_BYTES);
    let decoded = decode_request(&bytes).unwrap();
    assert_eq!(decoded.command.command, body);
    assert_eq!(decoded.command.command.as_ptr(), bytes[42..].as_ptr());
    let kind = StorageRpcMessageKind::MetadataCommandApplyAndRecord;
    for extra in [0, 1] {
        let frame =
            encode_storage_rpc_frame(17, kind, &vec![0; MAX_REQUEST_BYTES + extra]).unwrap();
        let result = read_storage_rpc_request_frame_from(&mut Cursor::new(frame));
        if extra == 0 {
            assert_eq!(result.unwrap().payload.len(), MAX_REQUEST_BYTES);
        } else {
            assert!(
                matches!(result, Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
            );
        }
    }
    assert!(encode_request(request(&[])).is_err());
    assert!(encode_request(request(&bytes)).is_err());
    assert!(decode_request(&vec![0; MAX_REQUEST_BYTES + 1]).is_err());
    for previous in [
        crate::tests::frozen_hex_fixture(include_str!(
            "../pg_store/testdata/complete_publication_payload_v1_command_v14_context_v1.hex"
        )),
        crate::tests::frozen_hex_fixture(include_str!(
            "../pg_store/testdata/complete_applied_rpc_v37_command_v14_context_v1.hex"
        )),
    ] {
        assert!(
            decode_request(&previous).is_err(),
            "no publication/probe fallback"
        );
    }
}

#[test]
fn complete_application_wire_confirmation_is_strict_and_binds_prefix() {
    let confirmation = || Confirmation {
        node_id: NodeId::new(4),
        id: MetadataCommandId::new(
            ClusterEpoch::new(5).unwrap(),
            PgId::new(7),
            MetadataCommandLogIndex::new(11).unwrap(),
        ),
        command_sha256: [0xA5; 32],
        state: MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(5).unwrap(),
            applied_log_index: 11,
            applied_log_hash: MetadataCommandLogHash::for_test(17),
            state_digest: CanonicalStateDigest::for_test(19),
        },
    };
    let bytes = encode_confirmation(confirmation());
    assert_eq!(bytes.len(), RESPONSE_BYTES);
    assert_eq!(decode_confirmation(&bytes).unwrap(), confirmation());
    for length in 0..bytes.len() {
        assert!(decode_confirmation(&bytes[..length]).is_err(), "{length}");
    }
    for offset in [0, 4, 62, 78, 87] {
        let mut wrong = bytes.clone();
        wrong[offset] ^= 0xFF;
        assert!(decode_confirmation(&wrong).is_err(), "{offset}");
    }
    let mut lower = confirmation();
    lower.state.applied_log_index = 10;
    assert!(decode_confirmation(&encode_confirmation(lower)).is_err());
    let mut later = confirmation();
    later.state.applied_log_index = 12;
    assert!(
        decode_confirmation(&encode_confirmation(later)).is_ok(),
        "exact replay may observe a later prefix"
    );
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_confirmation(&trailing).is_err());
}
