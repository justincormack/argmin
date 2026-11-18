// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn complete_abandonment_wire_frozen_composites_and_exact_frame_cap() {
    use std::io::Cursor;
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/complete_abandonment_payload_v1_command_v14_context_v1.hex"
    ));
    let response = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/complete_abandonment_confirmation_v1_command_v14_context_v1.hex"
    ));
    let decoded = decode_request(&request).unwrap();
    assert_eq!(decoded.deadline.portable_wall_valid_until_ms, 50_000);
    assert_eq!(decoded.command.node_id, NodeId::new(4));
    assert_eq!(decoded.command.epoch, ClusterEpoch::new(5).unwrap());
    assert_eq!(decoded.command.pg_id, PgId::new(4));
    assert_eq!(encode_request(decoded).unwrap(), request);
    let id = MetadataCommandId::new(
        ClusterEpoch::new(5).unwrap(),
        PgId::new(4),
        MetadataCommandLogIndex::new(11).unwrap(),
    );
    let sha: [u8; 32] = crate::tests::frozen_hex_fixture(
        "5c1438e559d8e1dd4a19e1f6808e6693645dc489063c10cbf0835a979a1115f8",
    )
    .try_into()
    .unwrap();
    assert_eq!(
        encode_confirmation(Confirmation {
            node_id: NodeId::new(4),
            id,
            command_sha256: sha
        }),
        response
    );
    decode_confirmation_for(&response, NodeId::new(4), id, sha).unwrap();
    let kind = StorageRpcMessageKind::MetadataCommandRecordAbandoned;
    for (payload, frozen) in [
        (request, include_str!("testdata/complete_abandonment_rpc_v37_payload_v1_command_v14_context_v1_request.hex")),
        (encode_storage_rpc_success_response(&response), include_str!("testdata/complete_abandonment_rpc_v37_payload_v1_command_v14_context_v1_response.hex")),
    ] {
        let frame = crate::tests::frozen_hex_fixture(frozen);
        assert_eq!(encode_storage_rpc_frame(17, kind, &payload).unwrap(), frame);
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
        for version in [36, 38] {
            let wrong = encode_storage_rpc_frame_with_version_for_test(17, kind, &payload, version);
            assert!(matches!(read_storage_rpc_request_frame_from(&mut Cursor::new(wrong)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::UnsupportedVersion(_)))));
        }
    }
    // A maximal command fits the new payload including the required cutoff.
    const {
        assert!(MAX_REQUEST_BYTES > STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN);
    }
    let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let request = encode_request(BorrowedRequest {
        command: complete_applied::BorrowedRequest {
            node_id: NodeId::new(4),
            epoch: id.cluster_epoch(),
            pg_id: id.pg_id(),
            checksum: 19,
            command: &body,
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
    })
    .unwrap();
    assert_eq!(request.len(), MAX_REQUEST_BYTES);
    assert_eq!(
        decode_request(&request).unwrap().command.command.as_ptr(),
        request[42..].as_ptr()
    );
    let frame_cap = complete_replica_abandonment::MAX_REQUEST_BYTES;
    for extra in [0, 1] {
        let frame = encode_storage_rpc_frame(17, kind, &vec![0; frame_cap + extra]).unwrap();
        let result = read_storage_rpc_request_frame_from(&mut Cursor::new(frame));
        if extra == 0 {
            assert_eq!(result.unwrap().payload.len(), frame_cap);
        } else {
            assert!(
                matches!(result, Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == frame_cap + 1 && limit == frame_cap)
            );
        }
    }
}

#[test]
fn complete_abandonment_wire_rejects_wrong_purpose_subject_version_length_and_trailing_bytes() {
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/complete_abandonment_payload_v1_command_v14_context_v1.hex"
    ));
    let response = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/complete_abandonment_confirmation_v1_command_v14_context_v1.hex"
    ));
    let id = MetadataCommandId::new(
        ClusterEpoch::new(5).unwrap(),
        PgId::new(4),
        MetadataCommandLogIndex::new(11).unwrap(),
    );
    let sha: [u8; 32] = response[30..].try_into().unwrap();
    let confirm = |bytes: &[u8]| decode_confirmation_for(bytes, NodeId::new(4), id, sha);
    for length in 0..request.len() {
        assert!(decode_request(&request[..length]).is_err(), "{length}");
    }
    for length in 0..response.len() {
        assert!(confirm(&response[..length]).is_err(), "{length}");
    }
    for offset in [0, 4, 6, 10, 18, 22, 30] {
        let mut crossed = response.clone();
        crossed[offset] ^= 1;
        assert!(confirm(&crossed).is_err(), "{offset}");
    }
    for version in [0_u16, 2] {
        let mut wrong = request.clone();
        wrong[4..6].copy_from_slice(&version.to_le_bytes());
        assert!(decode_request(&wrong).is_err());
    }
    for purpose in [b"APLY", b"ABOK"] {
        let mut wrong = request.clone();
        wrong[..4].copy_from_slice(purpose);
        assert!(decode_request(&wrong).is_err());
    }
    let mut trailing = request.clone();
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    let mut trailing = response.clone();
    trailing.push(0);
    assert!(confirm(&trailing).is_err());
    assert!(confirm(&request).is_err());
    assert!(decode_request(&response).is_err());
    assert!(confirm(&complete_publication::encode_confirmation(
        complete_publication::Confirmation {
            node_id: NodeId::new(4),
            id,
            command_sha256: sha
        }
    ))
    .is_err());
    assert!(complete_publication::decode_request(&request).is_err());
    assert!(complete_application::decode_request(&request).is_err());
    let oversized = vec![0; MAX_REQUEST_BYTES + 1];
    assert!(matches!(
        decode_request(&oversized),
        Err(StorageRpcPayloadError::PayloadTooLarge {
            limit: MAX_REQUEST_BYTES,
            ..
        })
    ));
}
