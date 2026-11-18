// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn fixtures() -> (Vec<u8>, Vec<u8>) {
    (crate::tests::frozen_hex_fixture(include_str!("../pg_store/testdata/complete_replica_abandonment_payload_v1_command_v14_context_v1.hex")),
     crate::tests::frozen_hex_fixture(include_str!("../pg_store/testdata/complete_replica_abandonment_confirmation_v1_command_v14_context_v1.hex")))
}

#[test]
fn complete_replica_abandonment_frozen_composites_and_exact_frame_cap() {
    let (request, response) = fixtures();
    let decoded = decode_request(&request).unwrap();
    assert_eq!(decoded.primary, NodeId::new(4));
    assert_eq!(decoded.command.node_id, NodeId::new(0));
    assert_eq!(decoded.deadline.portable_wall_valid_until_ms, 50_000);
    assert_eq!(encode_request(decoded).unwrap(), request);
    let id = MetadataCommandId::new(
        ClusterEpoch::new(5).unwrap(),
        PgId::new(4),
        MetadataCommandLogIndex::new(11).unwrap(),
    );
    let sha = crate::tests::frozen_hex_fixture(
        "5c1438e559d8e1dd4a19e1f6808e6693645dc489063c10cbf0835a979a1115f8",
    )
    .try_into()
    .unwrap();
    assert_eq!(
        encode_confirmation(
            NodeId::new(4),
            complete_abandonment::Confirmation {
                node_id: NodeId::new(0),
                id,
                command_sha256: sha
            }
        ),
        response
    );
    decode_confirmation_for(&response, NodeId::new(4), NodeId::new(0), id, sha).unwrap();
    let kind = StorageRpcMessageKind::MetadataCommandRecordAbandoned;
    for (payload, frozen) in [
        (request, include_str!("testdata/complete_replica_abandonment_rpc_v37_payload_v1_command_v14_context_v1_request.hex")),
        (encode_storage_rpc_success_response(&response), include_str!("testdata/complete_replica_abandonment_rpc_v37_payload_v1_command_v14_context_v1_response.hex")),
    ] {
        let frame = crate::tests::frozen_hex_fixture(frozen);
        assert_eq!(encode_storage_rpc_frame(17, kind, &payload).unwrap(), frame);
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
    }
    let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let payload = encode_request(BorrowedRequest {
        primary: NodeId::new(4),
        command: complete_applied::BorrowedRequest {
            node_id: NodeId::new(0),
            epoch: id.cluster_epoch(),
            pg_id: id.pg_id(),
            checksum: 17,
            command: &body,
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
    })
    .unwrap();
    assert_eq!(payload.len(), MAX_REQUEST_BYTES);
    assert_eq!(
        decode_request(&payload).unwrap().command.command.as_ptr(),
        payload[46..].as_ptr()
    );
    let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(frame))
            .unwrap()
            .payload,
        payload
    );
    let frame = encode_storage_rpc_frame(17, kind, &vec![0; MAX_REQUEST_BYTES + 1]).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(frame)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
}

#[test]
fn complete_replica_abandonment_rejects_crossed_purpose_roles_subject_and_framing() {
    let (request, response) = fixtures();
    let id = MetadataCommandId::new(
        ClusterEpoch::new(5).unwrap(),
        PgId::new(4),
        MetadataCommandLogIndex::new(11).unwrap(),
    );
    let sha = response[34..].try_into().unwrap();
    let confirm =
        |bytes: &[u8]| decode_confirmation_for(bytes, NodeId::new(4), NodeId::new(0), id, sha);
    for length in 0..request.len() {
        assert!(decode_request(&request[..length]).is_err(), "{length}");
    }
    for length in 0..response.len() {
        assert!(confirm(&response[..length]).is_err(), "{length}");
    }
    for offset in [0, 4, 6, 10, 14, 22, 26, 34] {
        let mut crossed = response.clone();
        crossed[offset] ^= 1;
        assert!(confirm(&crossed).is_err(), "{offset}");
    }
    for version in [0_u16, 2] {
        let mut wrong = request.clone();
        wrong[4..6].copy_from_slice(&version.to_le_bytes());
        assert!(decode_request(&wrong).is_err());
    }
    let mut crossed = request.clone();
    crossed[14..18].copy_from_slice(&0_u32.to_le_bytes());
    assert!(decode_request(&crossed).is_err());
    let mut decoded = decode_request(&request).unwrap();
    decoded.primary = decoded.command.node_id;
    assert!(encode_request(decoded).is_err());
    for purpose in [b"ABND", b"ABOK", b"APLY", b"RAOK"] {
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
    assert!(decode_request(&response).is_err());
    assert!(confirm(&request).is_err());
    assert!(complete_abandonment::decode_request(&request).is_err());
    assert!(
        complete_abandonment::decode_confirmation_for(&response, NodeId::new(0), id, sha).is_err()
    );
    assert!(complete_publication::decode_request(&request).is_err());
    assert!(complete_application::decode_request(&request).is_err());
    assert!(matches!(
        decode_request(&vec![0; MAX_REQUEST_BYTES + 1]),
        Err(StorageRpcPayloadError::PayloadTooLarge {
            limit: MAX_REQUEST_BYTES,
            ..
        })
    ));
}
