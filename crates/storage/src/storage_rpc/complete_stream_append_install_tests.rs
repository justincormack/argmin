// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn input() -> Vec<u8> {
    crate::tests::frozen_hex_fixture(include_str!(
        "testdata/stream_append_prepare_payload_v1_request.hex"
    ))
}
fn request(preparation: &[u8], body: &[u8]) -> Vec<u8> {
    encode_request(Request {
        preparation,
        command: command::BorrowedRequest {
            node_id: NodeId::new(4),
            epoch: ClusterEpoch::new(5).unwrap(),
            pg_id: PgId::new(4),
            checksum: 19,
            command: body,
        },
    })
    .unwrap()
}

#[test]
fn complete_stream_append_install_framing_is_bounded_and_request_bound() {
    let preparation = input();
    let bytes = request(&preparation, b"opaque context");
    // Append-only composite: independent literal outer grammar plus the frozen
    // SAPP1/SART1/SFPI1/SALL1/descriptor1/state10/SSE-S3-5 specimen.
    let mut frozen = crate::tests::frozen_hex_fixture("53415049010055040000");
    frozen.extend_from_slice(&preparation);
    frozen.extend_from_slice(&crate::tests::frozen_hex_fixture(
        "0400000005000000000000000400000013000000000000000e0000006f706171756520636f6e74657874",
    ));
    assert_eq!(bytes, frozen);
    let decoded = decode_request(&bytes).unwrap();
    assert_eq!(decoded.preparation, preparation);
    assert_eq!(decoded.command.command, b"opaque context");
    assert_eq!(
        decoded.command.command.as_ptr(),
        bytes[bytes.len() - 14..].as_ptr()
    );
    assert_eq!(encode_request(decoded).unwrap(), bytes);
    let reply = confirmation(&bytes).unwrap();
    assert_eq!(
        reply,
        crate::tests::frozen_hex_fixture(
            "5341494301007288fee2e3c7d4710031fd103894ea8114c04682f59a472d60d0814ce3ca1980"
        )
    );
    require_confirmation(&bytes, &reply).unwrap();
    assert!(require_confirmation(&request(&preparation, b"crossed context"), &reply).is_err());
    for end in 0..bytes.len() {
        assert!(decode_request(&bytes[..end]).is_err(), "{end}");
    }
    for end in 0..reply.len() {
        assert!(require_confirmation(&bytes, &reply[..end]).is_err());
    }
    for offset in [0, 4, 6] {
        let mut bad = bytes.clone();
        bad[offset] ^= 255;
        assert!(decode_request(&bad).is_err());
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(decode_request(&bad).is_err());
    let mut bad = reply.clone();
    bad.push(0);
    assert!(require_confirmation(&bytes, &bad).is_err());
    for offset in [0, 4, 6] {
        let mut bad = reply.clone();
        bad[offset] ^= 255;
        assert!(require_confirmation(&bytes, &bad).is_err());
    }
    assert!(decode_request(&preparation).is_err());
    assert!(preparation::decode_request(&bytes).is_err());
    for field in ["node", "pg", "epoch"] {
        let mut decoded = decode_request(&bytes).unwrap();
        match field {
            "node" => decoded.command.node_id = NodeId::new(1),
            "pg" => decoded.command.pg_id = PgId::new(0),
            "epoch" => decoded.command.epoch = ClusterEpoch::INITIAL,
            _ => unreachable!(),
        }
        assert!(encode_request(decoded).is_err());
        let mut bad = bytes.clone();
        let offset = 10
            + preparation.len()
            + match field {
                "node" => 0,
                "epoch" => 4,
                "pg" => 12,
                _ => unreachable!(),
            };
        bad[offset] ^= 1;
        assert!(decode_request(&bad).is_err());
    }
    let maximum = vec![0xa5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let bytes = request(&preparation, &maximum);
    assert_eq!(
        decode_request(&bytes).unwrap().command.command.len(),
        maximum.len()
    );
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingStreamAppendInstall,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
    assert_eq!(frame.kind as u16, 181);
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    let framed = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(framed))
            .unwrap()
            .payload
            .len(),
        MAX_REQUEST_BYTES
    );
    frame.payload.push(0);
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame), Err(StorageRpcFrameError::PayloadTooLarge { len, limit }) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    assert!(decode_request(&frame.payload).is_err());
    let framed = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(framed)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
}
