// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed framing evidence only. The opaque specimen is deliberately not a
//! decoded command, physical acknowledgement or installation capability.
use super::*;

fn input() -> Vec<u8> {
    crate::tests::frozen_hex_fixture(include_str!(
        "testdata/direct_prepare_payload_v1_request.hex"
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
fn complete_direct_install_frozen_composite_and_borrowed_command() {
    let preparation = input();
    let bytes = request(&preparation, b"opaque context");
    // Immutable DTIQ1 grammar composed with the already frozen
    // DTPQ1/DSNP1/descriptor1/state10/SSE-S3-5 specimen. Not a command fixture.
    let mut frozen = crate::tests::frozen_hex_fixture("445449510100c3030000");
    frozen.extend_from_slice(&preparation);
    frozen.extend_from_slice(&crate::tests::frozen_hex_fixture(
        "0400000005000000000000000400000013000000000000000e0000006f706171756520636f6e74657874",
    ));
    assert_eq!(bytes, frozen);
    assert_eq!(bytes.len(), 1015);
    let decoded = decode_request(&frozen).unwrap();
    assert_eq!(decoded.preparation, preparation);
    assert_eq!(decoded.preparation.as_ptr(), frozen[10..].as_ptr());
    assert_eq!(decoded.command.command, b"opaque context");
    assert_eq!(decoded.command.command.as_ptr(), frozen[1001..].as_ptr());
    assert_eq!(encode_request(decoded).unwrap(), frozen);
    let reply = confirmation(&frozen).unwrap();
    assert_eq!(
        reply,
        crate::tests::frozen_hex_fixture(
            "4454494301004a5e11e32c4cdae782a2670b1024aa3a6fb0a18f4ca1d9aa4b77d00082ee21c4",
        )
    );
    assert_eq!(reply.len(), CONFIRMATION_BYTES);
    require_confirmation(&frozen, &reply).unwrap();
    assert_eq!(StorageRpcMessageKind::StagingDirectInstall as u16, 184);
    assert_eq!(
        StorageRpcMessageKind::from_u16(184).unwrap(),
        StorageRpcMessageKind::StagingDirectInstall
    );
}

#[test]
fn complete_direct_install_confirmation_binds_preparation_command_and_route() {
    let preparation = input();
    let bytes = request(&preparation, b"opaque context");
    let reply = confirmation(&bytes).unwrap();
    for field in [
        "command",
        "checksum",
        "snapshot",
        "crc",
        "deadline",
        "logical",
        "provisional",
    ] {
        let mut prepared = preparation::decode_request(&preparation).unwrap();
        let mut snapshot = prepared.snapshot_request.to_vec();
        match field {
            "snapshot" => prepared.snapshot_sha256[0] ^= 1,
            "crc" => prepared.segment_crc64 ^= 1,
            "deadline" => snapshot[18] ^= 1,
            "logical" => prepared.commit.public_read = !prepared.commit.public_read,
            "provisional" => {
                prepared.provisional = ObjectEncryption::SseS3(crate::SseS3ObjectState::new(
                    2, [8; 12], [9; 48], [10; 6],
                ))
            }
            "command" | "checksum" => (),
            _ => unreachable!(),
        }
        prepared.snapshot_request = &snapshot;
        let prepared = preparation::encode_request(&prepared).unwrap();
        let mut changed = decode_request(&bytes).unwrap();
        changed.preparation = prepared.bytes();
        if field == "command" {
            changed.command.command = b"another context";
        }
        if field == "checksum" {
            changed.command.checksum ^= 1;
        }
        let changed = encode_request(changed).unwrap();
        assert_ne!(changed, bytes, "{field}");
        assert!(require_confirmation(&changed, &reply).is_err(), "{field}");
        assert!(
            require_confirmation(&bytes, &confirmation(&changed).unwrap()).is_err(),
            "{field}"
        );
    }
    for field in ["node", "pg", "epoch"] {
        let mut crossed = decode_request(&bytes).unwrap();
        let offset = match field {
            "node" => {
                crossed.command.node_id = NodeId::new(3);
                0
            }
            "epoch" => {
                crossed.command.epoch = ClusterEpoch::INITIAL;
                4
            }
            "pg" => {
                crossed.command.pg_id = PgId::new(3);
                12
            }
            _ => unreachable!(),
        };
        assert!(encode_request(crossed).is_err(), "{field}");
        let mut crossed = bytes.clone();
        crossed[10 + preparation.len() + offset] ^= 1;
        assert!(decode_request(&crossed).is_err(), "{field}");
        assert!(confirmation(&crossed).is_err(), "{field}");
    }
}

#[test]
fn complete_direct_install_rejects_malformed_and_cross_protocol_messages() {
    let preparation = input();
    let bytes = request(&preparation, b"opaque context");
    let reply = confirmation(&bytes).unwrap();
    for end in 0..bytes.len() {
        assert!(
            decode_request(&bytes[..end]).is_err(),
            "request truncation {end}"
        );
    }
    for end in 0..reply.len() {
        assert!(
            require_confirmation(&bytes, &reply[..end]).is_err(),
            "reply truncation {end}"
        );
    }
    for offset in [0, 4, 6] {
        let mut bad = bytes.clone();
        bad[offset] ^= 255;
        assert!(decode_request(&bad).is_err());
        let mut bad = reply.clone();
        bad[offset] ^= 255;
        assert!(require_confirmation(&bytes, &bad).is_err());
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(decode_request(&bad).is_err());
    let mut bad = reply.clone();
    bad.push(0);
    assert!(require_confirmation(&bytes, &bad).is_err());
    let mut bad = bytes.clone();
    bad[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_request(&bad),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    let mut bad = bytes.clone();
    bad[10 + preparation.len() + 24..10 + preparation.len() + 28].fill(0);
    assert!(decode_request(&bad).is_err());
    assert!(decode_request(&preparation).is_err());
    assert!(preparation::decode_request(&bytes).is_err());
    assert!(complete_stream_append_install::decode_request(&bytes).is_err());
    let stream_preparation = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/stream_append_prepare_payload_v1_request.hex"
    ));
    let stream =
        complete_stream_append_install::encode_request(complete_stream_append_install::Request {
            preparation: &stream_preparation,
            command: decode_request(&bytes).unwrap().command,
        })
        .unwrap();
    assert!(decode_request(&stream).is_err());
    assert!(require_confirmation(
        &bytes,
        &complete_stream_append_install::confirmation(&stream).unwrap()
    )
    .is_err());
}

#[test]
fn complete_direct_install_caps_preserve_maximum_command_and_frame_admission() {
    let preparation = input();
    let maximum = vec![0xa5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let bytes = request(&preparation, &maximum);
    assert_eq!(
        decode_request(&bytes).unwrap().command.command.len(),
        maximum.len()
    );
    let mut over = maximum;
    over.push(0);
    let mut decoded = decode_request(&bytes).unwrap();
    decoded.command.command = &over;
    assert!(encode_request(decoded).is_err());
    assert_eq!(
        MAX_REQUEST_BYTES,
        10 + preparation::MAX_REQUEST_BYTES + command::MAX_REQUEST_BYTES
    );
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingDirectInstall,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
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
    assert!(
        matches!(decode_request(&frame.payload), Err(StorageRpcPayloadError::PayloadTooLarge { len, limit }) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    let framed = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(framed)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
}
