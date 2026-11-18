// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn complete_direct_introduction_distinct_kind_and_directional_frame_cap() {
    let kind = StorageRpcMessageKind::StagingDirectIntroductionPrepare;
    assert_eq!(kind as u16, 185);
    assert_eq!(StorageRpcMessageKind::from_u16(185).unwrap(), kind);
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    let bytes = encode_storage_rpc_frame(frame.request_id, kind, &frame.payload).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes))
            .unwrap()
            .payload
            .len(),
        MAX_REQUEST_BYTES
    );
    frame.payload.push(0);
    let bytes = encode_storage_rpc_frame(frame.request_id, kind, &frame.payload).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
}

fn request() -> Request {
    let bucket = crate::tests::bucket_name("bucket");
    let key = crate::tests::object_key("key");
    let epoch = ClusterEpoch::new(5).unwrap();
    Request {
        route: route::Request {
            node: NodeId::new(4),
            epoch,
            pg: PgId::new(3),
            topology: [0xa5; 32],
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 10_000,
            },
        },
        bucket: bucket.clone(),
        key: key.clone(),
        attempt: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
        proof: BucketWriteReservationProof {
            bucket,
            reservation_id: "reservation".into(),
            owner_token: "owner".into(),
            cluster_epoch: epoch,
            bucket_execution_generation: 2,
            bucket_incarnation_generation: 1,
            operation_kind:
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into(),
            created_at: 1_000,
            lease_deadline: 20_000,
            target_context: Some(key.to_string()),
        },
    }
}

fn prepared(bytes: &[u8]) -> command::BorrowedRequest<'_> {
    command::BorrowedRequest {
        node_id: NodeId::new(4),
        epoch: ClusterEpoch::new(5).unwrap(),
        pg_id: PgId::new(3),
        checksum: 0x12345678,
        command: bytes,
    }
}

fn cutoff() -> StorageRpcOperationDeadline {
    StorageRpcOperationDeadline {
        portable_wall_valid_until_ms: 8_000,
    }
}

#[test]
fn complete_direct_introduction_frozen_framing_and_borrowed_response() {
    let encoded = encode_request(&request()).unwrap();
    assert_eq!(
        encoded.bytes(),
        crate::tests::frozen_hex_fixture(include_str!(
            "testdata/direct_introduction_v1_request.hex"
        ))
    );
    assert_eq!(
        encode_request(&decode_request(encoded.bytes()).unwrap())
            .unwrap()
            .bytes(),
        encoded.bytes()
    );
    // Deliberately opaque bytes: framing does not mint a decoded command or
    // claim that its checksum/context, durable proof or pending slot is valid.
    let reply = encode_response(&encoded, prepared(b"opaque context"), cutoff()).unwrap();
    assert_eq!(
        reply,
        crate::tests::frozen_hex_fixture(include_str!("testdata/direct_introduction_v1_reply.hex"))
    );
    let decoded = encoded.decode_response(&reply).unwrap();
    assert_eq!(decoded.command.command, b"opaque context");
    assert_eq!(
        decoded.command.command.as_ptr(),
        reply[RESPONSE_HEADER_BYTES + 28..].as_ptr()
    );
    assert_eq!(decoded.cutoff, cutoff());
}

#[test]
fn complete_direct_introduction_rejects_crossed_subjects_and_invalid_proofs() {
    for field in 0..12 {
        let mut changed = request();
        match field {
            0 => changed.proof.bucket = crate::tests::bucket_name("other"),
            1 => changed.proof.target_context = Some("other".into()),
            2 => changed.proof.operation_kind = "put-object-stream-create".into(),
            3 => changed.proof.cluster_epoch = ClusterEpoch::new(6).unwrap(),
            4 => changed.proof.reservation_id.clear(),
            5 => changed.proof.owner_token = "x".repeat(257),
            6 => changed.proof.reservation_id = "x".repeat(257),
            7 => changed.proof.bucket_incarnation_generation = 0,
            8 => changed.proof.bucket_execution_generation = 0,
            9 => changed.proof.lease_deadline = changed.proof.created_at,
            10 => changed.proof.owner_token = "\0owner".into(),
            11 => changed.route.deadline.portable_wall_valid_until_ms = 0,
            _ => unreachable!(),
        }
        assert!(encode_request(&changed).is_err(), "field {field}");
    }
    let mut maximal = request();
    maximal.proof.owner_token = "x".repeat(256);
    maximal.proof.reservation_id = "y".repeat(256);
    decode_request(encode_request(&maximal).unwrap().bytes()).unwrap();
    // Decode must enforce the narrower source proof bound too, not just encoding.
    let encoded = encode_request(&request()).unwrap();
    let mut raw = encoded.bytes().to_vec();
    let token = raw.windows(5).position(|bytes| bytes == b"owner").unwrap();
    raw[token - 4..token].copy_from_slice(&257u32.to_le_bytes());
    raw.splice(token..token + 5, vec![b'x'; 257]);
    assert!(decode_request(&raw).is_err());
}

#[test]
fn complete_direct_introduction_binds_every_request_field_and_response_cutoff() {
    let encoded = encode_request(&request()).unwrap();
    let reply = encode_response(&encoded, prepared(b"opaque context"), cutoff()).unwrap();
    for field in 0..14 {
        let mut changed = request();
        match field {
            0 => changed.route.node = NodeId::new(1),
            1 => changed.route.pg = PgId::new(2),
            2 => changed.route.topology[0] ^= 1,
            3 => changed.route.deadline.portable_wall_valid_until_ms += 1,
            4 => {
                changed.bucket = crate::tests::bucket_name("other");
                changed.proof.bucket = changed.bucket.clone();
            }
            5 => {
                changed.key = crate::tests::object_key("other");
                changed.proof.target_context = Some(changed.key.to_string());
            }
            6 => changed.attempt = SessionId::try_from("1123456789abcdef0123456789abcdef").unwrap(),
            7 => changed.proof.reservation_id.push('x'),
            8 => changed.proof.owner_token.push('x'),
            9 => changed.proof.created_at += 1,
            10 => changed.proof.lease_deadline += 1,
            11 => changed.proof.bucket_execution_generation += 1,
            12 => changed.proof.bucket_incarnation_generation += 1,
            13 => {
                changed.route.epoch = ClusterEpoch::new(6).unwrap();
                changed.proof.cluster_epoch = changed.route.epoch;
            }
            _ => unreachable!(),
        }
        assert!(
            encode_request(&changed)
                .unwrap()
                .decode_response(&reply)
                .is_err(),
            "field {field}"
        );
    }
    for field in 0..5 {
        let mut reply = reply.clone();
        match field {
            0 => reply[RESPONSE_HEADER_BYTES] ^= 1,      // node
            1 => reply[RESPONSE_HEADER_BYTES + 4] ^= 1,  // epoch
            2 => reply[RESPONSE_HEADER_BYTES + 12] ^= 1, // PG
            3 => reply[RESPONSE_HEADER_BYTES - 8..RESPONSE_HEADER_BYTES]
                .copy_from_slice(&10_001u64.to_le_bytes()),
            4 => *reply.last_mut().unwrap() ^= 1, // complete bytes, same claimed SHA
            _ => unreachable!(),
        }
        assert!(
            encoded.decode_response(&reply).is_err(),
            "reply field {field}"
        );
    }
    assert!(encode_response(
        &encoded,
        prepared(b"opaque context"),
        StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 10_001
        }
    )
    .is_err());
}

#[test]
fn complete_direct_introduction_bounded_truncation_unknown_versions_and_trailing_bytes() {
    let encoded = encode_request(&request()).unwrap();
    let reply = encode_response(&encoded, prepared(b"opaque context"), cutoff()).unwrap();
    for end in 0..encoded.bytes().len() {
        assert!(decode_request(&encoded.bytes()[..end]).is_err());
    }
    for end in 0..reply.len() {
        assert!(encoded.decode_response(&reply[..end]).is_err());
    }
    for offset in [0, 4, 6] {
        let mut bad = encoded.bytes().to_vec();
        bad[offset] ^= 2;
        assert!(decode_request(&bad).is_err());
    }
    let mut bad = encoded.bytes().to_vec();
    bad.push(0);
    assert!(decode_request(&bad).is_err());
    let mut bad = reply;
    bad.push(0);
    assert!(encoded.decode_response(&bad).is_err());
    assert!(decode_request(&vec![0; MAX_REQUEST_BYTES + 1]).is_err());
    assert!(encoded
        .decode_response(&vec![0; MAX_RESPONSE_BYTES + 1])
        .is_err());
}
