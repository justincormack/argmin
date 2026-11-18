// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Wire evidence only. These fixtures do not assert durable receipts or mint a
//! prepared payload. Subject validation and custody integration remain separate.
use super::*;

fn descriptor() -> Vec<u8> {
    crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ))
}

fn request(descriptor: &[u8]) -> ack::Request<'_> {
    ack::Request {
        receiver: complete_settled_prefix::Request {
            node: NodeId::new(4),
            pg: PgId::new(4),
            epoch: ClusterEpoch::new(5).unwrap(),
            topology: [0xa5; 32],
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 10_000,
            },
        },
        provenance: ack::Provenance::Direct {
            descriptor,
            logical_size: 4,
        },
        receipts: (0..3)
            .map(|shard_index| ack::Receipt {
                shard_index,
                ack: WriteAck {
                    stored_size: 38,
                    crc64: 0x1234 + u64::from(shard_index),
                },
            })
            .collect(),
    }
}

#[test]
fn complete_direct_snapshot_frozen_composite_and_borrowed_reply() {
    let descriptor = descriptor();
    let encoded = encode_request(&request(&descriptor)).unwrap();
    // Literal outer and acknowledgement headers, plus the independently frozen
    // descriptor. Do not derive versions, field order, or reply digest here.
    let mut expected = crate::tests::frozen_hex_fixture(concat!(
        "44534e500100230200005341525401000100102700000000000004000000050000000000000004000000",
        "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5",
        "000400000000000000a1010000",
    ));
    expected.extend_from_slice(&descriptor);
    expected.extend_from_slice(&crate::tests::frozen_hex_fixture(
        "0300002600000000000000341200000000000001260000000000000035120000000000000226000000000000003612000000000000",
    ));
    assert_eq!(encoded.bytes(), expected);
    assert_eq!(encoded.bytes().len(), 557);
    assert_eq!(
        encode_request(&decode_request(&expected).unwrap())
            .unwrap()
            .bytes(),
        expected
    );
    // Six absent fields in the current untagged direct-commit snapshot grammar.
    let snapshot = [0; 6];
    let expected_reply = crate::tests::frozen_hex_fixture(
        "44534e52010009ef5cd13999ef1bb54134e3af4a437837ebe8b0b14da51742f59241b837bafc06000000000000000000",
    );
    let reply = encode_response(encoded.bytes(), &snapshot).unwrap();
    assert_eq!(reply, expected_reply);
    let borrowed = encoded.decode_response(&reply).unwrap();
    assert_eq!(borrowed, snapshot);
    assert_eq!(borrowed.as_ptr(), reply[42..].as_ptr());
    let decoded = decode_direct_put_commit_snapshot_response(borrowed).unwrap();
    assert_eq!(
        encode_direct_put_commit_snapshot_response(&decoded),
        snapshot
    );
    assert!(decoded.snapshot.current.is_none());

    assert_eq!(StorageRpcMessageKind::StagingDirectSnapshot as u16, 182);
    for payload in [expected, encode_storage_rpc_success_response(&reply)] {
        let frame =
            encode_storage_rpc_frame(17, StorageRpcMessageKind::StagingDirectSnapshot, &payload)
                .unwrap();
        let frame = decode_storage_rpc_frame(&frame).unwrap();
        assert_eq!(frame.kind, StorageRpcMessageKind::StagingDirectSnapshot);
        assert_eq!(frame.payload, payload);
    }
}

#[test]
fn complete_direct_snapshot_binds_every_request_field_without_minting_authority() {
    let descriptor = descriptor();
    let original = encode_request(&request(&descriptor)).unwrap();
    let response = encode_response(original.bytes(), &[0; 6]).unwrap();
    let mut other_descriptor = descriptor.clone();
    other_descriptor[20] ^= 1;
    for case in 0..10 {
        let mut other = request(&descriptor);
        match case {
            0 => other.receiver.node = NodeId::new(3),
            1 => other.receiver.pg = PgId::new(3),
            2 => other.receiver.epoch = ClusterEpoch::new(6).unwrap(),
            3 => other.receiver.topology[0] ^= 1,
            4 => other.receiver.deadline.portable_wall_valid_until_ms -= 1,
            5 => {
                other.provenance = ack::Provenance::Direct {
                    descriptor: &other_descriptor,
                    logical_size: 4,
                }
            }
            6 => {
                other.provenance = ack::Provenance::Direct {
                    descriptor: &descriptor,
                    logical_size: 5,
                }
            }
            7 => other.receipts[0].ack.crc64 ^= 1,
            8 => {
                other.receipts.pop();
            }
            9 => other
                .receipts
                .iter_mut()
                .for_each(|r| r.ack.stored_size += 1),
            _ => unreachable!(),
        }
        // Framing is not descriptor/receipt authority; these are deliberately
        // accepted as distinct wire requests, never as a prepared payload.
        let other = encode_request(&other).unwrap();
        assert!(other.decode_response(&response).is_err(), "case {case}");
        let other_reply = encode_response(other.bytes(), &[0; 6]).unwrap();
        assert!(
            original.decode_response(&other_reply).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn complete_direct_snapshot_rejects_stream_provenance_and_noncanonical_receipts() {
    let descriptor = descriptor();
    let original = request(&descriptor);
    for case in 0..5 {
        let mut request = request(&descriptor);
        match case {
            0 => request.receipts.clear(),
            1 => request.receipts[1].shard_index = 0,
            2 => request.receipts[1].ack.stored_size = 0,
            3 => request.receipts[1].ack.stored_size += 1,
            4 => request.receiver.deadline.portable_wall_valid_until_ms = 0,
            _ => unreachable!(),
        }
        assert!(encode_request(&request).is_err(), "case {case}");
    }
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/stream_append_prepare_payload_v1_request.hex"
    ));
    let stream = complete_stream_append_prepare::decode_request(&frozen).unwrap();
    assert!(encode_request(&stream).is_err());
    // Correctly framed nested stream provenance must also fail on decoding.
    let nested = ack::encode_request(&stream).unwrap();
    let mut bytes = b"DSNP\x01\x00".to_vec();
    bytes.extend_from_slice(&(nested.bytes().len() as u32).to_le_bytes());
    bytes.extend_from_slice(nested.bytes());
    assert!(decode_request(&bytes).is_err());
    let direct = encode_request(&original).unwrap();
    assert!(complete_stream_append_prepare::decode_request(direct.bytes()).is_err());
    assert!(ack::decode_request(direct.bytes()).is_err());
    assert!(decode_direct_put_commit_snapshot_request(direct.bytes()).is_err());
}

#[test]
fn complete_direct_snapshot_rejects_truncation_trailing_bytes_versions_and_lengths() {
    let descriptor = descriptor();
    let request = encode_request(&request(&descriptor)).unwrap();
    let reply = encode_response(request.bytes(), &[0; 6]).unwrap();
    for len in 0..request.bytes().len() {
        assert!(
            decode_request(&request.bytes()[..len]).is_err(),
            "request {len}"
        );
    }
    for len in 0..reply.len() {
        assert!(
            request.decode_response(&reply[..len]).is_err(),
            "reply {len}"
        );
    }
    for bytes in [request.bytes(), reply.as_slice()] {
        for offset in [0, 4, 5] {
            let mut malformed = bytes.to_vec();
            malformed[offset] ^= 1;
            assert!(decode_request(&malformed).is_err());
            assert!(request.decode_response(&malformed).is_err());
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_request(&trailing).is_err());
        assert!(request.decode_response(&trailing).is_err());
    }
    let mut oversized = request.bytes().to_vec();
    oversized[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_request(&oversized),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    let mut oversized = reply.clone();
    oversized[38..42].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        request.decode_response(&oversized),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    let mut empty = reply[..42].to_vec();
    empty[38..42].copy_from_slice(&0u32.to_le_bytes());
    assert!(request.decode_response(&empty).is_err());
    assert!(encode_response(request.bytes(), &[]).is_err());
}

#[test]
fn complete_direct_snapshot_exact_caps_include_response_envelope() {
    assert_eq!(MAX_REQUEST_BYTES, 17_017);
    let max_descriptor = vec![0; 16 * 1024];
    let mut largest = request(&max_descriptor);
    largest.receipts = (0..32)
        .map(|shard_index| ack::Receipt {
            shard_index,
            ack: WriteAck {
                stored_size: 38,
                crc64: 0,
            },
        })
        .collect();
    let largest = encode_request(&largest).unwrap();
    assert_eq!(largest.bytes().len(), MAX_REQUEST_BYTES);
    decode_request(largest.bytes()).unwrap();
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingDirectSnapshot,
        payload: largest.bytes().to_vec(),
    };
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    frame.payload.push(0);
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame),
        Err(StorageRpcFrameError::PayloadTooLarge { len, limit }) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    assert!(matches!(
        decode_request(&frame.payload),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    let descriptor = descriptor();
    let request = encode_request(&request(&descriptor)).unwrap();
    let snapshot = vec![0; MAX_SNAPSHOT_BYTES + 1];
    assert!(matches!(
        encode_response(request.bytes(), &snapshot),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    let mut reply = encode_response(request.bytes(), &snapshot[..MAX_SNAPSHOT_BYTES]).unwrap();
    assert_eq!(reply.len(), MAX_RESPONSE_BYTES);
    assert_eq!(
        request.decode_response(&reply).unwrap().len(),
        MAX_SNAPSHOT_BYTES
    );
    let framed = encode_storage_rpc_success_response(&reply);
    assert_eq!(framed.len(), STORAGE_RPC_MAX_PAYLOAD_LEN);
    encode_storage_rpc_frame(17, StorageRpcMessageKind::StagingDirectSnapshot, &framed).unwrap();
    reply.push(0);
    assert!(matches!(
        request.decode_response(&reply),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
}
