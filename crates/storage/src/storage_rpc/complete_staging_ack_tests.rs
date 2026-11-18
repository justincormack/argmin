// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Canonical transport evidence, not proof of receipt ownership or SQL authority.
use super::*;

fn descriptor() -> Vec<u8> {
    crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ))
}
fn inspection() -> Vec<u8> {
    let frame = crate::tests::frozen_hex_fixture(include_str!("testdata/stream_file_primary_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_request.hex"));
    decode_storage_rpc_frame(&frame).unwrap().payload
}
fn request(provenance: Provenance<'_>) -> Request<'_> {
    Request {
        receiver: route::Request {
            node: NodeId::new(1),
            pg: PgId::new(4),
            epoch: ClusterEpoch::new(5).unwrap(),
            topology: [0xa5; 32],
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 50_000,
            },
        },
        provenance,
        receipts: vec![
            Receipt {
                shard_index: 0,
                ack: WriteAck {
                    stored_size: 13,
                    crc64: 0,
                },
            },
            Receipt {
                shard_index: 1,
                ack: WriteAck {
                    stored_size: 13,
                    crc64: 0x0123456789abcdef,
                },
            },
        ],
    }
}

#[test]
fn complete_staging_ack_frozen_composites_and_exact_roundtrip() {
    let descriptor = descriptor();
    let inspection = inspection();
    for (name, provenance) in [
        (
            "direct",
            Provenance::Direct {
                descriptor: &descriptor,
                logical_size: 4,
            },
        ),
        (
            "stream",
            Provenance::Stream {
                inspection: &inspection,
            },
        ),
    ] {
        let original = request(provenance);
        let encoded = encode_request(&original).unwrap();
        let decoded = decode_request(encoded.bytes()).unwrap();
        assert_eq!(decoded.receiver, original.receiver);
        assert_eq!(decoded.receipts, original.receipts);
        assert_eq!(encode_request(&decoded).unwrap().bytes(), encoded.bytes());
        let reply = encode_response(encoded.bytes()).unwrap();
        encoded.decode_response(&reply).unwrap();
        for (part, payload) in [
            ("request", encoded.bytes().to_vec()),
            ("reply", encode_storage_rpc_success_response(&reply)),
        ] {
            let frame = encode_storage_rpc_frame(
                17,
                StorageRpcMessageKind::StagingShardAckRecord,
                &payload,
            )
            .unwrap();
            assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
            let digest = checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &frame)
                .bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let expected = match (name, part) {
                ("direct", "request") => {
                    "dcb2f348c26c54a9665dbceb87af1926a7c1294a66f614a863305f939f8c4f55"
                }
                ("direct", "reply") => {
                    "c2e5f483f69aab2bf22dd1be45bf62c7384afba1982bbb237252b48605f628e7"
                }
                ("stream", "request") => {
                    "542c96463a99e2a319c8600e0cd2922c85f27b5f206edecb8e2ba1ef15ed9dd0"
                }
                ("stream", "reply") => {
                    "c2ecb9e5034a687aaf2a09fc51b7c51c5f272b418254716a21459d17d33f28f1"
                }
                _ => unreachable!(),
            };
            assert_eq!(digest, expected, "{name} {part}");
        }
        assert!(file::decode_request(encoded.bytes()).is_err());
        assert!(decode_shard_ack_record_request(encoded.bytes()).is_err());
    }
}

#[test]
fn complete_staging_ack_rejects_noncanonical_receipts_and_malformed_framing() {
    let descriptor = descriptor();
    let inspection = inspection();
    for provenance in [
        Provenance::Direct {
            descriptor: &descriptor,
            logical_size: 4,
        },
        Provenance::Stream {
            inspection: &inspection,
        },
    ] {
        let original = request(provenance);
        let encoded = encode_request(&original).unwrap();
        let reply = encode_response(encoded.bytes()).unwrap();
        for len in 0..encoded.bytes().len() {
            assert!(decode_request(&encoded.bytes()[..len]).is_err(), "{len}");
        }
        for len in 0..reply.len() {
            assert!(encoded.decode_response(&reply[..len]).is_err(), "{len}");
        }
        for offset in 0..reply.len() {
            let mut bad = reply.clone();
            bad[offset] ^= 1;
            assert!(encoded.decode_response(&bad).is_err(), "{offset}");
        }
        let mut bad = encoded.bytes().to_vec();
        bad.push(0);
        assert!(decode_request(&bad).is_err());
        let mut bad = reply.clone();
        bad.push(0);
        assert!(encoded.decode_response(&bad).is_err());
        for offset in [0, 4, 6, 6 + route::REQUEST_BYTES] {
            let mut bad = encoded.bytes().to_vec();
            bad[offset] = 255;
            assert!(decode_request(&bad).is_err());
        }
        for version in [0_u16, 2] {
            let mut bad = encoded.bytes().to_vec();
            bad[4..6].copy_from_slice(&version.to_le_bytes());
            assert!(decode_request(&bad).is_err());
        }
        let count_offset = encoded.bytes().len() - 2 * RECEIPT_BYTES - 2;
        for count in [0_u16, 1, 3, 33, u16::MAX] {
            let mut bad = encoded.bytes().to_vec();
            bad[count_offset..count_offset + 2].copy_from_slice(&count.to_le_bytes());
            assert!(decode_request(&bad).is_err());
        }
        for case in 0..6 {
            let mut bad = request(provenance);
            match case {
                0 => bad.receipts.clear(),
                1 => bad.receipts[1].shard_index = 0,
                2 => bad.receipts.swap(0, 1),
                3 => bad.receipts[1].shard_index = 2,
                4 => bad.receipts[0].ack.stored_size = 0,
                5 => bad.receipts[1].ack.stored_size += 1,
                _ => unreachable!(),
            }
            assert!(encode_request(&bad).is_err(), "{case}");
        }
        // Exercise decoding independently of the canonical encoder.
        for (offset, bytes) in [
            (count_offset + 2, vec![1]),
            (count_offset + 2 + RECEIPT_BYTES, vec![0]),
            (count_offset + 3, 0_u64.to_le_bytes().to_vec()),
            (count_offset + 3, u64::MAX.to_le_bytes().to_vec()),
        ] {
            let mut bad = encoded.bytes().to_vec();
            bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
            assert!(decode_request(&bad).is_err());
        }
    }
}

#[test]
fn complete_staging_ack_success_binds_all_subjects_and_every_receipt() {
    let descriptor = descriptor();
    let provenance = Provenance::Direct {
        descriptor: &descriptor,
        logical_size: 4,
    };
    let encoded = encode_request(&request(provenance)).unwrap();
    let reply = encode_response(encoded.bytes()).unwrap();
    for field in 0..11 {
        let mut changed = request(provenance);
        let mut altered = descriptor.clone();
        match field {
            0 => changed.receiver.node = NodeId::new(2),
            1 => changed.receiver.pg = PgId::new(5),
            2 => changed.receiver.epoch = ClusterEpoch::new(6).unwrap(),
            3 => changed.receiver.topology[0] ^= 1,
            4 => changed.receiver.deadline.portable_wall_valid_until_ms -= 1,
            5 => changed.receipts.pop().map(|_| ()).unwrap(),
            6 => {
                changed.receipts[0].ack.crc64 ^= 1;
            }
            7 => {
                changed.receipts[1].ack.crc64 ^= 1;
            }
            8 => {
                changed.provenance = Provenance::Direct {
                    descriptor: &descriptor,
                    logical_size: 5,
                }
            }
            9 => {
                altered[0] ^= 1;
                changed.provenance = Provenance::Direct {
                    descriptor: &altered,
                    logical_size: 4,
                };
            }
            10 => {
                for receipt in &mut changed.receipts {
                    receipt.ack.stored_size += 1;
                }
            }
            _ => unreachable!(),
        }
        assert!(
            encode_request(&changed)
                .unwrap()
                .decode_response(&reply)
                .is_err(),
            "{field}"
        );
    }
}

#[test]
fn complete_staging_ack_stream_cannot_cross_or_extend_allocation_authority() {
    let inspection = inspection();
    for field in 0..4 {
        let mut bad = request(Provenance::Stream {
            inspection: &inspection,
        });
        let encoded = encode_request(&bad).unwrap();
        match field {
            0 => bad.receiver.pg = PgId::new(5),
            1 => bad.receiver.epoch = ClusterEpoch::new(6).unwrap(),
            2 => bad.receiver.topology[0] ^= 1,
            3 => bad.receiver.deadline.portable_wall_valid_until_ms += 1,
            _ => unreachable!(),
        }
        assert!(encode_request(&bad).is_err());
        let mut bytes = encoded.bytes().to_vec();
        bytes[6..6 + route::REQUEST_BYTES].copy_from_slice(&route::encode_request(bad.receiver));
        assert!(decode_request(&bytes).is_err());
    }
    let mut good = request(Provenance::Stream {
        inspection: &inspection,
    });
    good.receiver.deadline.portable_wall_valid_until_ms -= 1;
    encode_request(&good).unwrap();
}

#[test]
fn complete_staging_ack_caps_admit_exact_frame_bound_and_reject_before_nested_allocation() {
    assert_eq!(MAX_RECEIPTS, 32);
    assert_eq!(MAX_RECEIPTS, ec::MAX_TOTAL_SHARDS);
    assert_eq!(StorageRpcMessageKind::StagingShardAckRecord as u16, 179);
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingShardAckRecord,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    frame.payload.push(0);
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame), Err(StorageRpcFrameError::PayloadTooLarge { len, limit }) if len == MAX_REQUEST_BYTES+1 && limit == MAX_REQUEST_BYTES)
    );
    assert!(decode_request(&frame.payload).is_err());
    let descriptor = vec![0x31; complete_window_primary::MAX_DESCRIPTOR_BYTES];
    let mut max = request(Provenance::Direct {
        descriptor: &descriptor,
        logical_size: 4,
    });
    max.receipts = (0..MAX_RECEIPTS)
        .map(|index| Receipt {
            shard_index: index as u8,
            ack: WriteAck {
                stored_size: file::MAX_SHARD_BYTES as u64,
                crc64: 0,
            },
        })
        .collect();
    let encoded = encode_request(&max).unwrap();
    assert_eq!(
        decode_request(encoded.bytes()).unwrap().receipts,
        max.receipts
    );
    assert!(encoded.bytes().len() <= MAX_REQUEST_BYTES);
    max.receipts.push(Receipt {
        shard_index: 32,
        ack: max.receipts[0].ack,
    });
    assert!(encode_request(&max).is_err());
    let mut bad = encoded.bytes().to_vec();
    let length = 6 + route::REQUEST_BYTES + 1 + 8;
    bad[length..length + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_request(&bad).is_err());
}
