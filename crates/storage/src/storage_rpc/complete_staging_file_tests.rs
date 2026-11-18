// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Transport specimens only: no claim that these arbitrary shard bytes are a
//! decryptable payload or that decoding is durable-window confirmation.
use super::*;

fn receiver() -> route::Request {
    route::Request {
        node: NodeId::new(1),
        epoch: ClusterEpoch::new(5).unwrap(),
        pg: PgId::new(4),
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
        topology: [0xa5; 32],
    }
}

fn descriptor() -> Vec<u8> {
    crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ))
}

fn inspection() -> Vec<u8> {
    let frame = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/stream_file_primary_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_request.hex"
    ));
    decode_storage_rpc_frame(&frame).unwrap().payload
}

fn request(provenance: Provenance<'_>) -> Request<'_> {
    Request {
        receiver: receiver(),
        provenance,
        shard_index: 0,
        bytes: b"wire specimen",
    }
}

fn ack(request: Request<'_>) -> WriteAck {
    WriteAck {
        stored_size: request.bytes.len() as u64,
        crc64: checksum::crc64::checksum(request.bytes),
    }
}

#[test]
fn complete_staging_file_frozen_request_and_reply_composites() {
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
        // Preserve the original kind-3 composites as predecessor evidence.
        // Candidate dispatch now uses kind 178; payload grammar is unchanged.
        // Stream nests SFPI-v1 / SALL-v1 / descriptor-v1 / state-v10 / SSE-S3-v5.
        let hashes = match provenance {
            Provenance::Direct { .. } => [
                "0a6410576a21a31f838dcbdba4fc201c4a3e58369d20dca8ec6bad1577e49bc6",
                "51f3d10f4dd94baca153527f02e3d9c75aec38830276447b5a3e03c5e7edc71c",
            ],
            Provenance::Stream { .. } => [
                "84d37aa469eea7346ae42f7df3f641bc5e40fc89a3731b340b8e9ab59b6f3247",
                "85df9f75f9d5ab0bead242886ae65d85ab8ff1f12106e02f66d96d735d4a5e89",
            ],
        };
        let current_hashes = match provenance {
            Provenance::Direct { .. } => [
                "95d937e81c31b6b81df270ad3cf90bcc347bdd27fd4f7f1b971c0e4e3420f275",
                "4a95df34cc0c10cd91e4cc69d8df6ac800e6838153b8e7f38771bdffc1d73034",
            ],
            Provenance::Stream { .. } => [
                "410ff6adeb5d86cda4a677460a9f424f040db3633c023db3a1f14a871727a1c9",
                "17707320f70171bfea8490cd833b4bd473a44a2ba836b7272686cf562f7ab722",
            ],
        };
        let request = request(provenance);
        let encoded = encode_request(request).unwrap();
        let reply = encode_response(encoded.bytes(), ack(request)).unwrap();
        assert_eq!(encoded.decode_response(&reply).unwrap(), ack(request));
        let decoded = decode_request(encoded.bytes()).unwrap();
        assert_eq!(decoded.receiver, request.receiver);
        assert_eq!(decoded.shard_index, request.shard_index);
        assert_eq!(decoded.bytes, request.bytes);
        assert_eq!(encode_request(decoded).unwrap().bytes(), encoded.bytes());
        for (payload, (expected, current_expected)) in [
            encoded.bytes().to_vec(),
            encode_storage_rpc_success_response(&reply),
        ]
        .into_iter()
        .zip(hashes.into_iter().zip(current_hashes))
        {
            let frame =
                encode_storage_rpc_frame(17, StorageRpcMessageKind::ShardWrite, &payload).unwrap();
            assert_eq!(
                checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &frame)
                    .bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                expected,
            );
            assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
            let current =
                encode_storage_rpc_frame(17, StorageRpcMessageKind::StagingShardWrite, &payload)
                    .unwrap();
            assert_eq!(
                checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &current)
                    .bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                current_expected,
            );
            let decoded = decode_storage_rpc_frame(&current).unwrap();
            assert_eq!(decoded.kind, StorageRpcMessageKind::StagingShardWrite);
            assert_eq!(decoded.payload, payload);
        }
        assert!(
            decode_shard_write_request(encoded.bytes()).is_err(),
            "live decoder must not select candidate effects"
        );
    }
}

#[test]
fn complete_staging_file_rejects_truncation_versions_tags_and_trailing_bytes() {
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
        let request = request(provenance);
        let encoded = encode_request(request).unwrap();
        let reply = encode_response(encoded.bytes(), ack(request)).unwrap();
        for len in 0..encoded.bytes().len() {
            assert!(decode_request(&encoded.bytes()[..len]).is_err(), "{len}");
        }
        for len in 0..reply.len() {
            assert!(encoded.decode_response(&reply[..len]).is_err(), "{len}");
        }
        for offset in [0, 4, 6, 6 + route::REQUEST_BYTES] {
            let mut bad = encoded.bytes().to_vec();
            bad[offset] = 255;
            assert!(decode_request(&bad).is_err(), "{offset}");
        }
        for version in [0_u16, 2] {
            let mut bad = encoded.bytes().to_vec();
            bad[4..6].copy_from_slice(&version.to_le_bytes());
            assert!(decode_request(&bad).is_err());
        }
        let mut bad = encoded.bytes().to_vec();
        bad.push(0);
        assert!(decode_request(&bad).is_err());
        let mut bad = reply.clone();
        bad.push(0);
        assert!(encoded.decode_response(&bad).is_err());
        for offset in 0..reply.len() {
            let mut bad = reply.clone();
            bad[offset] ^= 1;
            assert!(encoded.decode_response(&bad).is_err(), "{offset}");
        }
    }
}

#[test]
fn complete_staging_file_reply_binds_every_outer_subject_and_payload() {
    let descriptor = descriptor();
    let base = request(Provenance::Direct {
        descriptor: &descriptor,
        logical_size: 4,
    });
    let original = encode_request(base).unwrap();
    let reply = encode_response(original.bytes(), ack(base)).unwrap();
    for field in 0..10 {
        let mut crossed = base;
        let mut changed_descriptor = descriptor.clone();
        match field {
            0 => crossed.receiver.node = NodeId::new(2),
            1 => crossed.receiver.pg = PgId::new(5),
            2 => crossed.receiver.epoch = ClusterEpoch::new(6).unwrap(),
            3 => crossed.receiver.topology[0] ^= 1,
            4 => crossed.receiver.deadline.portable_wall_valid_until_ms -= 1,
            5 => crossed.shard_index += 1,
            6 => crossed.bytes = b"wire specimeN",
            7 => crossed.bytes = b"short",
            8 => {
                crossed.provenance = Provenance::Direct {
                    descriptor: &descriptor,
                    logical_size: 5,
                }
            }
            9 => {
                changed_descriptor[0] ^= 1;
                crossed.provenance = Provenance::Direct {
                    descriptor: &changed_descriptor,
                    logical_size: 4,
                };
            }
            _ => unreachable!(),
        }
        assert!(
            encode_request(crossed)
                .unwrap()
                .decode_response(&reply)
                .is_err(),
            "{field}"
        );
    }
    let mut wrong = ack(base);
    wrong.crc64 ^= 1;
    assert!(original
        .decode_response(&encode_response(original.bytes(), wrong).unwrap())
        .is_err());
    wrong = ack(base);
    wrong.stored_size += 1;
    assert!(encode_response(original.bytes(), wrong).is_err());
}

#[test]
fn complete_staging_file_reply_rejects_equal_crc_payload_substitution() {
    // Independent equal-length CRC64-NVME collision. Reply SHA-256 must bind
    // payload bytes even when both ordinary acknowledgement fields are equal.
    let collision = [28, 250, 229, 61, 115, 85, 169, 81, 98, 105, 109, 101, 110];
    assert_ne!(collision.as_slice(), b"wire specimen");
    assert_eq!(checksum::crc64::checksum(&collision), 0x34cd055b23265f73);
    assert_eq!(
        checksum::crc64::checksum(b"wire specimen"),
        0x34cd055b23265f73
    );
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
        let crossed = Request {
            bytes: &collision,
            ..original
        };
        assert_eq!(ack(original), ack(crossed));
        let encoded = encode_request(original).unwrap();
        let other = encode_request(crossed).unwrap();
        let reply = encode_response(encoded.bytes(), ack(original)).unwrap();
        encoded.decode_response(&reply).unwrap();
        assert!(other.decode_response(&reply).is_err());
    }
}

#[test]
fn complete_staging_file_stream_cannot_cross_or_extend_allocation_route() {
    let inspection = inspection();
    let base = request(Provenance::Stream {
        inspection: &inspection,
    });
    for field in 0..4 {
        let mut bad = base;
        match field {
            0 => bad.receiver.pg = PgId::new(5),
            1 => bad.receiver.epoch = ClusterEpoch::new(6).unwrap(),
            2 => bad.receiver.topology[0] ^= 1,
            3 => bad.receiver.deadline.portable_wall_valid_until_ms += 1,
            _ => unreachable!(),
        }
        assert!(encode_request(bad).is_err());
        let good = encode_request(base).unwrap();
        let mut bytes = good.bytes().to_vec();
        bytes[6..6 + route::REQUEST_BYTES].copy_from_slice(&route::encode_request(bad.receiver));
        assert!(decode_request(&bytes).is_err());
    }
    let mut shorter = base;
    shorter.receiver.deadline.portable_wall_valid_until_ms -= 1;
    let encoded = encode_request(shorter).unwrap();
    assert_eq!(
        decode_request(encoded.bytes()).unwrap().receiver,
        shorter.receiver
    );
    let files = stream::decode_request(&inspection).unwrap();
    assert!(encode_request(Request {
        provenance: Provenance::Stream {
            inspection: files.allocation
        },
        ..base
    })
    .is_err());
}

#[test]
fn complete_staging_file_caps_apply_before_nested_payload_access() {
    assert_eq!(StorageRpcMessageKind::StagingShardWrite as u16, 178);
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingShardWrite,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    frame.payload.push(0);
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame),
        Err(StorageRpcFrameError::PayloadTooLarge { len, limit })
        if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    let descriptor = vec![0x31; complete_window_primary::MAX_DESCRIPTOR_BYTES];
    let payload = vec![0xa5; MAX_SHARD_BYTES];
    let mut base = request(Provenance::Direct {
        descriptor: &descriptor,
        logical_size: 4,
    });
    base.bytes = &payload;
    let encoded = encode_request(base).unwrap();
    assert_eq!(
        decode_request(encoded.bytes()).unwrap().bytes.len(),
        MAX_SHARD_BYTES
    );
    assert!(encoded.bytes().len() <= MAX_REQUEST_BYTES);
    let oversized = vec![0; MAX_SHARD_BYTES + 1];
    assert!(encode_request(Request {
        bytes: &oversized,
        ..base
    })
    .is_err());
    assert!(decode_request(&vec![0; MAX_REQUEST_BYTES + 1]).is_err());
    let mut bad = encoded.bytes().to_vec();
    let descriptor_len_offset = 6 + route::REQUEST_BYTES + 1 + 8;
    bad[descriptor_len_offset..descriptor_len_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_request(&bad).is_err());
    let payload_len_offset = descriptor_len_offset + 4 + descriptor.len() + 1;
    bad = encoded.bytes().to_vec();
    bad[payload_len_offset..payload_len_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_request(&bad).is_err());
    assert!(encode_request(Request { bytes: &[], ..base }).is_err());
    base.receiver.deadline.portable_wall_valid_until_ms = 0;
    assert!(encode_request(base).is_err());
}
