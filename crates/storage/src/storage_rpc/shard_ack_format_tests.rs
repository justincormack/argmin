// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Pre-delegation evidence for the shared record/validate acknowledgement batch.
//! These are wire specimens, not payload-bearing object fixtures.

use super::*;
use std::collections::BTreeMap;

const REQUEST_ID: u64 = 0x0102_0304_0506_0708;

enum Specimen {
    Request(StorageRpcShardAckBatchRequest),
    Response {
        result: Result<Vec<u8>, StorageRpcErrorResponse>,
        reusable: bool,
    },
}

impl Specimen {
    fn payload(&self) -> Vec<u8> {
        match self {
            Self::Request(request) => encode_shard_ack_batch_request(request).unwrap(),
            Self::Response { result, reusable } => {
                let mut bytes = match result {
                    Ok(payload) => encode_storage_rpc_success_response(payload),
                    Err(error) => encode_storage_rpc_error_response(error).unwrap(),
                };
                set_storage_rpc_response_connection_reusable(&mut bytes, *reusable).unwrap();
                bytes
            }
        }
    }

    fn assert_payload(&self, payload: &[u8]) {
        match self {
            Self::Request(request) => {
                assert_eq!(decode_shard_ack_batch_request(payload).unwrap(), *request)
            }
            Self::Response { result, reusable } => {
                let decoded =
                    decode_storage_rpc_response_payload_with_connection_disposition(payload)
                        .unwrap();
                assert_eq!(decoded.response, *result);
                assert_eq!(decoded.connection_reusable, *reusable);
            }
        }
    }
}

fn request(count: usize) -> StorageRpcShardAckBatchRequest {
    StorageRpcShardAckBatchRequest {
        node_id: NodeId::new(7),
        cluster_epoch: ClusterEpoch::new(9).unwrap(),
        pg_id: PgId::new(11),
        items: (0..count)
            .map(|i| StorageRpcShardAckItem {
                shard_key: ShardKey::new(
                    &[0x42; 16],
                    0x0102_0304_0506_0708 + i as u64,
                    (i % 256) as u8,
                ),
                ack: WriteAck {
                    stored_size: 0x1122_3344_5566_7788 + i as u64,
                    crc64: 0x8877_6655_4433_2211 + i as u64,
                },
            })
            .collect(),
    }
}

fn specimens() -> BTreeMap<String, (StorageRpcMessageKind, Specimen)> {
    let mut cases = BTreeMap::new();
    for (name, kind) in [
        ("record", StorageRpcMessageKind::ShardAckRecord),
        ("validate", StorageRpcMessageKind::ShardAckValidate),
    ] {
        for count in [1, 2] {
            assert!(cases
                .insert(
                    format!("{name}-request-{count}"),
                    (kind, Specimen::Request(request(count)))
                )
                .is_none());
        }
        for reusable in [false, true] {
            let disposition = if reusable { "reuse" } else { "close" };
            for (label, result) in [
                ("success", Ok(Vec::new())),
                (
                    "expired",
                    Err(StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::StaleShardLocation,
                        message: "expired".into(),
                    }),
                ),
                (
                    "mismatch",
                    Err(StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::ShardIntegrity,
                        message: "ack mismatch".into(),
                    }),
                ),
            ] {
                assert!(cases
                    .insert(
                        format!("{name}-{label}-{disposition}"),
                        (kind, Specimen::Response { result, reusable })
                    )
                    .is_none());
            }
        }
    }
    cases
}

fn frozen() -> BTreeMap<String, Vec<u8>> {
    let mut cases = BTreeMap::new();
    for line in include_str!("testdata/shard_ack_rpc_v33.hex").lines() {
        if line.starts_with('#') {
            continue;
        }
        let (name, hex) = line.split_once(' ').unwrap();
        let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty(), "{name}: incomplete hex byte");
        let bytes = pairs
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        assert!(
            cases.insert(name.to_owned(), bytes).is_none(),
            "duplicate fixture: {name}"
        );
    }
    cases
}

#[test]
fn shard_ack_v33_writer_and_reader_match_frozen_frames() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 33);
    assert_eq!(StorageRpcMessageKind::ShardAckRecord as u16, 12);
    assert_eq!(StorageRpcMessageKind::ShardAckValidate as u16, 13);
    let frozen = frozen();
    let specimens = specimens();
    assert_eq!(frozen.len(), 16);
    assert_eq!(
        frozen.keys().collect::<Vec<_>>(),
        specimens.keys().collect::<Vec<_>>()
    );
    for (name, (kind, specimen)) in specimens {
        let bytes = &frozen[&name];
        assert_eq!(
            encode_storage_rpc_frame(REQUEST_ID, kind, &specimen.payload()).unwrap(),
            *bytes,
            "{name}"
        );
        let frame = decode_storage_rpc_frame(bytes).unwrap();
        assert_eq!(frame.request_id, REQUEST_ID);
        assert_eq!(frame.kind, kind);
        specimen.assert_payload(&frame.payload);
        // The literal input, rather than this test's encoder output, is also
        // read through the production bounded stream reader for requests.
        if matches!(specimen, Specimen::Request(_)) {
            let admitted =
                read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)).unwrap();
            specimen.assert_payload(&admitted.payload);
        }
    }
}

#[test]
fn shard_ack_v33_payloads_reject_truncation_trailing_data_and_invalid_counts() {
    for (name, (kind, specimen)) in specimens() {
        let frame = decode_storage_rpc_frame(&frozen()[&name]).unwrap();
        for end in 0..frame.payload.len() {
            let prefix = &frame.payload[..end];
            let rejected = match specimen {
                Specimen::Request(_) => decode_shard_ack_batch_request(prefix).is_err(),
                Specimen::Response { .. } => decode_storage_rpc_response_payload(prefix).is_err(),
            };
            assert!(rejected, "{name}: accepted truncation at {end}");
        }
        let mut trailing = frame.payload.clone();
        trailing.push(0);
        let rejected = match specimen {
            Specimen::Request(_) => decode_shard_ack_batch_request(&trailing).is_err(),
            Specimen::Response { .. } => decode_storage_rpc_response_payload(&trailing).is_err(),
        };
        assert!(rejected, "{name}: accepted trailing byte");
        for version in [32, 34] {
            let bytes = encode_storage_rpc_frame_with_version_for_test(
                REQUEST_ID,
                kind,
                &frame.payload,
                version,
            );
            assert!(
                matches!(decode_storage_rpc_frame(&bytes), Err(StorageRpcFrameError::UnsupportedVersion(actual)) if actual == version),
                "{name}"
            );
            assert!(
                matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::UnsupportedVersion(actual))) if actual == version),
                "{name}"
            );
        }
        if matches!(specimen, Specimen::Request(_)) {
            for count in [0_u32, 4097] {
                let mut invalid = frame.payload.clone();
                invalid[16..20].copy_from_slice(&count.to_le_bytes());
                assert!(
                    matches!(
                        decode_shard_ack_batch_request(&invalid),
                        Err(StorageRpcPayloadError::InvalidShardAckBatchRequest(_))
                            | Err(StorageRpcPayloadError::PayloadTooLarge { .. })
                    ),
                    "{name}"
                );
            }
        }
    }
}

#[test]
fn shard_ack_v33_batch_admits_exact_cap_and_rejects_one_extra_byte() {
    assert_eq!(STORAGE_RPC_MAX_SHARD_ACK_ITEMS, 4096);
    assert_eq!(STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN, 184_340);
    let request = request(4096);
    let payload = encode_shard_ack_batch_request(&request).unwrap();
    assert_eq!(payload.len(), 184_340);
    for kind in [
        StorageRpcMessageKind::ShardAckRecord,
        StorageRpcMessageKind::ShardAckValidate,
    ] {
        let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &payload).unwrap();
        let frame = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(
            decode_shard_ack_batch_request(&frame.payload).unwrap(),
            request
        );
        let mut oversized = payload.clone();
        oversized.push(0);
        let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &oversized).unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge {
                    len: 184_341,
                    limit: 184_340
                }
            ))
        ));
    }
    for count in [0, 4097] {
        assert!(encode_shard_ack_batch_request(&self::request(count)).is_err());
    }
}
