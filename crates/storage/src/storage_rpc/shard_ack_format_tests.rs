// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current delegated record/unchanged validate payloads and frozen v33 evidence.
//! These are wire specimens, not payload-bearing object fixtures.

use super::*;
use std::collections::BTreeMap;

const REQUEST_ID: u64 = 0x0102_0304_0506_0708;

enum Specimen {
    Request(StorageRpcShardAckBatchRequest),
    Record(StorageRpcShardAckRecordRequest),
    Response {
        result: Result<Vec<u8>, StorageRpcErrorResponse>,
        reusable: bool,
    },
}

impl Specimen {
    fn payload(&self) -> Vec<u8> {
        match self {
            Self::Request(request) => encode_shard_ack_batch_request(request).unwrap(),
            Self::Record(request) => encode_shard_ack_record_request(request).unwrap(),
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
            Self::Record(request) => {
                assert_eq!(decode_shard_ack_record_request(payload).unwrap(), *request)
            }
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
            if kind == StorageRpcMessageKind::ShardAckRecord {
                for bounded in [false, true] {
                    let effect_deadline =
                        bounded.then_some(StorageRpcAdmittedRouteEffectDeadline {
                            authority_valid_until_ms: 5_000,
                            portable_wall_valid_until_ms: 3_000,
                        });
                    assert!(cases
                        .insert(
                            format!(
                                "{name}-request-{count}-{}",
                                if bounded { "bounded" } else { "unbounded" }
                            ),
                            (
                                kind,
                                Specimen::Record(StorageRpcShardAckRecordRequest {
                                    batch: request(count),
                                    effect_deadline
                                })
                            )
                        )
                        .is_none());
                }
                continue;
            }
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

fn frozen_text(text: &str) -> BTreeMap<String, Vec<u8>> {
    let mut cases = BTreeMap::new();
    for line in text.lines() {
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

fn frozen() -> BTreeMap<String, Vec<u8>> {
    frozen_text(include_str!("testdata/shard_ack_rpc_v34.hex"))
}

#[test]
fn retired_shard_ack_v33_frames_are_rejected_unchanged() {
    let frozen = frozen_text(include_str!("testdata/shard_ack_rpc_v33.hex"));
    assert_eq!(frozen.len(), 16);
    for (name, bytes) in frozen {
        assert!(
            matches!(
                decode_storage_rpc_frame(&bytes),
                Err(StorageRpcFrameError::UnsupportedVersion(33))
            ),
            "{name}"
        );
        assert!(
            matches!(
                read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)),
                Err(StorageRpcStreamError::Frame(
                    StorageRpcFrameError::UnsupportedVersion(33)
                ))
            ),
            "{name}"
        );
    }
}

#[test]
fn shard_ack_v34_writer_and_reader_match_frozen_frames() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 34);
    assert_eq!(StorageRpcMessageKind::ShardAckRecord as u16, 12);
    assert_eq!(StorageRpcMessageKind::ShardAckValidate as u16, 13);
    let frozen = frozen();
    let specimens = specimens();
    assert_eq!(frozen.len(), 18);
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
        if matches!(specimen, Specimen::Request(_) | Specimen::Record(_)) {
            let admitted =
                read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)).unwrap();
            specimen.assert_payload(&admitted.payload);
        }
    }
}

#[test]
fn shard_ack_v34_payloads_reject_truncation_trailing_data_and_invalid_counts() {
    for (name, (kind, specimen)) in specimens() {
        let frame = decode_storage_rpc_frame(&frozen()[&name]).unwrap();
        for end in 0..frame.payload.len() {
            let prefix = &frame.payload[..end];
            let rejected = match specimen {
                Specimen::Record(_) => decode_shard_ack_record_request(prefix).is_err(),
                Specimen::Request(_) => decode_shard_ack_batch_request(prefix).is_err(),
                Specimen::Response { .. } => decode_storage_rpc_response_payload(prefix).is_err(),
            };
            assert!(rejected, "{name}: accepted truncation at {end}");
        }
        let mut trailing = frame.payload.clone();
        trailing.push(0);
        let rejected = match specimen {
            Specimen::Record(_) => decode_shard_ack_record_request(&trailing).is_err(),
            Specimen::Request(_) => decode_shard_ack_batch_request(&trailing).is_err(),
            Specimen::Response { .. } => decode_storage_rpc_response_payload(&trailing).is_err(),
        };
        assert!(rejected, "{name}: accepted trailing byte");
        for version in [33, 35] {
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
        if matches!(specimen, Specimen::Request(_) | Specimen::Record(_)) {
            for count in [0_u32, 4097] {
                let mut invalid = frame.payload.clone();
                let prefix = match &specimen {
                    Specimen::Record(record) => {
                        if record.effect_deadline.is_some() {
                            17
                        } else {
                            1
                        }
                    }
                    _ => 0,
                };
                invalid[prefix + 16..prefix + 20].copy_from_slice(&count.to_le_bytes());
                let result = if kind == StorageRpcMessageKind::ShardAckRecord {
                    decode_shard_ack_record_request(&invalid).map(|record| record.batch)
                } else {
                    decode_shard_ack_batch_request(&invalid)
                };
                assert!(
                    matches!(
                        result,
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
fn shard_ack_record_rejects_malformed_deadlines() {
    let mut request = StorageRpcShardAckRecordRequest {
        batch: request(1),
        effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: 5_000,
            portable_wall_valid_until_ms: 3_000,
        }),
    };
    let bytes = encode_shard_ack_record_request(&request).unwrap();
    let mut invalid = bytes.clone();
    invalid[0] = 2;
    assert!(decode_shard_ack_record_request(&invalid).is_err());
    for cutoff in [5_000_u64, 6_000] {
        request
            .effect_deadline
            .as_mut()
            .unwrap()
            .portable_wall_valid_until_ms = cutoff;
        assert!(encode_shard_ack_record_request(&request).is_err());
        let mut invalid = bytes.clone();
        invalid[9..17].copy_from_slice(&cutoff.to_le_bytes());
        assert!(decode_shard_ack_record_request(&invalid).is_err());
    }
}

#[test]
fn shard_ack_v34_batch_admits_exact_cap_and_rejects_one_extra_byte() {
    assert_eq!(STORAGE_RPC_MAX_SHARD_ACK_ITEMS, 4096);
    assert_eq!(STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN, 184_340);
    let request = request(4096);
    let payload = encode_shard_ack_batch_request(&request).unwrap();
    assert_eq!(payload.len(), 184_340);
    for kind in [
        StorageRpcMessageKind::ShardAckRecord,
        StorageRpcMessageKind::ShardAckValidate,
    ] {
        let (payload, limit) = if kind == StorageRpcMessageKind::ShardAckRecord {
            (
                encode_shard_ack_record_request(&StorageRpcShardAckRecordRequest {
                    batch: request.clone(),
                    effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                        authority_valid_until_ms: 5_000,
                        portable_wall_valid_until_ms: 3_000,
                    }),
                })
                .unwrap(),
                184_357,
            )
        } else {
            (payload.clone(), 184_340)
        };
        assert_eq!(payload.len(), limit);
        let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &payload).unwrap();
        let frame = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)).unwrap();
        let decoded = if kind == StorageRpcMessageKind::ShardAckRecord {
            decode_shard_ack_record_request(&frame.payload)
                .unwrap()
                .batch
        } else {
            decode_shard_ack_batch_request(&frame.payload).unwrap()
        };
        assert_eq!(decoded, request);
        let mut oversized = payload.clone();
        oversized.push(0);
        let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &oversized).unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge {
                    len,
                    limit: actual_limit
                }
            ))
            if len == limit + 1 && actual_limit == limit));
    }
    for count in [0, 4097] {
        assert!(encode_shard_ack_batch_request(&self::request(count)).is_err());
    }
}
