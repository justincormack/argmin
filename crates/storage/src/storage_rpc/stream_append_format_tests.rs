// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn specimen(
    part: bool,
    witness: bool,
    deadline: bool,
) -> StorageRpcStreamAppendCommandBuildRequest {
    StorageRpcStreamAppendCommandBuildRequest {
        object: StorageRpcObjectRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(0),
            bucket: crate::tests::bucket_name("abc"),
            key: crate::tests::object_key("k"),
        },
        target: if part {
            StreamUploadTarget::UploadPart {
                upload_id: UploadId::try_from("a".repeat(UPLOAD_ID_LEN)).unwrap(),
                part_number: 9,
            }
        } else {
            StreamUploadTarget::PutObject
        },
        segment: StreamUploadSegmentRecord {
            session_id: SessionId::try_from("0".repeat(SESSION_ID_LEN)).unwrap(),
            segment_index: 2,
            size: 3,
            segment_crc64: 5,
            payload_crc64: 7,
            segment_okh: [11; 16],
            segment_vid: GenerationId::new(13).unwrap(),
            data_pg_id: 17,
            placement_cluster_epoch: ClusterEpoch::INITIAL,
            ec_k: 1,
            ec_m: 0,
            shard_integrity: witness.then(|| {
                SegmentShardIntegrityRecord::for_zero_filled_segment(EcShape { k: 1, m: 0 }, 3).1
            }),
        },
        effect_deadline: deadline.then_some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: 5_000,
            portable_wall_valid_until_ms: 4_000,
        }),
    }
}

#[test]
fn stream_append_builder_v31_codec_matches_independent_frozen_evidence() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 33);
    assert_eq!(
        StorageRpcMessageKind::ObjectStreamAppendCommandBuild as u16,
        177
    );
    let frozen = include_str!("testdata/stream_append_build_rpc_v31.hex")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (name, hex) = line.split_once(' ').unwrap();
            let bytes = hex
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            (name.to_string(), bytes)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(frozen.len(), 8);
    for part in [false, true] {
        for witness in [false, true] {
            for deadline in [false, true] {
                let name = format!(
                    "{}-{}-{}",
                    if part { "part" } else { "put" },
                    if witness { "witness" } else { "none" },
                    if deadline { "bounded" } else { "unbounded" }
                );
                let request = specimen(part, witness, deadline);
                let bytes = &frozen[&name];
                assert_eq!(
                    encode_stream_append_command_build_request(&request),
                    *bytes,
                    "{name}"
                );
                assert_eq!(
                    decode_stream_append_command_build_request(bytes).unwrap(),
                    request,
                    "{name}"
                );
                for end in 0..bytes.len() {
                    assert!(
                        decode_stream_append_command_build_request(&bytes[..end]).is_err(),
                        "{name}: truncation {end}"
                    );
                }
                let mut trailing = bytes.clone();
                trailing.push(0);
                assert!(decode_stream_append_command_build_request(&trailing).is_err());
                if deadline {
                    let mut crossed = request;
                    crossed
                        .effect_deadline
                        .as_mut()
                        .unwrap()
                        .portable_wall_valid_until_ms = 5_000;
                    assert!(decode_stream_append_command_build_request(
                        &encode_stream_append_command_build_request(&crossed)
                    )
                    .is_err());
                }
            }
        }
    }
}

#[test]
fn stream_append_builder_request_admits_exact_maximum_geometry() {
    let mut request = specimen(true, true, true);
    request.object.bucket = crate::tests::bucket_name("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN));
    request.object.key = crate::tests::object_key("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN));
    request.segment.ec_k = 255;
    request.segment.ec_m = 1;
    request.segment.shard_integrity =
        Some(SegmentShardIntegrityRecord::for_zero_filled_segment(EcShape { k: 255, m: 1 }, 255).1);
    let mut payload = encode_stream_append_command_build_request(&request);
    assert_eq!(
        payload.len(),
        STORAGE_RPC_MAX_STREAM_APPEND_COMMAND_BUILD_REQUEST_PAYLOAD_LEN
    );
    let frame = encode_storage_rpc_frame(
        1,
        StorageRpcMessageKind::ObjectStreamAppendCommandBuild,
        &payload,
    )
    .unwrap();
    let decoded = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(frame)).unwrap();
    assert_eq!(
        decode_stream_append_command_build_request(&decoded.payload).unwrap(),
        request
    );
    payload.push(0);
    let frame = encode_storage_rpc_frame(
        1,
        StorageRpcMessageKind::ObjectStreamAppendCommandBuild,
        &payload,
    )
    .unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(frame)),
        Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit }))
        if len == STORAGE_RPC_MAX_STREAM_APPEND_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1
            && limit == STORAGE_RPC_MAX_STREAM_APPEND_COMMAND_BUILD_REQUEST_PAYLOAD_LEN)
    );
}
