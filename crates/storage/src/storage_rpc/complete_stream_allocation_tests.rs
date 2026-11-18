// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use std::io::Cursor;

fn specimen(descriptor: &[u8]) -> Request<'_> {
    let bucket = BucketName::try_from("staging-bucket").unwrap();
    let key = ObjectKey::try_from("payload").unwrap();
    let session_id = crate::tests::stream_session_id("staging-owner");
    let epoch = ClusterEpoch::new(5).unwrap();
    let route = super::super::complete_settled_prefix::Request {
        node: NodeId::new(4),
        epoch,
        pg: PgId::new(4),
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
        topology: [0xa5; 32],
    };
    Request {
        window: window::Request { route, descriptor },
        prefix: MetadataCommandReplicaState {
            cluster_epoch: epoch,
            applied_log_index: 11,
            applied_log_hash: MetadataCommandLogHash::for_test(0x1234),
            state_digest: CanonicalStateDigest::for_test(0x5678),
        },
        preparation: StorageRpcStreamSegmentAppendPrepareRequest {
            object: StorageRpcObjectRequest {
                node_id: route.node,
                cluster_epoch: epoch,
                pg_id: route.pg,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            request: PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: 4,
                segment_crc64: 17,
                payload_crc64: 23,
            },
            expected_append_state: Some(crate::ExpectedStreamAppendState::PutObject {
                session: StreamUploadRecord {
                    session_id,
                    bucket,
                    key,
                    target: StreamUploadTarget::PutObject,
                    state: StreamUploadState::InProgress,
                    created_at: 1000,
                    bucket_incarnation_generation: 17,
                    cleanup_after: Some(301_000),
                    encryption: crate::tests::metadata_only_test_encryption(),
                    next_segment_vid: GenerationId::new(1).unwrap(),
                    bucket_write_reservation: None,
                },
                generation_id: GenerationId::new(12).unwrap(),
            }),
            effect_deadline: None,
        },
    }
}

fn segment() -> StreamUploadSegmentRecord {
    StreamUploadSegmentRecord {
        session_id: crate::tests::stream_session_id("staging-owner"),
        segment_index: 0,
        size: 4,
        segment_crc64: 17,
        payload_crc64: 23,
        segment_okh: [0xa5; 16],
        segment_vid: GenerationId::new(1).unwrap(),
        data_pg_id: 5,
        placement_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        ec_k: 2,
        ec_m: 1,
        shard_integrity: None,
    }
}

#[test]
fn complete_stream_allocation_frozen_composites() {
    let descriptor = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_stream_put.hex"
    ));
    let request = specimen(&descriptor);
    let bytes = encode_request(&request).unwrap();
    let reply = encode_response(digest(&bytes), &segment());
    let kind = StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare;
    for (literal, payload) in [
        (include_str!("testdata/stream_allocation_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_request.hex"), bytes.clone()),
        (include_str!("testdata/stream_allocation_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_response.hex"), encode_storage_rpc_success_response(&reply)),
    ] {
        let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
        assert_eq!(frame, crate::tests::frozen_hex_fixture(literal));
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
    }
    assert_eq!(
        encode_request(&decode_request(&bytes).unwrap()).unwrap(),
        bytes
    );
    assert_eq!(decode_response(&reply, digest(&bytes)).unwrap(), segment());
}

#[test]
fn complete_stream_allocation_bounds_and_subjects_fail_closed() {
    let request = specimen(b"descriptor");
    let encoded = encode_request(&request).unwrap();
    let response = encode_response(digest(&encoded), &segment());
    for len in 0..encoded.len() {
        assert!(decode_request(&encoded[..len]).is_err(), "{len}");
    }
    for len in 0..response.len() {
        assert!(
            decode_response(&response[..len], digest(&encoded)).is_err(),
            "{len}"
        );
    }
    for field in ["node", "pg", "epoch", "prefix", "expected", "deadline"] {
        let mut crossed = specimen(b"descriptor");
        match field {
            "node" => crossed.preparation.object.node_id = NodeId::new(0),
            "pg" => crossed.preparation.object.pg_id = PgId::new(0),
            "epoch" => crossed.preparation.object.cluster_epoch = ClusterEpoch::INITIAL,
            "prefix" => crossed.prefix.cluster_epoch = ClusterEpoch::INITIAL,
            "expected" => crossed.preparation.expected_append_state = None,
            "deadline" => {
                crossed.preparation.effect_deadline = Some(StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: 1,
                    portable_wall_valid_until_ms: 1,
                })
            }
            _ => unreachable!(),
        }
        assert!(encode_request(&crossed).is_err(), "{field}");
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    let mut trailing = response.clone();
    trailing.push(0);
    assert!(decode_response(&trailing, digest(&encoded)).is_err());
    let mut other = digest(&encoded);
    other[0] ^= 1;
    assert!(decode_response(&response, other).is_err());
    let raw = encode_stream_segment_append_prepare_request(&request.preparation);
    assert!(decode_request(&raw).is_err());
    assert!(decode_stream_segment_append_prepare_request(&encoded).is_err());
    for offset in [0, 4] {
        let mut bad = encoded.clone();
        bad[offset] ^= 1;
        assert!(decode_request(&bad).is_err());
    }
    let kind = StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare;
    let payload = vec![0; MAX_REQUEST_BYTES];
    let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(frame))
            .unwrap()
            .payload,
        payload
    );
    assert!(decode_request(&vec![0; MAX_REQUEST_BYTES + 1]).is_err());
    let frame_limit = super::super::complete_stream_file_primary::MAX_REQUEST_BYTES;
    let frame = encode_storage_rpc_frame(17, kind, &vec![0; frame_limit + 1]).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut Cursor::new(frame)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == frame_limit + 1 && limit == frame_limit)
    );
}

#[test]
fn complete_stream_file_primary_codec_and_framing() {
    use super::super::complete_stream_file_primary as files;
    let descriptor = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_stream_put.hex"
    ));
    let allocation = encode_request(&specimen(&descriptor)).unwrap();
    let bytes = files::encode_request(&allocation, &segment()).unwrap();
    let response = files::encode_response(&bytes);
    let decoded = files::decode_request(&bytes).unwrap();
    assert_eq!(decoded.allocation, allocation);
    assert_eq!(decoded.segment, segment());
    files::decode_response(&response, &bytes).unwrap();
    for end in 0..bytes.len() {
        assert!(files::decode_request(&bytes[..end]).is_err(), "{end}");
    }
    for end in 0..response.len() {
        assert!(
            files::decode_response(&response[..end], &bytes).is_err(),
            "{end}"
        );
    }
    for offset in [0, 4, 10] {
        let mut bad = bytes.clone();
        bad[offset] ^= 1;
        assert!(files::decode_request(&bad).is_err());
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(files::decode_request(&bad).is_err());
    assert!(files::decode_response(&response, &bad).is_err());
    let mut bad = response.clone();
    bad.push(0);
    assert!(files::decode_response(&bad, &bytes).is_err());
    assert!(decode_request(&bytes).is_err());
    assert!(files::decode_request(&allocation).is_err());
    let allocation_response = encode_response(digest(&allocation), &segment());
    assert!(files::decode_response(&allocation_response, &bytes).is_err());
    assert!(decode_response(&response, digest(&allocation)).is_err());
    let kind = StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare;
    for (literal, payload) in [
        (include_str!("testdata/stream_file_primary_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_request.hex"), bytes),
        (include_str!("testdata/stream_file_primary_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_response.hex"), encode_storage_rpc_success_response(&response)),
    ] {
        let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
        assert_eq!(frame, crate::tests::frozen_hex_fixture(literal));
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
    }
    let payload = vec![0; files::MAX_REQUEST_BYTES];
    let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(frame))
            .unwrap()
            .payload,
        payload
    );
    let frame = encode_storage_rpc_frame(17, kind, &vec![0; files::MAX_REQUEST_BYTES + 1]).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut Cursor::new(frame)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == files::MAX_REQUEST_BYTES + 1 && limit == files::MAX_REQUEST_BYTES)
    );
}
