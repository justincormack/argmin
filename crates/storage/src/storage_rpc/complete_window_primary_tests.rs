// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::io::Cursor;

fn route() -> witness::Request {
    witness::Request {
        node: NodeId::new(4),
        epoch: ClusterEpoch::new(5).unwrap(),
        pg: PgId::new(4),
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 50_000,
        },
        topology: [0xa5; 32],
    }
}

fn state() -> MetadataCommandReplicaState {
    MetadataCommandReplicaState {
        cluster_epoch: ClusterEpoch::new(5).unwrap(),
        applied_log_index: 11,
        applied_log_hash: MetadataCommandLogHash::for_test(0x1234),
        state_digest: CanonicalStateDigest::for_test(0x5678),
    }
}

#[test]
fn complete_direct_file_primary_frozen_purpose_and_bindings() {
    let descriptor = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ));
    let request = Request {
        route: route(),
        descriptor: &descriptor,
    };
    let purpose = Purpose::DirectFileBinding;
    let encoded = encode_request_for(purpose, request).unwrap();
    let response = encode_response_for(purpose, request, state(), Some(NodeId::new(0)));
    for (payload, literal) in [
        (
            &encoded,
            include_str!("testdata/direct_file_primary_rpc_v37_descriptor_v1_request.hex"),
        ),
        (
            &encode_storage_rpc_success_response(&response),
            include_str!(
                "testdata/direct_file_primary_rpc_v37_descriptor_v1_state_v10_response.hex"
            ),
        ),
    ] {
        let frame = encode_storage_rpc_frame(
            17,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            payload,
        )
        .unwrap();
        assert_eq!(frame, crate::tests::frozen_hex_fixture(literal));
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, *payload);
    }
    let decoded = decode_request_for(purpose, &encoded).unwrap();
    assert_eq!(decoded.route, request.route);
    assert_eq!(decoded.descriptor, descriptor);
    assert_eq!(
        decode_response_for(purpose, &response, request, Some(NodeId::new(0))).unwrap(),
        state()
    );
    assert!(decode_request(&encoded).is_err());
    assert!(decode_response(&response, request, Some(NodeId::new(0))).is_err());
    assert!(decode_request_for(purpose, &encode_request(request).unwrap()).is_err());
    assert!(decode_response_for(
        purpose,
        &encode_response(request, state(), Some(NodeId::new(0))),
        request,
        Some(NodeId::new(0))
    )
    .is_err());
    for len in 0..encoded.len() {
        assert!(
            decode_request_for(purpose, &encoded[..len]).is_err(),
            "{len}"
        );
    }
    for len in 0..response.len() {
        assert!(
            decode_response_for(purpose, &response[..len], request, Some(NodeId::new(0))).is_err(),
            "{len}"
        );
    }
    for offset in [0, 4, 6, 14, 18, 26, 30, 62, 78, 87, 96, 128, 129] {
        let mut crossed = response.clone();
        crossed[offset] ^= 1;
        assert!(
            decode_response_for(purpose, &crossed, request, Some(NodeId::new(0))).is_err(),
            "{offset}"
        );
    }
    assert!(decode_response_for(
        purpose,
        &response,
        Request {
            descriptor: b"crossed",
            ..request
        },
        Some(NodeId::new(0))
    )
    .is_err());
    assert!(decode_response_for(purpose, &response, request, None).is_err());
    assert!(decode_response_for(purpose, &response, request, Some(NodeId::new(1))).is_err());
    let max = vec![0; MAX_DESCRIPTOR_BYTES];
    let max = encode_request_for(
        purpose,
        Request {
            descriptor: &max,
            ..request
        },
    )
    .unwrap();
    assert_eq!(max.len(), MAX_REQUEST_BYTES);
    let frame =
        encode_storage_rpc_frame(17, StorageRpcMessageKind::MetadataCommandReplicaState, &max)
            .unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(frame))
            .unwrap()
            .payload,
        max
    );
    assert!(decode_metadata_command_state_request(&encoded).is_err());
    assert!(witness::decode_request(&encoded).is_err());
}

#[test]
fn complete_window_primary_frozen_composites_and_exact_framed_cap() {
    let descriptor = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/staging_descriptor_v1_intro_direct_put.hex"
    ));
    let request = Request {
        route: route(),
        descriptor: &descriptor,
    };
    let encoded = encode_request(request).unwrap();
    let response = encode_response(request, state(), Some(NodeId::new(0)));
    let kind = StorageRpcMessageKind::MetadataCommandReplicaState;
    for (payload, literal) in [
        (
            encoded.clone(),
            include_str!("testdata/primary_window_rpc_v37_descriptor_v1_request.hex"),
        ),
        (
            encode_storage_rpc_success_response(&response),
            include_str!("testdata/primary_window_rpc_v37_descriptor_v1_state_v10_response.hex"),
        ),
    ] {
        let frame = crate::tests::frozen_hex_fixture(literal);
        assert_eq!(encode_storage_rpc_frame(17, kind, &payload).unwrap(), frame);
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
    }
    let decoded = decode_request(&encoded).unwrap();
    assert_eq!(decoded.route, request.route);
    assert_eq!(decoded.descriptor, descriptor);
    assert_eq!(
        decode_response(&response, request, Some(NodeId::new(0))).unwrap(),
        state()
    );
    let max = vec![0; MAX_DESCRIPTOR_BYTES];
    let max = encode_request(Request {
        route: route(),
        descriptor: &max,
    })
    .unwrap();
    assert_eq!(max.len(), MAX_REQUEST_BYTES);
    let frame = encode_storage_rpc_frame(17, kind, &max).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(frame))
            .unwrap()
            .payload,
        max
    );
    let cap = super::super::complete_stream_file_primary::MAX_REQUEST_BYTES;
    let frame = encode_storage_rpc_frame(17, kind, &vec![0; cap + 1]).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut Cursor::new(frame)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == cap + 1 && limit == cap)
    );
}

#[test]
fn complete_window_primary_rejects_crossed_purposes_echo_descriptors_and_witnesses() {
    let request = Request {
        route: route(),
        descriptor: b"opaque descriptor",
    };
    let encoded = encode_request(request).unwrap();
    let response = encode_response(request, state(), Some(NodeId::new(0)));
    for len in 0..encoded.len() {
        assert!(decode_request(&encoded[..len]).is_err(), "{len}");
    }
    for len in 0..response.len() {
        assert!(
            decode_response(&response[..len], request, Some(NodeId::new(0))).is_err(),
            "{len}"
        );
    }
    for offset in [0, 4, 6, 14, 18, 26, 30, 62, 78, 87, 96, 128, 129] {
        let mut crossed = response.clone();
        crossed[offset] ^= 1;
        assert!(
            decode_response(&crossed, request, Some(NodeId::new(0))).is_err(),
            "{offset}"
        );
    }
    assert!(decode_response(
        &response,
        Request {
            descriptor: b"another descriptor",
            ..request
        },
        Some(NodeId::new(0))
    )
    .is_err());
    assert!(decode_response(&response, request, None).is_err());
    assert!(decode_response(&response, request, Some(NodeId::new(1))).is_err());
    let singleton = encode_response(request, state(), None);
    assert_eq!(decode_response(&singleton, request, None).unwrap(), state());
    assert!(decode_response(&singleton, request, Some(NodeId::new(0))).is_err());
    for bytes in [
        witness::encode_request(route()),
        encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
            node_id: route().node,
            cluster_epoch: route().epoch,
            pg_id: route().pg,
        }),
    ] {
        assert!(decode_request(&bytes).is_err());
    }
    assert!(witness::decode_request(&encoded).is_err());
    assert!(decode_metadata_command_state_request(&encoded).is_err());
    assert!(witness::decode_response(&response, route()).is_err());
    assert!(decode_metadata_command_state_response(&response).is_err());
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    let mut zero = encoded;
    zero[62..66].fill(0);
    assert!(decode_request(&zero).is_err());
    zero[62..66].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_request(&zero).is_err());
    assert!(encode_request(Request {
        descriptor: &[],
        ..request
    })
    .is_err());
    assert!(encode_request(Request {
        descriptor: &vec![0; MAX_DESCRIPTOR_BYTES + 1],
        ..request
    })
    .is_err());
}
