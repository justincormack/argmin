// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::io::Cursor;

fn specimen() -> (Request, MetadataCommandReplicaState) {
    (
        Request {
            node: NodeId::new(0),
            epoch: ClusterEpoch::new(5).unwrap(),
            pg: PgId::new(7),
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 50_000,
            },
            topology: [0xa5; 32],
        },
        MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(5).unwrap(),
            applied_log_index: 11,
            applied_log_hash: MetadataCommandLogHash::for_test(0x1234),
            state_digest: CanonicalStateDigest::for_test(0x5678),
        },
    )
}

#[test]
fn complete_settled_witness_frozen_payloads_frames_and_exact_admission_cap() {
    let (request, state) = specimen();
    let req = crate::tests::frozen_hex_fixture("010050c300000000000000000000050000000000000007000000a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5");
    let res = crate::tests::frozen_hex_fixture("010050c300000000000000000000050000000000000007000000a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a505000000000000000b000000000000000134120000000000000a7856000000000000");
    assert_eq!(encode_request(request), req);
    assert_eq!(decode_request(&req).unwrap(), request);
    assert_eq!(encode_response(request, state), res);
    assert_eq!(decode_response(&res, request).unwrap(), state);
    let kind = StorageRpcMessageKind::MetadataCommandReplicaState;
    for (payload, literal) in [
        (
            req.clone(),
            include_str!("testdata/settled_witness_rpc_v37_payload_v1_request.hex"),
        ),
        (
            encode_storage_rpc_success_response(&res),
            include_str!("testdata/settled_witness_rpc_v37_payload_v1_state_v10_response.hex"),
        ),
    ] {
        let frame = crate::tests::frozen_hex_fixture(literal);
        assert_eq!(encode_storage_rpc_frame(17, kind, &payload).unwrap(), frame);
        assert_eq!(decode_storage_rpc_frame(&frame).unwrap().payload, payload);
        for version in [36, 38] {
            let wrong = encode_storage_rpc_frame_with_version_for_test(17, kind, &payload, version);
            assert!(matches!(
                decode_storage_rpc_frame(&wrong),
                Err(StorageRpcFrameError::UnsupportedVersion(_))
            ));
        }
    }
    assert_eq!(req.len(), REQUEST_BYTES);
    let at_cap = encode_storage_rpc_frame(17, kind, &req).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(at_cap))
            .unwrap()
            .payload,
        req
    );
    // The opcode also admits bounded primary-window and read-only stream-file grammars.
    let cap = super::super::complete_stream_file_primary::MAX_REQUEST_BYTES;
    let at_cap = encode_storage_rpc_frame(17, kind, &vec![0; cap]).unwrap();
    assert_eq!(
        read_storage_rpc_request_frame_from(&mut Cursor::new(at_cap))
            .unwrap()
            .payload
            .len(),
        cap
    );
    let over_cap = encode_storage_rpc_frame(17, kind, &vec![0; cap + 1]).unwrap();
    assert!(
        matches!(read_storage_rpc_request_frame_from(&mut Cursor::new(over_cap)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == cap + 1 && limit == cap)
    );
}

#[test]
fn complete_settled_witness_rejects_crossed_echo_prefix_and_raw_state_grammar() {
    let (request, state) = specimen();
    let req = encode_request(request);
    let res = encode_response(request, state);
    for len in 0..req.len() {
        assert!(decode_request(&req[..len]).is_err(), "{len}");
    }
    for len in 0..res.len() {
        assert!(decode_response(&res[..len], request).is_err(), "{len}");
    }
    for offset in [
        0,
        2,
        10,
        14,
        22,
        26,
        REQUEST_BYTES,
        REQUEST_BYTES + 16,
        REQUEST_BYTES + 25,
    ] {
        let mut crossed = res.clone();
        crossed[offset] ^= 1;
        assert!(decode_response(&crossed, request).is_err(), "{offset}");
    }
    let mut trailing = req.clone();
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    let mut trailing = res.clone();
    trailing.push(0);
    assert!(decode_response(&trailing, request).is_err());
    let old_req = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node,
        cluster_epoch: request.epoch,
        pg_id: request.pg,
    });
    let old_res =
        encode_metadata_command_state_response(&StorageRpcMetadataCommandStateResponse { state });
    assert!(decode_request(&old_req).is_err());
    assert!(decode_response(&old_res, request).is_err());
    assert!(decode_metadata_command_state_request(&req).is_err());
    assert!(decode_metadata_command_state_response(&res).is_err());
    let topology = crate::PgTopology::new(&[7, 9, 2]).unwrap();
    assert_eq!(
        topology_digest(&topology).as_slice(),
        crate::tests::frozen_hex_fixture(
            "f44c3b02edfb671f744f7b2b7bbfa3a997ef20f00bd04c85fb3b9f52a511d8b5"
        )
    );
    assert_eq!(
        topology_digest(&topology),
        topology_digest(&crate::PgTopology::new(&[9, 7, 2, 7]).unwrap())
    );
    assert_ne!(
        topology_digest(&topology),
        topology_digest(&crate::PgTopology::new(&[7, 9, 3]).unwrap())
    );
}
