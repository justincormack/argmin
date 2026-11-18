// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn complete_log_budget_v37_pins_request_bound_outcomes() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 37);
    for (kind, last, literal) in [
        (
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            4096,
            concat!(
                "00002e000000ffffffff7d00",
                "04000000050000000000000000000000",
                "010000000000000000100000000000001000000000000000"
            ),
        ),
        (
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            31,
            concat!(
                "00002e000000ffffffff7e00",
                "04000000050000000000000000000000",
                "01000000000000001f000000000000001000000000000000"
            ),
        ),
    ] {
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(4),
            cluster_epoch: ClusterEpoch::new(5).unwrap(),
            pg_id: PgId::new(0),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(last).unwrap(),
        };
        let frozen = crate::tests::frozen_hex_fixture(literal);
        assert_eq!(encode_response(kind, &request, 17).unwrap(), frozen);
        let bytes = decode_storage_rpc_response_payload(&frozen)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_hint(kind, &request, &bytes).unwrap().unwrap().get(),
            16
        );
        // Neither legacy page decoder can mistake a hint for a successful page.
        assert!(decode_metadata_command_log_hash_range_response(&bytes).is_err());
        assert!(super::super::complete_log_range::decode(&bytes).is_err());
        for end in 0..bytes.len() {
            assert!(decode_hint(kind, &request, &bytes[..end]).is_err(), "{end}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_hint(kind, &request, &trailing).is_err());
        // Independently cross operation, node, epoch, PG, first and last.
        for offset in [4, 6, 10, 18, 22, 30] {
            let mut crossed = bytes.clone();
            crossed[offset] ^= 1;
            assert!(decode_hint(kind, &request, &crossed).is_err(), "{offset}");
        }
        for end in [0, last, last + 1, u64::MAX] {
            let mut crossed = bytes.clone();
            crossed[38..46].copy_from_slice(&end.to_le_bytes());
            assert!(decode_hint(kind, &request, &crossed).is_err(), "{end}");
        }
        assert!(encode_response(kind, &request, 0).is_err());
        assert!(encode_response(kind, &request, 1).is_err());
        assert!(encode_response(kind, &request, last + 1).is_err());
        assert_eq!(decode_hint(kind, &request, &[0; 4]).unwrap(), None);
    }
}
