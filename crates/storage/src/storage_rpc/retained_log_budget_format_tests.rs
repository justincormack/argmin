// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! RPC v37 separates retained-log validation work from general resource pressure.
//! Keep every v36 composite unchanged, including the pre-activation V14 carrier.

use super::*;

#[test]
fn rpc_v37_preserves_v36_checkpoint_composite_fingerprints() {
    for (input, len, digest) in [
        (include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"), 7_884, [112, 184, 64, 121, 39, 254, 233, 87, 33, 82, 181, 65, 71, 165, 12, 166, 116, 216, 15, 196, 216, 81, 112, 194, 85, 207, 9, 104, 46, 177, 242, 213]),
        (include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"), 7_874, [106, 118, 241, 133, 110, 205, 112, 45, 237, 140, 239, 183, 77, 91, 51, 44, 139, 132, 34, 222, 64, 28, 205, 112, 219, 7, 206, 13, 15, 13, 227, 0]),
        (include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"), 9_707, [169, 230, 129, 116, 19, 150, 149, 105, 202, 14, 53, 169, 81, 213, 64, 16, 60, 52, 253, 19, 185, 168, 137, 205, 17, 235, 241, 59, 206, 138, 218, 165]),
        (include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"), 9_697, [43, 217, 39, 208, 129, 160, 246, 158, 188, 98, 233, 184, 107, 58, 233, 34, 56, 236, 154, 210, 40, 193, 137, 211, 31, 175, 186, 189, 187, 147, 90, 34]),
    ] {
        let bytes = crate::tests::frozen_hex_fixture(input);
        assert_eq!(bytes.len(), len);
        assert_eq!(checksum::sha256::digest(&bytes), digest);
        assert_eq!(decode_storage_rpc_frame(&bytes), Err(StorageRpcFrameError::UnsupportedVersion(36)));
    }
}

#[test]
fn retained_log_budget_v37_has_a_distinct_frozen_error_code_and_frame() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 37);
    for (code, tag, frozen) in [
        (StorageRpcErrorCode::ResourceExhausted, 11, "180000006172676d696e2d73746f726167652d7270632d6672616d65250011000000000000007e000d0000003d3bb3b79c126c1101000b00050000006c696d6974"),
        (StorageRpcErrorCode::RetainedLogValidationBudgetExceeded, 28, "180000006172676d696e2d73746f726167652d7270632d6672616d65250011000000000000007e000d0000007d4672128488645a01001c00050000006c696d6974"),
    ] {
        let error = StorageRpcErrorResponse { code, message: "limit".into() };
        let payload = encode_storage_rpc_error_response(&error).unwrap();
        assert_eq!(payload, [1, 0, tag, 0, 5, 0, 0, 0, b'l', b'i', b'm', b'i', b't']);
        let bytes = crate::tests::frozen_hex_fixture(frozen);
        assert_eq!(encode_storage_rpc_frame(17, StorageRpcMessageKind::MetadataCommandRetainedLogEntries, &payload).unwrap(), bytes);
        let frame = decode_storage_rpc_frame(&bytes).unwrap();
        assert_eq!(decode_storage_rpc_response_payload(&frame.payload).unwrap(), Err(error));
        for end in 0..payload.len() {
            assert!(decode_storage_rpc_response_payload(&payload[..end]).is_err());
        }
        let mut unknown = payload;
        unknown[2] = 29;
        assert!(matches!(decode_storage_rpc_response_payload(&unknown), Err(StorageRpcPayloadError::InvalidResponseEnvelope("unknown storage RPC error code"))));
    }
}

#[test]
fn rpc_v37_rejects_all_frozen_v36_composites_without_rewriting_them() {
    for input in [
        include_str!("testdata/staging_scavenger_rpc_v36_response.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"),
        include_str!("testdata/bucket_reservation_rpc_v36.hex"),
        include_str!("testdata/shard_ack_rpc_v36.hex"),
        include_str!("testdata/pending_staging_scavenger_rpc_v36_request.hex"),
        include_str!("testdata/abandoned_log_range_rpc_v36.hex"),
        include_str!("testdata/pending_staging_scavenger_rpc_v36_response.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"),
        include_str!("testdata/staging_scavenger_rpc_v36_request.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"),
        include_str!("testdata/stream_creation_rpc_v36_command_v13_sse_s3_v5.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"),
        include_str!("../pg_store/testdata/complete_log_range_rpc_v36_command_v14_context_v1.hex"),
    ] {
        let lines: Vec<_> = input.lines().filter(|line| !line.starts_with('#') && !line.is_empty()).collect();
        let frames = if lines[0].contains(' ') {
            lines.iter().map(|line| crate::tests::frozen_hex_fixture(line.split_once(' ').unwrap().1)).collect::<Vec<_>>()
        } else {
            vec![crate::tests::frozen_hex_fixture(input)]
        };
        for bytes in frames {
            assert_eq!(decode_storage_rpc_frame(&bytes), Err(StorageRpcFrameError::UnsupportedVersion(36)));
            assert!(matches!(read_storage_rpc_frame_from(&mut std::io::Cursor::new(bytes)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::UnsupportedVersion(36)))));
        }
    }
}
