// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete transfer-carrier composites. The base-install request and export
//! response are different operations, both containing a versioned checkpoint.
//! Old checkpoint bytes are evidence, not an authorization to read old formats.

use super::*;

const REQUEST_ID: u64 = 0x0102_0304_0506_0708;

fn current_checkpoint() -> MetadataCommandCheckpoint {
    let bytes = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/checkpoint_v8_canonical_v10_pg_v11_sse_s3_v5.hex"
    ));
    let checkpoint = decode_metadata_command_checkpoint_payload(&bytes).unwrap();
    checkpoint.verify().unwrap();
    checkpoint
}

fn install_request() -> StorageRpcMetadataCommandTransferCheckpointBaseRequest {
    StorageRpcMetadataCommandTransferCheckpointBaseRequest {
        node_id: NodeId::new(7),
        cluster_epoch: ClusterEpoch::new(4).unwrap(),
        pg_id: PgId::new(1),
        checkpoint: current_checkpoint(),
    }
}

fn frozen_frame(
    input: &str,
    len: usize,
    sha256: [u8; 32],
    kind: StorageRpcMessageKind,
) -> (Vec<u8>, StorageRpcFrame) {
    // An outer-version or checksum rejection cannot satisfy the nested-format
    // regressions: the production frame reader must first accept these bytes.
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 36);
    let bytes = crate::tests::frozen_hex_fixture(input);
    assert_eq!(bytes.len(), len);
    assert_eq!(checksum::sha256::digest(&bytes), sha256);
    let frame = decode_storage_rpc_frame(&bytes).expect("valid v36 outer frame");
    assert_eq!(frame.request_id, REQUEST_ID);
    assert_eq!(frame.kind, kind);
    (bytes, frame)
}

#[test]
fn checkpoint_transfer_v36_v7_install_request_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"),
        7_884,
        [112, 184, 64, 121, 39, 254, 233, 87, 33, 82, 181, 65, 71, 165, 12, 166, 116, 216, 15, 196, 216, 81, 112, 194, 85, 207, 9, 104, 46, 177, 242, 213],
        StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
    );
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    assert_eq!(
        &frame.payload[..16],
        &[7, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    );
    let checkpoint = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/checkpoint_v7_canonical_v9_pg_v10_sse_s3_v5.hex"
    ));
    assert_eq!(&frame.payload[16..], checkpoint);
    assert_eq!(
        decode_metadata_command_transfer_checkpoint_base_request(&frame.payload),
        Err(StorageRpcPayloadError::UnsupportedMetadataCheckpointEncodingVersion { actual: 7 }),
    );
}

#[test]
fn checkpoint_transfer_v36_v7_export_response_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"),
        7_874,
        [106, 118, 241, 133, 110, 205, 112, 45, 237, 140, 239, 183, 77, 91, 51, 44, 139, 132, 34, 222, 64, 28, 205, 112, 219, 7, 206, 13, 15, 13, 227, 0],
        StorageRpcMessageKind::MetadataCommandCheckpointExport,
    );
    let payload = decode_storage_rpc_response_payload(&frame.payload)
        .unwrap()
        .unwrap();
    let checkpoint = crate::tests::frozen_hex_fixture(include_str!(
        "../pg_store/testdata/checkpoint_v7_canonical_v9_pg_v10_sse_s3_v5.hex"
    ));
    assert_eq!(payload, checkpoint);
    assert_eq!(
        decode_metadata_command_checkpoint_response(&payload),
        Err(StorageRpcPayloadError::UnsupportedMetadataCheckpointEncodingVersion { actual: 7 }),
    );
}

#[test]
fn checkpoint_transfer_v36_v8_install_request_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"),
        9_707,
        [169, 230, 129, 116, 19, 150, 149, 105, 202, 14, 53, 169, 81, 213, 64, 16, 60, 52, 253, 19, 185, 168, 137, 205, 17, 235, 241, 59, 206, 138, 218, 165],
        StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
    );
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    let expected = install_request();
    assert_eq!(
        decode_metadata_command_transfer_checkpoint_base_request(&frame.payload).unwrap(),
        expected,
    );
    let payload = encode_metadata_command_transfer_checkpoint_base_request(&expected).unwrap();
    assert_eq!(
        encode_storage_rpc_frame(
            REQUEST_ID,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            &payload
        )
        .unwrap(),
        bytes,
    );
}

#[test]
fn checkpoint_transfer_v36_v8_export_response_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v36_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"),
        9_697,
        [43, 217, 39, 208, 129, 160, 246, 158, 188, 98, 233, 184, 107, 58, 233, 34, 56, 236, 154, 210, 40, 193, 137, 211, 31, 175, 186, 189, 187, 147, 90, 34],
        StorageRpcMessageKind::MetadataCommandCheckpointExport,
    );
    let payload = decode_storage_rpc_response_payload(&frame.payload)
        .unwrap()
        .unwrap();
    let expected = StorageRpcMetadataCommandCheckpointResponse {
        checkpoint: current_checkpoint(),
    };
    assert_eq!(
        decode_metadata_command_checkpoint_response(&payload).unwrap(),
        expected
    );
    let payload = encode_storage_rpc_success_response(
        &encode_metadata_command_checkpoint_response(&expected).unwrap(),
    );
    assert_eq!(
        encode_storage_rpc_frame(
            REQUEST_ID,
            StorageRpcMessageKind::MetadataCommandCheckpointExport,
            &payload
        )
        .unwrap(),
        bytes,
    );
}
