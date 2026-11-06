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
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 35);
    let bytes = crate::tests::frozen_hex_fixture(input);
    assert_eq!(bytes.len(), len);
    assert_eq!(checksum::sha256::digest(&bytes), sha256);
    let frame = decode_storage_rpc_frame(&bytes).expect("valid v35 outer frame");
    assert_eq!(frame.request_id, REQUEST_ID);
    assert_eq!(frame.kind, kind);
    (bytes, frame)
}

#[test]
fn checkpoint_transfer_v35_v7_install_request_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v35_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"),
        7_884,
        [190, 144, 42, 159, 57, 162, 114, 129, 247, 182, 251, 183, 93, 103, 94, 190, 16, 27, 194, 53, 146, 186, 160, 1, 209, 162, 167, 244, 15, 97, 181, 100],
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
fn checkpoint_transfer_v35_v7_export_response_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v35_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"),
        7_874,
        [62, 45, 50, 240, 130, 108, 97, 86, 12, 123, 53, 172, 38, 60, 162, 240, 242, 237, 55, 221, 133, 228, 157, 67, 187, 24, 52, 180, 22, 19, 180, 156],
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
fn checkpoint_transfer_v35_v8_install_request_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v35_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"),
        9_707,
        [205, 32, 240, 32, 181, 241, 240, 204, 117, 62, 162, 86, 185, 76, 125, 193, 116, 160, 83, 89, 236, 46, 78, 141, 61, 19, 148, 123, 4, 31, 204, 139],
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
fn checkpoint_transfer_v35_v8_export_response_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v35_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"),
        9_697,
        [95, 141, 74, 110, 28, 67, 57, 57, 108, 69, 64, 19, 201, 36, 125, 53, 105, 32, 162, 145, 80, 54, 26, 201, 166, 172, 120, 90, 213, 204, 7, 184],
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
