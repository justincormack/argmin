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
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 37);
    let bytes = crate::tests::frozen_hex_fixture(input);
    assert_eq!(bytes.len(), len);
    assert_eq!(checksum::sha256::digest(&bytes), sha256);
    let frame = decode_storage_rpc_frame(&bytes).expect("valid v37 outer frame");
    assert_eq!(frame.request_id, REQUEST_ID);
    assert_eq!(frame.kind, kind);
    (bytes, frame)
}

#[test]
fn checkpoint_transfer_v37_v7_install_request_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v37_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"),
        7_884,
        [224, 200, 232, 194, 103, 149, 187, 215, 234, 165, 204, 177, 199, 45, 28, 125, 164, 223, 198, 8, 236, 111, 7, 85, 242, 58, 233, 8, 253, 13, 73, 66],
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
fn checkpoint_transfer_v37_v7_export_response_rejects_exact_nested_version() {
    let (_, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v37_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"),
        7_874,
        [88, 229, 133, 93, 177, 164, 139, 252, 110, 254, 157, 195, 179, 201, 129, 224, 168, 28, 211, 198, 84, 13, 141, 2, 118, 13, 148, 155, 11, 153, 9, 183],
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
fn checkpoint_transfer_v37_v8_install_request_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v37_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"),
        9_707,
        [254, 142, 64, 205, 93, 85, 169, 176, 102, 102, 210, 243, 103, 92, 83, 31, 7, 209, 172, 158, 26, 3, 223, 245, 67, 85, 164, 12, 160, 38, 76, 39],
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
fn checkpoint_transfer_v37_v8_export_response_matches_frozen_composite() {
    let (bytes, frame) = frozen_frame(
        include_str!("testdata/checkpoint_transfer_rpc_v37_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"),
        9_697,
        [182, 153, 79, 212, 244, 13, 235, 25, 53, 75, 237, 10, 244, 174, 37, 96, 20, 191, 211, 193, 198, 126, 145, 235, 213, 102, 195, 6, 208, 62, 75, 133],
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
