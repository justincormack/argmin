// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn complete_pending_wire_maximum_is_bounded_borrowed_and_frame_admissible() {
    // Framing-only sentinel, deliberately not a command. Context validation
    // belongs to the PG owner, not to this borrowing wire parser.
    let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let payload = encode(Some((0x1234, &body)), true).unwrap();
    assert_eq!(payload.len(), MAX_RESPONSE_BYTES);
    let BorrowedPendingResponse {
        command: Some((checksum, bytes)),
        publication_started,
    } = decode(&payload).unwrap()
    else {
        panic!()
    };
    assert_eq!(checksum, 0x1234);
    assert!(publication_started);
    assert_eq!(bytes, body);
    assert_eq!(bytes.as_ptr(), payload[13..].as_ptr());
    let frame = encode_storage_rpc_frame(
        17,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope,
        &encode_storage_rpc_success_response(&payload),
    )
    .unwrap();
    let decoded = decode_storage_rpc_frame(&frame).unwrap();
    assert_eq!(
        decode_storage_rpc_response_payload(&decoded.payload)
            .unwrap()
            .unwrap(),
        payload
    );
    assert!(matches!(
        decode(&vec![0; MAX_RESPONSE_BYTES + 1]),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
    assert!(encode(Some((0, &vec![0; body.len() + 1])), false).is_err());
    // Reject an excessive announced length before trying to access its body.
    let mut header = vec![1];
    header.extend_from_slice(&0u64.to_le_bytes());
    header.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode(&header),
        Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "complete pending command size"
        ))
    ));
}

#[test]
fn complete_pending_wire_flags_and_unchanged_outer_grammar_are_exact() {
    assert_eq!(encode(None, false).unwrap(), [0, 0]);
    assert_eq!(
        decode(&[0, 0]).unwrap(),
        BorrowedPendingResponse {
            command: None,
            publication_started: false,
        }
    );
    assert!(encode(None, true).is_err());
    assert!(encode(Some((0, &[])), false).is_err());
    let literal = [1, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 0, 0, 0xA5, 1];
    assert_eq!(
        encode(Some((0x0102_0304_0506_0708, &[0xA5])), true).unwrap(),
        literal
    );
    for bytes in [
        vec![],
        vec![0],
        vec![0, 1],
        vec![0, 2],
        vec![2, 0],
        vec![0, 0, 0],
    ] {
        assert!(decode(&bytes).is_err(), "{bytes:?}");
    }
    for len in 0..literal.len() {
        assert!(decode(&literal[..len]).is_err());
    }
    let mut bad = literal.to_vec();
    *bad.last_mut().unwrap() = 2;
    assert!(decode(&bad).is_err());
    let mut bad = literal.to_vec();
    bad[9..13].copy_from_slice(&0u32.to_le_bytes());
    assert!(decode(&bad).is_err());
}
