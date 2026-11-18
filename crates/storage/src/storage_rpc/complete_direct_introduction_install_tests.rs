// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn specimen() -> Vec<u8> {
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/direct_introduction_v1_request.hex"
    ));
    let response =
        crate::tests::frozen_hex_fixture(include_str!("testdata/direct_introduction_v1_reply.hex"));
    let bytes = encode_request(Request {
        preparation: &request,
        response: &response,
    })
    .unwrap();
    // Immutable outer grammar, composed with immutable predecessor specimens.
    let mut frozen = crate::tests::frozen_hex_fixture("444949510100e3000000");
    frozen.extend_from_slice(&request);
    frozen.extend_from_slice(&crate::tests::frozen_hex_fixture("78000000"));
    frozen.extend_from_slice(&response);
    assert_eq!(bytes, frozen);
    bytes
}

#[test]
fn complete_direct_introduction_install_frozen_borrowed_exchange_and_confirmation() {
    let bytes = specimen();
    assert_eq!(bytes.len(), 361);
    let decoded = decode_request(&bytes).unwrap();
    assert_eq!(decoded.preparation.as_ptr(), bytes[10..].as_ptr());
    assert_eq!(decoded.response.as_ptr(), bytes[241..].as_ptr());
    assert_eq!(encode_request(decoded).unwrap(), bytes);
    let expected = crate::tests::frozen_hex_fixture(
        "44494943010055ec8efe91528a7b61631116ae0a03fdd2bfc6cf7c3722fd25eb04b6d6789265",
    );
    assert_eq!(confirmation(&bytes).unwrap(), expected);
    require_confirmation(&bytes, &expected).unwrap();
    for end in 0..bytes.len() {
        assert!(decode_request(&bytes[..end]).is_err(), "{end}");
    }
    for end in 0..expected.len() {
        assert!(require_confirmation(&bytes, &expected[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_request(&trailing).is_err());
    for offset in [0, 4, 6, 237, 241] {
        let mut crossed = bytes.clone();
        crossed[offset] ^= 255;
        assert!(decode_request(&crossed).is_err(), "{offset}");
    }
    let decoded = decode_request(&bytes).unwrap();
    let mut logical = preparation::decode_request(decoded.preparation).unwrap();
    logical.proof.owner_token.push('x');
    let new_request = preparation::encode_request(&logical).unwrap();
    assert!(encode_request(Request {
        preparation: new_request.bytes(),
        response: decoded.response
    })
    .is_err());
    let original =
        preparation::encode_request(&preparation::decode_request(decoded.preparation).unwrap())
            .unwrap();
    let response = original.decode_response(decoded.response).unwrap();
    let new_response =
        preparation::encode_response(&new_request, response.command, response.cutoff).unwrap();
    let changed = encode_request(Request {
        preparation: new_request.bytes(),
        response: &new_response,
    })
    .unwrap();
    assert!(require_confirmation(&changed, &expected).is_err());
    assert!(require_confirmation(&bytes, &confirmation(&changed).unwrap()).is_err());
}

#[test]
fn complete_direct_introduction_install_directional_cap_and_distinct_kind() {
    let kind = StorageRpcMessageKind::StagingDirectIntroductionInstall;
    assert_eq!(kind as u16, 186);
    assert_eq!(StorageRpcMessageKind::from_u16(186).unwrap(), kind);
    for len in [MAX_REQUEST_BYTES, MAX_REQUEST_BYTES + 1] {
        let bytes = encode_storage_rpc_frame(17, kind, &vec![0; len]).unwrap();
        let result = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes));
        if len == MAX_REQUEST_BYTES {
            assert_eq!(result.unwrap().payload.len(), len);
        } else {
            assert!(
                matches!(result, Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len: actual, limit })) if actual == len && limit == MAX_REQUEST_BYTES)
            );
        }
    }
    assert!(matches!(
        decode_request(&vec![0; MAX_REQUEST_BYTES + 1]),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
}
