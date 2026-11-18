// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate publication payload V1; owner-local servers select it under the
//! pending RPC-v37 transition. Production publication dispatch is not switched.
//! These borrowed records are untrusted framing, never publication authority.

use super::*;
use crate::metadata_command::MetadataCommandId;

const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 2 + 8 + complete_applied::MAX_REQUEST_BYTES;
pub(crate) const RESPONSE_BYTES: usize = 2 + 4 + 8 + 4 + 8 + 32;

pub(crate) struct BorrowedRequest<'a> {
    pub(crate) command: complete_applied::BorrowedRequest<'a>,
    pub(crate) deadline: StorageRpcOperationDeadline,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Confirmation {
    pub(crate) node_id: NodeId,
    pub(crate) id: MetadataCommandId,
    pub(crate) command_sha256: [u8; 32],
}

fn version(decoder: &mut StorageRpcDecoder<'_>) -> Result<(), StorageRpcPayloadError> {
    if decoder.read_u16()? != VERSION {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "complete publication payload version",
        ));
    }
    Ok(())
}

pub(crate) fn encode_request(
    request: BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let body = complete_applied::encode(request.command)?;
    let mut out = Vec::with_capacity(10 + body.len());
    put_u16(&mut out, VERSION);
    put_u64(&mut out, request.deadline.portable_wall_valid_until_ms);
    out.extend_from_slice(&body);
    Ok(out)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<BorrowedRequest<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_REQUEST_BYTES,
        });
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    version(&mut decoder)?;
    let deadline = StorageRpcOperationDeadline {
        portable_wall_valid_until_ms: decoder.read_u64()?,
    };
    Ok(BorrowedRequest {
        command: complete_applied::decode(&bytes[10..])?,
        deadline,
    })
}

pub(crate) fn encode_confirmation(confirmation: Confirmation) -> Vec<u8> {
    let mut out = Vec::with_capacity(RESPONSE_BYTES);
    put_u16(&mut out, VERSION);
    put_u32(&mut out, confirmation.node_id.as_u32());
    put_u64(&mut out, confirmation.id.cluster_epoch().get());
    put_u32(&mut out, confirmation.id.pg_id().get());
    put_u64(&mut out, confirmation.id.log_index().get());
    out.extend_from_slice(&confirmation.command_sha256);
    out
}

pub(crate) fn decode_confirmation(bytes: &[u8]) -> Result<Confirmation, StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "complete publication confirmation size",
        ));
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    version(&mut decoder)?;
    let node_id = NodeId::new(decoder.read_u32()?);
    let epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let log_index = MetadataCommandLogIndex::new(decoder.read_u64()?).ok_or(
        StorageRpcPayloadError::InvalidResponseEnvelope("complete publication confirmation index"),
    )?;
    let command_sha256 = decoder
        .read_exact(32)?
        .try_into()
        .expect("exact digest width");
    decoder.finish()?;
    Ok(Confirmation {
        node_id,
        id: MetadataCommandId::new(epoch, pg_id, log_index),
        command_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_publication_frames_pin_v37_composites_and_exact_admission_cap() {
        use std::io::Cursor;
        let kind = StorageRpcMessageKind::MetadataCommandPublicationStart;
        let request = crate::tests::frozen_hex_fixture(include_str!(
            "../pg_store/testdata/complete_publication_payload_v1_command_v14_context_v1.hex"
        ));
        let confirmation = crate::tests::frozen_hex_fixture(include_str!(
            "../pg_store/testdata/complete_publication_confirmation_v1_command_v14_context_v1.hex"
        ));
        let response = encode_storage_rpc_success_response(&confirmation);
        for (payload, literal) in [
            (request, include_str!("testdata/complete_publication_rpc_v37_payload_v1_command_v14_context_v1_request.hex")),
            (response, include_str!("testdata/complete_publication_rpc_v37_payload_v1_command_v14_context_v1_response.hex")),
        ] {
            let frozen = crate::tests::frozen_hex_fixture(literal);
            let frame = encode_storage_rpc_frame(17, kind, &payload).unwrap();
            assert_eq!(frame, frozen);
            let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(&frozen)).unwrap();
            assert_eq!(decoded.request_id, 17);
            assert_eq!(decoded.kind, kind);
            assert_eq!(decoded.payload, payload);
            for version in [36, 38] {
                let bytes =
                    encode_storage_rpc_frame_with_version_for_test(17, kind, &payload, version);
                assert!(matches!(
                    read_storage_rpc_request_frame_from(&mut Cursor::new(bytes)),
                    Err(StorageRpcStreamError::Frame(
                        StorageRpcFrameError::UnsupportedVersion { .. }
                    ))
                ));
            }
        }
        for extra in [0, 1] {
            let bytes =
                encode_storage_rpc_frame(17, kind, &vec![0; MAX_REQUEST_BYTES + extra]).unwrap();
            let result = read_storage_rpc_request_frame_from(&mut Cursor::new(bytes));
            if extra == 0 {
                assert_eq!(result.unwrap().payload.len(), MAX_REQUEST_BYTES);
            } else {
                assert!(
                    matches!(result, Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge { len, limit })) if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
                );
            }
        }
    }

    #[test]
    fn complete_publication_payload_bounds_and_frozen_outer_grammar() {
        let body = [0xAA, 0xBB];
        let request = |command| BorrowedRequest {
            command: complete_applied::BorrowedRequest {
                node_id: NodeId::new(4),
                epoch: ClusterEpoch::new(5).unwrap(),
                pg_id: PgId::new(7),
                checksum: 19,
                command,
            },
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 50_000,
            },
        };
        let bytes = encode_request(request(&body)).unwrap();
        let frozen = crate::tests::frozen_hex_fixture(
            "010050c300000000000004000000050000000000000007000000130000000000000002000000aabb",
        );
        assert_eq!(bytes, frozen);
        for length in 0..bytes.len() {
            assert!(decode_request(&bytes[..length]).is_err(), "{length}");
        }
        for version in [0_u16, 2] {
            let mut wrong = bytes.clone();
            wrong[..2].copy_from_slice(&version.to_le_bytes());
            assert!(decode_request(&wrong).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_request(&trailing).is_err());
        let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
        let mut bytes = encode_request(request(&body)).unwrap();
        assert_eq!(bytes.len(), MAX_REQUEST_BYTES);
        let decoded = decode_request(&bytes).unwrap();
        assert_eq!(decoded.command.command.as_ptr(), bytes[38..].as_ptr());
        assert_eq!(decoded.command.command, body);
        bytes.push(0);
        assert!(matches!(
            decode_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                limit: MAX_REQUEST_BYTES,
                ..
            })
        ));
        assert!(encode_request(request(&[])).is_err());
        assert!(encode_request(request(&bytes)).is_err());
    }

    #[test]
    fn complete_publication_confirmation_frozen_and_strict() {
        let confirmation = Confirmation {
            node_id: NodeId::new(4),
            id: MetadataCommandId::new(
                ClusterEpoch::new(5).unwrap(),
                PgId::new(7),
                MetadataCommandLogIndex::new(11).unwrap(),
            ),
            command_sha256: [0xA5; 32],
        };
        let frozen = crate::tests::frozen_hex_fixture("0100040000000500000000000000070000000b00000000000000a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5");
        assert_eq!(decode_confirmation(&frozen).unwrap(), confirmation);
        let bytes = encode_confirmation(confirmation);
        assert_eq!(bytes, frozen);
        for length in 0..bytes.len() {
            assert!(decode_confirmation(&bytes[..length]).is_err(), "{length}");
        }
        for version in [0_u16, 2] {
            let mut wrong = bytes.clone();
            wrong[..2].copy_from_slice(&version.to_le_bytes());
            assert!(decode_confirmation(&wrong).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_confirmation(&trailing).is_err());
    }
}
