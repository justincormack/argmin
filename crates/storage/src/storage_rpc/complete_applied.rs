// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Existing command-request grammar with bounded complete-command capacity.
//! Shared by exact terminal and acceptance observations, not mutation requests.
//! Only untrusted borrowed framing leaves this module, never decoded authority.

use super::*;

pub(crate) const MAX_REQUEST_BYTES: usize =
    4 + 8 + 4 + 8 + 4 + crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES;

pub(crate) struct BorrowedRequest<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) epoch: ClusterEpoch,
    pub(crate) pg_id: PgId,
    pub(crate) checksum: u64,
    pub(crate) command: &'a [u8],
}

fn validate_len(len: usize) -> Result<(), StorageRpcPayloadError> {
    if len == 0 || len > crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "complete applied probe command size",
        ));
    }
    Ok(())
}

pub(crate) fn encode(request: BorrowedRequest<'_>) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_len(request.command.len())?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_u64(&mut out, request.checksum);
    put_bytes(&mut out, request.command);
    Ok(out)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<BorrowedRequest<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_REQUEST_BYTES,
        });
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let checksum = decoder.read_u64()?;
    let len = decoder.read_u32()? as usize;
    validate_len(len)?;
    let command = decoder.read_exact(len)?;
    decoder.finish()?;
    Ok(BorrowedRequest {
        node_id,
        epoch,
        pg_id,
        checksum,
        command,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_applied_wire_exact_cap_is_admitted_and_borrowed() {
        let body = vec![0xA5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
        let payload = encode(BorrowedRequest {
            node_id: NodeId::new(4),
            epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(7),
            checksum: 19,
            command: &body,
        })
        .unwrap();
        assert_eq!(payload.len(), MAX_REQUEST_BYTES);
        for kind in [
            StorageRpcMessageKind::MetadataCommandAcceptance,
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            StorageRpcMessageKind::MetadataCommandAbandoned,
        ] {
            let frame = encode_storage_rpc_frame(1, kind, &payload).unwrap();
            let mut frame = decode_storage_rpc_frame(&frame).unwrap();
            validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
            let decoded = decode(&frame.payload).unwrap();
            assert_eq!(decoded.command, body);
            assert_eq!(decoded.command.as_ptr(), frame.payload[28..].as_ptr());
            frame.payload.push(0);
            assert!(matches!(
                validate_storage_rpc_request_frame_payload_limit(&frame),
                Err(StorageRpcFrameError::PayloadTooLarge {
                    limit: MAX_REQUEST_BYTES,
                    ..
                })
            ));
            assert!(matches!(
                decode(&frame.payload),
                Err(StorageRpcPayloadError::PayloadTooLarge {
                    limit: MAX_REQUEST_BYTES,
                    ..
                })
            ));
        }
        let mut header = payload[..28].to_vec();
        header[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&header),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "complete applied probe command size"
            ))
        ));
    }
}
