// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn applied(bytes: &[u8]) -> WireEntry<'_> {
    WireEntry {
        log_index: 1,
        previous_log_hash: MetadataCommandLogHash::for_test(0),
        log_hash: MetadataCommandLogHash::for_test(1),
        pre_state_digest: Some(CanonicalStateDigest::for_test(2)),
        post_state_digest: Some(CanonicalStateDigest::for_test(3)),
        kind: WireKind::Applied { checksum: 0, bytes },
    }
}

#[test]
fn complete_log_wire_bounds_bytes_before_payload_access_and_borrows_admitted_bodies() {
    // Framing-only data, deliberately not commands: this owner must not decode
    // or confer authority. Request-bound context validation is a separate gate.
    let body = vec![0xA5; 1024 * 1024];
    let mut encoder = Encoder::new();
    for _ in 0..16 {
        encoder = encoder.push(applied(&body)).unwrap();
    }
    let exact = encoder.finish();
    let frame = encode_storage_rpc_frame(
        17,
        StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
        &encode_storage_rpc_success_response(&exact),
    )
    .unwrap();
    let frame = decode_storage_rpc_frame(&frame).unwrap();
    assert_eq!(
        decode_storage_rpc_response_payload(&frame.payload)
            .unwrap()
            .unwrap(),
        exact
    );
    let decoded = decode(&exact).unwrap();
    assert_eq!(decoded.len(), 16);
    for row in decoded {
        let WireKind::Applied { bytes, .. } = row.kind else {
            panic!()
        };
        assert_eq!(bytes, body);
        assert!(bytes.as_ptr() >= exact.as_ptr());
        assert!(bytes.as_ptr_range().end <= exact.as_ptr_range().end);
    }
    let larger = vec![0xA5; body.len() + 1];
    let mut prefix = Encoder::new();
    for _ in 0..15 {
        prefix = prefix.push(applied(&body)).unwrap();
    }
    let mut excess = prefix.finish();
    let last = Encoder::new().push(applied(&larger)).unwrap().finish();
    excess.extend_from_slice(&last[4..]);
    excess[..4].copy_from_slice(&16_u32.to_le_bytes());
    assert!(excess.len() < MAX_RESPONSE_BYTES);
    assert!(matches!(
        decode(&excess),
        Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "complete log response command byte budget"
        ))
    ));
    let mut prefix = Encoder::new();
    for _ in 0..15 {
        prefix = prefix.push(applied(&body)).unwrap();
    }
    assert!(prefix.push(applied(&larger)).is_err());
    let oversized =
        vec![0; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES + 1];
    assert!(Encoder::new().push(applied(&oversized)).is_err());
    let mut missing = Encoder::new().push(applied(&[42])).unwrap().finish();
    let length = missing.len() - 5;
    missing[length..length + 4].copy_from_slice(&(oversized.len() as u32).to_le_bytes());
    missing.truncate(length + 4);
    assert!(
        matches!(
            decode(&missing),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "complete log command size"
            ))
        ),
        "must reject length before requesting missing body"
    );
    assert!(matches!(
        decode(&vec![0; MAX_RESPONSE_BYTES + 1]),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
}

#[test]
fn complete_log_wire_preserves_current_outer_grammar_for_every_digest_option_and_kind() {
    use crate::metadata_command::{
        MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload,
        ReserveObjectVersionCommand,
    };
    let id = MetadataCommandId::new(
        ClusterEpoch::new(5).unwrap(),
        PgId::new(4),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let command = MetadataCommandEnvelope::new(
        id,
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            BucketName::new("bucket").unwrap(),
            ObjectKey::new("key").unwrap(),
            crate::VersionId::from_u64(17),
        )),
    );
    let bytes = command.command_bytes();
    for pre in [None, Some(CanonicalStateDigest::for_test(2))] {
        for post in [None, Some(CanonicalStateDigest::for_test(3))] {
            for abandoned in [false, true] {
                let mut row = applied(&bytes);
                row.pre_state_digest = pre;
                row.post_state_digest = post;
                row.kind = if abandoned {
                    WireKind::Abandoned {
                        checksum: 55,
                        sha256: [66; 32],
                    }
                } else {
                    WireKind::Applied {
                        checksum: command.checksum_crc64(),
                        bytes: &bytes,
                    }
                };
                let old = StorageRpcMetadataCommandLogEntryRangeResponse {
                    entries: vec![MetadataCommandLogRangeEntry {
                        log_index: 1,
                        previous_log_hash: row.previous_log_hash,
                        log_hash: row.log_hash,
                        pre_state_digest: pre,
                        post_state_digest: post,
                        kind: if abandoned {
                            MetadataCommandLogRangeEntryKind::Abandoned {
                                original_command_checksum: 55,
                                original_command_sha256: [66; 32],
                            }
                        } else {
                            MetadataCommandLogRangeEntryKind::Applied(Box::new(command.clone()))
                        },
                    }],
                };
                let payload = Encoder::new().push(row).unwrap().finish();
                assert_eq!(
                    payload,
                    encode_metadata_command_log_entry_range_response(&old).unwrap()
                );
                let rows = decode(&payload).unwrap();
                let encoded = rows
                    .into_iter()
                    .try_fold(Encoder::new(), |encoder, row| encoder.push(row))
                    .unwrap()
                    .finish();
                assert_eq!(encoded, payload);
                for end in 0..payload.len() {
                    assert!(decode(&payload[..end]).is_err());
                }
                let mut trailing = payload.clone();
                trailing.push(0);
                assert!(decode(&trailing).is_err());
            }
        }
    }
}

#[test]
fn complete_log_wire_rejects_invalid_flags_indices_counts_and_kinds() {
    let good = Encoder::new().push(applied(&[42])).unwrap().finish();
    // Header: count4, index8, hash9, hash9, pre10, post10, kind1.
    for offset in [12, 21, 30, 40, 50] {
        let mut bytes = good.clone();
        bytes[offset] = 42;
        assert!(decode(&bytes).is_err(), "invalid tag at {offset}");
    }
    let mut zero = good.clone();
    zero[4..12].fill(0);
    assert!(decode(&zero).is_err());
    assert!(decode(&32_u32.to_le_bytes()).is_err());
    let mut encoder = Encoder::new();
    for _ in 0..31 {
        encoder = encoder.push(applied(&[42])).unwrap();
    }
    let exact = encoder.finish();
    assert_eq!(decode(&exact).unwrap().len(), 31);
    let mut encoder = Encoder::new();
    for _ in 0..31 {
        encoder = encoder.push(applied(&[42])).unwrap();
    }
    assert!(encoder.push(applied(&[42])).is_err());
    assert!(decode(&Encoder::new().finish()).unwrap().is_empty());
}
