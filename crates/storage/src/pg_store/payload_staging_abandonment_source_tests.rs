// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    MetadataCommandFormatError, MetadataCommandRecordFormat, MAX_METADATA_COMMAND_BYTES,
};

// Independent grammar offsets, not production constants: these pin framing as
// well as rejection precedence. The nested command is independently versioned.
const HEADER_BYTES: usize = 22;

/// Preserve CRC64 while changing the release proof. Eight ETag bytes supply a
/// full-rank linear compensation; this is not random corruption or a bad CRC.
pub(in super::super) fn crossed_proof_with_equal_crc(
    source: &MetadataCommandEnvelope,
    field: &str,
) -> MetadataCommandEnvelope {
    let mut crossed = payload(source).clone();
    // Preserve length so compensation also preserves the COMPLETE v14 CRC.
    if field == "etag" {
        // Keep every authority field unchanged: both commands remain applicable.
        // Byte eight is outside the eight compensation bytes below.
        crossed.part.etag[8] ^= 1;
    } else {
        let identity = match field {
            "reservation" => &mut crossed.bucket_write_reservation.reservation_id,
            "owner" => &mut crossed.bucket_write_reservation.owner_token,
            _ => unreachable!(),
        };
        let first = identity.as_bytes()[0];
        assert!(first.is_ascii());
        identity.replace_range(..1, if first == b'x' { "y" } else { "x" });
    }
    assert!(crossed.part.etag.len() >= 8);
    let envelope = |part: CommitStreamPartCommand| {
        MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(part)),
        )
    };
    let base_crc = envelope(crossed.clone()).checksum_crc64();
    let mut basis = [(0_u64, 0_u64); 64];
    for bit in 0..64 {
        let mut probe = crossed.clone();
        probe.part.etag[bit / 8] ^= 1 << (bit % 8);
        let mut delta = envelope(probe).checksum_crc64() ^ base_crc;
        let mut patch = 1_u64 << bit;
        while delta != 0 {
            let pivot = 63 - delta.leading_zeros() as usize;
            if basis[pivot].0 == 0 {
                basis[pivot] = (delta, patch);
                break;
            }
            delta ^= basis[pivot].0;
            patch ^= basis[pivot].1;
        }
    }
    assert!(basis.iter().all(|(delta, _)| *delta != 0));
    let mut delta = source.checksum_crc64() ^ base_crc;
    let mut patch = 0_u64;
    while delta != 0 {
        let pivot = 63 - delta.leading_zeros() as usize;
        delta ^= basis[pivot].0;
        patch ^= basis[pivot].1;
    }
    for (byte, correction) in crossed.part.etag.iter_mut().zip(patch.to_le_bytes()) {
        *byte ^= correction;
    }
    let crossed = envelope(crossed);
    assert!(crossed.verify_checksum());
    assert_eq!(crossed.checksum_crc64(), source.checksum_crc64());
    assert_eq!(
        checksum::crc64::checksum(&complete(&crossed)),
        checksum::crc64::checksum(&complete(source))
    );
    assert_ne!(
        checksum::sha256::digest(&crossed.command_bytes()),
        checksum::sha256::digest(&source.command_bytes())
    );
    crossed
}

#[test]
fn staging_abandonment_source_rejects_crc_collision_before_and_after_compaction() {
    for count in [0, 2] {
        for storage in ["retained", "reopened", "compacted"] {
            for field in ["reservation", "owner"] {
                let dir = test_util::tempdir();
                let pg_id = fixture(multipart_target()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let owner = part::initialize(&pg, count);
                let source = part::command(&pg, &owner);
                let follow_up = cleanup(&pg, &owner, &source);
                let crossed = crossed_proof_with_equal_crc(&source, field);
                let carrier = PartFinalizationSource::capture_for_test(&crossed).unwrap();
                let carrier = PartFinalizationSource::decode_for_test(&carrier.encode()).unwrap();
                terminal(&pg, &source, true);
                pg.conn.execute_batch("COMMIT").unwrap();
                if storage == "compacted" {
                    pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                        .unwrap();
                    pg.compact_metadata_command_log(source.id().cluster_epoch())
                        .unwrap();
                    assert_eq!(
                        pg.metadata_command_log_stats(source.id().cluster_epoch())
                            .unwrap()
                            .retained_entries,
                        0
                    );
                }
                let pg = if storage == "retained" {
                    pg
                } else {
                    drop(pg);
                    PgStore::open(dir.path(), pg_id).unwrap()
                };
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                let result = apply_carrier(&pg, &carrier, &follow_up);
                assert!(result.is_err(), "CRC-colliding {field} source acquired custody: {storage}, segments={count}: {result:?}");
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                apply(&pg, &source, &follow_up).unwrap();
                assert_closed(&pg, &owner, &source);
            }
        }
    }
}

fn specimen(index: usize) -> MetadataCommandEnvelope {
    let line = include_str!("../metadata_command_v13_sse_s3_v5_object_corpus.hex")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .nth(index)
        .unwrap();
    let bytes = crate::tests::frozen_hex_fixture(line.split_whitespace().nth(2).unwrap());
    crate::metadata_command::decode_metadata_command_envelope(
        &bytes,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap()
}

fn framed(command: &[u8]) -> Vec<u8> {
    let mut bytes = b"stg-abnd\x02\x00".to_vec();
    bytes.extend_from_slice(&checksum::crc64::checksum(command).to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(command.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(command);
    bytes
}

fn complete(source: &MetadataCommandEnvelope) -> Vec<u8> {
    // Codec-only helper intentionally encodes unrelated payloads to exercise
    // source-kind rejection independently of its constructor.
    crate::metadata_command::context_carrier::encode(
        source,
        &[1, 0, 0],
        &crate::node_runtime::CommandContextCodecAuthority::new_for_test(),
    )
    .unwrap()
}

#[test]
fn staging_abandonment_source_v2_command_v14_context_v1_composite() {
    let source = specimen(24);
    let captured = PartFinalizationSource::capture_for_test(&source).unwrap();
    let bytes = captured.encode();
    assert_eq!(
        PartFinalizationSource::decode_for_test(&bytes).unwrap(),
        captured
    );
    assert_eq!(&bytes[HEADER_BYTES..], complete(&source));
    let expected = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_abandonment_source_v2_command_v14_context_v1.hex"
    ));
    assert_eq!(bytes, expected);
    assert_eq!(bytes.len(), 1731);
    assert_eq!(&bytes[..10], b"stg-abnd\x02\x00");
    assert_eq!(
        u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
        0x9a9b_e703_ae74_7975
    );
    assert_eq!(
        PartFinalizationSource::decode_for_test(&expected)
            .unwrap()
            .encode(),
        expected
    );
}

#[test]
fn staging_abandonment_source_rejects_frozen_v1_command_v13_composite() {
    let source = specimen(24);
    assert!(matches!(
        source.payload(),
        MetadataCommandPayload::CommitStreamPart(_)
    ));
    let literal = include_str!("testdata/staging_abandonment_source_v1_command_v13.hex");
    let bytes = crate::tests::frozen_hex_fixture(literal);
    assert!(matches!(
        PartFinalizationSource::decode_for_test(&bytes),
        Err(
            super::super::super::abandonment::source::SourceError::Encoding("source magic/version")
        )
    ));
    assert_eq!(&bytes[HEADER_BYTES..], source.command_bytes());
    let tombstone = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_abandonment_tombstone_v2_command_v13.hex"
    ));
    assert_eq!(tombstone.len(), 99);
    assert_eq!(source.abandoned_log_bytes(), tombstone);
    assert_eq!(&tombstone[67..], source.command_sha256());
}

#[test]
fn staging_abandonment_source_v2_rejects_payload_draft_tombstone_without_mutation() {
    for compact in [false, true] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        // Construct a genuinely linked v13 source tombstone, not a malformed
        // row, and prove the current validator accepts it. The v2 source must
        // nevertheless reject it because the complete v14 identity differs.
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        for index in 1..source.id().log_index().get() {
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    source.id().cluster_epoch(),
                    source.id().pg_id(),
                    MetadataCommandLogIndex::new(index).unwrap(),
                ),
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    payload(&source).bucket.clone(),
                    payload(&source).key.clone(),
                    VersionId::from_u64(100 + index),
                )),
            );
            pg.record_metadata_command_abandoned(0, &command).unwrap();
        }
        pg.record_metadata_command_abandoned(0, &source).unwrap();
        if compact {
            pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                .unwrap();
            pg.compact_metadata_command_log(source.id().cluster_epoch())
                .unwrap();
            assert_eq!(
                pg.metadata_command_log_stats(source.id().cluster_epoch())
                    .unwrap()
                    .retained_entries,
                0
            );
        }
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            pg.metadata_command_startup_disposition(0, &source).unwrap(),
            crate::pg_store::MetadataCommandStartupDisposition::Abandoned { .. }
        ));
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let error = apply(&pg, &source, &follow_up).unwrap_err();
        assert!(
            matches!(error, AbandonmentError::Store(crate::StoreError::MetadataCommandLogConflict {
            node_id: 0, pg_id: actual_pg, cluster_epoch, log_index,
        }) if actual_pg == pg_id && cluster_epoch == source.id().cluster_epoch() && log_index == source.id().log_index().get())
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_abandonment_source_v2_rejects_nested_aborts_without_recursive_context_decoding() {
    let finalizer = specimen(24);
    let abort = specimen(23);
    assert!(matches!(
        abort.payload(),
        MetadataCommandPayload::AbortStreamUpload(_)
    ));
    let mut source = PartFinalizationSource::capture_for_test(&finalizer)
        .unwrap()
        .encode();
    // Each source carrier is well-framed/resealed and fits the bound. Only the
    // command-kind restriction keeps this from recursively decoding contexts.
    for _ in 0..2_000 {
        let mut field = vec![1, 0, 3];
        field.extend_from_slice(&u32::try_from(source.len()).unwrap().to_le_bytes());
        field.extend_from_slice(&source);
        let command = crate::metadata_command::context_carrier::encode(
            &abort,
            &field,
            &crate::node_runtime::CommandContextCodecAuthority::new_for_test(),
        )
        .unwrap();
        source = framed(&command);
    }
    assert!(source.len() < 2 * 1024 * 1024 + 25);
    assert!(matches!(
        PartFinalizationSource::decode_for_test(&source),
        Err(
            super::super::super::abandonment::source::SourceError::Encoding(
                "invalid source context"
            )
        )
    ));
}

#[test]
fn staging_abandonment_source_rejects_every_truncated_prefix_and_corrupt_framing() {
    let bytes = PartFinalizationSource::capture_for_test(&specimen(24))
        .unwrap()
        .encode();
    for end in 0..bytes.len() {
        assert!(
            PartFinalizationSource::decode_for_test(&bytes[..end]).is_err(),
            "prefix {end}"
        );
    }
    for offset in [0, 7, 8, 9, 10, 17, 18, 21, HEADER_BYTES, bytes.len() - 1] {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 0x80;
        assert!(
            PartFinalizationSource::decode_for_test(&corrupt).is_err(),
            "offset {offset}"
        );
    }
    for version in [0_u16, 1, 3, u16::MAX] {
        let mut corrupt = bytes.clone();
        corrupt[8..10].copy_from_slice(&version.to_le_bytes());
        assert!(
            PartFinalizationSource::decode_for_test(&corrupt).is_err(),
            "version {version}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(PartFinalizationSource::decode_for_test(&trailing).is_err());
    let mut huge_length = bytes;
    huge_length[18..22].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(PartFinalizationSource::decode_for_test(&huge_length).is_err());
}

#[test]
fn staging_abandonment_source_rejects_other_commands_and_resealed_nested_versions() {
    // Check all unrelated variants represented in the existing command corpus,
    // including roots and ordinary cleanup. A carrier can never nest itself.
    for index in 0..30 {
        let source = specimen(index);
        let captured = PartFinalizationSource::capture_for_test(&source);
        let decoded = PartFinalizationSource::decode_for_test(&framed(&complete(&source)));
        if index == 24 {
            assert_eq!(captured.unwrap(), decoded.unwrap());
        } else {
            assert!(captured.is_err(), "capture {index}");
            assert!(decoded.is_err(), "decode {index}");
        }
    }
    let source = specimen(24);
    for version in [0_u16, 12, 13, 15, u16::MAX] {
        let mut command = complete(&source);
        // 4-byte magic length, then the 23-byte command magic.
        command[27..29].copy_from_slice(&version.to_le_bytes());
        let error = PartFinalizationSource::decode_for_test(&framed(&command)).unwrap_err();
        assert!(matches!(error,
            super::super::super::abandonment::source::SourceError::Command(
                MetadataCommandFormatError::UnsupportedVersion {
                    format: MetadataCommandRecordFormat::Applied,
                    version: actual,
                }) if actual == version));
    }
    assert!(
        PartFinalizationSource::decode_for_test(&framed(&source.abandoned_log_bytes())).is_err()
    );
    assert!(PartFinalizationSource::decode_for_test(&framed(b"stg-abnd\x01\x00")).is_err());
}

#[test]
fn staging_abandonment_source_admits_maximum_command_plus_carrier_overhead() {
    let source = specimen(24);
    let MetadataCommandPayload::CommitStreamPart(mut payload) = source.payload().clone() else {
        unreachable!()
    };
    // Pure codec capacity fixture, not publishable object metadata. Use an
    // existing byte vector to reach the exact command cap without allocating
    // millions of segments or weakening the command decoder's vector limits.
    let original_len = source.command_bytes().len();
    payload.part.etag.resize(
        payload.part.etag.len() + MAX_METADATA_COMMAND_BYTES - original_len,
        0x41,
    );
    let at_cap = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(payload.clone()),
    );
    assert_eq!(at_cap.command_bytes().len(), MAX_METADATA_COMMAND_BYTES);
    let captured = PartFinalizationSource::capture_for_test(&at_cap).unwrap();
    let bytes = captured.encode();
    assert_eq!(bytes.len(), MAX_METADATA_COMMAND_BYTES + 3 + HEADER_BYTES);
    assert_eq!(
        PartFinalizationSource::decode_for_test(&bytes).unwrap(),
        captured
    );
    payload.part.etag.push(0x41);
    let over = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(payload),
    );
    assert!(PartFinalizationSource::capture_for_test(&over).is_err());
    assert!(PartFinalizationSource::decode_for_test(&framed(&over.command_bytes())).is_err());
}

#[test]
fn staging_abandonment_source_decoding_cannot_authorize_absent_or_substituted_tombstone() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        let bytes = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .encode();
        let carrier = PartFinalizationSource::decode_for_test(&bytes).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply_carrier(&pg, &carrier, &follow_up).is_err());
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);

        terminal(&pg, &source, true);
        // Same ID, same subject, independently resealed carrier and command:
        // only the complete original source is authenticated by the tombstone.
        let mut other = payload(&source).clone();
        other.bucket_write_reservation.owner_token.push('x');
        let other = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(other)),
        );
        let other = PartFinalizationSource::decode_for_test(&framed(&complete(&other))).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply_carrier(&pg, &other, &follow_up).is_err());
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(
            apply_carrier(&pg, &carrier, &follow_up).unwrap(),
            AbandonmentOutcome::ClosedAndRetained
        );
        assert_closed(&pg, &owner, &source);
    }
}
