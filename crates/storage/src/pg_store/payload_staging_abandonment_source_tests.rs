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
fn crossed_proof_with_equal_crc(
    source: &MetadataCommandEnvelope,
    field: &str,
) -> MetadataCommandEnvelope {
    let mut crossed = payload(source).clone();
    match field {
        "reservation" => crossed.bucket_write_reservation.reservation_id.push('x'),
        "owner" => crossed.bucket_write_reservation.owner_token.push('x'),
        _ => unreachable!(),
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
                let carrier = PartFinalizationSource::capture(&crossed).unwrap();
                let carrier = PartFinalizationSource::decode(&carrier.encode()).unwrap();
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
                let result =
                    carrier.apply_closure_in_open_transaction(&pg, &topology(), 0, &follow_up);
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
    let mut bytes = b"stg-abnd\x01\x00".to_vec();
    bytes.extend_from_slice(&checksum::crc64::checksum(command).to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(command.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(command);
    bytes
}

#[test]
fn staging_abandonment_source_frozen_v1_command_v13_composite() {
    let source = specimen(24);
    assert!(matches!(
        source.payload(),
        MetadataCommandPayload::CommitStreamPart(_)
    ));
    let literal = include_str!("testdata/staging_abandonment_source_v1_command_v13.hex");
    let bytes = crate::tests::frozen_hex_fixture(literal);
    let captured = PartFinalizationSource::capture(&source).unwrap();
    assert_eq!(captured.encode(), bytes);
    assert_eq!(PartFinalizationSource::decode(&bytes).unwrap(), captured);
    assert_eq!(&bytes[HEADER_BYTES..], source.command_bytes());
    let tombstone = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_abandonment_tombstone_v2_command_v13.hex"
    ));
    assert_eq!(tombstone.len(), 99);
    assert_eq!(source.abandoned_log_bytes(), tombstone);
    assert_eq!(&tombstone[67..], source.command_sha256());
}

#[test]
fn staging_abandonment_source_rejects_every_truncated_prefix_and_corrupt_framing() {
    let bytes = PartFinalizationSource::capture(&specimen(24))
        .unwrap()
        .encode();
    for end in 0..bytes.len() {
        assert!(
            PartFinalizationSource::decode(&bytes[..end]).is_err(),
            "prefix {end}"
        );
    }
    for offset in [0, 7, 8, 9, 10, 17, 18, 21, HEADER_BYTES, bytes.len() - 1] {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 0x80;
        assert!(
            PartFinalizationSource::decode(&corrupt).is_err(),
            "offset {offset}"
        );
    }
    for version in [0_u16, 2, u16::MAX] {
        let mut corrupt = bytes.clone();
        corrupt[8..10].copy_from_slice(&version.to_le_bytes());
        assert!(
            PartFinalizationSource::decode(&corrupt).is_err(),
            "version {version}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(PartFinalizationSource::decode(&trailing).is_err());
    let mut huge_length = bytes;
    huge_length[18..22].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(PartFinalizationSource::decode(&huge_length).is_err());
}

#[test]
fn staging_abandonment_source_rejects_other_commands_and_resealed_nested_versions() {
    // Check all unrelated variants represented in the existing command corpus,
    // including roots and ordinary cleanup. A carrier can never nest itself.
    for index in 0..30 {
        let source = specimen(index);
        let captured = PartFinalizationSource::capture(&source);
        let decoded = PartFinalizationSource::decode(&framed(&source.command_bytes()));
        if index == 24 {
            assert_eq!(captured.unwrap(), decoded.unwrap());
        } else {
            assert!(captured.is_err(), "capture {index}");
            assert!(decoded.is_err(), "decode {index}");
        }
    }
    let source = specimen(24);
    for version in [0_u16, 12, 14, u16::MAX] {
        let mut command = source.command_bytes();
        // 4-byte magic length, then the 23-byte command magic.
        command[27..29].copy_from_slice(&version.to_le_bytes());
        let error = PartFinalizationSource::decode(&framed(&command)).unwrap_err();
        assert!(matches!(error,
            super::super::super::abandonment::source::SourceError::Command(
                MetadataCommandFormatError::UnsupportedVersion {
                    format: MetadataCommandRecordFormat::Applied,
                    version: actual,
                }) if actual == version));
    }
    assert!(PartFinalizationSource::decode(&framed(&source.abandoned_log_bytes())).is_err());
    assert!(PartFinalizationSource::decode(&framed(b"stg-abnd\x01\x00")).is_err());
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
    let captured = PartFinalizationSource::capture(&at_cap).unwrap();
    let bytes = captured.encode();
    assert_eq!(bytes.len(), MAX_METADATA_COMMAND_BYTES + HEADER_BYTES);
    assert_eq!(PartFinalizationSource::decode(&bytes).unwrap(), captured);
    payload.part.etag.push(0x41);
    let over = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(payload),
    );
    assert!(PartFinalizationSource::capture(&over).is_err());
    assert!(PartFinalizationSource::decode(&framed(&over.command_bytes())).is_err());
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
        let bytes = PartFinalizationSource::capture(&source).unwrap().encode();
        let carrier = PartFinalizationSource::decode(&bytes).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(carrier
            .apply_closure_in_open_transaction(&pg, &topology(), 0, &follow_up)
            .is_err());
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
        let other = PartFinalizationSource::decode(&framed(&other.command_bytes())).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(other
            .apply_closure_in_open_transaction(&pg, &topology(), 0, &follow_up)
            .is_err());
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(
            carrier
                .apply_closure_in_open_transaction(&pg, &topology(), 0, &follow_up)
                .unwrap(),
            AbandonmentOutcome::ClosedAndRetained
        );
        assert_closed(&pg, &owner, &source);
    }
}
