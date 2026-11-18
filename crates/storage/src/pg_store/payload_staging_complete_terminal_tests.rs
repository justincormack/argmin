// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-identity integration with the real log/receipt/checkpoint validator.
//! Metadata-only fixtures; no production recovery admission or custody switch.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::command_context::ContextualCommand;
use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::metadata_command::{
    CommitStreamPartCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{MetadataCommandStartupDisposition, PgStore};
use crate::{StoreError, VersionId};

fn ordinary(command: &MetadataCommandEnvelope) -> ContextualCommand {
    ContextualCommand::decode_field(command.clone(), &[1, 0, 0], &topology()).unwrap()
}

fn unrelated(source: &MetadataCommandEnvelope, index: u64) -> MetadataCommandEnvelope {
    let MetadataCommandPayload::CommitStreamPart(part) = source.payload() else {
        unreachable!()
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            source.id().cluster_epoch(),
            source.id().pg_id(),
            MetadataCommandLogIndex::new(index).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            part.bucket.clone(),
            part.key.clone(),
            VersionId::from_u64(100 + index),
        )),
    )
}

fn setup(
    count: usize,
) -> (
    test_util::TempDir,
    PgStore,
    StagingOwnerRecord,
    MetadataCommandEnvelope,
) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, count);
    let source = part::command(&pg, &owner);
    // Anchor the metadata-only fixture, then build every prefix link using the
    // production transaction/log machinery, not raw authenticated-row injection.
    pg.refresh_metadata_command_state_digest().unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    for index in 1..source.id().log_index().get() {
        pg.record_metadata_command_abandoned(0, &unrelated(&source, index))
            .unwrap();
    }
    (dir, pg, owner, source)
}

fn inspect(
    pg: &PgStore,
    context: &ContextualCommand,
) -> Result<MetadataCommandStartupDisposition, StoreError> {
    let snapshot = pg.command_build_snapshot().unwrap();
    let before = introduction::state(pg);
    let changes = pg.conn.total_changes();
    let prefix = pg.metadata_command_replica_state().unwrap();
    let result = context
        .encode_command()
        .unwrap()
        .inspect_terminal_in_open_transaction(pg, 0);
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert_eq!(introduction::state(pg), before);
    snapshot.finish().unwrap();
    result
}

fn assert_conflict<T: std::fmt::Debug>(result: Result<T, StoreError>, id: MetadataCommandId) {
    assert!(
        matches!(&result, Err(StoreError::MetadataCommandLogConflict { node_id: 0, pg_id, cluster_epoch, log_index })
        if *pg_id == id.pg_id().get() && *cluster_epoch == id.cluster_epoch() && *log_index == id.log_index().get()),
        "{result:?}"
    );
}

fn assert_abandoned(pg: &PgStore, context: &ContextualCommand) {
    let result = inspect(pg, context).unwrap();
    assert!(
        matches!(result, MetadataCommandStartupDisposition::Abandoned { .. }),
        "{result:?}"
    );
}

fn assert_rejected_abandonment(pg: &PgStore, context: &ContextualCommand) {
    let state = introduction::state(pg);
    let changes = pg.conn.total_changes();
    let prefix = pg.metadata_command_replica_state().unwrap();
    let result = pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap());
    assert_conflict(result, context.id());
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert_eq!(introduction::state(pg), state);
}

fn reopen(pg: PgStore, dir: &test_util::TempDir) -> PgStore {
    let id = pg.pg_id;
    drop(pg);
    PgStore::open(dir.path(), id).unwrap()
}

fn compact(pg: &PgStore, source: &MetadataCommandEnvelope) {
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

#[test]
fn staging_complete_terminal_requires_transaction_and_exact_pg_before_inspection() {
    let (_dir, pg, _owner, source) = setup(0);
    let context = ordinary(&source);
    let encoded = context.encode_command().unwrap();
    let changes = pg.conn.total_changes();
    assert!(matches!(
        encoded.inspect_terminal_in_open_transaction(&pg, 0),
        Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "inspect complete command terminal identity outside transaction"
        })
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert!(matches!(
        inspect(&pg, &context).unwrap(),
        MetadataCommandStartupDisposition::Absent
    ));
    let foreign_dir = test_util::tempdir();
    let foreign = PgStore::open(foreign_dir.path(), source.id().pg_id().get() ^ 1).unwrap();
    let snapshot = foreign.command_build_snapshot().unwrap();
    let changes = foreign.conn.total_changes();
    assert!(
        matches!(encoded.inspect_terminal_in_open_transaction(&foreign, 0), Err(StoreError::MetadataCommandWrongPg { command_pg_id, target_pg_id, .. })
        if command_pg_id == source.id().pg_id().get() && target_pg_id == foreign.pg_id)
    );
    assert_eq!(foreign.conn.total_changes(), changes);
    snapshot.finish().unwrap();
}

#[test]
fn staging_complete_terminal_identity_survives_reopen_compaction_and_epoch_transition() {
    for count in [0, 2] {
        let (dir, pg, _owner, source) = setup(count);
        let context = ordinary(&source);
        let encoded = context.encode_command().unwrap();
        let digest = pg.metadata_state_digest().unwrap();
        let prefix = pg
            .test_record_complete_command_abandoned(0, &encoded)
            .unwrap();
        assert_eq!(prefix.applied_log_index, source.id().log_index().get());
        assert_eq!(
            prefix.state_digest.value(),
            digest,
            "abandonment does not mutate materialized state"
        );
        assert_abandoned(&pg, &context);
        assert_eq!(
            pg.test_record_complete_command_abandoned(0, &encoded)
                .unwrap(),
            prefix
        );
        // Same body and ID, but the old draft's v13 identity cannot stand in for
        // the checksummed v14 command with its required ordinary context.
        assert_conflict(
            pg.metadata_command_startup_disposition(0, &source),
            source.id(),
        );
        let pg = reopen(pg, &dir);
        assert_abandoned(&pg, &context);
        compact(&pg, &source);
        let pg = reopen(pg, &dir);
        assert_abandoned(&pg, &context);
        assert_eq!(
            pg.test_record_complete_command_abandoned(0, &encoded)
                .unwrap(),
            prefix
        );
        assert_conflict(
            pg.metadata_command_startup_disposition(0, &source),
            source.id(),
        );
        let next_epoch = ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap();
        let advanced = pg
            .initialize_metadata_transfer_matching_state(
                0,
                next_epoch,
                0,
                MetadataCommandLogHash::genesis(),
                prefix.state_digest,
            )
            .unwrap();
        assert_eq!(advanced.applied_log_index, 0);
        let pg = reopen(pg, &dir);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), advanced);
        assert_abandoned(&pg, &context);
    }
}

#[test]
fn staging_complete_terminal_does_not_accept_a_tombstone_for_the_payload_draft() {
    let (dir, pg, _owner, source) = setup(2);
    let context = ordinary(&source);
    pg.record_metadata_command_abandoned(0, &source).unwrap();
    assert!(matches!(
        pg.metadata_command_startup_disposition(0, &source).unwrap(),
        MetadataCommandStartupDisposition::Abandoned { .. }
    ));
    assert_conflict(inspect(&pg, &context), source.id());
    compact(&pg, &source);
    let pg = reopen(pg, &dir);
    assert_conflict(inspect(&pg, &context), source.id());
}

#[test]
fn staging_complete_terminal_binds_context_even_when_abort_payload_and_id_are_equal() {
    let (dir, pg, owner, source) = setup(2);
    pg.record_metadata_command_abandoned(0, &source).unwrap();
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let ordinary_abort = ordinary(&cleanup);
    let bound = ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap();
    let encoded = bound.encode_command().unwrap();
    assert_eq!(ordinary_abort.id(), encoded.id());
    assert_ne!(
        ordinary_abort.encode_command().unwrap().command_sha256(),
        encoded.command_sha256()
    );
    pg.test_record_complete_command_abandoned(0, &encoded)
        .unwrap();
    assert_abandoned(&pg, &bound);
    assert_conflict(inspect(&pg, &ordinary_abort), cleanup.id());
    compact(&pg, &source);
    let pg = reopen(pg, &dir);
    assert_abandoned(&pg, &bound);
    assert_conflict(inspect(&pg, &ordinary_abort), cleanup.id());
}

// Preserve the COMPLETE carrier CRC while substituting a reservation owner.
// Compensation is calculated over v14 + required context, not the v13 draft.
fn crossed_with_equal_complete_crc(source: &MetadataCommandEnvelope) -> ContextualCommand {
    let MetadataCommandPayload::CommitStreamPart(mut crossed) = source.payload().clone() else {
        unreachable!()
    };
    crossed.bucket_write_reservation.owner_token.push('x');
    let context = |part: CommitStreamPartCommand| {
        ordinary(&MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(part)),
        ))
    };
    let original = ordinary(source).encode_command().unwrap().checksum_crc64();
    let base = context((*crossed).clone())
        .encode_command()
        .unwrap()
        .checksum_crc64();
    let mut basis = [(0_u64, 0_u64); 64];
    for bit in 0..64 {
        let mut probe = (*crossed).clone();
        probe.part.etag[bit / 8] ^= 1 << (bit % 8);
        let mut delta = context(probe).encode_command().unwrap().checksum_crc64() ^ base;
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
    let mut delta = original ^ base;
    let mut patch = 0_u64;
    while delta != 0 {
        let pivot = 63 - delta.leading_zeros() as usize;
        delta ^= basis[pivot].0;
        patch ^= basis[pivot].1;
    }
    for (byte, correction) in crossed.part.etag.iter_mut().zip(patch.to_le_bytes()) {
        *byte ^= correction;
    }
    let crossed = context(*crossed);
    let bytes = crossed.encode_command().unwrap();
    assert_eq!(bytes.checksum_crc64(), original);
    assert_ne!(
        bytes.command_sha256(),
        ordinary(source).encode_command().unwrap().command_sha256()
    );
    ContextualCommand::decode_command(bytes.bytes(), bytes.checksum_crc64(), &topology()).unwrap()
}

#[test]
fn staging_complete_terminal_rejects_crc_collision_in_retained_and_compacted_evidence() {
    for count in [0, 2] {
        let (dir, pg, _owner, source) = setup(count);
        let context = ordinary(&source);
        let crossed = crossed_with_equal_complete_crc(&source);
        pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
            .unwrap();
        assert_abandoned(&pg, &context);
        assert_conflict(inspect(&pg, &crossed), source.id());
        assert_rejected_abandonment(&pg, &crossed);
        compact(&pg, &source);
        let pg = reopen(pg, &dir);
        assert_abandoned(&pg, &context);
        assert_conflict(inspect(&pg, &crossed), source.id());
        assert_rejected_abandonment(&pg, &crossed);
        assert_abandoned(&pg, &context);
    }
}

#[test]
fn staging_complete_terminal_authenticates_pruned_prefix_against_later_checkpoint() {
    for next_epoch in [false, true] {
        let (dir, pg, _owner, source) = setup(2);
        let context = ordinary(&source);
        pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
            .unwrap();
        pg.record_metadata_command_abandoned(
            0,
            &unrelated(&source, source.id().log_index().get() + 1),
        )
        .unwrap();
        let checkpoint = pg
            .record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
            .unwrap();
        assert_eq!(
            checkpoint.applied_log_index,
            source.id().log_index().get() + 1
        );
        pg.compact_metadata_command_log_with_receipt_retention(source.id().cluster_epoch(), 2)
            .unwrap();
        let (count, first): (u64, u64) = pg
            .conn
            .query_row(
                "SELECT count(*), min(log_index) FROM metadata_command_terminal_receipts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((count, first), (2, source.id().log_index().get()));
        assert!(first > 1);
        assert_eq!(
            pg.metadata_command_log_stats(source.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        assert!(pg
            .metadata_command_checkpoint_candidates(
                source.id().cluster_epoch(),
                source.id().log_index().get(),
                8
            )
            .unwrap()
            .is_empty());
        if next_epoch {
            pg.initialize_metadata_transfer_matching_state(
                0,
                ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap(),
                0,
                MetadataCommandLogHash::genesis(),
                checkpoint.state_digest,
            )
            .unwrap();
        }
        let pg = reopen(pg, &dir);
        assert_abandoned(&pg, &context);
        assert_conflict(
            inspect(&pg, &crossed_with_equal_complete_crc(&source)),
            source.id(),
        );
        // A surviving exact-source receipt alone is insufficient without the
        // authenticated contiguous suffix linking it to the later checkpoint.
        pg.conn
            .execute(
                "DELETE FROM metadata_command_terminal_receipts WHERE log_index = ?1",
                [source.id().log_index().get() + 1],
            )
            .unwrap();
        assert_conflict(inspect(&pg, &context), source.id());
    }
}

#[test]
fn staging_complete_terminal_v2_tombstone_pins_complete_v14_source_identity() {
    let (dir, pg, _owner, source) = setup(2);
    let context = ordinary(&source);
    let encoded = context.encode_command().unwrap();
    pg.test_record_complete_command_abandoned(0, &encoded)
        .unwrap();
    let expected = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_terminal_tombstone_v2_command_v14_context_v1.hex"
    ));
    assert_eq!(expected.len(), 99);
    assert_ne!(expected, source.abandoned_log_bytes());
    assert_eq!(&expected[67..], encoded.command_sha256());
    // Independent literals pin both the outer record checksum and the complete
    // source checksum carried inside it. Never rebuild predecessor evidence.
    assert_eq!(encoded.checksum_crc64(), 0xfd57_f06a_0af3_b590);
    assert_eq!(checksum::crc64::checksum(&expected), 0xb648_b267_a829_08d3);
    let check_stored = |pg: &PgStore| {
        let (bytes, crc): (Vec<u8>, i64) = pg
            .conn
            .query_row(
                "SELECT command_bytes, command_checksum FROM metadata_command_log \
                 WHERE cluster_epoch = ?1 AND log_index = ?2",
                rusqlite::params![
                    source.id().cluster_epoch().get(),
                    source.id().log_index().get()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(bytes, expected);
        assert_eq!(crc as u64, 0xb648_b267_a829_08d3);
        assert_abandoned(pg, &context);
    };
    check_stored(&pg);
    check_stored(&reopen(pg, &dir));
}
