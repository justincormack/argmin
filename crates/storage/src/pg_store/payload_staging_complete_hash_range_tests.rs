// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Hash observations validate complete rows without retaining their payloads.
//! These are metadata-only owner-local fixtures, not recovery authorization.

use super::complete_log_range::{apply, index, ordinary};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    metadata_command_log_hash, BucketSubresourceMutation, MetadataCommandEnvelope,
    MetadataCommandLogHashRangeEntry, MetadataCommandPayload, PutBucketSubresourceCommand,
};
use crate::node_runtime::pg_store::{command_log::CompleteLogReadError, PgStore};
use crate::StoreError;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn hashes(
    pg: &PgStore,
    epoch: ClusterEpoch,
    first: u64,
    last: u64,
) -> Result<Vec<MetadataCommandLogHashRangeEntry>, CompleteLogReadError> {
    pg.retained_complete_metadata_command_log_hashes(
        0,
        epoch,
        index(first),
        index(last),
        &topology(),
    )
}

#[test]
fn staging_complete_hash_range_has_its_own_bound_and_rejects_foreign_scope() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let epoch = context.id().cluster_epoch();
    let limit = crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES;
    let writes = pg.conn.total_changes();
    let before = introduction::state(&pg);
    let entries = hashes(&pg, epoch, 1, limit).unwrap();
    assert_eq!(entries.len() as u64, context.id().log_index().get());
    assert_eq!(
        entries.last().unwrap().log_hash,
        pg.metadata_command_replica_state()
            .unwrap()
            .applied_log_hash
    );
    assert!(
        pg.retained_complete_metadata_command_log_entries(
            0,
            epoch,
            index(1),
            index(limit),
            &topology(),
        )
        .is_err(),
        "hash bound must not enlarge full-payload pages"
    );
    for (first, last) in [(2, 1), (1, limit + 1), (u64::MAX, u64::MAX)] {
        assert!(matches!(
            hashes(&pg, epoch, first, last),
            Err(CompleteLogReadError::Store(
                StoreError::RouteCapabilitySubjectMismatch { .. }
            ))
        ));
    }
    assert!(hashes(&pg, ClusterEpoch::new(u64::MAX).unwrap(), 1, 1).is_err());
    for foreign in [
        PgTopology::new(&[99]).unwrap(),
        PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap(),
    ] {
        assert!(pg
            .retained_complete_metadata_command_log_hashes(
                0,
                epoch,
                index(1),
                context.id().log_index(),
                &foreign,
            )
            .is_err());
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let error = hashes(&pg, epoch, 1, limit).unwrap_err();
    assert!(
        error.to_string().contains("requires its own read snapshot"),
        "{error}"
    );
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_complete_hash_range_checks_genesis_and_links_across_entry_page_boundaries() {
    let _clock = test_time_override_guard(2_000);
    let boundary = crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES;
    for damaged_index in [1, boundary + 1] {
        let dir = test_util::tempdir();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let pg = PgStore::open(dir.path(), pg_id.get()).unwrap();
        let epoch = ClusterEpoch::new(7).unwrap();
        for position in 1..=boundary + 2 {
            let command = ordinary(MetadataCommandId::new(epoch, pg_id, index(position)));
            pg.record_metadata_command_abandoned(0, &command).unwrap();
        }
        let expected = hashes(&pg, epoch, 1, boundary + 2).unwrap();
        assert_eq!(expected.len() as u64, boundary + 2);
        assert_eq!(expected[0].previous_log_hash.value(), 0);
        for pair in expected.windows(2) {
            assert_eq!(pair[0].log_hash, pair[1].previous_log_hash);
        }
        let (previous, checksum): (i64, i64) = pg.conn.query_row(
            "SELECT previous_log_hash, command_checksum FROM metadata_command_log WHERE log_index = ?1",
            [damaged_index as i64], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        let previous = previous as u64 ^ 1;
        let hash = metadata_command_log_hash(
            epoch,
            pg_id,
            index(damaged_index),
            previous,
            checksum as u64,
        );
        pg.conn.execute(
            "UPDATE metadata_command_log SET previous_log_hash = ?1, log_hash = ?2 WHERE log_index = ?3",
            rusqlite::params![previous as i64, hash.value() as i64, damaged_index as i64],
        ).unwrap();
        let writes = pg.conn.total_changes();
        assert!(matches!(hashes(&pg, epoch, 1, boundary + 2), Err(
            CompleteLogReadError::Store(StoreError::MetadataCommandLogConflict { log_index, .. })
        ) if log_index == damaged_index));
        assert_eq!(pg.conn.total_changes(), writes);
        if damaged_index == 1 {
            assert!(
                hashes(&pg, epoch, 1, 1).is_err(),
                "genesis cannot borrow an external predecessor"
            );
        } else {
            assert_eq!(
                hashes(&pg, epoch, damaged_index, damaged_index)
                    .unwrap()
                    .len(),
                1,
                "a non-genesis isolated hash is self-consistent, not a whole-prefix proof"
            );
            pg.conn
                .execute(
                    "DELETE FROM metadata_command_log WHERE log_index IN (?1, ?2)",
                    rusqlite::params![boundary as i64, (boundary + 2) as i64],
                )
                .unwrap();
            let sparse = hashes(&pg, epoch, 1, boundary + 2).unwrap();
            assert_eq!(
                sparse
                    .iter()
                    .map(|entry| entry.log_index)
                    .collect::<Vec<_>>(),
                (1..boundary).chain([damaged_index]).collect::<Vec<_>>(),
                "missing retained rows must stay gaps, not be synthesized from receipts"
            );
        }
    }
}

#[test]
fn staging_complete_log_projections_pin_one_snapshot_across_concurrent_row_deletion() {
    let _clock = test_time_override_guard(2_000);
    for hash_only in [false, true] {
        let (dir, mut pg, context) = complete_application::setup(5);
        apply(&pg, &context);
        let id = context.id();
        assert!(id.log_index().get() > 1);
        let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
        let selects = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&selects);
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if sql.starts_with("SELECT command_checksum, command_bytes, abandoned,")
                            && observed.fetch_add(1, Ordering::SeqCst) == 1
                        {
                            assert_eq!(
                                other
                                    .conn
                                    .execute(
                                        "DELETE FROM metadata_command_log WHERE log_index = ?1",
                                        [id.log_index().get() as i64]
                                    )
                                    .unwrap(),
                                1
                            );
                        }
                    }
                }),
            )
            .unwrap();
        let writes = pg.conn.total_changes();
        let positions: Vec<_> = if hash_only {
            hashes(&pg, id.cluster_epoch(), 1, id.log_index().get())
                .unwrap()
                .into_iter()
                .map(|entry| entry.log_index)
                .collect()
        } else {
            pg.retained_complete_metadata_command_log_entries(
                0,
                id.cluster_epoch(),
                index(1),
                id.log_index(),
                &topology(),
            )
            .unwrap()
            .into_iter()
            .map(|entry| entry.id().log_index().get())
            .collect()
        };
        assert_eq!(positions, (1..=id.log_index().get()).collect::<Vec<_>>());
        assert_eq!(
            selects.load(Ordering::SeqCst),
            id.log_index().get() as usize
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(
            hashes(
                &pg,
                id.cluster_epoch(),
                id.log_index().get(),
                id.log_index().get()
            )
            .unwrap()
            .is_empty(),
            "next snapshot must see the committed concurrent deletion"
        );
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_complete_hash_range_retires_unconfirmed_snapshot_teardown() {
    let _clock = test_time_override_guard(2_000);
    for damaged in [false, true] {
        let (dir, pg, context) = complete_application::setup(5);
        apply(&pg, &context);
        let id = context.id();
        if damaged {
            pg.conn.execute("UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = ?1",
                [id.log_index().get() as i64]).unwrap();
        }
        let writes = pg.conn.total_changes();
        pg.fail_next_snapshot_rollback.set(true);
        let error = hashes(&pg, id.cluster_epoch(), 1, id.log_index().get()).unwrap_err();
        if damaged {
            assert!(matches!(
                error,
                CompleteLogReadError::Store(StoreError::MetadataCommandLogChecksumMismatch { .. })
            ));
        } else {
            assert!(
                error
                    .to_string()
                    .contains("injected read-snapshot rollback failure"),
                "{error}"
            );
        }
        assert!(
            !pg.conn.is_autocommit(),
            "must not hide uncertainty with a Drop retry"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.require_usable_connection().is_err());
        assert!(hashes(&pg, id.cluster_epoch(), 1, 1).is_err());
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(pg.require_usable_connection().is_err());
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(hashes(&pg, id.cluster_epoch(), 1, 1).unwrap().len(), 1);
    }
}

fn sized_complete_command(id: MetadataCommandId, size: usize) -> Vec<u8> {
    // Canonical metadata-only log specimens, not an HTTP size-limit or apply
    // fixture. A length-controlled subresource isolates reader work admission.
    let encode = |body| {
        let command = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                fixture(StagingTarget::DirectPut).subject.bucket,
                BucketSubresourceMutation::PutCors(body),
                1,
            )),
        );
        ContextualCommand::decode_field(command, &[1, 0, 0], &topology())
            .unwrap()
            .encode_command()
            .unwrap()
            .bytes()
            .to_vec()
    };
    let xml = "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let overhead = encode(xml.to_owned()).len();
    let bytes = encode(format!("{}{}", " ".repeat(size - overhead), xml));
    assert_eq!(bytes.len(), size);
    bytes
}

pub(super) fn seed_log_row(pg: &PgStore, id: MetadataCommandId, size: usize, previous: u64) -> u64 {
    let bytes = sized_complete_command(id, size);
    let checksum = checksum::crc64::checksum(&bytes);
    let hash = metadata_command_log_hash(
        id.cluster_epoch(),
        id.pg_id(),
        id.log_index(),
        previous,
        checksum,
    )
    .value();
    pg.conn.execute(
        "INSERT INTO metadata_command_log \
         (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, 0, 0)",
        rusqlite::params![id.cluster_epoch().get() as i64, id.pg_id().get(), id.log_index().get() as i64,
            checksum as i64, bytes, previous as i64, hash as i64],
    ).unwrap();
    hash
}

pub(super) fn count_payload_reads(pg: &mut PgStore) -> Arc<AtomicUsize> {
    let reads = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&reads);
    pg.conn
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(_, sql) = event {
                    if sql.starts_with("SELECT command_checksum, command_bytes, abandoned,") {
                        captured.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }),
        )
        .unwrap();
    reads
}

fn projection_len(
    pg: &PgStore,
    epoch: ClusterEpoch,
    last: u64,
    hash_only: bool,
) -> Result<usize, CompleteLogReadError> {
    if hash_only {
        hashes(pg, epoch, 1, last).map(|entries| entries.len())
    } else {
        pg.retained_complete_metadata_command_log_entries(
            0,
            epoch,
            index(1),
            index(last),
            &topology(),
        )
        .map(|entries| entries.len())
    }
}

#[test]
fn staging_complete_log_byte_budget_accepts_exact_cap_and_rejects_before_excess_row_io() {
    const MIB: usize = 1024 * 1024;
    let dir = test_util::tempdir();
    let bucket = fixture(StagingTarget::DirectPut).subject.bucket;
    let pg_id = PgId::new(topology().bucket_pg_for(&bucket));
    let mut pg = PgStore::open(dir.path(), pg_id.get()).unwrap();
    let epoch = ClusterEpoch::new(7).unwrap();
    let mut previous = 0;
    let mut last_previous = 0;
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for position in 1..=16 {
        last_previous = previous;
        previous = seed_log_row(
            &pg,
            MetadataCommandId::new(epoch, pg_id, index(position)),
            MIB,
            previous,
        );
    }
    pg.conn.execute_batch("COMMIT").unwrap();
    let reads = count_payload_reads(&mut pg);
    let writes = pg.conn.total_changes();
    for hash_only in [false, true] {
        reads.store(0, Ordering::SeqCst);
        assert_eq!(projection_len(&pg, epoch, 16, hash_only).unwrap(), 16);
        assert_eq!(reads.load(Ordering::SeqCst), 16);
        assert_eq!(pg.conn.total_changes(), writes);
    }
    // One extra encoded byte crosses the cumulative budget, although every
    // command individually fits and the page contains fewer than 31 positions.
    pg.conn
        .execute("DELETE FROM metadata_command_log WHERE log_index = 16", [])
        .unwrap();
    seed_log_row(
        &pg,
        MetadataCommandId::new(epoch, pg_id, index(16)),
        MIB + 1,
        last_previous,
    );
    let writes = pg.conn.total_changes();
    for hash_only in [false, true] {
        reads.store(0, Ordering::SeqCst);
        let error = projection_len(&pg, epoch, 16, hash_only).unwrap_err();
        assert!(
            matches!(error, CompleteLogReadError::ValidationBudgetExceeded {
            next_log_index: 16, admitted_bytes, limit_bytes,
        } if admitted_bytes == 15 * MIB && limit_bytes == 16 * MIB),
            "{error}"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            15,
            "excess BLOB was fetched before admission"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.conn.is_autocommit());
        assert!(!pg
            .conn
            .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
            .unwrap());
    }
    // Reducing the range, without skipping a retained row, is a valid retry.
    assert_eq!(hashes(&pg, epoch, 16, 16).unwrap().len(), 1);
}

#[test]
fn staging_complete_log_byte_admission_rejects_oversized_blobs_and_preserves_strict_types() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, mut pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let id = context.id();
    assert_eq!(id.log_index().get(), 1);
    let reads = count_payload_reads(&mut pg);
    for damaged in [
        "zeroblob(4194337)", // MAX_COMPLETE_COMMAND_BYTES (4 MiB + 32) + 1.
        "zeroblob(0)",
    ] {
        pg.conn
            .execute(
                &format!("UPDATE metadata_command_log SET command_bytes = {damaged}"),
                [],
            )
            .unwrap();
        let writes = pg.conn.total_changes();
        for hash_only in [false, true] {
            reads.store(0, Ordering::SeqCst);
            let error = projection_len(&pg, id.cluster_epoch(), 1, hash_only).unwrap_err();
            assert!(
                matches!(
                    error,
                    CompleteLogReadError::Store(StoreError::MetadataCommandLogConflict {
                        log_index: 1,
                        ..
                    })
                ),
                "{damaged}: {error}"
            );
            assert_eq!(
                reads.load(Ordering::SeqCst),
                0,
                "invalid-size/type BLOB was fetched"
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert!(pg.conn.is_autocommit());
        }
    }
    pg.conn
        .execute(
            "UPDATE metadata_command_log SET command_bytes = ?1",
            [context.encode_command().unwrap().bytes()],
        )
        .unwrap();
    for nonblob in ["CAST(zeroblob(16777217) AS TEXT)", "42"] {
        let writes = pg.conn.total_changes();
        let error = pg
            .conn
            .execute(
                &format!("UPDATE metadata_command_log SET command_bytes = {nonblob}"),
                [],
            )
            .unwrap_err();
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        );
        assert!(
            error
                .to_string()
                .contains("BLOB column metadata_command_log.command_bytes"),
            "{error}"
        );
        for hash_only in [false, true] {
            assert_eq!(
                projection_len(&pg, id.cluster_epoch(), 1, hash_only).unwrap(),
                1
            );
        }
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_log_byte_admission_and_payload_use_same_snapshot() {
    let _clock = test_time_override_guard(2_000);
    for hash_only in [false, true] {
        let (dir, mut pg, context) = complete_application::setup(0);
        apply(&pg, &context);
        let id = context.id();
        assert_eq!(id.log_index().get(), 1);
        let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&reads);
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.starts_with("SELECT command_checksum, command_bytes, abandoned,")
                    && observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(other.conn.execute(
                        "UPDATE metadata_command_log SET command_bytes = zeroblob(16777217) WHERE log_index = 1", [],
                    ).unwrap(), 1);
                }
            }
        })).unwrap();
        let writes = pg.conn.total_changes();
        assert_eq!(
            projection_len(&pg, id.cluster_epoch(), 1, hash_only).unwrap(),
            1
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert!(projection_len(&pg, id.cluster_epoch(), 1, hash_only).is_err());
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "next snapshot must reject enlargement before BLOB fetch"
        );
        assert_eq!(pg.conn.total_changes(), writes);
    }
}
