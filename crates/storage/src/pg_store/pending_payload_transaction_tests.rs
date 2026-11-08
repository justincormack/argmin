// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current pending engine, not authenticated-context activation. Metadata-only
//! fixtures and one raw fault connection; no second PG runtime or physical I/O.

use super::super::candidate::DescribedStagingCommand;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::pg_store::{
    command_log::register_metadata_digest_sql_functions,
    PendingMetadataCommandSlotInsertError as InsertError,
    PendingMetadataCommandSlotReplaceError as ReplaceError, PgStore,
};
use crate::types::AdmittedRouteEffectFence;
use crate::StoreError;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{Arc, Mutex};

const ENCRYPTION_READ: &str =
    "SELECT encryption_type, encryption_state FROM stream_uploads WHERE session_id";

pub(super) fn setup(
    target: StagingTarget,
    is_append: bool,
) -> (
    test_util::TempDir,
    PgStore,
    StagingOwnerRecord,
    MetadataCommandEnvelope,
) {
    if is_append {
        return candidate::setup(target, true);
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
    let owner = match target {
        StagingTarget::StreamPut => object::initialize(&pg, target, 2),
        StagingTarget::UploadPart { .. } => part::initialize(&pg, 2),
        StagingTarget::DirectPut => unreachable!(),
    };
    let command = close::command(&pg, &owner);
    pg.conn.execute_batch("COMMIT").unwrap();
    (dir, pg, owner, command)
}

pub(super) fn install(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    described: Option<&DescribedStagingCommand>,
) -> Result<(), InsertError> {
    if let Some(described) = described {
        let topology = topology();
        let bound = described.bind(&topology).unwrap();
        pg.try_insert_bound_staging_pending_metadata_command_slot(
            0,
            &bound.pending_protection(),
            &topology,
            Some(command.bucket_name()),
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
            None,
        )
    } else {
        pg.try_insert_pending_metadata_command_slot_classified(
            0,
            command,
            Some(command.bucket_name()),
        )
    }
}

pub(super) fn replace(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    replacement: &MetadataCommandEnvelope,
    described: Option<&DescribedStagingCommand>,
) -> Result<bool, ReplaceError> {
    if let Some(described) = described {
        let topology = topology();
        let bound = described.bind(&topology).unwrap();
        pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
            0,
            &bound.pending_protection(),
            replacement,
            &topology,
            Some(command.bucket_name()),
        )
    } else {
        pg.replace_pending_metadata_command_slot_for_reissue(
            0,
            command,
            replacement,
            Some(command.bucket_name()),
        )
    }
}

pub(super) fn successor(command: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(command.id().log_index().get() + 1).unwrap(),
        ),
        command.payload().clone(),
    )
}

#[derive(Default)]
struct Trace {
    statements: Vec<String>,
    attempted_write: Option<rusqlite::Result<usize>>,
}

fn trace_with_fault(
    pg: &mut PgStore,
    path: &std::path::Path,
    owner: &StagingOwnerRecord,
    before_transaction: bool,
) -> Arc<Mutex<Trace>> {
    let writer = rusqlite::Connection::open(path.join("metadata.db")).unwrap();
    register_metadata_digest_sql_functions(&writer).unwrap();
    writer.busy_timeout(std::time::Duration::ZERO).unwrap();
    let session = owner.subject.id.attempt.clone();
    let observed = Arc::new(Mutex::new(Trace::default()));
    let captured = Arc::clone(&observed);
    pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
        if let TraceEvent::Stmt(_, sql) = event {
            let mut trace = captured.lock().unwrap();
            trace.statements.push(sql.to_owned());
            let trigger = if before_transaction { sql == "BEGIN IMMEDIATE" }
                else { sql.starts_with(ENCRYPTION_READ) };
            if trigger && trace.attempted_write.is_none() {
                // Corrupt only the mutable source used by page derivation.
                // Before BEGIN this commits; during the projection the pending
                // transaction must exclude it. Assert outside SQLite's callback.
                trace.attempted_write = Some(writer.execute(
                    "UPDATE stream_uploads SET encryption_state = zeroblob(length(encryption_state)) WHERE session_id = ?1",
                    [session.as_str()],
                ));
            }
        }
    })).unwrap();
    observed
}

fn stop_trace(pg: &mut PgStore) {
    pg.conn
        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
        .unwrap();
}

pub(super) fn pages(pg: &PgStore) -> Vec<(String, u32, u32, Vec<u8>)> {
    let mut result = Vec::new();
    for table in [
        "metadata_command_pending_placed_reference_pages",
        "metadata_command_pending_reclaim_reference_pages",
    ] {
        let mut statement = pg.conn.prepare(&format!("SELECT page_index, reference_count, encoded_references FROM {table} ORDER BY page_index")).unwrap();
        result.extend(
            statement
                .query_map([], |row| {
                    Ok((table.to_owned(), row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap()
                .map(Result::unwrap),
        );
    }
    result
}

fn assert_transaction(trace: &Trace, mutation: &str) {
    assert_eq!(
        trace
            .attempted_write
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
    let statements = &trace.statements;
    for delimiter in ["BEGIN IMMEDIATE", "COMMIT"] {
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == delimiter)
                .count(),
            1,
            "{statements:?}"
        );
    }
    let begin = statements
        .iter()
        .position(|sql| sql == "BEGIN IMMEDIATE")
        .unwrap();
    let reads: Vec<_> = statements
        .iter()
        .enumerate()
        .filter_map(|(index, sql)| sql.starts_with(ENCRYPTION_READ).then_some(index))
        .collect();
    assert!(!reads.is_empty());
    let effect = statements
        .iter()
        .position(|sql| sql.starts_with(mutation))
        .unwrap();
    let commit = statements.iter().position(|sql| sql == "COMMIT").unwrap();
    assert!(reads.iter().all(|read| begin < *read && *read < effect));
    assert!(effect < commit);
}

fn assert_decode_error(error: &StoreError) {
    assert!(
        matches!(
            error,
            StoreError::Db {
                context: "load exact stream encryption for pending payload inventory",
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn pending_payload_inventory_insert_uses_one_write_transaction() {
    check_transaction(false);
}

#[test]
fn pending_payload_inventory_reissue_uses_one_write_transaction() {
    check_transaction(true);
}

fn check_transaction(reissue: bool) {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for is_append in [false, true] {
            for bound in [false, true]
                .into_iter()
                .filter(|bound| !bound || is_append)
            {
                let (dir, mut pg, owner, command) = setup(target.clone(), is_append);
                let described =
                    bound.then(|| candidate::capture(&pg, &owner, command.clone(), true));
                let before = introduction::state(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let expected_pages = pg
                    .encode_pending_shard_scavenger_reference_pages(&command)
                    .unwrap();
                assert_eq!(
                    expected_pages.placed_reference_count,
                    if is_append { 1 } else { 2 }
                );
                if reissue {
                    install(&pg, &command, described.as_ref()).unwrap();
                }
                let replacement = successor(&command);
                let trace = trace_with_fault(&mut pg, dir.path(), &owner, false);
                if reissue {
                    assert!(replace(&pg, &command, &replacement, described.as_ref()).unwrap());
                } else {
                    install(&pg, &command, described.as_ref()).unwrap();
                }
                stop_trace(&mut pg);
                assert_transaction(
                    &trace.lock().unwrap(),
                    if reissue {
                        "UPDATE metadata_command_pending_slot "
                    } else {
                        "INSERT INTO metadata_command_pending_slot "
                    },
                );
                assert_eq!(
                    pages(&pg)
                        .iter()
                        .map(|row| row.3.clone())
                        .collect::<Vec<_>>(),
                    expected_pages.placed_pages
                );
                assert_eq!(introduction::state(&pg), before);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                let slot = pending_install::observe(&pg);
                let indexed = pages(&pg);
                let writes = pg.conn.total_changes();
                if reissue {
                    assert!(replace(&pg, &command, &replacement, described.as_ref()).unwrap());
                } else {
                    install(&pg, &command, described.as_ref()).unwrap();
                }
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pages(&pg), indexed);
            }
        }
    }
}

#[test]
fn pending_payload_inventory_insert_reloads_after_pretransaction_change() {
    check_pretransaction_change(false);
}

#[test]
fn pending_payload_inventory_reissue_reloads_after_pretransaction_change() {
    check_pretransaction_change(true);
}

fn check_pretransaction_change(reissue: bool) {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for is_append in [false, true] {
            for bound in [false, true]
                .into_iter()
                .filter(|bound| !bound || is_append)
            {
                let (dir, mut pg, owner, command) = setup(target.clone(), is_append);
                let described =
                    bound.then(|| candidate::capture(&pg, &owner, command.clone(), true));
                // A positive canary before the mutation rules out an initially
                // invalid source or a command that never needs session geometry.
                assert_eq!(
                    pg.encode_pending_shard_scavenger_reference_pages(&command)
                        .unwrap()
                        .placed_reference_count,
                    if is_append { 1 } else { 2 }
                );
                if reissue {
                    install(&pg, &command, described.as_ref()).unwrap();
                }
                let slot = pending_install::observe(&pg);
                let indexed = pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                let trace = trace_with_fault(&mut pg, dir.path(), &owner, true);
                if reissue {
                    let error = replace(&pg, &command, &successor(&command), described.as_ref())
                        .unwrap_err();
                    let ReplaceError::MayHaveApplied(error) = error else {
                        panic!("{error:?}")
                    };
                    assert_decode_error(&error);
                } else {
                    let error = install(&pg, &command, described.as_ref()).unwrap_err();
                    let InsertError::Definitive(error) = error else {
                        panic!("{error:?}")
                    };
                    assert_decode_error(&error);
                }
                stop_trace(&mut pg);
                let trace = trace.lock().unwrap();
                assert_eq!(
                    *trace.attempted_write.as_ref().unwrap().as_ref().unwrap(),
                    1
                );
                assert_eq!(
                    trace
                        .statements
                        .iter()
                        .filter(|sql| sql.as_str() == "ROLLBACK")
                        .count(),
                    1
                );
                assert!(!trace.statements.iter().any(|sql| sql
                    .starts_with("INSERT INTO metadata_command_pending_")
                    || sql.starts_with("UPDATE metadata_command_pending_slot ")));
                assert!(pg.conn.is_autocommit());
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pages(&pg), indexed);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                if !reissue {
                    pending_install::assert_empty(&pg);
                }
            }
        }
    }
}
