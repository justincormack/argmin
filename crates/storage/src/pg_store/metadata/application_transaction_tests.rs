// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current command grammar, real metadata/log mutations and transaction failures.
//! No payload fixtures or complete-command activation.

use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

fn setup() -> (test_util::TempDir, PgStore, MetadataCommandEnvelope) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 1).unwrap();
    let command = create_bucket_probe_command(1, 1, trusted_bucket_name("current-apply"), 1);
    (dir, pg, command)
}

fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> MetadataCommandReplicaState {
    pg.apply_metadata_command_and_record(0, command).unwrap()
}

type LogRow = (i64, i64, i64, i64, Vec<u8>, i64, Option<i64>, Option<i64>);

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    materialized: Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>,
    prefix: MetadataCommandReplicaState,
    log: Vec<LogRow>,
}

fn snapshot(pg: &PgStore) -> Snapshot {
    let log = pg
        .conn
        .prepare(
            "SELECT cluster_epoch, pg_id, log_index, command_checksum, command_bytes, \
             abandoned, previous_log_hash, log_hash FROM metadata_command_log \
             ORDER BY cluster_epoch, pg_id, log_index",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    Snapshot {
        materialized: pg.metadata_checkpoint_table_blocks().unwrap(),
        prefix: pg.metadata_command_replica_state().unwrap(),
        log,
    }
}

fn detail(error: &BucketSnapshotLoadError) -> String {
    match error {
        BucketSnapshotLoadError::Store(error) => error.to_string(),
        BucketSnapshotLoadError::Metadata(error) => error.to_string(),
    }
}

fn assert_retired(pg: &PgStore, command: &MetadataCommandEnvelope) {
    assert!(!pg.conn.is_autocommit(), "hidden rollback retried teardown");
    assert!(pg.require_usable_connection().is_err());
    assert!(pg.require_live_allocation_lifetime().is_err());
    let writes = pg.conn.total_changes();
    let error = pg
        .apply_metadata_command_and_record(0, command)
        .unwrap_err();
    assert!(detail(&error).contains("retired PG metadata connection"));
    assert_eq!(pg.conn.total_changes(), writes);
    assert!(!pg.conn.is_autocommit());
    // Ending SQLite's transaction by hand cannot revive the retired store.
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert!(pg.require_usable_connection().is_err());
    assert!(pg.apply_metadata_command_and_record(0, command).is_err());
}

#[test]
fn current_apply_transaction_owns_one_commit_and_replay_fence() {
    let (_dir, mut pg, command) = setup();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&trace);
    pg.conn
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(_, sql) = event {
                    recorded.lock().unwrap().push(sql.to_owned());
                }
            }),
        )
        .unwrap();
    let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
    let mut committed = None;
    for replay in [false, true] {
        let mut fenced = false;
        let state = pg
            .apply_metadata_command_and_record_with_commit_guard(0, &command, || {
                fenced = true;
                assert!(!pg.conn.is_autocommit());
                assert_eq!(pg.metadata_command_replica_state()?.applied_log_index, 1);
                let error = pg
                    .commit_unmanaged_test_transaction("crossed current-apply commit")
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("owning metadata transaction scope"));
                let visible: u32 = observer
                    .query_row("SELECT count(*) FROM buckets", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(visible, u32::from(replay), "uncommitted creation escaped");
                Ok(())
            })
            .unwrap();
        assert!(fenced, "replay must also check the captured route fence");
        assert!(pg.conn.is_autocommit());
        assert_eq!(state.applied_log_index, 1);
        let after = snapshot(&pg);
        if let Some(before) = committed.as_ref() {
            assert_eq!(&after, before);
        }
        committed = Some(after);
    }
    let trace = trace.lock().unwrap();
    assert_eq!(
        trace.iter().filter(|sql| sql.starts_with("BEGIN")).count(),
        2
    );
    assert_eq!(
        trace.iter().filter(|sql| sql.as_str() == "COMMIT").count(),
        2
    );
    assert!(!trace.iter().any(|sql| sql.starts_with("ROLLBACK")));
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    RouteExpiry,
    Unwind,
    Commit,
}

#[test]
fn current_apply_transaction_error_unwind_and_commit_failure_roll_back_or_retire() {
    for fault in [Fault::RouteExpiry, Fault::Unwind, Fault::Commit] {
        for replay in [false, true] {
            for fail_rollback in [false, true] {
                let (dir, pg, command) = setup();
                if replay {
                    apply(&pg, &command);
                }
                let before = snapshot(&pg);
                if matches!(fault, Fault::Commit) {
                    pg.conn.execute_batch("CREATE TEMP TABLE current_apply_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES current_apply_commit_fault(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                }
                let mut reached = false;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    pg.apply_metadata_command_and_record_with_commit_guard(0, &command, || {
                        reached = true;
                        if !replay {
                            assert_ne!(snapshot(&pg), before, "fault must follow actual apply");
                        }
                        if fail_rollback {
                            pg.test_fail_metadata_command_rollbacks(1);
                        }
                        match fault {
                            Fault::RouteExpiry => Err(StoreError::RouteMapExpired {
                                cluster_epoch: ClusterEpoch::INITIAL,
                                valid_until_ms: 10,
                                now_ms: 10,
                            }),
                            Fault::Unwind => panic!("injected current apply unwind"),
                            Fault::Commit => {
                                pg.conn
                                    .execute(
                                        "INSERT INTO current_apply_commit_fault VALUES (1, 2)",
                                        [],
                                    )
                                    .unwrap();
                                Ok(())
                            }
                        }
                    })
                }));
                assert!(reached);
                if matches!(fault, Fault::Unwind) {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err();
                    let diagnostic = detail(&error);
                    if fail_rollback {
                        assert!(
                            diagnostic.contains("injected metadata-command rollback failure"),
                            "{diagnostic}"
                        );
                        assert!(diagnostic.contains("rollback failed"), "{diagnostic}");
                    }
                    if matches!(fault, Fault::Commit) {
                        assert!(
                            matches!(
                                &error,
                                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                                    context: "apply metadata command and record (commit txn)",
                                    ..
                                })
                            ),
                            "{diagnostic}"
                        );
                        assert!(
                            diagnostic.contains("FOREIGN KEY constraint failed"),
                            "{diagnostic}"
                        );
                    } else if !fail_rollback {
                        assert!(matches!(
                            error,
                            BucketSnapshotLoadError::Store(StoreError::RouteMapExpired {
                                valid_until_ms: 10,
                                now_ms: 10,
                                ..
                            })
                        ));
                    } else {
                        assert!(
                            diagnostic.contains("expired"),
                            "original route cause was lost: {diagnostic}"
                        );
                    }
                }
                if fail_rollback {
                    assert_retired(&pg, &command);
                } else {
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    assert_eq!(snapshot(&pg), before);
                }
                drop(pg);
                let pg = PgStore::open(dir.path(), 1).unwrap();
                assert_eq!(
                    snapshot(&pg),
                    before,
                    "{fault:?}, replay={replay}, rollback={fail_rollback}"
                );
                apply(&pg, &command);
                assert_eq!(
                    pg.metadata_command_replica_state()
                        .unwrap()
                        .applied_log_index,
                    1
                );
            }
        }
    }
}

#[test]
fn current_apply_transaction_record_failure_restores_materialized_rows_or_retires() {
    for fail_rollback in [false, true] {
        let (dir, pg, command) = setup();
        let before = snapshot(&pg);
        pg.conn.execute_batch("CREATE TEMP TRIGGER current_apply_record_fault BEFORE INSERT ON main.metadata_command_log BEGIN SELECT RAISE(ABORT, 'injected current log insert failure'); END").unwrap();
        if fail_rollback {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let error = pg
            .apply_metadata_command_and_record_with_commit_guard(0, &command, || {
                panic!("log insertion failure must precede commit guard")
            })
            .unwrap_err();
        let diagnostic = detail(&error);
        assert!(
            diagnostic.contains("injected current log insert failure"),
            "{diagnostic}"
        );
        if fail_rollback {
            assert!(diagnostic.contains("rollback failed"), "{diagnostic}");
            let count: u32 = pg
                .conn
                .query_row("SELECT count(*) FROM buckets", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 1, "injected rollback must follow bucket creation");
            assert_retired(&pg, &command);
        } else {
            assert_eq!(snapshot(&pg), before);
            pg.require_usable_connection().unwrap();
        }
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(snapshot(&pg), before);
        apply(&pg, &command);
    }
}

#[test]
fn current_apply_transaction_metadata_error_keeps_both_causes_if_rollback_fails() {
    for fail_rollback in [false, true] {
        let (dir, pg, command) = setup();
        apply(&pg, &command);
        let before = snapshot(&pg);
        let crossed = create_bucket_probe_command(1, 2, trusted_bucket_name("current-apply"), 2);
        if fail_rollback {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let error = pg
            .apply_metadata_command_and_record(0, &crossed)
            .unwrap_err();
        if fail_rollback {
            let diagnostic = detail(&error);
            assert!(diagnostic.contains("bucket already exists"), "{diagnostic}");
            assert!(
                diagnostic.contains("injected metadata-command rollback failure"),
                "{diagnostic}"
            );
            assert_retired(&pg, &crossed);
        } else {
            assert!(matches!(
                error,
                BucketSnapshotLoadError::Metadata(MetadataError::BucketAlreadyExists)
            ));
            assert_eq!(snapshot(&pg), before);
        }
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(snapshot(&pg), before);
        apply(&pg, &command);
    }
}

#[test]
fn current_apply_transaction_rejects_outer_scope_without_finishing_it() {
    let (_dir, pg, command) = setup();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = snapshot(&pg);
    let writes = pg.conn.total_changes();
    let error = pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap_err();
    assert!(detail(&error).contains("inside caller transaction"));
    assert!(!pg.conn.is_autocommit());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(snapshot(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
    pg.require_usable_connection().unwrap();
    apply(&pg, &command);
}

#[test]
fn current_apply_transaction_retired_allocator_still_allows_apply_and_replay() {
    let (_dir, pg, command) = setup();
    drop(pg.begin_allocation_lifetime_replacement().unwrap());
    assert!(pg.require_live_allocation_lifetime().is_err());
    pg.require_usable_connection().unwrap();
    let state = apply(&pg, &command);
    let before = snapshot(&pg);
    assert_eq!(apply(&pg, &command), state);
    assert_eq!(snapshot(&pg), before);
    assert!(pg.require_live_allocation_lifetime().is_err());
}
