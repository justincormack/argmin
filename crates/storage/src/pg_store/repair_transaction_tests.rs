// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Operational repair/backfill rows, not payload or cleanup authority fixtures.

use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

type FinishHook = Box<dyn FnOnce(&PgStore) -> Result<(), StoreError>>;
thread_local! {
    static BEFORE_FINISH: RefCell<Option<FinishHook>> = const { RefCell::new(None) };
}

pub(super) fn before_finish(pg: &PgStore) -> Result<(), StoreError> {
    let hook = BEFORE_FINISH.with(|slot| slot.borrow_mut().take());
    hook.map_or(Ok(()), |hook| hook(pg))
}

fn with_hook<T>(hook: FinishHook, action: impl FnOnce() -> T) -> T {
    struct Reset(Option<FinishHook>);
    impl Drop for Reset {
        fn drop(&mut self) {
            BEFORE_FINISH.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _reset = Reset(BEFORE_FINISH.with(|slot| slot.replace(Some(hook))));
    action()
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    InsertBackfill,
    CoalesceBackfill,
    ClaimRepair,
    ClaimBackfill,
}
const OPERATIONS: [Operation; 4] = [
    Operation::InsertBackfill,
    Operation::CoalesceBackfill,
    Operation::ClaimRepair,
    Operation::ClaimBackfill,
];

fn request() -> SegmentStoredBytesRequest {
    SegmentStoredBytesRequest {
        data_pg_id: 7,
        segment_okh: [0xB8; 16],
        segment_vid: GenerationId::new(42).unwrap(),
        stored_size: 1024,
        segment_crc64: 0x1234,
        ec: EcShape { k: 4, m: 2 },
    }
}

fn backfill() -> PlacedSegmentShardBackfillWorkItem {
    PlacedSegmentShardBackfillWorkItem {
        request: request(),
        source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
        desired_cluster_epoch: ClusterEpoch::new(4).unwrap(),
    }
}

fn setup(operation: Operation) -> (test_util::TempDir, PgStore) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 7).unwrap();
    match operation {
        Operation::InsertBackfill => (),
        Operation::CoalesceBackfill | Operation::ClaimBackfill => pg
            .record_placed_segment_shard_backfill(&backfill(), 2, None)
            .unwrap(),
        Operation::ClaimRepair => pg
            .record_placed_segment_shard_repair(
                &PlacedSegmentShardRepairWorkItem {
                    request: request(),
                    shard_index: ShardIndex::new(5),
                    shard_integrity: None,
                },
                None,
            )
            .unwrap(),
    }
    (dir, pg)
}

fn run(pg: &PgStore, operation: Operation) -> Result<(), StoreError> {
    match operation {
        Operation::InsertBackfill | Operation::CoalesceBackfill => {
            pg.record_placed_segment_shard_backfill(&backfill(), 1, Some("observed"))
        }
        Operation::ClaimRepair => pg
            .acquire_placed_segment_shard_repair_claim(&PlacedSegmentShardRepairClaimAcquire {
                claim_id: "claim".into(),
                owner_token: "owner".into(),
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                claimed_at: 10,
                lease_deadline: 20,
                now: 10,
            })
            .map(|claim| {
                let claim = claim.unwrap();
                assert_eq!(claim.claim_id, "claim");
                assert_eq!(claim.owner_token, "owner");
                assert_eq!(claim.attempt_count, 1);
            }),
        Operation::ClaimBackfill => pg
            .acquire_placed_segment_shard_backfill_claim(&PlacedSegmentShardBackfillClaimAcquire {
                claim_id: "claim".into(),
                owner_token: "owner".into(),
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                claimed_at: 10,
                lease_deadline: 20,
                now: 10,
            })
            .map(|claim| {
                let claim = claim.unwrap();
                assert_eq!(claim.claim_id, "claim");
                assert_eq!(claim.owner_token, "owner");
                assert_eq!(claim.attempt_count, 1);
            }),
    }
}

// Include every operational column: canonical checkpoint blocks do not include
// these tables and would give a vacuous rollback assertion.
fn snapshot(conn: &rusqlite::Connection) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    [
        "placed_segment_shard_repairs",
        "placed_segment_shard_backfills",
    ]
    .map(|table| {
        let mut statement = conn
            .prepare(&format!(
                "SELECT * FROM {table} ORDER BY segment_okh, segment_vid"
            ))
            .unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |row| {
                (0..columns)
                    .map(|i| row.get(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    })
    .to_vec()
}

#[derive(Clone, Copy, Debug)]
enum Parent {
    Standalone,
    Raw,
    Managed,
}
const PARENTS: [Parent; 3] = [Parent::Standalone, Parent::Raw, Parent::Managed];

fn begin_parent(pg: &PgStore, parent: Parent) -> Option<MetadataTransaction<'_>> {
    match parent {
        Parent::Standalone => None,
        Parent::Raw => {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            None
        }
        Parent::Managed => Some(MetadataTransaction::begin(pg, "repair parent").unwrap()),
    }
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Operation,
    Unwind,
    Commit,
}

#[test]
fn repair_transaction_failures_preserve_parent_or_retire_until_reopen() {
    for operation in OPERATIONS {
        for parent_kind in PARENTS {
            for failure in [Failure::Operation, Failure::Unwind, Failure::Commit] {
                if matches!(failure, Failure::Commit) && !matches!(parent_kind, Parent::Standalone)
                {
                    continue;
                }
                for failed_rollback in [false, true] {
                    let (dir, pg) = setup(operation);
                    let mut parent = begin_parent(&pg, parent_kind);
                    let canary = ShardKey::new(&[0xC2; 16], 1, 0);
                    let ack = WriteAck {
                        stored_size: 4,
                        crc64: 7,
                    };
                    if !matches!(parent_kind, Parent::Standalone) {
                        pg.register_written_shard(&canary, ack).unwrap();
                    }
                    let before = snapshot(&pg.conn);
                    let hook_before = before.clone();
                    let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let hook_reached = Arc::clone(&reached);
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        with_hook(
                            Box::new(move |pg| {
                                hook_reached.store(true, Ordering::Relaxed);
                                assert!(!pg.conn.is_autocommit());
                                assert_ne!(
                                    snapshot(&pg.conn),
                                    hook_before,
                                    "no real {operation:?} effect"
                                );
                                if failed_rollback {
                                    pg.test_fail_metadata_command_rollbacks(1);
                                }
                                match failure {
                                    Failure::Operation => {
                                        Err(StoreError::RouteCapabilitySubjectMismatch {
                                            operation: "injected repair transaction failure",
                                        })
                                    }
                                    Failure::Unwind => panic!("injected repair transaction unwind"),
                                    Failure::Commit => {
                                        pg.fail_next_metadata_txn_commit
                                            .store(true, Ordering::Relaxed);
                                        Ok(())
                                    }
                                }
                            }),
                            || run(&pg, operation),
                        )
                    }));
                    assert!(reached.load(Ordering::Relaxed));
                    if matches!(failure, Failure::Unwind) {
                        assert!(result.is_err());
                    } else {
                        let error = result.unwrap().unwrap_err().to_string();
                        assert!(
                            error.contains(if matches!(failure, Failure::Commit) {
                                "commit failure"
                            } else {
                                "repair transaction failure"
                            }),
                            "{error}"
                        );
                        if failed_rollback {
                            assert!(error.contains("rollback failure"), "{error}");
                        }
                    }
                    if failed_rollback {
                        assert!(pg.require_usable_connection().is_err());
                        assert!(pg.require_live_allocation_lifetime().is_err());
                        assert!(!pg.conn.is_autocommit(), "hidden child rollback retry");
                        assert_ne!(snapshot(&pg.conn), before);
                        let writes = pg.conn.total_changes();
                        assert!(run(&pg, operation)
                            .unwrap_err()
                            .to_string()
                            .contains("retired PG metadata connection"));
                        assert_eq!(pg.conn.total_changes(), writes);
                        if let Some(parent) = parent.take() {
                            assert!(parent.commit("must not commit").is_err());
                        }
                        assert!(pg
                            .commit_unmanaged_test_transaction("must not commit")
                            .is_err());
                        assert!(!pg.conn.is_autocommit(), "hidden parent rollback retry");
                        pg.conn.execute_batch("ROLLBACK").unwrap();
                        assert!(pg.require_usable_connection().is_err());
                    } else {
                        pg.require_usable_connection().unwrap();
                        assert_eq!(snapshot(&pg.conn), before);
                        assert_eq!(
                            pg.conn.is_autocommit(),
                            matches!(parent_kind, Parent::Standalone)
                        );
                        if !matches!(parent_kind, Parent::Standalone) {
                            pg.validate_written_shard_ack(&canary, ack).unwrap();
                            if let Some(parent) = parent.take() {
                                parent.commit("parent").unwrap();
                            } else {
                                pg.commit_unmanaged_test_transaction("raw parent").unwrap();
                            }
                        }
                    }
                    drop(parent);
                    drop(pg);
                    let pg = PgStore::open(dir.path(), 7).unwrap();
                    assert_eq!(snapshot(&pg.conn), before);
                    if !matches!(parent_kind, Parent::Standalone) {
                        assert_eq!(
                            pg.validate_written_shard_ack(&canary, ack).is_ok(),
                            !failed_rollback
                        );
                    }
                    run(&pg, operation).unwrap();
                    assert_ne!(snapshot(&pg.conn), before);
                }
            }
        }
    }
}

#[test]
fn repair_transaction_success_releases_unique_children_without_committing_parent() {
    for operation in OPERATIONS {
        for parent_kind in PARENTS {
            let (_dir, mut pg) = setup(operation);
            let before = snapshot(&pg.conn);
            let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
            let trace = Arc::new(Mutex::new(Vec::new()));
            let captured = Arc::clone(&trace);
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(_, sql) = event {
                            if ["BEGIN", "SAVEPOINT", "RELEASE", "COMMIT", "ROLLBACK"]
                                .iter()
                                .any(|prefix| sql.starts_with(prefix))
                            {
                                captured.lock().unwrap().push(sql.to_owned());
                            }
                        }
                    }),
                )
                .unwrap();
            let parent = begin_parent(&pg, parent_kind);
            run(&pg, operation).unwrap();
            if !matches!(parent_kind, Parent::Standalone) {
                assert_eq!(snapshot(&observer), before, "child release became durable");
                run(&pg, operation).unwrap();
                assert_eq!(snapshot(&observer), before);
                let statements = trace.lock().unwrap();
                assert!(!statements.iter().any(|sql| sql == "COMMIT"));
                let names: Vec<_> = statements
                    .iter()
                    .filter_map(|sql| sql.strip_prefix("SAVEPOINT "))
                    .collect();
                assert_eq!(names.len(), 2);
                assert_ne!(names[0], names[1]);
                for name in names {
                    assert!(statements.contains(&format!("RELEASE {name}")));
                }
            }
            if let Some(parent) = parent {
                parent.commit("parent").unwrap();
            } else if matches!(parent_kind, Parent::Raw) {
                pg.commit_unmanaged_test_transaction("raw parent").unwrap();
            }
            assert_ne!(snapshot(&observer), before);
            assert_eq!(snapshot(&observer), snapshot(&pg.conn));
            let statements = trace.lock().unwrap();
            assert_eq!(
                statements
                    .iter()
                    .filter(|sql| sql.starts_with("BEGIN"))
                    .count(),
                1
            );
            assert_eq!(
                statements
                    .iter()
                    .filter(|sql| sql.as_str() == "COMMIT")
                    .count(),
                1
            );
        }
    }
}
