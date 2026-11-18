// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current-format terminal bookkeeping: teardown ownership, not new custody.
//! Real reservation/tombstone/side-index effects; no physical payload fixtures.

use super::super::metadata_transaction::MetadataTransaction;
use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Kind {
    Abandon,
    RemoveApplied,
    RemoveAbandoned,
}
const KINDS: [Kind; 3] = [Kind::Abandon, Kind::RemoveApplied, Kind::RemoveAbandoned];

impl Kind {
    fn run(self, pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        match self {
            Self::Abandon => pg.record_metadata_command_abandoned(0, command).map(|_| ()),
            Self::RemoveApplied | Self::RemoveAbandoned => pg
                .remove_pending_metadata_command_slot(0, command)
                .map(|_| ()),
        }
    }

    fn with_hook(
        self,
        pg: &PgStore,
        command: &MetadataCommandEnvelope,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Abandon => pg
                .record_metadata_command_identity_abandoned_owned(
                    0,
                    &AbandonmentRecord::Current(command),
                    hook,
                )
                .map(|_| ()),
            Self::RemoveApplied | Self::RemoveAbandoned => pg
                .remove_pending_metadata_command_slot_owned(0, command, hook)
                .map(|_| ()),
        }
    }
}

fn setup(kind: Kind) -> (test_util::TempDir, PgStore, MetadataCommandEnvelope) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 1).unwrap();
    let bucket = crate::tests::bucket_name("current-terminal");
    pg.create_bucket(
        &bucket,
        "owner",
        &CanonicalUserId::from_principal("owner"),
        &AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    pg.refresh_metadata_command_state_digest().unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            crate::tests::object_key("key"),
            crate::tests::stream_session_id("terminal"),
            GenerationId::MIN,
            1_000,
        )),
    );
    pg.try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    match kind {
        Kind::Abandon => {}
        Kind::RemoveApplied => {
            pg.apply_metadata_command_and_record(0, &command).unwrap();
        }
        Kind::RemoveAbandoned => {
            pg.record_metadata_command_abandoned(0, &command).unwrap();
        }
    }
    assert_eq!(pending_counts(&pg.conn), [1, 1, 1]);
    (dir, pg, command)
}

fn pending_counts(conn: &rusqlite::Connection) -> [u32; 3] {
    [
        "metadata_command_pending_slot",
        "metadata_command_pending_route_dependencies",
        "metadata_command_pending_staging_protection",
    ]
    .map(|table| {
        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    })
}

fn snapshot(conn: &rusqlite::Connection) -> Vec<Vec<Vec<String>>> {
    [
        "buckets",
        "object_generation_reservations",
        "object_write_counters",
        "metadata_command_replica_state",
        "metadata_table_digests",
        "metadata_digest_revision",
        "metadata_command_log",
        "metadata_command_terminal_receipts",
        "metadata_command_checkpoints",
        "metadata_command_pending_slot",
        "metadata_command_pending_route_dependencies",
        "metadata_command_pending_staging_protection",
        "metadata_command_pending_placed_reference_pages",
        "metadata_command_pending_reclaim_reference_pages",
    ]
    .map(|table| {
        let mut statement = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let count = statement.column_count();
        let mut rows = statement
            .query_map([], |row| {
                (0..count)
                    .map(|i| row.get_ref(i).map(|value| format!("{value:?}")))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.sort();
        rows
    })
    .into()
}

fn trace(pg: &mut PgStore) -> Arc<Mutex<Vec<String>>> {
    let statements = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&statements);
    pg.conn
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(_, sql) = event {
                    if ["BEGIN", "SAVEPOINT", "RELEASE", "COMMIT", "ROLLBACK"]
                        .iter()
                        .any(|prefix| sql.starts_with(prefix))
                    {
                        recorded.lock().unwrap().push(sql.to_owned());
                    }
                }
            }),
        )
        .unwrap();
    statements
}

fn injected_error() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "injected current terminal operation failure",
    }
}

#[test]
fn current_terminal_transaction_commits_once_and_exact_retry_reopens() {
    for kind in KINDS {
        let (dir, mut pg, command) = setup(kind);
        let before = snapshot(&pg.conn);
        let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
        let statements = trace(&mut pg);
        let mut stages = Vec::new();
        kind.with_hook(&pg, &command, |stage| {
            stages.push(stage);
            assert_ne!(snapshot(&pg.conn), before);
            match stage {
                "before-finish" => {
                    assert_eq!(snapshot(&observer), before);
                    assert!(!pg.conn.is_autocommit());
                    assert!(pg
                        .commit_unmanaged_test_transaction("crossed terminal commit")
                        .is_err());
                }
                "after-finish" => {
                    assert!(pg.conn.is_autocommit());
                    assert_eq!(snapshot(&observer), snapshot(&pg.conn));
                }
                _ => panic!("unexpected stage {stage}"),
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(stages, ["before-finish", "after-finish"]);
        assert_eq!(*statements.lock().unwrap(), ["BEGIN IMMEDIATE", "COMMIT"]);
        let after = snapshot(&pg.conn);
        match kind {
            Kind::Abandon => {
                assert!(pg.metadata_command_abandoned(0, &command).unwrap());
                assert_eq!(pending_counts(&pg.conn), [1, 1, 1]);
            }
            _ => assert_eq!(pending_counts(&pg.conn), [0, 0, 0]),
        }
        kind.run(&pg, &command).unwrap();
        assert_eq!(snapshot(&pg.conn), after);
        drop(observer);
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(snapshot(&pg.conn), after);
        kind.run(&pg, &command).unwrap();
        assert_eq!(snapshot(&pg.conn), after);
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Error,
    Unwind,
    Commit,
}

#[test]
fn current_terminal_transaction_failures_restore_state_or_retire_until_reopen() {
    for kind in KINDS {
        for fault in [Fault::Error, Fault::Unwind, Fault::Commit] {
            for failed_rollback in [false, true] {
                let (dir, pg, command) = setup(kind);
                let before = snapshot(&pg.conn);
                pg.conn.execute_batch("CREATE TEMP TABLE terminal_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES terminal_commit_fault(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                let mut reached = false;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    kind.with_hook(&pg, &command, |stage| {
                        assert_eq!(stage, "before-finish");
                        reached = true;
                        assert_ne!(snapshot(&pg.conn), before);
                        if failed_rollback {
                            pg.test_fail_metadata_command_rollbacks(1);
                        }
                        match fault {
                            Fault::Error => Err(injected_error()),
                            Fault::Unwind => panic!("injected terminal unwind"),
                            Fault::Commit => {
                                pg.conn
                                    .execute("INSERT INTO terminal_commit_fault VALUES (1, 2)", [])
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
                    let detail = error.to_string();
                    assert!(
                        detail.contains(if matches!(fault, Fault::Commit) {
                            "FOREIGN KEY constraint failed"
                        } else {
                            "injected current terminal operation failure"
                        }),
                        "{detail}"
                    );
                    if failed_rollback {
                        assert!(
                            detail.contains("injected metadata-command rollback failure"),
                            "{detail}"
                        );
                    } else if matches!(fault, Fault::Error) {
                        assert!(matches!(
                            error,
                            StoreError::RouteCapabilitySubjectMismatch { .. }
                        ));
                    }
                }
                if failed_rollback {
                    assert!(!pg.conn.is_autocommit(), "hidden rollback retry");
                    assert_ne!(snapshot(&pg.conn), before);
                    assert!(pg.require_usable_connection().is_err());
                    assert!(pg.require_live_allocation_lifetime().is_err());
                    assert!(kind.run(&pg, &command).is_err());
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                }
                assert_eq!(snapshot(&pg.conn), before);
                drop(pg);
                let pg = PgStore::open(dir.path(), 1).unwrap();
                assert_eq!(snapshot(&pg.conn), before);
                kind.run(&pg, &command).unwrap();
            }
        }
    }
}

#[test]
fn current_terminal_transaction_body_failure_rolls_back_tombstone_or_cascade() {
    for kind in KINDS {
        for failed_rollback in [false, true] {
            let (dir, pg, command) = setup(kind);
            let before = snapshot(&pg.conn);
            // RAISE(FAIL), unlike ABORT, retains earlier statement effects until
            // the operation owner rolls back the transaction. Check them below.
            let event = if matches!(kind, Kind::Abandon) {
                "INSERT ON metadata_command_log"
            } else {
                "DELETE ON metadata_command_pending_slot"
            };
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER terminal_body_fault AFTER {event} BEGIN SELECT RAISE(FAIL, 'terminal body failure'); END")).unwrap();
            if failed_rollback {
                pg.test_fail_metadata_command_rollbacks(1);
            }
            let error = kind.run(&pg, &command).unwrap_err().to_string();
            assert!(error.contains("terminal body failure"), "{error}");
            if failed_rollback {
                assert!(
                    error.contains("injected metadata-command rollback failure"),
                    "{error}"
                );
                assert_ne!(snapshot(&pg.conn), before, "fixture must fail after writes");
                assert!(!pg.conn.is_autocommit());
                assert!(pg.require_usable_connection().is_err());
                pg.conn.execute_batch("ROLLBACK").unwrap();
            }
            assert_eq!(snapshot(&pg.conn), before);
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            assert_eq!(snapshot(&pg.conn), before);
            kind.run(&pg, &command).unwrap();
        }
    }
}

#[test]
fn current_terminal_transaction_postcommit_loss_does_not_undo_terminal_state() {
    for kind in KINDS {
        for unwind in [false, true] {
            let (dir, pg, command) = setup(kind);
            let before = snapshot(&pg.conn);
            let result = catch_unwind(AssertUnwindSafe(|| {
                kind.with_hook(&pg, &command, |stage| {
                    if stage == "after-finish" {
                        assert!(pg.conn.is_autocommit());
                        if unwind {
                            panic!("injected lost terminal confirmation");
                        }
                        return Err(injected_error());
                    }
                    Ok(())
                })
            }));
            assert!(if unwind {
                result.is_err()
            } else {
                result.unwrap().is_err()
            });
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            let after = snapshot(&pg.conn);
            assert_ne!(after, before);
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            assert_eq!(snapshot(&pg.conn), after);
            kind.run(&pg, &command).unwrap();
            assert_eq!(snapshot(&pg.conn), after);
        }
    }
}

#[test]
fn current_terminal_transaction_nested_removal_releases_only_its_savepoint() {
    for kind in [Kind::RemoveApplied, Kind::RemoveAbandoned] {
        for managed in [false, true] {
            for commit in [false, true] {
                let (dir, mut pg, command) = setup(kind);
                let before = snapshot(&pg.conn);
                let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
                let statements = trace(&mut pg);
                let outer = if managed {
                    Some(MetadataTransaction::begin(&pg, "outer").unwrap())
                } else {
                    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    None
                };
                assert!(pg
                    .remove_pending_metadata_command_slot(0, &command)
                    .unwrap());
                assert!(!pg.conn.is_autocommit());
                assert_eq!(pending_counts(&pg.conn), [0, 0, 0]);
                assert_eq!(snapshot(&observer), before);
                let sql = statements.lock().unwrap();
                assert_eq!(sql.len(), 3);
                assert_eq!(sql[0], "BEGIN IMMEDIATE");
                let name = sql[1].strip_prefix("SAVEPOINT ").unwrap();
                assert_eq!(sql[2], format!("RELEASE {name}"));
                drop(sql);
                match (outer, commit) {
                    (Some(outer), true) => outer.commit("outer commit").unwrap(),
                    (Some(outer), false) => outer.rollback().unwrap(),
                    (None, true) => pg
                        .commit_unmanaged_test_transaction("outer commit")
                        .unwrap(),
                    (None, false) => pg.conn.execute_batch("ROLLBACK").unwrap(),
                }
                if commit {
                    assert_eq!(pending_counts(&observer), [0, 0, 0]);
                } else {
                    assert_eq!(snapshot(&observer), before);
                }
                let after = snapshot(&pg.conn);
                drop(observer);
                drop(pg);
                let pg = PgStore::open(dir.path(), 1).unwrap();
                assert_eq!(snapshot(&pg.conn), after);
            }
        }
    }
}

#[test]
fn current_terminal_transaction_nested_failure_preserves_parent_or_retires_it() {
    for kind in [Kind::RemoveApplied, Kind::RemoveAbandoned] {
        for managed in [false, true] {
            for failed_rollback in [false, true] {
                for unwind in [false, true] {
                    let (dir, pg, command) = setup(kind);
                    let before = snapshot(&pg.conn);
                    pg.conn
                        .execute_batch("CREATE TEMP TABLE parent_probe (value INTEGER)")
                        .unwrap();
                    let mut outer = if managed {
                        Some(MetadataTransaction::begin(&pg, "outer").unwrap())
                    } else {
                        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                        None
                    };
                    pg.conn
                        .execute("INSERT INTO parent_probe VALUES (7)", [])
                        .unwrap();
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        kind.with_hook(&pg, &command, |stage| {
                            assert_eq!(stage, "before-finish");
                            assert_eq!(pending_counts(&pg.conn), [0, 0, 0]);
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            if unwind {
                                panic!("injected child removal unwind");
                            }
                            Err(injected_error())
                        })
                    }));
                    assert!(if unwind {
                        result.is_err()
                    } else {
                        result.unwrap().is_err()
                    });
                    assert!(!pg.conn.is_autocommit());
                    let value: u32 = pg
                        .conn
                        .query_row("SELECT value FROM parent_probe", [], |r| r.get(0))
                        .unwrap();
                    assert_eq!(value, 7, "parent write was rolled back");
                    if failed_rollback {
                        assert!(pg.require_usable_connection().is_err());
                        assert_eq!(pending_counts(&pg.conn), [0, 0, 0]);
                        if let Some(outer) = outer.take() {
                            assert!(outer.commit("must not commit").is_err());
                        }
                        assert!(pg
                            .commit_unmanaged_test_transaction("must not commit")
                            .is_err());
                        assert!(
                            !pg.conn.is_autocommit(),
                            "parent secretly rescued failed child"
                        );
                        pg.conn.execute_batch("ROLLBACK").unwrap();
                        assert!(pg.require_usable_connection().is_err());
                    } else {
                        assert_eq!(snapshot(&pg.conn), before);
                        pg.require_usable_connection().unwrap();
                        if let Some(outer) = outer.take() {
                            outer.commit("commit parent only").unwrap();
                        } else {
                            pg.commit_unmanaged_test_transaction("commit parent only")
                                .unwrap();
                        }
                        assert_eq!(
                            pg.conn
                                .query_row("SELECT value FROM parent_probe", [], |r| r
                                    .get::<_, u32>(0))
                                .unwrap(),
                            7
                        );
                    }
                    drop(outer);
                    drop(pg);
                    let pg = PgStore::open(dir.path(), 1).unwrap();
                    assert_eq!(snapshot(&pg.conn), before);
                    kind.run(&pg, &command).unwrap();
                }
            }
        }
    }
}

#[test]
fn current_terminal_transaction_abandonment_rejects_parent_and_removal_requires_terminal() {
    let (_dir, pg, command) = setup(Kind::Abandon);
    let before = snapshot(&pg.conn);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(pg.record_metadata_command_abandoned(0, &command).is_err());
    assert!(!pg.conn.is_autocommit());
    assert!(matches!(
        pg.remove_pending_metadata_command_slot(0, &command),
        Err(StoreError::MetadataCommandTerminalEntryPending { .. })
    ));
    assert!(!pg.conn.is_autocommit());
    assert_eq!(snapshot(&pg.conn), before);
    pg.require_usable_connection().unwrap();
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert!(matches!(
        pg.remove_pending_metadata_command_slot(0, &command),
        Err(StoreError::MetadataCommandTerminalEntryPending { .. })
    ));
    assert!(pg.conn.is_autocommit());
    assert_eq!(snapshot(&pg.conn), before);
}
