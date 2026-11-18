// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::engine::SharedStorageNode;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{Arc, Mutex};

fn read_only(pg: &PgStore) -> bool {
    pg.conn
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .unwrap()
}

fn canary(pg: &PgStore) -> i64 {
    pg.conn
        .query_row("SELECT value FROM builder_snapshot_canary", [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn fixture() -> (test_util::TempDir, PgStore) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    // Owner-local SQL isolation canary, not a production format or payload fixture.
    pg.conn
        .execute_batch("CREATE TABLE builder_snapshot_canary(value INTEGER NOT NULL); INSERT INTO builder_snapshot_canary VALUES (1)")
        .unwrap();
    (dir, pg)
}

fn assert_restored(pg: &PgStore) {
    assert!(pg.conn.is_autocommit());
    assert!(!read_only(pg));
    assert_eq!(canary(pg), 1);
    pg.conn
        .execute("UPDATE builder_snapshot_canary SET value = 2", [])
        .unwrap();
    assert_eq!(canary(pg), 2);
}

#[test]
fn command_build_snapshot_rejects_writes_and_restores_connection_on_success() {
    let (_dir, pg) = fixture();
    let writes = pg.conn.total_changes();
    let snapshot = pg.command_build_snapshot().unwrap();
    assert!(!pg.conn.is_autocommit());
    assert!(read_only(&pg));
    assert_eq!(canary(&pg), 1);
    assert_eq!(
        pg.conn.transaction_state(Some(rusqlite::MAIN_DB)).unwrap(),
        rusqlite::TransactionState::Read
    );
    let error = pg
        .conn
        .execute("UPDATE builder_snapshot_canary SET value = 2", [])
        .unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::ReadOnly)
    );
    assert_eq!(pg.conn.total_changes(), writes);
    snapshot.finish().unwrap();
    assert_restored(&pg);
}

#[test]
fn command_build_snapshot_restores_connection_on_error_and_unwind() {
    for unwind in [false, true] {
        let (_dir, pg) = fixture();
        let writes = pg.conn.total_changes();
        if unwind {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(canary(&pg), 1);
                panic!("builder unwind canary");
            }))
            .is_err());
        } else {
            let build = || -> Result<(), rusqlite::Error> {
                let _snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(canary(&pg), 1);
                pg.conn
                    .execute("SELECT missing_column FROM builder_snapshot_canary", [])?;
                Ok(())
            };
            assert!(build().is_err());
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_restored(&pg);
    }
}

#[test]
fn command_build_snapshot_failed_rollback_retires_store_on_finish_error_and_unwind() {
    for exit in ["finish", "error", "unwind"] {
        let (dir, pg) = fixture();
        let writes = pg.conn.total_changes();
        pg.fail_next_snapshot_rollback.set(true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let snapshot = pg.command_build_snapshot().unwrap();
            assert_eq!(canary(&pg), 1);
            match exit {
                "finish" => snapshot.finish(),
                "error" => Err(MetadataError::InvariantViolation {
                    context: "snapshot teardown regression",
                    reason: "injected builder failure".into(),
                }),
                "unwind" => panic!("snapshot teardown unwind regression"),
                _ => unreachable!(),
            }
        }));
        if exit == "unwind" {
            assert!(result.is_err());
        } else {
            let error = result.unwrap().unwrap_err();
            assert!(error.to_string().contains(if exit == "finish" {
                "injected read-snapshot rollback failure"
            } else {
                "injected builder failure"
            }));
        }
        assert!(
            !pg.conn.is_autocommit(),
            "failed rollback was hidden by a retry"
        );
        assert!(!read_only(&pg));
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.require_usable_connection().is_err());
        assert!(pg.stream_allocation_issuer(crate::NodeId::new(0)).is_err());
        assert!(pg.command_build_snapshot().is_err());
        // Ending the stale transaction is not permission to reuse the store.
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(pg.conn.is_autocommit());
        assert!(pg.require_usable_connection().is_err());
        assert!(pg.command_build_snapshot().is_err());
        drop(pg);
        let pg = PgStore::open(dir.path(), 0).unwrap();
        pg.require_usable_connection().unwrap();
        pg.command_build_snapshot().unwrap().finish().unwrap();
        assert_restored(&pg);
    }
}

#[test]
fn command_build_snapshot_rejects_nesting_without_touching_caller_transaction() {
    let (_dir, pg) = fixture();
    pg.conn
        .execute_batch("BEGIN IMMEDIATE; UPDATE builder_snapshot_canary SET value = 2")
        .unwrap();
    assert!(matches!(
        pg.command_build_snapshot(),
        Err(MetadataError::InvariantViolation {
            context: "primary metadata command build snapshot",
            ..
        })
    ));
    assert!(!pg.conn.is_autocommit());
    assert!(!read_only(&pg));
    assert_eq!(canary(&pg), 2);
    pg.conn.execute_batch("COMMIT").unwrap();
    assert_eq!(canary(&pg), 2);
}

#[test]
fn command_build_snapshot_preserves_preexisting_read_only_mode() {
    for explicit_finish in [false, true] {
        let (_dir, pg) = fixture();
        pg.conn.pragma_update(None, "query_only", true).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        assert_eq!(canary(&pg), 1);
        if explicit_finish {
            snapshot.finish().unwrap();
        } else {
            drop(snapshot);
        }
        assert!(pg.conn.is_autocommit());
        assert!(read_only(&pg));
        let error = pg
            .conn
            .execute("UPDATE builder_snapshot_canary SET value = 2", [])
            .unwrap_err();
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::ReadOnly)
        );
    }
}

#[test]
fn command_build_snapshot_keeps_one_view_across_wal_publication() {
    let (dir, pg) = fixture();
    // A raw connection to the canary only: do not reopen a second PG runtime or
    // mutate production metadata without its digest functions.
    let writer = Connection::open(dir.path().join("metadata.db")).unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    assert_eq!(canary(&pg), 1); // Pins the deferred snapshot.
    writer
        .execute("UPDATE builder_snapshot_canary SET value = 2", [])
        .unwrap();
    assert_eq!(canary(&pg), 1, "builder must not mix two committed views");
    snapshot.finish().unwrap();
    assert_eq!(canary(&pg), 2);
}

/// Owner-local instrumentation shared with real embedded/Unix builder tests.
/// Observes the SQL executed by the actual PG, rather than a test-only builder.
pub(in crate::node_runtime) struct BuilderSnapshotProbe {
    node: Arc<SharedStorageNode>,
    pg_id: u32,
    statements: Arc<Mutex<Vec<String>>>,
}

impl BuilderSnapshotProbe {
    pub(in crate::node_runtime) fn install(node: &Arc<SharedStorageNode>, pg_id: u32) -> Self {
        Self::install_with_before_rollback(node, pg_id, || {})
    }

    pub(in crate::node_runtime) fn install_with_before_rollback(
        node: &Arc<SharedStorageNode>,
        pg_id: u32,
        action: impl Fn() + Send + 'static,
    ) -> Self {
        let statements = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&statements);
        node.get_pg(pg_id)
            .unwrap()
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        captured.lock().unwrap().push(sql.to_owned());
                        if sql == "ROLLBACK" {
                            action();
                        }
                    }
                }),
            )
            .unwrap();
        Self {
            node: Arc::clone(node),
            pg_id,
            statements,
        }
    }

    pub(in crate::node_runtime) fn assert_finished(self) {
        let (autocommit, query_only) = {
            let pg = self.node.get_pg(self.pg_id).unwrap();
            (pg.conn.is_autocommit(), read_only(&pg))
        };
        assert!(autocommit, "builder leaked its SQL snapshot");
        assert!(!query_only, "builder leaked query_only mode");
        let statements = self.statements.lock().unwrap().clone();
        let transaction_events: Vec<_> = statements
            .iter()
            .filter(|sql| {
                ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"]
                    .iter()
                    .any(|prefix| sql.starts_with(prefix))
            })
            .map(String::as_str)
            .collect();
        assert_eq!(
            transaction_events,
            ["BEGIN DEFERRED", "ROLLBACK"],
            "{statements:?}"
        );
        let begin = statements
            .iter()
            .position(|sql| sql == "BEGIN DEFERRED")
            .unwrap();
        let end = statements.iter().position(|sql| sql == "ROLLBACK").unwrap();
        assert!(
            statements[begin + 1..end]
                .iter()
                .any(|sql| sql.trim_start().starts_with("SELECT")),
            "snapshot must enclose real builder reads: {statements:?}"
        );
    }
}

impl Drop for BuilderSnapshotProbe {
    fn drop(&mut self) {
        self.node
            .get_pg(self.pg_id)
            .unwrap()
            .conn
            .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
            .unwrap();
    }
}
