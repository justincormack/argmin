// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::MetadataError;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

fn setup() -> (test_util::TempDir, PgStore) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 4).unwrap();
    pg.conn
        .execute_batch("CREATE TABLE transaction_probe (value INTEGER PRIMARY KEY)")
        .unwrap();
    (dir, pg)
}

fn insert(pg: &PgStore, value: u32) {
    pg.conn
        .execute("INSERT INTO transaction_probe VALUES (?1)", [value])
        .unwrap();
}

fn values(pg: &PgStore) -> Vec<u32> {
    pg.conn
        .prepare("SELECT value FROM transaction_probe ORDER BY value")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn operation_error() -> MetadataError {
    MetadataError::InvariantViolation {
        context: "transaction test operation",
        reason: "injected operation failure".into(),
    }
}

#[test]
fn metadata_transaction_nested_success_has_one_outer_commit() {
    let (_dir, mut pg) = setup();
    let statements = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&statements);
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
    let outer = MetadataTransaction::begin(&pg, "outer").unwrap();
    insert(&pg, 1);
    let inner = MetadataTransaction::begin(&pg, "inner").unwrap();
    insert(&pg, 2);
    inner.commit("release").unwrap();
    assert!(!pg.conn.is_autocommit());
    assert_eq!(values(&pg), vec![1, 2]);
    // A second connection cannot observe either row after child release.
    let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
    let count = || {
        observer
            .query_row("SELECT count(*) FROM transaction_probe", [], |row| {
                row.get::<_, u32>(0)
            })
            .unwrap()
    };
    assert_eq!(count(), 0);
    outer.commit("commit").unwrap();
    assert_eq!(count(), 2);
    assert!(pg.conn.is_autocommit());
    assert!(!pg.metadata_transaction_scopes.has_active_scope());
    // Bootstrap also owns a scope. Pin the transaction sequence and exact
    // release identity, not the number of scopes used before this test began.
    let statements = statements.lock().unwrap();
    assert_eq!(statements.len(), 4);
    assert_eq!(statements[0], "BEGIN IMMEDIATE");
    let name = statements[1].strip_prefix("SAVEPOINT ").unwrap();
    assert!(name.starts_with("argmin_metadata_scope_"));
    assert_eq!(statements[2], format!("RELEASE {name}"));
    assert_eq!(statements[3], "COMMIT");
}

#[test]
fn metadata_transaction_nested_failure_preserves_only_outer_work() {
    let (_dir, pg) = setup();
    pg.with_immediate_txn("outer", "commit", |pg| {
        insert(pg, 1);
        let error = pg
            .with_immediate_txn("inner", "release", |pg| {
                insert(pg, 2);
                Err::<(), _>(operation_error())
            })
            .unwrap_err();
        assert!(error.to_string().contains("injected operation failure"));
        assert_eq!(values(pg), vec![1]);
        insert(pg, 3);
        Ok(())
    })
    .unwrap();
    assert_eq!(values(&pg), vec![1, 3]);
    pg.require_usable_connection().unwrap();
}

#[test]
fn metadata_transaction_failed_child_does_not_end_legacy_parent() {
    let (_dir, pg) = setup();
    // During migration, a shared helper also runs inside manual outer scopes.
    // It must own a child savepoint even when no managed parent ID exists.
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&pg, 1);
    let error = pg
        .with_immediate_txn("child", "release", |pg| {
            insert(pg, 2);
            Err::<(), _>(operation_error())
        })
        .unwrap_err();
    assert!(error.to_string().contains("injected operation failure"));
    assert!(!pg.conn.is_autocommit());
    assert!(!pg.metadata_transaction_scopes.has_active_scope());
    assert_eq!(values(&pg), vec![1]);
    pg.commit_unmanaged_test_transaction("legacy parent commit")
        .unwrap();
    assert!(pg.conn.is_autocommit());
    assert_eq!(values(&pg), vec![1]);
    pg.require_usable_connection().unwrap();
}

#[test]
fn metadata_transaction_unwind_rolls_back_owned_outer_or_child() {
    for nested in [false, true] {
        let (_dir, pg) = setup();
        let outer = nested.then(|| MetadataTransaction::begin(&pg, "outer").unwrap());
        if nested {
            insert(&pg, 1);
        }
        let panic = catch_unwind(AssertUnwindSafe(|| {
            pg.with_immediate_txn("panic", "commit", |pg| -> Result<(), MetadataError> {
                insert(pg, 2);
                panic!("transaction body panic")
            })
            .unwrap();
        }));
        assert!(panic.is_err());
        if let Some(outer) = outer {
            assert_eq!(values(&pg), vec![1]);
            outer.commit("commit").unwrap();
        } else {
            assert!(values(&pg).is_empty());
        }
        assert!(pg.conn.is_autocommit());
        pg.require_usable_connection().unwrap();
    }
}

#[test]
fn metadata_transaction_uncertain_child_blocks_outer_commit_and_retires_until_reopen() {
    for unwind in [false, true] {
        let (dir, pg) = setup();
        let outer = MetadataTransaction::begin(&pg, "outer").unwrap();
        insert(&pg, 1);
        pg.test_fail_metadata_command_rollbacks(1);
        let attempted = catch_unwind(AssertUnwindSafe(|| {
            pg.with_immediate_txn("inner", "release", |pg| {
                insert(pg, 2);
                if unwind {
                    panic!("child panic");
                }
                Err::<(), _>(operation_error())
            })
        }));
        if unwind {
            assert!(attempted.is_err());
        } else {
            let error = attempted.unwrap().unwrap_err().to_string();
            assert!(error.contains("injected operation failure"), "{error}");
            assert!(
                error.contains("injected metadata-command rollback failure"),
                "{error}"
            );
        }
        assert!(pg.require_usable_connection().is_err());
        assert!(pg.require_live_allocation_lifetime().is_err());
        assert!(outer.commit("must not commit").is_err());
        assert!(pg
            .commit_unmanaged_test_transaction("legacy must not commit")
            .is_err());
        assert!(
            !pg.conn.is_autocommit(),
            "a hidden rollback retried teardown"
        );
        assert_eq!(
            values(&pg),
            vec![1, 2],
            "owner-local poisoned-view evidence"
        );
        // Even a later successful raw rollback cannot restore node eligibility.
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(pg.require_usable_connection().is_err());
        drop(pg);
        // Drop the test-only extra schema before normal PgStore schema validation.
        let connection = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM transaction_probe", [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            0
        );
        connection
            .execute_batch("DROP TABLE transaction_probe")
            .unwrap();
        drop(connection);
        PgStore::open(dir.path(), 4)
            .unwrap()
            .require_usable_connection()
            .unwrap();
    }
}

#[test]
fn metadata_transaction_commit_failure_rolls_back_or_retires_without_hidden_retry() {
    for failed_rollback in [false, true] {
        let (_dir, pg) = setup();
        // Real deferred-constraint failure, not only an injected COMMIT result.
        pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
        let transaction = MetadataTransaction::begin(&pg, "begin").unwrap();
        insert(&pg, 1);
        pg.conn
            .execute("INSERT INTO deferred_failure VALUES (1, 2)", [])
            .unwrap();
        if failed_rollback {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let error = transaction
            .commit("commit deferred failure")
            .unwrap_err()
            .to_string();
        assert!(error.contains("commit deferred failure"), "{error}");
        if failed_rollback {
            assert!(
                error.contains("injected metadata-command rollback failure"),
                "{error}"
            );
            assert!(pg.require_usable_connection().is_err());
            assert!(!pg.conn.is_autocommit());
        } else {
            pg.require_usable_connection().unwrap();
            assert!(pg.conn.is_autocommit());
            assert!(values(&pg).is_empty());
        }
    }
}

#[test]
fn metadata_transaction_cannot_release_parent_before_child_or_commit_via_legacy_adapter() {
    let (_dir, pg) = setup();
    let outer = MetadataTransaction::begin(&pg, "outer").unwrap();
    insert(&pg, 1);
    assert!(pg
        .commit_unmanaged_test_transaction("legacy commit")
        .is_err());
    assert!(!pg.conn.is_autocommit());
    let inner = MetadataTransaction::begin(&pg, "inner").unwrap();
    insert(&pg, 2);
    assert!(outer.commit("out of order").is_err());
    assert!(pg.require_usable_connection().is_err());
    assert!(inner.commit("cannot commit retired child").is_err());
    assert!(!pg.conn.is_autocommit());
}
