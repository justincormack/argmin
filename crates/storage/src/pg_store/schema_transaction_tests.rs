// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Opening owns schema teardown; failed construction never publishes a store.

use super::*;
use crate::node_runtime::pg_store::{metadata_transaction::MetadataTransaction, PgStore};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

type Before = Box<dyn FnOnce(&mut PgStore) -> Result<(), StoreError>>;
type Effect = Box<dyn FnOnce(&PgStore) -> Result<(), StoreError>>;
type After = Box<dyn FnOnce(&PgStore, &Result<(), StoreError>)>;

#[derive(Default)]
struct Hooks {
    before: Option<Before>,
    effect: Option<Effect>,
    after: Option<After>,
}

thread_local! {
    static HOOKS: RefCell<Hooks> = RefCell::new(Hooks::default());
}

pub(super) fn before_initialize(pg: &mut PgStore) -> Result<(), StoreError> {
    let hook = HOOKS.with(|slot| slot.borrow_mut().before.take());
    hook.map_or(Ok(()), |hook| hook(pg))
}

pub(super) fn before_commit(pg: &PgStore) -> Result<(), StoreError> {
    let hook = HOOKS.with(|slot| slot.borrow_mut().effect.take());
    hook.map_or(Ok(()), |hook| hook(pg))
}

pub(super) fn after_initialize(pg: &PgStore, result: &Result<(), StoreError>) {
    if let Some(hook) = HOOKS.with(|slot| slot.borrow_mut().after.take()) {
        hook(pg, result);
    }
}

fn with_hooks<T>(hooks: Hooks, action: impl FnOnce() -> T) -> T {
    struct Reset(Option<Hooks>);
    impl Drop for Reset {
        fn drop(&mut self) {
            HOOKS.with(|slot| *slot.borrow_mut() = self.0.take().unwrap());
        }
    }
    let _reset = Reset(Some(HOOKS.with(|slot| slot.replace(hooks))));
    action()
}

fn user_objects(conn: &Connection) -> u32 {
    conn.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

fn memory_store() -> PgStore {
    PgStore::uninitialized(
        0,
        Default::default(),
        Default::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Operation,
    Unwind,
    Commit,
    DeferredCommit,
}

#[test]
fn schema_transaction_failed_open_closes_connection_and_retries_without_partial_schema() {
    for failure in [
        Failure::Operation,
        Failure::Unwind,
        Failure::Commit,
        Failure::DeferredCommit,
    ] {
        for failed_rollback in [false, true] {
            let dir = test_util::tempdir();
            let tmp = dir.path().join("tmp");
            std::fs::create_dir(&tmp).unwrap();
            let residue = tmp.join("unpublished-temp");
            std::fs::write(&residue, b"crash residue").unwrap();
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let hook_reached = Arc::clone(&reached);
            let checked = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let hook_checked = Arc::clone(&checked);
            let result = catch_unwind(AssertUnwindSafe(|| {
                with_hooks(
                    Hooks {
                        effect: Some(Box::new(move |pg| {
                            hook_reached.store(true, std::sync::atomic::Ordering::Relaxed);
                            assert!(!pg.conn.is_autocommit());
                            assert_eq!(pg_schema_version(&pg.conn).unwrap(), 13);
                            assert!(user_objects(&pg.conn) > 50, "no real schema effects");
                            let observer = Connection::open(pg.conn.path().unwrap()).unwrap();
                            assert_eq!(pg_schema_version(&observer).unwrap(), 0);
                            assert_eq!(user_objects(&observer), 0);
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            match failure {
                                Failure::Operation => {
                                    Err(StoreError::RouteCapabilitySubjectMismatch {
                                        operation: "injected schema construction failure",
                                    })
                                }
                                Failure::Unwind => panic!("injected schema construction unwind"),
                                Failure::Commit => {
                                    pg.fail_next_metadata_txn_commit
                                        .store(true, std::sync::atomic::Ordering::Relaxed);
                                    Ok(())
                                }
                                Failure::DeferredCommit => {
                                    pg.conn.execute_batch("CREATE TEMP TABLE deferred_schema (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_schema(id) DEFERRABLE INITIALLY DEFERRED); INSERT INTO deferred_schema VALUES (1, 2)").unwrap();
                                    Ok(())
                                }
                            }
                        })),
                        after: Some(Box::new(move |pg, result| {
                            hook_checked.store(true, std::sync::atomic::Ordering::Relaxed);
                            assert!(result.is_err());
                            assert_eq!(pg.conn.is_autocommit(), !failed_rollback);
                            if failed_rollback {
                                assert!(pg.require_usable_connection().is_err());
                                assert!(pg.require_live_allocation_lifetime().is_err());
                                assert_eq!(pg_schema_version(&pg.conn).unwrap(), 13);
                                let writes = pg.conn.total_changes();
                                assert!(initialize_inner(pg)
                                    .unwrap_err()
                                    .to_string()
                                    .contains("retired PG metadata connection"));
                                assert_eq!(pg.conn.total_changes(), writes);
                                assert!(!pg.conn.is_autocommit(), "hidden rollback retry");
                            } else {
                                pg.require_usable_connection().unwrap();
                                assert_eq!(pg_schema_version(&pg.conn).unwrap(), 0);
                                assert_eq!(user_objects(&pg.conn), 0);
                            }
                        })),
                        ..Hooks::default()
                    },
                    || PgStore::open(dir.path(), 0),
                )
            }));
            assert!(reached.load(std::sync::atomic::Ordering::Relaxed));
            if matches!(failure, Failure::Unwind) {
                let panic = result.err().expect("expected construction unwind");
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"injected schema construction unwind")
                );
                assert!(!checked.load(std::sync::atomic::Ordering::Relaxed));
            } else {
                assert!(checked.load(std::sync::atomic::Ordering::Relaxed));
                let error = result
                    .unwrap()
                    .err()
                    .expect("failed open must not return a store")
                    .to_string();
                assert!(
                    error.contains(if matches!(failure, Failure::DeferredCommit) {
                        "FOREIGN KEY constraint failed"
                    } else {
                        "injected"
                    }),
                    "{failure:?}: {error}"
                );
                if failed_rollback {
                    assert!(error.contains("rollback failure"), "{error}");
                }
            }
            assert_eq!(std::fs::read(&residue).unwrap(), b"crash residue");
            let observer = Connection::open(dir.path().join("metadata.db")).unwrap();
            observer.busy_timeout(std::time::Duration::ZERO).unwrap();
            // The failed unpublished connection is closed, including when its
            // explicit rollback failed. No writer lock or partial DDL remains.
            observer.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
            assert_eq!(pg_schema_version(&observer).unwrap(), 0);
            assert_eq!(user_objects(&observer), 0);
            drop(observer);
            let pg = PgStore::open(dir.path(), 0).unwrap();
            assert_eq!(pg_schema_version(&pg.conn).unwrap(), 13);
            pg.require_usable_connection().unwrap();
            pg.metadata_command_replica_state().unwrap();
            assert!(!residue.exists());
        }
    }
}

#[test]
fn schema_transaction_fresh_has_one_commit_and_current_reopen_has_none() {
    let dir = test_util::tempdir();
    for fresh in [true, false] {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&trace);
        let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_observed = Arc::clone(&observed);
        let pg = with_hooks(
            Hooks {
                before: Some(Box::new(move |pg| {
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
                    Ok(())
                })),
                after: Some(Box::new(move |pg, result| {
                    hook_observed.store(true, std::sync::atomic::Ordering::Relaxed);
                    result.as_ref().unwrap();
                    let statements = trace.lock().unwrap();
                    if fresh {
                        assert_eq!(*statements, ["BEGIN IMMEDIATE", "COMMIT"]);
                    } else {
                        assert!(
                            statements.is_empty(),
                            "current schema added a transaction: {statements:?}"
                        );
                    }
                    let observer = Connection::open(pg.conn.path().unwrap()).unwrap();
                    assert_eq!(pg_schema_version(&observer).unwrap(), 13);
                    assert_eq!(user_objects(&observer), user_objects(&pg.conn));
                })),
                ..Hooks::default()
            },
            || PgStore::open(dir.path(), 0),
        )
        .unwrap();
        assert!(observed.load(std::sync::atomic::Ordering::Relaxed));
        drop(pg);
    }
}

#[test]
fn schema_transaction_rejects_parent_and_retired_connection_before_pragmas() {
    for managed in [false, true] {
        let pg = memory_store();
        pg.conn.execute_batch("CREATE TABLE parent_canary(value); PRAGMA synchronous=OFF; PRAGMA foreign_keys=OFF").unwrap();
        let parent = if managed {
            Some(MetadataTransaction::begin(&pg, "schema parent").unwrap())
        } else {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            None
        };
        pg.conn
            .execute("INSERT INTO parent_canary VALUES (7)", [])
            .unwrap();
        let changes = pg.conn.total_changes();
        assert!(initialize_inner(&pg)
            .unwrap_err()
            .to_string()
            .contains("requires autocommit"));
        assert_eq!(pg.conn.total_changes(), changes);
        assert!(!pg.conn.is_autocommit());
        for pragma in ["foreign_keys", "synchronous"] {
            assert_eq!(
                pg.conn
                    .pragma_query_value(None, pragma, |row| row.get::<_, u32>(0))
                    .unwrap(),
                0
            );
        }
        if let Some(parent) = parent {
            parent.commit("parent").unwrap();
        } else {
            pg.commit_unmanaged_test_transaction("raw parent").unwrap();
        }
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM parent_canary", [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            7
        );
        pg.retire_connection();
        assert!(initialize_inner(&pg)
            .unwrap_err()
            .to_string()
            .contains("retired PG metadata connection"));
        assert_eq!(pg.conn.total_changes(), changes);
    }
}
