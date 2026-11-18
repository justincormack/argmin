// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only component ownership tests. No physical payload or live
//! cleanup authority is minted by these fixtures.

use super::super::super::metadata_transaction::MetadataTransaction;
use super::super::command_context::ContextualCommand;
use super::*;
use crate::node_runtime::pg_store::PgStore;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

const CASES: usize = 6 + application::TERMINALS.len();

fn setup(case: usize) -> (test_util::TempDir, PgStore, ContextualCommand) {
    if case < 6 {
        // Reserve/create/append for every target, plus source-bearing part
        // abandonment (not the unrelated ReserveObjectVersion fixture).
        return complete_pending::setup(if case == 5 { 6 } else { case });
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let command = application::setup_terminal(&pg, application::TERMINALS[case - 6]);
    let context = application::ordinary_context(&command, &topology()).unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    (dir, pg, context)
}

fn parent_values(pg: &PgStore) -> Vec<u32> {
    pg.conn
        .prepare("SELECT value FROM component_parent ORDER BY value")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn staging_component_success_has_unique_child_scopes_and_one_outer_commit() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for case in 0..CASES {
        let (dir, mut pg, context) = setup(case);
        let before = introduction::state(&pg);
        let revision_before = pg.metadata_digest_revision().unwrap();
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
        let parent = MetadataTransaction::begin(&pg, "component parent").unwrap();
        context
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        let after = introduction::state(&pg);
        assert_ne!(after, before, "{case}: no actual component mutation");
        assert!(!pg.conn.is_autocommit());
        let revision_after = pg.metadata_digest_revision().unwrap();
        assert!(revision_after > revision_before);
        let observer = rusqlite::Connection::open_with_flags(
            pg.conn.path().unwrap(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let visible_revision = || {
            observer
                .query_row(
                    "SELECT revision FROM metadata_digest_revision WHERE singleton = 0",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap()
        };
        assert_eq!(
            visible_revision(),
            revision_before,
            "{case}: child release published parent"
        );
        parent.commit("component parent commit").unwrap();
        assert_eq!(visible_revision(), revision_after);
        drop(observer);
        let statements = statements.lock().unwrap();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("BEGIN"))
                .count(),
            1,
            "{case}"
        );
        assert_eq!(
            statements.iter().filter(|sql| *sql == "COMMIT").count(),
            1,
            "{case}"
        );
        assert!(
            !statements.iter().any(|sql| sql.starts_with("ROLLBACK")),
            "{case}"
        );
        let scopes = statements
            .iter()
            .filter_map(|sql| sql.strip_prefix("SAVEPOINT "))
            .collect::<Vec<_>>();
        assert!(!scopes.is_empty(), "{case}");
        assert_eq!(
            scopes.iter().copied().collect::<BTreeSet<_>>().len(),
            scopes.len(),
            "{case}: reused scope identity"
        );
        for scope in &scopes {
            assert!(
                scope.starts_with("argmin_metadata_scope_"),
                "{case}: {scope}"
            );
            assert_eq!(
                statements
                    .iter()
                    .filter(|sql| **sql == format!("RELEASE {scope}"))
                    .count(),
                1,
                "{case}"
            );
        }
        drop(statements);
        let pg_id = pg.pg_id;
        drop(pg);
        let reopened = PgStore::open(dir.path(), pg_id).unwrap();
        assert_eq!(introduction::state(&reopened), after, "{case}");
    }
}

#[test]
fn staging_component_error_and_unwind_preserve_parent_or_retire_until_reopen() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for case in 0..CASES {
        for managed in [false, true] {
            for unwind in [false, true] {
                for fail_rollback in [false, true] {
                    let (dir, pg, context) = setup(case);
                    pg.conn
                        .execute_batch("CREATE TABLE component_parent (value INTEGER PRIMARY KEY)")
                        .unwrap();
                    let before = introduction::state(&pg);
                    let mut parent = if managed {
                        Some(MetadataTransaction::begin(&pg, "component parent").unwrap())
                    } else {
                        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                        None
                    };
                    pg.conn
                        .execute_batch("INSERT INTO component_parent VALUES (7)")
                        .unwrap();
                    let hit = Arc::new(AtomicBool::new(false));
                    let called = Arc::clone(&hit);
                    let initial = before.clone();
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        sql::test_with_before_component_finish(
                            move |pg| {
                                assert_ne!(
                                    introduction::state(pg),
                                    initial,
                                    "{case}: hook preceded actual mutation"
                                );
                                called.store(true, Ordering::SeqCst);
                                if fail_rollback {
                                    // One attempt only, even with enclosing component
                                    // scopes and a managed parent still on the stack.
                                    pg.test_fail_metadata_command_rollbacks(2);
                                }
                                assert!(!unwind, "injected component unwind");
                                Err(sql::PersistenceError::Row(
                                    "injected component finish failure",
                                ))
                            },
                            || {
                                context
                                    .application(&topology())
                                    .unwrap()
                                    .apply_in_open_transaction(&pg, 0)
                            },
                        )
                    }));
                    assert!(hit.load(Ordering::SeqCst), "{case}: fault was not reached");
                    if unwind {
                        assert!(result.is_err(), "{case}");
                    } else {
                        let error = result.unwrap().unwrap_err().to_string();
                        assert!(
                            error.contains("injected component finish failure"),
                            "{case}: {error}"
                        );
                        assert_eq!(
                            error.contains("rollback failed"),
                            fail_rollback,
                            "{case}: {error}"
                        );
                    }
                    assert!(!pg.conn.is_autocommit());
                    assert_eq!(
                        parent_values(&pg),
                        [7],
                        "{case}: child discarded parent work"
                    );
                    if fail_rollback {
                        assert!(pg.require_usable_connection().is_err());
                        assert!(pg.stream_allocation_issuer(NodeId::new(0)).is_err());
                        assert_eq!(pg.fail_metadata_command_rollbacks.get(), 1);
                        assert!(pg
                            .commit_unmanaged_test_transaction("must not commit partial component")
                            .is_err());
                        if let Some(parent) = parent.take() {
                            assert!(parent.commit("managed parent must not commit").is_err());
                        }
                        assert_eq!(
                            pg.fail_metadata_command_rollbacks.get(),
                            1,
                            "hidden teardown retry"
                        );
                        assert!(!pg.conn.is_autocommit());
                        pg.conn.execute_batch("ROLLBACK").unwrap();
                        assert!(
                            pg.require_usable_connection().is_err(),
                            "raw rollback revived store"
                        );
                    } else {
                        pg.require_usable_connection().unwrap();
                        assert_eq!(
                            introduction::state(&pg),
                            before,
                            "{case}: partial component survived"
                        );
                        if let Some(parent) = parent.take() {
                            parent.commit("commit preserved parent only").unwrap();
                        } else {
                            pg.commit_unmanaged_test_transaction(
                                "commit preserved legacy parent only",
                            )
                            .unwrap();
                        }
                    }
                    drop(parent);
                    let pg_id = pg.pg_id;
                    drop(pg);
                    let reopened = PgStore::open(dir.path(), pg_id).unwrap();
                    reopened.require_usable_connection().unwrap();
                    reopened.stream_allocation_issuer(NodeId::new(0)).unwrap();
                    assert_eq!(
                        introduction::state(&reopened),
                        before,
                        "{case}: failed component committed"
                    );
                    assert_eq!(
                        parent_values(&reopened),
                        if fail_rollback { vec![] } else { vec![7] }
                    );
                    let parent = MetadataTransaction::begin(&reopened, "retry component").unwrap();
                    context
                        .application(&topology())
                        .unwrap()
                        .apply_in_open_transaction(&reopened, 0)
                        .unwrap();
                    parent.commit("commit retry component").unwrap();
                    assert_ne!(
                        introduction::state(&reopened),
                        before,
                        "{case}: retry made no progress"
                    );
                }
            }
        }
    }
}

#[test]
fn staging_component_insert_binds_actual_store_and_requires_parent() {
    let owner = fixture(StagingTarget::DirectPut);
    let topology = topology();
    let checked = owner.validate(&topology).unwrap();
    for crossed_store in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            owner.subject.object_pg.get() + u32::from(crossed_store),
        )
        .unwrap();
        assert!(matches!(
            sql::CommandApplySavepoint::begin(&pg),
            Err(sql::PersistenceError::TransactionRequired)
        ));
        let parent = MetadataTransaction::begin(&pg, "owner insert").unwrap();
        let writes = pg.conn.total_changes();
        let result = sql::insert(&pg, owner.subject.object_pg, &topology, &checked);
        if crossed_store {
            assert!(matches!(result, Err(sql::PersistenceError::WrongPg)));
            assert_eq!(pg.conn.total_changes(), writes);
        } else {
            result.unwrap();
            assert!(pg.conn.total_changes() > writes);
        }
        parent.rollback().unwrap();
    }
}
