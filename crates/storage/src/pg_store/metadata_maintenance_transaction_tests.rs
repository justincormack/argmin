// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bootstrap and both log grammars share owned teardown without extra commits.
//! These fixtures contain real bucket metadata, not synthetic payloads.

use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Kind {
    Bootstrap,
    CurrentCompaction,
    CompleteCompaction,
}
const KINDS: [Kind; 3] = [
    Kind::Bootstrap,
    Kind::CurrentCompaction,
    Kind::CompleteCompaction,
];

fn topology() -> crate::PgTopology {
    crate::PgTopology::new(&[1]).unwrap()
}

impl Kind {
    fn run(self, pg: &PgStore) -> Result<(), StoreError> {
        match self {
            Self::Bootstrap => pg.ensure_metadata_digest_bootstrap(),
            Self::CurrentCompaction => pg
                .compact_metadata_command_log(ClusterEpoch::INITIAL)
                .map(|_| ()),
            Self::CompleteCompaction => pg
                .compact_complete_metadata_command_log(&topology())
                .map(|_| ()),
        }
    }

    fn with_hook(
        self,
        pg: &PgStore,
        hook: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        use complete_command_log_maintenance::LogCompaction;
        let topology = topology();
        match self {
            Self::Bootstrap => pg.ensure_metadata_digest_bootstrap_inner(hook),
            Self::CurrentCompaction | Self::CompleteCompaction => pg
                .compact_metadata_command_log_with_grammar_inner(
                    ClusterEpoch::INITIAL,
                    2,
                    if matches!(self, Self::CurrentCompaction) {
                        LogCompaction::Current
                    } else {
                        LogCompaction::Fixture(&topology)
                    },
                    hook,
                )
                .map(|_| ()),
        }
    }
}

fn setup(kind: Kind) -> (test_util::TempDir, PgStore) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 1).unwrap();
    for index in 1..=3 {
        let owner = OwnerIdentity::from_principal("owner");
        let name = format!("maintenance-{index}");
        let config = CreateBucketConfig {
            name: &name,
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&config, 123, 1).unwrap(),
            ),
        );
        if matches!(kind, Kind::CompleteCompaction) {
            let context =
                payload_staging::ContextualCommand::decode_field(command, &[1, 0, 0], &topology())
                    .unwrap();
            pg.apply_complete_metadata_command_and_record(
                0,
                &context.encode_command().unwrap(),
                &topology(),
            )
            .unwrap();
        } else {
            pg.apply_metadata_command_and_record(0, &command).unwrap();
        }
    }
    match kind {
        Kind::Bootstrap => {
            for table in METADATA_DIGEST_TABLES {
                for suffix in ["ai", "ad", "au"] {
                    pg.conn
                        .execute_batch(&format!(
                            "DROP TRIGGER metadata_digest_{}_{suffix}",
                            table.name
                        ))
                        .unwrap();
                }
            }
            pg.conn
                .execute_batch("DELETE FROM metadata_table_digests; UPDATE metadata_digest_bootstrap_state SET completed = 0")
                .unwrap();
        }
        Kind::CurrentCompaction => {
            pg.record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap();
        }
        Kind::CompleteCompaction => {
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
        }
    }
    (dir, pg)
}

fn snapshot(conn: &rusqlite::Connection) -> Vec<Vec<Vec<String>>> {
    [
        "SELECT * FROM metadata_command_replica_state",
        "SELECT * FROM metadata_digest_bootstrap_state",
        "SELECT * FROM metadata_table_digests ORDER BY table_name",
        "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
        "SELECT * FROM buckets ORDER BY name",
        "SELECT * FROM metadata_command_log ORDER BY cluster_epoch, pg_id, log_index",
        "SELECT * FROM metadata_command_terminal_receipts ORDER BY cluster_epoch, pg_id, log_index",
        "SELECT * FROM metadata_command_checkpoints ORDER BY cluster_epoch, pg_id, applied_log_index",
    ]
    .map(|sql| {
        let mut statement = conn.prepare(sql).unwrap();
        let count = statement.column_count();
        statement
            .query_map([], |row| {
                (0..count)
                    .map(|index| row.get_ref(index).map(|value| format!("{value:?}")))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    })
    .into()
}

#[test]
fn maintenance_transaction_commits_once_with_private_effects_and_reopens() {
    for kind in KINDS {
        let (dir, mut pg) = setup(kind);
        let before = snapshot(&pg.conn);
        let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
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
        let mut reached = false;
        kind.with_hook(&pg, || {
            reached = true;
            assert!(!pg.conn.is_autocommit());
            assert_ne!(snapshot(&pg.conn), before, "no effects for {kind:?}");
            assert_eq!(snapshot(&observer), before, "effects escaped {kind:?}");
            assert!(pg
                .commit_unmanaged_test_transaction("crossed maintenance commit")
                .unwrap_err()
                .to_string()
                .contains("owning metadata transaction scope"));
            Ok(())
        })
        .unwrap();
        assert!(reached);
        assert!(pg.conn.is_autocommit());
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
        assert!(!statements.iter().any(|sql| sql.starts_with("ROLLBACK")));
        drop(statements);
        let after = snapshot(&pg.conn);
        let writes = pg.conn.total_changes();
        if matches!(kind, Kind::Bootstrap) {
            kind.run(&pg).unwrap();
            assert_eq!(pg.conn.total_changes(), writes, "completed bootstrap wrote");
        }
        drop(observer);
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(snapshot(&pg.conn), after);
        kind.run(&pg).unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Operation,
    Unwind,
    Commit,
}

#[test]
fn maintenance_transaction_error_unwind_and_commit_failure_rollback_or_retire() {
    for kind in KINDS {
        for fault in [Fault::Operation, Fault::Unwind, Fault::Commit] {
            for failed_rollback in [false, true] {
                let (dir, pg) = setup(kind);
                let before = snapshot(&pg.conn);
                pg.conn.execute_batch("CREATE TEMP TABLE maintenance_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES maintenance_commit_fault(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                let mut reached = false;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    kind.with_hook(&pg, || {
                        reached = true;
                        assert_ne!(snapshot(&pg.conn), before);
                        if failed_rollback {
                            pg.test_fail_metadata_command_rollbacks(1);
                        }
                        match fault {
                            Fault::Operation => Err(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "injected maintenance operation failure",
                            }),
                            Fault::Unwind => panic!("injected maintenance unwind"),
                            Fault::Commit => {
                                pg.conn
                                    .execute(
                                        "INSERT INTO maintenance_commit_fault VALUES (1, 2)",
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
                    let detail = error.to_string();
                    assert!(
                        detail.contains(if matches!(fault, Fault::Commit) {
                            "FOREIGN KEY constraint failed"
                        } else {
                            "injected maintenance operation failure"
                        }),
                        "{detail}"
                    );
                    if failed_rollback {
                        assert!(
                            detail.contains("injected metadata-command rollback failure"),
                            "{detail}"
                        );
                    } else if matches!(fault, Fault::Operation) {
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
                    assert!(kind.run(&pg).is_err(), "retired store reused");
                    assert!(!pg.conn.is_autocommit());
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(
                        pg.require_usable_connection().is_err(),
                        "raw rollback revived store"
                    );
                } else {
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    pg.require_live_allocation_lifetime().unwrap();
                }
                assert_eq!(snapshot(&pg.conn), before);
                drop(pg);
                let pg = PgStore::open(dir.path(), 1).unwrap();
                // Open itself resumes incomplete bootstrap. Compaction is retried
                // explicitly; its log/receipts must still be the pre-operation state.
                if !matches!(kind, Kind::Bootstrap) {
                    assert_eq!(snapshot(&pg.conn), before);
                }
                kind.run(&pg).unwrap();
                pg.require_live_allocation_lifetime().unwrap();
                pg.validate_metadata_digest_bootstrap().unwrap();
            }
        }
    }
}

#[test]
fn maintenance_transaction_actual_body_failure_restores_partial_changes() {
    for kind in KINDS {
        for failed_rollback in [false, true] {
            let (dir, pg) = setup(kind);
            let before = snapshot(&pg.conn);
            let sql = if matches!(kind, Kind::Bootstrap) {
                "CREATE TEMP TRIGGER fail_maintenance BEFORE UPDATE ON metadata_digest_bootstrap_state BEGIN SELECT RAISE(ABORT, 'bootstrap body failure'); END"
            } else {
                "CREATE TEMP TRIGGER fail_maintenance BEFORE DELETE ON metadata_command_log BEGIN SELECT RAISE(ABORT, 'compaction body failure'); END"
            };
            pg.conn.execute_batch(sql).unwrap();
            if failed_rollback {
                pg.test_fail_metadata_command_rollbacks(1);
            }
            let error = kind.run(&pg).unwrap_err().to_string();
            assert!(error.contains("body failure"), "{error}");
            if failed_rollback {
                assert!(
                    error.contains("injected metadata-command rollback failure"),
                    "{error}"
                );
                assert_ne!(snapshot(&pg.conn), before, "must fail after real writes");
                assert!(pg.require_usable_connection().is_err());
                assert!(!pg.conn.is_autocommit());
                pg.conn.execute_batch("ROLLBACK").unwrap();
            }
            assert_eq!(snapshot(&pg.conn), before);
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            kind.run(&pg).unwrap();
        }
    }
}

#[test]
fn maintenance_transaction_rejects_caller_scope_without_finishing_it() {
    for kind in KINDS {
        let (_dir, pg) = setup(kind);
        let before = snapshot(&pg.conn);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let writes = pg.conn.total_changes();
        assert!(kind.run(&pg).is_err());
        assert!(!pg.conn.is_autocommit());
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(snapshot(&pg.conn), before);
        pg.require_usable_connection().unwrap();
        pg.conn.execute_batch("ROLLBACK").unwrap();
        kind.run(&pg).unwrap();
    }
}
