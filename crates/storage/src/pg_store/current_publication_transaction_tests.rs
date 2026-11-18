// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Live command publication: trial effects never escape, only the marker commits.
//! Metadata-only fixtures; no complete-command activation or physical authority.

use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

fn setup(applied: bool) -> (test_util::TempDir, PgStore, MetadataCommandEnvelope) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 1).unwrap();
    let bucket = crate::tests::bucket_name("current-publication");
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
            crate::tests::stream_session_id("publish"),
            GenerationId::MIN,
            1_000,
        )),
    );
    pg.try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    if applied {
        pg.apply_metadata_command_and_record(0, &command).unwrap();
    }
    (dir, pg, command)
}

#[derive(Debug, PartialEq, Eq)]
struct WithoutMarker {
    materialized: Vec<MetadataCheckpointTableBlock>,
    prefix: MetadataCommandReplicaState,
    pending_and_log: Vec<Vec<Vec<String>>>,
}

fn without_marker(pg: &PgStore) -> WithoutMarker {
    let pending_and_log = [
        "SELECT cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket, placed_segment_reference_count, reclaim_reference_count FROM metadata_command_pending_slot",
        "SELECT * FROM metadata_command_pending_route_dependencies",
        "SELECT * FROM metadata_command_pending_staging_protection",
        "SELECT * FROM metadata_command_pending_placed_reference_pages ORDER BY page_index",
        "SELECT * FROM metadata_command_pending_reclaim_reference_pages ORDER BY page_index",
        "SELECT * FROM metadata_command_log ORDER BY cluster_epoch, pg_id, log_index",
    ].map(|sql| {
        let mut stmt = pg.conn.prepare(sql).unwrap();
        let columns = stmt.column_count();
        stmt.query_map([], |row| {
            (0..columns).map(|i| row.get_ref(i).map(|value| format!("{value:?}"))).collect::<Result<Vec<_>, _>>()
        }).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    }).into();
    WithoutMarker {
        materialized: pg.metadata_checkpoint_table_blocks().unwrap(),
        prefix: pg.metadata_command_replica_state().unwrap(),
        pending_and_log,
    }
}

fn started(conn: &rusqlite::Connection) -> bool {
    conn.query_row(
        "SELECT publication_started FROM metadata_command_pending_slot",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

fn reservations(pg: &PgStore) -> u32 {
    pg.conn
        .query_row(
            "SELECT count(*) FROM object_generation_reservations",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn detail(error: &BucketSnapshotLoadError) -> String {
    match error {
        BucketSnapshotLoadError::Store(error) => error.to_string(),
        BucketSnapshotLoadError::Metadata(error) => error.to_string(),
    }
}

fn injected_error() -> StoreError {
    StoreError::RouteMapExpired {
        cluster_epoch: ClusterEpoch::INITIAL,
        valid_until_ms: 10,
        now_ms: 10,
    }
}

#[test]
fn current_publication_transaction_trials_are_private_and_only_marker_commits() {
    for applied in [false, true] {
        let (dir, mut pg, command) = setup(applied);
        let before = without_marker(&pg);
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
        for retry in [false, true] {
            let mut stages = Vec::new();
            pg.mark_pending_metadata_command_publication_started_inner(0, &command, |stage| {
                stages.push(stage);
                assert_eq!(pg.conn.is_autocommit(), stage == "after-commit");
                if stage != "after-commit" {
                    let error = pg
                        .commit_unmanaged_test_transaction("crossed current publication commit")
                        .unwrap_err();
                    assert!(error
                        .to_string()
                        .contains("owning metadata transaction scope"));
                    assert_eq!(started(&observer), retry, "uncommitted marker escaped");
                }
                match stage {
                    "after-trial" => {
                        assert_eq!(reservations(&pg), 1, "trial must actually apply");
                        if !applied {
                            assert_ne!(without_marker(&pg), before);
                        }
                        let visible: u32 = observer
                            .query_row(
                                "SELECT count(*) FROM object_generation_reservations",
                                [],
                                |row| row.get(0),
                            )
                            .unwrap();
                        assert_eq!(visible, u32::from(applied), "trial effects escaped");
                    }
                    "before-marker" => {
                        assert_eq!(without_marker(&pg), before);
                        assert_eq!(started(&pg.conn), retry);
                    }
                    "before-commit" | "after-commit" => {
                        assert_eq!(without_marker(&pg), before);
                        assert!(started(&pg.conn));
                    }
                    _ => panic!("unexpected stage {stage}"),
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(
                stages,
                [
                    "after-trial",
                    "before-marker",
                    "before-commit",
                    "after-commit"
                ]
            );
            assert_eq!(without_marker(&pg), before);
        }
        let statements = trace.lock().unwrap();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("BEGIN"))
                .count(),
            2
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == "COMMIT")
                .count(),
            2
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("ROLLBACK TO"))
                .count(),
            2
        );
        assert!(!statements.iter().any(|sql| sql == "ROLLBACK"));
        drop(statements);
        drop(observer);
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(without_marker(&pg), before);
        assert!(started(&pg.conn));
        pg.mark_pending_metadata_command_publication_started(0, &command)
            .unwrap();
        pg.apply_metadata_command_and_record(0, &command).unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    TrialRollback,
    TrialError,
    TrialUnwind,
    MarkerError,
    MarkerUnwind,
    Commit,
}

#[test]
fn current_publication_transaction_failure_unwind_and_commit_rollback_or_retire() {
    for applied in [false, true] {
        for fault in [
            Fault::TrialRollback,
            Fault::TrialError,
            Fault::TrialUnwind,
            Fault::MarkerError,
            Fault::MarkerUnwind,
            Fault::Commit,
        ] {
            for failed_rollback in [false, true] {
                if matches!(fault, Fault::TrialRollback) && !failed_rollback {
                    continue;
                }
                let (dir, pg, command) = setup(applied);
                let before = without_marker(&pg);
                if matches!(fault, Fault::Commit) {
                    pg.conn.execute_batch("CREATE TEMP TABLE publication_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES publication_commit_fault(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                }
                let mut reached = false;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    pg.mark_pending_metadata_command_publication_started_inner(
                        0,
                        &command,
                        |stage| {
                            let trial = matches!(
                                fault,
                                Fault::TrialRollback | Fault::TrialError | Fault::TrialUnwind
                            );
                            if stage
                                != if trial {
                                    "after-trial"
                                } else {
                                    "before-commit"
                                }
                            {
                                return Ok(());
                            }
                            assert!(!reached);
                            reached = true;
                            assert_eq!(
                                reservations(&pg),
                                if trial { 1 } else { u32::from(applied) }
                            );
                            assert_eq!(started(&pg.conn), !trial);
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            match fault {
                                Fault::TrialRollback => Ok(()),
                                Fault::TrialError | Fault::MarkerError => Err(injected_error()),
                                Fault::TrialUnwind | Fault::MarkerUnwind => {
                                    panic!("injected publication unwind")
                                }
                                Fault::Commit => {
                                    pg.conn
                                        .execute(
                                            "INSERT INTO publication_commit_fault VALUES (1, 2)",
                                            [],
                                        )
                                        .unwrap();
                                    Ok(())
                                }
                            }
                        },
                    )
                }));
                assert!(reached);
                if matches!(fault, Fault::TrialUnwind | Fault::MarkerUnwind) {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err();
                    let diagnostic = detail(&error);
                    if failed_rollback {
                        assert!(
                            diagnostic.contains("injected metadata-command rollback failure"),
                            "{fault:?}: {diagnostic}"
                        );
                    }
                    match fault {
                        Fault::TrialError | Fault::MarkerError if !failed_rollback => {
                            assert!(matches!(
                                error,
                                BucketSnapshotLoadError::Store(StoreError::RouteMapExpired { .. })
                            ))
                        }
                        Fault::TrialError | Fault::MarkerError => {
                            assert!(diagnostic.contains("expired"), "{diagnostic}")
                        }
                        Fault::Commit => assert!(
                            diagnostic.contains("FOREIGN KEY constraint failed"),
                            "{diagnostic}"
                        ),
                        _ => {}
                    }
                }
                if failed_rollback {
                    assert!(
                        !pg.conn.is_autocommit(),
                        "outer scope silently retried failed child teardown"
                    );
                    assert!(pg.require_usable_connection().is_err());
                    assert!(pg.require_live_allocation_lifetime().is_err());
                    let writes = pg.conn.total_changes();
                    assert!(pg
                        .mark_pending_metadata_command_publication_started(0, &command)
                        .is_err());
                    assert!(pg
                        .commit_unmanaged_test_transaction("retired publication commit")
                        .is_err());
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert!(!pg.conn.is_autocommit());
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    pg.require_usable_connection().unwrap();
                    assert!(pg.conn.is_autocommit());
                    assert_eq!(without_marker(&pg), before);
                    assert!(!started(&pg.conn));
                }
                drop(pg);
                let pg = PgStore::open(dir.path(), 1).unwrap();
                assert_eq!(
                    without_marker(&pg),
                    before,
                    "{fault:?}, applied={applied}, failed_rollback={failed_rollback}"
                );
                assert!(!started(&pg.conn));
                pg.mark_pending_metadata_command_publication_started(0, &command)
                    .unwrap();
                pg.apply_metadata_command_and_record(0, &command).unwrap();
            }
        }
    }
}

#[test]
fn current_publication_transaction_postcommit_loss_cannot_undo_marker() {
    for applied in [false, true] {
        for unwind in [false, true] {
            let (dir, pg, command) = setup(applied);
            let before = without_marker(&pg);
            let mut reached = false;
            let result = catch_unwind(AssertUnwindSafe(|| {
                pg.mark_pending_metadata_command_publication_started_inner(0, &command, |stage| {
                    if stage == "after-commit" {
                        reached = true;
                        assert!(pg.conn.is_autocommit());
                        assert!(started(&pg.conn));
                        if unwind {
                            panic!("injected lost publication confirmation");
                        }
                        return Err(injected_error());
                    }
                    Ok(())
                })
            }));
            assert!(reached);
            assert!(if unwind {
                result.is_err()
            } else {
                result.unwrap().is_err()
            });
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert!(started(&pg.conn));
            assert_eq!(without_marker(&pg), before);
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            assert!(started(&pg.conn));
            assert_eq!(without_marker(&pg), before);
            pg.mark_pending_metadata_command_publication_started(0, &command)
                .unwrap();
        }
    }
}

#[test]
fn current_publication_transaction_rejects_caller_scope_without_ending_it() {
    let (_dir, pg, command) = setup(false);
    let before = without_marker(&pg);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let writes = pg.conn.total_changes();
    let error = pg
        .mark_pending_metadata_command_publication_started(0, &command)
        .unwrap_err();
    assert!(detail(&error).contains("inside caller transaction"));
    assert!(!pg.conn.is_autocommit());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(without_marker(&pg), before);
    assert!(!started(&pg.conn));
    pg.conn.execute_batch("ROLLBACK").unwrap();
    pg.mark_pending_metadata_command_publication_started(0, &command)
        .unwrap();
}

#[test]
fn current_publication_transaction_missing_or_crossed_slot_rolls_back_trial() {
    for missing in [false, true] {
        let (_dir, pg, command) = setup(false);
        let candidate = if missing {
            assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
            command
        } else {
            let MetadataCommandPayload::ReserveObjectGeneration(mut reserve) =
                command.payload().clone()
            else {
                unreachable!()
            };
            reserve.key = crate::tests::object_key("crossed-key");
            MetadataCommandEnvelope::new(
                command.id(),
                MetadataCommandPayload::ReserveObjectGeneration(reserve),
            )
        };
        let before = without_marker(&pg);
        let mut trial = false;
        let error = pg
            .mark_pending_metadata_command_publication_started_inner(0, &candidate, |stage| {
                if stage == "after-trial" {
                    trial = true;
                    assert_eq!(reservations(&pg), 1);
                }
                assert_ne!(stage, "before-commit");
                Ok(())
            })
            .unwrap_err();
        assert!(trial, "must reject after the real trial effects");
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandPendingConflict { .. })
        ));
        assert_eq!(without_marker(&pg), before);
        assert_eq!(reservations(&pg), 0);
        assert!(pg.conn.is_autocommit());
        pg.require_usable_connection().unwrap();
    }
}
