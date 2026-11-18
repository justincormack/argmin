// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real transfer mutations, shared teardown and independent allocation lifetime.
//! Bucket-only metadata fixtures; no payload IO or new transfer protocol.

use super::super::*;
use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Kind {
    Empty,
    Matching,
    Checkpoint,
    Rebased,
}
const KINDS: [Kind; 4] = [Kind::Empty, Kind::Matching, Kind::Checkpoint, Kind::Rebased];

fn epoch() -> ClusterEpoch {
    ClusterEpoch::new(2).unwrap()
}

enum Work {
    Empty(CanonicalStateDigest),
    Matching(CanonicalStateDigest),
    Checkpoint(MetadataCommandCheckpoint),
    Rebased(Vec<MetadataTransferCommand>, CanonicalStateDigest),
}

impl Work {
    fn run(&self, pg: &PgStore) -> Result<MetadataCommandReplicaState, StoreError> {
        match self {
            Self::Empty(digest) => pg.initialize_metadata_transfer_empty_state(0, epoch(), *digest),
            Self::Matching(digest) => pg.initialize_metadata_transfer_matching_state(
                0,
                epoch(),
                0,
                MetadataCommandLogHash::genesis(),
                *digest,
            ),
            Self::Checkpoint(checkpoint) => pg.install_metadata_transfer_checkpoint_base(
                0,
                epoch(),
                checkpoint,
                &crate::PgTopology::new(&[1]).unwrap(),
            ),
            Self::Rebased(commands, digest) => pg
                .adopt_metadata_transfer_state_from_rebased_commands(0, epoch(), commands, *digest),
        }
    }
}

fn create(pg: &PgStore, name: &str) -> MetadataCommandEnvelope {
    let owner = OwnerIdentity::from_principal("owner");
    let config = CreateBucketConfig {
        name,
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
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config_for_test(&config, 123, 1).unwrap(),
        ),
    );
    pg.apply_metadata_command_and_record(0, &command).unwrap();
    command
}

fn setup(kind: Kind) -> (test_util::TempDir, PgStore, Work) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 1).unwrap();
    let pre_digest = pg.metadata_command_replica_state().unwrap().state_digest;
    let work = match kind {
        Kind::Empty => Work::Empty(pre_digest),
        Kind::Matching => {
            create(&pg, "previous");
            Work::Matching(pg.metadata_command_replica_state().unwrap().state_digest)
        }
        Kind::Checkpoint => {
            create(&pg, "previous");
            let source = PgStore::open(&dir.path().join("source"), 1).unwrap();
            create(&source, "incoming");
            Work::Checkpoint(
                source
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
            )
        }
        Kind::Rebased => {
            let command = create(&pg, "previous");
            let digest = pg.metadata_command_replica_state().unwrap().state_digest;
            Work::Rebased(
                vec![MetadataTransferCommand {
                    command: MetadataCommandEnvelope::new(
                        MetadataCommandId::new(epoch(), PgId::new(1), command.id().log_index()),
                        command.payload().clone(),
                    ),
                    pre_state_digest: pre_digest,
                    post_state_digest: digest,
                }],
                digest,
            )
        }
    };
    (dir, pg, work)
}

fn rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).unwrap();
    let count = statement.column_count();
    statement
        .query_map([], |row| {
            (0..count)
                .map(|i| row.get_ref(i).map(|v| format!("{v:?}")))
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Snapshot {
    blocks: Vec<MetadataCheckpointTableBlock>,
    rows: Vec<Vec<Vec<String>>>,
}

fn snapshot(pg: &PgStore) -> Snapshot {
    Snapshot {
        blocks: pg.metadata_checkpoint_table_blocks().unwrap(),
        rows: [
            "SELECT * FROM metadata_command_replica_state",
            "SELECT * FROM metadata_command_log ORDER BY cluster_epoch, pg_id, log_index",
            "SELECT * FROM metadata_command_terminal_receipts ORDER BY cluster_epoch, pg_id, log_index",
            "SELECT * FROM metadata_command_checkpoints ORDER BY cluster_epoch, pg_id, applied_log_index",
        ].iter().map(|sql| rows(&pg.conn, sql)).collect(),
    }
}

fn with_hook<T>(
    hook: impl FnMut(&PgStore, &'static str) -> Result<(), StoreError> + 'static,
    action: impl FnOnce() -> T,
) -> T {
    struct Reset(Option<Hook>);
    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_HOOK.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _reset = Reset(TEST_HOOK.with(|slot| slot.replace(Some(Box::new(hook)))));
    action()
}

fn injected_error() -> StoreError {
    StoreError::MetadataCommandContention {
        context: "injected transfer operation failure",
    }
}

fn assert_retired(pg: &PgStore, work: &Work) {
    assert!(!pg.conn.is_autocommit(), "hidden rollback retried teardown");
    assert!(pg.require_usable_connection().is_err());
    assert!(pg.require_live_allocation_lifetime().is_err());
    let changes = pg.conn.total_changes();
    let error = work.run(pg).unwrap_err();
    assert!(
        error.to_string().contains("retired PG metadata connection"),
        "{error}"
    );
    assert_eq!(pg.conn.total_changes(), changes);
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert!(pg.require_usable_connection().is_err());
    assert!(work.run(pg).is_err());
}

#[test]
fn transfer_transaction_commits_once_before_activating_replacement() {
    for kind in KINDS {
        let (dir, mut pg, work) = setup(kind);
        let before = snapshot(&pg);
        let old = pg.test_allocation_lifetime_identity().unwrap();
        let before_prefix = rows(&pg.conn, "SELECT * FROM metadata_command_replica_state");
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
        let stages = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&stages);
        let state = with_hook(
            move |pg, stage| {
                observed.lock().unwrap().push(stage);
                assert!(pg.require_live_allocation_lifetime().is_err());
                assert_eq!(pg.conn.is_autocommit(), stage == "after-commit");
                assert_eq!(pg.metadata_command_replica_state()?.cluster_epoch, epoch());
                assert_ne!(
                    snapshot(pg),
                    before,
                    "{kind:?}: require actual replacement effects"
                );
                let visible = rows(&observer, "SELECT * FROM metadata_command_replica_state");
                if stage == "before-commit" {
                    assert_eq!(visible, before_prefix);
                    let error = pg
                        .commit_unmanaged_test_transaction("crossed transfer commit")
                        .unwrap_err();
                    assert!(error
                        .to_string()
                        .contains("owning metadata transaction scope"));
                } else {
                    assert_ne!(visible, before_prefix);
                }
                Ok(())
            },
            || work.run(&pg),
        )
        .unwrap();
        assert_eq!(*stages.lock().unwrap(), ["before-commit", "after-commit"]);
        assert_ne!(pg.test_allocation_lifetime_identity().unwrap(), old);
        assert_eq!(state.cluster_epoch, epoch());
        let trace = trace.lock().unwrap();
        assert_eq!(
            trace.iter().filter(|sql| sql.starts_with("BEGIN")).count(),
            1
        );
        assert_eq!(
            trace.iter().filter(|sql| sql.as_str() == "COMMIT").count(),
            1
        );
        assert!(!trace.iter().any(|sql| sql.starts_with("ROLLBACK")));
        drop(trace);
        let after = snapshot(&pg);
        drop(pg);
        let pg = PgStore::open(dir.path(), 1).unwrap();
        assert_eq!(snapshot(&pg), after);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), state);
        pg.require_live_allocation_lifetime().unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Operation,
    Unwind,
    Commit,
}

#[test]
fn transfer_transaction_errors_and_unwind_restore_state_or_retire_connection() {
    for kind in KINDS {
        for fault in [Fault::Operation, Fault::Unwind, Fault::Commit] {
            for failed_rollback in [false, true] {
                let (dir, pg, work) = setup(kind);
                let before = snapshot(&pg);
                let expected = before.clone();
                if matches!(fault, Fault::Commit) {
                    pg.conn.execute_batch("CREATE TEMP TABLE transfer_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES transfer_commit_fault(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                }
                let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let observed = Arc::clone(&reached);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    with_hook(
                        move |pg, stage| {
                            assert_eq!(stage, "before-commit");
                            observed.store(true, Ordering::Relaxed);
                            assert_ne!(snapshot(pg), expected);
                            assert!(pg.require_live_allocation_lifetime().is_err());
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            match fault {
                                Fault::Operation => Err(injected_error()),
                                Fault::Unwind => panic!("injected transfer unwind"),
                                Fault::Commit => {
                                    pg.conn
                                        .execute(
                                            "INSERT INTO transfer_commit_fault VALUES (1, 2)",
                                            [],
                                        )
                                        .unwrap();
                                    Ok(())
                                }
                            }
                        },
                        || work.run(&pg),
                    )
                }));
                assert!(reached.load(Ordering::Relaxed));
                if matches!(fault, Fault::Unwind) {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err();
                    let diagnostic = error.to_string();
                    assert!(
                        diagnostic.contains(if matches!(fault, Fault::Commit) {
                            "FOREIGN KEY constraint failed"
                        } else {
                            "injected transfer operation failure"
                        }),
                        "{diagnostic}"
                    );
                    if failed_rollback {
                        assert!(
                            diagnostic.contains("injected metadata-command rollback failure"),
                            "{diagnostic}"
                        );
                    } else if matches!(fault, Fault::Operation) {
                        assert!(matches!(
                            error,
                            StoreError::MetadataCommandContention { .. }
                        ));
                    }
                }
                assert!(pg.require_live_allocation_lifetime().is_err());
                if failed_rollback {
                    assert_retired(&pg, &work);
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
                    "{kind:?}, {fault:?}, rollback={failed_rollback}"
                );
                work.run(&pg).unwrap();
                pg.require_live_allocation_lifetime().unwrap();
            }
        }
    }
}

#[test]
fn transfer_transaction_body_errors_preserve_cause_and_partial_work_rolls_back() {
    for kind in KINDS {
        for failed_rollback in [false, true] {
            let (dir, pg, mut work) = setup(kind);
            let before = snapshot(&pg);
            let expected = match &mut work {
                Work::Empty(digest) | Work::Matching(digest) => {
                    *digest = CanonicalStateDigest::for_test(digest.value() ^ 1);
                    "metadata state digest"
                }
                Work::Checkpoint(_) => {
                    pg.conn.execute_batch("CREATE TEMP TRIGGER transfer_insert_fault BEFORE INSERT ON buckets BEGIN SELECT RAISE(ABORT, 'injected transfer row insertion failure'); END").unwrap();
                    "injected transfer row insertion failure"
                }
                Work::Rebased(commands, _) => {
                    let mut crossed = commands[0].clone();
                    crossed.command = MetadataCommandEnvelope::new(
                        MetadataCommandId::new(
                            epoch(),
                            PgId::new(1),
                            MetadataCommandLogIndex::new(3).unwrap(),
                        ),
                        crossed.command.payload().clone(),
                    );
                    commands.push(crossed);
                    "conflicts with already-applied log index 3"
                }
            };
            if failed_rollback {
                pg.test_fail_metadata_command_rollbacks(1);
            }
            let error = work.run(&pg).unwrap_err();
            let diagnostic = error.to_string();
            assert!(diagnostic.contains(expected), "{kind:?}: {diagnostic}");
            assert!(pg.require_live_allocation_lifetime().is_err());
            if failed_rollback {
                assert!(
                    diagnostic.contains("injected metadata-command rollback failure"),
                    "{diagnostic}"
                );
                if matches!(kind, Kind::Checkpoint) {
                    assert!(
                        rows(&pg.conn, "SELECT name FROM buckets").is_empty(),
                        "fault must follow destination clearing"
                    );
                } else if matches!(kind, Kind::Rebased) {
                    assert_eq!(
                        rows(
                            &pg.conn,
                            "SELECT log_index FROM metadata_command_log WHERE cluster_epoch=2"
                        )
                        .len(),
                        1,
                        "fault must follow the first adopted row"
                    );
                }
                assert_retired(&pg, &work);
            } else {
                assert!(pg.conn.is_autocommit());
                assert_eq!(snapshot(&pg), before);
            }
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            assert_eq!(snapshot(&pg), before);
            pg.require_live_allocation_lifetime().unwrap();
        }
    }
}

#[test]
fn transfer_transaction_postcommit_loss_keeps_durable_state_but_not_allocation() {
    for kind in KINDS {
        for unwind in [false, true] {
            let (dir, pg, work) = setup(kind);
            let before = snapshot(&pg);
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = Arc::clone(&reached);
            let result = catch_unwind(AssertUnwindSafe(|| {
                with_hook(
                    move |pg, stage| {
                        if stage == "after-commit" {
                            observed.store(true, Ordering::Relaxed);
                            assert!(pg.conn.is_autocommit());
                            assert!(pg.require_live_allocation_lifetime().is_err());
                            if unwind {
                                panic!("injected lost transfer confirmation");
                            }
                            return Err(injected_error());
                        }
                        Ok(())
                    },
                    || work.run(&pg),
                )
            }));
            assert!(reached.load(Ordering::Relaxed));
            assert!(if unwind {
                result.is_err()
            } else {
                result.unwrap().is_err()
            });
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert!(pg.require_live_allocation_lifetime().is_err());
            let after = snapshot(&pg);
            assert_ne!(after, before);
            assert_eq!(
                pg.metadata_command_replica_state().unwrap().cluster_epoch,
                epoch()
            );
            drop(pg);
            let pg = PgStore::open(dir.path(), 1).unwrap();
            assert_eq!(snapshot(&pg), after);
            pg.require_live_allocation_lifetime().unwrap();
        }
    }
}

#[test]
fn transfer_transaction_rejects_outer_scope_without_finishing_caller_work() {
    for kind in KINDS {
        let (_dir, pg, work) = setup(kind);
        let before = snapshot(&pg);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let changes = pg.conn.total_changes();
        let error = work.run(&pg).unwrap_err();
        assert!(error.to_string().contains("inside caller transaction"));
        assert!(!pg.conn.is_autocommit());
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(snapshot(&pg), before);
        assert!(pg.require_live_allocation_lifetime().is_err());
        pg.conn.execute_batch("ROLLBACK").unwrap();
        pg.require_usable_connection().unwrap();
        work.run(&pg).unwrap();
        pg.require_live_allocation_lifetime().unwrap();
    }
}
