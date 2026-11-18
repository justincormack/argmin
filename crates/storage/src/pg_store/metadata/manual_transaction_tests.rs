// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Standalone drain/release commits: real effects, teardown and parent exclusion.

use super::*;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

type FinishHook = Box<dyn FnOnce(&PgStore) -> Result<(), MetadataError>>;
thread_local! {
    static BEFORE_FINISH: RefCell<Option<FinishHook>> = const { RefCell::new(None) };
}

pub(super) fn before_finish(pg: &PgStore) -> Result<(), MetadataError> {
    let hook = BEFORE_FINISH.with(|slot| slot.borrow_mut().take());
    hook.map_or(Ok(()), |hook| hook(pg))
}

fn with_finish_hook<T>(hook: FinishHook, action: impl FnOnce() -> T) -> T {
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
    Clear,
    Expire,
    Heartbeat,
    Release,
}

const OPERATIONS: [Operation; 4] = [
    Operation::Clear,
    Operation::Expire,
    Operation::Heartbeat,
    Operation::Release,
];

#[derive(Clone)]
struct Subject {
    drain: BucketWriteDrainRecord,
    proof: BucketWriteReservationProof,
}

fn setup() -> (test_util::TempDir, PgStore, Subject) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    let bucket = crate::tests::bucket_name("owned-manual-transaction");
    pg.create_bucket(
        &bucket,
        "owner",
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    let reservation = pg
        .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "reservation",
            owner_token: "owner",
            cluster_epoch: ClusterEpoch::INITIAL,
            operation_kind: "bucket-write-snapshot",
            created_at: 10,
            lease_deadline: 30,
            target_context: None,
        })
        .unwrap();
    let drain = pg
        .begin_durable_bucket_write_drain(&bucket, "drain", "owner", ClusterEpoch::INITIAL, 10, 30)
        .unwrap();
    let proof = BucketWriteReservationProof::from(&reservation);
    (dir, pg, Subject { drain, proof })
}

impl Operation {
    fn run(self, pg: &PgStore, subject: &Subject) -> Result<(), MetadataError> {
        let d = &subject.drain;
        match self {
            Self::Clear => pg.clear_durable_bucket_write_drain(
                &subject.proof.bucket,
                &d.drain_id,
                &d.owner_token,
                d.cluster_epoch,
                d.bucket_execution_generation,
                d.lease_deadline,
            ),
            Self::Expire => pg
                .clear_expired_durable_bucket_write_drain(&subject.proof.bucket, 31)
                .map(|record| assert_eq!(record, Some(d.clone()))),
            Self::Heartbeat => pg
                .heartbeat_durable_bucket_write_drain(
                    &subject.proof.bucket,
                    &d.drain_id,
                    &d.owner_token,
                    d.cluster_epoch,
                    d.bucket_execution_generation,
                    40,
                    11,
                )
                .map(|record| assert_eq!(record.lease_deadline, 40)),
            Self::Release => pg.release_metadata_command_bucket_write_reservation(&subject.proof),
        }
    }

    fn require_effect(self, pg: &PgStore, subject: &Subject) {
        let drain = pg
            .durable_bucket_write_drain(&subject.proof.bucket)
            .unwrap();
        let reservation = pg
            .durable_bucket_write_reservation(&subject.proof.bucket, &subject.proof.reservation_id)
            .unwrap();
        match self {
            Self::Clear | Self::Expire => {
                assert!(drain.is_none());
                assert!(subject.proof.matches_record(&reservation.unwrap()));
            }
            Self::Heartbeat => {
                let mut expected = subject.drain.clone();
                expected.lease_deadline = 40;
                assert_eq!(drain, Some(expected));
                assert!(subject.proof.matches_record(&reservation.unwrap()));
            }
            Self::Release => {
                assert_eq!(drain, Some(subject.drain.clone()));
                assert!(reservation.is_none());
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Operation,
    Unwind,
    Commit,
}

#[test]
fn manual_metadata_transactions_own_failure_teardown_and_reopen() {
    for operation in OPERATIONS {
        for failure in [Failure::Operation, Failure::Unwind, Failure::Commit] {
            for failed_rollback in [false, true] {
                let (dir, pg, subject) = setup();
                let before = pg.metadata_checkpoint_table_blocks().unwrap();
                let writes = pg.conn.total_changes();
                let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let hook_reached = Arc::clone(&reached);
                let hook_subject = subject.clone();
                let result = catch_unwind(AssertUnwindSafe(|| {
                    with_finish_hook(
                        Box::new(move |pg| {
                            hook_reached.store(true, Ordering::Relaxed);
                            assert!(!pg.conn.is_autocommit());
                            assert!(pg.conn.total_changes() > writes, "no real mutation");
                            operation.require_effect(pg, &hook_subject);
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            match failure {
                                Failure::Operation => Err(MetadataError::InvariantViolation {
                                    context: "after standalone metadata mutation",
                                    reason: "injected operation failure".into(),
                                }),
                                Failure::Unwind => panic!("after standalone metadata mutation"),
                                Failure::Commit => {
                                    pg.fail_next_metadata_txn_commit
                                        .store(true, Ordering::Relaxed);
                                    Ok(())
                                }
                            }
                        }),
                        || operation.run(&pg, &subject),
                    )
                }));
                assert!(reached.load(Ordering::Relaxed));
                if matches!(failure, Failure::Unwind) {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err().to_string();
                    assert!(error.contains("injected"), "{error}");
                    if failed_rollback {
                        assert!(error.contains("rollback failure"), "{error}");
                        assert!(
                            error.contains(if matches!(failure, Failure::Commit) {
                                "commit failure"
                            } else {
                                "operation failure"
                            }),
                            "{error}"
                        );
                    }
                }
                assert_eq!(pg.conn.is_autocommit(), !failed_rollback);
                if failed_rollback {
                    assert!(pg.require_usable_connection().is_err());
                    assert!(pg.require_live_allocation_lifetime().is_err());
                    operation.require_effect(&pg, &subject); // owner-local poisoned-view evidence
                    let writes = pg.conn.total_changes();
                    assert!(operation.run(&pg, &subject).is_err());
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert!(!pg.conn.is_autocommit(), "hidden rollback retry");
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    pg.require_usable_connection().unwrap();
                    assert_eq!(pg.metadata_checkpoint_table_blocks().unwrap(), before);
                }
                drop(pg);
                let reopened = PgStore::open(dir.path(), 0).unwrap();
                assert_eq!(reopened.metadata_checkpoint_table_blocks().unwrap(), before);
                operation.run(&reopened, &subject).unwrap();
                operation.require_effect(&reopened, &subject);
            }
        }
    }
}

#[test]
fn manual_metadata_transactions_reject_raw_and_managed_parents_without_effects() {
    for operation in OPERATIONS {
        for managed in [false, true] {
            let (_dir, pg, subject) = setup();
            let parent = if managed {
                Some(
                    super::metadata_transaction::MetadataTransaction::begin(&pg, "parent").unwrap(),
                )
            } else {
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                None
            };
            pg.conn
                .execute("UPDATE bucket_write_drains SET lease_deadline = 35", [])
                .unwrap();
            let before = pg.metadata_checkpoint_table_blocks().unwrap();
            let writes = pg.conn.total_changes();
            let error = operation.run(&pg, &subject).unwrap_err().to_string();
            assert!(
                error.contains("requires autocommit"),
                "{operation:?}: {error}"
            );
            assert!(!pg.conn.is_autocommit());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pg.metadata_checkpoint_table_blocks().unwrap(), before);
            pg.require_usable_connection().unwrap();
            if let Some(parent) = parent {
                parent.commit("parent").unwrap();
            } else {
                pg.commit_unmanaged_test_transaction("raw parent").unwrap();
            }
            assert_eq!(
                pg.durable_bucket_write_drain(&subject.proof.bucket)
                    .unwrap()
                    .unwrap()
                    .lease_deadline,
                35
            );
        }
    }
}

#[test]
fn manual_metadata_transactions_keep_one_durable_commit() {
    for operation in OPERATIONS {
        let (_dir, mut pg, subject) = setup();
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
        operation.run(&pg, &subject).unwrap();
        operation.require_effect(&pg, &subject);
        assert_eq!(*statements.lock().unwrap(), ["BEGIN IMMEDIATE", "COMMIT"]);
        assert!(pg.conn.is_autocommit());
    }
}
