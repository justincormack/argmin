// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Acknowledgement ownership, with real encrypted files already durable before SQL.

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

#[derive(Clone)]
struct Payload {
    key: ShardKey,
    ack: WriteAck,
    segment: StreamUploadSegmentRecord,
    ciphertext: Vec<u8>,
}

fn setup() -> (test_util::TempDir, PgStore, Payload) {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    let fixture = crate::tests::sse_s3_segment_test_fixture(b"owned shard acknowledgement", 0);
    let session_id = crate::tests::stream_session_id("owned-shard");
    pg.create_stream_upload(&CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: crate::tests::bucket_name("bucket"),
        key: crate::tests::object_key("key"),
        target: StreamUploadTarget::PutObject,
        encryption: fixture.encryption,
    })
    .unwrap();
    let segment = StreamUploadSegmentRecord {
        session_id,
        segment_index: 0,
        size: fixture.logical_size,
        segment_crc64: fixture.storage_crc64,
        payload_crc64: fixture.payload_crc64,
        segment_okh: [0xD7; 16],
        segment_vid: GenerationId::MIN,
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 1,
        ec_m: 0,
        shard_integrity: None,
    };
    let key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 0);
    let ack = PgStore::write_shard_file_durable(
        &pg.tmp_dir,
        &pg.shards_dir,
        &key,
        &fixture.storage_bytes,
    )
    .unwrap();
    (
        dir,
        pg,
        Payload {
            key,
            ack,
            segment,
            ciphertext: fixture.storage_bytes,
        },
    )
}

fn run(pg: &PgStore, payload: &Payload, combined: bool) -> Result<(), String> {
    let batch = [(&payload.key, payload.ack)];
    if combined {
        pg.register_written_shards_and_append_stream_segment(&batch, &payload.segment)
            .map_err(|error| error.to_string())
    } else {
        pg.register_written_shards_batch_exact(&batch)
            .map_err(|error| error.to_string())
    }
}

fn require_effect(pg: &PgStore, payload: &Payload, combined: bool) {
    pg.validate_written_shard_ack(&payload.key, payload.ack)
        .unwrap();
    let segments = pg
        .list_stream_segments(&payload.segment.session_id)
        .unwrap();
    if combined {
        assert_eq!(segments, vec![payload.segment.clone()]);
    } else {
        assert!(segments.is_empty());
    }
}

fn require_absent(pg: &PgStore, payload: &Payload) {
    assert!(matches!(
        pg.validate_written_shard_ack(&payload.key, payload.ack),
        Err(StoreError::NotFound)
    ));
    assert!(pg
        .list_stream_segments(&payload.segment.session_id)
        .unwrap()
        .is_empty());
    assert_eq!(
        fs::read(pg.shard_path(&payload.key)).unwrap(),
        payload.ciphertext
    );
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Operation,
    Unwind,
    Commit,
    DeferredCommit,
}

#[test]
fn shard_transaction_failure_and_unwind_retain_files_and_retry_after_reopen() {
    for combined in [false, true] {
        for failure in [
            Failure::Operation,
            Failure::Unwind,
            Failure::Commit,
            Failure::DeferredCommit,
        ] {
            for failed_rollback in [false, true] {
                let (dir, pg, payload) = setup();
                let before = pg.metadata_checkpoint_table_blocks().unwrap();
                let hook_payload = payload.clone();
                let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let hook_reached = Arc::clone(&reached);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    with_hook(
                        Box::new(move |pg| {
                            hook_reached.store(true, Ordering::Relaxed);
                            require_effect(pg, &hook_payload, combined);
                            if failed_rollback {
                                pg.test_fail_metadata_command_rollbacks(1);
                            }
                            match failure {
                                Failure::Operation => {
                                    Err(StoreError::RouteCapabilitySubjectMismatch {
                                        operation: "injected shard transaction failure",
                                    })
                                }
                                Failure::Unwind => panic!("injected shard transaction unwind"),
                                Failure::Commit => {
                                    pg.fail_next_metadata_txn_commit
                                        .store(true, Ordering::Relaxed);
                                    Ok(())
                                }
                                Failure::DeferredCommit => {
                                    pg.conn.execute_batch("CREATE TEMP TABLE deferred_ack (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_ack(id) DEFERRABLE INITIALLY DEFERRED); INSERT INTO deferred_ack VALUES (1, 2)").unwrap();
                                    Ok(())
                                }
                            }
                        }),
                        || run(&pg, &payload, combined),
                    )
                }));
                assert!(reached.load(Ordering::Relaxed));
                if matches!(failure, Failure::Unwind) {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err();
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
                assert_eq!(pg.conn.is_autocommit(), !failed_rollback);
                if failed_rollback {
                    assert!(pg.require_usable_connection().is_err());
                    assert!(pg.require_live_allocation_lifetime().is_err());
                    require_effect(&pg, &payload, combined); // poisoned view, not successful publication
                    let writes = pg.conn.total_changes();
                    assert!(run(&pg, &payload, combined)
                        .unwrap_err()
                        .contains("retired PG metadata connection"));
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert!(!pg.conn.is_autocommit(), "hidden rollback retry");
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    pg.require_usable_connection().unwrap();
                    require_absent(&pg, &payload);
                    assert_eq!(pg.metadata_checkpoint_table_blocks().unwrap(), before);
                }
                drop(pg);
                let reopened = PgStore::open(dir.path(), 0).unwrap();
                require_absent(&reopened, &payload);
                assert_eq!(reopened.metadata_checkpoint_table_blocks().unwrap(), before);
                run(&reopened, &payload, combined).unwrap();
                require_effect(&reopened, &payload, combined);
                assert_eq!(
                    reopened.read_shard(&payload.key).unwrap().data,
                    payload.ciphertext
                );
            }
        }
    }
}

#[test]
fn shard_transaction_rejects_parent_without_committing_or_rolling_it_back() {
    for combined in [false, true] {
        for managed in [false, true] {
            let (_dir, pg, payload) = setup();
            let parent = if managed {
                Some(MetadataTransaction::begin(&pg, "parent").unwrap())
            } else {
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                None
            };
            let canary = ShardKey::new(&[0xC1; 16], 1, 0);
            pg.register_written_shard(&canary, payload.ack).unwrap();
            let writes = pg.conn.total_changes();
            assert!(run(&pg, &payload, combined)
                .unwrap_err()
                .contains("requires autocommit"));
            assert_eq!(pg.conn.total_changes(), writes);
            assert!(!pg.conn.is_autocommit());
            require_absent(&pg, &payload);
            if let Some(parent) = parent {
                parent.commit("parent").unwrap();
            } else {
                pg.commit_unmanaged_test_transaction("raw parent").unwrap();
            }
            pg.validate_written_shard_ack(&canary, payload.ack).unwrap();
        }
    }
}

#[test]
fn shard_transaction_success_keeps_one_commit_and_exact_retry_does_not_rewrite() {
    for combined in [false, true] {
        let (_dir, mut pg, payload) = setup();
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
        let observer = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
        let hook_payload = payload.clone();
        with_hook(
            Box::new(move |pg| {
                require_effect(pg, &hook_payload, combined);
                for table in ["shards", "stream_upload_segments"] {
                    assert_eq!(
                        observer
                            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                                .get::<_, u32>(0))
                            .unwrap(),
                        0
                    );
                }
                Ok(())
            }),
            || run(&pg, &payload, combined),
        )
        .unwrap();
        assert_eq!(*trace.lock().unwrap(), ["BEGIN IMMEDIATE", "COMMIT"]);
        require_effect(&pg, &payload, combined);
        if !combined {
            let writes = pg.conn.total_changes();
            run(&pg, &payload, false).unwrap();
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn shard_transaction_partial_batch_mismatch_with_failed_rollback_retires_until_reopen() {
    let (dir, pg, payload) = setup();
    let other = ShardKey::new(&[0xD8; 16], 1, 0);
    pg.register_written_shard(&other, payload.ack).unwrap();
    pg.test_fail_metadata_command_rollbacks(1);
    let crossed = WriteAck {
        stored_size: payload.ack.stored_size + 1,
        ..payload.ack
    };
    let error = pg
        .register_written_shards_batch_exact(&[(&payload.key, payload.ack), (&other, crossed)])
        .unwrap_err()
        .to_string();
    assert!(error.contains("rollback failure"), "{error}");
    assert!(error.contains("ack"), "{error}");
    pg.validate_written_shard_ack(&payload.key, payload.ack)
        .unwrap();
    assert!(!pg.conn.is_autocommit());
    assert!(pg.require_usable_connection().is_err());
    drop(pg);
    let pg = PgStore::open(dir.path(), 0).unwrap();
    require_absent(&pg, &payload);
    pg.validate_written_shard_ack(&other, payload.ack).unwrap();
    run(&pg, &payload, false).unwrap();
}
