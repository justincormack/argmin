// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Checked arithmetic shared by local consumption and future replicated windows.
//! This range is a value, NOT write authority: it proves neither replication nor
//! issuer identity, placement, a deadline, or deletion exclusion.

use std::{num::NonZeroU32, ops::Range};

use rusqlite::params;

use super::PgStore;
use crate::{
    error::MetadataError, node_runtime::traits::PgMetadataStore, GenerationId, SessionId,
    StreamUploadState,
};

fn exhausted_window() -> MetadataError {
    MetadataError::InvariantViolation {
        context: "allocate stream segment VID range",
        reason: "reserved range and next cursor must fit the SQLite integer domain".into(),
    }
}

/// Both bounds must be representable in SQLite: the exclusive end is itself the
/// persisted next-allocation cursor. In particular, allocating i64::MAX is not
/// valid even though that individual identity fits in a SQLite INTEGER.
fn checked_segment_vid_window(
    start: GenerationId,
    count: NonZeroU32,
) -> Result<Range<GenerationId>, MetadataError> {
    let end = start
        .get()
        .checked_add(u64::from(count.get()))
        .filter(|end| *end <= i64::MAX as u64)
        .and_then(GenerationId::new)
        .ok_or_else(exhausted_window)?;
    Ok(start..end)
}

impl PgStore {
    /// Consumes a finite interval from this node's local allocator. It is not a
    /// replicated reserved end and cannot authorize replacement-primary reuse.
    /// The one-statement bound is checked by SQLite before the update; Rust only
    /// projects the checked result. Keep the normal single-VID path one statement
    /// with no extra transaction or pre-read between admission and mutation.
    pub(super) fn allocate_stream_segment_vid_window(
        &self,
        session_id: &SessionId,
        count: NonZeroU32,
    ) -> Result<Range<GenerationId>, MetadataError> {
        let count_sql = i64::from(count.get());
        let allocated = (|| -> rusqlite::Result<Option<GenerationId>> {
            let mut statement = self.conn.prepare_cached(
                "UPDATE stream_uploads \
             SET next_segment_vid = next_segment_vid + ?3 \
             WHERE session_id = ?1 AND state = ?2 AND next_segment_vid <= ?4 \
             RETURNING next_segment_vid - ?3",
            )?;
            let mut rows = statement.query(params![
                session_id.as_str(),
                StreamUploadState::InProgress as u8,
                count_sql,
                i64::MAX - count_sql
            ])?;
            let allocated = rows
                .next()?
                .map(|row| Self::parse_generation_id(row.get::<_, i64>(0)?, 0, "next_segment_vid"))
                .transpose()?;
            // RETURNING supplies a value before statement completion. Step to
            // DONE so a deferred constraint/commit failure cannot be ignored by
            // dropping a statement after its first row.
            if rows.next()?.is_some() {
                return Err(rusqlite::Error::QueryReturnedMoreThanOneRow);
            }
            Ok(allocated)
        })()
        .map_err(|source| MetadataError::Db {
            context: "allocate stream segment VID",
            source: source.into(),
        })?;
        match allocated {
            Some(start) => checked_segment_vid_window(start, count),
            None => match self.get_stream_upload(session_id) {
                Ok(session) if session.state == StreamUploadState::InProgress => {
                    Err(exhausted_window())
                }
                Ok(session) => Err(MetadataError::StreamSessionNotInProgress {
                    state: session.state as u8,
                }),
                Err(error) => Err(error),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BucketName, CreateStreamUploadReq, ObjectKey, StreamUploadTarget};

    fn session(store: &PgStore, hex: &str) -> SessionId {
        let session_id = SessionId::try_from(hex.repeat(16)).unwrap();
        store
            .create_stream_upload(&CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: BucketName::try_from("window-bucket").unwrap(),
                key: ObjectKey::try_from("object").unwrap(),
                target: StreamUploadTarget::PutObject,
                encryption: crate::tests::metadata_only_test_encryption(),
            })
            .unwrap();
        session_id
    }

    #[test]
    fn stream_segment_vid_windows_persist_and_do_not_rewind_on_reopen() {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let other = session(&store, "22");
        let other_before = store.get_stream_upload(&other).unwrap();
        let window = store
            .allocate_stream_segment_vid_window(&first, NonZeroU32::new(8).unwrap())
            .unwrap();
        assert_eq!((window.start.get(), window.end.get()), (1, 9));
        assert_eq!(store.get_stream_upload(&other).unwrap(), other_before);
        drop(store);
        let store = PgStore::open(dir.path(), 0).unwrap();
        assert_eq!(
            store
                .get_stream_upload(&first)
                .unwrap()
                .next_segment_vid
                .get(),
            9
        );
        assert_eq!(store.allocate_stream_segment_vid(&first).unwrap().get(), 9);
        assert_eq!(
            store
                .get_stream_upload(&first)
                .unwrap()
                .next_segment_vid
                .get(),
            10
        );
        assert_eq!(store.get_stream_upload(&other).unwrap(), other_before);
    }

    #[test]
    fn stream_segment_vid_windows_reject_overflow_without_mutating_any_session() {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let other = session(&store, "22");
        store
            .conn
            .execute(
                "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
                params![i64::MAX - 1, first.as_str()],
            )
            .unwrap();
        let before = store.get_stream_upload(&first).unwrap();
        let other_before = store.get_stream_upload(&other).unwrap();
        let digest = store.metadata_state_digest().unwrap();
        assert!(matches!(
            store.allocate_stream_segment_vid_window(&first, NonZeroU32::new(2).unwrap()),
            Err(MetadataError::InvariantViolation {
                context: "allocate stream segment VID range",
                ..
            })
        ));
        assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        assert_eq!(store.get_stream_upload(&other).unwrap(), other_before);
        assert_eq!(store.metadata_state_digest().unwrap(), digest);
        assert_eq!(
            store.allocate_stream_segment_vid(&first).unwrap().get(),
            i64::MAX as u64 - 1
        );
        let exhausted = store.get_stream_upload(&first).unwrap();
        assert_eq!(exhausted.next_segment_vid.get(), i64::MAX as u64);
        assert!(matches!(
            store.allocate_stream_segment_vid(&first),
            Err(MetadataError::InvariantViolation {
                context: "allocate stream segment VID range",
                ..
            })
        ));
        assert_eq!(store.get_stream_upload(&first).unwrap(), exhausted);
        assert_eq!(
            store.allocate_stream_segment_vid(&other).unwrap(),
            GenerationId::MIN
        );
    }

    #[test]
    fn stream_segment_vid_windows_preserve_missing_and_terminal_session_errors() {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let missing = SessionId::try_from("22".repeat(16)).unwrap();
        let before = store.get_stream_upload(&first).unwrap();
        assert!(matches!(
            store.allocate_stream_segment_vid(&missing),
            Err(MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        for (hex, state) in [
            ("33", StreamUploadState::Completing),
            ("44", StreamUploadState::Completed),
            ("55", StreamUploadState::Aborted),
        ] {
            let terminal = session(&store, hex);
            store.set_stream_upload_state(&terminal, state).unwrap();
            let terminal_before = store.get_stream_upload(&terminal).unwrap();
            assert!(matches!(store.allocate_stream_segment_vid(&terminal),
                Err(MetadataError::StreamSessionNotInProgress { state: actual }) if actual == state as u8));
            assert_eq!(store.get_stream_upload(&terminal).unwrap(), terminal_before);
            assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        }
    }

    #[test]
    fn stream_segment_vid_windows_join_outer_transaction_and_rollback_with_it() {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let before = store.get_stream_upload(&first).unwrap();
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let window = store
            .allocate_stream_segment_vid_window(&first, NonZeroU32::new(8).unwrap())
            .unwrap();
        assert_eq!((window.start.get(), window.end.get()), (1, 9));
        assert!(
            !store.conn.is_autocommit(),
            "allocator must not commit its caller's transaction"
        );
        store.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        assert_eq!(
            store.allocate_stream_segment_vid(&first).unwrap(),
            GenerationId::MIN
        );
    }

    #[test]
    fn stream_segment_vid_windows_report_interruption_after_returning_before_completion() {
        use rusqlite::trace::{TraceEvent, TraceEventCodes};
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let before = store.get_stream_upload(&first).unwrap();
        let interrupted = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&interrupted);
        let handle = store.conn.get_interrupt_handle();
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_ROW,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Row(statement) = event {
                        if statement.sql().starts_with("UPDATE stream_uploads") {
                            observed.store(true, Ordering::Relaxed);
                            handle.interrupt();
                        }
                    }
                }),
            )
            .unwrap();

        // Pin the fault timing independently: SQLite returns the candidate row,
        // then reports interruption at the next step and rolls back the update.
        {
            let mut statement = store
                .conn
                .prepare(
                    "UPDATE stream_uploads SET next_segment_vid = next_segment_vid + 1 \
                 WHERE session_id = ?1 RETURNING next_segment_vid - 1",
                )
                .unwrap();
            let mut rows = statement.query(params![first.as_str()]).unwrap();
            assert_eq!(rows.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);
            assert!(rows.next().is_err());
        }
        assert!(interrupted.swap(false, Ordering::Relaxed));
        assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        assert!(matches!(
            store.allocate_stream_segment_vid(&first),
            Err(MetadataError::Db {
                context: "allocate stream segment VID",
                ..
            })
        ));
        assert!(interrupted.load(Ordering::Relaxed));
        assert!(store.conn.is_autocommit());
        assert_eq!(store.get_stream_upload(&first).unwrap(), before);
        store
            .conn
            .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
            .unwrap();
        assert_eq!(
            store.allocate_stream_segment_vid(&first).unwrap(),
            GenerationId::MIN
        );
    }

    #[test]
    fn stream_segment_vid_windows_keep_successful_allocation_one_sql_statement() {
        use rusqlite::trace::{TraceEvent, TraceEventCodes};
        use std::sync::{Arc, Mutex};

        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        let first = session(&store, "11");
        let statements = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = Arc::clone(&statements);
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if !sql.starts_with("--") {
                            captured.lock().unwrap().push(sql.to_owned());
                        }
                    }
                }),
            )
            .unwrap();
        assert_eq!(
            store.allocate_stream_segment_vid(&first).unwrap(),
            GenerationId::MIN
        );
        let observed = statements.lock().unwrap();
        assert_eq!(
            observed.len(),
            1,
            "unexpected extra allocation statement: {observed:?}"
        );
        assert!(observed[0].starts_with("UPDATE stream_uploads"));
    }

    #[test]
    fn stream_segment_vid_windows_advance_beyond_the_entire_reserved_interval() {
        let count = NonZeroU32::new(8).unwrap();
        let first = checked_segment_vid_window(GenerationId::MIN, count).unwrap();
        assert_eq!((first.start.get(), first.end.get()), (1, 9));
        // A selected VID inside the block must not become the next reservation
        // floor. The caller supplies the replicated reserved end, not that VID.
        let next = checked_segment_vid_window(first.end, count).unwrap();
        assert_eq!((next.start.get(), next.end.get()), (9, 17));
    }

    #[test]
    fn stream_segment_vid_windows_reject_unrepresentable_exclusive_end() {
        let maximum = i64::MAX as u64;
        let last =
            checked_segment_vid_window(GenerationId::new(maximum - 1).unwrap(), NonZeroU32::MIN)
                .unwrap();
        assert_eq!(last.end.get(), maximum);
        for (start, count) in [
            (maximum, 1),
            (maximum - 1, 2),
            (maximum + 1, 1),
            (u64::MAX, 1),
        ] {
            assert!(matches!(
                checked_segment_vid_window(
                    GenerationId::new(start).unwrap(),
                    NonZeroU32::new(count).unwrap(),
                ),
                Err(MetadataError::InvariantViolation {
                    context: "allocate stream segment VID range",
                    ..
                })
            ));
        }
    }
}
