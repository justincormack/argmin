// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only fixtures exercising the production pending-slot boundary.

use super::pending_payload_transaction::{install, pages, replace, setup, successor};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::{
    PendingMetadataCommandSlotInsertError as InsertError,
    PendingMetadataCommandSlotReplaceError as ReplaceError, PgStore,
};
use crate::{StoreError, VersionId};

fn assert_incomplete(error: &StoreError, expected: &str) {
    assert!(
        matches!(error, StoreError::ShardScavengerScanIncomplete {
        context: "derive pending command payload inventory", errors
    } if errors == expected),
        "{error:?}"
    );
}

#[test]
fn pending_payload_subject_missing_or_crossed_live_geometry_never_becomes_empty_protection() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for append in [false, true] {
            for bound in [false, true].into_iter().filter(|bound| !bound || append) {
                for stage in ["fresh", "retry", "source reissue", "successor retry"] {
                    let (_dir, pg, owner, command) = setup(target.clone(), append);
                    let described =
                        bound.then(|| candidate::capture(&pg, &owner, command.clone(), true));
                    // A positive canary with the same command and loaded session.
                    let expected = pg
                        .encode_pending_shard_scavenger_reference_pages(&command)
                        .unwrap();
                    assert_eq!(expected.placed_reference_count, if append { 1 } else { 2 });
                    if stage != "fresh" {
                        install(&pg, &command, described.as_ref()).unwrap();
                    }
                    let replacement = successor(&command);
                    if stage == "successor retry" {
                        assert!(replace(&pg, &command, &replacement, described.as_ref()).unwrap());
                    }
                    for fault in ["missing", "bucket", "key", "session", "encoding"] {
                        pg.conn.execute_batch("SAVEPOINT crossed_geometry").unwrap();
                        if fault == "session" {
                            // Keep the fault setup SQL-valid: the command retains
                            // its exact old segments, while the live header is
                            // replaced by another same-object session identity.
                            pg.conn
                                .execute(
                                    "DELETE FROM stream_upload_segments WHERE session_id = ?1",
                                    [&owner.subject.id.attempt],
                                )
                                .unwrap();
                        }
                        let sql = match fault {
                            "missing" => "DELETE FROM stream_uploads WHERE session_id = ?1",
                            "bucket" => "UPDATE stream_uploads SET bucket = 'crossed-bucket' WHERE session_id = ?1",
                            "key" => "UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1",
                            "session" => "UPDATE stream_uploads SET session_id = 'crossed-session' WHERE session_id = ?1",
                            "encoding" => "UPDATE stream_uploads SET encryption_state = zeroblob(length(encryption_state)) WHERE session_id = ?1",
                            _ => unreachable!(),
                        };
                        assert_eq!(
                            pg.conn.execute(sql, [&owner.subject.id.attempt]).unwrap(),
                            1
                        );
                        let before = introduction::state(&pg);
                        let slot = pending_install::observe(&pg);
                        let indexed = pages(&pg);
                        let prefix = pg.metadata_command_replica_state().unwrap();
                        let writes = pg.conn.total_changes();
                        let error = PgStore::test_with_before_pending_slot_effect_hook(
                            |_| panic!("incomplete protection reached pending-slot INSERT"),
                            || match stage {
                                "fresh" | "retry" => {
                                    let error =
                                        install(&pg, &command, described.as_ref()).unwrap_err();
                                    match (stage, error) {
                                        ("fresh", InsertError::Definitive(error))
                                        | ("retry", InsertError::MayHaveApplied(error)) => error,
                                        (_, error) => panic!(
                                            "wrong custody classification: {stage}: {error:?}"
                                        ),
                                    }
                                }
                                _ => match replace(&pg, &command, &replacement, described.as_ref())
                                    .unwrap_err()
                                {
                                    ReplaceError::MayHaveApplied(error) => error,
                                    error => panic!("replacement lost custody: {error:?}"),
                                },
                            },
                        );
                        if fault == "encoding" {
                            assert!(matches!(error, StoreError::Db {
                                context: "load exact stream encryption for pending payload inventory", ..
                            }), "{error:?}");
                        } else {
                            assert_incomplete(
                                &error,
                                "missing exact stream encryption for pending payload",
                            );
                        }
                        assert_eq!(pg.conn.total_changes(), writes);
                        assert_eq!(introduction::state(&pg), before);
                        assert_eq!(pending_install::observe(&pg), slot);
                        assert_eq!(pages(&pg), indexed);
                        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                        pg.conn
                            .execute_batch("ROLLBACK TO crossed_geometry; RELEASE crossed_geometry")
                            .unwrap();
                    }
                    if matches!(stage, "fresh" | "retry") {
                        install(&pg, &command, described.as_ref()).unwrap();
                    } else {
                        assert!(replace(&pg, &command, &replacement, described.as_ref()).unwrap());
                    }
                    assert!(!pages(&pg).is_empty());
                }
            }
        }
    }
}

#[test]
fn pending_payload_subject_terminal_headers_must_cover_every_segment_before_installation() {
    for abort in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, 2);
        let command = if abort {
            abort::command(&pg, &owners[0])
        } else {
            completion::command(&pg, &owners[0], VersionId::Null)
        };
        pg.conn.execute_batch("COMMIT").unwrap();
        let expected = pg
            .encode_pending_shard_scavenger_reference_pages(&command)
            .unwrap();
        assert!(expected.placed_reference_count > 0);
        for fault in ["missing", "duplicate", "bucket", "key", "session"] {
            let mut payload = command.payload().clone();
            let (headers, segments) = match &mut payload {
                MetadataCommandPayload::CommitMultipartObject(command) => {
                    (&mut command.stream_uploads, &command.stream_upload_segments)
                }
                MetadataCommandPayload::AbortMultipartUpload(command) => (
                    &mut command.cleanup.stream_uploads,
                    &command.cleanup.stream_upload_segments,
                ),
                _ => unreachable!(),
            };
            assert!(!segments.is_empty());
            let index = headers
                .iter()
                .position(|header| header.session_id == segments[0].session_id)
                .unwrap();
            let reason = match fault {
                "missing" => {
                    headers.remove(index);
                    "missing terminal stream encryption for pending payload"
                }
                "duplicate" => {
                    headers.push(headers[index].clone());
                    "crossed or duplicate terminal stream header"
                }
                "bucket" => {
                    headers[index].bucket = BucketName::new("crossed-bucket").unwrap();
                    "crossed or duplicate terminal stream header"
                }
                "key" => {
                    headers[index].key = ObjectKey::new("crossed-key").unwrap();
                    "crossed or duplicate terminal stream header"
                }
                "session" => {
                    headers[index].session_id = crate::tests::stream_session_id("crossed-session");
                    "missing terminal stream encryption for pending payload"
                }
                _ => unreachable!(),
            };
            let changed = MetadataCommandEnvelope::new(command.id(), payload);
            assert!(changed.verify_checksum());
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let error = PgStore::test_with_before_pending_slot_effect_hook(
                |_| panic!("incomplete terminal protection reached pending-slot INSERT"),
                || install(&pg, &changed, None).unwrap_err(),
            );
            let InsertError::Definitive(error) = error else {
                panic!("{error:?}")
            };
            assert_incomplete(&error, reason);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
            assert!(pages(&pg).is_empty());
        }
        install(&pg, &command, None).unwrap();
        assert!(!pages(&pg).is_empty());
    }
}

#[test]
fn pending_payload_subject_failed_inspection_cannot_prove_nonacceptance() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, owner, command) = setup(StagingTarget::StreamPut, true);
    install(&pg, &command, None).unwrap();
    pg.conn
        .execute(
            "DELETE FROM stream_uploads WHERE session_id = ?1",
            [&owner.subject.id.attempt],
        )
        .unwrap();
    let slot = pending_install::observe(&pg);
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    pg.fail_next_pending_slot_inspection();
    let error = install(&pg, &command, None).unwrap_err();
    let InsertError::MayHaveApplied(error) = error else {
        panic!("{error:?}")
    };
    assert!(
        matches!(
            error,
            StoreError::Io {
                context: "injected pending metadata command slot reconciliation failure",
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(pending_install::observe(&pg), slot);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn pending_payload_subject_empty_abort_needs_no_removed_session_geometry() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let (_dir, pg, owner, command) = setup(target, false);
        let MetadataCommandPayload::AbortStreamUpload(mut abort) = command.payload().clone() else {
            unreachable!()
        };
        abort.staged_segments.clear();
        let command = MetadataCommandEnvelope::new(
            command.id(),
            MetadataCommandPayload::AbortStreamUpload(abort),
        );
        pg.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                [&owner.subject.id.attempt],
            )
            .unwrap();
        install(&pg, &command, None).unwrap();
        assert!(pages(&pg).is_empty());
        let slot = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        install(&pg, &command, None).unwrap();
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pending_install::observe(&pg), slot);
    }
}
