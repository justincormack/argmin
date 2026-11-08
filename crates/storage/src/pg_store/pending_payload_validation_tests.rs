// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact pending page validation using metadata-only owner fixtures. No physical
//! writes, contextual-carrier activation, or cleanup custody is granted here.

use super::pending_payload_transaction::{install, pages, replace, setup, successor};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    DeleteObjectVersionMode, DeleteObjectVersionTarget, MetadataCommandEnvelope,
    MetadataCommandPayload,
};
use crate::node_runtime::pg_store::{
    PendingMetadataCommandSlotInsertError as InsertError,
    PendingMetadataCommandSlotReplaceError as ReplaceError, PgStore,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::{StoreError, VersionId};

const TABLES: [(&str, &str, usize); 2] = [
    (
        "metadata_command_pending_placed_reference_pages",
        "placed_segment_reference_count",
        54,
    ),
    (
        "metadata_command_pending_reclaim_reference_pages",
        "reclaim_reference_count",
        30,
    ),
];

#[derive(Clone, Copy, Debug)]
enum Stage {
    Retry,
    SourceReissue,
    SuccessorRetry,
}

const STAGES: [Stage; 3] = [Stage::Retry, Stage::SourceReissue, Stage::SuccessorRetry];

fn attempt(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    stage: Stage,
) -> Result<(), StoreError> {
    match stage {
        Stage::Retry => install(pg, command, None).map_err(|error| {
            let InsertError::MayHaveApplied(error) = error else {
                panic!("lost installed custody: {error:?}")
            };
            error
        }),
        Stage::SourceReissue | Stage::SuccessorRetry => {
            replace(pg, command, &successor(command), None)
                .map(|replaced| assert!(replaced))
                .map_err(|error| {
                    let ReplaceError::MayHaveApplied(error) = error else {
                        panic!("lost installed custody: {error:?}")
                    };
                    error
                })
        }
    }
}

fn assert_mismatch(error: &StoreError) {
    assert!(
        matches!(
            error,
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-existing-payload-pages"
            }
        ),
        "{error:?}"
    );
}

fn assert_no_changes(pg: &PgStore, command: &MetadataCommandEnvelope, stage: Stage) {
    let slot = pending_install::observe(pg);
    let indexed = pages(pg);
    let state = introduction::state(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let counts: (i64, i64) = pg.conn.query_row("SELECT placed_segment_reference_count, reclaim_reference_count FROM metadata_command_pending_slot", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    let writes = pg.conn.total_changes();
    assert_mismatch(&attempt(pg, command, stage).unwrap_err());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(pending_install::observe(pg), slot);
    assert_eq!(pages(pg), indexed);
    assert_eq!(introduction::state(pg), state);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    let after: (i64, i64) = pg.conn.query_row("SELECT placed_segment_reference_count, reclaim_reference_count FROM metadata_command_pending_slot", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(after, counts);
}

fn mixed_command(pg: &PgStore, count: usize) -> MetadataCommandEnvelope {
    let old = object::initialize(pg, StagingTarget::StreamPut, count);
    object::apply(pg, &object::command(pg, &old, VersionId::Null)).unwrap();
    let delete = delete::command(pg, &old, VersionId::Null, DeleteObjectVersionMode::Specific);
    let MetadataCommandPayload::DeleteObjectVersion(delete) = delete.payload() else {
        unreachable!()
    };
    let DeleteObjectVersionTarget::Live { payload, .. } = &delete.target else {
        unreachable!()
    };
    let next = object::next_attempt(pg, &old, StagingTarget::StreamPut, count);
    let command = object::command(pg, &next, VersionId::Null);
    let mut commit = object::payload(&command).clone();
    commit.stale_payload = Some(payload.clone());
    let command = MetadataCommandEnvelope::new(
        command.id(),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(commit)),
    );
    // Positive application canary: the mixed specimen describes the real old
    // payload and new write, not an arbitrary protection-only test envelope.
    pg.conn.execute_batch("SAVEPOINT validate_fixture").unwrap();
    object::apply(pg, &command).unwrap();
    pg.conn
        .execute_batch("ROLLBACK TO validate_fixture; RELEASE validate_fixture; COMMIT")
        .unwrap();
    command
}

#[test]
fn pending_payload_validation_all_terminal_families_retry_reissue_and_reopen() {
    for family in application::TERMINALS {
        let dir = test_util::tempdir();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let command = application::setup_terminal(&pg, family);
        pg.conn.execute_batch("COMMIT").unwrap();
        install(&pg, &command, None).unwrap();
        let writes = pg.conn.total_changes();
        attempt(&pg, &command, Stage::Retry).unwrap();
        assert_eq!(pg.conn.total_changes(), writes);
        attempt(&pg, &command, Stage::SourceReissue).unwrap();
        let slot = pending_install::observe(&pg);
        let indexed = pages(&pg);
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let writes = pg.conn.total_changes();
        attempt(&pg, &command, Stage::SuccessorRetry).unwrap();
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pages(&pg), indexed);
    }
}

#[test]
fn pending_payload_validation_corrupt_pages_preserve_uncertainty_without_repair() {
    for count in [2, 65] {
        for stage in STAGES {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::StreamPut).subject.object_pg.get(),
            )
            .unwrap();
            let command = mixed_command(&pg, count);
            install(&pg, &command, None).unwrap();
            if matches!(stage, Stage::SuccessorRetry) {
                attempt(&pg, &command, Stage::SourceReissue).unwrap();
            }
            let expected = pg
                .encode_pending_shard_scavenger_reference_pages(&command)
                .unwrap();
            assert_eq!(expected.placed_reference_count as usize, count);
            assert_eq!(expected.reclaim_reference_count as usize, count);
            assert_eq!(expected.placed_pages.len(), count.div_ceil(64));
            assert_eq!(expected.reclaim_pages.len(), count.div_ceil(64));
            for (table, column, width) in TABLES {
                for fault in [
                    "missing first",
                    "missing last",
                    "bytes",
                    "gap",
                    "extra",
                    "slot count",
                    "co-crossed empty",
                    "page count",
                    "page count only",
                    "oversized blob",
                ] {
                    pg.conn.execute_batch("SAVEPOINT corruption").unwrap();
                    match fault {
                        "missing first" => {
                            pg.conn
                                .execute(&format!("DELETE FROM {table} WHERE page_index = 0"), [])
                                .unwrap();
                        }
                        "missing last" => {
                            pg.conn.execute(&format!("DELETE FROM {table} WHERE page_index = (SELECT max(page_index) FROM {table})"), []).unwrap();
                        }
                        "bytes" => {
                            let mut bytes: Vec<u8> = pg.conn.query_row(&format!("SELECT encoded_references FROM {table} WHERE page_index = 0"), [], |row| row.get(0)).unwrap();
                            bytes[0] ^= 1;
                            pg.conn.execute(&format!("UPDATE {table} SET encoded_references = ?1 WHERE page_index = 0"), [bytes]).unwrap();
                        }
                        "gap" => {
                            pg.conn
                                .execute(
                                    &format!(
                                        "UPDATE {table} SET page_index = 100 WHERE page_index = 0"
                                    ),
                                    [],
                                )
                                .unwrap();
                        }
                        "extra" => {
                            pg.conn.execute(&format!("INSERT INTO {table} SELECT singleton, 100, reference_count, encoded_references FROM {table} WHERE page_index = 0"), []).unwrap();
                        }
                        "slot count" => {
                            pg.conn.execute(&format!("UPDATE metadata_command_pending_slot SET {column} = {column} + 1"), []).unwrap();
                        }
                        "co-crossed empty" => {
                            pg.conn.execute_batch(&format!("DELETE FROM {table}; UPDATE metadata_command_pending_slot SET {column} = 0")).unwrap();
                        }
                        "page count" => {
                            // Preserve SQL's own size/count constraint while
                            // shortening the first page or growing the last.
                            pg.conn.execute(&format!("UPDATE {table} SET reference_count = 1, encoded_references = substr(encoded_references, 1, {width}) WHERE page_index = 0"), []).unwrap();
                        }
                        "page count only" => {
                            // Isolate the count predicate from byte equality;
                            // simulate corruption that bypassed SQL constraints.
                            pg.conn
                                .pragma_update(None, "ignore_check_constraints", true)
                                .unwrap();
                            pg.conn.execute(&format!("UPDATE {table} SET reference_count = reference_count - 1 WHERE page_index = 0"), []).unwrap();
                            pg.conn
                                .pragma_update(None, "ignore_check_constraints", false)
                                .unwrap();
                        }
                        "oversized blob" => {
                            pg.conn
                                .pragma_update(None, "ignore_check_constraints", true)
                                .unwrap();
                            pg.conn.execute(&format!("UPDATE {table} SET encoded_references = zeroblob(1048576) WHERE page_index = 0"), []).unwrap();
                            pg.conn
                                .pragma_update(None, "ignore_check_constraints", false)
                                .unwrap();
                        }
                        _ => unreachable!(),
                    }
                    assert_no_changes(&pg, &command, stage);
                    pg.conn
                        .execute_batch("ROLLBACK TO corruption; RELEASE corruption")
                        .unwrap();
                }
            }
            attempt(&pg, &command, stage).unwrap();
        }
    }
}

#[test]
fn pending_payload_validation_empty_indexes_reject_extra_pages() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases().into_iter().filter(|(_, append)| !append) {
        for bound in [false, true] {
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let described = bound.then(|| candidate::capture(&pg, &owner, command.clone(), false));
            install(&pg, &command, described.as_ref()).unwrap();
            assert!(pages(&pg).is_empty());
            for (table, _, width) in TABLES {
                pg.conn.execute_batch("SAVEPOINT extra").unwrap();
                pg.conn
                    .execute(
                        &format!("INSERT INTO {table} VALUES (0, 0, 1, zeroblob({width}))"),
                        [],
                    )
                    .unwrap();
                let before = pages(&pg);
                let writes = pg.conn.total_changes();
                let error = install(&pg, &command, described.as_ref()).unwrap_err();
                let InsertError::MayHaveApplied(error) = error else {
                    panic!("{error:?}")
                };
                assert_mismatch(&error);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(pages(&pg), before);
                pg.conn
                    .execute_batch("ROLLBACK TO extra; RELEASE extra")
                    .unwrap();
            }
            install(&pg, &command, described.as_ref()).unwrap();
        }
    }
}

#[test]
fn pending_payload_validation_bound_append_preserves_holder_on_missing_pages_or_session() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for remove_session in [false, true] {
            let (_dir, pg, owner, command) = setup(target.clone(), true);
            let mut submission =
                candidate::capture(&pg, &owner, command.clone(), true).into_submission();
            submission
                .install(
                    &pg,
                    0,
                    &topology(),
                    crate::types::AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
                    None,
                )
                .unwrap();
            if remove_session {
                pg.conn
                    .execute(
                        "DELETE FROM stream_uploads WHERE session_id = ?1",
                        [&owner.subject.id.attempt],
                    )
                    .unwrap();
            } else {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_pending_placed_reference_pages",
                        [],
                    )
                    .unwrap();
            }
            let slot = pending_install::observe(&pg);
            let state = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let error = submission
                .install(
                    &pg,
                    0,
                    &topology(),
                    crate::types::AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
                    None,
                )
                .unwrap_err();
            let super::super::submission::SubmissionError::Unconfirmed(error) = error else {
                panic!("{error:?}")
            };
            if remove_session {
                assert!(
                    matches!(error, StoreError::ShardScavengerScanIncomplete {
                    context: "derive pending command payload inventory", ref errors
                } if errors == "missing exact stream encryption for pending payload"),
                    "{error:?}"
                );
            } else {
                assert_mismatch(&error);
            }
            assert_eq!(
                submission.observation(),
                super::super::submission::InstallationObservation::Unconfirmed
            );
            drop(submission);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(introduction::state(&pg), state);
        }
    }
}

#[test]
fn pending_payload_validation_applied_abort_replay_does_not_require_removed_session() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let (dir, pg, owner, command) = setup(target, false);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                command.id().cluster_epoch(),
                command.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        pg.refresh_metadata_command_state_digest().unwrap();
        install(&pg, &command, None).unwrap();
        pg.mark_pending_metadata_command_publication_started(0, &command)
            .unwrap();
        let indexed = pages(&pg);
        assert!(!indexed.is_empty());
        let applied = pg.apply_metadata_command_and_record(0, &command).unwrap();
        assert!(matches!(
            pg.get_stream_upload(&owner.subject.id.attempt),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(pages(&pg), indexed);
        drop(pg);
        let pg = PgStore::open(dir.path(), command.id().pg_id().get()).unwrap();
        let before = introduction::state(&pg);
        let slot = pending_install::observe(&pg);
        // Applied convergence is a different boundary, not a fresh pending
        // installation. It must not rederive geometry from the removed session.
        assert_eq!(
            pg.apply_metadata_command_and_record(0, &command).unwrap(),
            applied
        );
        assert_eq!(pages(&pg), indexed);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(introduction::state(&pg), before);
        assert!(pg
            .remove_pending_metadata_command_slot(0, &command)
            .unwrap());
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn pending_payload_validation_bound_reissue_keeps_both_candidates_when_pages_are_lost() {
    use super::super::submission::{InstallationObservation, SubmissionError};
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for successor_installed in [false, true] {
            let (_dir, pg, owner, command) = setup(target.clone(), true);
            let mut submission =
                candidate::capture(&pg, &owner, command.clone(), true).into_submission();
            submission
                .install(
                    &pg,
                    0,
                    &topology(),
                    crate::types::AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
                    None,
                )
                .unwrap();
            let replacement = successor(&command);
            let mut pair = submission
                .into_same_epoch_reissue(replacement.id())
                .unwrap();
            if successor_installed {
                assert!(pair.replace(&pg, 0, &topology()).unwrap());
            }
            pg.conn
                .execute(
                    "DELETE FROM metadata_command_pending_placed_reference_pages",
                    [],
                )
                .unwrap();
            let slot = pending_install::observe(&pg);
            let state = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let error = pair.replace(&pg, 0, &topology()).unwrap_err();
            let SubmissionError::Unconfirmed(error) = error else {
                panic!("{error:?}")
            };
            assert_mismatch(&error);
            assert_eq!(pair.observation(), InstallationObservation::Unconfirmed);
            assert_eq!(pair.source().description().command(), &command);
            assert_eq!(pair.successor().command(), &replacement);
            let pair = pair
                .into_observed_successor()
                .err()
                .expect("damaged pages must not detach the source");
            drop(pair);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(introduction::state(&pg), state);
        }
    }
}
