// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only inventory tests. No physical reads/deletes, authenticated
//! context installation or production custody switch is claimed here.

use super::super::abandonment::{
    source::PartFinalizationSource, AbandonmentError, AbandonmentOutcome,
};
use super::super::candidate::CandidateBindingError;
use super::super::command_context::{Applied, ContextError, ContextualCommand};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PendingShardScavengerReferencePages;
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{
    ShardScavengerPayloadReference as Reference, ShardScavengerPlacedShardSetReference,
};
use crate::{StoreError, VersionId};

fn assert_pages(
    actual: PendingShardScavengerReferencePages,
    expected: PendingShardScavengerReferencePages,
) {
    assert_eq!(
        actual.placed_reference_count,
        expected.placed_reference_count
    );
    assert_eq!(actual.placed_pages, expected.placed_pages);
    assert_eq!(
        actual.reclaim_reference_count,
        expected.reclaim_reference_count
    );
    assert_eq!(actual.reclaim_pages, expected.reclaim_pages);
}

fn ordinary(command: &MetadataCommandEnvelope) -> ContextualCommand {
    application::ordinary_context(command, &topology()).unwrap()
}

fn crossed_abort_segment(
    command: &MetadataCommandEnvelope,
    index: usize,
    foreign: &crate::StreamUploadSegmentRecord,
    session_only: bool,
) -> MetadataCommandEnvelope {
    let MetadataCommandPayload::AbortStreamUpload(mut abort) = command.payload().clone() else {
        unreachable!()
    };
    assert_ne!(abort.session_id, foreign.session_id);
    if session_only {
        // Isolate session identity from placement, geometry and integrity.
        abort.staged_segments[index]
            .session_id
            .clone_from(&foreign.session_id);
    } else {
        abort.staged_segments[index] = foreign.clone();
    }
    let crossed = MetadataCommandEnvelope::new(
        command.id(),
        MetadataCommandPayload::AbortStreamUpload(abort),
    );
    assert!(crossed.verify_checksum());
    crate::metadata_command::decode_metadata_command_envelope(
        &crossed.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap()
}

fn assert_crossed_abort(error: StoreError) {
    assert!(
        matches!(
            error,
            StoreError::ShardScavengerScanIncomplete {
                context: "derive pending command shard scavenger reference subjects",
                ref errors,
            } if errors == "abort segment belongs to another stream session"
        ),
        "{error:?}"
    );
}

#[test]
fn staging_context_inventory_ordinary_abort_rejects_crossed_segments_before_projection() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let (owner, other) = if target == StagingTarget::StreamPut {
            let owner = object::initialize(&pg, target.clone(), 2);
            let other = object::next_attempt(&pg, &owner, target, 2);
            (owner, other)
        } else {
            let owner = part::initialize(&pg, 2);
            let other = completion::new_attempt(&pg, &owner, 1, 7, 2);
            (owner, other)
        };
        assert_eq!(owner.subject.bucket, other.subject.bucket);
        assert_eq!(owner.subject.key, other.subject.key);
        assert_eq!(owner.subject.object_pg, other.subject.object_pg);
        let command = close::command(&pg, &owner);
        let other_command = close::command(&pg, &other);
        let foreign = pg.list_stream_segments(&other.subject.id.attempt).unwrap();
        let expected = ordinary(&command)
            .pending_inventory(&pg, &topology())
            .unwrap();
        assert_eq!(expected.payload().len(), 2);
        assert_eq!(
            ordinary(&other_command)
                .pending_inventory(&pg, &topology())
                .unwrap()
                .payload()
                .len(),
            2
        );
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        for index in [0, 1] {
            for session_only in [true, false] {
                let crossed = crossed_abort_segment(&command, index, &foreign[index], session_only);
                let Err(ContextError::Retention(error)) =
                    ordinary(&crossed).pending_inventory(&pg, &topology())
                else {
                    panic!("crossed abort must reject its segment subject");
                };
                assert_crossed_abort(error);
                assert_crossed_abort(
                    pg.encode_pending_shard_scavenger_reference_pages(&crossed)
                        .err()
                        .expect("bare-command projection must also fail closed"),
                );

                // Both ordinary and source-bound callers use this projector.
                // Reject the complete set before lookup or any partial output.
                let mut references = expected.payload().to_vec();
                let error = PgStore::extend_command_payload_references_with_stream_encryption(
                    &mut references,
                    crossed.payload(),
                    |_, _, _| panic!("crossed abort must reject before encryption lookup"),
                )
                .unwrap_err();
                assert_crossed_abort(error);
                assert_eq!(references, expected.payload());
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), before);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
        assert_pages(
            ordinary(&command)
                .pending_inventory(&pg, &topology())
                .unwrap()
                .encode_reference_pages()
                .unwrap(),
            expected.encode_reference_pages().unwrap(),
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_inventory_abandonment_rejects_crossed_segments_at_binding_and_decode() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let other = completion::new_attempt(&pg, &owner, 1, 7, 2);
    let source = part::command(&pg, &owner);
    let command = abandonment::cleanup(&pg, &owner, &source);
    let context = ContextualCommand::abandoned_part(
        PartFinalizationSource::capture(&source)
            .unwrap()
            .bind_cleanup(command.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap();
    let field = context.encode_field().unwrap();
    let expected = context.pending_inventory(&pg, &topology()).unwrap();
    assert_eq!(expected.payload().len(), 2);
    let decoded = ContextualCommand::decode_field(command.clone(), &field, &topology()).unwrap();
    assert_eq!(
        decoded
            .pending_inventory(&pg, &topology())
            .unwrap()
            .payload(),
        expected.payload()
    );
    let foreign = pg.list_stream_segments(&other.subject.id.attempt).unwrap();
    let before = introduction::state(&pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let writes = pg.conn.total_changes();
    for index in [0, 1] {
        for session_only in [true, false] {
            let crossed = crossed_abort_segment(&command, index, &foreign[index], session_only);
            assert!(matches!(
                PartFinalizationSource::capture(&source)
                    .unwrap()
                    .bind_cleanup(crossed.clone(), &topology()),
                Err(AbandonmentError::Subject)
            ));
            assert!(matches!(
                ContextualCommand::decode_field(crossed, &field, &topology()),
                Err(ContextError::Abandonment(AbandonmentError::Subject))
            ));
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_context_inventory_keeps_selected_payload_separate_from_all_future_windows() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let context = ContextualCommand::described(
            candidate::capture(&pg, &owner, command.clone(), is_append),
            &topology(),
        )
        .unwrap();
        assert!(matches!(
            context.pending_inventory(&pg, &topology()),
            Err(ContextError::InventoryTransaction)
        ));
        let snapshot = pg.command_build_snapshot().unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let actual = context.pending_inventory(&pg, &topology()).unwrap();
        assert_eq!(
            actual.retention(),
            &context.pending_retention(&topology()).unwrap()
        );
        assert!(actual.retention().future_window().is_some());
        if let MetadataCommandPayload::AppendStreamSegment(append) = command.payload() {
            let segment = &append.segment;
            assert_eq!(
                actual.payload(),
                &[Reference::Placed(ShardScavengerPlacedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    stored_size: owner.windows[0]
                        .geometry
                        .codec
                        .stored_size(segment.size)
                        .unwrap(),
                    crc64: segment.segment_crc64,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m
                    },
                    shard_integrity: segment.shard_integrity.clone(),
                })]
            );
            let future = actual.retention().future_window().unwrap();
            assert_ne!(future.identity.okh, segment.segment_okh);
            assert_ne!(future.identity.generation_id, segment.segment_vid);
        } else {
            assert!(actual.payload().is_empty());
        }
        assert_pages(
            actual.encode_reference_pages().unwrap(),
            pg.encode_pending_shard_scavenger_reference_pages(&command)
                .unwrap(),
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        snapshot.finish().unwrap();
    }
}

#[test]
fn staging_context_inventory_preserves_every_terminal_payload_page() {
    use application::Terminal;
    for case in application::TERMINALS {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let command = application::setup_terminal(&pg, case);
        let context = ordinary(&command);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let actual = context.pending_inventory(&pg, &topology()).unwrap();
        assert!(actual.retention().future_window().is_none());
        let pages = actual.encode_reference_pages().unwrap();
        // Independent fixture counts keep the shared encoder from masking a
        // dropped command branch or omitted selected/displaced/active payload.
        let expected_counts = match case {
            Terminal::CloseDirect => (0, 0),
            Terminal::CloseStream
            | Terminal::ClosePart
            | Terminal::ObjectStream
            | Terminal::Part => (2, 0),
            Terminal::ObjectDirect => (1, 0),
            Terminal::Complete | Terminal::Abort => (8, 0),
            Terminal::Delete | Terminal::Marker | Terminal::Reclaim => (0, 1),
        };
        assert_eq!(
            (pages.placed_reference_count, pages.reclaim_reference_count),
            expected_counts,
            "{case:?}"
        );
        assert_pages(
            pages,
            pg.encode_pending_shard_scavenger_reference_pages(&command)
                .unwrap(),
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_inventory_abandonment_survives_compaction_reopen_then_session_removal() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let command = abandonment::cleanup(&pg, &owner, &source);
        let candidate = PartFinalizationSource::capture(&source)
            .unwrap()
            .bind_cleanup(command.clone(), &topology())
            .unwrap();
        let context = ContextualCommand::abandoned_part(candidate, &topology()).unwrap();
        let context = ContextualCommand::decode_field(
            command.clone(),
            &context.encode_field().unwrap(),
            &topology(),
        )
        .unwrap();
        let expected = context.pending_inventory(&pg, &topology()).unwrap();
        assert_eq!(expected.payload().len(), count);
        assert!(expected.retention().future_window().is_none());
        assert_pages(
            expected.encode_reference_pages().unwrap(),
            pg.encode_pending_shard_scavenger_reference_pages(&command)
                .unwrap(),
        );
        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
            .unwrap();
        pg.compact_metadata_command_log(source.id().cluster_epoch())
            .unwrap();
        assert_eq!(
            pg.metadata_command_log_stats(source.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        // Reopen the real compacted source evidence first. The new cleanup
        // component is exercised inside a rolled-back transaction: v13 cannot
        // log/authenticate its separate context or checkpoint that new state.
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before_closure = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        assert_eq!(
            context
                .pending_inventory(&pg, &topology())
                .unwrap()
                .payload(),
            expected.payload()
        );
        assert_eq!(
            context
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Abandonment(AbandonmentOutcome::ClosedAndRetained)
        );
        assert_eq!(
            pg.conn
                .query_row(
                    "SELECT COUNT(*) FROM stream_uploads WHERE session_id = ?1",
                    [&owner.subject.id.attempt],
                    |row| row.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let actual = context.pending_inventory(&pg, &topology()).unwrap();
        assert_eq!(actual.retention(), expected.retention());
        assert_eq!(actual.payload(), expected.payload());
        assert_pages(
            actual.encode_reference_pages().unwrap(),
            expected.encode_reference_pages().unwrap(),
        );
        let ordinary = ordinary(&command).pending_inventory(&pg, &topology());
        if count == 0 {
            assert!(ordinary.unwrap().payload().is_empty());
        } else {
            assert_incomplete(ordinary, "missing exact stream encryption");
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(introduction::state(&pg), before_closure);
    }
}

fn assert_incomplete<T>(result: Result<T, ContextError>, reason: &str) {
    match result {
        Err(ContextError::Retention(StoreError::ShardScavengerScanIncomplete {
            context,
            errors,
        })) => {
            assert_eq!(context, "derive pending command payload inventory");
            assert!(errors.contains(reason), "{errors}");
        }
        _ => panic!("expected incomplete inventory: {reason}"),
    }
}

#[test]
fn staging_context_inventory_missing_or_crossed_live_session_is_not_an_empty_index() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let (_dir, pg, owner, command) = candidate::setup(target, true);
        let context = ContextualCommand::described(
            candidate::capture(&pg, &owner, command, true),
            &topology(),
        )
        .unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            context
                .pending_inventory(&pg, &topology())
                .unwrap()
                .payload()
                .len(),
            1
        );
        for crossed in ["bucket", "key", "missing"] {
            pg.conn.execute_batch("SAVEPOINT crossed_session").unwrap();
            match crossed {
                "bucket" => {
                    pg.conn.execute("UPDATE stream_uploads SET bucket = 'crossed-bucket' WHERE session_id = ?1", [&owner.subject.id.attempt]).unwrap();
                }
                "key" => {
                    pg.conn
                        .execute(
                            "UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1",
                            [&owner.subject.id.attempt],
                        )
                        .unwrap();
                }
                "missing" => {
                    pg.conn
                        .execute(
                            "DELETE FROM stream_uploads WHERE session_id = ?1",
                            [&owner.subject.id.attempt],
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            assert_incomplete(
                context.pending_inventory(&pg, &topology()),
                "missing exact stream encryption",
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn
                .execute_batch("ROLLBACK TO crossed_session; RELEASE crossed_session")
                .unwrap();
        }
        assert_eq!(
            context
                .pending_inventory(&pg, &topology())
                .unwrap()
                .payload()
                .len(),
            1
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_inventory_rejects_missing_or_ambiguous_terminal_encryption() {
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
        let positive = ordinary(&command)
            .pending_inventory(&pg, &topology())
            .unwrap();
        assert!(!positive.payload().is_empty());
        for fault in ["missing", "duplicate", "bucket", "key"] {
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
                    "missing terminal stream encryption"
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
                _ => unreachable!(),
            };
            let changed = ordinary(&MetadataCommandEnvelope::new(command.id(), payload));
            let writes = pg.conn.total_changes();
            let before = introduction::state(&pg);
            assert_incomplete(changed.pending_inventory(&pg, &topology()), reason);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
        }
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_inventory_requires_the_configured_topology_owning_pg_and_valid_geometry() {
    let dir = test_util::tempdir();
    let pg_id = fixture(StagingTarget::StreamPut).subject.object_pg.get();
    let pg = PgStore::open(dir.path(), pg_id).unwrap();
    let owner = object::initialize(&pg, StagingTarget::StreamPut, 2);
    let command = object::command(&pg, &owner, VersionId::Null);
    let context = ordinary(&command);
    let foreign = PgTopology::new(&[pg_id]).unwrap();
    assert!(matches!(
        context.pending_inventory(&pg, &foreign),
        Err(ContextError::Candidate(CandidateBindingError::Topology))
    ));
    let other_dir = test_util::tempdir();
    let other = PgStore::open(other_dir.path(), (pg_id + 1) % 8).unwrap();
    other.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(matches!(
        context.pending_inventory(&other, &topology()),
        Err(ContextError::CommandPg)
    ));
    other.conn.execute_batch("ROLLBACK").unwrap();
    for bad_pg in [false, true] {
        let MetadataCommandPayload::CommitDirectPutObject(mut payload) = command.payload().clone()
        else {
            unreachable!()
        };
        if bad_pg {
            payload.segments[0].data_pg_id = 99;
        } else {
            payload.segments[0].size = 1 << 52;
        }
        let changed = ordinary(&MetadataCommandEnvelope::new(
            command.id(),
            MetadataCommandPayload::CommitDirectPutObject(payload),
        ));
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            changed.pending_inventory(&pg, &topology()),
            Err(ContextError::Retention(
                StoreError::ShardScavengerScanIncomplete { .. }
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_context_inventory_page_boundary_keeps_every_selected_segment() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = object::initialize(&pg, StagingTarget::StreamPut, 65);
    let command = object::command(&pg, &owner, VersionId::Null);
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let actual = ordinary(&command)
        .pending_inventory(&pg, &topology())
        .unwrap();
    assert_eq!(actual.payload().len(), 65);
    let pages = actual.encode_reference_pages().unwrap();
    assert_eq!(pages.placed_reference_count, 65);
    assert_eq!(
        pages.placed_pages.iter().map(Vec::len).collect::<Vec<_>>(),
        [64 * 54, 54]
    );
    assert_eq!(pages.reclaim_reference_count, 0);
    assert!(pages.reclaim_pages.is_empty());
    assert_pages(
        pages,
        pg.encode_pending_shard_scavenger_reference_pages(&command)
            .unwrap(),
    );
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
