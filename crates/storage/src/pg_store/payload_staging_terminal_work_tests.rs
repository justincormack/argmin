// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only classification canaries. These descriptions do not establish
//! convergence or authorize cleanup; the installed fanout tests cover handoff.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;
use crate::VersionId;

fn ordinary(command: MetadataCommandEnvelope) -> TerminalWorkObservation {
    ContextualCommand::decode_field(command, &[1, 0, 0], &topology())
        .unwrap()
        .terminal_work()
}

#[test]
fn staging_terminal_work_keeps_creation_and_finalization_reservations_distinct() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
        let context =
            ContextualCommand::from_described(candidate::capture(&pg, &owner, command, is_append));
        let expected = if is_append || target == StagingTarget::DirectPut {
            TerminalReservationWork::NoCommandProof
        } else if target == StagingTarget::StreamPut {
            TerminalReservationWork::RetainForStream
        } else {
            TerminalReservationWork::ReleaseCommandProof
        };
        assert_eq!(
            context.terminal_work(),
            TerminalWorkObservation {
                reservation: expected,
                payload: TerminalPayloadWork::None,
                durable_reclaim_handoff: false,
                retained_finalization_proof: false,
            }
        );
    }
}

#[test]
fn staging_terminal_work_source_bound_abort_keeps_the_additional_finalization_obligation() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let ordinary = ordinary(cleanup.clone());
        let context = ContextualCommand::abandoned_part(
            super::super::abandonment::source::PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup, &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        let expected = TerminalWorkObservation {
            reservation: TerminalReservationWork::NoCommandProof,
            payload: TerminalPayloadWork::StreamAbort { segments: count },
            durable_reclaim_handoff: false,
            retained_finalization_proof: false,
        };
        assert_eq!(ordinary, expected);
        assert_eq!(
            context.terminal_work(),
            TerminalWorkObservation {
                retained_finalization_proof: true,
                ..expected
            }
        );
        // Whole-carrier round-trip must not downgrade the source-bound variant.
        let encoded = context.encode_command().unwrap();
        assert_eq!(
            ContextualCommand::decode_command(
                encoded.bytes(),
                encoded.checksum_crc64(),
                &topology()
            )
            .unwrap()
            .terminal_work(),
            context.terminal_work()
        );
    }
}

#[test]
fn staging_terminal_work_part_replacement_counts_displaced_not_selected_segments() {
    for (old_count, new_count) in [(0, 2), (2, 1)] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let old = part::initialize(&pg, old_count);
        let first = part::command(&pg, &old);
        assert_eq!(
            ordinary(first.clone()).payload,
            TerminalPayloadWork::PartReplacement { segments: 0 }
        );
        part::apply(&pg, &first).unwrap();
        let next = part::next_attempt(&pg, &old, new_count);
        let command = part::command(&pg, &next);
        let work = ordinary(command);
        assert_eq!(
            work.reservation,
            TerminalReservationWork::ReleaseCommandProof
        );
        assert_eq!(
            work.payload,
            TerminalPayloadWork::PartReplacement {
                segments: old_count
            }
        );
        assert!(!work.retained_finalization_proof);
    }
}

#[test]
fn staging_terminal_work_multipart_completion_and_abort_keep_independent_cleanup_sets() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, count);
        let complete = ordinary(completion::command(
            &pg,
            &owners[0],
            VersionId::from_u64(42),
        ));
        assert_eq!(
            complete.reservation,
            TerminalReservationWork::ReleaseCommandProof
        );
        assert!(!complete.durable_reclaim_handoff);
        assert_eq!(
            complete.payload,
            TerminalPayloadWork::MultipartCompletion {
                omitted_parts: 1,
                omitted_segments: count,
                sessions: 2,
                session_segments: count,
            }
        );
        let abort = ordinary(abort::command(&pg, &owners[0]));
        assert_eq!(
            abort.reservation,
            TerminalReservationWork::ReleaseCommandProof
        );
        assert_eq!(
            abort.payload,
            TerminalPayloadWork::MultipartAbort {
                parts: 3,
                part_segments: count * 3,
                sessions: 2,
                session_segments: count,
            }
        );
    }
}

#[test]
fn staging_terminal_work_stream_abort_keeps_its_creation_proof_and_exact_segment_count() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = object::initialize(&pg, StagingTarget::StreamPut, 2);
    let command = super::close::command(&pg, &owner);
    assert_eq!(
        ordinary(command),
        TerminalWorkObservation {
            reservation: TerminalReservationWork::ReleaseStreamCreationProof,
            payload: TerminalPayloadWork::StreamAbort { segments: 2 },
            durable_reclaim_handoff: false,
            retained_finalization_proof: false,
        }
    );
}

#[test]
fn staging_terminal_work_reclaim_is_a_durable_handoff_not_synchronous_payload_cleanup() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let owners = delete::source(&pg, 0, VersionId::Null);
    let deletion = delete::command(
        &pg,
        &owners[0],
        VersionId::Null,
        crate::metadata_command::DeleteObjectVersionMode::Current,
    );
    let work = ordinary(deletion);
    assert!(work.durable_reclaim_handoff);
    assert_eq!(work.payload, TerminalPayloadWork::None);
    assert_eq!(
        work.reservation,
        TerminalReservationWork::ReleaseCommandProof
    );
    // The no-stale Direct PUT branch must not report an unrelated reclaim.
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = object::initialize(&pg, StagingTarget::DirectPut, 1);
    let command = object::command(&pg, &owner, VersionId::Null);
    assert!(matches!(
        command.payload(),
        MetadataCommandPayload::CommitDirectPutObject(_)
    ));
    let work = ordinary(command);
    assert!(!work.durable_reclaim_handoff);
    assert_eq!(
        work.reservation,
        TerminalReservationWork::ReleaseCommandProof
    );
}

#[test]
fn staging_terminal_work_overwrites_and_markers_keep_reclaim_handoffs_separate() {
    for kind in 0..3 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let old = displacement::source(&pg, 2);
        let (command, _) = displacement::incoming(&pg, &old, kind);
        let work = ordinary(command);
        assert!(work.durable_reclaim_handoff);
        assert_eq!(
            work.reservation,
            TerminalReservationWork::ReleaseCommandProof
        );
        if kind == 2 {
            assert!(matches!(
                work.payload,
                TerminalPayloadWork::MultipartCompletion { .. }
            ));
        } else {
            assert_eq!(work.payload, TerminalPayloadWork::None);
        }
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let owners = delete::source(&pg, 0, VersionId::Null);
    let marker = marker::command(&pg, &owners[0], VersionId::Null);
    assert!(ordinary(marker.clone()).durable_reclaim_handoff);
    super::super::marker::apply_in_open_transaction(&pg, &topology(), &marker).unwrap();
    let repeated = marker::command(&pg, &owners[0], VersionId::Null);
    assert!(!ordinary(repeated).durable_reclaim_handoff);
    let delete_marker = delete::command(
        &pg,
        &owners[0],
        VersionId::Null,
        crate::metadata_command::DeleteObjectVersionMode::Current,
    );
    let work = ordinary(delete_marker);
    assert!(!work.durable_reclaim_handoff);
    assert_eq!(work.payload, TerminalPayloadWork::None);
}
