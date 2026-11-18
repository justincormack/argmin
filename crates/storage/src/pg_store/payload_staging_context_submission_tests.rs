// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only one-way handoffs. Actual classified SQL outcomes seed history;
//! no fabricated observation or production carrier/custody activation.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::command_context::{ContextualCommand, PendingInventoryObservation};
use super::super::submission::reissue::StagingReissue;
use super::super::submission::{
    ContextSubmissionObservation, ContextualEpochReissue, ContextualReissue,
    ContextualReissueInspectionFailure, ContextualReissuePreparationFailure, ContextualSubmission,
    ContextualSubmissionPreparationFailure, EpochReissueInspectionFailure,
    InstallationObservation as Observed, PreparedContextualReissue, PreparedContextualSubmission,
    PreparedEpochReissue, StagingSubmission, SubmissionError,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[path = "payload_staging_context_submission_reissue_tests.rs"]
mod reissue;

#[path = "payload_staging_context_epoch_reissue_tests.rs"]
mod epoch_reissue;

#[path = "payload_staging_context_submission_preparation_tests.rs"]
mod preparation;

#[path = "payload_staging_context_reissue_preparation_tests.rs"]
mod reissue_preparation;

#[path = "payload_staging_context_fresh_submission_tests.rs"]
mod fresh;

#[path = "payload_staging_context_source_preparation_tests.rs"]
mod source_preparation;

#[path = "payload_staging_context_part_abandonment_tests.rs"]
mod part_abandonment;

fn described_context(
    command: &MetadataCommandEnvelope,
    descriptor: &[u8],
    append: bool,
) -> ContextualCommand {
    let mut field = vec![1, 0, if append { 2 } else { 1 }];
    field.extend_from_slice(&u32::try_from(descriptor.len()).unwrap().to_le_bytes());
    field.extend_from_slice(descriptor);
    ContextualCommand::decode_field(command.clone(), &field, &topology()).unwrap()
}

fn reject(submission: &mut StagingSubmission, pg: &PgStore) {
    let epoch = submission.description().command().id().cluster_epoch();
    assert!(matches!(
        submission.install(
            pg,
            0,
            &topology(),
            AdmittedRouteEffectFence::unbounded(epoch),
            Some(Instant::now())
        ),
        Err(SubmissionError::Rejected(
            crate::StoreError::OperationDeadlineExceeded { .. }
        ))
    ));
}

fn interrupt(submission: &mut StagingSubmission, pg: &PgStore) {
    let before = (pending_install::observe(pg), introduction::state(pg));
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PgStore::test_with_before_pending_slot_effect_hook(
            |in_transaction| {
                assert!(in_transaction);
                panic!("interrupted install");
            },
            || submission::install(submission, pg),
        )
    }));
    assert!(interrupted.is_err());
    assert_eq!(submission.observation(), Observed::Unconfirmed);
    assert!(pg.conn.is_autocommit());
    pg.require_usable_connection().unwrap();
    assert_eq!(
        (pending_install::observe(pg), introduction::state(pg)),
        before
    );
}

#[test]
fn staging_context_submission_promotes_all_installation_histories_without_mutation() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for outcome in [
            "fresh",
            "rejected",
            "installed",
            "uncertain commit",
            "unwind",
            "damaged retry",
        ] {
            clock.set(2_000);
            let (dir, pg, owner, command) = candidate::setup(target.clone(), append);
            let captured = candidate::capture(&pg, &owner, command.clone(), append);
            let expected = described_context(&command, &captured.descriptor_bytes(), append);
            let mut holder = captured.into_submission();
            let observed = match outcome {
                "fresh" => Observed::NoAcceptanceObserved,
                "rejected" => {
                    reject(&mut holder, &pg);
                    Observed::NoAcceptanceObserved
                }
                "installed" => {
                    submission::install(&mut holder, &pg).unwrap();
                    reject(&mut holder, &pg);
                    Observed::Installed
                }
                "uncertain commit" => {
                    pg.conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
                        CREATE TEMP TRIGGER fail_commit_after_index AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
                    assert!(matches!(
                        submission::install(&mut holder, &pg),
                        Err(SubmissionError::Unconfirmed(_))
                    ));
                    pending_install::assert_empty(&pg);
                    reject(&mut holder, &pg);
                    Observed::Unconfirmed
                }
                "unwind" => {
                    interrupt(&mut holder, &pg);
                    reject(&mut holder, &pg);
                    Observed::Unconfirmed
                }
                "damaged retry" => {
                    submission::install(&mut holder, &pg).unwrap();
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                        .unwrap();
                    assert!(matches!(
                        submission::install(&mut holder, &pg),
                        Err(SubmissionError::Unconfirmed(_))
                    ));
                    reject(&mut holder, &pg);
                    Observed::Unconfirmed
                }
                _ => unreachable!(),
            };
            let before = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            // No route/clock recapture; the original window has now expired.
            clock.set(9_000_000);
            let holder = holder.into_contextual();
            assert_eq!(
                holder.observation(),
                ContextSubmissionObservation {
                    command_id: command.id(),
                    installation: observed
                }
            );
            holder.assert_context_for_test(&expected, &topology());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            drop(pg);
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            holder.assert_context_for_test(&expected, &topology());
            assert_eq!(holder.observation().installation, observed);
            let writes = pg.conn.total_changes();
            // Unwinding Drop must not perform cleanup or erase durable evidence.
            let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _holder = holder;
                panic!("drop complete-context holder");
            }));
            assert!(dropped.is_err());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_submission_promotes_both_reissue_candidates_and_independent_histories() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for source_observed in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            for replacement in ["not sent", "rejected", "lost reply", "installed"] {
                let (_dir, pg, owner, command) = candidate::setup(target.clone(), append);
                let captured = candidate::capture(&pg, &owner, command.clone(), append);
                let descriptor = captured.descriptor_bytes();
                let expected_source = described_context(&command, &descriptor, append);
                // Install without observing its result through this holder.
                pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
                let mut source = captured.into_submission();
                match source_observed {
                    Observed::NoAcceptanceObserved => {}
                    Observed::Unconfirmed => interrupt(&mut source, &pg),
                    Observed::Installed => submission::install(&mut source, &pg).unwrap(),
                }
                let successor = pending_payload_transaction::successor(&command);
                let expected_successor = described_context(&successor, &descriptor, append);
                let mut pair = source.into_same_epoch_reissue(successor.id()).unwrap();
                let successor_observed = match replacement {
                    "not sent" => Observed::NoAcceptanceObserved,
                    "rejected" => {
                        pg.fail_next_pending_slot_inspection
                            .store(true, Ordering::Relaxed);
                        assert!(matches!(
                            pair.replace(&pg, 0, &topology()),
                            Err(SubmissionError::Rejected(_))
                        ));
                        Observed::NoAcceptanceObserved
                    }
                    "lost reply" => {
                        pg.fail_next_pending_slot_replace_after_commit
                            .store(true, Ordering::Relaxed);
                        assert!(matches!(
                            pair.replace(&pg, 0, &topology()),
                            Err(SubmissionError::Unconfirmed(_))
                        ));
                        // A subsequent rejected retry cannot erase that history.
                        pg.fail_next_pending_slot_inspection
                            .store(true, Ordering::Relaxed);
                        assert!(matches!(
                            pair.replace(&pg, 0, &topology()),
                            Err(SubmissionError::Rejected(_))
                        ));
                        Observed::Unconfirmed
                    }
                    "installed" => {
                        assert!(pair.replace(&pg, 0, &topology()).unwrap());
                        Observed::Installed
                    }
                    _ => unreachable!(),
                };
                let slot = pending_install::observe(&pg);
                let before = introduction::state(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                let pair = pair.into_contextual();
                assert_eq!(
                    pair.source(),
                    ContextSubmissionObservation {
                        command_id: command.id(),
                        installation: source_observed
                    }
                );
                assert_eq!(
                    pair.successor(),
                    ContextSubmissionObservation {
                        command_id: successor.id(),
                        installation: successor_observed
                    }
                );
                pair.assert_contexts_for_test(&expected_source, &expected_successor, &topology());
                match pair.into_observed_successor() {
                    Ok(holder) => {
                        assert_eq!(successor_observed, Observed::Installed);
                        assert_eq!(holder.observation().installation, Observed::Installed);
                        holder.assert_context_for_test(&expected_successor, &topology());
                        drop(holder);
                    }
                    Err(pair) => {
                        assert_ne!(successor_observed, Observed::Installed);
                        pair.assert_contexts_for_test(
                            &expected_source,
                            &expected_successor,
                            &topology(),
                        );
                        assert_eq!(pair.source().installation, source_observed);
                        assert_eq!(pair.successor().installation, successor_observed);
                        drop(pair);
                    }
                }
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(introduction::state(&pg), before);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
    }
}

#[test]
fn staging_context_submission_fresh_inputs_retain_all_context_kinds() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, append);
        let captured = candidate::capture(&pg, &owner, command.clone(), append);
        let expected = described_context(&command, &captured.descriptor_bytes(), append);
        check_fresh(
            ContextualCommand::described(captured, &topology()).unwrap(),
            &expected,
            &pg,
        );
    }
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
        let context = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup.clone(), &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        let expected = ContextualCommand::decode_field(
            cleanup.clone(),
            &context.encode_field().unwrap(),
            &topology(),
        )
        .unwrap();
        check_fresh(context, &expected, &pg);
        // Same raw abort, different mandatory context: neither may be dropped.
        let ordinary =
            ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
        let expected = ContextualCommand::decode_field(cleanup, &[1, 0, 0], &topology()).unwrap();
        check_fresh(ordinary, &expected, &pg);
        let command = MetadataCommandEnvelope::new(
            owner.subject.id.introduction,
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket.clone(),
                owner.subject.key.clone(),
                crate::VersionId::from_u64(17),
            )),
        );
        let ordinary =
            ContextualCommand::decode_field(command.clone(), &[1, 0, 0], &topology()).unwrap();
        let expected = ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap();
        check_fresh(ordinary, &expected, &pg);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

fn check_fresh(context: ContextualCommand, expected: &ContextualCommand, pg: &PgStore) {
    let before = introduction::state(pg);
    let writes = pg.conn.total_changes();
    let holder = context.into_submission();
    let ContextSubmissionObservation {
        command_id,
        installation,
    } = holder.observation();
    assert_eq!(command_id, expected.id());
    assert_eq!(installation, Observed::NoAcceptanceObserved);
    holder.assert_context_for_test(expected, &topology());
    drop(holder);
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
}

#[test]
fn staging_context_submission_api_is_consuming_scalar_only_and_noncloneable() {
    use super::super::capture::StagingCapture;
    use super::super::submission::FreshContextSubmissionError;
    use super::super::submission::{
        ContextualPartAbandonment, ContextualPartAbandonmentReissue,
        PartAbandonmentPreparationFailure, PartAbandonmentReissueFailure,
        PartAbandonmentReissueInspectionFailure, PreparedPartAbandonmentReissue,
    };

    let _: for<'pg> fn(
        ContextualPartAbandonmentReissue,
        &'pg PgStore,
        u32,
        &PgTopology,
    ) -> Result<
        PreparedPartAbandonmentReissue<'pg>,
        PartAbandonmentReissueInspectionFailure,
    > = ContextualPartAbandonmentReissue::prepare_pending;
    let _: fn(PartAbandonmentReissueInspectionFailure) -> ContextualPartAbandonmentReissue =
        PartAbandonmentReissueInspectionFailure::into_reissue;
    let _: fn(
        PreparedPartAbandonmentReissue<'static>,
    )
        -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueInspectionFailure> =
        PreparedPartAbandonmentReissue::finish;
    let _: fn(&PreparedPartAbandonmentReissue<'static>) -> PendingInventoryObservation =
        PreparedPartAbandonmentReissue::source_inventory;
    let _: fn(&PreparedPartAbandonmentReissue<'static>) -> PendingInventoryObservation =
        PreparedPartAbandonmentReissue::previous_cleanup_inventory;
    let _: fn(&PreparedPartAbandonmentReissue<'static>) -> PendingInventoryObservation =
        PreparedPartAbandonmentReissue::proposed_cleanup_inventory;

    let _: fn(
        ContextualPartAbandonment,
        MetadataCommandId,
    ) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure> =
        ContextualPartAbandonment::into_same_epoch_reissue;
    let _: fn(
        ContextualPartAbandonment,
        MetadataCommandId,
    ) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure> =
        ContextualPartAbandonment::into_epoch_reissue;
    let _: fn(PartAbandonmentReissueFailure) -> ContextualPartAbandonment =
        PartAbandonmentReissueFailure::into_proposal;
    let _: fn(&ContextualPartAbandonmentReissue) -> ContextSubmissionObservation =
        ContextualPartAbandonmentReissue::source;
    let _: fn(&ContextualPartAbandonmentReissue) -> ContextSubmissionObservation =
        ContextualPartAbandonmentReissue::previous_cleanup;
    let _: fn(&ContextualPartAbandonmentReissue) -> ContextSubmissionObservation =
        ContextualPartAbandonmentReissue::proposed_cleanup;

    let _: fn(
        ContextualSubmission,
        &PgStore,
        u32,
        &PgTopology,
        MetadataCommandId,
    ) -> Result<ContextualPartAbandonment, PartAbandonmentPreparationFailure> =
        ContextualSubmission::prepare_part_abandonment;
    let _: fn(PartAbandonmentPreparationFailure) -> ContextualSubmission =
        PartAbandonmentPreparationFailure::into_submission;
    let _: fn(&ContextualPartAbandonment) -> ContextSubmissionObservation =
        ContextualPartAbandonment::source;
    let _: fn(&ContextualPartAbandonment) -> ContextSubmissionObservation =
        ContextualPartAbandonment::cleanup;

    let _: fn(
        &StagingCapture<'static>,
        &PgStore,
        NodeId,
        MetadataCommandEnvelope,
        &BucketWriteReservationProof,
    ) -> Result<ContextualSubmission, FreshContextSubmissionError> =
        StagingCapture::introduction_submission;
    let _: fn(
        &StagingCapture<'static>,
        &PgStore,
        NodeId,
        MetadataCommandEnvelope,
    ) -> Result<ContextualSubmission, FreshContextSubmissionError> =
        StagingCapture::append_submission;
    let _: fn(ContextualCommand) -> ContextualSubmission = ContextualCommand::into_submission;
    let _: for<'pg> fn(
        ContextualSubmission,
        &'pg PgStore,
        u32,
        &PgTopology,
    ) -> Result<
        PreparedContextualSubmission<'pg>,
        ContextualSubmissionPreparationFailure,
    > = ContextualSubmission::prepare_abandoned_part_pending;
    let _: fn(StagingSubmission) -> ContextualSubmission = StagingSubmission::into_contextual;
    let _: fn(StagingReissue) -> ContextualReissue = StagingReissue::into_contextual;
    let _: fn(
        ContextualSubmission,
        MetadataCommandId,
    ) -> Result<ContextualReissue, ContextualReissuePreparationFailure> =
        ContextualSubmission::into_same_epoch_reissue;
    let _: fn(ContextualReissuePreparationFailure) -> ContextualSubmission =
        ContextualReissuePreparationFailure::into_submission;
    let _: fn(
        ContextualSubmission,
        MetadataCommandId,
    ) -> Result<ContextualEpochReissue, ContextualReissuePreparationFailure> =
        ContextualSubmission::into_epoch_reissue;
    let _: fn(&ContextualEpochReissue) -> ContextSubmissionObservation =
        ContextualEpochReissue::source;
    let _: fn(&ContextualEpochReissue) -> ContextSubmissionObservation =
        ContextualEpochReissue::successor;
    let _: for<'pg> fn(
        ContextualEpochReissue,
        &'pg PgStore,
        &PgTopology,
    ) -> Result<PreparedEpochReissue<'pg>, EpochReissueInspectionFailure> =
        ContextualEpochReissue::prepare_pending;
    let _: for<'pg> fn(
        ContextualEpochReissue,
        &'pg PgStore,
        u32,
        &PgTopology,
    ) -> Result<PreparedEpochReissue<'pg>, EpochReissueInspectionFailure> =
        ContextualEpochReissue::prepare_abandoned_part_pending;
    let _: fn(
        PreparedEpochReissue<'static>,
    ) -> Result<ContextualEpochReissue, EpochReissueInspectionFailure> =
        PreparedEpochReissue::finish;
    let _: fn(EpochReissueInspectionFailure) -> ContextualEpochReissue =
        EpochReissueInspectionFailure::into_reissue;
    let _: fn(&PreparedEpochReissue<'static>) -> ContextSubmissionObservation =
        PreparedEpochReissue::source;
    let _: fn(&PreparedEpochReissue<'static>) -> ContextSubmissionObservation =
        PreparedEpochReissue::successor;
    let _: fn(
        &PreparedEpochReissue<'static>,
    ) -> super::super::command_context::PendingInventoryObservation =
        PreparedEpochReissue::source_inventory;
    let _: fn(
        &PreparedEpochReissue<'static>,
    ) -> super::super::command_context::PendingInventoryObservation =
        PreparedEpochReissue::successor_inventory;
    let _: for<'pg> fn(
        ContextualReissue,
        &'pg PgStore,
        &PgTopology,
    ) -> Result<
        PreparedContextualReissue<'pg>,
        ContextualReissueInspectionFailure,
    > = ContextualReissue::prepare_pending;
    let _: for<'pg> fn(
        ContextualReissue,
        &'pg PgStore,
        u32,
        &PgTopology,
    ) -> Result<
        PreparedContextualReissue<'pg>,
        ContextualReissueInspectionFailure,
    > = ContextualReissue::prepare_abandoned_part_pending;
    let _: fn(
        PreparedContextualReissue<'static>,
    ) -> Result<ContextualReissue, ContextualReissueInspectionFailure> =
        PreparedContextualReissue::finish;
    let _: fn(ContextualReissueInspectionFailure) -> ContextualReissue =
        ContextualReissueInspectionFailure::into_reissue;
    let _: fn(&PreparedContextualReissue<'static>) -> ContextSubmissionObservation =
        PreparedContextualReissue::source;
    let _: fn(&PreparedContextualReissue<'static>) -> ContextSubmissionObservation =
        PreparedContextualReissue::successor;
    let _: fn(
        &PreparedContextualReissue<'static>,
    ) -> super::super::command_context::PendingInventoryObservation =
        PreparedContextualReissue::source_inventory;
    let _: fn(
        &PreparedContextualReissue<'static>,
    ) -> super::super::command_context::PendingInventoryObservation =
        PreparedContextualReissue::successor_inventory;
    let _: fn(
        &ContextualCommand,
        &PgStore,
        &PgTopology,
    ) -> Result<
        super::super::command_context::PendingInventoryObservation,
        super::super::command_context::ContextError,
    > = ContextualCommand::observe_pending_inventory_in_open_transaction;
    let _: fn(
        &ContextualCommand,
        &PgStore,
        u32,
        &PgTopology,
    ) -> Result<
        super::super::command_context::PendingInventoryObservation,
        super::super::command_context::ContextError,
    > = ContextualCommand::observe_abandoned_part_inventory_in_open_transaction;
    let _: for<'pg> fn(
        ContextualSubmission,
        &'pg PgStore,
        &PgTopology,
    ) -> Result<
        PreparedContextualSubmission<'pg>,
        ContextualSubmissionPreparationFailure,
    > = ContextualSubmission::prepare_pending;
    let _: fn(
        PreparedContextualSubmission<'static>,
    ) -> Result<ContextualSubmission, ContextualSubmissionPreparationFailure> =
        PreparedContextualSubmission::finish;
    let _: fn(ContextualSubmissionPreparationFailure) -> ContextualSubmission =
        ContextualSubmissionPreparationFailure::into_submission;
    let _: fn(&PreparedContextualSubmission<'static>) -> ContextSubmissionObservation =
        PreparedContextualSubmission::observation;
    let _: fn(
        &PreparedContextualSubmission<'static>,
    ) -> super::super::command_context::PendingInventoryObservation =
        PreparedContextualSubmission::inventory;
    let _: fn(&ContextualSubmission) -> ContextSubmissionObservation =
        ContextualSubmission::observation;
    let _: fn(&ContextualReissue) -> ContextSubmissionObservation = ContextualReissue::source;
    let _: fn(&ContextualReissue) -> ContextSubmissionObservation = ContextualReissue::successor;
    let _: fn(ContextualReissue) -> Result<ContextualSubmission, Box<ContextualReissue>> =
        ContextualReissue::into_observed_successor;
    // Compiled by Nextest, not a doctest: adding Clone makes inference ambiguous.
    trait NotClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    let _ = <ContextualSubmission as NotClone<_>>::check;
    let _ = <ContextualPartAbandonment as NotClone<_>>::check;
    let _ = <ContextualPartAbandonmentReissue as NotClone<_>>::check;
    let _ = <PartAbandonmentReissueFailure as NotClone<_>>::check;
    let _ = <PreparedPartAbandonmentReissue<'static> as NotClone<_>>::check;
    let _ = <PartAbandonmentReissueInspectionFailure as NotClone<_>>::check;
    let _ = <PartAbandonmentPreparationFailure as NotClone<_>>::check;
    let _ = <ContextualReissue as NotClone<_>>::check;
    let _ = <ContextualEpochReissue as NotClone<_>>::check;
    let _ = <PreparedEpochReissue<'static> as NotClone<_>>::check;
    let _ = <EpochReissueInspectionFailure as NotClone<_>>::check;
    let _ = <ContextualReissuePreparationFailure as NotClone<_>>::check;
    let _ = <ContextualSubmissionPreparationFailure as NotClone<_>>::check;
    let _ = <PreparedContextualSubmission<'static> as NotClone<_>>::check;
    let _ = <ContextualReissueInspectionFailure as NotClone<_>>::check;
    let _ = <PreparedContextualReissue<'static> as NotClone<_>>::check;
}
