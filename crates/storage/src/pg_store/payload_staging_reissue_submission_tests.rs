// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only owned reissue handoff, using the real pending replacement
//! engine. Not recovery-admission, wire-carrier or frontend custody activation.

use super::super::candidate::{
    CandidateBindingError, DescribedStagingCommand, StagingCommandInspection,
};
use super::super::submission::reissue::{ReissuePreparationFailure, StagingReissue};
use super::super::submission::{
    InstallationObservation as Observed, StagingSubmission, SubmissionError,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{PendingMetadataCommandSlotReplaceError, PgStore};
use std::sync::atomic::Ordering;

fn next_id(source: &MetadataCommandEnvelope) -> MetadataCommandId {
    MetadataCommandId::new(
        source.id().cluster_epoch(),
        source.id().pg_id(),
        MetadataCommandLogIndex::new(source.id().log_index().get() + 1).unwrap(),
    )
}

fn reissue(source: StagingSubmission) -> StagingReissue {
    let id = next_id(source.description().command());
    source.into_same_epoch_reissue(id).unwrap()
}

fn require_unresolved(pair: StagingReissue) -> StagingReissue {
    match pair.into_observed_successor() {
        Ok(_) => panic!("unconfirmed successor discarded its possible source"),
        Err(pair) => *pair,
    }
}

fn require_successor(pair: StagingReissue) -> StagingSubmission {
    match pair.into_observed_successor() {
        Ok(successor) => successor,
        Err(_) => panic!("confirmed replacement was not retained"),
    }
}

#[test]
fn staging_reissue_submission_inspection_cannot_detach_complete_descriptions() {
    // Type-check every holder escape as the narrow view, not a borrowed
    // DescribedStagingCommand. The view contains no production descriptor and
    // has no reissue, binding or submission constructor operations.
    fn submission_view(source: &StagingSubmission) -> StagingCommandInspection<'_> {
        source.description()
    }
    fn source_view(pair: &StagingReissue) -> StagingCommandInspection<'_> {
        pair.source().description()
    }
    fn successor_view(pair: &StagingReissue) -> StagingCommandInspection<'_> {
        pair.successor()
    }

    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected_source = command.clone();
        let mut source = candidate::capture(&pg, &owner, command, is_append).into_submission();
        assert_eq!(submission_view(&source).command(), &expected_source);
        submission::install(&mut source, &pg).unwrap();
        let mut pair = reissue(source);
        let expected_successor = successor_view(&pair).command().clone();
        pg.fail_next_pending_slot_replace_after_commit
            .store(true, Ordering::Relaxed);
        assert!(matches!(
            pair.replace(&pg, 0, &topology()),
            Err(SubmissionError::Unconfirmed(_))
        ));
        let pending = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        // Both commands remain inspectable after the lost reply, but inspecting
        // them cannot select a successor or clear the holder's prior history.
        assert_eq!(source_view(&pair).command(), &expected_source);
        assert_eq!(successor_view(&pair).command(), &expected_successor);
        let mut pair = require_unresolved(pair);
        assert_eq!(pair.observation(), Observed::Unconfirmed);
        assert_eq!(pair.source().observation(), Observed::Installed);
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let successor = require_successor(pair);
        assert_eq!(successor.observation(), Observed::Installed);
        assert_eq!(submission_view(&successor).command(), &expected_successor);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_reissue_submission_keeps_original_descriptor_through_successive_replacements() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let descriptor = captured.descriptor_bytes();
        let mut source = captured.into_submission();
        submission::install(&mut source, &pg).unwrap();
        let original_window = pg
            .pending_staging_protection(&topology())
            .unwrap()
            .unwrap()
            .window;
        let before = introduction::state(&pg);
        let replica_state = pg.metadata_command_replica_state().unwrap();
        let mut pair = require_unresolved(reissue(source));
        clock.set(900_000); // replacement does not renew original window authority
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let expected = pair.successor().command().clone();
        let pending = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pair.observation(), Observed::Installed);
        assert_eq!(pair.source().observation(), Observed::Installed);
        assert_eq!(pair.source().description().descriptor_bytes(), descriptor);
        assert_eq!(pair.successor().descriptor_bytes(), descriptor);
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let successor = require_successor(pair);
        assert_eq!(successor.description().command(), &expected);
        assert_eq!(successor.observation(), Observed::Installed);
        let mut pair = reissue(successor);
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        assert_eq!(pair.successor().descriptor_bytes(), descriptor);
        let indexed = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        assert_eq!(indexed.command, pair.successor().command().id());
        assert_eq!(indexed.window, original_window);
        let pending = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        drop(pair);
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes, "hidden Drop cleanup");
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), replica_state);
    }
}

#[test]
fn staging_reissue_submission_lost_reply_retains_both_candidates_across_rejected_retries() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected_source = command.clone();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let descriptor = captured.descriptor_bytes();
        let mut source = captured.into_submission();
        submission::install(&mut source, &pg).unwrap();
        let mut pair = reissue(source);
        let expected_successor = pair.successor().command().clone();
        pg.fail_next_pending_slot_replace_after_commit
            .store(true, Ordering::Relaxed);
        let error = pair.replace(&pg, 0, &topology()).unwrap_err();
        assert!(
            matches!(error, SubmissionError::Unconfirmed(_)),
            "{error:?}"
        );
        let mut pair = require_unresolved(pair);
        assert_eq!(pair.observation(), Observed::Unconfirmed);
        assert_eq!(
            pg.pending_metadata_command_envelope(0, expected_successor.id().cluster_epoch())
                .unwrap()
                .as_ref(),
            Some(&expected_successor)
        );
        let pending = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        pg.fail_next_pending_slot_inspection
            .store(true, Ordering::Relaxed);
        let error = pair.replace(&pg, 0, &topology()).unwrap_err();
        assert!(matches!(error, SubmissionError::Rejected(_)), "{error:?}");
        assert_eq!(pair.observation(), Observed::Unconfirmed);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            pair.replace(&pg, 0, &topology()),
            Err(SubmissionError::OpenTransaction)
        ));
        assert!(!pg.conn.is_autocommit());
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let foreign = PgTopology::new(&[0, 1, 2]).unwrap();
        assert!(matches!(
            pair.replace(&pg, 0, &foreign),
            Err(SubmissionError::Binding(CandidateBindingError::Topology))
        ));
        let mut pair = require_unresolved(pair);
        assert_eq!(pair.source().description().command(), &expected_source);
        assert_eq!(pair.successor().command(), &expected_successor);
        assert_eq!(pair.source().description().descriptor_bytes(), descriptor);
        assert_eq!(pair.successor().descriptor_bytes(), descriptor);
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "exact retry rewrote indexes"
        );
        let successor = require_successor(pair);
        assert_eq!(successor.description().command(), &expected_successor);
    }
}

#[test]
fn staging_reissue_submission_false_after_uncertainty_cannot_discard_either_command() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for slot_state in 0..3 {
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let mut source = candidate::capture(&pg, &owner, command, is_append).into_submission();
            submission::install(&mut source, &pg).unwrap();
            let mut pair = reissue(source);
            let source_command = pair.source().description().command().clone();
            pg.fail_next_pending_slot_replace_before_commit
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            // The injected inspection failure must not resolve the first result.
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Rejected(_))
            ));
            assert_eq!(
                pg.pending_metadata_command_envelope(0, source_command.id().cluster_epoch())
                    .unwrap()
                    .as_ref(),
                Some(&source_command)
            );
            match slot_state {
                0 | 1 => {
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_slot")
                        .unwrap();
                    if slot_state == 1 {
                        let contender = MetadataCommandEnvelope::new(
                            next_id(pair.successor().command()),
                            MetadataCommandPayload::ReserveObjectVersion(
                                ReserveObjectVersionCommand {
                                    bucket: owner.subject.bucket.clone(),
                                    key: owner.subject.key.clone(),
                                    version_id: crate::VersionId::from_u64(1),
                                },
                            ),
                        );
                        pg.try_insert_pending_metadata_command_slot(
                            0,
                            &contender,
                            Some(contender.bucket_name()),
                        )
                        .unwrap();
                    }
                }
                2 => {
                    pg.conn
                        .execute_batch(
                            "UPDATE metadata_command_pending_slot SET publication_started = 1",
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let pending = pending_install::observe(&pg);
            let writes = pg.conn.total_changes();
            assert!(!pair.replace(&pg, 0, &topology()).unwrap());
            assert_eq!(pair.observation(), Observed::Unconfirmed);
            let pair = require_unresolved(pair);
            assert_eq!(pair.source().description().command(), &source_command);
            drop(pair);
            assert_eq!(pending_install::observe(&pg), pending);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_reissue_submission_damaged_successor_indexes_remain_unconfirmed() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for table in [
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let mut source = candidate::capture(&pg, &owner, command, is_append).into_submission();
            submission::install(&mut source, &pg).unwrap();
            let mut pair = reissue(source);
            pg.fail_next_pending_slot_replace_after_commit
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            pg.conn
                .execute_batch(&format!("DELETE FROM {table}"))
                .unwrap();
            let pending = pending_install::observe(&pg);
            let writes = pg.conn.total_changes();
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            assert_eq!(pair.observation(), Observed::Unconfirmed);
            drop(require_unresolved(pair));
            assert_eq!(
                pg.conn.total_changes(),
                writes,
                "repaired or discarded damaged evidence"
            );
            assert_eq!(pending_install::observe(&pg), pending);
        }
    }
}

#[test]
fn staging_reissue_submission_rejects_invalid_positions_without_consuming_source_history() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected = command.clone();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let descriptor = captured.descriptor_bytes();
        let mut source = captured.into_submission();
        submission::install(&mut source, &pg).unwrap();
        pg.conn
            .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
            .unwrap();
        assert!(matches!(
            submission::install(&mut source, &pg),
            Err(SubmissionError::Unconfirmed(_))
        ));
        let id = expected.id();
        let pending = pending_install::observe(&pg);
        let writes = pg.conn.total_changes();
        for wrong in [
            id,
            MetadataCommandId::new(
                id.cluster_epoch(),
                PgId::new((id.pg_id().get() + 1) % 8),
                next_id(&expected).log_index(),
            ),
            MetadataCommandId::new(
                ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
                id.pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
        ] {
            let failure: ReissuePreparationFailure = match source.into_same_epoch_reissue(wrong) {
                Ok(_) => panic!("accepted invalid same-epoch reissue"),
                Err(failure) => failure,
            };
            assert!(matches!(
                failure.error(),
                CandidateBindingError::ReissuePosition
            ));
            source = failure.into_submission();
            assert_eq!(source.observation(), Observed::Unconfirmed);
            assert_eq!(source.description().command(), &expected);
            assert_eq!(source.description().descriptor_bytes(), descriptor);
        }
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_reissue_submission_rollback_retirement_preserves_pair_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for failures in [1, 2] {
            let (dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let captured = candidate::capture(&pg, &owner, command, is_append);
            // Independent owner-local setup for the lower-engine retirement
            // canary; do not extract a bindable candidate from an owned holder.
            let fixture_source = DescribedStagingCommand::decode_descriptor_field(
                captured.command().clone(),
                &captured.descriptor_bytes(),
                &topology(),
            )
            .unwrap();
            let mut source = captured.into_submission();
            submission::install(&mut source, &pg).unwrap();
            let original = pending_install::observe(&pg);
            let before = introduction::state(&pg);
            let mut pair = reissue(source);
            pg.conn.execute_batch("CREATE TEMP TRIGGER fail_reissue BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'reissue sidecar failure'); END;").unwrap();
            pg.test_fail_metadata_command_rollbacks(failures);
            let writes = pg.conn.total_changes();
            let error = pair.replace(&pg, 0, &topology()).unwrap_err();
            assert!(
                matches!(error, SubmissionError::Unconfirmed(_)),
                "{error:?}"
            );
            assert!(error
                .to_string()
                .contains("injected metadata-command rollback failure"));
            assert!(
                pg.conn.total_changes() > writes,
                "failure did not follow replacement writes"
            );
            let mut pair = require_unresolved(pair);
            {
                assert!(!pg.conn.is_autocommit());
                assert_eq!(
                    pg.fail_metadata_command_rollbacks.get(),
                    failures - 1,
                    "teardown secretly retried rollback"
                );
                assert!(matches!(
                    pair.replace(&pg, 0, &topology()),
                    Err(SubmissionError::Unconfirmed(_))
                ));
                let topology = topology();
                let bound = fixture_source.bind(&topology).unwrap();
                let result = pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
                    0,
                    &bound.pending_protection(),
                    pair.successor().command(),
                    &topology,
                    Some(pair.successor().command().bucket_name()),
                );
                assert!(
                    matches!(
                        result,
                        Err(PendingMetadataCommandSlotReplaceError::MayHaveApplied(
                            crate::StoreError::Db {
                                context: "access retired PG metadata connection",
                                ..
                            }
                        ))
                    ),
                    "{result:?}"
                );
            }
            drop(pg);
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            assert_eq!(pending_install::observe(&pg), original);
            assert_eq!(introduction::state(&pg), before);
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            let successor = require_successor(pair);
            assert_eq!(
                pg.pending_metadata_command_envelope(
                    0,
                    successor.description().command().id().cluster_epoch()
                )
                .unwrap()
                .as_ref(),
                Some(successor.description().command())
            );
        }
    }
}
