// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local candidate handoff; not a new command encoding or an installed
//! RPC/custody test. No physical payload is written by these metadata fixtures.

use super::super::candidate::{CandidateApplied, CandidateBindingError, DescribedStagingCommand};
use super::super::capture::StagingCapture;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;

pub(super) fn cases() -> [(StagingTarget, bool); 5] {
    [
        (StagingTarget::DirectPut, false),
        (StagingTarget::StreamPut, false),
        (multipart_target(), false),
        (StagingTarget::StreamPut, true),
        (multipart_target(), true),
    ]
}

pub(super) fn setup(
    target: StagingTarget,
    append: bool,
) -> (
    test_util::TempDir,
    PgStore,
    StagingOwnerRecord,
    MetadataCommandEnvelope,
) {
    let dir = test_util::tempdir();
    let template = if append {
        fixture(target.clone())
    } else {
        fresh_capture::template(target.clone())
    };
    let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
    let (owner, command) = if append {
        let owner = fresh_append_capture::initialize(&pg, target);
        fresh_append_capture::prepare(&pg, &owner);
        let command = append::command(&owner);
        (owner, command)
    } else {
        let mut owner = template;
        let command = fresh_capture::candidate(&pg, &mut owner);
        (owner, command)
    };
    (dir, pg, owner, command)
}

pub(super) fn capture(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: MetadataCommandEnvelope,
    append: bool,
) -> DescribedStagingCommand {
    let config = capture::config(owner);
    let topology = topology();
    let capture = StagingCapture {
        topology: &topology,
        placement: PayloadPlacementInput::from_server_config(&config),
        effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
    };
    let snapshot = pg.command_build_snapshot().unwrap();
    let writes = pg.conn.total_changes();
    let before = introduction::state(pg);
    let candidate = if append {
        capture
            .append_candidate(pg, config.node_id, command)
            .unwrap()
    } else {
        capture
            .introduction_candidate(
                pg,
                config.node_id,
                command,
                &proof(&owner.subject, owner.subject.creation_operation()),
            )
            .unwrap()
    };
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
    snapshot.finish().unwrap();
    assert!(pg.conn.is_autocommit());
    candidate
}

fn install(pg: &PgStore, candidate: &DescribedStagingCommand) {
    let topology = topology();
    let bound = candidate.bind(&topology).unwrap();
    pg.try_insert_bound_staging_pending_metadata_command_slot(
        0,
        &bound.pending_protection(),
        &topology,
        Some(candidate.command().bucket_name()),
        AdmittedRouteEffectFence::unbounded(candidate.command().id().cluster_epoch()),
        None,
    )
    .unwrap();
    assert_eq!(
        pg.pending_metadata_command_envelope(0, candidate.command().id().cluster_epoch())
            .unwrap()
            .as_ref(),
        Some(candidate.command())
    );
}

fn successor(candidate: &DescribedStagingCommand) -> MetadataCommandId {
    let id = candidate.command().id();
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

#[test]
fn staging_candidate_survives_snapshot_and_drives_exact_pending_and_application() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        let (_dir, pg, owner, command) = setup(target, is_append);
        let expected = command.clone();
        let candidate = capture(&pg, &owner, command, is_append);
        assert_eq!(candidate.command(), &expected);
        install(&pg, &candidate);
        let writes = pg.conn.total_changes();
        install(&pg, &candidate);
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "exact pending retry must not rewrite evidence"
        );
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let topology = topology();
        let before_index = pg
            .max_metadata_command_log_index(candidate.command().id().cluster_epoch())
            .unwrap();
        let result = candidate
            .bind(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg)
            .unwrap();
        assert_eq!(
            result,
            if is_append {
                CandidateApplied::Append(super::super::append::AppendApplied::Appended)
            } else {
                CandidateApplied::Introduction(
                    super::super::introduction::IntroductionApplied::Created,
                )
            }
        );
        let retained = append::load(&pg, &owner);
        assert_eq!(retained.subject, owner.subject);
        assert_eq!(retained.references.len(), usize::from(is_append));
        assert_eq!(retained.windows.len(), 1 + usize::from(is_append));
        // Application components must not falsely authenticate the separate
        // descriptor by recording the current v13 envelope in the applied log.
        assert_eq!(
            pg.max_metadata_command_log_index(candidate.command().id().cluster_epoch())
                .unwrap(),
            before_index
        );
        let writes = pg.conn.total_changes();
        let replay = candidate
            .bind(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg)
            .unwrap();
        assert_eq!(
            replay,
            if is_append {
                CandidateApplied::Append(super::super::append::AppendApplied::ExactExisting)
            } else {
                CandidateApplied::Introduction(
                    super::super::introduction::IntroductionApplied::ExactExisting,
                )
            }
        );
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_candidate_reissue_preserves_descriptor_after_expiry_and_restart() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        clock.set(2_000);
        let (dir, pg, owner, command) = setup(target, is_append);
        let candidate = capture(&pg, &owner, command, is_append);
        let bytes = candidate.descriptor_bytes();
        install(&pg, &candidate);
        drop(pg);
        clock.set(1_000_000);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        let topology = topology();
        let original = candidate.bind(&topology).unwrap();
        let replacement = candidate.reissued_at(successor(&candidate)).unwrap();
        assert_eq!(
            replacement.command().payload(),
            candidate.command().payload()
        );
        assert_eq!(replacement.descriptor_bytes(), bytes);
        assert_eq!(
            replacement
                .bind(&topology)
                .unwrap()
                .pending_protection()
                .reference(),
            original.pending_protection().reference()
        );
        assert!(pg
            .replace_bound_staging_pending_metadata_command_slot_for_reissue(
                0,
                &original.pending_protection(),
                replacement.command(),
                &topology,
                Some(candidate.command().bucket_name()),
            )
            .unwrap());
        install(&pg, &replacement);
        let historical = replacement
            .reissued_at(MetadataCommandId::new(
                ClusterEpoch::new(replacement.command().id().cluster_epoch().get() + 1).unwrap(),
                replacement.command().id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ))
            .unwrap();
        assert_eq!(historical.descriptor_bytes(), bytes);
        // Merely binding a later-epoch candidate does not install it, authorize
        // historical recovery, or renew original placement/cutoffs.
        assert_eq!(
            pg.pending_metadata_command_envelope(0, replacement.command().id().cluster_epoch())
                .unwrap()
                .as_ref(),
            Some(replacement.command())
        );
        assert_eq!(
            historical
                .bind(&topology)
                .unwrap()
                .pending_protection()
                .reference(),
            original.pending_protection().reference()
        );
    }
}

#[test]
fn staging_candidate_reissue_rejects_wrong_pg_or_nonadvancing_position() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        let (_dir, pg, owner, command) = setup(target, is_append);
        let candidate = capture(&pg, &owner, command, is_append);
        let id = candidate.command().id();
        let before = introduction::state(&pg);
        for crossed in [
            id,
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                id.pg_id(),
                MetadataCommandLogIndex::new(100).unwrap(),
            ),
            MetadataCommandId::new(
                id.cluster_epoch(),
                PgId::new((id.pg_id().get() + 1) % 8),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            ),
        ] {
            assert!(matches!(
                candidate.reissued_at(crossed),
                Err(CandidateBindingError::ReissuePosition)
            ));
        }
        let newer = candidate.reissued_at(successor(&candidate)).unwrap();
        assert!(matches!(
            newer.reissued_at(id),
            Err(CandidateBindingError::ReissuePosition)
        ));
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_candidate_capture_rejects_wrong_kind_and_requires_snapshot() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        let (_dir, pg, owner, command) = setup(target, is_append);
        let config = capture::config(&owner);
        let topology = topology();
        let capture = StagingCapture {
            topology: &topology,
            placement: PayloadPlacementInput::from_server_config(&config),
            effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        };
        let proof = proof(&owner.subject, owner.subject.creation_operation());
        let snapshot = pg.command_build_snapshot().unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let crossed = if is_append {
            capture.introduction_candidate(&pg, config.node_id, command.clone(), &proof)
        } else {
            capture.append_candidate(&pg, config.node_id, command.clone())
        };
        assert!(matches!(
            crossed,
            Err(super::super::capture::StagingCaptureError::Command)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        snapshot.finish().unwrap();
        let outside = if is_append {
            capture.append_candidate(&pg, config.node_id, command)
        } else {
            capture.introduction_candidate(&pg, config.node_id, command, &proof)
        };
        assert!(matches!(
            outside,
            Err(super::super::capture::StagingCaptureError::SnapshotRequired)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_candidate_requires_current_topology_even_when_object_pg_matches() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        let (_dir, pg, owner, command) = setup(target, is_append);
        let candidate = capture(&pg, &owner, command, is_append);
        let topology = topology();
        let bound = candidate.bind(&topology).unwrap();
        let protection = bound.pending_protection();
        let (_, _, subject, window) = protection.index_inputs();
        let foreign = (1_u32..256)
            .find_map(|mask| {
                let ids: Vec<_> = (0..8).filter(|pg| mask & (1 << pg) != 0).collect();
                let other = PgTopology::new(&ids).unwrap();
                (other.object_pg_for(&subject.bucket, &subject.key) == subject.object_pg.get()
                    && (subject.data_pg(&other, window.index) != window.participants.data_pg.get()
                        || other.bucket_pg_for(&subject.bucket)
                            != topology.bucket_pg_for(&subject.bucket)))
                .then_some(other)
            })
            .expect("same object PG with crossed physical or bucket route");
        assert_eq!(
            foreign.object_pg_for(&subject.bucket, &subject.key),
            subject.object_pg.get()
        );
        assert!(
            subject.data_pg(&foreign, window.index) != window.participants.data_pg.get()
                || foreign.bucket_pg_for(&subject.bucket)
                    != topology.bucket_pg_for(&subject.bucket)
        );
        let before = introduction::state(&pg);
        assert!(matches!(
            candidate.bind(&foreign),
            Err(CandidateBindingError::Topology)
        ));
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_candidate_binding_does_not_cache_live_application_authority() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in cases() {
        let (_dir, pg, owner, command) = setup(target, is_append);
        let candidate = capture(&pg, &owner, command, is_append);
        let topology = topology();
        let bound = candidate.bind(&topology).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        if is_append {
            pg.conn
                .execute(
                    "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                    [&owner.subject.id.attempt],
                )
                .unwrap();
        } else {
            pg.apply_metadata_command(&MetadataCommandEnvelope::new(
                candidate.command().id(),
                crate::metadata_command::MetadataCommandPayload::ReserveObjectGeneration(
                    crate::metadata_command::ReserveObjectGenerationCommand::new(
                        owner.subject.bucket.clone(),
                        owner.subject.key.clone(),
                        owner.subject.id.attempt.clone(),
                        owner.subject.payload_generation,
                        1_000,
                    ),
                ),
            ))
            .unwrap();
        }
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let result = bound.apply_in_open_transaction(&pg);
        assert!(
            if is_append {
                matches!(
                    result,
                    Err(CandidateBindingError::Append(
                        super::super::append::AppendError::LiveResponsibility
                    ))
                )
            } else {
                matches!(
                    result,
                    Err(CandidateBindingError::Introduction(
                        super::super::introduction::IntroductionError::IndependentlyOwned
                    ))
                )
            },
            "{result:?}"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
