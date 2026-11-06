// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local transaction components, not production dispatcher or physical
//! cleanup acceptance. Stream specimens use the real append mutation but no
//! shard I/O; no deletion capability is minted by closing their owner.

use super::super::close::{apply_in_open_transaction, CloseError};
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::metadata_command::{
    AbortStreamUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ReleaseObjectGenerationCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::{
    PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
    PgClusterMapHistoryRouteReferences, PgStore,
};
use crate::{MetadataError, StreamUploadState};

fn targets() -> [StagingTarget; 3] {
    [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ]
}

fn initialize(pg: &PgStore, target: StagingTarget, count: usize) -> StagingOwnerRecord {
    let mut owner = append::initialize(pg, target);
    if owner.subject.target != StagingTarget::DirectPut {
        for _ in 0..count {
            let command = append::command(&owner);
            let next = append::next(pg, &owner);
            super::super::descriptor::apply_append_for_test(pg, &topology(), &command, &next)
                .unwrap();
            owner = append::load(pg, &owner);
        }
    }
    owner
}

pub(super) fn command(pg: &PgStore, owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let payload = match subject.target {
        StagingTarget::DirectPut => {
            MetadataCommandPayload::ReleaseObjectGeneration(ReleaseObjectGenerationCommand::new(
                subject.bucket.clone(),
                subject.key.clone(),
                subject.id.attempt.clone(),
            ))
        }
        _ => MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            session_id: subject.id.attempt.clone(),
            staged_segments: pg.list_stream_segments(&subject.id.attempt).unwrap(),
            stream_create_bucket_write_reservation: pg
                .get_stream_upload(&subject.id.attempt)
                .unwrap()
                .bucket_write_reservation,
        })),
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(subject.originating_epoch.get() + 1).unwrap(),
            subject.object_pg,
            MetadataCommandLogIndex::new(20).unwrap(),
        ),
        payload,
    )
}

fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), CloseError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn expected_closed(owner: &StagingOwnerRecord) -> StagingOwnerRecord {
    // Independent expected state, not the production transition constructor.
    let mut expected = owner.clone();
    expected.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
    expected.references.clear();
    release_due(&mut expected.resources.generation);
    release_due(&mut expected.resources.creation);
    expected
}

fn expected_owner_routes(owner: &StagingOwnerRecord) -> PgClusterMapHistoryRouteReferences {
    let reference = |pg, epoch| {
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            epoch,
            pg,
        )
    };
    let mut routes = vec![reference(
        owner.subject.object_pg,
        owner.subject.originating_epoch,
    )];
    for window in &owner.windows {
        routes.push(reference(
            window.participants.data_pg,
            window.participants.placement_epoch,
        ));
    }
    for resource in [&owner.resources.creation, &owner.resources.finalization] {
        if let ResourceResponsibility::Live(resource)
        | ResourceResponsibility::ReleaseDue(resource) = resource
        {
            routes.push(reference(
                resource.release_pg,
                resource.identity.cluster_epoch,
            ));
        }
    }
    PgClusterMapHistoryRouteReferences::try_from_iter(routes).unwrap()
}

#[test]
fn staging_close_retains_unused_windows_and_exact_releases_after_reopen() {
    for target in targets() {
        for count in [0, 2] {
            let dir = test_util::tempdir();
            let pg_id = fixture(target.clone()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = initialize(&pg, target.clone(), count);
            let raw = command(&pg, &owner);
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    raw.id().cluster_epoch(),
                    raw.id().pg_id(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                ),
                raw.payload().clone(),
            );
            assert!(command.id().log_index() <= owner.subject.id.introduction.log_index());
            let expected = expected_closed(&owner);
            let routes = expected_owner_routes(&owner);
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            assert_ne!(
                pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                    .unwrap(),
                owner.subject.issuer
            );
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            with_time_and_monotonic_override(9_000_000, 99_000_000, || {
                apply(&pg, &command).unwrap()
            });
            assert_eq!(append::load(&pg, &owner), expected);
            assert!(!pg.conn.is_autocommit());
            assert!(matches!(
                pg.get_stream_upload(&owner.subject.id.attempt),
                Err(MetadataError::StreamSessionNotFound { .. })
            ));
            assert!(matches!(
                pg.get_object_generation_reservation(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    &owner.subject.id.attempt
                ),
                Err(MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
            assert!(pg
                .list_stream_segments(&owner.subject.id.attempt)
                .unwrap()
                .is_empty());
            assert_eq!(
                pg.cluster_map_history_route_references(&topology())
                    .unwrap(),
                routes
            );
            let high_water: u64 = pg.conn.query_row(
                "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
                rusqlite::params![owner.subject.bucket, owner.subject.key], |row| row.get(0),
            ).unwrap();
            assert!(high_water >= owner.subject.payload_generation.get());
            // Terminal state does not authorize another fresh closure or certify
            // an arbitrary replay. AlreadyApplied remains the log boundary's job.
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply(&pg, &command),
                Err(CloseError::CommandBinding)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(append::load(&pg, &owner), expected);
            assert_eq!(
                pg.cluster_map_history_route_references(&topology())
                    .unwrap(),
                routes
            );
        }
    }
}

#[test]
fn staging_close_crossed_command_subject_kind_and_order_do_not_mutate() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target, 2);
        let good = command(&pg, &owner);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for case in 0..4 {
            let mut payload = good.payload().clone();
            match &mut payload {
                MetadataCommandPayload::ReleaseObjectGeneration(release) => match case {
                    0 => release.bucket = crate::tests::bucket_name("crossed"),
                    1 => release.key = crate::tests::object_key("crossed"),
                    2 => release.reservation_id = crate::tests::stream_session_id("crossed"),
                    3 => {
                        payload = MetadataCommandPayload::AbortStreamUpload(Box::new(
                            AbortStreamUploadCommand {
                                bucket: release.bucket.clone(),
                                key: release.key.clone(),
                                session_id: release.reservation_id.clone(),
                                staged_segments: vec![],
                                stream_create_bucket_write_reservation: None,
                            },
                        ))
                    }
                    _ => unreachable!(),
                },
                MetadataCommandPayload::AbortStreamUpload(abort) => match case {
                    0 => abort.bucket = crate::tests::bucket_name("crossed"),
                    1 => abort.key = crate::tests::object_key("crossed"),
                    2 => abort.session_id = crate::tests::stream_session_id("crossed"),
                    3 => {
                        payload = MetadataCommandPayload::ReleaseObjectGeneration(
                            ReleaseObjectGenerationCommand::new(
                                abort.bucket.clone(),
                                abort.key.clone(),
                                abort.session_id.clone(),
                            ),
                        )
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
            assert!(
                apply(&pg, &MetadataCommandEnvelope::new(good.id(), payload)).is_err(),
                "case {case}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        for id in [
            owner.subject.id.introduction,
            MetadataCommandId::new(
                ClusterEpoch::new(4).unwrap(),
                good.id().pg_id(),
                good.id().log_index(),
            ),
            MetadataCommandId::new(
                good.id().cluster_epoch(),
                PgId::new((good.id().pg_id().get() + 1) % 8),
                good.id().log_index(),
            ),
        ] {
            assert!(matches!(
                apply(
                    &pg,
                    &MetadataCommandEnvelope::new(id, good.payload().clone())
                ),
                Err(CloseError::CommandBinding)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
        }
        assert_eq!(introduction::state(&pg), before);
        apply(&pg, &good).unwrap();
    }
}

#[test]
fn staging_close_requires_exact_live_generation_and_high_water() {
    for target in targets() {
        for mutation in [
            "DELETE FROM object_generation_reservations",
            "UPDATE object_generation_reservations SET generation_id = generation_id + 1",
            "DELETE FROM object_write_counters",
            "UPDATE object_write_counters SET max_committed_generation = 1",
            "DELETE FROM payload_staging_owners",
        ] {
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone(), 2);
            let command = command(&pg, &owner);
            pg.conn.execute_batch(mutation).unwrap();
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &command).is_err(), "accepted {mutation}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_close_stream_rejects_co_crossed_prefix_session_and_proof() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for mutation in [
            "UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1",
            "UPDATE stream_uploads SET allocation_issuer_lifetime = zeroblob(16)",
            "UPDATE stream_uploads SET next_segment_vid = 99",
            "UPDATE stream_upload_segments SET data_pg_id = (data_pg_id + 1) % 8",
            "UPDATE stream_upload_segments SET size = size + 1",
            "DELETE FROM stream_upload_segments",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone(), 2);
            pg.conn.execute_batch(mutation).unwrap();
            // Command follows the changed rows, so ordinary command/session
            // equality cannot mask a missing owner/window correspondence check.
            let command = command(&pg, &owner);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &command).is_err(), "accepted {mutation}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = initialize(&pg, StagingTarget::StreamPut, 1);
    pg.conn.execute_batch("UPDATE stream_uploads SET bucket_write_owner_token = bucket_write_owner_token || '-crossed'").unwrap();
    let command = command(&pg, &owner);
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        apply(&pg, &command),
        Err(CloseError::LiveResponsibility)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_close_failure_rolls_back_live_deletion_and_owner_transition() {
    for target in targets() {
        for injection in ["live", "owner", "references"] {
            if injection == "references" && target == StagingTarget::DirectPut {
                continue;
            }
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone(), 2);
            let command = command(&pg, &owner);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            let event = match injection {
                "live" => "DELETE ON object_generation_reservations",
                "owner" => "UPDATE ON payload_staging_owners",
                "references" => "DELETE ON payload_staging_references",
                _ => unreachable!(),
            };
            pg.conn.execute_batch(&format!(
                "CREATE TEMP TRIGGER fail_closure BEFORE {event} BEGIN SELECT RAISE(ABORT, 'closure failure'); END"
            )).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &command).unwrap_err();
            assert!(error.to_string().contains("closure failure"), "{error}");
            assert_eq!(introduction::state(&pg), before, "{injection}");
            assert!(!pg.conn.is_autocommit());
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM outer_work", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                42
            );
            pg.conn.execute_batch("DROP TRIGGER fail_closure").unwrap();
            apply(&pg, &command).unwrap();
            assert_eq!(append::load(&pg, &owner), expected_closed(&owner));
        }
    }
}

#[test]
fn staging_close_cannot_demote_published_ownership_or_run_without_transaction() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let mut owner = initialize(&pg, target, 2);
        let command = command(&pg, &owner);
        // Keep live responsibility present: the lifecycle check, not a missing
        // session/reservation, must prohibit a published-to-unpublished change.
        publish(&mut owner);
        pg.conn
            .execute(
                "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                [&owner.subject.id.attempt],
            )
            .unwrap();
        sql::insert(
            &pg.conn,
            owner.subject.object_pg,
            &topology(),
            &owner.validate(&topology()).unwrap(),
        )
        .unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            apply(&pg, &command),
            Err(CloseError::CommandBinding)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("COMMIT").unwrap();
        assert!(matches!(
            apply(&pg, &command),
            Err(CloseError::TransactionRequired)
        ));
    }
}

#[test]
fn staging_close_upload_part_does_not_require_a_surviving_parent_upload() {
    let dir = test_util::tempdir();
    let target = multipart_target();
    let pg = PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
    let owner = initialize(&pg, target, 2);
    let command = command(&pg, &owner);
    pg.conn
        .execute_batch("DELETE FROM multipart_uploads")
        .unwrap();
    assert_eq!(
        pg.get_stream_upload(&owner.subject.id.attempt)
            .unwrap()
            .state,
        StreamUploadState::InProgress
    );
    apply(&pg, &command).unwrap();
    assert_eq!(append::load(&pg, &owner), expected_closed(&owner));
}

#[test]
fn staging_close_preserves_an_independent_same_object_attempt() {
    use super::super::capture::{StagingCapture, StagingIntroduction};
    use crate::node_runtime::payload_placement::PayloadPlacementInput;
    use crate::types::AdmittedRouteEffectFence;

    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let first = initialize(&pg, target, 2);
        let config = capture::config(&first);
        let mut creation = proof(&first.subject, first.subject.creation_operation());
        creation.reservation_id.push_str("-second");
        creation.owner_token.push_str("-second");
        let mut second = with_time_and_monotonic_override(1_000, 70_000, || {
            StagingCapture {
                topology: &topology(),
                placement: PayloadPlacementInput::from_server_config(&config),
                effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            }
            .introduction(
                &pg,
                config.node_id,
                StagingIntroduction {
                    id: StagingOwnerId {
                        attempt: crate::tests::stream_session_id("second-owner"),
                        introduction: MetadataCommandId::new(
                            first.subject.originating_epoch,
                            first.subject.object_pg,
                            MetadataCommandLogIndex::new(10).unwrap(),
                        ),
                    },
                    bucket: first.subject.bucket.clone(),
                    key: first.subject.key.clone(),
                    payload_generation: GenerationId::new(
                        first.subject.payload_generation.get() + 1,
                    )
                    .unwrap(),
                    target: first.subject.target.clone(),
                },
                &creation,
            )
            .unwrap()
        });
        let create = introduction::command(&second);
        let mut payload = create.payload().clone();
        if let MetadataCommandPayload::CreateStreamUpload(command) = &mut payload {
            command.bucket_write_reservation = creation;
        }
        super::super::descriptor::apply_introduction_for_test(
            &pg,
            &topology(),
            &MetadataCommandEnvelope::new(create.id(), payload),
            &second,
        )
        .unwrap();
        if second.subject.target != StagingTarget::DirectPut {
            let append = append::command(&second);
            let append = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    append.id().cluster_epoch(),
                    append.id().pg_id(),
                    MetadataCommandLogIndex::new(11).unwrap(),
                ),
                append.payload().clone(),
            );
            super::super::descriptor::apply_append_for_test(
                &pg,
                &topology(),
                &append,
                &append::next(&pg, &second),
            )
            .unwrap();
            second = append::load(&pg, &second);
        }
        let second_session = pg.get_stream_upload(&second.subject.id.attempt).ok();
        let second_segments = pg.list_stream_segments(&second.subject.id.attempt).unwrap();
        let command = command(&pg, &first);
        apply(&pg, &command).unwrap();
        assert_eq!(append::load(&pg, &first), expected_closed(&first));
        assert_eq!(append::load(&pg, &second), second);
        assert_eq!(
            pg.get_stream_upload(&second.subject.id.attempt).ok(),
            second_session
        );
        assert_eq!(
            pg.list_stream_segments(&second.subject.id.attempt).unwrap(),
            second_segments
        );
        assert_eq!(
            pg.get_object_generation_reservation(
                &second.subject.bucket,
                &second.subject.key,
                &second.subject.id.attempt
            )
            .unwrap(),
            second.subject.payload_generation
        );
    }
}
