// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual reserve/create mutations plus retained-owner insertion in one open
//! transaction. The descriptor is still supplied separately: these are NOT wire,
//! dispatcher, frontend-custody or terminal-transfer acceptance tests.

use super::super::descriptor::apply_introduction_for_test;
use super::super::introduction::{IntroductionApplied, IntroductionError};
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::metadata_command::{
    CreateStreamUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectGenerationCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{AdmittedRouteEffectFence, CreateMultipartUploadReq, CreateStreamUploadReq};
use crate::{
    AclGrants, MetadataError, ObjectLockState, OwnerIdentity, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, StreamUploadTarget,
};

fn targets() -> [StagingTarget; 3] {
    [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ]
}

#[test]
fn staging_epoch_reset_introduction_reissue_preserves_original_revision() {
    for target in targets() {
        let dir = test_util::tempdir();
        let mut template = fixture(target);
        let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
        let owner = with_time_and_monotonic_override(1_000, 70_000, || {
            seed_upload(&pg, &mut template);
            let config = capture::config(&template);
            capture::introduce(
                &pg,
                &config,
                &template,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap()
        });
        assert_eq!(owner.subject.id.introduction.log_index().get(), 11);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let original = command(&owner);
        let reissued = epoch_order::at_epoch_start(&pg, &original, ClusterEpoch::new(6).unwrap());
        assert_eq!(
            apply(&pg, &owner, &reissued).unwrap(),
            IntroductionApplied::Created
        );
        assert_eq!(loaded(&pg, &owner), Some(owner.clone()));
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &reissued);
        let changes = pg.conn.total_changes();
        assert_eq!(
            apply(&pg, &owner, &reissued).unwrap(),
            IntroductionApplied::ExactExisting
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(loaded(&pg, &owner), Some(owner));
    }
}

pub(super) fn seed_upload(pg: &PgStore, owner: &mut StagingOwnerRecord) {
    if let StagingTarget::UploadPart {
        upload_id,
        upload_generation,
        ..
    } = &mut owner.subject.target
    {
        pg.create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: owner.subject.bucket.clone(),
            key: owner.subject.key.clone(),
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("owner"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: crate::tests::metadata_only_test_encryption(),
        })
        .unwrap();
        *upload_generation = pg
            .get_multipart_upload(upload_id)
            .unwrap()
            .object_generation_id;
    }
}

pub(super) fn command(owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let payload = match &subject.target {
        StagingTarget::DirectPut => MetadataCommandPayload::ReserveObjectGeneration(
            ReserveObjectGenerationCommand::new(
                subject.bucket.clone(), subject.key.clone(), subject.id.attempt.clone(),
                subject.payload_generation, 1_000,
            ),
        ),
        target => MetadataCommandPayload::CreateStreamUpload(Box::new(
            CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline_and_issuer(
                CreateStreamUploadReq {
                    session_id: subject.id.attempt.clone(),
                    bucket: subject.bucket.clone(),
                    key: subject.key.clone(),
                    target: match target {
                        StagingTarget::StreamPut => StreamUploadTarget::PutObject,
                        StagingTarget::UploadPart { upload_id, part_number, .. } => StreamUploadTarget::UploadPart {
                            upload_id: upload_id.clone(), part_number: *part_number,
                        },
                        StagingTarget::DirectPut => unreachable!(),
                    },
                    encryption: crate::tests::metadata_only_test_encryption(),
                },
                subject.payload_generation, 1_000, Some(50_000),
                proof(subject, subject.creation_operation()), subject.issuer.clone(),
            ),
        )),
    };
    MetadataCommandEnvelope::new(subject.id.introduction, payload)
}

pub(super) fn setup(
    pg: &PgStore,
    target: StagingTarget,
) -> (StagingOwnerRecord, MetadataCommandEnvelope) {
    let mut original = fixture(target);
    original.subject.id.introduction = MetadataCommandId::new(
        original.subject.originating_epoch,
        original.subject.object_pg,
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    with_time_and_monotonic_override(1_000, 70_000, || seed_upload(pg, &mut original));
    let config = capture::config(&original);
    let owner = with_time_and_monotonic_override(1_000, 70_000, || {
        capture::introduce(
            pg,
            &config,
            &original,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        )
        .unwrap()
    });
    let command = command(&owner);
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(decoded, command);
    (owner, decoded)
}

fn apply(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> Result<IntroductionApplied, IntroductionError> {
    apply_introduction_for_test(pg, &topology(), command, owner)
}

fn loaded(pg: &PgStore, owner: &StagingOwnerRecord) -> Option<StagingOwnerRecord> {
    sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt).unwrap()
}

pub(super) fn state(
    pg: &PgStore,
) -> Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock> {
    pg.metadata_checkpoint_table_blocks().unwrap()
}

#[test]
fn staging_introduction_witness_first_apply_preserves_primary_descriptor_and_reissue() {
    for target in targets() {
        let primary_dir = test_util::tempdir();
        let witness_dir = test_util::tempdir();
        let pg_id = fixture(target.clone()).subject.object_pg.get();
        let primary = PgStore::open(primary_dir.path(), pg_id).unwrap();
        let witness = PgStore::open(witness_dir.path(), pg_id).unwrap();
        let (owner, command) = setup(&primary, target);
        let mut witness_subject = owner.clone();
        with_time_and_monotonic_override(1_000, 70_000, || {
            seed_upload(&witness, &mut witness_subject);
        });
        assert_eq!(witness_subject, owner);
        assert_ne!(
            witness.stream_allocation_issuer(NodeId::new(2)).unwrap(),
            owner.subject.issuer
        );

        // Both replicas apply after expiry, with unrelated monotonic origins.
        // Application must preserve the primary's descriptor, not remint it.
        for (pg, node) in [(&witness, 2), (&primary, 4)] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            with_time_and_monotonic_override(9_000_000, u64::from(node) * 99_000_000, || {
                assert_eq!(
                    apply(pg, &owner, &command).unwrap(),
                    IntroductionApplied::Created
                );
            });
            assert!(
                !pg.conn.is_autocommit(),
                "apply committed its caller's transaction"
            );
            assert_eq!(loaded(pg, &owner), Some(owner.clone()));
            assert_eq!(
                pg.get_object_generation_reservation(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    &owner.subject.id.attempt,
                )
                .unwrap(),
                owner.subject.payload_generation
            );
            pg.record_metadata_command_applied_inner(node, &command)
                .unwrap();
            pg.conn.execute_batch("COMMIT; BEGIN IMMEDIATE").unwrap();

            let changes = pg.conn.total_changes();
            assert_eq!(
                apply(pg, &owner, &command).unwrap(),
                IntroductionApplied::ExactExisting
            );
            let reissued = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::new(6).unwrap(),
                    owner.subject.object_pg,
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                command.payload().clone(),
            );
            assert_eq!(
                apply(pg, &owner, &reissued).unwrap(),
                IntroductionApplied::ExactExisting
            );
            assert_eq!(loaded(pg, &owner), Some(owner.clone()));
            assert_eq!(
                pg.conn.total_changes(),
                changes,
                "exact replay mutated responsibility"
            );
            pg.conn.execute_batch("COMMIT").unwrap();
        }
        assert_eq!(
            primary.metadata_state_digest().unwrap(),
            witness.metadata_state_digest().unwrap()
        );
        drop(primary);
        let reopened = PgStore::open(primary_dir.path(), pg_id).unwrap();
        reopened.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_ne!(
            reopened.stream_allocation_issuer(NodeId::new(4)).unwrap(),
            owner.subject.issuer
        );
        assert_eq!(
            apply(&reopened, &owner, &command).unwrap(),
            IntroductionApplied::ExactExisting
        );
        assert_eq!(loaded(&reopened, &owner), Some(owner));
    }
}

fn crossed_command(command: &MetadataCommandEnvelope, case: u32) -> MetadataCommandEnvelope {
    let mut payload = command.payload().clone();
    match &mut payload {
        MetadataCommandPayload::ReserveObjectGeneration(reserve) => match case {
            0 => reserve.bucket = crate::tests::bucket_name("crossed-bucket"),
            1 => reserve.key = crate::tests::object_key("crossed-key"),
            2 => reserve.reservation_id = crate::tests::stream_session_id("crossed-attempt"),
            3 => {
                reserve.generation_id = GenerationId::new(reserve.generation_id.get() + 1).unwrap()
            }
            _ => unreachable!(),
        },
        MetadataCommandPayload::CreateStreamUpload(create) => match case {
            0 => create.session.bucket = crate::tests::bucket_name("crossed-bucket"),
            1 => create.session.key = crate::tests::object_key("crossed-key"),
            2 => create.session.session_id = crate::tests::stream_session_id("crossed-attempt"),
            3 => {
                create.payload_generation =
                    GenerationId::new(create.payload_generation.get() + 1).unwrap()
            }
            4 => create.session.bucket_incarnation_generation += 1,
            5 => create.initial_next_segment_vid = GenerationId::new(2).unwrap(),
            6 => create.allocation_issuer.lifetime[0] ^= 1,
            7 => create.allocation_issuer.node_id = NodeId::new(9),
            8 => create
                .bucket_write_reservation
                .reservation_id
                .push_str("-crossed"),
            9 => create.bucket_write_reservation.lease_deadline += 1,
            10 => create.session.state = crate::StreamUploadState::Completing,
            11 => {
                create.session.target = match &create.session.target {
                    StreamUploadTarget::PutObject => StreamUploadTarget::UploadPart {
                        upload_id: crate::tests::multipart_upload_id("crossed-upload"),
                        part_number: 1,
                    },
                    StreamUploadTarget::UploadPart { .. } => StreamUploadTarget::PutObject,
                }
            }
            12 => match &mut create.session.target {
                StreamUploadTarget::UploadPart { part_number, .. } => *part_number += 1,
                _ => unreachable!(),
            },
            13 => match &mut create.session.target {
                StreamUploadTarget::UploadPart { upload_id, .. } => {
                    *upload_id = crate::tests::multipart_upload_id("crossed-upload")
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
    MetadataCommandEnvelope::new(command.id(), payload)
}

#[test]
fn staging_introduction_rejects_independently_crossed_commands_before_any_mutation() {
    for target in targets() {
        let tmp = test_util::tempdir();
        let pg =
            PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let cases = match target {
            StagingTarget::DirectPut => 4,
            StagingTarget::StreamPut => 12,
            _ => 14,
        };
        let (owner, command) = setup(&pg, target);
        assert!(matches!(
            apply(&pg, &owner, &command),
            Err(IntroductionError::TransactionRequired)
        ));
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = state(&pg);
        for case in 0..cases {
            let crossed = crossed_command(&command, case);
            assert!(crossed.verify_checksum());
            let changes = pg.conn.total_changes();
            let error = apply(&pg, &owner, &crossed).unwrap_err();
            assert!(
                matches!(error, IntroductionError::CommandBinding),
                "crossed case {case}: {error:?}"
            );
            assert_eq!(
                pg.conn.total_changes(),
                changes,
                "crossed case {case} wrote before rejection"
            );
            assert_eq!(state(&pg), before);
        }
        assert_eq!(
            apply(&pg, &owner, &command).unwrap(),
            IntroductionApplied::Created
        );
    }
}

#[test]
fn staging_introduction_rejects_independent_live_rows_and_missing_replay_responsibility() {
    for target in targets() {
        for missing in [
            "owner",
            "owner_and_session",
            "owner_and_generation",
            "generation",
            "session",
        ] {
            if target == StagingTarget::DirectPut
                && matches!(
                    missing,
                    "session" | "owner_and_session" | "owner_and_generation"
                )
            {
                continue;
            }
            let tmp = test_util::tempdir();
            let pg =
                PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let (owner, command) = setup(&pg, target.clone());
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            if missing.starts_with("owner") {
                pg.apply_metadata_command(&command).unwrap();
                match missing {
                    "owner_and_session" => {
                        pg.conn
                            .execute(
                                "DELETE FROM stream_uploads WHERE session_id = ?1",
                                [&owner.subject.id.attempt],
                            )
                            .unwrap();
                    }
                    "owner_and_generation" => {
                        pg.conn.execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1", [&owner.subject.id.attempt],
                    ).unwrap();
                    }
                    "owner" => {}
                    _ => unreachable!(),
                }
            } else {
                apply(&pg, &owner, &command).unwrap();
                let sql = match missing {
                    "generation" => {
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1"
                    }
                    "session" => "DELETE FROM stream_uploads WHERE session_id = ?1",
                    _ => unreachable!(),
                };
                pg.conn.execute(sql, [&owner.subject.id.attempt]).unwrap();
            }
            let before = state(&pg);
            let changes = pg.conn.total_changes();
            let error = apply(&pg, &owner, &command).unwrap_err();
            if missing.starts_with("owner") {
                assert!(
                    matches!(error, IntroductionError::IndependentlyOwned),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(error, IntroductionError::LiveResponsibility),
                    "{error:?}"
                );
            }
            assert_eq!(state(&pg), before);
            assert_eq!(pg.conn.total_changes(), changes);
        }
    }
}

#[test]
fn staging_introduction_rolls_back_live_rows_and_high_water_on_child_insert_failure() {
    for target in targets() {
        let tmp = test_util::tempdir();
        let pg =
            PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let (owner, command) = setup(&pg, target);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        // Unrelated work already in the enclosing transaction must survive the
        // component's rollback, while all of the failed introduction disappears.
        let canary = MetadataCommandEnvelope::new(
            command.id(),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                owner.subject.bucket.clone(),
                crate::tests::object_key("canary"),
                crate::tests::stream_session_id("canary"),
                GenerationId::MIN,
                900,
            )),
        );
        pg.apply_metadata_command(&canary).unwrap();
        let before = state(&pg);
        pg.conn.execute_batch("CREATE TEMP TRIGGER fail_staging_resource BEFORE INSERT ON payload_staging_resources \
            BEGIN \
              SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = NEW.attempt) \
                THEN RAISE(ABORT, 'fault did not follow generation insertion') END; \
              SELECT CASE WHEN EXISTS(SELECT 1 FROM payload_staging_owners WHERE attempt = NEW.attempt AND target != 0) \
                AND NOT EXISTS(SELECT 1 FROM stream_uploads WHERE session_id = NEW.attempt) \
                THEN RAISE(ABORT, 'fault did not follow session insertion') END; \
              SELECT RAISE(ABORT, 'staging resource insertion failure'); \
            END").unwrap();
        let changes = pg.conn.total_changes();
        let error = apply(&pg, &owner, &command).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("staging resource insertion failure"),
            "{error:?}"
        );
        assert!(
            pg.conn.total_changes() > changes,
            "fault did not follow live row mutations"
        );
        assert!(!pg.conn.is_autocommit());
        assert_eq!(state(&pg), before);
        assert_eq!(loaded(&pg, &owner), None);
        pg.conn
            .execute_batch("DROP TRIGGER fail_staging_resource")
            .unwrap();
        assert_eq!(
            apply(&pg, &owner, &command).unwrap(),
            IntroductionApplied::Created
        );
        pg.conn.execute_batch("ROLLBACK; BEGIN").unwrap();
        assert_eq!(
            loaded(&pg, &owner),
            None,
            "component committed its caller's transaction"
        );
        assert!(matches!(
            pg.get_object_generation_reservation(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
            ),
            Err(MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn staging_introduction_rejects_changed_owner_without_overwriting_the_original() {
    for target in targets() {
        let tmp = test_util::tempdir();
        let pg =
            PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let (owner, command) = setup(&pg, target);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        apply(&pg, &owner, &command).unwrap();
        let mut changed = owner.clone();
        changed.windows[0].cutoff_ms -= 1;
        changed.maximum_cutoff_ms -= 1;
        changed.validate(&topology()).unwrap();
        let before = state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            apply(&pg, &changed, &command),
            Err(IntroductionError::OwnerConflict)
        ));
        assert_eq!(state(&pg), before);
        assert_eq!(pg.conn.total_changes(), changes);
    }
}

#[test]
fn staging_introduction_binds_enclosing_upload_generation_before_mutation() {
    let tmp = test_util::tempdir();
    let pg = PgStore::open(
        tmp.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let (owner, command) = setup(&pg, multipart_target());
    let mut crossed = owner.clone();
    let StagingTarget::UploadPart {
        upload_generation, ..
    } = &mut crossed.subject.target
    else {
        unreachable!();
    };
    *upload_generation = GenerationId::new(upload_generation.get() + 1).unwrap();
    // Recapture a structurally valid descriptor, including its corresponding
    // data-PG placement, to isolate binding to the actual upload row.
    let config = capture::config(&crossed);
    let crossed = with_time_and_monotonic_override(1_000, 70_000, || {
        capture::introduce(
            &pg,
            &config,
            &crossed,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        )
        .unwrap()
    });
    crossed.validate(&topology()).unwrap();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        apply(&pg, &crossed, &command),
        Err(IntroductionError::CommandBinding)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(state(&pg), before);
    assert_eq!(
        apply(&pg, &owner, &command).unwrap(),
        IntroductionApplied::Created
    );
}

#[test]
fn staging_introduction_replay_does_not_accept_changed_live_creation() {
    for target in targets() {
        for change in [
            "created_at",
            "cleanup_after",
            "next_segment_vid",
            "allocation_issuer",
        ] {
            if target == StagingTarget::DirectPut && change != "created_at" {
                continue;
            }
            let tmp = test_util::tempdir();
            let pg =
                PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let (owner, command) = setup(&pg, target.clone());
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            apply(&pg, &owner, &command).unwrap();
            let statement = match change {
                "created_at" => "UPDATE object_generation_reservations SET created_at = created_at + 1 WHERE reservation_id = ?1",
                "cleanup_after" => "UPDATE stream_uploads SET cleanup_after = cleanup_after + 1 WHERE session_id = ?1",
                "next_segment_vid" => "UPDATE stream_uploads SET next_segment_vid = next_segment_vid + 1 WHERE session_id = ?1",
                "allocation_issuer" => "UPDATE stream_uploads SET allocation_issuer_node = allocation_issuer_node + 1 WHERE session_id = ?1",
                _ => unreachable!(),
            };
            assert_eq!(
                pg.conn
                    .execute(statement, [&owner.subject.id.attempt])
                    .unwrap(),
                1
            );
            let before = state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply(&pg, &owner, &command),
                Err(IntroductionError::LiveResponsibility)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(state(&pg), before);
        }
    }
}

#[test]
fn staging_introduction_replay_requires_exact_object_generation_high_water() {
    for target in targets() {
        for mutation in [
            "missing",
            "null",
            "lowered",
            "foreign-bucket",
            "foreign-key",
        ] {
            let tmp = test_util::tempdir();
            let pg =
                PgStore::open(tmp.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let (owner, command) = setup(&pg, target.clone());
            let subject = &owner.subject;
            let reissued = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::new(6).unwrap(),
                    subject.object_pg,
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                command.payload().clone(),
            );
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(
                apply(&pg, &owner, &command).unwrap(),
                IntroductionApplied::Created
            );
            // Equality is sufficient; a later allocation raising the high-water
            // must not invalidate this older attempt's exact replay either.
            for high_water in [
                subject.payload_generation.get(),
                subject.payload_generation.get() + 4,
            ] {
                pg.conn.execute("UPDATE object_write_counters SET max_committed_generation = ?1 WHERE bucket = ?2 AND key = ?3",
                    rusqlite::params![high_water, subject.bucket, subject.key]).unwrap();
                let changes = pg.conn.total_changes();
                for envelope in [&command, &reissued] {
                    assert_eq!(
                        apply(&pg, &owner, envelope).unwrap(),
                        IntroductionApplied::ExactExisting
                    );
                }
                assert_eq!(pg.conn.total_changes(), changes);
            }
            let statement = match mutation {
                "missing" => "DELETE FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
                "null" => "UPDATE object_write_counters SET max_committed_generation = NULL WHERE bucket = ?1 AND key = ?2",
                "lowered" => "UPDATE object_write_counters SET max_committed_generation = max_committed_generation - 5 WHERE bucket = ?1 AND key = ?2",
                "foreign-bucket" => "UPDATE object_write_counters SET bucket = 'foreign-bucket' WHERE bucket = ?1 AND key = ?2",
                "foreign-key" => "UPDATE object_write_counters SET key = 'foreign-key' WHERE bucket = ?1 AND key = ?2",
                _ => unreachable!(),
            };
            assert_eq!(
                pg.conn
                    .execute(statement, rusqlite::params![subject.bucket, subject.key])
                    .unwrap(),
                1
            );
            // Reservation/session still exist: the failure must be the missing
            // durable allocator fence, not some other replay check.
            assert_eq!(
                pg.get_object_generation_reservation(
                    &subject.bucket,
                    &subject.key,
                    &subject.id.attempt
                )
                .unwrap(),
                subject.payload_generation
            );
            let before = state(&pg);
            let changes = pg.conn.total_changes();
            for envelope in [&command, &reissued] {
                let error = apply(&pg, &owner, envelope).unwrap_err();
                assert!(
                    matches!(
                        error,
                        IntroductionError::Invariant(StagingInvariant::GenerationFence)
                    ),
                    "{mutation}: {error:?}"
                );
            }
            assert_eq!(
                pg.conn.total_changes(),
                changes,
                "{mutation} repaired or mutated the counter"
            );
            assert_eq!(state(&pg), before);
            assert_eq!(loaded(&pg, &owner), Some(owner));
        }
    }
}
