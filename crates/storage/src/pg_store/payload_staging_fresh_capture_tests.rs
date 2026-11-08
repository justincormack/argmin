// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local primary snapshot/candidate tests. No physical payload, RPC
//! activation or frontend custody is claimed by this metadata-only matrix.

use super::super::capture::{StagingCapture, StagingCaptureError};
use super::super::descriptor::IntroductionDescriptor;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::AdmittedRouteEffectFence;

fn targets() -> [StagingTarget; 3] {
    [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ]
}

pub(super) fn template(target: StagingTarget) -> StagingOwnerRecord {
    let mut owner = fixture(target);
    if let StagingTarget::UploadPart { part_number, .. } = owner.subject.target {
        owner.subject.key = (0..1000)
            .map(|i| crate::tests::object_key(format!("fresh-capture-{i}")))
            .find(|key| {
                topology().object_generation_multipart_part_segment_data_pg(
                    &owner.subject.bucket,
                    key,
                    GenerationId::MIN,
                    part_number,
                    0,
                ) != topology().object_generation_multipart_part_segment_data_pg(
                    &owner.subject.bucket,
                    key,
                    GenerationId::new(2).unwrap(),
                    part_number,
                    0,
                )
            })
            .expect("distinct MPU and part placement");
    }
    owner.subject.object_pg =
        PgId::new(topology().object_pg_for(&owner.subject.bucket, &owner.subject.key));
    owner.subject.id.introduction = MetadataCommandId::new(
        owner.subject.originating_epoch,
        owner.subject.object_pg,
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    owner
}

pub(super) fn candidate(pg: &PgStore, owner: &mut StagingOwnerRecord) -> MetadataCommandEnvelope {
    introduction::seed_upload(pg, owner);
    owner.subject.payload_generation = pg
        .next_generation_id(&owner.subject.bucket, &owner.subject.key)
        .unwrap();
    owner.subject.issuer = pg
        .stream_allocation_issuer(owner.subject.issuer.node_id)
        .unwrap();
    // Only logical subject fields are used to construct this draft command.
    // Old template windows/resources are not supplied to the capture API.
    introduction::command(owner)
}

fn capture_command(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
    proof: &BucketWriteReservationProof,
    fence: AdmittedRouteEffectFence,
) -> Result<IntroductionDescriptor, StagingCaptureError> {
    let config = capture::config(owner);
    StagingCapture {
        topology: &topology(),
        placement: PayloadPlacementInput::from_server_config(&config),
        effect_fence: fence,
    }
    .introduction_for_command(pg, config.node_id, command, proof)
}

fn capture(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> Result<IntroductionDescriptor, StagingCaptureError> {
    capture_command(
        pg,
        owner,
        command,
        &proof(&owner.subject, owner.subject.creation_operation()),
        AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch),
    )
}

#[test]
fn staging_fresh_capture_loads_authoritative_introductions_without_effects() {
    let _time = test_time_override_guard(1000);
    for target in targets() {
        let mut owner = template(target);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let command = candidate(&pg, &mut owner);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let descriptor = capture(&pg, &owner, &command).unwrap();
        assert_eq!(
            IntroductionDescriptor::decode(&descriptor.encode().unwrap()).unwrap(),
            descriptor
        );
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        let topology = topology();
        let bound = descriptor.bind(&topology, &command).unwrap();
        let (_, _, captured) = bound.inputs();
        assert_eq!(captured.subject, owner.subject);
        assert_eq!(captured.windows[0].issuer, owner.subject.issuer);
        if let StagingTarget::UploadPart {
            upload_generation,
            part_number,
            ..
        } = captured.subject.target
        {
            assert_ne!(upload_generation, captured.subject.payload_generation);
            let expected = topology.object_generation_multipart_part_segment_data_pg(
                &owner.subject.bucket,
                &owner.subject.key,
                upload_generation,
                part_number,
                0,
            );
            let crossed = topology.object_generation_multipart_part_segment_data_pg(
                &owner.subject.bucket,
                &owner.subject.key,
                captured.subject.payload_generation,
                part_number,
                0,
            );
            assert_ne!(expected, crossed);
            assert_eq!(
                captured.windows[0].participants.data_pg.get(),
                expected.get()
            );
        }
        let captured = captured.clone();
        bound.apply_in_open_transaction(&pg).unwrap();
        assert_eq!(
            sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt).unwrap(),
            Some(captured)
        );
        pg.conn.execute_batch("COMMIT").unwrap();
    }
}

#[test]
fn staging_fresh_capture_requires_snapshot_and_current_candidate_position() {
    let _time = test_time_override_guard(1000);
    for target in targets() {
        let mut owner = template(target);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let command = candidate(&pg, &mut owner);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::SnapshotRequired)
        ));
        pg.conn.execute_batch("BEGIN").unwrap();
        capture(&pg, &owner, &command).unwrap();
        for (epoch, pg_id, index) in [
            (ClusterEpoch::new(6).unwrap(), command.id().pg_id(), 1),
            (command.id().cluster_epoch(), PgId::new(1000), 1),
            (command.id().cluster_epoch(), command.id().pg_id(), 2),
        ] {
            let crossed = MetadataCommandEnvelope::new(
                MetadataCommandId::new(epoch, pg_id, MetadataCommandLogIndex::new(index).unwrap()),
                command.payload().clone(),
            );
            assert!(matches!(
                capture(&pg, &owner, &crossed),
                Err(StagingCaptureError::Command)
            ));
        }
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        pg.try_insert_pending_metadata_command_slot(0, &command, Some(command.bucket_name()))
            .unwrap();
        pg.conn.execute_batch("BEGIN").unwrap();
        let writes = pg.conn.total_changes();
        assert!(
            matches!(
                capture(&pg, &owner, &command),
                Err(StagingCaptureError::Command)
            ),
            "pending introductions must use the retained descriptor, not recapture"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_capture_rejects_independently_owned_attempts_and_stale_generations() {
    let _time = test_time_override_guard(1000);
    for target in targets() {
        for fault in ["reservation", "session", "owner", "another generation"] {
            let mut owner = template(target.clone());
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            let command = candidate(&pg, &mut owner);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let descriptor = capture(&pg, &owner, &command).unwrap();
            if fault == "another generation" || fault == "reservation" {
                let reservation_id = if fault == "another generation" {
                    crate::tests::stream_session_id("other-owner")
                } else {
                    owner.subject.id.attempt.clone()
                };
                pg.apply_reserve_object_generation_command(
                    &crate::metadata_command::ReserveObjectGenerationCommand::new(
                        owner.subject.bucket.clone(),
                        owner.subject.key.clone(),
                        reservation_id,
                        owner.subject.payload_generation,
                        1000,
                    ),
                )
                .unwrap();
            } else {
                let topology = topology();
                let bound = descriptor.bind(&topology, &command).unwrap();
                if fault == "session" && matches!(target, StagingTarget::DirectPut) {
                    // A same-ID stream on the same PG cannot be adopted by PUT.
                    let mut stream = owner.clone();
                    stream.subject.target = StagingTarget::StreamPut;
                    let MetadataCommandPayload::CreateStreamUpload(create) =
                        introduction::command(&stream).payload().clone()
                    else {
                        unreachable!()
                    };
                    pg.apply_create_stream_upload_command(&create).unwrap();
                } else {
                    bound.apply_in_open_transaction(&pg).unwrap();
                    if fault == "session" {
                        pg.conn
                            .execute("DELETE FROM payload_staging_owners", [])
                            .unwrap();
                    }
                }
                pg.conn
                    .execute("DELETE FROM object_generation_reservations", [])
                    .unwrap();
                if fault == "owner" {
                    pg.conn.execute("DELETE FROM stream_uploads", []).unwrap();
                }
            }
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let result = capture(&pg, &owner, &command);
            if fault == "another generation" {
                assert!(
                    matches!(result, Err(StagingCaptureError::StaleGeneration)),
                    "{result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(StagingCaptureError::IndependentlyOwned)),
                    "{fault}: {result:?}"
                );
            }
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
            pg.conn.execute_batch("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn staging_fresh_capture_rejects_crossed_proof_and_stream_material_before_effects() {
    let _time = test_time_override_guard(1000);
    for target in targets() {
        let mut owner = template(target);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let command = candidate(&pg, &mut owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        let writes = pg.conn.total_changes();
        for field in ["bucket", "key", "epoch", "operation"] {
            let mut crossed = proof(&owner.subject, owner.subject.creation_operation());
            match field {
                "bucket" => crossed.bucket = crate::tests::bucket_name("other-bucket"),
                "key" => crossed.target_context = Some("other-key".into()),
                "epoch" => crossed.cluster_epoch = ClusterEpoch::new(6).unwrap(),
                "operation" => crossed.operation_kind = "other-operation".into(),
                _ => unreachable!(),
            }
            assert!(
                capture_command(
                    &pg,
                    &owner,
                    &command,
                    &crossed,
                    AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch)
                )
                .is_err(),
                "{field}"
            );
        }
        if let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() {
            for field in ["issuer", "incarnation", "cursor"] {
                let mut crossed = create.clone();
                match field {
                    "issuer" => crossed.allocation_issuer.lifetime[0] ^= 1,
                    "incarnation" => crossed.session.bucket_incarnation_generation += 1,
                    "cursor" => crossed.initial_next_segment_vid = GenerationId::new(2).unwrap(),
                    _ => unreachable!(),
                }
                let crossed = MetadataCommandEnvelope::new(
                    command.id(),
                    MetadataCommandPayload::CreateStreamUpload(crossed),
                );
                assert!(
                    matches!(
                        capture(&pg, &owner, &crossed),
                        Err(StagingCaptureError::Introduction(_))
                    ),
                    "{field}"
                );
            }
        }
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_capture_rejects_terminal_missing_and_crossed_uploads() {
    let _time = test_time_override_guard(1000);
    for fault in ["missing", "terminal", "key", "encryption"] {
        let mut owner = template(multipart_target());
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let command = candidate(&pg, &mut owner);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let command = if fault == "encryption" {
            let MetadataCommandPayload::CreateStreamUpload(mut create) = command.payload().clone()
            else {
                unreachable!()
            };
            let crate::ObjectEncryption::SseS3(state) = &mut create.session.encryption else {
                unreachable!()
            };
            let mut nonce = *state.wrap_nonce();
            nonce[0] ^= 1;
            *state = crate::SseS3ObjectState::new(
                state.wrapping_key_id(),
                nonce,
                *state.wrapped_dek(),
                *state.segment_nonce_prefix(),
            );
            MetadataCommandEnvelope::new(
                command.id(),
                MetadataCommandPayload::CreateStreamUpload(create),
            )
        } else {
            let sql = match fault {
                "missing" => "DELETE FROM multipart_uploads",
                "terminal" => "UPDATE multipart_uploads SET state = 1",
                "key" => "UPDATE multipart_uploads SET key = 'crossed-key'",
                _ => unreachable!(),
            };
            pg.conn.execute(sql, []).unwrap();
            command
        };
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let result = capture(&pg, &owner, &command);
        if fault == "missing" {
            assert!(
                matches!(
                    result,
                    Err(StagingCaptureError::Metadata(
                        crate::MetadataError::NoSuchUpload { .. }
                    ))
                ),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(result, Err(StagingCaptureError::Upload)),
                "{fault}: {result:?}"
            );
        }
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_capture_expiry_during_generation_read_returns_no_descriptor_or_mutation() {
    let time = test_time_override_guard(1000);
    let mut owner = template(StagingTarget::DirectPut);
    let dir = test_util::tempdir();
    let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    let command = candidate(&pg, &mut owner);
    pg.conn.execute_batch("BEGIN").unwrap();
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fired_hook = std::sync::Arc::clone(&fired);
    let control = time.control();
    pg.conn
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: rusqlite::trace::TraceEvent<'_>| {
                if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
                    if sql.contains("SELECT MAX(generation_id)") {
                        fired_hook.store(true, std::sync::atomic::Ordering::SeqCst);
                        control.set(4500);
                    }
                }
            }),
        )
        .unwrap();
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let result = capture_command(
        &pg,
        &owner,
        &command,
        &proof(&owner.subject, owner.subject.creation_operation()),
        AdmittedRouteEffectFence::bounded(owner.subject.originating_epoch, 5000, 4000),
    );
    assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        matches!(result, Err(StagingCaptureError::Store(_))),
        "{result:?}"
    );
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
