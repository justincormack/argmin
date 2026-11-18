// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only primary capture schedules using the retained preparation and
//! apply components. No shard I/O, frontend custody or production dispatch.

use super::super::append::{AppendApplied, AppendError};
use super::super::capture::{StagingCapture, StagingCaptureError};
use super::super::descriptor::AppendDescriptor;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::server::StorageNodeProcessConfig;
use crate::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use crate::{StoreError, StreamUploadTarget};

fn targets() -> [StagingTarget; 2] {
    [StagingTarget::StreamPut, multipart_target()]
}

pub(super) fn initialize(pg: &PgStore, target: StagingTarget) -> StagingOwnerRecord {
    let owner = append::initialize(pg, target);
    pg.record_metadata_command_applied_inner(
        owner.subject.issuer.node_id.as_u32(),
        &introduction::command(&owner),
    )
    .unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    owner
}

fn fence(owner: &StagingOwnerRecord) -> AdmittedRouteEffectFence {
    AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch)
}

pub(super) fn prepare(pg: &PgStore, owner: &StagingOwnerRecord) {
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let expected = preparation::expected(pg, owner);
    let segment = preparation::prepare(pg, owner, &expected, fence(owner)).unwrap();
    assert_eq!(segment.segment_index, owner.windows.last().unwrap().index);
    assert_eq!(segment.segment_vid, owner.windows.last().unwrap().start);
    pg.conn.execute_batch("COMMIT").unwrap();
}

fn capture_with(
    pg: &PgStore,
    config: &StorageNodeProcessConfig,
    command: &MetadataCommandEnvelope,
    effect_fence: AdmittedRouteEffectFence,
) -> Result<AppendDescriptor, StagingCaptureError> {
    StagingCapture {
        topology: &topology(),
        placement: PayloadPlacementInput::from_server_config(config),
        effect_fence,
    }
    .append_for_command(pg, config.node_id, command)
}

fn capture(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> Result<AppendDescriptor, StagingCaptureError> {
    capture_with(pg, &capture::config(owner), command, fence(owner))
}

fn apply(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
    descriptor: &AppendDescriptor,
) -> StagingOwnerRecord {
    assert_eq!(
        descriptor
            .bind(&topology(), command)
            .unwrap()
            .apply_in_open_transaction(pg)
            .unwrap(),
        AppendApplied::Appended,
    );
    pg.record_metadata_command_applied_inner(owner.subject.issuer.node_id.as_u32(), command)
        .unwrap();
    append::load(pg, owner)
}

#[test]
fn staging_fresh_append_capture_preserves_prefix_and_requires_retained_descriptors_for_replay() {
    let clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let mut owner = initialize(&pg, target);
        let mut first = None;
        for index in 0..3 {
            clock.set(2_000 + index * 1_000);
            prepare(&pg, &owner);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let command = append::command(&owner);
            let state = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let descriptor = capture(&pg, &owner, &command).unwrap();
            assert_eq!(
                AppendDescriptor::decode(&descriptor.encode().unwrap()).unwrap(),
                descriptor
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), state);
            let after = apply(&pg, &owner, &command, &descriptor);
            assert_eq!(after.windows[..owner.windows.len()], owner.windows);
            assert_eq!(after.resources, owner.resources);
            assert_eq!(after.references.len(), owner.references.len() + 1);
            assert_eq!(after.windows.last().unwrap().start, owner.reserved_end);
            assert_eq!(
                after.windows.last().unwrap().issued_at_ms,
                2_000 + index * 1_000
            );
            assert_eq!(
                after.windows.last().unwrap().cutoff_ms,
                302_000 + index * 1_000
            );
            pg.conn.execute_batch("COMMIT").unwrap();
            first.get_or_insert((command, descriptor));
            owner = after;
        }
        let (command, descriptor) = first.unwrap();
        pg.conn.execute_batch("BEGIN").unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        // Give an old selected segment a fresh log position: position alone
        // must not permit recapture and renewal of its original lookahead.
        let old_at_next_position =
            MetadataCommandEnvelope::new(append::command(&owner).id(), command.payload().clone());
        assert!(matches!(
            capture(&pg, &owner, &old_at_next_position),
            Err(StagingCaptureError::Append(AppendError::SelectedPrefix))
        ));
        assert_eq!(
            descriptor
                .bind(&topology(), &command)
                .unwrap()
                .apply_in_open_transaction(&pg)
                .unwrap(),
            AppendApplied::ExactExisting
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_requires_snapshot_preparation_and_next_unoccupied_position() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        let command = append::command(&owner);
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::SnapshotRequired)
        ));
        pg.conn.execute_batch("BEGIN").unwrap();
        let changes = pg.conn.total_changes();
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::Append(AppendError::LiveResponsibility))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        capture(&pg, &owner, &command).unwrap();
        let changes = pg.conn.total_changes();
        for (epoch, pg_id, index) in [
            (ClusterEpoch::new(6).unwrap(), command.id().pg_id(), 2),
            (command.id().cluster_epoch(), PgId::new(1000), 2),
            (command.id().cluster_epoch(), command.id().pg_id(), 1),
            (command.id().cluster_epoch(), command.id().pg_id(), 3),
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
        assert_eq!(pg.conn.total_changes(), changes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        pg.try_insert_pending_metadata_command_slot(
            owner.subject.issuer.node_id.as_u32(),
            &command,
            Some(command.bucket_name()),
        )
        .unwrap();
        pg.conn.execute_batch("BEGIN").unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::Command)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_rejects_crossed_segment_subjects_and_integrity() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        let good = append::command(&owner);
        capture(&pg, &owner, &good).unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for case in 0..13 {
            let MetadataCommandPayload::AppendStreamSegment(mut command) = good.payload().clone()
            else {
                unreachable!()
            };
            match case {
                0 => command.bucket = crate::tests::bucket_name("crossed"),
                1 => command.key = crate::tests::object_key("crossed"),
                2 => command.segment.session_id = crate::tests::stream_session_id("crossed"),
                3 => command.segment.segment_index += 1,
                4 => command.segment.segment_okh[0] ^= 1,
                5 => command.segment.segment_vid = GenerationId::new(2).unwrap(),
                6 => command.segment.data_pg_id = (command.segment.data_pg_id + 1) % 8,
                7 => command.segment.placement_cluster_epoch = ClusterEpoch::new(4).unwrap(),
                8 => command.segment.ec_m += 1,
                9 => command.segment.size += 1,
                10 => {
                    command.target = match command.target {
                        StreamUploadTarget::PutObject => StreamUploadTarget::UploadPart {
                            upload_id: crate::tests::multipart_upload_id("crossed"),
                            part_number: 1,
                        },
                        StreamUploadTarget::UploadPart { .. } => StreamUploadTarget::PutObject,
                    }
                }
                11 => command.segment.shard_integrity = None,
                12 => command.segment.segment_crc64 ^= 1,
                _ => unreachable!(),
            }
            let crossed = MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::AppendStreamSegment(command),
            );
            assert!(capture(&pg, &owner, &crossed).is_err(), "case {case}");
            assert_eq!(pg.conn.total_changes(), changes, "case {case}");
            assert_eq!(introduction::state(&pg), state, "case {case}");
        }
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_revalidates_durable_responsibility_and_selected_prefix() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let mutations = [
            "DELETE FROM payload_staging_owners",
            "DELETE FROM object_generation_reservations",
            "UPDATE object_generation_reservations SET generation_id = generation_id + 1",
            "DELETE FROM object_write_counters",
            "UPDATE object_write_counters SET max_committed_generation = 1",
            "DELETE FROM stream_uploads",
            "UPDATE stream_uploads SET state = 1",
            "UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1",
            "UPDATE stream_uploads SET next_segment_vid = 5",
            "DELETE FROM stream_upload_segments",
            "UPDATE stream_upload_segments SET data_pg_id = (data_pg_id + 1) % 8",
            "DELETE FROM multipart_uploads",
            "UPDATE multipart_uploads SET state = 1",
            "UPDATE multipart_uploads SET object_generation_id = object_generation_id + 1",
            "UPDATE multipart_uploads SET key = 'crossed-key'",
        ];
        for mutation in mutations {
            if matches!(target, StagingTarget::StreamPut) && mutation.contains("multipart_uploads")
            {
                continue;
            }
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let mut owner = initialize(&pg, target.clone());
            prepare(&pg, &owner);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let first = append::command(&owner);
            let descriptor = capture(&pg, &owner, &first).unwrap();
            owner = apply(&pg, &owner, &first, &descriptor);
            pg.conn.execute_batch("COMMIT").unwrap();
            prepare(&pg, &owner);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let command = append::command(&owner);
            capture(&pg, &owner, &command).unwrap();
            pg.conn.execute(mutation, []).unwrap();
            let state = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(
                capture(&pg, &owner, &command).is_err(),
                "accepted {mutation}"
            );
            assert_eq!(pg.conn.total_changes(), changes, "{mutation}");
            assert_eq!(introduction::state(&pg), state, "{mutation}");
            pg.conn.execute_batch("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn staging_fresh_append_capture_binds_actual_file_and_acknowledgement_participants() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let mut original = fixture(target);
        if let StagingTarget::UploadPart {
            upload_generation, ..
        } = &mut original.subject.target
        {
            *upload_generation = GenerationId::MIN;
        }
        original.subject.key = (0..1000)
            .map(|i| crate::tests::object_key(format!("capture-placement-{i}")))
            .find(|key| {
                let mut subject = original.subject.clone();
                subject.key = key.clone();
                subject.data_pg(&topology(), 0) != topology().object_pg_for(&subject.bucket, key)
            })
            .expect("distinct object and data PGs");
        original.subject.object_pg =
            PgId::new(topology().object_pg_for(&original.subject.bucket, &original.subject.key));
        original.subject.id.introduction = MetadataCommandId::new(
            original.subject.originating_epoch,
            original.subject.object_pg,
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), original.subject.object_pg.get()).unwrap();
        introduction::seed_upload(&pg, &mut original);
        let config = capture::config(&original);
        let owner = capture::introduce(&pg, &config, &original, fence(&original)).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let create = introduction::command(&owner);
        super::super::descriptor::apply_introduction_for_test(&pg, &topology(), &create, &owner)
            .unwrap();
        pg.record_metadata_command_applied_inner(config.node_id.as_u32(), &create)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        assert_ne!(
            owner.windows[0].participants.data_pg,
            owner.subject.object_pg
        );
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        let command = append::command(&owner);
        capture(&pg, &owner, &command).unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for fault in ["files", "ack", "ec"] {
            let mut config = capture::config(&owner);
            let data_pg = owner.windows[0].participants.data_pg.get();
            let route = config
                .pg_routes
                .iter_mut()
                .find(|route| route.pg_id == data_pg)
                .unwrap();
            match fault {
                "files" => {
                    let old_file = owner.windows[0]
                        .participants
                        .files
                        .iter()
                        .copied()
                        .find(|node| *node != config.node_id)
                        .unwrap();
                    *route
                        .acting_set
                        .iter_mut()
                        .find(|node| **node == old_file)
                        .unwrap() = NodeId::new(6);
                }
                "ack" => route.primary_node_id = NodeId::new(2),
                "ec" => config.default_ec_shape = EcShape { k: 1, m: 2 },
                _ => unreachable!(),
            }
            let result = capture_with(&pg, &config, &command, fence(&owner));
            if fault == "ec" {
                assert!(
                    matches!(
                        result,
                        Err(StagingCaptureError::Store(
                            StoreError::RouteCapabilitySubjectMismatch { .. }
                        ))
                    ),
                    "{result:?}"
                );
            } else {
                assert!(
                    matches!(
                        result,
                        Err(StagingCaptureError::Invariant(StagingInvariant::Placement))
                    ),
                    "{fault}: {result:?}"
                );
            }
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), state);
        }
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_rejects_restarted_issuer_but_retained_apply_converges() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg_id = fixture(target.clone()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = initialize(&pg, target);
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        let command = append::command(&owner);
        let descriptor = capture(&pg, &owner, &command).unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::Invariant(
                StagingInvariant::WindowOwner
            ))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        let after = apply(&pg, &owner, &command, &descriptor);
        assert_eq!(after.subject.issuer, owner.subject.issuer);
        assert_eq!(after.windows[0], owner.windows[0]);
        assert_eq!(after.windows[1].issued_at_ms, 2_000);
        pg.conn.execute_batch("COMMIT").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_rejects_closed_owner_despite_prepared_live_session() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let command = append::command(&owner);
        capture(&pg, &owner, &command).unwrap();
        let closed = owner.closed_unpublished().unwrap();
        let topology = topology();
        sql::close_unpublished(
            &pg,
            owner.subject.object_pg,
            &topology,
            &owner.validate(&topology).unwrap(),
            &closed.validate(&topology).unwrap(),
        )
        .unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            capture(&pg, &owner, &command),
            Err(StagingCaptureError::Invariant(StagingInvariant::Lifecycle))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_fresh_append_capture_expiry_during_prefix_validation_returns_no_descriptor() {
    let clock = test_time_override_guard(2_000);
    for target in targets() {
        clock.set(2_000);
        let dir = test_util::tempdir();
        let mut pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        prepare(&pg, &owner);
        pg.conn.execute_batch("BEGIN").unwrap();
        let command = append::command(&owner);
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_fired = std::sync::Arc::clone(&fired);
        let control = clock.control();
        pg.conn
            .trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: rusqlite::trace::TraceEvent<'_>| {
                    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
                        if sql.contains("FROM stream_upload_segments") {
                            hook_fired.store(true, std::sync::atomic::Ordering::SeqCst);
                            control.set(4_500);
                        }
                    }
                }),
            )
            .unwrap();
        let result = capture_with(
            &pg,
            &capture::config(&owner),
            &command,
            AdmittedRouteEffectFence::bounded(owner.subject.originating_epoch, 5_000, 4_000),
        );
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            matches!(
                result,
                Err(StagingCaptureError::Store(
                    StoreError::RouteMapExpired { .. }
                ))
            ),
            "{result:?}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
