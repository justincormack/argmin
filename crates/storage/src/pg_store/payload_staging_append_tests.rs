// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real append mutations and owner updates, but no shard I/O: integrity records
//! describe metadata-only specimens. Descriptors are separate component inputs
//! until the coordinated command-format/custody integration activates dispatch.

use super::super::append::{AppendApplied, AppendError};
use super::super::capture::StagingCapture;
use super::super::descriptor::apply_append_for_test;
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::metadata_command::{
    AppendStreamSegmentCommand, MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{AdmittedRouteEffectFence, SegmentShardIntegrityWitness};
use crate::{ShardKey, StreamUploadSegmentRecord, StreamUploadTarget, WriteAck};

fn targets() -> [StagingTarget; 2] {
    [StagingTarget::StreamPut, multipart_target()]
}

#[test]
fn staging_epoch_reset_append_preserves_captured_lookahead_and_issuer() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        // Capture before epoch/lifetime replacement; replica application cannot
        // mint another allocation or renew the original window's cutoff.
        let next = next(&pg, &owner);
        let original = command(&owner);
        let reissued = epoch_order::at_epoch_start(&pg, &original, ClusterEpoch::new(6).unwrap());
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        assert_eq!(
            apply(&pg, &reissued, &next).unwrap(),
            AppendApplied::Appended
        );
        let expected = load(&pg, &owner);
        assert_eq!(
            expected.windows,
            vec![owner.windows[0].clone(), next.clone()]
        );
        assert_eq!(expected.resources, owner.resources);
        assert_eq!(
            expected.references,
            vec![owner.selected_stream_reference(&segment(&owner)).unwrap()]
        );
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &reissued);
        assert_eq!(load(&pg, &owner), expected);
    }
}

pub(super) fn initialize(pg: &PgStore, target: StagingTarget) -> StagingOwnerRecord {
    let (owner, command) = introduction::setup(pg, target);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    super::super::descriptor::apply_introduction_for_test(pg, &topology(), &command, &owner)
        .unwrap();
    owner
}

pub(super) fn load(pg: &PgStore, owner: &StagingOwnerRecord) -> StagingOwnerRecord {
    sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt)
        .unwrap()
        .unwrap()
}

pub(super) fn next(pg: &PgStore, owner: &StagingOwnerRecord) -> StagingWindow {
    let config = capture::config(owner);
    with_time_and_monotonic_override(2_000, 71_000, || {
        StagingCapture {
            topology: &topology(),
            placement: PayloadPlacementInput::from_server_config(&config),
            effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        }
        .next_window(pg, config.node_id, owner)
        .unwrap()
    })
}

pub(super) fn segment(owner: &StagingOwnerRecord) -> StreamUploadSegmentRecord {
    let window = owner.windows.last().unwrap();
    let logical_size = 4;
    let stored_size = window.geometry.codec.stored_size(logical_size).unwrap();
    let crc = checksum::crc64::checksum(&vec![0; stored_size as usize]);
    let shard_size = stored_size.div_ceil(u64::from(window.geometry.ec.k));
    let ack = WriteAck {
        stored_size: shard_size,
        crc64: checksum::crc64::checksum(&vec![0; shard_size as usize]),
    };
    let keys = (0..window.geometry.ec.k + window.geometry.ec.m)
        .map(|index| ShardKey::new(&window.segment_hash, window.start.get(), index))
        .collect::<Vec<_>>();
    let acks = keys.iter().map(|key| (key, ack)).collect::<Vec<_>>();
    let witness = SegmentShardIntegrityWitness::from_ordered_shard_acks(
        window.participants.data_pg.get(),
        window.geometry.ec,
        &window.segment_hash,
        window.start,
        stored_size,
        crc,
        &acks,
    )
    .unwrap();
    StreamUploadSegmentRecord {
        session_id: owner.subject.id.attempt.clone(),
        segment_index: window.index,
        size: logical_size,
        segment_crc64: crc,
        payload_crc64: 0,
        segment_okh: window.segment_hash,
        segment_vid: window.start,
        data_pg_id: window.participants.data_pg.get(),
        placement_cluster_epoch: window.participants.placement_epoch,
        ec_k: window.geometry.ec.k,
        ec_m: window.geometry.ec.m,
        shard_integrity: Some(witness.durable_record()),
    }
}

pub(super) fn command(owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    let window = owner.windows.last().unwrap();
    let target = match &owner.subject.target {
        StagingTarget::StreamPut => StreamUploadTarget::PutObject,
        StagingTarget::UploadPart {
            upload_id,
            part_number,
            ..
        } => StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: *part_number,
        },
        StagingTarget::DirectPut => unreachable!(),
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            owner.subject.originating_epoch,
            owner.subject.object_pg,
            MetadataCommandLogIndex::new(
                owner.subject.id.introduction.log_index().get() + u64::from(window.index) + 1,
            )
            .unwrap(),
        ),
        MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
            bucket: owner.subject.bucket.clone(),
            key: owner.subject.key.clone(),
            target,
            segment: segment(owner),
        })),
    )
}

fn apply(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    next: &StagingWindow,
) -> Result<AppendApplied, AppendError> {
    // Exercise the current wire shape too; the separate lookahead is NOT yet a
    // wire-field acceptance test and must be made mandatory before activation.
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_append_for_test(pg, &topology(), &decoded, next)
}

#[test]
fn staging_append_witness_first_is_atomic_and_preserves_primary_lookahead() {
    for target in targets() {
        let primary_dir = test_util::tempdir();
        let witness_dir = test_util::tempdir();
        let pg_id = fixture(target.clone()).subject.object_pg.get();
        let primary = PgStore::open(primary_dir.path(), pg_id).unwrap();
        let witness = PgStore::open(witness_dir.path(), pg_id).unwrap();
        let (owner, create) = introduction::setup(&primary, target);
        let mut witness_input = owner.clone();
        with_time_and_monotonic_override(1_000, 70_000, || {
            introduction::seed_upload(&witness, &mut witness_input)
        });
        assert_eq!(witness_input, owner);
        assert_ne!(
            witness.stream_allocation_issuer(NodeId::new(2)).unwrap(),
            owner.subject.issuer
        );
        for pg in [&primary, &witness] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            super::super::descriptor::apply_introduction_for_test(pg, &topology(), &create, &owner)
                .unwrap();
        }
        let command = command(&owner);
        let next = next(&primary, &owner);
        assert_eq!(
            primary
                .allocate_stream_segment_vid_for_admitted_append(
                    &owner.subject.id.attempt,
                    0,
                    owner.subject.issuer.node_id,
                    AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch),
                )
                .unwrap(),
            Some(owner.windows[0].start)
        );
        assert_eq!(
            witness
                .get_stream_upload(&owner.subject.id.attempt)
                .unwrap()
                .next_segment_vid,
            owner.windows[0].start
        );
        for pg in [&witness, &primary] {
            with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                assert_eq!(apply(pg, &command, &next).unwrap(), AppendApplied::Appended);
            });
            let after = load(pg, &owner);
            assert_eq!(after.windows, vec![owner.windows[0].clone(), next.clone()]);
            assert_eq!(after.resources, owner.resources);
            assert_eq!(after.reserved_end, next.end);
            assert_eq!(after.maximum_cutoff_ms, next.cutoff_ms);
            assert_eq!(after.references.len(), 1);
            assert_eq!(
                pg.list_stream_segments(&owner.subject.id.attempt)
                    .unwrap()
                    .len(),
                1
            );
            // Replica apply advances only the consumed VID; lookahead is not a
            // second local allocation, and does not gain a new issuer lifetime.
            assert_eq!(
                pg.get_stream_upload(&owner.subject.id.attempt)
                    .unwrap()
                    .next_segment_vid,
                next.start
            );
            assert!(!pg.conn.is_autocommit());
        }
        assert_eq!(load(&primary, &owner), load(&witness, &owner));
    }
}

#[test]
fn staging_append_replay_after_later_append_and_restart_is_read_only() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg_id = fixture(target.clone()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = initialize(&pg, target);
        let first = command(&owner);
        let first_next = next(&pg, &owner);
        assert_eq!(
            apply(&pg, &first, &first_next).unwrap(),
            AppendApplied::Appended
        );
        let middle = load(&pg, &owner);
        let second = command(&middle);
        let mut second_next = next(&pg, &middle);
        second_next.cutoff_ms -= 1;
        // Updating or reinserting earlier rows must not be an implementation
        // shortcut for append. No resource obligation changes at this boundary.
        pg.conn.execute_batch(
            "CREATE TEMP TRIGGER immutable_windows BEFORE UPDATE ON payload_staging_windows BEGIN SELECT RAISE(ABORT, 'rewrote prior window'); END;
             CREATE TEMP TRIGGER retained_windows BEFORE DELETE ON payload_staging_windows BEGIN SELECT RAISE(ABORT, 'removed prior window'); END;
             CREATE TEMP TRIGGER immutable_references BEFORE UPDATE ON payload_staging_references BEGIN SELECT RAISE(ABORT, 'rewrote prior reference'); END;
             CREATE TEMP TRIGGER retained_references BEFORE DELETE ON payload_staging_references BEGIN SELECT RAISE(ABORT, 'removed prior reference'); END;
             CREATE TEMP TRIGGER retained_resources BEFORE DELETE ON payload_staging_resources BEGIN SELECT RAISE(ABORT, 'removed resource'); END;
             CREATE TEMP TRIGGER immutable_resources BEFORE UPDATE ON payload_staging_resources BEGIN SELECT RAISE(ABORT, 'rewrote resource'); END;
             CREATE TEMP TRIGGER no_new_resource BEFORE INSERT ON payload_staging_resources BEGIN SELECT RAISE(ABORT, 'added resource'); END"
        ).unwrap();
        assert_eq!(
            apply(&pg, &second, &second_next).unwrap(),
            AppendApplied::Appended
        );
        let expected = load(&pg, &owner);
        assert_eq!(expected.maximum_cutoff_ms, first_next.cutoff_ms);
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let reissue = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(6).unwrap(),
                owner.subject.object_pg,
                MetadataCommandLogIndex::new(20).unwrap(),
            ),
            first.payload().clone(),
        );
        for command in [&first, &second, &reissue] {
            let next = if command == &second {
                &second_next
            } else {
                &first_next
            };
            with_time_and_monotonic_override(9_000_000, 900_000_000, || {
                assert_eq!(
                    apply(&pg, command, next).unwrap(),
                    AppendApplied::ExactExisting
                );
            });
        }
        assert_eq!(load(&pg, &owner), expected);
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        // A new but individually valid descriptor is not an exact replay.
        let mut crossed = first_next;
        crossed.cutoff_ms -= 1;
        assert!(matches!(
            apply(&pg, &first, &crossed),
            Err(AppendError::SelectedPrefix)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
    }
}

#[test]
fn staging_append_crossed_command_and_descriptor_reject_before_mutation() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        let good = command(&owner);
        let next = next(&pg, &owner);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for case in 0..12 {
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
                _ => unreachable!(),
            }
            let crossed = MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::AppendStreamSegment(command),
            );
            assert!(
                apply(&pg, &crossed, &next).is_err(),
                "accepted crossed command case {case}"
            );
            assert_eq!(pg.conn.total_changes(), changes, "case {case}");
            assert_eq!(introduction::state(&pg), before, "case {case}");
        }
        for case in 0..10 {
            let mut crossed = next.clone();
            match case {
                0 => crossed.owner.attempt = crate::tests::stream_session_id("crossed"),
                1 => crossed.owner.introduction = good.id(),
                2 => crossed.issuer.lifetime[0] ^= 1,
                3 => crossed.index += 1,
                4 => crossed.segment_hash[0] ^= 1,
                5 => {
                    crossed.start = GenerationId::new(3).unwrap();
                    crossed.end = GenerationId::new(4).unwrap();
                }
                6 => {
                    crossed.participants.data_pg =
                        PgId::new((crossed.participants.data_pg.get() + 1) % 8)
                }
                7 => crossed.participants.placement_epoch = ClusterEpoch::new(6).unwrap(),
                8 => crossed.geometry.stored_bound += 1,
                9 => crossed.cutoff_ms = crossed.issued_at_ms,
                _ => unreachable!(),
            }
            assert!(
                apply(&pg, &good, &crossed).is_err(),
                "accepted crossed descriptor case {case}"
            );
            assert_eq!(pg.conn.total_changes(), changes, "case {case}");
            assert_eq!(introduction::state(&pg), before, "case {case}");
        }
        assert_eq!(apply(&pg, &good, &next).unwrap(), AppendApplied::Appended);
    }
}

#[test]
fn staging_append_rejects_missing_or_changed_live_responsibility_and_prefix() {
    for target in targets() {
        for mutation in [
            "DELETE FROM object_generation_reservations",
            "UPDATE object_generation_reservations SET generation_id = generation_id + 1",
            "DELETE FROM object_write_counters",
            "UPDATE object_write_counters SET max_committed_generation = 1",
            "UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1",
            "UPDATE stream_uploads SET next_segment_vid = 5",
            "DELETE FROM stream_upload_segments",
            "UPDATE stream_upload_segments SET data_pg_id = (data_pg_id + 1) % 8",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let original = initialize(&pg, target.clone());
            let first = command(&original);
            let first_next = next(&pg, &original);
            apply(&pg, &first, &first_next).unwrap();
            let owner = load(&pg, &original);
            let command = command(&owner);
            let next = next(&pg, &owner);
            pg.conn.execute_batch(mutation).unwrap();
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &command, &next).is_err(), "accepted {mutation}");
            // Replay cannot resurrect or silently bless the damaged prefix.
            assert!(apply(&pg, &first, &first_next).is_err(), "replay accepted {mutation}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_append_child_failures_rollback_live_segment_cursor_and_ownership() {
    for target in targets() {
        for table in [
            "payload_staging_windows",
            "payload_staging_participants",
            "payload_staging_references",
        ] {
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone());
            let command = command(&owner);
            let next = next(&pg, &owner);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!(
                "CREATE TEMP TRIGGER reject_staging_append BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'append child failure'); END"
            )).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &command, &next).unwrap_err();
            assert!(
                error.to_string().contains("append child failure"),
                "{error}"
            );
            assert_eq!(introduction::state(&pg), before, "{table}");
            assert!(!pg.conn.is_autocommit());
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM outer_work", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                42
            );
            pg.conn
                .execute_batch("DROP TRIGGER reject_staging_append")
                .unwrap();
            assert_eq!(
                apply(&pg, &command, &next).unwrap(),
                AppendApplied::Appended
            );
        }
    }
}

#[test]
fn staging_append_rejects_crossed_envelope_and_missing_transaction() {
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        let good = command(&owner);
        let next = next(&pg, &owner);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for id in [
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
            owner.subject.id.introduction,
        ] {
            let crossed = MetadataCommandEnvelope::new(id, good.payload().clone());
            assert!(matches!(
                apply(&pg, &crossed, &next),
                Err(AppendError::CommandBinding)
            ));
        }
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        // Bind while the fixture's transaction is open, then exercise the real
        // application boundary without a transaction. The fixture adapter needs
        // a transaction just to load the subject and cannot test this boundary.
        let topology = topology();
        let descriptor = super::super::descriptor::AppendDescriptor::capture(
            &owner.validate(&topology).unwrap(),
            next,
        )
        .unwrap();
        let bound = descriptor.bind(&topology, &good).unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        assert!(matches!(
            super::super::append::apply_in_open_transaction(&pg, bound),
            Err(AppendError::TransactionRequired)
        ));
    }
}

#[test]
fn staging_append_multipart_requires_the_exact_live_upload_generation() {
    for mutation in [
        "UPDATE multipart_uploads SET object_generation_id = object_generation_id + 1",
        "UPDATE multipart_uploads SET key = 'crossed'",
        "UPDATE stream_uploads SET allocation_issuer_lifetime = zeroblob(16)",
    ] {
        let dir = test_util::tempdir();
        let target = multipart_target();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target);
        let command = command(&owner);
        let next = next(&pg, &owner);
        pg.conn.execute_batch(mutation).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(
            matches!(
                apply(&pg, &command, &next),
                Err(AppendError::LiveResponsibility)
            ),
            "{mutation}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}
