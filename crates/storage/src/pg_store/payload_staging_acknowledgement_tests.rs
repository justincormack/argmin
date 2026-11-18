// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{DirectStagingFileWriter, StreamStagingFileWriter};
use crate::node_runtime::traits::ShardStore;
use crate::{ShardKey, StoreError, WriteAck};

#[path = "payload_staging_direct_integrity_tests.rs"]
mod direct_integrity;
#[path = "payload_staging_acknowledged_direct_preparation_tests.rs"]
mod direct_preparation;
#[path = "payload_staging_direct_terminal_tests.rs"]
mod direct_terminal;
#[path = "payload_staging_acknowledged_append_install_tests.rs"]
mod installation;

enum Writer<'a> {
    Direct(Box<DirectStagingFileWriter<'a>>),
    Stream(Box<StreamStagingFileWriter<'a>>),
}

#[test]
fn staging_acknowledged_append_preparation_derives_complete_command_without_effects() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            with_writer_and_section(target.clone(), appended, |f, owner, mut writer, section| {
                let shards = files::shards(owner, b"data");
                let acks: Vec<_> = shards
                    .iter()
                    .enumerate()
                    .map(|(i, bytes)| writer.write(i as u8, bytes))
                    .collect();
                writer.persist().unwrap();
                let Writer::Stream(writer) = writer else {
                    unreachable!()
                };
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let history = ready.test_binding_state();
                let old_id = ready.id();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let changes = node
                    .get_pg(old_id.pg_id().get())
                    .unwrap()
                    .conn
                    .total_changes();
                let prepared = ready
                    .prepare_append_embedded(section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(prepared.test_binding_state(), history);
                assert_eq!(prepared.id().pg_id(), old_id.pg_id());
                assert_eq!(prepared.id().cluster_epoch(), old_id.cluster_epoch());
                assert_eq!(
                    prepared.id().log_index().get(),
                    old_id.log_index().get() + 1
                );
                let command = prepared.test_command();
                let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(command) =
                    command.payload()
                else {
                    panic!("not append")
                };
                let tail = owner.windows.last().unwrap();
                assert_eq!(command.bucket, owner.subject.bucket);
                assert_eq!(command.key, owner.subject.key);
                assert_eq!(command.segment.session_id, owner.subject.id.attempt);
                assert_eq!(command.segment.segment_index, tail.index);
                assert_eq!(command.segment.segment_vid, tail.start);
                assert_eq!(command.segment.segment_okh, tail.segment_hash);
                let keys: Vec<_> = (0..acks.len())
                    .map(|i| ShardKey::new(&tail.segment_hash, tail.start.get(), i as u8))
                    .collect();
                let expected = crate::types::SegmentShardIntegrityWitness::from_ordered_shard_acks(
                    tail.participants.data_pg.get(),
                    tail.geometry.ec,
                    &tail.segment_hash,
                    tail.start,
                    tail.geometry.codec.stored_size(4).unwrap(),
                    command.segment.segment_crc64,
                    &keys.iter().zip(acks.iter().copied()).collect::<Vec<_>>(),
                )
                .unwrap();
                assert_eq!(
                    command.segment.shard_integrity,
                    Some(expected.durable_record())
                );
                drop(prepared);
                assert_eq!(
                    node.get_pg(old_id.pg_id().get())
                        .unwrap()
                        .conn
                        .total_changes(),
                    changes
                );
                assert_eq!(custody(f, owner), *owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_preparation_rejects_changed_session_and_preserves_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        with_writer_and_section(target, false, |f, owner, mut writer, section| {
            for (i, bytes) in files::shards(owner, b"data").iter().enumerate() {
                writer.write(i as u8, bytes);
            }
            writer.persist().unwrap();
            let Writer::Stream(writer) = writer else {
                unreachable!()
            };
            let ready = writer
                .into_acknowledged()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let history = ready.test_binding_state();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            {
                let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                // Revalidate the unchanged command prefix after changing local,
                // noncanonical allocator state, to reach exact cursor binding.
                pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = next_segment_vid + 1 WHERE session_id = ?1", [owner.subject.id.attempt.as_str()]).unwrap();
                pg.recover_complete_command_state(
                    PgStoreRecoveryContext::for_node(NodeId::new(4)),
                    &topology(),
                )
                .unwrap();
            }
            let changes = node
                .get_pg(owner.subject.object_pg.get())
                .unwrap()
                .conn
                .total_changes();
            let failure = match ready.prepare_append_embedded(section) {
                Ok(_) => panic!("changed session authority accepted"),
                Err(failure) => failure,
            };
            assert!(
                failure
                    .error()
                    .to_string()
                    .contains("stream file consumed cursor"),
                "{}",
                failure.error()
            );
            let ready = failure.into_acknowledged();
            assert_eq!(ready.test_binding_state(), history);
            {
                let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                assert_eq!(pg.conn.total_changes(), changes);
                pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = next_segment_vid - 1 WHERE session_id = ?1", [owner.subject.id.attempt.as_str()]).unwrap();
                pg.recover_complete_command_state(
                    PgStoreRecoveryContext::for_node(NodeId::new(4)),
                    &topology(),
                )
                .unwrap();
            }
            let prepared = ready
                .prepare_append_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(prepared.test_binding_state(), history);
            drop(prepared);
            assert_eq!(custody(f, owner), *owner);
        });
    }
}

#[test]
fn staging_acknowledged_append_preparation_rejects_receipt_crc_mismatch_and_expiry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for corrupt in [false, true] {
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let mut shards = files::shards(owner, b"data");
                if corrupt {
                    shards[0][0] ^= 1;
                }
                for (i, bytes) in shards.iter().enumerate() {
                    writer.write(i as u8, bytes);
                }
                writer.persist().unwrap();
                let Writer::Stream(writer) = writer else {
                    unreachable!()
                };
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let history = ready.test_binding_state();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let changes = node
                    .get_pg(owner.subject.object_pg.get())
                    .unwrap()
                    .conn
                    .total_changes();
                let result = if corrupt {
                    ready.prepare_append_embedded(section)
                } else {
                    crate::clock::with_time_override(
                        crate::clock::current_time_millis() + 6_000,
                        || ready.prepare_append_embedded(section),
                    )
                };
                let failure = match result {
                    Ok(_) => panic!("invalid preparation succeeded"),
                    Err(failure) => failure,
                };
                if corrupt {
                    assert!(
                        failure
                            .error()
                            .to_string()
                            .contains("acknowledged append integrity"),
                        "{}",
                        failure.error()
                    );
                }
                assert_eq!(failure.into_acknowledged().test_binding_state(), history);
                assert_eq!(
                    node.get_pg(owner.subject.object_pg.get())
                        .unwrap()
                        .conn
                        .total_changes(),
                    changes
                );
                assert_eq!(custody(f, owner), *owner);
            });
        }
    }
}

impl Writer<'_> {
    fn reject_handoff(self) -> Self {
        match self {
            Self::Direct(writer) => {
                let before = writer.test_binding_state();
                let failure = match writer.into_acknowledged() {
                    Ok(_) => panic!("unconfirmed files handed off"),
                    Err(failure) => failure,
                };
                assert!(matches!(failure.error(), E::Window(message)
                    if message == "staging acknowledgement persistence is not confirmed"));
                let writer = failure.into_writer();
                assert_eq!(writer.test_binding_state(), before);
                Self::Direct(Box::new(writer))
            }
            Self::Stream(writer) => {
                let before = writer.test_binding_state();
                let failure = match writer.into_acknowledged() {
                    Ok(_) => panic!("unconfirmed files handed off"),
                    Err(failure) => failure,
                };
                assert!(matches!(failure.error(), E::Window(message)
                    if message == "staging acknowledgement persistence is not confirmed"));
                let writer = failure.into_writer();
                assert_eq!(writer.test_binding_state(), before);
                Self::Stream(Box::new(writer))
            }
        }
    }

    fn finish(self) {
        match self {
            Self::Direct(writer) => {
                let before = writer.test_binding_state();
                let id = writer.id();
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(ready.test_binding_state(), before);
                assert_eq!(ready.id(), id);
                drop(ready);
            }
            Self::Stream(writer) => {
                let before = writer.test_binding_state();
                let id = writer.id();
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(ready.test_binding_state(), before);
                assert_eq!(ready.id(), id);
                drop(ready);
            }
        }
    }

    fn persist_on(&self, runtime: &crate::node::LocalNodeRuntime) -> Result<(), E> {
        match self {
            Self::Direct(writer) => writer.test_persist_acknowledgements_on_runtime(runtime),
            Self::Stream(writer) => writer.test_persist_acknowledgements_on_runtime(runtime),
        }
    }
    fn write(&mut self, index: u8, bytes: &[u8]) -> WriteAck {
        match self {
            Self::Direct(writer) => writer.write_embedded(index, 4, bytes),
            Self::Stream(writer) => writer.write_embedded(index, bytes),
        }
        .unwrap()
    }
    fn persist(&mut self) -> Result<(), E> {
        match self {
            Self::Direct(writer) => writer.persist_acknowledgements_embedded(),
            Self::Stream(writer) => writer.persist_acknowledgements_embedded(),
        }
    }
}

fn with_writer(
    target: StagingTarget,
    action: impl FnOnce(&Setup, &StagingOwnerRecord, Writer<'_>),
) {
    with_writer_and_section(target, false, |f, owner, writer, _| {
        action(f, owner, writer)
    });
}

fn with_writer_and_section(
    target: StagingTarget,
    appended: bool,
    action: impl FnOnce(
        &Setup,
        &StagingOwnerRecord,
        Writer<'_>,
        &crate::cluster::EmbeddedPublicationSection<'_>,
    ),
) {
    with_writer_and_section_duration(target, appended, Duration::from_secs(5), action);
}

fn with_writer_and_section_duration(
    target: StagingTarget,
    appended: bool,
    duration: Duration,
    action: impl FnOnce(
        &Setup,
        &StagingOwnerRecord,
        Writer<'_>,
        &crate::cluster::EmbeddedPublicationSection<'_>,
    ),
) {
    let f = allocation::setup_live(target.clone(), appended);
    allocation::with_confirmed_duration(&f, "installed", 1, duration, |section, mut windows| {
        let window = windows.pop().unwrap();
        let (owner, writer) = if target == StagingTarget::DirectPut {
            (
                files::load_owner(&f),
                Writer::Direct(Box::new(
                    window
                        .into_direct_file_writer_embedded(
                            section,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                        .unwrap_or_else(|e| panic!("{}", e.error())),
                )),
            )
        } else {
            let (owner, mut request, expected) = allocation::inputs(&f, target);
            let shards = files::shards(&owner, b"data");
            let tail = owner.windows.last().unwrap();
            let mut stored = shards[..usize::from(tail.geometry.ec.k)].concat();
            stored.truncate(tail.geometry.codec.stored_size(4).unwrap() as usize);
            request.segment_crc64 = checksum::crc64::checksum(&stored);
            request.payload_crc64 = checksum::crc64::checksum(b"data");
            let allocated = window
                .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                .unwrap_or_else(|e| panic!("{}", e.error()));
            (
                owner,
                Writer::Stream(Box::new(
                    allocated
                        .into_stream_file_writer_embedded(section)
                        .unwrap_or_else(|e| panic!("{}", e.error())),
                )),
            )
        };
        action(&f, &owner, writer, section);
    });
}

fn keys(owner: &StagingOwnerRecord) -> Vec<ShardKey> {
    let window = &owner.windows[0];
    (0..window.participants.files.len())
        .map(|index| ShardKey::new(&window.segment_hash, window.start.get(), index as u8))
        .collect()
}

fn custody(f: &Setup, owner: &StagingOwnerRecord) -> StagingOwnerRecord {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    let current = append::load(&pg, owner);
    snapshot.finish().unwrap();
    current
}

#[test]
fn staging_acknowledgements_require_every_actual_file_receipt_and_retry_exactly() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        with_writer(target, |f, owner, mut writer| {
            let shards = files::shards(owner, b"data");
            let p = &owner.windows[0].participants;
            // Primary differs from shard-zero's physical owner in this fixture.
            assert_ne!(p.acknowledgement_primary, p.files[0]);
            let node = f.map.node(p.acknowledgement_primary).unwrap().test_node();
            let keys = keys(owner);
            assert!(writer
                .persist()
                .unwrap_err()
                .to_string()
                .contains("no staging file receipts"));
            let mut acks = Vec::new();
            for (index, bytes) in shards.iter().enumerate() {
                writer = writer.reject_handoff();
                acks.push(writer.write(index as u8, bytes));
                if index + 1 < shards.len() {
                    assert!(writer
                        .persist()
                        .unwrap_err()
                        .to_string()
                        .contains("incomplete staging acknowledgement set"));
                    let pg = node.get_pg(p.data_pg.get()).unwrap();
                    for key in &keys {
                        assert!(matches!(pg.stat_shard(key), Err(StoreError::NotFound)));
                    }
                }
            }
            writer = writer.reject_handoff();
            writer.persist().unwrap();
            let changes = node.get_pg(p.data_pg.get()).unwrap().conn.total_changes();
            // Re-observing the same file must not discard complete batch evidence.
            assert_eq!(writer.write(0, &shards[0]), acks[0]);
            writer.persist().unwrap();
            assert_eq!(
                node.get_pg(p.data_pg.get()).unwrap().conn.total_changes(),
                changes
            );
            for member in 0..5 {
                let node = f.map.node(NodeId::new(member)).unwrap().test_node();
                let pg = node.get_pg(p.data_pg.get()).unwrap();
                for (key, ack) in keys.iter().zip(&acks) {
                    if NodeId::new(member) == p.acknowledgement_primary {
                        pg.validate_written_shard_ack(key, *ack).unwrap();
                    } else {
                        assert!(matches!(pg.stat_shard(key), Err(StoreError::NotFound)));
                    }
                }
            }
            writer.finish();
            assert_eq!(custody(f, owner), *owner);
        });
    }
}

#[test]
fn staging_acknowledgements_rollback_a_later_conflict_without_losing_custody() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        with_writer(target, |f, owner, mut writer| {
            let shards = files::shards(owner, b"data");
            let acks: Vec<_> = shards
                .iter()
                .enumerate()
                .map(|(i, bytes)| writer.write(i as u8, bytes))
                .collect();
            let keys = keys(owner);
            let p = &owner.windows[0].participants;
            let node = f.map.node(p.acknowledgement_primary).unwrap().test_node();
            let bad = WriteAck {
                crc64: acks.last().unwrap().crc64 ^ 1,
                ..*acks.last().unwrap()
            };
            node.get_pg(p.data_pg.get())
                .unwrap()
                .register_written_shard(keys.last().unwrap(), bad)
                .unwrap();
            assert!(matches!(
                writer.persist(),
                Err(E::Store(StoreError::ShardAckMismatch { .. }))
            ));
            writer = writer.reject_handoff();
            {
                let pg = node.get_pg(p.data_pg.get()).unwrap();
                assert!(pg.conn.is_autocommit());
                for key in &keys[..keys.len() - 1] {
                    assert!(matches!(pg.stat_shard(key), Err(StoreError::NotFound)));
                }
                pg.validate_written_shard_ack(keys.last().unwrap(), bad)
                    .unwrap();
                pg.delete_shard_record(keys.last().unwrap()).unwrap();
            }
            assert_eq!(custody(f, owner), *owner);
            writer.persist().unwrap();
            let pg = node.get_pg(p.data_pg.get()).unwrap();
            for (key, ack) in keys.iter().zip(acks) {
                pg.validate_written_shard_ack(key, ack).unwrap();
            }
            drop(pg);
            writer.finish();
            assert_eq!(custody(f, owner), *owner);
        });
    }
}

#[test]
fn staging_acknowledgements_recheck_shorter_original_cutoff_beside_insert() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let now = crate::clock::current_time_millis();
        let clock = crate::clock::test_time_override_guard(now);
        with_writer(target, |f, owner, mut writer| {
            let shards = files::shards(owner, b"data");
            for (index, bytes) in shards.iter().enumerate() {
                writer.write(index as u8, bytes);
            }
            let p = &owner.windows[0].participants;
            let node = f.map.node(p.acknowledgement_primary).unwrap().test_node();
            let changes = node.get_pg(p.data_pg.get()).unwrap().conn.total_changes();
            let control = clock.control();
            let result = PgStore::test_with_before_shard_ack_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction);
                    control.set(now + 6_000);
                },
                || writer.persist(),
            );
            assert!(now + 6_000 < owner.maximum_cutoff_ms);
            assert!(matches!(
                result,
                Err(E::Store(StoreError::RouteMapExpired { .. }))
            ));
            let pg = node.get_pg(p.data_pg.get()).unwrap();
            assert!(pg.conn.is_autocommit());
            assert_eq!(pg.conn.total_changes(), changes);
            for key in keys(owner) {
                assert!(matches!(pg.stat_shard(&key), Err(StoreError::NotFound)));
            }
            drop(pg);
            drop(writer.reject_handoff());
            assert_eq!(custody(f, owner), *owner);
        });
    }
}

#[test]
fn staging_acknowledgements_pin_direct_logical_size_before_file_dispatch() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    with_writer(StagingTarget::DirectPut, |f, owner, mut writer| {
        let shards = files::shards(owner, b"data");
        let Writer::Direct(direct) = &mut writer else {
            unreachable!()
        };
        direct.write_embedded(0, 4, &shards[0]).unwrap();
        // Adjacent logical lengths can share the padded shard length. Find one
        // so this is not merely rejected by ordinary geometry validation.
        let geometry = &owner.windows[0].geometry;
        let crossed = (1..16)
            .find(|size| {
                *size != 4
                    && geometry
                        .codec
                        .stored_size(*size)
                        .unwrap()
                        .div_ceil(u64::from(geometry.ec.k))
                        == shards[1].len() as u64
            })
            .unwrap();
        let error = direct.write_embedded(1, crossed, &shards[1]).unwrap_err();
        assert!(error.to_string().contains("logical size changed"));
        assert!(matches!(
            f.map
                .node(owner.windows[0].participants.files[1])
                .unwrap()
                .test_node()
                .read_shard_file(owner.windows[0].participants.data_pg.get(), &keys(owner)[1]),
            Err(StoreError::NotFound)
        ));
        for (index, bytes) in shards.iter().enumerate().skip(1) {
            direct.write_embedded(index as u8, 4, bytes).unwrap();
        }
        direct.persist_acknowledgements_embedded().unwrap();
    });
}

#[test]
fn staging_acknowledgements_reject_foreign_runtime_and_nonprimary_without_rows() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let foreign = allocation::setup_live(target.clone(), false);
        with_writer(target, |f, owner, mut writer| {
            let shards = files::shards(owner, b"data");
            for (index, bytes) in shards.iter().enumerate() {
                writer.write(index as u8, bytes);
            }
            let p = &owner.windows[0].participants;
            let foreign_admission = foreign.handle.admit_current_route().unwrap();
            let foreign_route = foreign_admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let foreign_runtime = foreign_route
                .embedded_staging_ack_runtime(
                    p.data_pg,
                    p.acknowledgement_primary,
                    p.placement_epoch,
                )
                .unwrap();
            assert!(writer
                .persist_on(foreign_runtime)
                .unwrap_err()
                .to_string()
                .contains("runtime domain"));
            let admission = f.handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let nonprimary = p
                .files
                .iter()
                .copied()
                .find(|node| *node != p.acknowledgement_primary)
                .unwrap();
            assert!(route
                .embedded_staging_ack_runtime(p.data_pg, nonprimary, p.placement_epoch)
                .is_err());
            let index = p.files.iter().position(|node| *node == nonprimary).unwrap() as u8;
            let location = crate::cluster::ShardLocation::new(
                p.placement_epoch,
                crate::DataPgId::from_validated_placement(p.data_pg),
                crate::ShardIndex::new(index),
                nonprimary,
            );
            let nonprimary_runtime = route.embedded_staging_file_runtime(&location).unwrap();
            assert!(writer
                .persist_on(nonprimary_runtime)
                .unwrap_err()
                .to_string()
                .contains("runtime domain"));
            for map in [&f.map, &foreign.map] {
                for node_id in [nonprimary, p.acknowledgement_primary] {
                    let node = map.node(node_id).unwrap().test_node();
                    let pg = node.get_pg(p.data_pg.get()).unwrap();
                    for key in keys(owner) {
                        assert!(matches!(pg.stat_shard(&key), Err(StoreError::NotFound)));
                    }
                }
            }
            writer.persist().unwrap();
        });
    }
}

#[test]
fn staging_acknowledgements_hold_pg_exclusion_until_batch_commit() {
    use crate::node_runtime::engine::pg_lock_test_hook::with_contention;
    use std::sync::mpsc;
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        with_writer(target, |f, owner, mut writer| {
            let shards = files::shards(owner, b"data");
            let acks: Vec<_> = shards
                .iter()
                .enumerate()
                .map(|(index, bytes)| writer.write(index as u8, bytes))
                .collect();
            let p = &owner.windows[0].participants;
            let node = Arc::clone(f.map.node(p.acknowledgement_primary).unwrap().test_node());
            let pg_id = p.data_pg.get();
            let keys = keys(owner);
            let (start_tx, start_rx) = mpsc::channel();
            let (blocked_tx, blocked_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let deleting = std::thread::spawn(move || {
                start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                let expected = keys.clone();
                with_contention(
                    move |pg| {
                        assert_eq!(pg, pg_id);
                        blocked_tx.send(()).unwrap();
                    },
                    move |pg_id_seen, pg| {
                        assert_eq!(pg_id_seen, pg_id);
                        assert!(
                            pg.conn.is_autocommit(),
                            "deletion observes a committed batch"
                        );
                        for (key, ack) in expected.iter().zip(acks) {
                            pg.validate_written_shard_ack(key, ack).unwrap();
                        }
                    },
                    || {
                        let pg = node.get_pg(pg_id).unwrap();
                        for key in keys {
                            pg.delete_shard_record(&key).unwrap();
                        }
                    },
                );
                let _ = done_tx.send(());
            });
            PgStore::test_with_before_shard_ack_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction);
                    start_tx.send(()).unwrap();
                    blocked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
                },
                || writer.persist(),
            )
            .unwrap();
            deleting.join().unwrap();
            assert_eq!(custody(f, owner), *owner);
        });
    }
}
