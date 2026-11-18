// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Embedded file effects through complete custody. No acknowledgement-row or
//! terminal-publication claim: ciphertext is EC encoded for real file geometry.
use super::*;
use crate::node_runtime::traits::ShardStore;
use introduction::state;

pub(super) fn load_owner(f: &Setup) -> StagingOwnerRecord {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
    snapshot.finish().unwrap();
    owner
}

pub(super) fn shards(owner: &StagingOwnerRecord, data: &[u8]) -> Vec<Vec<u8>> {
    let encrypted = crate::tests::sse_s3_segment_test_fixture(data, 0);
    let shape = owner.windows[0].geometry.ec;
    assert_eq!(
        encrypted.storage_bytes.len() as u64,
        owner.windows[0]
            .geometry
            .codec
            .stored_size(data.len() as u64)
            .unwrap()
    );
    // Systematic input/padding plus parity, using the same encoder as placement.
    let config = ec::EcConfig::new(shape.k, shape.m).unwrap();
    let encoder = ec::ErasureCodec::new(config).unwrap();
    let len = encrypted.storage_bytes.len().div_ceil(usize::from(shape.k));
    let mut shards = vec![vec![0; len]; usize::from(shape.k + shape.m)];
    for (shard, bytes) in shards.iter_mut().zip(encrypted.storage_bytes.chunks(len)) {
        shard[..bytes.len()].copy_from_slice(bytes);
    }
    let (data, parity) = shards.split_at_mut(usize::from(shape.k));
    encoder
        .encode(
            &data.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            &mut parity.iter_mut().map(Vec::as_mut_slice).collect::<Vec<_>>(),
        )
        .unwrap();
    shards
}

fn key(owner: &StagingOwnerRecord, index: u8) -> crate::ShardKey {
    let window = &owner.windows[0];
    crate::ShardKey::new(&window.segment_hash, window.start.get(), index)
}

fn equal_crc_different_bytes(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() > 8);
    let mut changed = bytes.to_vec();
    changed[8] ^= 1;
    let base = checksum::crc64::checksum(&changed);
    let mut basis = [(0_u64, 0_u64); 64];
    for bit in 0..64 {
        let mut probe = changed.clone();
        probe[bit / 8] ^= 1 << (bit % 8);
        let mut delta = checksum::crc64::checksum(&probe) ^ base;
        let mut patch = 1_u64 << bit;
        while delta != 0 {
            let pivot = 63 - delta.leading_zeros() as usize;
            if basis[pivot].0 == 0 {
                basis[pivot] = (delta, patch);
                break;
            }
            delta ^= basis[pivot].0;
            patch ^= basis[pivot].1;
        }
    }
    assert!(basis.iter().all(|entry| entry.0 != 0));
    let mut delta = checksum::crc64::checksum(bytes) ^ base;
    let mut patch = 0_u64;
    while delta != 0 {
        let pivot = 63 - delta.leading_zeros() as usize;
        delta ^= basis[pivot].0;
        patch ^= basis[pivot].1;
    }
    for (byte, correction) in changed.iter_mut().zip(patch.to_le_bytes()) {
        *byte ^= correction;
    }
    assert_ne!(changed, bytes);
    assert_eq!(
        checksum::crc64::checksum(&changed),
        checksum::crc64::checksum(bytes)
    );
    changed
}

#[test]
fn staging_direct_files_capture_provisional_subject_before_any_write() {
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_count(&f, "installed", 2, |section, mut windows| {
        let owner = load_owner(&f);
        let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
        let expected = crate::ObjectEncryptionSubject::standard_object(
            owner.subject.bucket_incarnation,
            &owner.subject.bucket,
            &owner.subject.key,
            owner.subject.payload_generation,
            &fixture.encryption,
        );
        let node = section.runtime().unwrap().test_node();
        let before = state(&node.get_pg(owner.subject.object_pg.get()).unwrap());
        let writer = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(section, &fixture.encryption)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(writer.encryption_subject(), &expected);
        let committed = fixture.committed_encryption(writer.encryption_subject());
        let failure = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(section, &committed)
            .err()
            .expect("committed state cannot replace the original provisional input");
        assert!(
            matches!(failure.error(), E::Window(message) if message == "direct provisional encryption binding")
        );
        assert_eq!(failure.id(), f.context.id());
        drop(failure);
        drop(writer);
        assert_eq!(
            state(&node.get_pg(owner.subject.object_pg.get()).unwrap()),
            before
        );
        assert_eq!(load_owner(&f), owner);
    });
}

#[test]
fn staging_direct_files_reuse_reserved_vid_and_reject_different_bytes() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for history in ["installed", "inspected", "uncertain"] {
        let f = allocation::setup_live_with_floor(StagingTarget::DirectPut, false, Some(8));
        allocation::with_confirmed_count(&f, history, 2, |section, mut windows| {
            let owner = load_owner(&f);
            assert_eq!(owner.subject.payload_generation.get(), 9);
            assert_eq!(owner.windows[0].start, owner.subject.payload_generation);
            let payload = b"direct file gate ciphertext";
            let shards = shards(&owner, payload);
            let before = state(
                &section
                    .runtime()
                    .unwrap()
                    .test_node()
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap(),
            );
            let mut first = windows
                .pop()
                .unwrap()
                .into_direct_file_writer_embedded(
                    section,
                    &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                )
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let mut second = windows
                .pop()
                .unwrap()
                .into_direct_file_writer_embedded(
                    section,
                    &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                )
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(first.id(), f.context.id());
            for (index, bytes) in shards.iter().enumerate() {
                let index = index as u8;
                let ack = first
                    .write_embedded(index, payload.len() as u64, bytes)
                    .unwrap();
                assert_eq!(ack.stored_size, bytes.len() as u64);
                assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
                assert_eq!(
                    second
                        .write_embedded(index, payload.len() as u64, bytes)
                        .unwrap(),
                    ack
                );
                let mut different = bytes.clone();
                different[0] ^= 1;
                assert!(matches!(
                    second.write_embedded(index, payload.len() as u64, &different),
                    Err(E::Store(crate::StoreError::ShardAckMismatch { .. }))
                ));
                let colliding = equal_crc_different_bytes(bytes);
                assert!(matches!(
                    second.write_embedded(index, payload.len() as u64, &colliding),
                    Err(E::Store(crate::StoreError::PayloadShardSetMismatch { .. }))
                ));
                let node = f
                    .map
                    .node(owner.windows[0].participants.files[usize::from(index)])
                    .unwrap()
                    .test_node();
                let pg_id = owner.windows[0].participants.data_pg.get();
                assert_eq!(
                    node.read_shard_file(pg_id, &key(&owner, index)).unwrap(),
                    *bytes
                );
                assert!(matches!(
                    node.get_pg(pg_id).unwrap().stat_shard(&key(&owner, index)),
                    Err(crate::StoreError::NotFound)
                ));
            }
            drop(first);
            drop(second);
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            assert_eq!(
                state(&node.get_pg(f.context.id().pg_id().get()).unwrap()),
                before
            );
        });
    }
}

#[test]
fn staging_direct_files_reject_geometry_and_foreign_runtime_without_effects() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
        let owner = load_owner(&f);
        let shards = shards(&owner, b"hello");
        let mut writer = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(
                section,
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|e| panic!("{}", e.error()));
        for (index, size, bytes) in [
            (255, 5, shards[0].as_slice()),
            (0, u64::MAX, shards[0].as_slice()),
            (0, 5, &shards[0][1..]),
        ] {
            assert!(matches!(
                writer.write_embedded(index, size, bytes),
                Err(E::Window(_))
            ));
        }
        let foreign = allocation::setup_live(StagingTarget::DirectPut, false);
        let admission = foreign.handle.admit_current_route().unwrap();
        let route = admission
            .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
            .unwrap();
        let participants = &owner.windows[0].participants;
        let location = crate::cluster::ShardLocation::new(
            participants.placement_epoch,
            crate::DataPgId::from_validated_placement(participants.data_pg),
            crate::ShardIndex::new(0),
            participants.files[0],
        );
        // Identical node ID, PG, epoch and topology; only the runtime domain differs.
        assert!(matches!(
            route
                .embedded_staging_file_runtime(&location)
                .unwrap()
                .write_direct_staging_file(&writer, 0, 5, &shards[0]),
            Err(E::Window(_))
        ));
        for index in 0..shards.len() {
            let node = f
                .map
                .node(owner.windows[0].participants.files[index])
                .unwrap()
                .test_node();
            assert!(matches!(
                node.read_shard_file(
                    owner.windows[0].participants.data_pg.get(),
                    &key(&owner, index as u8)
                ),
                Err(crate::StoreError::NotFound)
            ));
        }
    });
}

#[test]
fn staging_direct_files_expire_before_final_publication_and_preserve_owner() {
    let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
        let owner = load_owner(&f);
        let shards = shards(&owner, b"hello");
        let mut writer = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(
                section,
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let control = clock.control();
        let cutoff = owner.maximum_cutoff_ms;
        let error = PgStore::test_with_before_shard_publication_hook(
            move || control.set(cutoff),
            || writer.write_embedded(0, 5, &shards[0]),
        )
        .unwrap_err();
        assert!(
            matches!(error, E::Store(crate::StoreError::RouteMapExpired { .. })),
            "{error:?}"
        );
        let node = f
            .map
            .node(owner.windows[0].participants.files[0])
            .unwrap()
            .test_node();
        assert!(matches!(
            node.read_shard_file(owner.windows[0].participants.data_pg.get(), &key(&owner, 0)),
            Err(crate::StoreError::NotFound)
        ));
        drop(writer);
        assert_eq!(load_owner(&f), owner);
    });
}

#[test]
fn staging_direct_files_keep_exclusion_through_acknowledgement_production() {
    use crate::node_runtime::pg_store::shard_file_mutation_test_hook::{with_hook, Point};
    use std::sync::mpsc;
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
        let owner = load_owner(&f);
        let shards = shards(&owner, b"hello");
        let mut writer = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(
                section,
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let node = Arc::clone(
            f.map
                .node(owner.windows[0].participants.files[0])
                .unwrap()
                .test_node(),
        );
        let pg = owner.windows[0].participants.data_pg.get();
        let key = key(&owner, 0);
        let (start_tx, start_rx) = mpsc::channel();
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let deleting = std::thread::spawn(move || {
            start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            with_hook(
                move |point, _| {
                    if point == Point::DeleteWaiting {
                        waiting_tx.send(()).unwrap();
                    }
                },
                || node.delete_shard_file(pg, &key).unwrap(),
            );
            // The hook's receiver exists only until acknowledgement production;
            // joining below confirms successful deletion after that interval.
            let _ = done_tx.send(());
        });
        let ack = PgStore::test_with_after_shard_publication_hook(
            move || {
                start_tx.send(()).unwrap();
                waiting_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            },
            || writer.write_embedded(0, 5, &shards[0]),
        )
        .unwrap();
        assert_eq!(ack.crc64, checksum::crc64::checksum(&shards[0]));
        deleting.join().unwrap();
        assert_eq!(load_owner(&f), owner);
    });
}

#[test]
fn staging_direct_files_recheck_cutoff_after_waiting_for_exclusion() {
    use crate::node_runtime::pg_store::shard_file_mutation_test_hook::{with_hook, Point};
    use std::sync::mpsc;
    let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
        let owner = load_owner(&f);
        let shards = shards(&owner, b"hello");
        let mut writer = windows
            .pop()
            .unwrap()
            .into_direct_file_writer_embedded(
                section,
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let node = Arc::clone(
            f.map
                .node(owner.windows[0].participants.files[0])
                .unwrap()
                .test_node(),
        );
        let deleting_node = Arc::clone(&node);
        let pg = owner.windows[0].participants.data_pg.get();
        let key = key(&owner, 0);
        let deleting_key = key.clone();
        let (held_tx, held_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let deleting = std::thread::spawn(move || {
            with_hook(
                move |point, _| {
                    if point == Point::DeleteAcquired {
                        held_tx.send(()).unwrap();
                        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    }
                },
                || deleting_node.delete_shard_file(pg, &deleting_key).unwrap(),
            );
        });
        held_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let control = clock.control();
        let cutoff = owner.maximum_cutoff_ms;
        let result = with_hook(
            move |point, _| {
                if point == Point::PublishWaiting {
                    control.set(cutoff);
                    resume_tx.send(()).unwrap();
                }
            },
            || writer.write_embedded(0, 5, &shards[0]),
        );
        deleting.join().unwrap();
        assert!(matches!(
            result,
            Err(E::Store(crate::StoreError::RouteMapExpired { .. }))
        ));
        assert!(matches!(
            node.read_shard_file(pg, &key),
            Err(crate::StoreError::NotFound)
        ));
        assert_eq!(load_owner(&f), owner);
    });
}

#[test]
fn staging_direct_files_reject_stream_authority_and_lost_allocation_lifetime() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let f = allocation::setup_live(target.clone(), false);
        allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
            if matches!(target, StagingTarget::DirectPut) {
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                drop(pg.begin_allocation_lifetime_replacement().unwrap());
            }
            let before = state(
                &f.map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap(),
            );
            let failure = windows
                .pop()
                .unwrap()
                .into_direct_file_writer_embedded(
                    section,
                    &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                )
                .err()
                .expect("wrong target/lifetime must not bind");
            assert_eq!(failure.id(), f.context.id());
            drop(failure);
            assert_eq!(
                state(
                    &f.map
                        .node(NodeId::new(4))
                        .unwrap()
                        .test_node()
                        .get_pg(f.context.id().pg_id().get())
                        .unwrap()
                ),
                before
            );
        });
    }
}

#[test]
fn staging_direct_files_keep_shorter_confirmation_deadline_inside_effect() {
    let now = crate::clock::current_time_millis();
    let clock = crate::clock::test_time_override_guard(now);
    let f = allocation::setup_live(StagingTarget::DirectPut, false);
    allocation::with_confirmed_duration(
        &f,
        "installed",
        1,
        Duration::from_secs(5),
        |section, mut windows| {
            let owner = load_owner(&f);
            let shards = shards(&owner, b"hello");
            let mut writer = windows
                .pop()
                .unwrap()
                .into_direct_file_writer_embedded(
                    section,
                    &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                )
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert!(now + 6_000 < owner.maximum_cutoff_ms);
            let control = clock.control();
            let result = PgStore::test_with_before_shard_publication_hook(
                move || control.set(now + 6_000),
                || writer.write_embedded(0, 5, &shards[0]),
            );
            assert!(matches!(
                result,
                Err(E::Store(crate::StoreError::RouteMapExpired { .. }))
            ));
            section.require_effect().unwrap();
            let node = f
                .map
                .node(owner.windows[0].participants.files[0])
                .unwrap()
                .test_node();
            assert!(matches!(
                node.read_shard_file(owner.windows[0].participants.data_pg.get(), &key(&owner, 0)),
                Err(crate::StoreError::NotFound)
            ));
            assert_eq!(load_owner(&f), owner);
        },
    );
}
