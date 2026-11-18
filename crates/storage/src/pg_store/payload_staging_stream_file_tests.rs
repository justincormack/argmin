// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate physical gate, using real encrypted EC bytes. This does not bind
//! the frontend encryption subject or publish acknowledgement metadata yet.
use super::*;
use crate::node_runtime::traits::ShardStore;

fn observe(
    f: &Setup,
) -> Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock> {
    introduction::state(
        &f.map
            .node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(f.context.id().pg_id().get())
            .unwrap(),
    )
}

fn file_key(owner: &StagingOwnerRecord, index: u8) -> crate::ShardKey {
    let window = owner.windows.last().unwrap();
    crate::ShardKey::new(&window.segment_hash, window.start.get(), index)
}

#[test]
fn staging_stream_files_consume_allocation_for_both_targets_and_windows() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["installed", "inspected", "uncertain"] {
                let f = allocation::setup_live(target.clone(), appended);
                allocation::with_confirmed_count(&f, history, 2, |section, mut windows| {
                    let (owner, mut request, expected) = allocation::inputs(&f, target.clone());
                    let shards = files::shards(&owner, b"data");
                    let window = owner.windows.last().unwrap();
                    let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
                    stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
                    request.segment_crc64 = checksum::crc64::checksum(&stored);
                    request.payload_crc64 = checksum::crc64::checksum(b"data");
                    let allocated = windows
                        .pop()
                        .unwrap()
                        .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(allocated.segment().segment_vid, window.start);
                    assert_eq!(allocated.segment().segment_index, u32::from(appended));
                    let mut writer = allocated
                        .into_stream_file_writer_embedded(section)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let duplicate = windows
                        .pop()
                        .unwrap()
                        .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                        .err()
                        .expect("cursor cannot be consumed twice");
                    assert_eq!(duplicate.id(), f.context.id());
                    let before = observe(&f);
                    assert_eq!(writer.id(), f.context.id());
                    for (index, bytes) in shards.iter().enumerate() {
                        let index = index as u8;
                        let ack = writer.write_embedded(index, bytes).unwrap();
                        assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
                        assert_eq!(writer.write_embedded(index, bytes).unwrap(), ack);
                        let mut changed = bytes.clone();
                        changed[0] ^= 1;
                        assert!(matches!(
                            writer.write_embedded(index, &changed),
                            Err(E::Store(crate::StoreError::ShardAckMismatch { .. }))
                        ));
                        let node = f
                            .map
                            .node(window.participants.files[usize::from(index)])
                            .unwrap()
                            .test_node();
                        let key = file_key(&owner, index);
                        assert_eq!(
                            node.read_shard_file(window.participants.data_pg.get(), &key)
                                .unwrap(),
                            *bytes
                        );
                        assert!(matches!(
                            node.get_pg(window.participants.data_pg.get())
                                .unwrap()
                                .stat_shard(&key),
                            Err(crate::StoreError::NotFound)
                        ));
                    }
                    drop(writer);
                    drop(duplicate);
                    assert_eq!(
                        observe(&f),
                        before,
                        "file attempts/Drop never change metadata custody"
                    );
                });
            }
        }
    }
}

#[test]
fn staging_stream_files_reject_geometry_and_crossed_runtime() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let f = allocation::setup_live(target.clone(), false);
        let foreign = allocation::setup_live(target.clone(), false);
        allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
            let (owner, request, expected) = allocation::inputs(&f, target.clone());
            let shards = files::shards(&owner, b"data");
            let mut writer = windows
                .pop()
                .unwrap()
                .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                .unwrap_or_else(|e| panic!("{}", e.error()))
                .into_stream_file_writer_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let before = observe(&f);
            for (index, bytes) in [(255, shards[0].as_slice()), (0, &shards[0][1..])] {
                assert!(matches!(
                    writer.write_embedded(index, bytes),
                    Err(E::Window(_))
                ));
            }
            let admission = foreign.handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let p = &owner.windows[0].participants;
            let location = crate::cluster::ShardLocation::new(
                p.placement_epoch,
                crate::DataPgId::from_validated_placement(p.data_pg),
                crate::ShardIndex::new(0),
                p.files[0],
            );
            let error = route
                .embedded_staging_file_runtime(&location)
                .unwrap()
                .write_stream_staging_file(&writer, 0, &shards[0])
                .unwrap_err();
            assert!(error.to_string().contains("stream file runtime domain"));
            for map in [&f.map, &foreign.map] {
                assert!(matches!(
                    map.node(p.files[0])
                        .unwrap()
                        .test_node()
                        .read_shard_file(p.data_pg.get(), &file_key(&owner, 0)),
                    Err(crate::StoreError::NotFound)
                ));
            }
            assert_eq!(observe(&f), before);
        });
    }
}

#[test]
fn staging_stream_files_recheck_authority_and_consumed_cursor_before_binding() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for change in ["cursor", "subject", "issuer"] {
            let _clock =
                crate::clock::test_time_override_guard(crate::clock::current_time_millis());
            let f = allocation::setup_live(target.clone(), false);
            allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
                let (owner, request, expected) = allocation::inputs(&f, target.clone());
                let allocated = windows
                    .pop()
                    .unwrap()
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                {
                    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    match change {
                        "cursor" => {
                            pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2", rusqlite::params![owner.windows[0].start.get(), owner.subject.id.attempt]).unwrap();
                            // Cursor is noncanonical local state. Explicitly
                            // validate the unchanged command prefix so the
                            // binding must reject the cursor itself.
                            pg.recover_complete_command_state(
                                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                                &topology(),
                            )
                            .unwrap();
                        }
                        "subject" => {
                            pg.conn.execute("UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1", [&owner.subject.id.attempt]).unwrap();
                        }
                        "issuer" => {
                            drop(pg.begin_allocation_lifetime_replacement().unwrap());
                        }
                        _ => unreachable!(),
                    }
                }
                let before = observe(&f);
                let error = allocated
                    .into_stream_file_writer_embedded(section)
                    .err()
                    .expect("changed authority must fail binding");
                assert_eq!(error.id(), f.context.id());
                if change == "cursor" {
                    assert!(
                        error.error().to_string().contains("consumed cursor"),
                        "{}",
                        error.error()
                    );
                }
                drop(error);
                assert_eq!(observe(&f), before);
            });
        }
    }
}

#[test]
fn staging_stream_files_enforce_original_shorter_cutoff_at_publication() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let now = crate::clock::current_time_millis();
        let clock = crate::clock::test_time_override_guard(now);
        let f = allocation::setup_live(target.clone(), false);
        allocation::with_confirmed_duration(
            &f,
            "installed",
            1,
            Duration::from_secs(5),
            |section, mut windows| {
                let (owner, request, expected) = allocation::inputs(&f, target.clone());
                let shards = files::shards(&owner, b"data");
                let mut writer = windows
                    .pop()
                    .unwrap()
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .unwrap_or_else(|e| panic!("{}", e.error()))
                    .into_stream_file_writer_embedded(section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let before = observe(&f);
                let control = clock.control();
                let result = PgStore::test_with_before_shard_publication_hook(
                    move || control.set(now + 6_000),
                    || writer.write_embedded(0, &shards[0]),
                );
                assert!(now + 6_000 < owner.maximum_cutoff_ms);
                assert!(matches!(
                    result,
                    Err(E::Store(crate::StoreError::RouteMapExpired { .. }))
                ));
                section.require_effect().unwrap();
                let p = &owner.windows[0].participants;
                assert!(matches!(
                    f.map
                        .node(p.files[0])
                        .unwrap()
                        .test_node()
                        .read_shard_file(p.data_pg.get(), &file_key(&owner, 0)),
                    Err(crate::StoreError::NotFound)
                ));
                assert_eq!(observe(&f), before);
            },
        );
    }
}

#[test]
fn staging_stream_files_recheck_expiry_inside_shared_file_exclusion() {
    use crate::node_runtime::pg_store::shard_file_mutation_test_hook::{with_hook, Point};
    use std::sync::mpsc;
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let f = allocation::setup_live(target.clone(), true);
        allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
            let (owner, request, expected) = allocation::inputs(&f, target.clone());
            let shards = files::shards(&owner, b"data");
            let mut writer = windows
                .pop()
                .unwrap()
                .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                .unwrap_or_else(|e| panic!("{}", e.error()))
                .into_stream_file_writer_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let before = observe(&f);
            let p = &owner.windows.last().unwrap().participants;
            let node = Arc::clone(f.map.node(p.files[0]).unwrap().test_node());
            let deleting_node = Arc::clone(&node);
            let pg = p.data_pg.get();
            let key = file_key(&owner, 0);
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
            let result = with_hook(
                move |point, _| {
                    if point == Point::PublishWaiting {
                        control.set(owner.maximum_cutoff_ms);
                        resume_tx.send(()).unwrap();
                    }
                },
                || writer.write_embedded(0, &shards[0]),
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
            assert_eq!(observe(&f), before);
        });
    }
}

#[test]
fn staging_stream_files_hold_exclusion_through_acknowledgement() {
    use crate::node_runtime::pg_store::shard_file_mutation_test_hook::{with_hook, Point};
    use std::sync::mpsc;
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let f = allocation::setup_live(target.clone(), false);
        allocation::with_confirmed_count(&f, "installed", 1, |section, mut windows| {
            let (owner, request, expected) = allocation::inputs(&f, target.clone());
            let shards = files::shards(&owner, b"data");
            let mut writer = windows
                .pop()
                .unwrap()
                .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                .unwrap_or_else(|e| panic!("{}", e.error()))
                .into_stream_file_writer_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let before = observe(&f);
            let p = &owner.windows[0].participants;
            let node = Arc::clone(f.map.node(p.files[0]).unwrap().test_node());
            let pg = p.data_pg.get();
            let key = file_key(&owner, 0);
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
                let _ = done_tx.send(());
            });
            let ack = PgStore::test_with_after_shard_publication_hook(
                move || {
                    start_tx.send(()).unwrap();
                    waiting_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
                },
                || writer.write_embedded(0, &shards[0]),
            )
            .unwrap();
            assert_eq!(ack.crc64, checksum::crc64::checksum(&shards[0]));
            deleting.join().unwrap();
            assert_eq!(observe(&f), before);
        });
    }
}
