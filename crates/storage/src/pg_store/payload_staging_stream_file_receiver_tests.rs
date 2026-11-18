// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate stream receiver: real allocation, release, peer inspection and
//! ciphertext file effects. Frontend encryption binding/live selection are gated.
use super::*;
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{
    self as rpc, complete_stream_allocation as allocation, complete_stream_file_primary as wire,
};

thread_local! {
    // Expire only the file effect, not the authenticated response envelope.
    static FILE_EFFECT_CLOCK: std::cell::RefCell<Option<crate::clock::TestTimeAndMonotonicOverrideGuard>> = const {
        std::cell::RefCell::new(None)
    };
}

struct Receiver<'a> {
    f: &'a Mixed,
    server: std::sync::Arc<StorageNodeServer>,
    client: crate::node_client::UnixStorageNodeClient,
    owner: StagingOwnerRecord,
    inspection: Vec<u8>,
    shards: Vec<Vec<u8>>,
}

fn with_receiver(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    action: impl FnOnce(&Receiver<'_>),
) {
    let (_dir, f, proof) = allocation_remote::setup(tls, target.clone(), appended);
    run_terminal(
        &f,
        proof.as_ref(),
        "installed",
        |f, primary, mut terminal| {
            primary.retire(&mut terminal);
            let confirmed = primary
                .confirm(terminal, deadline(), |_| {})
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let (owner, mut request, expected) = allocation_remote::inputs(f, target);
            let shards = super::super::files::shards(&owner, b"data");
            let window = owner.windows.last().unwrap();
            let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
            stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
            request.segment_crc64 = checksum::crc64::checksum(&stored);
            request.payload_crc64 = checksum::crc64::checksum(b"data");
            let Primary::Remote(client, session) = primary else {
                unreachable!()
            };
            let original = confirmed.test_stream_allocation_bytes(&request, &expected);
            let allocated = confirmed
                .allocate_stream_remote(client, session, &request, &expected)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let inspection = wire::encode_request(&original, allocated.segment()).unwrap();
            let writer = allocated
                .into_stream_file_writer_remote(client, session)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            f.remote(NodeId::new(4))
                .unwrap()
                .server
                .test_require_window_primary_released(f.context.id().pg_id());
            let receiver_dir = test_util::tempdir();
            let node = f.node(NodeId::new(4));
            let (server, client) = super::super::super::application_sender::replica_from_pg(
                tls,
                &node.get_pg(f.context.id().pg_id().get()).unwrap(),
                &config(1),
                &receiver_dir,
                window.participants.files[0],
            );
            let receiver = Receiver {
                f,
                server: std::sync::Arc::new(server),
                client,
                owner,
                inspection,
                shards,
            };
            action(&receiver);
            drop(writer); // Still owns allocation/history; no cleanup on Drop.
        },
    );
}

impl Receiver<'_> {
    fn install_rpc_peers(&self) {
        self.server.test_install_window_receiver_peers(
            self.f
                .remote
                .iter()
                .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::super::complete_log_rpc::window_receiver_auth_for(
                self.f.context.id().cluster_epoch(),
                self.owner.windows.last().unwrap().participants.files[0].as_u32(),
            ),
        );
    }

    fn rpc_bytes(&self, cutoff: Option<u64>) -> rpc::complete_staging_file::EncodedRequest {
        let files = wire::decode_request(&self.inspection).unwrap();
        let mut receiver = allocation::decode_request(files.allocation)
            .unwrap()
            .window
            .route;
        receiver.node = self.owner.windows.last().unwrap().participants.files[0];
        if let Some(cutoff) = cutoff {
            receiver.deadline.portable_wall_valid_until_ms = cutoff;
        }
        rpc::complete_staging_file::encode_request(rpc::complete_staging_file::Request {
            receiver,
            provenance: rpc::complete_staging_file::Provenance::Stream {
                inspection: &self.inspection,
            },
            shard_index: 0,
            bytes: &self.shards[0],
        })
        .unwrap()
    }

    fn write(
        &self,
        inspection: &[u8],
        index: u8,
        bytes: &[u8],
        hook: impl FnMut(&'static str),
    ) -> Result<crate::WriteAck, rpc::StorageRpcErrorResponse> {
        let node = self.owner.windows.last().unwrap().participants.files[0];
        self.server.test_write_stream_file_receiver(
            self.f
                .remote
                .iter()
                .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::super::complete_log_rpc::window_receiver_auth_for(
                self.f.context.id().cluster_epoch(),
                node.as_u32(),
            ),
            inspection,
            (index, bytes),
            deadline(),
            hook,
        )
    }

    fn read(&self) -> Result<Vec<u8>, StoreError> {
        let window = self.owner.windows.last().unwrap();
        self.server.test_storage_node().read_shard_file(
            window.participants.data_pg.get(),
            &crate::ShardKey::new(&window.segment_hash, window.start.get(), 0),
        )
    }

    fn sql(
        &self,
    ) -> Vec<Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>> {
        let node = self.server.test_storage_node();
        config(1)
            .pg_ids
            .iter()
            .map(|pg| introduction::state(&node.get_pg(*pg).unwrap()))
            .collect()
    }
}

#[test]
fn staging_file_rpc_stream_unix_tls_targets_windows_lost_reply_and_retry() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_receiver(tls, target.clone(), appended, |r| {
                    r.install_rpc_peers();
                    let sql = r.sql();
                    let before = r.f.observe();
                    let request = r.rpc_bytes(None);
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    let primary = Arc::clone(&r.f.remote(NodeId::new(4)).unwrap().server);
                    let pg = r.f.context.id().pg_id();
                    r.server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == rpc::StorageRpcMessageKind::StagingShardWrite {
                                primary.test_require_window_primary_released(pg);
                                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                                    StorageNodeServer::test_lose_staging_file_reply();
                                }
                            }
                        }));
                    let _server = super::super::super::application_sender::RunningReplica::start(
                        Arc::clone(&r.server),
                        tls,
                    );
                    assert!(r
                        .client
                        .test_staging_file_request_until(request.bytes().to_vec(), deadline())
                        .is_err());
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        1,
                        "no automatic retry after lost reply"
                    );
                    assert_eq!(r.read().unwrap(), r.shards[0]);
                    let reply = r
                        .client
                        .test_staging_file_request_until(request.bytes().to_vec(), deadline())
                        .unwrap()
                        .unwrap();
                    request.decode_response(&reply).unwrap();
                    assert_eq!(calls.load(Ordering::SeqCst), 2);
                    assert_eq!(r.read().unwrap(), r.shards[0]);
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                });
            }
        }
    }
}

#[test]
fn staging_file_rpc_stream_shorter_outer_cutoff_reaches_physical_exclusion() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_receiver(tls, target.clone(), appended, |r| {
                    let now = crate::clock::current_time_millis();
                    let expiry = now + 2_500;
                    // Keep the receiving route live beyond the injected outer
                    // cutoff; this case must isolate only delegated authority.
                    r.server.test_receiver_route_validity(now + 30_000);
                    r.install_rpc_peers();
                    assert!(expiry < r.owner.maximum_cutoff_ms);
                    let original = r.rpc_bytes(None);
                    assert!(
                        expiry
                            < rpc::complete_staging_file::decode_request(original.bytes())
                                .unwrap()
                                .receiver
                                .deadline
                                .portable_wall_valid_until_ms
                    );
                    let request = r.rpc_bytes(Some(now + 2_000));
                    let sql = r.sql();
                    let before = r.f.observe();
                    r.server
                        .set_response_frame_test_hook(std::sync::Arc::new(|kind, _| {
                            if kind == rpc::StorageRpcMessageKind::StagingShardWrite {
                                FILE_EFFECT_CLOCK.with(|clock| drop(clock.borrow_mut().take()));
                            }
                        }));
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| {
                            let reached = std::rc::Rc::new(std::cell::Cell::new(false));
                            let at_effect = std::rc::Rc::clone(&reached);
                            let monotonic = crate::clock::monotonic_time_millis();
                            crate::clock::with_time_and_monotonic_override(
                                now,
                                monotonic,
                                || {
                                    PgStore::test_with_before_shard_publication_hook(
                                        move || {
                                            at_effect.set(true);
                                            FILE_EFFECT_CLOCK.with(|clock| {
                                                assert!(clock.borrow().is_none());
                                                clock.replace(Some(
                                                    crate::clock::test_time_and_monotonic_override_guard(
                                                        expiry, monotonic + expiry - now,
                                                    ),
                                                ));
                                            });
                                        },
                                        || r.server.accept_one().unwrap(),
                                    );
                                    FILE_EFFECT_CLOCK.with(|clock| {
                                        assert!(clock.borrow().is_none(), "restore before signing");
                                    });
                                },
                            );
                            assert!(reached.get(), "must reach final physical effect boundary");
                        });
                        let error = r
                            .client
                            .test_staging_file_request_until(request.bytes().to_vec(), deadline())
                            .unwrap_or_else(|error| match error {
                                StoreError::StorageRpc { detail, .. } => {
                                    panic!("{}", detail.as_str())
                                }
                                error => panic!("{error}"),
                            })
                            .unwrap_err();
                        assert!(
                            error.message.contains("expired") || error.message.contains("deadline"),
                            "{error:?}"
                        );
                        worker.join().unwrap();
                    });
                    assert!(matches!(r.read(), Err(StoreError::NotFound)));
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                });
            }
        }
    }
}

#[test]
fn staging_file_rpc_authenticated_crossed_reply_is_not_a_file_receipt() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for tls in [false, true] {
        with_receiver(tls, StagingTarget::StreamPut, false, |r| {
            r.install_rpc_peers();
            let sql = r.sql();
            let before = r.f.observe();
            let request = r.rpc_bytes(None);
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            r.server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == rpc::StorageRpcMessageKind::StagingShardWrite {
                        count.fetch_add(1, Ordering::SeqCst);
                        let mut reply = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap()
                            .to_vec();
                        reply[6] ^= 1;
                        frame.payload = rpc::encode_storage_rpc_success_response(&reply);
                    }
                }));
            let _server = super::super::super::application_sender::RunningReplica::start(
                Arc::clone(&r.server),
                tls,
            );
            let reply = r
                .client
                .test_staging_file_request_until(request.bytes().to_vec(), deadline())
                .unwrap()
                .unwrap();
            assert!(request.decode_response(&reply).is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                r.read().unwrap(),
                r.shards[0],
                "invalid reply does not prove absence"
            );
            assert_eq!(r.sql(), sql);
            assert_eq!(r.f.observe(), before);
        });
    }
}

#[test]
fn staging_stream_file_receiver_unix_tls_both_targets_and_windows_write_without_sql() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_receiver(tls, target.clone(), appended, |r| {
                    let before = r.f.observe();
                    let sql = r.sql();
                    let mut effects = 0;
                    for _ in 0..2 {
                        let ack = r
                            .write(&r.inspection, 0, &r.shards[0], |point| {
                                if point == "before physical write" {
                                    effects += 1;
                                    r.f.remote(NodeId::new(4))
                                        .unwrap()
                                        .server
                                        .test_require_window_primary_released(
                                            r.f.context.id().pg_id(),
                                        );
                                }
                            })
                            .unwrap();
                        assert_eq!(ack.stored_size, r.shards[0].len() as u64);
                        assert_eq!(ack.crc64, checksum::crc64::checksum(&r.shards[0]));
                        assert_eq!(r.read().unwrap(), r.shards[0]);
                    }
                    assert_eq!(effects, 2);
                    let mut crossed = r.shards[0].clone();
                    crossed[0] ^= 1;
                    assert!(r.write(&r.inspection, 0, &crossed, |_| {}).is_err());
                    assert_eq!(r.read().unwrap(), r.shards[0]);
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                });
            }
        }
    }
}

#[test]
fn staging_stream_file_receiver_rejects_crossed_segments_and_primary_authority() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            with_receiver(false, target.clone(), appended, |r| {
                let sql = r.sql();
                let before = r.f.observe();
                let original = wire::decode_request(&r.inspection).unwrap();
                for fault in [
                    "session", "vid", "index", "epoch", "hash", "ec", "data-pg", "size",
                ] {
                    let mut segment = original.segment.clone();
                    match fault {
                        "session" => {
                            segment.session_id = crate::tests::stream_session_id("crossed")
                        }
                        "vid" => {
                            segment.segment_vid =
                                GenerationId::new(segment.segment_vid.get() + 1).unwrap()
                        }
                        "index" => segment.segment_index += 1,
                        "epoch" => {
                            segment.placement_cluster_epoch =
                                ClusterEpoch::new(segment.placement_cluster_epoch.get() + 1)
                                    .unwrap()
                        }
                        "hash" => segment.segment_okh[0] ^= 1,
                        "ec" => segment.ec_m += 1,
                        "data-pg" => segment.data_pg_id += 1,
                        "size" => segment.size = u64::MAX,
                        _ => unreachable!(),
                    }
                    let bad = wire::encode_request(original.allocation, &segment).unwrap();
                    let error = r
                        .write(&bad, 0, &r.shards[0], |_| {
                            panic!("crossed segment reached confirmation: {fault}")
                        })
                        .unwrap_err();
                    assert_eq!(error.code, rpc::StorageRpcErrorCode::PayloadDecode);
                }
                for fault in ["prefix", "expected-session", "checksum"] {
                    let mut request = allocation::decode_request(original.allocation).unwrap();
                    match fault {
                        "prefix" => request.prefix.applied_log_index += 1,
                        "checksum" => request.preparation.request.segment_crc64 ^= 1,
                        "expected-session" => {
                            match request.preparation.expected_append_state.as_mut().unwrap() {
                                crate::types::ExpectedStreamAppendState::PutObject {
                                    session,
                                    ..
                                }
                                | crate::types::ExpectedStreamAppendState::UploadPart {
                                    session,
                                    ..
                                } => session.created_at += 1,
                            }
                        }
                        _ => unreachable!(),
                    }
                    let bad = wire::encode_request(
                        &allocation::encode_request(&request).unwrap(),
                        &original.segment,
                    )
                    .unwrap();
                    assert!(
                        r.write(&bad, 0, &r.shards[0], |point| assert_ne!(
                            point,
                            "before physical write"
                        ))
                        .is_err(),
                        "{fault}"
                    );
                }
                assert!(matches!(r.read(), Err(StoreError::NotFound)));
                assert_eq!(r.sql(), sql);
                assert_eq!(r.f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_stream_file_receiver_requires_consumed_cursor_and_live_issuer() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            with_receiver(true, target.clone(), appended, |r| {
                let node = r.f.node(NodeId::new(4));
                let window = r.owner.windows.last().unwrap();
                for fault in ["unconsumed", "overrun", "issuer"] {
                    {
                        let pg = node.get_pg(r.f.context.id().pg_id().get()).unwrap();
                        let cursor = match fault {
                            "unconsumed" => window.start.get(),
                            "overrun" => window.end.get() + 1,
                            _ => window.end.get(),
                        };
                        pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2", rusqlite::params![cursor, r.owner.subject.id.attempt]).unwrap();
                        if fault == "issuer" {
                            pg.begin_allocation_lifetime_replacement()
                                .unwrap()
                                .complete()
                                .unwrap();
                        }
                        pg.recover_complete_command_state(
                            PgStoreRecoveryContext::for_node(NodeId::new(4)),
                            &topology(),
                        )
                        .unwrap();
                    }
                    let before = r.f.observe();
                    let sql = r.sql();
                    let mut witnessed = false;
                    assert!(
                        r.write(&r.inspection, 0, &r.shards[0], |point| {
                            witnessed |= point == "after witness";
                            assert_ne!(point, "before physical write");
                        })
                        .is_err(),
                        "{fault}"
                    );
                    assert!(witnessed);
                    assert!(matches!(r.read(), Err(StoreError::NotFound)));
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                }
            });
        }
    }
}

#[test]
fn staging_stream_file_receiver_peer_and_release_failures_do_not_write() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_receiver(tls, target, true, |r| {
                let sql = r.sql();
                let before = r.f.observe();
                for fault in ["witness", "allocation-inspection", "release"] {
                    for node in [0, 4] {
                        r.f.remote(NodeId::new(node)).unwrap().server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            let selected = match fault {
                                "witness" => node == 0 && kind == rpc::StorageRpcMessageKind::MetadataCommandReplicaState,
                                "allocation-inspection" => node == 4 && kind == rpc::StorageRpcMessageKind::MetadataCommandReplicaState && matches!(rpc::decode_storage_rpc_response_payload(&frame.payload), Ok(Ok(ref bytes)) if bytes.starts_with(b"SFOK")),
                                "release" => node == 4 && kind == rpc::StorageRpcMessageKind::MetadataCommandPgLockRelease,
                                _ => unreachable!(),
                            };
                            if selected { frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse { code: rpc::StorageRpcErrorCode::MetadataCommandContention, message: "stream receiver refusal".into() }).unwrap(); }
                        }));
                    }
                    let error = r
                        .write(&r.inspection, 0, &r.shards[0], |point| {
                            assert_ne!(point, "before physical write")
                        })
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        rpc::StorageRpcErrorCode::MetadataCommandContention
                    );
                    assert!(matches!(r.read(), Err(StoreError::NotFound)));
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                }
            });
        }
    }
}

fn expiry_matrix(target: StagingTarget) {
    for appended in [false, true] {
        for boundary in ["request", "route"] {
            for inside in [false, true] {
                with_receiver(false, target.clone(), appended, |r| {
                    let sql = r.sql();
                    let before = r.f.observe();
                    let now = crate::clock::current_time_millis();
                    let cutoff = now + 5_000;
                    assert!(cutoff < r.owner.maximum_cutoff_ms);
                    let original = wire::decode_request(&r.inspection).unwrap();
                    let mut allocation = allocation::decode_request(original.allocation).unwrap();
                    assert!(
                        cutoff
                            < allocation
                                .window
                                .route
                                .deadline
                                .portable_wall_valid_until_ms
                    );
                    if boundary == "request" {
                        allocation
                            .window
                            .route
                            .deadline
                            .portable_wall_valid_until_ms = now + 4_000;
                    } else {
                        r.server.test_receiver_route_validity(now + 4_000);
                    }
                    let inspection = wire::encode_request(
                        &allocation::encode_request(&allocation).unwrap(),
                        &original.segment,
                    )
                    .unwrap();
                    let override_guard = std::rc::Rc::new(std::cell::RefCell::new(None));
                    let run = || {
                        r.write(&inspection, 0, &r.shards[0], |point| {
                            if !inside && point == "before physical write" {
                                override_guard
                                    .replace(Some(crate::clock::test_time_override_guard(cutoff)));
                            }
                        })
                    };
                    let result = if inside {
                        let slot = std::rc::Rc::clone(&override_guard);
                        PgStore::test_with_before_shard_publication_hook(
                            move || {
                                slot.replace(Some(crate::clock::test_time_override_guard(cutoff)));
                            },
                            run,
                        )
                    } else {
                        run()
                    };
                    assert!(
                        override_guard.borrow().is_some(),
                        "must reach requested effect boundary"
                    );
                    let error = result.unwrap_err();
                    assert!(
                        error.message.contains("expired") || error.message.contains("deadline"),
                        "{error:?}"
                    );
                    assert!(matches!(r.read(), Err(StoreError::NotFound)));
                    assert_eq!(r.sql(), sql);
                    assert_eq!(r.f.observe(), before);
                });
            }
        }
    }
}

#[test]
fn staging_stream_file_receiver_put_expiry_cannot_renew_physical_authority() {
    expiry_matrix(StagingTarget::StreamPut);
}

#[test]
fn staging_stream_file_receiver_part_expiry_cannot_renew_physical_authority() {
    expiry_matrix(multipart_target());
}

#[test]
fn staging_stream_file_receiver_read_opcode_cannot_allocate() {
    for tls in [false, true] {
        with_receiver(tls, StagingTarget::StreamPut, false, |r| {
            let peer = r.f.remote(NodeId::new(4)).unwrap();
            let session = peer
                .client
                .open_metadata_command_critical_section_until(r.f.context.id().pg_id(), deadline())
                .unwrap();
            let before = r.f.observe();
            let bytes = session
                .test_stream_inspection_payload(r.inspection.clone(), deadline())
                .unwrap();
            wire::decode_response(&bytes, &r.inspection).unwrap();
            let original = wire::decode_request(&r.inspection).unwrap();
            let error = session
                .test_stream_inspection_payload(original.allocation.to_vec(), deadline())
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: rpc::StorageRpcErrorCode::PayloadDecode,
                        ..
                    }
                ),
                "{error:?}"
            );
            assert_eq!(r.f.observe(), before);
            assert!(matches!(r.read(), Err(StoreError::NotFound)));
        });
    }
}
