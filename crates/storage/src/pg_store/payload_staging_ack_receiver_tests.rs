// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{self as rpc, complete_staging_ack as wire};
use std::sync::Arc;

thread_local! {
    static ACK_EFFECT_CLOCK: std::cell::RefCell<Option<crate::clock::TestTimeAndMonotonicOverrideGuard>> = const {
        std::cell::RefCell::new(None)
    };
}

struct Receiver<'a> {
    f: &'a Mixed,
    server: Arc<StorageNodeServer>,
    client: crate::node_client::UnixStorageNodeClient,
    owner: StagingOwnerRecord,
    inspection: Option<Vec<u8>>,
    descriptor: Vec<u8>,
    receipts: Vec<wire::Receipt>,
}

type AckRow = (Vec<u8>, i64, i64, i64, i64);

#[derive(Debug, PartialEq)]
struct SqlState {
    metadata: Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>,
    acknowledgements: Vec<AckRow>,
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
            let node = f.node(NodeId::new(4));
            let owner = {
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let snapshot = pg.command_build_snapshot().unwrap();
                let owner = append::load(&pg, &fresh_capture::template(target.clone()));
                snapshot.finish().unwrap();
                owner
            };
            let shards = super::super::files::shards(&owner, b"data");
            let Primary::Remote(client, session) = primary else {
                unreachable!()
            };
            let run = |inspection| {
                let window = owner.windows.last().unwrap();
                assert_ne!(
                    window.participants.acknowledgement_primary,
                    window.participants.files[0]
                );
                let receiver_dir = test_util::tempdir();
                let (server, client) = super::super::super::application_sender::replica_from_pg(
                    tls,
                    &node.get_pg(f.context.id().pg_id().get()).unwrap(),
                    &config(1),
                    &receiver_dir,
                    window.participants.acknowledgement_primary,
                );
                let field = f.context.encode_field().unwrap();
                let r = Receiver {
                    f,
                    server: Arc::new(server),
                    client,
                    owner: owner.clone(),
                    inspection,
                    descriptor: field[7..].to_vec(),
                    receipts: shards
                        .iter()
                        .enumerate()
                        .map(|(i, bytes)| wire::Receipt {
                            shard_index: i as u8,
                            ack: crate::WriteAck {
                                stored_size: bytes.len() as u64,
                                crc64: checksum::crc64::checksum(bytes),
                            },
                        })
                        .collect(),
                };
                r.install_peers();
                action(&r);
            };
            if target == StagingTarget::DirectPut {
                let writer = confirmed
                    .into_direct_file_writer_remote(
                        client,
                        session,
                        &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                    )
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                run(None);
                drop(writer);
            } else {
                let (_, mut request, expected) = allocation_remote::inputs(f, target);
                let window = owner.windows.last().unwrap();
                let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
                stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
                request.segment_crc64 = checksum::crc64::checksum(&stored);
                request.payload_crc64 = checksum::crc64::checksum(b"data");
                let original = confirmed.test_stream_allocation_bytes(&request, &expected);
                let allocated = confirmed
                    .allocate_stream_remote(client, session, &request, &expected)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let inspection = rpc::complete_stream_file_primary::encode_request(
                    &original,
                    allocated.segment(),
                )
                .unwrap();
                let writer = allocated
                    .into_stream_file_writer_remote(client, session)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                run(Some(inspection));
                drop(writer);
            }
        },
    );
}

impl Receiver<'_> {
    fn install_peers(&self) {
        self.server.test_install_window_receiver_peers(
            self.f
                .remote
                .iter()
                .map(|p| (p.node, p.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::super::complete_log_rpc::window_receiver_auth_for(
                self.f.context.id().cluster_epoch(),
                self.owner
                    .windows
                    .last()
                    .unwrap()
                    .participants
                    .acknowledgement_primary
                    .as_u32(),
            ),
        );
    }

    fn request(&self) -> wire::Request<'_> {
        let deadline = match &self.inspection {
            Some(inspection) => {
                let files = rpc::complete_stream_file_primary::decode_request(inspection).unwrap();
                rpc::complete_stream_allocation::decode_request(files.allocation)
                    .unwrap()
                    .window
                    .route
                    .deadline
            }
            None => rpc::StorageRpcOperationDeadline::from_instant(deadline()),
        };
        wire::Request {
            receiver: rpc::complete_settled_prefix::Request {
                node: self
                    .owner
                    .windows
                    .last()
                    .unwrap()
                    .participants
                    .acknowledgement_primary,
                pg: self.f.context.id().pg_id(),
                epoch: self.f.context.id().cluster_epoch(),
                topology: rpc::complete_settled_prefix::topology_digest(&topology()),
                deadline,
            },
            provenance: match &self.inspection {
                Some(inspection) => wire::Provenance::Stream { inspection },
                None => wire::Provenance::Direct {
                    descriptor: &self.descriptor,
                    logical_size: 4,
                },
            },
            receipts: self.receipts.clone(),
        }
    }

    fn sql(&self) -> Vec<SqlState> {
        let node = self.server.test_storage_node();
        config(1)
            .pg_ids
            .iter()
            .map(|id| {
                let pg = node.get_pg(*id).unwrap();
                let mut query = pg.conn.prepare(
                    "SELECT shard_key, data_size, crc64_nvme, created_at, status FROM shards ORDER BY shard_key",
                ).unwrap();
                let acknowledgements = query.query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
                }).unwrap().collect::<Result<Vec<AckRow>, _>>().unwrap();
                SqlState { metadata: introduction::state(&pg), acknowledgements }
            })
            .collect()
    }

    fn send(&self, request: &wire::EncodedRequest) -> Result<(), rpc::StorageRpcErrorResponse> {
        let reply = self
            .client
            .test_staging_ack_request_until(request.bytes().to_vec(), deadline())
            .unwrap()?;
        request.decode_response(&reply).unwrap();
        Ok(())
    }

    fn key(&self, index: u8) -> crate::ShardKey {
        let window = self.owner.windows.last().unwrap();
        crate::ShardKey::new(&window.segment_hash, window.start.get(), index)
    }
}

#[test]
fn staging_ack_receiver_confirms_derives_and_retries_exactly_unix_tls() {
    for tls in [false, true] {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            for appended in [false, true] {
                if appended && target == StagingTarget::DirectPut {
                    continue;
                }
                with_receiver(tls, target.clone(), appended, |r| {
                    let before = r.f.observe();
                    let receiver_before = r.sql();
                    let request = wire::encode_request(&r.request()).unwrap();
                    let _running = super::super::super::application_sender::RunningReplica::start(
                        Arc::clone(&r.server),
                        tls,
                    );
                    r.send(&request).unwrap();
                    let after = r.sql();
                    assert_ne!(
                        after, receiver_before,
                        "snapshot must include acknowledgement rows"
                    );
                    let node = r.server.test_storage_node();
                    let window = r.owner.windows.last().unwrap();
                    let pg = node.get_pg(window.participants.data_pg.get()).unwrap();
                    let changes = pg.conn.total_changes();
                    for receipt in &r.receipts {
                        let key = r.key(receipt.shard_index);
                        pg.validate_written_shard_ack(&key, receipt.ack).unwrap();
                        assert!(
                            node.read_shard_file(window.participants.data_pg.get(), &key)
                                .is_err(),
                            "ack receiver must not write files"
                        );
                    }
                    drop(pg);
                    r.send(&request).unwrap();
                    assert_eq!(r.sql(), after);
                    assert_eq!(
                        node.get_pg(window.participants.data_pg.get())
                            .unwrap()
                            .conn
                            .total_changes(),
                        changes
                    );
                    assert_eq!(
                        r.f.observe(),
                        before,
                        "confirmation must not change custody or cursors"
                    );
                });
            }
        }
    }
}

#[test]
fn staging_ack_receiver_rejects_crossed_geometry_and_conflicting_rows_atomically() {
    for tls in [false, true] {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            with_receiver(tls, target, false, |r| {
                let _running = super::super::super::application_sender::RunningReplica::start(
                    Arc::clone(&r.server),
                    tls,
                );
                let before = r.sql();
                for fault in 0..4 {
                    let mut request = r.request();
                    match fault {
                        0 => {
                            request.receipts.pop();
                        }
                        1 => {
                            for row in &mut request.receipts {
                                row.ack.stored_size += 1;
                            }
                        }
                        2 => {
                            request.receiver.node = r.owner.windows[0].participants.files[0];
                        }
                        _ => {
                            request.receiver.topology[0] ^= 1;
                        }
                    }
                    let request = wire::encode_request(&request);
                    // Stream framing rejects crossed topology before transport.
                    if let Ok(request) = request {
                        assert!(r.send(&request).is_err());
                    }
                    assert_eq!(r.sql(), before);
                }
                let node = r.server.test_storage_node();
                let window = r.owner.windows.last().unwrap();
                let last = r.receipts.last().unwrap();
                let key = r.key(last.shard_index);
                let mut conflict = last.ack;
                conflict.crc64 ^= 1;
                node.get_pg(window.participants.data_pg.get())
                    .unwrap()
                    .register_written_shards_batch_exact(&[(&key, conflict)])
                    .unwrap();
                let before = r.sql();
                assert!(r
                    .send(&wire::encode_request(&r.request()).unwrap())
                    .is_err());
                assert_eq!(
                    r.sql(),
                    before,
                    "earlier inserts must roll back on final-row conflict"
                );
            });
        }
    }
}

#[test]
fn staging_ack_receiver_rechecks_client_and_server_cutoffs_beside_sql_effect() {
    for tls in [false, true] {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            for server_first in [false, true] {
                with_receiver(tls, target.clone(), false, |r| {
                    let now = crate::clock::current_time_millis();
                    // Leave room for the existing conservative inter-host skew
                    // bounds during confirmation. Expire deterministically at
                    // the SQL hook, not while forwarding witness inspection.
                    let expiry = now + 4_500;
                    r.server.test_receiver_route_validity(
                        now + if server_first { 4_000 } else { 30_000 },
                    );
                    r.install_peers();
                    let mut request = r.request();
                    if !server_first {
                        request.receiver.deadline.portable_wall_valid_until_ms = now + 4_000;
                    } else {
                        assert!(request.receiver.deadline.portable_wall_valid_until_ms > expiry);
                    }
                    assert!(r.owner.maximum_cutoff_ms > expiry);
                    let request = wire::encode_request(&request).unwrap();
                    let before = r.sql();
                    let custody = r.f.observe();
                    r.server.set_response_frame_test_hook(Arc::new(|kind, _| {
                        if kind == rpc::StorageRpcMessageKind::StagingShardAckRecord {
                            ACK_EFFECT_CLOCK.with(|clock| drop(clock.borrow_mut().take()));
                        }
                    }));
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| {
                            let reached = std::rc::Rc::new(std::cell::Cell::new(false));
                            let at_effect = std::rc::Rc::clone(&reached);
                            let monotonic = crate::clock::monotonic_time_millis();
                            crate::clock::with_time_and_monotonic_override(now, monotonic, || {
                                PgStore::test_with_before_shard_ack_effect_hook(move |in_transaction| {
                                    assert!(in_transaction);
                                    at_effect.set(true);
                                    ACK_EFFECT_CLOCK.with(|clock| {
                                        assert!(clock.borrow().is_none());
                                        clock.replace(Some(crate::clock::test_time_and_monotonic_override_guard(
                                            expiry, monotonic + expiry - now,
                                        )));
                                    });
                                }, || r.server.accept_one().unwrap());
                                ACK_EFFECT_CLOCK.with(|clock| assert!(clock.borrow().is_none(), "restore before signing"));
                            });
                            assert!(reached.get(), "must reach transactional mutation boundary");
                        });
                        let error = r.send(&request).unwrap_err();
                        assert!(
                            error.message.contains("expired") || error.message.contains("deadline"),
                            "{error:?}"
                        );
                        worker.join().unwrap();
                    });
                    assert_eq!(r.sql(), before);
                    assert_eq!(r.f.observe(), custody);
                });
            }
        }
    }
}

#[test]
fn staging_ack_receiver_requires_successful_confirmation_and_acknowledged_release() {
    for tls in [false, true] {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            for fault in ["witness", "primary", "release"] {
                with_receiver(tls, target.clone(), false, |r| {
                    let before = r.sql();
                    let peer = if fault == "witness" { 0 } else { 4 };
                    r.f.remote(NodeId::new(peer))
                        .unwrap()
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            let selected = if fault == "release" {
                                rpc::StorageRpcMessageKind::MetadataCommandPgLockRelease
                            } else {
                                rpc::StorageRpcMessageKind::MetadataCommandReplicaState
                            };
                            if kind == selected {
                                frame.payload = rpc::encode_storage_rpc_error_response(
                                    &rpc::StorageRpcErrorResponse {
                                        code: rpc::StorageRpcErrorCode::MetadataCommandContention,
                                        message: "ack receiver injected refusal".into(),
                                    },
                                )
                                .unwrap();
                            }
                        }));
                    let _running = super::super::super::application_sender::RunningReplica::start(
                        Arc::clone(&r.server),
                        tls,
                    );
                    let error = r
                        .send(&wire::encode_request(&r.request()).unwrap())
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        rpc::StorageRpcErrorCode::MetadataCommandContention
                    );
                    assert_eq!(r.sql(), before);
                });
            }
        }
    }
}
