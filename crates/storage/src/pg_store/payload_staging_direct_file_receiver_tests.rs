// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-owned candidate effects with real authenticated primary/witness IO.
//! Direct entry is test-only; no shard-write RPC or live frontend is activated.
use super::super::fanout_mixed::Mixed;
use super::*;
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{
    self as rpc, complete_settled_prefix as witness, complete_window_primary as wire,
    StorageRpcOperationDeadline,
};

struct Case {
    _dir: test_util::TempDir,
    _receiver_dir: test_util::TempDir,
    f: Mixed,
    receiver: std::sync::Arc<StorageNodeServer>,
    client: crate::node_client::UnixStorageNodeClient,
    owner: StagingOwnerRecord,
}

impl Case {
    fn install_rpc_peers(&self) {
        self.receiver.test_install_window_receiver_peers(
            self.f
                .remote
                .iter()
                .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::complete_log_rpc::window_receiver_auth_for(
                self.f.context.id().cluster_epoch(),
                self.owner.windows[0].participants.files[0].as_u32(),
            ),
        );
    }

    fn rpc_bytes(
        &self,
        bytes: &[u8],
        logical_size: u64,
    ) -> rpc::complete_staging_file::EncodedRequest {
        let field = self.f.context.encode_field().unwrap();
        rpc::complete_staging_file::encode_request(rpc::complete_staging_file::Request {
            receiver: witness::Request {
                node: self.owner.windows[0].participants.files[0],
                epoch: self.f.context.id().cluster_epoch(),
                pg: self.f.context.id().pg_id(),
                deadline: StorageRpcOperationDeadline::from_instant(deadline()),
                topology: witness::topology_digest(&topology()),
            },
            provenance: rpc::complete_staging_file::Provenance::Direct {
                descriptor: &field[7..],
                logical_size,
            },
            shard_index: 0,
            bytes,
        })
        .unwrap()
    }

    fn new(tls: bool) -> Self {
        // Capture under the installed primary's actual issuer. A copied window
        // from an earlier runtime is valid inspection evidence but cannot write.
        let (dir, f, _) = handoff::allocation_remote::setup(tls, StagingTarget::DirectPut, false);
        for member in 0..5 {
            let node = f.node(NodeId::new(member));
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            apply(&pg, &f.context);
            pg.recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(member)),
                &topology(),
            )
            .unwrap();
        }
        Self::from_fixture(tls, dir, f)
    }

    fn from_fixture(tls: bool, dir: test_util::TempDir, f: Mixed) -> Self {
        let node = f.node(NodeId::new(4));
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
        snapshot.finish().unwrap();
        let receiver_dir = test_util::tempdir();
        let (receiver, client) = super::super::application_sender::replica_from_pg(
            tls,
            &pg,
            &config(1),
            &receiver_dir,
            owner.windows[0].participants.files[0],
        );
        Self {
            _dir: dir,
            _receiver_dir: receiver_dir,
            f,
            receiver: std::sync::Arc::new(receiver),
            client,
            owner,
        }
    }

    fn write(
        &self,
        index: u8,
        size: u64,
        bytes: &[u8],
        hook: impl FnMut(&'static str),
    ) -> Result<crate::WriteAck, rpc::StorageRpcErrorResponse> {
        self.write_until(index, size, bytes, deadline(), hook)
    }

    fn write_until(
        &self,
        index: u8,
        size: u64,
        bytes: &[u8],
        cutoff: Instant,
        hook: impl FnMut(&'static str),
    ) -> Result<crate::WriteAck, rpc::StorageRpcErrorResponse> {
        let field = self.f.context.encode_field().unwrap();
        let id = self.f.context.id();
        let node = self.owner.windows[0].participants.files[0];
        self.receiver.test_write_direct_file_receiver(
            self.f
                .remote
                .iter()
                .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::complete_log_rpc::window_receiver_auth_for(
                id.cluster_epoch(),
                node.as_u32(),
            ),
            wire::Request {
                route: witness::Request {
                    node,
                    epoch: id.cluster_epoch(),
                    pg: id.pg_id(),
                    deadline: StorageRpcOperationDeadline::from_instant(cutoff),
                    topology: witness::topology_digest(&topology()),
                },
                descriptor: &field[7..],
            },
            (index, size, bytes),
            cutoff,
            hook,
        )
    }

    fn read(&self) -> Result<Vec<u8>, StoreError> {
        let window = &self.owner.windows[0];
        self.receiver.test_storage_node().read_shard_file(
            window.participants.data_pg.get(),
            &crate::ShardKey::new(&window.segment_hash, window.start.get(), 0),
        )
    }

    fn sql(
        &self,
    ) -> Vec<Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>> {
        let node = self.receiver.test_storage_node();
        config(1)
            .pg_ids
            .iter()
            .map(|id| introduction::state(&node.get_pg(*id).unwrap()))
            .collect()
    }
}

#[test]
fn staging_file_rpc_direct_unix_tls_exact_retry_and_lost_reply_preserve_owner() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for tls in [false, true] {
        for lost in [false, true] {
            let case = Case::new(tls);
            case.install_rpc_peers();
            let before = case.f.observe();
            let sql = case.sql();
            let payload = b"installed receiver direct ciphertext";
            let shards = files::shards(&case.owner, payload);
            let request = case.rpc_bytes(&shards[0], payload.len() as u64);
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            let primary = Arc::clone(&case.f.remote(NodeId::new(4)).unwrap().server);
            let pg = case.f.context.id().pg_id();
            case.receiver
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == rpc::StorageRpcMessageKind::StagingShardWrite {
                        primary.test_require_window_primary_released(pg);
                        if count.fetch_add(1, Ordering::SeqCst) == 0 && lost {
                            StorageNodeServer::test_lose_staging_file_reply();
                        }
                    }
                }));
            let _peers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
            let _server = super::super::application_sender::RunningReplica::start(
                Arc::clone(&case.receiver),
                tls,
            );
            let first = case
                .client
                .test_staging_file_request_until(request.bytes().to_vec(), deadline());
            if lost {
                assert!(first.is_err(), "lost success must not be confirmed");
            } else {
                request.decode_response(&first.unwrap().unwrap()).unwrap();
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1, "transport does not retry");
            assert_eq!(
                case.read().unwrap(),
                shards[0],
                "loss does not imply absence"
            );
            let retry = case
                .client
                .test_staging_file_request_until(request.bytes().to_vec(), deadline())
                .unwrap()
                .unwrap();
            request.decode_response(&retry).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert_eq!(case.read().unwrap(), shards[0]);
            assert_eq!(case.sql(), sql);
            assert_eq!(case.f.observe(), before);
        }
    }
}

#[test]
fn staging_file_rpc_direct_rejects_crossed_subjects_before_peer_io() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for tls in [false, true] {
        let case = Case::new(tls);
        case.install_rpc_peers();
        let sql = case.sql();
        let shards = files::shards(&case.owner, b"data");
        let base = case.rpc_bytes(&shards[0], 4);
        let calls = Arc::new(AtomicUsize::new(0));
        for peer in &case.f.remote {
            let calls = Arc::clone(&calls);
            peer.server
                .set_response_frame_test_hook(Arc::new(move |_, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                }));
        }
        let _peers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
        let _server = super::super::application_sender::RunningReplica::start(
            Arc::clone(&case.receiver),
            tls,
        );
        for field in 0..7 {
            let mut crossed = rpc::complete_staging_file::decode_request(base.bytes()).unwrap();
            match field {
                0 => crossed.receiver.node = NodeId::new(999),
                1 => crossed.receiver.epoch = ClusterEpoch::new(999).unwrap(),
                2 => crossed.receiver.pg = PgId::new(999),
                3 => crossed.receiver.topology[0] ^= 1,
                4 => crossed.receiver.deadline.portable_wall_valid_until_ms = 1,
                5 => crossed.shard_index = 255,
                6 => {
                    let rpc::complete_staging_file::Provenance::Direct { descriptor, .. } =
                        crossed.provenance
                    else {
                        unreachable!()
                    };
                    crossed.provenance = rpc::complete_staging_file::Provenance::Direct {
                        descriptor,
                        logical_size: u64::MAX,
                    };
                }
                _ => unreachable!(),
            }
            let encoded = rpc::complete_staging_file::encode_request(crossed).unwrap();
            assert!(
                case.client
                    .test_staging_file_request_until(encoded.bytes().to_vec(), deadline())
                    .unwrap()
                    .is_err(),
                "{field}"
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "reject before primary/witness IO: {field}"
            );
            assert!(matches!(case.read(), Err(StoreError::NotFound)));
            assert_eq!(case.sql(), sql);
        }
    }
}

#[test]
fn staging_file_rpc_live_dispatch_rejects_candidate_without_mutation() {
    for tls in [false, true] {
        let mut case = Case::new(tls);
        std::sync::Arc::get_mut(&mut case.receiver)
            .unwrap()
            .test_disable_complete_log_operations();
        case.install_rpc_peers();
        let sql = case.sql();
        let shards = files::shards(&case.owner, b"data");
        let request = case.rpc_bytes(&shards[0], 4);
        let _server = super::super::application_sender::RunningReplica::start(
            std::sync::Arc::clone(&case.receiver),
            tls,
        );
        let error = case
            .client
            .test_staging_file_request_until(request.bytes().to_vec(), deadline())
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, rpc::StorageRpcErrorCode::UnsupportedOperation);
        assert!(matches!(case.read(), Err(StoreError::NotFound)));
        assert_eq!(case.sql(), sql);
    }
}

#[test]
fn staging_file_rpc_legacy_swrt_epoch_is_not_candidate_dispatch() {
    use crate::node_client::PlacedShardNodeClient;
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mut config = config(1);
        config.cluster_epoch = ClusterEpoch::new(1_414_682_451).unwrap();
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
        }
        config.data_dir = dir.path().join("node");
        config.socket_path = dir.path().join("socket/node.sock");
        let (server, client) = if tls {
            super::super::tls::tls_server_at(config.clone(), false, config.cluster_epoch)
        } else {
            let (server, client, _) = installed_config(&dir, config.clone(), true);
            (server, client)
        };
        let server = std::sync::Arc::new(server);
        let key = crate::ShardKey::new(&[0x39; 16], 1, 0);
        let pg = config.pg_ids[0];
        let location = crate::cluster::ShardLocation::new(
            config.cluster_epoch,
            crate::DataPgId::new_for_test(PgId::new(pg)),
            key.shard_index(),
            config.node_id,
        );
        // Raw file-protocol specimen, not an encrypted object fixture.
        let bytes = b"legacy epoch collision";
        let payload = rpc::encode_shard_write_request(&rpc::StorageRpcShardWriteRequest {
            location: location.into(),
            shard_key: key.clone(),
            expected_size: bytes.len() as u64,
            expected_crc64: checksum::crc64::checksum(bytes),
            effect_deadline: None,
            payload: bytes.to_vec(),
        })
        .unwrap();
        assert_eq!(&payload[..4], b"SWRT");
        rpc::decode_shard_write_request(&payload).unwrap();
        let _running = super::super::application_sender::RunningReplica::start(
            std::sync::Arc::clone(&server),
            tls,
        );
        // A valid legacy payload must not be accepted by the new message kind.
        let error = client
            .test_staging_file_request_until(payload, deadline())
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, rpc::StorageRpcErrorCode::PayloadDecode);
        assert!(matches!(
            server.test_storage_node().read_shard_file(pg, &key),
            Err(StoreError::NotFound)
        ));
        // Candidate selection is enabled: the ordinary client must still use
        // the legacy decoder, even with the formerly colliding epoch prefix.
        let ack = client
            .open_placed_shard_route(location, &key)
            .unwrap()
            .write_placed_shard(bytes)
            .unwrap();
        assert_eq!(ack.stored_size, bytes.len() as u64);
        assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
        assert_eq!(
            server
                .test_storage_node()
                .read_shard_file(pg, &key)
                .unwrap(),
            bytes
        );
    }
}

#[test]
fn staging_file_rpc_requires_released_inbound_metadata_section() {
    for tls in [false, true] {
        let case = Case::new(tls);
        let shards = files::shards(&case.owner, b"data");
        let request = case.rpc_bytes(&shards[0], 4);
        let primary = case.f.remote(NodeId::new(4)).unwrap();
        let _server = primary.run();
        let session = primary
            .client
            .open_metadata_command_critical_section_until(case.f.context.id().pg_id(), deadline())
            .unwrap();
        let error = session
            .test_staging_file_request(request.bytes().to_vec())
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, rpc::StorageRpcErrorCode::PayloadDecode);
        assert!(error.message.contains("released metadata sections"));
        primary
            .server
            .test_require_window_primary_held(case.f.context.id().pg_id());
        assert!(matches!(case.read(), Err(StoreError::NotFound)));
    }
}

#[test]
fn staging_direct_file_receiver_requires_live_issuer_not_just_retained_window() {
    for tls in [false, true] {
        let (dir, f) = primary::mixed(tls, true, StagingTarget::DirectPut, false);
        let case = Case::from_fixture(tls, dir, f);
        let payload = b"old primary lifetime";
        let shards = files::shards(&case.owner, payload);
        let before = case.sql();
        let _servers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
        let mut witnessed = false;
        assert!(case
            .write(0, payload.len() as u64, &shards[0], |point| {
                witnessed |= point == "after witness";
                assert_ne!(point, "before physical write");
            })
            .is_err());
        assert!(
            witnessed,
            "must reach primary validation after successful witness inspection"
        );
        assert!(matches!(case.read(), Err(StoreError::NotFound)));
        assert_eq!(case.sql(), before);
    }
}

#[test]
fn staging_direct_file_receiver_unix_tls_writes_exact_ciphertext_after_release_without_sql() {
    for tls in [false, true] {
        let case = Case::new(tls);
        let payload = b"receiver-owned direct payload";
        let shards = files::shards(&case.owner, payload);
        let before = case.f.observe();
        let receiver_before = case.sql();
        let _servers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
        let mut effects = 0;
        for _ in 0..2 {
            let ack = case
                .write(0, payload.len() as u64, &shards[0], |point| {
                    if point == "before physical write" {
                        effects += 1;
                        case.f
                            .remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .test_require_window_primary_released(case.f.context.id().pg_id());
                    }
                })
                .unwrap();
            assert_eq!(ack.stored_size, shards[0].len() as u64);
            assert_eq!(ack.crc64, checksum::crc64::checksum(&shards[0]));
            assert_eq!(case.read().unwrap(), shards[0]);
        }
        assert_eq!(effects, 2);
        let mut crossed = shards[0].clone();
        crossed[0] ^= 1;
        assert!(case
            .write(0, payload.len() as u64, &crossed, |_| {})
            .is_err());
        assert_eq!(case.read().unwrap(), shards[0]);
        assert_eq!(
            case.sql(),
            receiver_before,
            "file ack must not insert acknowledgement SQL rows"
        );
        assert_eq!(case.f.observe(), before);
    }
}

#[test]
fn staging_direct_file_receiver_rejects_wrong_participant_and_geometry_before_network() {
    let case = Case::new(false);
    let payload = b"receiver geometry";
    let shards = files::shards(&case.owner, payload);
    let before = case.sql();
    // No peer accept loops: every rejection must precede network admission.
    for (index, size, bytes) in [
        (1, payload.len() as u64, shards[0].as_slice()),
        (u8::MAX, payload.len() as u64, shards[0].as_slice()),
        (0, u64::MAX, shards[0].as_slice()),
        (0, payload.len() as u64, &shards[0][1..]),
    ] {
        let error = case
            .write(index, size, bytes, |_| panic!("unexpected confirmation"))
            .unwrap_err();
        assert_eq!(
            error.code,
            rpc::StorageRpcErrorCode::PayloadDecode,
            "{error:?}"
        );
        assert!(matches!(case.read(), Err(StoreError::NotFound)));
    }
    assert_eq!(case.sql(), before);
}

#[test]
fn staging_direct_file_receiver_failed_confirmation_or_release_never_writes() {
    for tls in [false, true] {
        for fault in ["witness", "primary", "release"] {
            let case = Case::new(tls);
            let payload = b"receiver rejection";
            let shards = files::shards(&case.owner, payload);
            let before = case.sql();
            let target = if fault == "witness" { 0 } else { 4 };
            case.f
                .remote(NodeId::new(target))
                .unwrap()
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    let selected = if fault == "release" {
                        rpc::StorageRpcMessageKind::MetadataCommandPgLockRelease
                    } else {
                        rpc::StorageRpcMessageKind::MetadataCommandReplicaState
                    };
                    if kind == selected {
                        frame.payload =
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::MetadataCommandContention,
                                message: "receiver injected refusal".into(),
                            })
                            .unwrap();
                    }
                }));
            let _servers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
            let error = case
                .write(0, payload.len() as u64, &shards[0], |point| {
                    assert_ne!(point, "before physical write")
                })
                .unwrap_err();
            assert_eq!(
                error.code,
                rpc::StorageRpcErrorCode::MetadataCommandContention
            );
            assert!(matches!(case.read(), Err(StoreError::NotFound)));
            assert_eq!(case.sql(), before);
        }
    }
}

#[test]
fn staging_direct_file_receiver_rechecks_cutoff_before_effect_and_inside_file_exclusion() {
    for boundary in ["window", "request", "route"] {
        for inside in [false, true] {
            let case = Case::new(false);
            let payload = b"receiver expiry";
            let shards = files::shards(&case.owner, payload);
            let before = case.sql();
            let _servers: Vec<_> = case.f.remote.iter().map(|peer| peer.run()).collect();
            let now = crate::clock::current_time_millis();
            let cutoff = if boundary == "window" {
                case.owner.maximum_cutoff_ms
            } else {
                assert!(now + 5_000 < case.owner.maximum_cutoff_ms);
                now + 5_000
            };
            if boundary == "route" {
                case.receiver.test_receiver_route_validity(now + 4_000);
            }
            let work_deadline = if boundary == "request" {
                Instant::now() + Duration::from_secs(4)
            } else {
                // Isolate the durable window from the caller's later request cap.
                Instant::now()
                    + Duration::from_millis(
                        case.owner.maximum_cutoff_ms.saturating_sub(now) + 30_000,
                    )
            };
            // Do not freeze the authenticated client's clock during fixture setup
            // or peer IO: remote clocks continue advancing. Override only after all
            // peer exchanges, at the exact effect boundary under test.
            let override_guard = std::rc::Rc::new(std::cell::RefCell::new(None));
            let run = || {
                case.write_until(
                    0,
                    payload.len() as u64,
                    &shards[0],
                    work_deadline,
                    |point| {
                        if !inside && point == "before physical write" {
                            override_guard
                                .replace(Some(crate::clock::test_time_override_guard(cutoff)));
                        }
                    },
                )
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
            let error = result.unwrap_err();
            assert!(
                override_guard.borrow().is_some(),
                "must reach the requested expiry boundary"
            );
            assert!(
                error.message.contains("expired") || error.message.contains("deadline"),
                "{error:?}"
            );
            assert!(matches!(case.read(), Err(StoreError::NotFound)));
            assert_eq!(case.sql(), before);
        }
    }
}
