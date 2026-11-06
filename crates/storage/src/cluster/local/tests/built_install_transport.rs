// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installation-boundary tests, not completed retained-owner custody tests.
//! Both publisher helpers must preserve the exact candidate on ambiguous RPC
//! results without draining it or pretending that an absent reply means rejection.

use super::*;
use crate::cluster::{
    AllocatorCleanupFreshInstallOutcome, ApplyValidatedFreshInstallOutcome,
    BuiltPendingCommandInstallFailure, RequestWorkBudget,
};
use crate::node_client::MetadataCommandApplyErrorKind;
use crate::storage_rpc::{
    decode_metadata_command_pending_slot_insert_response,
    decode_metadata_command_pending_slot_request, decode_storage_rpc_response_payload,
    encode_storage_rpc_error_response, encode_storage_rpc_success_response,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to, StorageRpcErrorResponse,
    StorageRpcMessageKind, StorageRpcMetadataCommandPendingSlotInsertOutcome,
};
use std::os::unix::net::{UnixListener, UnixStream};

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Healthy,
    Reject,
    DropBeforeDispatch,
    DropAfterInsertion,
    MalformedReply,
}

#[derive(Clone, Copy, Debug)]
enum Publisher {
    Allocator,
    ApplyValidated,
}

fn install(
    cluster: &StorageCluster,
    publisher: Publisher,
    command: &MetadataCommandEnvelope,
    fence: Option<AdmittedRouteEffectFence>,
) -> Result<Box<MetadataCommandEnvelope>, BuiltPendingCommandInstallFailure> {
    let mut budget = RequestWorkBudget::new(Duration::from_secs(30), None);
    match publisher {
        Publisher::Allocator => {
            let outcome = cluster.install_allocator_cleanup_built_metadata_command(
                crate::metadata_command::metadata_command_publisher!(ReservePutObjectGeneration),
                command.id().pg_id(),
                command.bucket_name(),
                fence,
                &mut budget,
                || Ok(command.clone()),
            )?;
            let AllocatorCleanupFreshInstallOutcome::Installed(command) = outcome else {
                panic!("test never introduces a contender");
            };
            Ok(command)
        }
        Publisher::ApplyValidated => {
            let outcome = cluster.install_apply_validated_built_metadata_command(
                crate::metadata_command::metadata_command_publisher!(CommitStreamSegmentAppend),
                command.id().pg_id(),
                command.bucket_name(),
                fence,
                &budget,
                || Ok(command.clone()),
            )?;
            let ApplyValidatedFreshInstallOutcome::Installed(command) = outcome else {
                panic!("test never introduces a contender");
            };
            Ok(command)
        }
    }
}

fn connection_pair(listener: &UnixListener, backend: &std::path::Path) -> (UnixStream, UnixStream) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let front = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "proxy request did not arrive");
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept proxy request: {error}"),
        }
    };
    front
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    front
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let back = UnixStream::connect(backend).unwrap();
    back.set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    back.set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    (front, back)
}

fn exchange(
    listener: UnixListener,
    backend: std::path::PathBuf,
    expected: MetadataCommandEnvelope,
    delivery: Delivery,
    exchanges: Arc<AtomicUsize>,
) {
    listener.set_nonblocking(true).unwrap();
    let (mut front, mut back) = connection_pair(&listener, &backend);

    let inspection = read_storage_rpc_frame_from(&mut front).unwrap();
    assert_eq!(
        inspection.kind,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope
    );
    exchanges.fetch_add(1, Ordering::SeqCst);
    write_storage_rpc_frame_to(&mut back, &inspection).unwrap();
    let response = read_storage_rpc_frame_from(&mut back).unwrap();
    write_storage_rpc_frame_to(&mut front, &response).unwrap();

    // Ordinary metadata RPCs open one connection per request; they are not a
    // persistent critical-section session. Model that actual transport lifecycle.
    drop((front, back));
    let (mut front, mut back) = connection_pair(&listener, &backend);

    let request = read_storage_rpc_frame_from(&mut front).unwrap();
    assert_eq!(
        request.kind,
        StorageRpcMessageKind::MetadataCommandPendingSlotInsert
    );
    exchanges.fetch_add(1, Ordering::SeqCst);
    let decoded = decode_metadata_command_pending_slot_request(
        &request.payload,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(decoded.command, expected);
    assert!(
        decoded.operation_deadline.is_some(),
        "must retain the caller's budget on the wire"
    );
    match delivery {
        Delivery::DropBeforeDispatch => return,
        Delivery::Reject => {
            let mut response = request;
            response.payload = encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "deliberate pre-install rejection".into(),
            })
            .unwrap();
            write_storage_rpc_frame_to(&mut front, &response).unwrap();
            return;
        }
        _ => {}
    }
    write_storage_rpc_frame_to(&mut back, &request).unwrap();
    let mut response = read_storage_rpc_frame_from(&mut back).unwrap();
    let payload = decode_storage_rpc_response_payload(&response.payload)
        .unwrap()
        .unwrap();
    let inserted = decode_metadata_command_pending_slot_insert_response(&payload).unwrap();
    assert_eq!(
        inserted.outcome,
        StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted
    );
    match delivery {
        Delivery::DropAfterInsertion => return,
        Delivery::MalformedReply => response.payload = encode_storage_rpc_success_response(&[255]),
        Delivery::Healthy => {}
        Delivery::Reject | Delivery::DropBeforeDispatch => unreachable!(),
    }
    write_storage_rpc_frame_to(&mut front, &response).unwrap();
}

fn check_delivery(delivery: Delivery, publisher: Publisher, fenced: bool) {
    let (_serial, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let epoch = ClusterEpoch::INITIAL;
    let ec = EcShape { k: 1, m: 0 };
    let backend = tmp.path().join("sockets/backend.sock");
    let proxy = tmp.path().join("sockets/proxy.sock");
    private_socket_dir(backend.parent().unwrap());
    let config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: tmp.path().join("backend"),
        default_ec_shape: ec,
        pg_ids: vec![0],
        socket_path: backend.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: epoch,
            state: PgState::Active,
            primary_node_id: node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![node_id],
        }],
        historical_pg_routes: vec![],
        pending_metadata_command_recoveries: vec![],
    };
    let server = StorageNodeServer::bind(config).unwrap();
    let remote = server.test_storage_node();
    let _server = spawn_storage_node_server(server);
    let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        node_id,
        [node_id],
        &[0],
        ec,
        epoch,
    )
    .unwrap();
    map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
        node_id, &proxy,
    )])
    .unwrap();
    let cluster = StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    // This test covers generic installation, never apply or payload semantics.
    // Reserve is a canonical object-PG command with no prerequisite shard I/O.
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            crate::tests::bucket_name("built-install"),
            crate::tests::object_key("key"),
            crate::tests::stream_session_id("built-install"),
            GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    let positions = || {
        let pg = remote.get_pg(0).unwrap();
        (
            pg.metadata_command_replica_state().unwrap(),
            pg.max_metadata_command_log_index(epoch).unwrap(),
        )
    };
    let before = positions();
    let exchanges = Arc::new(AtomicUsize::new(0));
    let observed_exchanges = Arc::clone(&exchanges);
    let expected = command.clone();
    let listener = UnixListener::bind(proxy).unwrap();
    let proxy_thread =
        thread::spawn(move || exchange(listener, backend, expected, delivery, observed_exchanges));
    let fence = fenced.then(|| {
        AdmittedRouteEffectFence::bounded(
            epoch,
            crate::clock::current_time_millis() + 30_000,
            crate::clock::monotonic_time_millis() + 30_000,
        )
    });
    let result = install(&cluster, publisher, &command, fence);
    proxy_thread.join().unwrap();
    assert_eq!(exchanges.load(Ordering::SeqCst), 2);
    let expected_pending = match delivery {
        Delivery::Healthy => {
            assert_eq!(*result.unwrap(), command);
            Some(command.clone())
        }
        Delivery::Reject => {
            let BuiltPendingCommandInstallFailure::Rejected {
                command: actual,
                error,
            } = result.unwrap_err()
            else {
                panic!("definite rejection lost its candidate/classification");
            };
            assert_eq!(*actual, command);
            assert_eq!(error.kind(), MetadataCommandApplyErrorKind::Definitive);
            let StoreError::StorageRpc { detail, .. } = error.source() else {
                panic!("expected the peer's definite rejection: {error:?}");
            };
            assert_eq!(detail.as_str(), "deliberate pre-install rejection");
            None
        }
        Delivery::DropBeforeDispatch | Delivery::DropAfterInsertion | Delivery::MalformedReply => {
            let BuiltPendingCommandInstallFailure::Unconfirmed {
                command: actual,
                error,
            } = result.unwrap_err()
            else {
                panic!("ambiguous installation lost its exact candidate/classification");
            };
            assert_eq!(*actual, command);
            assert_eq!(error.kind(), MetadataCommandApplyErrorKind::MayHaveApplied);
            if matches!(delivery, Delivery::DropBeforeDispatch) {
                None
            } else {
                Some(command.clone())
            }
        }
    };
    assert_eq!(
        remote
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(node_id.as_u32(), epoch)
            .unwrap(),
        expected_pending
    );
    assert_eq!(
        positions(),
        before,
        "installation must not drain/apply on uncertainty"
    );
}

#[test]
fn built_install_unix_preserves_exact_candidate_for_lost_and_malformed_replies() {
    for publisher in [Publisher::Allocator, Publisher::ApplyValidated] {
        for fenced in [false, true] {
            for delivery in [
                Delivery::DropBeforeDispatch,
                Delivery::DropAfterInsertion,
                Delivery::MalformedReply,
            ] {
                check_delivery(delivery, publisher, fenced);
            }
        }
    }
}

#[test]
fn built_install_unix_distinguishes_success_and_definite_rejection() {
    for publisher in [Publisher::Allocator, Publisher::ApplyValidated] {
        for fenced in [false, true] {
            for delivery in [Delivery::Healthy, Delivery::Reject] {
                check_delivery(delivery, publisher, fenced);
            }
        }
    }
}
