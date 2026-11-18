// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real TLS transport for the explicitly selected candidate publication path.
//! Dispatch classification is tested here, not frontend custody/activation.

use super::super::complete_log_rpc::authenticated_server;
use super::transport::encode_at;
use super::*;
use crate::node_runtime::clients::{
    LocalUnixStorageNodeClientAdmissionSettings, MetadataCommandApplyErrorKind,
    UnixStorageNodeClient,
};
use crate::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeProcessConfig, StorageNodeRpcListenerConfig,
    StorageNodeServer,
};
use crate::storage_rpc::{StorageRpcErrorCode as Code, StorageRpcMessageKind as Kind};
use crate::storage_rpc_transport::{self as transport, StorageRpcClientEndpoint};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandPublicationStart;

fn tls_server(
    config: StorageNodeProcessConfig,
    reopen: bool,
) -> (StorageNodeServer, UnixStorageNodeClient) {
    let epoch = config.cluster_epoch;
    tls_server_at(config, reopen, epoch)
}

pub(super) fn tls_server_at(
    config: StorageNodeProcessConfig,
    reopen: bool,
    client_epoch: ClusterEpoch,
) -> (StorageNodeServer, UnixStorageNodeClient) {
    let certificates = CertificateDer::pem_slice_iter(include_bytes!(
        "../../../s3-tests/testdata/localhost-cert.pem"
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let key = PrivateKeyDer::from_pem_slice(include_bytes!(
        "../../../s3-tests/testdata/localhost-key.pem"
    ))
    .unwrap();
    let certified = Arc::new(
        rustls::sign::CertifiedKey::from_der(certificates, key, &tls_provider::build_provider())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(
            CertificateDer::pem_slice_iter(include_bytes!(
                "../../../s3-tests/testdata/ca-cert.pem"
            ))
            .next()
            .unwrap()
            .unwrap(),
        )
        .unwrap();
    let (prepared, auth) = authenticated_server(PreparedStorageNodeServer::new(config.clone()));
    let prepared = prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp(
        "127.0.0.1:0".parse().unwrap(),
        certified,
    )
    .unwrap()]);
    let server = if reopen {
        prepared.bind_complete_for_test().unwrap()
    } else {
        let mut server = prepared.bind().unwrap();
        server.select_complete_log_operations_for_test();
        server
    };
    let address = server.tcp_listener_addr_for_test();
    let endpoint = StorageRpcClientEndpoint::tcp_with_config(
        format!("tcp://localhost:{}", address.port()),
        vec![address],
        "localhost",
        transport::storage_rpc_tls_client_config(Arc::new(roots)).unwrap(),
    )
    .unwrap();
    let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
        config.node_id,
        client_epoch,
        endpoint,
        LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        Some(auth),
    )
    .with_pg_topology(Arc::new(topology()));
    (server, client)
}

pub(super) fn installed_tls(
    dir: &test_util::TempDir,
    case: usize,
) -> (
    StorageNodeServer,
    UnixStorageNodeClient,
    StorageNodeProcessConfig,
) {
    let mut config = config(case);
    config.data_dir = dir.path().join("node");
    // TLS is the only listener; this path is not used as a fallback endpoint.
    config.socket_path = dir.path().join("unused.sock");
    let (server, client) = tls_server(config.clone(), false);
    (server, client, config)
}

#[test]
fn staging_complete_publication_tls_contexts_retry_apply_and_restart() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, client, config) = installed_tls(&dir, case);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let transport = request.prepare_transport(deadline());
        let payload = transport.encode_request().unwrap();
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        let applied = std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            for _ in 0..2 {
                let reply = session
                    .test_complete_publication_payload_until(payload.clone(), deadline())
                    .unwrap();
                assert_eq!(
                    transport.decode_confirmation(&reply).unwrap().id(),
                    context.id()
                );
                assert!(started(&node.get_pg(pg_id.get()).unwrap()));
                assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
            }
            apply(&node.get_pg(pg_id.get()).unwrap(), &context);
            let applied = snapshot(&node.get_pg(pg_id.get()).unwrap());
            let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
            let reply = session
                .test_complete_publication_payload_until(payload.clone(), deadline())
                .unwrap();
            assert_eq!(
                transport.decode_confirmation(&reply).unwrap().id(),
                context.id()
            );
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), applied);
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes
            );
            drop(session);
            worker.join().unwrap();
            applied
        });
        drop(client);
        drop(server);
        drop(node);
        let (server, client) = tls_server(config, true);
        let node = server.test_storage_node();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
            let reply = session
                .test_complete_publication_payload_until(payload, deadline())
                .unwrap();
            assert_eq!(
                transport.decode_confirmation(&reply).unwrap().id(),
                context.id()
            );
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), applied);
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes
            );
            drop(session);
            worker.join().unwrap();
        });
    }
}

#[test]
fn staging_complete_publication_tls_expiry_before_arrival_and_after_decode_does_not_mutate() {
    for delayed in [false, true] {
        // Different frontend and server wall origins; the wire cutoff is still
        // absolute. Decode must not renew it into a duration at a later layer.
        let _clock = crate::clock::test_time_override_guard(9_000);
        let dir = test_util::tempdir();
        let (server, client, _) = installed_tls(&dir, 0);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let payload = encode_at(&context, if delayed { 11_000 } else { 9_999 });
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let _clock = crate::clock::test_time_override_guard(10_000);
                if delayed {
                    server.test_accept_one_expiring_publication().unwrap();
                } else {
                    server.accept_one().unwrap();
                }
            });
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            let error = session
                .test_complete_publication_dispatch_until(payload, deadline())
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, Code::TransportTimeout, "{error:?}");
            drop(session);
            worker.join().unwrap();
        });
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
        assert!(!started(&node.get_pg(pg_id.get()).unwrap()));
    }
}

#[derive(Clone, Copy, Debug)]
enum ReplyFault {
    Disconnect,
    Authentication,
    CrossedConfirmation,
}

fn uncertain_reply(tls: bool, fault: ReplyFault) {
    // All context forms must preserve their exact source/descriptor when the
    // marker committed but its confirmation cannot be trusted.
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, client, config) = if tls {
            installed_tls(&dir, case)
        } else {
            installed_config(&dir, config(case), true)
        };
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let transport = request.prepare_transport(deadline());
        let payload = transport.encode_request().unwrap();
        let replies = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&replies);
        server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind == KIND
                && observed.fetch_add(1, Ordering::SeqCst) == 0
                && matches!(fault, ReplyFault::CrossedConfirmation)
            {
                let mut reply =
                    crate::storage_rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                reply[26] ^= 1; // Authenticated, but not the requested full-context SHA.
                frame.payload = crate::storage_rpc::encode_storage_rpc_success_response(&reply);
            }
        }));
        if matches!(fault, ReplyFault::Authentication) {
            let observed = Arc::clone(&replies);
            server.set_response_envelope_test_hook(Arc::new(move |kind, envelope| {
                if kind == KIND && observed.load(Ordering::SeqCst) == 1 {
                    *envelope.last_mut().unwrap() ^= 1;
                }
            }));
        }
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let materialized = without_marker(&node.get_pg(pg_id.get()).unwrap());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                if matches!(fault, ReplyFault::Disconnect) {
                    server.test_accept_one_losing_publication_reply().unwrap();
                } else {
                    server.accept_one().unwrap();
                }
            });
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
            let error = session
                .test_complete_publication_dispatch_until(payload.clone(), Instant::now())
                .unwrap_err();
            assert_eq!(error.kind(), MetadataCommandApplyErrorKind::NotSent);
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes
            );
            assert_eq!(replies.load(Ordering::SeqCst), 0);
            let result =
                session.test_complete_publication_dispatch_until(payload.clone(), deadline());
            match fault {
                ReplyFault::Disconnect | ReplyFault::Authentication => {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error.kind(),
                        MetadataCommandApplyErrorKind::MayHaveApplied,
                        "{fault:?}: {error:?}"
                    );
                }
                ReplyFault::CrossedConfirmation => {
                    assert!(transport
                        .decode_confirmation(&result.unwrap().unwrap())
                        .is_err());
                }
            }
            drop(session);
            worker.join().unwrap();
        });
        assert_eq!(replies.load(Ordering::SeqCst), 1, "no automatic retry");
        assert!(started(&node.get_pg(pg_id.get()).unwrap()));
        assert_eq!(
            without_marker(&node.get_pg(pg_id.get()).unwrap()),
            materialized
        );
        // Drop the failed session. The retained holder/cutoff, not a freshly
        // minted context, is used for an explicit exact retry on a new session.
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            let retry = request.prepare_transport(deadline());
            assert_eq!(retry.encode_request().unwrap(), payload);
            let reply = session
                .test_complete_publication_dispatch_until(payload, deadline())
                .unwrap()
                .unwrap();
            assert_eq!(
                retry.decode_confirmation(&reply).unwrap().id(),
                context.id()
            );
            assert!(started(&node.get_pg(pg_id.get()).unwrap()));
            assert_eq!(
                without_marker(&node.get_pg(pg_id.get()).unwrap()),
                materialized
            );
            drop(session);
            worker.join().unwrap();
        });
        assert_eq!(replies.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn staging_complete_publication_tls_lost_reply_is_uncertain_and_exact_retry_confirms() {
    uncertain_reply(true, ReplyFault::Disconnect);
}

#[test]
fn staging_complete_publication_unix_lost_reply_is_uncertain_and_exact_retry_confirms() {
    uncertain_reply(false, ReplyFault::Disconnect);
}

#[test]
fn staging_complete_publication_tls_bad_authentication_does_not_resolve_committed_marker() {
    uncertain_reply(true, ReplyFault::Authentication);
}

#[test]
fn staging_complete_publication_tls_crossed_confirmation_does_not_resolve_committed_marker() {
    uncertain_reply(true, ReplyFault::CrossedConfirmation);
}
