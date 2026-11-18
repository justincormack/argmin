// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated candidate publication; not frontend activation.

use super::super::complete_log_rpc::{prepared, Running};
use super::transport::encode_at;
use super::*;
use crate::storage_rpc::{
    self as wire, StorageRpcErrorCode as Code, StorageRpcMessageKind as Kind,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandPublicationStart;

#[test]
fn staging_complete_publication_rpc_rejects_nonprimary_peering_unadmitted_historical_and_unselected_routes(
) {
    for mode in ["replica", "peering", "historical", "unselected"] {
        let dir = test_util::tempdir();
        let mut config = config(0);
        if mode == "historical" {
            config.historical_pg_routes = config.pg_routes.clone();
            config.cluster_epoch = ClusterEpoch::new(6).unwrap();
        }
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
            if mode == "replica" {
                route.primary_node_id = NodeId::new(2);
            }
            if mode == "peering" {
                route.state = PgState::Peering;
            }
        }
        let (server, client, config) = installed_config(&dir, config, mode != "unselected");
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let payload = encode_at(&context, crate::clock::current_time_millis() + 60_000);
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
        let _running = Running::start(server, config.socket_path);
        let error = client
            .rpc_request_result(KIND, payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            if mode == "unselected" {
                Code::PayloadDecode
            } else if mode == "peering" || mode == "historical" {
                Code::InactivePgRoute
            } else if mode == "replica" {
                Code::NonActingSetAccess
            } else {
                Code::StaleShardLocation
            },
            "{mode}: {error:?}"
        );
        if mode == "historical" {
            // Historical publication is now separately admitted. This fixture
            // has a current Active route, not the required recovery Peering
            // route; rejection must occur before considering command authority.
            assert!(error.message.contains("requires a retained Peering route"));
        }
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
}

#[test]
fn staging_complete_publication_rpc_contexts_exact_retry_apply_and_restart() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, client, config) = installed_config(&dir, config(case), true);
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
        let running = Running::start(server, config.socket_path.clone());
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
            let pg = node.get_pg(pg_id.get()).unwrap();
            assert!(started(&pg));
            assert_eq!(without_marker(&pg), before);
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
        drop(running);
        drop(node);
        let (prepared, client) = prepared(&config);
        let server = prepared.bind_complete_for_test().unwrap();
        let node = server.test_storage_node();
        let _running = Running::start(server, config.socket_path);
        let session = client
            .open_metadata_command_critical_section_until(pg_id, deadline())
            .unwrap();
        let reply = session
            .test_complete_publication_payload_until(payload, deadline())
            .unwrap();
        assert_eq!(
            transport.decode_confirmation(&reply).unwrap().id(),
            context.id()
        );
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), applied);
    }
}

#[test]
fn staging_complete_publication_rpc_rejects_malformed_crossed_and_unlocked_requests() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let payload = encode_at(&context, crate::clock::current_time_millis() + 60_000);
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
    let _running = Running::start(server, config.socket_path);
    let unlocked = client
        .rpc_request_result(KIND, payload.clone())
        .unwrap()
        .unwrap_err();
    assert_eq!(unlocked.code, Code::StaleShardLocation);
    {
        let wrong = client
            .open_metadata_command_critical_section_until(
                PgId::new((pg_id.get() + 1) % 8),
                deadline(),
            )
            .unwrap();
        assert!(wrong
            .test_complete_publication_payload_until(payload.clone(), deadline())
            .is_err());
    }
    let session = client
        .open_metadata_command_critical_section_until(pg_id, deadline())
        .unwrap();
    for offset in [0, 10, 14, 22, 26, 65] {
        let mut crossed = payload.clone();
        crossed[offset] ^= 1;
        assert!(
            session
                .test_complete_publication_payload_until(crossed, deadline())
                .is_err(),
            "offset {offset}"
        );
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
    let old = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_applied_rpc_v37_command_v14_context_v1.hex"
    ));
    assert!(session
        .test_complete_publication_payload_until(old, deadline())
        .is_err());
    let mut trailing = payload.clone();
    trailing.push(0);
    assert!(session
        .test_complete_publication_payload_until(trailing, deadline())
        .is_err());
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    assert_eq!(
        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
        writes
    );
    let reply = session
        .test_complete_publication_payload_until(payload, deadline())
        .unwrap();
    assert_eq!(
        wire::complete_publication::decode_confirmation(&reply)
            .unwrap()
            .id,
        context.id()
    );
}

#[test]
fn staging_complete_publication_rpc_wire_expiry_after_decode_is_not_renewed() {
    for delayed in [false, true] {
        let _clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let (server, client, _) = installed_config(&dir, config(0), true);
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
                .test_complete_publication_payload_until(payload, deadline())
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: Code::TransportTimeout,
                        ..
                    }
                ),
                "{error:?}"
            );
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

#[test]
fn staging_complete_publication_rpc_crossed_confirmation_does_not_erase_committed_marker() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let transport = request.prepare_transport(deadline());
    let payload = transport.encode_request().unwrap();
    let injected = Arc::new(AtomicBool::new(false));
    let hook = Arc::clone(&injected);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind == KIND && !hook.swap(true, Ordering::SeqCst) {
            let mut body = wire::decode_storage_rpc_response_payload(&frame.payload)
                .unwrap()
                .unwrap();
            body[26] ^= 1; // Complete command digest; response is authenticated afterward.
            frame.payload = wire::encode_storage_rpc_success_response(&body);
        }
    }));
    let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
    let _running = Running::start(server, config.socket_path);
    {
        let session = client
            .open_metadata_command_critical_section_until(pg_id, deadline())
            .unwrap();
        let reply = session
            .test_complete_publication_payload_until(payload.clone(), deadline())
            .unwrap();
        assert!(transport.decode_confirmation(&reply).is_err());
    }
    assert!(injected.load(Ordering::SeqCst));
    assert!(started(&node.get_pg(pg_id.get()).unwrap()));
    assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
    let session = client
        .open_metadata_command_critical_section_until(pg_id, deadline())
        .unwrap();
    let reply = session
        .test_complete_publication_payload_until(payload, deadline())
        .unwrap();
    assert_eq!(
        transport.decode_confirmation(&reply).unwrap().id(),
        context.id()
    );
    assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
}
