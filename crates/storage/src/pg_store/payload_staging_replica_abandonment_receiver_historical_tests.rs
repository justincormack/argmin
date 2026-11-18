// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::super::super::replica_abandonment_receiver::{confirm, encode};
use super::*;
use crate::storage_rpc::StorageRpcErrorCode;

#[test]
fn staging_replica_abandonment_receiver_historical_unix_tls_replay_after_reopen() {
    for tls in [false, true] {
        for case in 0..4 {
            let mixed = Mixed::new(case, &[(0, tls)]);
            let peer = mixed.peer(0);
            let context = &mixed.fixture.context;
            let node = mixed.node(0);
            install(&node.get_pg(context.id().pg_id().get()).unwrap(), context);
            let retained = {
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                (introduction::state(&pg), complete_pending::observe(&pg))
            };
            let before = mixed.observe();
            let shadows = mixed.shadows();
            {
                let _running = peer.run();
                let bytes = encode(
                    context,
                    NodeId::new(4),
                    peer.id,
                    crate::clock::current_time_millis() + 60_000,
                );
                let response = peer
                    .client
                    .test_replica_abandonment_request_until(bytes, deadline())
                    .unwrap()
                    .unwrap();
                confirm(&response, context, peer.id);
            }
            assert_ne!(mixed.observe()[0], before[0]);
            assert_eq!(mixed.shadows(), shadows);
            drop(node);
            let mixed = lifetime::reopen(mixed);
            let before_retry = mixed.observe();
            let peer = mixed.peer(0);
            let _running = peer.run();
            let context = &mixed.fixture.context;
            let bytes = encode(
                context,
                NodeId::new(4),
                peer.id,
                crate::clock::current_time_millis() + 60_000,
            );
            let response = peer
                .client
                .test_replica_abandonment_request_until(bytes, deadline())
                .unwrap()
                .unwrap();
            confirm(&response, context, peer.id);
            assert_eq!(mixed.observe(), before_retry);
            let node = mixed.node(0);
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            assert_eq!(
                (introduction::state(&pg), complete_pending::observe(&pg)),
                retained
            );
        }
    }
}

#[test]
fn staging_replica_abandonment_receiver_historical_rejects_crossed_report_and_primary_role() {
    for tls in [false, true] {
        let mixed = Mixed::new(2, &[(0, tls), (4, !tls)]);
        let peer = mixed.peer(0);
        let context = &mixed.fixture.context;
        let before = mixed.observe();
        let mut config = peer.config.clone();
        let recovery = &config.pending_metadata_command_recoveries[0].1;
        let p = recovery.pending();
        let mut sha = p.command_sha256();
        sha[0] ^= 1;
        config.pending_metadata_command_recoveries[0].1 =
            crate::control_plane::PendingMetadataCommandRecovery::new(
                recovery.reporting_node_id(),
                PendingMetadataCommandObservation::new(
                    p.cluster_epoch(),
                    NonZeroU64::new(p.log_index()).unwrap(),
                    p.command_checksum(),
                    sha,
                ),
            );
        peer.server
            .install_control_plane_runtime_config(config)
            .unwrap();
        {
            let _running = peer.run();
            let bytes = encode(
                context,
                NodeId::new(4),
                peer.id,
                crate::clock::current_time_millis() + 60_000,
            );
            let error = peer
                .client
                .test_replica_abandonment_request_until(bytes, deadline())
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
            assert!(error
                .message
                .contains("exact historical pending-command authorization"));
        }
        assert_eq!(mixed.observe(), before);
        let primary = mixed.peer(4);
        let _running = primary.run();
        // Lie about the primary in order to pass framing's distinct-node check.
        let bytes = encode(
            context,
            NodeId::new(0),
            primary.id,
            crate::clock::current_time_millis() + 60_000,
        );
        let error = primary
            .client
            .test_replica_abandonment_request_until(bytes, deadline())
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(error.message.contains("distinct replica"));
        assert_eq!(mixed.observe(), before);
    }
}

#[test]
fn staging_replica_abandonment_receiver_historical_server_cutoff_survives_raw_renewal() {
    for boundary in ["before-tombstone", "before-commit"] {
        for tls in [false, true] {
            let mixed = Mixed::new(1, &[(0, tls)]);
            let peer = mixed.peer(0);
            let cutoff = crate::clock::current_time_millis() + 3_000;
            let mut config = peer.config.clone();
            config.route_map_validity = RouteMapValidity::until_ms(cutoff).unwrap();
            peer.server
                .install_control_plane_runtime_config(config.clone())
                .unwrap();
            let context = &mixed.fixture.context;
            let bytes = encode(context, NodeId::new(4), peer.id, cutoff + 60_000);
            let before = mixed.observe();
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let seen = Arc::clone(&reached);
            let server = Arc::clone(&peer.server);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    peer.server
                        .test_accept_one_replica_abandonment_hook(
                            move |at| {
                                if at == boundary {
                                    assert!(crate::clock::current_time_millis() < cutoff);
                                    seen.store(true, Ordering::SeqCst);
                                    config.route_map_validity =
                                        RouteMapValidity::until_ms(cutoff + 60_000).unwrap();
                                    server
                                        .install_control_plane_runtime_config(config.clone())
                                        .unwrap();
                                    while crate::clock::current_time_millis() <= cutoff {
                                        std::thread::sleep(Duration::from_millis(1));
                                    }
                                }
                            },
                            false,
                        )
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(peer));
                let error = peer
                    .client
                    .test_replica_abandonment_request_until(bytes, deadline())
                    .unwrap()
                    .unwrap_err();
                wake.disarm();
                worker.join().unwrap();
                assert!(
                    matches!(
                        error.code,
                        StorageRpcErrorCode::StaleShardLocation
                            | StorageRpcErrorCode::TransportTimeout
                    ),
                    "{error:?}"
                );
            });
            assert!(reached.load(Ordering::SeqCst));
            for ((after, changes), (before, old_changes)) in mixed.observe().iter().zip(&before) {
                assert_eq!(after, before);
                if boundary == "before-tombstone" {
                    assert_eq!(changes, old_changes);
                }
            }
        }
    }
}
