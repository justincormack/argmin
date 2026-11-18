// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact historical admission on the installed candidate path. These tests do
//! not select the frontend publisher or the embedded historical recovery bridge.

use super::super::complete_log_rpc::authenticated_server;
use super::*;
use crate::control_plane::{PendingMetadataCommandObservation, PendingMetadataCommandRecovery};
use crate::node_runtime::clients::{
    LocalUnixStorageNodeClientAdmissionSettings, UnixStorageNodeClient,
};
use crate::node_runtime::pg_store::PublicationObservation;
use crate::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeProcessConfig, StorageNodeServer,
};
use crate::storage_rpc::{StorageRpcErrorCode, StorageRpcMessageKind};
use std::num::NonZeroU64;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(super) fn historical_config(
    mut config: StorageNodeProcessConfig,
    context: &ContextualCommand,
) -> StorageNodeProcessConfig {
    let id = context.id();
    assert_eq!(id.cluster_epoch(), config.cluster_epoch);
    config.historical_pg_routes = config.pg_routes.clone();
    config.cluster_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
    config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis() + 60_000).unwrap();
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
        route.state = PgState::Peering;
    }
    config.pending_metadata_command_recoveries = vec![(
        id.pg_id(),
        PendingMetadataCommandRecovery::new(
            config.node_id,
            PendingMetadataCommandObservation::new(
                id.cluster_epoch(),
                NonZeroU64::new(id.log_index().get()).unwrap(),
                context.encode_command().unwrap().checksum_crc64(),
                context.encode_command().unwrap().command_sha256(),
            ),
        ),
    )];
    config
}

pub(super) fn reopen(
    config: &StorageNodeProcessConfig,
    epoch: ClusterEpoch,
    tls: bool,
) -> (StorageNodeServer, UnixStorageNodeClient) {
    if tls {
        return super::tls::tls_server_at(config.clone(), true, epoch);
    }
    let (prepared, auth) = authenticated_server(PreparedStorageNodeServer::new(config.clone()));
    let client = UnixStorageNodeClient::with_rpc_admission_settings_and_auth(
        config.node_id,
        epoch,
        &config.socket_path,
        LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        Some(auth),
    )
    .with_pg_topology(Arc::new(topology()));
    let server = prepared.bind_complete_for_test().unwrap_or_else(|error| {
        if let crate::storage_node_server::StorageNodeServerError::Store(failure) = &error {
            panic!("historical reopen: {:?}", failure.retained_store_error());
        }
        panic!("historical reopen: {error:?}");
    });
    (server, client)
}

fn setup(dir: &test_util::TempDir, case: usize) -> (StorageNodeProcessConfig, ContextualCommand) {
    let (server, _, config) = installed_config(dir, config(case), true);
    let node = server.test_storage_node();
    let pg = fixture(StagingTarget::DirectPut).subject.object_pg;
    if case == 1 {
        // The introduction is position 1. Anchor its empty epoch before any
        // pending work, as real epoch setup must; startup rejects future slots.
        let pg = node.get_pg(pg.get()).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_empty_state(4, config.cluster_epoch, digest)
            .unwrap();
    }
    let context = context(&node.get_pg(pg.get()).unwrap(), case);
    install(&node.get_pg(pg.get()).unwrap(), &context);
    // Only copied configuration/context survives. No second runtime opens the
    // same SQLite files while the source runtime remains live.
    drop(node);
    drop(server);
    (historical_config(config, &context), context)
}

fn expect_rejection(
    server: &StorageNodeServer,
    code: StorageRpcErrorCode,
    message: &'static str,
) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    // Observe the actual server result before client-side diagnostic redaction.
    // No response replacement or synthetic classification is involved.
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind == StorageRpcMessageKind::MetadataCommandPublicationStart {
            let error = crate::storage_rpc::decode_storage_rpc_response_payload(&frame.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, code, "{error:?}");
            assert!(error.message.contains(message), "{error:?}");
            seen.fetch_add(1, Ordering::SeqCst);
        }
    }));
    count
}

fn historical_roundtrip(tls: bool) {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (config, context) = setup(&dir, case);
        let id = context.id();
        let (server, client) = reopen(&config, id.cluster_epoch(), tls);
        let node = server.test_storage_node();
        let mut sender = pending(&context).into_publication_sender(config.node_id, deadline());
        let before = without_marker(&node.get_pg(id.pg_id().get()).unwrap());
        assert!(!started(&node.get_pg(id.pg_id().get()).unwrap()));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.test_accept_one_losing_publication_reply().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            assert!(sender.send_remote(&session, deadline()).is_err());
            assert_eq!(sender.observation(), PublicationObservation::Unconfirmed);
            drop(session);
            worker.join().unwrap();
        });
        assert!(started(&node.get_pg(id.pg_id().get()).unwrap()));
        assert_eq!(
            without_marker(&node.get_pg(id.pg_id().get()).unwrap()),
            before
        );
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            sender.send_remote(&session, deadline()).unwrap();
            assert_eq!(
                sender.observation(),
                PublicationObservation::MarkerConfirmed
            );
            assert_eq!(
                without_marker(&node.get_pg(id.pg_id().get()).unwrap()),
                before
            );
            apply(&node.get_pg(id.pg_id().get()).unwrap(), &context);
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let applied = snapshot(&pg);
            let writes = pg.conn.total_changes();
            drop(pg);
            sender.send_remote(&session, deadline()).unwrap();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!(snapshot(&pg), applied);
            assert_eq!(pg.conn.total_changes(), writes);
            drop(pg);
            drop(session);
            worker.join().unwrap();
        });
        let applied = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        drop(node);
        drop(client);
        drop(server);
        let (server, client) = reopen(&config, id.cluster_epoch(), tls);
        let node = server.test_storage_node();
        let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            sender.send_remote(&session, deadline()).unwrap();
            drop(session);
            worker.join().unwrap();
        });
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!(snapshot(&pg), applied);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_publication_historical_unix_exact_retry_apply_and_restart() {
    historical_roundtrip(false);
}

#[test]
fn staging_complete_publication_historical_tls_exact_retry_apply_and_restart() {
    historical_roundtrip(true);
}

#[test]
fn staging_complete_publication_historical_rejects_inexact_map_authorization_before_effects() {
    for tls in [false, true] {
        for mode in ["checksum", "sha256", "index", "reporter", "missing"] {
            let dir = test_util::tempdir();
            let (mut config, context) = setup(&dir, 1);
            let id = context.id();
            let recovery = config.pending_metadata_command_recoveries[0].1;
            config.pending_metadata_command_recoveries[0].1 = PendingMetadataCommandRecovery::new(
                if mode == "reporter" {
                    NodeId::new(2)
                } else {
                    config.node_id
                },
                PendingMetadataCommandObservation::new(
                    id.cluster_epoch(),
                    NonZeroU64::new(id.log_index().get() + u64::from(mode == "index")).unwrap(),
                    recovery.pending().command_checksum() ^ u64::from(mode == "checksum"),
                    {
                        let mut sha = recovery.pending().command_sha256();
                        sha[0] ^= u8::from(mode == "sha256");
                        sha
                    },
                ),
            );
            if mode == "missing" {
                config.pending_metadata_command_recoveries.clear();
            }
            if mode == "reporter" {
                config
                    .historical_pg_routes
                    .iter_mut()
                    .find(|route| route.pg_id == id.pg_id().get())
                    .unwrap()
                    .primary_node_id = NodeId::new(2);
            }
            let (server, client) = reopen(&config, id.cluster_epoch(), tls);
            let seen = expect_rejection(
                &server,
                StorageRpcErrorCode::StaleShardLocation,
                if matches!(mode, "checksum" | "sha256" | "index") {
                    "exact historical pending-command authorization"
                } else {
                    "not authorized for primary"
                },
            );
            let node = server.test_storage_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
            let payload =
                transport::encode_at(&context, crate::clock::current_time_millis() + 30_000);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| server.accept_one().unwrap());
                if matches!(mode, "checksum" | "sha256" | "index") {
                    // PG/epoch section admission succeeds. The exact command
                    // check, not a missing lock, must reject publication.
                    let session = client
                        .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                        .unwrap();
                    assert!(session
                        .test_complete_publication_payload_until(payload, deadline())
                        .is_err());
                    drop(session);
                } else {
                    let error = client
                        .rpc_request_result(
                            StorageRpcMessageKind::MetadataCommandPublicationStart,
                            payload,
                        )
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        StorageRpcErrorCode::StaleShardLocation,
                        "{mode}: {error:?}"
                    );
                    assert!(
                        error.message.contains("not authorized for primary"),
                        "{mode}: {error:?}"
                    );
                }
                worker.join().unwrap();
            });
            assert_eq!(seen.load(Ordering::SeqCst), 1);
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!(snapshot(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_publication_historical_requires_exact_slot_not_just_map_report() {
    use crate::metadata_command::{
        MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
    };
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let (mut config, original) = setup(&dir, 0);
        let id = original.id();
        let owner = fixture(StagingTarget::DirectPut);
        let other = ContextualCommand::decode_field(
            MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    owner.subject.bucket,
                    owner.subject.key,
                    crate::VersionId::from_u64(18),
                )),
            ),
            &[1, 0, 0],
            &topology(),
        )
        .unwrap();
        assert_ne!(
            original.encode_command().unwrap().checksum_crc64(),
            other.encode_command().unwrap().checksum_crc64()
        );
        config.pending_metadata_command_recoveries[0].1 = PendingMetadataCommandRecovery::new(
            config.node_id,
            PendingMetadataCommandObservation::new(
                id.cluster_epoch(),
                NonZeroU64::new(id.log_index().get()).unwrap(),
                other.encode_command().unwrap().checksum_crc64(),
                other.encode_command().unwrap().command_sha256(),
            ),
        );
        let (server, client) = reopen(&config, id.cluster_epoch(), tls);
        let seen = expect_rejection(
            &server,
            StorageRpcErrorCode::Internal,
            "pending-staging-install-existing-scope",
        );
        let node = server.test_storage_node();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
        let mut sender = pending(&other).into_publication_sender(config.node_id, deadline());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            assert!(sender.send_remote(&session, deadline()).is_err());
            assert_eq!(sender.observation(), PublicationObservation::Unconfirmed);
            drop(session);
            worker.join().unwrap();
        });
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_publication_historical_revalidates_captured_route_at_effects() {
    for stage in ["before-trial", "before-marker", "before-commit"] {
        let clock = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let (mut config, context) = setup(&dir, 1);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        let id = context.id();
        let (server, _) = reopen(&config, id.cluster_epoch(), false);
        let node = server.test_storage_node();
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        server.test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            let mut reached = false;
            let error = request.test_start_with_hook(pg, authority, |at| {
                if at == stage { reached = true; clock.set(6_000); }
                Ok(())
            }).unwrap_err();
            assert!(reached, "{stage}");
            assert!(matches!(error.into_store_error().unwrap(), StoreError::RouteMapExpired { cluster_epoch, valid_until_ms: 5_000, .. } if cluster_epoch == id.cluster_epoch()));
            assert_eq!(snapshot(pg), before);
            assert!(!started(pg));
            assert!(pg.conn.is_autocommit());
        }).unwrap();
    }
}

#[test]
fn staging_complete_publication_historical_requires_exact_section_and_active_admission_domain() {
    let dir = test_util::tempdir();
    let (config, context) = setup(&dir, 1);
    let id = context.id();
    let (server, _) = reopen(&config, id.cluster_epoch(), false);
    let node = server.test_storage_node();
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
    let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
    server.test_complete_publication_rejects_wrong_sections(&request);
    let foreign_dir = test_util::tempdir();
    let (foreign, _, _) = installed_config(&foreign_dir, super::config(1), true);
    for retained in [false, true] {
        for draining in [false, true] {
            server.test_complete_publication_rejects_wrong_permit(
                &foreign, &request, retained, draining,
            );
        }
    }
    let pg = node.get_pg(id.pg_id().get()).unwrap();
    assert_eq!(snapshot(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}
