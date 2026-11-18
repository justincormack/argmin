// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Managed startup keeps complete custody through heartbeat, preparation and
//! bind. Metadata-only fixtures; no publication or physical cleanup is enabled.

use super::complete_log_range::apply;
use super::complete_log_rpc::{authenticated, installed_config, seed_context, Running};
use super::complete_node_startup::{install, next, reopen_raw, snapshot};
use super::*;
use crate::control_plane::{
    FileControlPlaneStore, NodeHeartbeat, NodeMembershipState, PendingMetadataCommandObservation,
    PgMetadataProof, SingleAuthorityControlPlane,
};
use crate::storage_node_server::{
    StorageNodeBootstrap, StorageNodeProcessConfig, StorageNodeServerError,
};
use crate::{PgState, StoreError};
use std::time::{Duration, Instant};

fn setup(
    dir: &test_util::TempDir,
    epoch: ClusterEpoch,
) -> (
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    StorageNodeProcessConfig,
) {
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        dir.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            (0..5)
                .map(|id| {
                    (
                        NodeId::new(id),
                        if id == 4 {
                            dir.path()
                                .join("socket/node.sock")
                                .to_str()
                                .unwrap()
                                .to_owned()
                        } else {
                            format!("/unused-bootstrap-node-{id}.sock")
                        },
                    )
                })
                .collect(),
            (0..8).map(PgId::new).collect(),
        )
        .unwrap();
    while authority.snapshot().cluster_epoch() < epoch {
        authority
            .set_node_membership(
                NodeId::new(100 + authority.snapshot().cluster_epoch().get() as u32),
                NodeMembershipState::Active,
            )
            .unwrap();
    }
    assert_eq!(authority.snapshot().cluster_epoch(), epoch);
    let map = authority.snapshot().runtime_map(1_000).unwrap();
    let config = StorageNodeProcessConfig::from_runtime_map(
        NodeId::new(4),
        dir.path().join("node"),
        EcShape { k: 2, m: 1 },
        &map,
    )
    .unwrap();
    (authority, config)
}

fn bootstrap(
    config: &StorageNodeProcessConfig,
) -> Result<StorageNodeBootstrap, StorageNodeServerError> {
    StorageNodeBootstrap::open_complete_for_test(
        config.node_id,
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
        &config.socket_path,
    )
}

fn assert_locked(config: &StorageNodeProcessConfig) {
    assert!(matches!(
        bootstrap(config),
        Err(StorageNodeServerError::DataDirAlreadyLocked { .. })
    ));
}

fn assert_pending(
    heartbeat: &NodeHeartbeat,
    pg_id: PgId,
    expected: PendingMetadataCommandObservation,
    state: &crate::metadata_command::MetadataCommandReplicaState,
) {
    let observed = heartbeat
        .pg_observations
        .iter()
        .find(|pg| pg.pg_id == pg_id)
        .expect("pending PG must be reported");
    assert_eq!(observed.pending_metadata_command, Some(expected));
    assert_eq!(observed.metadata_log_epoch, state.cluster_epoch);
    assert_eq!(
        observed.metadata_proof,
        PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        }
    );
}

#[test]
fn staging_complete_bootstrap_keeps_context_selection_and_pending_custody_through_prepare_bind_restart(
) {
    let _clock = crate::clock::test_time_override_guard(1_000);
    for case in 0..3 {
        for terminal in ["unresolved", "applied", "abandoned"] {
            let dir = test_util::tempdir();
            let epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
            let (authority, config) = setup(&dir, epoch);
            let (server, _, config) = installed_config(&dir, config, true);
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let (before, prefix, pending, routes) = {
                let pg = node.get_pg(pg_id.get()).unwrap();
                seed_context(&pg, case);
                let context = next(&pg, epoch);
                install(&pg, &context);
                match terminal {
                    "unresolved" => {}
                    "applied" => apply(&pg, &context),
                    "abandoned" => {
                        pg.test_record_complete_command_abandoned(
                            4,
                            &context.encode_command().unwrap(),
                        )
                        .unwrap();
                    }
                    _ => unreachable!(),
                }
                let encoded = context.encode_command().unwrap();
                (
                    snapshot(&pg),
                    pg.metadata_command_replica_state().unwrap(),
                    PendingMetadataCommandObservation::new(
                        epoch,
                        std::num::NonZeroU64::new(context.id().log_index().get()).unwrap(),
                        encoded.checksum_crc64(),
                        encoded.command_sha256(),
                    ),
                    pg.cluster_map_history_route_references(&topology())
                        .unwrap(),
                )
            };
            assert!(!routes.is_empty());
            config.persist_control_plane_runtime_config().unwrap();
            drop(node);
            drop(server);

            let opened = bootstrap(&config).unwrap();
            assert_eq!(opened.node_incarnation(), 1);
            assert_locked(&config);
            let heartbeat = opened.control_plane_heartbeat(10_000).unwrap();
            assert_eq!(heartbeat.observed_epoch, epoch);
            assert_eq!(heartbeat.cluster_map_history_route_references, routes);
            assert_pending(&heartbeat, pg_id, pending, &prefix);
            let map = authority.snapshot().runtime_map(1_000).unwrap();
            let prepared = opened.prepare(&map).unwrap();
            assert_locked(&config);
            // Ordinary bind must retain the bootstrap's private selection: no
            // second test-only override is allowed at this handoff.
            let (prepared, client) = authenticated(prepared);
            let server = prepared.bind().unwrap();
            assert_locked(&config);
            let heartbeat = server.control_plane_heartbeat(1, 10_000).unwrap();
            assert_pending(&heartbeat, pg_id, pending, &prefix);
            assert_eq!(heartbeat.cluster_map_history_route_references, routes);
            assert_eq!(
                snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
                before
            );
            let running = Running::start_at(server, config.socket_path.clone(), 1_000);
            // The selected installed read handler must understand the retained
            // complete applied prefix, not merely complete startup.
            client
                .complete_metadata_log_page_until(
                    pg_id,
                    epoch,
                    MetadataCommandLogIndex::new(1).unwrap(),
                    MetadataCommandLogIndex::new(prefix.applied_log_index).unwrap(),
                    Instant::now() + Duration::from_secs(10),
                )
                .unwrap_or_else(|error| {
                    if let StoreError::StorageRpc { detail, .. } = &error {
                        panic!("case {case}, {terminal}: {error:?}: {}", detail.as_str());
                    }
                    panic!("case {case}, {terminal}: {error:?}");
                });
            drop(running);

            let reopened = bootstrap(&config).unwrap();
            assert_eq!(reopened.node_incarnation(), 2);
            assert_pending(
                &reopened.control_plane_heartbeat(10_000).unwrap(),
                pg_id,
                pending,
                &prefix,
            );
            let server = reopened.prepare(&map).unwrap().bind().unwrap();
            assert_eq!(
                snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
                before
            );
            drop(server);
        }
    }
}

#[test]
fn staging_complete_bootstrap_reports_unrouted_pending_and_retains_exact_historical_routes() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (mut authority, config) = setup(&dir, epoch);
    let (server, _, config) = installed_config(&dir, config, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let (before, prefix, pending) = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        seed_context(&pg, 1);
        let context = next(&pg, epoch);
        install(&pg, &context);
        (
            snapshot(&pg),
            pg.metadata_command_replica_state().unwrap(),
            PendingMetadataCommandObservation::new(
                epoch,
                std::num::NonZeroU64::new(context.id().log_index().get()).unwrap(),
                context.encode_command().unwrap().checksum_crc64(),
                context.encode_command().unwrap().command_sha256(),
            ),
        )
    };
    let routes = node.cluster_map_history_route_references().unwrap();
    authority
        .set_pg_acting_set(pg_id, [0, 1, 2, 3].map(NodeId::new).to_vec())
        .unwrap();
    let map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(1_000, config.node_id, epoch)
        .unwrap();
    let changed =
        StorageNodeProcessConfig::from_runtime_map_refresh(&config, &map, routes.summary())
            .unwrap();
    assert!(!changed
        .pg_routes
        .iter()
        .find(|route| route.pg_id == pg_id.get())
        .unwrap()
        .acting_set
        .contains(&config.node_id));
    changed.persist_control_plane_runtime_config().unwrap();
    drop(node);
    drop(server);
    let opened = bootstrap(&changed).unwrap();
    let heartbeat = opened.control_plane_heartbeat(10_000).unwrap();
    assert_pending(&heartbeat, pg_id, pending, &prefix);
    assert_eq!(
        heartbeat
            .pg_observations
            .iter()
            .find(|pg| pg.pg_id == pg_id)
            .map(|pg| pg.state),
        Some(PgState::Peering)
    );
    assert_eq!(heartbeat.cluster_map_history_route_references, routes);
    let prepared = opened.prepare(&map).unwrap();
    for reference in routes.iter() {
        assert!(
            prepared
                .config()
                .historical_pg_routes
                .iter()
                .any(|route| route.pg_id == reference.pg_id().get()
                    && route.cluster_epoch == reference.cluster_epoch()),
            "lost {reference:?}"
        );
    }
    let server = prepared.bind().unwrap();
    assert_pending(
        &server.control_plane_heartbeat(1, 10_000).unwrap(),
        pg_id,
        pending,
        &prefix,
    );
    assert_eq!(
        snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
        before
    );
}

#[test]
fn staging_complete_bootstrap_without_saved_config_reports_pending_before_first_prepare() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (authority, config) = setup(&dir, epoch);
    let (server, _, config) = installed_config(&dir, config, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let (before, prefix, pending) = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        seed_context(&pg, 1);
        let context = next(&pg, epoch);
        install(&pg, &context);
        (
            snapshot(&pg),
            pg.metadata_command_replica_state().unwrap(),
            PendingMetadataCommandObservation::new(
                epoch,
                std::num::NonZeroU64::new(context.id().log_index().get()).unwrap(),
                context.encode_command().unwrap().checksum_crc64(),
                context.encode_command().unwrap().command_sha256(),
            ),
        )
    };
    drop(node);
    drop(server);
    let opened = bootstrap(&config).unwrap();
    let heartbeat = opened.control_plane_heartbeat(10_000).unwrap();
    assert_eq!(heartbeat.observed_epoch, ClusterEpoch::INITIAL);
    assert_eq!(heartbeat.pg_observations.len(), 1);
    assert_pending(&heartbeat, pg_id, pending, &prefix);
    let map = authority.snapshot().runtime_map(1_000).unwrap();
    let server = opened.prepare(&map).unwrap().bind().unwrap();
    assert_eq!(
        snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
        before
    );
}

#[test]
fn staging_complete_bootstrap_rejects_damaged_custody_before_heartbeat_or_prepare() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    for damage in ["route_index", "scope", "digest"] {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(5).unwrap();
        let (_, config) = setup(&dir, epoch);
        let (server, _, config) = installed_config(&dir, config, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let before = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            seed_context(&pg, 1);
            install(&pg, &next(&pg, epoch));
            pg.conn.execute_batch(match damage {
                "route_index" => "DELETE FROM metadata_command_pending_route_dependencies",
                "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                "digest" => "UPDATE object_write_counters SET max_committed_generation = max_committed_generation + 1",
                _ => unreachable!(),
            }).unwrap();
            snapshot(&pg)
        };
        config.persist_control_plane_runtime_config().unwrap();
        drop(node);
        drop(server);
        let error = match bootstrap(&config) {
            Ok(_) => panic!("{damage} was admitted"),
            Err(error) => error,
        };
        let StorageNodeServerError::Store(failure) = &error else {
            panic!("{error:?}");
        };
        assert!(
            matches!(
                failure.retained_store_error(),
                Some(
                    StoreError::MetadataCommandLogConflict { .. }
                        | StoreError::MetadataStateDigestMismatch { .. }
                        | StoreError::RouteCapabilitySubjectMismatch { .. }
                )
            ),
            "{damage}: {error:?}"
        );
        assert!(std::os::unix::net::UnixStream::connect(&config.socket_path).is_err());
        assert_eq!(snapshot(&reopen_raw(&config, pg_id)), before);
        crate::node_runtime::server::StorageNodeDataDirGuard::acquire(&config.data_dir).unwrap();
    }
}
