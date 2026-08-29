// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::{PendingMetadataCommandRefreshRecoveryError, RequestWorkBudget};
use crate::control_plane::{
    ControlPlaneError, ControlPlaneRuntimeMapSource, PendingMetadataCommandObservation,
    PendingMetadataCommandRecovery, PendingMetadataCommandRecoveryDiscoveryFailure,
    PendingMetadataCommandRecoveryDiscoveryFailureKind, PendingMetadataCommandRecoveryListing,
    PendingMetadataCommandRecoveryTask,
};
use crate::StorageClusterRouteHandle;
use crate::{BucketSnapshotLoadError, ObjectPgActionError};

struct UnrelatedFullMapFailureSource<S> {
    authority: crate::control_plane::SingleAuthorityControlPlane<S>,
    tasks: Vec<PendingMetadataCommandRecoveryTask>,
}

struct RepeatedFullMapFailureSource {
    refresh_attempts: Arc<std::sync::atomic::AtomicU64>,
    include_discovery_failure: bool,
}

impl ControlPlaneRuntimeMapSource for RepeatedFullMapFailureSource {
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.refresh_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
        })
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        let failures = self.include_discovery_failure.then(|| {
            PendingMetadataCommandRecoveryDiscoveryFailure::new(
                PgId::new(999),
                PendingMetadataCommandRecoveryDiscoveryFailureKind::HistoricalRouteInvalid,
                "injected discovery failure".to_string(),
            )
        });
        Ok(PendingMetadataCommandRecoveryListing::new(
            Vec::new(),
            failures.into_iter().collect(),
        ))
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
    }
}

#[test]
fn repeated_runtime_map_failure_triggers_one_fallback_scan_per_outage() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &[NodeId::new(0)], &[1], EcShape { k: 1, m: 0 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let cluster_epoch = handle.current().cluster_epoch();
    let refresh_attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let source = RepeatedFullMapFailureSource {
        refresh_attempts: Arc::clone(&refresh_attempts),
        include_discovery_failure: true,
    };
    let bucket = BucketName::new("fallback-with-nonempty-listing").unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command_at_epoch(cluster_epoch, pg_id, 1, bucket.clone());
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .try_insert_pending_metadata_command_slot(NodeId::new(0).as_u32(), &command, Some(&bucket))
        .unwrap();
    let mut refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_millis(1), || 1_000)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while refresh_attempts.load(std::sync::atomic::Ordering::SeqCst) < 25
        || refresh_loop.status().fallback_recovery_attempts == 0
        || map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .pending_metadata_command_envelope(NodeId::new(0).as_u32(), cluster_epoch)
            .unwrap()
            .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "runtime-map outage did not exercise refresh and fallback workers: {:?}",
            refresh_loop.status()
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(refresh_loop.status().fallback_recovery_attempts, 1);
    assert_eq!(refresh_loop.status().fallback_recovery_failures, 0);
    refresh_loop.stop();
}

impl<S: crate::control_plane::ControlPlaneStore> ControlPlaneRuntimeMapSource
    for UnrelatedFullMapFailureSource<S>
{
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 999,
            cluster_epoch: self.authority.snapshot().cluster_epoch(),
        })
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        Ok(PendingMetadataCommandRecoveryListing::new(
            self.tasks.clone(),
            Vec::new(),
        ))
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.authority
            .pg_runtime_map_snapshot(pg_id, authority_now_ms)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.authority
            .serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
    }
}

#[test]
fn route_independent_listing_recovers_later_real_authority_task_after_earlier_failure() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "old-epoch-zero-apply-");
    let pg_id = PgId::new(1);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .into_iter()
                .map(|node_id| {
                    (
                        node_id,
                        format!("/tmp/real-authority-node-{}.sock", node_id.as_u32()),
                    )
                })
                .collect(),
            vec![pg_id],
        )
        .unwrap();
    for (node_id, now_ms) in node_ids.into_iter().zip([990, 991, 992]) {
        heartbeat_authority_with_pending(&mut authority, &map, node_id, pg_id, None, now_ms);
    }
    for (node_id, now_ms) in node_ids.into_iter().zip([1_000, 1_001, 1_002]) {
        heartbeat_authority_with_pending(&mut authority, &map, node_id, pg_id, None, now_ms);
    }
    let primary_incarnation = authority
        .snapshot()
        .node(NodeId::new(0))
        .unwrap()
        .node_incarnation();
    authority
        .complete_pg_peering(pg_id, NodeId::new(0), primary_incarnation, 1_003)
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let command = create_bucket_metadata_command_at_epoch(active_epoch, pg_id, 1, bucket.clone());
    let pending = PendingMetadataCommandObservation::new(
        active_epoch,
        std::num::NonZeroU64::MIN,
        command.checksum_crc64(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    authority.set_pg_state(pg_id, PgState::Peering).unwrap();
    for (node_id, now_ms) in node_ids.into_iter().zip([1_010, 1_011, 1_012]) {
        heartbeat_authority_with_pending(
            &mut authority,
            &map,
            node_id,
            pg_id,
            (node_id == NodeId::new(0)).then_some(pending),
            now_ms,
        );
    }
    let runtime_map =
        crate::control_plane::ControlPlaneRuntimeMapSource::runtime_map_snapshot(&authority, 1_020)
            .unwrap();
    assert_eq!(
        runtime_map.pg_routes()[0]
            .pending_metadata_command_recovery()
            .unwrap()
            .pending(),
        pending
    );
    for node_id in node_ids {
        let node_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(
                1_020,
                node_id,
                authority.snapshot().cluster_epoch(),
            )
            .unwrap();
        assert!(node_map.historical_pg_routes().iter().any(|route| {
            route.pg_id() == pg_id
                && route.cluster_epoch() == active_epoch
                && route.state() == PgState::Active
        }));
        assert_eq!(
            node_map.pg_routes()[0]
                .pending_metadata_command_recovery()
                .unwrap()
                .pending(),
            pending,
            "node {} did not receive the acting-set-wide recovery authorization",
            node_id.as_u32()
        );
    }

    let stale_task = authority
        .snapshot()
        .pending_metadata_command_recoveries()
        .tasks()
        .iter()
        .copied()
        .next()
        .unwrap();
    heartbeat_authority_with_pending(&mut authority, &map, NodeId::new(0), pg_id, None, 1_021);
    let stale_recovery = stale_task.recovery();
    let error = handle
        .recover_reported_pending_metadata_command(
            &authority,
            1_022,
            None,
            stale_task.pg_id(),
            stale_recovery.reporting_node_id(),
            stale_recovery.pending(),
        )
        .expect_err("fresh PG map must reject stale listed recovery authorization");
    assert!(
        error.to_string().contains("authorization changed"),
        "unexpected stale authorization error: {error}"
    );
    assert!(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .pending_metadata_command_envelope(NodeId::new(0).as_u32(), active_epoch)
            .unwrap()
            .is_some(),
        "stale listed authorization must not mutate the pending slot"
    );
    heartbeat_authority_with_pending(
        &mut authority,
        &map,
        NodeId::new(0),
        pg_id,
        Some(pending),
        1_023,
    );
    let real_task = authority
        .snapshot()
        .pending_metadata_command_recoveries()
        .tasks()
        .iter()
        .copied()
        .next()
        .unwrap();
    let refreshed_pg_map = authority.pg_runtime_map_snapshot(pg_id, 1_024).unwrap();
    assert_eq!(
        refreshed_pg_map.pg_routes()[0].pending_metadata_command_recovery(),
        Some(real_task.recovery()),
        "fresh PG map must carry the restored exact recovery authorization"
    );
    let unavailable_first_task = PendingMetadataCommandRecoveryTask::new(
        PgId::new(0),
        PendingMetadataCommandRecovery::new(NodeId::new(0), pending),
    );
    let source = UnrelatedFullMapFailureSource {
        authority,
        tasks: vec![unavailable_first_task, real_task],
    };

    let refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_millis(1), || 1_020)
        .unwrap();
    let pending_on_old_primary = || {
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .pending_metadata_command_envelope(NodeId::new(0).as_u32(), active_epoch)
            .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while pending_on_old_primary().is_some() || refresh_loop.status().failures == 0 {
        assert!(
            Instant::now() < deadline,
            "background runtime-map refresh did not converge old-epoch pending command: {:?}",
            refresh_loop.status()
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(refresh_loop.status().failures > 0);
    drop(refresh_loop);

    assert!(pending_on_old_primary().is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(pg.max_metadata_command_log_index(active_epoch).unwrap(), 1);
    }
}

#[test]
fn refresh_recovery_applies_direct_put_from_historical_active_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let base_map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap());
    let bucket = BucketName::new("historical-direct-put-recovery").unwrap();
    let key = crate::ObjectKey::new("object").unwrap();
    let object_pg = 1;
    let pg_id = PgId::new(1);

    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .into_iter()
                .map(|node_id| {
                    (
                        node_id,
                        format!("/tmp/direct-put-recovery-node-{}.sock", node_id.as_u32()),
                    )
                })
                .collect(),
            vec![pg_id],
        )
        .unwrap();
    let mut heartbeat_now_ms = crate::clock::current_time_millis();
    for _ in 0..2 {
        for node_id in node_ids {
            heartbeat_authority_with_pending(
                &mut authority,
                &base_map,
                node_id,
                pg_id,
                None,
                heartbeat_now_ms,
            );
            heartbeat_now_ms += 1;
        }
    }
    let primary_incarnation = authority
        .snapshot()
        .node(NodeId::new(0))
        .unwrap()
        .node_incarnation();
    authority
        .complete_pg_peering(pg_id, NodeId::new(0), primary_incarnation, heartbeat_now_ms)
        .unwrap();
    heartbeat_now_ms += 1;
    for _ in 0..2 {
        for node_id in node_ids {
            heartbeat_authority_with_pending(
                &mut authority,
                &base_map,
                node_id,
                pg_id,
                None,
                heartbeat_now_ms,
            );
            heartbeat_now_ms += 1;
        }
    }
    let active_epoch = authority.snapshot().cluster_epoch();
    let runtime_map = authority.runtime_map_snapshot(heartbeat_now_ms).unwrap();
    let active_map = Arc::new(
        LocalClusterMap::open_runtime_map_with_existing_local_nodes(&base_map, &runtime_map)
            .unwrap(),
    );
    let cluster =
        crate::StorageCluster::from_runtime_local_map(Arc::clone(&active_map), &runtime_map)
            .unwrap();
    let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster.clone());
    cluster.require_route_map_valid_now().unwrap();

    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"historical direct put recovery";
    let segment_okh = [0x73; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let primary = active_map
        .metadata_pg_primary_node(active_epoch, pg_id)
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    object_pg_store
        .try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    object_pg_store
        .apply_metadata_command_and_record(primary.node_id().as_u32(), &command)
        .unwrap();
    drop(object_pg_store);

    let pending = PendingMetadataCommandObservation::new(
        active_epoch,
        std::num::NonZeroU64::new(command.id().log_index().get()).unwrap(),
        command.checksum_crc64(),
    );
    authority.set_pg_state(pg_id, PgState::Peering).unwrap();
    for node_id in node_ids {
        heartbeat_now_ms += 1;
        heartbeat_authority_with_pending(
            &mut authority,
            &active_map,
            node_id,
            pg_id,
            (node_id == NodeId::new(0)).then_some(pending),
            heartbeat_now_ms,
        );
    }
    cluster.require_route_map_valid_now().unwrap();

    assert_eq!(
        handle
            .recover_reported_pending_metadata_command(
                &authority,
                heartbeat_now_ms + 1,
                None,
                pg_id,
                NodeId::new(0),
                pending,
            )
            .unwrap(),
        1
    );
    for node_id in node_ids {
        let pg = active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get()
        );
    }
}

struct HistoricalRouteRecoveryFixture {
    node_ids: [NodeId; 3],
    pg_id: PgId,
    authority: crate::control_plane::SingleAuthorityControlPlane<
        crate::control_plane::FileControlPlaneStore,
    >,
    active_epoch: ClusterEpoch,
    active_map: Arc<LocalClusterMap>,
    cluster: Arc<crate::StorageCluster>,
    handle: StorageClusterRouteHandle,
    now_ms: u64,
}

impl HistoricalRouteRecoveryFixture {
    fn open(path: &std::path::Path, endpoint_prefix: &str) -> Self {
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let pg_id = PgId::new(1);
        let base_map = Arc::new(
            LocalClusterMap::open(path, &node_ids, &[pg_id.get()], EcShape { k: 2, m: 1 }).unwrap(),
        );
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                path.join("historical-route-control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                node_ids
                    .into_iter()
                    .map(|node_id| {
                        (
                            node_id,
                            format!("/tmp/{endpoint_prefix}-node-{}.sock", node_id.as_u32()),
                        )
                    })
                    .collect(),
                vec![pg_id],
            )
            .unwrap();

        let mut now_ms = crate::clock::current_time_millis();
        for _ in 0..2 {
            for node_id in node_ids {
                heartbeat_authority_with_pending(
                    &mut authority,
                    &base_map,
                    node_id,
                    pg_id,
                    None,
                    now_ms,
                );
                now_ms += 1;
            }
        }
        let primary_incarnation = authority
            .snapshot()
            .node(NodeId::new(0))
            .unwrap()
            .node_incarnation();
        authority
            .complete_pg_peering(pg_id, NodeId::new(0), primary_incarnation, now_ms)
            .unwrap();
        now_ms += 1;
        for _ in 0..2 {
            for node_id in node_ids {
                heartbeat_authority_with_pending(
                    &mut authority,
                    &base_map,
                    node_id,
                    pg_id,
                    None,
                    now_ms,
                );
                now_ms += 1;
            }
        }

        let active_epoch = authority.snapshot().cluster_epoch();
        let runtime_map = authority.runtime_map_snapshot(now_ms).unwrap();
        let active_map = Arc::new(
            LocalClusterMap::open_runtime_map_with_existing_local_nodes(&base_map, &runtime_map)
                .unwrap(),
        );
        let cluster =
            crate::StorageCluster::from_runtime_local_map(Arc::clone(&active_map), &runtime_map)
                .unwrap();
        let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster.clone());
        cluster.require_route_map_valid_now().unwrap();
        Self {
            node_ids,
            pg_id,
            authority,
            active_epoch,
            active_map,
            cluster,
            handle,
            now_ms,
        }
    }

    fn authorize_pending_recovery(
        &mut self,
        command: &MetadataCommandEnvelope,
    ) -> PendingMetadataCommandObservation {
        let pending = PendingMetadataCommandObservation::new(
            self.active_epoch,
            std::num::NonZeroU64::new(command.id().log_index().get()).unwrap(),
            command.checksum_crc64(),
        );
        self.authority
            .set_pg_state(self.pg_id, PgState::Peering)
            .unwrap();
        for node_id in self.node_ids {
            self.now_ms += 1;
            heartbeat_authority_with_pending(
                &mut self.authority,
                &self.active_map,
                node_id,
                self.pg_id,
                (node_id == NodeId::new(0)).then_some(pending),
                self.now_ms,
            );
        }
        pending
    }

    fn authorize_current_pending_recovery(
        &mut self,
        command: &MetadataCommandEnvelope,
    ) -> PendingMetadataCommandObservation {
        let pending = PendingMetadataCommandObservation::new(
            self.active_epoch,
            std::num::NonZeroU64::new(command.id().log_index().get()).unwrap(),
            command.checksum_crc64(),
        );
        for node_id in self.node_ids {
            self.now_ms += 1;
            heartbeat_authority_with_pending(
                &mut self.authority,
                &self.active_map,
                node_id,
                self.pg_id,
                (node_id == NodeId::new(0)).then_some(pending),
                self.now_ms,
            );
        }
        assert_eq!(self.authority.snapshot().cluster_epoch(), self.active_epoch);
        pending
    }

    fn recover(&self, pending: PendingMetadataCommandObservation) -> usize {
        self.handle
            .recover_reported_pending_metadata_command(
                &self.authority,
                self.now_ms + 1,
                None,
                self.pg_id,
                NodeId::new(0),
                pending,
            )
            .unwrap_or_else(|error| match error {
                PendingMetadataCommandRefreshRecoveryError::Recover(
                    ObjectPgActionError::Store(source),
                ) => panic!("historical recovery store failure: {source:?}"),
                error => panic!("historical recovery failed: {error:?}"),
            })
    }

    fn recover_after_zero_apply_exact_conflict(
        &self,
        pending: PendingMetadataCommandObservation,
        command: &MetadataCommandEnvelope,
    ) -> usize {
        let pg_runtime_map = self
            .authority
            .pg_runtime_map_snapshot(self.pg_id, self.now_ms + 1)
            .unwrap();
        let historical_runtime_map = pg_runtime_map
            .runtime_map_at_epoch(pending.cluster_epoch())
            .unwrap();
        let recovery_map = Arc::new(
            LocalClusterMap::open_runtime_map_with_existing_local_nodes(
                &self.active_map,
                &historical_runtime_map,
            )
            .unwrap(),
        );
        let recovery_cluster =
            crate::StorageCluster::from_runtime_local_map(recovery_map, &historical_runtime_map)
                .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let conflict_injected = Arc::new(AtomicBool::new(false));
        let conflict_injected_hook = Arc::clone(&conflict_injected);
        let hook_map = Arc::clone(&self.active_map);
        let hook_command = command.clone();
        let node_ids = self.node_ids;
        let hook_guard = recovery_cluster.test_install_before_metadata_command_apply_hook(
            Arc::new(move |node_id, candidate| {
                if node_id != NodeId::new(0) || *candidate != hook_command {
                    return Ok(());
                }
                assert!(!conflict_injected_hook.swap(true, Ordering::SeqCst));
                for replica_node_id in node_ids {
                    let pg = hook_map
                        .node(replica_node_id)
                        .unwrap()
                        .storage_node()
                        .get_pg(candidate.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(replica_node_id.as_u32(), candidate)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual historical command apply failed: {error}")
                            }
                        })?;
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id: node_id.as_u32(),
                    pg_id: candidate.id().pg_id().get(),
                    cluster_epoch: candidate.id().cluster_epoch(),
                    log_index: candidate.id().log_index().get(),
                })
            }),
        );

        let outcome = recovery_cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                self.pg_id,
                command,
                &self.cluster,
            )
            .unwrap();
        drop(hook_guard);
        assert!(conflict_injected.load(Ordering::SeqCst));
        assert_eq!(outcome, PendingMetadataCommandOutcome::Applied);
        1
    }
}

#[test]
fn refresh_recovery_converges_current_active_pending_command_without_epoch_bump() {
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "current-active-pending-command");
    let bucket = BucketName::new("current-active-pending-command").unwrap();
    let command = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        fixture
            .active_map
            .test_next_metadata_command_log_index(fixture.pg_id)
            .get(),
        bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &fixture.active_map,
        NodeId::new(0),
        fixture.pg_id,
        &bucket,
        &command,
    );
    let pending = fixture.authorize_current_pending_recovery(&command);
    let route = fixture
        .authority
        .pg_runtime_map_snapshot(fixture.pg_id, fixture.now_ms + 1)
        .unwrap()
        .pg_routes()[0]
        .clone();
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(
        route.pending_metadata_command_recovery(),
        Some(PendingMetadataCommandRecovery::new(NodeId::new(0), pending))
    );

    assert_eq!(fixture.recover(pending), 1);
    assert_eq!(
        fixture.authority.snapshot().cluster_epoch(),
        fixture.active_epoch
    );
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get()
        );
    }
    assert_fixture_pending_command(&fixture, None);
}

#[test]
fn refresh_recovery_uses_certified_lineage_root_for_reissued_pending_tip() {
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-reissued-command");
    let bucket = BucketName::new("historical-reissued-command").unwrap();
    let source_index = fixture
        .active_map
        .test_next_metadata_command_log_index(fixture.pg_id);
    let source = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        source_index.get(),
        bucket.clone(),
    );
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            fixture.active_epoch,
            fixture.pg_id,
            MetadataCommandLogIndex::new(source_index.get() + 1).unwrap(),
        ),
        source.payload().clone(),
    );
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        pg.record_metadata_command_abandoned(node_id.as_u32(), &source)
            .unwrap();
    }
    force_insert_pending_metadata_command_for_node_for_test(
        &fixture.active_map,
        NodeId::new(0),
        fixture.pg_id,
        &bucket,
        &replacement,
    );

    let runtime_state = fixture.active_map.runtime_state();
    let MetadataCommandRecoveryAdmission::Leader(owner) =
        runtime_state.join_metadata_command_recovery(fixture.pg_id, &source)
    else {
        panic!("source command should own the recovery flight");
    };
    owner
        .bind_reissued_command(fixture.pg_id, &source, &replacement)
        .unwrap();
    owner.mark_irreversible_handoff(
        MetadataCommandRecoveryResolution::IrrevocableConvergencePending,
    );
    owner.relinquish_for_authorized_recovery();

    let pending = fixture.authorize_pending_recovery(&source);
    assert_eq!(fixture.recover(pending), 1);
    assert_eq!(
        runtime_state.test_metadata_command_recovery_flight_count(),
        0
    );
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert!(pg
            .pending_metadata_command_envelope(node_id.as_u32(), fixture.active_epoch)
            .unwrap()
            .is_none());
    }
}

#[test]
fn refresh_recovery_clears_zero_apply_fully_applied_object_command() {
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-fully-applied-object-command");
    let bucket = BucketName::new("historical-fully-applied-object-command").unwrap();
    let key = crate::ObjectKey::new("object").unwrap();
    create_test_bucket(&fixture.cluster, &bucket);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            fixture.active_epoch,
            fixture.pg_id,
            fixture
                .active_map
                .test_next_metadata_command_log_index(fixture.pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("82".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            fixture.now_ms,
        )),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &fixture.active_map,
        NodeId::new(0),
        fixture.pg_id,
        &bucket,
        &command,
    );
    let pending = fixture.authorize_pending_recovery(&command);

    assert_eq!(
        fixture.recover_after_zero_apply_exact_conflict(pending, &command),
        1
    );
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get()
        );
        assert!(pg
            .pending_metadata_command_envelope(node_id.as_u32(), fixture.active_epoch)
            .unwrap()
            .is_none());
    }
}

#[test]
fn refresh_recovery_clears_zero_apply_fully_applied_bucket_command() {
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-fully-applied-bucket-command");
    let bucket = BucketName::new("historical-fully-applied-bucket-command").unwrap();
    let command = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        fixture
            .active_map
            .test_next_metadata_command_log_index(fixture.pg_id)
            .get(),
        bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &fixture.active_map,
        NodeId::new(0),
        fixture.pg_id,
        &bucket,
        &command,
    );
    let pending = fixture.authorize_pending_recovery(&command);

    assert_eq!(
        fixture.recover_after_zero_apply_exact_conflict(pending, &command),
        1
    );
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get()
        );
        assert!(pg
            .pending_metadata_command_envelope(node_id.as_u32(), fixture.active_epoch)
            .unwrap()
            .is_none());
    }
}

#[test]
fn refresh_recovery_does_not_count_published_command_with_trailing_gap_as_recovered() {
    let tmp = test_util::tempdir();
    let mut fixture = HistoricalRouteRecoveryFixture::open(
        tmp.path(),
        "historical-published-pending-recovery-count",
    );
    let first_bucket = BucketName::new("historical-pending-gap-first").unwrap();
    let pending_bucket = BucketName::new("historical-pending-gap-second").unwrap();
    let first = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        1,
        first_bucket,
    );
    let command = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        2,
        pending_bucket.clone(),
    );

    for node_id in [NodeId::new(1), NodeId::new(0)] {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let primary = fixture
        .active_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(fixture.pg_id.get())
        .unwrap();
    primary
        .try_insert_pending_metadata_command_slot(
            NodeId::new(0).as_u32(),
            &command,
            Some(&pending_bucket),
        )
        .unwrap();
    drop(primary);
    for node_id in [NodeId::new(1), NodeId::new(0)] {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }

    let pending = fixture.authorize_pending_recovery(&command);
    assert_eq!(fixture.recover(pending), 0);
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_commands_for_current_map()
            .unwrap(),
        0,
        "fallback recovery must not count a retained published command as drained"
    );
    let primary = fixture
        .active_map
        .metadata_pg_primary_node(fixture.active_epoch, fixture.pg_id)
        .unwrap();
    assert_eq!(
        primary
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap()
            .pending_metadata_command_envelope(primary.node_id().as_u32(), fixture.active_epoch,)
            .unwrap(),
        Some(command),
        "published command with a trailing gap must remain reported as pending"
    );
}

fn seed_fully_applied_pending_bucket_command(
    fixture: &HistoricalRouteRecoveryFixture,
    bucket: BucketName,
) -> MetadataCommandEnvelope {
    let command = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        1,
        bucket.clone(),
    );
    let primary = fixture
        .active_map
        .metadata_pg_primary_node(fixture.active_epoch, fixture.pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.pg_id.get())
        .unwrap()
        .try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    for node_id in [NodeId::new(1), NodeId::new(0), NodeId::new(2)] {
        fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    command
}

fn assert_fixture_pending_command(
    fixture: &HistoricalRouteRecoveryFixture,
    expected: Option<&MetadataCommandEnvelope>,
) {
    let primary = fixture
        .active_map
        .metadata_pg_primary_node(fixture.active_epoch, fixture.pg_id)
        .unwrap();
    let pending = primary
        .storage_node()
        .get_pg(fixture.pg_id.get())
        .unwrap()
        .pending_metadata_command_envelope(primary.node_id().as_u32(), fixture.active_epoch)
        .unwrap();
    assert_eq!(pending.as_ref(), expected);
}

#[test]
fn recovery_accounting_does_not_count_applied_command_with_reservation_cleanup_deferred() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let mut fixture = HistoricalRouteRecoveryFixture::open(
        tmp.path(),
        "historical-reservation-cleanup-pending-count",
    );
    let command = seed_fully_applied_pending_bucket_command(
        &fixture,
        BucketName::new("historical-reservation-cleanup-pending").unwrap(),
    );
    let checksum = command.checksum_crc64();
    let hook = fixture
        .cluster
        .test_install_global_metadata_command_terminal_reservation_release_hook(Arc::new(
            move |candidate| {
                if candidate.checksum_crc64() == checksum {
                    return Err(BucketSnapshotLoadError::Store(StoreError::Io {
                        context: "injected terminal reservation cleanup deferral",
                        source: std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "injected terminal reservation cleanup deferral",
                        ),
                    }));
                }
                Ok(())
            },
        ));

    let pending = fixture.authorize_pending_recovery(&command);
    assert_eq!(
        fixture.recover(pending),
        0,
        "reported recovery must not count a command whose reservation cleanup was deferred"
    );
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_commands_for_current_map()
            .unwrap(),
        0,
        "recovery must not count a command whose reservation cleanup was deferred"
    );
    assert_fixture_pending_command(&fixture, Some(&command));

    drop(hook);
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_commands_for_current_map()
            .unwrap(),
        1
    );
    assert_fixture_pending_command(&fixture, None);
}

#[test]
fn recovery_accounting_does_not_count_applied_command_with_slot_removal_deferred() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-slot-removal-pending-count");
    let command = seed_fully_applied_pending_bucket_command(
        &fixture,
        BucketName::new("historical-slot-removal-pending").unwrap(),
    );
    let checksum = command.checksum_crc64();
    let hook = fixture
        .cluster
        .test_install_global_metadata_command_terminal_slot_removal_hook(Arc::new(
            move |candidate| candidate.checksum_crc64() == checksum,
        ));

    let pending = fixture.authorize_pending_recovery(&command);
    assert_eq!(
        fixture.recover(pending),
        0,
        "reported recovery must not count a command whose pending-slot removal was deferred"
    );
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_commands_for_current_map()
            .unwrap(),
        0,
        "recovery must not count a command whose pending-slot removal was deferred"
    );
    assert_fixture_pending_command(&fixture, Some(&command));

    drop(hook);
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_commands_for_current_map()
            .unwrap(),
        1
    );
    assert_fixture_pending_command(&fixture, None);
}

#[test]
fn refresh_recovery_reissues_zero_apply_bucket_command_from_historical_active_route() {
    let tmp = test_util::tempdir();
    let mut fixture = HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-bucket-reissue");
    let first_bucket = BucketName::new("historical-reissue-first").unwrap();
    let reissued_bucket = BucketName::new("historical-reissue-second").unwrap();
    let applied = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        1,
        first_bucket.clone(),
    );
    fixture
        .cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command_at_epoch(
        fixture.active_epoch,
        fixture.pg_id,
        1,
        reissued_bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &fixture.active_map,
        NodeId::new(0),
        fixture.pg_id,
        &reissued_bucket,
        &stale,
    );
    let pending = fixture.authorize_pending_recovery(&stale);

    assert_eq!(fixture.recover(pending), 1);
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &reissued_bucket).unwrap();
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            2
        );
    }
}

#[test]
fn refresh_recovery_abandons_stale_reservation_from_historical_active_route() {
    let tmp = test_util::tempdir();
    let mut fixture =
        HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-stale-reservation");
    let bucket = BucketName::new("historical-stale-reservation").unwrap();
    let key = crate::ObjectKey::new("object").unwrap();
    create_test_bucket(&fixture.cluster, &bucket);

    let first_reservation_id = crate::SessionId::try_from("74".repeat(16)).unwrap();
    let stale_reservation_id = crate::SessionId::try_from("75".repeat(16)).unwrap();
    let generation_id = crate::GenerationId::MIN;
    let first = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            fixture.active_epoch,
            fixture.pg_id,
            fixture
                .active_map
                .test_next_metadata_command_log_index(fixture.pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            first_reservation_id.clone(),
            generation_id,
            fixture.now_ms,
        )),
    );
    fixture
        .cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &first)
        .unwrap();
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            fixture.active_epoch,
            fixture.pg_id,
            fixture
                .active_map
                .test_next_metadata_command_log_index(fixture.pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            stale_reservation_id.clone(),
            generation_id,
            fixture.now_ms + 1,
        )),
    );
    let primary = fixture
        .active_map
        .metadata_pg_primary_node(fixture.active_epoch, fixture.pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.pg_id.get())
        .unwrap()
        .try_insert_pending_metadata_command_slot(primary.node_id().as_u32(), &stale, Some(&bucket))
        .unwrap();
    let pending = fixture.authorize_pending_recovery(&stale);

    assert_eq!(fixture.recover(pending), 1);
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &first_reservation_id,
            )
            .unwrap(),
            generation_id
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &stale_reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            stale.id().log_index().get()
        );
    }
}

#[test]
fn refresh_recovery_abandons_stream_create_with_certified_generation_cleanup() {
    let tmp = test_util::tempdir();
    let mut fixture = HistoricalRouteRecoveryFixture::open(tmp.path(), "historical-stream-abandon");
    let bucket = BucketName::new("historical-stream-abandon").unwrap();
    let key = crate::ObjectKey::new("object").unwrap();
    let session_id = crate::SessionId::try_from("77".repeat(16)).unwrap();
    create_test_bucket(&fixture.cluster, &bucket);
    let generation_id = fixture
        .cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let bucket_write_reservation = fixture
        .cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof = crate::metadata_command::BucketWriteReservationProof::from(
        &bucket_write_reservation.record,
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            fixture.active_epoch,
            fixture.pg_id,
            fixture
                .active_map
                .test_next_metadata_command_log_index(fixture.pg_id),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::PutObject,
                    encryption: crate::ObjectEncryption::None,
                },
                fixture.now_ms,
                bucket_write_proof,
            ),
        )),
    );
    let primary = fixture
        .active_map
        .metadata_pg_primary_node(fixture.active_epoch, fixture.pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.pg_id.get())
        .unwrap()
        .try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    fixture
        .cluster
        .release_durable_bucket_write_reservation(bucket_write_reservation)
        .unwrap();
    let pending = fixture.authorize_pending_recovery(&command);

    assert_eq!(fixture.recover(pending), 1);
    for node_id in fixture.node_ids {
        let pg = fixture
            .active_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.pg_id.get())
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get() + 1
        );
    }
    assert_eq!(generation_id, crate::GenerationId::MIN);
}

fn heartbeat_authority_with_pending<S: crate::control_plane::ControlPlaneStore>(
    authority: &mut crate::control_plane::SingleAuthorityControlPlane<S>,
    map: &Arc<LocalClusterMap>,
    node_id: NodeId,
    pg_id: PgId,
    pending_metadata_command: Option<PendingMetadataCommandObservation>,
    now_ms: u64,
) {
    let snapshot = authority.snapshot();
    let node_incarnation = snapshot.node(node_id).unwrap().node_incarnation().max(1);
    let endpoint = snapshot.node(node_id).unwrap().endpoint().to_owned();
    let observed_epoch = snapshot.cluster_epoch();
    let pg_state = snapshot.pg(pg_id).unwrap().state();
    let state = map
        .node(node_id)
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    authority
        .heartbeat(
            crate::control_plane::NodeHeartbeat {
                node_id,
                node_incarnation,
                endpoint,
                observed_epoch,
                requested_lease_duration_ms: 10_000,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
                    pg_id,
                    state: pg_state,
                    metadata_proof: crate::control_plane::PgMetadataProof::current(
                        state.applied_log_index,
                        state.applied_log_hash,
                        state.state_digest,
                    ),
                    pending_metadata_command,
                }],
            },
            now_ms,
        )
        .unwrap();
}

#[test]
fn metadata_command_recovery_single_flight_waits_for_matching_command() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let waiter_state = Arc::clone(&runtime_state);
    let waiter_command = command.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        let admission = waiter_state.join_metadata_command_recovery(pg_id, &waiter_command);
        tx.send(matches!(
            admission,
            MetadataCommandRecoveryAdmission::Waited { .. }
        ))
        .unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "second recovery caller should wait while the leader is active"
    );
    drop(leader_guard);
    assert!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "second recovery caller should return as a waiter after the leader finishes"
    );
    waiter.join().unwrap();
}

#[test]
fn metadata_command_recovery_single_flight_wait_is_bounded() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-timeout-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(_leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let timed_out = runtime_state.join_metadata_command_recovery(pg_id, &command);
    assert!(
        matches!(timed_out, MetadataCommandRecoveryAdmission::TimedOut { .. }),
        "waiter should return a bounded timeout while the leader remains active"
    );
}

#[test]
fn metadata_command_recovery_reissue_lineage_shares_owner_and_deadline() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-reissue-lineage").unwrap();
    let source = create_bucket_metadata_command(pg_id, 1, bucket);
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        source.payload().clone(),
    );
    let MetadataCommandRecoveryAdmission::Leader(owner) =
        runtime_state.join_metadata_command_recovery(pg_id, &source)
    else {
        panic!("source command should own the recovery flight");
    };
    owner
        .bind_reissued_command(pg_id, &source, &replacement)
        .unwrap();

    let deadline = Instant::now();
    let admission = runtime_state.join_metadata_command_recovery_until(pg_id, &source, deadline);
    assert!(matches!(
        admission,
        MetadataCommandRecoveryAdmission::TimedOut {
            lineage_tip,
            ..
        } if lineage_tip == replacement
    ));

    drop(owner);
    assert_eq!(
        runtime_state.test_metadata_command_recovery_flight_count(),
        0
    );
}

#[test]
fn metadata_command_recovery_handoff_releases_waiter_and_retains_root_owner() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-authorized-handoff").unwrap();
    let source = create_bucket_metadata_command(pg_id, 1, bucket);
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        source.payload().clone(),
    );
    let MetadataCommandRecoveryAdmission::Leader(owner) =
        runtime_state.join_metadata_command_recovery(pg_id, &source)
    else {
        panic!("source command should own the recovery flight");
    };
    owner
        .bind_reissued_command(pg_id, &source, &replacement)
        .unwrap();
    assert_eq!(owner.lineage_root(), source);
    assert_eq!(owner.lineage_tip(), replacement);
    owner.mark_irreversible_handoff(
        MetadataCommandRecoveryResolution::IrrevocableConvergencePending,
    );

    let waiter_state = Arc::clone(&runtime_state);
    let waiter_command = replacement.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        tx.send(waiter_state.join_metadata_command_recovery(pg_id, &waiter_command))
            .unwrap();
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "reissued waiter must remain behind the handoff owner"
    );

    owner.relinquish_for_authorized_recovery();
    let admission = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        admission,
        MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
            lineage_tip,
            ..
        } if lineage_tip == replacement
    ));
    waiter.join().unwrap();

    let MetadataCommandRecoveryAdmission::Leader(recovery_owner) = runtime_state
        .join_authorized_metadata_command_recovery_until(
            pg_id,
            &replacement,
            &source,
            Instant::now() + Duration::from_secs(1),
        )
    else {
        panic!("authorized C1 recovery must claim the transferred C2 flight");
    };
    assert_eq!(recovery_owner.lineage_root(), source);
    recovery_owner.record_outcome(PendingMetadataCommandOutcome::Applied);
    drop(recovery_owner);
    assert_eq!(
        runtime_state.test_metadata_command_recovery_flight_count(),
        0
    );
}

#[test]
fn expired_unrelated_drainer_registers_authorized_recovery_without_leading() {
    let runtime_state = LocalClusterRuntimeState::new();
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("expired-unrelated-recovery-handoff").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);

    let admission = runtime_state.join_metadata_command_recovery_as_unrelated_drainer_until(
        pg_id,
        &command,
        Instant::now(),
    );
    assert!(matches!(
        admission,
        MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
            lineage_tip,
            resolution: None,
            ..
        } if lineage_tip == command
    ));
    assert!(runtime_state.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));

    let MetadataCommandRecoveryAdmission::Leader(recovery_owner) = runtime_state
        .join_authorized_metadata_command_recovery_until(
            pg_id,
            &command,
            &command,
            Instant::now() + Duration::from_secs(1),
        )
    else {
        panic!("authorized recovery must claim the expired drainer's retained flight");
    };
    recovery_owner.record_outcome(PendingMetadataCommandOutcome::Abandoned);
    drop(recovery_owner);
    assert_eq!(
        runtime_state.test_metadata_command_recovery_flight_count(),
        0
    );
}

#[test]
fn metadata_command_recovery_definite_reissue_failure_restores_waiter_lineage() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-reissue-rollback").unwrap();
    let source = create_bucket_metadata_command(pg_id, 1, bucket);
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        source.payload().clone(),
    );
    let MetadataCommandRecoveryAdmission::Leader(owner) =
        runtime_state.join_metadata_command_recovery(pg_id, &source)
    else {
        panic!("source command should own the recovery flight");
    };
    owner
        .bind_reissued_command(pg_id, &source, &replacement)
        .unwrap();

    let waiter_state = Arc::clone(&runtime_state);
    let waiter_replacement = replacement.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        tx.send(waiter_state.join_metadata_command_recovery(pg_id, &waiter_replacement))
            .unwrap();
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "replacement waiter should share the source owner's flight"
    );

    owner
        .rollback_reissued_command(pg_id, &source, &replacement)
        .unwrap();
    let expired_replacement =
        runtime_state.join_metadata_command_recovery_until(pg_id, &replacement, Instant::now());
    assert!(matches!(
        expired_replacement,
        MetadataCommandRecoveryAdmission::TimedOut { lineage_tip, .. }
            if lineage_tip == replacement
    ));

    drop(owner);
    let admission = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        admission,
        MetadataCommandRecoveryAdmission::Waited { lineage_tip, .. }
            if lineage_tip == source
    ));
    waiter.join().unwrap();
}

#[test]
fn reissued_bucket_command_waiter_preserves_published_lineage_after_budget_expiry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "lineage-first-");
    let second_bucket = bucket_for_pg(topology, 1, "lineage-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();
    let stale = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &stale);

    let _serial = lock_metadata_command_apply_hook_test();
    let (trailing_arrived_tx, trailing_arrived_rx) = std::sync::mpsc::channel();
    let (trailing_release_tx, trailing_release_rx) = std::sync::mpsc::channel();
    let trailing_release_rx = Arc::new(Mutex::new(trailing_release_rx));
    let block_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = second_bucket.clone();
    let hook_release = Arc::clone(&trailing_release_rx);
    let hook_once = Arc::clone(&block_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id == NodeId::new(2)
                && command.bucket_name() == &hook_bucket
                && command.id().log_index().get() == 2
                && hook_once.swap(false, Ordering::SeqCst)
            {
                trailing_arrived_tx.send(()).unwrap();
                hook_release
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv_timeout(Duration::from_secs(5))
                    .expect("timed out waiting to release trailing apply");
            }
            Ok(())
        },
    ));

    let owner_cluster = cluster.clone();
    let owner_bucket = second_bucket.clone();
    let owner_command = stale.clone();
    let owner = thread::spawn(move || {
        owner_cluster.finish_pending_metadata_command_to_acting_set(
            pg_id,
            &owner_bucket,
            &owner_command,
            false,
        )
    });
    trailing_arrived_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reissued command did not reach trailing apply");
    let replacement = pending_metadata_command_for_test(&map, pg_id, &second_bucket)
        .expect("reissued command should remain pending while trailing apply is blocked");
    assert_eq!(replacement.id().log_index().get(), 2);

    let mut waiter_budget = RequestWorkBudget::new(Duration::from_millis(20), None)
        .for_operation("test_reissued_bucket_command_waiter")
        .for_pg(pg_id);
    waiter_budget.expire_for_test();
    let outcome = cluster
        .finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            &stale,
            false,
            &mut waiter_budget,
        )
        .unwrap();
    assert_eq!(
        outcome,
        PendingMetadataCommandOutcome::PublishedPendingRecovery
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &second_bucket),
        Some(replacement),
        "published success must retain the slot for trailing recovery"
    );

    trailing_release_tx.send(()).unwrap();
    assert_eq!(
        owner.join().unwrap().unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    drop(hook_guard);
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn reissued_object_command_transfers_owner_and_stale_waiter_lineage() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, target_key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let prior_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "lineage-prior-",
    );
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let stale_index = map.test_next_metadata_command_log_index(pg_id);
    let prior = MetadataCommandEnvelope::new(
        MetadataCommandId::new(cluster.operation_epoch(), pg_id, stale_index),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            prior_key,
            crate::VersionId::from_u64(1),
        )),
    );
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &prior)
        .unwrap();
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(cluster.operation_epoch(), pg_id, stale_index),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            target_key,
            crate::VersionId::from_u64(1),
        )),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale);

    let replacement_index = stale_index.get() + 1;
    let _serial = lock_metadata_command_apply_hook_test();
    let (trailing_arrived_tx, trailing_arrived_rx) = std::sync::mpsc::channel();
    let (trailing_release_tx, trailing_release_rx) = std::sync::mpsc::channel();
    let trailing_release_rx = Arc::new(Mutex::new(trailing_release_rx));
    let block_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_release = Arc::clone(&trailing_release_rx);
    let hook_once = Arc::clone(&block_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id == NodeId::new(2)
                && command.bucket_name() == &hook_bucket
                && command.id().log_index().get() == replacement_index
                && hook_once.swap(false, Ordering::SeqCst)
            {
                trailing_arrived_tx.send(()).unwrap();
                hook_release
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv_timeout(Duration::from_secs(5))
                    .expect("timed out waiting to release trailing object-command apply");
            }
            Ok(())
        },
    ));

    let owner_cluster = cluster.clone();
    let owner_command = stale.clone();
    let owner = thread::spawn(move || {
        owner_cluster.drain_pending_metadata_command_with_recovery_gate(pg_id, &owner_command)
    });
    trailing_arrived_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reissued object command did not reach trailing apply");
    let replacement = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("reissued object command should remain pending while trailing apply is blocked");
    assert_eq!(replacement.id().log_index().get(), replacement_index);
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(pg_id, &stale, &replacement));

    let mut waiter_budget = RequestWorkBudget::new(Duration::from_millis(20), None)
        .for_operation("test_reissued_object_command_stale_waiter")
        .for_pg(pg_id);
    waiter_budget.expire_for_test();
    let outcome = cluster
        .drain_pending_metadata_command_with_recovery_gate_and_work_budget(
            pg_id,
            &stale,
            &mut waiter_budget,
        )
        .unwrap();
    assert_eq!(
        outcome,
        PendingMetadataCommandOutcome::PublishedPendingRecovery
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(replacement),
        "published success must retain the object-command slot for trailing recovery"
    );

    trailing_release_tx.send(()).unwrap();
    assert_eq!(
        owner.join().unwrap().unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    drop(hook_guard);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn unrelated_object_operation_converges_after_authorized_recovery_handoff() {
    struct GateRelease(Option<std::sync::mpsc::SyncSender<()>>);

    impl GateRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for GateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let other_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "authorized-handoff-other-",
    );
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key,
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let checksum = command.checksum_crc64();
    let (cleanup_reached_tx, cleanup_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (cleanup_release_tx, cleanup_release_rx) = std::sync::mpsc::sync_channel(1);
    let cleanup_release_rx = Arc::new(Mutex::new(cleanup_release_rx));
    let cleanup_release_rx_for_hook = Arc::clone(&cleanup_release_rx);
    let cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |candidate| {
            if candidate.checksum_crc64() != checksum {
                return false;
            }
            cleanup_reached_tx.send(()).unwrap();
            cleanup_release_rx_for_hook
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(Duration::from_secs(5))
                .expect("terminal cleanup gate was not released");
            true
        }),
    );

    let transfer_observed = Arc::new(AtomicBool::new(false));
    let (transfer_reached_tx, transfer_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (transfer_release_tx, transfer_release_rx) = std::sync::mpsc::sync_channel(1);
    let transfer_release_rx = Arc::new(Mutex::new(transfer_release_rx));
    let transfer_observed_for_hook = Arc::clone(&transfer_observed);
    let transfer_release_rx_for_hook = Arc::clone(&transfer_release_rx);
    let transfer_hook = cluster
        .test_install_pending_object_metadata_command_recovery_transferred_hook(Arc::new(
            move |candidate| {
                if candidate.checksum_crc64() != checksum
                    || transfer_observed_for_hook.swap(true, Ordering::SeqCst)
                {
                    return;
                }
                transfer_reached_tx.send(()).unwrap();
                transfer_release_rx_for_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv_timeout(Duration::from_secs(5))
                    .expect("recovery-transfer gate was not released");
            },
        ));

    let mut cleanup_release = GateRelease(Some(cleanup_release_tx));
    let mut transfer_release = GateRelease(Some(transfer_release_tx));
    let other_reservation_id = crate::tests::stream_session_id("auth-handoff");
    let (owner_outcome, waiter_result) = thread::scope(|scope| {
        let (owner_tx, owner_rx) = std::sync::mpsc::sync_channel(1);
        let owner_cluster = &cluster;
        let owner_command = &command;
        scope.spawn(move || {
            owner_tx
                .send(
                    owner_cluster
                        .drain_pending_metadata_command_with_recovery_gate(pg_id, owner_command),
                )
                .unwrap();
        });
        cleanup_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("exact object-command owner did not reach terminal cleanup");

        let (waiter_tx, waiter_rx) = std::sync::mpsc::sync_channel(1);
        let waiter_cluster = &cluster;
        let waiter_bucket = &bucket;
        let waiter_key = &other_key;
        let waiter_reservation_id = &other_reservation_id;
        scope.spawn(move || {
            waiter_tx
                .send(waiter_cluster.reserve_put_object_generation(
                    waiter_bucket,
                    waiter_key,
                    waiter_reservation_id,
                ))
                .unwrap();
        });
        transfer_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated operation did not observe the authorized-recovery handoff");

        cleanup_release.release();
        let owner_outcome = owner_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("exact object-command owner did not finish after cleanup release")
            .unwrap();
        assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));
        drop(cleanup_hook);
        assert_eq!(
            cluster
                .drain_pending_metadata_command_with_authorized_recovery_route(
                    pg_id, &command, &cluster,
                )
                .unwrap(),
            PendingMetadataCommandOutcome::Applied
        );

        transfer_release.release();
        let waiter_result = waiter_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated operation did not finish after authorized recovery");
        (owner_outcome, waiter_result)
    });

    assert_eq!(
        owner_outcome,
        PendingMetadataCommandOutcome::TerminalCleanupPending { applied: true }
    );
    assert_eq!(
        waiter_result.expect(
            "an unrelated operation must retry after authorized recovery instead of surfacing contention",
        ),
        crate::GenerationId::new(1).unwrap()
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    drop(transfer_hook);
}

#[test]
fn object_generation_reservation_waits_after_transferring_authorized_recovery() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let checksum = command.checksum_crc64();
    let inject_transfer = Arc::new(AtomicBool::new(true));
    let inject_transfer_for_hook = Arc::clone(&inject_transfer);
    let (transfer_reached_tx, transfer_reached_rx) = std::sync::mpsc::sync_channel(1);
    let _apply_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate.checksum_crc64() == checksum
                && inject_transfer_for_hook.swap(false, Ordering::SeqCst)
            {
                transfer_reached_tx.send(()).unwrap();
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: candidate.id().cluster_epoch(),
                    valid_until_ms: 1,
                    now_ms: 2,
                });
            }
            Ok(())
        }));

    let reservation_id = crate::tests::stream_session_id("transfer-wait");
    let reservation_result = thread::scope(|scope| {
        let (reservation_tx, reservation_rx) = std::sync::mpsc::sync_channel(1);
        let reservation_cluster = &cluster;
        let reservation_bucket = &bucket;
        let reservation_key = &key;
        let reservation_id = &reservation_id;
        scope.spawn(move || {
            reservation_tx
                .send(reservation_cluster.reserve_put_object_generation(
                    reservation_bucket,
                    reservation_key,
                    reservation_id,
                ))
                .unwrap();
        });

        transfer_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("generation reservation did not transfer the pending command");
        let handoff_deadline = Instant::now() + Duration::from_secs(5);
        while !cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command) {
            assert!(
                Instant::now() < handoff_deadline,
                "generation reservation did not relinquish recovery authority"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            cluster
                .drain_pending_metadata_command_with_authorized_recovery_route(
                    pg_id, &command, &cluster,
                )
                .unwrap(),
            PendingMetadataCommandOutcome::Applied
        );
        reservation_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("generation reservation did not finish after authorized recovery")
    });

    assert_eq!(
        reservation_result.expect(
            "generation reservation must observe authorized recovery instead of exposing contention",
        ),
        crate::GenerationId::new(1).unwrap()
    );
    assert!(!inject_transfer.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn object_generation_install_race_waits_for_authorized_recovery() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );

    let checksum = command.checksum_crc64();
    let inject_transfer = Arc::new(AtomicBool::new(true));
    let inject_transfer_for_hook = Arc::clone(&inject_transfer);
    let _apply_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate.checksum_crc64() == checksum
                && inject_transfer_for_hook.swap(false, Ordering::SeqCst)
            {
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: candidate.id().cluster_epoch(),
                    valid_until_ms: 1,
                    now_ms: 2,
                });
            }
            Ok(())
        }));

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command = command.clone();
    let (handoff_tx, handoff_rx) = std::sync::mpsc::sync_channel(1);
    let _install_hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
            let error = hook_cluster
                .drain_pending_metadata_command_with_recovery_gate(pg_id, &hook_command)
                .expect_err("injected command must transfer recovery authority");
            assert!(matches!(
                error,
                ObjectPgActionError::MetadataCommandRecoveryTransferred
            ));
            handoff_tx.send(()).unwrap();
        }));
    let allocator_wait_observed = Arc::new(AtomicBool::new(false));
    let allocator_wait_observed_for_hook = Arc::clone(&allocator_wait_observed);
    let (allocator_wait_tx, allocator_wait_rx) = std::sync::mpsc::sync_channel(1);
    let _allocator_wait_hook = cluster
        .test_install_pending_object_metadata_command_recovery_transferred_hook(Arc::new(
            move |candidate| {
                if candidate.checksum_crc64() == checksum
                    && !allocator_wait_observed_for_hook.swap(true, Ordering::SeqCst)
                {
                    allocator_wait_tx.send(()).unwrap();
                }
            },
        ));

    let reservation_id = crate::tests::stream_session_id("install-race");
    let reservation_result = thread::scope(|scope| {
        let (reservation_tx, reservation_rx) = std::sync::mpsc::sync_channel(1);
        let reservation_cluster = Arc::clone(&cluster);
        let reservation_bucket = &bucket;
        let reservation_key = &key;
        let reservation_id = &reservation_id;
        scope.spawn(move || {
            reservation_tx
                .send(reservation_cluster.reserve_put_object_generation(
                    reservation_bucket,
                    reservation_key,
                    reservation_id,
                ))
                .unwrap();
        });

        handoff_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("generation reservation did not encounter the injected install race");
        allocator_wait_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("allocator install did not preserve the pending recovery command");
        assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));
        assert!(matches!(
            reservation_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(
            pending_metadata_command_for_test(&map, pg_id, &bucket),
            Some(command.clone())
        );

        assert_eq!(
            cluster
                .drain_pending_metadata_command_with_authorized_recovery_route(
                    pg_id, &command, &cluster,
                )
                .unwrap(),
            PendingMetadataCommandOutcome::Applied
        );
        reservation_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("generation reservation did not finish after authorized recovery")
    });

    assert_eq!(
        reservation_result.expect(
            "generation install race must wait instead of exposing internal recovery contention",
        ),
        GenerationId::new(1).unwrap()
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(!inject_transfer.load(Ordering::SeqCst));
    assert!(allocator_wait_observed.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn object_generation_reservation_preserves_unrelated_direct_put_transfer() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let other_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "unrelated-direct-put-transfer-",
    );
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::tests::stream_session_id("pending-direct");
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"unrelated pending direct PUT";
    let segment_okh = [0x8d; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let shard_batch: Vec<(&ShardKey, WriteAck)> = written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(commit_req.data_pg_id, &shard_batch)
        .unwrap();
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &pg,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let checksum = command.checksum_crc64();
    let inject_transfer = Arc::new(AtomicBool::new(true));
    let inject_transfer_for_hook = Arc::clone(&inject_transfer);
    let (transfer_reached_tx, transfer_reached_rx) = std::sync::mpsc::sync_channel(1);
    let _apply_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate.checksum_crc64() == checksum
                && inject_transfer_for_hook.swap(false, Ordering::SeqCst)
            {
                transfer_reached_tx.send(()).unwrap();
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: candidate.id().cluster_epoch(),
                    valid_until_ms: 1,
                    now_ms: 2,
                });
            }
            Ok(())
        }));

    let other_reservation_id = crate::tests::stream_session_id("unrelated-wait");
    let reservation_result = thread::scope(|scope| {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let reservation_cluster = &cluster;
        let reservation_bucket = &bucket;
        let reservation_key = &other_key;
        let reservation_id = &other_reservation_id;
        scope.spawn(move || {
            result_tx
                .send(reservation_cluster.reserve_put_object_generation(
                    reservation_bucket,
                    reservation_key,
                    reservation_id,
                ))
                .unwrap();
        });
        transfer_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated reservation did not transfer the direct PUT command");
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated direct PUT transfer must remain terminal")
    });

    assert!(matches!(
        reservation_result,
        Err(ObjectPgActionError::MetadataCommandRecoveryTransferred)
    ));
    assert!(!inject_transfer.load(Ordering::SeqCst));
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id, &command, &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn object_generation_pending_drain_cannot_outlive_reservation_budget() {
    struct GateRelease(Option<std::sync::mpsc::SyncSender<()>>);

    impl GateRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for GateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let checksum = command.checksum_crc64();
    let pending_drain_blocked = Arc::new(AtomicBool::new(false));
    let (pending_drain_reached_tx, pending_drain_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (pending_drain_release_tx, pending_drain_release_rx) = std::sync::mpsc::sync_channel(1);
    let pending_drain_release_rx = Arc::new(Mutex::new(pending_drain_release_rx));
    let pending_drain_blocked_for_hook = Arc::clone(&pending_drain_blocked);
    let pending_drain_release_rx_for_hook = Arc::clone(&pending_drain_release_rx);
    let _pending_drain_hook = cluster.test_install_object_generation_pending_drain_hook(Arc::new(
        move |candidate, work_budget| {
            if candidate.checksum_crc64() != checksum
                || pending_drain_blocked_for_hook.swap(true, Ordering::SeqCst)
            {
                return;
            }
            work_budget.expire_for_test();
            pending_drain_reached_tx.send(()).unwrap();
            pending_drain_release_rx_for_hook
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(Duration::from_secs(5))
                .expect("pending-drain gate was not released");
        },
    ));

    let recovery_blocked = Arc::new(AtomicBool::new(false));
    let (recovery_reached_tx, recovery_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (recovery_release_tx, recovery_release_rx) = std::sync::mpsc::sync_channel(1);
    let recovery_release_rx = Arc::new(Mutex::new(recovery_release_rx));
    let recovery_blocked_for_hook = Arc::clone(&recovery_blocked);
    let recovery_release_rx_for_hook = Arc::clone(&recovery_release_rx);
    let _recovery_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate.checksum_crc64() != checksum
                || recovery_blocked_for_hook.swap(true, Ordering::SeqCst)
            {
                return Ok(());
            }
            recovery_reached_tx.send(()).unwrap();
            recovery_release_rx_for_hook
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(Duration::from_secs(5))
                .expect("authorized-recovery gate was not released");
            Ok(())
        }));

    let mut pending_drain_release = GateRelease(Some(pending_drain_release_tx));
    let mut recovery_release = GateRelease(Some(recovery_release_tx));
    let reservation_id = crate::tests::stream_session_id("expired-drain");
    let (reservation_result, recovery_outcome) = thread::scope(|scope| {
        let (reservation_tx, reservation_rx) = std::sync::mpsc::sync_channel(1);
        let reservation_cluster = &cluster;
        let reservation_bucket = &bucket;
        let reservation_key = &key;
        let reservation_id = &reservation_id;
        scope.spawn(move || {
            reservation_tx
                .send(reservation_cluster.reserve_put_object_generation(
                    reservation_bucket,
                    reservation_key,
                    reservation_id,
                ))
                .unwrap();
        });
        pending_drain_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("generation reservation did not reach the pending drain");

        let (recovery_tx, recovery_rx) = std::sync::mpsc::sync_channel(1);
        let recovery_cluster = &cluster;
        let recovery_command = &command;
        scope.spawn(move || {
            recovery_tx
                .send(
                    recovery_cluster.drain_pending_metadata_command_with_authorized_recovery_route(
                        pg_id,
                        recovery_command,
                        recovery_cluster,
                    ),
                )
                .unwrap();
        });
        recovery_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("authorized recovery did not reach its blocked apply");

        pending_drain_release.release();
        let reservation_result = reservation_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("expired reservation waited for authorized recovery to finish");

        recovery_release.release();
        let recovery_outcome = recovery_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("authorized recovery did not finish after release");
        (reservation_result, recovery_outcome)
    });

    assert!(matches!(
        reservation_result,
        Err(ObjectPgActionError::Store(
            StoreError::MetadataCommandContention { .. }
        ))
    ));
    assert_eq!(
        recovery_outcome.unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn stale_duplicate_metadata_command_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-index-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-index-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale_duplicate = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &stale_duplicate);

    create_test_bucket(&cluster, &second_bucket);

    assert!(pending_metadata_command_for_test(&map, pg_id, &second_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            2
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn pending_slot_drain_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "pending-drain-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        observability::TraceContextIds {
            trace_id: "trace-pending-slot-drain".to_string(),
            request_id: "request-pending-slot-drain".to_string(),
        },
    ));

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
    }
    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-pending-slot-drain"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn fresh_bucket_pg_command_finish_does_not_record_drain_attempt() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "fresh-pending-finish-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        observability::TraceContextIds {
            trace_id: "trace-fresh-pending-finish".to_string(),
            request_id: "request-fresh-pending-finish".to_string(),
        },
    ));

    let outcome = cluster
        .finish_pending_metadata_command_to_acting_set(pg_id, &bucket, &command, false)
        .unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Applied);

    let records = observability::flight_recorder_snapshot();
    assert!(
        !records.iter().any(|record| {
            record.request_id == "request-fresh-pending-finish"
                && record.event == "metadata_command_pending_slot_action"
        }),
        "freshly installed bucket commands must not be labelled as drains"
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn direct_bucket_pg_pending_finish_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-pending-finish-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        observability::TraceContextIds {
            trace_id: "trace-direct-pending-finish".to_string(),
            request_id: "request-direct-pending-finish".to_string(),
        },
    ));

    let outcome = cluster
        .drain_bucket_pg_pending_metadata_command(pg_id, &bucket, &command, false)
        .unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Applied);

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-direct-pending-finish"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_pg_pending_slot_helper_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "helper-pending-drain-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        observability::TraceContextIds {
            trace_id: "trace-helper-pending-drain".to_string(),
            request_id: "request-helper-pending-drain".to_string(),
        },
    ));

    cluster
        .drain_pending_metadata_command_pg_slot(pg_id, &bucket, &command)
        .unwrap();

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-helper-pending-drain"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_preserves_exact_applied_object_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "collect-waiter-stream-create-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key,
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.record_metadata_command_applied(node_id.as_u32(), &command)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    assert!(primary_pg
        .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &command)
        .unwrap());
    drop(primary_pg);

    let outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &command)
        .unwrap();
    assert_eq!(
        outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::Applied)
    );
    assert!(
        crate::StorageCluster::metadata_command_recovery_applied_collectable_object_command(
            &command,
            outcome.pending_outcome().unwrap()
        ),
        "collect drains must preserve exact applied object commands for idempotent recovery"
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_distinguishes_missing_and_replaced_unapplied_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "collect-waiter-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let missing_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );

    let missing_outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &missing_command)
        .unwrap();
    assert_eq!(
        missing_outcome,
        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
    );
    assert_eq!(
        missing_outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
    );

    let replacement_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key,
            crate::VersionId::from_u64(2),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &replacement_command);

    let replaced_outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &missing_command)
        .unwrap();
    assert_eq!(
        replaced_outcome,
        MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied
    );
    assert_eq!(
        replaced_outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.record_metadata_command_applied(node_id.as_u32(), &replacement_command)
            .unwrap();
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(pg_id.get()).unwrap();
        assert!(pg
            .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &replacement_command,)
            .unwrap());
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_drain_treats_missing_unapplied_command_as_abandoned() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "drain-waiter-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("71".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    let MetadataCommandRecoveryAdmission::Leader(leader_guard) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("first recovery caller should lead the single-flight");
    };

    let waiter_cluster = cluster.clone();
    let waiter_command = command.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (outcome_tx, outcome_rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        outcome_tx
            .send(
                waiter_cluster
                    .drain_pending_metadata_command_with_recovery_gate(pg_id, &waiter_command)
                    .unwrap(),
            )
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        outcome_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "waiter should block while another request owns command recovery"
    );

    let inspection_failed = Arc::new(AtomicBool::new(false));
    let inspection_failed_for_hook = Arc::clone(&inspection_failed);
    let inspection_guard =
        cluster.test_install_post_budget_metadata_command_inspection_hook(Arc::new(move |_, _| {
            (!inspection_failed_for_hook.swap(true, Ordering::SeqCst)).then_some(Err(
                StoreError::MetadataCommandContention {
                    context: "injected recovery waiter observation contention",
                },
            ))
        }));

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
    drop(pg);
    drop(leader_guard);

    let outcome = outcome_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Abandoned);
    assert!(inspection_failed.load(Ordering::SeqCst));
    drop(inspection_guard);
    waiter.join().unwrap();
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_command_finisher_waits_for_existing_exact_recovery_owner() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "bucket-command-single-flight-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let MetadataCommandRecoveryAdmission::Leader(owner) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("first bucket-command finisher should own the exact recovery flight");
    };
    cluster.test_install_metadata_command_recovery_owner_completion_hook(
        pg_id,
        &command,
        Arc::new(Barrier::new(1)),
    );

    let waiter_cluster = cluster.clone();
    let waiter_bucket = bucket.clone();
    let waiter_command = command.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx
            .send(
                waiter_cluster.finish_pending_metadata_command_to_acting_set(
                    pg_id,
                    &waiter_bucket,
                    &waiter_command,
                    false,
                ),
            )
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "the second finisher must not bypass the exact-command recovery owner"
    );

    drop(owner);
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bucket-command finisher did not resume after owner release")
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    waiter.join().unwrap();
    assert_eq!(
        cluster.test_take_metadata_command_recovery_wait_hook_observation(),
        (1, 0),
        "the second finisher must select the existing exact-command flight"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn reissue_accepts_terminal_pending_command_after_stale_primary_max_snapshot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let topology = primary.storage_node().pg_topology();
    let bucket = bucket_for_pg(topology, pg_id.get(), "terminal-pending-reissue-");
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &command)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let reloaded = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            primary.node_id(),
            primary.metadata_command_inspection_client().as_ref(),
            0,
            command.id().log_index().get(),
            &command,
            command.clone(),
        )
        .unwrap();

    assert_eq!(reloaded.as_ref(), Some(&command));
}

#[test]
fn terminal_pending_reissue_rejects_different_stale_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let topology = primary.storage_node().pg_topology();
    let current_bucket = bucket_for_pg(topology, pg_id.get(), "terminal-current-");
    let stale_bucket = bucket_for_pg(topology, pg_id.get(), "terminal-stale-");
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    let stale = create_bucket_metadata_command(pg_id, 1, stale_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &current)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let reloaded = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            primary.node_id(),
            primary.metadata_command_inspection_client().as_ref(),
            0,
            current.id().log_index().get(),
            &stale,
            current,
        )
        .unwrap();

    assert_eq!(reloaded, None);
}

#[test]
fn terminal_pending_reissue_rejects_divergent_replica_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let current_bucket = bucket_for_pg(topology, 1, "terminal-divergent-current-");
    let divergent_bucket = bucket_for_pg(topology, 1, "terminal-divergent-other-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &current)
        .unwrap();
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let divergent = create_bucket_metadata_command(pg_id, 1, divergent_bucket);
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            NodeId::new(1),
            map.node(NodeId::new(1))
                .unwrap()
                .metadata_command_inspection_client()
                .as_ref(),
            0,
            current.id().log_index().get(),
            &current,
            current.clone(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        }
    ));
}

#[test]
fn terminal_pending_reissue_rejects_live_replica_ahead_of_stale_max() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let current_bucket = bucket_for_pg(topology, 1, "terminal-ahead-current-");
    let tail_bucket = bucket_for_pg(topology, 1, "terminal-ahead-tail-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &current)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let tail = create_bucket_metadata_command(pg_id, 2, tail_bucket);
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &tail)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            NodeId::new(1),
            map.node(NodeId::new(1))
                .unwrap()
                .metadata_command_inspection_client()
                .as_ref(),
            0,
            current.id().log_index().get(),
            &current,
            current.clone(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        }
    ));
}

#[test]
fn stale_duplicate_metadata_command_index_on_non_primary_fails_closed_without_dropping_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-second-");
    let occupant_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-occupant-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale_duplicate = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    cluster
        .try_set_pending_metadata_command_for_bucket(pg_id, &second_bucket, &stale_duplicate)
        .unwrap()
        .unwrap();
    let occupant = create_bucket_metadata_command(pg_id, 2, occupant_bucket.clone());
    let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
    non_primary
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &occupant)
        .unwrap();

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let err = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: second_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        })
    ));

    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &second_bucket)
            .as_ref()
            .map(MetadataCommandEnvelope::id),
        Some(stale_duplicate.id())
    );
    {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let slot = primary_pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .unwrap();
        assert_eq!(slot.id, stale_duplicate.id());
    }
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
        if node_id == NodeId::new(0) {
            crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).unwrap();
        } else {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).is_err());
        }
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            if node_id == NodeId::new(0) { 2 } else { 1 }
        );
    }
}

#[test]
fn stale_duplicate_reissue_reloads_replaced_slot_during_primary_last_fanout() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-race-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-race-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &replacement);
    {
        let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
        non_primary
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &replacement)
            .unwrap();
    }

    let reissued = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap()
        .expect("reissue should reload the replacement pending slot");
    assert_eq!(reissued, replacement);

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &reissued)
        .unwrap();
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &reissued)
            .unwrap());
    }

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            2
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn pending_slot_reissue_deadline_expires_before_pg_lock_and_preserves_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "reissue-deadline-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let pg_lock = map.runtime_state().metadata_command_pg_lock(pg_id);
    let _guard = pg_lock.lock();
    let error = cluster
        .test_reissue_pending_metadata_command_until(
            pg_id,
            &command,
            Instant::now() + Duration::from_millis(20),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::OperationDeadlineExceeded {
            context: "acquire metadata command reissue PG lock"
        })
    ));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command),
        "deadline expiry before PG serialization must not replace the pending slot"
    );
}

#[test]
fn pending_slot_reissue_rejected_by_publication_marker_rolls_back_flight_alias() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "reissue-flight-rollback-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .mark_pending_metadata_command_publication_started(NodeId::new(1).as_u32(), &command)
        .unwrap();
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        command.payload().clone(),
    );
    let MetadataCommandRecoveryAdmission::Leader(owner) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("source command should own the recovery flight");
    };

    cluster
        .test_reissue_pending_metadata_command_with_recovery_guard_until(
            pg_id,
            &command,
            &owner,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    let replacement_admission = map.runtime_state().join_metadata_command_recovery_until(
        pg_id,
        &replacement,
        Instant::now() + Duration::from_millis(50),
    );
    assert!(matches!(
        replacement_admission,
        MetadataCommandRecoveryAdmission::Leader(_)
    ));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command)
    );
}

#[test]
fn pending_slot_reissue_adopts_replacement_after_post_commit_failure() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "reissue-post-commit-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit();

    let replacement = cluster
        .test_reissue_pending_metadata_command(pg_id, &command)
        .unwrap()
        .expect("reissue must inspect and adopt a replacement committed before failure");
    assert_eq!(replacement.id().log_index().get(), 2);
    assert_eq!(replacement.payload(), command.payload());
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(replacement)
    );
}

#[test]
fn ambiguous_bucket_reissue_retains_c1_c2_flight_for_authorized_recovery() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let applied_bucket = bucket_for_pg(topology, 1, "ambiguous-reissue-applied-");
    let pending_bucket = bucket_for_pg(topology, 1, "ambiguous-reissue-pending-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, applied_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();
    let source = create_bucket_metadata_command(pg_id, 1, pending_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &source);
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit_with_inconclusive_inspection();

    let mut work_budget = RequestWorkBudget::new(Duration::from_secs(1), None)
        .for_operation("test_ambiguous_bucket_reissue_handoff")
        .for_pg(pg_id);
    let error = cluster
        .finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            &source,
            false,
            &mut work_budget,
        )
        .expect_err("lost replacement response must require authorized recovery");
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandOutcomeUnconfirmed {
            log_index: 2,
            ..
        })
    ));
    let replacement = pending_metadata_command_for_test(&map, pg_id, &pending_bucket)
        .expect("committed C2 must remain pending");
    assert_eq!(replacement.id().log_index().get(), 2);
    assert_eq!(replacement.payload(), source.payload());
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(pg_id, &source, &replacement));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &replacement));

    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &replacement,
                &source,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn fatal_bucket_reissue_response_preserves_error_and_c1_c2_recovery_flight() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let applied_bucket = bucket_for_pg(topology, 1, "fatal-reissue-applied-");
    let pending_bucket = bucket_for_pg(topology, 1, "fatal-reissue-pending-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, applied_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();
    let source = create_bucket_metadata_command(pg_id, 1, pending_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &source);
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit_with_fatal_response();

    let mut work_budget = RequestWorkBudget::new(Duration::from_secs(1), None)
        .for_operation("test_fatal_bucket_reissue_handoff")
        .for_pg(pg_id);
    let error = cluster
        .finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            &source,
            false,
            &mut work_budget,
        )
        .expect_err("fatal replacement response validation must remain fail closed");
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let replacement = pending_metadata_command_for_test(&map, pg_id, &pending_bucket)
        .expect("committed C2 must remain pending after fatal response validation");
    assert_eq!(replacement.id().log_index().get(), 2);
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(pg_id, &source, &replacement));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &replacement));

    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &replacement,
                &source,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn pending_slot_reissue_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "pending-reissue-first-");
    let second_bucket = bucket_for_pg(topology, 1, "pending-reissue-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &stale);
    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        observability::TraceContextIds {
            trace_id: "trace-pending-slot-reissue".to_string(),
            request_id: "request-pending-slot-reissue".to_string(),
        },
    ));

    let reissued = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap()
        .expect("stale duplicate pending command should be reissued");
    assert_eq!(reissued.id().log_index().get(), 2);

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-pending-slot-reissue"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=reissue_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(second_bucket.as_str()));
}

#[test]
fn stale_duplicate_reissue_rejects_same_payload_replacement_over_divergent_gap() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-gap-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-gap-second-");
    let occupant_bucket = bucket_for_pg(topology, 1, "duplicate-gap-occupant-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &replacement);
    let divergent = create_bucket_metadata_command(pg_id, 2, occupant_bucket.clone());
    let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
    non_primary
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent)
        .unwrap();

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 3,
            ..
        })
    ));

    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &second_bucket)
            .as_ref()
            .map(MetadataCommandEnvelope::id),
        Some(replacement.id())
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
        if node_id == NodeId::new(0) {
            crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).unwrap();
        } else {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).is_err());
        }
    }
}

#[test]
fn stale_duplicate_reissue_rejects_same_payload_replacement_on_divergent_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let primary_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-primary-");
    let replacement_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-replacement-");
    let divergent_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-divergent-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);

    let primary_prefix = create_bucket_metadata_command(pg_id, 1, primary_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &primary_prefix)
        .unwrap();
    drop(primary_pg);

    let stale = create_bucket_metadata_command(pg_id, 1, replacement_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &replacement_bucket, &replacement);

    let divergent_prefix = create_bucket_metadata_command(pg_id, 1, divergent_bucket.clone());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent_prefix)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &replacement)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*primary_pg, &primary_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*primary_pg, &replacement_bucket).is_err());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &divergent_bucket).unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &replacement_bucket).unwrap();
}

#[test]
fn stale_duplicate_reissue_rejects_below_replacement_divergent_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let primary_bucket = bucket_for_pg(topology, 1, "duplicate-below-primary-");
    let replacement_bucket = bucket_for_pg(topology, 1, "duplicate-below-replacement-");
    let divergent_bucket = bucket_for_pg(topology, 1, "duplicate-below-divergent-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);

    let primary_prefix = create_bucket_metadata_command(pg_id, 1, primary_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &primary_prefix)
        .unwrap();
    drop(primary_pg);

    let stale = create_bucket_metadata_command(pg_id, 1, replacement_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &replacement_bucket, &replacement);

    let divergent_prefix = create_bucket_metadata_command(pg_id, 1, divergent_bucket.clone());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent_prefix)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*primary_pg, &primary_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*primary_pg, &replacement_bucket).is_err());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &divergent_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*non_primary_pg, &replacement_bucket).is_err());
}

#[test]
fn stale_duplicate_direct_put_commit_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "duplicate-direct-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(0));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("64".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let payload = b"direct put duplicate index reissue";
    let segment_okh = [0x64; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        bucket_write_proof.clone(),
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let stale_command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(object_pg),
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            bucket_write_proof,
        )
        .unwrap();
    drop(object_pg_store);
    let duplicate_index = stale_command.id().log_index().get();
    let occupant =
        create_bucket_metadata_command(PgId::new(object_pg), duplicate_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(
        &map,
        PgId::new(object_pg),
        &bucket,
        &stale_command,
    );

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            duplicate_index + 1
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

struct DirectPutReissueFailureFixture {
    map: Arc<LocalClusterMap>,
    cluster: Arc<crate::StorageCluster>,
    bucket: crate::BucketName,
    object_pg: u32,
    written: crate::DirectPutWrittenSegment,
    commit_req: crate::CommitDirectPutObjectReq,
    source: MetadataCommandEnvelope,
}

#[derive(Clone, Copy)]
enum DirectPutReissueSourceState {
    Unrecorded,
    Abandoned,
    ConflictingOccupant,
}

fn direct_put_reissue_failure_fixture(
    path: &std::path::Path,
    bucket_prefix: &str,
    identity_byte: u8,
    source_state: DirectPutReissueSourceState,
) -> DirectPutReissueFailureFixture {
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(path, &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, bucket_prefix);
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id =
        crate::SessionId::try_from(format!("{identity_byte:02x}").repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put replacement failure";
    let segment_okh = [identity_byte; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let source = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(object_pg_store);
    match source_state {
        DirectPutReissueSourceState::Unrecorded => {}
        DirectPutReissueSourceState::Abandoned => cluster
            .test_record_abandoned_metadata_command_to_acting_set_until(
                &source,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap(),
        DirectPutReissueSourceState::ConflictingOccupant => {
            let occupant = create_bucket_metadata_command(
                pg_id,
                source.id().log_index().get(),
                occupant_bucket,
            );
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &occupant)
                .unwrap();
        }
    }
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &source);

    DirectPutReissueFailureFixture {
        map,
        cluster,
        bucket,
        object_pg,
        written,
        commit_req,
        source,
    }
}

#[test]
fn direct_put_fatal_reissue_response_does_not_reinspect_after_abandonment() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "fatal-direct-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("74".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put fatal replacement response";
    let segment_okh = [0x74; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let source = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(object_pg_store);
    let source_index = source.id().log_index().get();
    let occupant = create_bucket_metadata_command(pg_id, source_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &source);
    primary
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit_with_fatal_response();

    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_commit = Arc::clone(&action_calls);
    let error = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_commit.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .expect_err("fatal replacement response validation must remain fail closed");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        0,
        "fatal replacement response must not rerun the direct PUT condition"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 0);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn direct_put_definitive_fatal_reissue_response_does_not_reinspect() {
    let tmp = test_util::tempdir();
    let fixture = direct_put_reissue_failure_fixture(
        tmp.path(),
        "definitive-fatal-direct-occupant-",
        0x76,
        DirectPutReissueSourceState::Abandoned,
    );
    let pg_id = PgId::new(fixture.object_pg);
    let primary = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.object_pg)
        .unwrap()
        .fail_next_pending_slot_replace_definitively_with_fatal_response();

    let mut work_budget = RequestWorkBudget::new(Duration::from_secs(1), None)
        .for_operation("test direct PUT definitive fatal reissue")
        .for_pg(pg_id);
    let outcome = fixture
        .cluster
        .apply_new_object_metadata_command_for_bucket_or_reinspect(
            pg_id,
            &fixture.bucket,
            &fixture.source,
            &mut work_budget,
        )
        .expect("definitive fatal replacement response must be returned as an outcome");
    assert!(matches!(
        outcome,
        crate::cluster::request_ops::NewObjectMetadataCommandApplyOutcome::Abandoned(
            crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                ..
            })
        )
    ));
    assert!(pending_metadata_command_for_test(&fixture.map, pg_id, &fixture.bucket).is_none());
    assert_eq!(
        fixture
            .cluster
            .test_metadata_command_recovery_flight_count(),
        0
    );
    assert_bucket_write_reservations_released(&fixture.map, &fixture.bucket);
}

#[test]
fn direct_put_definitive_fatal_reissue_response_survives_retryable_abandonment_failure() {
    let tmp = test_util::tempdir();
    let fixture = direct_put_reissue_failure_fixture(
        tmp.path(),
        "definitive-fatal-deferred-abandon-direct-occupant-",
        0x7a,
        DirectPutReissueSourceState::Unrecorded,
    );
    let pg_id = PgId::new(fixture.object_pg);
    let primary = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(fixture.object_pg).unwrap();
    primary_pg.fail_next_metadata_command_abandon_before_commit();
    drop(primary_pg);

    let error = fixture
        .cluster
        .test_finish_definitively_not_reissued_object_metadata_command_until(
            pg_id,
            &fixture.bucket,
            &fixture.source,
            crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                node_id: primary.node_id().as_u32(),
                operation: "validate definitive replacement response",
                failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                detail: crate::StorageNodeFailureDetail::new(
                    "injected fatal definitive replacement response",
                ),
            }),
            Instant::now() + Duration::from_secs(1),
        )
        .expect_err("retryable abandonment failure must not overwrite fatal replacement error");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    assert_eq!(
        pending_metadata_command_for_test(&fixture.map, pg_id, &fixture.bucket),
        Some(fixture.source.clone()),
        "deferred abandonment must retain the exact pending command"
    );

    let reservation_id = fixture
        .source
        .payload()
        .primary_bucket_write_reservation_proof()
        .expect("direct PUT command must retain its bucket-write proof")
        .reservation_id
        .clone();
    let bucket_pg_id = PgId::new(
        fixture
            .map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(&fixture.bucket),
    );
    let bucket_primary = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &fixture.bucket)
            .unwrap();
    assert!(reservations
        .iter()
        .any(|reservation| reservation.reservation_id == reservation_id));
}

#[test]
fn direct_put_fatal_abandonment_failure_is_not_rewritten_as_convergence() {
    let tmp = test_util::tempdir();
    let fixture = direct_put_reissue_failure_fixture(
        tmp.path(),
        "fatal-abandon-direct-occupant-",
        0x78,
        DirectPutReissueSourceState::Unrecorded,
    );
    let pg_id = PgId::new(fixture.object_pg);
    let primary = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.object_pg)
        .unwrap()
        .fail_next_metadata_command_abandon_before_commit_with_fatal_response();

    let error = fixture
        .cluster
        .test_finish_definitively_not_reissued_object_metadata_command_until(
            pg_id,
            &fixture.bucket,
            &fixture.source,
            crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                node_id: primary.node_id().as_u32(),
                operation: "validate definitive replacement response",
                failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                detail: crate::StorageNodeFailureDetail::new(
                    "injected fatal definitive replacement response",
                ),
            }),
            Instant::now() + Duration::from_secs(1),
        )
        .expect_err("fatal abandonment response must remain fail closed");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandIntegrity,
            ..
        })
    ));
    assert_eq!(
        pending_metadata_command_for_test(&fixture.map, pg_id, &fixture.bucket),
        Some(fixture.source.clone())
    );
    for node_id in [NodeId::new(0), NodeId::new(1), NodeId::new(2)] {
        let pg = fixture
            .map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.object_pg)
            .unwrap();
        assert!(!pg
            .metadata_command_abandoned(node_id.as_u32(), &fixture.source)
            .unwrap());
    }
}

#[test]
fn direct_put_expired_definitive_reissue_cleanup_does_not_start_abandonment() {
    let tmp = test_util::tempdir();
    let fixture = direct_put_reissue_failure_fixture(
        tmp.path(),
        "expired-definitive-direct-occupant-",
        0x79,
        DirectPutReissueSourceState::Unrecorded,
    );
    let pg_id = PgId::new(fixture.object_pg);

    let error = fixture
        .cluster
        .test_finish_definitively_not_reissued_object_metadata_command_until(
            pg_id,
            &fixture.bucket,
            &fixture.source,
            crate::ObjectPgActionError::Store(StoreError::OperationDeadlineExceeded {
                context: "injected definitive reissue response after deadline",
            }),
            Instant::now(),
        )
        .expect_err("expired abandonment must retain the pending command for recovery");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed { .. })
    ));
    assert_eq!(
        pending_metadata_command_for_test(&fixture.map, pg_id, &fixture.bucket),
        Some(fixture.source.clone())
    );
    for node_id in [NodeId::new(0), NodeId::new(1), NodeId::new(2)] {
        let pg = fixture
            .map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(fixture.object_pg)
            .unwrap();
        assert!(!pg
            .metadata_command_abandoned(node_id.as_u32(), &fixture.source)
            .unwrap());
    }
}

#[test]
fn direct_put_fatal_reissue_response_survives_published_c2_classification() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "fatal-direct-handoff-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("75".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put fatal replacement classification";
    let segment_okh = [0x75; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let source = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(object_pg_store);
    let source_index = source.id().log_index().get();
    let occupant = create_bucket_metadata_command(pg_id, source_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &source);
    primary
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit_with_fatal_response();

    let inspection_guard = Arc::new(Mutex::new(None));
    let inspection_guard_for_reissue = Arc::clone(&inspection_guard);
    let cluster_for_reissue = Arc::clone(&cluster);
    let source_id = source.id();
    let _reissue_hook = cluster.test_install_before_object_metadata_command_reissue_hook(Arc::new(
        move |command| {
            if command.id() != source_id {
                return;
            }
            let guard = cluster_for_reissue
                .test_install_post_budget_metadata_command_inspection_hook(Arc::new(
                    move |_, _| Some(Ok(Some((0x1122, 0x3344)))),
                ));
            *inspection_guard_for_reissue
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(guard);
        },
    ));

    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_commit = Arc::clone(&action_calls);
    let error = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_commit.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .expect_err("published C2 must not overwrite fatal replacement response validation");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        0,
        "published C2 must not rerun the direct PUT condition after a fatal response"
    );
    let replacement = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("committed direct PUT C2 must remain pending after fatal classification");
    assert_eq!(replacement.id().log_index().get(), source_index + 1);
    assert_eq!(replacement.payload(), source.payload());
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(pg_id, &source, &replacement));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &replacement));

    drop(
        inspection_guard
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take(),
    );
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &replacement,
                &source,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn direct_put_fatal_reissue_response_survives_deferred_c2_abandonment() {
    let tmp = test_util::tempdir();
    let fixture = direct_put_reissue_failure_fixture(
        tmp.path(),
        "deferred-fatal-direct-occupant-",
        0x77,
        DirectPutReissueSourceState::ConflictingOccupant,
    );
    let pg_id = PgId::new(fixture.object_pg);
    let primary = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    primary
        .storage_node()
        .get_pg(fixture.object_pg)
        .unwrap()
        .fail_next_pending_slot_replace_after_commit_with_fatal_response();

    primary
        .storage_node()
        .get_pg(fixture.object_pg)
        .unwrap()
        .fail_next_metadata_command_abandon_before_commit();

    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_commit = Arc::clone(&action_calls);
    let error = fixture
        .cluster
        .commit_direct_put_object_from_payload_shards(
            &fixture.commit_req,
            &fixture.written.written_shards,
            move |_| {
                action_calls_for_commit.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .expect_err("deferred C2 abandonment must not overwrite fatal replacement response");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    assert_eq!(action_calls.load(Ordering::SeqCst), 0);
    let replacement = pending_metadata_command_for_test(&fixture.map, pg_id, &fixture.bucket)
        .expect("C2 must remain pending while abandonment is deferred");
    assert_eq!(
        replacement.id().log_index().get(),
        fixture.source.id().log_index().get() + 1
    );
    assert_eq!(replacement.payload(), fixture.source.payload());
    assert!(fixture
        .map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(
            pg_id,
            &fixture.source,
            &replacement,
        ));
    assert!(fixture
        .map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &replacement));

    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &replacement,
                &fixture.source,
                &fixture.cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_clean_metadata_command_stream(&fixture.map, &[fixture.object_pg]);
}

#[test]
fn stale_duplicate_stream_append_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "duplicate-stream-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("65".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append duplicate index reissue";
    let (target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(segment.data_pg_id, &shard_batch)
        .unwrap();
    let stale_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(PgId::new(object_pg))
            .unwrap(),
        MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            target: target.clone(),
            segment: segment.clone(),
        })),
    );
    let duplicate_index = stale_command.id().log_index().get();
    let occupant =
        create_bucket_metadata_command(PgId::new(object_pg), duplicate_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(
        &map,
        PgId::new(object_pg),
        &bucket,
        &stale_command,
    );

    let mut work_budget = RequestWorkBudget::new(Duration::from_secs(10), None)
        .for_operation("test_stream_append_duplicate_index_reissue")
        .for_pg(PgId::new(object_pg));
    cluster
        .apply_new_stream_append_command(
            PgId::new(object_pg),
            &bucket,
            &stale_command,
            &mut work_budget,
        )
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            duplicate_index + 1
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn zero_apply_command_failure_records_tombstone_for_later_hash_chain_convergence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "zero-apply-first-");
    let second_bucket = bucket_for_pg(topology, 1, "zero-apply-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let first_bucket_for_hook = first_bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket().name == first_bucket_for_hook
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply create bucket failure",
                        source: std::io::Error::other("injected zero-apply create bucket failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let err = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: first_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected zero-apply create bucket failure",
                ..
            })
        ),
        "expected injected zero-apply failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &first_bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }

    create_test_bucket(&cluster, &second_bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }

    create_test_bucket(&cluster, &first_bucket);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 3);
        let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn peering_reconstruction_gather_accepts_converged_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-converged-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();

    let state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    );

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
}

#[test]
fn peering_reconstruction_gather_allows_peering_route_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-route-gather-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    let state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    );

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
}

#[test]
fn peering_reconstruction_gather_uses_primary_retained_suffix_for_lagging_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-lagging-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-lagging-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let lagging_state = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::CatchUpRequired {
            proof,
            replicas: vec![
                crate::peering::PgPeeringReplicaCatchUp {
                    node_id: NodeId::new(1),
                    from_log_index: 1,
                    from_log_hash: lagging_state.applied_log_hash,
                    to_log_index: 2,
                    to_log_hash: primary_state.applied_log_hash,
                },
                crate::peering::PgPeeringReplicaCatchUp {
                    node_id: NodeId::new(2),
                    from_log_index: 1,
                    from_log_hash: lagging_state.applied_log_hash,
                    to_log_index: 2,
                    to_log_hash: primary_state.applied_log_hash,
                },
            ],
        }
    );
}

#[test]
fn peering_reconstruction_gather_fails_closed_when_replica_is_ahead_of_primary() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-ahead-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-ahead-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(1, &second)
        .unwrap();

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::ReplicaAheadOfPrimary {
                node_id,
                replica_log_index: 2,
                primary_log_index: 1,
            }
        ) if node_id == NodeId::new(1)
    ));
}

#[test]
fn peering_reconstruction_gather_fails_closed_on_stale_replica_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-stale-epoch-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }

    let current_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = current_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = current_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id,
                replica_epoch: ClusterEpoch::INITIAL,
                cluster_epoch,
            }
        ) if node_id == NodeId::new(0) && cluster_epoch == current_epoch
    ));
}

#[test]
fn peering_replay_catches_up_lagging_replicas_from_primary_retained_entries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_catches_up_replicas_with_different_lag_distances() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=4)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(
                        topology,
                        1,
                        &format!("peering-replay-mixed-lag-{log_index}-"),
                    ),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    for command in &commands[1..] {
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(0, command)
            .unwrap();
    }
    for command in &commands[1..3] {
        map.node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(1, command)
            .unwrap();
    }
    let primary_state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn metadata_transfer_export_packages_authoritative_retained_log() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(artifact.pg_id, pg_id);
    assert_eq!(artifact.source_node_id, NodeId::new(0));
    assert_eq!(artifact.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(
        artifact.proof,
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 2);
    assert_eq!(artifact.retained_log_entries[0].log_index, 1);
    assert_eq!(artifact.retained_log_entries[0].previous_log_hash, 0);
    assert_eq!(
        artifact.retained_log_entries[1].previous_log_hash,
        artifact.retained_log_entries[0].log_hash
    );
}

#[test]
fn metadata_transfer_export_preserves_source_log_epoch_under_fenced_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let fenced_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = fenced_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = fenced_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(cluster.operation_epoch(), fenced_epoch);
    assert_eq!(source_state.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(artifact.cluster_epoch, source_state.cluster_epoch);
    assert_eq!(
        artifact.proof.applied_log_index,
        source_state.applied_log_index
    );
    assert_eq!(artifact.proof.state_digest, source_state.state_digest);
    assert_eq!(
        artifact.proof.applied_log_hash,
        source_state.applied_log_hash
    );
    assert_eq!(artifact.retained_log_entries.len(), 2);
    assert!(artifact.retained_log_entries.iter().all(|entry| matches!(
        &entry.kind,
        crate::metadata_command::MetadataCommandLogRangeEntryKind::Applied(command)
            if command.id().cluster_epoch() == source_state.cluster_epoch
    )));

    let checkpoint_suffix_artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap();
    assert_eq!(
        checkpoint_suffix_artifact.cluster_epoch,
        source_state.cluster_epoch
    );
    assert_eq!(checkpoint_suffix_artifact.retained_log_entries.len(), 1);
    assert!(checkpoint_suffix_artifact
        .retained_log_entries
        .iter()
        .all(|entry| matches!(
            &entry.kind,
            crate::metadata_command::MetadataCommandLogRangeEntryKind::Applied(command)
                if command.id().cluster_epoch() == source_state.cluster_epoch
        )));

    let checkpoint_artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        checkpoint_artifact.cluster_epoch,
        source_state.cluster_epoch
    );
    assert!(checkpoint_artifact.retained_log_entries.is_empty());
    assert_eq!(checkpoint_artifact.source_metadata_proof(), artifact.proof);
}

#[test]
fn metadata_transfer_export_rejects_active_route_without_fence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-active-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id: err_pg_id,
                cluster_epoch,
                state: PgState::Active,
            }
        ) if err_pg_id == pg_id && cluster_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn metadata_transfer_export_rejects_non_primary_source() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-non-primary-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-non-primary-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    for node_id in node_ids {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id: err_pg_id,
                cluster_epoch,
                source_node,
                primary,
            }
        ) if err_pg_id == pg_id
            && cluster_epoch == ClusterEpoch::INITIAL
            && source_node == NodeId::new(1)
            && primary == NodeId::new(0)
    ));
}

#[test]
fn metadata_transfer_export_packages_retained_suffix_with_base_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-missing-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-missing-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .test_delete_metadata_command_log_entry(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix
    );
    assert_eq!(artifact.base_proof.applied_log_index, 1);
    assert_eq!(
        artifact.base_proof.applied_log_hash.value(),
        artifact.retained_log_entries[0].previous_log_hash
    );
    assert_eq!(
        Some(artifact.base_proof.state_digest.value()),
        artifact.retained_log_entries[0]
            .pre_state_digest
            .map(crate::control_plane::CanonicalStateDigest::value)
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
}

#[test]
fn metadata_transfer_import_rejects_suffix_into_empty_destination_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-empty-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-empty-suffix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .test_delete_metadata_command_log_entry(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix
    );
    assert_eq!(artifact.base_proof.applied_log_index, 1);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::peering::PgPeeringReconstructionFailure::Reconstruction(
                crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination { .. }
            )
        ),
        "unexpected error: {err:?}"
    );

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
    }
}

#[test]
fn metadata_transfer_export_fails_closed_when_retained_state_digest_chain_is_broken() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-digest-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-digest-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .test_increment_metadata_command_log_pre_state_digest(ClusterEpoch::INITIAL, 2)
        .unwrap();
    drop(source_pg);

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                node_id,
                pg_id: err_pg_id,
                log_index: 2,
                ..
            }
        ) if node_id == NodeId::new(0) && err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_export_fails_closed_on_abandoned_retained_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 1,
            }
        ) if node_id == NodeId::new(0)
    ));
}

#[test]
fn metadata_transfer_import_replays_rebased_artifact_to_peering_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(artifact.pg_id(), pg_id);
    assert_eq!(artifact.source_node_id(), NodeId::new(0));
    assert_eq!(artifact.cluster_epoch(), cluster.operation_epoch());
    assert_eq!(artifact.source_metadata_proof(), artifact.proof);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();
    let retried_proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof, expected_proof);
    assert_eq!(retried_proof, proof);
    assert_eq!(proof.applied_log_index, artifact.proof.applied_log_index);
    assert_eq!(proof.state_digest, artifact.proof.state_digest);
    assert_ne!(
        proof.applied_log_hash, artifact.proof.applied_log_hash,
        "rebased destination epoch must produce a distinct metadata log hash"
    );
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_retries_after_partial_destination_pending_failure() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-second-");
    let pending_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-pending-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let pending = create_bucket_metadata_command_at_epoch(
        destination_epoch,
        pg_id,
        99,
        pending_bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &map,
        NodeId::new(2),
        pg_id,
        &pending_bucket,
        &pending,
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand { node_id }
        ) if node_id == NodeId::new(2)
    ));

    let imported_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let imported_state = imported_pg.metadata_command_replica_state().unwrap();
    assert_eq!(imported_state.cluster_epoch, destination_epoch);
    assert_eq!(
        imported_state.applied_log_index,
        expected_proof.applied_log_index
    );
    assert_eq!(
        imported_state.applied_log_hash,
        expected_proof.applied_log_hash
    );
    assert_eq!(imported_state.state_digest, expected_proof.state_digest);
    crate::PgMetadataStore::head_bucket(&*imported_pg, &first_bucket).unwrap();
    crate::PgMetadataStore::head_bucket(&*imported_pg, &second_bucket).unwrap();
    drop(imported_pg);

    let blocked_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let blocked_state = blocked_pg.metadata_command_replica_state().unwrap();
    assert_eq!(blocked_state.applied_log_index, 0);
    assert!(blocked_pg
        .pending_metadata_command_envelope(2, destination_epoch)
        .unwrap()
        .is_some());
    drop(blocked_pg);

    clear_pending_metadata_command_for_node_for_test(&map, NodeId::new(2), pg_id);
    let proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_rejects_artifact_with_mismatched_final_state_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-final-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-final-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let mut artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    artifact
        .retained_log_entries
        .last_mut()
        .expect("test artifact should contain retained commands")
        .post_state_digest = Some(crate::control_plane::CanonicalStateDigest::for_test(
        artifact.proof.state_digest.value().wrapping_add(1),
    ));

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { .. }
        ),
        "unexpected error: {err:?}"
    );
    assert!(
        err.to_string().contains("state digest fork"),
        "unexpected error: {err}"
    );
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
    }
}

#[test]
fn metadata_transfer_import_rejects_unproven_prefix_zero_base_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-first-");
    let source_second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-second-");
    let unrelated_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-unrelated-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source_first = create_bucket_metadata_command(pg_id, 1, source_first_bucket.clone());
    let source_second = create_bucket_metadata_command(pg_id, 2, source_second_bucket.clone());
    let unrelated = create_bucket_metadata_command(pg_id, 1, unrelated_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &source_first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &source_second)
        .unwrap();
    drop(source_pg);
    let mut artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    let unrelated_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    unrelated_pg
        .apply_metadata_command_and_record(1, &unrelated)
        .unwrap();
    let unrelated_state = unrelated_pg.metadata_command_replica_state().unwrap();
    drop(unrelated_pg);
    drop(cluster);

    artifact
        .retained_log_entries
        .first_mut()
        .expect("test artifact should contain retained commands")
        .pre_state_digest = Some(unrelated_state.state_digest);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { .. }
        ),
        "unexpected error: {err:?}"
    );
    assert!(
        err.to_string().contains("state digest fork"),
        "unexpected error: {err}"
    );
    let destination_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let destination_state = destination_pg.metadata_command_replica_state().unwrap();
    assert_eq!(destination_state, unrelated_state);
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &unrelated_bucket).is_ok());
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &source_first_bucket).is_err());
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &source_second_bucket).is_err());
}

#[test]
fn metadata_transfer_import_adopts_matching_existing_destination_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-adopt-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-adopt-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Empty
    );
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof.applied_log_index, artifact.proof.applied_log_index);
    assert_eq!(proof.state_digest, artifact.proof.state_digest);
    assert_ne!(proof.applied_log_hash, artifact.proof.applied_log_hash);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_suffix_over_proven_older_prefix_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_retained_suffix_over_exact_source_base_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-third-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let third = create_bucket_metadata_command(pg_id, 3, third_bucket.clone());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let base_state = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &third)
        .unwrap();
    let mut artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    artifact.base_kind = crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix;
    artifact.base_proof = crate::control_plane::PgMetadataProof::current(
        base_state.applied_log_index,
        base_state.applied_log_hash,
        base_state.state_digest,
    );
    artifact
        .retained_log_entries
        .retain(|entry| entry.log_index > base_state.applied_log_index);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    assert_eq!(proof.applied_log_index, 1);
    assert_ne!(proof.applied_log_hash, artifact.proof.applied_log_hash);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_rejects_older_prefix_with_unproven_log_hash() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-prefix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-prefix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.test_increment_metadata_command_replica_applied_log_hash()
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::peering::PgPeeringReconstructionFailure::Reconstruction(
                crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination { .. }
            )
        ),
        "unexpected error: {err:?}"
    );

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(state.applied_log_index, 1);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
    }
}

#[test]
fn metadata_transfer_import_installs_checkpoint_base() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-base-");
    let pending_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-base-pending-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert!(artifact.checkpoint_base().is_some());
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    let pending = create_bucket_metadata_command_at_epoch(
        destination_epoch,
        pg_id,
        99,
        pending_bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &map,
        NodeId::new(2),
        pg_id,
        &pending_bucket,
        &pending,
    );
    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand { node_id }
        ) if node_id == NodeId::new(2)
    ));

    clear_pending_metadata_command_for_node_for_test(&map, NodeId::new(2), pg_id);
    let retry_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    assert_eq!(retry_proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert_eq!(state.state_digest, expected_proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_retained_suffix_over_checkpoint_base() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-third-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let third = create_bucket_metadata_command(pg_id, 3, third_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let checkpoint_proof = crate::control_plane::PgMetadataProof::current(
        checkpoint.applied_log_index,
        checkpoint.applied_log_hash,
        checkpoint.state_digest,
    );
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &third)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint.clone(),
        )
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.source_base_metadata_proof(), checkpoint_proof);
    assert_eq!(artifact.retained_log_entries.len(), 2);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    let rebased_commands =
        crate::peering::rebase_pg_metadata_transfer_artifact_commands(&artifact, destination_epoch)
            .unwrap();
    let partial_client = map
        .node(NodeId::new(1))
        .unwrap()
        .metadata_command_peering_client()
        .clone();
    let partial_route = partial_client
        .open_metadata_command_peering_route(pg_id, destination_epoch)
        .unwrap();
    partial_route
        .install_metadata_transfer_checkpoint_base(&checkpoint)
        .unwrap();
    partial_route
        .replay_metadata_command_for_peering(&rebased_commands[0].command)
        .unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let retry_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof, expected_proof);
    assert_eq!(retry_proof, expected_proof);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, expected_proof.applied_log_index);
        assert_eq!(state.applied_log_hash, expected_proof.applied_log_hash);
        assert_eq!(state.state_digest, expected_proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn prop_metadata_transfer_checkpoint_suffix_retry_resumes_every_prefix(
        total_commands in 2_usize..7,
        checkpoint_offset in 0_usize..6,
        partial_prefix_offset in 0_usize..7,
    ) {
        let checkpoint_after = 1 + checkpoint_offset % (total_commands - 1);
        let suffix_len = total_commands - checkpoint_after;
        let partial_prefix_len = partial_prefix_offset % (suffix_len + 1);
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let buckets = (1..=total_commands)
            .map(|log_index| {
                let prefix = format!(
                    "prop-transfer-checkpoint-suffix-{total_commands}-{checkpoint_after}-{partial_prefix_len}-{log_index}-"
                );
                bucket_for_pg(topology, 1, &prefix)
            })
            .collect::<Vec<_>>();
        set_route_primary(&mut map, 1, NodeId::new(0));
        set_route_state(&mut map, 1, PgState::Peering);
        let mut map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let pg_id = PgId::new(1);
        let source_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let mut checkpoint = None;
        for (offset, bucket) in buckets.iter().enumerate() {
            let log_index = (offset + 1) as u64;
            let command = create_bucket_metadata_command(pg_id, log_index, bucket.clone());
            source_pg
                .apply_metadata_command_and_record(0, &command)
                .unwrap();
            if offset + 1 == checkpoint_after {
                checkpoint = Some(
                    source_pg
                        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                        .unwrap(),
                );
            }
        }
        drop(source_pg);

        let checkpoint = checkpoint.expect("generated checkpoint position is in range");
        let artifact = cluster
            .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
                pg_id,
                NodeId::new(0),
                checkpoint.clone(),
            )
            .unwrap();
        prop_assert_eq!(artifact.retained_log_entries.len(), suffix_len);
        drop(cluster);

        let destination_epoch = ClusterEpoch::new(2).unwrap();
        let map_mut = Arc::get_mut(&mut map).unwrap();
        map_mut.epoch = destination_epoch;
        let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
        route.cluster_epoch = destination_epoch;
        route.primary_node_id = NodeId::new(1);
        route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
        route.state = PgState::Peering;
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
            &artifact,
            destination_epoch,
        )
        .unwrap();
        let rebased_commands =
            crate::peering::rebase_pg_metadata_transfer_artifact_commands(
                &artifact,
                destination_epoch,
            )
            .unwrap();
        prop_assert_eq!(rebased_commands.len(), suffix_len);

        let partial_client = map
            .node(NodeId::new(1))
            .unwrap()
            .metadata_command_peering_client()
            .clone();
        let partial_route = partial_client
            .open_metadata_command_peering_route(pg_id, destination_epoch)
            .unwrap();
        partial_route
            .install_metadata_transfer_checkpoint_base(&checkpoint)
            .unwrap();
        for command in &rebased_commands[..partial_prefix_len] {
            partial_route
                .replay_metadata_command_for_peering(&command.command)
                .unwrap();
        }

        let proof = cluster
            .import_pg_metadata_transfer_from_retained_log(&artifact)
            .unwrap();
        let retry_proof = cluster
            .import_pg_metadata_transfer_from_retained_log(&artifact)
            .unwrap();

        prop_assert_eq!(proof, expected_proof);
        prop_assert_eq!(retry_proof, expected_proof);
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            prop_assert_eq!(state.cluster_epoch, destination_epoch);
            prop_assert_eq!(state.applied_log_index, expected_proof.applied_log_index);
            prop_assert_eq!(state.applied_log_hash, expected_proof.applied_log_hash);
            prop_assert_eq!(state.state_digest, expected_proof.state_digest);
            for bucket in &buckets {
                crate::PgMetadataStore::head_bucket(&*pg, bucket).unwrap();
            }
        }
    }
}

#[test]
fn metadata_transfer_checkpoint_suffix_export_rejects_wrong_pg_checkpoint() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pg_id = PgId::new(1);
    let checkpoint_pg_id = PgId::new(2);
    let bucket = bucket_for_pg(topology, 2, "metadata-transfer-wrong-checkpoint-pg-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    set_route_primary(&mut map, 2, NodeId::new(0));
    set_route_state(&mut map, 2, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(checkpoint_pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                pg_id: err_pg_id,
                ..
            }
        ) if err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_checkpoint_suffix_export_rejects_checkpoint_ahead_of_source() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-source-prefix-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-ahead-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    drop(source_pg);
    let ahead_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    ahead_pg
        .apply_metadata_command_and_record(1, &first)
        .unwrap();
    ahead_pg
        .apply_metadata_command_and_record(1, &second)
        .unwrap();
    let checkpoint = ahead_pg
        .metadata_command_checkpoint(1, ClusterEpoch::INITIAL)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                pg_id: err_pg_id,
                ..
            }
        ) if err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_checkpoint_import_replaces_stale_older_epoch_destination() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_bucket = bucket_for_pg(topology, 1, "metadata-transfer-stale-checkpoint-source-");
    let stale_bucket = bucket_for_pg(topology, 1, "metadata-transfer-stale-checkpoint-dirty-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source_command =
        create_bucket_metadata_command_at_epoch(ClusterEpoch::INITIAL, pg_id, 1, source_bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &source_command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let stale_command =
        create_bucket_metadata_command_at_epoch(ClusterEpoch::INITIAL, pg_id, 1, stale_bucket);
    let stale_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    stale_pg
        .apply_metadata_command_and_record(1, &stale_command)
        .unwrap();
    drop(stale_pg);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let expected_import_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    let imported = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(imported, expected_import_proof);
    let replaced_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let replaced_state = replaced_pg.metadata_command_replica_state().unwrap();
    assert_eq!(replaced_state.cluster_epoch, destination_epoch);
    assert_eq!(replaced_state.applied_log_index, 0);
    assert_eq!(replaced_state.applied_log_hash, 0);
    assert_eq!(replaced_state.state_digest, artifact.proof.state_digest);
    let retained_rows = replaced_pg.test_metadata_command_log_row_count().unwrap();
    assert_eq!(retained_rows, 0);
}

#[test]
fn metadata_transfer_live_export_replays_self_contained_retained_log() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-retained-replay-");
    let pg_id = PgId::new(1);
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);

    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Empty
    );
    assert_eq!(
        artifact.source_base_metadata_proof().state_digest,
        crate::PgStore::canonical_empty_metadata_state_digest()
    );
    assert!(artifact.checkpoint_base().is_none());
    assert_eq!(artifact.retained_log_entries.len(), 1);
}

#[test]
fn metadata_transfer_live_export_checkpoints_nonempty_genesis_base() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-genesis-base-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-genesis-base-second-");
    let pg_id = PgId::new(1);
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);

    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let first_state = source_pg.metadata_command_replica_state().unwrap();
    let source_epoch = ClusterEpoch::new(2).unwrap();
    source_pg
        .initialize_metadata_transfer_matching_state(
            0,
            source_epoch,
            0,
            crate::control_plane::MetadataCommandLogHash::genesis(),
            first_state.state_digest,
        )
        .unwrap();
    let second =
        create_bucket_metadata_command_at_epoch(source_epoch, pg_id, 1, second_bucket.clone());
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);

    map.epoch = source_epoch;
    let route = map.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = source_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0)]);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let retained = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        retained.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Empty
    );
    assert_eq!(
        retained.source_base_metadata_proof().state_digest,
        first_state.state_digest
    );
    let destination_before = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    assert_ne!(destination_before.state_digest, first_state.state_digest);

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(3).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let imported = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(imported, expected);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(
            pg.metadata_command_replica_state().unwrap().state_digest,
            imported.state_digest
        );
    }
}

#[test]
fn metadata_transfer_live_export_falls_back_to_checkpoint_without_retained_state_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-fallback-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    source_pg
        .test_clear_metadata_command_log_post_state_digest(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    let checkpoint = artifact.checkpoint_base().unwrap();
    assert_eq!(checkpoint.pg_id, pg_id);
    assert_eq!(checkpoint.applied_log_index, 1);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
}

#[test]
fn metadata_transfer_live_checkpoint_fallback_preserves_source_epoch_under_fenced_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-checkpoint-fallback-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .test_clear_metadata_command_log_post_state_digest(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let fenced_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = fenced_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = fenced_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                node_id,
                log_index: 1,
                ..
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.cluster_epoch, source_state.cluster_epoch);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
}

#[test]
fn metadata_transfer_live_export_prefers_checkpoint_suffix_candidate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let mut corrupt_newest_checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    corrupt_newest_checkpoint.checkpoint_crc64 ^= 1;
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .test_clear_metadata_command_log_post_state_digest(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                node_id,
                log_index: 1,
                ..
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
            pg_id,
            NodeId::new(0),
            [corrupt_newest_checkpoint, checkpoint.clone()],
        )
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.cluster_epoch, checkpoint.cluster_epoch);
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn prop_metadata_transfer_checkpoint_candidate_selection_picks_newest_usable(
        total_commands in 2_usize..7,
        valid_checkpoint_offset in 0_usize..6,
    ) {
        let valid_after = 1 + valid_checkpoint_offset % (total_commands - 1);
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let buckets = (1..=total_commands)
            .map(|log_index| {
                let prefix = format!(
                    "prop-transfer-candidate-{total_commands}-{valid_after}-{log_index}-"
                );
                bucket_for_pg(topology, 1, &prefix)
            })
            .collect::<Vec<_>>();
        set_route_primary(&mut map, 1, NodeId::new(0));
        set_route_state(&mut map, 1, PgState::Peering);
        let pg_id = PgId::new(1);
        let source_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let mut checkpoints = Vec::with_capacity(total_commands);
        for (offset, bucket) in buckets.iter().enumerate() {
            let log_index = (offset + 1) as u64;
            let command = create_bucket_metadata_command(pg_id, log_index, bucket.clone());
            source_pg
                .apply_metadata_command_and_record(0, &command)
                .unwrap();
            checkpoints.push(
                source_pg
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
            );
        }
        let source_state = source_pg.metadata_command_replica_state().unwrap();
        source_pg
            .test_clear_metadata_command_log_post_state_digest(ClusterEpoch::INITIAL, 1)
            .unwrap();
        drop(source_pg);

        let valid_checkpoint = checkpoints[valid_after - 1].clone();
        let mut candidates = Vec::new();
        for checkpoint in checkpoints.iter().rev() {
            let mut candidate = checkpoint.clone();
            if candidate.applied_log_index > valid_after as u64 {
                candidate.checkpoint_crc64 ^= 1;
            }
            candidates.push(candidate);
        }
        let mut wrong_pg_candidate = valid_checkpoint.clone();
        wrong_pg_candidate.pg_id = PgId::new(0);
        candidates.insert(0, wrong_pg_candidate);
        let mut ahead_candidate = valid_checkpoint.clone();
        ahead_candidate.applied_log_index = source_state.applied_log_index + 1;
        candidates.push(ahead_candidate);

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let retained_log_err = cluster
            .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
            .unwrap_err();
        let retained_log_fallback_allowed = matches!(
            retained_log_err,
            crate::peering::PgPeeringReconstructionFailure::Reconstruction(
                crate::peering::PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id,
                    log_index: 1,
                    ..
                }
            ) if node_id == NodeId::new(0)
        );
        prop_assert!(retained_log_fallback_allowed);

        let artifact = cluster
            .export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
                pg_id,
                NodeId::new(0),
                candidates,
            )
            .unwrap();

        prop_assert_eq!(
            artifact.source_base_kind(),
            crate::peering::PgMetadataTransferBaseKind::Checkpoint
        );
        prop_assert_eq!(
            artifact.source_base_metadata_proof(),
            crate::control_plane::PgMetadataProof::current(valid_checkpoint.applied_log_index, valid_checkpoint.applied_log_hash, valid_checkpoint.state_digest)
        );
        prop_assert_eq!(artifact.retained_log_entries.len(), total_commands - valid_after);
        if let Some(first_retained) = artifact.retained_log_entries.first() {
            prop_assert_eq!(first_retained.log_index, valid_after as u64 + 1);
        }
        prop_assert_eq!(
            artifact.source_metadata_proof(),
            crate::control_plane::PgMetadataProof::current(source_state.applied_log_index, source_state.applied_log_hash, source_state.state_digest)
        );
    }
}

#[test]
fn metadata_transfer_live_export_uses_durable_checkpoint_candidate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-durable-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-durable-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .test_clear_metadata_command_log_post_state_digest(ClusterEpoch::INITIAL, 1)
        .unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
}

#[test]
fn metadata_transfer_live_export_uses_checkpoint_candidate_after_compaction() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-compacted-first-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert!(matches!(
        source_pg
            .compact_metadata_command_log(ClusterEpoch::INITIAL)
            .unwrap(),
        crate::pg_store::MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 1,
            ..
        }
    ));
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                node_id,
                ..
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
    assert!(artifact.retained_log_entries.is_empty());
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
}

#[test]
fn metadata_transfer_live_export_uses_checkpoint_when_retained_log_has_compacted_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert!(matches!(
        source_pg
            .compact_metadata_command_log(ClusterEpoch::INITIAL)
            .unwrap(),
        crate::pg_store::MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 1,
            ..
        }
    ));
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let retained_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        retained_artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix
    );

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest
        )
    );
}

#[test]
fn routine_metadata_checkpoint_records_current_primary_candidate_once() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "routine-metadata-checkpoint-");
    let second_bucket = bucket_for_pg(topology, 1, "routine-metadata-checkpoint-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 1);
    assert_eq!(summary.already_current, 0);
    assert_eq!(summary.compacted, 1);
    assert_eq!(summary.compaction_deleted_entries, 1);
    assert_eq!(summary.failed, 0);

    let primary_pg = cluster
        .local_pg_route(pg_id)
        .and_then(|route| map.node(route.primary_node_id()))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let candidates = primary_pg
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 4)
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].applied_log_index, state.applied_log_index);
    assert_eq!(candidates[0].applied_log_hash, state.applied_log_hash);
    assert_eq!(candidates[0].state_digest, state.state_digest);
    let stats = primary_pg
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(stats.retained_entries, 0);
    assert_eq!(
        primary_pg
            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        state.applied_log_index
    );
    drop(primary_pg);

    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 0);
    assert_eq!(summary.already_current, 1);
    assert_eq!(summary.compaction_noop, 1);
    assert_eq!(summary.compaction_deleted_entries, 0);
    assert_eq!(summary.failed, 0);

    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let next_state = primary_pg.metadata_command_replica_state().unwrap();
    assert_eq!(next_state.applied_log_index, state.applied_log_index + 1);
    assert_eq!(
        crate::cluster::metadata_command_checkpoint_record_decision(
            &next_state,
            candidates.first(),
            1,
            usize::MAX,
        )
        .unwrap(),
        crate::cluster::MetadataCommandCheckpointRecordDecision::Record
    );
    assert_eq!(
        crate::cluster::metadata_command_checkpoint_record_decision(
            &next_state,
            candidates.first(),
            crate::cluster::METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE,
            1,
        )
        .unwrap(),
        crate::cluster::MetadataCommandCheckpointRecordDecision::Record
    );
    drop(primary_pg);

    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 0);
    assert_eq!(summary.already_current, 0);
    assert_eq!(summary.skipped_cadence, 1);
    assert_eq!(summary.compaction_noop, 1);
    assert_eq!(summary.compaction_deleted_entries, 0);
    assert_eq!(summary.failed, 0);
}

#[test]
fn routine_metadata_checkpoint_skips_active_empty_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.skipped_empty, 1);
    assert_eq!(summary.recorded, 0);
    assert_eq!(summary.compacted, 0);
    assert_eq!(summary.failed, 0);
}

#[test]
fn routine_metadata_checkpoint_scan_is_pg_bounded_and_canonical() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[3, 1, 2], ec_shape).unwrap();
    for pg_id in [1, 2, 3] {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let mut cursor = crate::cluster::MetadataCommandCheckpointScanCursor::default();
    for expected_pg_id in [1, 2, 3, 1] {
        let summary = cluster
            .record_routine_metadata_command_checkpoints_with_limit(&mut cursor, 4, 1)
            .unwrap();
        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.skipped_empty, 1);
        assert!(summary.limit_reached);
        assert_eq!(cursor.after_pg_id, Some(PgId::new(expected_pg_id)));
    }
}

#[test]
fn metadata_command_checkpoint_catalogue_retains_newest_candidates_per_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();

    for log_index in 1..=10 {
        let bucket = bucket_for_pg(
            topology,
            1,
            &format!("metadata-checkpoint-retention-{log_index}-"),
        );
        let command = create_bucket_metadata_command(pg_id, log_index, bucket);
        source_pg
            .apply_metadata_command_and_record(0, &command)
            .unwrap();
        source_pg
            .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
    }

    let candidates = source_pg
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 16)
        .unwrap();
    assert_eq!(candidates.len(), 8);
    assert_eq!(candidates.first().unwrap().applied_log_index, 10);
    assert_eq!(candidates.last().unwrap().applied_log_index, 3);
}

#[test]
fn metadata_transfer_live_export_checkpoint_candidates_do_not_mask_hard_source_error() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-hard-error-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let err = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
            pg_id,
            NodeId::new(0),
            [checkpoint],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::error::PgMetadataTransferError::Reconstruction { ref message }
            if message.contains("expected Peering")
    ));
}

#[test]
fn metadata_transfer_live_export_falls_back_to_checkpoint_for_abandoned_retained_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 1,
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    let checkpoint = artifact.checkpoint_base().unwrap();
    assert_eq!(checkpoint.pg_id, pg_id);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::current(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
}

#[test]
fn metadata_transfer_import_rejects_checkpoint_base_without_payload() {
    let artifact = crate::peering::PgMetadataTransferArtifact {
        pg_id: PgId::new(1),
        source_node_id: NodeId::new(0),
        cluster_epoch: ClusterEpoch::INITIAL,
        base_kind: crate::peering::PgMetadataTransferBaseKind::Checkpoint,
        base_proof: crate::control_plane::PgMetadataProof::current(1, 2, 3),
        checkpoint_base: None,
        proof: crate::control_plane::PgMetadataProof::current(1, 2, 3),
        retained_log_entries: Vec::new(),
    };

    let err = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { ref message }
                if message.contains("missing checkpoint base payload")
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn metadata_transfer_import_replays_suffix_over_round_trip_base_state() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 1, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-base-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-base-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    map.pg_routes.get_mut(&PgId::new(1)).unwrap().acting_set =
        Arc::from([NodeId::new(0), NodeId::new(1)]);
    let mut map = Arc::new(map);
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let first_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let first_destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = first_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = first_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let imported_base_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&first_artifact)
        .unwrap();
    drop(cluster);

    let second = create_bucket_metadata_command_at_epoch(
        first_destination_epoch,
        pg_id,
        2,
        second_bucket.clone(),
    );
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let pre_state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(pre_state.state_digest, imported_base_proof.state_digest);
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let second_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(4).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&second_artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &second_artifact,
        return_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    let mut pre_reopen_states = BTreeMap::new();
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, return_epoch);
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        pre_reopen_states.insert(node_id, state);
        pg.record_current_metadata_command_checkpoint(node_id.as_u32(), return_epoch)
            .unwrap();
        assert!(matches!(
            pg.compact_metadata_command_log(return_epoch).unwrap(),
            crate::pg_store::MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 2,
                ..
            }
        ));
    }
    drop(cluster);
    drop(map);

    let reopened_node_ids = [NodeId::new(0), NodeId::new(1)];
    let reopened_configs = reopened_node_ids.iter().map(|&node_id| {
        LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join(format!("node-{:04}", node_id.as_u32())),
        )
    });
    let mut reopened_map = LocalClusterMap::open_with_configs_and_epoch(
        NodeId::new(0),
        reopened_configs,
        &[1],
        ec_shape,
        return_epoch,
    )
    .unwrap();
    set_route_primary(&mut reopened_map, 1, NodeId::new(0));
    reopened_map.pg_routes.get_mut(&pg_id).unwrap().acting_set =
        Arc::from([NodeId::new(0), NodeId::new(1)]);
    let reopened_map = Arc::new(reopened_map);
    for node_id in reopened_node_ids {
        let pg = reopened_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let reopened_state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(
            reopened_state,
            *pre_reopen_states.get(&node_id).unwrap(),
            "checkpointed returned transfer proof must survive reopen for node {node_id:?}"
        );
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_suffix_across_repeated_reshuffles() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-third-");
    let pg_id = PgId::new(1);

    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    map.pg_routes.get_mut(&pg_id).unwrap().acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    let mut map = Arc::new(map);

    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let first_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let first_destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = first_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = first_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    cluster
        .import_pg_metadata_transfer_from_retained_log(&first_artifact)
        .unwrap();
    drop(cluster);

    let second = create_bucket_metadata_command_at_epoch(
        first_destination_epoch,
        pg_id,
        2,
        second_bucket.clone(),
    );
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let second_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(3).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    cluster
        .import_pg_metadata_transfer_from_retained_log(&second_artifact)
        .unwrap();
    drop(cluster);

    let third =
        create_bucket_metadata_command_at_epoch(return_epoch, pg_id, 3, third_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &third)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let third_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let second_destination_epoch = ClusterEpoch::new(4).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = second_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = second_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&third_artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &third_artifact,
        second_destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    assert_eq!(proof.applied_log_index, 3);

    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, second_destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_bootstraps_matching_old_base_before_suffix_replay() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-return-base-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-return-base-second-");
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let source_epoch = ClusterEpoch::new(2).unwrap();
    let second =
        create_bucket_metadata_command_at_epoch(source_epoch, pg_id, 1, second_bucket.clone());

    set_route_primary(&mut map, 1, NodeId::new(2));
    set_route_state(&mut map, 1, PgState::Peering);
    map.epoch = source_epoch;
    let route = map.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = source_epoch;
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    let mut map = Arc::new(map);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let base = pg.metadata_command_replica_state().unwrap();
        pg.initialize_metadata_transfer_matching_state(
            node_id.as_u32(),
            source_epoch,
            0,
            crate::control_plane::MetadataCommandLogHash::genesis(),
            base.state_digest,
        )
        .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }

    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(3).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof =
        crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(&artifact, return_epoch)
            .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, return_epoch);
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_initializes_empty_destination_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(artifact.source_metadata_proof().applied_log_index, 0);
    assert_eq!(artifact.source_metadata_proof().applied_log_hash, 0);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert_eq!(state.state_digest, expected_proof.state_digest);
    }
}

fn create_bucket_metadata_command_at_epoch(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    log_index: u64,
    bucket: crate::BucketName,
) -> MetadataCommandEnvelope {
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config_for_test(&config, 1_234, log_index).unwrap(),
        ),
    )
}

#[test]
fn metadata_transfer_import_rejects_dirty_destination_replicas() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-source-");
    let stale_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-stale-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source = create_bucket_metadata_command(pg_id, 1, source_bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &source)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let stale = create_bucket_metadata_command_at_epoch(destination_epoch, pg_id, 1, stale_bucket);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &stale)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id: dirty_pg_id,
                cluster_epoch,
                ..
            }
        ) if node_id == NodeId::new(1)
            && dirty_pg_id == pg_id
            && cluster_epoch == destination_epoch
    ));
}

#[test]
fn metadata_transfer_import_rejects_active_destination_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-active-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Active;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch,
            state: PgState::Active,
        }) if cluster_epoch == destination_epoch
    ));
}

#[test]
fn peering_replay_retries_after_partial_replica_catchup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=3)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(
                        topology,
                        1,
                        &format!("peering-replay-partial-retry-{log_index}-"),
                    ),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        for command in &commands[1..] {
            pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                .unwrap();
        }
    }
    let primary_state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_fetches_primary_retained_entries_across_batches() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let command_count =
        crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES + 2;
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=command_count)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(topology, 1, &format!("peering-replay-batch-{log_index}-")),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    for command in &commands[1..] {
        primary_pg
            .apply_metadata_command_and_record(0, command)
            .unwrap();
    }
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_rejects_active_route_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "active-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "active-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();

    let replica_states_before = [NodeId::new(1), NodeId::new(2)].map(|node_id| {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
    });

    let err = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch,
            state: PgState::Active,
        }) if cluster_epoch == cluster.operation_epoch()
    ));
    for (node_id, expected_state) in [NodeId::new(1), NodeId::new(2)]
        .into_iter()
        .zip(replica_states_before)
    {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, expected_state);
    }
}

#[test]
fn peering_replay_fails_closed_on_primary_abandoned_tombstone() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-tombstone-first-");
    let abandoned_bucket = bucket_for_pg(topology, 1, "peering-tombstone-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let abandoned = create_bucket_metadata_command(pg_id, 2, abandoned_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &abandoned)
        .unwrap();

    let err = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 2,
            }
        ) if node_id == NodeId::new(1) || node_id == NodeId::new(2)
    ));
}

#[test]
fn peering_replay_then_fresh_heartbeats_complete_authority_activation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(&tmp.path().join("nodes"), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "authority-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "authority-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::current(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            vec![
                (NodeId::new(0), "node-0.sock".to_owned()),
                (NodeId::new(1), "node-1.sock".to_owned()),
                (NodeId::new(2), "node-2.sock".to_owned()),
            ],
            vec![pg_id],
        )
        .unwrap();

    for (node_id, now_ms) in node_ids.into_iter().zip([990, 991, 992]) {
        heartbeat_authority_node(&mut authority, node_id, now_ms);
    }
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(0), pg_id, 1_000);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(1), pg_id, 1_001);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(2), pg_id, 1_002);
    let pre_replay_completion = authority.complete_pg_peering(pg_id, NodeId::new(0), 0, 1_003);
    assert!(
        matches!(
            pre_replay_completion,
            Err(crate::control_plane::ControlPlaneError::PgPeeringMetadataProofMismatch { .. })
        ),
        "unexpected pre-replay completion result: {pre_replay_completion:?}"
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );

    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(0), pg_id, 1_010);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(1), pg_id, 1_011);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(2), pg_id, 1_012);

    let activated = authority
        .complete_pg_peering(pg_id, NodeId::new(0), 0, 1_013)
        .unwrap();
    let pg = activated.pg(pg_id).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(0)));
    assert_eq!(pg.active_metadata_proof(), Some(proof));
}

fn heartbeat_authority_node<S: crate::control_plane::ControlPlaneStore>(
    authority: &mut crate::control_plane::SingleAuthorityControlPlane<S>,
    node_id: NodeId,
    now_ms: u64,
) {
    let record = authority.snapshot().node(node_id).unwrap();
    let heartbeat = crate::control_plane::NodeHeartbeat {
        node_id,
        node_incarnation: record.node_incarnation(),
        endpoint: record.endpoint().to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    authority.heartbeat(heartbeat, now_ms).unwrap();
}

fn heartbeat_authority_with_local_pg_proof<S: crate::control_plane::ControlPlaneStore>(
    authority: &mut crate::control_plane::SingleAuthorityControlPlane<S>,
    map: &Arc<LocalClusterMap>,
    node_id: NodeId,
    pg_id: PgId,
    now_ms: u64,
) {
    let state = map
        .node(node_id)
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let record = authority.snapshot().node(node_id).unwrap();
    let heartbeat = crate::control_plane::NodeHeartbeat {
        node_id,
        node_incarnation: record.node_incarnation(),
        endpoint: record.endpoint().to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Peering,
            metadata_proof: crate::control_plane::PgMetadataProof::current(
                state.applied_log_index,
                state.applied_log_hash,
                state.state_digest,
            ),
            pending_metadata_command: None,
        }],
    };
    authority.heartbeat(heartbeat, now_ms).unwrap();
}

#[test]
fn peering_reconstruction_gather_fails_closed_on_pending_metadata_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-pending-first-");
    let pending_bucket = bucket_for_pg(topology, 1, "peering-pending-next-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &first)
        .unwrap();
    let pending = create_bucket_metadata_command(pg_id, 2, pending_bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &pending);

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand {
                node_id
            }
        ) if node_id == NodeId::new(0)
    ));
}

#[test]
fn partial_tombstone_recording_retries_as_idempotent_abandon() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "partial-tombstone-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());

    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    cluster
        .record_abandoned_metadata_command_to_acting_set(&command)
        .unwrap();
    cluster
        .record_abandoned_metadata_command_to_acting_set(&command)
        .unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }
}

#[test]
fn partial_abandoned_create_bucket_retry_rebuilds_command_before_reporting_created() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "partial-create-tombstone-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let abandoned_index = map.test_next_metadata_command_log_index(pg_id).get();
    let command = create_bucket_metadata_command(pg_id, abandoned_index, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let outcome = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let crate::BucketCreateAttemptOutcome::Created(info) = outcome else {
        panic!("abandoned create retry must create a fresh bucket, got {outcome:?}");
    };
    assert_eq!(info.name(), &bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, abandoned_index + 1);
        let stored = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(stored.name, bucket);
    }
}

#[test]
fn fully_applied_primary_pending_reservation_drain_clears_exact_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("59".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            123,
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let conflict_injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_command = command.clone();
    let conflict_injected_hook = Arc::clone(&conflict_injected);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, candidate| {
            if node_id != NodeId::new(1) || *candidate != hook_command {
                return Ok(());
            }
            assert!(!conflict_injected_hook.swap(true, Ordering::SeqCst));
            for replica_node_id in node_ids {
                let pg = hook_map
                    .node(replica_node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(candidate.id().pg_id().get())?;
                pg.apply_metadata_command_and_record(replica_node_id.as_u32(), candidate)
                    .map_err(|error| match error {
                        crate::BucketSnapshotLoadError::Store(error) => error,
                        crate::BucketSnapshotLoadError::Metadata(error) => {
                            panic!("manual reservation apply failed: {error}")
                        }
                    })?;
            }
            Err(StoreError::MetadataCommandLogConflict {
                node_id: node_id.as_u32(),
                pg_id: candidate.id().pg_id().get(),
                cluster_epoch: candidate.id().cluster_epoch(),
                log_index: candidate.id().log_index().get(),
            })
        },
    ));

    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_recovery_gate(pg_id, &command)
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    drop(hook_guard);
    assert!(conflict_injected.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(map.test_next_metadata_command_log_index(pg_id).get(), 2);
}

#[test]
fn stale_normal_route_detaches_object_command_for_authorized_recovery() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("5a".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            123,
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let route_failure_injected = Arc::new(AtomicBool::new(false));
    let route_failure_injected_for_hook = Arc::clone(&route_failure_injected);
    let hook_command = command.clone();
    let hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, candidate| {
            if *candidate == hook_command
                && !route_failure_injected_for_hook.swap(true, Ordering::SeqCst)
            {
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: candidate.id().cluster_epoch(),
                    valid_until_ms: 1,
                    now_ms: 2,
                });
            }
            Ok(())
        },
    ));

    let error = cluster
        .drain_pending_metadata_command_with_recovery_gate(pg_id, &command)
        .expect_err("an expired normal route must hand the command to authorized recovery");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::MetadataCommandRecoveryTransferred
    ));
    assert!(route_failure_injected.load(Ordering::SeqCst));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &command));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command.clone())
    );

    drop(hook);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id, &command, &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn partial_abandoned_reservation_retry_does_not_report_skipped_command_success() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let reservation_id = crate::SessionId::try_from("58".repeat(16)).unwrap();
    let skipped_generation_id = crate::GenerationId::MIN;
    let command = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                skipped_generation_id,
                123,
            ),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert_eq!(generation_id, skipped_generation_id);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            generation_id
        );
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
    }
}

#[test]
fn abandoned_put_object_stream_create_releases_reserved_generation_on_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("59".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn stream_create_recovery_rejects_crossed_reservation_authority_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-recovery-crossed-");
    let key = key_for_object_pg(topology, &bucket, 2, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let session_id = crate::SessionId::try_from("cb".repeat(16)).unwrap();
    let pg_id = PgId::new(2);
    let command = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::UploadPart {
                        upload_id: crate::tests::multipart_upload_id("crossed-recovery-upload"),
                        part_number: 1,
                    },
                    encryption: crate::ObjectEncryption::None,
                },
                crate::clock::current_time_millis(),
                crate::metadata_command::BucketWriteReservationProof::from(&reservation.record),
            ),
        )),
    );
    let crossed_command = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            crate::metadata_command::MetadataCommandLogIndex::new(
                command.id().log_index().get() + 1,
            )
            .unwrap(),
        ),
        command.payload().clone(),
    );
    let crossed_error = cluster
        .test_apply_metadata_command_to_acting_set_for_recovery_under_leader(
            &command,
            &crossed_command,
            &cluster,
        )
        .expect_err("one command's recovery leader must not authorize another command");
    assert!(
        matches!(
            &crossed_error.source,
            crate::BucketSnapshotLoadError::Store(
                crate::StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-subject"
                }
            )
        ),
        "unexpected crossed recovery-subject error: {crossed_error:?}"
    );
    assert_eq!(crossed_error.applied_nodes, 0);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    let error = cluster
        .test_apply_metadata_command_to_acting_set_for_recovery(&command, &cluster)
        .expect_err("crossed stream-create recovery authority must fail");
    assert!(
        matches!(
            &error.source,
            crate::BucketSnapshotLoadError::Metadata(
                crate::MetadataError::BucketWriteReservationConflict { .. }
            )
        ),
        "unexpected crossed stream-create recovery error: {error:?}"
    );
    assert_eq!(error.applied_nodes, 0);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert!(reservations
        .iter()
        .any(|record| record.reservation_id == reservation.record.reservation_id));
}

#[test]
fn abandoned_put_object_stream_create_release_failure_retries_to_terminal_cleanup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("5a".repeat(16)).unwrap();
    let _generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&hook_bucket, &hook_key, &hook_session)
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected abandoned stream create release failure",
                        source: std::io::Error::other(
                            "injected abandoned stream create release failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .expect("pre-witness cleanup failure should retry the exact derivative");
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn abandoned_cleanup_derivative_handoff_retains_predecessor_and_root_outcome() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("5c".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let source = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::PutObject,
                    encryption: crate::ObjectEncryption::None,
                },
                123,
                proof,
            )),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &source);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &source)
        .unwrap();
    let speculative_derivative = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            source.id().cluster_epoch(),
            pg_id,
            MetadataCommandLogIndex::new(source.id().log_index().get() + 1).unwrap(),
        ),
        source
            .payload()
            .abandoned_recovery_follow_up()
            .expect("abandoned stream creation must require generation cleanup"),
    );
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .fail_next_pending_slot_replace_before_commit_with_inconclusive_inspection();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_for_hook = Arc::clone(&fail_once);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session = session_id.clone();
    let hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&hook_bucket, &hook_key, &hook_session)
            ) && fail_once_for_hook.swap(false, Ordering::SeqCst)
            {
                let id = command.id();
                return Err(StoreError::MetadataCommandOutcomeUnconfirmed {
                    pg_id: id.pg_id().get(),
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                });
            }
            Ok(())
        },
    ));

    let error = cluster
        .drain_pending_metadata_command_with_authorized_recovery_source(
            pg_id, &source, &source, &cluster,
        )
        .expect_err("pre-commit replacement uncertainty must detach for authorized recovery");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed {
            log_index,
            ..
        }) if log_index == speculative_derivative.id().log_index().get()
    ));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(source.clone()),
        "the ambiguous pre-commit attempt must leave durable C1 installed"
    );
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(
            pg_id,
            &source,
            &speculative_derivative,
        ));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &source));

    let error = cluster
        .drain_pending_metadata_command_with_authorized_recovery_source(
            pg_id, &source, &source, &cluster,
        )
        .expect_err("the exact speculative derivative retry must retain apply uncertainty");
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed { .. })
    ));
    assert!(!fail_once.load(Ordering::SeqCst));
    let derivative = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("cleanup derivative must remain durably pending");
    assert_eq!(derivative, speculative_derivative);
    assert!(matches!(
        derivative.payload(),
        MetadataCommandPayload::ReleaseObjectGeneration(release)
            if release.matches_request(&bucket, &key, &session_id)
    ));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_commands_share_flight(pg_id, &source, &derivative));
    assert!(map
        .runtime_state()
        .test_metadata_command_recovery_awaiting_authorized(pg_id, &derivative));
    drop(hook);

    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &derivative,
                &source,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Abandoned,
        "successful C2 cleanup must resolve the C1 request as abandoned"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn abandoned_stream_create_cleanup_derivative_reissues_and_converges() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("5b".repeat(16)).unwrap();
    let _generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::PutObject,
                    encryption: crate::ObjectEncryption::None,
                },
                123,
                proof,
            ),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
        .unwrap();

    let cleanup_without_predecessor = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            pg_id,
            MetadataCommandLogIndex::new(command.id().log_index().get() + 1).unwrap(),
        ),
        command
            .payload()
            .abandoned_recovery_follow_up()
            .expect("PUT stream creation has a certified reservation-release derivative"),
    );
    let omitted_predecessor_error = cluster
        .test_apply_recovery_derivative_without_predecessor(
            &command,
            &cleanup_without_predecessor,
            &cluster,
        )
        .expect_err("cleanup derivative proof must retain its abandoned source");
    assert!(
        matches!(
            omitted_predecessor_error.source,
            crate::BucketSnapshotLoadError::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata-command-recovery-predecessor-context"
            })
        ),
        "unexpected omitted predecessor error: {omitted_predecessor_error:?}"
    );
    assert_eq!(omitted_predecessor_error.applied_nodes, 0);

    let _serial = lock_metadata_command_apply_hook_test();
    let conflict_once = Arc::new(AtomicBool::new(true));
    let first_cleanup_log_index = Arc::new(AtomicU64::new(0));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session = session_id.clone();
    let conflict_once_hook = Arc::clone(&conflict_once);
    let first_cleanup_log_index_hook = Arc::clone(&first_cleanup_log_index);
    let hook_map = Arc::clone(&map);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, candidate| {
            match candidate.payload() {
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&hook_bucket, &hook_key, &hook_session)
                        && node_id == NodeId::new(0)
                        && conflict_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    first_cleanup_log_index_hook
                        .store(candidate.id().log_index().get(), Ordering::SeqCst);
                    for replica_node_id in node_ids {
                        hook_map
                            .node(replica_node_id)
                            .unwrap()
                            .storage_node()
                            .get_pg(candidate.id().pg_id().get())?
                            .record_metadata_command_abandoned(
                                replica_node_id.as_u32(),
                                candidate,
                            )?;
                    }
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: candidate.id().pg_id().get(),
                        cluster_epoch: candidate.id().cluster_epoch(),
                        log_index: candidate.id().log_index().get(),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    drop(hook_guard);
    assert!(!conflict_once.load(Ordering::SeqCst));
    let first_cleanup_log_index = first_cleanup_log_index.load(Ordering::SeqCst);
    assert_ne!(first_cleanup_log_index, 0);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert_eq!(
            pg.max_metadata_command_log_index(cluster.operation_epoch())
                .unwrap(),
            first_cleanup_log_index + 1
        );
    }
}

#[test]
fn zero_apply_generation_reservation_records_tombstone_and_later_reserves() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let before_index = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index;

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectGeneration(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply reservation failure",
                        source: std::io::Error::other("injected zero-apply reservation failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let reservation_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
    let err = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected zero-apply reservation failure",
                ..
            })
        ),
        "expected injected zero-apply reservation failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 1);
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }

    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert!(generation_id.get() >= crate::GenerationId::MIN.get());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 2);
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            generation_id
        );
    }
}

#[test]
fn zero_apply_direct_put_commit_records_tombstone_and_cleans_new_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("53".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_index = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index;
    let payload = b"direct put zero apply tombstone";
    let segment_okh = [93; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply direct put commit failure",
                        source: std::io::Error::other(
                            "injected zero-apply direct put commit failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected zero-apply direct put commit failure",
                ..
            })
        ),
        "expected injected zero-apply direct PUT commit failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 2);
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn abandoned_physically_mismatched_direct_put_fails_before_recovery() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);

    let abandoned_payload = b"abandoned direct put payload";
    let abandoned_okh = [94; 16];
    let abandoned_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &abandoned_okh,
            abandoned_payload,
        )
        .unwrap();
    let abandoned_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: abandoned_payload,
            segment_okh: abandoned_okh,
            written: &abandoned_written,
        },
    );
    let mut abandoned_req = abandoned_req;
    abandoned_req.bucket_write_reservation = bucket_write_proof.clone();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(object_pg),
            &object_pg_store,
            &abandoned_req,
            crate::VersionId::Null,
            bucket_write_proof.clone(),
        )
        .unwrap();
    drop(object_pg_store);
    let abandoned_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = abandoned_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(data_pg, &abandoned_shard_batch)
        .unwrap();

    insert_pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let current_payload = b"current direct put retry payload";
    let current_okh = [95; 16];
    let current_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &current_okh,
            current_payload,
        )
        .unwrap();
    let current_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: current_payload,
            segment_okh: current_okh,
            written: &current_written,
        },
    );
    let mut current_req = current_req;
    current_req.bucket_write_reservation = bucket_write_proof;
    let current_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = current_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(data_pg, &current_shard_batch)
        .unwrap();

    let data_primary = map.node(NodeId::new(2)).unwrap().storage_node();
    for written in abandoned_written
        .written_shards
        .iter()
        .chain(current_written.written_shards.iter())
    {
        assert!(data_primary
            .test_shard_exists(data_pg, &written.key)
            .unwrap());
    }

    let err = cluster
        .commit_direct_put_object_from_payload_shards(
            &current_req,
            &current_written.written_shards,
            |_| -> Result<(), ()> {
                panic!("matching abandoned pending direct PUT must not build a new command")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "pending direct PUT command payload differs from request",
            })
        ),
        "expected physical direct PUT mismatch conflict, got {err:?}"
    );

    let retained = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("physical mismatch must not recover even an abandoned command");
    assert_eq!(retained.id(), command.id());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            generation_id,
            "the pending command must retain its generation reservation"
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    for shard_index in 0..abandoned_written.ec.k + abandoned_written.ec.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                abandoned_written.data_pg_id,
                abandoned_written.ec,
                &abandoned_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
        assert!(!cluster
            .test_payload_shard_file_exists(
                current_written.data_pg_id,
                current_written.ec,
                &current_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    for written in &abandoned_written.written_shards {
        assert!(data_primary
            .test_shard_exists(data_pg, &written.key)
            .unwrap());
    }
    for written in &current_written.written_shards {
        assert!(!data_primary
            .test_shard_exists(data_pg, &written.key)
            .unwrap());
    }
}

#[test]
fn direct_put_commit_drains_unrelated_pending_command_before_publish() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pending_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "pending-tags-",
    );
    write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &pending_key,
        b"unrelated pending object",
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = pending_key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected unrelated metadata command apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let tags =
        "<Tagging><TagSet><Tag><Key>phase</Key><Value>pending</Value></Tag></TagSet></Tagging>";
    cluster
        .put_object_tags_if(&bucket, &pending_key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .expect("published metadata update must hand trailing convergence to recovery")
        .expect("object tag precondition should succeed");
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "unrelated object metadata command must remain pending"
    );

    let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put drains unrelated pending command";
    let segment_okh = [54; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;
    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        outcome.version_id.to_u64() + 1,
    );

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key).unwrap();
        assert_eq!(
            stored
                .as_live()
                .unwrap()
                .tags
                .as_ref()
                .map(crate::SerializedTagSet::as_str),
            Some(crate::tests::object_tags(tags).as_str())
        );
    }
}

#[test]
fn direct_put_maps_irrevocable_unrelated_partial_pending_conflict_to_contention() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pending_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "pending-partial-tags-",
    );
    write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &pending_key,
        b"unrelated partial pending object",
    );

    let reservation_id = crate::SessionId::try_from("55".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put drains unrelated partial exact conflict";
    let segment_okh = [55; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;

    let tags =
        "<Tagging><TagSet><Tag><Key>phase</Key><Value>partial</Value></Tag></TagSet></Tagging>";
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let live = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(pending_key.as_str()),
    );
    let pending_reservation_id = proof.reservation_id.clone();
    let pending_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(
            PutObjectMetadataCommand::from_live_object_and_mutation(
                live,
                PutObjectMetadataMutation::PutTags(crate::tests::object_tags(tags)),
                proof,
            ),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_command);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).as_ref(),
        Some(&pending_command),
        "unrelated object metadata command must start pending"
    );
    let witness_node_id = NodeId::new(0);
    map.node(witness_node_id)
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .apply_metadata_command_and_record(witness_node_id.as_u32(), &pending_command)
        .unwrap();

    let partial_conflict_attempts = Arc::new(AtomicUsize::new(0));
    let partial_conflict_attempts_for_hook = Arc::clone(&partial_conflict_attempts);
    let partial_conflict_hook = cluster.test_install_pending_object_metadata_partial_conflict_hook(
        Arc::new(move |command| {
            let matches_pending = matches!(
                command.payload(),
                MetadataCommandPayload::PutObjectMetadata(_)
            );
            if !matches_pending {
                return false;
            }
            let attempt = partial_conflict_attempts_for_hook.fetch_add(1, Ordering::SeqCst);
            attempt < 2
        }),
    );
    let error = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
            context: "request blocked by unrelated object metadata command convergence"
        })
    ));
    assert_eq!(partial_conflict_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).as_ref(),
        Some(&pending_command),
        "the irrevocable unrelated command must remain available to recovery"
    );
    let bucket_pg_id = PgId::new(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(&bucket),
    );
    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(
        reservations.len(),
        1,
        "the blocked direct PUT must release only its own reservation"
    );
    assert_eq!(
        reservations[0].reservation_id, pending_reservation_id,
        "the unrelated pending command must retain its exact reservation"
    );
    assert_eq!(
        reservations[0].target_context.as_deref(),
        Some(pending_key.as_str())
    );
    drop(bucket_pg);
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(object_pg).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
    drop(pg);
    drop(partial_conflict_hook);

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            stored.tags.as_ref().map(crate::SerializedTagSet::as_str),
            Some(crate::tests::object_tags(tags).as_str())
        );
    }
}

#[test]
fn direct_put_commit_retries_partial_exact_command_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put exact partial retry";
    let segment_okh = [56; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual direct put command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(applied_by_hook.load(Ordering::SeqCst));
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_retry_converges_pending_then_allocates_fresh_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectVersion(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected reserve object version apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let first_reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .expect("published version reservation must hand trailing convergence to recovery");
    assert_eq!(first_reserved, crate::VersionId::from_u64(1));
    drop(hook_guard);

    let pending = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("partial version reservation must remain pending");
    let MetadataCommandPayload::ReserveObjectVersion(reservation) = pending.payload() else {
        panic!("expected pending ReserveObjectVersion, got {pending:?}");
    };
    assert_eq!(reservation.version_id, crate::VersionId::from_u64(1));
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(1)],
        object_pg,
        &bucket,
        &key,
        2,
    );
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(0)],
        object_pg,
        &bucket,
        &key,
        2,
    );
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(2)],
        object_pg,
        &bucket,
        &key,
        0,
    );

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
}

#[test]
fn reserve_object_version_abandons_stale_pending_reservation_and_retries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let first = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first)
        .unwrap();
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);

    let stale_pending = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale_pending);

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_retries_pending_allocator_recovery_contention() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);
    let pending = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending);

    let _serial = lock_metadata_command_apply_hook_test();
    let apply_attempts = Arc::new(AtomicUsize::new(0));
    let apply_attempts_for_hook = Arc::clone(&apply_attempts);
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |_command| {
            if apply_attempts_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected pre-witness pending allocator recovery race",
                });
            }
            Ok(())
        }));

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .expect("a partial allocator conflict should retry from the current pending slot");
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(apply_attempts.load(Ordering::SeqCst) >= 3);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_retries_fresh_allocator_apply_contention() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);
    let _serial = lock_metadata_command_apply_hook_test();
    let apply_attempts = Arc::new(AtomicUsize::new(0));
    let apply_attempts_for_hook = Arc::clone(&apply_attempts);
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |_command| {
            if apply_attempts_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected pre-witness fresh allocator apply race",
                });
            }
            Ok(())
        }));

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .expect("fresh allocator apply contention should converge from the pending slot");
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(apply_attempts.load(Ordering::SeqCst) >= 3);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_clears_fully_applied_pending_then_allocates_fresh_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "test setup must leave the fully applied command pending"
    );
    let version = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(version, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_partial_apply_after_reopen_converges() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(0));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectVersion(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lost reserve object version replica apply failure",
                        source: std::io::Error::other(
                            "injected lost reserve object version replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(published, crate::VersionId::from_u64(1));
    drop(hook_guard);

    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(0), NodeId::new(1)],
        object_pg,
        &bucket,
        &key,
        2,
    );
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(2)],
        object_pg,
        &bucket,
        &key,
        0,
    );

    drop(cluster);
    drop(map);

    let mut reopened_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    set_route_primary(&mut reopened_map, object_pg, NodeId::new(0));
    let reopened_map = Arc::new(reopened_map);
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened_map)).unwrap();
    assert!(
        pending_metadata_command_for_test(&reopened_map, pg_id, &bucket).is_none(),
        "open-time recovery should converge and clear the partial version reservation slot"
    );
    assert_object_version_counter_on_acting_nodes(
        &reopened_map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        2,
    );
    let reserved = reopened_cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&reopened_map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(
        &reopened_map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        3,
    );
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
}

#[test]
fn direct_put_action_failure_does_not_reserve_object_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let reservation_id = crate::SessionId::try_from("75".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"conditional direct put should not reserve a version";
    let segment_okh = [75; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Err::<(), _>("conditional write rejected")
        })
        .unwrap()
        .unwrap_err();
    assert_eq!(err, "conditional write rejected");
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 0);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn local_cluster_reopen_rejects_future_epoch_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pg_id = PgId::new(1);
    let bucket = bucket_for_pg(topology, 1, "orphan-reopen-");
    let primary_node_id = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap()
        .node_id();

    // Inject a pending slot at a future epoch on the primary. This can be an
    // in-flight first command of a newer epoch if the primary crashes before
    // recording locally, so local open-time recovery must not classify it as a
    // cleanable orphan without acting-set evidence.
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let command = create_bucket_metadata_command_with_epoch(pg_id, 1, bucket.clone(), future_epoch);
    {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        pg.try_insert_pending_metadata_command_slot(
            primary_node_id.as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
        assert!(pg
            .pending_metadata_command_slot_any_epoch(primary_node_id.as_u32())
            .unwrap()
            .is_some());
    }
    drop(map);

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err.open_local_node_store_error(),
            Some((
                node_id,
                StoreError::MetadataCommandLogConflict {
                    pg_id: 1,
                    cluster_epoch,
                    ..
                }
            )) if node_id == primary_node_id.as_u32() && *cluster_epoch == future_epoch
        ),
        "future-epoch pending slot should fail closed, got {err:?}"
    );
}
