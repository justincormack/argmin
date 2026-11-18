// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real control-plane reports and admission-pinned embedded runtimes. No
//! reconstructed freshness proof or caller-supplied historical authority.

use super::*;
use crate::control_plane::{
    ClusterRuntimeMapSnapshot, FileControlPlaneStore, NodeHeartbeat, NodePgHeartbeatObservation,
    PendingMetadataCommandObservation, PgMetadataProof, SingleAuthorityControlPlane,
};
use crate::node_runtime::pg_store::PublicationObservation;
use crate::{
    LocalClusterMap, LocalNodeStoreConfig, PgClusterMapHistoryRouteReferences, StorageCluster,
    StorageClusterRouteHandle,
};
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

#[path = "payload_staging_complete_application_embedded_historical_tests.rs"]
mod application;
#[path = "payload_staging_complete_fanout_historical_tests.rs"]
mod fanout_historical;
#[path = "payload_staging_complete_fanout_historical_remote_tests.rs"]
mod fanout_remote;
#[path = "payload_staging_complete_application_historical_bridge_tests.rs"]
mod remote_application;

#[path = "payload_staging_unstarted_abandonment_historical_tests.rs"]
mod unstarted_abandonment;

struct Fixture {
    authority: SingleAuthorityControlPlane<FileControlPlaneStore>,
    heartbeat_sequence: u64,
    map: Arc<LocalClusterMap>,
    runtime_map: ClusterRuntimeMapSnapshot,
    handle: StorageClusterRouteHandle,
    context: ContextualCommand,
}

#[test]
fn staging_window_confirmation_rejects_historical_recovery_section() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let dir = test_util::tempdir();
    let fixture = Fixture::new(&dir, 1);
    let id = fixture.context.id();
    let admission = fixture.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
    let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
    let error = fixture
        .context
        .inspect_window_confirmation_embedded(&section, deadline(), |_| {
            panic!("historical route must not inspect a witness")
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("original admitted primary section"),
        "{error}"
    );
    assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
}

fn proof(pg: &PgStore) -> PgMetadataProof {
    let state = pg.metadata_command_replica_state().unwrap();
    PgMetadataProof {
        applied_log_index: state.applied_log_index,
        applied_log_hash: state.applied_log_hash,
        state_digest: state.state_digest,
    }
}

fn heartbeat(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    sequence: &mut u64,
    node_id: NodeId,
    pg_id: PgId,
    proof: PgMetadataProof,
    pending: Option<PendingMetadataCommandObservation>,
) {
    heartbeat_with_bucket(authority, sequence, node_id, pg_id, proof, pending, None);
}

fn heartbeat_with_bucket(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    sequence: &mut u64,
    node_id: NodeId,
    pg_id: PgId,
    proof: PgMetadataProof,
    pending: Option<PendingMetadataCommandObservation>,
    bucket_pg: Option<PgId>,
) {
    *sequence += 1;
    let snapshot = authority.snapshot();
    let node = snapshot.node(node_id).unwrap();
    let mut heartbeat = NodeHeartbeat::test_fixture(
        node.node_id(),
        node.node_incarnation().max(1),
        node.endpoint().to_owned(),
        snapshot.cluster_epoch(),
        10_000,
        PgClusterMapHistoryRouteReferences::try_from_iter([]).unwrap(),
        vec![NodePgHeartbeatObservation {
            pg_id,
            state: snapshot.pg(pg_id).unwrap().state(),
            metadata_proof: proof,
            metadata_log_epoch: pending.map_or(snapshot.cluster_epoch(), |p| p.cluster_epoch()),
            pending_metadata_command: pending,
        }],
    );
    if let Some(bucket_pg) = bucket_pg {
        heartbeat.pg_observations.push(NodePgHeartbeatObservation {
            pg_id: bucket_pg,
            state: snapshot.pg(bucket_pg).unwrap().state(),
            metadata_proof: PgMetadataProof::empty(),
            metadata_log_epoch: snapshot.cluster_epoch(),
            pending_metadata_command: None,
        });
    }
    heartbeat.cluster_map_history_route_scan_generation = NonZeroU64::new(*sequence).unwrap();
    authority
        .heartbeat(heartbeat, crate::clock::current_time_millis())
        .unwrap();
}

impl Fixture {
    fn report(
        &mut self,
        pending: Option<PendingMetadataCommandObservation>,
    ) -> ClusterRuntimeMapSnapshot {
        let pg_id = self.context.id().pg_id();
        let node = self.map.node(NodeId::new(4)).unwrap().test_node();
        heartbeat(
            &mut self.authority,
            &mut self.heartbeat_sequence,
            NodeId::new(4),
            pg_id,
            proof(&node.get_pg(pg_id.get()).unwrap()),
            pending,
        );
        self.authority
            .snapshot()
            .runtime_map(crate::clock::current_time_millis())
            .unwrap()
    }

    fn handle_for(&self, runtime_map: &ClusterRuntimeMapSnapshot) -> StorageClusterRouteHandle {
        let map =
            LocalClusterMap::open_runtime_map_with_existing_local_nodes(&self.map, runtime_map)
                .unwrap();
        StorageClusterRouteHandle::from_authorized_cluster(
            StorageCluster::from_runtime_local_map(Arc::new(map), runtime_map).unwrap(),
        )
    }

    fn new(dir: &test_util::TempDir, case: usize) -> Self {
        Self::with_members(dir, case, &[4], None)
    }

    fn with_members(
        dir: &test_util::TempDir,
        case: usize,
        members: &[u32],
        peering_members: Option<(&[u32], bool)>,
    ) -> Self {
        Self::with_members_and_release_route(dir, case, members, peering_members, false)
    }

    fn with_members_and_release_route(
        dir: &test_util::TempDir,
        case: usize,
        members: &[u32],
        peering_members: Option<(&[u32], bool)>,
        release_route: bool,
    ) -> Self {
        Self::with_context_builder(dir, case, members, peering_members, release_route, |pg| {
            context(pg, case)
        })
    }

    fn with_context_builder(
        dir: &test_util::TempDir,
        case: usize,
        members: &[u32],
        peering_members: Option<(&[u32], bool)>,
        release_route: bool,
        build: impl FnOnce(&PgStore) -> ContextualCommand,
    ) -> Self {
        let source_epoch = config(case).cluster_epoch;
        let mut config = config(case);
        // Bootstrap, first heartbeats and Active publication advance the real
        // control-plane epoch. Larger cohorts need a later command position.
        config.cluster_epoch =
            ClusterEpoch::new(source_epoch.get().max(members.len() as u64 + 3)).unwrap();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let bucket_pg =
            PgId::new(topology().bucket_pg_for(&fixture(StagingTarget::DirectPut).subject.bucket));
        assert_ne!(bucket_pg, pg_id);
        let mut authority =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(dir.path().join("cp")))
                .unwrap();
        // Each newly heartbeating member advances the control-plane epoch. A
        // multi-member bootstrap already has the desired cohort, avoiding an
        // unnecessary placement transition before the fixed-epoch command.
        let bootstrap_members = if members.len() == 1 {
            vec![4, 2, 0, 3, 1]
        } else {
            members.to_vec()
        };
        authority
            .bootstrap_initial_cluster_map(
                bootstrap_members
                    .iter()
                    .map(|&id| (NodeId::new(id), format!("/embedded-{id}.sock")))
                    .collect(),
                config.pg_ids.iter().copied().map(PgId::new).collect(),
            )
            .unwrap();
        let mut sequence = 0;
        for &id in members {
            heartbeat_with_bucket(
                &mut authority,
                &mut sequence,
                NodeId::new(id),
                pg_id,
                PgMetadataProof::empty(),
                None,
                release_route.then_some(bucket_pg),
            );
        }
        if bootstrap_members != members {
            authority
                .set_pg_acting_set(pg_id, members.iter().copied().map(NodeId::new).collect())
                .unwrap();
        }
        // Reach the command epoch through real control-plane transitions, not
        // mutated route DTOs. Reissue the shared source fixture below if needed.
        let unrelated = PgId::new((pg_id.get() + 1) % 8);
        while authority.snapshot().cluster_epoch().get() + 1 < config.cluster_epoch.get() {
            let state = match authority.snapshot().pg(unrelated).unwrap().state() {
                PgState::Inconsistent => PgState::Peering,
                _ => PgState::Inconsistent,
            };
            authority.set_pg_state(unrelated, state).unwrap();
        }
        for &id in members {
            heartbeat_with_bucket(
                &mut authority,
                &mut sequence,
                NodeId::new(id),
                pg_id,
                PgMetadataProof::empty(),
                None,
                release_route.then_some(bucket_pg),
            );
        }
        let completed = authority
            .complete_ready_pg_peerings(crate::clock::current_time_millis())
            .unwrap();
        let mut expected = vec![pg_id];
        if release_route {
            expected.push(bucket_pg);
        }
        expected.sort();
        assert_eq!(completed, expected);
        assert_eq!(authority.snapshot().cluster_epoch(), config.cluster_epoch);
        // Extra nodes are genuine registered nonmembers. Add them only after
        // retaining the command's Active epoch; their later registration does
        // not change that historical cohort.
        for id in (0..5).filter(|id| !bootstrap_members.contains(id)) {
            let node_id = NodeId::new(id);
            authority
                .set_node_membership(node_id, crate::control_plane::NodeMembershipState::Active)
                .unwrap();
            authority
                .heartbeat(
                    NodeHeartbeat::test_fixture(
                        node_id,
                        1,
                        format!("/embedded-{id}.sock"),
                        authority.snapshot().cluster_epoch(),
                        10_000,
                        PgClusterMapHistoryRouteReferences::try_from_iter([]).unwrap(),
                        vec![],
                    ),
                    crate::clock::current_time_millis(),
                )
                .unwrap();
        }
        for &id in members {
            heartbeat_with_bucket(
                &mut authority,
                &mut sequence,
                NodeId::new(id),
                pg_id,
                PgMetadataProof::empty(),
                None,
                release_route.then_some(bucket_pg),
            );
        }
        let active = authority
            .snapshot()
            .runtime_map(crate::clock::current_time_millis())
            .unwrap();
        assert_eq!(
            active
                .pg_routes()
                .iter()
                .find(|r| r.pg_id() == pg_id)
                .unwrap()
                .state(),
            PgState::Active
        );
        // Establish the requested placement history while the source proof is
        // still empty. Adding a peer after reporting the pending command would
        // correctly require metadata transfer. These owner-local fixtures then
        // populate the historical command/prefix before reporting it.
        if let Some((peering_members, retain_previous_peering)) = peering_members {
            if retain_previous_peering {
                authority.set_pg_state(pg_id, PgState::Peering).unwrap();
                heartbeat(
                    &mut authority,
                    &mut sequence,
                    NodeId::new(4),
                    pg_id,
                    PgMetadataProof::empty(),
                    None,
                );
            }
            authority
                .set_pg_acting_set(
                    pg_id,
                    peering_members.iter().copied().map(NodeId::new).collect(),
                )
                .unwrap();
        }
        // Create coherent metadata-only replica images before any runtime opens
        // them; do not introduce overlapping runtimes or independent random IDs.
        let seed_dir = test_util::tempdir();
        let seed = PgStore::open(seed_dir.path(), pg_id.get()).unwrap();
        if case == 1 {
            let digest = seed.metadata_command_replica_state().unwrap().state_digest;
            seed.initialize_metadata_transfer_empty_state(4, source_epoch, digest)
                .unwrap();
        }
        let mut context = build(&seed);
        if context.id().cluster_epoch() < config.cluster_epoch {
            let id = MetadataCommandId::new(
                config.cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(context.id().log_index().get().max(2)).unwrap(),
            );
            context = context.into_reissue(id).unwrap();
            complete_application::anchor(&seed, id);
        }
        for id in 0..5 {
            let pg_dir = dir
                .path()
                .join(id.to_string())
                .join(format!("pg-{:04}", pg_id.get()));
            std::fs::create_dir_all(&pg_dir).unwrap();
            let db = pg_dir.join("metadata.db");
            seed.conn
                .execute("VACUUM INTO ?1", [db.to_str().unwrap()])
                .unwrap();
            std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        drop(seed);
        let map = LocalClusterMap::open_frontend_with_configs_and_runtime_map(
            NodeId::new(4),
            [0, 1, 2, 3, 4].map(|id| {
                LocalNodeStoreConfig::new(NodeId::new(id), dir.path().join(id.to_string()))
            }),
            config.default_ec_shape,
            &active,
        )
        .unwrap();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        assert_eq!(
            node.get_pg(pg_id.get())
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .cluster_epoch,
            context.id().cluster_epoch(),
            "the reported pending epoch must have a durable prefix"
        );
        let proof = proof(&node.get_pg(pg_id.get()).unwrap());
        let pending = PendingMetadataCommandObservation::new(
            context.id().cluster_epoch(),
            NonZeroU64::new(context.id().log_index().get()).unwrap(),
            context.encode_command().unwrap().checksum_crc64(),
            context.encode_command().unwrap().command_sha256(),
        );
        heartbeat_with_bucket(
            &mut authority,
            &mut sequence,
            NodeId::new(4),
            pg_id,
            proof,
            Some(pending),
            release_route.then_some(bucket_pg),
        );
        if peering_members.is_none() {
            authority.set_pg_state(pg_id, PgState::Peering).unwrap();
        }
        if release_route {
            // The reservation proof belongs to the old Active bucket route. Its
            // replacement has a different primary and must not receive release.
            // Refresh the surviving peers after the object-PG epoch transition;
            // real migration admission requires current-epoch source evidence.
            let replacement: Vec<_> = members.iter().copied().filter(|&id| id != 4).collect();
            for &id in &replacement {
                heartbeat(
                    &mut authority,
                    &mut sequence,
                    NodeId::new(id),
                    bucket_pg,
                    PgMetadataProof::empty(),
                    None,
                );
            }
            authority
                .set_pg_acting_set(
                    bucket_pg,
                    replacement.into_iter().map(NodeId::new).collect(),
                )
                .unwrap();
        }
        // Node 4 no longer belongs to the replacement bucket acting set.
        heartbeat(
            &mut authority,
            &mut sequence,
            NodeId::new(4),
            pg_id,
            proof,
            Some(pending),
        );
        if release_route {
            // The pending index and retained resources, not merely the
            // object-PG recovery report, pin the exact bucket release routes.
            // Exercise production discovery and the real refresh projection.
            let references = node.cluster_map_history_route_references().unwrap();
            assert!(references.iter().any(|reference| {
                reference.pg_id() == bucket_pg
                    && reference.kind()
                        == crate::PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource
            }));
            let snapshot = authority.snapshot();
            let registered = snapshot.node(NodeId::new(4)).unwrap();
            let mut report = NodeHeartbeat::test_fixture(
                registered.node_id(),
                registered.node_incarnation(),
                registered.endpoint().to_owned(),
                snapshot.cluster_epoch(),
                10_000,
                references,
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: snapshot.pg(pg_id).unwrap().state(),
                    metadata_proof: proof,
                    metadata_log_epoch: pending.cluster_epoch(),
                    pending_metadata_command: Some(pending),
                }],
            );
            sequence += 1;
            report.cluster_map_history_route_scan_generation = NonZeroU64::new(sequence).unwrap();
            authority
                .heartbeat(report, crate::clock::current_time_millis())
                .unwrap();
        }
        let runtime_map = authority
            .snapshot()
            .runtime_map(crate::clock::current_time_millis())
            .unwrap();
        let map = Arc::new(
            LocalClusterMap::open_runtime_map_with_existing_local_nodes(&map, &runtime_map)
                .unwrap(),
        );
        let cluster =
            StorageCluster::from_runtime_local_map(Arc::clone(&map), &runtime_map).unwrap();
        Self {
            authority,
            heartbeat_sequence: sequence,
            map,
            runtime_map,
            handle: StorageClusterRouteHandle::from_authorized_cluster(cluster),
            context,
        }
    }
}

#[test]
fn staging_complete_publication_embedded_historical_binds_report_content_and_exact_command() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for field in ["index", "checksum", "sha256"] {
        let dir = test_util::tempdir();
        let mut fixture = Fixture::new(&dir, 0);
        let id = fixture.context.id();
        let checksum = fixture.context.encode_command().unwrap().checksum_crc64();
        let crossed_report = PendingMetadataCommandObservation::new(
            id.cluster_epoch(),
            NonZeroU64::new(id.log_index().get() + u64::from(field == "index")).unwrap(),
            checksum ^ u64::from(field == "checksum"),
            {
                let mut sha = fixture.context.encode_command().unwrap().command_sha256();
                sha[0] ^= u8::from(field == "sha256");
                sha
            },
        );
        let crossed_map = fixture.report(Some(crossed_report));
        assert_eq!(
            crossed_map.cluster_epoch(),
            fixture.runtime_map.cluster_epoch()
        );
        assert_ne!(
            crossed_map.content_digest(),
            fixture.runtime_map.content_digest()
        );
        let admission = fixture.handle.admit_current_route().unwrap();
        assert!(matches!(
            admission.embedded_historical_publication_route(&crossed_map, id.pg_id()),
            Err(StoreError::RouteCapabilitySubjectMismatch { .. })
        ));

        // A correctly admitted *different* report must still reject this exact
        // request. Position and checksum are independently constrained.
        let crossed_handle = fixture.handle_for(&crossed_map);
        let crossed_admission = crossed_handle.admit_current_route().unwrap();
        let route = crossed_admission
            .embedded_historical_publication_route(&crossed_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
        let evidence = pending(&fixture.context);
        let request = evidence
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        let error = section
            .start_complete_publication(&request, deadline())
            .unwrap_err()
            .into_store_error()
            .unwrap();
        assert!(
            matches!(
                error,
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "embedded publication command differs from admitted subject"
                }
            ),
            "{error:?}"
        );
        assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        assert_eq!(
            node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
            writes
        );
        drop(section);
        // Positive canary using the original admission and exact report.
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        route
            .lock_until(deadline())
            .unwrap()
            .start_complete_publication(&request, deadline())
            .unwrap();
    }
}

#[test]
fn staging_complete_publication_embedded_historical_rejects_static_missing_and_crossed_pg_authority(
) {
    let _clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let mut fixture = Fixture::new(&dir, 0);
    let pg_id = fixture.context.id().pg_id();
    let absent = fixture.report(None);
    let absent_handle = fixture.handle_for(&absent);
    let absent_admission = absent_handle.admit_current_route().unwrap();
    assert!(matches!(
        absent_admission.embedded_historical_publication_route(&absent, pg_id),
        Err(StoreError::RouteCapabilitySubjectMismatch { .. })
    ));
    let admission = fixture.handle.admit_current_route().unwrap();
    assert!(matches!(
        admission.embedded_historical_publication_route(
            &fixture.runtime_map,
            PgId::new((pg_id.get() + 1) % 8)
        ),
        Err(StoreError::RouteCapabilitySubjectMismatch { .. })
    ));
    let static_map = Arc::new(
        fixture
            .map
            .test_clone_with_route_map_validity(RouteMapValidity::Forever),
    );
    let static_cluster = StorageCluster::test_from_local_map_with_epoch(
        static_map,
        fixture.runtime_map.cluster_epoch(),
    )
    .unwrap();
    let static_handle = StorageClusterRouteHandle::from_authorized_cluster(static_cluster);
    let static_admission = static_handle.admit_current_route().unwrap();
    assert!(matches!(
        static_admission.embedded_historical_publication_route(&fixture.runtime_map, pg_id),
        Err(StoreError::RouteCapabilitySubjectMismatch { .. })
    ));
    let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
    assert!(!started(&node.get_pg(pg_id.get()).unwrap()));
}

#[test]
fn staging_complete_publication_embedded_historical_report_does_not_replace_durable_identity() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let mut fixture = Fixture::new(&dir, 0);
    let id = fixture.context.id();
    let owner = super::super::fixture(StagingTarget::DirectPut);
    let command = crate::metadata_command::MetadataCommandEnvelope::new(
        id,
        crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
            crate::metadata_command::ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                crate::VersionId::from_u64(99),
            ),
        ),
    );
    let crossed = ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap();
    let encoded = crossed.encode_command().unwrap();
    assert_ne!(
        encoded.checksum_crc64(),
        fixture.context.encode_command().unwrap().checksum_crc64()
    );
    let report = fixture.report(Some(PendingMetadataCommandObservation::new(
        id.cluster_epoch(),
        NonZeroU64::new(id.log_index().get()).unwrap(),
        encoded.checksum_crc64(),
        encoded.command_sha256(),
    )));
    let handle = fixture.handle_for(&report);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_historical_publication_route(&report, id.pg_id())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let evidence = pending(&crossed);
    let request = evidence
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    section
        .require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )
        .unwrap();
    let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
    let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
    let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
    assert!(section
        .start_complete_publication(&request, deadline())
        .is_err());
    assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
    assert_eq!(
        node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
        writes
    );
}

#[test]
fn staging_complete_publication_embedded_historical_captured_fence_survives_raw_renewal() {
    for failure_stage in [
        "before-trial",
        "after-trial",
        "before-marker",
        "before-commit",
    ] {
        let clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let fixture = Fixture::new(&dir, 1);
        let id = fixture.context.id();
        let admission = fixture.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let shared_lock = fixture
            .map
            .runtime_state()
            .metadata_command_pg_lock(id.pg_id());
        let guard = shared_lock.lock();
        assert!(matches!(
            route.lock_until(Instant::now() + Duration::from_millis(5)),
            Err(StoreError::OperationDeadlineExceeded { .. })
        ));
        drop(guard);
        let section = route.lock_until(deadline()).unwrap();
        assert!(shared_lock.try_lock().is_none());
        let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let evidence = pending(&fixture.context);
        let request = evidence
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        let mut reached = false;
        let error = section
            .test_with_publication_authority(&request, deadline(), |pg, authority| {
                request.test_start_embedded_with_hook(pg, authority, |stage| {
                    if stage == failure_stage {
                        reached = true;
                        fixture.map.test_store_route_map_validity(
                            RouteMapValidity::until_ms(40_000).unwrap(),
                        );
                        clock.set(21_000);
                        fixture.map.require_route_map_valid_now().unwrap();
                        admission.require_valid_now().unwrap();
                    }
                    Ok(())
                })
            })
            .unwrap_err()
            .into_store_error()
            .unwrap();
        assert!(reached, "{failure_stage}");
        assert!(
            matches!(error, StoreError::RouteMapExpired { .. }),
            "{failure_stage}: {error:?}"
        );
        assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        assert!(section
            .start_complete_publication(&request, deadline())
            .is_err());
        drop(section);
        assert!(shared_lock.try_lock().is_some());
    }
}

#[test]
fn staging_complete_publication_embedded_historical_exact_retry_and_apply() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        let dir = test_util::tempdir();
        let fixture = Fixture::new(&dir, case);
        let id = fixture.context.id();
        let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
        let admission = fixture.handle.admit_current_route().unwrap();
        assert!(admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .is_err());
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        assert_eq!(section.epoch(), id.cluster_epoch());
        assert!(section.epoch() < admission.cluster_epoch());
        let mut sender =
            pending(&fixture.context).into_publication_sender(NodeId::new(4), deadline());
        let before = without_marker(&node.get_pg(id.pg_id().get()).unwrap());
        let error = sender
            .test_send_embedded_with_hook(&section, deadline(), |stage| {
                if stage == "after-commit" {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "lost embedded historical publication confirmation",
                    });
                }
                Ok(())
            })
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("lost embedded historical publication confirmation"));
        assert_eq!(sender.observation(), PublicationObservation::Unconfirmed);
        assert!(started(&node.get_pg(id.pg_id().get()).unwrap()));
        assert_eq!(
            without_marker(&node.get_pg(id.pg_id().get()).unwrap()),
            before
        );
        sender.send_embedded(&section, deadline()).unwrap();
        assert_eq!(
            sender.observation(),
            PublicationObservation::MarkerConfirmed
        );
        apply(&node.get_pg(id.pg_id().get()).unwrap(), &fixture.context);
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
        sender.send_embedded(&section, deadline()).unwrap();
        assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        assert_eq!(
            node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
            writes
        );
    }
}
