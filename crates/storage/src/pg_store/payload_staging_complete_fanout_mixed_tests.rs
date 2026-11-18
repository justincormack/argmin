// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Genuine mixed execution endpoints under one embedded primary admission.
//! Opposite-direction coverage lives in the remote-primary module.
//! Metadata-only fixtures: no physical cleanup or live writer activation.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::engine::SharedStorageNode;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, CompleteFanoutSender,
    TerminalSlotRetirementObservation as Retired,
};
use crate::storage_node_server::StorageNodeServer;
use crate::{LocalClusterMap, StorageCluster, StorageClusterRouteHandle};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(super) struct Remote {
    pub(super) node: NodeId,
    pub(super) tls: bool,
    pub(super) server: Arc<StorageNodeServer>,
    pub(super) client: Arc<UnixStorageNodeClient>,
    _dir: test_util::TempDir,
}

impl Remote {
    pub(super) fn run(&self) -> application_sender::RunningReplica {
        application_sender::RunningReplica::start(Arc::clone(&self.server), self.tls)
    }

    fn count_applications(&self) -> Arc<AtomicUsize> {
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        self.server
            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                if kind == crate::storage_rpc::StorageRpcMessageKind::MetadataCommandApplyAndRecord
                {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            }));
        count
    }
}

pub(super) struct Mixed {
    pub(super) map: Arc<LocalClusterMap>,
    pub(super) handle: StorageClusterRouteHandle,
    pub(super) context: ContextualCommand,
    pub(super) remote: Vec<Remote>,
}

impl Mixed {
    fn setup(dir: &test_util::TempDir, case: usize, witness_tls: Option<bool>) -> Self {
        let endpoints = match witness_tls {
            Some(tls) => [(0, tls), (2, !tls)],
            None => [(2, false), (3, true)],
        };
        Self::with_endpoints(dir, case, endpoints)
    }

    pub(super) fn with_endpoints(
        dir: &test_util::TempDir,
        case: usize,
        endpoints: [(u32, bool); 2],
    ) -> Self {
        let (map, handle, context) =
            application_embedded::setup(dir, case, RouteMapValidity::Forever);
        Self::from_fixture(map, handle, context, case, endpoints)
    }

    pub(super) fn from_fixture(
        map: Arc<LocalClusterMap>,
        handle: StorageClusterRouteHandle,
        context: ContextualCommand,
        case: usize,
        endpoints: [(u32, bool); 2],
    ) -> Self {
        Self::from_fixture_with_config(map, handle, context, &config(case), endpoints)
    }

    pub(super) fn from_fixture_with_config(
        map: Arc<LocalClusterMap>,
        handle: StorageClusterRouteHandle,
        context: ContextualCommand,
        config: &crate::storage_node_server::StorageNodeProcessConfig,
        endpoints: [(u32, bool); 2],
    ) -> Self {
        Self::from_fixture_state(map, handle, context, config, endpoints, true)
    }

    pub(super) fn from_settled_fixture<const N: usize>(
        map: Arc<LocalClusterMap>,
        handle: StorageClusterRouteHandle,
        context: ContextualCommand,
        endpoints: [(u32, bool); N],
    ) -> Self {
        Self::from_fixture_state(map, handle, context, &config(1), endpoints, false)
    }

    fn from_fixture_state<const N: usize>(
        map: Arc<LocalClusterMap>,
        handle: StorageClusterRouteHandle,
        context: ContextualCommand,
        config: &crate::storage_node_server::StorageNodeProcessConfig,
        endpoints: [(u32, bool); N],
        install_primary: bool,
    ) -> Self {
        // Change fixture endpoints before minting the final route authority.
        // Retain the already-open embedded runtimes, never reopen their files.
        drop(handle);
        let mut map = Arc::try_unwrap(map).unwrap_or_else(|_| panic!("fixture map still shared"));
        let mut remote = Vec::new();
        for (id, tls) in endpoints {
            let remote_dir = test_util::tempdir();
            let (server, client) = application_sender::replica_from_pg(
                tls,
                &map.node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(context.id().pg_id().get())
                    .unwrap(),
                config,
                &remote_dir,
                NodeId::new(id),
            );
            let client = Arc::new(client);
            if id == 4 && install_primary {
                install(
                    &server
                        .test_storage_node()
                        .get_pg(context.id().pg_id().get())
                        .unwrap(),
                    &context,
                );
            }
            map.test_register_complete_fanout_client(Arc::clone(&client));
            remote.push(Remote {
                node: NodeId::new(id),
                tls,
                server: Arc::new(server),
                client,
                _dir: remote_dir,
            });
        }
        let map = Arc::new(map);
        let cluster = StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            context.id().cluster_epoch(),
        )
        .unwrap();
        let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
        Self {
            map,
            handle,
            context,
            remote,
        }
    }

    pub(super) fn remote(&self, node: NodeId) -> Option<&Remote> {
        self.remote.iter().find(|peer| peer.node == node)
    }

    pub(super) fn node(&self, node: NodeId) -> Arc<SharedStorageNode> {
        self.remote(node).map_or_else(
            || Arc::clone(self.map.node(node).unwrap().test_node()),
            |peer| peer.server.test_storage_node(),
        )
    }

    pub(super) fn apply(
        &self,
        fanout: &mut CompleteFanoutSender<'_>,
        section: &EmbeddedPublicationSection<'_>,
        node: u32,
    ) -> Result<(), String> {
        let node = NodeId::new(node);
        if let Some(peer) = self.remote(node) {
            fanout
                .apply_replica(&peer.client, deadline())
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            fanout
                .apply_embedded(section, node, deadline())
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }

    pub(super) fn observe(&self) -> Vec<(super::super::complete_node_startup::Snapshot, u64)> {
        (0..5)
            .map(|id| {
                let node = self.node(NodeId::new(id));
                let pg = node.get_pg(self.context.id().pg_id().get()).unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            })
            .collect()
    }

    pub(super) fn shadows(&self) -> Vec<(super::super::complete_node_startup::Snapshot, u64)> {
        // These are separate fixture files left behind the remote endpoints.
        // They must not become an implicit process-local alternate target.
        self.remote
            .iter()
            .map(|peer| {
                let pg = self
                    .map
                    .node(peer.node)
                    .unwrap()
                    .test_node()
                    .get_pg(self.context.id().pg_id().get())
                    .unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            })
            .collect()
    }
}

#[test]
fn staging_complete_fanout_mixed_unix_witness_all_contexts_and_custody_origins() {
    terminal_matrix(Some(false));
}

#[test]
fn staging_complete_fanout_mixed_tls_witness_all_contexts_and_custody_origins() {
    terminal_matrix(Some(true));
}

#[test]
fn staging_complete_fanout_mixed_embedded_witness_all_contexts_and_custody_origins() {
    terminal_matrix(None);
}

fn terminal_matrix(witness_tls: Option<bool>) {
    for case in 0..4 {
        for history in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let mixed = Mixed::setup(&dir, case, witness_tls);
            let id = mixed.context.id();
            let admission = mixed.handle.admit_current_route().unwrap();
            let owner = fanout_embedded::sender(&mixed.map, &mixed.context, history, deadline());
            let original = owner.installation_observation();
            let mut fanout = owner
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let shadows = mixed.shadows();
            for peer in &mixed.remote {
                assert!(section
                    .application_target(peer.node)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("embedded execution endpoint"));
            }
            let before = mixed.observe();
            // No listeners yet: admission/ordering must reject before transport.
            assert!(mixed
                .apply(&mut fanout, &section, 0)
                .unwrap_err()
                .contains("publication marker is not confirmed"));
            assert_eq!(mixed.observe(), before);
            fanout.publish_embedded(&section, deadline()).unwrap();
            assert!(mixed
                .apply(&mut fanout, &section, 4)
                .unwrap_err()
                .contains("witness confirmation"));
            for id in [1, 2, 3] {
                assert!(mixed
                    .apply(&mut fanout, &section, id)
                    .unwrap_err()
                    .contains("primary confirmation"));
            }
            let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
            let mut expected = None;
            for (position, node_id) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                mixed.apply(&mut fanout, &section, node_id).unwrap();
                let node = mixed.node(NodeId::new(node_id));
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let state = (
                    introduction::state(&pg),
                    complete_maintenance::log_state(&pg),
                    pg.metadata_command_replica_state().unwrap(),
                );
                if let Some(expected) = &expected {
                    assert_eq!(&state, expected);
                } else {
                    expected = Some(state);
                }
                let applied = (snapshot(&pg), pg.conn.total_changes());
                drop(pg);
                mixed.apply(&mut fanout, &section, node_id).unwrap();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!(
                    (snapshot(&pg), pg.conn.total_changes()),
                    applied,
                    "exact replay changed member {node_id}"
                );
                drop(pg);
                if position < 4 {
                    assert!(fanout.completion().is_err());
                    fanout = fanout.into_terminal().err().unwrap().into_fanout();
                    assert_eq!(fanout.installation_observation(), original);
                }
            }
            assert_eq!(fanout.completion().unwrap().confirmed_peers(), 5);
            assert_eq!(mixed.shadows(), shadows);
            let mut terminal = fanout
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let before = mixed.observe();
            if case == 2 {
                assert!(terminal
                    .retire_embedded(&section, deadline())
                    .unwrap_err()
                    .to_string()
                    .contains("terminal work integration"));
                assert_eq!(
                    terminal.slot_retirement_observation(),
                    Retired::NoAttemptObserved
                );
                assert_eq!(mixed.observe(), before);
            } else {
                terminal.retire_embedded(&section, deadline()).unwrap();
                assert_eq!(terminal.slot_retirement_observation(), Retired::Retired);
                for node_id in 0..5 {
                    let node = mixed.node(NodeId::new(node_id));
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                    assert_eq!(
                        &(
                            introduction::state(&pg),
                            complete_maintenance::log_state(&pg),
                            pg.metadata_command_replica_state().unwrap()
                        ),
                        expected.as_ref().unwrap()
                    );
                }
                let after = mixed.observe();
                terminal
                    .test_retire_embedded_with_hook(&section, deadline(), |_| {
                        panic!("confirmed retirement redispatched")
                    })
                    .unwrap();
                assert_eq!(mixed.observe(), after);
            }
            assert_eq!(terminal.installation_observation(), original);
            let before_drop = mixed.observe();
            drop(terminal);
            assert_eq!(mixed.observe(), before_drop);
            assert_eq!(mixed.shadows(), shadows);
        }
    }
}

#[test]
fn staging_complete_fanout_mixed_remote_loss_retains_history_through_exact_retry_and_retirement() {
    for tls in [false, true] {
        for lost_node in [0, 2] {
            let dir = test_util::tempdir();
            let mixed = Mixed::setup(&dir, 3, Some(if lost_node == 0 { tls } else { !tls }));
            let id = mixed.context.id();
            let admission = mixed.handle.admit_current_route().unwrap();
            let owner =
                fanout_embedded::sender(&mixed.map, &mixed.context, "uncertain", deadline());
            let original = owner.installation_observation();
            let mut fanout = owner
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            fanout.publish_embedded(&section, deadline()).unwrap();
            let lost = mixed.remote(NodeId::new(lost_node)).unwrap();
            assert_eq!(lost.tls, tls);
            let replies = lost.count_applications();
            let _running: Vec<_> = mixed
                .remote
                .iter()
                .filter(|peer| peer.node != lost.node)
                .map(Remote::run)
                .collect();
            if lost_node != 0 {
                mixed.apply(&mut fanout, &section, 0).unwrap();
                mixed.apply(&mut fanout, &section, 4).unwrap();
                mixed.apply(&mut fanout, &section, 1).unwrap();
                mixed.apply(&mut fanout, &section, 3).unwrap();
            }
            let node = mixed.node(lost.node);
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    lost.server
                        .test_accept_one_complete_application_fault(false, true)
                        .unwrap()
                });
                assert!(mixed.apply(&mut fanout, &section, lost_node).is_err());
                worker.join().unwrap();
            });
            assert_eq!(replies.load(Ordering::SeqCst), 1, "no hidden retry");
            assert_eq!(
                fanout.application_observation(lost.node),
                Applied::Unconfirmed
            );
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_ne!(
                snapshot(&pg),
                before,
                "lost response must follow actual apply"
            );
            let applied = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            if lost_node == 0 {
                assert!(mixed
                    .apply(&mut fanout, &section, 4)
                    .unwrap_err()
                    .contains("witness confirmation"));
            } else {
                assert!(fanout
                    .completion()
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("confirmed 4 of 5"));
            }
            fanout = fanout.into_terminal().err().unwrap().into_fanout();
            assert_eq!(fanout.installation_observation(), original);
            assert!(fanout.apply_replica(&lost.client, Instant::now()).is_err());
            assert_eq!(replies.load(Ordering::SeqCst), 1);
            assert_eq!(
                fanout.application_observation(lost.node),
                Applied::Unconfirmed
            );
            let _retry = lost.run();
            mixed.apply(&mut fanout, &section, lost_node).unwrap();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), applied);
            drop(pg);
            assert_eq!(replies.load(Ordering::SeqCst), 2);
            if lost_node == 0 {
                for node in [4, 2, 3, 1] {
                    mixed.apply(&mut fanout, &section, node).unwrap();
                }
            }
            let mut terminal = fanout
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let mut reached = false;
            assert!(terminal
                .test_retire_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "after-commit" {
                        reached = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "mixed retirement confirmation loss",
                        });
                    }
                    Ok(())
                })
                .is_err());
            assert!(reached);
            assert_eq!(terminal.slot_retirement_observation(), Retired::Unconfirmed);
            let retired = mixed.observe();
            terminal.retire_embedded(&section, deadline()).unwrap();
            assert_eq!(
                mixed.observe(),
                retired,
                "absence confirmation must not mutate"
            );
            assert_eq!(terminal.installation_observation(), original);
            drop(terminal);
            assert_eq!(mixed.observe(), retired);
        }
    }
}

#[test]
fn staging_complete_fanout_mixed_embedded_primary_loss_blocks_remote_followers() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mixed = Mixed::setup(&dir, 1, Some(!tls));
        let id = mixed.context.id();
        let admission = mixed.handle.admit_current_route().unwrap();
        let owner = fanout_embedded::sender(&mixed.map, &mixed.context, "uncertain", deadline());
        let original = owner.installation_observation();
        let mut fanout = owner
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let follower = mixed.remote(NodeId::new(2)).unwrap();
        assert_eq!(follower.tls, tls);
        let replies = follower.count_applications();
        let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
        fanout.publish_embedded(&section, deadline()).unwrap();
        mixed.apply(&mut fanout, &section, 0).unwrap();
        let mut reached = false;
        assert!(fanout
            .test_apply_embedded_with_hook(&section, NodeId::new(4), deadline(), |stage| {
                if stage == "after-commit" {
                    reached = true;
                    return Err(StoreError::OperationDeadlineExceeded {
                        context: "mixed primary confirmation loss",
                    });
                }
                Ok(())
            })
            .is_err());
        assert!(reached);
        assert_eq!(
            fanout.application_observation(NodeId::new(4)),
            Applied::Unconfirmed
        );
        let before = mixed.observe();
        assert!(mixed
            .apply(&mut fanout, &section, 2)
            .unwrap_err()
            .contains("primary confirmation"));
        assert_eq!(replies.load(Ordering::SeqCst), 0);
        assert_eq!(mixed.observe(), before);
        fanout = fanout.into_terminal().err().unwrap().into_fanout();
        mixed.apply(&mut fanout, &section, 4).unwrap();
        assert_eq!(
            mixed.observe(),
            before,
            "exact primary replay must not mutate"
        );
        for node in [2, 3, 1] {
            mixed.apply(&mut fanout, &section, node).unwrap();
        }
        assert_eq!(replies.load(Ordering::SeqCst), 1);
        let mut terminal = fanout
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(terminal.installation_observation(), original);
        terminal.retire_embedded(&section, deadline()).unwrap();
    }
}

#[test]
fn staging_complete_fanout_mixed_raw_renewal_cannot_revive_either_transport() {
    for witness_tls in [Some(false), Some(true), None] {
        let dir = test_util::tempdir();
        let mixed = Mixed::setup(&dir, 0, witness_tls);
        let now = crate::clock::current_time_millis();
        let monotonic = crate::clock::monotonic_time_millis();
        mixed
            .map
            .test_store_route_map_validity(RouteMapValidity::until_ms(now + 60_000).unwrap());
        let id = mixed.context.id();
        let admission = mixed.handle.admit_current_route().unwrap();
        let mut fanout =
            fanout_embedded::sender(&mixed.map, &mixed.context, "uncertain", deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let replies: Vec<_> = mixed
            .remote
            .iter()
            .map(Remote::count_applications)
            .collect();
        let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
        fanout.publish_embedded(&section, deadline()).unwrap();
        mixed.apply(&mut fanout, &section, 0).unwrap();
        mixed.apply(&mut fanout, &section, 4).unwrap();
        let before = mixed.observe();
        let counts: Vec<_> = replies
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .collect();
        crate::clock::with_time_and_monotonic_override(now + 120_000, monotonic + 120_000, || {
            mixed
                .map
                .test_store_route_map_validity(RouteMapValidity::until_ms(now + 240_000).unwrap());
            mixed.map.require_route_map_valid_now().unwrap();
            for node in 0..5 {
                assert!(mixed.apply(&mut fanout, &section, node).is_err());
            }
            assert!(fanout.completion().is_err());
            fanout = fanout.into_terminal().err().unwrap().into_fanout();
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Applied
            );
            assert_eq!(
                fanout.application_observation(NodeId::new(4)),
                Applied::Applied
            );
            for node in [1, 2, 3] {
                assert_eq!(
                    fanout.application_observation(NodeId::new(node)),
                    Applied::NoAttemptObserved
                );
            }
        });
        assert_eq!(mixed.observe(), before);
        assert_eq!(
            replies
                .iter()
                .map(|count| count.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
            counts
        );
    }
}
