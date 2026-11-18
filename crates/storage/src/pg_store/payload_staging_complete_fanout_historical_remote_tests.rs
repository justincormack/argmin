// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated historical cohorts and shared terminal-test setup: mixed
//! endpoints, retained custody, no physical cleanup or live publisher selection.
//! Real clocks cross transports.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};
use crate::node_runtime::engine::SharedStorageNode;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, CompleteFanoutSender, CompletePublicationSender,
};
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::StorageRpcMessageKind as Kind;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "payload_staging_abandonment_handoff_historical_tests.rs"]
mod abandonment_handoff_historical;
#[path = "payload_staging_abandonment_historical_remote_tests.rs"]
mod abandonment_historical;
#[path = "payload_staging_terminal_historical_command_release_tests.rs"]
mod command_release_historical;
#[path = "payload_staging_complete_fanout_historical_remote_lifetime_tests.rs"]
mod lifetime;
#[path = "payload_staging_replica_abandonment_receiver_historical_tests.rs"]
mod replica_abandonment_receiver_historical;
#[path = "payload_staging_replica_abandonment_sender_historical_tests.rs"]
mod replica_abandonment_sender_historical;
#[path = "payload_staging_retirement_historical_tests.rs"]
mod retirement;
#[path = "payload_staging_terminal_historical_release_tests.rs"]
mod terminal_release_historical;

struct Remote {
    id: NodeId,
    tls: bool,
    server: Arc<StorageNodeServer>,
    client: Arc<UnixStorageNodeClient>,
    config: StorageNodeProcessConfig,
    _dir: test_util::TempDir,
}

// If a regression rejects before connecting (or the caller panics), wake the
// one-shot fault server so its join reports failure instead of hanging forever.
struct WakeAcceptOnDrop<'a>(Option<&'a Remote>);

impl WakeAcceptOnDrop<'_> {
    fn disarm(mut self) {
        self.0.take();
    }
}

impl Drop for WakeAcceptOnDrop<'_> {
    fn drop(&mut self) {
        let Some(remote) = self.0 else { return };
        if remote.tls {
            let _ = std::net::TcpStream::connect(remote.server.tcp_listener_addr_for_test());
        } else {
            let _ = std::os::unix::net::UnixStream::connect(remote.server.socket_path_for_test());
        }
    }
}

impl Remote {
    fn run(&self) -> application_sender::RunningReplica {
        application_sender::RunningReplica::start(Arc::clone(&self.server), self.tls)
    }

    fn count(&self, kind: Kind) -> Arc<AtomicUsize> {
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        self.server
            .set_response_frame_test_hook(Arc::new(move |at, _| {
                if at == kind {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            }));
        count
    }
}

struct Mixed {
    fixture: Fixture,
    peers: Vec<Remote>,
    _dir: test_util::TempDir,
}

impl Mixed {
    fn new(case: usize, endpoints: &[(u32, bool)]) -> Self {
        Self::with_placement(case, endpoints, None, None)
    }

    fn with_placement(
        case: usize,
        endpoints: &[(u32, bool)],
        placement: Option<(&[u32], bool)>,
        crossed_report_node: Option<u32>,
    ) -> Self {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members(&dir, case, &[4, 2, 0], placement);
        Self::from_fixture(dir, fixture, case, endpoints, crossed_report_node)
    }

    fn with_finalizer_release(endpoints: &[(u32, bool)]) -> Self {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members_and_release_route(&dir, 2, &[4, 2, 0], None, true);
        Self::from_fixture(dir, fixture, 2, endpoints, None)
    }

    fn from_fixture(
        dir: test_util::TempDir,
        mut fixture: Fixture,
        case: usize,
        endpoints: &[(u32, bool)],
        crossed_report_node: Option<u32>,
    ) -> Self {
        let mut map = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
            &fixture.map,
            &fixture.runtime_map,
        )
        .unwrap();
        let mut peers = Vec::new();
        for &(id, tls) in endpoints {
            let remote_dir = test_util::tempdir();
            // Frontends receive compact route history. Storage nodes receive
            // the exact required epochs through the production refresh projection.
            let node_map = fixture
                .authority
                .snapshot()
                .runtime_map_for_storage_node_refresh(
                    crate::clock::current_time_millis(),
                    NodeId::new(id),
                    fixture.context.id().cluster_epoch(),
                )
                .unwrap();
            let mut config = StorageNodeProcessConfig::from_runtime_map(
                NodeId::new(id),
                remote_dir.path().join("node"),
                config(case).default_ec_shape,
                &node_map,
            )
            .unwrap();
            config.socket_path = remote_dir.path().join("socket/node.sock");
            if crossed_report_node == Some(id) {
                let recovery = &mut config.pending_metadata_command_recoveries[0].1;
                let pending = recovery.pending();
                let mut sha = pending.command_sha256();
                sha[0] ^= 1;
                *recovery = crate::control_plane::PendingMetadataCommandRecovery::new(
                    NodeId::new(4),
                    PendingMetadataCommandObservation::new(
                        pending.cluster_epoch(),
                        NonZeroU64::new(pending.log_index()).unwrap(),
                        pending.command_checksum(),
                        sha,
                    ),
                );
            }
            let (server, client) = application_sender::replica_from_pg(
                tls,
                &fixture
                    .map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(fixture.context.id().pg_id().get())
                    .unwrap(),
                &config,
                &remote_dir,
                NodeId::new(id),
            );
            if id == 4 {
                install(
                    &server
                        .test_storage_node()
                        .get_pg(fixture.context.id().pg_id().get())
                        .unwrap(),
                    &fixture.context,
                );
            }
            let client = Arc::new(client);
            map.test_register_complete_fanout_client(Arc::clone(&client));
            peers.push(Remote {
                id: NodeId::new(id),
                tls,
                server: Arc::new(server),
                client,
                config,
                _dir: remote_dir,
            });
        }
        fixture.map = Arc::new(map);
        fixture.handle = StorageClusterRouteHandle::from_authorized_cluster(
            StorageCluster::from_runtime_local_map(Arc::clone(&fixture.map), &fixture.runtime_map)
                .unwrap(),
        );
        Self {
            fixture,
            peers,
            _dir: dir,
        }
    }

    fn peer(&self, id: u32) -> &Remote {
        self.peers.iter().find(|p| p.id == NodeId::new(id)).unwrap()
    }

    fn node(&self, id: u32) -> Arc<SharedStorageNode> {
        self.peers
            .iter()
            .find(|p| p.id == NodeId::new(id))
            .map_or_else(
                || Arc::clone(self.fixture.map.node(NodeId::new(id)).unwrap().test_node()),
                |p| p.server.test_storage_node(),
            )
    }

    fn observe(&self) -> Vec<(super::super::super::complete_node_startup::Snapshot, u64)> {
        (0..5)
            .map(|id| {
                let node = self.node(id);
                let pg = node
                    .get_pg(self.fixture.context.id().pg_id().get())
                    .unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            })
            .collect()
    }

    fn shadows(&self) -> Vec<(super::super::super::complete_node_startup::Snapshot, u64)> {
        self.peers
            .iter()
            .map(|peer| {
                let pg = self
                    .fixture
                    .map
                    .node(peer.id)
                    .unwrap()
                    .test_node()
                    .get_pg(self.fixture.context.id().pg_id().get())
                    .unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            })
            .collect()
    }

    fn owner(&self, origin: &str, cutoff: Instant) -> CompletePublicationSender {
        let context = &self.fixture.context;
        if origin == "inspected" {
            return pending(context).into_publication_sender(NodeId::new(4), cutoff);
        }
        let encoded = context.encode_command().unwrap();
        let mut holder = ContextualCommand::decode_command(
            encoded.bytes(),
            encoded.checksum_crc64(),
            &topology(),
        )
        .unwrap()
        .into_submission();
        let node = self.node(4);
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        if origin == "uncertain" {
            pg.conn
                .execute_batch("DELETE FROM metadata_command_pending_slot")
                .unwrap();
            handoff::uncertain_install(&mut holder, &pg);
            install(&pg, context);
        } else {
            assert_eq!(origin, "installed");
            complete_pending::install(&mut holder, &pg).unwrap();
        }
        holder.into_publication_sender(NodeId::new(4), cutoff)
    }

    fn apply(
        &self,
        owner: &mut CompleteFanoutSender<'_>,
        section: Option<&EmbeddedPublicationSection<'_>>,
        session: Option<&UnixStorageNodeMetadataCommandSession>,
        id: u32,
    ) -> Result<(), String> {
        let result = if let Some(peer) = self.peers.iter().find(|p| p.id == NodeId::new(id)) {
            if id == 4 {
                owner.apply_primary(&peer.client, session.unwrap(), deadline())
            } else {
                owner.apply_replica(&peer.client, deadline())
            }
        } else if let Some(section) = section {
            owner.apply_embedded(section, NodeId::new(id), deadline())
        } else {
            owner.apply_embedded_replica(
                &self.peer(4).client,
                session.unwrap(),
                NodeId::new(id),
                deadline(),
            )
        };
        result.map(|_| ()).map_err(|e| e.to_string())
    }
}

use super::fanout_historical::check_terminal_handoff;

#[test]
fn staging_complete_fanout_historical_remote_embedded_primary_contexts_and_origins() {
    matrix(false, false);
}

#[test]
fn staging_complete_fanout_historical_remote_unix_primary_contexts_and_origins() {
    matrix(true, false);
}

#[test]
fn staging_complete_fanout_historical_remote_tls_primary_contexts_and_origins() {
    matrix(true, true);
}

fn matrix(remote_primary: bool, tls: bool) {
    for remote_witness in [false, true] {
        for case in 0..4 {
            for origin in ["inspected", "installed", "uncertain"] {
                let endpoints = if remote_primary {
                    vec![(4, tls), (if remote_witness { 0 } else { 2 }, !tls)]
                } else if remote_witness {
                    vec![(0, false), (2, true)]
                } else {
                    vec![(2, tls)]
                };
                let mixed = Mixed::new(case, &endpoints);
                let f = &mixed.fixture;
                let id = f.context.id();
                let admission = f.handle.admit_current_route().unwrap();
                let mut owner = mixed
                    .owner(origin, deadline())
                    .into_fanout(
                        admission
                            .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                let installed = owner.installation_observation();
                let shadows = mixed.shadows();
                let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
                let route = (!remote_primary).then(|| {
                    admission
                        .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                        .unwrap()
                });
                let section = route
                    .as_ref()
                    .map(|route| route.lock_until(deadline()).unwrap());
                let session = remote_primary.then(|| {
                    owner
                        .open_primary_session(&mixed.peer(4).client, deadline())
                        .unwrap()
                });
                assert!(mixed
                    .apply(&mut owner, section.as_ref(), session.as_ref(), 0)
                    .unwrap_err()
                    .contains("publication marker is not confirmed"));
                if let Some(section) = &section {
                    owner.publish_embedded(section, deadline()).unwrap();
                } else {
                    owner
                        .publish(&mixed.peer(4).client, session.as_ref().unwrap(), deadline())
                        .unwrap();
                }
                let before = mixed.observe();
                assert!(mixed
                    .apply(&mut owner, section.as_ref(), session.as_ref(), 4)
                    .unwrap_err()
                    .contains("witness confirmation"));
                assert!(mixed
                    .apply(&mut owner, section.as_ref(), session.as_ref(), 2)
                    .unwrap_err()
                    .contains("primary confirmation"));
                assert!(mixed
                    .apply(&mut owner, section.as_ref(), session.as_ref(), 1)
                    .is_err());
                assert_eq!(mixed.observe(), before);
                let mut expected = None;
                for (position, node) in [0, 4, 2].into_iter().enumerate() {
                    mixed
                        .apply(&mut owner, section.as_ref(), session.as_ref(), node)
                        .unwrap();
                    let applied = mixed.observe();
                    mixed
                        .apply(&mut owner, section.as_ref(), session.as_ref(), node)
                        .unwrap();
                    assert_eq!(
                        mixed.observe(),
                        applied,
                        "no-write exact application replay"
                    );
                    let n = mixed.node(node);
                    let pg = n.get_pg(id.pg_id().get()).unwrap();
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
                    if position < 2 {
                        assert!(owner.completion().is_err());
                    }
                    assert_eq!(owner.installation_observation(), installed);
                }
                assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
                let complete = mixed.observe();
                check_terminal_handoff(owner);
                assert_eq!(mixed.observe(), complete);
                assert_eq!(
                    mixed.shadows(),
                    shadows,
                    "remote members never use their local shadows"
                );
                assert_eq!(complete[1], before[1]);
                assert_eq!(complete[3], before[3]);
            }
        }
    }
}

#[test]
fn staging_complete_fanout_historical_remote_requires_retained_cohort_and_exact_peer_report() {
    for retained in [false, true] {
        let mixed =
            Mixed::with_placement(0, &[(0, false), (2, true)], Some((&[4, 1], retained)), None);
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let before = mixed.observe();
        let route = admission.complete_historical_fanout_route(&f.runtime_map, id.pg_id());
        if !retained {
            assert!(route
                .err()
                .unwrap()
                .to_string()
                .contains("complete Active cohort"));
            assert_eq!(mixed.observe(), before);
            continue;
        }
        let mut owner = mixed
            .owner("uncertain", deadline())
            .into_fanout(route.unwrap())
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        owner.publish_embedded(&section, deadline()).unwrap();
        for node in [0, 4, 2] {
            mixed.apply(&mut owner, Some(&section), None, node).unwrap();
        }
        assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
        check_terminal_handoff(owner);
    }
    for tls in [false, true] {
        let mixed = Mixed::with_placement(0, &[(0, tls)], None, Some(0));
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let mut owner = mixed
            .owner("uncertain", deadline())
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let original = owner.installation_observation();
        let route = admission
            .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        owner.publish_embedded(&section, deadline()).unwrap();
        let before = mixed.observe();
        let peer = mixed.peer(0);
        let replies = peer.count(Kind::MetadataCommandApplyAndRecord);
        let _running = peer.run();
        assert!(mixed.apply(&mut owner, Some(&section), None, 0).is_err());
        assert_eq!(replies.load(Ordering::SeqCst), 1);
        assert_eq!(
            owner.application_observation(NodeId::new(0)),
            Applied::Unconfirmed
        );
        assert_eq!(owner.installation_observation(), original);
        assert_eq!(
            mixed.observe(),
            before,
            "a same-ID/CRC different-SHA peer report cannot authorize apply"
        );
        assert!(mixed
            .apply(&mut owner, Some(&section), None, 4)
            .unwrap_err()
            .contains("witness confirmation"));
    }
}

#[test]
fn staging_complete_fanout_historical_remote_lost_confirmations_preserve_order_and_custody() {
    for tls in [false, true] {
        for lost_id in [0, 4, 2] {
            let mixed = Mixed::new(3, &[(4, tls), (0, !tls), (2, tls)]);
            let f = &mixed.fixture;
            let id = f.context.id();
            let admission = f.handle.admit_current_route().unwrap();
            let mut owner = mixed
                .owner("uncertain", deadline())
                .into_fanout(
                    admission
                        .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let installed = owner.installation_observation();
            let lost = mixed.peer(lost_id);
            let count = lost.count(Kind::MetadataCommandApplyAndRecord);
            let _running: Vec<_> = mixed
                .peers
                .iter()
                .filter(|p| p.id != lost.id)
                .map(Remote::run)
                .collect();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    lost.server
                        .test_accept_one_complete_application_fault(false, true)
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(lost));
                let session = owner
                    .open_primary_session(&mixed.peer(4).client, deadline())
                    .unwrap();
                owner
                    .publish(&mixed.peer(4).client, &session, deadline())
                    .unwrap();
                for node in [0, 4, 2] {
                    let result = mixed.apply(&mut owner, None, Some(&session), node);
                    if node == lost_id {
                        assert!(result.is_err());
                        break;
                    }
                    result.unwrap();
                }
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    1,
                    "fault must be reached before join"
                );
                wake.disarm();
                worker.join().unwrap();
                assert_eq!(owner.application_observation(lost.id), Applied::Unconfirmed);
                assert_eq!(count.load(Ordering::SeqCst), 1, "no hidden transport retry");
                if lost_id == 0 {
                    assert!(mixed
                        .apply(&mut owner, None, Some(&session), 4)
                        .unwrap_err()
                        .contains("witness confirmation"));
                } else if lost_id == 4 {
                    assert!(mixed
                        .apply(&mut owner, None, Some(&session), 2)
                        .unwrap_err()
                        .contains("primary confirmation"));
                }
            });
            assert!(owner.completion().is_err());
            owner = owner.into_terminal().err().unwrap().into_fanout();
            assert_eq!(owner.installation_observation(), installed);
            let _retry = lost.run();
            let session = owner
                .open_primary_session(&mixed.peer(4).client, deadline())
                .unwrap();
            let applied = mixed.observe();
            mixed
                .apply(&mut owner, None, Some(&session), lost_id)
                .unwrap();
            assert_eq!(
                mixed.observe(),
                applied,
                "lost application reply retries without writes"
            );
            assert_eq!(count.load(Ordering::SeqCst), 2);
            for node in [0, 4, 2] {
                mixed.apply(&mut owner, None, Some(&session), node).unwrap();
            }
            assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
            check_terminal_handoff(owner);
        }

        // Publication loss must not license application even though the marker
        // is durable. Reopening a session keeps the SAME owning holder/history.
        let mixed = Mixed::new(0, &[(4, tls), (0, !tls)]);
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let mut owner = mixed
            .owner("uncertain", deadline())
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let installed = owner.installation_observation();
        let primary = mixed.peer(4);
        let count = primary.count(Kind::MetadataCommandPublicationStart);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                primary
                    .server
                    .test_accept_one_losing_publication_reply()
                    .unwrap()
            });
            let wake = WakeAcceptOnDrop(Some(primary));
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            assert!(owner
                .publish(&primary.client, &session, deadline())
                .is_err());
            assert_eq!(
                count.load(Ordering::SeqCst),
                1,
                "fault must be reached before join"
            );
            wake.disarm();
            worker.join().unwrap();
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(owner
            .apply_replica(&mixed.peer(0).client, deadline())
            .unwrap_err()
            .to_string()
            .contains("publication marker is not confirmed"));
        assert!(started(&mixed.node(4).get_pg(id.pg_id().get()).unwrap()));
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        let session = owner
            .open_primary_session(&primary.client, deadline())
            .unwrap();
        owner
            .publish(&primary.client, &session, deadline())
            .unwrap();
        for node in [0, 4, 2] {
            mixed.apply(&mut owner, None, Some(&session), node).unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(owner.installation_observation(), installed);
        check_terminal_handoff(owner);
    }
}

#[test]
fn staging_complete_fanout_historical_remote_rejects_crossed_clients_and_report_bound_targets() {
    for tls in [false, true] {
        let mixed = Mixed::new(0, &[(4, tls), (2, !tls)]);
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
            .unwrap();
        let mut owner = mixed
            .owner("uncertain", deadline())
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let primary = mixed.peer(4);
        let duplicate = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            NodeId::new(4),
            f.runtime_map.cluster_epoch(),
            primary.client.test_execution_endpoint().clone(),
            crate::node_client::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            None,
        );
        let before = mixed.observe();
        // No listener: reject exact-instance and role crossings before transport.
        assert!(owner
            .open_primary_session(&duplicate, deadline())
            .err()
            .unwrap()
            .to_string()
            .contains("exact admitted peer client"));
        assert!(owner
            .open_primary_session(&mixed.peer(2).client, deadline())
            .err()
            .unwrap()
            .to_string()
            .contains("admitted primary"));
        assert_eq!(mixed.observe(), before);
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        let session = owner
            .open_primary_session(&primary.client, deadline())
            .unwrap();
        let target = route
            .embedded_replica_target(&primary.client, &session, NodeId::new(0), deadline())
            .unwrap();
        let encoded = f.context.encode_command().unwrap();
        target
            .require_command(id, encoded.checksum_crc64(), encoded.command_sha256())
            .unwrap();
        let mut crossed_sha = encoded.command_sha256();
        crossed_sha[0] ^= 1;
        assert!(target
            .require_command(id, encoded.checksum_crc64(), crossed_sha)
            .unwrap_err()
            .to_string()
            .contains("differs from admitted recovery report"));
        assert_eq!(mixed.observe(), before);
        drop(target);
        owner
            .publish(&primary.client, &session, deadline())
            .unwrap();
        for node in [0, 4, 2] {
            mixed.apply(&mut owner, None, Some(&session), node).unwrap();
        }
        check_terminal_handoff(owner);
    }
}

#[test]
fn staging_complete_fanout_historical_remote_session_waits_are_bounded_without_reset_or_send() {
    for tls in [false, true] {
        let mixed = Mixed::new(0, &[(4, tls), (2, !tls)]);
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let mut owner = mixed
            .owner("uncertain", deadline())
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let installed = owner.installation_observation();
        let primary = mixed.peer(4);
        let _running: Vec<_> = mixed
            .peers
            .iter()
            .map(|p| {
                application_sender::RunningReplica::start_concurrent(Arc::clone(&p.server), p.tls)
            })
            .collect();
        let session = owner
            .open_primary_session(&primary.client, deadline())
            .unwrap();
        let waiting = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&waiting);
        primary
            .server
            .test_observe_metadata_command_lock_wait(move |pg| {
                assert_eq!(pg, id.pg_id());
                observed.store(true, Ordering::SeqCst);
            });
        // An independently requested second section cannot wait beyond its own
        // attempt deadline behind the first session's remotely held PG lock.
        let start = Instant::now();
        assert!(owner
            .open_primary_session(&primary.client, start + Duration::from_secs(1))
            .is_err());
        assert!(start.elapsed() < Duration::from_secs(3));
        assert!(
            waiting.load(Ordering::SeqCst),
            "second session must reach the held PG lock"
        );
        for publish in [true, false] {
            if !publish {
                owner
                    .publish(&primary.client, &session, deadline())
                    .unwrap();
                mixed.apply(&mut owner, None, Some(&session), 0).unwrap();
            }
            let before = mixed.observe();
            let prior = owner.application_observation(NodeId::new(4));
            let count = primary.count(if publish {
                Kind::MetadataCommandPublicationStart
            } else {
                Kind::MetadataCommandApplyAndRecord
            });
            let replies = count.load(Ordering::SeqCst);
            let result = std::thread::scope(|scope| {
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let session_ref = &session;
                let worker = scope.spawn(move || {
                    let _held = session_ref
                        .borrow_complete_primary(
                            &primary.client,
                            id.pg_id(),
                            id.cluster_epoch(),
                            deadline(),
                        )
                        .unwrap();
                    ready_tx.send(()).unwrap();
                    // A broken blocking send must fail this test, not hang the
                    // suite forever. The normal path explicitly releases us.
                    release_rx.recv_timeout(Duration::from_secs(2))
                });
                ready_rx.recv().unwrap();
                let start = Instant::now();
                let result = if publish {
                    owner.publish(&primary.client, &session, start + Duration::from_millis(50))
                } else {
                    owner
                        .apply_primary(&primary.client, &session, start + Duration::from_millis(50))
                        .map(|_| ())
                };
                let elapsed = start.elapsed();
                let released = release_tx.send(());
                assert!(
                    worker.join().unwrap().is_ok(),
                    "session wait exceeded contention failsafe"
                );
                released.unwrap();
                assert!(elapsed < Duration::from_secs(1));
                result
            });
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("acquire metadata command session RPC"));
            assert_eq!(
                count.load(Ordering::SeqCst),
                replies,
                "no frame dispatched while mutex busy"
            );
            assert_eq!(owner.application_observation(NodeId::new(4)), prior);
            assert_eq!(owner.installation_observation(), installed);
            assert_eq!(mixed.observe(), before);
        }
        for node in [4, 2] {
            mixed.apply(&mut owner, None, Some(&session), node).unwrap();
        }
        assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
        check_terminal_handoff(owner);
    }
}
