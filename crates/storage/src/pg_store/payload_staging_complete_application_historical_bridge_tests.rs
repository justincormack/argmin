// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Historical remote-primary custody borrowed by an embedded replica. No fanout
//! completion or frontend activation; the primary is a real Unix/TLS server.

use super::*;
use crate::node_client::{LocalUnixStorageNodeClientAdmissionSettings, UnixStorageNodeClient};
use crate::node_runtime::pg_store::{ApplicationObservation, CompleteApplicationRequest};
use crate::storage_node_server::StorageNodeServer;

struct Bridge {
    fixture: Fixture,
    server: Arc<StorageNodeServer>,
    client: Arc<UnixStorageNodeClient>,
    config: crate::storage_node_server::StorageNodeProcessConfig,
    _remote_dir: test_util::TempDir,
    _dir: test_util::TempDir,
}

impl Bridge {
    fn new(
        case: usize,
        tls: bool,
        placement: Option<(&[u32], bool)>,
        mismatch: Option<&str>,
    ) -> Self {
        let dir = test_util::tempdir();
        let mut fixture = Fixture::with_members(&dir, case, &[4, 2], placement);
        if let Some(field) = mismatch {
            let original = application::observation(&fixture.context);
            let mut sha = original.command_sha256();
            sha[0] ^= u8::from(field == "sha");
            fixture.runtime_map = fixture.report(Some(PendingMetadataCommandObservation::new(
                original.cluster_epoch(),
                NonZeroU64::new(original.log_index() + u64::from(field == "index")).unwrap(),
                original.command_checksum() ^ u64::from(field == "crc"),
                sha,
            )));
            fixture.handle = fixture.handle_for(&fixture.runtime_map);
        }
        let remote_dir = test_util::tempdir();
        let mut config = historical::historical_config(config(case), &fixture.context);
        config.cluster_epoch = fixture.runtime_map.cluster_epoch();
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
        }
        config.data_dir = remote_dir.path().join("node");
        config.socket_path = remote_dir.path().join("socket/node.sock");
        std::fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        std::fs::set_permissions(
            config.socket_path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let id = fixture.context.id();
        let pg_dir = config.data_dir.join(format!("pg-{:04}", id.pg_id().get()));
        std::fs::create_dir_all(&pg_dir).unwrap();
        let db = pg_dir.join("metadata.db");
        fixture
            .map
            .node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap()
            .conn
            .execute("VACUUM INTO ?1", [db.to_str().unwrap()])
            .unwrap();
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
        let (server, client) = historical::reopen(&config, id.cluster_epoch(), tls);
        let (_, auth) = super::super::super::complete_log_rpc::authenticated_server(
            crate::storage_node_server::PreparedStorageNodeServer::new(config.clone()),
        );
        // Real maps register a current-epoch client. Only the admitted route
        // may select the historical epoch for its session.
        let client = Arc::new(
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                NodeId::new(4),
                config.cluster_epoch,
                client.test_execution_endpoint().clone(),
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(auth),
            )
            .with_pg_topology(Arc::new(topology())),
        );
        // Preserve every embedded runtime; the primary endpoint uses separate
        // copied fixture files. Its shadow must never be used as a fallback.
        let map = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
            &fixture.map,
            &fixture.runtime_map,
        )
        .unwrap();
        let mut map = map;
        map.test_register_complete_fanout_client(Arc::clone(&client));
        fixture.map = Arc::new(map);
        fixture.handle = StorageClusterRouteHandle::from_authorized_cluster(
            StorageCluster::from_runtime_local_map(Arc::clone(&fixture.map), &fixture.runtime_map)
                .unwrap(),
        );
        // Keep the original fixture directory alive with the remote directory.
        // The wrapper below owns it; no stores are reopened while live.
        Self {
            fixture,
            server: Arc::new(server),
            client,
            config,
            _remote_dir: remote_dir,
            _dir: dir,
        }
    }
}

#[test]
fn staging_complete_application_historical_bridge_unix_tls_contexts_retry_and_custody() {
    for tls in [false, true] {
        for case in 0..4 {
            let bridge = Bridge::new(case, tls, None, None);
            let fixture = &bridge.fixture;
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            assert!(admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .is_err());
            // Owning historical fanout now supports this cohort, but this
            // receiver-only route still cannot mint all-member completion.
            admission
                .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let route = admission
                .historical_remote_application_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let shadow = fixture.map.node(NodeId::new(4)).unwrap().test_node();
            let shadow_before = snapshot(&shadow.get_pg(id.pg_id().get()).unwrap());
            let _running =
                application_sender::RunningReplica::start(Arc::clone(&bridge.server), tls);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            let mut sender =
                pending(&fixture.context).into_publication_sender(NodeId::new(4), deadline());
            sender.send_remote(&session, deadline()).unwrap();
            let target = route
                .embedded_replica_target(&bridge.client, &session, NodeId::new(2), deadline())
                .unwrap();
            let node = fixture.map.node(NodeId::new(2)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let error = sender
                .test_apply_embedded_with_hook(&target, deadline(), |at| {
                    if at == "after-commit" {
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "lost historical bridge confirmation",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("lost historical bridge confirmation"));
            assert_eq!(
                sender.application_observation(NodeId::new(2)),
                ApplicationObservation::Unconfirmed
            );
            let after = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            assert_ne!(before, after);
            let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
            sender.apply_embedded(&target, deadline()).unwrap();
            assert_eq!(
                sender.application_observation(NodeId::new(2)),
                ApplicationObservation::Applied
            );
            assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), after);
            assert_eq!(
                node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                writes
            );
            drop(target); // Release session borrow before sending on it again.
            sender
                .apply_remote_in_session(&session, deadline())
                .unwrap();
            let primary = bridge.server.test_storage_node();
            let pg = primary.get_pg(id.pg_id().get()).unwrap();
            let replica = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!(introduction::state(&pg), introduction::state(&replica));
            assert_eq!(
                complete_maintenance::log_state(&pg),
                complete_maintenance::log_state(&replica)
            );
            assert!(started(&pg), "receiver must not retire pending custody");
            assert_eq!(
                snapshot(&shadow.get_pg(id.pg_id().get()).unwrap()),
                shadow_before
            );
        }
    }
}

#[test]
fn staging_complete_application_historical_bridge_binds_report_before_store_access() {
    for tls in [false, true] {
        for field in ["index", "crc", "sha"] {
            let bridge = Bridge::new(0, tls, None, Some(field));
            let fixture = &bridge.fixture;
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let route = admission
                .historical_remote_application_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let _running =
                application_sender::RunningReplica::start(Arc::clone(&bridge.server), tls);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            let target = route
                .embedded_replica_target(&bridge.client, &session, NodeId::new(2), deadline())
                .unwrap();
            let node = fixture.map.node(NodeId::new(2)).unwrap().test_node();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let before = snapshot(&pg);
            let writes = pg.conn.total_changes();
            let encoded = fixture.context.encode_command().unwrap();
            let error = target
                .apply_complete_command(&CompleteApplicationRequest::new(
                    NodeId::new(2),
                    &encoded,
                    Instant::now() + Duration::from_millis(30),
                ))
                .unwrap_err()
                .into_store_error()
                .unwrap();
            assert!(
                matches!(
                    error,
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation:
                            "historical embedded application differs from admitted recovery report"
                    }
                ),
                "{field}: {error:?}"
            );
            assert_eq!(snapshot(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_application_historical_bridge_membership_and_exact_session() {
    for tls in [false, true] {
        for retained in [false, true] {
            let bridge = Bridge::new(0, tls, Some((&[4, 0], retained)), None);
            let fixture = &bridge.fixture;
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let route = admission
                .historical_remote_application_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let _running =
                application_sender::RunningReplica::start(Arc::clone(&bridge.server), tls);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            for member in [0, 4, 99] {
                assert!(route
                    .embedded_replica_target(
                        &bridge.client,
                        &session,
                        NodeId::new(member),
                        deadline()
                    )
                    .is_err());
            }
            let node = fixture.map.node(NodeId::new(2)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let target =
                route.embedded_replica_target(&bridge.client, &session, NodeId::new(2), deadline());
            if !retained {
                assert!(target.is_err());
                assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
                continue;
            }
            let target = target.unwrap();
            // Holding this target holds the exact primary session; another
            // borrower must stop at its own deadline, not wait for this drop.
            let start = Instant::now();
            let error = route
                .embedded_replica_target(
                    &bridge.client,
                    &session,
                    NodeId::new(2),
                    start + Duration::from_millis(30),
                )
                .err()
                .unwrap();
            assert!(error
                .to_string()
                .contains("borrow complete primary session"));
            assert!(start.elapsed() < Duration::from_secs(1));
            assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
            let encoded = fixture.context.encode_command().unwrap();
            target
                .apply_complete_command(&CompleteApplicationRequest::new(
                    NodeId::new(2),
                    &encoded,
                    deadline(),
                ))
                .unwrap();
            drop(target);
            drop(session);

            let (_, auth) = super::super::super::complete_log_rpc::authenticated_server(
                crate::storage_node_server::PreparedStorageNodeServer::new(bridge.config.clone()),
            );
            let foreign = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                NodeId::new(4),
                id.cluster_epoch(),
                bridge.client.test_execution_endpoint().clone(),
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(auth),
            )
            .with_pg_topology(Arc::new(topology()));
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            // Same node/endpoint, independent client instance and session.
            let foreign_session = foreign
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            let error = route
                .embedded_replica_target(
                    &bridge.client,
                    &foreign_session,
                    NodeId::new(2),
                    deadline(),
                )
                .err()
                .unwrap();
            assert!(error.to_string().contains("another client instance"));
            let error = route
                .embedded_replica_target(&foreign, &foreign_session, NodeId::new(2), deadline())
                .err()
                .unwrap();
            assert!(error.to_string().contains("exact admitted primary client"));
            drop(foreign_session);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            let wrong_pg = PgId::new((id.pg_id().get() + 1) % 8);
            let error = session
                .borrow_complete_primary(&bridge.client, wrong_pg, id.cluster_epoch(), deadline())
                .err()
                .unwrap();
            assert!(error.to_string().contains("exact open primary session"));
            assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        }
    }
}

#[test]
fn staging_complete_application_historical_bridge_captured_cutoffs_at_effect_commit_and_replay() {
    for kind in ["route", "target", "request"] {
        for (stage, replay) in [
            ("before-effect", false),
            ("before-commit", false),
            ("before-commit", true),
        ] {
            let bridge = Bridge::new(0, false, None, None);
            let fixture = &bridge.fixture;
            let id = fixture.context.id();
            // Use one real clock domain across the server thread and frontend.
            // A TLS clock installed before server construction would capture a
            // different process clock-health baseline from the handler thread.
            let cutoff = Instant::now() + Duration::from_secs(if kind == "route" { 4 } else { 1 });
            if kind == "route" {
                fixture.map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(crate::clock::current_time_millis() + 4_000)
                        .unwrap(),
                );
            }
            let admission = fixture.handle.admit_current_route().unwrap();
            let route = admission
                .historical_remote_application_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let _running =
                application_sender::RunningReplica::start(Arc::clone(&bridge.server), false);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            let target = route
                .embedded_replica_target(
                    &bridge.client,
                    &session,
                    NodeId::new(2),
                    if kind == "target" { cutoff } else { deadline() },
                )
                .unwrap();
            let encoded = fixture.context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(
                NodeId::new(2),
                &encoded,
                if kind == "request" {
                    cutoff
                } else {
                    deadline()
                },
            );
            if replay {
                target.apply_complete_command(&request).unwrap();
            }
            target
                .test_with_application_authority(&request, |pg, authority| {
                    let before = snapshot(pg);
                    let mut reached = false;
                    let error = pg
                        .test_apply_embedded_complete_command_with_hook(authority, |at| {
                            if at == stage {
                                reached = true;
                                if kind == "route" {
                                    let expires = fixture.map.route_map_valid_until_ms().unwrap();
                                    fixture.map.test_store_route_map_validity(
                                        RouteMapValidity::until_ms(expires + 60_000).unwrap(),
                                    );
                                }
                                assert!(Instant::now() < cutoff);
                                while let Some(left) = cutoff.checked_duration_since(Instant::now())
                                {
                                    std::thread::sleep(left);
                                }
                                fixture.map.require_route_map_valid_now().unwrap();
                                admission.require_valid_now().unwrap();
                            }
                            Ok(())
                        })
                        .unwrap_err()
                        .into_store_error()
                        .unwrap();
                    assert!(reached);
                    assert!(
                        matches!(
                            error,
                            StoreError::RouteMapExpired { .. }
                                | StoreError::OperationDeadlineExceeded { .. }
                        ),
                        "{kind}: {error:?}"
                    );
                    assert!(pg.conn.is_autocommit());
                    assert_eq!(snapshot(pg), before);
                    assert!(authority
                        .require_effect(
                            pg,
                            &CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline())
                        )
                        .is_err());
                    Ok(())
                })
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_application_historical_bridge_lost_primary_reply_revokes_session() {
    for tls in [false, true] {
        let bridge = Bridge::new(0, tls, None, None);
        let fixture = &bridge.fixture;
        let id = fixture.context.id();
        let admission = fixture.handle.admit_current_route().unwrap();
        let route = admission
            .historical_remote_application_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let mut sender =
            pending(&fixture.context).into_publication_sender(NodeId::new(4), deadline());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                bridge
                    .server
                    .test_accept_one_complete_application_fault(false, true)
                    .unwrap()
            });
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            sender.send_remote(&session, deadline()).unwrap();
            assert!(sender
                .apply_remote_in_session(&session, deadline())
                .is_err());
            worker.join().unwrap();
            assert_eq!(
                sender.application_observation(NodeId::new(4)),
                ApplicationObservation::Unconfirmed
            );
            let node = fixture.map.node(NodeId::new(2)).unwrap().test_node();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let before = snapshot(&pg);
            let error = route
                .embedded_replica_target(&bridge.client, &session, NodeId::new(2), deadline())
                .err()
                .unwrap();
            assert!(error.to_string().contains("exact open primary session"));
            assert_eq!(snapshot(&pg), before);
            assert_eq!(
                sender.application_observation(NodeId::new(2)),
                ApplicationObservation::NoAttemptObserved
            );
            drop(pg);
            drop(session);
            let _running =
                application_sender::RunningReplica::start(Arc::clone(&bridge.server), tls);
            let session = route
                .open_primary_session(&bridge.client, deadline())
                .unwrap();
            sender
                .apply_remote_in_session(&session, deadline())
                .unwrap();
            let target = route
                .embedded_replica_target(&bridge.client, &session, NodeId::new(2), deadline())
                .unwrap();
            sender.apply_embedded(&target, deadline()).unwrap();
        });
    }
}
