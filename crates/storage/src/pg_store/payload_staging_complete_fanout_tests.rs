// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::{ApplicationObservation, CompleteFanoutSender};
use crate::storage_rpc as wire;
use crate::{
    LocalClusterMap, LocalNodeStoreConfig, LocalPgRoute, StorageCluster, StorageClusterRouteHandle,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(super) fn map(
    dir: &test_util::TempDir,
    config: &crate::storage_node_server::StorageNodeProcessConfig,
    clients: &[Arc<UnixStorageNodeClient>],
) -> (Arc<LocalClusterMap>, StorageClusterRouteHandle) {
    let mut map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        config.node_id,
        [0, 1, 2, 3, 4]
            .map(|id| LocalNodeStoreConfig::new(NodeId::new(id), dir.path().join(id.to_string()))),
        &config.pg_ids,
        config.default_ec_shape,
        config.cluster_epoch,
        config.pg_routes.iter().map(|route| {
            LocalPgRoute::from(&crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch,
                PgId::new(route.pg_id),
                route.primary_node_id,
                route.acting_set.clone(),
                route.state,
            ))
        }),
    )
    .unwrap();
    map.test_set_route_map_validity(config.route_map_validity);
    map.test_install_historical_pg_routes(config.historical_pg_routes.iter().map(|route| {
        crate::control_plane::PgRouteSnapshot::reconstructed(
            route.cluster_epoch,
            PgId::new(route.pg_id),
            route.primary_node_id,
            route.acting_set.clone(),
            route.state,
        )
    }));
    for client in clients {
        map.test_register_complete_fanout_client(Arc::clone(client));
    }
    let map = Arc::new(map);
    let cluster =
        StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), config.cluster_epoch)
            .unwrap();
    (
        map,
        StorageClusterRouteHandle::from_authorized_cluster(cluster),
    )
}

#[test]
fn staging_complete_fanout_terminal_handoff_requires_all_five_members_after_lost_reply_unix() {
    terminal_handoff_matrix(false);
}

#[test]
fn staging_complete_fanout_terminal_handoff_requires_all_five_members_after_lost_reply_tls() {
    terminal_handoff_matrix(true);
}

fn terminal_handoff_matrix(tls: bool) {
    for history in ["inspected", "installed", "uncertain"] {
        for case in 0..4 {
            let primary_dir = test_util::tempdir();
            let (primary, primary_client, config, context) =
                application_rpc::setup(tls, case, false, &primary_dir);
            let primary_client = Arc::new(primary_client);
            let replica_dirs: Vec<_> = (0..4).map(|_| test_util::tempdir()).collect();
            let peers: Vec<_> = [2, 0, 3, 1]
                .into_iter()
                .zip(&replica_dirs)
                .map(|(id, dir)| {
                    let (server, client) = application_sender::replica_from_primary(
                        tls,
                        &primary,
                        &config,
                        context.id().pg_id(),
                        dir,
                        NodeId::new(id),
                    );
                    (Arc::new(server), Arc::new(client))
                })
                .collect();
            let mut clients = vec![Arc::clone(&primary_client)];
            clients.extend(peers.iter().map(|(_, client)| Arc::clone(client)));
            let map_dir = test_util::tempdir();
            let now = crate::clock::current_time_millis();
            let monotonic = crate::clock::monotonic_time_millis();
            let mut frontend_config = config.clone();
            frontend_config.route_map_validity =
                crate::RouteMapValidity::until_ms(now + 60_000).unwrap();
            let (map, handle) = map(&map_dir, &frontend_config, &clients);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
                .unwrap();
            assert_eq!(
                route.nodes().collect::<Vec<_>>(),
                [4, 2, 0, 3, 1].map(NodeId::new)
            );
            let encoded = context.encode_command().unwrap();
            let copy = ContextualCommand::decode_command(
                encoded.bytes(),
                encoded.checksum_crc64(),
                &topology(),
            )
            .unwrap();
            let sender = if history == "inspected" {
                pending(&context).into_publication_sender(config.node_id, deadline())
            } else {
                let mut submission = copy.into_submission();
                let node = primary.test_storage_node();
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                if history == "uncertain" {
                    // Failed COMMIT followed by a different installer's recovery;
                    // this holder must retain the unconfirmed installation result.
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_slot")
                        .unwrap();
                    handoff::uncertain_install(&mut submission, &pg);
                    install(&pg, &context);
                } else {
                    complete_pending::install(&mut submission, &pg).unwrap();
                }
                submission.into_publication_sender(config.node_id, deadline())
            };
            let original = sender.installation_observation();
            assert_eq!(original.is_some(), history != "inspected");
            let mut fanout = sender
                .into_fanout(route)
                .unwrap_or_else(|(_, err)| panic!("{err}"));
            assert_eq!(fanout.installation_observation(), original);
            assert!(fanout.completion().is_err());
            let replies = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&replies);
            peers[0]
                .0
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind
                        == crate::storage_rpc::StorageRpcMessageKind::MetadataCommandApplyAndRecord
                    {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let terminal = std::thread::scope(|scope| {
                let worker = scope.spawn(|| primary.accept_one().unwrap());
                let session = primary_client
                    .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                    .unwrap();
                fanout
                    .publish(&primary_client, &session, deadline())
                    .unwrap();
                assert!(
                    fanout.apply_replica(&primary_client, deadline()).is_err(),
                    "primary must use its held session"
                );
                assert!(fanout
                    .apply_primary(&primary_client, &session, deadline())
                    .unwrap_err()
                    .to_string()
                    .contains("witness confirmation"));
                assert!(fanout
                    .apply_replica(&peers[0].1, deadline())
                    .unwrap_err()
                    .to_string()
                    .contains("primary confirmation"));
                let running: Vec<_> = peers[1..]
                    .iter()
                    .map(|(server, _)| {
                        application_sender::RunningReplica::start(Arc::clone(server), tls)
                    })
                    .collect();
                // The witness is node zero, not acting-set position one (node two).
                fanout.apply_replica(&peers[1].1, deadline()).unwrap();
                fanout
                    .apply_primary(&primary_client, &session, deadline())
                    .unwrap();
                let lost = scope.spawn(|| {
                    peers[0]
                        .0
                        .test_accept_one_complete_application_fault(false, true)
                        .unwrap()
                });
                assert!(fanout.apply_replica(&peers[0].1, deadline()).is_err());
                lost.join().unwrap();
                assert_eq!(
                    fanout.application_observation(NodeId::new(2)),
                    ApplicationObservation::Unconfirmed
                );
                assert_eq!(replies.load(Ordering::SeqCst), 1);
                let retried =
                    application_sender::RunningReplica::start(Arc::clone(&peers[0].0), tls);
                for (_, client) in &peers[2..] {
                    fanout.apply_replica(client, deadline()).unwrap();
                }
                let error = fanout.completion().err().unwrap();
                assert!(error.to_string().contains("confirmed 4 of 5"), "{error}");
                let failure = fanout.into_terminal().err().unwrap();
                assert!(failure.error().to_string().contains("confirmed 4 of 5"));
                fanout = failure.into_fanout();
                assert_eq!(fanout.installation_observation(), original);
                assert_eq!(
                    fanout.application_observation(NodeId::new(2)),
                    ApplicationObservation::Unconfirmed
                );
                assert!(fanout.apply_replica(&peers[0].1, Instant::now()).is_err());
                assert!(fanout.completion().is_err());
                let node = peers[0].0.test_storage_node();
                let pg_id = context.id().pg_id().get();
                let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
                fanout.apply_replica(&peers[0].1, deadline()).unwrap();
                assert_eq!(
                    node.get_pg(pg_id).unwrap().conn.total_changes(),
                    writes,
                    "exact retry must not apply twice"
                );
                assert_eq!(replies.load(Ordering::SeqCst), 2);
                let completion = fanout.completion().unwrap();
                assert_eq!(completion.id(), context.id());
                assert_eq!(completion.confirmed_peers(), 5);
                completion.require_valid().unwrap();
                assert_eq!(fanout.installation_observation(), original);
                assert!(
                    started(&primary.test_storage_node().get_pg(pg_id).unwrap()),
                    "completion evidence cannot clear primary custody"
                );
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        map.test_store_route_map_validity(
                            crate::RouteMapValidity::until_ms(now + 240_000).unwrap(),
                        );
                        map.require_route_map_valid_now().unwrap();
                        admission.require_valid_now().unwrap();
                        assert!(matches!(completion.require_valid(),
                            Err(StoreError::RouteMapExpired { valid_until_ms, .. })
                            if valid_until_ms == now + 60_000));
                        assert!(fanout.completion().is_err());
                    },
                );
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        assert!(fanout
                            .publish(&primary_client, &session, deadline())
                            .is_err());
                        assert!(fanout
                            .apply_primary(&primary_client, &session, deadline())
                            .is_err());
                        assert!(fanout.apply_replica(&peers[0].1, deadline()).is_err());
                        assert_eq!(replies.load(Ordering::SeqCst), 2);
                        for node in [4, 2, 0, 3, 1] {
                            assert_eq!(
                                fanout.application_observation(NodeId::new(node)),
                                ApplicationObservation::Applied
                            );
                        }
                    },
                );
                drop(retried);
                drop(running);
                drop(session);
                worker.join().unwrap();
                let failure = crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || fanout.into_terminal().err().unwrap(),
                );
                assert!(failure.error().to_string().contains("expired"));
                fanout = failure.into_fanout();
                assert_eq!(fanout.installation_observation(), original);
                let mut terminal = fanout
                    .into_terminal()
                    .unwrap_or_else(|failure| panic!("{}", failure.error()));
                assert_eq!(terminal.id(), context.id());
                assert_eq!(terminal.installation_observation(), original);
                assert_eq!(terminal.work(), context.terminal_work());
                assert_eq!(terminal.work().retained_finalization_proof, case == 2);
                terminal.release_reservation().unwrap();
                assert_eq!(
                    terminal.reservation_observation(),
                    crate::node_runtime::pg_store::TerminalReservationObservation::NotRequired
                );
                for node in [4, 2, 0, 3, 1] {
                    assert_eq!(
                        terminal.application_observation(NodeId::new(node)),
                        ApplicationObservation::Applied
                    );
                }
                terminal.require_valid().unwrap();
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        assert!(terminal.require_valid().is_err());
                        assert_eq!(terminal.installation_observation(), original);
                    },
                );
                terminal
            });
            let node = primary.test_storage_node();
            let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
            if case % 2 == 0 {
                let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _terminal = terminal;
                    panic!("interrupt terminal owner before effects");
                }));
                assert!(caught.is_err());
            } else {
                drop(terminal);
            }
            assert_eq!(
                snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
                before
            );
        }
    }
}

#[test]
fn staging_complete_fanout_freezes_the_intersection_of_route_and_original_work_cutoffs() {
    for bounded_route in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, mut config, context) = application_rpc::setup(false, 0, false, &dir);
        let client = Arc::new(client);
        if bounded_route {
            config.route_map_validity =
                crate::RouteMapValidity::until_ms(crate::clock::current_time_millis() + 60_000)
                    .unwrap();
        }
        let map_dir = test_util::tempdir();
        let (_, handle) = map(&map_dir, &config, &[Arc::clone(&client)]);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
            .unwrap();
        let work_cutoff = Instant::now() + Duration::from_secs(120);
        let (expected_local, expected_wire) = route.intersect_deadline(
            work_cutoff,
            crate::storage_rpc::StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: u64::MAX,
            },
        );
        let mut fanout = pending(&context)
            .into_publication_sender(config.node_id, work_cutoff)
            .into_fanout(route)
            .unwrap_or_else(|(_, error)| panic!("{error}"));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                .unwrap();
            fanout.publish(&client, &session, deadline()).unwrap();
            let mut frozen = None;
            let clock = crate::clock::test_time_override_guard(1_000);
            for now in [1_000, 500, 200_000] {
                clock.set(now);
                fanout.test_attempt(
                    config.node_id,
                    work_cutoff + Duration::from_secs(60),
                    |bytes, local| {
                        assert_eq!(local, expected_local);
                        let portable = wire::complete_application::decode_request(bytes)
                            .unwrap()
                            .deadline
                            .portable_wall_valid_until_ms;
                        if bounded_route {
                            assert_eq!(portable, expected_wire.portable_wall_valid_until_ms);
                        }
                        if let Some(expected) = frozen {
                            assert_eq!(portable, expected);
                        }
                        frozen = Some(portable);
                    },
                );
            }
            drop(clock);
            drop(session);
            worker.join().unwrap();
        });
    }
}

#[test]
fn staging_complete_fanout_cannot_renew_an_expired_original_work_cutoff() {
    let dir = test_util::tempdir();
    let (server, client, config, context) = application_rpc::setup(false, 0, false, &dir);
    let client = Arc::new(client);
    let map_dir = test_util::tempdir();
    let (_, handle) = map(&map_dir, &config, &[Arc::clone(&client)]);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
        .unwrap();
    let mut fanout = pending(&context)
        .into_publication_sender(config.node_id, Instant::now())
        .into_fanout(route)
        .unwrap_or_else(|(_, error)| panic!("{error}"));
    let node = server.test_storage_node();
    let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let session = client
            .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
            .unwrap();
        let error = fanout.publish(&client, &session, deadline()).unwrap_err();
        assert!(
            error.to_string().contains("original work cutoff"),
            "{error}"
        );
        assert!(fanout.apply_primary(&client, &session, deadline()).is_err());
        assert!(fanout.completion().is_err());
        assert_eq!(
            fanout.application_observation(config.node_id),
            ApplicationObservation::NoAttemptObserved
        );
        drop(session);
        worker.join().unwrap();
    });
    assert_eq!(
        snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
        before
    );
}

#[test]
fn staging_complete_fanout_rejects_foreign_registered_client_and_same_endpoint_session() {
    let dir = test_util::tempdir();
    let (server, client, config, context) = application_rpc::setup(false, 0, false, &dir);
    let client = Arc::new(client);
    let (_, foreign) = prepared(&config);
    let foreign = Arc::new(foreign); // Same node, epoch, endpoint and credentials; distinct instance.
    let map_dir = test_util::tempdir();
    let (_, handle) = map(&map_dir, &config, &[Arc::clone(&client)]);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
        .unwrap();
    assert!(route.require_client(&foreign).is_err());
    route.require_client(&client).unwrap();
    let mut fanout = pending(&context)
        .into_publication_sender(config.node_id, deadline())
        .into_fanout(route)
        .unwrap_or_else(|(_, err)| panic!("{err}"));
    let before = snapshot(
        &server
            .test_storage_node()
            .get_pg(context.id().pg_id().get())
            .unwrap(),
    );
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let foreign_session = foreign
            .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
            .unwrap();
        let error = fanout
            .publish(&client, &foreign_session, deadline())
            .unwrap_err();
        assert!(
            error.to_string().contains("another client instance"),
            "{error}"
        );
        assert!(fanout
            .publish(&foreign, &foreign_session, deadline())
            .unwrap_err()
            .to_string()
            .contains("exact admitted peer client"));
        assert!(fanout.apply_replica(&foreign, deadline()).is_err());
        assert_eq!(
            fanout.application_observation(config.node_id),
            ApplicationObservation::NoAttemptObserved
        );
        drop(foreign_session);
        worker.join().unwrap();
    });
    assert_eq!(
        snapshot(
            &server
                .test_storage_node()
                .get_pg(context.id().pg_id().get())
                .unwrap()
        ),
        before
    );
}

#[test]
fn staging_complete_fanout_binding_rejects_crossed_subject_and_preexisting_publication_history() {
    let dir = test_util::tempdir();
    let (server, client, config, context) = application_rpc::setup(false, 0, false, &dir);
    let client = Arc::new(client);
    let map_dir = test_util::tempdir();
    let (_, handle) = map(&map_dir, &config, &[Arc::clone(&client)]);
    let admission = handle.admit_current_route().unwrap();
    let pg_id = context.id().pg_id();
    for primary in [config.node_id, NodeId::new(2)] {
        let route_pg = if primary == config.node_id {
            PgId::new((pg_id.get() + 1) % 8)
        } else {
            pg_id
        };
        let route = admission
            .complete_fanout_route(route_pg, context.id().cluster_epoch())
            .unwrap();
        let owner = pending(&context).into_publication_sender(primary, deadline());
        let (owner, error) = owner.into_fanout(route).err().unwrap();
        assert!(error.to_string().contains("exact route before publication"));
        assert_eq!(owner.id(), Some(context.id()));
    }
    for epoch in [4, 6] {
        assert!(admission
            .complete_fanout_route(pg_id, ClusterEpoch::new(epoch).unwrap())
            .is_err());
    }
    let mut newer = config.clone();
    newer.cluster_epoch = ClusterEpoch::new(6).unwrap();
    for route in &mut newer.pg_routes {
        route.cluster_epoch = newer.cluster_epoch;
    }
    let newer_dir = test_util::tempdir();
    let (_, newer_handle) = map(&newer_dir, &newer, &[Arc::clone(&client)]);
    let newer_admission = newer_handle.admit_current_route().unwrap();
    let newer_route = newer_admission
        .complete_fanout_route(pg_id, newer.cluster_epoch)
        .unwrap();
    let owner = pending(&context).into_publication_sender(config.node_id, deadline());
    let (owner, error) = owner.into_fanout(newer_route).err().unwrap();
    assert!(error.to_string().contains("exact route before publication"));
    assert_eq!(owner.id(), Some(context.id()));

    let mut peering = config.clone();
    for route in &mut peering.pg_routes {
        route.state = crate::PgState::Peering;
    }
    let peering_dir = test_util::tempdir();
    let (_, peering_handle) = map(&peering_dir, &peering, &[Arc::clone(&client)]);
    let peering_admission = peering_handle.admit_current_route().unwrap();
    assert!(peering_admission
        .complete_fanout_route(pg_id, context.id().cluster_epoch())
        .is_err());
    let mut owner = pending(&context).into_publication_sender(config.node_id, deadline());
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let session = client
            .open_metadata_command_critical_section_until(pg_id, deadline())
            .unwrap();
        owner.send_remote(&session, deadline()).unwrap();
        owner.apply_remote_in_session(&session, deadline()).unwrap();
        drop(session);
        worker.join().unwrap();
    });
    let route = admission
        .complete_fanout_route(pg_id, context.id().cluster_epoch())
        .unwrap();
    let (owner, _) = owner.into_fanout(route).err().unwrap();
    assert_eq!(
        owner.application_observation(config.node_id),
        ApplicationObservation::Applied
    );
    assert_eq!(
        owner.observation(),
        crate::node_runtime::pg_store::PublicationObservation::MarkerConfirmed
    );
}

#[test]
fn staging_complete_fanout_and_completion_are_noncloneable() {
    trait NotClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    let _ = <CompleteFanoutSender<'_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::CompleteFanoutCompletion<'_, '_> as NotClone<_>>::check;
    let _ = <crate::cluster::CompleteFanoutRoute<'_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::CompleteTerminalHandoff<'_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::TerminalHandoffFailure<'_> as NotClone<_>>::check;
}
