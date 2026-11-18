// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn reopen(mixed: Mixed) -> Mixed {
    let Mixed {
        mut fixture,
        peers,
        _dir,
    } = mixed;
    let mut map = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
        &fixture.map,
        &fixture.runtime_map,
    )
    .unwrap();
    let peers = peers
        .into_iter()
        .map(|peer| {
            let Remote {
                id,
                tls,
                server,
                client,
                config,
                _dir,
            } = peer;
            // No handlers/sessions/PG borrows survive this call. Drop each physical
            // runtime before opening its files; the frontend's shadow files differ.
            drop(server);
            drop(client);
            let (server, client) = historical::reopen(&config, config.cluster_epoch, tls);
            let client = Arc::new(client);
            map.test_register_complete_fanout_client(Arc::clone(&client));
            Remote {
                id,
                tls,
                server: Arc::new(server),
                client,
                config,
                _dir,
            }
        })
        .collect();
    fixture.map = Arc::new(map);
    fixture.handle = StorageClusterRouteHandle::from_authorized_cluster(
        StorageCluster::from_runtime_local_map(Arc::clone(&fixture.map), &fixture.runtime_map)
            .unwrap(),
    );
    Mixed {
        fixture,
        peers,
        _dir,
    }
}

#[test]
fn staging_complete_fanout_historical_remote_restart_reconfirms_partial_application() {
    for tls in [false, true] {
        for case in 0..4 {
            let mixed = Mixed::new(case, &[(4, tls), (0, !tls), (2, tls)]);
            let id = mixed.fixture.context.id();
            let shadows = mixed.shadows();
            {
                let f = &mixed.fixture;
                let admission = f.handle.admit_current_route().unwrap();
                let mut owner = mixed
                    .owner("uncertain", deadline())
                    .into_fanout(
                        admission
                            .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                let _running: Vec<_> = mixed
                    .peers
                    .iter()
                    .filter(|p| p.id != NodeId::new(0))
                    .map(Remote::run)
                    .collect();
                let count = mixed.peer(0).count(Kind::MetadataCommandApplyAndRecord);
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        mixed
                            .peer(0)
                            .server
                            .test_accept_one_complete_application_fault(false, true)
                            .unwrap()
                    });
                    let wake = WakeAcceptOnDrop(Some(mixed.peer(0)));
                    let session = owner
                        .open_primary_session(&mixed.peer(4).client, deadline())
                        .unwrap();
                    owner
                        .publish(&mixed.peer(4).client, &session, deadline())
                        .unwrap();
                    assert!(mixed.apply(&mut owner, None, Some(&session), 0).is_err());
                    assert_eq!(
                        count.load(Ordering::SeqCst),
                        1,
                        "fault must be reached before join"
                    );
                    wake.disarm();
                    worker.join().unwrap();
                    assert_eq!(
                        owner.application_observation(NodeId::new(0)),
                        Applied::Unconfirmed
                    );
                });
            }
            let before = mixed.observe();
            let mixed = reopen(mixed);
            for (node, ((after, _), (before, _))) in
                mixed.observe().iter().zip(before.iter()).enumerate()
            {
                assert_eq!(after, before, "node {node}: restart changed durable state");
            }
            let f = &mixed.fixture;
            let admission = f.handle.admit_current_route().unwrap();
            let mut owner = mixed
                .owner("inspected", deadline())
                .into_fanout(
                    admission
                        .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
            let session = owner
                .open_primary_session(&mixed.peer(4).client, deadline())
                .unwrap();
            owner
                .publish(&mixed.peer(4).client, &session, deadline())
                .unwrap();
            assert!(mixed
                .apply(&mut owner, None, Some(&session), 4)
                .unwrap_err()
                .contains("witness confirmation"));
            let before = mixed.observe();
            mixed.apply(&mut owner, None, Some(&session), 0).unwrap();
            assert_eq!(
                mixed.observe(),
                before,
                "durable witness replay performs no writes"
            );
            for node in [4, 2] {
                mixed.apply(&mut owner, None, Some(&session), node).unwrap();
            }
            assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
            check_terminal_handoff(owner);
            assert_eq!(mixed.shadows(), shadows);
        }
    }
}

#[test]
fn staging_complete_fanout_historical_remote_cutoffs_survive_renewal_and_session_reopening() {
    for kind in ["route", "work"] {
        for boundary in ["before-effect", "before-commit", "completion"] {
            let mixed = Mixed::new(0, &[(4, true), (2, false)]);
            let f = &mixed.fixture;
            let id = f.context.id();
            // A cross-process work cutoff is shortened by the real skew budget.
            // Leave time for setup, then expire exactly inside the chosen hook.
            let budget = Duration::from_millis(
                crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 3_000,
            );
            let cutoff = Instant::now() + budget;
            if kind == "route" {
                f.map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(crate::clock::current_time_millis() + 4_000)
                        .unwrap(),
                );
            }
            let admission = f.handle.admit_current_route().unwrap();
            let mut owner = mixed
                .owner(
                    "uncertain",
                    if kind == "work" { cutoff } else { deadline() },
                )
                .into_fanout(
                    admission
                        .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let installed = owner.installation_observation();
            let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
            let session = owner
                .open_primary_session(&mixed.peer(4).client, deadline())
                .unwrap();
            owner
                .publish(&mixed.peer(4).client, &session, deadline())
                .unwrap_or_else(|error| panic!("{kind}/{boundary} publication setup: {error}"));
            let expire = || {
                if kind == "route" {
                    f.map.test_store_route_map_validity(
                        RouteMapValidity::until_ms(crate::clock::current_time_millis() + 60_000)
                            .unwrap(),
                    );
                }
                assert!(Instant::now() < cutoff);
                while let Some(left) = cutoff.checked_duration_since(Instant::now()) {
                    std::thread::sleep(left);
                }
                f.map.require_route_map_valid_now().unwrap();
                admission.require_valid_now().unwrap();
            };
            let before;
            if boundary == "completion" {
                for node in [0, 4, 2] {
                    mixed.apply(&mut owner, None, Some(&session), node).unwrap();
                }
                assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
                before = mixed.observe();
                expire();
            } else {
                before = mixed.observe();
                let mut reached = false;
                let error = owner
                    .test_apply_embedded_replica_with_hook(
                        &mixed.peer(4).client,
                        &session,
                        NodeId::new(0),
                        deadline(),
                        |at| {
                            if at == boundary {
                                reached = true;
                                expire();
                            }
                            Ok(())
                        },
                    )
                    .unwrap_err();
                assert!(reached, "{kind}/{boundary}: {error}");
                assert_eq!(
                    owner.application_observation(NodeId::new(0)),
                    Applied::Unconfirmed
                );
            }
            assert!(owner.completion().is_err());
            let count = mixed.peer(4).count(Kind::MetadataCommandPgLockAcquire);
            assert!(owner
                .open_primary_session(&mixed.peer(4).client, deadline())
                .is_err());
            assert_eq!(
                count.load(Ordering::SeqCst),
                0,
                "expired custody cannot start a fresh section"
            );
            assert!(mixed.apply(&mut owner, None, Some(&session), 0).is_err());
            assert!(mixed.apply(&mut owner, None, Some(&session), 2).is_err());
            owner = owner.into_terminal().err().unwrap().into_fanout();
            assert_eq!(owner.installation_observation(), installed);
            for (node, ((after, _), (before, _))) in
                mixed.observe().iter().zip(before.iter()).enumerate()
            {
                assert_eq!(after, before, "{kind}/{boundary}/node {node}");
            }
        }
    }
}
