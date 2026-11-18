// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::super::super::abandonment_handoff::incomplete;
use super::super::super::abandonment_remote::sender;
use super::*;

#[test]
fn staging_abandonment_handoff_historical_mixed_cohort_not_current_membership() {
    for remote_primary in [false, true] {
        for tls in [false, true] {
            for case in 0..4 {
                let endpoints = if remote_primary {
                    vec![(4, tls), (0, !tls)]
                } else {
                    vec![(0, tls)]
                };
                // Peering migration needs a freshly observed source member.
                // Keep node 4, replace 0/2 by 1, then prove the handoff still
                // requires the OLD 4/2/0 cohort rather than current 4/1.
                let mixed = Mixed::with_placement(case, &endpoints, Some((&[4, 1], true)), None);
                let f = &mixed.fixture;
                let id = f.context.id();
                assert_eq!(
                    f.runtime_map
                        .pg_routes()
                        .iter()
                        .find(|r| r.pg_id() == id.pg_id())
                        .unwrap()
                        .acting_set(),
                    &[NodeId::new(4), NodeId::new(1)],
                    "current replacement members must differ from the historical recovery cohort"
                );
                let admission = f.handle.admit_current_route().unwrap();
                let source = sender(
                    &mixed.node(4).get_pg(id.pg_id().get()).unwrap(),
                    &f.context,
                    "uncertain",
                    deadline(),
                );
                let route = admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap();
                assert_eq!(
                    route.nodes().collect::<Vec<_>>(),
                    [4, 2, 0].map(NodeId::new)
                );
                let mut owner = source
                    .into_fanout(route)
                    .unwrap_or_else(|(_, e)| panic!("{e}"))
                    .into_unstarted_abandonment()
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                owner = incomplete(owner, 0, 3);
                let history = owner.installation_observation();
                let shadows = mixed.shadows();
                let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
                let primary_route = (!remote_primary).then(|| {
                    admission
                        .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                        .unwrap()
                });
                let section = primary_route
                    .as_ref()
                    .map(|r| r.lock_until(deadline()).unwrap());
                let session = remote_primary.then(|| {
                    owner
                        .open_primary_session(&mixed.peer(4).client, deadline())
                        .unwrap()
                });
                if let Some(section) = &section {
                    owner.record_primary_embedded(section, deadline()).unwrap();
                    owner = incomplete(owner, 1, 3);
                    owner
                        .record_remote_replica_from_embedded_primary(
                            section,
                            &mixed.peer(0).client,
                            deadline(),
                        )
                        .unwrap();
                    owner = incomplete(owner, 2, 3);
                    owner
                        .record_replica_embedded(section, NodeId::new(2), deadline())
                        .unwrap();
                } else {
                    let primary = &mixed.peer(4).client;
                    let session = session.as_ref().unwrap();
                    owner
                        .record_primary_remote(primary, session, deadline())
                        .unwrap();
                    owner = incomplete(owner, 1, 3);
                    owner
                        .record_remote_replica_from_remote_primary(
                            primary,
                            session,
                            &mixed.peer(0).client,
                            deadline(),
                        )
                        .unwrap();
                    owner = incomplete(owner, 2, 3);
                    owner
                        .record_embedded_replica_from_remote_primary(
                            primary,
                            session,
                            NodeId::new(2),
                            deadline(),
                        )
                        .unwrap();
                }
                let before = mixed.observe();
                let handoff = owner
                    .into_abandoned()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(handoff.confirmed_peers(), 3);
                assert_eq!(handoff.id(), id);
                handoff.assert_context_for_test(&f.context, &topology());
                assert_eq!(handoff.installation_observation(), history);
                handoff.require_valid().unwrap();
                if let Some(section) = &section {
                    let failure = handoff
                        .prepare_part_cleanup(section)
                        .err()
                        .expect("historical source authority minted a successor");
                    assert!(
                        failure
                            .error()
                            .to_string()
                            .contains("requires current primary admission"),
                        "{}",
                        failure.error()
                    );
                    let handoff = failure.into_handoff();
                    assert_eq!(handoff.installation_observation(), history);
                    handoff.assert_context_for_test(&f.context, &topology());
                } else {
                    drop(handoff);
                }
                assert_eq!(mixed.observe(), before);
                assert_eq!(mixed.shadows(), shadows);
            }
        }
    }
}

#[test]
fn staging_abandonment_handoff_restart_reauthenticates_all_tombstones_without_writes() {
    for tls in [false, true] {
        let mixed = Mixed::new(2, &[(4, tls), (0, !tls), (2, tls)]);
        // Finish all durable effects, but lose one confirmation and the entire
        // frontend holder. Reopening never imports process-local confirmations.
        {
            let f = &mixed.fixture;
            let id = f.context.id();
            let admission = f.handle.admit_current_route().unwrap();
            let mut owner = sender(
                &mixed.node(4).get_pg(id.pg_id().get()).unwrap(),
                &f.context,
                "uncertain",
                deadline(),
            )
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"))
            .into_unstarted_abandonment()
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            let _primary = mixed.peer(4).run();
            let _second = mixed.peer(2).run();
            let session = owner
                .open_primary_session(&mixed.peer(4).client, deadline())
                .unwrap();
            owner
                .record_primary_remote(&mixed.peer(4).client, &session, deadline())
                .unwrap();
            owner
                .record_remote_replica_from_remote_primary(
                    &mixed.peer(4).client,
                    &session,
                    &mixed.peer(2).client,
                    deadline(),
                )
                .unwrap();
            std::thread::scope(|scope| {
                let peer = mixed.peer(0);
                let worker = scope.spawn(|| {
                    peer.server
                        .test_accept_one_abandonment_fault(false, true)
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(peer));
                assert!(owner
                    .record_remote_replica_from_remote_primary(
                        &mixed.peer(4).client,
                        &session,
                        &peer.client,
                        deadline()
                    )
                    .is_err());
                wake.disarm();
                worker.join().unwrap();
            });
            drop(incomplete(owner, 2, 3));
        }
        let mixed = lifetime::reopen(mixed);
        let f = &mixed.fixture;
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let mut owner = sender(
            &mixed.node(4).get_pg(id.pg_id().get()).unwrap(),
            &f.context,
            "inspected",
            deadline(),
        )
        .into_fanout(
            admission
                .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
        let before = mixed.observe();
        owner = incomplete(owner, 0, 3);
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        let primary = &mixed.peer(4).client;
        let session = owner.open_primary_session(primary, deadline()).unwrap();
        owner
            .record_primary_remote(primary, &session, deadline())
            .unwrap();
        for (index, node) in [0, 2].into_iter().enumerate() {
            owner = incomplete(owner, index + 1, 3);
            owner
                .record_remote_replica_from_remote_primary(
                    primary,
                    &session,
                    &mixed.peer(node).client,
                    deadline(),
                )
                .unwrap();
        }
        let handoff = owner
            .into_abandoned()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(handoff.confirmed_peers(), 3);
        assert!(handoff.installation_observation().is_none());
        handoff.assert_context_for_test(&f.context, &topology());
        drop(handoff);
        assert_eq!(
            mixed.observe(),
            before,
            "all exact reinspection/handoff paths are write-free"
        );
    }
}
