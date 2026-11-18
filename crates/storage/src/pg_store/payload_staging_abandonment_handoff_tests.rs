// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! No-effect custody transfer. Real tombstones and retained pending/owner rows;
//! metadata-only fixtures, not physical cleanup or live publisher activation.

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::node_runtime::pg_store::{
    AbandonmentHandoffError as E, CompleteUnstartedAbandonment, ReplicaAbandonmentObservation as R,
    UnstartedAbandonmentObservation as O,
};

pub(super) fn incomplete(
    owner: CompleteUnstartedAbandonment<'_>,
    confirmed: usize,
    required: usize,
) -> CompleteUnstartedAbandonment<'_> {
    let failure = owner
        .into_abandoned()
        .err()
        .expect("incomplete cohort accepted");
    assert!(
        matches!(failure.error(), E::Incomplete { confirmed: c, required: r } if *c == confirmed && *r == required),
        "{}",
        failure.error()
    );
    failure.into_abandonment()
}

#[test]
fn staging_abandonment_handoff_requires_every_embedded_member_and_preserves_all_origins() {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, context) =
                application_embedded::setup(&dir, case, RouteMapValidity::Forever);
            let id = context.id();
            let admission = handle.admit_current_route().unwrap();
            let mut owner = unstarted_abandonment::sender(&map, &context, origin, deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"))
                .into_unstarted_abandonment()
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let history = owner.installation_observation();
            owner = incomplete(owner, 0, 5);
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.record_primary_embedded(&section, deadline()).unwrap();
            for (count, node) in [2, 0, 3, 1].into_iter().enumerate() {
                owner = incomplete(owner, count + 1, 5);
                assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                assert_eq!(owner.installation_observation(), history);
                owner
                    .record_replica_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
                // A repeated confirmation must not count as another member.
                owner
                    .record_replica_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
            }
            let observe = || {
                (0..5)
                    .map(|node| {
                        let node = map.node(NodeId::new(node)).unwrap().test_node();
                        let pg = node.get_pg(id.pg_id().get()).unwrap();
                        (snapshot(&pg), pg.conn.total_changes())
                    })
                    .collect::<Vec<_>>()
            };
            let before = observe();
            let handoff = owner
                .into_abandoned()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(handoff.id(), id);
            handoff.assert_context_for_test(&context, &topology());
            assert_eq!(handoff.confirmed_peers(), 5);
            assert_eq!(handoff.installation_observation(), history);
            handoff.require_valid().unwrap();
            assert_eq!(observe(), before);
            drop(handoff);
            assert_eq!(
                observe(),
                before,
                "no hidden cleanup, release or retirement on Drop"
            );
        }
    }
}

#[test]
fn staging_abandonment_handoff_mixed_embedded_primary_unix_tls_contexts_origins() {
    mixed_matrix(false);
}

#[test]
fn staging_abandonment_handoff_mixed_remote_primary_unix_tls_contexts_origins() {
    mixed_matrix(true);
}

fn mixed_matrix(remote_primary: bool) {
    for tls in [false, true] {
        for case in 0..4 {
            for origin in ["inspected", "installed", "uncertain"] {
                let dir = test_util::tempdir();
                let endpoints = if remote_primary {
                    [(4, tls), (0, !tls)]
                } else {
                    [(0, tls), (2, !tls)]
                };
                let mixed = Mixed::with_endpoints(&dir, case, endpoints);
                let id = mixed.context.id();
                let admission = mixed.handle.admit_current_route().unwrap();
                let node = mixed.node(NodeId::new(4));
                let source = abandonment_remote::sender(
                    &node.get_pg(id.pg_id().get()).unwrap(),
                    &mixed.context,
                    origin,
                    deadline(),
                );
                let mut owner = source
                    .into_fanout(
                        admission
                            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"))
                    .into_unstarted_abandonment()
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                let history = owner.installation_observation();
                let shadows = mixed.shadows();
                let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                let route = (!remote_primary).then(|| {
                    admission
                        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                        .unwrap()
                });
                let section = route
                    .as_ref()
                    .map(|route| route.lock_until(deadline()).unwrap());
                let primary = mixed.remote(NodeId::new(4));
                let session =
                    primary.map(|p| owner.open_primary_session(&p.client, deadline()).unwrap());
                if let Some(section) = &section {
                    owner.record_primary_embedded(section, deadline()).unwrap();
                } else {
                    owner
                        .record_primary_remote(
                            &primary.unwrap().client,
                            session.as_ref().unwrap(),
                            deadline(),
                        )
                        .unwrap();
                }
                for (count, node) in [2, 0, 3, 1].into_iter().enumerate() {
                    let before = mixed.observe();
                    owner = incomplete(owner, count + 1, 5);
                    assert_eq!(mixed.observe(), before);
                    assert_eq!(owner.installation_observation(), history);
                    let node = NodeId::new(node);
                    match (section.as_ref(), mixed.remote(node)) {
                        (Some(s), Some(p)) => owner
                            .record_remote_replica_from_embedded_primary(s, &p.client, deadline())
                            .unwrap(),
                        (Some(s), None) => {
                            owner.record_replica_embedded(s, node, deadline()).unwrap()
                        }
                        (None, Some(p)) => owner
                            .record_remote_replica_from_remote_primary(
                                &primary.unwrap().client,
                                session.as_ref().unwrap(),
                                &p.client,
                                deadline(),
                            )
                            .unwrap(),
                        (None, None) => owner
                            .record_embedded_replica_from_remote_primary(
                                &primary.unwrap().client,
                                session.as_ref().unwrap(),
                                node,
                                deadline(),
                            )
                            .unwrap(),
                    }
                }
                let before = mixed.observe();
                let handoff = owner
                    .into_abandoned()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(handoff.installation_observation(), history);
                handoff.assert_context_for_test(&mixed.context, &topology());
                assert_eq!(handoff.confirmed_peers(), 5);
                handoff.require_valid().unwrap();
                drop(handoff);
                assert_eq!(mixed.observe(), before);
                assert_eq!(mixed.shadows(), shadows);
            }
        }
    }
}

#[test]
fn staging_abandonment_handoff_lost_confirmation_cannot_be_counted_or_reset() {
    for missing in [2, 0, 3, 1] {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 2, RouteMapValidity::Forever);
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let mut owner = unstarted_abandonment::sender(&map, &context, "uncertain", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"))
            .into_unstarted_abandonment()
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        owner.record_primary_embedded(&section, deadline()).unwrap();
        for node in [2, 0, 3, 1] {
            if node == missing {
                assert!(owner
                    .test_record_replica_embedded_with_hook(
                        &section,
                        NodeId::new(node),
                        deadline(),
                        |at| {
                            if at == "after-commit" {
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "lost replica tombstone confirmation",
                                });
                            }
                            Ok(())
                        }
                    )
                    .is_err());
            } else {
                owner
                    .record_replica_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
            }
        }
        for _ in 0..2 {
            owner = incomplete(owner, 4, 5);
            assert_eq!(
                owner.replica_observation(NodeId::new(missing)),
                R::Unconfirmed
            );
        }
        let node = map.node(NodeId::new(missing)).unwrap().test_node();
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        owner
            .record_replica_embedded(&section, NodeId::new(missing), deadline())
            .unwrap();
        let handoff = owner
            .into_abandoned()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(handoff.confirmed_peers(), 5);
        drop(handoff);
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_abandonment_handoff_single_member_still_requires_primary_confirmation() {
    use std::sync::Arc;
    let dir = test_util::tempdir();
    let (map, _, context) = application_embedded::setup(&dir, 0, RouteMapValidity::Forever);
    let id = context.id();
    let routes = config(0).pg_routes.into_iter().map(|route| {
        crate::control_plane::PgRouteSnapshot::reconstructed(
            route.cluster_epoch,
            PgId::new(route.pg_id),
            NodeId::new(4),
            vec![NodeId::new(4)],
            PgState::Active,
        )
    });
    let map = Arc::new(
        map.test_clone_with_pg_routes(id.cluster_epoch(), routes, [])
            .unwrap(),
    );
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), id.cluster_epoch())
            .unwrap(),
    );
    let admission = handle.admit_current_route().unwrap();
    let mut owner = unstarted_abandonment::sender(&map, &context, "uncertain", deadline())
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    owner = incomplete(owner, 0, 1);
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    assert!(owner
        .test_record_primary_embedded_with_hook(&section, deadline(), |at| {
            if at == "after-commit" {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "lost primary confirmation",
                });
            }
            Ok(())
        })
        .is_err());
    owner = incomplete(owner, 0, 1);
    assert_eq!(owner.observation(), O::Unconfirmed);
    owner.record_primary_embedded(&section, deadline()).unwrap();
    assert!(owner
        .record_replica_embedded(&section, NodeId::new(0), deadline())
        .is_err());
    let handoff = owner
        .into_abandoned()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    assert_eq!(handoff.confirmed_peers(), 1);
    assert_eq!(handoff.id(), id);
}

#[test]
fn staging_abandonment_handoff_never_renews_route_or_work_authority() {
    for bound in ["route", "work"] {
        for expire_before in [true, false] {
            let clock = crate::clock::test_time_override_guard(10_000);
            let dir = test_util::tempdir();
            let validity = if bound == "route" {
                RouteMapValidity::until_ms(20_000).unwrap()
            } else {
                RouteMapValidity::Forever
            };
            let (map, handle, context) = application_embedded::setup(&dir, 2, validity);
            let id = context.id();
            let admission = handle.admit_current_route().unwrap();
            let cutoff = Instant::now() + Duration::from_secs(2);
            let mut owner = unstarted_abandonment::sender(
                &map,
                &context,
                "uncertain",
                if bound == "work" { cutoff } else { deadline() },
            )
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"))
            .into_unstarted_abandonment()
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            let history = owner.installation_observation();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.record_primary_embedded(&section, deadline()).unwrap();
            for node in [2, 0, 3, 1] {
                owner
                    .record_replica_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
            }
            let before: Vec<_> = (0..5)
                .map(|node| {
                    let node = map.node(NodeId::new(node)).unwrap().test_node();
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    (snapshot(&pg), pg.conn.total_changes())
                })
                .collect();
            let expire = || {
                assert!(Instant::now() < cutoff, "setup must leave authority live");
                map.test_store_route_map_validity(RouteMapValidity::until_ms(40_000).unwrap());
                if bound == "route" {
                    clock.set(21_000);
                } else {
                    while let Some(left) = cutoff.checked_duration_since(Instant::now()) {
                        std::thread::sleep(left);
                    }
                }
                map.require_route_map_valid_now().unwrap();
            };
            if expire_before {
                expire();
                let failure = owner
                    .into_abandoned()
                    .err()
                    .expect("expired handoff accepted");
                assert!(
                    matches!(failure.error(), E::Scope(_)),
                    "{}",
                    failure.error()
                );
                let owner = failure.into_abandonment();
                assert_eq!(owner.installation_observation(), history);
                assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                for node in [2, 0, 3, 1] {
                    assert_eq!(
                        owner.replica_observation(NodeId::new(node)),
                        R::TombstoneConfirmed
                    );
                }
                assert!(
                    owner.into_abandoned().is_err(),
                    "returning the holder cannot renew it"
                );
            } else {
                let handoff = owner
                    .into_abandoned()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                handoff.require_valid().unwrap();
                expire();
                assert!(handoff.require_valid().is_err());
                assert_eq!(
                    handoff.confirmed_peers(),
                    5,
                    "past evidence is not live authority"
                );
                assert_eq!(handoff.installation_observation(), history);
                drop(handoff);
            }
            for (index, previous) in before.into_iter().enumerate() {
                let node = map.node(NodeId::new(index as u32)).unwrap().test_node();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!((snapshot(&pg), pg.conn.total_changes()), previous);
            }
        }
    }
}
