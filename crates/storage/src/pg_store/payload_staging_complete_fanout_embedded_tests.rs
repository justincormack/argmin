// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed owning embedded fanout, not raw per-replica request construction.
//! Complete metadata fixtures; no physical payload or live publisher activation.

use super::*;
use crate::node_runtime::pg_store::{ApplicationObservation as Applied, CompletePublicationSender};
use crate::{LocalClusterMap, StorageClusterRouteHandle};
use std::sync::Arc;

pub(super) fn sender(
    map: &LocalClusterMap,
    context: &ContextualCommand,
    history: &str,
    cutoff: Instant,
) -> CompletePublicationSender {
    if history == "inspected" {
        return pending(context).into_publication_sender(NodeId::new(4), cutoff);
    }
    let encoded = context.encode_command().unwrap();
    let copy =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    let mut submission = copy.into_submission();
    let pg = map
        .node(NodeId::new(4))
        .unwrap()
        .test_node()
        .get_pg(context.id().pg_id().get())
        .unwrap();
    if history == "uncertain" {
        pg.conn
            .execute_batch("DELETE FROM metadata_command_pending_slot")
            .unwrap();
        handoff::uncertain_install(&mut submission, &pg);
        install(&pg, context);
    } else {
        assert_eq!(history, "installed");
        complete_pending::install(&mut submission, &pg).unwrap();
    }
    submission.into_publication_sender(NodeId::new(4), cutoff)
}

#[test]
fn staging_complete_fanout_embedded_single_member_has_no_witness_requirement() {
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
    let handle = StorageClusterRouteHandle::from_authorized_cluster(
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), id.cluster_epoch())
            .unwrap(),
    );
    let admission = handle.admit_current_route().unwrap();
    let mut fanout = sender(&map, &context, "installed", deadline())
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
    assert!(
        fanout
            .apply_embedded(&section, NodeId::new(0), deadline())
            .is_err(),
        "existing node outside captured cohort"
    );
    fanout
        .apply_embedded(&section, NodeId::new(4), deadline())
        .unwrap();
    assert_eq!(fanout.completion().unwrap().confirmed_peers(), 1);
    assert_eq!(
        fanout
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()))
            .id(),
        id
    );
}

#[test]
fn staging_complete_fanout_embedded_publication_loss_blocks_apply_until_exact_confirmation() {
    for stage in ["before-marker", "after-commit"] {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 1, RouteMapValidity::Forever);
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let mut fanout = sender(&map, &context, "uncertain", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let original = fanout.installation_observation();
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let before = without_marker(&node.get_pg(id.pg_id().get()).unwrap());
        let witness = map.node(NodeId::new(0)).unwrap().test_node();
        let witness_before = snapshot(&witness.get_pg(id.pg_id().get()).unwrap());
        let mut reached = false;
        let error = fanout
            .test_publish_embedded_with_hook(&section, deadline(), |at| {
                if at == stage {
                    reached = true;
                    return Err(StoreError::OperationDeadlineExceeded {
                        context: "injected embedded publication confirmation loss",
                    });
                }
                Ok(())
            })
            .unwrap_err();
        assert!(reached);
        assert!(error
            .to_string()
            .contains("injected embedded publication confirmation loss"));
        assert_eq!(
            started(&node.get_pg(id.pg_id().get()).unwrap()),
            stage == "after-commit"
        );
        assert_eq!(
            without_marker(&node.get_pg(id.pg_id().get()).unwrap()),
            before
        );
        assert!(fanout
            .apply_embedded(&section, NodeId::new(0), deadline())
            .unwrap_err()
            .to_string()
            .contains("publication marker is not confirmed"));
        assert_eq!(
            snapshot(&witness.get_pg(id.pg_id().get()).unwrap()),
            witness_before
        );
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::NoAttemptObserved
        );
        assert!(fanout.publish_embedded(&section, Instant::now()).is_err());
        assert_eq!(fanout.installation_observation(), original);
        fanout.publish_embedded(&section, deadline()).unwrap();
        assert_eq!(
            without_marker(&node.get_pg(id.pg_id().get()).unwrap()),
            before
        );
        fanout
            .apply_embedded(&section, NodeId::new(0), deadline())
            .unwrap();
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::Applied
        );
        assert_eq!(fanout.installation_observation(), original);
    }
}

#[test]
fn staging_complete_fanout_embedded_all_contexts_keep_installation_custody_and_order() {
    for case in 0..4 {
        for history in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, context) =
                application_embedded::setup(&dir, case, RouteMapValidity::Forever);
            let id = context.id();
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap();
            let owner = sender(&map, &context, history, deadline());
            let original = owner.installation_observation();
            let mut fanout = owner
                .into_fanout(route)
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let lock = map.runtime_state().metadata_command_pg_lock(id.pg_id());
            assert!(lock.try_lock().is_none());
            assert!(fanout
                .apply_embedded(&section, NodeId::new(0), deadline())
                .unwrap_err()
                .to_string()
                .contains("publication marker is not confirmed"));
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::NoAttemptObserved
            );
            fanout.publish_embedded(&section, deadline()).unwrap();
            for (node, message) in [(4, "witness confirmation"), (2, "primary confirmation")] {
                let pg = map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap();
                let before = snapshot(&pg);
                let writes = pg.conn.total_changes();
                drop(pg);
                assert!(fanout
                    .apply_embedded(&section, NodeId::new(node), deadline())
                    .unwrap_err()
                    .to_string()
                    .contains(message));
                assert_eq!(
                    fanout.application_observation(NodeId::new(node)),
                    Applied::NoAttemptObserved
                );
                let pg = map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap();
                assert_eq!(snapshot(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            }
            assert!(fanout
                .apply_embedded(&section, NodeId::new(99), deadline())
                .is_err());
            let mut expected = None;
            for (position, node) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                let state = fanout
                    .apply_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
                let pg = map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap();
                let materialized = (
                    introduction::state(&pg),
                    complete_maintenance::log_state(&pg),
                    state,
                );
                if let Some(expected) = &expected {
                    assert_eq!(&materialized, expected);
                } else {
                    expected = Some(materialized);
                }
                let after = snapshot(&pg);
                let writes = pg.conn.total_changes();
                drop(pg);
                fanout
                    .apply_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
                let pg = map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap();
                assert_eq!(snapshot(&pg), after);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(fanout.installation_observation(), original);
                assert_eq!(
                    fanout.application_observation(NodeId::new(node)),
                    Applied::Applied
                );
                if position < 4 {
                    assert!(
                        fanout.completion().is_err(),
                        "quorum is not all-member completion"
                    );
                    fanout = fanout.into_terminal().err().unwrap().into_fanout();
                    assert_eq!(fanout.installation_observation(), original);
                }
            }
            assert_eq!(fanout.completion().unwrap().confirmed_peers(), 5);
            let terminal = fanout
                .into_terminal()
                .unwrap_or_else(|failure| panic!("{}", failure.error()));
            assert_eq!(terminal.id(), id);
            assert_eq!(terminal.installation_observation(), original);
            terminal.require_valid().unwrap();
            let before: Vec<_> = (0..5)
                .map(|node| {
                    snapshot(
                        &map.node(NodeId::new(node))
                            .unwrap()
                            .test_node()
                            .get_pg(id.pg_id().get())
                            .unwrap(),
                    )
                })
                .collect();
            drop(terminal);
            for (node, before) in before.into_iter().enumerate() {
                assert_eq!(
                    snapshot(
                        &map.node(NodeId::new(u32::try_from(node).unwrap()))
                            .unwrap()
                            .test_node()
                            .get_pg(id.pg_id().get())
                            .unwrap()
                    ),
                    before,
                    "no hidden cleanup or retirement on Drop"
                );
            }
            assert!(lock.try_lock().is_none());
        }
    }
}

#[test]
fn staging_complete_fanout_embedded_crossed_domain_admission_and_pg_fail_before_effects() {
    let dir = test_util::tempdir();
    let (map, handle, context) = application_embedded::setup(&dir, 0, RouteMapValidity::Forever);
    let other_dir = test_util::tempdir();
    let (other_map, other_handle, _) =
        application_embedded::setup(&other_dir, 0, RouteMapValidity::Forever);
    let id = context.id();
    let admission = handle.admit_current_route().unwrap();
    let duplicate = handle.admit_current_route().unwrap();
    let foreign = other_handle.admit_current_route().unwrap();
    let mut fanout = sender(&map, &context, "uncertain", deadline())
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let original = fanout.installation_observation();
    let before = snapshot(
        &map.node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap(),
    );
    let other_before = snapshot(
        &other_map
            .node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap(),
    );
    for (admission, pg) in [
        (&foreign, id.pg_id()),
        (&duplicate, id.pg_id()),
        (&admission, PgId::new((id.pg_id().get() + 1) % 8)),
    ] {
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg, id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        for error in [
            fanout.publish_embedded(&section, deadline()).unwrap_err(),
            fanout
                .apply_embedded(&section, NodeId::new(0), deadline())
                .unwrap_err(),
        ] {
            assert!(
                error
                    .to_string()
                    .contains("original admitted primary section"),
                "{error}"
            );
        }
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::NoAttemptObserved
        );
        assert_eq!(fanout.installation_observation(), original);
    }
    assert_eq!(
        snapshot(
            &map.node(NodeId::new(4))
                .unwrap()
                .test_node()
                .get_pg(id.pg_id().get())
                .unwrap()
        ),
        before
    );
    assert_eq!(
        snapshot(
            &other_map
                .node(NodeId::new(4))
                .unwrap()
                .test_node()
                .get_pg(id.pg_id().get())
                .unwrap()
        ),
        other_before
    );
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    fanout.publish_embedded(&section, deadline()).unwrap();
    fanout
        .apply_embedded(&section, NodeId::new(0), deadline())
        .unwrap();
}

#[test]
fn staging_complete_fanout_embedded_uncertainty_and_unwind_survive_exact_retry() {
    for stage in [
        "before-effect",
        "after-effect",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let (map, handle, context) =
                application_embedded::setup(&dir, 3, RouteMapValidity::Forever);
            let id = context.id();
            let admission = handle.admit_current_route().unwrap();
            let mut fanout = sender(&map, &context, "uncertain", deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let original = fanout.installation_observation();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            fanout.publish_embedded(&section, deadline()).unwrap();
            let node = map.node(NodeId::new(0)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let mut reached = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fanout.test_apply_embedded_with_hook(&section, NodeId::new(0), deadline(), |at| {
                    if at == stage {
                        reached = true;
                        if unwind {
                            panic!("injected embedded fanout unwind");
                        }
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected embedded fanout interruption",
                        });
                    }
                    Ok(())
                })
            }));
            assert!(reached);
            if unwind {
                assert!(result.is_err());
            } else {
                assert!(result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("injected embedded fanout interruption"));
            }
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
            assert_eq!(fanout.installation_observation(), original);
            assert!(fanout.completion().is_err());
            assert!(fanout
                .apply_embedded(&section, NodeId::new(4), deadline())
                .is_err());
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let committed = if stage == "after-commit" {
                assert_ne!(snapshot(&pg), before);
                Some((snapshot(&pg), pg.conn.total_changes()))
            } else {
                assert_eq!(snapshot(&pg), before);
                None
            };
            drop(pg);
            // A later unsent/expired attempt cannot erase prior uncertainty.
            assert!(fanout
                .apply_embedded(&section, NodeId::new(0), Instant::now())
                .is_err());
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
            fanout
                .apply_embedded(&section, NodeId::new(0), deadline())
                .unwrap();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            if let Some((state, writes)) = committed {
                assert_eq!(snapshot(&pg), state);
                assert_eq!(pg.conn.total_changes(), writes);
            }
            drop(pg);
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Applied
            );
            assert!(fanout
                .apply_embedded(&section, NodeId::new(0), Instant::now())
                .is_err());
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Applied
            );
            for node in [4, 2, 3, 1] {
                fanout
                    .apply_embedded(&section, NodeId::new(node), deadline())
                    .unwrap();
            }
            fanout.completion().unwrap().require_valid().unwrap();
            assert_eq!(fanout.installation_observation(), original);
        }
    }
}

#[test]
fn staging_complete_fanout_embedded_original_deadline_is_not_replaced_by_later_attempts() {
    for stage in ["before-effect", "before-commit"] {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 0, RouteMapValidity::Forever);
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let cutoff = Instant::now() + Duration::from_secs(1);
        let mut fanout = sender(&map, &context, "inspected", cutoff)
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
        let node = map.node(NodeId::new(0)).unwrap().test_node();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let mut reached = false;
        let error = fanout
            .test_apply_embedded_with_hook(&section, NodeId::new(0), deadline(), |at| {
                if stage == "before-effect" && at == "after-effect" {
                    panic!("expired attempt reached mutation");
                }
                if at == stage {
                    reached = true;
                    assert!(Instant::now() < cutoff, "did not reach intended expiry gap");
                    while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
                        std::thread::sleep(remaining);
                    }
                }
                Ok(())
            })
            .unwrap_err();
        assert!(reached);
        assert!(error.to_string().contains("deadline"), "{error}");
        assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::Unconfirmed
        );
        assert!(fanout
            .apply_embedded(&section, NodeId::new(0), deadline())
            .is_err());
        assert!(fanout.completion().is_err());
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::Unconfirmed
        );
        // Dropping/reacquiring serialization never renews the owning holder.
        drop(section);
        let section = route.lock_until(deadline()).unwrap();
        assert!(fanout
            .apply_embedded(&section, NodeId::new(0), deadline())
            .is_err());
    }
}

#[test]
fn staging_complete_fanout_embedded_raw_route_renewal_cannot_revive_completion() {
    let clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let (map, handle, context) =
        application_embedded::setup(&dir, 0, RouteMapValidity::until_ms(20_000).unwrap());
    let id = context.id();
    let admission = handle.admit_current_route().unwrap();
    let mut fanout = sender(&map, &context, "installed", deadline())
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
    for node in [0, 4, 2, 3, 1] {
        fanout
            .apply_embedded(&section, NodeId::new(node), deadline())
            .unwrap();
    }
    fanout.completion().unwrap();
    let before = snapshot(
        &map.node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap(),
    );
    map.test_store_route_map_validity(RouteMapValidity::until_ms(40_000).unwrap());
    clock.set(21_000);
    map.require_route_map_valid_now().unwrap();
    assert!(fanout.completion().is_err());
    fanout = fanout.into_terminal().err().unwrap().into_fanout();
    assert!(fanout.publish_embedded(&section, deadline()).is_err());
    assert!(fanout
        .apply_embedded(&section, NodeId::new(4), deadline())
        .is_err());
    for node in 0..5 {
        assert_eq!(
            fanout.application_observation(NodeId::new(node)),
            Applied::Applied
        );
    }
    assert_eq!(
        snapshot(
            &map.node(NodeId::new(4))
                .unwrap()
                .test_node()
                .get_pg(id.pg_id().get())
                .unwrap()
        ),
        before
    );
}
