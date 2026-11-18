// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate primary tombstone, not replica abandonment or cleanup. All fixture
//! payload metadata is owner-local; these tests make no physical-I/O claims.

use super::*;
use crate::node_runtime::pg_store::{
    CompletePendingRequest, CompletePublicationSender, MetadataCommandStartupDisposition,
    UnstartedAbandonmentObservation as O,
};
use crate::LocalClusterMap;

pub(super) fn sender(
    map: &LocalClusterMap,
    context: &ContextualCommand,
    origin: &str,
    cutoff: Instant,
) -> CompletePublicationSender {
    if origin != "inspected" {
        return fanout_embedded::sender(map, context, origin, cutoff);
    }
    let pg = map
        .node(NodeId::new(4))
        .unwrap()
        .test_node()
        .get_pg(context.id().pg_id().get())
        .unwrap();
    CompletePendingRequest::new(
        context.id().pg_id(),
        context.id().cluster_epoch(),
        &topology(),
    )
    .unwrap()
    .read(&pg, 4)
    .unwrap()
    .into_publication_sender(NodeId::new(4), cutoff)
}

#[test]
fn staging_unstarted_abandonment_contexts_origins_replay_and_restart_keep_pending_custody() {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, context) =
                application_embedded::setup(&dir, case, RouteMapValidity::Forever);
            let id = context.id();
            {
                let admission = handle.admit_current_route().unwrap();
                let fanout = sender(&map, &context, origin, deadline())
                    .into_fanout(
                        admission
                            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                let original = fanout.installation_observation();
                let mut owner = fanout
                    .into_unstarted_abandonment()
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                assert_eq!(owner.observation(), O::NoAttemptObserved);
                let route = admission
                    .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                let node = map.node(NodeId::new(4)).unwrap().test_node();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let before = (introduction::state(&pg), complete_pending::observe(&pg));
                let old_log = complete_maintenance::log_state(&pg);
                drop(pg);
                let witness = map.node(NodeId::new(0)).unwrap().test_node();
                let witness_before = snapshot(&witness.get_pg(id.pg_id().get()).unwrap());
                owner.record_primary_embedded(&section, deadline()).unwrap();
                assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                assert_eq!(owner.installation_observation(), original);
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!(
                    (introduction::state(&pg), complete_pending::observe(&pg)),
                    before
                );
                assert_ne!(complete_maintenance::log_state(&pg), old_log);
                assert!(!started(&pg));
                assert!(matches!(
                    pg.inspect_complete_pending_command(4, &topology())
                        .unwrap()
                        .unwrap()
                        .terminal,
                    MetadataCommandStartupDisposition::Abandoned { .. }
                ));
                let after = (snapshot(&pg), pg.conn.total_changes());
                drop(pg);
                owner
                    .test_record_primary_embedded_with_hook(&section, deadline(), |_| {
                        panic!("confirmed operation redispatched")
                    })
                    .unwrap();
                drop(owner);
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
                assert_eq!(
                    snapshot(&witness.get_pg(id.pg_id().get()).unwrap()),
                    witness_before
                );
            }
            // No second runtime over live stores. Reinspection must authenticate
            // the exact tombstone and confirm it without another durable write.
            drop(handle);
            drop(map);
            let (map, handle) =
                embedded::embedded(&dir, case, PgState::Active, RouteMapValidity::Forever);
            let admission = handle.admit_current_route().unwrap();
            let mut owner = sender(&map, &context, "inspected", deadline())
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
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let before = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            owner.record_primary_embedded(&section, deadline()).unwrap();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
        }
    }
}

#[test]
fn staging_unstarted_abandonment_durable_marker_applied_and_damaged_indexes_fail_closed() {
    for damage in [
        "UPDATE metadata_command_pending_slot SET publication_started = 1",
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-abandonment-bucket'",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "UPDATE metadata_command_pending_slot SET command_bytes = zeroblob(length(command_bytes))",
        "applied",
    ] {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 1, RouteMapValidity::Forever);
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let mut owner = sender(&map, &context, "uncertain", deadline())
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
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        if damage == "applied" {
            apply(&pg, &context);
        } else {
            pg.conn.execute_batch(damage).unwrap();
        }
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        assert!(
            owner
                .test_record_primary_embedded_with_hook(&section, deadline(), |_| panic!(
                    "rejected input reached effect hook"
                ))
                .is_err(),
            "{damage}"
        );
        assert_eq!(owner.observation(), O::Unconfirmed);
        drop(owner);
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before, "{damage}");
    }
}

#[test]
fn staging_unstarted_abandonment_cannot_discard_publication_or_application_history() {
    for outcome in [
        "before-marker",
        "after-commit",
        "confirmed",
        "witness",
        "observed-marker",
    ] {
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
        if outcome == "observed-marker" {
            // A real published observation cannot become fresh unstarted custody.
            fanout.publish_embedded(&section, deadline()).unwrap();
            drop(fanout);
            fanout = sender(&map, &context, "inspected", deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
        } else if ["confirmed", "witness"].contains(&outcome) {
            fanout.publish_embedded(&section, deadline()).unwrap();
            if outcome == "witness" {
                fanout
                    .apply_embedded(&section, NodeId::new(0), deadline())
                    .unwrap();
            }
        } else {
            let mut reached = false;
            assert!(fanout
                .test_publish_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == outcome {
                        reached = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected publication ambiguity",
                        });
                    }
                    Ok(())
                })
                .is_err());
            assert!(reached);
        }
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        let (retained, error) = fanout
            .into_unstarted_abandonment()
            .err()
            .expect("must retain fanout");
        assert!(error
            .to_string()
            .contains("cannot discard publication/application history"));
        if outcome != "observed-marker" {
            assert_eq!(retained.installation_observation(), original);
        }
        drop(retained);
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_unstarted_abandonment_errors_unwind_and_lost_confirmation_keep_custody() {
    for stage in ["before-tombstone", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let (map, handle, context) =
                application_embedded::setup(&dir, 2, RouteMapValidity::Forever);
            let id = context.id();
            let admission = handle.admit_current_route().unwrap();
            let mut owner = sender(&map, &context, "uncertain", deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"))
                .into_unstarted_abandonment()
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let original = owner.installation_observation();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let mut reached = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                owner.test_record_primary_embedded_with_hook(&section, deadline(), |at| {
                    if at == stage {
                        reached = true;
                        assert!(!unwind, "injected abandonment unwind");
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "lost primary abandonment confirmation",
                        });
                    }
                    Ok(())
                })
            }));
            assert!(reached);
            if unwind {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(owner.observation(), O::Unconfirmed);
            assert_eq!(owner.installation_observation(), original);
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert!(pg.conn.is_autocommit());
            if stage == "after-commit" {
                assert_ne!(snapshot(&pg), before);
            } else {
                assert_eq!(snapshot(&pg), before);
            }
            let writes = pg.conn.total_changes();
            drop(pg);
            // A subsequent pre-send failure must not erase uncertainty.
            assert!(owner
                .record_primary_embedded(&section, Instant::now())
                .is_err());
            assert_eq!(owner.observation(), O::Unconfirmed);
            owner.record_primary_embedded(&section, deadline()).unwrap();
            assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
            assert_eq!(owner.installation_observation(), original);
            if stage == "after-commit" {
                assert_eq!(
                    node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                    writes
                );
            }
        }
    }
}

#[test]
fn staging_unstarted_abandonment_revalidates_captured_fence_at_effect_and_commit() {
    for stage in ["before-tombstone", "before-commit"] {
        let clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 3, RouteMapValidity::until_ms(20_000).unwrap());
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let mut owner = sender(&map, &context, "uncertain", deadline())
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
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
        let mut reached = false;
        let error = owner
            .test_record_primary_embedded_with_hook(&section, deadline(), |at| {
                if at == stage {
                    reached = true;
                    map.test_store_route_map_validity(RouteMapValidity::until_ms(40_000).unwrap());
                    clock.set(21_000);
                    map.require_route_map_valid_now().unwrap();
                }
                Ok(())
            })
            .unwrap_err();
        assert!(reached);
        assert!(matches!(
            error.into_store_error().unwrap(),
            StoreError::RouteMapExpired { .. }
        ));
        assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
        assert!(owner.record_primary_embedded(&section, deadline()).is_err());
        assert_eq!(owner.observation(), O::Unconfirmed);
    }
}

#[test]
fn staging_unstarted_abandonment_unconfirmed_rollback_retires_connection() {
    let dir = test_util::tempdir();
    let (map, handle, context) = application_embedded::setup(&dir, 3, RouteMapValidity::Forever);
    let id = context.id();
    let admission = handle.admit_current_route().unwrap();
    let mut owner = sender(&map, &context, "uncertain", deadline())
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
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    node.get_pg(id.pg_id().get())
        .unwrap()
        .test_fail_metadata_command_rollbacks(1);
    let mut reached = false;
    let error = owner
        .test_record_primary_embedded_with_hook(&section, deadline(), |at| {
            if at == "before-commit" {
                reached = true;
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "injected abandonment rollback failure",
                });
            }
            Ok(())
        })
        .unwrap_err();
    assert!(reached);
    assert!(error.to_string().contains("rollback failed"), "{error}");
    assert_eq!(owner.observation(), O::Unconfirmed);
    assert!(node.get_pg(id.pg_id().get()).is_err());
    assert!(owner.record_primary_embedded(&section, deadline()).is_err());
}

#[test]
fn staging_unstarted_abandonment_work_cutoff_reaches_sql_and_cannot_be_extended() {
    for stage in ["before-tombstone", "before-commit"] {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, 1, RouteMapValidity::Forever);
        let id = context.id();
        let admission = handle.admit_current_route().unwrap();
        let cutoff = Instant::now() + Duration::from_secs(1);
        let mut owner = sender(&map, &context, "uncertain", cutoff)
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
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        let mut reached = false;
        let error = owner
            .test_record_primary_embedded_with_hook(&section, deadline(), |at| {
                if at == stage {
                    reached = true;
                    assert!(Instant::now() < cutoff);
                    while let Some(left) = cutoff.checked_duration_since(Instant::now()) {
                        std::thread::sleep(left);
                    }
                }
                Ok(())
            })
            .unwrap_err();
        assert!(reached);
        assert!(matches!(
            error.into_store_error().unwrap(),
            StoreError::OperationDeadlineExceeded { .. }
        ));
        assert_eq!(owner.observation(), O::Unconfirmed);
        assert!(owner.record_primary_embedded(&section, deadline()).is_err());
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        if stage == "before-tombstone" {
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}
