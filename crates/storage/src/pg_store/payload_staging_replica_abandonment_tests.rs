// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only tombstone convergence; no cleanup/slot-retirement authority.

use super::super::complete_node_startup::Snapshot;
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::{
    CompleteUnstartedAbandonment, ReplicaAbandonmentObservation as R,
    UnstartedAbandonmentObservation as P,
};
use crate::LocalClusterMap;

fn with_owner(
    case: usize,
    origin: &str,
    validity: RouteMapValidity,
    cutoff: Instant,
    action: impl FnOnce(
        &LocalClusterMap,
        &ContextualCommand,
        &mut CompleteUnstartedAbandonment<'_>,
        &EmbeddedPublicationSection<'_>,
    ),
) {
    let dir = test_util::tempdir();
    let (map, handle, context) = application_embedded::setup(&dir, case, validity);
    let id = context.id();
    let admission = handle.admit_current_route().unwrap();
    let mut owner = unstarted_abandonment::sender(&map, &context, origin, cutoff)
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, error)| panic!("{error}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, error)| panic!("{error}"));
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    action(&map, &context, &mut owner, &section);
}

fn observe(map: &LocalClusterMap, context: &ContextualCommand) -> Vec<(Snapshot, u64)> {
    (0..5)
        .map(|n| {
            let node = map.node(NodeId::new(n)).unwrap().test_node();
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            (snapshot(&pg), pg.conn.total_changes())
        })
        .collect()
}

#[test]
fn staging_replica_abandonment_all_contexts_origins_preserve_pending_and_materialized_state() {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            with_owner(
                case,
                origin,
                RouteMapValidity::Forever,
                deadline(),
                |map, context, owner, section| {
                    let id = context.id();
                    let installation = owner.installation_observation();
                    let before = observe(map, context);
                    for n in [0, 4, 99] {
                        assert!(owner
                            .record_replica_embedded(section, NodeId::new(n), deadline())
                            .is_err());
                        assert_eq!(
                            owner.replica_observation(NodeId::new(n)),
                            R::NoAttemptObserved
                        );
                    }
                    assert_eq!(observe(map, context), before);
                    // An exact local pending slot is optional, but never removed.
                    install(
                        &map.node(NodeId::new(0))
                            .unwrap()
                            .test_node()
                            .get_pg(id.pg_id().get())
                            .unwrap(),
                        context,
                    );
                    owner.record_primary_embedded(section, deadline()).unwrap();
                    let primary = observe(map, context).remove(4);
                    let lock = map.runtime_state().metadata_command_pg_lock(id.pg_id());
                    for n in [0, 2, 1, 3] {
                        let node = map.node(NodeId::new(n)).unwrap().test_node();
                        let pg = node.get_pg(id.pg_id().get()).unwrap();
                        let retained = (introduction::state(&pg), complete_pending::observe(&pg));
                        let before = snapshot(&pg);
                        drop(pg);
                        assert!(lock.try_lock().is_none());
                        owner
                            .record_replica_embedded(section, NodeId::new(n), deadline())
                            .unwrap();
                        assert_eq!(
                            owner.replica_observation(NodeId::new(n)),
                            R::TombstoneConfirmed
                        );
                        let pg = node.get_pg(id.pg_id().get()).unwrap();
                        assert_ne!(snapshot(&pg), before);
                        assert_eq!(
                            (introduction::state(&pg), complete_pending::observe(&pg)),
                            retained
                        );
                        assert_eq!(
                            complete_maintenance::log_state(&pg),
                            complete_maintenance::log_state(
                                &map.node(NodeId::new(4))
                                    .unwrap()
                                    .test_node()
                                    .get_pg(id.pg_id().get())
                                    .unwrap()
                            )
                        );
                        drop(pg);
                        let confirmed = observe(map, context);
                        owner
                            .test_record_replica_embedded_with_hook(
                                section,
                                NodeId::new(n),
                                deadline(),
                                |_| panic!("confirmed replica redispatched"),
                            )
                            .unwrap();
                        assert_eq!(observe(map, context), confirmed);
                        assert_eq!(confirmed[4], primary);
                    }
                    assert_eq!(owner.installation_observation(), installation);
                    assert_eq!(owner.observation(), P::PrimaryTombstoneConfirmed);
                    assert!(lock.try_lock().is_none());
                },
            );
        }
    }
}

#[test]
fn staging_replica_abandonment_failures_unwind_and_lost_reply_keep_per_member_history() {
    for stage in ["before-tombstone", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            with_owner(
                3,
                "uncertain",
                RouteMapValidity::Forever,
                deadline(),
                |map, context, owner, section| {
                    owner.record_primary_embedded(section, deadline()).unwrap();
                    owner
                        .record_replica_embedded(section, NodeId::new(2), deadline())
                        .unwrap();
                    let before = observe(map, context);
                    let mut reached = false;
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        owner.test_record_replica_embedded_with_hook(
                            section,
                            NodeId::new(0),
                            deadline(),
                            |at| {
                                if at == stage {
                                    reached = true;
                                    assert!(!unwind, "injected replica abandonment unwind");
                                    return Err(StoreError::OperationDeadlineExceeded {
                                        context: "injected replica abandonment failure",
                                    });
                                }
                                Ok(())
                            },
                        )
                    }));
                    assert!(reached);
                    if unwind {
                        assert!(outcome.is_err());
                    } else {
                        assert!(outcome.unwrap().is_err());
                    }
                    assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
                    assert_eq!(
                        owner.replica_observation(NodeId::new(2)),
                        R::TombstoneConfirmed
                    );
                    let after = observe(map, context);
                    assert_eq!(&after[1..], &before[1..]);
                    if stage == "after-commit" {
                        assert_ne!(after[0].0, before[0].0);
                    } else {
                        assert_eq!(after[0].0, before[0].0);
                    }
                    assert!(owner
                        .record_replica_embedded(section, NodeId::new(0), Instant::now())
                        .is_err());
                    assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
                    owner
                        .record_replica_embedded(section, NodeId::new(0), deadline())
                        .unwrap();
                    assert_eq!(
                        owner.replica_observation(NodeId::new(0)),
                        R::TombstoneConfirmed
                    );
                    if stage == "after-commit" {
                        assert_eq!(observe(map, context), after);
                    }
                },
            );
        }
    }
}

#[test]
fn staging_replica_abandonment_rejects_applied_crossed_or_damaged_local_pending() {
    for damage in [
        "applied",
        "crossed-terminal",
        "scope",
        "null-scope",
        "started",
        "routes",
        "staging",
        "bytes",
        "orphan",
    ] {
        with_owner(
            1,
            "installed",
            RouteMapValidity::Forever,
            deadline(),
            |map, context, owner, section| {
                owner.record_primary_embedded(section, deadline()).unwrap();
                let node = map.node(NodeId::new(0)).unwrap().test_node();
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                if damage == "applied" {
                    apply(&pg, context);
                } else if damage == "crossed-terminal" {
                    let subject = fixture(StagingTarget::DirectPut).subject;
                    let crossed = ContextualCommand::decode_field(
                        crate::metadata_command::MetadataCommandEnvelope::new(
                            context.id(),
                            crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
                                crate::metadata_command::ReserveObjectVersionCommand::new(
                                    subject.bucket,
                                    subject.key,
                                    crate::VersionId::from_u64(99),
                                ),
                            ),
                        ),
                        &[1, 0, 0],
                        &topology(),
                    )
                    .unwrap();
                    assert_ne!(
                        crossed.encode_command().unwrap().command_sha256(),
                        context.encode_command().unwrap().command_sha256()
                    );
                    pg.test_record_complete_command_abandoned(
                        0,
                        &crossed.encode_command().unwrap(),
                    )
                    .unwrap();
                } else {
                    install(&pg, context);
                    pg.conn.execute_batch(match damage {
                    "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-replica'",
                    "null-scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                    "started" => "UPDATE metadata_command_pending_slot SET publication_started = 1",
                    "routes" => "DELETE FROM metadata_command_pending_route_dependencies",
                    "staging" => "DELETE FROM metadata_command_pending_staging_protection",
                    "bytes" => "UPDATE metadata_command_pending_slot SET command_bytes = zeroblob(length(command_bytes))",
                    "orphan" => "PRAGMA foreign_keys = OFF; DELETE FROM metadata_command_pending_slot; PRAGMA foreign_keys = ON;",
                    _ => unreachable!(),
                }).unwrap();
                }
                drop(pg);
                let before = observe(map, context);
                assert!(
                    owner
                        .test_record_replica_embedded_with_hook(
                            section,
                            NodeId::new(0),
                            deadline(),
                            |_| panic!("invalid replica reached effect")
                        )
                        .is_err(),
                    "{damage}"
                );
                assert_eq!(observe(map, context), before, "{damage}");
                assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
            },
        );
    }
}

#[test]
fn staging_replica_abandonment_captured_route_cutoff_rechecked_at_insert_and_commit() {
    for stage in ["before-tombstone", "before-commit"] {
        let clock = crate::clock::test_time_override_guard(10_000);
        with_owner(
            2,
            "uncertain",
            RouteMapValidity::until_ms(20_000).unwrap(),
            deadline(),
            |map, context, owner, section| {
                owner.record_primary_embedded(section, deadline()).unwrap();
                let before = observe(map, context);
                let mut reached = false;
                let error = owner
                    .test_record_replica_embedded_with_hook(
                        section,
                        NodeId::new(0),
                        deadline(),
                        |at| {
                            if at == stage {
                                reached = true;
                                map.test_store_route_map_validity(
                                    RouteMapValidity::until_ms(40_000).unwrap(),
                                );
                                clock.set(21_000);
                                map.require_route_map_valid_now().unwrap();
                            }
                            Ok(())
                        },
                    )
                    .unwrap_err();
                assert!(reached);
                assert!(matches!(
                    error.into_store_error().unwrap(),
                    StoreError::RouteMapExpired { .. }
                ));
                let after = observe(map, context);
                assert_eq!(
                    after.iter().map(|x| &x.0).collect::<Vec<_>>(),
                    before.iter().map(|x| &x.0).collect::<Vec<_>>()
                );
                if stage == "before-tombstone" {
                    assert_eq!(after, before);
                }
                assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
                assert!(owner
                    .record_replica_embedded(section, NodeId::new(0), deadline())
                    .is_err());
            },
        );
    }
}

#[test]
fn staging_replica_abandonment_work_cutoff_rechecked_at_insert_and_commit() {
    for stage in ["before-tombstone", "before-commit"] {
        let cutoff = Instant::now() + Duration::from_secs(2);
        with_owner(
            1,
            "uncertain",
            RouteMapValidity::Forever,
            cutoff,
            |map, context, owner, section| {
                owner.record_primary_embedded(section, deadline()).unwrap();
                let before = observe(map, context);
                let mut reached = false;
                let error = owner
                    .test_record_replica_embedded_with_hook(
                        section,
                        NodeId::new(0),
                        deadline(),
                        |at| {
                            if at == stage {
                                reached = true;
                                assert!(Instant::now() < cutoff);
                                while let Some(left) = cutoff.checked_duration_since(Instant::now())
                                {
                                    std::thread::sleep(left);
                                }
                            }
                            Ok(())
                        },
                    )
                    .unwrap_err();
                assert!(reached);
                assert!(matches!(
                    error.into_store_error().unwrap(),
                    StoreError::OperationDeadlineExceeded { .. }
                ));
                let after = observe(map, context);
                assert_eq!(
                    after.iter().map(|x| &x.0).collect::<Vec<_>>(),
                    before.iter().map(|x| &x.0).collect::<Vec<_>>()
                );
                if stage == "before-tombstone" {
                    assert_eq!(after, before);
                }
                assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
                assert!(owner
                    .record_replica_embedded(section, NodeId::new(0), deadline())
                    .is_err());
            },
        );
    }
}

#[test]
fn staging_replica_abandonment_rollback_failure_retires_only_the_replica_connection() {
    with_owner(
        3,
        "uncertain",
        RouteMapValidity::Forever,
        deadline(),
        |map, context, owner, section| {
            owner.record_primary_embedded(section, deadline()).unwrap();
            let node = map.node(NodeId::new(0)).unwrap().test_node();
            node.get_pg(context.id().pg_id().get())
                .unwrap()
                .test_fail_metadata_command_rollbacks(1);
            let mut reached = false;
            let error = owner
                .test_record_replica_embedded_with_hook(section, NodeId::new(0), deadline(), |at| {
                    if at == "before-commit" {
                        reached = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "replica rollback fault",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(
                reached && error.to_string().contains("rollback failed"),
                "{error}"
            );
            assert!(node.get_pg(context.id().pg_id().get()).is_err());
            assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
            owner
                .record_replica_embedded(section, NodeId::new(2), deadline())
                .unwrap();
            assert_eq!(
                owner.replica_observation(NodeId::new(2)),
                R::TombstoneConfirmed
            );
        },
    );
}

#[test]
fn staging_replica_abandonment_restart_reinspects_partial_tombstones_without_losing_pending_custody(
) {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, case, RouteMapValidity::Forever);
        let id = context.id();
        {
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
            owner
                .record_replica_embedded(&section, NodeId::new(2), deadline())
                .unwrap();
            let mut committed = false;
            owner
                .test_record_replica_embedded_with_hook(
                    &section,
                    NodeId::new(0),
                    deadline(),
                    |at| {
                        if at == "after-commit" {
                            committed = true;
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "replica reply lost before restart",
                            });
                        }
                        Ok(())
                    },
                )
                .unwrap_err();
            assert!(committed);
            assert_eq!(owner.replica_observation(NodeId::new(0)), R::Unconfirmed);
        }
        let before = observe(&map, &context);
        drop(handle);
        drop(map);
        let (map, handle) =
            embedded::embedded(&dir, case, PgState::Active, RouteMapValidity::Forever);
        assert_eq!(
            observe(&map, &context)
                .iter()
                .map(|x| &x.0)
                .collect::<Vec<_>>(),
            before.iter().map(|x| &x.0).collect::<Vec<_>>()
        );
        let admission = handle.admit_current_route().unwrap();
        let mut owner = unstarted_abandonment::sender(&map, &context, "inspected", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"))
            .into_unstarted_abandonment()
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        assert_eq!(
            owner.replica_observation(NodeId::new(0)),
            R::NoAttemptObserved
        );
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before = observe(&map, &context);
        owner.record_primary_embedded(&section, deadline()).unwrap();
        for n in [0, 2] {
            owner
                .record_replica_embedded(&section, NodeId::new(n), deadline())
                .unwrap();
        }
        assert_eq!(
            observe(&map, &context),
            before,
            "restart reinspection must not write"
        );
        for n in [1, 3] {
            owner
                .record_replica_embedded(&section, NodeId::new(n), deadline())
                .unwrap();
        }
        let primary = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = primary.get_pg(id.pg_id().get()).unwrap();
        assert!(pg
            .inspect_complete_pending_command(4, &topology())
            .unwrap()
            .is_some());
        let expected = complete_maintenance::log_state(&pg);
        drop(pg);
        for n in 0..4 {
            assert_eq!(
                owner.replica_observation(NodeId::new(n)),
                R::TombstoneConfirmed
            );
            let node = map.node(NodeId::new(n)).unwrap().test_node();
            assert_eq!(
                complete_maintenance::log_state(&node.get_pg(id.pg_id().get()).unwrap()),
                expected
            );
        }
    }
}
