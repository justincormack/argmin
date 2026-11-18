// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning embedded fanout through exact quiescent retirement, using the original
//! admission and the node-selected store. No raw retirement authority in callers.

use super::*;
use crate::cluster::StorageClusterRouteAdmission;
use crate::node_runtime::pg_store::{
    CompleteTerminalHandoff, TerminalSlotRetirementObservation as O,
};
use crate::{LocalClusterMap, StorageClusterRouteHandle};

struct Fixture<'a> {
    map: &'a LocalClusterMap,
    handle: &'a StorageClusterRouteHandle,
    admission: &'a StorageClusterRouteAdmission,
    context: &'a ContextualCommand,
    cutoff: Instant,
}

#[test]
fn staging_settled_prefix_after_embedded_publication_and_retirement() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    let _clock = crate::clock::test_time_override_guard(2_000);
    // Direct PUT introduction and a streamed append both retain a future
    // window. Unlike isolated apply tests, traverse the real primary slot,
    // publication trial/marker, witness-first fanout and owning retirement.
    for case in [1, 3] {
        let seed_dir = test_util::tempdir();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let seed = PgStore::open(seed_dir.path(), pg_id.get()).unwrap();
        if case == 1 {
            let digest = seed.metadata_command_replica_state().unwrap().state_digest;
            seed.initialize_metadata_transfer_empty_state(4, config(case).cluster_epoch, digest)
                .unwrap();
        }
        let context = context(&seed, case);
        let dir = test_util::tempdir();
        let (map, handle, context) = application_embedded::from_seed_without_pending(
            &dir,
            &seed,
            context,
            case,
            RouteMapValidity::Forever,
        );
        let id = context.id();
        let commits: Vec<_> = (0..5).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let startup_scans = Arc::new(AtomicUsize::new(0));
        for member in 0..5 {
            let node = map.node(NodeId::new(member)).unwrap().test_node();
            let mut pg = node.get_pg(pg_id.get()).unwrap();
            let expected = pg
                .recover_complete_command_state(
                    crate::node_runtime::pg_store::PgStoreRecoveryContext::for_node(NodeId::new(
                        member,
                    )),
                    &topology(),
                )
                .unwrap();
            let read = pg
                .inspect_complete_settled_prefix(member, id.cluster_epoch(), &topology())
                .unwrap();
            assert_eq!(*read.prefix(), expected);
            read.finish().unwrap();
            let commits = Arc::clone(&commits[member as usize]);
            let scans = Arc::clone(&startup_scans);
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(_, sql) = event {
                            if sql == "COMMIT" {
                                commits.fetch_add(1, Ordering::Relaxed);
                            }
                            if sql.contains("WHERE pg_id <>") {
                                scans.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }),
                )
                .unwrap();
        }

        let admission = handle.admit_current_route().unwrap();
        let mut fanout = fanout_embedded::sender(&map, &context, "installed", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(pg_id, id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        fanout.publish_embedded(&section, deadline()).unwrap();
        for member in [0, 4, 2, 3, 1] {
            fanout
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        let mut terminal = fanout
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        terminal.retire_embedded(&section, deadline()).unwrap();
        assert_eq!(terminal.slot_retirement_observation(), O::Retired);
        // No recovery or validation refresh is allowed between startup and
        // this observation. Settlement must work on an ordinary write path.
        let mut expected = None;
        for member in [0, 4] {
            let node = map.node(NodeId::new(member)).unwrap().test_node();
            let pg = node.get_pg(pg_id.get()).unwrap();
            assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
            let writes = pg.conn.total_changes();
            let read = pg
                .inspect_complete_settled_prefix(member, id.cluster_epoch(), &topology())
                .unwrap_or_else(|error| panic!("case {case}, member {member}: {error}"));
            let state = *read.prefix();
            read.finish().unwrap();
            assert_eq!(state.applied_log_index, id.log_index().get());
            if let Some(expected) = expected {
                assert_eq!(state, expected);
            } else {
                expected = Some(state);
            }
            assert_eq!(pg.conn.total_changes(), writes);
        }
        assert_eq!(startup_scans.load(Ordering::Relaxed), 0);
        for (member, commits) in commits.iter().enumerate() {
            assert_eq!(commits.load(Ordering::Relaxed), if member == 4 { 4 } else { 1 },
                "case {case}, member {member}: only existing install/publication/apply/retirement commits");
        }
    }
}

fn with_terminal(
    case: usize,
    history: &str,
    validity: RouteMapValidity,
    work_budget: Duration,
    action: impl FnOnce(&Fixture<'_>, &mut CompleteTerminalHandoff<'_>),
) {
    let dir = test_util::tempdir();
    let (map, handle, context) = application_embedded::setup(&dir, case, validity);
    let admission = handle.admit_current_route().unwrap();
    let cutoff = Instant::now() + work_budget;
    let mut fanout = fanout_embedded::sender(&map, &context, history, cutoff)
        .into_fanout(
            admission
                .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let original = fanout.installation_observation();
    let fixture = Fixture {
        map: &map,
        handle: &handle,
        admission: &admission,
        context: &context,
        cutoff,
    };
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    fanout.publish_embedded(&section, deadline()).unwrap();
    for node in [0, 4, 2, 3, 1] {
        fanout
            .apply_embedded(&section, NodeId::new(node), deadline())
            .unwrap();
    }
    drop(section);
    let mut terminal = fanout
        .into_terminal()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
    action(&fixture, &mut terminal);
    assert_eq!(terminal.installation_observation(), original);
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let observe = || {
        node.get_pg(context.id().pg_id().get())
            .map(|pg| (snapshot(&pg), pg.conn.total_changes()))
            .map_err(|e| e.to_string())
    };
    let before = observe();
    drop(terminal);
    assert_eq!(
        observe(),
        before,
        "Drop must not retry retirement or release resources"
    );
}

#[test]
fn staging_retirement_embedded_preserves_owners_and_custody_and_never_redispatches() {
    for case in [0, 1, 3] {
        for history in ["inspected", "installed", "uncertain"] {
            with_terminal(
                case,
                history,
                RouteMapValidity::Forever,
                Duration::from_secs(60),
                |f, terminal| {
                    let route = f
                        .admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let section = route.lock_until(deadline()).unwrap();
                    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    let retained = (
                        introduction::state(&pg),
                        complete_maintenance::log_state(&pg),
                        pg.metadata_command_replica_state().unwrap(),
                    );
                    assert!(!complete_pending::observe(&pg)[0].is_empty());
                    drop(pg);
                    terminal.retire_embedded(&section, deadline()).unwrap();
                    assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                    assert_eq!(
                        (
                            introduction::state(&pg),
                            complete_maintenance::log_state(&pg),
                            pg.metadata_command_replica_state().unwrap()
                        ),
                        retained
                    );
                    // A later slot must never be removed by a previously confirmed holder.
                    let next = complete_node_startup::next(&pg, f.context.id().cluster_epoch());
                    install(&pg, &next);
                    let after = (snapshot(&pg), pg.conn.total_changes());
                    drop(pg);
                    terminal
                        .test_retire_embedded_with_hook(&section, deadline(), |_| {
                            panic!("confirmed retirement redispatched")
                        })
                        .unwrap();
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
                },
            );
        }
    }
}

#[test]
fn staging_retirement_embedded_rejects_nonquiescent_and_crossed_admission_before_effects() {
    for case in [0, 2] {
        with_terminal(
            case,
            "uncertain",
            RouteMapValidity::Forever,
            Duration::from_secs(60),
            |f, terminal| {
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let before = snapshot(&node.get_pg(f.context.id().pg_id().get()).unwrap());
                let other_dir = test_util::tempdir();
                let (other_map, other_handle, _) =
                    application_embedded::setup(&other_dir, case, RouteMapValidity::Forever);
                let foreign = other_handle.admit_current_route().unwrap();
                let duplicate = f.handle.admit_current_route().unwrap();
                let other_node = other_map.node(NodeId::new(4)).unwrap().test_node();
                let other_before =
                    snapshot(&other_node.get_pg(f.context.id().pg_id().get()).unwrap());
                for (admission, pg) in [
                    (&foreign, f.context.id().pg_id()),
                    (&duplicate, f.context.id().pg_id()),
                    (
                        f.admission,
                        PgId::new((f.context.id().pg_id().get() + 1) % 8),
                    ),
                ] {
                    let route = admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            pg,
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let section = route.lock_until(deadline()).unwrap();
                    let error = terminal
                        .test_retire_embedded_with_hook(&section, deadline(), |_| {
                            panic!("crossed section reached transaction")
                        })
                        .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("original admitted primary section"),
                        "{error}"
                    );
                    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                }
                assert_eq!(
                    snapshot(&other_node.get_pg(f.context.id().pg_id().get()).unwrap()),
                    other_before
                );
                assert_eq!(
                    snapshot(&node.get_pg(f.context.id().pg_id().get()).unwrap()),
                    before
                );
                let route = f
                    .admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        f.context.id().pg_id(),
                        f.context.id().cluster_epoch(),
                    )
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                if case == 2 {
                    let error = terminal
                        .test_retire_embedded_with_hook(&section, deadline(), |_| {
                            panic!("nonquiescent work reached transaction")
                        })
                        .unwrap_err();
                    assert!(
                        error.to_string().contains("terminal work integration"),
                        "{error}"
                    );
                    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                    assert_eq!(
                        snapshot(&node.get_pg(f.context.id().pg_id().get()).unwrap()),
                        before
                    );
                } else {
                    terminal.retire_embedded(&section, deadline()).unwrap();
                }
            },
        );
    }
}

#[test]
fn staging_retirement_embedded_uncertainty_survives_failure_unwind_and_compacted_retry() {
    for stage in ["before-delete", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            with_terminal(
                3,
                "uncertain",
                RouteMapValidity::Forever,
                Duration::from_secs(60),
                |f, terminal| {
                    let route = f
                        .admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let section = route.lock_until(deadline()).unwrap();
                    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                    let before = snapshot(&node.get_pg(f.context.id().pg_id().get()).unwrap());
                    let mut reached = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        terminal.test_retire_embedded_with_hook(&section, deadline(), |at| {
                            if at == stage {
                                reached = true;
                                if unwind {
                                    panic!("injected embedded retirement unwind");
                                }
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "injected embedded retirement failure",
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
                            .contains("injected embedded retirement failure"));
                    }
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    let confirmed_state = if stage == "after-commit" {
                        assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                        pg.record_complete_metadata_command_checkpoint(4, &topology())
                            .unwrap();
                        pg.compact_complete_metadata_command_log(&topology())
                            .unwrap();
                        assert!(complete_maintenance::log_state(&pg)[0].is_empty());
                        Some((snapshot(&pg), pg.conn.total_changes()))
                    } else {
                        assert_eq!(snapshot(&pg), before);
                        None
                    };
                    drop(pg);
                    assert!(terminal.retire_embedded(&section, Instant::now()).is_err());
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    terminal.retire_embedded(&section, deadline()).unwrap();
                    assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                    if let Some(expected) = confirmed_state {
                        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                        assert_eq!(
                            (snapshot(&pg), pg.conn.total_changes()),
                            expected,
                            "absence confirmation must not mutate"
                        );
                    }
                },
            );
        }
    }
}

#[test]
fn staging_retirement_embedded_revalidates_captured_route_work_attempt_and_section_cutoffs() {
    for kind in ["route", "work", "attempt", "section"] {
        for stage in ["before-delete", "before-commit"] {
            let clock = crate::clock::test_time_override_guard(10_000);
            let validity = if kind == "route" {
                RouteMapValidity::until_ms(20_000).unwrap()
            } else {
                RouteMapValidity::Forever
            };
            with_terminal(
                0,
                "installed",
                validity,
                Duration::from_secs(60),
                |f, terminal| {
                    let route = f
                        .admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let local_cutoff = Instant::now() + Duration::from_secs(1);
                    let section = route
                        .lock_until(if kind == "section" {
                            local_cutoff
                        } else {
                            deadline()
                        })
                        .unwrap();
                    let attempt = if kind == "attempt" {
                        local_cutoff
                    } else {
                        deadline()
                    };
                    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                    let before = snapshot(&node.get_pg(f.context.id().pg_id().get()).unwrap());
                    let writes = node
                        .get_pg(f.context.id().pg_id().get())
                        .unwrap()
                        .conn
                        .total_changes();
                    let mut reached = false;
                    let error = terminal
                        .test_retire_embedded_with_hook(&section, attempt, |at| {
                            if stage == "before-delete" && at == "before-commit" {
                                panic!("expired authority reached deletion");
                            }
                            if at == stage {
                                reached = true;
                                match kind {
                                    "route" => {
                                        f.map.test_store_route_map_validity(
                                            RouteMapValidity::until_ms(100_000).unwrap(),
                                        );
                                        clock.set(21_000);
                                        f.map.require_route_map_valid_now().unwrap();
                                    }
                                    "work" => {
                                        assert!(Instant::now() < f.cutoff);
                                        // Expire the original absolute work bound, not a newly
                                        // supplied attempt. No real minute-long sleep required.
                                        clock.set(71_000);
                                    }
                                    "attempt" | "section" => {
                                        assert!(
                                            Instant::now() < local_cutoff,
                                            "missed intended interleaving"
                                        );
                                        while let Some(remaining) =
                                            local_cutoff.checked_duration_since(Instant::now())
                                        {
                                            std::thread::sleep(remaining);
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            Ok(())
                        })
                        .unwrap_err();
                    assert!(reached);
                    assert!(
                        error.to_string().contains("deadline")
                            || error.to_string().contains("expired")
                            || error.to_string().contains("cutoff"),
                        "{error}"
                    );
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert_eq!(snapshot(&pg), before);
                    if stage == "before-delete" {
                        assert_eq!(pg.conn.total_changes(), writes);
                    }
                    drop(pg);
                    if kind == "route" || kind == "work" {
                        assert!(terminal.retire_embedded(&section, deadline()).is_err());
                        assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    }
                },
            );
        }
    }
}

#[test]
fn staging_retirement_embedded_damaged_exact_slot_fails_closed() {
    for damage in [
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-bucket'",
        "UPDATE metadata_command_pending_slot SET publication_started = 0",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "PRAGMA foreign_keys = OFF; DELETE FROM metadata_command_pending_slot; PRAGMA foreign_keys = ON",
        "DELETE FROM metadata_command_log; DELETE FROM metadata_command_terminal_receipts",
    ] {
        with_terminal(3, "uncertain", RouteMapValidity::Forever, Duration::from_secs(60), |f, terminal| {
            let route = f.admission.embedded_publication_route(NodeId::new(4), f.context.id().pg_id(), f.context.id().cluster_epoch()).unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            pg.conn.execute_batch(damage).unwrap();
            let before = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            assert!(terminal.test_retire_embedded_with_hook(&section, deadline(), |_| panic!("invalid slot reached deletion")).is_err(), "{damage}");
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
        });
    }
}

#[test]
fn staging_retirement_embedded_sql_commit_failure_keeps_exact_retry_owner() {
    with_terminal(
        3,
        "uncertain",
        RouteMapValidity::Forever,
        Duration::from_secs(60),
        |f, terminal| {
            let route = f
                .admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            pg.conn.execute_batch("CREATE TEMP TABLE retirement_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES retirement_commit_fault(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_retirement AFTER DELETE ON metadata_command_pending_slot BEGIN INSERT INTO retirement_commit_fault VALUES (1, 2); END;").unwrap();
            let before = snapshot(&pg);
            drop(pg);
            let mut deleted = false;
            let error = terminal
                .test_retire_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "before-commit" {
                        deleted = true;
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(deleted);
            assert!(
                error
                    .to_string()
                    .contains("complete slot retirement commit"),
                "{error}"
            );
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            assert!(pg.conn.is_autocommit());
            assert_eq!(snapshot(&pg), before);
            pg.conn
                .execute_batch("DROP TRIGGER fail_retirement; DROP TABLE retirement_commit_fault")
                .unwrap();
            drop(pg);
            terminal.retire_embedded(&section, deadline()).unwrap();
            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
        },
    );
}

#[test]
fn staging_retirement_embedded_bounds_store_wait_without_losing_custody() {
    with_terminal(
        0,
        "uncertain",
        RouteMapValidity::Forever,
        Duration::from_secs(60),
        |f, terminal| {
            let route = f
                .admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            let before = (snapshot(&pg), pg.conn.total_changes());
            let error = terminal
                .retire_embedded(&section, Instant::now() + Duration::from_millis(30))
                .unwrap_err();
            assert!(error.to_string().contains("deadline"), "{error}");
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
            drop(pg);
            terminal.retire_embedded(&section, deadline()).unwrap();
            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
        },
    );
}

#[test]
fn staging_retirement_embedded_unconfirmed_rollback_retires_connection() {
    with_terminal(
        3,
        "uncertain",
        RouteMapValidity::Forever,
        Duration::from_secs(60),
        |f, terminal| {
            let route = f
                .admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            node.get_pg(f.context.id().pg_id().get())
                .unwrap()
                .test_fail_metadata_command_rollbacks(1);
            let mut reached = false;
            let error = terminal
                .test_retire_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "before-commit" {
                        reached = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected embedded retirement rollback failure",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(reached);
            assert!(error.to_string().contains("rollback failed"), "{error}");
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            assert!(node.get_pg(f.context.id().pg_id().get()).is_err());
            assert!(terminal.retire_embedded(&section, deadline()).is_err());
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
        },
    );
}

#[test]
fn staging_retirement_embedded_expiry_cannot_confirm_absence_after_lost_reply() {
    let clock = crate::clock::test_time_override_guard(10_000);
    with_terminal(
        3,
        "uncertain",
        RouteMapValidity::Forever,
        Duration::from_secs(60),
        |f, terminal| {
            let route = f
                .admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let error = terminal
                .test_retire_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "after-commit" {
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "lost embedded retirement confirmation",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("lost embedded retirement confirmation"));
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
            let before = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            let mut reached = false;
            let error = terminal
                .test_retire_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "before-delete" {
                        reached = true;
                        clock.set(71_000);
                    }
                    assert_ne!(stage, "before-commit", "expired absence reached commit");
                    Ok(())
                })
                .unwrap_err();
            assert!(reached);
            assert!(error.to_string().contains("cutoff"), "{error}");
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
        },
    );
}
