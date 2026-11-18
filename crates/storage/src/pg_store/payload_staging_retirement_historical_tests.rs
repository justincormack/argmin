// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Historical all-member custody through quiescent retirement. Metadata-only:
//! removing a pending slot never removes a retained owner or authorizes cleanup.

use super::super::super::super::complete_node_startup;
use super::*;
use crate::cluster::StorageClusterRouteAdmission;
use crate::node_runtime::pg_store::{
    CompleteRetirementSendError, CompleteTerminalHandoff, TerminalSlotRetirementObservation as O,
};

#[path = "payload_staging_retirement_historical_receiver_tests.rs"]
mod receiver;

pub(super) fn with_terminal(
    mixed: &Mixed,
    origin: &str,
    cutoff: Instant,
    action: impl FnOnce(&StorageClusterRouteAdmission, &mut CompleteTerminalHandoff<'_>),
) {
    let f = &mixed.fixture;
    let admission = f.handle.admit_current_route().unwrap();
    let id = f.context.id();
    let owner = mixed.owner(origin, cutoff);
    let mut owner = owner
        .into_fanout(
            admission
                .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let original = owner.installation_observation();
    {
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
        let route = remote.is_none().then(|| {
            admission
                .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                .unwrap()
        });
        let section = route.as_ref().map(|r| r.lock_until(deadline()).unwrap());
        let session = remote.map(|p| owner.open_primary_session(&p.client, deadline()).unwrap());
        if let Some(section) = &section {
            owner.publish_embedded(section, deadline()).unwrap();
        } else {
            owner
                .publish(
                    &remote.unwrap().client,
                    session.as_ref().unwrap(),
                    deadline(),
                )
                .unwrap();
        }
        for node in [0, 4, 2] {
            mixed
                .apply(&mut owner, section.as_ref(), session.as_ref(), node)
                .unwrap();
            if node != 2 {
                owner = owner
                    .into_terminal()
                    .err()
                    .expect("all peers are required")
                    .into_fanout();
                assert_eq!(owner.installation_observation(), original);
            }
        }
    }
    let before = mixed.observe();
    let mut terminal = owner
        .into_terminal()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    assert_eq!(mixed.observe(), before, "handoff has no effects");
    assert_eq!(terminal.installation_observation(), original);
    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
    action(&admission, &mut terminal);
    assert_eq!(terminal.installation_observation(), original);
    for node in [0, 4, 2] {
        assert_eq!(
            terminal.application_observation(NodeId::new(node)),
            Applied::Applied
        );
    }
    let before = mixed.observe();
    drop(terminal);
    assert_eq!(
        mixed.observe(),
        before,
        "no implicit retry or cleanup on Drop"
    );
}

fn retained(
    pg: &PgStore,
) -> (
    Vec<crate::node_runtime::pg_store::MetadataCheckpointTableBlock>,
    Vec<Vec<String>>,
    crate::metadata_command::MetadataCommandReplicaState,
) {
    (
        introduction::state(pg),
        complete_maintenance::log_state(pg),
        pg.metadata_command_replica_state().unwrap(),
    )
}

fn matrix(primary: &str) {
    for case in [0, 1, 3] {
        for origin in ["inspected", "installed", "uncertain"] {
            let endpoints = match primary {
                "embedded" => vec![(0, false), (2, true)],
                "unix" => vec![(4, false), (2, true)],
                "tls" => vec![(4, true), (0, false)],
                _ => unreachable!(),
            };
            let mixed = Mixed::new(case, &endpoints);
            with_terminal(&mixed, origin, deadline(), |admission, terminal| {
                assert!(terminal.work().is_quiescent());
                let f = &mixed.fixture;
                let id = f.context.id();
                let node = mixed.node(4);
                let retained_before = retained(&node.get_pg(id.pg_id().get()).unwrap());
                let before = mixed.observe();
                let shadows = mixed.shadows();
                let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
                let _running = remote.map(Remote::run);
                let route = remote.is_none().then(|| {
                    admission
                        .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                        .unwrap()
                });
                let section = route.as_ref().map(|r| r.lock_until(deadline()).unwrap());
                let session = remote.map(|p| {
                    terminal
                        .open_primary_session(&p.client, deadline())
                        .unwrap()
                });
                let retire = |terminal: &mut CompleteTerminalHandoff<'_>| {
                    if let Some(section) = &section {
                        terminal.retire_embedded(section, deadline())
                    } else {
                        terminal.retire_remote(
                            &remote.unwrap().client,
                            session.as_ref().unwrap(),
                            deadline(),
                        )
                    }
                };
                retire(terminal).unwrap();
                assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                assert_eq!(
                    retained(&pg),
                    retained_before,
                    "owners/resources/manifests/log unchanged"
                );
                // Confirmed custody must not dispatch against a subsequent slot.
                let next = complete_node_startup::next(&pg, id.cluster_epoch());
                install(&pg, &next);
                drop(pg);
                let after = mixed.observe();
                retire(terminal).unwrap();
                assert_eq!(mixed.observe(), after);
                assert_eq!(mixed.shadows(), shadows);
                for node in [0, 1, 2, 3] {
                    assert_eq!(after[node], before[node]);
                }
            });
        }
    }
}

#[test]
fn staging_retirement_historical_embedded_primary_mixed_peers_keeps_owners_and_custody() {
    matrix("embedded");
}
#[test]
fn staging_retirement_historical_unix_primary_mixed_peers_keeps_owners_and_custody() {
    matrix("unix");
}
#[test]
fn staging_retirement_historical_tls_primary_mixed_peers_keeps_owners_and_custody() {
    matrix("tls");
}

#[test]
fn staging_retirement_historical_embedded_failed_or_lost_commit_preserves_retry() {
    for boundary in ["before-delete", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            let mixed = Mixed::new(1, &[]);
            with_terminal(&mixed, "uncertain", deadline(), |admission, terminal| {
                let f = &mixed.fixture;
                let route = admission
                    .embedded_historical_publication_route(&f.runtime_map, f.context.id().pg_id())
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                let before = mixed.observe();
                let node = mixed.node(4);
                let state = retained(&node.get_pg(f.context.id().pg_id().get()).unwrap());
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    terminal.test_retire_embedded_with_hook(&section, deadline(), |at| {
                        if at == boundary {
                            reached = true;
                            if unwind {
                                panic!("injected historical retirement unwind");
                            }
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "injected historical retirement failure",
                            });
                        }
                        Ok(())
                    })
                }));
                // Do not infer terminal success from missing pending metadata.
                assert!(reached);
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                for (index, ((after, _), (before, _))) in
                    mixed.observe().iter().zip(&before).enumerate()
                {
                    if boundary != "after-commit" || index != 4 {
                        assert_eq!(after, before);
                    }
                }
                assert_eq!(
                    retained(&node.get_pg(f.context.id().pg_id().get()).unwrap()),
                    state
                );
                terminal.retire_embedded(&section, deadline()).unwrap();
                assert_eq!(terminal.slot_retirement_observation(), O::Retired);
            });
        }
    }
}

#[test]
fn staging_retirement_historical_embedded_captured_cutoffs_at_delete_and_commit() {
    for kind in ["route", "work", "attempt", "section"] {
        for boundary in ["before-delete", "before-commit"] {
            let clock = crate::clock::test_time_override_guard(10_000);
            let mixed = Mixed::new(0, &[]);
            let work_cutoff = if kind == "work" {
                Instant::now() + Duration::from_secs(5)
            } else {
                deadline()
            };
            with_terminal(&mixed, "uncertain", work_cutoff, |admission, terminal| {
                let f = &mixed.fixture;
                let route = admission
                    .embedded_historical_publication_route(&f.runtime_map, f.context.id().pg_id())
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
                let before = mixed.observe();
                let mut reached = false;
                let error = terminal
                    .test_retire_embedded_with_hook(&section, attempt, |at| {
                        if boundary == "before-delete" && at == "before-commit" {
                            panic!("expired historical authority reached deletion");
                        }
                        if at == boundary {
                            reached = true;
                            if kind == "route" {
                                f.map.test_store_route_map_validity(
                                    RouteMapValidity::until_ms(40_000).unwrap(),
                                );
                                clock.set(21_000);
                                f.map.require_route_map_valid_now().unwrap();
                                admission.require_valid_now().unwrap();
                            } else if kind == "work" {
                                assert!(Instant::now() < work_cutoff);
                                // Expire only the original absolute work cutoff;
                                // the captured route remains valid until 20_000.
                                clock.set(16_000);
                                admission.require_valid_now().unwrap();
                            } else {
                                assert!(
                                    Instant::now() < local_cutoff,
                                    "missed {kind}/{boundary} interleaving"
                                );
                                while let Some(remaining) =
                                    local_cutoff.checked_duration_since(Instant::now())
                                {
                                    std::thread::sleep(remaining);
                                }
                            }
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached, "{kind}/{boundary}: {error}");
                assert!(
                    error.to_string().contains("deadline")
                        || error.to_string().contains("expired")
                        || error.to_string().contains("cutoff"),
                    "{error}"
                );
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                for ((after, writes), (before, previous_writes)) in
                    mixed.observe().iter().zip(&before)
                {
                    assert_eq!(after, before);
                    if boundary == "before-delete" {
                        assert_eq!(writes, previous_writes);
                    }
                }
                // Neither a renewed raw map nor a fresh attempt can renew the
                // original admission/work bound retained by terminal custody.
                if kind == "route" || kind == "work" {
                    let before = mixed.observe();
                    assert!(terminal.retire_embedded(&section, deadline()).is_err());
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    assert_eq!(mixed.observe(), before);
                }
            });
        }
    }
}

#[test]
fn staging_retirement_historical_remote_lost_reply_reopens_same_custody() {
    for tls in [false, true] {
        let mixed = Mixed::new(1, &[(4, tls), (0, !tls)]);
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            let primary = mixed.peer(4);
            let id = mixed.fixture.context.id();
            let count = primary.count(Kind::MetadataCommandPendingSlotRemove);
            let node = mixed.node(4);
            let state = retained(&node.get_pg(id.pg_id().get()).unwrap());
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    primary
                        .server
                        .test_accept_one_retirement_fault(false, true)
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(primary));
                let session = terminal
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                assert!(matches!(
                    terminal.retire_remote(&primary.client, &session, deadline()),
                    Err(CompleteRetirementSendError::Unconfirmed(_))
                ));
                assert_eq!(count.load(Ordering::SeqCst), 1);
                wake.disarm();
                drop(session);
                worker.join().unwrap();
            });
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            assert_eq!(retained(&node.get_pg(id.pg_id().get()).unwrap()), state);
            assert!(
                complete_pending::observe(&node.get_pg(id.pg_id().get()).unwrap())
                    .iter()
                    .all(Vec::is_empty)
            );
            let before = mixed.observe();
            let _running = primary.run();
            let session = terminal
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            terminal
                .retire_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
            assert_eq!(
                mixed.observe(),
                before,
                "absence confirmation must not write"
            );
            assert_eq!(count.load(Ordering::SeqCst), 2);
            terminal
                .retire_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(count.load(Ordering::SeqCst), 2);
        });
    }
}
