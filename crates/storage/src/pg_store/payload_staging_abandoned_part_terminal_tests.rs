// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming source/cleanup handoff, not resource release or payload deletion.
//! Real embedded/Unix/TLS application over metadata-only owner fixtures.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::payload_staging::{
    ContextSubmissionObservation, TerminalPayloadWork, TerminalReservationWork,
};
use crate::node_runtime::pg_store::{
    AbandonedPartInstallationObservation as I, AbandonedPartTerminalHandoff,
    ApplicationObservation as A, PreparedAbandonedPart, PublicationObservation as P,
};
use crate::LocalClusterMap;

fn failure() -> StoreError {
    StoreError::OperationDeadlineExceeded {
        context: "injected terminal-handoff predecessor loss",
    }
}

fn with_prepared(
    count: usize,
    origin: &str,
    validity: RouteMapValidity,
    work: Option<Duration>,
    action: impl FnOnce(
        &LocalClusterMap,
        &ContextualCommand,
        &ContextualCommand,
        PreparedAbandonedPart<'_>,
        &EmbeddedPublicationSection<'_>,
        Instant,
    ),
) {
    let dir = test_util::tempdir();
    let (map, handle, source, cleanup) = abandonment_preparation::setup(&dir, count);
    map.test_store_route_map_validity(validity);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let cutoff = work.map_or_else(deadline, |work| Instant::now() + work);
    let owner =
        abandonment_preparation::handoff(&map, &admission, &section, &source, origin, cutoff)
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
    action(&map, &source, &cleanup, owner, &section, cutoff);
}

fn observe(
    map: &LocalClusterMap,
    pg: PgId,
) -> Vec<(super::super::complete_node_startup::Snapshot, u64)> {
    [4, 2, 0, 3, 1]
        .into_iter()
        .map(|id| {
            let node = map.node(NodeId::new(id)).unwrap().test_node();
            let pg = node.get_pg(pg.get()).unwrap();
            (snapshot(&pg), pg.conn.total_changes())
        })
        .collect()
}

fn apply_all(owner: &mut PreparedAbandonedPart<'_>, section: &EmbeddedPublicationSection<'_>) {
    owner.install_cleanup(section).unwrap();
    owner.publish_cleanup(section).unwrap();
    for id in [0, 4, 2, 3, 1] {
        owner
            .apply_cleanup_embedded(section, NodeId::new(id))
            .unwrap();
    }
}

fn assert_terminal(
    terminal: &AbandonedPartTerminalHandoff<'_>,
    source: &ContextualCommand,
    cleanup: &ContextualCommand,
    origin: Option<ContextSubmissionObservation>,
    count: usize,
) {
    terminal.require_valid().unwrap();
    assert_eq!(terminal.source_id(), source.id());
    assert_eq!(terminal.cleanup_id(), cleanup.id());
    assert_ne!(terminal.source_id(), terminal.cleanup_id());
    terminal.assert_custody_for_test(source, cleanup, &topology(), origin);
    for id in [4, 2, 0, 3, 1] {
        assert_eq!(terminal.cleanup_application(NodeId::new(id)), A::Applied);
    }
    assert_eq!(
        terminal.cleanup_application(NodeId::new(99)),
        A::NoAttemptObserved
    );
    let work = terminal.work();
    assert_eq!(
        work.payload,
        TerminalPayloadWork::StreamAbort { segments: count }
    );
    assert_eq!(work.reservation, TerminalReservationWork::NoCommandProof);
    assert!(work.retained_finalization_proof);
    assert!(!work.durable_reclaim_handoff);
    assert!(
        !work.is_quiescent(),
        "even empty aborts retain resource responsibilities"
    );
}

#[test]
fn staging_abandoned_part_terminal_all_origins_require_every_original_member_without_effects() {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            with_prepared(
                count,
                origin,
                RouteMapValidity::Forever,
                None,
                |map, source, cleanup, mut owner, section, _| {
                    let history = owner.installation_observation();
                    owner.install_cleanup(section).unwrap();
                    owner.publish_cleanup(section).unwrap();
                    for (confirmed, id) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                        let before = observe(map, source.id().pg_id());
                        let failure = owner
                            .into_terminal()
                            .err()
                            .expect("incomplete cohort handed off");
                        assert_eq!(
                            failure.error().to_string(),
                            format!(
                                "complete fanout has confirmed {confirmed} of 5 required peers"
                            )
                        );
                        owner = failure.into_prepared();
                        owner.assert_contexts_for_test(source, cleanup, &topology());
                        assert_eq!(owner.installation_observation(), history);
                        assert_eq!(observe(map, source.id().pg_id()), before);
                        owner
                            .apply_cleanup_embedded(section, NodeId::new(id))
                            .unwrap();
                    }
                    let before = observe(map, source.id().pg_id());
                    let terminal = owner
                        .into_terminal()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_terminal(&terminal, source, cleanup, history, count);
                    let custody = terminal.inspect_retained_custody(section).unwrap();
                    assert_eq!(custody.retained_windows, count + 1);
                    assert_eq!(custody.selected_references, 0);
                    assert_eq!(custody.additional_finalizers, 0);
                    assert_eq!(observe(map, source.id().pg_id()), before);
                    drop(terminal);
                    assert_eq!(
                        observe(map, source.id().pg_id()),
                        before,
                        "Drop performed terminal effects"
                    );
                    assert!(started(
                        &map.node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(source.id().pg_id().get())
                            .unwrap()
                    ));
                },
            );
        }
    }
}

#[test]
fn staging_abandoned_part_terminal_unconfirmed_installation_and_publication_return_same_owner() {
    with_prepared(
        2,
        "uncertain",
        RouteMapValidity::Forever,
        None,
        |map, source, cleanup, mut owner, section, _| {
            let history = owner.installation_observation();
            for attempted in [false, true] {
                if attempted {
                    owner
                        .test_install_cleanup(section, |at| {
                            if at == "after-commit" {
                                Err(failure())
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_err();
                }
                let before = observe(map, source.id().pg_id());
                let failed = owner
                    .into_terminal()
                    .err()
                    .expect("unconfirmed installation handed off");
                assert!(failed
                    .error()
                    .to_string()
                    .contains("confirmed installation"));
                owner = failed.into_prepared();
                assert_eq!(
                    owner.cleanup_installation(),
                    if attempted {
                        I::Unconfirmed
                    } else {
                        I::NoAttemptObserved
                    }
                );
                assert_eq!(owner.installation_observation(), history);
                owner.assert_contexts_for_test(source, cleanup, &topology());
                assert_eq!(observe(map, source.id().pg_id()), before);
            }
            owner.install_cleanup(section).unwrap();
            for attempted in [false, true] {
                if attempted {
                    owner
                        .test_publish_cleanup(section, |at| {
                            if at == "after-commit" {
                                Err(failure())
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_err();
                }
                let before = observe(map, source.id().pg_id());
                let failed = owner
                    .into_terminal()
                    .err()
                    .expect("unconfirmed publication handed off");
                assert!(failed
                    .error()
                    .to_string()
                    .contains("marker is not confirmed"));
                owner = failed.into_prepared();
                assert_eq!(
                    owner.cleanup_publication(),
                    if attempted {
                        P::Unconfirmed
                    } else {
                        P::NoAttemptObserved
                    }
                );
                assert_eq!(owner.cleanup_installation(), I::Installed);
                assert_eq!(owner.installation_observation(), history);
                owner.assert_contexts_for_test(source, cleanup, &topology());
                assert_eq!(observe(map, source.id().pg_id()), before);
            }
            apply_all(&mut owner, section);
            let before = observe(map, source.id().pg_id());
            let terminal = owner
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_terminal(&terminal, source, cleanup, history, 2);
            drop(terminal);
            assert_eq!(observe(map, source.id().pg_id()), before);
        },
    );
}

#[test]
fn staging_abandoned_part_terminal_lost_apply_reply_never_counts_as_confirmation() {
    for lost in [0, 4, 2, 3, 1] {
        with_prepared(
            2,
            "uncertain",
            RouteMapValidity::Forever,
            None,
            |map, source, cleanup, mut owner, section, _| {
                let history = owner.installation_observation();
                owner.install_cleanup(section).unwrap();
                owner.publish_cleanup(section).unwrap();
                for (confirmed, id) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                    if id == lost {
                        let mut reached = false;
                        owner
                            .test_apply_cleanup_embedded(section, NodeId::new(id), |at| {
                                if at == "after-commit" {
                                    reached = true;
                                    Err(failure())
                                } else {
                                    Ok(())
                                }
                            })
                            .unwrap_err();
                        assert!(reached);
                        assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Unconfirmed);
                        let before = observe(map, source.id().pg_id());
                        let failed = owner
                            .into_terminal()
                            .err()
                            .expect("lost reply authorized handoff");
                        assert_eq!(
                            failed.error().to_string(),
                            format!(
                                "complete fanout has confirmed {confirmed} of 5 required peers"
                            )
                        );
                        owner = failed.into_prepared();
                        assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Unconfirmed);
                        assert_eq!(owner.installation_observation(), history);
                        owner.assert_contexts_for_test(source, cleanup, &topology());
                        assert_eq!(observe(map, source.id().pg_id()), before);
                        owner
                            .apply_cleanup_embedded(section, NodeId::new(id))
                            .unwrap();
                        assert_eq!(
                            observe(map, source.id().pg_id()),
                            before,
                            "exact retry repeated transfers"
                        );
                    } else {
                        owner
                            .apply_cleanup_embedded(section, NodeId::new(id))
                            .unwrap();
                    }
                }
                let before = observe(map, source.id().pg_id());
                let terminal = owner
                    .into_terminal()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_terminal(&terminal, source, cleanup, history, 2);
                drop(terminal);
                assert_eq!(observe(map, source.id().pg_id()), before);
            },
        );
    }
}

fn mixed_terminal(endpoints: [(u32, bool); 2]) {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            abandoned_part_remote::with_owned(
                endpoints,
                count,
                origin,
                None,
                |mixed, cleanup, mut owner, section| {
                    let history = owner.installation_observation();
                    owner.publish_cleanup(section).unwrap();
                    let running: Vec<_> =
                        mixed.remote.iter().map(fanout_mixed::Remote::run).collect();
                    for (confirmed, id) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                        let before = mixed.observe();
                        let failed = owner
                            .into_terminal()
                            .err()
                            .expect("partial mixed cohort handed off");
                        assert_eq!(
                            failed.error().to_string(),
                            format!(
                                "complete fanout has confirmed {confirmed} of 5 required peers"
                            )
                        );
                        owner = failed.into_prepared();
                        assert_eq!(mixed.observe(), before);
                        if let Some(peer) = mixed.remote(NodeId::new(id)) {
                            owner.apply_cleanup_remote(section, &peer.client).unwrap();
                        } else {
                            owner
                                .apply_cleanup_embedded(section, NodeId::new(id))
                                .unwrap();
                        }
                    }
                    // Close transport first. Handoff must use retained confirmations,
                    // not hidden reinspection RPCs, and must not mutate any member.
                    for peer in &mixed.remote {
                        peer.client
                            .test_execution_endpoint()
                            .close_idle_requests_for_test();
                    }
                    drop(running);
                    let before = mixed.observe();
                    let terminal = owner
                        .into_terminal()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_terminal(&terminal, &mixed.context, cleanup, history, count);
                    let custody = terminal.inspect_retained_custody(section).unwrap();
                    assert_eq!(custody.retained_windows, count + 1);
                    assert_eq!(custody.selected_references, 0);
                    assert_eq!(custody.additional_finalizers, 0);
                    assert_eq!(mixed.observe(), before);
                    drop(terminal);
                    assert_eq!(mixed.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_abandoned_part_terminal_unix_witness_mixed_cohort_preserves_both_contexts() {
    mixed_terminal([(0, false), (2, true)]);
}
#[test]
fn staging_abandoned_part_terminal_tls_witness_mixed_cohort_preserves_both_contexts() {
    mixed_terminal([(0, true), (2, false)]);
}
#[test]
fn staging_abandoned_part_terminal_embedded_witness_mixed_cohort_preserves_both_contexts() {
    mixed_terminal([(2, false), (3, true)]);
}

#[test]
fn staging_abandoned_part_terminal_original_work_expiry_survives_success_and_failed_handoff() {
    for before_handoff in [true, false] {
        with_prepared(
            0,
            "uncertain",
            RouteMapValidity::Forever,
            Some(Duration::from_secs(2)),
            |map, source, cleanup, mut owner, section, cutoff| {
                apply_all(&mut owner, section);
                let history = owner.installation_observation();
                let before = observe(map, source.id().pg_id());
                if before_handoff {
                    std::thread::sleep(
                        cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                    );
                    let failed = owner
                        .into_terminal()
                        .err()
                        .expect("expired pair handed off");
                    assert!(failed.error().to_string().contains("original work cutoff"));
                    owner = failed.into_prepared();
                    owner.assert_contexts_for_test(source, cleanup, &topology());
                    assert_eq!(owner.installation_observation(), history);
                    for id in [4, 2, 0, 3, 1] {
                        assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
                    }
                    assert!(
                        owner.into_terminal().is_err(),
                        "retry renewed original cutoff"
                    );
                } else {
                    let terminal = owner
                        .into_terminal()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_terminal(&terminal, source, cleanup, history, 0);
                    std::thread::sleep(
                        cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                    );
                    assert!(terminal
                        .require_valid()
                        .unwrap_err()
                        .to_string()
                        .contains("original work cutoff"));
                    terminal.assert_custody_for_test(source, cleanup, &topology(), history);
                    drop(terminal);
                }
                assert_eq!(observe(map, source.id().pg_id()), before);
            },
        );
    }
}

#[test]
fn staging_abandoned_part_terminal_raw_route_renewal_cannot_revive_either_holder() {
    let now = crate::clock::current_time_millis();
    let monotonic = crate::clock::monotonic_time_millis();
    with_prepared(
        2,
        "uncertain",
        RouteMapValidity::until_ms(now + 60_000).unwrap(),
        None,
        |map, source, cleanup, mut owner, section, _| {
            apply_all(&mut owner, section);
            let history = owner.installation_observation();
            let before = observe(map, source.id().pg_id());
            let failed = crate::clock::with_time_and_monotonic_override(
                now + 120_000,
                monotonic + 120_000,
                || {
                    map.test_store_route_map_validity(
                        RouteMapValidity::until_ms(now + 240_000).unwrap(),
                    );
                    map.require_route_map_valid_now().unwrap();
                    owner
                        .into_terminal()
                        .err()
                        .expect("raw renewal revived original route")
                },
            );
            assert!(failed.error().to_string().contains("expired"));
            owner = failed.into_prepared();
            owner.assert_contexts_for_test(source, cleanup, &topology());
            assert_eq!(owner.installation_observation(), history);
            let terminal = owner
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_terminal(&terminal, source, cleanup, history, 2);
            crate::clock::with_time_and_monotonic_override(
                now + 120_000,
                monotonic + 120_000,
                || {
                    assert!(
                        matches!(terminal.require_valid(), Err(StoreError::RouteMapExpired { valid_until_ms, .. }) if valid_until_ms == now + 60_000)
                    );
                    assert!(terminal.inspect_retained_custody(section).is_err());
                },
            );
            drop(terminal);
            assert_eq!(observe(map, source.id().pg_id()), before);
        },
    );
}

#[test]
fn staging_abandoned_part_terminal_custody_reloads_owner_and_rejects_crossed_admission() {
    with_prepared(
        2,
        "uncertain",
        RouteMapValidity::Forever,
        None,
        |map, source, cleanup, mut owner, section, _| {
            apply_all(&mut owner, section);
            let history = owner.installation_observation();
            let terminal = owner
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let custody = terminal.inspect_retained_custody(section).unwrap();
            let before = observe(map, source.id().pg_id());
            with_prepared(
                2,
                "uncertain",
                RouteMapValidity::Forever,
                None,
                |foreign, foreign_source, _, _, foreign_section, _| {
                    let foreign_before = observe(foreign, foreign_source.id().pg_id());
                    foreign_section.require_effect().unwrap();
                    assert!(terminal.inspect_retained_custody(foreign_section).is_err());
                    assert_eq!(
                        observe(foreign, foreign_source.id().pg_id()),
                        foreign_before
                    );
                },
            );
            assert_eq!(observe(map, source.id().pg_id()), before);
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            node.get_pg(source.id().pg_id().get()).unwrap().conn.execute(
                "UPDATE payload_staging_resources SET owner_token = 'crossed-finalizer' WHERE role = 1", [],
            ).unwrap();
            let damaged = observe(map, source.id().pg_id());
            assert!(
                terminal.inspect_retained_custody(section).is_err(),
                "cached scalar {custody:?} cannot stand in for a fresh check"
            );
            terminal.assert_custody_for_test(source, cleanup, &topology(), history);
            assert_eq!(observe(map, source.id().pg_id()), damaged);
            drop(terminal);
            assert_eq!(observe(map, source.id().pg_id()), damaged);
        },
    );
}
