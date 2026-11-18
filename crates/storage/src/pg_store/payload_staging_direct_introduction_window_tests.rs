// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning introduction → retirement → window inspection on the same section.
use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, TerminalSlotRetirementObservation as Retirement,
};
use application::members;

// Keep every fixture guard in the caller's scope; no store reopen or detached
// terminal/section pair is manufactured for these tests.
macro_rules! introduced {
    ($f:ident, $section:ident, $terminal:ident, $until:expr) => {
        let ($f, owner, proof) = inputs();
        let admission = $f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                $f.context.id().pg_id(),
                $f.context.id().cluster_epoch(),
            )
            .unwrap();
        let $section = route.lock_until($until).unwrap();
        let mut captured = $section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .unwrap();
        captured.install().unwrap();
        let mut publication = captured
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        publication.publish().unwrap();
        for node in [0, 4, 2, 3, 1] {
            publication.apply(NodeId::new(node)).unwrap();
        }
        let mut $terminal = publication
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
    };
}

#[path = "payload_staging_direct_introduction_file_tests.rs"]
mod files;

#[test]
fn staging_direct_introduction_window_requires_confirmed_retirement_and_preserves_custody() {
    let clock = crate::clock::test_time_override_guard(2_000);
    introduced!(f, section, terminal, deadline());
    let cutoffs = terminal.test_original_deadlines();
    for expected in [Retirement::NoAttemptObserved, Retirement::Unconfirmed] {
        let before = members(&f);
        let failure = terminal
            .test_confirm_window(|_| panic!("unconfirmed retirement reached inspection"))
            .err()
            .expect("unconfirmed retirement must retain custody");
        assert!(failure
            .error()
            .to_string()
            .contains("confirmed slot retirement"));
        terminal = failure.into_terminal();
        assert_eq!(terminal.retirement(), expected);
        assert_eq!(terminal.test_original_deadlines(), cutoffs);
        for node in 0..5 {
            assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
        }
        assert_eq!(members(&f), before);
        if expected == Retirement::NoAttemptObserved {
            terminal
                .test_retire(|point| {
                    if point == "after-commit" {
                        return Err(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "lost retirement confirmation before window",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
        }
    }
    terminal.retire().unwrap();
    let before = members(&f);
    // The injected storage error exits before PrefixTransition::finish. Exact
    // slot-absence confirmation does not silently recover that invalidated
    // validation state. Preserve custody until explicit local recovery succeeds.
    let failure = terminal
        .into_confirmed_window()
        .err()
        .expect("cold prefix must fail closed");
    assert!(matches!(
        failure.error(),
        crate::node_runtime::pg_store::StagingWindowConfirmationError::Prefix(
            crate::node_runtime::pg_store::CompleteSettledPrefixError::ValidationRequired
        )
    ));
    terminal = failure.into_terminal();
    assert_eq!(terminal.retirement(), Retirement::Retired);
    assert_eq!(members(&f), before);
    {
        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(section.pg_id().get()).unwrap();
        pg.recover_complete_command_state(
            crate::node_runtime::pg_store::PgStoreRecoveryContext::for_node(NodeId::new(4)),
            &topology(),
        )
        .unwrap();
    }
    assert_eq!(
        members(&f),
        before,
        "local validation mutated already-settled state"
    );
    let mut points = Vec::new();
    let window = terminal
        .test_confirm_window(|point| points.push(point))
        .unwrap_or_else(|error| panic!("{}", error.error()));
    assert!(points.contains(&"before witness"));
    assert!(points.contains(&"after witness"));
    assert!(points.contains(&"after introduction window inspection"));
    assert_eq!(window.id(), f.context.id());
    assert_eq!(window.observation().witness, Some(NodeId::new(0)));
    assert_eq!(
        window.observation().prefix.applied_log_index,
        window.id().log_index().get()
    );
    assert_eq!(window.test_original_deadlines(), cutoffs);
    window.require_valid().unwrap();
    assert_eq!(members(&f), before);
    clock.set(100_000);
    assert!(window.require_valid().is_err());
    drop(window);
    assert_eq!(
        members(&f),
        before,
        "Drop performed physical or terminal work"
    );
}

#[test]
fn staging_direct_introduction_window_expiry_cannot_mint_or_renew_custody() {
    let clock = crate::clock::test_time_override_guard(2_000);
    for stage in [
        "before inspection",
        "before witness",
        "after witness",
        "after introduction window inspection",
        "monotonic cutoff",
    ] {
        clock.set(2_000);
        introduced!(f, section, terminal, deadline());
        terminal.retire().unwrap();
        let cutoffs = terminal.test_original_deadlines();
        let before = members(&f);
        if stage == "before inspection" {
            clock.set(100_000);
        }
        let mut reached = stage == "before inspection";
        let failure = terminal
            .test_confirm_window(|point| {
                assert_ne!(
                    stage, "before inspection",
                    "expired authority reached inspection"
                );
                if point == stage {
                    reached = true;
                    clock.set(100_000);
                }
                if stage == "monotonic cutoff" && point == "after introduction window inspection" {
                    reached = true;
                    // Use the actual captured owner bound, created after fixture
                    // setup. Keep wall time frozen to isolate monotonic expiry.
                    std::thread::sleep(cutoffs.0.saturating_duration_since(Instant::now()));
                }
            })
            .err()
            .expect(stage);
        assert!(reached, "{stage}: {}", failure.error());
        assert!(
            matches!(
                failure.error(),
                crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                    StoreError::OperationDeadlineExceeded { .. }
                        | StoreError::RouteMapExpired { .. }
                )
            ),
            "{stage}: {}",
            failure.error()
        );
        let terminal = failure.into_terminal();
        assert_eq!(terminal.retirement(), Retirement::Retired);
        assert_eq!(terminal.test_original_deadlines(), cutoffs);
        let failure = terminal
            .test_confirm_window(|_| panic!("expired retry inspected"))
            .err()
            .expect("retry must not renew deadline");
        assert_eq!(failure.into_terminal().test_original_deadlines(), cutoffs);
        assert_eq!(members(&f), before);
    }
}

#[test]
fn staging_direct_introduction_window_rejects_changed_primary_or_witness_without_effects() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for member in [4, 0] {
        introduced!(f, section, terminal, deadline());
        terminal.retire().unwrap();
        let cutoffs = terminal.test_original_deadlines();
        {
            let node = f.map.node(NodeId::new(member)).unwrap().test_node();
            let pg = node.get_pg(section.pg_id().get()).unwrap();
            let damage = if member == 4 {
                "UPDATE payload_staging_windows SET cutoff_ms = cutoff_ms + 1"
            } else {
                "UPDATE metadata_command_log SET command_checksum = command_checksum + 1"
            };
            pg.conn.execute_batch(damage).unwrap();
        }
        let before = members(&f);
        let failure = terminal
            .into_confirmed_window()
            .err()
            .expect("changed window or witness prefix evidence must fail");
        let terminal = failure.into_terminal();
        assert_eq!(terminal.retirement(), Retirement::Retired);
        assert_eq!(terminal.test_original_deadlines(), cutoffs);
        for node in 0..5 {
            assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
        }
        drop(terminal);
        assert_eq!(
            members(&f),
            before,
            "confirmation repaired or mutated changed evidence"
        );
    }
}
