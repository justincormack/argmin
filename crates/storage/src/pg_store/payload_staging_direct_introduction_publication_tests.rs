// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh admitted construction → classified installation → owning publication.
//! Marker confirmation is deliberately not application or physical authority.
use super::*;
use crate::node_runtime::pg_store::PublicationObservation as Published;

#[path = "payload_staging_direct_introduction_application_tests.rs"]
mod application;

#[path = "payload_staging_direct_introduction_remote_tests.rs"]
mod remote;

#[path = "payload_staging_direct_introduction_retirement_tests.rs"]
mod retirement;

#[path = "payload_staging_direct_introduction_window_tests.rs"]
mod window;

fn started(f: &Setup) -> bool {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    pg.conn
        .query_row(
            "SELECT publication_started FROM metadata_command_pending_slot",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn staging_direct_introduction_publication_transfer_retains_original_history_and_cutoffs() {
    let clock = crate::clock::test_time_override_guard(2_000);
    for state in ["fresh", "uncertain", "installed", "expired-installed"] {
        clock.set(2_000);
        let (f, owner, proof) = inputs();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut captured = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .unwrap();
        if state == "uncertain" {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                PgStore::test_with_before_pending_slot_finish_hook(
                    |_| panic!("lost install outcome"),
                    || captured.install(),
                )
            }))
            .is_err());
            assert_eq!(captured.installation(), Installed::Unconfirmed);
        } else if state.ends_with("installed") {
            captured.install().unwrap();
        }
        let expected = captured.installation();
        let cutoffs = captured.test_original_deadlines();
        let id = captured.id();
        let before = observe(&f);
        // No clock sampling during handoff, including a backward wall adjustment.
        clock.set(if state == "expired-installed" {
            100_000
        } else {
            1_000
        });
        match captured.into_publication() {
            Ok(mut publication) => {
                assert_eq!(expected, Installed::Installed);
                assert_eq!(publication.id(), id);
                assert_eq!(publication.installation(), Installed::Installed);
                assert_eq!(
                    publication.test_installation(),
                    InstallationObservation::Installed
                );
                assert_eq!(publication.publication(), Published::NoAttemptObserved);
                assert_eq!(publication.test_original_deadlines(), cutoffs);
                if state == "expired-installed" {
                    assert!(publication.publish().is_err());
                    assert_eq!(publication.publication(), Published::NoAttemptObserved);
                }
                drop(publication);
            }
            Err(failure) => {
                assert_ne!(expected, Installed::Installed);
                assert!(failure
                    .error()
                    .to_string()
                    .contains("requires confirmed installation"));
                let captured = failure.into_introduction();
                assert_eq!(captured.installation(), expected);
                assert_eq!(captured.id(), id);
                assert_eq!(captured.test_original_deadlines(), cutoffs);
                drop(captured);
            }
        }
        assert_eq!(
            observe(&f),
            before,
            "{state}: transfer/Drop mutated storage"
        );
    }
}

#[test]
fn staging_direct_introduction_publication_marker_is_not_owner_application() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    captured.install().unwrap();
    let before = observe(&f);
    let mut publication = captured
        .into_publication()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    publication.publish().unwrap();
    assert!(started(&f));
    assert_eq!(publication.publication(), Published::MarkerConfirmed);
    assert_eq!(
        publication.test_installation(),
        InstallationObservation::Installed
    );
    let published = observe(&f);
    assert_eq!(
        published.0, before.0,
        "trial application leaked owner/reservation changes"
    );
    publication.publish().unwrap();
    assert_eq!(publication.publication(), Published::MarkerConfirmed);
    assert_eq!(observe(&f).1, published.1);
    let before_drop = observe(&f);
    drop(publication);
    assert_eq!(observe(&f), before_drop);
}

#[test]
fn staging_direct_introduction_publication_failures_and_unwind_preserve_custody() {
    let clock = crate::clock::test_time_override_guard(2_000);
    for point in [
        "before-marker",
        "before-commit",
        "after-commit",
        "unwind-after-commit",
    ] {
        clock.set(2_000);
        let (f, owner, proof) = inputs();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut captured = section
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
        let before = observe(&f);
        let cutoffs = publication.test_original_deadlines();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publication.test_publish(|at| {
                if point == "unwind-after-commit" && at == "after-commit" {
                    panic!("lost marker confirmation");
                }
                if at == point {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "injected publication failure",
                    });
                }
                Ok(())
            })
        }));
        if point == "unwind-after-commit" {
            assert!(result.is_err());
        } else {
            assert!(result
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("injected publication failure"));
        }
        assert_eq!(publication.publication(), Published::Unconfirmed);
        assert_eq!(
            publication.test_installation(),
            InstallationObservation::Installed
        );
        assert_eq!(started(&f), point.ends_with("after-commit"));
        assert_eq!(observe(&f).0, before.0);
        clock.set(100_000);
        let rejected = observe(&f);
        assert!(publication.publish().is_err());
        assert_eq!(publication.publication(), Published::Unconfirmed);
        assert_eq!(observe(&f), rejected);
        // Test-only rewind exercises exact retry, without changing either cutoff.
        clock.set(2_000);
        publication.publish().unwrap();
        assert_eq!(publication.publication(), Published::MarkerConfirmed);
        clock.set(100_000);
        assert!(publication.publish().is_err());
        assert_eq!(publication.publication(), Published::MarkerConfirmed);
        assert_eq!(publication.test_original_deadlines(), cutoffs);
        let before_drop = observe(&f);
        drop(publication);
        assert_eq!(observe(&f), before_drop);
    }
}

#[test]
fn staging_direct_introduction_publication_rechecks_original_deadline_before_commit() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route
        .lock_until(Instant::now() + Duration::from_secs(4))
        .unwrap();
    let mut captured = section
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
    let (until, _) = publication.test_original_deadlines();
    let before = observe(&f);
    let mut reached = false;
    let error = publication
        .test_publish(|at| {
            if at == "before-commit" {
                reached = true;
                std::thread::sleep(until.saturating_duration_since(Instant::now()));
            }
            Ok(())
        })
        .unwrap_err();
    assert!(reached, "{error:?}");
    assert!(error.to_string().contains("deadline"), "{error:?}");
    assert_eq!(publication.publication(), Published::Unconfirmed);
    assert_eq!(
        publication.test_installation(),
        InstallationObservation::Installed
    );
    assert!(!started(&f));
    assert_eq!(observe(&f).0, before.0);
    assert_eq!(observe(&f).1, before.1);
    assert!(publication.publish().is_err(), "retry renewed deadline");
}
