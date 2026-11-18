// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Connection continuation across receiver calls, not socket dispatch coverage.
use super::*;
use crate::node_runtime::pg_store::RemoteIntroductionInstallation;
use crate::node_runtime::server::ParkedIntroductionTest;

#[test]
fn staging_direct_introduction_parked_fresh_installs_exact_command() {
    receiver_capture("park-fresh");
}
#[test]
fn staging_direct_introduction_parked_confirmed_history_never_regresses() {
    receiver_capture("park-installed");
}
#[test]
fn staging_direct_introduction_parked_lost_confirmation_retries_same_submission() {
    receiver_capture("park-lost");
}
#[test]
fn staging_direct_introduction_parked_unwind_preserves_prior_history() {
    receiver_capture("park-unwind");
}
#[test]
fn staging_direct_introduction_parked_resumed_unwind_keeps_state_in_connection() {
    receiver_capture("park-resumed-unwind");
}
#[test]
fn staging_direct_introduction_parked_expired_custody_is_retained_without_renewal() {
    receiver_capture("park-expired");
}
#[test]
fn staging_direct_introduction_parked_original_work_deadline_is_not_rebound() {
    receiver_capture("park-work");
}
#[test]
fn staging_direct_introduction_parked_original_cutoff_reaches_sql() {
    receiver_capture("park-sql-cutoff");
}
#[test]
fn staging_direct_introduction_parked_damaged_retry_preserves_uncertainty() {
    receiver_capture("park-damaged");
}

pub(super) fn before(case: &str, owner: &mut RemoteIntroductionInstallation<'_>) {
    assert_eq!(owner.installation(), Installed::NoAcceptanceObserved);
    match case {
        "park-installed" => {
            owner.install().unwrap();
            assert_eq!(owner.installation(), Installed::Installed);
        }
        "park-lost" | "park-damaged" => {
            let error = PgStore::test_with_lost_complete_install_confirmation(|| owner.install())
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("lost complete installation confirmation"),
                "{error}"
            );
            assert_eq!(owner.installation(), Installed::Unconfirmed);
        }
        "park-unwind" => {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                PgStore::test_with_before_pending_slot_finish_hook(
                    |_| panic!("interrupt introduction before parking"),
                    || owner.install(),
                )
            }));
            assert!(panic.is_err());
            assert_eq!(owner.installation(), Installed::Unconfirmed);
        }
        _ => {}
    }
}

pub(super) fn after(
    case: &str,
    f: &fanout_mixed::Mixed,
    expected: &[u8],
    retained: &mut ParkedIntroductionTest<'_>,
) {
    let before = f.observe();
    retained.reject_crossed_calls();
    assert_eq!(
        f.observe(),
        before,
        "crossed receiver calls changed storage"
    );
    let node = f.node(NodeId::new(4));
    let pg_id = f.context.id().pg_id();
    if matches!(case, "park-installed" | "park-damaged") {
        node.get_pg(pg_id.get())
            .unwrap()
            .conn
            .execute(
                "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                [],
            )
            .unwrap();
        let damaged = f.observe();
        if case == "park-installed" {
            retained.install().unwrap();
            assert_eq!(retained.observation(), Installed::Installed);
        } else {
            assert!(retained.install().is_err());
            assert_eq!(retained.observation(), Installed::Unconfirmed);
        }
        assert_eq!(
            f.observe(),
            damaged,
            "retry repaired or rewrote damaged slot"
        );
        return;
    }
    if matches!(case, "park-expired" | "park-work" | "park-sql-cutoff") {
        let (deadline, cutoff) = retained.bounds();
        let error = if case == "park-work" {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            retained.install().unwrap_err()
        } else if case == "park-sql-cutoff" {
            let clock = crate::clock::test_time_override_guard(2_000);
            let control = clock.control();
            let reached = std::rc::Rc::new(std::cell::Cell::new(false));
            let hit = std::rc::Rc::clone(&reached);
            let error = PgStore::test_with_before_pending_slot_effect_hook(
                move |transaction| {
                    assert!(transaction);
                    hit.set(true);
                    control.set(cutoff);
                },
                || retained.install(),
            )
            .unwrap_err();
            assert!(reached.get(), "must reach SQLite pre-effect boundary");
            assert!(retained.install().is_err(), "retry renewed expired cutoff");
            error
        } else {
            retained.install().unwrap_err()
        };
        assert!(error.message.contains("expired"), "{error:?}");
        assert_eq!(retained.observation(), Installed::NoAcceptanceObserved);
        assert_eq!(f.observe(), before);
        return;
    }
    if case == "park-resumed-unwind" {
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_before_pending_slot_finish_hook(
                |pg| {
                    assert!(pg
                        .pending_staging_protection(&topology())
                        .unwrap()
                        .is_some());
                    panic!("interrupt parked introduction before commit");
                },
                || retained.install(),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(retained.observation(), Installed::Unconfirmed);
        assert!(f.observe().iter().zip(&before).all(|(a, b)| a.0 == b.0));
    }
    retained.install().unwrap();
    assert_eq!(retained.observation(), Installed::Installed);
    {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let bytes: Vec<u8> = pg
            .conn
            .query_row(
                "SELECT command_bytes FROM metadata_command_pending_slot",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bytes, expected);
        assert!(pg
            .pending_staging_protection(&topology())
            .unwrap()
            .unwrap()
            .window
            .is_some());
        assert_eq!(
            pg.pending_metadata_command_route_references(4)
                .unwrap()
                .unwrap()
                .id,
            f.context.id()
        );
    }
    let installed = f.observe();
    retained.install().unwrap();
    assert_eq!(retained.observation(), Installed::Installed);
    assert_eq!(f.observe(), installed);
}
