// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual receiver/session ownership and SQLite effects, not socket evidence.
use super::*;
use crate::node_runtime::pg_store::RemoteIntroductionInstallation;

#[test]
fn staging_direct_introduction_receiver_installation_is_exclusive_and_exact() {
    receiver_capture("install-success");
}
#[test]
fn staging_direct_introduction_receiver_installation_lost_confirmation_retains_history() {
    receiver_capture("install-lost");
}
#[test]
fn staging_direct_introduction_receiver_installation_unwind_retains_history() {
    receiver_capture("install-unwind");
}
#[test]
fn staging_direct_introduction_receiver_installation_rollback_is_definitive() {
    receiver_capture("install-rollback");
}
#[test]
fn staging_direct_introduction_receiver_installation_failed_commit_is_unconfirmed() {
    receiver_capture("install-commit");
}
#[test]
fn staging_direct_introduction_receiver_installation_original_work_deadline_reaches_sql() {
    receiver_capture("install-work");
}
#[test]
fn staging_direct_introduction_receiver_installation_original_cutoff_reaches_sql() {
    receiver_capture("install-cutoff");
}
#[test]
fn staging_direct_introduction_receiver_installation_damaged_retry_cannot_reset_history() {
    receiver_capture("install-damaged");
}

pub(super) fn exercise(
    case: &str,
    f: &fanout_mixed::Mixed,
    expected: &[u8],
    owner: &mut RemoteIntroductionInstallation<'_>,
) {
    let clock = crate::clock::test_time_override_guard(2_000);
    let before = f.observe();
    let node = f.node(NodeId::new(4));
    let pg_id = owner.id().pg_id();
    assert_eq!(owner.installation(), Installed::NoAcceptanceObserved);
    match case {
        "install-work" | "install-cutoff" => {
            let (deadline, cutoff) = owner.test_bounds();
            let reached = std::rc::Rc::new(std::cell::Cell::new(false));
            let hit = std::rc::Rc::clone(&reached);
            let control = clock.control();
            let work = case == "install-work";
            let result = PgStore::test_with_before_pending_slot_effect_hook(
                move |transaction| {
                    assert!(transaction);
                    hit.set(true);
                    if work {
                        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                    } else {
                        control.set(cutoff);
                    }
                },
                || owner.install(),
            );
            assert!(reached.get(), "must reach final pre-effect check");
            let error = result.unwrap_err();
            assert!(error.to_string().contains("expired"), "{error}");
            assert_eq!(owner.installation(), Installed::NoAcceptanceObserved);
            assert_eq!(f.observe(), before);
            assert!(owner.install().is_err(), "retry renewed original cutoff");
            assert_eq!(f.observe(), before);
            return;
        }
        "install-unwind" => {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                PgStore::test_with_before_pending_slot_finish_hook(
                    |pg| {
                        assert!(pg
                            .pending_staging_protection(&topology())
                            .unwrap()
                            .is_some());
                        panic!("receiver introduction interrupted before commit");
                    },
                    || owner.install(),
                )
            }));
            assert!(panic.is_err());
            assert_eq!(owner.installation(), Installed::Unconfirmed);
            assert!(f.observe().iter().zip(&before).all(|(a, b)| a.0 == b.0));
        }
        "install-rollback" | "install-commit" => {
            {
                let pg = node.get_pg(pg_id.get()).unwrap();
                let sql = if case == "install-rollback" {
                    "CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'receiver installation fault'); END;"
                } else {
                    "CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_pending AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;"
                };
                pg.conn.execute_batch(sql).unwrap();
            }
            let error = owner.install().unwrap_err();
            let (observed, diagnostic) = if case == "install-rollback" {
                (
                    Installed::NoAcceptanceObserved,
                    "receiver installation fault",
                )
            } else {
                (
                    Installed::Unconfirmed,
                    "commit pending metadata command slot transaction",
                )
            };
            assert!(error.to_string().contains(diagnostic), "{error}");
            assert_eq!(owner.installation(), observed);
            assert!(f.observe().iter().zip(&before).all(|(a, b)| a.0 == b.0));
            assert!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes() > before[4].1,
                "fault must follow pending writes"
            );
            node.get_pg(pg_id.get())
                .unwrap()
                .conn
                .execute_batch("DROP TRIGGER fail_pending")
                .unwrap();
        }
        "install-lost" | "install-damaged" => {
            let error = PgStore::test_with_lost_complete_install_confirmation(|| owner.install())
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("lost complete installation confirmation"),
                "{error}"
            );
            assert_eq!(owner.installation(), Installed::Unconfirmed);
            if case == "install-damaged" {
                node.get_pg(pg_id.get())
                    .unwrap()
                    .conn
                    .execute(
                        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                        [],
                    )
                    .unwrap();
                let damaged = f.observe();
                assert!(owner.install().is_err());
                assert_eq!(owner.installation(), Installed::Unconfirmed);
                assert_eq!(
                    f.observe(),
                    damaged,
                    "damaged retry repaired pending indexes"
                );
                return;
            }
            let installed = f.observe();
            clock.set(owner.test_bounds().1);
            assert!(owner.install().is_err());
            assert_eq!(owner.installation(), Installed::Unconfirmed);
            assert_eq!(f.observe(), installed);
            clock.set(2_000);
        }
        "install-success" => {}
        _ => unreachable!(),
    }
    owner.install().unwrap();
    assert_eq!(owner.installation(), Installed::Installed);
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
            owner.id()
        );
    }
    let installed = f.observe();
    owner.install().unwrap();
    assert_eq!(f.observe(), installed);
    // Confirmed history does not regress or claim a currently healthy slot.
    node.get_pg(pg_id.get())
        .unwrap()
        .conn
        .execute(
            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            [],
        )
        .unwrap();
    let damaged = f.observe();
    owner.install().unwrap();
    assert_eq!(owner.installation(), Installed::Installed);
    assert_eq!(f.observe(), damaged);
}
