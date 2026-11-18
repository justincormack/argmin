// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh admitted introduction through exact slot retirement. No owner/resource
//! retirement, window confirmation or physical permission is implied.
use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, TerminalSlotRetirementObservation as Retirement,
};
use application::members;

fn pending(f: &Setup) -> bool {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    pg.conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM metadata_command_pending_slot)",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn staging_direct_introduction_retirement_preserves_owner_and_retries_lost_confirmation() {
    let clock = crate::clock::test_time_override_guard(2_000);
    for fault in [
        "none",
        "before-delete",
        "before-commit",
        "after-commit",
        "unwind",
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
        publication.publish().unwrap();
        for node in [0, 4, 2, 3, 1] {
            publication.apply(NodeId::new(node)).unwrap();
        }
        let cutoffs = publication.test_original_deadlines();
        let mut terminal = publication
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(terminal.retirement(), Retirement::NoAttemptObserved);
        let before = members(&f);
        if fault != "none" {
            let mut reached = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                terminal.test_retire(|point| {
                    if point == "after-commit" && fault == "unwind" {
                        reached = true;
                        panic!("lost retirement confirmation");
                    }
                    if point == fault {
                        reached = true;
                        return Err(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "injected introduction retirement failure",
                        });
                    }
                    Ok(())
                })
            }));
            assert!(reached);
            if fault == "unwind" {
                assert!(result.is_err());
            } else {
                assert!(result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("injected introduction retirement failure"));
            }
            assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
            assert_eq!(
                pending(&f),
                matches!(fault, "before-delete" | "before-commit")
            );
        }
        terminal.retire().unwrap();
        assert_eq!(terminal.retirement(), Retirement::Retired);
        assert!(!pending(&f));
        assert_eq!(terminal.test_original_deadlines(), cutoffs);
        let after = members(&f);
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(
                old.0, new.0,
                "slot retirement changed owner, generation or resource state"
            );
            assert_eq!(old.1, new.1);
        }
        for node in 0..5 {
            assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
        }
        terminal
            .test_retire(|_| panic!("confirmed retry entered storage"))
            .unwrap();
        assert_eq!(members(&f), after);
        clock.set(100_000);
        assert!(terminal.retire().is_err());
        assert_eq!(terminal.retirement(), Retirement::Retired);
        drop(terminal);
        assert_eq!(
            members(&f),
            after,
            "Drop retried or released responsibility"
        );
    }
}

#[test]
fn staging_direct_introduction_retirement_rejects_damaged_custody_and_expiry_without_hidden_retry()
{
    let clock = crate::clock::test_time_override_guard(2_000);
    for fault in [
        "scope",
        "index",
        "owner",
        "absent-owner",
        "incarnation",
        "window",
        "resource",
        "proof",
        "high-water",
        "cutoff",
        "work",
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
        let until = if fault == "work" {
            Instant::now() + Duration::from_secs(4)
        } else {
            deadline()
        };
        let section = route.lock_until(until).unwrap();
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
        publication.publish().unwrap();
        for node in [0, 4, 2, 3, 1] {
            publication.apply(NodeId::new(node)).unwrap();
        }
        let mut terminal = publication
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        if fault == "absent-owner" {
            let error = terminal
                .test_retire(|point| {
                    if point == "after-commit" {
                        return Err(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "lost retirement reply before custody loss",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error.to_string().contains("lost retirement reply"));
            assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
            assert!(!pending(&f));
        }
        let sql = match fault {
            "scope" => Some("UPDATE metadata_command_pending_slot SET scope_bucket = NULL"),
            "index" => Some("DELETE FROM metadata_command_pending_staging_protection"),
            "owner" | "absent-owner" => Some("DELETE FROM payload_staging_owners"),
            "incarnation" => Some("UPDATE payload_staging_owners SET incarnation = incarnation + 1"),
            "window" => Some("UPDATE payload_staging_windows SET cutoff_ms = cutoff_ms + 1"),
            "resource" => Some("DELETE FROM payload_staging_resources WHERE role = 0"),
            "proof" => Some("UPDATE payload_staging_resources SET owner_token = owner_token || '-crossed' WHERE role = 0"),
            "high-water" => Some("DELETE FROM object_write_counters"),
            _ => None,
        };
        if let Some(sql) = sql {
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(section.pg_id().get()).unwrap();
            pg.conn.execute_batch(sql).unwrap();
        }
        let before = members(&f);
        let mut reached = false;
        let error = terminal
            .test_retire(|point| {
                if point == "before-delete" {
                    reached = true;
                    if fault == "cutoff" {
                        clock.set(100_000);
                    }
                    if fault == "work" {
                        std::thread::sleep(until.saturating_duration_since(Instant::now()));
                    }
                }
                Ok(())
            })
            .expect_err(fault);
        if matches!(fault, "cutoff" | "work") {
            assert!(reached, "{error:?}");
            assert!(error.to_string().contains("deadline"), "{error:?}");
        } else {
            assert!(!reached, "damaged custody reached deletion: {fault}");
        }
        assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
        assert_eq!(pending(&f), fault != "absent-owner");
        assert_eq!(members(&f), before);
        drop(terminal);
        assert_eq!(members(&f), before);
        assert_eq!(pending(&f), fault != "absent-owner");
    }
}
