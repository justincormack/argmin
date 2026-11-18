// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::TerminalReservationObservation as Reservation;

#[path = "payload_staging_direct_terminal_slot_tests.rs"]
mod slot;

macro_rules! installed {
    ($owner:expr, $writer:ident, $section:expr) => {{
        let prepared = prepared!($owner, $writer, $section);
        let mut submission = prepared.into_submission();
        submission.install_embedded($section).unwrap();
        submission
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[test]
fn staging_direct_terminal_handoff_requires_every_original_member_and_preserves_history() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let mut publication = installed!(owner, writer, section);
            let history = publication.test_binding_state();
            let cutoff = publication.test_cutoff();
            for next in [None, Some(0), Some(4), Some(2), Some(3), Some(1)] {
                let before = changes(f);
                let failure = match publication.into_terminal() {
                    Ok(_) => panic!("incomplete cohort"),
                    Err(e) => e,
                };
                assert!(failure.error().to_string().contains("confirmed"));
                publication = failure.into_publication();
                assert_eq!(publication.test_binding_state(), history);
                assert_eq!(publication.test_cutoff(), cutoff);
                assert_eq!(changes(f), before);
                match next {
                    None => publication.publish_embedded(section).unwrap(),
                    Some(1) => {
                        assert!(publication
                            .test_apply(section, NodeId::new(1), |stage| {
                                if stage == "after-commit" {
                                    return Err(StoreError::OperationDeadlineExceeded {
                                        context: "last replica reply lost",
                                    });
                                }
                                Ok(())
                            })
                            .is_err());
                    }
                    Some(member) => {
                        publication
                            .apply_embedded(section, NodeId::new(member))
                            .unwrap();
                    }
                }
            }
            let before = changes(f);
            let failure = match publication.into_terminal() {
                Ok(_) => panic!("unconfirmed final replica"),
                Err(e) => e,
            };
            publication = failure.into_publication();
            assert_eq!(
                publication.application(NodeId::new(1)),
                Applied::Unconfirmed
            );
            publication.apply_embedded(section, NodeId::new(1)).unwrap();
            assert_eq!(changes(f), before);
            let installation = publication.test_installation();
            let id = publication.id();
            let terminal = publication
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(terminal.id(), id);
            assert_eq!(terminal.test_installation(), installation);
            assert_eq!(terminal.reservation(), Reservation::NoAttemptObserved);
            assert_eq!(terminal.work().reservation, crate::node_runtime::pg_store::payload_staging::TerminalReservationWork::ReleaseCommandProof);
            for node in 0..5 {
                assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
            }
            assert_eq!(changes(f), before);
            drop(terminal);
            assert_eq!(changes(f), before);
            installation::assert_files_retained(f, owner);
        },
    );
}

#[test]
fn staging_direct_terminal_handoff_expiry_returns_the_same_publication() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let mut publication = installed!(owner, writer, section);
            publication.publish_embedded(section).unwrap();
            for node in [0, 4, 2, 3, 1] {
                publication
                    .apply_embedded(section, NodeId::new(node))
                    .unwrap();
            }
            let history = publication.test_binding_state();
            let cutoff = publication.test_cutoff();
            let before = changes(f);
            let failure =
                crate::clock::with_time_override(cutoff.1, || match publication.into_terminal() {
                    Ok(_) => panic!("expired handoff"),
                    Err(e) => e,
                });
            let mut publication = failure.into_publication();
            assert_eq!(publication.test_cutoff(), cutoff);
            assert_eq!(publication.test_binding_state(), history);
            for node in 0..5 {
                assert_eq!(publication.application(NodeId::new(node)), Applied::Applied);
            }
            crate::clock::with_time_override(cutoff.1, || {
                // An already-confirmed marker is only a historical observation:
                // this no-effect retry does not authorize the rejected handoff.
                publication.publish_embedded(section).unwrap();
            });
            assert_eq!(changes(f), before);
            drop(
                publication
                    .into_terminal()
                    .unwrap_or_else(|e| panic!("{}", e.error())),
            );
            assert_eq!(changes(f), before);
            installation::assert_files_retained(f, owner);
        },
    );
}

#[test]
fn staging_direct_terminal_releases_only_exact_bucket_proof_before_slot_retirement() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let prepared = prepared!(owner, writer, section);
            let command = prepared.test_command();
            let proof = command
                .payload()
                .primary_bucket_write_reservation_proof()
                .unwrap();
            let bucket_pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
            assert_ne!(bucket_pg, command.id().pg_id());
            let primary = f.map.pg_route(bucket_pg).unwrap().primary_node_id();
            let node = f.map.node(primary).unwrap().test_node();
            {
                let pg = node.get_pg(bucket_pg.get()).unwrap();
                terminal_release::seed_pg_reservations(&pg, proof, None);
                pg.refresh_metadata_command_state_digest().unwrap();
            }
            let reservations = || {
                let pg = node.get_pg(bucket_pg.get()).unwrap();
                (
                    pg.conn.total_changes(),
                    pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
                )
            };
            let before = reservations();
            assert_eq!(before.1.len(), 2);
            assert!(before.1.iter().any(|record| proof.matches_record(record)));
            let mut submission = prepared.into_submission();
            submission.install_embedded(section).unwrap();
            let mut publication = submission
                .into_publication()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish_embedded(section).unwrap();
            for member in [0, 4, 2, 3, 1] {
                publication
                    .apply_embedded(section, NodeId::new(member))
                    .unwrap();
            }
            let cutoff = publication.test_cutoff();
            let mut terminal = publication
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let object_before = changes(f);
            assert!(terminal.retire_embedded(section).is_err());
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || terminal.test_interrupt_release()
            ))
            .is_err());
            assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
            assert_eq!(reservations(), before);
            node.get_pg(bucket_pg.get()).unwrap().conn.execute_batch("CREATE TEMP TRIGGER refuse_release BEFORE DELETE ON bucket_write_reservations BEGIN SELECT RAISE(ABORT, 'direct release fault'); END;").unwrap();
            let error = terminal.release_reservation().unwrap_err();
            let crate::BucketSnapshotLoadError::Metadata(error) = error else {
                panic!("expected injected metadata error: {error:?}");
            };
            assert!(
                error.to_string().contains("direct release fault"),
                "{error}"
            );
            assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
            assert_eq!(reservations(), before);
            node.get_pg(bucket_pg.get())
                .unwrap()
                .conn
                .execute_batch("DROP TRIGGER refuse_release")
                .unwrap();
            crate::clock::with_time_override(cutoff.1, || {
                assert!(terminal.release_reservation().is_err())
            });
            assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
            terminal.release_reservation().unwrap();
            assert_eq!(terminal.reservation(), Reservation::Released);
            let after = reservations();
            assert_eq!(after.0, before.0 + 1);
            assert_eq!(
                after.1,
                before
                    .1
                    .into_iter()
                    .filter(|r| r.reservation_id != proof.reservation_id)
                    .collect::<Vec<_>>()
            );
            assert_eq!(after.1.len(), 1);
            terminal.release_reservation().unwrap();
            assert_eq!(reservations(), after);
            assert_eq!(changes(f), object_before);
            for member in 0..5 {
                let pg = f
                    .map
                    .node(NodeId::new(member))
                    .unwrap()
                    .test_node()
                    .get_pg(owner.subject.object_pg.get())
                    .unwrap();
                object::assert_object(&pg, &command);
                let snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                snapshot.finish().unwrap();
            }
            assert!(marker(
                &f.map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(owner.subject.object_pg.get())
                    .unwrap()
            ));
            terminal.retire_embedded(section).unwrap();
            assert_eq!(
                terminal.retirement(),
                crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired
            );
            let retired_changes = changes(f);
            assert_ne!(retired_changes, object_before);
            terminal.retire_embedded(section).unwrap();
            assert_eq!(changes(f), retired_changes);
            for member in 0..5 {
                let pg = f
                    .map
                    .node(NodeId::new(member))
                    .unwrap()
                    .test_node()
                    .get_pg(owner.subject.object_pg.get())
                    .unwrap();
                object::assert_object(&pg, &command);
                let snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                snapshot.finish().unwrap();
                if member == 4 {
                    assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                }
            }
            drop(terminal);
            assert_eq!(changes(f), retired_changes);
            assert_eq!(reservations(), after);
            installation::assert_files_retained(f, owner);
        },
    );
}
