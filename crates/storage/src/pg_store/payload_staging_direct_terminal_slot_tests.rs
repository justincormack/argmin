// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retirement;

macro_rules! terminal {
    ($f:ident, $owner:expr, $writer:ident, $section:expr) => {{
        let prepared = prepared!($owner, $writer, $section);
        let command = prepared.test_command();
        let proof = command
            .payload()
            .primary_bucket_write_reservation_proof()
            .unwrap();
        let bucket_pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
        let primary = $f.map.pg_route(bucket_pg).unwrap().primary_node_id();
        {
            let pg = $f
                .map
                .node(primary)
                .unwrap()
                .test_node()
                .get_pg(bucket_pg.get())
                .unwrap();
            terminal_release::seed_pg_reservations(&pg, proof, None);
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let mut submission = prepared.into_submission();
        submission.install_embedded($section).unwrap();
        let mut publication = submission
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        publication.publish_embedded($section).unwrap();
        for member in [0, 4, 2, 3, 1] {
            publication
                .apply_embedded($section, NodeId::new(member))
                .unwrap();
        }
        let mut terminal = publication
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        terminal.release_reservation().unwrap();
        (terminal, command)
    }};
}

#[test]
fn staging_direct_terminal_slot_retirement_rechecks_cutoff_beside_delete_and_commit() {
    let now = crate::clock::current_time_millis();
    let clock = crate::clock::test_time_override_guard(now);
    for stage in ["before-delete", "before-commit"] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let (mut terminal, command) = terminal!(f, owner, writer, section);
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pending =
                    complete_pending::observe(&node.get_pg(owner.subject.object_pg.get()).unwrap());
                let reached = std::cell::Cell::new(false);
                let error = terminal
                    .test_retire(section, |point| {
                        if point == stage {
                            reached.set(true);
                            clock.set(owner.windows[0].cutoff_ms);
                        }
                        Ok(())
                    })
                    .unwrap_err();
                clock.set(now);
                assert!(reached.get());
                assert!(error.to_string().contains("deadline"), "{error}");
                assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
                let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                assert_eq!(complete_pending::observe(&pg), pending, "{stage}");
                object::assert_object(&pg, &command);
                drop(pg);
                terminal.retire_embedded(section).unwrap();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                installation::assert_files_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_slot_retirement_faults_preserve_exact_custody_and_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for stage in ["before-delete", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            with_writer_and_section(
                StagingTarget::DirectPut,
                false,
                |f, owner, mut writer, section| {
                    let (mut terminal, command) = terminal!(f, owner, writer, section);
                    let reached = std::cell::Cell::new(false);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        terminal.test_retire(section, |point| {
                            if point == stage {
                                reached.set(true);
                                assert!(!unwind, "injected direct slot retirement unwind");
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "injected direct slot retirement fault",
                                });
                            }
                            Ok(())
                        })
                    }));
                    assert!(reached.get(), "{stage}");
                    if unwind {
                        assert!(result.is_err());
                    } else {
                        assert!(result.unwrap().is_err());
                    }
                    assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
                    assert_eq!(terminal.reservation(), Reservation::Released);
                    {
                        let pg = f
                            .map
                            .node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(owner.subject.object_pg.get())
                            .unwrap();
                        assert_eq!(
                            complete_pending::observe(&pg).iter().all(Vec::is_empty),
                            stage == "after-commit"
                        );
                        object::assert_object(&pg, &command);
                        let snapshot = pg.command_build_snapshot().unwrap();
                        assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                        snapshot.finish().unwrap();
                    }
                    let before = changes(f);
                    terminal.retire_embedded(section).unwrap();
                    assert_eq!(terminal.retirement(), Retirement::Retired);
                    if stage == "after-commit" {
                        assert_eq!(changes(f), before, "absence confirmation must not write");
                    }
                    let after = changes(f);
                    terminal
                        .test_retire(section, |_| panic!("confirmed retirement redispatched"))
                        .unwrap();
                    drop(terminal);
                    assert_eq!(changes(f), after);
                    installation::assert_files_retained(f, owner);
                },
            );
        }
    }
}

#[test]
fn staging_direct_terminal_slot_retirement_rejects_corrupt_custody_even_on_absence_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for absent in [false, true] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let (mut terminal, _) = terminal!(f, owner, writer, section);
                if absent {
                    assert!(terminal
                        .test_retire(section, |point| {
                            if point == "after-commit" {
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "lost direct retirement reply",
                                });
                            }
                            Ok(())
                        })
                        .is_err());
                }
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let original = {
                    let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                    let token: String = pg.conn.query_row("SELECT owner_token FROM payload_staging_resources WHERE attempt = ?1 AND role = 0", [&owner.subject.id.attempt], |row| row.get(0)).unwrap();
                    pg.conn.execute("UPDATE payload_staging_resources SET owner_token = 'crossed-terminal-owner' WHERE attempt = ?1 AND role = 0", [&owner.subject.id.attempt]).unwrap();
                    token
                };
                let before = changes(f);
                let error = terminal
                    .test_retire(section, |_| panic!("corrupt custody reached removal"))
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("staging object publication does not bind"),
                    "{error}"
                );
                assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
                assert_eq!(changes(f), before);
                {
                    let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                    assert_eq!(
                        complete_pending::observe(&pg).iter().all(Vec::is_empty),
                        absent
                    );
                    pg.conn.execute("UPDATE payload_staging_resources SET owner_token = ?1 WHERE attempt = ?2 AND role = 0", rusqlite::params![original, owner.subject.id.attempt]).unwrap();
                }
                terminal.retire_embedded(section).unwrap();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                installation::assert_files_retained(f, owner);
            },
        );
    }
}
