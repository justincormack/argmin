// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::node_runtime::pg_store::DirectTerminalInstallationObservation as Observed;

macro_rules! prepared {
    ($owner:expr, $writer:ident, $section:expr) => {
        witnessed!($owner, $writer, $section)
            .prepare_terminal_embedded($section, &input($owner), &evidence($owner))
            .unwrap_or_else(|e| panic!("{}", e.error()))
    };
}

#[path = "payload_staging_direct_terminal_publish_tests.rs"]
mod publication_tests;

#[test]
fn staging_direct_terminal_installation_preserves_exact_command_and_confirmed_history() {
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let prepared = prepared!(owner, writer, section);
            let command = prepared.test_command();
            let history = prepared.test_binding_state();
            let mut submission = prepared.into_submission();
            assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
            assert_eq!(submission.id(), command.id());
            submission.install_embedded(section).unwrap();
            assert_eq!(submission.installation(), Observed::Installed);
            assert_eq!(submission.test_binding_state(), history);
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg_id = owner.subject.object_pg.get();
            let before = {
                let pg = node.get_pg(pg_id).unwrap();
                let (bytes, crc, scope): (Vec<u8>, i64, String) = pg.conn.query_row("SELECT command_bytes, command_checksum, scope_bucket FROM metadata_command_pending_slot", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
                ContextualCommand::decode_command(&bytes, crc as u64, &topology())
                    .unwrap()
                    .assert_command_for_test(&command);
                assert_eq!(scope, owner.subject.bucket);
                assert_eq!(
                    pg.pending_metadata_command_route_references(4)
                        .unwrap()
                        .unwrap()
                        .id,
                    command.id()
                );
                assert!(pg
                    .pending_staging_protection(&topology())
                    .unwrap()
                    .unwrap()
                    .window
                    .is_none());
                assert_eq!(
                    pg.metadata_command_replica_state()
                        .unwrap()
                        .applied_log_index,
                    f.context.id().log_index().get()
                );
                (complete_pending::observe(&pg), pg.conn.total_changes())
            };
            submission.install_embedded(section).unwrap();
            {
                let pg = node.get_pg(pg_id).unwrap();
                assert_eq!(
                    (complete_pending::observe(&pg), pg.conn.total_changes()),
                    before
                );
                pg.conn
                    .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                    .unwrap();
            }
            let damaged = complete_pending::observe(&node.get_pg(pg_id).unwrap());
            submission.install_embedded(section).unwrap();
            assert_eq!(submission.installation(), Observed::Installed);
            assert_eq!(
                complete_pending::observe(&node.get_pg(pg_id).unwrap()),
                damaged
            );
            crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                assert!(submission.install_embedded(section).is_err())
            });
            assert_eq!(submission.installation(), Observed::Installed);
            drop(submission);
            installation::assert_payload_retained(f, owner);
        },
    );
}

#[test]
fn staging_direct_terminal_installation_uncertainty_survives_failure_and_retry() {
    for unwind in [false, true] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                if unwind {
                    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        PgStore::test_with_before_pending_slot_effect_hook(
                            |inside| {
                                assert!(inside);
                                panic!("interrupted direct installation");
                            },
                            || submission.install_embedded(section),
                        )
                    }))
                    .is_err());
                } else {
                    node.get_pg(pg_id).unwrap().conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
                    CREATE TEMP TRIGGER fail_commit_after_index AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
                    let error = submission.install_embedded(section).unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("commit pending metadata command slot transaction"),
                        "{error}"
                    );
                    node.get_pg(pg_id)
                        .unwrap()
                        .conn
                        .execute_batch("DROP TRIGGER fail_commit_after_index")
                        .unwrap();
                }
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    pg.require_usable_connection().unwrap();
                    pending_install::assert_empty(&pg);
                }
                crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                    assert!(submission.install_embedded(section).is_err())
                });
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                assert_eq!(submission.test_binding_state(), history);
                submission.install_embedded(section).unwrap();
                assert_eq!(submission.installation(), Observed::Installed);
                drop(submission);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_installation_revalidates_snapshot_and_reservation_in_transaction() {
    for fault in ["snapshot", "reservation"] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let command = prepared.test_command();
                let mut submission = prepared.into_submission();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    if fault == "snapshot" {
                        let MetadataCommandPayload::CommitDirectPutObject(commit) =
                            command.payload()
                        else {
                            unreachable!()
                        };
                        pg.put_object_with_segments(&commit.object, &commit.segments)
                            .unwrap();
                    } else {
                        pg.conn.execute_batch("UPDATE object_generation_reservations SET generation_id = generation_id + 1").unwrap();
                    }
                }
                let before = changes(f);
                let error = submission.install_embedded(section).unwrap_err();
                assert!(
                    matches!(
                        error,
                        E::Store(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "direct terminal installation retained snapshot"
                        })
                    ),
                    "{fault}: {error}"
                );
                assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                assert_eq!(changes(f), before);
                pending_install::assert_empty(&node.get_pg(pg_id).unwrap());
                drop(submission);
                installation::assert_files_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_installation_rejects_foreign_section_without_history_change() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let foreign = allocation::setup_live(StagingTarget::DirectPut, false);
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let prepared = prepared!(owner, writer, section);
            let history = prepared.test_binding_state();
            let mut submission = prepared.into_submission();
            let admission = foreign.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    submission.id().pg_id(),
                    submission.id().cluster_epoch(),
                )
                .unwrap();
            let foreign_section = route.lock_until(deadline()).unwrap();
            for installed in [false, true] {
                if installed {
                    submission.install_embedded(section).unwrap();
                }
                let observation = submission.installation();
                let states: Vec<_> = [&f.map, &foreign.map]
                    .into_iter()
                    .map(|map| {
                        let pg = map
                            .node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(owner.subject.object_pg.get())
                            .unwrap();
                        (complete_pending::observe(&pg), pg.conn.total_changes())
                    })
                    .collect();
                assert!(submission.install_embedded(&foreign_section).is_err());
                assert_eq!(submission.installation(), observation);
                assert_eq!(submission.test_binding_state(), history);
                for (map, expected) in [&f.map, &foreign.map].into_iter().zip(states) {
                    let pg = map
                        .node(NodeId::new(4))
                        .unwrap()
                        .test_node()
                        .get_pg(owner.subject.object_pg.get())
                        .unwrap();
                    assert_eq!(
                        (complete_pending::observe(&pg), pg.conn.total_changes()),
                        expected
                    );
                }
            }
            drop(submission);
            installation::assert_payload_retained(f, owner);
        },
    );
}

#[test]
fn staging_direct_terminal_installation_exact_retry_validates_durable_evidence_after_lost_confirmation(
) {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for damage in [
        None,
        Some("DELETE FROM metadata_command_pending_route_dependencies"),
        Some("DELETE FROM metadata_command_pending_staging_protection"),
        Some("UPDATE metadata_command_pending_slot SET scope_bucket = NULL"),
        Some("UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'"),
    ] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                let error = PgStore::test_with_lost_complete_install_confirmation(|| {
                    submission.install_embedded(section)
                })
                .unwrap_err();
                assert!(
                    matches!(
                        error,
                        E::Store(StoreError::OperationDeadlineExceeded {
                            context: "injected lost complete installation confirmation"
                        })
                    ),
                    "{error}"
                );
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    assert_eq!(
                        pg.conn
                            .query_row(
                                "SELECT COUNT(*) FROM metadata_command_pending_slot",
                                [],
                                |r| r.get::<_, u32>(0)
                            )
                            .unwrap(),
                        1
                    );
                    if let Some(sql) = damage {
                        pg.conn.execute_batch(sql).unwrap();
                    }
                }
                let before = changes(f);
                let retry = submission.install_embedded(section);
                if damage.is_some() {
                    assert!(retry.is_err(), "damaged evidence: {damage:?}");
                    assert_eq!(submission.installation(), Observed::Unconfirmed);
                } else {
                    retry.unwrap();
                    assert_eq!(submission.installation(), Observed::Installed);
                }
                assert_eq!(changes(f), before);
                assert_eq!(submission.test_binding_state(), history);
                drop(submission);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_installation_rechecks_cutoffs_beside_insert() {
    for monotonic in [false, true] {
        let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                let before = changes(f);
                let control = clock.control();
                let cutoff = owner.windows[0].cutoff_ms;
                let error = PgStore::test_with_before_pending_slot_effect_hook(
                    move |inside| {
                        assert!(inside);
                        if monotonic {
                            std::thread::sleep(history.0.saturating_duration_since(Instant::now()));
                        } else {
                            control.set(cutoff);
                        }
                    },
                    || submission.install_embedded(section),
                )
                .unwrap_err();
                if monotonic {
                    assert!(
                        matches!(
                            error,
                            E::Store(StoreError::OperationDeadlineExceeded { .. })
                        ),
                        "{error}"
                    );
                }
                assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                assert_eq!(submission.test_binding_state(), history);
                assert_eq!(changes(f), before);
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                pending_install::assert_empty(&node.get_pg(owner.subject.object_pg.get()).unwrap());
                drop(submission);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}
