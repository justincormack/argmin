// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, PublicationObservation as Published,
};

#[path = "payload_staging_direct_terminal_retire_tests.rs"]
mod terminal_tests;

fn marker(pg: &PgStore) -> bool {
    pg.conn
        .query_row(
            "SELECT publication_started FROM metadata_command_pending_slot",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn staging_direct_terminal_publication_consumes_only_confirmed_installation_and_keeps_cutoff() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let prepared = prepared!(owner, writer, section);
            let history = prepared.test_binding_state();
            let mut submission = prepared.into_submission();
            for uncertain in [false, true] {
                if uncertain {
                    assert!(PgStore::test_with_lost_complete_install_confirmation(
                        || submission.install_embedded(section)
                    )
                    .is_err());
                }
                let before = submission.installation();
                let failure = match submission.into_publication() {
                    Ok(_) => panic!("unconfirmed publication"),
                    Err(e) => e,
                };
                assert!(failure
                    .error()
                    .to_string()
                    .contains("requires confirmed installation"));
                submission = failure.into_submission();
                assert_eq!(submission.installation(), before);
                assert_eq!(submission.test_binding_state(), history);
                installation::assert_payload_retained(f, owner);
            }
            submission.install_embedded(section).unwrap();
            let id = submission.id();
            let before = changes(f);
            let publication = crate::clock::with_time_override(
                crate::clock::current_time_millis() + 3_000,
                || {
                    submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()))
                },
            );
            assert_eq!(publication.id(), id);
            assert_eq!(publication.installation(), Observed::Installed);
            let observation = publication.test_installation().unwrap();
            assert_eq!(observation.command_id, id);
            assert_eq!(observation.installation, crate::node_runtime::pg_store::payload_staging::submission::InstallationObservation::Installed);
            assert_eq!(publication.publication(), Published::NoAttemptObserved);
            assert_eq!(publication.test_binding_state(), history);
            assert!(publication.test_cutoff().0 <= history.0);
            assert!(publication.test_cutoff().1 <= owner.windows[0].cutoff_ms);
            assert_eq!(changes(f), before);
            drop(publication);
            installation::assert_payload_retained(f, owner);
        },
    );
}

#[test]
fn staging_direct_terminal_publication_fanout_transfers_exact_object_references_on_every_replica() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for versioning in [
        BucketVersioningState::Disabled,
        BucketVersioningState::Suspended,
        BucketVersioningState::Enabled,
    ] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let witnessed = witnessed!(owner, writer, section);
                let mut input = input(owner);
                input.versioning = versioning;
                if versioning == BucketVersioningState::Enabled {
                    input.version_id = VersionId::from_u64(100);
                }
                let prepared = witnessed
                    .prepare_terminal_embedded(section, &input, &evidence(owner))
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let command = prepared.test_command();
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                submission.install_embedded(section).unwrap();
                let mut publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert!(publication.apply_embedded(section, NodeId::new(0)).is_err());
                assert_eq!(
                    publication.application(NodeId::new(0)),
                    Applied::NoAttemptObserved
                );
                publication.publish_embedded(section).unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                installation::assert_payload_retained(f, owner); // marker trial must roll back ownership effects
                for premature in [4, 2] {
                    assert!(publication
                        .apply_embedded(section, NodeId::new(premature))
                        .is_err());
                    assert_eq!(
                        publication.application(NodeId::new(premature)),
                        Applied::NoAttemptObserved
                    );
                }
                for member in [0, 4, 2, 3, 1] {
                    let id = publication.id();
                    let state = publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                    assert_eq!(state.applied_log_index, id.log_index().get());
                    assert_eq!(
                        publication.application(NodeId::new(member)),
                        Applied::Applied
                    );
                    let node = f.map.node(NodeId::new(member)).unwrap().test_node();
                    let before = {
                        let pg = node.get_pg(id.pg_id().get()).unwrap();
                        object::assert_object(&pg, &command);
                        let snapshot = pg.command_build_snapshot().unwrap();
                        assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                        snapshot.finish().unwrap();
                        pg.conn.total_changes()
                    };
                    publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                    assert_eq!(
                        node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                        before
                    );
                }
                assert!(publication
                    .apply_embedded(section, NodeId::new(99))
                    .is_err());
                assert_eq!(
                    publication.application(NodeId::new(99)),
                    Applied::NoAttemptObserved
                );
                assert_eq!(publication.test_binding_state(), history);
                assert!(marker(
                    &f.map
                        .node(NodeId::new(4))
                        .unwrap()
                        .test_node()
                        .get_pg(owner.subject.object_pg.get())
                        .unwrap()
                ));
                drop(publication);
                installation::assert_files_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_publication_marker_failure_and_unwind_keep_custody() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for (at, unwind) in [
        ("before-commit", false),
        ("after-commit", false),
        ("after-commit", true),
    ] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let mut submission = prepared!(owner, writer, section).into_submission();
                submission.install_embedded(section).unwrap();
                let mut publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let cutoff = publication.test_cutoff();
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    publication.test_publish(section, |stage| {
                        if stage == at {
                            reached = true;
                            assert!(!unwind, "lost marker confirmation");
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "lost direct marker confirmation",
                            });
                        }
                        Ok(())
                    })
                }));
                assert!(reached);
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("lost direct marker confirmation"));
                }
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(publication.installation(), Observed::Installed);
                assert_eq!(
                    marker(
                        &f.map
                            .node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(owner.subject.object_pg.get())
                            .unwrap()
                    ),
                    at == "after-commit"
                );
                assert!(publication.apply_embedded(section, NodeId::new(0)).is_err());
                crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                    assert!(publication.publish_embedded(section).is_err())
                });
                assert_eq!(publication.publication(), Published::Unconfirmed);
                publication.publish_embedded(section).unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                assert_eq!(publication.test_cutoff(), cutoff);
                crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                    assert!(publication.publish_embedded(section).is_err())
                });
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                drop(publication);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_publication_rejects_foreign_section_and_damaged_slot() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for damage in ["foreign", "scope", "index"] {
        let foreign = allocation::setup_live(StagingTarget::DirectPut, false);
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let mut submission = prepared!(owner, writer, section).into_submission();
                submission.install_embedded(section).unwrap();
                let mut publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let admission = foreign.handle.admit_current_route().unwrap();
                let route = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        publication.id().pg_id(),
                        publication.id().cluster_epoch(),
                    )
                    .unwrap();
                let foreign_section = route.lock_until(deadline()).unwrap();
                let pg_id = owner.subject.object_pg.get();
                let primary = f.map.node(NodeId::new(4)).unwrap().test_node();
                {
                    let pg = primary.get_pg(pg_id).unwrap();
                    match damage {
                        "scope" => pg
                            .conn
                            .execute_batch(
                                "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'",
                            )
                            .unwrap(),
                        "index" => pg
                            .conn
                            .execute_batch(
                                "DELETE FROM metadata_command_pending_staging_protection",
                            )
                            .unwrap(),
                        _ => (),
                    }
                }
                let states: Vec<_> = [&f.map, &foreign.map]
                    .into_iter()
                    .map(|map| {
                        let pg = map
                            .node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(pg_id)
                            .unwrap();
                        (
                            complete_pending::observe(&pg),
                            pg.metadata_command_replica_state().unwrap(),
                        )
                    })
                    .collect();
                let selected = if damage == "foreign" {
                    &foreign_section
                } else {
                    section
                };
                assert!(publication.publish_embedded(selected).is_err());
                assert_eq!(
                    publication.publication(),
                    if damage == "foreign" {
                        Published::NoAttemptObserved
                    } else {
                        Published::Unconfirmed
                    }
                );
                assert!(publication
                    .apply_embedded(selected, NodeId::new(0))
                    .is_err());
                assert_eq!(
                    publication.application(NodeId::new(0)),
                    Applied::NoAttemptObserved
                );
                for (map, expected) in [&f.map, &foreign.map].into_iter().zip(states) {
                    let pg = map
                        .node(NodeId::new(4))
                        .unwrap()
                        .test_node()
                        .get_pg(pg_id)
                        .unwrap();
                    assert_eq!(
                        (
                            complete_pending::observe(&pg),
                            pg.metadata_command_replica_state().unwrap()
                        ),
                        expected
                    );
                }
                assert!(!marker(&primary.get_pg(pg_id).unwrap()));
                drop(publication);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_fanout_lost_confirmation_blocks_later_members_until_exact_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for (target, at, unwind) in [
        (0, "before-commit", false),
        (0, "after-commit", false),
        (0, "after-commit", true),
        (4, "before-commit", false),
        (4, "after-commit", false),
        (4, "after-commit", true),
    ] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let command = prepared.test_command();
                let mut submission = prepared.into_submission();
                submission.install_embedded(section).unwrap();
                let mut publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                publication.publish_embedded(section).unwrap();
                if target == 4 {
                    publication.apply_embedded(section, NodeId::new(0)).unwrap();
                }
                let target = NodeId::new(target);
                let blocked = NodeId::new(if target == NodeId::new(0) { 4 } else { 1 });
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    publication.test_apply(section, target, |stage| {
                        if stage == at {
                            reached = true;
                            assert!(!unwind, "lost apply confirmation");
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "lost direct apply confirmation",
                            });
                        }
                        Ok(())
                    })
                }));
                assert!(reached);
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("lost direct apply confirmation"));
                }
                assert_eq!(publication.application(target), Applied::Unconfirmed);
                assert!(publication.apply_embedded(section, blocked).is_err());
                assert_eq!(publication.application(blocked), Applied::NoAttemptObserved);
                let witness = f.map.node(target).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                let writes = {
                    let pg = witness.get_pg(pg_id).unwrap();
                    let snapshot = pg.command_build_snapshot().unwrap();
                    assert_eq!(
                        append::load(&pg, owner),
                        if at == "after-commit" {
                            object::expected(owner, &command)
                        } else {
                            owner.clone()
                        }
                    );
                    snapshot.finish().unwrap();
                    pg.conn.total_changes()
                };
                crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                    assert!(publication.apply_embedded(section, target).is_err())
                });
                assert_eq!(publication.application(target), Applied::Unconfirmed);
                publication.apply_embedded(section, target).unwrap();
                if at == "after-commit" {
                    assert_eq!(witness.get_pg(pg_id).unwrap().conn.total_changes(), writes);
                }
                for node in [0, 4, 1, 2, 3] {
                    publication
                        .apply_embedded(section, NodeId::new(node))
                        .unwrap();
                }
                for node in 0..5 {
                    object::assert_object(
                        &f.map
                            .node(NodeId::new(node))
                            .unwrap()
                            .test_node()
                            .get_pg(pg_id)
                            .unwrap(),
                        &command,
                    );
                }
                drop(publication);
                installation::assert_files_retained(f, owner);
            },
        );
    }
}
