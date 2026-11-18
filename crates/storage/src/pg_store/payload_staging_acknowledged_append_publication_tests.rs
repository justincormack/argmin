// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, PublicationObservation as Published,
};
use crate::node_runtime::traits::PgMetadataStore;

#[path = "payload_staging_acknowledged_append_terminal_tests.rs"]
mod terminal;

fn marker(pg: &PgStore) -> bool {
    pg.conn
        .query_row(
            "SELECT publication_started FROM metadata_command_pending_slot",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

fn segment_count(f: &Setup, owner: &StagingOwnerRecord, node: u32) -> usize {
    f.map
        .node(NodeId::new(node))
        .unwrap()
        .test_node()
        .get_pg(owner.subject.object_pg.get())
        .unwrap()
        .list_stream_segments(&owner.subject.id.attempt)
        .unwrap()
        .len()
}

#[test]
fn staging_acknowledged_append_publication_consumes_only_confirmed_installation() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        with_writer_and_section(target, false, |f, owner, mut writer, section| {
            let prepared = prepared!(owner, writer, section);
            let history = prepared.test_binding_state();
            let mut submission = prepared.into_submission();
            for uncertain in [false, true] {
                if uncertain {
                    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        PgStore::test_with_before_pending_slot_effect_hook(
                            |_| panic!("append installation interrupted"),
                            || submission.install_embedded(section),
                        )
                    }))
                    .is_err());
                }
                let before = submission.installation();
                let failure = match submission.into_publication() {
                    Ok(_) => panic!("unconfirmed installation published"),
                    Err(failure) => failure,
                };
                assert!(failure
                    .error()
                    .to_string()
                    .contains("requires confirmed installation"));
                submission = failure.into_submission();
                assert_eq!(submission.installation(), before);
                assert_eq!(submission.test_binding_state(), history);
                assert_payload_retained(f, owner);
            }
            submission.install_embedded(section).unwrap();
            let id = submission.id();
            let pg_id = id.pg_id().get();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let before = node.get_pg(pg_id).unwrap().conn.total_changes();
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
            assert_eq!(
                publication.test_append_installation().unwrap().command_id,
                id
            );
            assert_eq!(publication.test_append_installation().unwrap().installation, crate::node_runtime::pg_store::payload_staging::submission::InstallationObservation::Installed);
            assert_eq!(publication.publication(), Published::NoAttemptObserved);
            assert_eq!(publication.test_binding_state(), history);
            assert!(publication.test_cutoff().0 <= history.0);
            assert!(
                publication.test_cutoff().1 <= crate::clock::current_time_millis() + 5_000,
                "handoff must not recompute a later wall cutoff"
            );
            assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), before);
            assert!(!marker(&node.get_pg(pg_id).unwrap()));
            drop(publication);
            assert_payload_retained(f, owner);
        });
    }
}

#[test]
fn staging_acknowledged_append_publication_and_fanout_keep_exact_payload_on_every_replica() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            with_writer_and_section(target.clone(), appended, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let command = prepared.test_command();
                let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(
                    append_command,
                ) = command.payload()
                else {
                    unreachable!()
                };
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
                    let node = f.map.node(NodeId::new(member)).unwrap().test_node();
                    let id = publication.id();
                    let state = publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                    assert_eq!(state.applied_log_index, id.log_index().get());
                    assert_eq!(
                        publication.application(NodeId::new(member)),
                        Applied::Applied
                    );
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    let segments = pg.list_stream_segments(&owner.subject.id.attempt).unwrap();
                    assert_eq!(segments.last().unwrap(), &append_command.segment);
                    assert_eq!(segments.len(), usize::from(appended) + 1);
                    let snapshot = pg.command_build_snapshot().unwrap();
                    let advanced = append::load(&pg, owner);
                    snapshot.finish().unwrap();
                    assert_eq!(advanced.subject, owner.subject);
                    assert_eq!(advanced.resources, owner.resources);
                    assert_eq!(advanced.windows.len(), owner.windows.len() + 1);
                    drop(pg);
                    let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
                    publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                    assert_eq!(
                        node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                        writes
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
                let primary = f.map.node(NodeId::new(4)).unwrap().test_node();
                assert!(
                    marker(&primary.get_pg(owner.subject.object_pg.get()).unwrap()),
                    "no terminal slot retirement in this slice"
                );
                drop(publication);
                assert_files_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_publication_lost_reply_and_unwind_preserve_history() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for (at, unwind) in [
            ("before-commit", false),
            ("after-commit", false),
            ("after-commit", true),
        ] {
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let mut submission = prepared.into_submission();
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
                            assert!(!unwind, "append publication reply lost on unwind");
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "append marker reply lost",
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
                        .contains("append marker reply lost"));
                }
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(publication.installation(), Observed::Installed);
                let pg_id = owner.subject.object_pg.get();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                assert_eq!(marker(&node.get_pg(pg_id).unwrap()), at == "after-commit");
                assert!(publication.apply_embedded(section, NodeId::new(0)).is_err());
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(publication.publish_embedded(section).is_err());
                    },
                );
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(publication.test_cutoff(), cutoff);
                publication.publish_embedded(section).unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(publication.publish_embedded(section).is_err());
                    },
                );
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                drop(publication);
                assert_payload_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_publication_rejects_foreign_section_and_damaged_slot() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for damage in ["foreign", "scope", "index"] {
            let foreign = allocation::setup_live(target.clone(), false);
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let mut submission = prepared.into_submission();
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
                let primary = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = publication.id().pg_id().get();
                {
                    let pg = primary.get_pg(pg_id).unwrap();
                    match damage {
                        "scope" => pg.conn.execute_batch("UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-bucket'").unwrap(),
                        "index" => pg.conn.execute_batch("DELETE FROM metadata_command_pending_staging_protection").unwrap(),
                        "foreign" => (),
                        _ => unreachable!(),
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
                assert_eq!(publication.installation(), Observed::Installed);
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
                assert_payload_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_fanout_lost_witness_reply_blocks_primary_until_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for unwind in [false, true] {
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let mut submission = prepared.into_submission();
                submission.install_embedded(section).unwrap();
                let mut publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                publication.publish_embedded(section).unwrap();
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    publication.test_apply(section, NodeId::new(0), |stage| {
                        if stage == "after-commit" {
                            reached = true;
                            assert!(!unwind, "append apply reply lost on unwind");
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "append apply reply lost",
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
                        .contains("append apply reply lost"));
                }
                assert_eq!(
                    publication.application(NodeId::new(0)),
                    Applied::Unconfirmed
                );
                assert_eq!(segment_count(f, owner, 0), 1);
                assert_eq!(segment_count(f, owner, 4), 0);
                assert!(publication.apply_embedded(section, NodeId::new(4)).is_err());
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(publication.apply_embedded(section, NodeId::new(0)).is_err());
                    },
                );
                assert_eq!(
                    publication.application(NodeId::new(0)),
                    Applied::Unconfirmed
                );
                let witness = f.map.node(NodeId::new(0)).unwrap().test_node();
                let changes = witness
                    .get_pg(owner.subject.object_pg.get())
                    .unwrap()
                    .conn
                    .total_changes();
                publication.apply_embedded(section, NodeId::new(0)).unwrap();
                assert_eq!(
                    witness
                        .get_pg(owner.subject.object_pg.get())
                        .unwrap()
                        .conn
                        .total_changes(),
                    changes
                );
                for member in [4, 2, 3, 1] {
                    publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                }
                for member in [0, 4, 2, 3, 1] {
                    assert_eq!(
                        publication.application(NodeId::new(member)),
                        Applied::Applied
                    );
                    assert_eq!(segment_count(f, owner, member), 1);
                }
                drop(publication);
                assert_files_retained(f, owner);
            });
        }
    }
}
