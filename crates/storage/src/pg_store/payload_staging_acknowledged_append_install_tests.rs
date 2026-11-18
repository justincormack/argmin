// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real acknowledged ciphertext -> captured append -> owning installation.
//! No fanout, terminal publication, or live publisher activation is claimed.
use super::*;
use crate::node_runtime::pg_store::StreamAppendInstallationObservation as Observed;

macro_rules! prepared {
    ($owner:expr, $writer:ident, $section:expr) => {{
        for (i, bytes) in files::shards($owner, b"data").iter().enumerate() {
            $writer.write(i as u8, bytes);
        }
        $writer.persist().unwrap();
        let Writer::Stream(writer) = $writer else {
            unreachable!()
        };
        writer
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()))
            .prepare_append_embedded($section)
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[path = "payload_staging_acknowledged_append_publication_tests.rs"]
mod publication;

pub(super) fn assert_payload_retained(f: &Setup, owner: &StagingOwnerRecord) {
    assert_eq!(custody(f, owner), *owner);
    assert_files_retained(f, owner);
}

pub(super) fn assert_files_retained(f: &Setup, owner: &StagingOwnerRecord) {
    let window = owner.windows.last().unwrap();
    for (index, bytes) in files::shards(owner, b"data").iter().enumerate() {
        let key = ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
        let p = &window.participants;
        assert_eq!(
            f.map
                .node(p.files[index])
                .unwrap()
                .test_node()
                .read_shard_file(p.data_pg.get(), &key)
                .unwrap(),
            *bytes
        );
        f.map
            .node(p.acknowledgement_primary)
            .unwrap()
            .test_node()
            .get_pg(p.data_pg.get())
            .unwrap()
            .validate_written_shard_ack(
                &key,
                WriteAck {
                    stored_size: bytes.len() as u64,
                    crc64: checksum::crc64::checksum(bytes),
                },
            )
            .unwrap();
    }
}

#[test]
fn staging_acknowledged_append_installation_preserves_exact_custody_and_confirmation() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            with_writer_and_section(target.clone(), appended, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let command = prepared.test_command();
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                assert_eq!(submission.id(), command.id());
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                let prefix = node
                    .get_pg(pg_id)
                    .unwrap()
                    .metadata_command_replica_state()
                    .unwrap();
                submission.install_embedded(section).unwrap();
                assert_eq!(submission.installation(), Observed::Installed);
                assert_eq!(submission.test_binding_state(), history);
                let (before, changes) = {
                    let pg = node.get_pg(pg_id).unwrap();
                    let (bytes, checksum, scope): (Vec<u8>, i64, String) = pg.conn.query_row(
                        "SELECT command_bytes, command_checksum, scope_bucket FROM metadata_command_pending_slot",
                        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
                    let context =
                        ContextualCommand::decode_command(&bytes, checksum as u64, &topology())
                            .unwrap();
                    context.assert_command_for_test(&command);
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
                        .is_some());
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                    (complete_pending::observe(&pg), pg.conn.total_changes())
                };
                submission.install_embedded(section).unwrap();
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    assert_eq!(complete_pending::observe(&pg), before);
                    assert_eq!(pg.conn.total_changes(), changes);
                    // Confirmed history is not an instruction to reinstall or
                    // repair the slot. A later publication must validate it.
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
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(submission.install_embedded(section).is_err());
                    },
                );
                assert_eq!(submission.installation(), Observed::Installed);
                drop(submission);
                assert_payload_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_installation_uncertainty_survives_rejection_and_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for unwind in [false, true] {
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                if unwind {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        PgStore::test_with_before_pending_slot_effect_hook(
                            |in_transaction| {
                                assert!(in_transaction);
                                panic!("interrupted append install");
                            },
                            || submission.install_embedded(section),
                        )
                    }));
                    assert!(result.is_err());
                } else {
                    node.get_pg(pg_id).unwrap().conn.execute_batch(
                        "CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
                         CREATE TEMP TRIGGER fail_commit_after_index AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;"
                    ).unwrap();
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
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    pending_install::assert_empty(&pg);
                }
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(submission.install_embedded(section).is_err());
                    },
                );
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                assert_eq!(submission.test_binding_state(), history);
                assert_payload_retained(f, owner);
                submission.install_embedded(section).unwrap();
                assert_eq!(submission.installation(), Observed::Installed);
                drop(submission);
                assert_payload_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_installation_rejects_foreign_section_without_history_change() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let foreign = allocation::setup_live(target.clone(), false);
        with_writer_and_section(target, false, |f, owner, mut writer, section| {
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
                let before = submission.installation();
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
                assert_eq!(submission.installation(), before);
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
            assert_payload_retained(f, owner);
        });
    }
}

#[test]
fn staging_acknowledged_append_installation_rechecks_cutoffs_at_sql_effect() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for monotonic in [false, true] {
            let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let prepared = prepared!(owner, writer, section);
                let history = prepared.test_binding_state();
                let mut submission = prepared.into_submission();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                let before = node.get_pg(pg_id).unwrap().conn.total_changes();
                let now = crate::clock::current_time_millis();
                let control = clock.control();
                let result = PgStore::test_with_before_pending_slot_effect_hook(
                    move |in_transaction| {
                        assert!(in_transaction);
                        if monotonic {
                            std::thread::sleep(
                                history
                                    .0
                                    .saturating_duration_since(std::time::Instant::now()),
                            );
                        } else {
                            control.set(now + 60_000);
                        }
                    },
                    || submission.install_embedded(section),
                );
                let error = result.unwrap_err();
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
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    pending_install::assert_empty(&pg);
                    assert_eq!(pg.conn.total_changes(), before);
                }
                drop(submission);
                assert_payload_retained(f, owner);
            });
        }
    }
}
