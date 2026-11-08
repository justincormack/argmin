// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bound descriptor -> real pending-slot transaction. These tests do not claim
//! command-carrier activation, owner application or frontend custody transfer.

use super::super::descriptor::{AppendDescriptor, IntroductionDescriptor};
use super::*;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::pg_store::{PendingMetadataCommandSlotInsertError as Failure, PgStore};
use crate::types::AdmittedRouteEffectFence;
use std::sync::{Arc, Mutex};

#[path = "pending_staging_reissue_tests.rs"]
mod reissue;

fn install(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> Result<(), Failure> {
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, command).unwrap();
    pg.try_insert_bound_staging_pending_metadata_command_slot(
        0,
        &bound.pending_protection(),
        &topology,
        Some(command.bucket_name()),
        AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
        None,
    )
}

pub(super) fn assert_empty(pg: &PgStore) {
    for table in [
        "metadata_command_pending_slot",
        "metadata_command_pending_route_dependencies",
        "metadata_command_pending_staging_protection",
        "metadata_command_pending_placed_reference_pages",
        "metadata_command_pending_reclaim_reference_pages",
    ] {
        let count: u32 = pg
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}

pub(super) fn observe(
    pg: &PgStore,
) -> (
    Option<super::super::super::command_log::PendingMetadataCommandSlot>,
    Vec<(String, String)>,
) {
    // Include exact index bytes and hashes, not only decoded windows, so a retry
    // cannot silently repair/rewrite corrupt evidence and still pass.
    let rows = [
        ("route", "SELECT hex(encoded_dependencies), printf('%lld:%lld', command_checksum, dependency_checksum) FROM metadata_command_pending_route_dependencies"),
        ("window", "SELECT hex(encoded_protection), printf('%lld:%lld', command_checksum, protection_checksum) FROM metadata_command_pending_staging_protection"),
    ].into_iter().filter_map(|(_, sql)| pg.conn.query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?))).optional().unwrap()).collect();
    (pg.pending_metadata_command_slot_any_epoch(0).unwrap(), rows)
}

use rusqlite::OptionalExtension;

#[test]
fn staging_pending_install_introductions_are_atomic_exact_and_reopenable() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let command = introduction::command(&owner);
        let dir = test_util::tempdir();
        let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let statements = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&statements);
        pg.conn
            .trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: rusqlite::trace::TraceEvent<'_>| {
                    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
                        recorded.lock().unwrap().push(sql.to_owned());
                    }
                }),
            )
            .unwrap();
        install(&pg, &owner, &command).unwrap();
        let trace = statements.lock().unwrap().clone();
        assert_eq!(
            trace
                .iter()
                .filter(|sql| sql.as_str() == "BEGIN IMMEDIATE")
                .count(),
            1
        );
        assert_eq!(
            trace.iter().filter(|sql| sql.as_str() == "COMMIT").count(),
            1
        );
        let commit = trace.iter().position(|sql| sql == "COMMIT").unwrap();
        for table in [
            "metadata_command_pending_slot",
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            assert!(
                trace
                    .iter()
                    .position(|sql| sql.starts_with(&format!("INSERT INTO {table} ")))
                    .unwrap()
                    < commit,
                "{table}"
            );
        }
        let expected = observe(&pg);
        let writes = pg.conn.total_changes();
        install(&pg, &owner, &command).unwrap();
        assert_eq!(observe(&pg), expected);
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "exact retry must not rewrite indexes"
        );
        let topology = topology();
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
        let bound = descriptor.bind(&topology, &command).unwrap();
        let routes = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap();
        for required in bound.pending_protection().route_dependencies() {
            assert!(
                routes.dependencies.contains(&required),
                "missing {required:?}"
            );
        }
        // A stream command's ordinary reservation route remains alongside the
        // descriptor's exact object/data/release PG dependencies.
        assert_eq!(
            routes.dependencies.len(),
            bound.pending_protection().route_dependencies().len()
                + usize::from(!matches!(owner.subject.target, StagingTarget::DirectPut))
        );
        assert_eq!(
            pg.pending_staging_protection(&topology)
                .unwrap()
                .unwrap()
                .window
                .unwrap()
                .protection,
            match bound.pending_protection().reference() {
                crate::types::ShardScavengerPayloadReference::StagingProtection(value) => value,
                _ => unreachable!(),
            }
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_eq!(observe(&pg), expected);
        install(&pg, &owner, &command).unwrap();
        assert_eq!(observe(&pg), expected);
        // A newly installed reissue retains the original owner/window routes,
        // while also pinning its new carrying epoch. This is index projection,
        // not a test of recovery authorization or occupied-slot replacement.
        pg.test_clear_pending_metadata_command_slot().unwrap();
        let reissued = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(6).unwrap(),
                command.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        install(&pg, &owner, &reissued).unwrap();
        let reissued_routes = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap();
        assert!(reissued_routes
            .dependencies
            .iter()
            .any(|route| route.cluster_epoch == reissued.id().cluster_epoch()
                && route.target
                    == crate::node_runtime::pg_store::command_log::PendingRouteTarget::ExactPg(
                        command.id().pg_id()
                    )));
        for original in routes.dependencies {
            assert!(reissued_routes.dependencies.contains(&original));
        }
        let reissued_state = observe(&pg);
        let writes = pg.conn.total_changes();
        install(&pg, &owner, &reissued).unwrap();
        assert_eq!(observe(&pg), reissued_state);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_pending_install_appends_preserve_selected_and_unused_scope() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let window = append::next(&pg, &owner);
        let command = append::command(&owner);
        let topology = topology();
        let descriptor =
            AppendDescriptor::capture(&owner.validate(&topology).unwrap(), window.clone()).unwrap();
        let bound = descriptor.bind(&topology, &command).unwrap();
        let protection = bound.pending_protection();
        let owner_before = append::load(&pg, &owner);
        pg.conn.execute_batch("CREATE TEMP TRIGGER fail_append_index BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'append index failure'); END;").unwrap();
        let error = pg
            .try_insert_bound_staging_pending_metadata_command_slot(
                0,
                &protection,
                &topology,
                Some(command.bucket_name()),
                AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
                None,
            )
            .unwrap_err();
        assert!(matches!(error, Failure::Definitive(_)), "{error:?}");
        assert_empty(&pg); // includes the previously inserted placed-reference page
        assert_eq!(append::load(&pg, &owner), owner_before);
        assert!(!pg.conn.is_autocommit());
        pg.conn
            .execute_batch("DROP TRIGGER fail_append_index")
            .unwrap();
        pg.try_insert_bound_staging_pending_metadata_command_slot(
            0,
            &protection,
            &topology,
            Some(command.bucket_name()),
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
            None,
        )
        .unwrap();
        assert!(
            !pg.conn.is_autocommit(),
            "must not commit the caller's transaction"
        );
        assert_eq!(append::load(&pg, &owner), owner_before);
        pg.conn.execute_batch("COMMIT").unwrap();
        let read = pg
            .pending_staging_protection(&topology)
            .unwrap()
            .unwrap()
            .window
            .unwrap();
        assert_eq!(read.window_index, window.index);
        assert_eq!(read.protection.identity.okh, window.segment_hash);
        let page = pg
            .list_placed_shard_scavenger_reference_page(
                None,
                std::num::NonZeroU16::new(64).unwrap(),
            )
            .unwrap();
        assert!(page.complete);
        assert_eq!(page.items.len(), 1, "selected append must remain readable");
        assert!(matches!(
            page.items[0].reference,
            crate::types::ShardScavengerPayloadReference::Placed(_)
        ));
        let expected = observe(&pg);
        let changes = pg.conn.total_changes();
        pg.try_insert_bound_staging_pending_metadata_command_slot(
            0,
            &protection,
            &topology,
            Some(command.bucket_name()),
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
            None,
        )
        .unwrap();
        assert_eq!(observe(&pg), expected);
        assert_eq!(pg.conn.total_changes(), changes);
    }
}

#[test]
fn staging_pending_install_rolls_back_every_index_and_preserves_outer_transaction() {
    for nested in [false, true] {
        for table in [
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            let owner = fixture(StagingTarget::DirectPut);
            let command = introduction::command(&owner);
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            pg.conn
                .execute_batch("CREATE TEMP TABLE caller_work (value INTEGER)")
                .unwrap();
            if nested {
                pg.conn
                    .execute_batch("BEGIN IMMEDIATE; INSERT INTO caller_work VALUES (17)")
                    .unwrap();
            }
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_index BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'bound pending index failure'); END;")).unwrap();
            let error = install(&pg, &owner, &command).unwrap_err();
            assert!(matches!(&error, Failure::Definitive(_)), "{error:?}");
            assert!(error
                .into_source()
                .to_string()
                .contains("bound pending index failure"));
            assert_empty(&pg);
            assert_eq!(pg.conn.is_autocommit(), !nested);
            if nested {
                assert_eq!(
                    pg.conn
                        .query_row("SELECT value FROM caller_work", [], |row| row
                            .get::<_, u32>(0))
                        .unwrap(),
                    17
                );
                pg.conn.execute_batch("COMMIT").unwrap();
                assert_empty(&pg);
            }
            pg.conn.execute_batch("DROP TRIGGER fail_index").unwrap();
            install(&pg, &owner, &command).unwrap();
            assert!(pg
                .pending_staging_protection(&topology())
                .unwrap()
                .unwrap()
                .window
                .is_some());
        }
    }
}

#[test]
fn staging_pending_install_exact_slot_index_mismatch_preserves_acceptance_classification() {
    for fault in [
        "window absent",
        "window checksum",
        "route absent",
        "route checksum",
        "changed participants",
    ] {
        let mut owner = fixture(StagingTarget::DirectPut);
        let command = introduction::command(&owner);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        install(&pg, &owner, &command).unwrap();
        match fault {
            "window absent" => {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_pending_staging_protection",
                        [],
                    )
                    .unwrap();
            }
            "window checksum" => {
                pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1", []).unwrap();
            }
            "route absent" => {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_pending_route_dependencies",
                        [],
                    )
                    .unwrap();
            }
            "route checksum" => {
                pg.conn.execute("UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1", []).unwrap();
            }
            "changed participants" => owner.windows[0].participants.files.swap(0, 1),
            _ => unreachable!(),
        }
        let before = observe(&pg);
        let writes = pg.conn.total_changes();
        let result = install(&pg, &owner, &command);
        assert!(
            matches!(result, Err(Failure::MayHaveApplied(_))),
            "{fault}: {result:?}"
        );
        assert_eq!(observe(&pg), before, "{fault}");
        assert_eq!(pg.conn.total_changes(), writes, "{fault}");
    }
}

#[test]
fn staging_pending_install_current_exact_retry_validates_both_indexes() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for fault in [
            "window absent",
            "window checksum",
            "window command checksum",
            "window bytes",
            "window other command position",
            "valid nonempty window",
            "route absent",
            "route checksum",
            "route command checksum",
            "route bytes",
        ] {
            let owner = fixture(target.clone());
            let command = introduction::command(&owner);
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            // Exercise the production current(command) projection, not the
            // owner-local bound installer. Pin an unchanged successful retry.
            let retry = |pg: &PgStore| {
                pg.try_insert_pending_metadata_command_slot_classified(
                    0,
                    &command,
                    Some(command.bucket_name()),
                )
            };
            retry(&pg).unwrap();
            let healthy = observe(&pg);
            let writes = pg.conn.total_changes();
            retry(&pg).unwrap();
            assert_eq!(observe(&pg), healthy);
            assert_eq!(pg.conn.total_changes(), writes);

            match fault {
                "window absent" => {
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_staging_protection",
                            [],
                        )
                        .unwrap();
                }
                "window checksum" => {
                    pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1", []).unwrap();
                }
                "window command checksum" => {
                    pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET command_checksum = command_checksum + 1", []).unwrap();
                }
                "window bytes" => {
                    pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET encoded_protection = x'01'", []).unwrap();
                }
                "window other command position" => {
                    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    pg.initialize_pending_staging_index(
                        MetadataCommandId::new(
                            command.id().cluster_epoch(),
                            command.id().pg_id(),
                            MetadataCommandLogIndex::new(command.id().log_index().get() + 1)
                                .unwrap(),
                        ),
                        command.checksum_crc64(),
                    )
                    .unwrap();
                    pg.conn.execute_batch("COMMIT").unwrap();
                }
                "valid nonempty window" => {
                    let topology = topology();
                    let descriptor =
                        IntroductionDescriptor::capture(&owner.validate(&topology).unwrap())
                            .unwrap();
                    let bound = descriptor.bind(&topology, &command).unwrap();
                    let protection = bound.pending_protection();
                    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    super::super::super::pending_staging_index::PendingStagingIndex::from_bound(
                        &protection,
                    )
                    .install_in_open_transaction(&pg, &topology)
                    .unwrap();
                    pg.conn.execute_batch("COMMIT").unwrap();
                    // A valid same-command nonempty row must still fail the
                    // ordinary projection's empty-index requirement. Routes
                    // remain untouched, so they cannot mask this check.
                    assert!(pg
                        .pending_staging_protection(&topology)
                        .unwrap()
                        .unwrap()
                        .window
                        .is_some());
                }
                "route absent" => {
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_route_dependencies",
                            [],
                        )
                        .unwrap();
                }
                "route checksum" => {
                    pg.conn.execute("UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1", []).unwrap();
                }
                "route command checksum" => {
                    pg.conn.execute("UPDATE metadata_command_pending_route_dependencies SET command_checksum = command_checksum + 1", []).unwrap();
                }
                "route bytes" => {
                    // Preserve SQL framing but corrupt the encoded epoch.
                    pg.conn.execute("UPDATE metadata_command_pending_route_dependencies SET dependency_count = 1, encoded_dependencies = zeroblob(17)", []).unwrap();
                }
                _ => unreachable!(),
            }
            let before = observe(&pg);
            let check_retry = |pg: &PgStore| {
                let writes = pg.conn.total_changes();
                let result = retry(pg);
                assert!(
                    matches!(result, Err(Failure::MayHaveApplied(_))),
                    "{target:?}/{fault}: {result:?}"
                );
                assert_eq!(observe(pg), before, "{target:?}/{fault}");
                assert_eq!(pg.conn.total_changes(), writes, "{target:?}/{fault}");
            };
            check_retry(&pg);
            drop(pg);
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            check_retry(&pg);
        }
    }
}

#[test]
fn staging_pending_install_crossed_topology_and_contender_do_not_mutate() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, &command).unwrap();
    let protection = bound.pending_protection();
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    let foreign = PgTopology::new(&[0, 1, 2, 3, 4, 5, 6, 7, 1000]).unwrap();
    let writes = pg.conn.total_changes();
    assert!(matches!(
        pg.try_insert_bound_staging_pending_metadata_command_slot(
            0,
            &protection,
            &foreign,
            None,
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
            None
        ),
        Err(Failure::Definitive(_))
    ));
    assert_empty(&pg);
    assert_eq!(pg.conn.total_changes(), writes);
    let contender = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(12).unwrap(),
        ),
        command.payload().clone(),
    );
    pg.try_insert_pending_metadata_command_slot(0, &contender, None)
        .unwrap();
    let before = observe(&pg);
    let writes = pg.conn.total_changes();
    assert!(matches!(
        install(&pg, &owner, &command),
        Err(Failure::Definitive(
            crate::StoreError::MetadataCommandPendingConflict { .. }
        ))
    ));
    assert_eq!(observe(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_pending_install_checks_captured_fence_at_first_effect() {
    let time = crate::clock::test_time_override_guard(1_000);
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, &command).unwrap();
    let protection = bound.pending_protection();
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    let fence = AdmittedRouteEffectFence::bounded(command.id().cluster_epoch(), 5_000, 4_000);
    let changes = pg.conn.total_changes();
    let replica_state = pg.metadata_command_replica_state().unwrap();
    let control = time.control();
    let result = PgStore::test_with_before_pending_slot_effect_hook(
        move |in_tx| {
            assert!(in_tx);
            control.set(4_500);
        },
        || {
            pg.try_insert_bound_staging_pending_metadata_command_slot(
                0,
                &protection,
                &topology,
                None,
                fence,
                None,
            )
        },
    );
    assert!(matches!(result, Err(Failure::Definitive(_))), "{result:?}");
    assert_empty(&pg);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), replica_state);
    assert_eq!(
        pg.conn.total_changes(),
        changes,
        "must reject before INSERT, not merely roll back"
    );
    time.set(1_000);
    pg.try_insert_bound_staging_pending_metadata_command_slot(
        0,
        &protection,
        &topology,
        None,
        fence,
        None,
    )
    .unwrap();
    assert!(pg
        .pending_staging_protection(&topology)
        .unwrap()
        .unwrap()
        .window
        .is_some());
}

#[test]
fn staging_pending_install_commit_failure_is_uncertain_and_rolls_back_indexes() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    pg.conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TEMP TRIGGER fail_commit_after_index AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
    let error = install(&pg, &owner, &command).unwrap_err();
    assert!(matches!(&error, Failure::MayHaveApplied(_)), "{error:?}");
    assert!(error
        .into_source()
        .to_string()
        .contains("commit pending metadata command slot transaction"));
    assert!(pg.conn.is_autocommit());
    assert_empty(&pg);
    pg.conn
        .execute_batch("DROP TRIGGER fail_commit_after_index")
        .unwrap();
    install(&pg, &owner, &command).unwrap();
}
