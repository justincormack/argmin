// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Occupied-slot replacement using bound descriptions. Not command-carrier or
//! recovery-admission activation, and no physical payload is written here.

use super::*;
use crate::node_runtime::pg_store::PendingMetadataCommandSlotReplaceError as ReplaceFailure;
use std::sync::atomic::Ordering;

fn successor(command: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(command.id().log_index().get() + 1).unwrap(),
        ),
        command.payload().clone(),
    )
}

fn replace(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    source: &MetadataCommandEnvelope,
    replacement: &MetadataCommandEnvelope,
) -> Result<bool, ReplaceFailure> {
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, source).unwrap();
    pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
        0,
        &bound.pending_protection(),
        replacement,
        &topology,
        Some(source.bucket_name()),
    )
}

fn pages(pg: &PgStore) -> Vec<(String, u32, u32, String)> {
    let mut rows = Vec::new();
    for table in [
        "metadata_command_pending_placed_reference_pages",
        "metadata_command_pending_reclaim_reference_pages",
    ] {
        let mut stmt = pg.conn.prepare(&format!("SELECT page_index, reference_count, hex(encoded_references) FROM {table} ORDER BY page_index")).unwrap();
        rows.extend(
            stmt.query_map([], |row| {
                Ok((table.to_owned(), row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap()
            .map(Result::unwrap),
        );
    }
    rows
}

fn load_owner(pg: &PgStore, owner: &StagingOwnerRecord) -> StagingOwnerRecord {
    let open_snapshot = pg.conn.is_autocommit();
    if open_snapshot {
        pg.conn.execute_batch("BEGIN").unwrap();
    }
    let loaded = append::load(pg, owner);
    if open_snapshot {
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
    loaded
}

#[test]
fn staging_pending_reissue_introductions_preserve_exact_scope_through_reply_loss_and_reopen() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let source = introduction::command(&owner);
        let replacement = successor(&source);
        let dir = test_util::tempdir();
        let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        install(&pg, &owner, &source).unwrap();
        let before_window = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        let before_routes = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap();
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
        pg.fail_next_pending_slot_replace_after_commit
            .store(true, Ordering::Relaxed);
        let result = replace(&pg, &owner, &source, &replacement);
        assert!(
            matches!(result, Err(ReplaceFailure::MayHaveApplied(_))),
            "{result:?}"
        );
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
        for prefix in [
            "UPDATE metadata_command_pending_slot ",
            "INSERT INTO metadata_command_pending_route_dependencies ",
            "INSERT INTO metadata_command_pending_staging_protection ",
        ] {
            assert!(
                trace
                    .iter()
                    .position(|sql| sql.starts_with(prefix))
                    .unwrap()
                    < commit,
                "{prefix}"
            );
        }
        let retained = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        assert_eq!(retained.command, replacement.id());
        assert_eq!(retained.window, before_window.window);
        let routes = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap();
        assert_eq!(routes.id, replacement.id());
        assert_eq!(routes.dependencies, before_routes.dependencies);
        let expected = observe(&pg);
        let writes = pg.conn.total_changes();
        assert!(replace(&pg, &owner, &source, &replacement).unwrap());
        assert_eq!(observe(&pg), expected);
        assert_eq!(pg.conn.total_changes(), writes);
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let writes = pg.conn.total_changes();
        assert!(replace(&pg, &owner, &source, &replacement).unwrap());
        assert_eq!(observe(&pg), expected);
        assert_eq!(pg.conn.total_changes(), writes);
        let second_reissue = successor(&replacement);
        assert!(replace(&pg, &owner, &replacement, &second_reissue).unwrap());
        let retained = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        assert_eq!(retained.command, second_reissue.id());
        assert_eq!(retained.window, before_window.window);
    }
}

#[test]
fn staging_pending_reissue_carries_the_same_full_description_not_only_indexed_fields() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let replacement = successor(&command);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, &command).unwrap();
    let original = bound.pending_protection();
    let reissued = original.reissue(&replacement).unwrap();
    let (_, old_topology, old_subject, old_window) = original.index_inputs();
    let (new_command, new_topology, new_subject, new_window) = reissued.index_inputs();
    assert_eq!(new_command, &replacement);
    assert!(std::ptr::eq(old_topology, new_topology));
    assert!(std::ptr::eq(old_subject, new_subject));
    assert!(std::ptr::eq(old_window, new_window));
    // The borrowing API retains fields omitted by the inspection index too.
    assert_eq!(new_window.cutoff_ms, owner.windows[0].cutoff_ms);
    assert_eq!(new_window.issuer, owner.subject.issuer);
    assert_eq!(new_window.geometry, owner.windows[0].geometry);
}

#[test]
fn staging_pending_reissue_append_is_atomic_and_keeps_selected_and_unused_windows() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for nested in [false, true] {
            for table in [
                "metadata_command_pending_route_dependencies",
                "metadata_command_pending_staging_protection",
            ] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get())
                    .unwrap();
                let owner = append::initialize(&pg, target.clone());
                let next = append::next(&pg, &owner);
                let source = append::command(&owner);
                let replacement = successor(&source);
                let topology = topology();
                let descriptor =
                    AppendDescriptor::capture(&owner.validate(&topology).unwrap(), next).unwrap();
                let bound = descriptor.bind(&topology, &source).unwrap();
                let protection = bound.pending_protection();
                pg.try_insert_bound_staging_pending_metadata_command_slot(
                    0,
                    &protection,
                    &topology,
                    Some(source.bucket_name()),
                    AdmittedRouteEffectFence::unbounded(source.id().cluster_epoch()),
                    None,
                )
                .unwrap();
                pg.conn.execute_batch("COMMIT").unwrap();
                let owner_before = load_owner(&pg, &owner);
                let before = observe(&pg);
                let before_pages = pages(&pg);
                assert_eq!(before_pages.len(), 1, "selected segment must be present");
                let window = pg
                    .pending_staging_protection(&topology)
                    .unwrap()
                    .unwrap()
                    .window;
                pg.conn
                    .execute_batch("CREATE TEMP TABLE caller_work (value INTEGER)")
                    .unwrap();
                if nested {
                    pg.conn
                        .execute_batch("BEGIN IMMEDIATE; INSERT INTO caller_work VALUES (1)")
                        .unwrap();
                }
                pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_reissue_index BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'reissue index failure'); END;")).unwrap();
                let replace = || {
                    pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
                        0,
                        &protection,
                        &replacement,
                        &topology,
                        Some(source.bucket_name()),
                    )
                };
                let result = replace();
                assert!(
                    matches!(result, Err(ReplaceFailure::MayHaveApplied(_))),
                    "{result:?}"
                );
                assert_eq!(observe(&pg), before, "{nested}/{table}");
                assert_eq!(pages(&pg), before_pages);
                assert_eq!(load_owner(&pg, &owner), owner_before);
                assert_eq!(pg.conn.is_autocommit(), !nested);
                if nested {
                    let count: u32 = pg
                        .conn
                        .query_row("SELECT count(*) FROM caller_work", [], |row| row.get(0))
                        .unwrap();
                    assert_eq!(count, 1);
                    pg.conn.execute_batch("COMMIT").unwrap();
                    assert_eq!(
                        observe(&pg),
                        before,
                        "outer commit must not publish a partial successor"
                    );
                }
                pg.conn
                    .execute_batch("DROP TRIGGER fail_reissue_index")
                    .unwrap();
                assert!(replace().unwrap());
                assert_eq!(
                    pg.pending_staging_protection(&topology)
                        .unwrap()
                        .unwrap()
                        .window,
                    window
                );
                assert_eq!(pages(&pg), before_pages);
                assert_eq!(load_owner(&pg, &owner), owner_before);
                let after = observe(&pg);
                let writes = pg.conn.total_changes();
                assert!(replace().unwrap());
                assert_eq!(observe(&pg), after);
                assert_eq!(pg.conn.total_changes(), writes);
            }
        }
    }
}

#[test]
fn staging_pending_reissue_rejects_corrupt_source_and_replayed_successor_indexes_without_repair() {
    for bound in [false, true] {
        for after_replacement in [false, true] {
            for fault in [
                "route absent",
                "route checksum",
                "window absent",
                "window checksum",
            ] {
                let owner = fixture(StagingTarget::StreamPut);
                let source = introduction::command(&owner);
                let replacement = successor(&source);
                let dir = test_util::tempdir();
                let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                if bound {
                    install(&pg, &owner, &source).unwrap();
                } else {
                    pg.try_insert_pending_metadata_command_slot_classified(
                        0,
                        &source,
                        Some(source.bucket_name()),
                    )
                    .unwrap();
                }
                let replace = || {
                    if bound {
                        replace(&pg, &owner, &source, &replacement)
                    } else {
                        pg.replace_pending_metadata_command_slot_for_reissue(
                            0,
                            &source,
                            &replacement,
                            Some(source.bucket_name()),
                        )
                    }
                };
                if after_replacement {
                    assert!(replace().unwrap());
                }
                let sql = match fault {
                    "route absent" => "DELETE FROM metadata_command_pending_route_dependencies",
                    "route checksum" => "UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1",
                    "window absent" => "DELETE FROM metadata_command_pending_staging_protection",
                    "window checksum" => "UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1",
                    _ => unreachable!(),
                };
                pg.conn.execute(sql, []).unwrap();
                let before = observe(&pg);
                let writes = pg.conn.total_changes();
                let result = replace();
                assert!(
                    matches!(result, Err(ReplaceFailure::MayHaveApplied(_))),
                    "{bound}/{after_replacement}/{fault}: {result:?}"
                );
                assert_eq!(observe(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            }
        }
    }
}

#[test]
fn staging_pending_reissue_ordinary_wrapper_cannot_erase_bound_protection() {
    let owner = fixture(StagingTarget::DirectPut);
    let source = introduction::command(&owner);
    let replacement = successor(&source);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &source).unwrap();
    let before = observe(&pg);
    let writes = pg.conn.total_changes();
    let result = pg.replace_pending_metadata_command_slot_for_reissue(
        0,
        &source,
        &replacement,
        Some(source.bucket_name()),
    );
    assert!(
        matches!(result, Err(ReplaceFailure::MayHaveApplied(_))),
        "{result:?}"
    );
    assert_eq!(observe(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    assert!(replace(&pg, &owner, &source, &replacement).unwrap());
}

#[test]
fn staging_pending_reissue_rejects_crossed_command_topology_and_started_publication() {
    let owner = fixture(StagingTarget::StreamPut);
    let source = introduction::command(&owner);
    let replacement = successor(&source);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &source).unwrap();
    let before = observe(&pg);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, &source).unwrap();
    let protection = bound.pending_protection();
    let foreign = PgTopology::new(&[0, 1, 2, 3, 4, 5, 6, 7, 1000]).unwrap();
    let writes = pg.conn.total_changes();
    assert!(matches!(
        pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
            0,
            &protection,
            &replacement,
            &foreign,
            Some(source.bucket_name())
        ),
        Err(ReplaceFailure::Definitive(_))
    ));
    for (epoch, pg_id, index) in [
        (
            source.id().cluster_epoch(),
            source.id().pg_id(),
            source.id().log_index(),
        ),
        (
            ClusterEpoch::new(6).unwrap(),
            source.id().pg_id(),
            replacement.id().log_index(),
        ),
        (
            source.id().cluster_epoch(),
            PgId::new(1000),
            replacement.id().log_index(),
        ),
        (
            source.id().cluster_epoch(),
            source.id().pg_id(),
            MetadataCommandLogIndex::new(replacement.id().log_index().get() + 1).unwrap(),
        ),
    ] {
        let crossed = MetadataCommandEnvelope::new(
            MetadataCommandId::new(epoch, pg_id, index),
            source.payload().clone(),
        );
        assert!(matches!(
            replace(&pg, &owner, &source, &crossed),
            Err(ReplaceFailure::Definitive(_))
        ));
    }
    let mut changed_owner = owner.clone();
    changed_owner.subject.id.attempt = crate::tests::stream_session_id("crossed-reissue");
    let crossed = successor(&introduction::command(&changed_owner));
    assert!(matches!(
        replace(&pg, &owner, &source, &crossed),
        Err(ReplaceFailure::Definitive(_))
    ));
    assert_eq!(observe(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    pg.test_mark_pending_metadata_command_publication_started(&source)
        .unwrap();
    let before = observe(&pg);
    let writes = pg.conn.total_changes();
    assert!(!replace(&pg, &owner, &source, &replacement).unwrap());
    assert_eq!(observe(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_pending_reissue_commit_failure_preserves_original_slot_and_indexes() {
    let owner = fixture(StagingTarget::DirectPut);
    let source = introduction::command(&owner);
    let replacement = successor(&source);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &source).unwrap();
    let before = observe(&pg);
    pg.conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TEMP TRIGGER fail_reissue_commit AFTER UPDATE ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
    let result = replace(&pg, &owner, &source, &replacement);
    assert!(
        matches!(result, Err(ReplaceFailure::MayHaveApplied(_))),
        "{result:?}"
    );
    assert!(pg.conn.is_autocommit());
    assert_eq!(observe(&pg), before);
    pg.conn
        .execute_batch("DROP TRIGGER fail_reissue_commit")
        .unwrap();
    assert!(replace(&pg, &owner, &source, &replacement).unwrap());
}
