// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only preparation scopes, not authenticated installation or cleanup.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::candidate::CandidateBindingError;
use super::super::command_context::{
    ContextError, ContextualCommand, PendingContextPreparation, PendingInventoryObservation,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::StoreError;

fn read_only(pg: &PgStore) -> bool {
    pg.conn
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .unwrap()
}

fn ordinary(command: &MetadataCommandEnvelope) -> ContextualCommand {
    application::ordinary_context(command, &topology()).unwrap()
}

fn canary(pg: &PgStore) {
    // Isolate connection-mode assertions from production metadata/digests.
    pg.conn.execute_batch("CREATE TABLE preparation_canary(value INTEGER NOT NULL); INSERT INTO preparation_canary VALUES(1)").unwrap();
}

fn assert_write_blocked(pg: &PgStore) {
    assert!(read_only(pg));
    let error = pg
        .conn
        .execute("UPDATE preparation_canary SET value = 2", [])
        .unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::ReadOnly)
    );
}

#[test]
fn staging_context_preparation_exports_only_scalar_inventory_observations() {
    // Pin the real production accessor's return type, not a separate test
    // facade or a method-name scan. Exhaustive destructuring below also makes
    // additions to this observation's fields an explicit API-review decision.
    fn observe(prepared: &PendingContextPreparation<'_>) -> PendingInventoryObservation {
        prepared.inventory()
    }
    let _clock = test_time_override_guard(2_000);
    for finish in [false, true] {
        for (target, is_append) in candidate::cases() {
            let (_dir, pg, owner, command) = candidate::setup(target, is_append);
            // Compute the fixture's obligations from its inputs, not an exact
            // retention projection obtained through the context under test.
            assert_eq!(
                command.id().cluster_epoch(),
                owner.subject.originating_epoch
            );
            let mut exact_pgs = vec![
                owner.subject.object_pg.get(),
                owner.subject.data_pg(&topology(), u32::from(is_append)),
            ];
            if !is_append {
                exact_pgs.push(topology().bucket_pg_for(&owner.subject.bucket));
            }
            exact_pgs.sort_unstable();
            exact_pgs.dedup();
            // A create's bucket-hash proof route is distinct from an exact PG
            // route, even when both resolve to the same configured PG.
            let expected_routes = exact_pgs.len()
                + usize::from(matches!(
                    command.payload(),
                    MetadataCommandPayload::CreateStreamUpload(_)
                ));
            let context = ContextualCommand::described(
                candidate::capture(&pg, &owner, command, is_append),
                &topology(),
            )
            .unwrap();
            assert!(expected_routes > 0);
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let prepared = context.prepare_pending(&pg, &topology()).unwrap();
            let observation = observe(&prepared);
            if finish {
                drop(prepared.finish().unwrap());
            } else {
                drop(prepared);
            }
            // The context and snapshot are now gone. Only harmless scalar
            // diagnostics survive, not pages or cloneable reference subjects.
            let PendingInventoryObservation {
                payload_reference_count,
                route_dependency_count,
                has_future_window,
            } = observation;
            let scalars: (usize, usize, bool) = (
                payload_reference_count,
                route_dependency_count,
                has_future_window,
            );
            assert_eq!(scalars, (usize::from(is_append), expected_routes, true));
            assert!(pg.conn.is_autocommit());
            assert!(!read_only(&pg));
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_context_preparation_retains_all_descriptors_and_one_read_only_snapshot() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (dir, pg, owner, command) = candidate::setup(target, is_append);
        canary(&pg);
        let context = ContextualCommand::described(
            candidate::capture(&pg, &owner, command.clone(), is_append),
            &topology(),
        )
        .unwrap();
        let field = context.encode_field().unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let prefix = pg.metadata_command_replica_state().unwrap();
        let prepared = context.prepare_pending(&pg, &topology()).unwrap();
        assert!(!pg.conn.is_autocommit());
        assert_write_blocked(&pg);
        assert_eq!(prepared.id(), command.id());
        assert_eq!(prepared.prefix(), &prefix);
        // Introduction inventory has no session read to pin a deferred
        // transaction. The preparation's prefix read must already have done so.
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        writer
            .execute("UPDATE preparation_canary SET value = 2", [])
            .unwrap();
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM preparation_canary", [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            1
        );
        let observation = prepared.inventory();
        assert!(observation.has_future_window);
        assert_eq!(observation.payload_reference_count, usize::from(is_append));
        let inventory = prepared.inventory_for_test();
        assert_eq!(inventory.retention().command_id(), command.id());
        assert!(inventory.retention().future_window().is_some());
        assert_eq!(inventory.payload().len(), usize::from(is_append));
        assert_eq!(
            inventory
                .encode_reference_pages()
                .unwrap()
                .placed_reference_count,
            u32::from(is_append)
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        let context = prepared.finish().unwrap();
        assert!(pg.conn.is_autocommit());
        assert!(!read_only(&pg));
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM preparation_canary", [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            2
        );
        context.assert_command_for_test(&command);
        assert_eq!(context.encode_field().unwrap(), field);
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn
            .execute("UPDATE preparation_canary SET value = 2", [])
            .unwrap();
    }
}

#[test]
fn staging_context_preparation_preserves_ordinary_and_abandonment_variants() {
    for count in [0, 2] {
        for source_bound in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owner = part::initialize(&pg, count);
            let source = part::command(&pg, &owner);
            let command = abandonment::cleanup(&pg, &owner, &source);
            assert_ne!(
                topology().bucket_pg_for(&owner.subject.bucket),
                owner.subject.object_pg.get()
            );
            let context = if source_bound {
                ContextualCommand::abandoned_part(
                    PartFinalizationSource::capture(&source)
                        .unwrap()
                        .bind_cleanup(command.clone(), &topology())
                        .unwrap(),
                    &topology(),
                )
                .unwrap()
            } else {
                ordinary(&command)
            };
            let field = context.encode_field().unwrap();
            pg.conn.execute_batch("COMMIT").unwrap();
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let prepared = context.prepare_pending(&pg, &topology()).unwrap();
            assert_eq!(prepared.inventory().payload_reference_count, count);
            assert!(!prepared.inventory().has_future_window);
            assert_eq!(
                prepared.inventory().route_dependency_count,
                if source_bound { 2 } else { 0 }
            );
            let context = prepared.finish().unwrap();
            context.assert_command_for_test(&command);
            assert_eq!(context.encode_field().unwrap(), field);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            // Description never authorizes application without the source's
            // real tombstone. Finishing cannot downgrade this to ordinary.
            if source_bound {
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert!(context
                    .application(&topology())
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0)
                    .is_err());
                assert_eq!(pg.conn.total_changes(), writes);
                pg.conn.execute_batch("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn staging_context_preparation_errors_return_complete_input_without_adopting_transactions() {
    let dir = test_util::tempdir();
    let pg_id = fixture(StagingTarget::StreamPut).subject.object_pg.get();
    let pg = PgStore::open(dir.path(), pg_id).unwrap();
    let owner = object::initialize(&pg, StagingTarget::StreamPut, 2);
    let command = close::command(&pg, &owner);
    let mut context = ordinary(&command);
    let field = context.encode_field().unwrap();
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let failure = context
        .prepare_pending(&pg, &topology())
        .err()
        .expect("must reject caller transaction");
    assert!(matches!(
        failure.error(),
        ContextError::Snapshot(crate::MetadataError::InvariantViolation {
            context: "primary metadata command build snapshot",
            ..
        })
    ));
    context = failure.into_context();
    assert!(!pg.conn.is_autocommit());
    assert!(!read_only(&pg));
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("COMMIT").unwrap();

    let other_dir = test_util::tempdir();
    let other = PgStore::open(other_dir.path(), (pg_id + 1) % 8).unwrap();
    let failure = context
        .prepare_pending(&other, &topology())
        .err()
        .expect("wrong store PG");
    assert!(matches!(failure.error(), ContextError::CommandPg));
    context = failure.into_context();
    assert!(other.conn.is_autocommit());
    assert!(!read_only(&other));
    let foreign = PgTopology::new(&[pg_id]).unwrap();
    let failure = context
        .prepare_pending(&pg, &foreign)
        .err()
        .expect("foreign topology");
    assert!(matches!(
        failure.error(),
        ContextError::Candidate(CandidateBindingError::Topology)
    ));
    context = failure.into_context();
    assert!(pg.conn.is_autocommit());
    assert!(!read_only(&pg));
    context.assert_command_for_test(&command);
    assert_eq!(context.encode_field().unwrap(), field);
    assert_eq!(
        context
            .prepare_pending(&pg, &topology())
            .unwrap()
            .inventory()
            .payload_reference_count,
        2
    );
    assert!(pg.conn.is_autocommit());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_context_preparation_drop_and_unwind_restore_prior_connection_mode() {
    for originally_read_only in [false, true] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owner = part::initialize(&pg, 2);
            let command = close::command(&pg, &owner);
            pg.conn.execute_batch("COMMIT").unwrap();
            canary(&pg);
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            pg.conn
                .pragma_update(None, "query_only", originally_read_only)
                .unwrap();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let prepared = ordinary(&command)
                    .prepare_pending(&pg, &topology())
                    .unwrap();
                assert_write_blocked(&pg);
                assert_eq!(prepared.inventory().payload_reference_count, 2);
                assert!(!unwind, "controlled unwind with owned pending context");
                drop(prepared);
            }));
            assert_eq!(outcome.is_err(), unwind);
            assert!(pg.conn.is_autocommit());
            assert_eq!(read_only(&pg), originally_read_only);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            if originally_read_only {
                assert_write_blocked(&pg);
            } else {
                pg.conn
                    .execute("UPDATE preparation_canary SET value = 2", [])
                    .unwrap();
            }
        }
    }
}

#[test]
fn staging_context_preparation_keeps_one_wal_view_and_reinspects_after_finish() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = object::initialize(&pg, StagingTarget::StreamPut, 2);
    let command = close::command(&pg, &owner);
    pg.conn.execute_batch("COMMIT").unwrap();
    let prepared = ordinary(&command)
        .prepare_pending(&pg, &topology())
        .unwrap();
    let original = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
    let prefix = *prepared.prefix();
    // A raw fault-injection connection, not another PG runtime: simulate a
    // crossed live session after the preparation snapshot has been pinned.
    let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
    crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
        .unwrap();
    assert_eq!(
        writer
            .execute(
                "UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1",
                [&owner.subject.id.attempt]
            )
            .unwrap(),
        1
    );
    assert_eq!(
        pg.get_stream_upload(&owner.subject.id.attempt).unwrap(),
        original
    );
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert_eq!(prepared.inventory().payload_reference_count, 2);
    let context = prepared.finish().unwrap();
    assert_ne!(
        pg.get_stream_upload(&owner.subject.id.attempt).unwrap().key,
        original.key
    );
    let writes = pg.conn.total_changes();
    let before = introduction::state(&pg);
    let field = context.encode_field().unwrap();
    let failure = context
        .prepare_pending(&pg, &topology())
        .err()
        .expect("must reload rather than reuse cached inventory");
    assert!(
        matches!(failure.error(), ContextError::Retention(StoreError::ShardScavengerScanIncomplete { context: "derive pending command payload inventory", errors }) if errors.contains("missing exact stream encryption"))
    );
    let context = failure.into_context();
    context.assert_command_for_test(&command);
    assert_eq!(context.encode_field().unwrap(), field);
    assert!(pg.conn.is_autocommit());
    assert!(!read_only(&pg));
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
}
