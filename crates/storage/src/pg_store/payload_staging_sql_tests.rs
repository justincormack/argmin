// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rusqlite::{Connection, OptionalExtension};

#[path = "payload_staging_persistence_tests.rs"]
mod live;

fn connection() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    conn.execute_batch(sql::SCHEMA).unwrap();
    conn
}

fn insert(conn: &Connection, owner: &StagingOwnerRecord) {
    sql::insert(
        conn,
        owner.subject.object_pg,
        &topology(),
        &owner.validate(&topology()).unwrap(),
    )
    .unwrap();
}

fn load(
    conn: &Connection,
    owner: &StagingOwnerRecord,
) -> Result<Option<StagingOwnerRecord>, sql::PersistenceError> {
    sql::load(conn, owner.subject.object_pg, &owner.subject.id.attempt)
}

fn assert_empty(conn: &Connection) {
    for table in sql::TABLES {
        let count: u64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {}", table.name), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{} retained a partial owner", table.name);
    }
}

fn round_trip(owner: &StagingOwnerRecord) {
    let conn = connection();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&conn, owner);
    assert_eq!(load(&conn, owner).unwrap().as_ref(), Some(owner));
    conn.execute_batch("COMMIT; BEGIN").unwrap();
    let loaded = load(&conn, owner).unwrap().unwrap();
    assert_eq!(&loaded, owner);
    loaded.validate(&topology()).unwrap();
    conn.execute_batch("COMMIT").unwrap();
}

#[test]
fn staging_sql_round_trips_target_lifecycle_destination_and_resource_matrix() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for segments in [0, 2] {
            let mut owner = fixture(target.clone());
            if target != StagingTarget::DirectPut {
                for _ in 0..segments {
                    append(&mut owner);
                }
            }
            round_trip(&owner);

            let mut unpublished = owner.clone();
            unpublished.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
            unpublished.references.clear();
            release_due(&mut unpublished.resources.generation);
            release_due(&mut unpublished.resources.creation);
            round_trip(&unpublished);
            if matches!(target, StagingTarget::UploadPart { .. }) {
                unpublished.resources.finalization =
                    ResourceResponsibility::ReleaseDue(finalization_resource(&unpublished));
                round_trip(&unpublished);
            }

            publish(&mut owner);
            round_trip(&owner);
            let part_number = match target {
                StagingTarget::UploadPart { part_number, .. } => Some(part_number),
                _ => None,
            };
            for destination in [
                StagingDestination::Object {
                    generation: owner.subject.published_generation(),
                    part_number,
                },
                StagingDestination::Reclaim {
                    generation: owner.subject.published_generation(),
                    part_number,
                },
            ] {
                for lifecycle in [
                    StagingLifecycle::Published,
                    StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
                ] {
                    let mut transferred = owner.clone();
                    transferred.lifecycle = lifecycle;
                    for reference in &mut transferred.references {
                        reference.destination = destination.clone();
                    }
                    round_trip(&transferred);
                }
            }
            owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
            round_trip(&owner);
            owner.references.clear();
            owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
            round_trip(&owner);
        }
    }
}

#[test]
fn staging_sql_requires_outer_transaction_and_does_not_commit_it() {
    let conn = connection();
    let owner = fixture(StagingTarget::DirectPut);
    let configured_topology = topology();
    let checked = owner.validate(&configured_topology).unwrap();
    assert!(matches!(
        sql::insert(
            &conn,
            owner.subject.object_pg,
            &configured_topology,
            &checked
        ),
        Err(sql::PersistenceError::TransactionRequired)
    ));
    assert!(matches!(
        load(&conn, &owner),
        Err(sql::PersistenceError::TransactionRequired)
    ));
    assert_empty(&conn);
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&conn, &owner);
    assert!(!conn.is_autocommit());
    assert_eq!(load(&conn, &owner).unwrap(), Some(owner));
    conn.execute_batch("ROLLBACK").unwrap();
    assert_empty(&conn);
}

#[test]
fn staging_sql_rejects_crossed_pg_before_mutation_or_observation() {
    let conn = connection();
    let owner = fixture(StagingTarget::DirectPut);
    let foreign = PgId::new(owner.subject.object_pg.get() + 1);
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(matches!(
        sql::insert(
            &conn,
            foreign,
            &topology(),
            &owner.validate(&topology()).unwrap()
        ),
        Err(sql::PersistenceError::WrongPg)
    ));
    assert_empty(&conn);
    insert(&conn, &owner);
    assert!(matches!(
        sql::load(&conn, foreign, &owner.subject.id.attempt),
        Err(sql::PersistenceError::WrongPg)
    ));
    assert_eq!(load(&conn, &owner).unwrap(), Some(owner));
}

// Select subjects under two independently constructed topologies. The object
// PG must coincide, and exactly one of data/release routing must differ, so
// neither negative can pass merely because another subject check rejected it.
fn crossed_topology_owner(
    target: StagingTarget,
    data_differs: bool,
) -> (StagingOwnerRecord, PgTopology) {
    let source = topology();
    // Same cardinality, different canonical PG sets. Comparing only pg_count
    // would accept these just as comparing only the object PG would.
    let destination = PgTopology::new(&[0, 1, 2, 3, 4, 5, 6, 8]).unwrap();
    assert_eq!(source.pg_count(), destination.pg_count());
    for bucket_index in 0..128 {
        let bucket = BucketName::try_from(format!("staging-topology-{bucket_index}")).unwrap();
        let bucket_differs = source.bucket_pg_for(&bucket) != destination.bucket_pg_for(&bucket);
        if bucket_differs == data_differs {
            continue;
        }
        for key_index in 0..128 {
            let key = ObjectKey::try_from(format!("payload-{key_index}")).unwrap();
            let object_pg = PgId::new(source.object_pg_for(&bucket, &key));
            if object_pg.get() != destination.object_pg_for(&bucket, &key) {
                continue;
            }
            let mut owner = fixture(target.clone());
            owner.subject.bucket = bucket.clone();
            owner.subject.key = key;
            owner.subject.object_pg = object_pg;
            owner.subject.id.introduction = MetadataCommandId::new(
                owner.subject.originating_epoch,
                object_pg,
                owner.subject.id.introduction.log_index(),
            );
            if (owner.subject.data_pg(&source, 0) != owner.subject.data_pg(&destination, 0))
                != data_differs
            {
                continue;
            }
            owner.windows[0] = window(&owner.subject, 0, owner.subject.first_vid());
            let creation = StagingBucketResource::capture(
                &proof(&owner.subject, owner.subject.creation_operation()),
                PgId::new(source.bucket_pg_for(&bucket)),
            );
            owner.resources.creation = if matches!(target, StagingTarget::UploadPart { .. }) {
                ResourceResponsibility::ReleaseDue(creation)
            } else {
                ResourceResponsibility::Live(creation)
            };
            owner.validate(&source).unwrap();
            return (owner, destination);
        }
    }
    panic!("no independently crossed topology subject for {target:?}, data_differs={data_differs}");
}

fn assert_crossed_topology_rejected(data_differs: bool) {
    let source = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let (owner, destination) = crossed_topology_owner(target, data_differs);
        assert_eq!(
            source.object_pg_for(&owner.subject.bucket, &owner.subject.key),
            destination.object_pg_for(&owner.subject.bucket, &owner.subject.key)
        );
        assert_eq!(
            source.bucket_pg_for(&owner.subject.bucket)
                != destination.bucket_pg_for(&owner.subject.bucket),
            !data_differs
        );
        assert_eq!(
            owner.windows[0].participants.data_pg.get() != owner.subject.data_pg(&destination, 0),
            data_differs
        );
        assert_eq!(
            owner.validate(&destination).unwrap_err(),
            if data_differs {
                StagingInvariant::Placement
            } else {
                StagingInvariant::Resource
            }
        );

        let checked = owner.validate(&source).unwrap();
        let conn = connection();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            sql::insert(&conn, owner.subject.object_pg, &destination, &checked),
            Err(sql::PersistenceError::Invariant(StagingInvariant::Topology))
        ));
        assert_empty(&conn);
        assert!(!conn.is_autocommit());
        // The exact same record/view is insertable under its actual topology.
        sql::insert(&conn, owner.subject.object_pg, &source, &checked).unwrap();
        assert_eq!(load(&conn, &owner).unwrap().as_ref(), Some(&owner));
    }
}

#[test]
fn staging_sql_rejects_crossed_topology_with_same_object_and_bucket_pg() {
    assert_crossed_topology_rejected(true);
}

#[test]
fn staging_sql_rejects_crossed_topology_with_same_object_and_data_pg() {
    assert_crossed_topology_rejected(false);
}

#[test]
fn staging_sql_accepts_independent_canonically_equivalent_topology() {
    let source = topology();
    let destination = PgTopology::new(&[7, 6, 5, 4, 3, 2, 1, 0, 3, 7]).unwrap();
    assert!(!std::ptr::eq(&source, &destination));
    let owner = fixture(StagingTarget::DirectPut);
    let checked = owner.validate(&source).unwrap();
    let conn = connection();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    sql::insert(&conn, owner.subject.object_pg, &destination, &checked).unwrap();
    assert_eq!(load(&conn, &owner).unwrap(), Some(owner));
}

#[test]
fn staging_sql_failed_child_insert_rolls_back_whole_owner_only() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    for table in sql::TABLES {
        let conn = connection();
        conn.execute_batch(&format!(
            "CREATE TABLE sentinel (value INTEGER); BEGIN IMMEDIATE; INSERT INTO sentinel VALUES (7); \
             CREATE TEMP TRIGGER fail_staging_insert BEFORE INSERT ON {} BEGIN \
             SELECT RAISE(ABORT, 'injected staging child failure'); END", table.name,
        )).unwrap();
        let error = sql::insert(
            &conn,
            owner.subject.object_pg,
            &topology(),
            &owner.validate(&topology()).unwrap(),
        )
        .unwrap_err();
        assert!(matches!(error, sql::PersistenceError::Sql(_)), "{error:?}");
        assert!(error.to_string().contains("injected staging child failure"));
        assert!(!conn.is_autocommit());
        assert_empty(&conn);
        assert_eq!(
            conn.query_row("SELECT value FROM sentinel", [], |row| row.get::<_, u64>(0))
                .unwrap(),
            7
        );
        conn.execute_batch("DROP TRIGGER fail_staging_insert")
            .unwrap();
        insert(&conn, &owner);
        assert_eq!(load(&conn, &owner).unwrap().as_ref(), Some(&owner));
        conn.execute_batch("ROLLBACK").unwrap();
        assert_empty(&conn);
    }
}

#[test]
fn staging_sql_does_not_adopt_exact_or_crossed_existing_owner() {
    let conn = connection();
    let owner = fixture(StagingTarget::DirectPut);
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&conn, &owner);
    let mut different = owner.clone();
    different.resources.creation = ResourceResponsibility::Live(StagingBucketResource::capture(
        &BucketWriteReservationProof {
            reservation_id: "other-reservation".into(),
            ..proof(&owner.subject, owner.subject.creation_operation())
        },
        PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
    ));
    for contender in [&owner, &different] {
        assert!(matches!(
            sql::insert(
                &conn,
                owner.subject.object_pg,
                &topology(),
                &contender.validate(&topology()).unwrap()
            ),
            Err(sql::PersistenceError::OwnerExists)
        ));
        assert_eq!(load(&conn, &owner).unwrap().as_ref(), Some(&owner));
    }
}

#[test]
fn staging_sql_retains_exact_participants_and_both_historical_release_routes_on_reopen() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("owner-contract.sqlite");
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    let ResourceResponsibility::ReleaseDue(finalization) = &mut owner.resources.finalization else {
        unreachable!()
    };
    finalization.identity.cluster_epoch = ClusterEpoch::new(8).unwrap();
    finalization.observed_lease_deadline = 22_000;
    owner.windows[1].participants.placement_epoch = ClusterEpoch::new(7).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
        conn.execute_batch(sql::SCHEMA).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        insert(&conn, &owner);
        conn.execute_batch("COMMIT").unwrap();
    }
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    let loaded = load(&conn, &owner).unwrap().unwrap();
    assert_eq!(loaded, owner);
    assert_eq!(
        loaded.windows[0].participants.files,
        [2, 0, 3].map(NodeId::new)
    );
    assert_eq!(
        loaded.windows[0].participants.acknowledgement_primary,
        NodeId::new(1)
    );
}

#[test]
fn staging_sql_column_inventory_covers_every_table_and_column() {
    let conn = connection();
    let actual = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap();
    let expected = sql::TABLES
        .iter()
        .map(|table| table.name.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    for table in sql::TABLES {
        let columns = conn
            .prepare(&format!("PRAGMA table_info({})", table.name))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(columns, table.columns, "{}", table.name);
        let references = conn
            .prepare(&format!("PRAGMA foreign_key_list({})", table.name))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(2))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for referenced in references {
            assert!(
                expected.contains(&referenced),
                "unexpected cascade to {referenced}"
            );
        }
    }
}

fn reject_mutation(owner: &StagingOwnerRecord, statement: &str, invariant: StagingInvariant) {
    let conn = connection();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&conn, owner);
    assert_eq!(load(&conn, owner).unwrap().as_ref(), Some(owner));
    assert!(conn.execute(statement, []).unwrap() > 0, "{statement}");
    assert!(
        matches!(load(&conn, owner), Err(sql::PersistenceError::Invariant(error)) if error == invariant),
        "{statement}"
    );
}

#[test]
fn staging_sql_rejects_missing_and_split_terminal_references() {
    let mut direct = fixture(StagingTarget::DirectPut);
    publish(&mut direct);
    reject_mutation(
        &direct,
        "DELETE FROM payload_staging_references",
        StagingInvariant::Reference,
    );
    let mut multipart = fixture(multipart_target());
    append(&mut multipart);
    append(&mut multipart);
    publish(&mut multipart);
    reject_mutation(
        &multipart,
        "DELETE FROM payload_staging_references WHERE window_index = 0",
        StagingInvariant::Reference,
    );
    reject_mutation(&multipart, "UPDATE payload_staging_references SET destination = 2, upload_id = NULL, generation = 7 WHERE window_index = 1", StagingInvariant::Reference);
    reject_mutation(&multipart, "UPDATE payload_staging_references SET destination = 3, upload_id = NULL, generation = 7 WHERE window_index = 0", StagingInvariant::Reference);
}

#[test]
fn staging_sql_rejects_lost_or_premature_resource_obligations() {
    let mut owner = fixture(multipart_target());
    reject_mutation(
        &owner,
        "DELETE FROM payload_staging_resources WHERE role = 0",
        StagingInvariant::Resource,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_resources SET state = 1 WHERE role = 0",
        StagingInvariant::Resource,
    );
    publish(&mut owner);
    reject_mutation(
        &owner,
        "DELETE FROM payload_staging_resources WHERE role = 1",
        StagingInvariant::Resource,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_resources SET state = 1 WHERE role = 1",
        StagingInvariant::Resource,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_resources SET target_context = 'crossed-key' WHERE role = 1",
        StagingInvariant::Resource,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_owners SET lifecycle = 0, generation_state = 1",
        StagingInvariant::Resource,
    );
}

#[test]
fn staging_sql_rejects_noncanonical_windows_and_geometry() {
    let owner = fixture(StagingTarget::StreamPut);
    reject_mutation(
        &owner,
        "DELETE FROM payload_staging_windows",
        StagingInvariant::Windows,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_windows SET end_vid = end_vid + 1",
        StagingInvariant::Interval,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
        StagingInvariant::Geometry,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_windows SET cutoff_ms = cutoff_ms + 1",
        StagingInvariant::Cutoff,
    );
    reject_mutation(
        &owner,
        "DELETE FROM payload_staging_participants WHERE shard_index = 2",
        StagingInvariant::Placement,
    );
    reject_mutation(
        &owner,
        "UPDATE payload_staging_windows SET segment_hash = zeroblob(16)",
        StagingInvariant::WindowOwner,
    );
}

#[test]
fn staging_sql_decoders_reject_unknown_tags_and_crossed_optional_shapes() {
    for statement in [
        "UPDATE payload_staging_owners SET target = 9",
        "UPDATE payload_staging_owners SET upload_generation = 7",
        "UPDATE payload_staging_owners SET lifecycle = 9",
        "UPDATE payload_staging_owners SET generation_state = 0",
        "UPDATE payload_staging_owners SET introduction_index = 0",
        "UPDATE payload_staging_owners SET issuer_lifetime = zeroblob(15)",
        "UPDATE payload_staging_windows SET codec = 1",
        "UPDATE payload_staging_resources SET role = 9",
        "UPDATE payload_staging_resources SET state = 0",
    ] {
        let conn = connection();
        let owner = fixture(StagingTarget::DirectPut);
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        insert(&conn, &owner);
        // Model corrupted/imported rows independently of SQL CHECK constraints.
        conn.execute_batch("PRAGMA ignore_check_constraints = ON")
            .unwrap();
        assert!(conn.execute(statement, []).unwrap() > 0);
        assert!(
            matches!(load(&conn, &owner), Err(sql::PersistenceError::Row(_))),
            "{statement}"
        );
    }
}

#[test]
fn staging_obligations_sql_round_trip_rejects_gaps_live_extras_and_duplicate_proofs() {
    let owner = owner_with_abandoned_obligations();
    for statement in [
        "DELETE FROM payload_staging_resources WHERE role = 2",
        "UPDATE payload_staging_resources SET state = 1 WHERE role = 2",
        "UPDATE payload_staging_resources SET role = 4294967296 WHERE role = 3",
        "UPDATE payload_staging_resources SET reservation_id = (SELECT reservation_id FROM payload_staging_resources WHERE role = 2), owner_token = (SELECT owner_token FROM payload_staging_resources WHERE role = 2), cluster_epoch = (SELECT cluster_epoch FROM payload_staging_resources WHERE role = 2), observed_lease_deadline = observed_lease_deadline + 1000 WHERE role = 3",
        "UPDATE payload_staging_resources SET reservation_id = (SELECT reservation_id FROM payload_staging_resources WHERE role = 1), owner_token = (SELECT owner_token FROM payload_staging_resources WHERE role = 1), cluster_epoch = (SELECT cluster_epoch FROM payload_staging_resources WHERE role = 1) WHERE role = 2",
    ] {
        let conn = connection();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        insert(&conn, &owner);
        assert_eq!(load(&conn, &owner).unwrap(), Some(owner.clone()));
        conn.execute_batch("PRAGMA ignore_check_constraints = ON").unwrap();
        conn.execute_batch(statement).unwrap();
        assert!(load(&conn, &owner).is_err(), "{statement}");
    }
}

#[test]
fn staging_sql_foreign_keys_reject_orphan_windows_and_participants() {
    let conn = connection();
    let owner = fixture(StagingTarget::DirectPut);
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    insert(&conn, &owner);
    for table in [
        "payload_staging_windows",
        "payload_staging_participants",
        "payload_staging_resources",
    ] {
        let result = conn.execute(&format!("UPDATE {table} SET attempt = 'absent-owner'"), []);
        assert!(result.is_err(), "{table}");
    }
    assert_eq!(load(&conn, &owner).unwrap(), Some(owner));
    assert!(conn
        .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
        .optional()
        .unwrap()
        .is_none());
}

fn checkpoint_blocks(
    owners: &[StagingOwnerRecord],
) -> Vec<super::super::super::command_log::MetadataCheckpointTableBlock> {
    let tmp = test_util::tempdir();
    let store =
        super::super::super::PgStore::open(tmp.path(), owners[0].subject.object_pg.get()).unwrap();
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for owner in owners {
        store
            .advance_object_generation_high_water_in_open_txn(
                &owner.subject.bucket,
                &owner.subject.key,
                owner.subject.payload_generation,
            )
            .unwrap();
        insert(&store.conn, owner);
    }
    store.refresh_metadata_command_state_digest().unwrap();
    let checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let blocks = checkpoint
        .table_blocks
        .into_iter()
        .filter(|block| {
            sql::TABLES
                .iter()
                .any(|table| table.name == block.table_name)
        })
        .collect();
    store.conn.execute_batch("ROLLBACK").unwrap();
    blocks
}

#[test]
fn staging_sql_checkpoint_projection_uses_production_encoder_and_shared_decoder() {
    let mut direct = fixture(StagingTarget::DirectPut);
    publish(&mut direct);
    let mut multipart = fixture(multipart_target());
    // Different owner, identical object/PG. Both selected and unused windows
    // must survive; the ack primary is deliberately outside the file set.
    multipart.subject.id.attempt = crate::tests::stream_session_id("second-owner");
    multipart.windows[0] = window(&multipart.subject, 0, multipart.subject.first_vid());
    let ResourceResponsibility::Live(generation) = &mut multipart.resources.generation else {
        unreachable!()
    };
    generation.reservation_id = multipart.subject.id.attempt.clone();
    append(&mut multipart);
    append(&mut multipart);
    publish(&mut multipart);
    let mut owners = vec![direct, multipart];
    owners.sort_by(|left, right| left.subject.id.attempt.cmp(&right.subject.id.attempt));
    let blocks = checkpoint_blocks(&owners);
    assert_eq!(
        sql::decode_checkpoint_blocks(owners[0].subject.object_pg, &blocks).unwrap(),
        owners
    );

    // There are two unrelated roles with the same creation operation shape.
    // Import must preserve their individual identities, not coalesce by PG.
    assert_eq!(blocks[4].rows.len(), 3);
    assert!(sql::decode_checkpoint_blocks(
        PgId::new(owners[0].subject.object_pg.get() + 1),
        &blocks
    )
    .is_err());
}

#[test]
fn staging_sql_checkpoint_rejects_orphans_duplicates_and_omitted_tables() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    append(&mut owner);
    publish(&mut owner);
    let blocks = checkpoint_blocks(std::slice::from_ref(&owner));
    assert_eq!(
        sql::decode_checkpoint_blocks(owner.subject.object_pg, &blocks).unwrap(),
        [owner.clone()]
    );
    for table in 0..sql::TABLES.len() {
        let mut missing = blocks.clone();
        missing.remove(table);
        assert!(
            sql::decode_checkpoint_blocks(owner.subject.object_pg, &missing).is_err(),
            "missing table {table}"
        );
        let mut duplicate = blocks.clone();
        let first = duplicate[table].rows[0].clone();
        duplicate[table].rows.push(first);
        assert!(
            sql::decode_checkpoint_blocks(owner.subject.object_pg, &duplicate).is_err(),
            "duplicate table {table} row"
        );
        let mut missing_row = blocks.clone();
        missing_row[table].rows.remove(0);
        assert!(
            sql::decode_checkpoint_blocks(owner.subject.object_pg, &missing_row).is_err(),
            "missing table {table} row"
        );
    }
    for table in 1..sql::TABLES.len() {
        let mut orphan = blocks.clone();
        orphan[table].rows[0].values[0] =
            super::super::super::command_log::MetadataCheckpointValue::Text(
                crate::tests::stream_session_id("absent-owner")
                    .as_str()
                    .as_bytes()
                    .to_vec(),
            );
        assert!(
            sql::decode_checkpoint_blocks(owner.subject.object_pg, &orphan).is_err(),
            "orphan table {table}"
        );
    }
}

#[test]
fn staging_sql_checkpoint_rejects_noncanonical_table_columns_and_values() {
    use super::super::super::command_log::MetadataCheckpointValue as Value;
    let owner = fixture(StagingTarget::DirectPut);
    let blocks = checkpoint_blocks(std::slice::from_ref(&owner));
    for table in 0..sql::TABLES.len() {
        let mut wrong_columns = blocks.clone();
        wrong_columns[table].columns.swap(0, 1);
        assert!(matches!(
            sql::decode_checkpoint_blocks(owner.subject.object_pg, &wrong_columns),
            Err(sql::PersistenceError::Row(
                "checkpoint staging table identity"
            ))
        ));
        if !blocks[table].rows.is_empty() {
            let mut extra_column = blocks.clone();
            extra_column[table].rows[0].values.push(Value::Null);
            assert!(matches!(
                sql::decode_checkpoint_blocks(owner.subject.object_pg, &extra_column),
                Err(sql::PersistenceError::Row("checkpoint row width"))
            ));
        }
    }
    for (table, column, value) in [
        (0, 1, Value::Integer(-1)),
        (0, 2, Value::Integer(i64::MAX)),
        (0, 3, Value::Integer(0)),
        (0, 10, Value::Integer(9)),
        (0, 15, Value::Blob(vec![1; 15])),
        (1, 7, Value::Integer(1)),
        (1, 8, Value::RealBits(2.0_f64.to_bits())),
        (4, 2, Value::Integer(0)),
    ] {
        let mut invalid = blocks.clone();
        invalid[table].rows[0].values[column] = value;
        assert!(
            matches!(
                sql::decode_checkpoint_blocks(owner.subject.object_pg, &invalid),
                Err(sql::PersistenceError::Row(_))
            ),
            "table={table}, column={column}"
        );
    }
}

#[test]
fn staging_sql_checkpoint_rejects_split_destinations_and_invalid_resource_lifecycle() {
    use super::super::super::command_log::MetadataCheckpointValue as Value;
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    append(&mut owner);
    publish(&mut owner);
    let blocks = checkpoint_blocks(std::slice::from_ref(&owner));
    let mut split = blocks.clone();
    split[3].rows[1].values[6] = Value::Integer(2);
    split[3].rows[1].values[7] = Value::Null;
    split[3].rows[1].values[8] = Value::Integer(7);
    assert!(matches!(
        sql::decode_checkpoint_blocks(owner.subject.object_pg, &split),
        Err(sql::PersistenceError::Invariant(
            StagingInvariant::Reference
        ))
    ));
    let mut lost_proof = blocks.clone();
    lost_proof[4]
        .rows
        .retain(|row| row.values[1] == Value::Integer(0));
    assert!(matches!(
        sql::decode_checkpoint_blocks(owner.subject.object_pg, &lost_proof),
        Err(sql::PersistenceError::Invariant(StagingInvariant::Resource))
    ));
    let mut live_proof = blocks;
    live_proof[4].rows[1].values[2] = Value::Integer(1);
    assert!(matches!(
        sql::decode_checkpoint_blocks(owner.subject.object_pg, &live_proof),
        Err(sql::PersistenceError::Invariant(StagingInvariant::Resource))
    ));
}
