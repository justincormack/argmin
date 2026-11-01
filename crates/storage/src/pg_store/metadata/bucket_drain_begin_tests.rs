// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn seed(store: &PgStore, name: &str) -> BucketName {
    let bucket = crate::tests::bucket_name(name);
    store
        .create_bucket(
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    bucket
}

fn begin(
    store: &PgStore,
    bucket: &BucketName,
    id: &str,
    fence: AdmittedRouteEffectFence,
) -> Result<BucketWriteDrainRecord, MetadataError> {
    store.begin_durable_bucket_write_drain_with_effect_fence(
        bucket,
        id,
        "owner",
        ClusterEpoch::INITIAL,
        1_000,
        20_000,
        fence,
    )
}

fn fresh() -> AdmittedRouteEffectFence {
    AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000)
}

#[test]
fn bucket_drain_expiry_rejects_before_any_sql_change() {
    for occupied in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let bucket = seed(&store, "drain-effect");
        let foreign = seed(&store, "independent-drain");
        let canary = begin(&store, &foreign, "canary", fresh()).unwrap();
        let reservation = store
            .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-canary",
                owner_token: "writer",
                cluster_epoch: ClusterEpoch::INITIAL,
                operation_kind:
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                created_at: 1_000,
                lease_deadline: 20_000,
                target_context: Some("key"),
            })
            .unwrap();
        if occupied {
            begin(&store, &bucket, "target", fresh()).unwrap();
        }
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let changes = store.conn.total_changes();
        let control = time.control();
        let error = PgStore::test_with_before_bucket_drain_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                control.set(4_500);
            },
            || {
                begin(
                    &store,
                    &bucket,
                    "target",
                    AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000),
                )
            },
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                MetadataError::RouteEffectRejected {
                    source: StoreError::RouteMapExpired {
                        valid_until_ms: 5_000,
                        now_ms: 4_500,
                        ..
                    }
                }
            ),
            "{error:?}"
        );
        assert!(store.conn.is_autocommit());
        assert_eq!(
            store.conn.total_changes(),
            changes,
            "no insert-then-rollback loophole"
        );
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        assert_eq!(
            store.durable_bucket_write_drain(&foreign).unwrap(),
            Some(canary)
        );
        assert_eq!(
            store.durable_bucket_write_reservations(&bucket).unwrap(),
            vec![reservation]
        );
        let record = begin(&store, &bucket, "target", fresh()).unwrap();
        assert_eq!(record.drain_id, "target");
        assert_eq!(
            record.bucket_execution_generation,
            store
                .head_bucket_raw(&bucket)
                .unwrap()
                .bucket_execution_generation
        );
        let after = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let changes = store.conn.total_changes();
        assert_eq!(begin(&store, &bucket, "target", fresh()).unwrap(), record);
        assert!(matches!(
            begin(&store, &bucket, "contender", fresh()),
            Err(MetadataError::BucketWriteDrainConflict { .. })
        ));
        assert_eq!(store.conn.total_changes(), changes);
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            after
        );
    }
}

#[test]
fn bucket_drain_effect_failures_rollback_and_allow_retry() {
    for fail_commit in [false, true] {
        let _time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let bucket = seed(&store, "drain-rollback");
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        if fail_commit {
            store.fail_next_metadata_txn_commit();
        } else {
            store
                .conn
                .execute_batch(
                    "CREATE TEMP TRIGGER fail_drain BEFORE INSERT ON bucket_write_drains
                BEGIN SELECT RAISE(ABORT, 'injected drain insertion failure'); END",
                )
                .unwrap();
        }
        let error = begin(&store, &bucket, "target", fresh()).unwrap_err();
        assert!(
            matches!(error, MetadataError::Db { context, .. } if context == if fail_commit {
            "begin durable bucket write drain (commit txn)"
        } else { "begin durable bucket write drain" }),
            "{error:?}"
        );
        assert!(store.conn.is_autocommit());
        assert_eq!(store.durable_bucket_write_drain(&bucket).unwrap(), None);
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        if !fail_commit {
            store.conn.execute_batch("DROP TRIGGER fail_drain").unwrap();
        }
        begin(&store, &bucket, "target", fresh()).unwrap();
    }
}

#[test]
fn bucket_drain_effect_has_one_commit_and_joins_outer_transaction() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    let _time = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let mut store = PgStore::open(dir.path(), 0).unwrap();
    let bucket = seed(&store, "drain-one-commit");
    let before = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    begin(&store, &bucket, "target", fresh()).unwrap();
    assert!(!store.conn.is_autocommit());
    store.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap(),
        before
    );
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = std::sync::Arc::clone(&observed);
    store
        .conn
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(_, sql) = event {
                    if !sql.starts_with("--") {
                        captured.lock().unwrap().push(sql.to_owned());
                    }
                }
            }),
        )
        .unwrap();
    begin(&store, &bucket, "target", fresh()).unwrap();
    let statements = observed.lock().unwrap();
    assert_eq!(
        statements
            .iter()
            .map(|s| s.split_whitespace().next().unwrap())
            .collect::<Vec<_>>(),
        ["BEGIN", "INSERT", "SELECT", "COMMIT"],
        "{statements:?}"
    );
    assert_eq!(statements[0], "BEGIN IMMEDIATE");
}
