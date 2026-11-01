// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[cfg(test)]
type ReservationEffectHook = Box<dyn FnOnce(bool)>;

#[cfg(test)]
thread_local! {
    static BEFORE_RESERVATION_EFFECT: std::cell::RefCell<Option<ReservationEffectHook>> =
        const { std::cell::RefCell::new(None) };
}

impl PgStore {
    /// The caller holds the bucket PG lock. SQLite writer admission must also
    /// precede the final effect check, rather than occur inside an implicit
    /// INSERT transaction after authority has already been checked.
    pub(in crate::node_runtime) fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        let created_at = i64::try_from(acquire.created_at).map_err(|source| MetadataError::Db {
            context: "acquire durable bucket write reservation created_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline =
            i64::try_from(acquire.lease_deadline).map_err(|source| MetadataError::Db {
                context: "acquire durable bucket write reservation lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        self.with_immediate_txn(
            "acquire durable bucket write reservation (begin txn)",
            "acquire durable bucket write reservation (commit txn)",
            |store| {
                let mut insert = store.conn.prepare_cached(
                    "INSERT INTO bucket_write_reservations \
                     (bucket_name, reservation_id, owner_token, cluster_epoch, bucket_execution_generation, \
                      bucket_incarnation_generation, operation_kind, created_at, lease_deadline, target_context) \
                     SELECT name, ?1, ?2, ?3, bucket_execution_generation, bucket_incarnation_generation, ?4, ?5, ?6, ?7 \
                     FROM buckets \
                     WHERE name = ?8 AND state = ?9 \
                       AND NOT EXISTS (\
                           SELECT 1 FROM bucket_write_drains \
                           WHERE bucket_name = buckets.name AND state = ?10\
                       ) \
                     ON CONFLICT(bucket_name, reservation_id) DO NOTHING"
                ).map_err(|source| MetadataError::Db {
                    context: "prepare durable bucket write reservation",
                    source: source.into(),
                })?;
                #[cfg(test)]
                if let Some(hook) = BEFORE_RESERVATION_EFFECT.with(|slot| slot.borrow_mut().take()) {
                    hook(!store.conn.is_autocommit());
                }
                effect_fence.require_valid_for(acquire.cluster_epoch)
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
                let inserted = insert.execute(params![
                    acquire.reservation_id, acquire.owner_token, acquire.cluster_epoch.get(),
                    acquire.operation_kind, created_at, lease_deadline, acquire.target_context,
                    acquire.name.as_str(), BucketState::Active as u8, BucketWriteDrainState::Draining as u8,
                ]).map_err(|source| MetadataError::Db {
                    context: "acquire durable bucket write reservation",
                    source: source.into(),
                })?;
                if inserted == 0 {
                    if let Some(existing) = store.durable_bucket_write_reservation(acquire.name, acquire.reservation_id)? {
                        if existing.owner_token == acquire.owner_token
                            && existing.cluster_epoch == acquire.cluster_epoch
                            && existing.operation_kind == acquire.operation_kind
                            && existing.created_at == created_at as u64
                            && existing.lease_deadline == lease_deadline as u64
                            && existing.target_context.as_deref() == acquire.target_context
                        {
                            return Ok(existing);
                        }
                        return Err(MetadataError::BucketWriteReservationConflict {
                            reservation_id: acquire.reservation_id.to_owned(),
                        });
                    }
                    let info = store.head_bucket_raw(acquire.name)?;
                    if info.state == BucketState::Active && store.durable_bucket_write_drain(acquire.name)?.is_some() {
                        return Err(MetadataError::BucketWriteDraining);
                    }
                    return Err(bucket_not_found(acquire.name.as_str()));
                }
                store.durable_bucket_write_reservation(acquire.name, acquire.reservation_id)?
                    .ok_or_else(|| MetadataError::BucketWriteReservationNotFound {
                        reservation_id: acquire.reservation_id.to_owned(),
                    })
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_before_bucket_reservation_effect_hook<T>(
        hook: impl FnOnce(bool) + 'static,
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<ReservationEffectHook>);
        impl Drop for Reset {
            fn drop(&mut self) {
                BEFORE_RESERVATION_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset =
            Reset(BEFORE_RESERVATION_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))));
        action()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(store: &PgStore) -> BucketName {
        let bucket = crate::tests::bucket_name("reservation-transaction");
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
        bucket
    }

    fn acquire<'a>(
        bucket: &'a BucketName,
        id: &'a str,
    ) -> DurableBucketWriteReservationAcquire<'a> {
        DurableBucketWriteReservationAcquire {
            name: bucket,
            reservation_id: id,
            owner_token: "owner",
            cluster_epoch: ClusterEpoch::INITIAL,
            operation_kind:
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            created_at: 1_000,
            lease_deadline: 20_000,
            target_context: Some("key"),
        }
    }

    #[test]
    fn bucket_reservation_expiry_rejects_before_any_sql_change() {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let bucket = seed(&store);
        let canary = store
            .acquire_durable_bucket_write_reservation(acquire(&bucket, "canary"))
            .unwrap();
        let changes = store.conn.total_changes();
        let control = time.control();
        let error = PgStore::test_with_before_bucket_reservation_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                control.set(4_500);
            },
            || {
                store.acquire_durable_bucket_write_reservation_with_effect_fence(
                    acquire(&bucket, "target"),
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
            "INSERT then rollback is not pre-effect rejection"
        );
        assert_eq!(
            store.durable_bucket_write_reservations(&bucket).unwrap(),
            vec![canary]
        );
        let record = store
            .acquire_durable_bucket_write_reservation_with_effect_fence(
                acquire(&bucket, "target"),
                AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000),
            )
            .unwrap();
        assert_eq!(record.reservation_id, "target");
    }

    #[test]
    fn bucket_reservation_transaction_rolls_back_insert_and_commit_failures() {
        for fail_commit in [false, true] {
            let dir = test_util::tempdir();
            let store = PgStore::open(dir.path(), 0).unwrap();
            let bucket = seed(&store);
            let canary = store
                .acquire_durable_bucket_write_reservation(acquire(&bucket, "canary"))
                .unwrap();
            if fail_commit {
                store.fail_next_metadata_txn_commit();
            } else {
                store.conn.execute_batch("CREATE TEMP TRIGGER fail_reservation BEFORE INSERT ON bucket_write_reservations \
                    BEGIN SELECT RAISE(ABORT, 'injected reservation failure'); END").unwrap();
            }
            let error = store
                .acquire_durable_bucket_write_reservation(acquire(&bucket, "target"))
                .unwrap_err();
            assert!(
                matches!(&error, MetadataError::Db { context, .. } if *context ==
                    if fail_commit { "acquire durable bucket write reservation (commit txn)" }
                    else { "acquire durable bucket write reservation" }
                ),
                "{error:?}"
            );
            assert!(store.conn.is_autocommit());
            assert_eq!(
                store.durable_bucket_write_reservations(&bucket).unwrap(),
                vec![canary]
            );
            if !fail_commit {
                store
                    .conn
                    .execute_batch("DROP TRIGGER fail_reservation")
                    .unwrap();
            }
            let record = store
                .acquire_durable_bucket_write_reservation(acquire(&bucket, "target"))
                .unwrap();
            assert_eq!(
                store
                    .acquire_durable_bucket_write_reservation(acquire(&bucket, "target"))
                    .unwrap(),
                record
            );
        }
    }

    #[test]
    fn bucket_reservation_acquisition_joins_outer_transaction() {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let bucket = seed(&store);
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        store
            .acquire_durable_bucket_write_reservation(acquire(&bucket, "target"))
            .unwrap();
        assert!(!store.conn.is_autocommit());
        assert!(store
            .durable_bucket_write_reservation(&bucket, "target")
            .unwrap()
            .is_some());
        store.conn.execute_batch("ROLLBACK").unwrap();
        assert!(store
            .durable_bucket_write_reservation(&bucket, "target")
            .unwrap()
            .is_none());
    }

    #[test]
    fn bucket_reservation_acquisition_has_one_successful_write_transaction() {
        use rusqlite::trace::{TraceEvent, TraceEventCodes};
        use std::sync::{Arc, Mutex};
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        let bucket = seed(&store);
        let statements = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = Arc::clone(&statements);
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
        store
            .acquire_durable_bucket_write_reservation(acquire(&bucket, "target"))
            .unwrap();
        let observed = statements.lock().unwrap();
        let kinds = observed
            .iter()
            .map(|sql| sql.split_whitespace().next().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            ["BEGIN", "INSERT", "SELECT", "COMMIT"],
            "{observed:?}"
        );
        assert_eq!(observed[0], "BEGIN IMMEDIATE");
        assert!(store.conn.is_autocommit());
    }
}
