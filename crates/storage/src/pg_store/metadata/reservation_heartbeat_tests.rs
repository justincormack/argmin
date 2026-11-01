// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    CreateStreamUploadCommand, PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
};

struct Fixture {
    proof: BucketWriteReservationProof,
    session_id: SessionId,
}

impl Fixture {
    fn new(store: &PgStore, lease_deadline: u64) -> Self {
        let bucket = crate::tests::bucket_name("heartbeat-effect");
        let key = crate::tests::object_key("key");
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
        let acquire = |id| DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: id,
            owner_token: "owner",
            cluster_epoch: ClusterEpoch::INITIAL,
            operation_kind: PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            created_at: 1_000,
            lease_deadline,
            target_context: Some(key.as_str()),
        };
        store
            .acquire_durable_bucket_write_reservation(acquire("canary"))
            .unwrap();
        let proof = BucketWriteReservationProof::from(
            &store
                .acquire_durable_bucket_write_reservation(acquire("target"))
                .unwrap(),
        );
        let session_id = crate::tests::stream_session_id("heartbeat-effect");
        let create = CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline_and_issuer(
            CreateStreamUploadReq {
                session_id: session_id.clone(), bucket: bucket.clone(), key: key.clone(),
                target: StreamUploadTarget::PutObject,
                // Metadata-only fixture. No shard payload is written or read.
                encryption: crate::tests::metadata_only_test_encryption(),
            },
            store.next_generation_id(&bucket, &key).unwrap(), 1_000, Some(100_000), proof.clone(),
            store.stream_allocation_issuer(NodeId::new(0)).unwrap(),
        );
        store.refresh_metadata_command_state_digest().unwrap();
        store
            .apply_metadata_command_and_record(
                0,
                &MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        ClusterEpoch::INITIAL,
                        PgId::new(0),
                        MetadataCommandLogIndex::new(1).unwrap(),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(create)),
                ),
            )
            .unwrap();
        Self { proof, session_id }
    }

    fn heartbeat(&self) -> DurableBucketWriteReservationHeartbeat<'_> {
        DurableBucketWriteReservationHeartbeat {
            name: &self.proof.bucket,
            reservation_id: &self.proof.reservation_id,
            owner_token: &self.proof.owner_token,
            cluster_epoch: self.proof.cluster_epoch,
            bucket_execution_generation: self.proof.bucket_execution_generation,
            bucket_incarnation_generation: self.proof.bucket_incarnation_generation,
            current_lease_deadline: self.proof.lease_deadline,
            lease_deadline: 30_000,
            now: 1_000,
        }
    }

    fn prepare_effect(&self, store: &PgStore, effect: HeartbeatEffect) {
        if effect == HeartbeatEffect::StreamProof {
            // The second hop starts only after the bucket-PG renewal committed.
            store
                .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                    self.heartbeat(),
                    AdmittedRouteEffectFence::unbounded(self.proof.cluster_epoch),
                )
                .unwrap();
        }
    }

    fn update(
        &self,
        store: &PgStore,
        effect: HeartbeatEffect,
        fence: AdmittedRouteEffectFence,
    ) -> Result<(), MetadataError> {
        match effect {
            HeartbeatEffect::BucketReservation => store
                .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                    self.heartbeat(),
                    fence,
                )
                .map(|_| ()),
            HeartbeatEffect::StreamProof => {
                let mut renewed = self.proof.clone();
                renewed.lease_deadline = 30_000;
                store.update_stream_upload_bucket_write_reservation_with_effect_fence(
                    &self.session_id,
                    &self.proof,
                    &renewed,
                    fence,
                )
            }
        }
    }

    fn observe(&self, store: &PgStore) -> (Vec<BucketWriteReservationRecord>, StreamUploadRecord) {
        (
            store
                .durable_bucket_write_reservations(&self.proof.bucket)
                .unwrap(),
            store.get_stream_upload(&self.session_id).unwrap(),
        )
    }
}

#[test]
fn heartbeat_effect_expiry_rejects_before_any_sql_change() {
    for effect in [
        HeartbeatEffect::BucketReservation,
        HeartbeatEffect::StreamProof,
    ] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let fixture = Fixture::new(&store, 20_000);
        fixture.prepare_effect(&store, effect);
        let before = fixture.observe(&store);
        let checkpoint = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let changes = store.conn.total_changes();
        let control = time.control();
        let error = PgStore::test_with_before_heartbeat_effect_hook(
            move |actual, in_transaction| {
                assert_eq!(actual, effect);
                assert!(in_transaction);
                control.set(4_500);
            },
            || {
                fixture.update(
                    &store,
                    effect,
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
        assert_eq!(store.conn.total_changes(), changes);
        assert_eq!(fixture.observe(&store), before);
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint
        );
        // The proof's epoch is deliberately older than the fresh authorizing
        // route. Neither hop may compare its historical epoch to route epoch 2.
        fixture
            .update(
                &store,
                effect,
                AdmittedRouteEffectFence::bounded(ClusterEpoch::new(2).unwrap(), 20_000, 19_000),
            )
            .unwrap();
        let after = fixture.observe(&store);
        assert_ne!(after, before);
        assert_eq!(after.0[0], before.0[0], "independent canary remains exact");
    }
}

#[test]
fn heartbeat_effect_does_not_revive_a_lease_expired_during_write_admission() {
    let time = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 0).unwrap();
    let fixture = Fixture::new(&store, 4_000);
    let before = fixture.observe(&store);
    let changes = store.conn.total_changes();
    let control = time.control();
    let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000);
    let error = PgStore::test_with_before_heartbeat_effect_hook(
        move |actual, in_transaction| {
            assert_eq!(actual, HeartbeatEffect::BucketReservation);
            assert!(in_transaction);
            control.set(4_500);
        },
        || fixture.update(&store, HeartbeatEffect::BucketReservation, fence),
    )
    .unwrap_err();
    assert!(
        matches!(error, MetadataError::BucketWriteReservationNotFound { .. }),
        "{error:?}"
    );
    assert_eq!(store.conn.total_changes(), changes);
    assert_eq!(fixture.observe(&store), before);
    assert!(fixture
        .update(&store, HeartbeatEffect::BucketReservation, fence)
        .is_err());
    assert_eq!(fixture.observe(&store), before);
}

#[test]
fn heartbeat_effect_rollback_preserves_both_records_and_allows_retry() {
    for effect in [
        HeartbeatEffect::BucketReservation,
        HeartbeatEffect::StreamProof,
    ] {
        for fail_commit in [false, true] {
            let _time = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let store = PgStore::open(dir.path(), 0).unwrap();
            let fixture = Fixture::new(&store, 20_000);
            fixture.prepare_effect(&store, effect);
            let before = fixture.observe(&store);
            if fail_commit {
                store.fail_next_metadata_txn_commit();
            } else {
                let table = match effect {
                    HeartbeatEffect::BucketReservation => "bucket_write_reservations",
                    HeartbeatEffect::StreamProof => "stream_uploads",
                };
                store
                    .conn
                    .execute_batch(&format!(
                        "CREATE TEMP TRIGGER fail_heartbeat BEFORE UPDATE ON {table}
                    BEGIN SELECT RAISE(ABORT, 'injected heartbeat update failure'); END"
                    ))
                    .unwrap();
            }
            let fence = AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL);
            let error = fixture.update(&store, effect, fence).unwrap_err();
            assert!(
                matches!(error, MetadataError::Db { context, .. }
                if context == match (effect, fail_commit) {
                    (HeartbeatEffect::BucketReservation, false) => "heartbeat durable bucket write reservation",
                    (HeartbeatEffect::BucketReservation, true) => "heartbeat durable bucket write reservation (commit txn)",
                    (HeartbeatEffect::StreamProof, false) => "update stream upload bucket write reservation",
                    (HeartbeatEffect::StreamProof, true) => "update stream upload bucket write reservation (commit txn)",
                }),
                "{error:?}"
            );
            assert!(store.conn.is_autocommit());
            assert_eq!(fixture.observe(&store), before);
            if !fail_commit {
                store
                    .conn
                    .execute_batch("DROP TRIGGER fail_heartbeat")
                    .unwrap();
            }
            fixture.update(&store, effect, fence).unwrap();
            assert_ne!(fixture.observe(&store), before);
        }
    }
}

#[test]
fn heartbeat_effect_has_one_commit_and_joins_outer_transaction() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    for effect in [
        HeartbeatEffect::BucketReservation,
        HeartbeatEffect::StreamProof,
    ] {
        let _time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        let fixture = Fixture::new(&store, 20_000);
        fixture.prepare_effect(&store, effect);
        let before = fixture.observe(&store);
        let fence = AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL);
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        fixture.update(&store, effect, fence).unwrap();
        assert!(!store.conn.is_autocommit());
        store.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(fixture.observe(&store), before);
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
        fixture.update(&store, effect, fence).unwrap();
        let statements = observed.lock().unwrap();
        let kinds = statements
            .iter()
            .map(|s| s.split_whitespace().next().unwrap())
            .collect::<Vec<_>>();
        let expected = match effect {
            HeartbeatEffect::BucketReservation => vec!["BEGIN", "UPDATE", "SELECT", "COMMIT"],
            HeartbeatEffect::StreamProof => vec!["BEGIN", "UPDATE", "COMMIT"],
        };
        assert_eq!(kinds, expected, "{statements:?}");
        assert_eq!(statements[0], "BEGIN IMMEDIATE");
    }
}
