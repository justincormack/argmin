use super::*;
use crate::metadata_command::{
    MetadataCommandId, MetadataCommandLogIndex, MetadataTransferCommand,
};
use crate::traits::PgMetadataStore;

fn test_owner() -> OwnerIdentity {
    OwnerIdentity::from_principal("owner")
}

fn create_bucket_probe_command(
    pg_id: u32,
    log_index: u64,
    bucket: BucketName,
    bucket_execution_generation: u64,
) -> MetadataCommandEnvelope {
    let owner = test_owner();
    let config = CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: &owner.principal,
        owner_canonical_id: &owner.canonical_id,
        acl_grants: &AclGrants::default(),
        public_read: false,
        public_write: false,
        versioning: BucketVersioningState::Disabled,
        object_lock: BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(pg_id),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&config, 123, bucket_execution_generation).unwrap(),
        ),
    )
}

fn mark_bucket_deleting_probe_command(
    store: &PgStore,
    log_index: u64,
    bucket: &BucketName,
) -> MetadataCommandEnvelope {
    let current = store.head_bucket_record_raw(bucket).unwrap();
    let deleting_generation = store.next_bucket_execution_generation_candidate().unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(store.pg_id),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(deleting_generation),
        )),
    )
}

fn delete_finalized_bucket_probe_command(
    store: &PgStore,
    log_index: u64,
    bucket: &BucketName,
) -> MetadataCommandEnvelope {
    let deleting = store.head_bucket_record_raw(bucket).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(store.pg_id),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            deleting.bucket_execution_generation,
            deleting.bucket_incarnation_generation,
        )),
    )
}

fn apply_delete_finalized_bucket_probe_command(
    store: &PgStore,
    node_id: u32,
    log_index: u64,
    bucket: &BucketName,
) -> MetadataCommandReplicaState {
    let command = delete_finalized_bucket_probe_command(store, log_index, bucket);
    store
        .apply_metadata_command_and_record(node_id, &command)
        .unwrap()
}

fn rebase_probe_commands(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    commands: &[MetadataCommandEnvelope],
) -> Vec<MetadataCommandEnvelope> {
    commands
        .iter()
        .map(|command| {
            MetadataCommandEnvelope::new(
                MetadataCommandId::new(cluster_epoch, pg_id, command.id().log_index()),
                command.payload().clone(),
            )
        })
        .collect()
}

fn metadata_transfer_commands(
    commands: Vec<MetadataCommandEnvelope>,
    post_state_digests: &[u64],
) -> Vec<MetadataTransferCommand> {
    assert_eq!(commands.len(), post_state_digests.len());
    commands
        .into_iter()
        .zip(post_state_digests.iter().copied())
        .map(|(command, post_state_digest)| MetadataTransferCommand {
            command,
            pre_state_digest: 0,
            post_state_digest,
        })
        .collect()
}

fn test_bucket_write_reservation_proof(
    bucket: &BucketName,
    key: &ObjectKey,
    operation_kind: &str,
) -> BucketWriteReservationProof {
    BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: format!("{operation_kind}-proof"),
        owner_token: format!("{operation_kind}-proof-owner"),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: operation_kind.to_string(),
        created_at: 1,
        lease_deadline: 2,
        target_context: Some(key.as_str().to_string()),
    }
}

fn create_probe_bucket_direct(store: &PgStore, bucket: &BucketName) {
    let owner = test_owner();
    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
}

fn put_probe_lifecycle_direct(store: &PgStore, bucket: &BucketName) {
    store
        .put_bucket_subresource(
            bucket,
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
}

#[test]
fn create_bucket_with_config_persists_required_ownership_controls() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket = trusted_bucket_name("raw-create-ownership");
    let owner = test_owner();
    let acl_grants = AclGrants::default();
    let ownership_controls = crate::BucketOwnershipControls {
        object_ownership: crate::BucketObjectOwnership::BucketOwnerPreferred,
    };

    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls,
        })
        .unwrap();

    assert_eq!(
        store
            .head_bucket_record_raw(&bucket)
            .unwrap()
            .ownership_controls,
        Some(ownership_controls)
    );
}

#[test]
fn lifecycle_sweep_claim_is_bucket_incarnation_owner_and_expires() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket = trusted_bucket_name("lifecycle-claim-bucket");
    create_probe_bucket_direct(&store, &bucket);
    put_probe_lifecycle_direct(&store, &bucket);
    let record = store.head_bucket_record_raw(&bucket).unwrap();

    let first = store
        .acquire_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("active bucket should be lifecycle-claimable");
    assert_eq!(first.claim_id, "claim-a");
    assert_eq!(first.pg_id, 12);
    assert_eq!(first.claimed_at, 10);
    assert_eq!(first.heartbeat_at, 10);
    assert_eq!(first.attempt_count, 1);

    assert!(
        store
            .acquire_lifecycle_sweep_claim(
                &bucket,
                record.bucket_incarnation_generation,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                11,
                Some(30),
                11,
            )
            .unwrap()
            .is_none(),
        "non-expired lifecycle claim must block another owner"
    );

    let idempotent = store
        .acquire_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            12,
            Some(40),
            12,
        )
        .unwrap()
        .expect("same owner should reacquire idempotently");
    assert_eq!(idempotent, first);

    let heartbeat = store
        .heartbeat_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            15,
            Some(25),
        )
        .unwrap();
    assert_eq!(heartbeat.claim_id, "claim-a");
    assert_eq!(heartbeat.heartbeat_at, 15);
    assert_eq!(heartbeat.lease_deadline, Some(25));

    let errored = store
        .record_lifecycle_sweep_claim_error(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            "candidate scan failed",
        )
        .unwrap();
    assert_eq!(errored.claim_id, "claim-a");
    assert_eq!(errored.last_error.as_deref(), Some("candidate scan failed"));

    let stale_error_record = store
        .record_lifecycle_sweep_claim_error(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-b",
            ClusterEpoch::INITIAL,
            "wrong owner",
        )
        .unwrap_err();
    assert!(matches!(
        stale_error_record,
        MetadataError::ReclaimClaimConflict { .. }
    ));

    let stolen = store
        .acquire_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            26,
            Some(40),
            26,
        )
        .unwrap()
        .expect("expired lifecycle claim should be stealable");
    assert_eq!(stolen.claim_id, "claim-b");
    assert_eq!(stolen.attempt_count, 2);
    assert_eq!(
        stolen.last_error.as_deref(),
        Some("candidate scan failed"),
        "stealing an expired lifecycle claim should preserve retry context"
    );

    let stale_release = store
        .release_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        stale_release,
        MetadataError::ReclaimClaimConflict { .. }
    ));

    store
        .release_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
    let missing_release = store
        .release_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        missing_release,
        MetadataError::ReclaimClaimNotFound { .. }
    ));
}

#[test]
fn lifecycle_sweep_claim_rejects_deleting_or_drained_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let drained = trusted_bucket_name("lifecycle-drained-bucket");
    let deleting = trusted_bucket_name("lifecycle-deleting-bucket");
    create_probe_bucket_direct(&store, &drained);
    create_probe_bucket_direct(&store, &deleting);
    put_probe_lifecycle_direct(&store, &drained);
    put_probe_lifecycle_direct(&store, &deleting);
    let drained_record = store.head_bucket_record_raw(&drained).unwrap();
    store
        .begin_durable_bucket_write_drain(
            &drained,
            "delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            10,
            Some(30),
        )
        .unwrap();

    assert!(
        store
            .acquire_lifecycle_sweep_claim(
                &drained,
                drained_record.bucket_incarnation_generation,
                "claim",
                "owner",
                ClusterEpoch::INITIAL,
                11,
                Some(30),
                11,
            )
            .unwrap()
            .is_none(),
        "active delete drains must fence lifecycle claims"
    );

    store.mark_bucket_deleting(&deleting).unwrap();
    let deleting_record = store.head_bucket_record_raw(&deleting).unwrap();
    assert!(
        store
            .acquire_lifecycle_sweep_claim(
                &deleting,
                deleting_record.bucket_incarnation_generation,
                "claim",
                "owner",
                ClusterEpoch::INITIAL,
                11,
                Some(30),
                11,
            )
            .unwrap()
            .is_none(),
        "Deleting buckets must not be lifecycle-claimable"
    );
}

#[test]
fn bucket_delete_attempt_outcome_records_last_state() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket = trusted_bucket_name("delete-attempt-outcome-bucket");
    create_probe_bucket_direct(&store, &bucket);

    let drain = store
        .begin_durable_bucket_write_drain(
            &bucket,
            "delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            10,
            Some(100),
        )
        .unwrap();
    let first = BucketDeleteAttemptOutcomeRecord {
        bucket: bucket.clone(),
        drain_id: drain.drain_id.clone(),
        cluster_epoch: drain.cluster_epoch,
        bucket_execution_generation: drain.bucket_execution_generation,
        outcome: BucketDeleteAttemptOutcomeKind::Retryable,
        phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
        detail: "route expired".to_string(),
        post_reservation_next_object_pg_id: Some(7),
        updated_at: 11,
    };
    store.record_bucket_delete_attempt_outcome(&first).unwrap();
    assert_eq!(
        store.bucket_delete_attempt_outcome(&bucket).unwrap(),
        Some(first.clone())
    );

    let second = BucketDeleteAttemptOutcomeRecord {
        outcome: BucketDeleteAttemptOutcomeKind::MarkDeleting,
        phase: BucketDeleteAttemptPhase::MarkDeleting,
        detail: "mark bucket deleting applied".to_string(),
        updated_at: 12,
        ..first
    };
    store.record_bucket_delete_attempt_outcome(&second).unwrap();
    assert_eq!(
        store.bucket_delete_attempt_outcome(&bucket).unwrap(),
        Some(second)
    );
}

#[test]
fn bucket_write_drain_heartbeat_fences_stale_delete_owner() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket = trusted_bucket_name("delete-drain-heartbeat-bucket");
    let expired_bucket = trusted_bucket_name("expired-delete-drain-heartbeat-bucket");
    create_probe_bucket_direct(&store, &bucket);
    create_probe_bucket_direct(&store, &expired_bucket);

    let record = store
        .begin_durable_bucket_write_drain(
            &bucket,
            "delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            10,
            Some(100),
        )
        .unwrap();
    let renewed = store
        .heartbeat_durable_bucket_write_drain(
            &bucket,
            "delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            record.bucket_execution_generation,
            200,
            50,
        )
        .unwrap();
    assert_eq!(renewed.lease_deadline, Some(200));

    let wrong_owner = store
        .heartbeat_durable_bucket_write_drain(
            &bucket,
            "delete-drain",
            "other-owner",
            ClusterEpoch::INITIAL,
            record.bucket_execution_generation,
            300,
            60,
        )
        .unwrap_err();
    assert!(matches!(
        wrong_owner,
        MetadataError::BucketWriteDrainConflict { .. }
    ));

    let expired = store
        .begin_durable_bucket_write_drain(
            &expired_bucket,
            "expired-delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
        )
        .unwrap();
    let expired_heartbeat = store
        .heartbeat_durable_bucket_write_drain(
            &expired_bucket,
            "expired-delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            expired.bucket_execution_generation,
            200,
            20,
        )
        .unwrap_err();
    assert!(matches!(
        expired_heartbeat,
        MetadataError::BucketWriteDrainConflict { .. }
    ));

    store.mark_bucket_deleting(&bucket).unwrap();
    let inactive_bucket = store
        .heartbeat_durable_bucket_write_drain(
            &bucket,
            "delete-drain",
            "delete-owner",
            ClusterEpoch::INITIAL,
            record.bucket_execution_generation,
            400,
            70,
        )
        .unwrap_err();
    assert!(matches!(
        inactive_bucket,
        MetadataError::BucketWriteDrainConflict { .. }
    ));
}

#[test]
fn lifecycle_sweep_roots_include_expired_claim_before_earlier_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket_a = trusted_bucket_name("lifecycle-root-a");
    let bucket_b = trusted_bucket_name("lifecycle-root-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    put_probe_lifecycle_direct(&store, &bucket_b);
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    store
        .acquire_lifecycle_sweep_claim(
            &bucket_b,
            bucket_b_record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("later lifecycle bucket should be claimable");

    put_probe_lifecycle_direct(&store, &bucket_a);
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    assert_eq!(
        store.get_lifecycle_sweep_roots(21, 16).unwrap(),
        vec![
            LifecycleSweepRoot {
                bucket: bucket_b.clone(),
                bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::ExpiredClaim,
            },
            LifecycleSweepRoot {
                bucket: bucket_a,
                bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::LifecycleConfig,
            },
        ],
        "expired lifecycle claim work must be rediscovered before unrelated roots"
    );
    assert_eq!(
        store.get_lifecycle_sweep_roots(21, 1).unwrap(),
        vec![LifecycleSweepRoot {
            bucket: bucket_b,
            bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
            source: LifecycleSweepRootSource::ExpiredClaim,
        }],
        "claim recovery must not be hidden by an earlier lifecycle bucket"
    );
}

#[test]
fn lifecycle_sweep_roots_skip_busy_null_deadline_claim() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket_a = trusted_bucket_name("lifecycle-null-root-a");
    let bucket_b = trusted_bucket_name("lifecycle-null-root-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    put_probe_lifecycle_direct(&store, &bucket_b);
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    store
        .acquire_lifecycle_sweep_claim(
            &bucket_b,
            bucket_b_record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            10,
            None,
            10,
        )
        .unwrap()
        .expect("later lifecycle bucket should be claimable");

    assert_eq!(
        store.get_lifecycle_sweep_roots(21, 16).unwrap(),
        vec![LifecycleSweepRoot {
            bucket: bucket_b.clone(),
            bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
            source: LifecycleSweepRootSource::BusyClaim,
        }],
        "non-expiring lifecycle claims should surface as busy work, not disappear from the scan"
    );

    put_probe_lifecycle_direct(&store, &bucket_a);
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    assert_eq!(
        store.get_lifecycle_sweep_roots(21, 16).unwrap(),
        vec![
            LifecycleSweepRoot {
                bucket: bucket_b,
                bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::BusyClaim,
            },
            LifecycleSweepRoot {
                bucket: bucket_a,
                bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::LifecycleConfig,
            },
        ],
        "busy claimed buckets should not hide unrelated unclaimed lifecycle roots"
    );
}

#[test]
fn lifecycle_sweep_old_incarnation_busy_claim_does_not_block_current_incarnation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let bucket = trusted_bucket_name("lifecycle-recreated-bucket");
    create_probe_bucket_direct(&store, &bucket);
    put_probe_lifecycle_direct(&store, &bucket);
    let record = store.head_bucket_record_raw(&bucket).unwrap();
    let old_incarnation = record.bucket_incarnation_generation.saturating_sub(1);

    store
        .test_insert_lifecycle_sweep_claim(
            &bucket,
            old_incarnation,
            "old-claim",
            "old-owner",
            ClusterEpoch::INITIAL,
            None,
        )
        .unwrap();

    assert_eq!(
        store.get_lifecycle_sweep_roots(10, 16).unwrap(),
        vec![LifecycleSweepRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: record.bucket_incarnation_generation,
            source: LifecycleSweepRootSource::LifecycleConfig,
        }],
        "non-expiring old-incarnation claims must not hide the current lifecycle root"
    );

    let current_claim = store
        .acquire_lifecycle_sweep_claim(
            &bucket,
            record.bucket_incarnation_generation,
            "current-claim",
            "current-owner",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("current incarnation must be claimable despite stale old-incarnation claim");
    assert_eq!(current_claim.claim_id, "current-claim");
}

#[test]
fn lifecycle_sweep_roots_clear_expired_stale_claims_before_limit() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 12).unwrap();
    let stale_bucket = trusted_bucket_name("lifecycle-stale-claim");
    let recreated_bucket = trusted_bucket_name("lifecycle-recreated-expired");
    let valid_bucket = trusted_bucket_name("lifecycle-valid-root");
    create_probe_bucket_direct(&store, &stale_bucket);
    create_probe_bucket_direct(&store, &recreated_bucket);
    create_probe_bucket_direct(&store, &valid_bucket);
    put_probe_lifecycle_direct(&store, &recreated_bucket);
    put_probe_lifecycle_direct(&store, &valid_bucket);
    let stale_record = store.head_bucket_record_raw(&stale_bucket).unwrap();
    let recreated_record = store.head_bucket_record_raw(&recreated_bucket).unwrap();
    let valid_record = store.head_bucket_record_raw(&valid_bucket).unwrap();

    store
        .test_insert_lifecycle_sweep_claim(
            &stale_bucket,
            stale_record.bucket_incarnation_generation,
            "stale-no-work",
            "owner",
            ClusterEpoch::INITIAL,
            Some(10),
        )
        .unwrap();
    store
        .test_insert_lifecycle_sweep_claim(
            &recreated_bucket,
            recreated_record
                .bucket_incarnation_generation
                .saturating_sub(1),
            "stale-incarnation",
            "owner",
            ClusterEpoch::INITIAL,
            Some(10),
        )
        .unwrap();

    assert_eq!(
        store.get_lifecycle_sweep_roots(11, 1).unwrap(),
        vec![LifecycleSweepRoot {
            bucket: recreated_bucket.clone(),
            bucket_incarnation_generation: recreated_record.bucket_incarnation_generation,
            source: LifecycleSweepRootSource::LifecycleConfig,
        }],
        "stale expired lifecycle claims must not fill the expired-root scan limit"
    );
    assert_eq!(
        store.get_lifecycle_sweep_roots(11, 16).unwrap(),
        vec![
            LifecycleSweepRoot {
                bucket: recreated_bucket,
                bucket_incarnation_generation: recreated_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::LifecycleConfig,
            },
            LifecycleSweepRoot {
                bucket: valid_bucket,
                bucket_incarnation_generation: valid_record.bucket_incarnation_generation,
                source: LifecycleSweepRootSource::LifecycleConfig,
            },
        ],
    );
    assert!(
        store
            .acquire_lifecycle_sweep_claim(
                &stale_bucket,
                stale_record.bucket_incarnation_generation,
                "new-claim",
                "owner",
                ClusterEpoch::INITIAL,
                12,
                Some(20),
                12,
            )
            .unwrap()
            .is_none(),
        "active buckets without lifecycle or aborting uploads should not be claimable"
    );
}

#[test]
fn object_payload_reclaim_claim_is_single_owner_and_expires() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 9).unwrap();
    let bucket = trusted_bucket_name("claim-bucket");
    let key = trusted_object_key("object");
    let generation_id = GenerationId::new(11).unwrap();

    assert!(
        store
            .acquire_object_payload_reclaim_claim(
                &bucket,
                3,
                &key,
                generation_id,
                ObjectPayloadReclaimKind::ObjectSegments,
                "claim-a",
                "owner-a",
                ClusterEpoch::INITIAL,
                10,
                Some(20),
                10,
            )
            .unwrap()
            .is_none(),
        "claim must not be acquired without a durable reclaim root"
    );

    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 1,
            segments: Vec::new(),
        })
        .unwrap();

    let first = store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("first worker should acquire claim");
    assert_eq!(first.claim_id, "claim-a");
    assert_eq!(first.pg_id, 9);
    assert_eq!(first.attempt_count, 1);

    assert!(
        store
            .acquire_object_payload_reclaim_claim(
                &bucket,
                3,
                &key,
                generation_id,
                ObjectPayloadReclaimKind::ObjectSegments,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                11,
                Some(30),
                11,
            )
            .unwrap()
            .is_none(),
        "non-expired claim must block another owner"
    );

    let idempotent = store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            12,
            Some(40),
            12,
        )
        .unwrap()
        .expect("same owner should reacquire idempotently");
    assert_eq!(idempotent, first);

    let stolen = store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            21,
            Some(40),
            21,
        )
        .unwrap()
        .expect("expired claim should be stealable");
    assert_eq!(stolen.claim_id, "claim-b");
    assert_eq!(stolen.attempt_count, 2);

    let stale_release = store
        .release_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        stale_release,
        MetadataError::ReclaimClaimConflict { .. }
    ));

    store
        .release_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
    let missing_release = store
        .release_object_payload_reclaim_claim(
            &bucket,
            3,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        missing_release,
        MetadataError::ReclaimClaimNotFound { .. }
    ));
}

#[test]
fn object_payload_reclaim_claim_release_is_bucket_incarnation_fenced() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 10).unwrap();
    let bucket = trusted_bucket_name("claim-incarnation-bucket");
    let key = trusted_object_key("object");
    let generation_id = GenerationId::new(12).unwrap();
    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 1,
            segments: Vec::new(),
        })
        .unwrap();

    store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            7,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim",
            "owner",
            ClusterEpoch::INITIAL,
            10,
            None,
            10,
        )
        .unwrap()
        .expect("claim should be acquired");

    let wrong_incarnation = store
        .release_object_payload_reclaim_claim(
            &bucket,
            8,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim",
            "owner",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        wrong_incarnation,
        MetadataError::ReclaimClaimConflict { .. }
    ));

    store
        .release_object_payload_reclaim_claim(
            &bucket,
            7,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim",
            "owner",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
}

#[test]
fn object_payload_reclaim_claim_does_not_clear_expired_different_root() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 10).unwrap();
    let bucket = trusted_bucket_name("claim-different-root-bucket");
    let key_a = trusted_object_key("object-a");
    let key_b = trusted_object_key("object-b");
    let generation_id = GenerationId::new(12).unwrap();
    for key in [&key_a, &key_b] {
        store
            .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
                created_at: 1,
                segments: Vec::new(),
            })
            .unwrap();
    }

    let first = store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            7,
            &key_a,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("first root should be claimable");
    assert_eq!(first.claim_id, "claim-a");

    assert!(
        store
            .acquire_object_payload_reclaim_claim(
                &bucket,
                7,
                &key_b,
                generation_id,
                ObjectPayloadReclaimKind::ObjectSegments,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                21,
                Some(40),
                21,
            )
            .unwrap()
            .is_none(),
        "expired different-root claim must remain the PG work-class owner"
    );

    let still_owned = store
        .acquire_object_payload_reclaim_claim(
            &bucket,
            7,
            &key_a,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            22,
            Some(50),
            22,
        )
        .unwrap()
        .expect("original claim identity must remain visible");
    assert_eq!(still_owned, first);

    store
        .release_object_payload_reclaim_claim(
            &bucket,
            7,
            &key_a,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
}

#[test]
fn bucket_delete_finalize_claim_requires_deleting_bucket_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket = trusted_bucket_name("finalize-claim-bucket");
    create_probe_bucket_direct(&store, &bucket);
    let active = store.head_bucket_record_raw(&bucket).unwrap();

    assert!(
        store
            .acquire_bucket_delete_finalize_claim(
                &bucket,
                active.bucket_incarnation_generation,
                "claim-a",
                "owner-a",
                ClusterEpoch::INITIAL,
                10,
                Some(20),
                10,
            )
            .unwrap()
            .is_none(),
        "active buckets must not be finalizer-claimable"
    );

    store.mark_bucket_deleting(&bucket).unwrap();
    let deleting = store.head_bucket_record_raw(&bucket).unwrap();
    let first = store
        .acquire_bucket_delete_finalize_claim(
            &bucket,
            deleting.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("deleting bucket should be finalizer-claimable");
    assert_eq!(first.claim_id, "claim-a");
    assert_eq!(first.pg_id, 11);
    assert_eq!(first.attempt_count, 1);

    assert!(
        store
            .acquire_bucket_delete_finalize_claim(
                &bucket,
                deleting.bucket_incarnation_generation,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                11,
                Some(30),
                11,
            )
            .unwrap()
            .is_none(),
        "non-expired finalizer claim must block another owner"
    );

    let wrong_incarnation = store
        .release_bucket_delete_finalize_claim(
            &bucket,
            deleting.bucket_incarnation_generation + 1,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        wrong_incarnation,
        MetadataError::ReclaimClaimConflict { .. }
    ));

    let stolen = store
        .acquire_bucket_delete_finalize_claim(
            &bucket,
            deleting.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            21,
            Some(40),
            21,
        )
        .unwrap()
        .expect("expired finalizer claim should be stealable");
    assert_eq!(stolen.claim_id, "claim-b");
    assert_eq!(stolen.attempt_count, 2);

    store
        .release_bucket_delete_finalize_claim(
            &bucket,
            deleting.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
}

#[test]
fn delete_finalized_bucket_clears_finalizer_claim() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-claim-clear-a");
    let bucket_b = trusted_bucket_name("finalize-claim-clear-b");
    let create_a = create_bucket_probe_command(11, 1, bucket_a.clone(), 1);
    store
        .apply_metadata_command_and_record(0, &create_a)
        .unwrap();
    let mark_a = mark_bucket_deleting_probe_command(&store, 2, &bucket_a);
    store.apply_metadata_command_and_record(0, &mark_a).unwrap();
    let deleting_a = store.head_bucket_record_raw(&bucket_a).unwrap();
    store
        .acquire_bucket_delete_finalize_claim(
            &bucket_a,
            deleting_a.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(70_000),
            10,
        )
        .unwrap()
        .expect("deleting bucket should be finalizer-claimable");

    apply_delete_finalized_bucket_probe_command(&store, 0, 3, &bucket_a);
    let released = store
        .release_bucket_delete_finalize_claim(
            &bucket_a,
            deleting_a.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap_err();
    assert!(matches!(
        released,
        MetadataError::ReclaimClaimNotFound { .. }
    ));

    let create_b = create_bucket_probe_command(11, 4, bucket_b.clone(), 4);
    store
        .apply_metadata_command_and_record(0, &create_b)
        .unwrap();
    let mark_b = mark_bucket_deleting_probe_command(&store, 5, &bucket_b);
    store.apply_metadata_command_and_record(0, &mark_b).unwrap();
    let deleting_b = store.head_bucket_record_raw(&bucket_b).unwrap();
    assert!(
        store
            .acquire_bucket_delete_finalize_claim(
                &bucket_b,
                deleting_b.bucket_incarnation_generation,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                11,
                Some(70_000),
                11,
            )
            .unwrap()
            .is_some(),
        "finalized bucket deletion must clear the singleton PG claim before later bucket work"
    );
}

#[test]
fn delete_finalized_bucket_command_advances_metadata_command_log() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket = trusted_bucket_name("finalize-command-log");
    let create = create_bucket_probe_command(11, 1, bucket.clone(), 1);
    store.apply_metadata_command_and_record(0, &create).unwrap();

    let mark = mark_bucket_deleting_probe_command(&store, 2, &bucket);
    store.apply_metadata_command_and_record(0, &mark).unwrap();
    let before_delete = store.metadata_command_replica_state().unwrap();
    let delete = delete_finalized_bucket_probe_command(&store, 3, &bucket);

    let after_delete = store.apply_metadata_command_and_record(0, &delete).unwrap();
    assert_eq!(after_delete.applied_log_index, 3);
    assert_ne!(
        after_delete.applied_log_hash,
        before_delete.applied_log_hash
    );
    assert_eq!(
        after_delete.state_digest,
        store.cached_metadata_state_digest().unwrap()
    );
    assert_eq!(
        after_delete.state_digest,
        store.metadata_state_digest().unwrap()
    );
    assert!(matches!(
        store.head_bucket_record_raw(&bucket),
        Err(MetadataError::BucketNotFound { .. })
    ));
}

#[test]
fn stale_delete_finalized_bucket_command_does_not_delete_recreated_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket = trusted_bucket_name("finalize-command-stale");
    let create = create_bucket_probe_command(11, 1, bucket.clone(), 1);
    store.apply_metadata_command_and_record(0, &create).unwrap();

    let mark = mark_bucket_deleting_probe_command(&store, 2, &bucket);
    store.apply_metadata_command_and_record(0, &mark).unwrap();
    let old_deleting = store.head_bucket_record_raw(&bucket).unwrap();
    apply_delete_finalized_bucket_probe_command(&store, 0, 3, &bucket);

    let recreate = create_bucket_probe_command(
        11,
        4,
        bucket.clone(),
        old_deleting.bucket_execution_generation + 1,
    );
    store
        .apply_metadata_command_and_record(0, &recreate)
        .unwrap();
    let recreated = store.head_bucket_record_raw(&bucket).unwrap();
    assert_eq!(recreated.state, BucketState::Active);
    assert_ne!(
        recreated.bucket_execution_generation,
        old_deleting.bucket_execution_generation
    );
    let stale_delete = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(11),
            MetadataCommandLogIndex::new(5).unwrap(),
        ),
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            old_deleting.bucket_execution_generation,
            old_deleting.bucket_incarnation_generation,
        )),
    );

    let after_stale = store
        .apply_metadata_command_and_record(0, &stale_delete)
        .unwrap();
    assert_eq!(after_stale.applied_log_index, 5);
    assert_eq!(store.head_bucket_record_raw(&bucket).unwrap(), recreated);
}

#[test]
fn delete_finalized_bucket_command_commit_failure_invalidates_clean_digest_revision() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket = trusted_bucket_name("finalize-command-rollback");
    let create = create_bucket_probe_command(11, 1, bucket.clone(), 1);
    store.apply_metadata_command_and_record(0, &create).unwrap();

    let mark = mark_bucket_deleting_probe_command(&store, 2, &bucket);
    store.apply_metadata_command_and_record(0, &mark).unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    assert_ne!(
        store.clean_metadata_digest_revision.load(Ordering::Relaxed),
        UNCLEAN_METADATA_DIGEST_REVISION
    );

    store.fail_next_metadata_txn_commit();
    let delete = delete_finalized_bucket_probe_command(&store, 3, &bucket);
    let err = store
        .apply_metadata_command_and_record(0, &delete)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(_)
            | BucketSnapshotLoadError::Metadata(MetadataError::Db { .. })
    ));
    assert_eq!(
        store.clean_metadata_digest_revision.load(Ordering::Relaxed),
        UNCLEAN_METADATA_DIGEST_REVISION
    );
    assert_eq!(
        store.head_bucket_record_raw(&bucket).unwrap().state,
        BucketState::Deleting
    );
}

#[test]
fn fail_next_metadata_txn_commit_rolls_back_common_metadata_transaction() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("generic-commit-failure");

    store.fail_next_metadata_txn_commit();
    let err = PgMetadataStore::create_bucket(
        &store,
        &bucket,
        "owner",
        &CanonicalUserId::from_principal("owner"),
        &AclGrants::default(),
        false,
        false,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        MetadataError::Db {
            context: "create bucket (commit txn)",
            ..
        }
    ));
    assert!(
        matches!(
            store.head_bucket_record_raw(&bucket),
            Err(MetadataError::BucketNotFound { .. })
        ),
        "injected commit failure must roll back the metadata transaction"
    );
    assert_eq!(
        store.clean_metadata_digest_revision.load(Ordering::Relaxed),
        UNCLEAN_METADATA_DIGEST_REVISION
    );
}

#[test]
fn get_bucket_delete_finalize_roots_returns_deleting_buckets_in_order() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-root-a");
    let bucket_b = trusted_bucket_name("finalize-root-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);

    assert!(
        store
            .get_bucket_delete_finalize_roots(10, 16)
            .unwrap()
            .is_empty(),
        "active buckets are not finalizer roots"
    );

    store.mark_bucket_deleting(&bucket_b).unwrap();
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();
    assert_eq!(
        store.get_bucket_delete_finalize_roots(10, 16).unwrap(),
        vec![BucketDeleteFinalizeRoot {
            bucket: bucket_b.clone(),
            bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
        }]
    );

    store.mark_bucket_deleting(&bucket_a).unwrap();
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    assert_eq!(
        store.get_bucket_delete_finalize_roots(10, 16).unwrap(),
        vec![
            BucketDeleteFinalizeRoot {
                bucket: bucket_a,
                bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
            },
            BucketDeleteFinalizeRoot {
                bucket: bucket_b,
                bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
            }
        ],
        "scan roots should be deterministic when multiple deleting buckets exist"
    );
    assert_eq!(
        store.get_bucket_delete_finalize_roots(10, 1).unwrap(),
        vec![BucketDeleteFinalizeRoot {
            bucket: trusted_bucket_name("finalize-root-a"),
            bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
        }],
        "scan root limit should bound work per pass"
    );
}

#[test]
fn bucket_delete_finalize_claim_does_not_clear_expired_different_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-claim-bucket-a");
    let bucket_b = trusted_bucket_name("finalize-claim-bucket-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    store.mark_bucket_deleting(&bucket_a).unwrap();
    store.mark_bucket_deleting(&bucket_b).unwrap();
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    let first = store
        .acquire_bucket_delete_finalize_claim(
            &bucket_a,
            bucket_a_record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("first bucket finalizer should be claimable");
    assert_eq!(first.claim_id, "claim-a");

    assert!(
        store
            .acquire_bucket_delete_finalize_claim(
                &bucket_b,
                bucket_b_record.bucket_incarnation_generation,
                "claim-b",
                "owner-b",
                ClusterEpoch::INITIAL,
                21,
                Some(40),
                21,
            )
            .unwrap()
            .is_none(),
        "expired different-bucket finalizer claim must remain the PG work-class owner"
    );

    let still_owned = store
        .acquire_bucket_delete_finalize_claim(
            &bucket_a,
            bucket_a_record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            22,
            Some(50),
            22,
        )
        .unwrap()
        .expect("original finalizer claim identity must remain visible");
    assert_eq!(still_owned, first);

    store
        .release_bucket_delete_finalize_claim(
            &bucket_a,
            bucket_a_record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
        )
        .unwrap();
}

#[test]
fn bucket_delete_finalize_claim_clears_expired_non_deleting_different_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-stale-claim-a");
    let bucket_b = trusted_bucket_name("finalize-stale-claim-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    store.mark_bucket_deleting(&bucket_a).unwrap();
    store.mark_bucket_deleting(&bucket_b).unwrap();
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    store
        .acquire_bucket_delete_finalize_claim(
            &bucket_b,
            bucket_b_record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("later bucket finalizer should be claimable");
    store
        .connection()
        .execute(
            "UPDATE buckets SET state = ?1 WHERE name = ?2",
            params![BucketState::Active as u8, &bucket_b],
        )
        .unwrap();

    let claimed_a = store
        .acquire_bucket_delete_finalize_claim(
            &bucket_a,
            bucket_a_record.bucket_incarnation_generation,
            "claim-a",
            "owner-a",
            ClusterEpoch::INITIAL,
            21,
            Some(40),
            21,
        )
        .unwrap()
        .expect("expired non-deleting different-bucket claim should be cleared");
    assert_eq!(claimed_a.bucket, bucket_a);
    assert_eq!(claimed_a.claim_id, "claim-a");
    assert_eq!(claimed_a.attempt_count, 1);

    store
        .release_bucket_delete_finalize_claim(
            &claimed_a.bucket,
            claimed_a.bucket_incarnation_generation,
            &claimed_a.claim_id,
            &claimed_a.owner_token,
            claimed_a.cluster_epoch,
        )
        .unwrap();
}

#[test]
fn bucket_delete_finalize_roots_include_expired_claim_before_earlier_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-claim-root-a");
    let bucket_b = trusted_bucket_name("finalize-claim-root-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    store.mark_bucket_deleting(&bucket_b).unwrap();
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    store
        .acquire_bucket_delete_finalize_claim(
            &bucket_b,
            bucket_b_record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("later deleting bucket should be claimable");

    store.mark_bucket_deleting(&bucket_a).unwrap();
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    assert_eq!(
        store.get_bucket_delete_finalize_roots(21, 16).unwrap(),
        vec![
            BucketDeleteFinalizeRoot {
                bucket: bucket_b.clone(),
                bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
            },
            BucketDeleteFinalizeRoot {
                bucket: bucket_a,
                bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
            },
        ],
        "expired singleton claim work must be rediscovered before unrelated roots"
    );
    assert_eq!(
        store.get_bucket_delete_finalize_roots(21, 1).unwrap(),
        vec![BucketDeleteFinalizeRoot {
            bucket: bucket_b,
            bucket_incarnation_generation: bucket_b_record.bucket_incarnation_generation,
        }],
        "claim recovery must not be hidden by an earlier deleting bucket"
    );
}

#[test]
fn bucket_delete_finalize_roots_skip_busy_null_deadline_claim() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 11).unwrap();
    let bucket_a = trusted_bucket_name("finalize-null-claim-root-a");
    let bucket_b = trusted_bucket_name("finalize-null-claim-root-b");
    create_probe_bucket_direct(&store, &bucket_a);
    create_probe_bucket_direct(&store, &bucket_b);
    store.mark_bucket_deleting(&bucket_b).unwrap();
    let bucket_b_record = store.head_bucket_record_raw(&bucket_b).unwrap();

    store
        .acquire_bucket_delete_finalize_claim(
            &bucket_b,
            bucket_b_record.bucket_incarnation_generation,
            "claim-b",
            "owner-b",
            ClusterEpoch::INITIAL,
            10,
            None,
            10,
        )
        .unwrap()
        .expect("later deleting bucket should be claimable");

    assert!(
        store
            .get_bucket_delete_finalize_roots(21, 16)
            .unwrap()
            .is_empty(),
        "non-expiring finalizer claims should remain busy, not expired scan work"
    );

    store.mark_bucket_deleting(&bucket_a).unwrap();
    let bucket_a_record = store.head_bucket_record_raw(&bucket_a).unwrap();
    assert_eq!(
        store.get_bucket_delete_finalize_roots(21, 16).unwrap(),
        vec![BucketDeleteFinalizeRoot {
            bucket: bucket_a,
            bucket_incarnation_generation: bucket_a_record.bucket_incarnation_generation,
        }],
        "busy claimed buckets should not hide unrelated unclaimed deleting roots"
    );
}

fn assert_metadata_state_digest_mismatch(err: StoreError) {
    assert!(
        matches!(err, StoreError::MetadataStateDigestMismatch { .. }),
        "expected metadata state digest mismatch, got {err:?}"
    );
}

fn assert_metadata_state_digest_covers_mutation(
    setup: impl FnOnce(&PgStore),
    mutate: impl FnOnce(&PgStore),
) {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    setup(&store);
    store.refresh_metadata_command_state_digest().unwrap();
    mutate(&store);

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

fn assert_cached_metadata_digest_matches_materialized(store: &PgStore) {
    for table in crate::pg_store::command_log::METADATA_DIGEST_TABLES {
        let cached = store.cached_metadata_table_digest(table).unwrap();
        let materialized = store.metadata_table_digest(table).unwrap();
        assert_eq!(
            cached, materialized,
            "cached digest for {} should match materialized rows",
            table.name
        );
    }
    let cached = store.cached_metadata_state_digest().unwrap();
    let materialized = store.metadata_state_digest().unwrap();
    assert_eq!(cached, materialized);
}

fn assert_pg_recovery_invariants_after_commit_failure(
    store: &PgStore,
    case_name: &str,
) -> MetadataCommandReplicaState {
    assert_cached_metadata_digest_matches_materialized(store);
    let state = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_or_else(|err| panic!("{case_name}: replay validation failed: {err:?}"));
    assert_eq!(
        state.state_digest,
        store.metadata_state_digest().unwrap(),
        "{case_name}: replica state digest should match materialized rows"
    );
    assert!(
        store
            .pending_metadata_command_slot_any_epoch(0)
            .unwrap()
            .is_none(),
        "{case_name}: recovered PG should not retain a pending slot"
    );
    state
}

fn assert_pg_accepts_next_command_after_recovery(
    store: &PgStore,
    case_name: &str,
    state: &MetadataCommandReplicaState,
) {
    let next_bucket = trusted_bucket_name(format!("recovered-next-{case_name}"));
    let next_command = create_bucket_probe_command(
        1,
        state
            .applied_log_index
            .checked_add(1)
            .expect("test command log index can advance"),
        next_bucket,
        10_000 + state.applied_log_index,
    );
    assert_eq!(
        store
            .metadata_command_acceptance(0, &next_command)
            .unwrap_or_else(|err| {
                panic!("{case_name}: next metadata command acceptance failed: {err:?}")
            }),
        MetadataCommandAcceptance::Apply,
        "{case_name}: recovered PG should accept the next command without retry or drain"
    );
    store
        .record_metadata_command_applied(0, &next_command)
        .unwrap_or_else(|err| panic!("{case_name}: next metadata command failed: {err:?}"));
    assert_cached_metadata_digest_matches_materialized(store);
}

fn assert_commit_failure_recovers_for_metadata_mutator(
    case_name: &str,
    setup: impl FnOnce(&PgStore),
    mutate: impl FnOnce(&PgStore) -> Result<(), MetadataError>,
) {
    let tmp = test_util::tempdir();
    {
        let store = PgStore::open(tmp.path(), 1).unwrap();
        setup(&store);
        store.refresh_metadata_command_state_digest().unwrap();
        assert_pg_recovery_invariants_after_commit_failure(&store, case_name);

        store.fail_next_metadata_txn_commit();
        let err = match mutate(&store) {
            Ok(()) => panic!("{case_name}: injected commit failure was not used"),
            Err(err) => err,
        };
        assert!(
            matches!(err, MetadataError::Db { .. }),
            "{case_name}: expected injected commit failure DB error, got {err:?}"
        );
        assert_eq!(
            store.clean_metadata_digest_revision.load(Ordering::Relaxed),
            UNCLEAN_METADATA_DIGEST_REVISION,
            "{case_name}: injected commit failure should invalidate the clean digest marker"
        );
    }

    let recovered = PgStore::open(tmp.path(), 1).unwrap();
    recovered
        .recover(super::super::PgStoreRecoveryContext::for_node(NodeId::new(
            0,
        )))
        .unwrap_or_else(|err| panic!("{case_name}: recovery failed: {err:?}"));
    let state = assert_pg_recovery_invariants_after_commit_failure(&recovered, case_name);
    assert_pg_accepts_next_command_after_recovery(&recovered, case_name, &state);
}

fn test_live_object(bucket: BucketName, key: ObjectKey, generation_id: u64) -> PutLiveObjectReq {
    PutLiveObjectReq {
        bucket,
        key,
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::new(generation_id).unwrap(),
        size: 64,
        etag: ObjectEtag::single_part(generation_id),
        ec: EcShape { k: 2, m: 1 },
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    }
}

fn test_object_segment(
    bucket: BucketName,
    key: ObjectKey,
    version_id: VersionId,
    segment_index: u32,
) -> ObjectSegmentRecord {
    ObjectSegmentRecord {
        bucket,
        key,
        version_id,
        segment_index,
        size: 64,
        segment_crc64: 0x1234 + u64::from(segment_index),
        segment_okh: [segment_index as u8; 16],
        segment_vid: GenerationId::new(u64::from(segment_index) + 1).unwrap(),
        data_pg_id: segment_index + 1,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 2,
        ec_m: 1,
    }
}

fn test_multipart_part(upload_id: UploadId, part_number: u32) -> MultipartPartRecord {
    MultipartPartRecord {
        upload_id,
        part_number,
        generation: 0,
        size: 64,
        payload_crc64: 0x5678 + u64::from(part_number),
        etag: format!("part-{part_number}").into_bytes(),
        etag_kind: EtagKind::Crc64,
        part_okh: [part_number as u8; 16],
        part_vid: GenerationId::new(u64::from(part_number) + 10).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 2,
        ec_m: 1,
        last_modified: 42 + u64::from(part_number),
        checksum: None,
    }
}

fn test_object_part(
    bucket: BucketName,
    key: ObjectKey,
    version_id: VersionId,
    part_number: u32,
) -> ObjectPartRecord {
    ObjectPartRecord {
        bucket,
        key,
        version_id,
        part_number,
        size: 64,
        payload_crc64: 0xabcd + u64::from(part_number),
        etag: format!("object-part-{part_number}").into_bytes(),
        etag_kind: EtagKind::Crc64,
        part_okh: [part_number as u8; 16],
        part_vid: GenerationId::new(u64::from(part_number) + 30).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 2,
        ec_m: 1,
        data_pg_id: part_number,
        checksum: None,
    }
}

fn test_commit_multipart_req(bucket: BucketName, key: ObjectKey) -> CommitMultipartReq {
    CommitMultipartReq {
        bucket,
        key,
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 64,
        etag_crc64: [0x44, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 2, m: 1 },
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    }
}

fn test_multipart_part_segment(
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
    part_number: u32,
    segment_index: u32,
) -> MultipartPartSegmentRecord {
    MultipartPartSegmentRecord {
        bucket,
        key,
        upload_id,
        version_id: PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number,
        segment_index,
        size: 64,
        segment_crc64: 0x9abc + u64::from(segment_index),
        segment_okh: [segment_index as u8; 16],
        segment_vid: GenerationId::new(u64::from(segment_index) + 20).unwrap(),
        data_pg_id: segment_index + 1,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 2,
        ec_m: 1,
    }
}

#[test]
fn metadata_txn_commit_failure_recovers_representative_mutators() {
    assert_commit_failure_recovers_for_metadata_mutator(
        "create-bucket",
        |_| {},
        |store| {
            let bucket = trusted_bucket_name("commit-fail-create-bucket");
            PgMetadataStore::create_bucket(
                store,
                &bucket,
                "owner",
                &CanonicalUserId::from_principal("owner"),
                &AclGrants::default(),
                false,
                false,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-versioning",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-versioning")),
        |store| {
            PgMetadataStore::put_bucket_versioning(
                store,
                &trusted_bucket_name("commit-fail-versioning"),
                BucketVersioningState::Enabled,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-acl",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-acl")),
        |store| {
            PgMetadataStore::put_bucket_acl(
                store,
                &trusted_bucket_name("commit-fail-acl"),
                &AclGrants::default(),
                true,
                false,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "mark-bucket-deleting",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete")),
        |store| store.mark_bucket_deleting(&trusted_bucket_name("commit-fail-delete")),
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "release-metadata-command-bucket-reservation",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-reservation"));
            PgMetadataStore::acquire_durable_bucket_write_reservation(
                store,
                &trusted_bucket_name("commit-fail-reservation"),
                "reservation-id",
                "owner-token",
                ClusterEpoch::INITIAL,
                "test",
                10,
                100,
                None,
            )
            .unwrap();
        },
        |store| {
            let reservation = PgMetadataStore::durable_bucket_write_reservation(
                store,
                &trusted_bucket_name("commit-fail-reservation"),
                "reservation-id",
            )
            .unwrap()
            .expect("test setup should create reservation");
            PgMetadataStore::release_metadata_command_bucket_write_reservation(
                store,
                &reservation.bucket,
                &reservation.reservation_id,
                &reservation.owner_token,
                reservation.cluster_epoch,
                reservation.bucket_execution_generation,
                reservation.bucket_incarnation_generation,
                reservation.lease_deadline,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "clear-expired-durable-bucket-drain",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-clear-drain"));
            PgMetadataStore::begin_durable_bucket_write_drain(
                store,
                &trusted_bucket_name("commit-fail-clear-drain"),
                "drain-id",
                "owner-token",
                ClusterEpoch::INITIAL,
                10,
                Some(20),
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::clear_expired_durable_bucket_write_drain(
                store,
                &trusted_bucket_name("commit-fail-clear-drain"),
                21,
            )
            .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "heartbeat-durable-bucket-drain",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-heartbeat-drain"));
            PgMetadataStore::begin_durable_bucket_write_drain(
                store,
                &trusted_bucket_name("commit-fail-heartbeat-drain"),
                "drain-id",
                "owner-token",
                ClusterEpoch::INITIAL,
                10,
                Some(30),
            )
            .unwrap();
        },
        |store| {
            let drain = PgMetadataStore::durable_bucket_write_drain(
                store,
                &trusted_bucket_name("commit-fail-heartbeat-drain"),
            )
            .unwrap()
            .expect("test setup should create drain");
            PgMetadataStore::heartbeat_durable_bucket_write_drain(
                store,
                &drain.bucket,
                &drain.drain_id,
                &drain.owner_token,
                drain.cluster_epoch,
                drain.bucket_execution_generation,
                40,
                20,
            )
            .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "create-multipart-upload",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-mpu")),
        |store| {
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("commit-fail-mpu-upload"),
                    bucket: trusted_bucket_name("commit-fail-mpu"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "upsert-multipart-part",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-upsert-part"));
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("commit-fail-upsert-part-upload"),
                    bucket: trusted_bucket_name("commit-fail-upsert-part"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            let upload_id = crate::tests::multipart_upload_id("commit-fail-upsert-part-upload");
            PgMetadataStore::upsert_multipart_part(store, &test_multipart_part(upload_id, 1))
                .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-encryption",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-encryption")),
        |store| {
            PgMetadataStore::put_bucket_encryption(
                store,
                &trusted_bucket_name("commit-fail-encryption"),
                BucketEncryptionConfig {
                    default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                    sse_c_blocked: true,
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-object-lock",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-lock"));
            PgMetadataStore::put_bucket_versioning(
                store,
                &trusted_bucket_name("commit-fail-lock"),
                BucketVersioningState::Enabled,
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::put_bucket_object_lock(
                store,
                &trusted_bucket_name("commit-fail-lock"),
                BucketObjectLockConfig {
                    enabled: true,
                    default_retention: Some(ObjectLockDefaultRetention {
                        mode: ObjectLockMode::Governance,
                        period: RetentionPeriod::days(7).unwrap(),
                    }),
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-public-access-block",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-pab")),
        |store| {
            PgMetadataStore::put_bucket_public_access_block(
                store,
                &trusted_bucket_name("commit-fail-pab"),
                PublicAccessBlockConfig {
                    block_public_acls: true,
                    ignore_public_acls: true,
                    block_public_policy: true,
                    restrict_public_buckets: true,
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-bucket-public-access-block",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete-pab"));
            PgMetadataStore::put_bucket_public_access_block(
                store,
                &trusted_bucket_name("commit-fail-delete-pab"),
                PublicAccessBlockConfig {
                    block_public_acls: true,
                    ignore_public_acls: false,
                    block_public_policy: true,
                    restrict_public_buckets: false,
                },
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_bucket_public_access_block(
                store,
                &trusted_bucket_name("commit-fail-delete-pab"),
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-ownership-controls",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-ownership")),
        |store| {
            PgMetadataStore::put_bucket_ownership_controls(
                store,
                &trusted_bucket_name("commit-fail-ownership"),
                BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-bucket-ownership-controls",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete-ownership"));
            PgMetadataStore::put_bucket_ownership_controls(
                store,
                &trusted_bucket_name("commit-fail-delete-ownership"),
                BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
                },
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_bucket_ownership_controls(
                store,
                &trusted_bucket_name("commit-fail-delete-ownership"),
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-abac-enabled",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-abac")),
        |store| {
            PgMetadataStore::put_bucket_abac_enabled(
                store,
                &trusted_bucket_name("commit-fail-abac"),
                true,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-bucket-subresource",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-subresource")),
        |store| {
            PgMetadataStore::put_bucket_subresource(
                store,
                &trusted_bucket_name("commit-fail-subresource"),
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Policy,
                    body: r#"{"Statement":[]}"#,
                    aux: BucketSubresourceAux::policy(true),
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-bucket-subresource",
        |store| {
            create_probe_bucket_direct(
                store,
                &trusted_bucket_name("commit-fail-delete-subresource"),
            );
            PgMetadataStore::put_bucket_subresource(
                store,
                &trusted_bucket_name("commit-fail-delete-subresource"),
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Policy,
                    body: r#"{"Statement":[]}"#,
                    aux: BucketSubresourceAux::policy(false),
                },
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_bucket_subresource(
                store,
                &trusted_bucket_name("commit-fail-delete-subresource"),
                BucketSubresourceKind::Policy,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-object-meta",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-object-meta")),
        |store| {
            let bucket = trusted_bucket_name("commit-fail-object-meta");
            let key = trusted_object_key("object");
            PgMetadataStore::put_object_meta(
                store,
                &PutObjectReq::Live(test_live_object(bucket, key, 1)),
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-object-version",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete-version"));
            let bucket = trusted_bucket_name("commit-fail-delete-version");
            let key = trusted_object_key("object");
            PgMetadataStore::put_object_meta(
                store,
                &PutObjectReq::Live(test_live_object(bucket, key, 1)),
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_object_version(
                store,
                &trusted_bucket_name("commit-fail-delete-version"),
                &trusted_object_key("object"),
                VersionId::Null,
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "reserve-object-generation",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-reserve")),
        |store| {
            PgMetadataStore::reserve_object_generation(
                store,
                &trusted_bucket_name("commit-fail-reserve"),
                &trusted_object_key("object"),
                &SessionId::try_from("a1".repeat(16)).unwrap(),
            )
            .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-object-with-segments",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-segment-object"));
        },
        |store| {
            let bucket = trusted_bucket_name("commit-fail-segment-object");
            let key = trusted_object_key("object");
            let object = test_live_object(bucket.clone(), key.clone(), 2);
            let segments = vec![test_object_segment(bucket, key, VersionId::Null, 0)];
            PgMetadataStore::put_object_with_segments(store, &object, &segments)
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-object-segments-reclaim",
        |store| create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-reclaim")),
        |store| {
            let bucket = trusted_bucket_name("commit-fail-reclaim");
            let key = trusted_object_key("object");
            PgMetadataStore::put_object_segments_reclaim(
                store,
                &ObjectSegmentsReclaimRecord {
                    bucket,
                    key,
                    generation_id: GenerationId::new(3).unwrap(),
                    created_at: 50,
                    segments: vec![ObjectSegmentsReclaimSegmentRecord {
                        segment_index: 0,
                        segment_okh: [3; 16],
                        segment_vid: GenerationId::new(4).unwrap(),
                        data_pg_id: 1,
                        ec: EcShape { k: 2, m: 1 },
                    }],
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "put-multipart-reclaim",
        |store| {
            create_probe_bucket_direct(
                store,
                &trusted_bucket_name("commit-fail-multipart-reclaim"),
            );
        },
        |store| {
            PgMetadataStore::put_multipart_reclaim(
                store,
                &MultipartReclaimRecord {
                    bucket: trusted_bucket_name("commit-fail-multipart-reclaim"),
                    key: trusted_object_key("object"),
                    generation_id: GenerationId::new(5).unwrap(),
                    created_at: 51,
                    parts: vec![MultipartReclaimPartRecord::ShardSet {
                        part_number: 1,
                        part_okh: [4; 16],
                        part_vid: GenerationId::new(6).unwrap(),
                        data_pg_id: 1,
                        ec: EcShape { k: 2, m: 1 },
                    }],
                },
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-multipart-upload",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete-mpu"));
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("commit-fail-delete-mpu-upload"),
                    bucket: trusted_bucket_name("commit-fail-delete-mpu"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_multipart_upload(
                store,
                &crate::tests::multipart_upload_id("commit-fail-delete-mpu-upload"),
            )
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "commit-object-parts",
        |_| {},
        |store| {
            let bucket = trusted_bucket_name("commit-fail-object-parts");
            let key = trusted_object_key("object");
            let part = test_object_part(bucket, key, VersionId::from_u64(1), 1);
            PgMetadataStore::commit_object_parts(store, std::slice::from_ref(&part))
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "complete-multipart-commit",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-complete-mpu"));
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("commit-fail-complete-mpu-upload"),
                    bucket: trusted_bucket_name("commit-fail-complete-mpu"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("commit-fail-complete-mpu");
            let key = trusted_object_key("object");
            let upload_id = crate::tests::multipart_upload_id("commit-fail-complete-mpu-upload");
            let obj = test_commit_multipart_req(bucket.clone(), key.clone());
            let parts = vec![test_object_part(bucket, key, VersionId::Null, 1)];
            PgMetadataStore::complete_multipart_commit(store, &upload_id, 1, &obj, &parts)
                .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "upsert-multipart-part-segments",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-part-segments"));
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id(
                        "commit-fail-part-segments-upload",
                    ),
                    bucket: trusted_bucket_name("commit-fail-part-segments"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            let upload_id = crate::tests::multipart_upload_id("commit-fail-part-segments-upload");
            let bucket = trusted_bucket_name("commit-fail-part-segments");
            let key = trusted_object_key("object");
            let part = MultipartPartRecord {
                part_okh: [0; 16],
                ..test_multipart_part(upload_id.clone(), 1)
            };
            let segments = vec![test_multipart_part_segment(bucket, key, upload_id, 1, 0)];
            PgMetadataStore::upsert_multipart_part_segments(store, &part, &segments).map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "commit-stream-part",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-stream-part"));
            PgMetadataStore::create_multipart_upload(
                store,
                &CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("commit-fail-stream-part-upload"),
                    bucket: trusted_bucket_name("commit-fail-stream-part"),
                    key: trusted_object_key("object"),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: test_owner(),
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
            PgMetadataStore::create_stream_upload(
                store,
                &CreateStreamUploadReq {
                    session_id: SessionId::try_from("c1".repeat(16)).unwrap(),
                    bucket: trusted_bucket_name("commit-fail-stream-part"),
                    key: trusted_object_key("object"),
                    target: StreamUploadTarget::UploadPart {
                        upload_id: crate::tests::multipart_upload_id(
                            "commit-fail-stream-part-upload",
                        ),
                        part_number: 1,
                    },
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            let upload_id = crate::tests::multipart_upload_id("commit-fail-stream-part-upload");
            let bucket = trusted_bucket_name("commit-fail-stream-part");
            let key = trusted_object_key("object");
            let part = test_multipart_part(upload_id.clone(), 1);
            let segments = vec![test_multipart_part_segment(bucket, key, upload_id, 1, 0)];
            PgMetadataStore::commit_stream_part(
                store,
                &SessionId::try_from("c1".repeat(16)).unwrap(),
                &part,
                &segments,
            )
            .map(|_| ())
        },
    );

    assert_commit_failure_recovers_for_metadata_mutator(
        "delete-stream-upload",
        |store| {
            create_probe_bucket_direct(store, &trusted_bucket_name("commit-fail-delete-stream"));
            PgMetadataStore::create_stream_upload(
                store,
                &CreateStreamUploadReq {
                    session_id: SessionId::try_from("d1".repeat(16)).unwrap(),
                    bucket: trusted_bucket_name("commit-fail-delete-stream"),
                    key: trusted_object_key("object"),
                    target: StreamUploadTarget::PutObject,
                    encryption: ObjectEncryption::None,
                },
            )
            .unwrap();
        },
        |store| {
            PgMetadataStore::delete_stream_upload(
                store,
                &SessionId::try_from("d1".repeat(16)).unwrap(),
            )
        },
    );
}

#[test]
fn metadata_digest_cache_tracks_row_changes_incrementally() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");

    assert_cached_metadata_digest_matches_materialized(&store);
    store
        .conn
        .execute(
            "INSERT INTO object_version_counters \
             (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 2_i64],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "UPDATE object_version_counters SET next_version_id = ?1 \
             WHERE bucket = ?2 AND key = ?3",
            params![3_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "INSERT OR REPLACE INTO object_version_counters \
             (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 4_i64],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
            params![bucket.as_str(), key.as_str()],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "INSERT INTO object_write_counters \
             (bucket, key, next_write_sequence, max_committed_generation) \
             VALUES (?1, ?2, ?3, ?4)",
            params![bucket.as_str(), key.as_str(), 2_i64, 1_i64],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "UPDATE object_write_counters \
             SET next_write_sequence = ?1, max_committed_generation = ?2 \
             WHERE bucket = ?3 AND key = ?4",
            params![3_i64, 2_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    store
        .conn
        .execute(
            "DELETE FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
            params![bucket.as_str(), key.as_str()],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);
}

#[test]
fn metadata_digest_cache_tracks_multipart_upload_create() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-mpu-bucket");
    let key = trusted_object_key("object");
    PgMetadataStore::create_bucket(
        &store,
        &bucket,
        "owner",
        &CanonicalUserId::from_principal("owner"),
        &AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);

    PgMetadataStore::create_multipart_upload(
        &store,
        &CreateMultipartUploadReq {
            upload_id: crate::tests::multipart_upload_id("digest-mpu-upload"),
            bucket: bucket.clone(),
            key,
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: test_owner(),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        },
    )
    .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);
}

#[test]
fn metadata_command_apply_tracks_multipart_upload_create_digest() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-mpu-command-bucket");
    let key = trusted_object_key("object");
    PgMetadataStore::create_bucket(
        &store,
        &bucket,
        "owner",
        &CanonicalUserId::from_principal("owner"),
        &AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();

    let create = CreateMultipartUploadReq {
        upload_id: crate::tests::multipart_upload_id("digest-mpu-command-upload"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: test_owner(),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CreateMultipartUpload(Box::new(
            CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                create,
                GenerationId::new(1).unwrap(),
                123,
                test_bucket_write_reservation_proof(&bucket, &key, "create-multipart-upload"),
            ),
        )),
    );
    store
        .apply_metadata_command_and_record(7, &command)
        .unwrap();

    assert_cached_metadata_digest_matches_materialized(&store);
    let before_reopen: Vec<(&'static str, u64, u64)> =
        crate::pg_store::command_log::METADATA_DIGEST_TABLES
            .iter()
            .map(|table| {
                (
                    table.name,
                    store.cached_metadata_table_digest(table).unwrap(),
                    store.metadata_table_digest(table).unwrap(),
                )
            })
            .collect();
    let state_before_reopen = store.metadata_state_digest().unwrap();
    store
        .validate_metadata_command_replay_state(7, ClusterEpoch::INITIAL)
        .unwrap();
    drop(store);

    let reopened = PgStore::open(tmp.path(), 1).unwrap();
    assert_cached_metadata_digest_matches_materialized(&reopened);
    for (table_name, cached_before, materialized_before) in before_reopen {
        let table = crate::pg_store::command_log::METADATA_DIGEST_TABLES
            .iter()
            .find(|table| table.name == table_name)
            .unwrap();
        assert_eq!(
            cached_before,
            reopened.cached_metadata_table_digest(table).unwrap(),
            "cached digest for {table_name} changed across reopen"
        );
        assert_eq!(
            materialized_before,
            reopened.metadata_table_digest(table).unwrap(),
            "materialized digest for {table_name} changed across reopen"
        );
    }
    assert_eq!(
        state_before_reopen,
        reopened.metadata_state_digest().unwrap(),
        "materialized state digest changed across reopen"
    );
    reopened
        .validate_metadata_command_replay_state(7, ClusterEpoch::INITIAL)
        .unwrap();
}

#[test]
fn metadata_command_acceptance_detects_dirty_cached_digest() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    store
        .conn
        .execute(
            "INSERT INTO object_version_counters \
				 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 2_i64],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();

    store
        .conn
        .execute(
            "UPDATE object_version_counters SET next_version_id = ?1 \
				 WHERE bucket = ?2 AND key = ?3",
            params![3_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();

    let command = create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
    let err = store.metadata_command_acceptance(0, &command).unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn metadata_command_acceptance_detects_cross_connection_dirty_cached_digest() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    store
        .conn
        .execute(
            "INSERT INTO object_version_counters \
				 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 2_i64],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();

    let other_store = PgStore::open(tmp.path(), 1).unwrap();
    other_store
        .conn
        .execute(
            "UPDATE object_version_counters SET next_version_id = ?1 \
				 WHERE bucket = ?2 AND key = ?3",
            params![3_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();

    let command = create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
    let err = store.metadata_command_acceptance(0, &command).unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn rolled_back_command_record_does_not_mark_digest_revision_clean() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    store
        .conn
        .execute(
            "INSERT INTO object_version_counters \
             (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 2_i64],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    let clean_revision = store.clean_metadata_digest_revision.load(Ordering::Relaxed);

    let command = create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    store.apply_metadata_command(&command).unwrap();
    store.record_metadata_command_applied(0, &command).unwrap();
    let transaction_revision = store.metadata_digest_revision().unwrap();
    assert_ne!(clean_revision, transaction_revision);
    store.conn.execute_batch("ROLLBACK").unwrap();

    assert_eq!(
        clean_revision,
        store.clean_metadata_digest_revision.load(Ordering::Relaxed),
        "recording inside an uncommitted transaction must not advance the clean revision"
    );

    let other_store = PgStore::open(tmp.path(), 1).unwrap();
    other_store
        .conn
        .execute(
            "UPDATE object_version_counters SET next_version_id = ?1 \
             WHERE bucket = ?2 AND key = ?3",
            params![3_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();

    let err = store.metadata_command_acceptance(0, &command).unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn metadata_digest_trigger_bootstrap_repairs_partial_install() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    {
        let store = PgStore::open(tmp.path(), 1).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);

        store
            .conn
            .execute_batch(
                "DROP TRIGGER metadata_digest_object_version_counters_ad; \
                 DROP TRIGGER metadata_digest_object_version_counters_au;",
            )
            .unwrap();
        store
            .conn
            .execute(
                "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                params![bucket.as_str(), key.as_str()],
            )
            .unwrap();
        assert_ne!(
            store.cached_metadata_state_digest().unwrap(),
            store.metadata_state_digest().unwrap(),
            "simulated partial trigger install should leave stale cached digest before reopen",
        );
    }

    let store = PgStore::open(tmp.path(), 1).unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);
    store
        .conn
        .execute(
            "INSERT INTO object_version_counters \
             (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
            params![bucket.as_str(), key.as_str(), 2_i64],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE object_version_counters SET next_version_id = ?1 \
             WHERE bucket = ?2 AND key = ?3",
            params![3_i64, bucket.as_str(), key.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
            params![bucket.as_str(), key.as_str()],
        )
        .unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);
}

#[test]
fn metadata_digest_bootstrap_marker_repairs_stale_cache_with_complete_triggers() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    {
        let store = PgStore::open(tmp.path(), 1).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);

        store
            .conn
            .execute(
                "UPDATE metadata_table_digests \
                 SET table_digest = 0, row_count = 0, row_hash_xor = 0, row_hash_sum = 0 \
                 WHERE table_name = ?1",
                params!["object_version_counters"],
            )
            .unwrap();
        store
            .conn
            .execute("DELETE FROM metadata_digest_bootstrap_state", [])
            .unwrap();
        assert_ne!(
            store.cached_metadata_state_digest().unwrap(),
            store.metadata_state_digest().unwrap(),
            "simulated interrupted bootstrap should leave stale cache with all triggers present",
        );
    }

    let store = PgStore::open(tmp.path(), 1).unwrap();
    assert_cached_metadata_digest_matches_materialized(&store);
    assert!(store.metadata_digest_bootstrap_complete().unwrap());
}

#[test]
fn pg_store_recovery_repairs_cache_only_table_digest_drift() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("cache-drift-first");
    let first_command = create_bucket_probe_command(store.pg_id, 1, first_bucket.clone(), 1);
    store
        .apply_metadata_command_and_record(0, &first_command)
        .unwrap();

    let state_before_drift = store.metadata_command_replica_state().unwrap();
    let materialized_before_drift = store.metadata_state_digest().unwrap();
    assert_eq!(state_before_drift.state_digest, materialized_before_drift);
    assert!(
        store
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty(),
        "test setup should start with cached table digests matching materialized rows"
    );

    store
        .conn
        .execute(
            "UPDATE metadata_table_digests \
             SET table_digest = table_digest + 1, row_hash_sum = row_hash_sum + 1 \
             WHERE table_name = ?1",
            params!["buckets"],
        )
        .unwrap();
    assert_eq!(
        store.metadata_command_replica_state().unwrap().state_digest,
        materialized_before_drift,
        "cache-only drift must not change the durable replica-state digest"
    );
    assert_eq!(
        store.metadata_state_digest().unwrap(),
        materialized_before_drift,
        "cache-only drift must not change materialized rows"
    );
    assert!(
        !store
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty(),
        "test setup must create cached-vs-materialized table digest drift"
    );

    drop(store);
    let recovered = PgStore::open(tmp.path(), 1).unwrap();
    recovered
        .recover(super::super::PgStoreRecoveryContext::for_node(
            placement::NodeId::new(0),
        ))
        .unwrap();
    assert!(
        recovered
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty(),
        "recovery should refresh cache-only per-table digest drift"
    );
    assert_eq!(
        recovered.cached_metadata_state_digest().unwrap(),
        recovered.metadata_state_digest().unwrap(),
        "cached state digest should match materialized rows after recovery"
    );
    recovered
        .metadata_command_replica_state_for_heartbeat(0, ClusterEpoch::INITIAL)
        .unwrap();

    let second_bucket = trusted_bucket_name("cache-drift-second");
    let second_command = create_bucket_probe_command(recovered.pg_id, 2, second_bucket.clone(), 2);
    recovered
        .metadata_command_acceptance(0, &second_command)
        .expect("recovery should leave the cached digest clean for the next command");
    recovered
        .apply_metadata_command_and_record(0, &second_command)
        .unwrap();
    let final_state = recovered.metadata_command_replica_state().unwrap();
    assert_eq!(
        final_state.state_digest,
        recovered.metadata_state_digest().unwrap()
    );
    assert!(
        recovered
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty(),
        "later metadata command should not recreate cached table digest drift"
    );
}

fn insert_digest_multipart_upload(store: &PgStore, upload_id: &UploadId) {
    let owner = test_owner();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    store
        .conn
        .execute(
            "INSERT INTO multipart_uploads \
             (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
              system_metadata_blob, owner_principal, owner_canonical_id, \
              initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, \
              encryption_type, encryption_state, acl_grants, public_read, object_generation_id, \
              object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                upload_id.as_str(),
                bucket.as_str(),
                key.as_str(),
                101_i64,
                UploadState::InProgress as u8,
                Option::<&str>::None,
                b"meta".as_slice(),
                b"system".as_slice(),
                owner.principal.as_str(),
                owner.canonical_id.as_str(),
                owner.principal.as_str(),
                owner.canonical_id.as_str(),
                Option::<u8>::None,
                Option::<u8>::None,
                ObjectEncryption::None.encryption_type() as u8,
                Option::<&[u8]>::None,
                "",
                0_i64,
                1_i64,
                Option::<i64>::None,
                Option::<i64>::None,
                StoredLegalHoldStatus::NotSet as u8,
            ],
        )
        .unwrap();
}

#[test]
fn metadata_state_digest_table_inventory_is_explicit() {
    let tables: Vec<_> = METADATA_DIGEST_TABLES
        .iter()
        .map(|table| (table.name, table.filter))
        .collect();
    assert_eq!(
        tables,
        vec![
            ("bucket_subresources", MetadataDigestFilter::AllRows),
            ("buckets", MetadataDigestFilter::AllRows),
            ("completed_multipart_uploads", MetadataDigestFilter::AllRows),
            ("multipart_part_segments", MetadataDigestFilter::AllRows),
            ("multipart_parts", MetadataDigestFilter::AllRows),
            (
                "multipart_reclaim_part_segments",
                MetadataDigestFilter::AllRows
            ),
            ("multipart_reclaim_parts", MetadataDigestFilter::AllRows),
            ("multipart_reclaims", MetadataDigestFilter::AllRows),
            ("multipart_uploads", MetadataDigestFilter::AllRows),
            (
                "object_generation_reservations",
                MetadataDigestFilter::AllRows
            ),
            ("object_version_counters", MetadataDigestFilter::AllRows),
            ("object_write_counters", MetadataDigestFilter::AllRows),
            ("object_parts", MetadataDigestFilter::AllRows),
            (
                "object_segment_reclaim_segments",
                MetadataDigestFilter::AllRows
            ),
            ("object_segments", MetadataDigestFilter::AllRows),
            ("object_segments_reclaims", MetadataDigestFilter::AllRows),
            ("objects", MetadataDigestFilter::AllRows),
            ("pg_counters", MetadataDigestFilter::AllRows),
            ("stream_upload_segments", MetadataDigestFilter::AllRows),
            ("stream_uploads", MetadataDigestFilter::AllRows),
        ]
    );

    for table in METADATA_DIGEST_TABLES {
        assert!(
            !table.columns.is_empty(),
            "{} must have explicit digest columns",
            table.name
        );
    }
}

#[test]
fn canonical_metadata_value_encoding_is_typed() {
    fn digest_for(value: ValueRef<'_>) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        PgStore::digest_canonical_sql_value(&mut hasher, value);
        hasher.finalize()
    }

    assert_ne!(digest_for(ValueRef::Null), digest_for(ValueRef::Text(b"")));
    assert_ne!(
        digest_for(ValueRef::Integer(12)),
        digest_for(ValueRef::Text(b"12"))
    );
    assert_ne!(
        digest_for(ValueRef::Text(b"bytes")),
        digest_for(ValueRef::Blob(b"bytes"))
    );
    assert_ne!(
        digest_for(ValueRef::Integer(-1)),
        digest_for(ValueRef::Integer(1))
    );
}

#[test]
fn canonical_metadata_variable_length_encoding_is_prefix_free() {
    fn digest_values(values: &[ValueRef<'_>]) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        for value in values {
            PgStore::digest_canonical_sql_value(&mut hasher, *value);
        }
        hasher.finalize()
    }

    fn digest_names(names: &[&[u8]]) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        for name in names {
            digest_len_prefixed_bytes(&mut hasher, name);
        }
        hasher.finalize()
    }

    assert_ne!(
        digest_values(&[ValueRef::Blob(b"\x04"), ValueRef::Blob(b"")]),
        digest_values(&[ValueRef::Blob(b""), ValueRef::Blob(b"\x04")])
    );
    assert_ne!(
        digest_values(&[ValueRef::Text(b"a"), ValueRef::Text(b"bc")]),
        digest_values(&[ValueRef::Text(b"ab"), ValueRef::Text(b"c")])
    );
    assert_ne!(
        digest_names(&[b"table", b"_range"]),
        digest_names(&[b"table_", b"range"])
    );
}

// ── prefix_end ────────────────────────────────────────────────────

#[test]
fn key_prefix_upper_bound_basic() {
    assert_eq!(key_prefix_upper_bound("foo"), Some("fop".to_string()));
}

#[test]
fn key_prefix_upper_bound_empty() {
    assert_eq!(key_prefix_upper_bound(""), None);
}

#[test]
fn key_prefix_upper_bound_del_char() {
    assert_eq!(key_prefix_upper_bound("\x7f"), Some("\u{80}".to_string()));
}

#[test]
fn key_prefix_upper_bound_trailing_del() {
    assert_eq!(
        key_prefix_upper_bound("abc\x7f"),
        Some("abc\u{80}".to_string())
    );
}

#[test]
fn key_prefix_upper_bound_tilde() {
    assert_eq!(key_prefix_upper_bound("~"), Some("\x7f".to_string()));
}

// ── pg_id accessor ────────────────────────────────────────────────

#[test]
fn pg_id_accessor() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 42).unwrap();
    assert_eq!(store.pg_id(), 42);
}

#[test]
fn metadata_command_apply_and_record_rolls_back_metadata_on_record_conflict() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("rollback-bucket");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    let conflicting_command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("conflict-bucket"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                conflicting_command.checksum_crc64() as i64,
                conflicting_command.command_bytes(),
            ],
        )
        .unwrap();

    let err = store
        .apply_metadata_command_and_record(0, &command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "expected log conflict, got {err:?}"
    );
    assert!(
        matches!(
            store.head_bucket_raw(&bucket).unwrap_err(),
            MetadataError::BucketNotFound { .. }
        ),
        "bucket creation must roll back when log recording fails"
    );
}

#[test]
fn metadata_command_apply_and_record_rolls_back_log_on_metadata_failure() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("rollback-log-bucket");
    let first = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    let conflicting_create = create_bucket_probe_command(1, 2, bucket, 2);

    store.apply_metadata_command_and_record(0, &first).unwrap();
    let err = store
        .apply_metadata_command_and_record(0, &conflicting_create)
        .unwrap_err();
    assert!(
        matches!(
            err,
            BucketSnapshotLoadError::Metadata(MetadataError::BucketAlreadyExists)
        ),
        "expected metadata conflict, got {err:?}"
    );

    let log_entry_count: u64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM metadata_command_log WHERE log_index = 2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        log_entry_count, 0,
        "failed metadata mutation must not leave a command-log record"
    );
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 1);
}

#[test]
fn metadata_command_acceptance_rejects_non_contiguous_log_index_before_mutation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("contiguous-first");
    let gap_bucket = trusted_bucket_name("contiguous-gap");
    let first = create_bucket_probe_command(1, 1, first_bucket, 1);
    let gap = create_bucket_probe_command(1, 3, gap_bucket.clone(), 2);

    store.apply_metadata_command_and_record(0, &first).unwrap();

    let acceptance_error = store.metadata_command_acceptance(0, &gap).unwrap_err();
    assert!(
        matches!(
            acceptance_error,
            StoreError::MetadataCommandLogGap {
                node_id: 0,
                pg_id: 1,
                cluster_epoch,
                log_index: 3,
                expected_log_index: 2,
            } if cluster_epoch == ClusterEpoch::INITIAL
        ),
        "expected non-contiguous command rejection, got {acceptance_error:?}"
    );

    let apply_error = store
        .apply_metadata_command_and_record(0, &gap)
        .unwrap_err();
    assert!(
        matches!(
            apply_error,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogGap {
                node_id: 0,
                pg_id: 1,
                cluster_epoch,
                log_index: 3,
                expected_log_index: 2,
            }) if cluster_epoch == ClusterEpoch::INITIAL
        ),
        "expected apply to fail before mutation, got {apply_error:?}"
    );
    assert!(
        matches!(
            store.head_bucket_raw(&gap_bucket).unwrap_err(),
            MetadataError::BucketNotFound { .. }
        ),
        "gap command must not create materialized bucket rows"
    );
    let gap_log_count: u64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM metadata_command_log WHERE log_index = 3",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(gap_log_count, 0, "gap command must not insert a log row");
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(state.applied_log_index, 1);
}

#[test]
fn metadata_command_apply_rejects_stale_epoch_without_rewinding_replica_state() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("epoch-first"), 1);
    store.apply_metadata_command_and_record(0, &first).unwrap();

    let epoch_two = ClusterEpoch::new(2).unwrap();
    let epoch_two_bucket = trusted_bucket_name("epoch-two");
    let epoch_two_command = rebase_probe_commands(
        epoch_two,
        PgId::new(1),
        &[create_bucket_probe_command(1, 1, epoch_two_bucket, 2)],
    )
    .pop()
    .unwrap();
    store
        .apply_metadata_command_and_record(0, &epoch_two_command)
        .unwrap();
    let epoch_two_state = store.metadata_command_replica_state().unwrap();
    assert_eq!(epoch_two_state.cluster_epoch, epoch_two);
    assert_eq!(epoch_two_state.applied_log_index, 1);

    let stale_bucket = trusted_bucket_name("epoch-stale");
    let stale = create_bucket_probe_command(1, 2, stale_bucket.clone(), 3);
    let err = store
        .apply_metadata_command_and_record(0, &stale)
        .unwrap_err();
    assert!(
        matches!(
            err,
            BucketSnapshotLoadError::Store(StoreError::StaleMetadataCommand {
                node_id: 0,
                pg_id: 1,
                command_epoch,
                current_epoch,
            }) if command_epoch == ClusterEpoch::INITIAL && current_epoch == epoch_two
        ),
        "expected stale epoch command rejection, got {err:?}"
    );
    assert!(
        matches!(
            store.head_bucket_raw(&stale_bucket).unwrap_err(),
            MetadataError::BucketNotFound { .. }
        ),
        "stale epoch command must not mutate materialized rows"
    );
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        epoch_two_state
    );
}

#[test]
fn recovery_cleans_only_older_epoch_pending_slot() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("old-slot-first"), 1);
    store.apply_metadata_command_and_record(0, &first).unwrap();

    let epoch_two = ClusterEpoch::new(2).unwrap();
    let epoch_two_command = rebase_probe_commands(
        epoch_two,
        PgId::new(1),
        &[create_bucket_probe_command(
            1,
            1,
            trusted_bucket_name("old-slot-epoch-two"),
            2,
        )],
    )
    .pop()
    .unwrap();
    store
        .apply_metadata_command_and_record(0, &epoch_two_command)
        .unwrap();
    let epoch_two_state = store.metadata_command_replica_state().unwrap();
    assert_eq!(epoch_two_state.cluster_epoch, epoch_two);

    let old_bucket = trusted_bucket_name("old-slot-pending");
    let old_slot = create_bucket_probe_command(1, 2, old_bucket.clone(), 3);
    store
        .try_insert_pending_metadata_command_slot(0, &old_slot, Some(&old_bucket))
        .unwrap();
    assert!(store
        .pending_metadata_command_slot_any_epoch(0)
        .unwrap()
        .is_some());

    store
        .recover_clean_orphan_pending_command_slots(super::super::PgStoreRecoveryContext::for_node(
            NodeId::new(0),
        ))
        .unwrap();
    assert!(store
        .pending_metadata_command_slot_any_epoch(0)
        .unwrap()
        .is_none());
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        epoch_two_state
    );
}

#[test]
fn recovery_rejects_future_epoch_pending_slot_without_cleanup() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let future_bucket = trusted_bucket_name("future-slot-pending");
    let future_slot = rebase_probe_commands(
        future_epoch,
        PgId::new(1),
        &[create_bucket_probe_command(1, 1, future_bucket.clone(), 1)],
    )
    .pop()
    .unwrap();
    store
        .try_insert_pending_metadata_command_slot(0, &future_slot, Some(&future_bucket))
        .unwrap();

    let err = store
        .recover_clean_orphan_pending_command_slots(super::super::PgStoreRecoveryContext::for_node(
            NodeId::new(0),
        ))
        .unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: 1,
                cluster_epoch,
                log_index: 1,
            } if cluster_epoch == future_epoch
        ),
        "future-epoch pending slot must fail closed, got {err:?}"
    );
    let stored = store
        .pending_metadata_command_slot_any_epoch(0)
        .unwrap()
        .expect("future-epoch pending slot must remain durable");
    assert_eq!(stored.id, future_slot.id());
    assert_eq!(stored.command_checksum, future_slot.checksum_crc64());
    assert_eq!(stored.command_bytes, future_slot.command_bytes());
    assert_eq!(stored.scope_bucket, Some(future_bucket));
}

#[test]
fn metadata_command_acceptance_rejects_missing_already_applied_log_entry() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("missing-old-first");
    let second_bucket = trusted_bucket_name("missing-old-second");
    let first = create_bucket_probe_command(1, 1, first_bucket.clone(), 1);
    let second = create_bucket_probe_command(1, 2, second_bucket, 2);

    store.apply_metadata_command_and_record(0, &first).unwrap();
    store.apply_metadata_command_and_record(0, &second).unwrap();
    store
        .conn
        .execute(
            "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![ClusterEpoch::INITIAL.get() as i64, 1_i64, 1_i64],
        )
        .unwrap();

    let acceptance_error = store.metadata_command_acceptance(0, &first).unwrap_err();
    assert!(
        matches!(
            acceptance_error,
            StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
            }
        ),
        "missing old log row must fail closed, got {acceptance_error:?}"
    );
    let apply_error = store
        .apply_metadata_command_and_record(0, &first)
        .unwrap_err();
    assert!(
        matches!(
            apply_error,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
            })
        ),
        "missing old log row must fail before reapplying, got {apply_error:?}"
    );
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 2);
    let info = store.head_bucket_raw(&first_bucket).unwrap();
    assert_eq!(info.name, first_bucket);
}

#[test]
fn retained_metadata_command_log_entries_returns_applied_payloads() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("retained-entry-applied");
    let command = create_bucket_probe_command(1, 1, bucket, 1);

    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    let entries = store
        .retained_metadata_command_log_entries(
            0,
            ClusterEpoch::INITIAL,
            MetadataCommandLogIndex::new(1).unwrap(),
            MetadataCommandLogIndex::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_index, 1);
    assert_eq!(entries[0].previous_log_hash, 0);
    match &entries[0].kind {
        MetadataCommandLogRangeEntryKind::Applied(decoded) => {
            assert_eq!(decoded.command_bytes(), command.command_bytes());
        }
        other => panic!("expected applied command entry, got {other:?}"),
    }
}

#[test]
fn adopt_metadata_transfer_state_installs_rebased_log_over_matching_materialized_state() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("adopt-transfer-first");
    let second_bucket = trusted_bucket_name("adopt-transfer-second");
    let commands = vec![
        create_bucket_probe_command(1, 1, first_bucket.clone(), 1),
        create_bucket_probe_command(1, 2, second_bucket.clone(), 2),
    ];
    let mut post_state_digests = Vec::new();
    for command in &commands {
        let state = store.apply_metadata_command_and_record(0, command).unwrap();
        post_state_digests.push(state.state_digest);
    }
    let source_state = store.metadata_command_replica_state().unwrap();
    assert_eq!(source_state.cluster_epoch, ClusterEpoch::INITIAL);

    let destination_epoch = ClusterEpoch::new(7).unwrap();
    let rebased = metadata_transfer_commands(
        rebase_probe_commands(destination_epoch, PgId::new(1), &commands),
        &post_state_digests,
    );
    let mut expected_log_hash = 0;
    for transfer_command in &rebased {
        let command = &transfer_command.command;
        expected_log_hash = metadata_command_log_hash(
            destination_epoch,
            PgId::new(1),
            command.id().log_index(),
            expected_log_hash,
            command.checksum_crc64(),
        );
    }

    let adopted = store
        .adopt_metadata_transfer_state_from_rebased_commands(
            0,
            destination_epoch,
            &rebased,
            source_state.state_digest,
        )
        .unwrap();
    assert_eq!(adopted.cluster_epoch, destination_epoch);
    assert_eq!(adopted.applied_log_index, 2);
    assert_eq!(adopted.applied_log_hash, expected_log_hash);
    assert_eq!(adopted.state_digest, source_state.state_digest);
    assert!(store.head_bucket_raw(&first_bucket).is_ok());
    assert!(store.head_bucket_raw(&second_bucket).is_ok());

    let stats = store.metadata_command_log_stats(destination_epoch).unwrap();
    assert_eq!(stats.retained_entries, 2);
    assert_eq!(stats.applied_log_index, 2);
    assert_eq!(stats.missing_applied_prefix_entries, 0);
    assert_eq!(stats.pending_tail_entries, 0);

    let retried = store
        .adopt_metadata_transfer_state_from_rebased_commands(
            0,
            destination_epoch,
            &rebased,
            source_state.state_digest,
        )
        .unwrap();
    assert_eq!(retried, adopted);
}

#[test]
fn adopt_metadata_transfer_state_rejects_dirty_materialized_state() {
    let tmp = test_util::tempdir();
    let source = PgStore::open(&tmp.path().join("source"), 1).unwrap();
    let source_bucket = trusted_bucket_name("adopt-transfer-source");
    let source_command = create_bucket_probe_command(1, 1, source_bucket, 1);
    source
        .apply_metadata_command_and_record(0, &source_command)
        .unwrap();
    let source_state = source.metadata_command_replica_state().unwrap();

    let destination = PgStore::open(&tmp.path().join("destination"), 1).unwrap();
    let dirty_bucket = trusted_bucket_name("adopt-transfer-dirty");
    let dirty_command = create_bucket_probe_command(1, 1, dirty_bucket, 1);
    destination
        .apply_metadata_command_and_record(0, &dirty_command)
        .unwrap();
    let rebased = metadata_transfer_commands(
        rebase_probe_commands(
            ClusterEpoch::new(8).unwrap(),
            PgId::new(1),
            std::slice::from_ref(&source_command),
        ),
        &[source_state.state_digest],
    );

    let err = destination
        .adopt_metadata_transfer_state_from_rebased_commands(
            0,
            ClusterEpoch::new(8).unwrap(),
            &rebased,
            source_state.state_digest,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
    ));
    assert_eq!(
        destination
            .metadata_command_replica_state()
            .unwrap()
            .cluster_epoch,
        ClusterEpoch::INITIAL
    );
}

#[test]
fn adopt_metadata_transfer_state_rejects_empty_command_list() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("adopt-transfer-empty");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let source_state = store.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(10).unwrap();

    let err = store
        .adopt_metadata_transfer_state_from_rebased_commands(
            0,
            destination_epoch,
            &[],
            source_state.state_digest,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataTransferEmpty {
            pg_id: 1,
            cluster_epoch,
        } if cluster_epoch == destination_epoch
    ));
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        source_state,
        "empty adoption must not move the replica to a new epoch"
    );
}

#[test]
fn initialize_metadata_transfer_empty_state_moves_canonical_empty_replica() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let source_state = store.metadata_command_replica_state().unwrap();
    assert_eq!(source_state.applied_log_index, 0);
    assert_eq!(source_state.applied_log_hash, 0);

    let destination_epoch = ClusterEpoch::new(11).unwrap();
    let initialized = store
        .initialize_metadata_transfer_empty_state(0, destination_epoch, source_state.state_digest)
        .unwrap();
    assert_eq!(initialized.cluster_epoch, destination_epoch);
    assert_eq!(initialized.applied_log_index, 0);
    assert_eq!(initialized.applied_log_hash, 0);
    assert_eq!(initialized.state_digest, source_state.state_digest);
    assert_eq!(store.metadata_command_replica_state().unwrap(), initialized);
}

#[test]
fn initialize_metadata_transfer_empty_state_rejects_nonempty_destination() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("initialize-empty-transfer-dirty");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let before = store.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(12).unwrap();

    let err = store
        .initialize_metadata_transfer_empty_state(0, destination_epoch, before.state_digest)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandReplicaStateMissing { pg_id: 1 }
    ));
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        before,
        "failed empty-state initialization must not move a non-empty replica"
    );
}

#[test]
fn initialize_metadata_transfer_matching_state_moves_matching_nonempty_replica() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("initialize-matching-transfer");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let source_state = store.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(13).unwrap();

    let initialized = store
        .initialize_metadata_transfer_matching_state(
            0,
            destination_epoch,
            0,
            0,
            source_state.state_digest,
        )
        .unwrap();
    assert_eq!(initialized.cluster_epoch, destination_epoch);
    assert_eq!(initialized.applied_log_index, 0);
    assert_eq!(initialized.applied_log_hash, 0);
    assert_eq!(initialized.state_digest, source_state.state_digest);
    assert_eq!(store.metadata_command_replica_state().unwrap(), initialized);
    assert_eq!(
        store
            .head_bucket_raw(&trusted_bucket_name("initialize-matching-transfer"))
            .unwrap()
            .name,
        trusted_bucket_name("initialize-matching-transfer")
    );
}

#[test]
fn initialize_metadata_transfer_matching_state_rejects_dirty_digest() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("initialize-matching-transfer-dirty");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let before = store.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(14).unwrap();

    let err = store
        .initialize_metadata_transfer_matching_state(
            0,
            destination_epoch,
            0,
            0,
            before.state_digest.wrapping_add(1),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
    ));
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        before,
        "failed matching-state initialization must not move the replica"
    );
}

#[test]
fn initialize_metadata_transfer_matching_state_rejects_unproven_log_tuple() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("initialize-matching-transfer-forged-proof");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let before = store.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(15).unwrap();

    let err = store
        .initialize_metadata_transfer_matching_state(
            0,
            destination_epoch,
            7,
            0x1234,
            before.state_digest,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataTransferUnsupportedProof {
            pg_id: 1,
            applied_log_index: 7,
            applied_log_hash: 0x1234,
            ..
        }
    ));
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        before,
        "failed matching-state initialization must not forge a proof tuple"
    );
}

#[test]
fn metadata_command_checkpoint_exports_checked_table_digest_summary() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("metadata-checkpoint-first");
    let first_command = create_bucket_probe_command(1, 1, first_bucket, 1);
    store
        .apply_metadata_command_and_record(0, &first_command)
        .unwrap();

    let checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(checkpoint.cluster_epoch, state.cluster_epoch);
    assert_eq!(checkpoint.applied_log_index, state.applied_log_index);
    assert_eq!(checkpoint.applied_log_hash, state.applied_log_hash);
    assert_eq!(checkpoint.state_digest, state.state_digest);
    assert_eq!(checkpoint.canonical_state_encoding_version, 1);
    assert_eq!(checkpoint.table_digests.len(), METADATA_DIGEST_TABLES.len());
    assert_eq!(checkpoint.table_blocks.len(), METADATA_DIGEST_TABLES.len());
    assert_ne!(checkpoint.checkpoint_crc64, 0);
    checkpoint.verify().unwrap();
    assert_eq!(
        checkpoint
            .table_digests
            .iter()
            .map(|table| table.table_name.as_str())
            .collect::<Vec<_>>(),
        METADATA_DIGEST_TABLES
            .iter()
            .map(|table| table.name)
            .collect::<Vec<_>>()
    );
    assert!(checkpoint
        .table_digests
        .iter()
        .any(|table| table.table_name == "buckets" && table.row_count == 1));
    let bucket_block = checkpoint
        .table_blocks
        .iter()
        .find(|table| table.table_name == "buckets")
        .unwrap();
    assert_eq!(bucket_block.row_count, 1);
    assert_eq!(bucket_block.rows.len(), 1);
    assert_eq!(
        bucket_block.table_digest,
        checkpoint
            .table_digests
            .iter()
            .find(|table| table.table_name == "buckets")
            .unwrap()
            .table_digest
    );

    let second_bucket = trusted_bucket_name("metadata-checkpoint-second");
    let second_command = create_bucket_probe_command(1, 2, second_bucket, 2);
    store
        .apply_metadata_command_and_record(0, &second_command)
        .unwrap();
    let updated = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_ne!(updated.state_digest, checkpoint.state_digest);
    assert_ne!(updated.checkpoint_crc64, checkpoint.checkpoint_crc64);
    assert!(updated
        .table_digests
        .iter()
        .any(|table| table.table_name == "buckets" && table.row_count == 2));
    updated.verify().unwrap();
}

#[test]
fn metadata_command_checkpoint_catalogue_persists_and_lists_newest_valid_candidates() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first_bucket = trusted_bucket_name("metadata-checkpoint-catalogue-first");
    let second_bucket = trusted_bucket_name("metadata-checkpoint-catalogue-second");
    let first_command = create_bucket_probe_command(1, 1, first_bucket, 1);
    store
        .apply_metadata_command_and_record(0, &first_command)
        .unwrap();
    let first_checkpoint = store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let second_command = create_bucket_probe_command(1, 2, second_bucket, 2);
    store
        .apply_metadata_command_and_record(0, &second_command)
        .unwrap();
    let second_checkpoint = store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let candidates = store
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 8)
        .unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|checkpoint| checkpoint.applied_log_index)
            .collect::<Vec<_>>(),
        vec![
            second_checkpoint.applied_log_index,
            first_checkpoint.applied_log_index
        ]
    );
    drop(store);

    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bounded_candidates = store
        .metadata_command_checkpoint_candidates(
            ClusterEpoch::INITIAL,
            second_checkpoint.applied_log_index - 1,
            8,
        )
        .unwrap();
    assert_eq!(bounded_candidates, vec![first_checkpoint.clone()]);

    store
        .connection()
        .execute(
            "UPDATE metadata_command_checkpoints SET checkpoint_bytes = X'00' \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND applied_log_index = ?3",
            rusqlite::params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                second_checkpoint.applied_log_index as i64,
            ],
        )
        .unwrap();
    let candidates = store
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 8)
        .unwrap();
    assert_eq!(candidates, vec![first_checkpoint]);
}

#[test]
fn metadata_command_checkpoint_catalogue_prunes_old_epochs() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();

    let initial_state = store.metadata_command_replica_state().unwrap();
    let state_digest = initial_state.state_digest;
    for epoch_value in 1..=6 {
        let cluster_epoch = ClusterEpoch::new(epoch_value).unwrap();
        if cluster_epoch != initial_state.cluster_epoch {
            store
                .initialize_metadata_transfer_matching_state(0, cluster_epoch, 0, 0, state_digest)
                .unwrap();
        }
        store
            .record_current_metadata_command_checkpoint(0, cluster_epoch)
            .unwrap();
    }

    let retained_epochs = (3..=6).collect::<Vec<_>>();
    let mut observed_retained_epochs = Vec::new();
    for epoch_value in 1..=6 {
        let cluster_epoch = ClusterEpoch::new(epoch_value).unwrap();
        let candidates = store
            .metadata_command_checkpoint_candidates(cluster_epoch, u64::MAX, 8)
            .unwrap();
        if candidates.is_empty() {
            assert!(epoch_value < 3, "newer epoch {epoch_value} was pruned");
        } else {
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].cluster_epoch, cluster_epoch);
            observed_retained_epochs.push(epoch_value);
        }
    }

    assert_eq!(observed_retained_epochs, retained_epochs);
}

#[test]
fn cluster_map_history_reference_summary_reports_live_payload_and_backfill_epochs() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 7).unwrap();
    assert_eq!(
        store.cluster_map_history_reference_summary().unwrap(),
        PgClusterMapHistoryReferenceSummary::default()
    );

    store
        .connection()
        .execute(
            "INSERT INTO object_segments \
             (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, \
              segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, 1, 0, 1024, ?3, ?4, 10, 7, ?5, 4, 2)",
            rusqlite::params![
                "history-floor-bucket",
                "segment-object",
                0x1234_i64,
                [0x11_u8; 16].as_slice(),
                6_i64,
            ],
        )
        .unwrap();
    store
        .connection()
        .execute(
            "INSERT INTO object_parts \
             (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, \
              etag, etag_kind, part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id) \
             VALUES (?1, ?2, 1, 1, 0, 2048, ?3, ?4, 0, ?5, 11, ?6, 4, 2, 7)",
            rusqlite::params![
                "history-floor-bucket",
                "multipart-object",
                0x5678_i64,
                [0x22_u8; 16].as_slice(),
                [0x33_u8; 16].as_slice(),
                4_i64,
            ],
        )
        .unwrap();

    let backfill = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: 7,
            segment_okh: [0x44; 16],
            segment_vid: GenerationId::new(12).unwrap(),
            stored_size: 4096,
            segment_crc64: 0x9abc,
            ec: EcShape { k: 4, m: 2 },
        },
        source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
        desired_cluster_epoch: ClusterEpoch::new(9).unwrap(),
    };
    store
        .record_placed_segment_shard_backfill(&backfill, backfill.request.ec.m, None)
        .unwrap();

    let summary = store.cluster_map_history_reference_summary().unwrap();
    assert_eq!(
        summary.oldest_live_placement_epoch,
        Some(ClusterEpoch::new(4).unwrap())
    );
    assert_eq!(
        summary.oldest_durable_backfill_epoch,
        Some(ClusterEpoch::new(3).unwrap())
    );
    assert_eq!(
        summary.oldest_required_epoch(),
        Some(ClusterEpoch::new(3).unwrap())
    );
}

#[test]
fn metadata_command_checkpoint_verification_rejects_tampered_row_payload() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-tamper");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    let mut checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let bucket_block = checkpoint
        .table_blocks
        .iter_mut()
        .find(|table| table.table_name == "buckets")
        .unwrap();
    match &mut bucket_block.rows[0].values[0] {
        MetadataCheckpointValue::Text(value) => value.extend_from_slice(b"-tampered"),
        value => panic!("expected bucket name text value, got {value:?}"),
    }

    let err = checkpoint.verify().unwrap_err();
    assert!(matches!(
        err,
        MetadataCommandCheckpointValidationError::RowDigestMismatch {
            table_name,
            row_index: 0,
            ..
        } if table_name == "buckets"
    ));
}

#[test]
fn metadata_command_checkpoint_exports_blob_backed_metadata_rows() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-blob");
    let key = trusted_object_key("object");
    let okh = [7_u8; 16];
    store
        .conn
        .execute(
            "INSERT INTO object_segments \
             (bucket, key, version_id, segment_index, size, segment_crc64, \
              segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                bucket.as_str(),
                key.as_str(),
                1_i64,
                0_i64,
                32_i64,
                99_i64,
                okh.as_slice(),
                1_i64,
                1_i64,
                1_i64,
                4_i64,
                2_i64,
            ],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();

    let checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    checkpoint.verify().unwrap();
    let segment_block = checkpoint
        .table_blocks
        .iter()
        .find(|table| table.table_name == "object_segments")
        .unwrap();
    assert_eq!(segment_block.row_count, 1);
    assert!(segment_block.rows[0]
        .values
        .iter()
        .any(|value| matches!(value, MetadataCheckpointValue::Blob(bytes) if bytes == &okh)));
}

#[test]
fn install_metadata_transfer_checkpoint_base_restores_materialized_rows() {
    let source_tmp = test_util::tempdir();
    let source = PgStore::open(source_tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-install");
    let key = trusted_object_key("object");
    let okh = [9_u8; 16];
    source
        .conn
        .execute(
            "INSERT INTO object_segments \
             (bucket, key, version_id, segment_index, size, segment_crc64, \
              segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                bucket.as_str(),
                key.as_str(),
                1_i64,
                0_i64,
                32_i64,
                99_i64,
                okh.as_slice(),
                1_i64,
                1_i64,
                1_i64,
                4_i64,
                2_i64,
            ],
        )
        .unwrap();
    source.refresh_metadata_command_state_digest().unwrap();
    let checkpoint = source
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let destination_tmp = test_util::tempdir();
    let destination = PgStore::open(destination_tmp.path(), 1).unwrap();
    let destination_epoch = ClusterEpoch::new(19).unwrap();
    let state = destination
        .install_metadata_transfer_checkpoint_base(2, destination_epoch, &checkpoint)
        .unwrap();
    assert_eq!(state.cluster_epoch, destination_epoch);
    assert_eq!(state.applied_log_index, 0);
    assert_eq!(state.applied_log_hash, 0);
    assert_eq!(state.state_digest, checkpoint.state_digest);

    let restored = destination
        .metadata_command_checkpoint(2, destination_epoch)
        .unwrap();
    assert_eq!(restored.state_digest, checkpoint.state_digest);
    restored.verify().unwrap();
    let segment_block = restored
        .table_blocks
        .iter()
        .find(|table| table.table_name == "object_segments")
        .unwrap();
    assert_eq!(segment_block.row_count, 1);
    assert!(segment_block.rows[0]
        .values
        .iter()
        .any(|value| matches!(value, MetadataCheckpointValue::Blob(bytes) if bytes == &okh)));
}

#[test]
fn install_metadata_transfer_checkpoint_base_defers_foreign_keys_until_full_restore() {
    let source_tmp = test_util::tempdir();
    let source = PgStore::open(source_tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-install-fk");
    create_probe_bucket_direct(&source, &bucket);
    put_probe_lifecycle_direct(&source, &bucket);
    source.refresh_metadata_command_state_digest().unwrap();
    let checkpoint = source
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let subresource_block = checkpoint
        .table_blocks
        .iter()
        .find(|table| table.table_name == "bucket_subresources")
        .unwrap();
    let buckets_block = checkpoint
        .table_blocks
        .iter()
        .find(|table| table.table_name == "buckets")
        .unwrap();
    assert_eq!(subresource_block.row_count, 1);
    assert_eq!(buckets_block.row_count, 1);

    let destination_tmp = test_util::tempdir();
    let destination = PgStore::open(destination_tmp.path(), 1).unwrap();
    let destination_epoch = ClusterEpoch::new(29).unwrap();
    destination
        .install_metadata_transfer_checkpoint_base(2, destination_epoch, &checkpoint)
        .unwrap();

    assert_eq!(destination.head_bucket_raw(&bucket).unwrap().name, bucket);
    let lifecycle = PgMetadataStore::get_bucket_subresource(
        &destination,
        &bucket,
        BucketSubresourceKind::Lifecycle,
    )
    .unwrap()
    .expect("lifecycle subresource should restore after parent bucket");
    assert_eq!(lifecycle.body, "<LifecycleConfiguration/>");
}

#[test]
fn install_metadata_transfer_checkpoint_base_rejects_tampered_checkpoint_without_mutation() {
    let source_tmp = test_util::tempdir();
    let source = PgStore::open(source_tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-install-tamper");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    source
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let mut checkpoint = source
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let bucket_block = checkpoint
        .table_blocks
        .iter_mut()
        .find(|table| table.table_name == "buckets")
        .unwrap();
    match &mut bucket_block.rows[0].values[0] {
        MetadataCheckpointValue::Text(value) => value.extend_from_slice(b"-tampered"),
        value => panic!("expected bucket name text value, got {value:?}"),
    }

    let destination_tmp = test_util::tempdir();
    let destination = PgStore::open(destination_tmp.path(), 1).unwrap();
    let before = destination.metadata_command_replica_state().unwrap();
    let destination_epoch = ClusterEpoch::new(23).unwrap();
    let err = destination
        .install_metadata_transfer_checkpoint_base(2, destination_epoch, &checkpoint)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCheckpointInvalid {
            pg_id: 1,
            cluster_epoch,
            ..
        } if cluster_epoch == destination_epoch
    ));
    assert_eq!(
        destination.metadata_command_replica_state().unwrap(),
        before
    );
    assert!(
        destination
            .metadata_command_replica_state_can_initialize()
            .unwrap(),
        "failed checkpoint install must leave destination empty"
    );
}

#[test]
fn install_metadata_transfer_checkpoint_base_rejects_current_epoch_dirty_zero_log_destination() {
    let source_tmp = test_util::tempdir();
    let source = PgStore::open(source_tmp.path(), 1).unwrap();
    let source_bucket = trusted_bucket_name("metadata-checkpoint-install-source-current-dirty");
    create_probe_bucket_direct(&source, &source_bucket);
    source.refresh_metadata_command_state_digest().unwrap();
    let checkpoint = source
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let destination_tmp = test_util::tempdir();
    let destination = PgStore::open(destination_tmp.path(), 1).unwrap();
    let dirty_bucket = trusted_bucket_name("metadata-checkpoint-install-dirty-current");
    destination.metadata_command_replica_state().unwrap();
    create_probe_bucket_direct(&destination, &dirty_bucket);
    destination.refresh_metadata_command_state_digest().unwrap();
    let before = destination.metadata_command_replica_state().unwrap();
    assert_eq!(before.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(before.applied_log_index, 0);
    assert_eq!(before.applied_log_hash, 0);
    assert!(!destination
        .metadata_command_replica_state_can_initialize()
        .unwrap());

    let err = destination
        .install_metadata_transfer_checkpoint_base(2, ClusterEpoch::INITIAL, &checkpoint)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandReplicaStateMissing { pg_id: 1 }
    ));
    assert_eq!(
        destination.metadata_command_replica_state().unwrap(),
        before
    );
    assert_eq!(
        destination.head_bucket_raw(&dirty_bucket).unwrap().name,
        dirty_bucket
    );
    assert!(destination.head_bucket_raw(&source_bucket).is_err());
}

#[test]
fn metadata_command_checkpoint_rejects_pending_command() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("metadata-checkpoint-pending");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();

    let err = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandContention {
            context: "export metadata command checkpoint with pending command"
        }
    ));
}

#[test]
fn metadata_command_checkpoint_rejects_stale_epoch() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let stale_epoch = ClusterEpoch::new(17).unwrap();

    let err = store
        .metadata_command_checkpoint(0, stale_epoch)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataCommand {
            pg_id: 1,
            command_epoch: ClusterEpoch::INITIAL,
            current_epoch,
            ..
        } if current_epoch == stale_epoch
    ));
}

#[test]
fn metadata_command_checkpoint_rejects_abandoned_tail_without_recovery_side_effects() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("checkpoint-abandoned-tail"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
        )
        .unwrap();

    let before = store.metadata_command_replica_state().unwrap();
    let err = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        }
    ));
    assert_eq!(
        store.metadata_command_replica_state().unwrap(),
        before,
        "checkpoint export must not advance abandoned tails"
    );
    let (previous_log_hash, log_hash): (Option<i64>, Option<i64>) = store
        .conn
        .query_row(
            "SELECT previous_log_hash, log_hash \
             FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![ClusterEpoch::INITIAL.get() as i64, 1_i64, 1_i64],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(previous_log_hash, None);
    assert_eq!(log_hash, None);
}

#[test]
fn adopt_metadata_transfer_state_rejects_pending_command() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("adopt-transfer-pending");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let source_state = store.metadata_command_replica_state().unwrap();
    let pending_bucket = trusted_bucket_name("adopt-transfer-pending-next");
    let pending = create_bucket_probe_command(1, 2, pending_bucket.clone(), 2);
    store
        .try_insert_pending_metadata_command_slot(0, &pending, Some(&pending_bucket))
        .unwrap();

    let destination_epoch = ClusterEpoch::new(9).unwrap();
    let rebased = metadata_transfer_commands(
        rebase_probe_commands(
            destination_epoch,
            PgId::new(1),
            std::slice::from_ref(&command),
        ),
        &[source_state.state_digest],
    );
    let err = store
        .adopt_metadata_transfer_state_from_rebased_commands(
            0,
            destination_epoch,
            &rebased,
            source_state.state_digest,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandContention {
            context: "adopt metadata transfer state with pending command"
        }
    ));
    assert_eq!(
        store
            .metadata_command_replica_state()
            .unwrap()
            .cluster_epoch,
        ClusterEpoch::INITIAL
    );
}

#[test]
fn retained_metadata_command_log_entries_preserves_abandoned_tombstones() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("retained-entry-abandoned");
    let command = create_bucket_probe_command(1, 1, bucket, 1);

    store
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let entries = store
        .retained_metadata_command_log_entries(
            0,
            ClusterEpoch::INITIAL,
            MetadataCommandLogIndex::new(1).unwrap(),
            MetadataCommandLogIndex::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_index, 1);
    assert_eq!(entries[0].previous_log_hash, 0);
    match entries[0].kind {
        MetadataCommandLogRangeEntryKind::Abandoned {
            original_command_checksum,
        } => {
            assert_eq!(original_command_checksum, command.checksum_crc64());
        }
        ref other => panic!("expected abandoned command entry, got {other:?}"),
    }
}

#[test]
fn metadata_command_log_contiguous_insert_uses_prefix_fast_path() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command = create_bucket_probe_command(1, 1, trusted_bucket_name("fast-path"), 1);
    let before = store
        .metadata_command_log_prefix_fast_path_hits
        .load(Ordering::Relaxed);

    let state = store.record_metadata_command_applied(0, &command).unwrap();

    assert!(
        store
            .metadata_command_log_prefix_fast_path_hits
            .load(Ordering::Relaxed)
            > before,
        "contiguous insert without a durable tail should take the prefix fast path"
    );
    let expected_log_hash = metadata_command_log_hash(
        ClusterEpoch::INITIAL,
        PgId::new(1),
        MetadataCommandLogIndex::new(1).unwrap(),
        0,
        command.checksum_crc64(),
    );
    assert_eq!(state.applied_log_index, 1);
    assert_eq!(state.applied_log_hash, expected_log_hash);

    let (previous_log_hash, log_hash): (Option<i64>, Option<i64>) = store
        .conn
        .query_row(
            "SELECT previous_log_hash, log_hash \
             FROM metadata_command_log WHERE log_index = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(previous_log_hash, Some(0));
    assert_eq!(log_hash, Some(expected_log_hash as i64));
}

#[test]
fn pending_metadata_command_slot_is_pg_scoped_and_persistent() {
    let tmp = test_util::tempdir();
    let first_bucket = trusted_bucket_name("pending-slot-one");
    let second_bucket = trusted_bucket_name("pending-slot-two");
    let first_command = create_bucket_probe_command(1, 1, first_bucket.clone(), 1);
    let second_command = create_bucket_probe_command(1, 2, second_bucket.clone(), 2);

    {
        let store = PgStore::open(tmp.path(), 1).unwrap();
        store
            .try_insert_pending_metadata_command_slot(0, &first_command, Some(&first_bucket))
            .unwrap();
        store
            .try_insert_pending_metadata_command_slot(0, &first_command, Some(&first_bucket))
            .unwrap();

        let err = store
            .try_insert_pending_metadata_command_slot(0, &second_command, Some(&second_bucket))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandPendingConflict {
                pg_id: 1,
                existing_log_index: 1,
                candidate_log_index: 2,
                ..
            }
        ));
    }

    let store = PgStore::open(tmp.path(), 1).unwrap();
    let slot = store
        .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
        .unwrap()
        .expect("pending slot should persist across reopen");
    assert_eq!(slot.id, first_command.id());
    assert_eq!(slot.command_checksum, first_command.checksum_crc64());
    assert_eq!(slot.command_bytes, first_command.command_bytes());
    assert_eq!(slot.scope_bucket.as_ref(), Some(&first_bucket));
    let err = store
        .remove_pending_metadata_command_slot(0, &first_command)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "slot removal before a terminal log row must fail, got {err:?}"
    );
    store
        .record_metadata_command_abandoned(0, &first_command)
        .unwrap();

    assert!(
        !store
            .remove_pending_metadata_command_slot(0, &second_command)
            .unwrap(),
        "non-matching command must not clear the durable slot"
    );
    assert!(
        store
            .remove_pending_metadata_command_slot(0, &first_command)
            .unwrap(),
        "matching command should clear the durable slot"
    );
    assert!(
        store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_none(),
        "slot should be empty after exact removal"
    );
}

#[test]
fn pending_metadata_command_slot_rejects_terminal_log_index() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("pending-slot-terminal");
    let first_command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    let stale_command = create_bucket_probe_command(1, 1, bucket.clone(), 2);
    let next_command = create_bucket_probe_command(1, 2, bucket.clone(), 3);

    let store = PgStore::open(tmp.path(), 1).unwrap();
    store
        .apply_metadata_command_and_record(0, &first_command)
        .unwrap();

    let err = store
        .try_insert_pending_metadata_command_slot(0, &stale_command, Some(&bucket))
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            log_index: 1,
            ..
        }
    ));
    store
        .try_insert_pending_metadata_command_slot(0, &next_command, Some(&bucket))
        .unwrap();
}

#[test]
fn pending_metadata_command_slot_rejects_huge_repeated_count_before_allocation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("pending-slot-huge-count");
    let key = trusted_object_key("object");
    let reservation_id = SessionId::try_from("52".repeat(16)).unwrap();
    let proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "pending-slot-proof".to_string(),
        owner_token: "pending-slot-owner".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "direct-put-commit".to_string(),
        created_at: 1,
        lease_deadline: 2,
        target_context: Some(key.as_str().to_string()),
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key,
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: reservation_id.clone(),
            write_sequence: 1,
            last_modified_millis: 2,
            stale_payload: None,
            bucket_write_reservation: proof,
            stream_create_bucket_write_reservation: None,
        })),
    );
    let mut malformed_bytes = command.command_bytes();
    let reservation_id_bytes = reservation_id.as_str().as_bytes();
    let mut empty_segments_then_reservation = Vec::new();
    empty_segments_then_reservation.extend_from_slice(&0_u32.to_le_bytes());
    empty_segments_then_reservation
        .extend_from_slice(&(reservation_id_bytes.len() as u32).to_le_bytes());
    empty_segments_then_reservation.extend_from_slice(reservation_id_bytes);
    let segments_count_offset = malformed_bytes
        .windows(empty_segments_then_reservation.len())
        .position(|window| window == empty_segments_then_reservation)
        .expect("direct PUT command should encode empty segments before reservation id");
    malformed_bytes[segments_count_offset..segments_count_offset + 4]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    let malformed_checksum = checksum::crc64::checksum(&malformed_bytes);

    store
        .conn
        .execute(
            "INSERT INTO metadata_command_pending_slot \
             (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                malformed_checksum as i64,
                malformed_bytes,
                bucket.as_str(),
            ],
        )
        .unwrap();

    let err = store
        .pending_metadata_command_envelope(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "malformed repeated count must fail closed as command conflict, got {err:?}"
    );
}

#[test]
fn pending_metadata_command_slot_rejects_proofless_create_multipart_upload() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("pending-slot-proofless-mpu");
    let key = trusted_object_key("object");
    let upload_id = UploadId::try_from(format!("{}{}", "upload", ".".repeat(122))).unwrap();
    let proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "proofless-mpu-reservation".to_string(),
        owner_token: "owner-token".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "create-multipart-upload".to_string(),
        created_at: 2,
        lease_deadline: 3,
        target_context: Some(key.as_str().to_string()),
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CreateMultipartUpload(Box::new(
            CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                CreateMultipartUploadReq {
                    upload_id,
                    bucket: bucket.clone(),
                    key: key.clone(),
                    tags: None,
                    metadata_blob: SerializedMetadataBlob::default(),
                    system_metadata_blob: SerializedSystemMetadataBlob::default(),
                    initiator: OwnerIdentity::from_principal("initiator"),
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: ObjectLockState::default(),
                    checksum: None,
                    encryption: ObjectEncryption::None,
                },
                GenerationId::MIN,
                3,
                proof,
            ),
        )),
    );
    let mut proofless_bytes = command.command_bytes();
    let reservation_id_bytes = b"proofless-mpu-reservation";
    let reservation_id_offset = proofless_bytes
        .windows(reservation_id_bytes.len())
        .position(|window| window == reservation_id_bytes)
        .expect("proof reservation id should be encoded");
    let encoded_bucket_name = {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(bucket.as_str().len() as u32).to_le_bytes());
        encoded.extend_from_slice(bucket.as_str().as_bytes());
        encoded
    };
    let proof_start = proofless_bytes[..reservation_id_offset]
        .windows(encoded_bucket_name.len())
        .rposition(|window| window == encoded_bucket_name)
        .expect("proof bucket name should be encoded before reservation id");
    proofless_bytes.truncate(proof_start);
    let proofless_checksum = checksum::crc64::checksum(&proofless_bytes);

    store
        .conn
        .execute(
            "INSERT INTO metadata_command_pending_slot \
             (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                proofless_checksum as i64,
                proofless_bytes,
                bucket.as_str(),
            ],
        )
        .unwrap();

    let err = store
        .pending_metadata_command_envelope(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "proofless MPU-create command must fail closed as command conflict, got {err:?}"
    );
}

#[test]
fn pending_metadata_command_slot_validation_rejects_log_gap() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("pending-slot-gap");
    let command = create_bucket_probe_command(1, 2, bucket.clone(), 2);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "pending slot ahead of applied prefix must fail validation, got {err:?}"
    );
}

#[test]
fn terminal_pending_metadata_command_slot_is_cleaned_on_validation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("terminal-pending-slot");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    store.record_metadata_command_applied(0, &command).unwrap();

    let state = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(state.applied_log_index, 1);
    assert!(
        store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_none(),
        "validation should clean a pending slot whose terminal record is already durable"
    );
}

#[test]
fn pg_store_recover_preserves_terminal_pending_metadata_command_slot() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("recover-terminal-pending-slot");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    store.record_metadata_command_applied(0, &command).unwrap();

    let state = store
        .recover(super::super::PgStoreRecoveryContext::for_node(NodeId::new(
            0,
        )))
        .unwrap();
    assert_eq!(state.applied_log_index, 1);
    let slot = store
        .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
        .unwrap()
        .expect("local recovery must preserve terminal pending slot until acting-set evidence");
    assert_eq!(slot.id, command.id());
    assert_eq!(slot.command_checksum, command.checksum_crc64());
}

#[test]
fn pending_slot_finalization_rejects_mismatched_scope_bucket() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("pending-slot-scope");
    let wrong_scope = trusted_bucket_name("pending-slot-wrong-scope");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store.record_metadata_command_applied(0, &command).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_pending_slot \
             (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get() as i64,
                command.id().log_index().get() as i64,
                command.checksum_crc64() as i64,
                command.command_bytes(),
                wrong_scope.as_str(),
            ],
        )
        .unwrap();

    let err = store
        .remove_pending_metadata_command_slot(0, &command)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandPendingConflict { .. }),
        "mismatched scope bucket must fail closed during finalization, got {err:?}"
    );
    assert!(
        store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_some(),
        "failed finalization must not remove the mismatched scoped slot"
    );
}

#[test]
fn abandoned_pending_slot_with_unadvanced_replica_state_recovers_on_validation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("abandoned-pending-slot");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
        )
        .unwrap();

    let before = store.metadata_command_replica_state().unwrap();
    assert_eq!(before.applied_log_index, 0);

    let recovered = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(recovered.applied_log_index, 1);
    assert!(
        store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_none(),
        "validation should advance matching abandoned row and clean the pending slot"
    );
}

#[test]
fn abandoned_pending_slot_rejects_mismatched_scope_bucket_on_validation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("abandoned-pending-slot-scope");
    let wrong_scope = trusted_bucket_name("abandoned-pending-slot-wrong-scope");
    let command = create_bucket_probe_command(1, 1, bucket, 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&wrong_scope))
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "mismatched scope bucket must fail closed before abandoned cleanup, got {err:?}"
    );
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 0);
    assert!(
        store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_some(),
        "failed abandoned cleanup must not remove the mismatched scoped slot"
    );
}

#[test]
fn abandoned_log_tail_without_pending_slot_recovers_on_validation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("abandoned-tail-without-slot"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
        )
        .unwrap();

    let before = store.metadata_command_replica_state().unwrap();
    assert_eq!(before.applied_log_index, 0);
    let recovered = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
    let expected_log_hash = metadata_command_log_hash(
        ClusterEpoch::INITIAL,
        PgId::new(1),
        MetadataCommandLogIndex::new(1).unwrap(),
        0,
        command.abandoned_log_checksum_crc64(),
    );
    assert_eq!(recovered.applied_log_index, 1);
    assert_eq!(recovered.applied_log_hash, expected_log_hash);
}

#[test]
fn abandoned_log_tail_preserves_digest_and_rejects_materialized_mutation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("abandoned-tail-materialized-mutation");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    create_probe_bucket_direct(&store, &bucket);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 1);
    assert_ne!(state.state_digest, store.metadata_state_digest().unwrap());
}

#[test]
fn abandoned_pending_slot_preserves_digest_and_rejects_materialized_mutation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("abandoned-pending-materialized-mutation");
    let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    create_probe_bucket_direct(&store, &bucket);
    store
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 1);
    assert_ne!(state.state_digest, store.metadata_state_digest().unwrap());
}

#[test]
fn applied_log_tail_without_advanced_state_fails_validation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("applied-tail-without-state"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command.checksum_crc64() as i64,
                command.command_bytes(),
            ],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert!(
        matches!(err, StoreError::MetadataCommandLogConflict { .. }),
        "unadvanced applied log tail must fail closed, got {err:?}"
    );
}

#[test]
fn abandoned_metadata_command_log_rows_match_original_command() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let abandoned_command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("abandoned-bucket"), 1);
    let different_command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("different-bucket"), 1);

    store
        .record_metadata_command_abandoned(0, &abandoned_command)
        .unwrap();

    assert_eq!(
        store
            .metadata_command_abandon_acceptance(0, &abandoned_command)
            .unwrap(),
        MetadataCommandAcceptance::AlreadyApplied
    );
    assert!(
        store
            .metadata_command_abandoned(0, &abandoned_command)
            .unwrap(),
        "abandoned row should match its original command"
    );
    assert!(
        !store
            .metadata_command_abandoned(0, &different_command)
            .unwrap(),
        "abandoned tombstones are tied to the original command checksum"
    );
    assert!(matches!(
        store
            .metadata_command_abandon_acceptance(0, &different_command)
            .unwrap_err(),
        StoreError::MetadataCommandLogConflict { .. }
    ));
}

#[test]
fn metadata_command_log_stats_include_retained_prefix_and_tail() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let applied_command =
        create_bucket_probe_command(1, 1, trusted_bucket_name("stats-applied"), 1);
    let abandoned_command =
        create_bucket_probe_command(1, 2, trusted_bucket_name("stats-abandoned"), 2);
    let tail_command = create_bucket_probe_command(1, 4, trusted_bucket_name("stats-tail"), 4);

    store
        .record_metadata_command_applied(0, &applied_command)
        .unwrap();
    store
        .record_metadata_command_abandoned(0, &abandoned_command)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                4_i64,
                tail_command.checksum_crc64() as i64,
                tail_command.command_bytes(),
            ],
        )
        .unwrap();

    let stats = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(stats.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(stats.pg_id, PgId::new(1));
    assert_eq!(stats.min_log_index, Some(1));
    assert_eq!(stats.max_log_index, Some(4));
    assert_eq!(stats.applied_log_index, 2);
    assert_eq!(stats.retained_entries, 3);
    assert_eq!(stats.abandoned_entries, 1);
    assert_eq!(stats.pending_tail_entries, 1);
    assert_eq!(stats.missing_applied_prefix_entries, 0);
    assert_eq!(stats.compactable_before, None);
}

#[test]
fn metadata_command_log_compaction_without_checkpoint_is_noop() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("retain-first"), 1);
    let second = create_bucket_probe_command(1, 2, trusted_bucket_name("retain-second"), 2);

    store.record_metadata_command_applied(0, &first).unwrap();
    store.record_metadata_command_abandoned(0, &second).unwrap();

    let before = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(before.retained_entries, 2);

    let status = store
        .compact_metadata_command_log(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        status,
        MetadataCommandLogCompactionStatus::NoCheckpoint {
            retained_entries: 2
        }
    );

    let after = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(after, before);
}

#[test]
fn metadata_command_log_compaction_deletes_checkpoint_covered_prefix() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("compact-first"), 1);
    let second = create_bucket_probe_command(1, 2, trusted_bucket_name("compact-second"), 2);
    let third = create_bucket_probe_command(1, 3, trusted_bucket_name("compact-third"), 3);

    store.record_metadata_command_applied(0, &first).unwrap();
    store.record_metadata_command_applied(0, &second).unwrap();
    let checkpoint = store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(checkpoint.applied_log_index, 2);
    store.record_metadata_command_applied(0, &third).unwrap();

    let before = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(before.retained_entries, 3);
    assert_eq!(before.pending_tail_entries, 0);
    assert_eq!(before.missing_applied_prefix_entries, 0);
    assert_eq!(before.compactable_before, Some(3));

    let status = store
        .compact_metadata_command_log(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        status,
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 2,
            compacted_before: 3,
        }
    );

    let after = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(after.min_log_index, Some(3));
    assert_eq!(after.max_log_index, Some(3));
    assert_eq!(after.retained_entries, 1);
    assert_eq!(after.pending_tail_entries, 0);
    assert_eq!(after.missing_applied_prefix_entries, 0);
    assert_eq!(after.compactable_before, Some(3));

    store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
    let next_checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(next_checkpoint.applied_log_index, 3);
}

#[test]
fn metadata_command_log_compaction_preserves_next_index_when_checkpoint_is_current() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("compact-current-first"), 1);
    let second =
        create_bucket_probe_command(1, 2, trusted_bucket_name("compact-current-second"), 2);

    store.record_metadata_command_applied(0, &first).unwrap();
    store.record_metadata_command_applied(0, &second).unwrap();
    store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let status = store
        .compact_metadata_command_log(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        status,
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 2,
            compacted_before: 3,
        }
    );

    let after = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(after.min_log_index, None);
    assert_eq!(after.max_log_index, None);
    assert_eq!(after.retained_entries, 0);
    assert_eq!(after.missing_applied_prefix_entries, 0);
    assert_eq!(after.compactable_before, Some(3));
    assert_eq!(
        store
            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        2
    );

    let third = create_bucket_probe_command(1, 3, trusted_bucket_name("compact-current-third"), 3);
    store.record_metadata_command_applied(0, &third).unwrap();
    let final_state = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(final_state.applied_log_index, 3);
}

#[test]
fn metadata_command_log_compaction_rejects_pending_command() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let first = create_bucket_probe_command(1, 1, trusted_bucket_name("compact-pending-base"), 1);
    let pending_bucket = trusted_bucket_name("compact-pending-next");
    let pending = create_bucket_probe_command(1, 2, pending_bucket.clone(), 2);

    store.record_metadata_command_applied(0, &first).unwrap();
    store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    store
        .try_insert_pending_metadata_command_slot(0, &pending, Some(&pending_bucket))
        .unwrap();

    let status = store
        .compact_metadata_command_log(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        status,
        MetadataCommandLogCompactionStatus::PendingCommand {
            retained_entries: 1
        }
    );

    let after = store
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(after.retained_entries, 1);
    assert_eq!(after.compactable_before, Some(2));
}

#[test]
fn metadata_command_log_stats_reject_stale_epoch() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let stale_epoch = ClusterEpoch::new(2).unwrap();

    let err = store.metadata_command_log_stats(stale_epoch).unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataOperation {
            pg_id: 1,
            operation_epoch,
            current_epoch: ClusterEpoch::INITIAL,
        } if operation_epoch == stale_epoch
    ));

    let err = store.compact_metadata_command_log(stale_epoch).unwrap_err();
    assert!(matches!(
        err,
        StoreError::StaleMetadataOperation {
            pg_id: 1,
            operation_epoch,
            current_epoch: ClusterEpoch::INITIAL,
        } if operation_epoch == stale_epoch
    ));
}

#[test]
fn metadata_command_log_prefix_rejects_row_key_and_kind_mismatch() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command_with_wrong_embedded_index =
        create_bucket_probe_command(1, 2, trusted_bucket_name("wrong-index"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                command_with_wrong_embedded_index.checksum_crc64() as i64,
                command_with_wrong_embedded_index.command_bytes(),
            ],
        )
        .unwrap();
    let next_command = create_bucket_probe_command(1, 1, trusted_bucket_name("next"), 1);
    let err = store
        .record_metadata_command_applied(0, &next_command)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            log_index: 1,
            ..
        }
    ));

    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let applied_command = create_bucket_probe_command(1, 1, trusted_bucket_name("applied"), 1);
    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                applied_command.checksum_crc64() as i64,
                applied_command.command_bytes(),
            ],
        )
        .unwrap();
    let next_command = create_bucket_probe_command(1, 1, trusted_bucket_name("applied"), 1);
    let err = store
        .record_metadata_command_applied(0, &next_command)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            log_index: 1,
            ..
        }
    ));
}

#[test]
fn metadata_command_log_prefix_rejects_malformed_applied_bytes() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let command = create_bucket_probe_command(1, 1, trusted_bucket_name("malformed"), 1);
    let mut malformed_bytes = command.command_bytes();
    malformed_bytes.push(0);
    let malformed_checksum = checksum::crc64::checksum(&malformed_bytes);

    store
        .conn
        .execute(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            params![
                ClusterEpoch::INITIAL.get() as i64,
                1_i64,
                1_i64,
                malformed_checksum as i64,
                malformed_bytes,
            ],
        )
        .unwrap();
    let next_command = create_bucket_probe_command(1, 1, trusted_bucket_name("malformed"), 1);
    let err = store
        .record_metadata_command_applied(0, &next_command)
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            log_index: 1,
            ..
        }
    ));
}

#[test]
fn metadata_state_digest_covers_object_segments() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    let okh = [1_u8; 16];
    store
        .conn
        .execute(
            "INSERT INTO object_segments \
             (bucket, key, version_id, segment_index, size, segment_crc64, \
              segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                bucket.as_str(),
                key.as_str(),
                1_i64,
                0_i64,
                32_i64,
                99_i64,
                okh.as_slice(),
                1_i64,
                1_i64,
                1_i64,
                4_i64,
                2_i64,
            ],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    store
        .conn
        .execute(
            "UPDATE object_segments SET data_pg_id = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND version_id = ?4",
            params![2_i64, bucket.as_str(), key.as_str(), 1_i64],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn metadata_state_digest_covers_multipart_part_segments() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
    let okh = [2_u8; 16];
    store
        .conn
        .execute(
            "INSERT INTO multipart_part_segments \
             (bucket, key, upload_id, version_id, part_number, segment_index, size, \
              segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                bucket.as_str(),
                key.as_str(),
                upload_id.as_str(),
                1_i64,
                1_i64,
                0_i64,
                32_i64,
                99_i64,
                okh.as_slice(),
                1_i64,
                1_i64,
                1_i64,
                4_i64,
                2_i64,
            ],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    store
        .conn
        .execute(
            "UPDATE multipart_part_segments SET ec_m = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4",
            params![3_i64, bucket.as_str(), key.as_str(), upload_id.as_str()],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn metadata_state_digest_covers_multipart_part_staging_segments() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("digest-bucket");
    let key = trusted_object_key("object");
    let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
    let okh = [3_u8; 16];
    store
        .conn
        .execute(
            "INSERT INTO multipart_part_segments \
             (bucket, key, upload_id, version_id, part_number, segment_index, size, \
              segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                bucket.as_str(),
                key.as_str(),
                upload_id.as_str(),
                PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                1_i64,
                0_i64,
                32_i64,
                99_i64,
                okh.as_slice(),
                1_i64,
                1_i64,
                1_i64,
                4_i64,
                2_i64,
            ],
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    store
        .conn
        .execute(
            "UPDATE multipart_part_segments SET ec_m = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4",
            params![3_i64, bucket.as_str(), key.as_str(), upload_id.as_str()],
        )
        .unwrap();

    let err = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap_err();
    assert_metadata_state_digest_mismatch(err);
}

#[test]
fn metadata_state_digest_covers_multipart_upload_and_part_state() {
    assert_metadata_state_digest_covers_mutation(
        |store| {
            let upload_id = UploadId::new("s".repeat(UPLOAD_ID_LEN)).unwrap();
            insert_digest_multipart_upload(store, &upload_id);
        },
        |store| {
            let upload_id = UploadId::new("s".repeat(UPLOAD_ID_LEN)).unwrap();
            store
                .conn
                .execute(
                    "UPDATE multipart_uploads SET state = ?1 WHERE upload_id = ?2",
                    params![UploadState::Aborting as u8, upload_id.as_str()],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
            insert_digest_multipart_upload(store, &upload_id);
        },
        |store| {
            let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
            store
                .conn
                .execute(
                    "UPDATE multipart_uploads SET metadata_blob = ?1 WHERE upload_id = ?2",
                    params![b"changed".as_slice(), upload_id.as_str()],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let upload_id = UploadId::new("p".repeat(UPLOAD_ID_LEN)).unwrap();
            let okh = [4_u8; 16];
            insert_digest_multipart_upload(store, &upload_id);
            store
                .conn
                .execute(
                    "INSERT INTO multipart_parts \
                     (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, part_okh, \
                      part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        upload_id.as_str(),
                        1_i64,
                        1_i64,
                        64_i64,
                        0x1234_i64,
                        b"etag".as_slice(),
                        EtagKind::Crc64 as u8,
                        okh.as_slice(),
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                        102_i64,
                        Option::<&[u8]>::None,
                    ],
                )
                .unwrap();
        },
        |store| {
            let upload_id = UploadId::new("p".repeat(UPLOAD_ID_LEN)).unwrap();
            store
                .conn
                .execute(
                    "UPDATE multipart_parts SET checksum = ?1 WHERE upload_id = ?2",
                    params![b"checksum".as_slice(), upload_id.as_str()],
                )
                .unwrap();
        },
    );
}

#[test]
fn metadata_state_digest_covers_completed_multipart_uploads() {
    assert_metadata_state_digest_covers_mutation(
        |store| {
            let owner = test_owner();
            let upload_id = UploadId::new("c".repeat(UPLOAD_ID_LEN)).unwrap();
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO completed_multipart_uploads \
                     (upload_id, bucket, key, completion_order, completed_at, \
                      owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        upload_id.as_str(),
                        bucket.as_str(),
                        key.as_str(),
                        1_i64,
                        103_i64,
                        owner.principal.as_str(),
                        owner.canonical_id.as_str(),
                        owner.principal.as_str(),
                        owner.canonical_id.as_str(),
                    ],
                )
                .unwrap();
        },
        |store| {
            let upload_id = UploadId::new("c".repeat(UPLOAD_ID_LEN)).unwrap();
            store
                .conn
                .execute(
                    "UPDATE completed_multipart_uploads SET completion_order = ?1 WHERE upload_id = ?2",
                    params![2_i64, upload_id.as_str()],
                )
                .unwrap();
        },
    );
}

#[test]
fn terminal_stream_upload_cleanup_record_excludes_allocator_floor() {
    let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
    let session = StreamUploadRecord {
        session_id: SessionId::try_from("ab".repeat(16)).unwrap(),
        bucket: trusted_bucket_name("cleanup-bucket"),
        key: trusted_object_key("cleanup-key"),
        target: StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        },
        state: StreamUploadState::InProgress,
        created_at: 123,
        encryption: ObjectEncryption::None,
        next_segment_vid: GenerationId::new(2).unwrap(),
        bucket_write_reservation: None,
    };
    let expected = TerminalStreamCleanupRecord::from(&session);
    let mut replica = session.clone();
    replica.next_segment_vid = GenerationId::MIN;
    assert!(PgStore::stream_upload_cleanup_records_match(
        &[replica.clone()],
        std::slice::from_ref(&expected),
    ));

    replica.state = StreamUploadState::Completing;
    assert!(!PgStore::stream_upload_cleanup_records_match(
        &[replica],
        &[expected],
    ));
}

#[test]
fn abort_multipart_command_accepts_lagging_stream_allocator_floor() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let upload_id = UploadId::new("v".repeat(UPLOAD_ID_LEN)).unwrap();
    insert_digest_multipart_upload(&store, &upload_id);
    let upload = store.get_multipart_upload(&upload_id).unwrap();
    let session = StreamUploadRecord {
        session_id: SessionId::try_from("ac".repeat(16)).unwrap(),
        bucket: upload.bucket.clone(),
        key: upload.key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
        state: StreamUploadState::InProgress,
        created_at: 456,
        encryption: ObjectEncryption::None,
        next_segment_vid: GenerationId::MIN,
        bucket_write_reservation: None,
    };
    store
        .create_stream_upload_explicit(
            &StreamUploadCommandRecord::from(&session),
            session.next_segment_vid,
            None,
        )
        .unwrap();

    let bucket_write_reservation = BucketWriteReservationProof {
        bucket: upload.bucket.clone(),
        reservation_id: "abort-proof".to_string(),
        owner_token: "owner-token".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "abort-multipart-upload".to_string(),
        created_at: 456,
        lease_deadline: 789,
        target_context: Some(upload.key.as_str().to_string()),
    };
    let command = AbortMultipartUploadCommand {
        bucket: upload.bucket.clone(),
        key: upload.key.clone(),
        upload_id: upload_id.clone(),
        cleanup: AbortMultipartUploadCleanup {
            upload,
            parts: Vec::new(),
            streaming_segments: Vec::new(),
            stream_uploads: vec![TerminalStreamCleanupRecord::from(&session)],
            stream_upload_segments: Vec::new(),
        },
        bucket_write_reservation,
    };
    store
        .apply_abort_multipart_upload_command(&command)
        .unwrap();
    assert!(matches!(
        store.get_stream_upload(&session.session_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        store.get_multipart_upload(&upload_id),
        Err(MetadataError::NoSuchUpload { .. })
    ));
}

#[test]
fn metadata_state_digest_covers_stream_upload_state() {
    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO stream_uploads \
                     (session_id, bucket, key, op_kind, upload_id, part_number, state, \
                      created_at, encryption_type, encryption_state) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        "session",
                        bucket.as_str(),
                        key.as_str(),
                        StreamUploadKind::PutObject as u8,
                        Option::<&str>::None,
                        Option::<i64>::None,
                        StreamUploadState::InProgress as u8,
                        104_i64,
                        ObjectEncryption::None.encryption_type() as u8,
                        Option::<&[u8]>::None,
                    ],
                )
                .unwrap();
        },
        |store| {
            store
                .conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, "session"],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            let okh = [5_u8; 16];
            store
                .conn
                .execute(
                    "INSERT INTO stream_uploads \
                     (session_id, bucket, key, op_kind, upload_id, part_number, state, \
                      created_at, encryption_type, encryption_state) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        "seg-session",
                        bucket.as_str(),
                        key.as_str(),
                        StreamUploadKind::PutObject as u8,
                        Option::<&str>::None,
                        Option::<i64>::None,
                        StreamUploadState::InProgress as u8,
                        105_i64,
                        ObjectEncryption::None.encryption_type() as u8,
                        Option::<&[u8]>::None,
                    ],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                     (session_id, segment_index, size, segment_crc64, payload_crc64, segment_okh, \
                      segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        "seg-session",
                        0_i64,
                        64_i64,
                        99_i64,
                        99_i64,
                        okh.as_slice(),
                        1_i64,
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                    ],
                )
                .unwrap();
        },
        |store| {
            store
                .conn
                .execute(
                    "UPDATE stream_upload_segments SET data_pg_id = ?1 WHERE session_id = ?2",
                    params![2_i64, "seg-session"],
                )
                .unwrap();
        },
    );
}

#[test]
fn metadata_state_digest_covers_reclaim_and_reservation_state() {
    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO object_generation_reservations \
                     (reservation_id, bucket, key, generation_id, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params!["reservation", bucket.as_str(), key.as_str(), 1_i64, 106_i64],
                )
                .unwrap();
        },
        |store| {
            store
                .conn
                .execute(
                    "UPDATE object_generation_reservations SET created_at = ?1 WHERE reservation_id = ?2",
                    params![107_i64, "reservation"],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            let okh = [6_u8; 16];
            store
                .conn
                .execute(
                    "INSERT INTO object_segments_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket.as_str(), key.as_str(), 1_i64, 108_i64],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO object_segment_reclaim_segments \
                     (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                      data_pg_id, ec_k, ec_m) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        bucket.as_str(),
                        key.as_str(),
                        1_i64,
                        0_i64,
                        okh.as_slice(),
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                    ],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE object_segment_reclaim_segments SET ec_m = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                    params![3_i64, bucket.as_str(), key.as_str(), 1_i64],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO object_segments_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket.as_str(), key.as_str(), 2_i64, 110_i64],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE object_segments_reclaims SET created_at = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                    params![111_i64, bucket.as_str(), key.as_str(), 2_i64],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            let okh = [7_u8; 16];
            let segment_okh = [8_u8; 16];
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket.as_str(), key.as_str(), 1_i64, 109_i64],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaim_parts \
                     (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                      part_vid, data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        bucket.as_str(),
                        key.as_str(),
                        1_i64,
                        1_i64,
                        0_i64,
                        okh.as_slice(),
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                    ],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaim_part_segments \
                     (bucket, key, generation_id, part_number, segment_index, segment_okh, \
                      segment_vid, data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        bucket.as_str(),
                        key.as_str(),
                        1_i64,
                        1_i64,
                        0_i64,
                        segment_okh.as_slice(),
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                    ],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE multipart_reclaim_part_segments SET data_pg_id = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                    params![2_i64, bucket.as_str(), key.as_str(), 1_i64],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            let okh = [9_u8; 16];
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket.as_str(), key.as_str(), 2_i64, 112_i64],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaim_parts \
                     (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                      part_vid, data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        bucket.as_str(),
                        key.as_str(),
                        2_i64,
                        1_i64,
                        0_i64,
                        okh.as_slice(),
                        1_i64,
                        1_i64,
                        4_i64,
                        2_i64,
                    ],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE multipart_reclaim_parts SET ec_m = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                    params![3_i64, bucket.as_str(), key.as_str(), 2_i64],
                )
                .unwrap();
        },
    );

    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket.as_str(), key.as_str(), 3_i64, 113_i64],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE multipart_reclaims SET created_at = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                    params![114_i64, bucket.as_str(), key.as_str(), 3_i64],
                )
                .unwrap();
        },
    );
}

#[test]
fn metadata_state_digest_covers_object_version_counters() {
    assert_metadata_state_digest_covers_mutation(
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "INSERT INTO object_version_counters \
                     (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                    params![bucket.as_str(), key.as_str(), 2_i64],
                )
                .unwrap();
        },
        |store| {
            let bucket = trusted_bucket_name("digest-bucket");
            let key = trusted_object_key("object");
            store
                .conn
                .execute(
                    "UPDATE object_version_counters SET next_version_id = ?1 \
                     WHERE bucket = ?2 AND key = ?3",
                    params![3_i64, bucket.as_str(), key.as_str()],
                )
                .unwrap();
        },
    );
}

#[test]
fn metadata_state_digest_covers_pg_counters() {
    assert_metadata_state_digest_covers_mutation(
        |_| {},
        |store| {
            store
                .conn
                .execute(
                    "UPDATE pg_counters SET next_bucket_execution_generation = ?1 \
                     WHERE singleton = 0",
                    params![3_i64],
                )
                .unwrap();
        },
    );
}

#[test]
fn durable_bucket_write_coordination_does_not_dirty_metadata_command_state() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("coordination-bucket");
    create_probe_bucket_direct(&store, &bucket);
    store.refresh_metadata_command_state_digest().unwrap();

    let initial_digest = store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap()
        .state_digest;

    let reservation = store
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "reservation-1",
            "owner-token-1",
            ClusterEpoch::INITIAL,
            "put-object",
            1,
            2,
            Some("key=a"),
        )
        .unwrap();
    assert_eq!(
        store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap()
            .state_digest,
        initial_digest
    );

    store
        .release_durable_bucket_write_reservation(
            &bucket,
            "reservation-1",
            "owner-token-1",
            ClusterEpoch::INITIAL,
            reservation.bucket_execution_generation,
            reservation.bucket_incarnation_generation,
            reservation.lease_deadline,
        )
        .unwrap();
    assert_eq!(
        store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap()
            .state_digest,
        initial_digest
    );

    let drain = store
        .begin_durable_bucket_write_drain(
            &bucket,
            "drain-1",
            "owner-token-1",
            ClusterEpoch::INITIAL,
            3,
            Some(4),
        )
        .unwrap();
    assert_eq!(
        store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap()
            .state_digest,
        initial_digest
    );

    store
        .clear_durable_bucket_write_drain(
            &bucket,
            "drain-1",
            "owner-token-1",
            ClusterEpoch::INITIAL,
            drain.bucket_execution_generation,
        )
        .unwrap();
    assert_eq!(
        store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap()
            .state_digest,
        initial_digest
    );
}

#[test]
fn bucket_execution_generation_candidate_advances_only_on_command_apply() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");

    let first = store.next_bucket_execution_generation_candidate().unwrap();
    assert_eq!(first, 1);
    assert_eq!(
        store.next_bucket_execution_generation_candidate().unwrap(),
        first
    );

    let command = create_bucket_probe_command(1, 1, bucket, first);
    store.apply_metadata_command(&command).unwrap();

    assert_eq!(
        store.next_bucket_execution_generation_candidate().unwrap(),
        first + 1
    );
}

#[test]
fn reserve_object_version_command_advances_counter_exactly() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("object");

    let first = PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap();
    assert_eq!(first, VersionId::from_u64(1));

    let reserve_first = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            first,
        )),
    );
    store.apply_metadata_command(&reserve_first).unwrap();
    assert_eq!(
        PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
        VersionId::from_u64(2)
    );

    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            first,
        )),
    );
    let err = store.apply_metadata_command(&stale).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::ObjectVersionReservationConflict { version_id }
                if version_id == first
        ),
        "expected stale version reservation rejection, got {err:?}"
    );

    let second = PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap();
    let reserve_second = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            second,
        )),
    );
    store.apply_metadata_command(&reserve_second).unwrap();
    assert_eq!(
        PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
        VersionId::from_u64(3)
    );

    let null_reservation = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(4).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket,
            key,
            VersionId::Null,
        )),
    );
    let err = store.apply_metadata_command(&null_reservation).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::Db {
                context: "reserve object version command null version",
                ..
            }
        ),
        "expected null version reservation rejection, got {err:?}"
    );
}

#[test]
fn apply_metadata_command_and_record_rechecks_already_applied_before_mutation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("already-applied-version-bucket");
    let key = trusted_object_key("object");
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            VersionId::from_u64(1),
        )),
    );

    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    assert_eq!(
        PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
        VersionId::from_u64(2)
    );
    let state = store.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 1);
}

#[test]
fn stale_delete_marker_delete_command_cannot_delete_newer_null_marker() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("stale-delete-marker-delete");
    let key = trusted_object_key("object");
    let owner = test_owner();

    let first_marker = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: test_bucket_write_reservation_proof(
                &bucket,
                &key,
                "first-delete-marker",
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: owner.clone(),
            write_sequence: 1,
            last_modified_millis: 10,
            stale_payload: None,
        }),
    );
    store.apply_metadata_command(&first_marker).unwrap();

    let stale_delete = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: test_bucket_write_reservation_proof(
                &bucket,
                &key,
                "stale-delete-marker-delete",
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 },
        })),
    );

    let replacement_reservation_id = SessionId::try_from("75".repeat(16)).unwrap();
    let replacement_live = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: owner.clone(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: replacement_reservation_id.clone(),
            write_sequence: 2,
            last_modified_millis: 20,
            stale_payload: None,
            bucket_write_reservation: test_bucket_write_reservation_proof(
                &bucket,
                &key,
                "replacement-live",
            ),
            stream_create_bucket_write_reservation: None,
        })),
    );
    insert_direct_put_terminal_staging(&store, &bucket, &key, &replacement_reservation_id);
    store.apply_metadata_command(&replacement_live).unwrap();

    let newer_marker = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(4).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: test_bucket_write_reservation_proof(
                &bucket,
                &key,
                "newer-delete-marker",
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: owner.clone(),
            write_sequence: 3,
            last_modified_millis: 30,
            stale_payload: None,
        }),
    );
    store.apply_metadata_command(&newer_marker).unwrap();

    let stale_replay = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(5).unwrap(),
        ),
        stale_delete.payload().clone(),
    );
    let err = store.apply_metadata_command(&stale_replay).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleObjectWriteCommand {
                ref bucket,
                ref key,
                write_sequence: 1,
                generation_id: None,
            } if bucket.as_str() == "stale-delete-marker-delete"
                && key.as_str() == "object"
        ),
        "expected stale delete-marker target rejection, got {err:?}"
    );
    assert_eq!(
        store
            .object_write_sequence(bucket.as_str(), key.as_str(), VersionId::Null)
            .unwrap(),
        Some(3)
    );
    let stored = store
        .get_object_version(&bucket, &key, VersionId::Null)
        .unwrap();
    let StoredObject::DeleteMarker(marker) = stored else {
        panic!("expected newer delete marker to remain, got {stored:?}");
    };
    assert_eq!(marker.last_modified, 30);
}

#[test]
fn already_applied_direct_put_command_cleans_terminal_staging_on_apply() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("direct-put-terminal-cleanup");
    let key = trusted_object_key("object");
    let reservation_id = SessionId::try_from("73".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: reservation_id.clone(),
            write_sequence: 1,
            last_modified_millis: 2,
            stale_payload: None,
            bucket_write_reservation: BucketWriteReservationProof {
                bucket: bucket.clone(),
                reservation_id: "direct-put-proof".to_string(),
                owner_token: "direct-put-proof-owner".to_string(),
                cluster_epoch: ClusterEpoch::INITIAL,
                bucket_execution_generation: 1,
                bucket_incarnation_generation: 1,
                operation_kind: "direct-put-commit".to_string(),
                created_at: 1,
                lease_deadline: 2,
                target_context: Some(key.as_str().to_string()),
            },
            stream_create_bucket_write_reservation: None,
        })),
    );

    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.apply_metadata_command(&command).unwrap();

    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.apply_metadata_command(&command).unwrap();

    assert!(matches!(
        store.get_stream_upload(&reservation_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        store.get_object_generation_reservation(&bucket, &key, &reservation_id),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
}

#[test]
fn direct_put_already_applied_requires_matching_write_sequence() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("direct-put-stale-write-sequence");
    let key = trusted_object_key("object");
    let reservation_id = SessionId::try_from("76".repeat(16)).unwrap();

    let first =
        direct_put_terminal_cleanup_command_with_write_sequence(&bucket, &key, &reservation_id, 1);
    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.apply_metadata_command(&first).unwrap();

    let newer =
        direct_put_terminal_cleanup_command_with_write_sequence(&bucket, &key, &reservation_id, 2);
    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.apply_metadata_command(&newer).unwrap();

    let stale_same_image =
        direct_put_terminal_cleanup_command_with_write_sequence(&bucket, &key, &reservation_id, 1);
    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    let err = store.apply_metadata_command(&stale_same_image).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleObjectWriteCommand {
                ref bucket,
                ref key,
                write_sequence: 1,
                generation_id: Some(1),
            } if bucket.as_str() == "direct-put-stale-write-sequence"
                && key.as_str() == "object"
        ),
        "expected stale write-sequence rejection, got {err:?}"
    );
    assert_eq!(
        store
            .object_write_sequence(bucket.as_str(), key.as_str(), VersionId::Null)
            .unwrap(),
        Some(2)
    );
}

#[test]
fn already_recorded_direct_put_command_cleans_terminal_staging_and_digest() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("direct-put-record-cleanup");
    let key = trusted_object_key("object");
    let reservation_id = SessionId::try_from("74".repeat(16)).unwrap();
    let command = direct_put_terminal_cleanup_command(&bucket, &key, &reservation_id);

    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.refresh_metadata_command_state_digest().unwrap();
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    insert_direct_put_terminal_staging(&store, &bucket, &key, &reservation_id);
    store.refresh_metadata_command_state_digest().unwrap();
    store
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    assert!(matches!(
        store.get_stream_upload(&reservation_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        store.get_object_generation_reservation(&bucket, &key, &reservation_id),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    store
        .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
        .unwrap();
}

#[test]
fn object_generation_reservation_conflict_filter_only_accepts_uniqueness_constraints() {
    fn sqlite_failure(code: rusqlite::ffi::ErrorCode, extended_code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code,
                extended_code,
            },
            None,
        )
    }

    assert_eq!(
        PgStore::object_generation_reservation_constraint_kind(&sqlite_failure(
            rusqlite::ffi::ErrorCode::ConstraintViolation,
            rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
        )),
        Some(ObjectGenerationReservationConstraint::ReservationId)
    );
    assert_eq!(
        PgStore::object_generation_reservation_constraint_kind(&sqlite_failure(
            rusqlite::ffi::ErrorCode::ConstraintViolation,
            rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE,
        )),
        Some(ObjectGenerationReservationConstraint::Generation)
    );
    assert_eq!(
        PgStore::object_generation_reservation_constraint_kind(&sqlite_failure(
            rusqlite::ffi::ErrorCode::ConstraintViolation,
            rusqlite::ffi::SQLITE_CONSTRAINT_CHECK,
        )),
        None
    );
    assert_eq!(
        PgStore::object_generation_reservation_constraint_kind(&sqlite_failure(
            rusqlite::ffi::ErrorCode::DatabaseBusy,
            rusqlite::ffi::SQLITE_BUSY
        )),
        None
    );
}

#[test]
fn reserve_object_generation_explicit_primary_key_conflict_must_match_identity() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let reservation_id = SessionId::try_from("85".repeat(16)).unwrap();
    let existing_bucket = trusted_bucket_name("existing-reservation-bucket");
    let existing_key = trusted_object_key("existing-key");
    let requested_bucket = trusted_bucket_name("requested-reservation-bucket");
    let requested_key = trusted_object_key("requested-key");
    store
        .conn
        .execute(
            "INSERT INTO object_generation_reservations \
             (reservation_id, bucket, key, generation_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                reservation_id.as_str(),
                existing_bucket.as_str(),
                existing_key.as_str(),
                1_i64,
                1_i64,
            ],
        )
        .unwrap();

    let err = store
        .reserve_object_generation_explicit(
            &requested_bucket,
            &requested_key,
            &reservation_id,
            GenerationId::new(1).unwrap(),
            2,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        MetadataError::Db {
            context: "reserve object generation explicit",
            ..
        }
    ));
}

fn direct_put_terminal_cleanup_command(
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
) -> MetadataCommandEnvelope {
    direct_put_terminal_cleanup_command_with_write_sequence(bucket, key, reservation_id, 1)
}

fn direct_put_terminal_cleanup_command_with_write_sequence(
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
    write_sequence: u64,
) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: reservation_id.clone(),
            write_sequence,
            last_modified_millis: 2,
            stale_payload: None,
            bucket_write_reservation: BucketWriteReservationProof {
                bucket: bucket.clone(),
                reservation_id: "direct-put-proof".to_string(),
                owner_token: "direct-put-proof-owner".to_string(),
                cluster_epoch: ClusterEpoch::INITIAL,
                bucket_execution_generation: 1,
                bucket_incarnation_generation: 1,
                operation_kind: "direct-put-commit".to_string(),
                created_at: 1,
                lease_deadline: 2,
                target_context: Some(key.as_str().to_string()),
            },
            stream_create_bucket_write_reservation: None,
        })),
    )
}

fn insert_direct_put_terminal_staging(
    store: &PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &SessionId,
) {
    store
        .conn
        .execute(
            "INSERT INTO object_generation_reservations \
             (reservation_id, bucket, key, generation_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                reservation_id.as_str(),
                bucket.as_str(),
                key.as_str(),
                GenerationId::MIN.get() as i64,
                1_i64,
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO stream_uploads \
             (session_id, bucket, key, op_kind, upload_id, part_number, state, \
              created_at, encryption_type, encryption_state) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                reservation_id.as_str(),
                bucket.as_str(),
                key.as_str(),
                StreamUploadKind::PutObject as u8,
                Option::<&str>::None,
                Option::<i64>::None,
                StreamUploadState::InProgress as u8,
                1_i64,
                ObjectEncryption::None.encryption_type() as u8,
                Option::<&[u8]>::None,
            ],
        )
        .unwrap();
}

#[test]
fn reserve_object_version_command_advances_lower_counter_forward() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("object");

    let forward = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            VersionId::from_u64(3),
        )),
    );
    store.apply_metadata_command(&forward).unwrap();
    assert_eq!(
        PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
        VersionId::from_u64(4)
    );
}

#[test]
fn put_bucket_versioning_command_does_not_lower_execution_generation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let owner = test_owner();
    let acl_grants = AclGrants::default();
    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    let newer = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(12),
            BucketVersioningState::Enabled,
        )),
    );
    store.apply_metadata_command(&newer).unwrap();

    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(11),
            BucketVersioningState::Enabled,
        )),
    );
    let err = store.apply_metadata_command(&stale).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleBucketMetadataCommand {
                ref name,
                bucket_execution_generation: 11,
            } if name == &bucket
        ),
        "expected stale command rejection, got {err:?}"
    );

    let conflicting = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(12),
            BucketVersioningState::Suspended,
        )),
    );
    let err = store.apply_metadata_command(&conflicting).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::Db {
                context: "apply conflicting bucket versioning command",
                ..
            }
        ),
        "expected conflicting command rejection, got {err:?}"
    );

    let info = store.head_bucket_raw(&bucket).unwrap();
    assert_eq!(info.versioning, BucketVersioningState::Enabled);
    assert_eq!(info.bucket_execution_generation, 12);
}

#[test]
fn put_bucket_acl_command_does_not_lower_execution_generation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let owner = test_owner();
    let acl_grants = AclGrants::default();
    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    let newer = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(12),
            acl_grants.clone(),
            true,
            false,
        )),
    );
    store.apply_metadata_command(&newer).unwrap();

    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(11),
            acl_grants.clone(),
            true,
            false,
        )),
    );
    let err = store.apply_metadata_command(&stale).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleBucketMetadataCommand {
                ref name,
                bucket_execution_generation: 11,
            } if name == &bucket
        ),
        "expected stale command rejection, got {err:?}"
    );

    let conflicting = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
            store
                .head_bucket_record_raw(&bucket)
                .unwrap()
                .with_execution_generation(12),
            acl_grants.clone(),
            false,
            true,
        )),
    );
    let err = store.apply_metadata_command(&conflicting).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::Db {
                context: "apply conflicting bucket acl command",
                ..
            }
        ),
        "expected conflicting command rejection, got {err:?}"
    );

    let info = store.head_bucket_raw(&bucket).unwrap();
    assert_eq!(info.acl_grants, acl_grants);
    assert!(info.public_read);
    assert!(!info.public_write);
    assert_eq!(info.bucket_execution_generation, 12);
}

#[test]
fn put_bucket_property_command_does_not_lower_execution_generation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let owner = test_owner();
    let acl_grants = AclGrants::default();
    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    let newer_config = BucketEncryptionConfig {
        default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
        sse_c_blocked: true,
    };
    let newer = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::PutBucketProperty(
            PutBucketPropertyCommand::from_bucket_and_mutation(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                BucketPropertyMutation::Encryption(newer_config),
            ),
        ),
    );
    store.apply_metadata_command(&newer).unwrap();

    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::PutBucketProperty(
            PutBucketPropertyCommand::from_bucket_and_mutation(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(11),
                BucketPropertyMutation::Encryption(newer_config),
            ),
        ),
    );
    let err = store.apply_metadata_command(&stale).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleBucketMetadataCommand {
                ref name,
                bucket_execution_generation: 11,
            } if name == &bucket
        ),
        "expected stale command rejection, got {err:?}"
    );

    let same_effective_but_different_stored_config = BucketEncryptionConfig {
        default_encryption: None,
        sse_c_blocked: true,
    };
    assert_eq!(
        newer_config.effective(),
        same_effective_but_different_stored_config.effective()
    );
    let conflicting = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::PutBucketProperty(
            PutBucketPropertyCommand::from_bucket_and_mutation(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                BucketPropertyMutation::Encryption(same_effective_but_different_stored_config),
            ),
        ),
    );
    let err = store.apply_metadata_command(&conflicting).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::Db {
                context: "apply conflicting bucket encryption command",
                ..
            }
        ),
        "expected conflicting command rejection, got {err:?}"
    );

    assert_eq!(
        PgMetadataStore::get_bucket_encryption(&store, &bucket).unwrap(),
        newer_config
    );
    let info = store.head_bucket_raw(&bucket).unwrap();
    assert_eq!(info.encryption, newer_config.effective());
    assert_eq!(info.bucket_execution_generation, 12);
}

#[test]
fn put_bucket_subresource_command_does_not_lower_execution_generation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let owner = test_owner();
    let acl_grants = AclGrants::default();
    store
        .create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    let policy_body = r#"{"Statement":[]}"#.to_owned();
    let mutation = BucketSubresourceMutation::Put {
        kind: BucketSubresourceKind::Policy,
        body: policy_body.clone(),
        aux: BucketSubresourceAux::policy(true),
    };
    let newer = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
            bucket.clone(),
            mutation.clone(),
            12,
        )),
    );
    store.apply_metadata_command(&newer).unwrap();

    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
            bucket.clone(),
            mutation.clone(),
            11,
        )),
    );
    let err = store.apply_metadata_command(&stale).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::StaleBucketMetadataCommand {
                ref name,
                bucket_execution_generation: 11,
            } if name == &bucket
        ),
        "expected stale command rejection, got {err:?}"
    );

    let conflicting = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
            bucket.clone(),
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Policy,
                body: r#"{"Statement":[{"Effect":"Deny"}]}"#.to_owned(),
                aux: BucketSubresourceAux::policy(false),
            },
            12,
        )),
    );
    let err = store.apply_metadata_command(&conflicting).unwrap_err();
    assert!(
        matches!(
            err,
            MetadataError::Db {
                context: "apply conflicting put bucket subresource command",
                ..
            }
        ),
        "expected conflicting command rejection, got {err:?}"
    );

    let stored =
        PgMetadataStore::get_bucket_subresource(&store, &bucket, BucketSubresourceKind::Policy)
            .unwrap()
            .unwrap();
    assert_eq!(stored.body, policy_body);
    assert_eq!(stored.generation, Some(1));
    assert_eq!(stored.aux, BucketSubresourceAux::policy(true));
    let info = store.head_bucket_raw(&bucket).unwrap();
    assert!(info.bucket_policy_present);
    assert!(info.bucket_policy_public);
    assert_eq!(info.bucket_policy_generation, 1);
    assert_eq!(info.bucket_execution_generation, 12);
}

// ── list_objects with prefix + start_after ────────────────────────

#[test]
fn list_objects_prefix_and_start_after() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();

    // Insert several objects
    for key in &["photos/a.jpg", "photos/b.jpg", "photos/c.jpg", "docs/x"] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: trusted_bucket_name("bucket"),
                key: trusted_object_key(*key),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 10,
                etag: ObjectEtag::SinglePart([0; 8]),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    // List with prefix=photos/ and start_after=photos/a.jpg
    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: trusted_bucket_name("bucket"),
            prefix: Some(trusted_object_key("photos/")),
            start_after: Some(trusted_object_key("photos/a.jpg")),
            start_at: None,
            max_keys: 10,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 2);
    assert_eq!(resp.objects[0].key(), "photos/b.jpg");
    assert_eq!(resp.objects[1].key(), "photos/c.jpg");
    assert!(!resp.is_truncated);
}

// ── stat_shard on quarantined shard ───────────────────────────────

#[test]
fn connection_accessor() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    // Just verify we can call it without panicking
    let _conn = store.connection();
}

#[test]
fn row_to_object_record_rejects_negative_parts_count() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let err = store
        .connection()
        .query_row(
            "SELECT \
                'bucket' AS bucket, \
                'k' AS key, \
                0 AS version_id, \
                1 AS generation_id, \
                1 AS size, \
                zeroblob(8) AS etag, \
                1 AS etag_kind, \
                0 AS last_modified, \
                0 AS storage_class, \
                4 AS ec_k, \
                2 AS ec_m, \
                0 AS status, \
                NULL AS tags, \
                1 AS data_layout, \
                -1 AS parts_count, \
                NULL AS metadata_blob, \
                NULL AS system_metadata_blob, \
                0 AS encryption_type, \
                NULL AS encryption_state, \
                'owner' AS owner_principal, \
                ?1 AS owner_canonical_id, \
                '' AS acl_grants, \
                0 AS public_read, \
                NULL AS object_lock_retention_mode, \
                NULL AS object_lock_retain_until, \
                0 AS object_lock_legal_hold, \
                NULL AS became_noncurrent_at",
            params![CanonicalUserId::from_principal("owner").as_str()],
            PgStore::row_to_object_record,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        rusqlite::Error::FromSqlConversionFailure(14, rusqlite::types::Type::Integer, _)
    ));
}

#[test]
fn row_to_object_record_rejects_pending_delete_status() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let err = store
        .connection()
        .query_row(
            "SELECT \
                'b' AS bucket, \
                'k' AS key, \
                0 AS version_id, \
                1 AS generation_id, \
                1 AS size, \
                zeroblob(8) AS etag, \
                1 AS etag_kind, \
                0 AS last_modified, \
                0 AS storage_class, \
                4 AS ec_k, \
                2 AS ec_m, \
                2 AS status, \
                NULL AS tags, \
                0 AS data_layout, \
                NULL AS parts_count, \
                NULL AS metadata_blob, \
                NULL AS system_metadata_blob, \
                0 AS encryption_type, \
                NULL AS encryption_state, \
                'owner' AS owner_principal, \
                ?1 AS owner_canonical_id, \
                '' AS acl_grants, \
                0 AS public_read, \
                NULL AS object_lock_retention_mode, \
                NULL AS object_lock_retain_until, \
                0 AS object_lock_legal_hold, \
                NULL AS became_noncurrent_at",
            params![CanonicalUserId::from_principal("owner").as_str()],
            PgStore::row_to_object_record,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Integer, _)
    ));
}

// ── list_objects pagination ──────────────────────────────────────

#[test]
fn list_objects_pagination() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();

    for i in 0..5 {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: trusted_bucket_name("bucket"),
                key: trusted_object_key(format!("key-{:02}", i)),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::SinglePart([0; 8]),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    // First page
    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: trusted_bucket_name("bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 2,
        })
        .unwrap();
    assert_eq!(resp.objects.len(), 2);
    assert!(resp.is_truncated);
    assert_eq!(resp.objects[0].key(), "key-00");
    assert_eq!(resp.objects[1].key(), "key-01");

    // Second page
    let resp2 = store
        .list_objects(&ListObjectsReq {
            bucket: trusted_bucket_name("bucket"),
            prefix: None,
            start_after: resp.next_start_after,
            start_at: None,
            max_keys: 2,
        })
        .unwrap();
    assert_eq!(resp2.objects.len(), 2);
    assert!(resp2.is_truncated);
    assert_eq!(resp2.objects[0].key(), "key-02");
}

#[test]
fn list_objects_start_at_is_inclusive() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();

    for key in ["alpha", "beta", "gamma"] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: trusted_bucket_name("bucket"),
                key: trusted_object_key(key),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag: ObjectEtag::SinglePart([0; 8]),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: trusted_bucket_name("bucket"),
            prefix: None,
            start_after: None,
            start_at: Some(trusted_object_key("beta")),
            max_keys: 10,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 2);
    assert_eq!(resp.objects[0].key(), "beta");
    assert_eq!(resp.objects[1].key(), "gamma");
}
