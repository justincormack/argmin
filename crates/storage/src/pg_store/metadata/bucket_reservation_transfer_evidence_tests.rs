// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Characterize CURRENT coordination durability, not the proposed cleanup fix.
//! Bucket reservations are primary-local and absent from replica checkpoints.
//! This does not require preserving sessions or replicating their reservations.
//! Cleanup ownership must survive independently before physical authorization;
//! these absence assertions alone are not acceptance tests for that contract.

use super::*;
use crate::metadata_command::{MetadataCommandPayload, ReserveObjectGenerationCommand};

struct Evidence {
    record: BucketWriteReservationRecord,
    generation: ReserveObjectGenerationCommand,
    checkpoint: crate::node_runtime::pg_store::MetadataCommandCheckpoint,
}

fn seed(store: &PgStore) -> Evidence {
    let bucket = trusted_bucket_name("bucket-discovery-evidence");
    let key = trusted_object_key("key");
    store
        .apply_metadata_command_and_record(0, &create_bucket_probe_command(0, 1, bucket.clone(), 1))
        .unwrap();
    // Replicated positive canary: distinguish the current object-generation
    // reservation from the separate, primary-local bucket-write reservation.
    // This is metadata-only and does not fabricate payload or staging authority.
    let generation = ReserveObjectGenerationCommand::new(
        bucket.clone(),
        key.clone(),
        crate::tests::stream_session_id("discovery"),
        store.next_generation_id(&bucket, &key).unwrap(),
        10,
    );
    store
        .apply_metadata_command_and_record(
            0,
            &MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    PgId::new(0),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                MetadataCommandPayload::ReserveObjectGeneration(generation.clone()),
            ),
        )
        .unwrap();
    let checkpoint = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let record = store
        .acquire_durable_bucket_write_reservation(
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "bucket-proof",
                owner_token: "frontend",
                cluster_epoch: ClusterEpoch::INITIAL,
                operation_kind:
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                created_at: 10,
                lease_deadline: 60_000,
                target_context: Some(key.as_str()),
            },
        )
        .unwrap();
    assert_eq!(
        store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap(),
        checkpoint,
        "primary-local acquisition is deliberately outside checkpoint state"
    );
    assert!(!checkpoint
        .table_blocks
        .iter()
        .any(|table| table.table_name == "bucket_write_reservations"));
    Evidence {
        record,
        generation,
        checkpoint,
    }
}

fn assert_bucket_record(store: &PgStore, evidence: &Evidence) {
    assert!(evidence.record.lease_deadline > crate::clock::current_time_millis());
    assert_eq!(
        store
            .durable_bucket_write_reservation(
                &evidence.record.bucket,
                &evidence.record.reservation_id,
            )
            .unwrap(),
        Some(evidence.record.clone())
    );
}

fn assert_replicated_state(store: &PgStore, evidence: &Evidence, epoch: ClusterEpoch) {
    let checkpoint = store.metadata_command_checkpoint(0, epoch).unwrap();
    assert_eq!(checkpoint.table_blocks, evidence.checkpoint.table_blocks);
    assert_eq!(checkpoint.table_digests, evidence.checkpoint.table_digests);
    assert_eq!(checkpoint.state_digest, evidence.checkpoint.state_digest);
    assert_eq!(
        store
            .get_object_generation_reservation(
                &evidence.generation.bucket,
                &evidence.generation.key,
                &evidence.generation.reservation_id,
            )
            .unwrap(),
        evidence.generation.generation_id
    );
    assert_eq!(
        store
            .head_bucket_raw(&evidence.record.bucket)
            .unwrap()
            .bucket_incarnation_generation,
        evidence.record.bucket_incarnation_generation
    );
}

#[test]
fn bucket_reservation_discovery_survives_local_reopen_but_is_not_transferred() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let source_dir = test_util::tempdir();
    let source = PgStore::open(source_dir.path(), 0).unwrap();
    let evidence = seed(&source);
    assert_bucket_record(&source, &evidence);
    drop(source);
    let source = PgStore::open(source_dir.path(), 0).unwrap();
    assert_bucket_record(&source, &evidence);
    assert_replicated_state(&source, &evidence, ClusterEpoch::INITIAL);

    let destination_dir = test_util::tempdir();
    let destination = PgStore::open(destination_dir.path(), 0).unwrap();
    let epoch = ClusterEpoch::new(2).unwrap();
    let source_changes = source.conn.total_changes();
    destination
        .install_metadata_transfer_checkpoint_base(1, epoch, &evidence.checkpoint)
        .unwrap();
    drop(destination);
    let destination = PgStore::open(destination_dir.path(), 0).unwrap();
    assert_replicated_state(&destination, &evidence, epoch);
    assert!(
        destination
            .durable_bucket_write_reservations(&evidence.record.bucket)
            .unwrap()
            .is_empty(),
        "an imported empty coordination table is NOT proof of no old-primary reservation"
    );
    assert_eq!(source.conn.total_changes(), source_changes);
    assert_bucket_record(&source, &evidence);
    // Idempotent proof release on the empty candidate cannot certify release at
    // the old owner. These are store primitives, not route-authorized RPCs.
    let proof = BucketWriteReservationProof::from(&evidence.record);
    let destination_changes = destination.conn.total_changes();
    destination
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap();
    assert_eq!(destination.conn.total_changes(), destination_changes);
    assert_bucket_record(&source, &evidence);
    source
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap();
    assert!(source
        .durable_bucket_write_reservations(&evidence.record.bucket)
        .unwrap()
        .is_empty());
    assert_replicated_state(&source, &evidence, ClusterEpoch::INITIAL);
}

#[test]
fn bucket_reservation_discovery_is_lost_when_local_checkpoint_replaces_bucket_rows() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 0).unwrap();
    let evidence = seed(&store);
    drop(store);
    let store = PgStore::open(dir.path(), 0).unwrap();
    assert_bucket_record(&store, &evidence);
    let epoch = ClusterEpoch::new(2).unwrap();
    // Even an otherwise identical materialized state clears/reinserts buckets.
    // The current bucket-row FK cascades to local coordination rows, which are
    // absent from the checkpoint and therefore are not restored by installation.
    store
        .install_metadata_transfer_checkpoint_base(0, epoch, &evidence.checkpoint)
        .unwrap();
    assert_replicated_state(&store, &evidence, epoch);
    assert!(store
        .durable_bucket_write_reservations(&evidence.record.bucket)
        .unwrap()
        .is_empty());
    drop(store);
    let store = PgStore::open(dir.path(), 0).unwrap();
    assert_replicated_state(&store, &evidence, epoch);
    assert!(store
        .durable_bucket_write_reservations(&evidence.record.bucket)
        .unwrap()
        .is_empty());
}

#[test]
fn bucket_reservation_discovery_is_restored_when_checkpoint_replacement_rolls_back() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 0).unwrap();
    let evidence = seed(&store);
    let prefix = store.metadata_command_replica_state().unwrap();
    // Fail during insertion, AFTER checkpoint installation has cleared the
    // existing bucket and cascaded its local reservation. Rollback must restore
    // that local evidence as well as the command-owned tables and prefix.
    store
        .conn
        .execute_batch(
            "CREATE TEMP TRIGGER fail_checkpoint_bucket_insert BEFORE INSERT ON buckets \
         WHEN NOT EXISTS (SELECT 1 FROM bucket_write_reservations) \
         BEGIN SELECT RAISE(ABORT, 'checkpoint bucket insert sentinel'); END;",
        )
        .unwrap();
    let error = store
        .install_metadata_transfer_checkpoint_base(
            0,
            ClusterEpoch::new(2).unwrap(),
            &evidence.checkpoint,
        )
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("checkpoint bucket insert sentinel"),
        "{error:?}"
    );
    assert_eq!(store.metadata_command_replica_state().unwrap(), prefix);
    assert_bucket_record(&store, &evidence);
    assert_replicated_state(&store, &evidence, ClusterEpoch::INITIAL);
    drop(store);
    let store = PgStore::open(dir.path(), 0).unwrap();
    assert_eq!(store.metadata_command_replica_state().unwrap(), prefix);
    assert_bucket_record(&store, &evidence);
    assert_replicated_state(&store, &evidence, ClusterEpoch::INITIAL);
}
