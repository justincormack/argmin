// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Evidence from production store/peering primitives for Direct PUT introduction.
//! No shard writes, retained-owner tables or control-plane route authorization are
//! simulated. A generation row is NOT an owner record or bucket-proof obligation.

use super::*;
use crate::metadata_command::{MetadataCommandPayload, ReserveObjectGenerationCommand};
use crate::peering::{
    reconstruct_pg_peering_from_primary_retained_log, PgPeeringReconstructionDecision,
    PgPeeringReconstructionError, PgPeeringReplicaReconstructionInput,
};
use placement::NodeId;

fn fixture(store: &PgStore) -> (ReserveObjectGenerationCommand, MetadataCommandEnvelope) {
    let bucket = trusted_bucket_name("direct-introduction");
    let key = trusted_object_key("object");
    create_probe_bucket_direct(store, &bucket);
    let generation = store.next_generation_id(&bucket, &key).unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    let reserve = ReserveObjectGenerationCommand::new(
        bucket,
        key,
        crate::tests::stream_session_id("direct-intro"),
        generation,
        10,
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(reserve.clone()),
    );
    (reserve, command)
}

fn assert_reserved(store: &PgStore, reserve: &ReserveObjectGenerationCommand) {
    assert_eq!(
        store
            .get_object_generation_reservation(
                &reserve.bucket,
                &reserve.key,
                &reserve.reservation_id
            )
            .unwrap(),
        reserve.generation_id,
    );
    assert_eq!(
        store
            .next_generation_id(&reserve.bucket, &reserve.key)
            .unwrap()
            .get(),
        reserve.generation_id.get() + 1
    );
}

#[test]
fn direct_put_reservation_reissue_preserves_ownership_but_cannot_resurrect_release() {
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 1).unwrap();
    let (reserve, original) = fixture(&store);
    let applied = store
        .apply_metadata_command_and_record(0, &original)
        .unwrap();
    assert_eq!(
        store
            .apply_metadata_command_and_record(0, &original)
            .unwrap(),
        applied,
        "exact log replay must remain idempotent"
    );
    let later = create_bucket_probe_command(1, 2, trusted_bucket_name("later-prefix"), 20);
    store.apply_metadata_command_and_record(0, &later).unwrap();
    let before = store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    store
        .compact_metadata_command_log_with_receipt_retention(ClusterEpoch::INITIAL, 1)
        .unwrap();
    assert_eq!(
        store
            .applied_metadata_command_log_entry_hashes(0, &original)
            .unwrap(),
        None
    );
    drop(store);
    let store = PgStore::open(dir.path(), 1).unwrap();
    let decode_at = |index, payload| {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            payload,
        );
        let decoded = decode_metadata_command_envelope(
            &command.command_bytes(),
            &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
        )
        .unwrap();
        assert_eq!(decoded, command);
        decoded
    };
    let reissued = decode_at(3, original.payload().clone());
    assert!(reissued
        .payload()
        .is_ordinary_pending_slot_reissue_of(original.payload()));
    // Exercise payload application, not the original command's AlreadyApplied
    // shortcut. The exact materialized reservation must support same-payload
    // reissue even when its original log/receipt has been compacted away.
    assert_eq!(
        store
            .apply_metadata_command_and_record(0, &reissued)
            .unwrap()
            .applied_log_index,
        3
    );
    assert_reserved(&store, &reserve);
    assert_eq!(
        store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap()
            .table_blocks,
        before.table_blocks
    );
    let release = decode_at(
        4,
        MetadataCommandPayload::ReleaseObjectGeneration(
            crate::metadata_command::ReleaseObjectGenerationCommand::new(
                reserve.bucket.clone(),
                reserve.key.clone(),
                reserve.reservation_id.clone(),
            ),
        ),
    );
    store
        .apply_metadata_command_and_record(0, &release)
        .unwrap();
    let released = store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    drop(store);
    let store = PgStore::open(dir.path(), 1).unwrap();
    let changes = store.conn.total_changes();
    let error = store
        .apply_metadata_command_and_record(0, &decode_at(5, original.payload().clone()))
        .unwrap_err();
    assert!(
        matches!(
            error,
            BucketSnapshotLoadError::Metadata(
                MetadataError::ObjectGenerationReservationConflict { .. }
            )
        ),
        "{error:?}"
    );
    assert_eq!(store.conn.total_changes(), changes);
    assert_eq!(
        store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap(),
        released
    );
    assert!(matches!(
        store.get_object_generation_reservation_by_id(&reserve.reservation_id),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
}

fn replica(store: &PgStore, node: u32) -> PgPeeringReplicaReconstructionInput {
    PgPeeringReplicaReconstructionInput {
        node_id: NodeId::new(node),
        state: store.metadata_command_replica_state().unwrap(),
        has_pending_metadata_command: store
            .pending_metadata_command_slot(node, ClusterEpoch::INITIAL)
            .unwrap()
            .is_some(),
        // The tested decisions reject pending/ahead replicas or compare equal
        // prefixes. They do not construct a retained-log catch-up plan.
        retained_log_hashes: Vec::new(),
    }
}

#[test]
fn direct_put_introduction_evidence_successor_cannot_use_local_absence() {
    let primary_dir = test_util::tempdir();
    let witness_dir = test_util::tempdir();
    let successor_dir = test_util::tempdir();
    let primary = PgStore::open(primary_dir.path(), 1).unwrap();
    let witness = PgStore::open(witness_dir.path(), 1).unwrap();
    let successor = PgStore::open(successor_dir.path(), 1).unwrap();
    let (reserve, command) = fixture(&primary);
    let base = primary
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    for (node, store) in [(1, &witness), (2, &successor)] {
        store
            .install_metadata_transfer_checkpoint_base(node, ClusterEpoch::INITIAL, &base)
            .unwrap();
    }
    primary
        .try_insert_pending_metadata_command_slot(0, &command, Some(&reserve.bucket))
        .unwrap();
    let mut pending = primary
        .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        pending.as_ref().unwrap().command_bytes,
        command.command_bytes()
    );
    drop(primary);
    let primary = PgStore::open(primary_dir.path(), 1).unwrap();
    assert_eq!(
        primary
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap(),
        pending
    );
    assert!(!primary
        .pending_metadata_command_publication_started(0, &command)
        .unwrap());

    for stage in 0..3 {
        if stage == 1 {
            primary
                .mark_pending_metadata_command_publication_started(0, &command)
                .unwrap();
            pending.as_mut().unwrap().publication_started = true;
        } else if stage == 2 {
            witness
                .apply_metadata_command_and_record(1, &command)
                .unwrap();
            assert_reserved(&witness, &reserve);
        }
        for store in [&primary, &successor] {
            assert!(matches!(
                store.get_object_generation_reservation(
                    &reserve.bucket,
                    &reserve.key,
                    &reserve.reservation_id
                ),
                Err(MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
            assert_eq!(
                store
                    .next_generation_id(&reserve.bucket, &reserve.key)
                    .unwrap(),
                reserve.generation_id
            );
        }
        // The candidate has an empty local slot and a valid local checkpoint at
        // every stage. Neither observation settles another replica's pending work.
        successor
            .metadata_command_checkpoint(2, ClusterEpoch::INITIAL)
            .unwrap()
            .verify()
            .unwrap();
        let before = [
            replica(&primary, 0),
            replica(&witness, 1),
            replica(&successor, 2),
        ];
        let changes = [
            primary.conn.total_changes(),
            witness.conn.total_changes(),
            successor.conn.total_changes(),
        ];
        assert_eq!(
            reconstruct_pg_peering_from_primary_retained_log(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                NodeId::new(2),
                &before
            )
            .unwrap_err(),
            PgPeeringReconstructionError::PendingMetadataCommand {
                node_id: NodeId::new(0)
            },
        );
        assert!(matches!(
            primary.metadata_command_checkpoint(0, ClusterEpoch::INITIAL),
            Err(StoreError::MetadataCommandContention {
                context: "export metadata command checkpoint with pending command"
            })
        ));
        assert_eq!(
            [
                replica(&primary, 0),
                replica(&witness, 1),
                replica(&successor, 2)
            ],
            before
        );
        assert_eq!(
            [
                primary.conn.total_changes(),
                witness.conn.total_changes(),
                successor.conn.total_changes()
            ],
            changes
        );
        assert_eq!(
            primary
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap(),
            pending
        );
        assert_eq!(
            primary
                .pending_metadata_command_publication_started(0, &command)
                .unwrap(),
            stage != 0
        );
    }

    // Even without the old primary in this inspection input, the witness's
    // applied prefix prevents choosing the empty candidate as authoritative.
    assert_eq!(
        reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            NodeId::new(2),
            &[replica(&witness, 1), replica(&successor, 2)]
        )
        .unwrap_err(),
        PgPeeringReconstructionError::ReplicaAheadOfPrimary {
            node_id: NodeId::new(1),
            replica_log_index: 1,
            primary_log_index: 0
        },
    );
    drop(primary);
    let primary = PgStore::open(primary_dir.path(), 1).unwrap();
    assert_eq!(
        primary
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap(),
        pending
    );
    assert!(primary
        .pending_metadata_command_publication_started(0, &command)
        .unwrap());
    for (node, store) in [(0, &primary), (2, &successor)] {
        store
            .apply_metadata_command_and_record(node, &command)
            .unwrap();
        assert_reserved(store, &reserve);
    }
    // Primitive-level convergence only: removing the slot here is not a test
    // of the distributed authorized recovery/route activation protocol.
    primary
        .remove_pending_metadata_command_slot(0, &command)
        .unwrap();
    let inputs = [
        replica(&primary, 0),
        replica(&witness, 1),
        replica(&successor, 2),
    ];
    assert!(matches!(
        reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            NodeId::new(2),
            &inputs
        )
        .unwrap(),
        PgPeeringReconstructionDecision::AlreadyConverged { .. }
    ));
}

#[test]
fn direct_put_introduction_evidence_survives_compaction_transfer_and_restart() {
    let source_dir = test_util::tempdir();
    let source = PgStore::open(source_dir.path(), 1).unwrap();
    let (reserve, command) = fixture(&source);
    source
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let record = source
        .get_object_generation_reservation_by_id(&reserve.reservation_id)
        .unwrap();
    assert_eq!(record.bucket, reserve.bucket);
    assert_eq!(record.key, reserve.key);
    assert_eq!(record.generation_id, reserve.generation_id);
    let later = create_bucket_probe_command(1, 2, trusted_bucket_name("later-prefix"), 20);
    source.apply_metadata_command_and_record(0, &later).unwrap();
    let checkpoint = source
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(
        source
            .compact_metadata_command_log_with_receipt_retention(ClusterEpoch::INITIAL, 1)
            .unwrap(),
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 2,
            compacted_before: 3
        }
    );
    assert_eq!(
        source
            .metadata_command_log_stats(ClusterEpoch::INITIAL)
            .unwrap()
            .retained_entries,
        0
    );
    assert_eq!(
        source
            .applied_metadata_command_log_entry_hashes(0, &command)
            .unwrap(),
        None
    );
    assert_reserved(&source, &reserve);
    drop(source);
    let source = PgStore::open(source_dir.path(), 1).unwrap();
    assert_eq!(
        source
            .get_object_generation_reservation_by_id(&reserve.reservation_id)
            .unwrap(),
        record
    );
    assert_eq!(
        source
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap(),
        checkpoint
    );
    assert_reserved(&source, &reserve);

    let mut imported_checkpoint = checkpoint.clone();
    for epoch in [2, 3] {
        let destination_dir = test_util::tempdir();
        let destination = PgStore::open(destination_dir.path(), 1).unwrap();
        let epoch = ClusterEpoch::new(epoch).unwrap();
        let installed = destination
            .install_metadata_transfer_checkpoint_base(2, epoch, &imported_checkpoint)
            .unwrap();
        assert_eq!(installed.cluster_epoch, epoch);
        assert_eq!(installed.applied_log_index, 0);
        assert_eq!(installed.applied_log_hash, 0);
        assert_eq!(installed.state_digest, checkpoint.state_digest);
        assert_reserved(&destination, &reserve);
        assert_eq!(
            destination
                .get_object_generation_reservation_by_id(&reserve.reservation_id)
                .unwrap(),
            record
        );
        imported_checkpoint = destination.metadata_command_checkpoint(2, epoch).unwrap();
        assert_eq!(imported_checkpoint.table_blocks, checkpoint.table_blocks);
        assert_eq!(imported_checkpoint.table_digests, checkpoint.table_digests);
        drop(destination);
        let reopened = PgStore::open(destination_dir.path(), 1).unwrap();
        assert_reserved(&reopened, &reserve);
        assert_eq!(
            reopened
                .get_object_generation_reservation_by_id(&reserve.reservation_id)
                .unwrap(),
            record
        );
        assert_eq!(
            reopened.metadata_command_checkpoint(2, epoch).unwrap(),
            imported_checkpoint
        );
        assert_eq!(
            reopened
                .applied_metadata_command_log_entry_hashes(2, &command)
                .unwrap(),
            None
        );
    }
}
