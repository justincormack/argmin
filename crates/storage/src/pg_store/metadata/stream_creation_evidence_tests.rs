// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Production-store evidence for the proposed cleanup-window confirmation rule.
//! These tests use existing session/reservation rows, NOT unimplemented owner or
//! window records. All fixtures are metadata-only and write no shard payload.

use super::*;

pub(super) fn compact_creation_out_of_exact_receipt_window(
    store: &PgStore,
    create: &CreateStreamUploadCommand,
) -> crate::node_runtime::pg_store::MetadataCommandCheckpoint {
    let creation = envelope(create, 1);
    store
        .apply_metadata_command_and_record(0, &creation)
        .unwrap();
    assert!(store
        .applied_metadata_command_log_entry_hashes(0, &creation)
        .unwrap()
        .is_some());
    // A real later command, not record-only log seeding, advances the prefix.
    let later = create_bucket_probe_command(1, 2, trusted_bucket_name("later-prefix"), 20);
    store.apply_metadata_command_and_record(0, &later).unwrap();
    let checkpoint = store
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(checkpoint.applied_log_index, 2);
    // Exercise the production pruning implementation with a small retention
    // bound, rather than manually deleting the creation receipt.
    assert_eq!(
        store
            .compact_metadata_command_log_with_receipt_retention(ClusterEpoch::INITIAL, 1)
            .unwrap(),
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 2,
            compacted_before: 3
        }
    );
    assert_eq!(
        store
            .metadata_command_log_stats(ClusterEpoch::INITIAL)
            .unwrap()
            .retained_entries,
        0
    );
    assert_eq!(
        store
            .applied_metadata_command_log_entry_hashes(0, &creation)
            .unwrap(),
        None
    );
    assert!(store
        .applied_metadata_command_log_entry_hashes(0, &later)
        .unwrap()
        .is_some());
    checkpoint
}

fn assert_materialized_creation(
    store: &PgStore,
    create: &CreateStreamUploadCommand,
    expected_session: &StreamUploadRecord,
) {
    assert_eq!(
        store.get_stream_upload(&create.session.session_id).unwrap(),
        *expected_session
    );
    assert_eq!(
        store
            .get_object_generation_reservation(
                &create.session.bucket,
                &create.session.key,
                &create.session.session_id
            )
            .unwrap(),
        create.payload_generation
    );
    assert_eq!(
        store
            .next_generation_id(&create.session.bucket, &create.session.key)
            .unwrap()
            .get(),
        create.payload_generation.get() + 1
    );
    if let StreamUploadTarget::UploadPart { upload_id, .. } = &create.session.target {
        let upload = store.get_multipart_upload(upload_id).unwrap();
        assert_eq!(upload.bucket, create.session.bucket);
        assert_eq!(upload.key, create.session.key);
        assert_eq!(upload.state, UploadState::InProgress);
        assert!(upload.object_generation_id.get() < create.payload_generation.get());
    }
}

#[test]
fn stream_creation_evidence_survives_log_and_receipt_pruning_and_reopen() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        let checkpoint = compact_creation_out_of_exact_receipt_window(&store, &create);
        let session = store.get_stream_upload(&create.session.session_id).unwrap();
        assert_materialized_creation(&store, &create, &session);
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint
        );
        drop(store);

        let reopened = PgStore::open(dir.path(), 1).unwrap();
        assert_materialized_creation(&reopened, &create, &session);
        assert_eq!(
            reopened
                .applied_metadata_command_log_entry_hashes(0, &envelope(&create, 1))
                .unwrap(),
            None
        );
        assert_eq!(
            reopened
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint
        );
        let before_changes = reopened.conn.total_changes();
        let before_replica = reopened.metadata_command_replica_state().unwrap();
        let before_slot = reopened
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap();
        // Exact old-command replay must NOT be fabricated from materialized
        // state now that its receipt has legitimately left the retention bound.
        assert!(matches!(
            reopened.apply_metadata_command_and_record(0, &envelope(&create, 1)),
            Err(BucketSnapshotLoadError::Store(
                StoreError::MetadataCommandLogConflict { log_index: 1, .. }
            ))
        ));
        assert_eq!(reopened.conn.total_changes(), before_changes);
        assert_eq!(
            reopened.metadata_command_replica_state().unwrap(),
            before_replica
        );
        assert_eq!(
            reopened
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before_slot
        );
        assert_eq!(
            reopened
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint
        );
    }
}

#[test]
fn stream_creation_evidence_checkpoint_import_keeps_ownership_without_old_log_identity() {
    for part in [false, true] {
        let source_dir = test_util::tempdir();
        let source = PgStore::open(source_dir.path(), 1).unwrap();
        let create = fixture(&source, part);
        let checkpoint = compact_creation_out_of_exact_receipt_window(&source, &create);
        let session = source
            .get_stream_upload(&create.session.session_id)
            .unwrap();
        let destination_dir = test_util::tempdir();
        let destination = PgStore::open(destination_dir.path(), 1).unwrap();
        let epoch = ClusterEpoch::new(2).unwrap();
        let installed = destination
            .install_metadata_transfer_checkpoint_base(
                1,
                epoch,
                &checkpoint,
                &crate::pg_topology::PgTopology::new(&[destination.pg_id]).unwrap(),
            )
            .unwrap();
        assert_eq!(installed.cluster_epoch, epoch);
        assert_eq!(installed.applied_log_index, 0);
        assert_eq!(installed.applied_log_hash, 0);
        assert_eq!(installed.state_digest, checkpoint.state_digest);
        assert_materialized_creation(&destination, &create, &session);
        assert_eq!(
            destination
                .applied_metadata_command_log_entry_hashes(1, &envelope(&create, 1))
                .unwrap(),
            None
        );
        let imported = destination.metadata_command_checkpoint(1, epoch).unwrap();
        assert_eq!(imported.table_blocks, checkpoint.table_blocks);
        assert_eq!(imported.table_digests, checkpoint.table_digests);
        drop(destination);
        let reopened = PgStore::open(destination_dir.path(), 1).unwrap();
        assert_materialized_creation(&reopened, &create, &session);
        assert_eq!(
            reopened.metadata_command_checkpoint(1, epoch).unwrap(),
            imported
        );
        assert_eq!(
            reopened
                .applied_metadata_command_log_entry_hashes(1, &envelope(&create, 1))
                .unwrap(),
            None
        );
    }
}

#[test]
fn stream_creation_evidence_witness_rows_are_not_primary_settlement() {
    for part in [false, true] {
        let primary_dir = test_util::tempdir();
        let witness_dir = test_util::tempdir();
        let primary = PgStore::open(primary_dir.path(), 1).unwrap();
        let witness = PgStore::open(witness_dir.path(), 1).unwrap();
        let create = fixture(&primary, part);
        // Start from exactly the same certified-input candidate, including
        // timestamps, so later prefix comparisons have no incidental differences.
        let base = primary
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        witness
            .install_metadata_transfer_checkpoint_base(
                1,
                ClusterEpoch::INITIAL,
                &base,
                &crate::pg_topology::PgTopology::new(&[witness.pg_id]).unwrap(),
            )
            .unwrap();
        let command = envelope(&create, 1);
        primary
            .try_insert_pending_metadata_command_slot(0, &command, Some(&create.session.bucket))
            .unwrap();
        primary
            .mark_pending_metadata_command_publication_started(0, &command)
            .unwrap();
        witness
            .apply_metadata_command_and_record(1, &command)
            .unwrap();
        assert_present(&witness, &create);
        assert_absent(&primary, &create);
        assert_ne!(
            primary.metadata_command_replica_state().unwrap(),
            witness.metadata_command_replica_state().unwrap()
        );
        // A witness has an internally valid checkpoint without being the active
        // primary or proving distributed settlement. This is deliberately a
        // positive canary: checkpoint verification alone cannot confer authority.
        witness
            .metadata_command_checkpoint(1, ClusterEpoch::INITIAL)
            .unwrap()
            .verify()
            .unwrap();
        for primary_applied in [false, true] {
            if primary_applied {
                primary
                    .apply_metadata_command_and_record(0, &command)
                    .unwrap();
                assert_present(&primary, &create);
                assert!(primary
                    .applied_metadata_command_log_entry_hashes(0, &command)
                    .unwrap()
                    .is_some());
                assert_eq!(
                    primary.metadata_command_replica_state().unwrap(),
                    witness.metadata_command_replica_state().unwrap()
                );
            }
            let before = primary.metadata_command_replica_state().unwrap();
            let pending = primary
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap();
            let changes = primary.conn.total_changes();
            assert!(matches!(
                primary.metadata_command_checkpoint(0, ClusterEpoch::INITIAL),
                Err(StoreError::MetadataCommandContention {
                    context: "export metadata command checkpoint with pending command"
                })
            ));
            assert_eq!(primary.conn.total_changes(), changes);
            assert_eq!(primary.metadata_command_replica_state().unwrap(), before);
            assert_eq!(
                primary
                    .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                    .unwrap(),
                pending
            );
            assert!(primary
                .pending_metadata_command_publication_started(0, &command)
                .unwrap());
        }
        primary
            .remove_pending_metadata_command_slot(0, &command)
            .unwrap();
        let settled = primary
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        settled.verify().unwrap();
        assert_present(&primary, &create);
        // Exact stream-row equality is insufficient if the observed prefixes
        // differ. A confirmation implementation must bind the prefix as well.
        let later =
            create_bucket_probe_command(1, 2, trusted_bucket_name("witness-later-prefix"), 30);
        witness
            .apply_metadata_command_and_record(1, &later)
            .unwrap();
        assert_eq!(
            primary
                .get_stream_upload(&create.session.session_id)
                .unwrap(),
            witness
                .get_stream_upload(&create.session.session_id)
                .unwrap()
        );
        let witness_checkpoint = witness
            .metadata_command_checkpoint(1, ClusterEpoch::INITIAL)
            .unwrap();
        witness_checkpoint.verify().unwrap();
        assert_ne!(witness_checkpoint.state_digest, settled.state_digest);
        assert_ne!(
            witness.metadata_command_replica_state().unwrap(),
            primary.metadata_command_replica_state().unwrap()
        );
    }
}
