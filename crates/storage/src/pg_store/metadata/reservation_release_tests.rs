// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

type RecordMutation = (&'static str, fn(&mut BucketWriteReservationRecord));

fn seed(store: &PgStore, target: Option<&str>) -> BucketWriteReservationRecord {
    let bucket = crate::tests::bucket_name("exact-reservation-release");
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
        owner_token: "owner-token",
        cluster_epoch: ClusterEpoch::INITIAL,
        operation_kind: if target.is_some() {
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND
        } else {
            "bucket-write-snapshot"
        },
        created_at: 1_000,
        lease_deadline: 10_000,
        target_context: target,
    };
    store
        .acquire_durable_bucket_write_reservation(acquire("canary"))
        .unwrap();
    store
        .acquire_durable_bucket_write_reservation(acquire("target"))
        .unwrap()
}

#[test]
fn bucket_reservation_record_release_requires_every_exact_field() {
    for target in [None, Some("key")] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let original = seed(&store, target);
        let before = store
            .durable_bucket_write_reservations(&original.bucket)
            .unwrap();
        // Each candidate changes exactly one field. Even the nullable target
        // must compare by value; SQL `= NULL` would break the positive canary.
        let changes: &[RecordMutation] = &[
            ("operation", |row| {
                row.operation_kind = "another-operation".into()
            }),
            ("created_at", |row| row.created_at += 1),
            ("target", |row| {
                row.target_context = Some("other-key".into())
            }),
            ("bucket", |row| {
                row.bucket = crate::tests::bucket_name("other-bucket")
            }),
            ("id", |row| row.reservation_id = "missing".into()),
            ("owner", |row| row.owner_token = "other-owner".into()),
            ("epoch", |row| {
                row.cluster_epoch = ClusterEpoch::new(2).unwrap()
            }),
            ("execution", |row| row.bucket_execution_generation += 1),
            ("incarnation", |row| row.bucket_incarnation_generation += 1),
            ("lease", |row| row.lease_deadline += 1),
        ];
        for (field, change) in changes {
            let mut crossed = original.clone();
            change(&mut crossed);
            assert_ne!(crossed, original, "{field}");
            let sql_changes = store.conn.total_changes();
            let result = store.release_durable_bucket_write_reservation(&crossed);
            assert!(
                matches!(
                    result,
                    Err(MetadataError::BucketWriteReservationNotFound { .. })
                ),
                "{field}: {result:?}"
            );
            assert_eq!(store.conn.total_changes(), sql_changes, "{field}");
            assert_eq!(
                store
                    .durable_bucket_write_reservations(&original.bucket)
                    .unwrap(),
                before,
                "{field}"
            );
        }
        if target.is_some() {
            let mut crossed = original.clone();
            crossed.target_context = None;
            let sql_changes = store.conn.total_changes();
            assert!(matches!(
                store.release_durable_bucket_write_reservation(&crossed),
                Err(MetadataError::BucketWriteReservationNotFound { .. })
            ));
            assert_eq!(store.conn.total_changes(), sql_changes);
            assert_eq!(
                store
                    .durable_bucket_write_reservations(&original.bucket)
                    .unwrap(),
                before
            );
        }
        store
            .release_durable_bucket_write_reservation(&original)
            .unwrap();
        let remaining = store
            .durable_bucket_write_reservations(&original.bucket)
            .unwrap();
        assert_eq!(remaining, vec![before[0].clone()]);
        assert_eq!(remaining[0].reservation_id, "canary");
    }
}

#[test]
fn bucket_reservation_record_release_preserves_renewal_while_proof_release_owns_identity() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 0).unwrap();
    let original = seed(&store, Some("key"));
    let proof = BucketWriteReservationProof::from(&original);
    let renewed = store
        .heartbeat_durable_bucket_write_reservation(DurableBucketWriteReservationHeartbeat {
            name: &proof.bucket,
            reservation_id: &proof.reservation_id,
            owner_token: &proof.owner_token,
            cluster_epoch: proof.cluster_epoch,
            bucket_execution_generation: proof.bucket_execution_generation,
            bucket_incarnation_generation: proof.bucket_incarnation_generation,
            current_lease_deadline: proof.lease_deadline,
            lease_deadline: 20_000,
            now: 1_000,
        })
        .unwrap();
    assert_ne!(renewed.lease_deadline, original.lease_deadline);
    let before = store
        .durable_bucket_write_reservations(&original.bucket)
        .unwrap();
    let sql_changes = store.conn.total_changes();
    assert!(matches!(
        store.release_durable_bucket_write_reservation(&original),
        Err(MetadataError::BucketWriteReservationNotFound { .. })
    ));
    assert_eq!(store.conn.total_changes(), sql_changes);
    assert_eq!(
        store
            .durable_bucket_write_reservations(&original.bucket)
            .unwrap(),
        before
    );
    // Proof-based convergence deliberately ignores the renewable lease, but
    // never any of the immutable operation/creation/target identity fields.
    for field in ["operation", "created_at", "target"] {
        let mut crossed = proof.clone();
        match field {
            "operation" => crossed.operation_kind = "another-operation".into(),
            "created_at" => crossed.created_at += 1,
            "target" => crossed.target_context = Some("other-key".into()),
            _ => unreachable!(),
        }
        assert!(
            matches!(
                store.release_metadata_command_bucket_write_reservation(&crossed),
                Err(MetadataError::BucketWriteReservationConflict { .. })
            ),
            "{field}"
        );
        assert_eq!(store.conn.total_changes(), sql_changes, "{field}");
        assert_eq!(
            store
                .durable_bucket_write_reservations(&original.bucket)
                .unwrap(),
            before,
            "{field}"
        );
    }
    store
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap();
    store
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap();
    let remaining = store
        .durable_bucket_write_reservations(&original.bucket)
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].reservation_id, "canary");
    assert!(store
        .durable_bucket_write_reservation(&original.bucket, &original.reservation_id)
        .unwrap()
        .is_none());
}
