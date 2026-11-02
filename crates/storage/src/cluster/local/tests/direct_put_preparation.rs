// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::ObjectPgActionError;

#[test]
fn matching_pending_direct_put_ack_restoration_never_uses_caller_cleanup() {
    for fail_restoration in [true, false] {
        let fixture = Fixture::new();
        let handle =
            crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .unwrap();
        let generation = route
            .test_reserve_generation_with_id(&fixture.reservations[0])
            .unwrap();
        let prepared = fixture.prepared(0, generation.generation_id());
        let encrypted = &fixture.encrypted[0];
        let reservation = generation
            .bind_encryption(&encrypted.encryption)
            .bind_standard_segment(encrypted.logical_size)
            .bind_committed_encryption(&prepared.encryption)
            .unwrap();
        let payload = route
            .write_direct_object_segment_payload(reservation, &encrypted.storage_bytes)
            .unwrap();
        fixture.record_payload_integrity(0, &payload).unwrap();
        let identity = direct_payload_test_identity(&payload);
        let shards = payload.written.written_shards.clone();
        let command = fixture
            .install_matching_pending(0, &payload, &prepared)
            .unwrap();
        fixture
            .cluster
            .test_delete_metadata_primary_payload_shard_acks(
                identity.data_pg_id,
                &shards.iter().map(|s| s.key.clone()).collect::<Vec<_>>(),
            )
            .unwrap();
        let ack_node = fixture
            .map
            .metadata_pg_primary_node(
                fixture.cluster.operation_epoch(),
                PgId::new(identity.data_pg_id),
            )
            .unwrap()
            .storage_node();
        for shard in &shards {
            assert!(matches!(
                ack_node
                    .get_pg(identity.data_pg_id)
                    .unwrap()
                    .validate_written_shard_ack(&shard.key, shard.ack),
                Err(StoreError::NotFound)
            ));
        }
        fixture
            .observe(
                [Ownership::Pending, Ownership::Unstaged],
                &[Some(identity), None],
                Some(&command),
            )
            .unwrap();
        let registrations = Arc::new(AtomicUsize::new(0));
        let observed_registrations = Arc::clone(&registrations);
        let _registration_hook = fixture
            .cluster
            .test_install_before_direct_put_payload_ack_registration_hook(Arc::new(move || {
                observed_registrations.fetch_add(1, Ordering::SeqCst);
                if fail_restoration {
                    return Err(ObjectPgActionError::Store(StoreError::Io {
                        context: "pending ack restoration failure",
                        source: std::io::Error::other("injected"),
                    }));
                }
                Ok(())
            }));
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let file_observer = Arc::clone(&cleanup_calls);
        let ack_observer = Arc::clone(&cleanup_calls);
        let _file_hook = fixture
            .cluster
            .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
                file_observer.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }));
        let _ack_hook = fixture
            .cluster
            .test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(move |_| {
                ack_observer.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }));
        let result = route.commit_direct_object(payload, &prepared, |_| -> Result<(), ()> {
            panic!("matching pending publication must not evaluate a fresh request snapshot")
        });
        assert_eq!(registrations.load(Ordering::SeqCst), 1);
        assert_eq!(
            cleanup_calls.load(Ordering::SeqCst),
            0,
            "command-owned staging must not enter caller cleanup or Drop"
        );
        if fail_restoration {
            assert_eq!(
                result.unwrap_err().diagnostic_cause_label(),
                "store_io_failure"
            );
            fixture
                .observe(
                    [Ownership::Pending, Ownership::Unstaged],
                    &[Some(identity), None],
                    Some(&command),
                )
                .unwrap();
        } else {
            result.unwrap().unwrap();
            fixture
                .observe(
                    [Ownership::Visible, Ownership::Unstaged],
                    &[Some(identity), None],
                    None,
                )
                .unwrap();
        }
        for shard in &shards {
            let ack = ack_node
                .get_pg(identity.data_pg_id)
                .unwrap()
                .validate_written_shard_ack(&shard.key, shard.ack);
            if fail_restoration {
                assert!(matches!(ack, Err(StoreError::NotFound)));
            } else {
                ack.unwrap();
            }
        }
    }
}
