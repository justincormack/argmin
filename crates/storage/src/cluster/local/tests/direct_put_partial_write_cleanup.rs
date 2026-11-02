// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn direct_put_partial_write_cleanup_failure_preserves_generation() {
    // None is a successful rollback; the other cases independently fail a
    // physical deletion or acknowledgement-store contact during rollback.
    for failed_participant in [None, Some("file"), Some("ack")] {
        let fixture = Fixture::new();
        let handle =
            crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .unwrap();
        let reservation = route
            .test_reserve_generation_with_id(&fixture.reservations[0])
            .unwrap();
        let generation = reservation.generation_id();
        let segment_okh = crate::direct_put_segment_key_hash(&fixture.reservations[0], 0);
        let data_pg = fixture
            .map
            .object_generation_segment_data_pg(&fixture.bucket, &fixture.key, generation, 0)
            .get();
        let ec = fixture.cluster.default_payload_ec_shape();
        let first_path = fixture
            .cluster
            .test_payload_shard_file_path(data_pg, ec, &segment_okh, generation, 0)
            .unwrap();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let observed_writes = Arc::clone(&writes);
        let _write_hook = fixture
            .cluster
            .test_install_before_placed_payload_shard_write_hook(Arc::new(move |_, key| {
                observed_writes
                    .lock()
                    .unwrap()
                    .push(key.shard_index().get());
                if key.shard_index().get() == 1 {
                    assert!(
                        !std::fs::read(&first_path).unwrap().is_empty(),
                        "the prefix was actually written"
                    );
                    return Err(StoreError::NotFound);
                }
                Ok(())
            }));
        let files = Arc::new(Mutex::new(Vec::new()));
        let acks = Arc::new(Mutex::new(Vec::new()));
        let observed_files = Arc::clone(&files);
        let observed_acks = Arc::clone(&acks);
        let _file_hook = fixture
            .cluster
            .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
                observed_files.lock().unwrap().push(key.clone());
                if failed_participant == Some("file") {
                    return Err(StoreError::Io {
                        context: "partial write file cleanup",
                        source: std::io::Error::other("injected"),
                    });
                }
                Ok(())
            }));
        let _ack_hook = fixture
            .cluster
            .test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(move |key| {
                observed_acks.lock().unwrap().push(key.clone());
                if failed_participant == Some("ack") {
                    return Err(StoreError::Io {
                        context: "partial write acknowledgement cleanup",
                        source: std::io::Error::other("injected"),
                    });
                }
                Ok(())
            }));
        let encrypted = &fixture.encrypted[0];
        let error = route
            .write_direct_object_payload(
                reservation.bind_encryption(&encrypted.encryption),
                encrypted.logical_size,
                &encrypted.storage_bytes,
            )
            .unwrap_err();
        assert_eq!(
            error.diagnostic_cause_label(),
            if failed_participant.is_some() {
                "store_io_failure"
            } else {
                "store_not_found"
            }
        );
        assert_eq!(*writes.lock().unwrap(), [0, 1]);
        let first_key = ShardKey::new(&segment_okh, generation.get(), 0);
        assert_eq!(
            files.lock().unwrap().as_slice(),
            std::slice::from_ref(&first_key)
        );
        assert_eq!(
            acks.lock().unwrap().as_slice(),
            std::slice::from_ref(&first_key)
        );
        for index in 0..ec.k + ec.m {
            assert_eq!(
                fixture
                    .cluster
                    .test_payload_shard_file_exists(data_pg, ec, &segment_okh, generation, index)
                    .unwrap(),
                failed_participant == Some("file") && index == 0
            );
        }
        for node in fixture.map.nodes.values() {
            let pg = node.storage_node().get_pg(0).unwrap();
            let reserved = crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &fixture.bucket,
                &fixture.key,
                &fixture.reservations[0],
            );
            if failed_participant.is_some() {
                assert_eq!(
                    reserved.unwrap(),
                    generation,
                    "failed cleanup must not release generation on Drop"
                );
            } else {
                assert!(
                    matches!(
                        reserved,
                        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
                    ),
                    "{reserved:?}"
                );
            }
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &fixture.bucket, &fixture.key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }
        if failed_participant.is_none() {
            assert_single_generation_release(
                &fixture.map,
                0,
                &fixture.bucket,
                &fixture.key,
                &fixture.reservations[0],
            );
        }
        assert!(
            pending_metadata_command_for_test(&fixture.map, PgId::new(0), &fixture.bucket)
                .is_none()
        );
    }
}
