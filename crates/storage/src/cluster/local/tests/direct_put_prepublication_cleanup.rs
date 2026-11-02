// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    File,
    Acknowledgement,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RejectionStage {
    Condition,
    ProjectedCondition,
    RegisteredPayload,
    AbandonedBudget,
    AbandonedCondition,
}

#[test]
fn admitted_direct_put_rejection_cleans_once_without_drop_release() {
    check_rejected_commit(Fault::None, RejectionStage::Condition);
}

#[test]
fn admitted_direct_put_rejection_preserves_generation_after_physical_cleanup_failure() {
    for fault in [Fault::File, Fault::Acknowledgement] {
        check_rejected_commit(fault, RejectionStage::Condition);
    }
}

#[test]
fn admitted_direct_put_projected_rejection_cleans_payload_before_budgeted_generation_release() {
    for fault in [Fault::None, Fault::File, Fault::Acknowledgement] {
        check_rejected_commit(fault, RejectionStage::ProjectedCondition);
    }
}

#[test]
fn fresh_direct_put_ack_validation_failure_uses_prepublication_cleanup() {
    for fault in [Fault::None, Fault::File, Fault::Acknowledgement] {
        check_rejected_commit(fault, RejectionStage::RegisteredPayload);
    }
}

#[test]
fn abandoned_direct_put_reports_cleanup_failure_before_reinspection_result() {
    for stage in [
        RejectionStage::AbandonedBudget,
        RejectionStage::AbandonedCondition,
    ] {
        for fault in [Fault::None, Fault::File, Fault::Acknowledgement] {
            check_rejected_commit(fault, stage);
        }
    }
}

fn check_rejected_commit(fault: Fault, stage: RejectionStage) {
    let with_contender = stage == RejectionStage::ProjectedCondition;
    let abandon = matches!(
        stage,
        RejectionStage::AbandonedBudget | RejectionStage::AbandonedCondition
    );
    let failed_cleanup_index = if stage == RejectionStage::RegisteredPayload {
        1
    } else {
        0
    };
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
    let encryption_reservation = generation
        .bind_encryption(&encrypted.encryption)
        .bind_standard_segment(encrypted.logical_size)
        .bind_committed_encryption(&prepared.encryption)
        .unwrap();
    let payload = route
        .write_direct_object_segment_payload(encryption_reservation, &encrypted.storage_bytes)
        .unwrap();
    let identity = direct_payload_test_identity(&payload);
    let shards = payload.written.written_shards.clone();
    if stage != RejectionStage::RegisteredPayload {
        fixture
            .cluster
            .test_register_payload_shard_acks(identity.data_pg_id, &shards)
            .unwrap();
    }
    assert_direct_payload_shards_exist(&fixture.cluster, identity);
    let ack_node = fixture
        .map
        .metadata_pg_primary_node(
            fixture.cluster.operation_epoch(),
            PgId::new(identity.data_pg_id),
        )
        .unwrap()
        .storage_node();
    for shard in &shards {
        let ack = ack_node
            .get_pg(identity.data_pg_id)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack);
        if stage == RejectionStage::RegisteredPayload {
            assert!(matches!(ack, Err(StoreError::NotFound)), "{ack:?}");
        } else {
            ack.unwrap();
        }
    }
    if stage == RejectionStage::RegisteredPayload {
        // Keep the valid write acknowledgements, but lose a physical file before
        // commit. Witness construction and registration succeed; the later
        // registered-payload read must reject it before pending installation.
        let path = fixture
            .cluster
            .test_payload_shard_file_path(
                identity.data_pg_id,
                identity.ec,
                &identity.segment_okh,
                identity.segment_vid,
                0,
            )
            .unwrap();
        std::fs::remove_file(path).unwrap();
    }
    let registrations = Arc::new(AtomicUsize::new(0));
    let observed_registrations = Arc::clone(&registrations);
    let _registration_hook = fixture
        .cluster
        .test_install_before_direct_put_payload_ack_registration_hook(Arc::new(move || {
            observed_registrations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
    let contender = if with_contender {
        let generation = route
            .test_reserve_generation_with_id(&fixture.reservations[1])
            .unwrap();
        let prepared = fixture.prepared(1, generation.generation_id());
        let encrypted = &fixture.encrypted[1];
        let reservation = generation
            .bind_encryption(&encrypted.encryption)
            .bind_standard_segment(encrypted.logical_size)
            .bind_committed_encryption(&prepared.encryption)
            .unwrap();
        let payload = route
            .write_direct_object_segment_payload(reservation, &encrypted.storage_bytes)
            .unwrap();
        fixture.record_payload_integrity(1, &payload).unwrap();
        let identity = direct_payload_test_identity(&payload);
        fixture
            .leave_pending_with_prepared_after_inspection_failure(1, &route, payload, &prepared)
            .unwrap();
        Some(identity)
    } else {
        None
    };
    let file_calls = Arc::new(AtomicUsize::new(0));
    let ack_calls = Arc::new(AtomicUsize::new(0));
    let file_observer = Arc::clone(&file_calls);
    let ack_observer = Arc::clone(&ack_calls);
    let _file_hook = fixture
        .cluster
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            file_observer.fetch_add(1, Ordering::SeqCst);
            if fault == Fault::File && key.shard_index().get() == failed_cleanup_index {
                return Err(StoreError::Io {
                    context: "prepublication cleanup file fault",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let _ack_hook = fixture
        .cluster
        .test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(move |key| {
            ack_observer.fetch_add(1, Ordering::SeqCst);
            if fault == Fault::Acknowledgement && key.shard_index().get() == failed_cleanup_index {
                return Err(StoreError::Io {
                    context: "prepublication cleanup acknowledgement fault",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let installed = Arc::new(AtomicUsize::new(0));
    let observed_installed = Arc::clone(&installed);
    let installed_command = Arc::new(Mutex::new(None));
    let observed_command = Arc::clone(&installed_command);
    let _installed_hook = abandon.then(|| {
        fixture
            .cluster
            .test_install_direct_put_pending_installed_hook(Arc::new(move |command| {
                observed_installed.fetch_add(1, Ordering::SeqCst);
                *observed_command.lock().unwrap() = Some(command.clone());
                true // Exhaust the work budget after installation, before any apply.
            }))
    });
    let _reinspection_hook = (stage == RejectionStage::AbandonedCondition).then(|| {
        fixture
            .cluster
            .test_install_snapshot_reinspection_hook(Arc::new(|| Duration::from_secs(5)))
    });
    let action_calls = AtomicUsize::new(0);
    let result = route.commit_direct_object(payload, &prepared, |snapshot| {
        assert_eq!(snapshot.existing_etag.is_some(), with_contender);
        let first_action = action_calls.fetch_add(1, Ordering::SeqCst) == 0;
        if stage == RejectionStage::RegisteredPayload || (abandon && first_action) {
            Ok(())
        } else {
            Err("condition rejected")
        }
    });
    if abandon {
        assert_eq!(installed.load(Ordering::SeqCst), 1);
        assert_eq!(
            action_calls.load(Ordering::SeqCst),
            if stage == RejectionStage::AbandonedCondition {
                2
            } else {
                1
            }
        );
    }
    if stage == RejectionStage::RegisteredPayload {
        assert_eq!(
            registrations.load(Ordering::SeqCst),
            1,
            "the fresh path must reach acknowledgement registration"
        );
    }
    if fault == Fault::None {
        if stage == RejectionStage::RegisteredPayload {
            assert_eq!(
                result.unwrap_err().diagnostic_cause_label(),
                "store_not_found"
            );
        } else if stage == RejectionStage::AbandonedBudget {
            assert_eq!(
                result.unwrap_err().kind(),
                crate::DirectPutFailureKind::SnapshotReinspectionConflict
            );
        } else {
            assert!(
                matches!(result, Ok(Err("condition rejected"))),
                "{result:?}"
            );
        }
        assert_single_generation_release(
            &fixture.map,
            0,
            &fixture.bucket,
            &fixture.key,
            &fixture.reservations[0],
        );
        assert_eq!(file_calls.load(Ordering::SeqCst), shards.len());
        assert_eq!(ack_calls.load(Ordering::SeqCst), shards.len());
        assert_direct_payload_staging_cleaned(
            &fixture.map,
            &fixture.cluster,
            &fixture.bucket,
            &fixture.key,
            &fixture.reservations[0],
            identity,
        );
    } else {
        let error = result.unwrap_err();
        assert_eq!(error.diagnostic_cause_label(), "store_io_failure");
        for node in fixture.map.nodes.values() {
            let pg = node.storage_node().get_pg(0).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &fixture.bucket,
                    &fixture.key,
                    &fixture.reservations[0]
                )
                .unwrap(),
                identity.segment_vid
            );
        }
    }
    for shard in &shards {
        let index = shard.key.shard_index().get();
        assert_eq!(
            fixture
                .cluster
                .test_payload_shard_file_exists(
                    identity.data_pg_id,
                    identity.ec,
                    &identity.segment_okh,
                    identity.segment_vid,
                    index
                )
                .unwrap(),
            fault == Fault::File && index == failed_cleanup_index
        );
        let ack = ack_node
            .get_pg(identity.data_pg_id)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack);
        if fault == Fault::Acknowledgement && index == failed_cleanup_index {
            ack.unwrap();
        } else {
            assert!(matches!(ack, Err(StoreError::NotFound)), "{ack:?}");
        }
    }
    assert_bucket_write_reservations_released(&fixture.map, &fixture.bucket);
    assert!(
        pending_metadata_command_for_test(&fixture.map, PgId::new(0), &fixture.bucket).is_none()
    );
    for node in fixture.map.nodes.values() {
        let pg = node.storage_node().get_pg(0).unwrap();
        if abandon {
            assert!(pg
                .metadata_command_abandoned(
                    node.node_id().as_u32(),
                    installed_command.lock().unwrap().as_ref().unwrap(),
                )
                .unwrap());
        }
        let object = crate::PgMetadataStore::get_object_meta(&*pg, &fixture.bucket, &fixture.key);
        if let Some(contender) = contender {
            assert_eq!(
                object.unwrap().as_live().unwrap().generation_id,
                contender.segment_vid
            );
        } else {
            assert!(matches!(object, Err(crate::MetadataError::ObjectNotFound)));
        }
    }
    if let Some(contender) = contender {
        assert_direct_payload_shards_exist(&fixture.cluster, contender);
    }
}
