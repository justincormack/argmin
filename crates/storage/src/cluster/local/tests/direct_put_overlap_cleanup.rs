// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    File,
    Acknowledgement,
}

#[test]
fn mismatched_pending_direct_put_reports_caller_subset_cleanup_failures() {
    for layout in [Layout::PartialOverlap, Layout::Disjoint] {
        for fault in [Fault::File, Fault::Acknowledgement] {
            check_cleanup_failure(layout, fault);
        }
    }
}

fn check_cleanup_failure(layout: Layout, fault: Fault) {
    let mut fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_put_object_route(&fixture.bucket, &fixture.key)
        .unwrap();
    let reservation = reserve_shared_generation(&mut fixture, &route).unwrap();
    let generation = reservation.generation_id();
    let prepared = fixture.prepared();
    let payload = route
        .write_direct_object_payload(
            reservation.bind_encryption(&fixture.encrypted.encryption),
            fixture.encrypted.logical_size,
            &fixture.encrypted.storage_bytes,
        )
        .unwrap();
    let caller_identity = direct_payload_test_identity(&payload);
    let caller_shards = payload.written.written_shards.clone();
    let caller_files = fixture.files(caller_identity).unwrap();
    fixture
        .cluster
        .test_register_payload_shard_acks(0, &caller_shards)
        .unwrap();
    let mut command_identity = caller_identity;
    command_identity.ec = COMMAND_EC;
    if layout == Layout::Disjoint {
        command_identity.segment_okh[0] ^= 1;
    }
    let command_written = fixture.write_command_payload(command_identity).unwrap();
    let command_files = fixture.files(command_identity).unwrap();
    let command = fixture
        .install_pending(command_identity, &command_written, &prepared)
        .unwrap();
    let state = State {
        layout,
        command_first: false,
        trace: vec![Step::Stage],
    };
    fixture
        .observe(&state, &command, true, false, &command_files, &caller_files)
        .unwrap();
    let ack_node = fixture
        .map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(0))
        .unwrap()
        .storage_node();
    for shard in caller_shards.iter().chain(&command_written.written_shards) {
        ack_node
            .get_pg(0)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack)
            .unwrap();
    }
    // Independent partition oracle: partial overlap has the first three
    // physical locations/keys in common, leaving only index 3 caller-owned.
    let caller_only: Vec<_> = caller_shards
        .iter()
        .enumerate()
        .filter(|(index, _)| layout == Layout::Disjoint || *index == 3)
        .map(|(_, shard)| shard.key.clone())
        .collect();
    for (index, (path, _)) in caller_files.iter().enumerate() {
        assert_eq!(
            command_files
                .iter()
                .any(|(command_path, _)| command_path == path),
            layout == Layout::PartialOverlap && index < 3,
        );
    }
    let failed_key = caller_only[0].clone();
    let file_calls = Arc::new(Mutex::new(Vec::new()));
    let ack_calls = Arc::new(Mutex::new(Vec::new()));
    let file_observer = Arc::clone(&file_calls);
    let ack_observer = Arc::clone(&ack_calls);
    let failed_file = failed_key.clone();
    let failed_ack = failed_key.clone();
    let file_hook = fixture
        .cluster
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            file_observer.lock().unwrap().push(key.clone());
            if fault == Fault::File && key == &failed_file {
                return Err(StoreError::Io {
                    context: "mismatched pending caller file cleanup",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let ack_hook = fixture
        .cluster
        .test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(move |key| {
            ack_observer.lock().unwrap().push(key.clone());
            if fault == Fault::Acknowledgement && key == &failed_ack {
                return Err(StoreError::Io {
                    context: "mismatched pending caller acknowledgement cleanup",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let error = route
        .commit_direct_object(payload, &prepared, |_| -> Result<(), ()> {
            panic!("mismatched pending payload must reject before snapshot evaluation")
        })
        .unwrap_err();
    assert_eq!(
        error.diagnostic_cause_label(),
        "store_io_failure",
        "{layout:?} {fault:?}: {error:?}"
    );
    // Exactly one pass over only caller-owned keys: an armed Drop would repeat
    // these effects and could delete the pending command's overlapping payload.
    assert_eq!(*file_calls.lock().unwrap(), caller_only);
    assert_eq!(*ack_calls.lock().unwrap(), caller_only);
    for (shard, (path, bytes)) in caller_shards.iter().zip(&caller_files) {
        let shared = !caller_only.contains(&shard.key);
        let file_remains = shared || (fault == Fault::File && shard.key == failed_key);
        assert_eq!(path.exists(), file_remains);
        if file_remains {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
        let ack = ack_node
            .get_pg(0)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack);
        if shared || (fault == Fault::Acknowledgement && shard.key == failed_key) {
            ack.unwrap();
        } else {
            assert!(matches!(ack, Err(StoreError::NotFound)), "{ack:?}");
        }
    }
    // Command files, acknowledgements, generation ownership and exact pending
    // envelope survive even if caller-only cleanup cannot complete.
    fixture
        .observe(&state, &command, false, false, &command_files, &[])
        .unwrap();
    for shard in &command_written.written_shards {
        ack_node
            .get_pg(0)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack)
            .unwrap();
    }
    drop(file_hook);
    drop(ack_hook);
    fixture
        .cluster
        .try_delete_payload_shard_keys_at_epoch(
            ClusterEpoch::INITIAL,
            caller_identity.data_pg_id,
            caller_identity.ec,
            &caller_identity.segment_okh,
            generation,
            caller_only,
        )
        .unwrap();
    fixture
        .observe(
            &state,
            &command,
            false,
            false,
            &command_files,
            &caller_files,
        )
        .unwrap();
    assert_eq!(
        fixture
            .cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                PgId::new(0),
                &command,
                &fixture.cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied,
    );
    fixture
        .observe(&state, &command, false, true, &command_files, &caller_files)
        .unwrap();
}
