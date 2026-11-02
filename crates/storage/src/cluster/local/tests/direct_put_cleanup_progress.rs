// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Lost-handle discovery regressions require production cleanup to supply its
//! own progress, without test-scheduled release, background threads or sleeps.
//! The separate retained-handle control explicitly retries request-local cleanup;
//! it does not satisfy the lost-handle discovery requirement.

use super::*;

#[derive(Clone, Copy, Debug)]
enum Cancellation {
    Drop,
    Discard,
    RetainedCleanup,
}

#[derive(Clone, Copy, Debug)]
enum CancelWhen {
    BeforeRecovery,
    AfterRecovery,
}

// Invoke the same operations used by the static recovery, stream-session,
// shard-scavenger and durable-reclaim scanners. The fixture has one PG, no
// deleted objects/buckets and no stream sessions. Assert that the reclaim
// scan really completes and has no work for its worker to execute.
fn maintenance_pass(fixture: &Fixture) -> Result<usize, String> {
    let recovered = fixture
        .cluster
        .drain_pending_metadata_commands_for_static_map()
        .map_err(diagnostic)?;
    let streams = fixture.cluster.scavenge_abandoned_stream_sessions(0);
    equal(
        streams.discovered,
        0,
        "direct PUT must not invent a stream cleanup record",
    )?;
    equal(
        streams.reservation_check_failed,
        0,
        "stream scan reservation errors",
    )?;
    equal(streams.abort_failed, 0, "stream scan abort errors")?;
    fixture
        .cluster
        .audit_shard_storage_for_scavenger()
        .map_err(diagnostic)?;
    let reclaim = fixture
        .cluster
        .enqueue_durable_reclaim_work_batch_excluding(
            None,
            1,
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
        );
    equal(
        reclaim.outcome,
        crate::DurableReclaimScanOutcome::Complete,
        "reclaim scan completes",
    )?;
    equal(
        reclaim.next_pg_id,
        None,
        "reclaim scan covers the whole fixture",
    )?;
    equal(reclaim.scanned_pgs, 1, "reclaim scan did not skip the PG")?;
    equal(
        reclaim.retry_pass_required,
        false,
        "reclaim scan had no failures",
    )?;
    if let Some(work) = fixture.cluster.try_take_reclaim_work() {
        return Err(format!(
            "maintenance fixture has unprocessed reclaim work: {work:?}"
        ));
    }
    Ok(recovered)
}

fn cancellation_progress(
    owner: usize,
    cancellation: Cancellation,
    when: CancelWhen,
) -> Result<(), String> {
    let fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admissions = [
        handle.admit_current_route().map_err(diagnostic)?,
        handle.admit_current_route().map_err(diagnostic)?,
    ];
    let routes = [
        admissions[0]
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .map_err(diagnostic)?,
        admissions[1]
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .map_err(diagnostic)?,
    ];
    let mut payloads = [None, None];
    let mut identities = [None, None];
    let generations = [
        routes[0]
            .test_reserve_generation_with_id(&fixture.reservations[0])
            .map_err(diagnostic)?,
        routes[1]
            .test_reserve_generation_with_id(&fixture.reservations[1])
            .map_err(diagnostic)?,
    ];
    let prepared = [
        fixture.prepared(0, generations[0].generation_id()),
        fixture.prepared(1, generations[1].generation_id()),
    ];
    for (actor, generation) in generations.into_iter().enumerate() {
        let encrypted = &fixture.encrypted[actor];
        let encryption_reservation = generation
            .bind_encryption(&encrypted.encryption)
            .bind_standard_segment(encrypted.logical_size)
            .bind_committed_encryption(&prepared[actor].encryption)
            .map_err(diagnostic)?;
        let payload = routes[actor]
            .write_direct_object_segment_payload(encryption_reservation, &encrypted.storage_bytes)
            .map_err(diagnostic)?;
        fixture.record_payload_integrity(actor, &payload)?;
        identities[actor] = Some(direct_payload_test_identity(&payload));
        payloads[actor] = Some(payload);
    }
    // Safety control: maintenance must not remove a still-live request's
    // payload or reservation just because it has no pending commit yet.
    for _ in 0..3 {
        equal(
            maintenance_pass(&fixture)?,
            0,
            "no command exists before commit",
        )?;
        fixture.observe([Ownership::Caller; 2], &identities, None)?;
    }
    let command = fixture.leave_pending_with_prepared_after_inspection_failure(
        owner,
        &routes[owner],
        payloads[owner].take().unwrap(),
        &prepared[owner],
    )?;
    let runtime = fixture.map.runtime_state();
    let MetadataCommandRecoveryAdmission::Leader(guard) =
        runtime.join_metadata_command_recovery(PgId::new(0), &command)
    else {
        return Err("fixture must acquire recovery leadership".into());
    };
    guard.relinquish_for_authorized_recovery();
    let other = 1 - owner;
    let mut ownership = [Ownership::Caller; 2];
    ownership[owner] = Ownership::Pending;
    fixture.observe(ownership, &identities, Some(&command))?;

    let mut retained_cleanup = None;
    let cancel = |payload| -> Result<_, String> {
        let mut cleanup_to_retry = None;
        match cancellation {
            Cancellation::Drop => drop(payload),
            Cancellation::Discard => {
                let result = routes[other].discard_direct_object_payload(payload);
                if matches!(when, CancelWhen::BeforeRecovery) {
                    let error = result
                        .err()
                        .ok_or("discard hid failed reservation release")?;
                    equal(
                        error.diagnostic_cause_label(),
                        "metadata_command_awaiting_authorized_recovery",
                        "discard reports blocked release",
                    )?;
                } else {
                    result.map_err(diagnostic)?;
                }
            }
            Cancellation::RetainedCleanup => {
                let mut cleanup = payload.into_cleanup();
                let error = cleanup
                    .finish()
                    .err()
                    .ok_or("cleanup hid blocked release")?;
                equal(
                    error.diagnostic_cause_label(),
                    "metadata_command_awaiting_authorized_recovery",
                    "retained cleanup reports blocked release",
                )?;
                cleanup_to_retry = Some(cleanup);
            }
        }
        // The production bucket-write wrapper releases caller authority after
        // its action returns, even if best-effort payload-handle cleanup failed.
        fixture
            .cluster
            .release_bucket_write_reservation_proof(&prepared[other].bucket_write_reservation)
            .map_err(diagnostic)?;
        Ok(cleanup_to_retry)
    };
    if matches!(when, CancelWhen::BeforeRecovery) {
        retained_cleanup = cancel(payloads[other].take().unwrap())?;
        ownership[other] = Ownership::CancelledPendingRelease;
        fixture.observe(ownership, &identities, Some(&command))?;
    }

    // Unlike the earlier model, the production scan chooses and authorizes
    // recovery itself; the test does not hand it the exact pending envelope.
    equal(
        maintenance_pass(&fixture)?,
        1,
        "production static recovery finishes owner command",
    )?;
    ownership[owner] = Ownership::Visible;
    if matches!(when, CancelWhen::AfterRecovery) {
        fixture.observe(ownership, &identities, None)?;
        retained_cleanup = cancel(payloads[other].take().unwrap())?;
    }
    for round in 0..3 {
        equal(
            maintenance_pass(&fixture)?,
            0,
            &format!("healthy maintenance round {round} has no pending commands"),
        )?;
    }
    if let Some(mut cleanup) = retained_cleanup {
        cleanup.finish().map_err(diagnostic)?;
        cleanup.finish().map_err(diagnostic)?;
        drop(cleanup);
        assert_single_generation_release(
            &fixture.map,
            0,
            &fixture.bucket,
            &fixture.key,
            &fixture.reservations[other],
        );
    }
    equal(
        runtime.test_metadata_command_recovery_flight_count(),
        0,
        "recovery has no retained flight",
    )?;

    // The requirement is release, not the old model's CancelledPendingRelease
    // expectation. Do not repair the state with a manual release here.
    ownership[other] = Ownership::Released;
    fixture.observe(ownership, &identities, None)
}

#[test]
fn direct_put_cleanup_handle_retries_generation_release_after_recovery() {
    for owner in 0..2 {
        cancellation_progress(
            owner,
            Cancellation::RetainedCleanup,
            CancelWhen::BeforeRecovery,
        )
        .unwrap();
    }
}

#[test]
fn crossed_route_discard_reports_blocked_generation_release_without_drop_retry() {
    let fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_put_object_route(&fixture.bucket, &fixture.key)
        .unwrap();
    let mut payloads = Vec::new();
    let mut identities = [None, None];
    for (actor, identity) in identities.iter_mut().enumerate() {
        let generation = route
            .test_reserve_generation_with_id(&fixture.reservations[actor])
            .unwrap();
        let prepared = fixture.prepared(actor, generation.generation_id());
        let encrypted = &fixture.encrypted[actor];
        let encryption_reservation = generation
            .bind_encryption(&encrypted.encryption)
            .bind_standard_segment(encrypted.logical_size)
            .bind_committed_encryption(&prepared.encryption)
            .unwrap();
        let payload = route
            .write_direct_object_segment_payload(encryption_reservation, &encrypted.storage_bytes)
            .unwrap();
        fixture.record_payload_integrity(actor, &payload).unwrap();
        *identity = Some(direct_payload_test_identity(&payload));
        payloads.push(payload);
    }
    let cancelled = payloads.pop().unwrap();
    let command = fixture
        .leave_pending_after_inspection_failure(0, &route, payloads.pop().unwrap())
        .unwrap();
    let runtime = fixture.map.runtime_state();
    let MetadataCommandRecoveryAdmission::Leader(guard) =
        runtime.join_metadata_command_recovery(PgId::new(0), &command)
    else {
        panic!("fixture must acquire recovery leadership");
    };
    guard.relinquish_for_authorized_recovery();
    let crossed_key = crate::tests::object_key("crossed-discard-key");
    let crossed_route = admission
        .active_put_object_route(&fixture.bucket, &crossed_key)
        .unwrap();
    let file_calls = Arc::new(AtomicUsize::new(0));
    let ack_calls = Arc::new(AtomicUsize::new(0));
    let file_observer = Arc::clone(&file_calls);
    let ack_observer = Arc::clone(&ack_calls);
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
    let ec = cancelled.written.ec;
    let error = crossed_route
        .discard_direct_object_payload(cancelled)
        .unwrap_err();
    assert_eq!(
        error.diagnostic_cause_label(),
        "metadata_command_awaiting_authorized_recovery"
    );
    assert_eq!(file_calls.load(Ordering::SeqCst), usize::from(ec.k + ec.m));
    assert_eq!(ack_calls.load(Ordering::SeqCst), usize::from(ec.k + ec.m));
    fixture
        .observe(
            [Ownership::Pending, Ownership::CancelledPendingRelease],
            &identities,
            Some(&command),
        )
        .unwrap();
}

fn assert_progress(when: CancelWhen) {
    let mut failures = Vec::new();
    for owner in 0..2 {
        for cancellation in [Cancellation::Drop, Cancellation::Discard] {
            if let Err(error) = cancellation_progress(owner, cancellation, when) {
                failures.push(format!(
                    "owner={owner}, cancellation={cancellation:?}, when={when:?}: {error}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "production cleanup made no progress:\n{}",
        failures.join("\n")
    );
}

#[test]
#[ignore = "known lost reservation cleanup obligation; see plans/durable-generation-cleanup-ownership.md"]
fn production_maintenance_releases_cancelled_direct_put_reservation_after_recovery() {
    assert_progress(CancelWhen::BeforeRecovery);
}

#[test]
fn production_maintenance_preserves_live_direct_put_and_cleans_cancellation_after_recovery() {
    assert_progress(CancelWhen::AfterRecovery);
}

#[test]
#[ignore = "known late-write cleanup gap; see plans/durable-generation-cleanup-ownership.md"]
fn direct_put_release_fences_delayed_staging() {
    let fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_put_object_route(&fixture.bucket, &fixture.key)
        .unwrap();
    let generation_reservation = route
        .test_reserve_generation_with_id(&fixture.reservations[0])
        .unwrap();
    let generation = generation_reservation.generation_id();

    // Derive the expected identities independently of the write result, so an
    // error returned after partial physical publication cannot hide leaked shards.
    let segment_okh = crate::direct_put_segment_key_hash(&fixture.reservations[0], 0);
    let data_pg =
        fixture
            .map
            .object_generation_segment_data_pg(&fixture.bucket, &fixture.key, generation, 0);
    let ec = fixture.cluster.default_payload_ec_shape();
    let shard_state = || {
        (0..ec.k + ec.m)
            .map(|index| {
                let key = crate::ShardKey::new(&segment_okh, generation.get(), index);
                (
                    fixture
                        .cluster
                        .test_shard_exists(data_pg.get(), &key)
                        .unwrap(),
                    fixture
                        .cluster
                        .test_payload_shard_file_exists(
                            data_pg.get(),
                            ec,
                            &segment_okh,
                            generation,
                            index,
                        )
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    let absent = vec![(false, false); usize::from(ec.k + ec.m)];

    // Pause before the first physical write, finish reservation release, then
    // resume using the original still-valid admission. This uses existing
    // explicit release, not a cleanup scanner supplied by the removed prototype.
    fixture
        .cluster
        .release_object_generation_reservation(
            &fixture.bucket,
            &fixture.key,
            &fixture.reservations[0],
        )
        .unwrap();
    for node_id in 0..3 {
        let node = fixture.map.node(NodeId::new(node_id)).unwrap();
        let pg = node.storage_node().get_pg(0).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &fixture.bucket,
                &fixture.key,
                &fixture.reservations[0],
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
    assert_eq!(
        shard_state(),
        absent,
        "release must leave no staged payload"
    );
    let encryption = crate::tests::metadata_only_test_encryption();
    let late = route.write_direct_object_payload(
        generation_reservation.bind_encryption(&encryption),
        DATA[0].len() as u64,
        DATA[0],
    );
    // Inspect before dropping any returned RAII payload: its compensation must
    // not mask the late write. Check physical state even when the call returns Err.
    assert_eq!(
        shard_state(),
        absent,
        "released generation must not recreate shards; write returned error={}; \
         shard observations are (durable acknowledgement, placed file)",
        late.is_err(),
    );
    assert!(
        late.is_err(),
        "released generation must reject a delayed staging write"
    );
}
