// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact historical command-proof conflicts and immutable effect cutoffs.

use super::*;

fn endpoints(transport: &str) -> Vec<(u32, bool)> {
    match transport {
        "embedded" => vec![],
        "unix" => vec![(4, false)],
        "tls" => vec![(4, true)],
        _ => unreachable!(),
    }
}

#[test]
fn staging_historical_command_release_conflict_preserves_owner_and_later_exact_retry() {
    for transport in ["embedded", "unix", "tls"] {
        let (mixed, proof) = setup(Case::AbortPut, &endpoints(transport));
        let proof = proof.unwrap();
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            seed(&mixed, &proof);
            let node = mixed.node(4);
            let pg_id = topology().bucket_pg_for(&proof.bucket);
            node.get_pg(pg_id)
                .unwrap()
                .conn
                .execute(
                    "UPDATE bucket_write_reservations SET owner_token = 'crossed' \
                 WHERE reservation_id = ?1",
                    [&proof.reservation_id],
                )
                .unwrap();
            let before = reservations(&mixed, 4, &proof);
            let objects = mixed.observe();
            let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
            let count = remote.map(|p| p.count(Kind::ProofRelease));
            let _running = remote.map(Remote::run);
            let error = terminal.release_reservation().unwrap_err();
            assert!(
                matches!(
                    error,
                    crate::BucketSnapshotLoadError::Metadata(
                        crate::MetadataError::BucketWriteReservationConflict { .. }
                    )
                ),
                "{error:?}"
            );
            assert_eq!(terminal.reservation_observation(), O::Unconfirmed);
            assert_eq!(reservations(&mixed, 4, &proof), before);
            assert_eq!(mixed.observe(), objects);
            node.get_pg(pg_id)
                .unwrap()
                .conn
                .execute(
                    "UPDATE bucket_write_reservations SET owner_token = ?1, \
                 lease_deadline = lease_deadline + 1000 WHERE reservation_id = ?2",
                    [&proof.owner_token, &proof.reservation_id],
                )
                .unwrap();
            terminal.release_reservation().unwrap();
            assert_eq!(terminal.reservation_observation(), O::Released);
            assert_eq!(reservations(&mixed, 4, &proof).1.len(), 1);
            if let Some(count) = count {
                assert_eq!(count.load(Ordering::SeqCst), 2);
            }
            assert_eq!(mixed.observe(), objects);
        });
    }
}

#[test]
fn staging_historical_command_release_work_and_route_expiry_cannot_borrow_renewed_authority() {
    for bound in ["work", "route"] {
        for transport in ["embedded", "unix", "tls"] {
            let (mixed, proof) = setup(Case::ReplacePart, &endpoints(transport));
            let proof = proof.unwrap();
            let now = crate::clock::current_time_millis();
            let monotonic = crate::clock::monotonic_time_millis();
            let cutoff = if bound == "work" {
                Instant::now() + Duration::from_secs(5)
            } else {
                deadline()
            };
            with_terminal(&mixed, "uncertain", cutoff, |admission, terminal| {
                seed(&mixed, &proof);
                let before = reservations(&mixed, 4, &proof);
                let objects = mixed.observe();
                let advance = if bound == "work" { 6_000 } else { 15_000 };
                crate::clock::with_time_and_monotonic_override(
                    now + advance,
                    monotonic + advance,
                    || {
                        mixed.fixture.map.test_store_route_map_validity(
                            RouteMapValidity::until_ms(now + 120_000).unwrap(),
                        );
                        admission.require_valid_now().unwrap();
                        let error = terminal.release_reservation().unwrap_err();
                        assert!(
                            matches!(
                                error,
                                crate::BucketSnapshotLoadError::Store(
                                    StoreError::OperationDeadlineExceeded { .. }
                                        | StoreError::RouteMapExpired { .. }
                                )
                            ),
                            "{error:?}"
                        );
                        assert_eq!(terminal.reservation_observation(), O::NoAttemptObserved);
                    },
                );
                assert_eq!(reservations(&mixed, 4, &proof), before);
                assert_eq!(mixed.observe(), objects);
            });
        }
    }
}

#[test]
fn staging_historical_command_release_expiry_beside_sql_delete_preserves_every_obligation() {
    for transport in ["embedded", "unix", "tls"] {
        let (mixed, proof) = setup(Case::AbortPut, &endpoints(transport));
        let proof = proof.unwrap();
        let cutoff = Instant::now() + Duration::from_secs(5);
        with_terminal(&mixed, "uncertain", cutoff, |_, terminal| {
            seed(&mixed, &proof);
            let before = reservations(&mixed, 4, &proof);
            let objects = mixed.observe();
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let called = Arc::clone(&reached);
            let map = Arc::clone(&mixed.fixture.map);
            let hook = move |transaction: bool, deadline: Option<Instant>| {
                assert!(transaction);
                let deadline = deadline.expect("immutable deadline reaches SQL");
                assert!(deadline <= cutoff);
                assert!(
                    Instant::now() < deadline,
                    "expiry must occur after entering SQL"
                );
                called.store(true, Ordering::SeqCst);
                map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(crate::clock::current_time_millis() + 60_000)
                        .unwrap(),
                );
                while let Some(left) = deadline.checked_duration_since(Instant::now()) {
                    std::thread::sleep(left);
                }
            };
            if transport == "embedded" {
                let error = PgStore::test_with_before_proof_release_effect_hook(hook, || {
                    terminal.release_reservation()
                })
                .unwrap_err();
                assert!(
                    matches!(
                        error,
                        crate::BucketSnapshotLoadError::Store(
                            StoreError::OperationDeadlineExceeded {
                                context: "release metadata command bucket write reservation"
                            }
                        )
                    ),
                    "{error:?}"
                );
            } else {
                let peer = mixed.peer(4);
                let count = peer.count(Kind::ProofRelease);
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        PgStore::test_with_before_proof_release_effect_hook(hook, || {
                            peer.server.accept_one().unwrap()
                        })
                    });
                    let wake = WakeAcceptOnDrop(Some(peer));
                    let error = terminal.release_reservation().unwrap_err();
                    assert!(
                        matches!(
                            error,
                            crate::BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                                failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                                ..
                            })
                        ),
                        "{error:?}"
                    );
                    assert_eq!(
                        count.load(Ordering::SeqCst),
                        1,
                        "actual server rejection, not client timeout"
                    );
                    wake.disarm();
                    worker.join().unwrap();
                });
            }
            assert!(reached.load(Ordering::SeqCst));
            assert_eq!(terminal.reservation_observation(), O::Unconfirmed);
            assert_eq!(reservations(&mixed, 4, &proof), before);
            assert_eq!(mixed.observe(), objects);
        });
    }
}
