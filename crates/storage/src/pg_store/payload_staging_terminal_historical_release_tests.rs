// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Already-applied, source-bearing historical cleanup: release its exact
//! abandoned-finalizer proof, never its owner, pending slot or physical payload.

use super::*;
use crate::node_runtime::pg_store::{CompleteTerminalHandoff, TerminalReservationObservation as O};
use crate::node_runtime::traits::PgMetadataStore;

fn require_successful_release_reply(remote: &Remote) {
    remote
        .server
        .set_response_frame_test_hook(Arc::new(|kind, frame| {
            if kind == Kind::ProofRelease {
                crate::storage_rpc::decode_storage_rpc_response_payload(&frame.payload)
                    .unwrap()
                    .expect("release must commit before the reply is lost");
            }
        }));
}

fn retained_proof(mixed: &Mixed) -> BucketWriteReservationProof {
    let node = mixed.node(4);
    let pg = node
        .get_pg(mixed.fixture.context.id().pg_id().get())
        .unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = sql::load(
        &pg.conn,
        mixed.fixture.context.id().pg_id(),
        &fixture(multipart_target()).subject.id.attempt,
    )
    .unwrap()
    .unwrap();
    let ResourceResponsibility::ReleaseDue(resource) = owner.resources.finalization else {
        panic!("applied cleanup must retain the abandoned finalizer");
    };
    let identity = resource.identity;
    let proof = BucketWriteReservationProof {
        bucket: identity.bucket,
        reservation_id: identity.reservation_id,
        owner_token: identity.owner_token,
        cluster_epoch: identity.cluster_epoch,
        bucket_execution_generation: identity.execution_generation,
        bucket_incarnation_generation: identity.incarnation,
        operation_kind: identity.operation_kind,
        created_at: identity.created_at,
        lease_deadline: resource.observed_lease_deadline,
        target_context: identity.target_context,
    };
    assert_eq!(
        resource.release_pg.get(),
        topology().bucket_pg_for(&proof.bucket)
    );
    snapshot.finish().unwrap();
    proof
}

fn reservations(
    mixed: &Mixed,
    node: u32,
    proof: &BucketWriteReservationProof,
) -> (u64, Vec<crate::BucketWriteReservationRecord>) {
    let node = mixed.node(node);
    let pg = node
        .get_pg(topology().bucket_pg_for(&proof.bucket))
        .unwrap();
    (
        pg.conn.total_changes(),
        pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
    )
}

fn with_release(
    mixed: &Mixed,
    history: &str,
    action: impl FnOnce(
        &crate::cluster::StorageClusterRouteAdmission,
        &mut CompleteTerminalHandoff<'_>,
        &BucketWriteReservationProof,
    ),
) {
    with_release_until(mixed, history, deadline(), action);
}

fn with_release_until(
    mixed: &Mixed,
    history: &str,
    cutoff: Instant,
    action: impl FnOnce(
        &crate::cluster::StorageClusterRouteAdmission,
        &mut CompleteTerminalHandoff<'_>,
        &BucketWriteReservationProof,
    ),
) {
    retirement::with_terminal(mixed, history, cutoff, |admission, terminal| {
        assert!(terminal.work().retained_finalization_proof);
        assert!(!terminal.work().is_quiescent());
        assert_eq!(terminal.reservation_observation(), O::NotRequired);
        assert_eq!(
            terminal.retained_finalizer_reservation_observation(),
            O::NoAttemptObserved
        );
        let proof = retained_proof(mixed);
        let pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
        assert_ne!(pg, mixed.fixture.context.id().pg_id());
        assert_eq!(proof.cluster_epoch.get(), 6);
        assert!(proof.cluster_epoch < mixed.fixture.runtime_map.cluster_epoch());
        assert_eq!(
            mixed
                .fixture
                .map
                .metadata_pg_primary_node_for_retained_cleanup(proof.cluster_epoch, pg)
                .unwrap()
                .node_id(),
            NodeId::new(4)
        );
        assert_eq!(
            mixed.fixture.map.pg_route(pg).unwrap().primary_node_id(),
            NodeId::new(2)
        );
        // Matching proof on the replacement bucket primary is a canary: release
        // must use the exact historical bucket route, not either current PG.
        for id in [4, 2] {
            let node = mixed.node(id);
            let pg = node.get_pg(pg.get()).unwrap();
            terminal_release::seed_pg_reservations(&pg, &proof, None);
            // The metadata-only fixture creates the bucket outside command
            // apply; seal that setup before testing release and restart.
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let object_before = mixed.observe();
        let replacement_before = reservations(mixed, 2, &proof);
        terminal
            .test_require_release_context(&mixed.fixture.context)
            .unwrap();
        let crossed = ContextualCommand::decode_field(
            complete_log_range::ordinary(mixed.fixture.context.id()),
            &[1, 0, 0],
            &topology(),
        )
        .unwrap();
        assert!(terminal.test_require_release_context(&crossed).is_err());
        action(admission, terminal, &proof);
        assert_eq!(terminal.reservation_observation(), O::NotRequired);
        assert_eq!(
            mixed.observe(),
            object_before,
            "release must preserve exact pending indexes, log and all retained owners/resources"
        );
        assert_eq!(
            reservations(mixed, 2, &proof),
            replacement_before,
            "replacement primary must be untouched"
        );
    });
}

fn matrix(transport: &str) {
    for history in ["inspected", "installed", "uncertain"] {
        let endpoints = match transport {
            "embedded" => vec![(0, false), (2, true)],
            "unix" => vec![(4, false), (2, true)],
            "tls" => vec![(4, true), (0, false)],
            _ => unreachable!(),
        };
        let mixed = Mixed::with_finalizer_release(&endpoints);
        with_release(&mixed, history, |admission, terminal, proof| {
            let before = reservations(&mixed, 4, proof);
            let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
            let _running = remote.map(Remote::run);
            let replies = Arc::new(AtomicUsize::new(0));
            if let Some(remote) = remote {
                require_successful_release_reply(remote);
                let count = Arc::clone(&replies);
                remote
                    .server
                    .set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
                        if kind == Kind::ProofRelease && count.fetch_add(1, Ordering::SeqCst) == 0 {
                            *bytes.last_mut().unwrap() ^= 1;
                        }
                    }));
            }
            terminal.release_reservation().unwrap();
            assert_eq!(reservations(&mixed, 4, proof), before);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || terminal.test_interrupt_retained_finalizer_release()
            ))
            .is_err());
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::Unconfirmed
            );
            assert_eq!(reservations(&mixed, 4, proof), before);
            let (result, cutoffs) =
                UnixStorageNodeClient::test_observe_proof_release_deadlines(|| {
                    terminal.release_retained_finalizer_reservation()
                });
            if remote.is_some() {
                assert!(result.is_err());
                assert_eq!(cutoffs.len(), 1);
                assert_eq!(replies.load(Ordering::SeqCst), 1, "no hidden retry");
                assert_eq!(
                    terminal.retained_finalizer_reservation_observation(),
                    O::Unconfirmed
                );
            } else {
                result.unwrap();
                assert!(cutoffs.is_empty());
            }
            let after = reservations(&mixed, 4, proof);
            assert_eq!(after.0, before.0 + 1);
            assert_eq!(
                after.1,
                before
                    .1
                    .into_iter()
                    .filter(|p| p.reservation_id != proof.reservation_id)
                    .collect::<Vec<_>>()
            );
            assert_eq!(after.1.len(), 1, "independent reservation survives");
            let (result, retry_cutoffs) =
                UnixStorageNodeClient::test_observe_proof_release_deadlines(|| {
                    terminal.release_retained_finalizer_reservation()
                });
            result.unwrap();
            if remote.is_some() {
                assert_eq!(cutoffs, retry_cutoffs, "reopening cannot renew cutoff");
                assert_eq!(replies.load(Ordering::SeqCst), 2);
            }
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::Released
            );
            assert_eq!(
                reservations(&mixed, 4, proof),
                after,
                "exact absence does no writes"
            );
            terminal.release_retained_finalizer_reservation().unwrap();
            assert_eq!(reservations(&mixed, 4, proof), after);
            assert_eq!(
                replies.load(Ordering::SeqCst),
                if remote.is_some() { 2 } else { 0 }
            );
            // Proof release never closes outstanding physical work or permits
            // pending-slot removal, even after exact confirmation.
            let error = if let Some(remote) = remote {
                let session = terminal
                    .open_primary_session(&remote.client, deadline())
                    .unwrap();
                terminal
                    .retire_remote(&remote.client, &session, deadline())
                    .unwrap_err()
            } else {
                let route = admission
                    .embedded_historical_publication_route(
                        &mixed.fixture.runtime_map,
                        mixed.fixture.context.id().pg_id(),
                    )
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                terminal.retire_embedded(&section, deadline()).unwrap_err()
            };
            assert!(
                error.to_string().contains("terminal work integration"),
                "{error}"
            );
        });
    }
}

#[test]
fn staging_terminal_historical_finalizer_release_embedded_preserves_exact_custody() {
    matrix("embedded");
}
#[test]
fn staging_terminal_historical_finalizer_release_unix_preserves_exact_custody() {
    matrix("unix");
}
#[test]
fn staging_terminal_historical_finalizer_release_tls_preserves_exact_custody() {
    matrix("tls");
}

#[test]
fn staging_terminal_historical_finalizer_conflict_preserves_proof_and_retry_owner() {
    let mixed = Mixed::with_finalizer_release(&[]);
    with_release(&mixed, "uncertain", |_, terminal, proof| {
        let node = mixed.node(4);
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        pg.conn.execute("UPDATE bucket_write_reservations SET owner_token = 'crossed' WHERE reservation_id = ?1", [&proof.reservation_id]).unwrap();
        drop(pg);
        let before = reservations(&mixed, 4, proof);
        let error = terminal
            .release_retained_finalizer_reservation()
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "{error:?}"
        );
        assert_eq!(
            terminal.retained_finalizer_reservation_observation(),
            O::Unconfirmed
        );
        assert_eq!(reservations(&mixed, 4, proof), before);
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        pg.conn.execute("UPDATE bucket_write_reservations SET owner_token = ?1, lease_deadline = lease_deadline + 1000 WHERE reservation_id = ?2", [&proof.owner_token, &proof.reservation_id]).unwrap();
        drop(pg);
        terminal.release_retained_finalizer_reservation().unwrap();
        assert_eq!(
            terminal.retained_finalizer_reservation_observation(),
            O::Released
        );
        assert_eq!(
            reservations(&mixed, 4, proof).1.len(),
            1,
            "renewable lease evidence does not change stable identity"
        );
    });
}

#[test]
fn staging_terminal_historical_finalizer_missing_active_bucket_history_fails_closed() {
    let mixed = Mixed::new(2, &[]);
    retirement::with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
        let proof = retained_proof(&mixed);
        let node = mixed.node(4);
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        terminal_release::seed_pg_reservations(&pg, &proof, None);
        drop(pg);
        let before = reservations(&mixed, 4, &proof);
        let objects = mixed.observe();
        let error = terminal
            .release_retained_finalizer_reservation()
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Store(StoreError::PgNotActive { .. })
            ),
            "{error:?}"
        );
        assert_eq!(
            terminal.retained_finalizer_reservation_observation(),
            O::Unconfirmed
        );
        assert_eq!(reservations(&mixed, 4, &proof), before);
        assert_eq!(mixed.observe(), objects);
    });
}

#[test]
fn staging_terminal_historical_finalizer_frozen_route_and_work_cutoffs_reject_before_dispatch() {
    for kind in ["route", "work"] {
        for endpoints in [vec![], vec![(4, false)], vec![(4, true)]] {
            let mixed = Mixed::with_finalizer_release(&endpoints);
            let now = crate::clock::current_time_millis();
            let monotonic = crate::clock::monotonic_time_millis();
            let cutoff = if kind == "work" {
                Instant::now() + Duration::from_secs(5)
            } else {
                deadline()
            };
            with_release_until(&mixed, "uncertain", cutoff, |admission, terminal, proof| {
                let before = reservations(&mixed, 4, proof);
                let advance = if kind == "work" { 6_000 } else { 15_000 };
                crate::clock::with_time_and_monotonic_override(
                    now + advance,
                    monotonic + advance,
                    || {
                        mixed.fixture.map.test_store_route_map_validity(
                            RouteMapValidity::until_ms(now + 120_000).unwrap(),
                        );
                        admission.require_valid_now().unwrap();
                        let error = terminal
                            .release_retained_finalizer_reservation()
                            .unwrap_err();
                        assert_eq!(
                            terminal.retained_finalizer_reservation_observation(),
                            O::NoAttemptObserved
                        );
                        assert!(
                            matches!(
                                error,
                                crate::BucketSnapshotLoadError::Store(
                                    StoreError::OperationDeadlineExceeded { .. }
                                        | StoreError::RouteMapExpired { .. }
                                )
                            ),
                            "{kind}: {error:?}"
                        );
                    },
                );
                assert_eq!(reservations(&mixed, 4, proof), before);
            });
        }
    }
}

#[test]
fn staging_terminal_historical_finalizer_expiry_inside_release_transaction_keeps_custody() {
    for transport in ["embedded", "unix", "tls"] {
        let endpoints = if transport == "embedded" {
            vec![]
        } else {
            vec![(4, transport == "tls")]
        };
        let mixed = Mixed::with_finalizer_release(&endpoints);
        let cutoff = Instant::now() + Duration::from_secs(5);
        with_release_until(&mixed, "uncertain", cutoff, |_, terminal, proof| {
            let before = reservations(&mixed, 4, proof);
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let hook_reached = Arc::clone(&reached);
            let map = Arc::clone(&mixed.fixture.map);
            let hook = move |transaction: bool, deadline: Option<Instant>| {
                assert!(transaction);
                let deadline = deadline.expect("same frozen cutoff reaches SQL");
                assert!(deadline <= cutoff);
                assert!(Instant::now() < deadline, "expiry must occur beside DELETE");
                hook_reached.store(true, Ordering::SeqCst);
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
                    terminal.release_retained_finalizer_reservation()
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
                let replies = peer.count(Kind::ProofRelease);
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        PgStore::test_with_before_proof_release_effect_hook(hook, || {
                            peer.server.accept_one().unwrap()
                        })
                    });
                    let wake = WakeAcceptOnDrop(Some(peer));
                    let error = terminal
                        .release_retained_finalizer_reservation()
                        .unwrap_err();
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
                        replies.load(Ordering::SeqCst),
                        1,
                        "observe server rejection, not client timeout"
                    );
                    wake.disarm();
                    worker.join().unwrap();
                });
            }
            assert!(reached.load(Ordering::SeqCst));
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::Unconfirmed
            );
            assert_eq!(
                reservations(&mixed, 4, proof),
                before,
                "no DELETE or hidden retry"
            );
        });
    }
}

#[test]
fn staging_terminal_historical_finalizer_restart_reconfirms_lost_release_from_retained_source() {
    for tls in [false, true] {
        let mixed = Mixed::with_finalizer_release(&[(4, tls)]);
        with_release(&mixed, "uncertain", |_, terminal, proof| {
            let peer = mixed.peer(4);
            require_successful_release_reply(peer);
            let count = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&count);
            peer.server
                .set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
                    if kind == Kind::ProofRelease {
                        observed.fetch_add(1, Ordering::SeqCst);
                        *bytes.last_mut().unwrap() ^= 1;
                    }
                }));
            let _running = peer.run();
            assert!(terminal.release_retained_finalizer_reservation().is_err());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::Unconfirmed
            );
            assert_eq!(reservations(&mixed, 4, proof).1.len(), 1);
        });
        let before = mixed.observe();
        let mixed = lifetime::reopen(mixed);
        for ((after, _), (before, _)) in mixed.observe().iter().zip(&before) {
            assert_eq!(after, before);
        }
        // Reinspection recovers the still-pending exact source-bearing context,
        // not the previous process's confirmation or a reconstructed raw proof.
        retirement::with_terminal(&mixed, "inspected", deadline(), |_, terminal| {
            let proof = retained_proof(&mixed);
            let before = reservations(&mixed, 4, &proof);
            assert_eq!(before.1.len(), 1);
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::NoAttemptObserved
            );
            let peer = mixed.peer(4);
            let count = peer.count(Kind::ProofRelease);
            let _running = peer.run();
            terminal.release_retained_finalizer_reservation().unwrap();
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                O::Released
            );
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(
                reservations(&mixed, 4, &proof),
                before,
                "absence confirmation must not rewrite state"
            );
        });
    }
}
