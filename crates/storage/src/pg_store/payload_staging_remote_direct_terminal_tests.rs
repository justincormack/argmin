// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact terminal cleanup retains the original section, not frontend file work.
use super::*;
use crate::node_runtime::pg_store::{
    TerminalReservationObservation as Reservation, TerminalSlotRetirementObservation as Retired,
};
use crate::node_runtime::traits::PgMetadataStore;

const RETIRE: Kind = Kind::MetadataCommandPendingSlotRemove;
const RELEASE: Kind = Kind::ProofRelease;

fn reservations(
    f: &Mixed,
    proof: &BucketWriteReservationProof,
) -> Vec<crate::BucketWriteReservationRecord> {
    let pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
    let node = f.node(f.map.pg_route(pg).unwrap().primary_node_id());
    let records = node
        .get_pg(pg.get())
        .unwrap()
        .durable_bucket_write_reservations(&proof.bucket)
        .unwrap();
    records
}

macro_rules! with_terminal {
    ($endpoints:expr, $origin:expr, |$f:ident, $primary:ident, $session:ident, $terminal:ident, $owner:ident, $shards:ident, $command:ident, $proof:ident| $body:block) => {
        with_mixed_publication!(@endpoints $endpoints, $origin, |$f, $session, publication, $owner, $shards, $command| {
            let $proof = $command.payload().primary_bucket_write_reservation_proof().unwrap();
            let bucket_pg = PgId::new(topology().bucket_pg_for(&$proof.bucket));
            assert_ne!(bucket_pg, $command.id().pg_id());
            {
                let node = $f.node($f.map.pg_route(bucket_pg).unwrap().primary_node_id());
                let pg = node.get_pg(bucket_pg.get()).unwrap();
                terminal_release::seed_pg_reservations(&pg, $proof, None);
                pg.refresh_metadata_command_state_digest().unwrap();
            }
            let $primary = $f.remote(NodeId::new(4)).unwrap();
            publication.publish_remote(&$primary.client, &$session).unwrap();
            for n in [0, 4, 2, 3, 1] {
                if let Some(peer) = $f.remote(NodeId::new(n)) {
                    publication.apply_remote(&$primary.client, &$session, &peer.client).unwrap();
                } else {
                    publication.apply_embedded_replica(&$primary.client, &$session, NodeId::new(n)).unwrap();
                }
            }
            let id = publication.id();
            let cutoff = publication.test_cutoff();
            let installed = publication.test_installation();
            let before = $f.observe();
            let mut $terminal = publication.into_remote_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!($terminal.id(), id);
            assert_eq!($terminal.test_cutoff(), cutoff);
            assert_eq!($terminal.test_installation(), installed);
            assert_eq!($terminal.reservation(), Reservation::NoAttemptObserved);
            assert_eq!($terminal.retirement(), Retired::NoAttemptObserved);
            assert_eq!($terminal.work().reservation, crate::node_runtime::pg_store::payload_staging::TerminalReservationWork::ReleaseCommandProof);
            for n in 0..5 { assert_eq!($terminal.application(NodeId::new(n)), Applied::Applied); }
            assert_eq!($f.observe(), before, "handoff is effect-free");
            $body
        });
    };
}

macro_rules! succeeds {
    ($endpoints:expr, $origin:expr) => {
        with_terminal!(
            $endpoints,
            $origin,
            |f, primary, session, terminal, owner, shards, command, proof| {
                let before = f.observe();
                let reserved = reservations(f, proof);
                assert_eq!(reserved.len(), 2);
                assert!(terminal.retire(&primary.client, &session).is_err());
                assert_eq!(terminal.retirement(), Retired::NoAttemptObserved);
                terminal
                    .release_reservation(&primary.client, &session)
                    .unwrap();
                assert_eq!(terminal.reservation(), Reservation::Released);
                let remaining = reservations(f, proof);
                assert_eq!(
                    remaining,
                    reserved
                        .into_iter()
                        .filter(|r| !proof.matches_record(r))
                        .collect::<Vec<_>>()
                );
                assert_eq!(remaining.len(), 1);
                assert_eq!(
                    f.observe(),
                    before,
                    "bucket release cannot change object-PG state"
                );
                terminal
                    .release_reservation(&primary.client, &session)
                    .unwrap();
                assert_eq!(reservations(f, proof), remaining);
                terminal.retire(&primary.client, &session).unwrap();
                assert_eq!(terminal.retirement(), Retired::Retired);
                let retired = f.observe();
                assert_ne!(retired[4], before[4]);
                assert_eq!(retired[..4], before[..4]);
                terminal.retire(&primary.client, &session).unwrap();
                assert_eq!(f.observe(), retired);
                for n in 0..5 {
                    let node = f.node(NodeId::new(n));
                    let pg = node.get_pg(command.id().pg_id().get()).unwrap();
                    object::assert_object(&pg, &command);
                    let snapshot = pg.command_build_snapshot().unwrap();
                    assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                    snapshot.finish().unwrap();
                    if n == 4 {
                        assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                    }
                }
                drop(terminal);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), retired);
                assert_eq!(reservations(f, proof), remaining);
            }
        );
    };
}

#[test]
fn staging_remote_direct_terminal_release_holds_primary_until_bucket_confirmation() {
    for tls in [false, true] {
        with_terminal!(
            std::array::from_fn::<_, 5, _>(|i| (i as u32, tls)),
            "installed",
            |f, primary, session, terminal, owner, shards, _command, proof| {
                let bucket_pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
                let peer = f
                    .remote(f.map.pg_route(bucket_pg).unwrap().primary_node_id())
                    .unwrap();
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let release_rx = std::sync::Mutex::new(release_rx);
                peer.server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == RELEASE {
                            ready_tx.send(()).unwrap();
                            release_rx
                                .lock()
                                .unwrap()
                                .recv_timeout(std::time::Duration::from_secs(5))
                                .unwrap();
                        }
                    }));
                std::thread::scope(|scope| {
                    let session_ref = &session;
                    let worker = scope.spawn(move || {
                        ready_rx
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap();
                        let error = session_ref
                            .borrow_complete_primary(
                                &primary.client,
                                f.context.id().pg_id(),
                                f.context.id().cluster_epoch(),
                                Instant::now() + std::time::Duration::from_millis(30),
                            )
                            .err();
                        release_tx.send(()).unwrap();
                        assert!(error
                            .expect("release must retain original primary borrow")
                            .to_string()
                            .contains("borrow complete primary session"));
                    });
                    terminal
                        .release_reservation(&primary.client, &session)
                        .unwrap();
                    worker.join().unwrap();
                });
                assert_eq!(terminal.reservation(), Reservation::Released);
                let after = f.observe();
                drop(terminal);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), after);
                assert_eq!(reservations(f, proof).len(), 1);
            }
        );
    }
}

#[test]
fn staging_remote_direct_terminal_failed_and_ambiguous_release_keeps_exact_obligation() {
    for tls in [false, true] {
        with_terminal!(
            std::array::from_fn::<_, 5, _>(|i| (i as u32, tls)),
            "uncertain",
            |f, primary, session, terminal, owner, shards, _command, proof| {
                let bucket_pg = PgId::new(topology().bucket_pg_for(&proof.bucket));
                let peer = f
                    .remote(f.map.pg_route(bucket_pg).unwrap().primary_node_id())
                    .unwrap();
                let node = f.node(peer.node);
                let before = f.observe();
                let reserved = reservations(f, proof);
                node.get_pg(bucket_pg.get()).unwrap().conn.execute_batch("CREATE TEMP TRIGGER refuse_direct_release BEFORE DELETE ON bucket_write_reservations BEGIN SELECT RAISE(ABORT, 'direct release fault'); END;").unwrap();
                assert!(terminal
                    .release_reservation(&primary.client, &session)
                    .is_err());
                assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
                assert_eq!(reservations(f, proof), reserved);
                assert!(terminal.retire(&primary.client, &session).is_err());
                node.get_pg(bucket_pg.get())
                    .unwrap()
                    .conn
                    .execute_batch("DROP TRIGGER refuse_direct_release")
                    .unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                peer.server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == RELEASE && count.fetch_add(1, Ordering::SeqCst) == 0 {
                            frame.payload = rpc::encode_storage_rpc_error_response(
                                &rpc::StorageRpcErrorResponse {
                                    code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                    message: "lost direct release confirmation".into(),
                                },
                            )
                            .unwrap();
                            rpc::set_storage_rpc_response_connection_reusable(
                                &mut frame.payload,
                                true,
                            )
                            .unwrap();
                        }
                    }));
                assert!(terminal
                    .release_reservation(&primary.client, &session)
                    .is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
                let remaining = reservations(f, proof);
                assert_eq!(remaining.len(), 1);
                assert!(!remaining.iter().any(|r| proof.matches_record(r)));
                assert!(terminal.retire(&primary.client, &session).is_err());
                crate::clock::with_time_override(terminal.test_cutoff().1, || {
                    assert!(terminal
                        .release_reservation(&primary.client, &session)
                        .is_err());
                });
                assert_eq!(terminal.reservation(), Reservation::Unconfirmed);
                terminal
                    .release_reservation(&primary.client, &session)
                    .unwrap();
                assert_eq!(terminal.reservation(), Reservation::Released);
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                terminal
                    .release_reservation(&primary.client, &session)
                    .unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                assert_eq!(reservations(f, proof), remaining);
                assert_eq!(f.observe(), before);
                drop(terminal);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), before);
            }
        );
    }
}

#[test]
fn staging_remote_direct_terminal_releases_exact_proof_and_retires_without_payload_cleanup() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            succeeds!(std::array::from_fn::<_, 5, _>(|i| (i as u32, tls)), origin);
            succeeds!([(0, tls), (2, tls), (3, tls), (4, tls)], origin);
        }
    }
}

#[test]
fn staging_remote_direct_terminal_handoff_requires_every_confirmation_and_original_cutoff() {
    for tls in [false, true] {
        with_mixed_publication!(
            tls,
            "uncertain",
            |f, session, publication, owner, shards, _command| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let history = publication.test_binding_state();
                let cutoff = publication.test_cutoff();
                for next in [None, Some(0), Some(4), Some(2), Some(3), Some(1)] {
                    let before = f.observe();
                    let failure = publication
                        .into_remote_terminal()
                        .err()
                        .expect("incomplete cohort");
                    assert!(failure.error().to_string().contains("confirmed"));
                    publication = failure.into_publication();
                    assert_eq!(publication.test_binding_state(), history);
                    assert_eq!(publication.test_cutoff(), cutoff);
                    assert_eq!(f.observe(), before);
                    match next {
                        None => publication
                            .publish_remote(&primary.client, &session)
                            .unwrap(),
                        Some(1) => {
                            assert!(publication
                                .test_apply_embedded_replica(
                                    &primary.client,
                                    &session,
                                    embedded(),
                                    |stage| {
                                        if stage == "after-commit" {
                                            return Err(StoreError::OperationDeadlineExceeded {
                                                context: "last direct replica reply lost",
                                            });
                                        }
                                        Ok(())
                                    }
                                )
                                .is_err());
                        }
                        Some(n) => {
                            publication
                                .apply_remote(
                                    &primary.client,
                                    &session,
                                    &f.remote(NodeId::new(n)).unwrap().client,
                                )
                                .unwrap();
                        }
                    }
                }
                let before = f.observe();
                let failure = publication
                    .into_remote_terminal()
                    .err()
                    .expect("unconfirmed final replica");
                publication = failure.into_publication();
                assert_eq!(publication.application(embedded()), Applied::Unconfirmed);
                publication
                    .apply_embedded_replica(&primary.client, &session, embedded())
                    .unwrap();
                assert_eq!(f.observe(), before);
                let failure = crate::clock::with_time_override(cutoff.1, || {
                    publication
                        .into_remote_terminal()
                        .err()
                        .expect("expired handoff")
                });
                publication = failure.into_publication();
                assert_eq!(publication.test_binding_state(), history);
                assert_eq!(publication.test_cutoff(), cutoff);
                for n in 0..5 {
                    assert_eq!(publication.application(NodeId::new(n)), Applied::Applied);
                }
                drop(
                    publication
                        .into_remote_terminal()
                        .unwrap_or_else(|e| panic!("{}", e.error())),
                );
                assert_eq!(f.observe(), before);
                require_payload(f, owner, shards, true);
            }
        );
    }
}

#[test]
fn staging_remote_direct_terminal_rejects_crossed_closed_reopened_and_expired_sessions() {
    for tls in [false, true] {
        for phase in 0..3 {
            with_terminal!(
                [(0, tls), (2, tls), (3, tls), (4, tls)],
                "installed",
                |f, primary, session, terminal, owner, shards, _command, proof| {
                    if phase >= 1 {
                        terminal
                            .release_reservation(&primary.client, &session)
                            .unwrap();
                    }
                    if phase == 2 {
                        terminal.retire(&primary.client, &session).unwrap();
                    }
                    let observed = (terminal.reservation(), terminal.retirement());
                    let before = f.observe();
                    let reserved = reservations(f, proof);
                    let other = &f.remote(NodeId::new(0)).unwrap().client;
                    assert!(terminal.release_reservation(other, &session).is_err());
                    assert!(terminal.retire(other, &session).is_err());
                    crate::clock::with_time_override(terminal.test_cutoff().1, || {
                        assert!(terminal
                            .release_reservation(&primary.client, &session)
                            .is_err());
                        assert!(terminal.retire(&primary.client, &session).is_err());
                    });
                    session.test_close_primary_section();
                    assert!(terminal
                        .release_reservation(&primary.client, &session)
                        .is_err());
                    let replacement = primary
                        .client
                        .open_metadata_command_critical_section_until(
                            f.context.id().pg_id(),
                            deadline(),
                        )
                        .unwrap();
                    assert!(matches!(
                        terminal.release_reservation(&primary.client, &replacement),
                        Err(crate::BucketSnapshotLoadError::Store(
                            StoreError::RouteCapabilitySubjectMismatch {
                                operation:
                                    "confirmed window requires its original remote primary session"
                            }
                        ))
                    ));
                    assert!(terminal
                        .retire(&primary.client, &replacement)
                        .unwrap_err()
                        .to_string()
                        .contains("original remote primary session"));
                    assert_eq!((terminal.reservation(), terminal.retirement()), observed);
                    drop(terminal);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), before);
                    assert_eq!(reservations(f, proof), reserved);
                }
            );
        }
    }
}

#[test]
fn staging_remote_direct_terminal_ambiguous_retirement_preserves_history_and_exact_absence_retry() {
    for tls in [false, true] {
        for fault in ["empty", "error", "closed-error"] {
            with_terminal!(
                [(0, tls), (2, tls), (3, tls), (4, tls)],
                "installed",
                |f, primary, session, terminal, owner, shards, _command, proof| {
                    terminal
                        .release_reservation(&primary.client, &session)
                        .unwrap();
                    let reserved = reservations(f, proof);
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == RETIRE && count.fetch_add(1, Ordering::SeqCst) == 0 {
                                frame.payload = if fault == "empty" {
                                    rpc::encode_storage_rpc_success_response(&[])
                                } else {
                                    rpc::encode_storage_rpc_error_response(
                                        &rpc::StorageRpcErrorResponse {
                                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                            message: "after direct retirement".into(),
                                        },
                                    )
                                    .unwrap()
                                };
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    fault != "closed-error",
                                )
                                .unwrap();
                            }
                        }));
                    assert!(terminal.retire(&primary.client, &session).is_err());
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                    let after = f.observe();
                    if fault == "closed-error" {
                        session.test_close_primary_section();
                        let replacement = primary
                            .client
                            .open_metadata_command_critical_section_until(
                                f.context.id().pg_id(),
                                deadline(),
                            )
                            .unwrap();
                        assert!(terminal.retire(&primary.client, &replacement).is_err());
                        assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                    } else {
                        terminal.retire(&primary.client, &session).unwrap();
                        assert_eq!(terminal.retirement(), Retired::Retired);
                        assert_eq!(calls.load(Ordering::SeqCst), 2);
                        terminal.retire(&primary.client, &session).unwrap();
                        assert_eq!(calls.load(Ordering::SeqCst), 2);
                    }
                    assert_eq!(f.observe(), after);
                    drop(terminal);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), after);
                    assert_eq!(reservations(f, proof), reserved);
                }
            );
        }
    }
}
