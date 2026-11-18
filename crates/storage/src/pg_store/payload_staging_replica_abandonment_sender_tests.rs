// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::{
    CompleteAbandonmentSendError as E, CompleteUnstartedAbandonment,
    ReplicaAbandonmentObservation as R,
};
use crate::storage_rpc::{
    self as rpc, complete_replica_abandonment as wire, StorageRpcMessageKind as Kind,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandRecordAbandoned;

fn with_embedded_primary(
    tls: bool,
    case: usize,
    origin: &str,
    cutoff: Instant,
    action: impl FnOnce(&Mixed, &mut CompleteUnstartedAbandonment<'_>, &EmbeddedPublicationSection<'_>),
) {
    let dir = test_util::tempdir();
    let mixed = Mixed::with_endpoints(&dir, case, [(0, tls), (2, !tls)]);
    let id = mixed.context.id();
    let admission = mixed.handle.admit_current_route().unwrap();
    let node = mixed.node(NodeId::new(4));
    let source = abandonment_remote::sender(
        &node.get_pg(id.pg_id().get()).unwrap(),
        &mixed.context,
        origin,
        cutoff,
    );
    let mut owner = source
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    action(&mixed, &mut owner, &section);
}

fn count(peer: &Remote) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    peer.server
        .set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                seen.fetch_add(1, Ordering::SeqCst);
            }
        }));
    count
}

#[test]
fn staging_replica_abandonment_sender_embedded_primary_unix_all_contexts_origins() {
    embedded_matrix(false);
}
#[test]
fn staging_replica_abandonment_sender_embedded_primary_tls_all_contexts_origins() {
    embedded_matrix(true);
}

fn embedded_matrix(tls: bool) {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            with_embedded_primary(tls, case, origin, deadline(), |mixed, owner, section| {
                let primary_history = owner.installation_observation();
                let before = mixed.observe();
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                assert!(matches!(
                    owner.record_remote_replica_from_embedded_primary(
                        section,
                        &peer.client,
                        deadline()
                    ),
                    Err(E::NotSent(_))
                ));
                assert_eq!(owner.replica_observation(peer.node), R::NoAttemptObserved);
                assert_eq!(mixed.observe(), before);
                owner.record_primary_embedded(section, deadline()).unwrap();
                let shadows = mixed.shadows();
                let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                for node_id in [0, 2] {
                    let peer = mixed.remote(NodeId::new(node_id)).unwrap();
                    let count = count(peer);
                    let node = mixed.node(peer.node);
                    let pg = node.get_pg(mixed.context.id().pg_id().get()).unwrap();
                    if node_id == 2 {
                        install(&pg, &mixed.context);
                    }
                    let retained = (introduction::state(&pg), complete_pending::observe(&pg));
                    drop(pg);
                    let before = mixed.observe();
                    owner
                        .record_remote_replica_from_embedded_primary(
                            section,
                            &peer.client,
                            deadline(),
                        )
                        .unwrap();
                    assert_eq!(owner.replica_observation(peer.node), R::TombstoneConfirmed);
                    assert_eq!(owner.installation_observation(), primary_history);
                    let after = mixed.observe();
                    assert_ne!(after[node_id as usize], before[node_id as usize]);
                    for n in (0..5).filter(|n| *n != node_id as usize) {
                        assert_eq!(after[n], before[n]);
                    }
                    let pg = node.get_pg(mixed.context.id().pg_id().get()).unwrap();
                    assert_eq!(
                        (introduction::state(&pg), complete_pending::observe(&pg)),
                        retained
                    );
                    drop(pg);
                    owner
                        .record_remote_replica_from_embedded_primary(
                            section,
                            &peer.client,
                            deadline(),
                        )
                        .unwrap();
                    assert_eq!(
                        count.load(Ordering::SeqCst),
                        1,
                        "confirmed member must not redispatch"
                    );
                    assert_eq!(mixed.observe(), after);
                }
                assert_eq!(mixed.shadows(), shadows);
            });
        }
    }
}

#[test]
fn staging_replica_abandonment_sender_remote_primary_unix_all_contexts_origins() {
    remote_matrix(false);
}
#[test]
fn staging_replica_abandonment_sender_remote_primary_tls_all_contexts_origins() {
    remote_matrix(true);
}

fn remote_matrix(tls: bool) {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            abandonment_remote::with_owner(tls, case, origin, |mixed, owner| {
                let primary = mixed.remote(NodeId::new(4)).unwrap();
                let replica = mixed.remote(NodeId::new(0)).unwrap();
                let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                let count = count(replica);
                let before = mixed.observe();
                assert!(matches!(
                    owner.record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline()
                    ),
                    Err(E::NotSent(_))
                ));
                assert_eq!(mixed.observe(), before);
                assert_eq!(count.load(Ordering::SeqCst), 0);
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                let before = mixed.observe();
                assert!(matches!(
                    owner.record_remote_replica_from_remote_primary(
                        &replica.client,
                        &session,
                        &replica.client,
                        deadline()
                    ),
                    Err(E::NotSent(_))
                ));
                assert!(matches!(
                    owner.record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &primary.client,
                        deadline()
                    ),
                    Err(E::NotSent(_))
                ));
                assert_eq!(mixed.observe(), before);
                let history = owner.installation_observation();
                owner
                    .record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(
                    owner.replica_observation(replica.node),
                    R::TombstoneConfirmed
                );
                assert_eq!(owner.installation_observation(), history);
                let after = mixed.observe();
                assert_ne!(after[0], before[0]);
                for n in 1..5 {
                    assert_eq!(after[n], before[n]);
                }
                owner
                    .record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(count.load(Ordering::SeqCst), 1);
                assert_eq!(mixed.observe(), after);
                // The same holder remains able to converge an embedded member.
                owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(
                    owner.replica_observation(NodeId::new(2)),
                    R::TombstoneConfirmed
                );
            });
        }
    }
}

#[test]
fn staging_replica_abandonment_sender_uncertainty_cutoff_and_transport_cannot_be_detached() {
    with_embedded_primary(
        false,
        2,
        "uncertain",
        deadline(),
        |mixed, owner, section| {
            owner.record_primary_embedded(section, deadline()).unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let before = mixed.observe();
            let reply = wire::encode_confirmation(
                NodeId::new(4),
                rpc::complete_abandonment::Confirmation {
                    node_id: peer.node,
                    id: mixed.context.id(),
                    command_sha256: mixed.context.encode_command().unwrap().command_sha256(),
                },
            );
            let failure = || StoreError::OperationDeadlineExceeded {
                context: "injected replica abandonment send",
            };
            let mut frozen = None;
            assert!(matches!(
                owner.test_remote_replica_attempt(section, &peer.client, deadline(), |a| {
                    frozen = Some((a.encode_request().unwrap(), a.deadline()));
                    Err(a.not_sent(failure()))
                }),
                Err(E::NotSent(_))
            ));
            assert_eq!(owner.replica_observation(peer.node), R::NoAttemptObserved);
            let (bytes, cutoff) = frozen.unwrap();
            assert!(matches!(
                owner.test_remote_replica_attempt(
                    section,
                    &peer.client,
                    deadline() + Duration::from_secs(600),
                    |a| {
                        assert_eq!(a.encode_request().unwrap(), bytes);
                        assert_eq!(a.deadline(), cutoff);
                        // A separately decoded identical command cannot replace the attempt's
                        // captured transport, including with a newly extended wire cutoff.
                        let decoded = replica_abandonment_receiver::encode(
                            &mixed.context,
                            NodeId::new(4),
                            peer.node,
                            crate::clock::current_time_millis() + 600_000,
                        );
                        let query =
                            crate::node_runtime::pg_store::CompleteReplicaAbandonmentQuery::decode(
                                &decoded,
                                &topology(),
                            )
                            .unwrap();
                        let request = query.request().unwrap();
                        let other = request
                            .publication_request()
                            .prepare_transport(deadline() + Duration::from_secs(600));
                        assert!(other
                            .encode_replica_abandonment_request(&a)
                            .unwrap_err()
                            .to_string()
                            .contains("exact attempt transport"));
                        Err(a.unconfirmed(failure()))
                    }
                ),
                Err(E::Unconfirmed(_))
            ));
            assert!(matches!(
                owner.test_remote_replica_attempt(section, &peer.client, deadline(), |a| Err(
                    a.not_sent(failure())
                )),
                Err(E::NotSent(_))
            ));
            assert_eq!(owner.replica_observation(peer.node), R::Unconfirmed);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ =
                    owner.test_remote_replica_attempt(section, &peer.client, deadline(), |_| {
                        panic!("interrupt replica send")
                    });
            }))
            .is_err());
            for offset in [0, 4, 6, 10, 14, 22, 26, 34] {
                let mut crossed = reply.clone();
                crossed[offset] ^= 1;
                assert!(matches!(
                    owner.test_remote_replica_attempt(section, &peer.client, deadline(), |a| a
                        .confirm(&crossed)),
                    Err(E::Unconfirmed(_))
                ));
                assert_eq!(owner.replica_observation(peer.node), R::Unconfirmed);
                assert_eq!(
                    owner.replica_observation(NodeId::new(2)),
                    R::NoAttemptObserved
                );
            }
            owner
                .test_remote_replica_attempt(section, &peer.client, deadline(), |a| {
                    a.confirm(&reply)
                })
                .unwrap();
            owner
                .test_remote_replica_attempt(section, &peer.client, deadline(), |_| {
                    panic!("confirmed redispatch")
                })
                .unwrap();
            assert_eq!(owner.replica_observation(peer.node), R::TombstoneConfirmed);
            assert_eq!(
                mixed.observe(),
                before,
                "parser evidence grants no mutation"
            );
        },
    );
}

struct Wake<'a>(Option<&'a Remote>);
impl Drop for Wake<'_> {
    fn drop(&mut self) {
        if let Some(peer) = self.0 {
            if peer.tls {
                let _ = std::net::TcpStream::connect(peer.server.tcp_listener_addr_for_test());
            } else {
                let _ = std::os::unix::net::UnixStream::connect(peer.server.socket_path_for_test());
            }
        }
    }
}

#[test]
fn staging_replica_abandonment_sender_lost_reply_preserves_partial_progress_and_exact_retry() {
    for tls in [false, true] {
        with_embedded_primary(tls, 3, "uncertain", deadline(), |mixed, owner, section| {
            owner.record_primary_embedded(section, deadline()).unwrap();
            let first = mixed.remote(NodeId::new(2)).unwrap();
            let _running = first.run();
            owner
                .record_remote_replica_from_embedded_primary(section, &first.client, deadline())
                .unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let before = mixed.observe();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    peer.server
                        .test_accept_one_abandonment_fault(false, true)
                        .unwrap()
                });
                let mut wake = Wake(Some(peer));
                assert!(matches!(
                    owner.record_remote_replica_from_embedded_primary(
                        section,
                        &peer.client,
                        deadline()
                    ),
                    Err(E::Unconfirmed(_))
                ));
                wake.0 = None;
                worker.join().unwrap();
            });
            assert_eq!(owner.replica_observation(peer.node), R::Unconfirmed);
            assert_eq!(owner.replica_observation(first.node), R::TombstoneConfirmed);
            let after = mixed.observe();
            assert_ne!(after[0], before[0]);
            for n in 1..5 {
                assert_eq!(after[n], before[n]);
            }
            assert!(matches!(
                owner.record_remote_replica_from_embedded_primary(
                    section,
                    &peer.client,
                    Instant::now()
                ),
                Err(E::NotSent(_))
            ));
            assert_eq!(owner.replica_observation(peer.node), R::Unconfirmed);
            let _running = peer.run();
            owner
                .record_remote_replica_from_embedded_primary(section, &peer.client, deadline())
                .unwrap();
            assert_eq!(owner.replica_observation(peer.node), R::TombstoneConfirmed);
            assert_eq!(mixed.observe(), after);
        });
    }
}

#[test]
fn staging_replica_abandonment_sender_signed_crossed_reply_retains_uncertainty() {
    for tls in [false, true] {
        with_embedded_primary(tls, 1, "installed", deadline(), |mixed, owner, section| {
            owner.record_primary_embedded(section, deadline()).unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let count = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&count);
            peer.server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == KIND && seen.fetch_add(1, Ordering::SeqCst) == 0 {
                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        *frame.payload.last_mut().unwrap() ^= 1;
                    }
                }));
            let _running = peer.run();
            assert!(matches!(
                owner.record_remote_replica_from_embedded_primary(
                    section,
                    &peer.client,
                    deadline()
                ),
                Err(E::Unconfirmed(StoreError::StorageRpc {
                    failure: rpc::StorageRpcErrorCode::PayloadDecode,
                    ..
                }))
            ));
            assert_eq!(owner.replica_observation(peer.node), R::Unconfirmed);
            let before = mixed.observe();
            owner
                .record_remote_replica_from_embedded_primary(section, &peer.client, deadline())
                .unwrap();
            assert_eq!(owner.replica_observation(peer.node), R::TombstoneConfirmed);
            assert_eq!(count.load(Ordering::SeqCst), 2);
            assert_eq!(mixed.observe(), before);
        });
    }
}

#[test]
fn staging_replica_abandonment_sender_rejects_foreign_client_and_section_before_transport() {
    let foreign_dir = test_util::tempdir();
    let foreign = Mixed::with_endpoints(&foreign_dir, 1, [(0, false), (2, true)]);
    let foreign_admission = foreign.handle.admit_current_route().unwrap();
    let id = foreign.context.id();
    let foreign_route = foreign_admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let foreign_section = foreign_route.lock_until(deadline()).unwrap();
    with_embedded_primary(
        false,
        1,
        "uncertain",
        deadline(),
        |mixed, owner, section| {
            owner.record_primary_embedded(section, deadline()).unwrap();
            let before = mixed.observe();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            // Both receivers intentionally have no server running. A transport
            // attempt would produce a timeout, not the required subject diagnostic.
            for result in [
                owner.record_remote_replica_from_embedded_primary(
                    section,
                    &foreign.remote(peer.node).unwrap().client,
                    deadline(),
                ),
                owner.record_remote_replica_from_embedded_primary(
                    &foreign_section,
                    &peer.client,
                    deadline(),
                ),
            ] {
                assert!(
                    matches!(
                        result,
                        Err(E::NotSent(
                            StoreError::RouteCapabilitySubjectMismatch { .. }
                        ))
                    ),
                    "{result:?}"
                );
            }
            assert_eq!(owner.replica_observation(peer.node), R::NoAttemptObserved);
            assert_eq!(mixed.observe(), before);
        },
    );
}

#[test]
fn staging_replica_abandonment_sender_holds_primary_session_through_replica_reply() {
    for tls in [false, true] {
        abandonment_remote::with_owner(tls, 1, "installed", |mixed, owner| {
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
            let session = Arc::new(
                owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap(),
            );
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            let observed = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&observed);
            let primary_client = Arc::clone(&primary.client);
            let borrowed_session = Arc::clone(&session);
            let id = mixed.context.id();
            peer.server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == KIND {
                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        let result = borrowed_session.borrow_complete_primary(
                            &primary_client,
                            id.pg_id(),
                            id.cluster_epoch(),
                            Instant::now() + Duration::from_millis(50),
                        );
                        if matches!(result, Err(StoreError::OperationDeadlineExceeded { .. })) {
                            seen.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }));
            owner
                .record_remote_replica_from_remote_primary(
                    &primary.client,
                    &session,
                    &peer.client,
                    deadline(),
                )
                .unwrap();
            peer.server
                .set_response_frame_test_hook(Arc::new(|_, _| {}));
            assert_eq!(
                observed.load(Ordering::SeqCst),
                1,
                "the exact primary transport mutex must remain held until replica confirmation"
            );
            drop(
                session
                    .borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        deadline(),
                    )
                    .unwrap(),
            );
            assert_eq!(owner.replica_observation(peer.node), R::TombstoneConfirmed);
        });
    }
}

#[test]
fn staging_replica_abandonment_sender_primary_mutex_wait_respects_attempt_work_and_route_cutoffs() {
    use std::sync::mpsc;
    for bound in ["attempt", "work", "route"] {
        let dir = test_util::tempdir();
        let mixed = Mixed::with_endpoints(&dir, 1, [(4, false), (0, true)]);
        let primary = mixed.remote(NodeId::new(4)).unwrap();
        let replica = mixed.remote(NodeId::new(0)).unwrap();
        let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
        let cutoff = Instant::now() + Duration::from_secs(2);
        if bound == "route" {
            mixed.map.test_store_route_map_validity(
                RouteMapValidity::until_ms(crate::clock::current_time_millis() + 2_000).unwrap(),
            );
        }
        let admission = mixed.handle.admit_current_route().unwrap();
        let id = mixed.context.id();
        let node = mixed.node(NodeId::new(4));
        let source = abandonment_remote::sender(
            &node.get_pg(id.pg_id().get()).unwrap(),
            &mixed.context,
            "uncertain",
            if bound == "work" { cutoff } else { deadline() },
        );
        let mut owner = source
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"))
            .into_unstarted_abandonment()
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let session = owner
            .open_primary_session(&primary.client, deadline())
            .unwrap();
        owner
            .record_primary_remote(&primary.client, &session, deadline())
            .unwrap();
        let count = count(replica);
        let before = mixed.observe();
        let history = owner.installation_observation();
        std::thread::scope(|scope| {
            let (ready_tx, ready_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (finished_tx, finished_rx) = mpsc::channel();
            let session_ref = &session;
            let worker = scope.spawn(move || {
                let held = session_ref
                    .borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        deadline(),
                    )
                    .unwrap();
                ready_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(8));
                drop(held);
                finished_tx.send(()).unwrap();
            });
            ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(Instant::now() < cutoff, "setup must leave authority live");
            let start = Instant::now();
            let result = owner.record_remote_replica_from_remote_primary(
                &primary.client,
                &session,
                &replica.client,
                if bound == "attempt" {
                    start + Duration::from_millis(50)
                } else {
                    deadline()
                },
            );
            let elapsed = start.elapsed();
            let still_held = matches!(finished_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            let _ = release_tx.send(());
            worker.join().unwrap();
            assert!(
                still_held,
                "{bound}: must fail while competing session borrow is still live"
            );
            assert!(
                elapsed
                    < if bound == "attempt" {
                        Duration::from_secs(1)
                    } else {
                        Duration::from_secs(3)
                    },
                "{bound}: {elapsed:?}"
            );
            assert!(matches!(result, Err(E::NotSent(_))), "{bound}: {result:?}");
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("borrow complete primary session"));
        });
        assert_eq!(owner.installation_observation(), history);
        assert_eq!(
            owner.replica_observation(replica.node),
            R::NoAttemptObserved
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(mixed.observe(), before);
    }
}
