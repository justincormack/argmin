// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Remote retirement removes only the exact slot, never durable cleanup custody.
use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retirement;

const RETIRE: Kind = Kind::MetadataCommandPendingSlotRemove;

macro_rules! with_terminal {
    ($tls:expr, |$f:ident, $session:ident, $terminal:ident| $body:block) => {
        with_application!(@consuming [(4, $tls), (0, !$tls), (2, $tls)], |$f, $session, publication, owner, proof| {
            publication.publish().unwrap();
            for member in [0, 4, 2, 3, 1] { apply_member!($f, publication, member).unwrap(); }
            let mut $terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            $body
        });
    };
}

#[path = "payload_staging_direct_introduction_sender_window_tests.rs"]
mod window;

#[test]
fn staging_direct_introduction_sender_retirement_holds_original_session_through_confirmation() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let release_rx = std::sync::Mutex::new(release_rx);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == RETIRE {
                        ready_tx.send(()).unwrap();
                        release_rx
                            .lock()
                            .unwrap()
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap();
                    }
                }));
            std::thread::scope(|scope| {
                let id = terminal.id();
                let session_ref = &session;
                let worker = scope.spawn(move || {
                    ready_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    let error = session_ref
                        .borrow_complete_primary(
                            &primary.client,
                            id.pg_id(),
                            id.cluster_epoch(),
                            Instant::now() + std::time::Duration::from_millis(30),
                        )
                        .err();
                    release_tx.send(()).unwrap();
                    assert!(error
                        .expect("borrow must span retirement confirmation")
                        .to_string()
                        .contains("borrow complete primary session"));
                });
                terminal.retire().unwrap();
                worker.join().unwrap();
            });
            assert_eq!(terminal.retirement(), Retirement::Retired);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_retirement_preserves_custody_and_confirmed_history() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = calls.clone();
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == RETIRE {
                        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }));
            let before = f.observe();
            let node = f.node(NodeId::new(4));
            let preserved = {
                let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                assert!(complete_pending::observe(&pg)
                    .iter()
                    .any(|rows| !rows.is_empty()));
                (
                    introduction::state(&pg),
                    complete_maintenance::log_state(&pg),
                    pg.metadata_command_replica_state().unwrap(),
                )
            };
            let bounds = terminal.test_bounds();
            assert_eq!(terminal.retirement(), Retirement::NoAttemptObserved);
            terminal.retire().unwrap();
            assert_eq!(terminal.retirement(), Retirement::Retired);
            assert_eq!(terminal.test_bounds(), bounds);
            {
                let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                assert_eq!(
                    (
                        introduction::state(&pg),
                        complete_maintenance::log_state(&pg),
                        pg.metadata_command_replica_state().unwrap()
                    ),
                    preserved
                );
            }
            let after = f.observe();
            for member in 0..4 {
                assert_eq!(before[member], after[member]);
            }
            terminal.retire().unwrap();
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "confirmed retirement must not redispatch"
            );
            assert_eq!(f.observe(), after);
            crate::clock::with_time_override(bounds.1, || {
                assert!(terminal.retire().is_err());
            });
            assert_eq!(terminal.retirement(), Retirement::Retired);
            session.test_close_primary_section();
            let _replacement = primary
                .client
                .open_metadata_command_critical_section_until(terminal.id().pg_id(), deadline())
                .unwrap();
            assert!(terminal.retire().is_err());
            assert_eq!(terminal.retirement(), Retirement::Retired);
            drop(terminal);
            assert_eq!(f.observe(), after, "no resource release on Drop");
        });
    }
}

#[test]
fn staging_direct_introduction_sender_retirement_validates_exact_reply_and_absent_slot_custody() {
    for tls in [false, true] {
        for fault in ["digest", "purpose", "empty", "error"] {
            with_terminal!(tls, |f, session, terminal| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != RETIRE {
                            return;
                        }
                        let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        match fault {
                            "digest" => *body.last_mut().unwrap() ^= 1,
                            "purpose" => body[0] ^= 1,
                            "empty" => body.clear(),
                            "error" => (),
                            _ => unreachable!(),
                        }
                        frame.payload = if fault == "error" {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected after introduction slot retirement".into(),
                            })
                            .unwrap()
                        } else {
                            rpc::encode_storage_rpc_success_response(&body)
                        };
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true)
                            .unwrap();
                    }));
                assert!(terminal.retire().is_err(), "{fault}");
                assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
                let node = f.node(NodeId::new(4));
                {
                    let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                    assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                    // An absent slot alone must never confirm lost retirement.
                    pg.conn.execute_batch("UPDATE payload_staging_resources SET owner_token = owner_token || '-crossed'").unwrap();
                }
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|_, _| {}));
                let damaged = f.observe();
                assert!(terminal.retire().is_err());
                assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
                assert_eq!(f.observe(), damaged);
                {
                    let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                    pg.conn.execute_batch("UPDATE payload_staging_resources SET owner_token = substr(owner_token, 1, length(owner_token) - 8)").unwrap();
                }
                let restored = f.observe();
                terminal.retire().unwrap();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                assert_eq!(
                    f.observe(),
                    restored,
                    "exact absence retry confirms without mutation"
                );
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_retirement_not_sent_unwind_and_expiry_preserve_history() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            let before = f.observe();
            let bounds = terminal.test_bounds();
            crate::clock::with_time_override(bounds.1, || {
                assert!(terminal.retire().is_err());
            });
            assert_eq!(terminal.retirement(), Retirement::NoAttemptObserved);
            session.test_introduction_retire_before_dispatch(false);
            assert!(terminal
                .retire()
                .unwrap_err()
                .to_string()
                .contains("not sent"));
            assert_eq!(terminal.retirement(), Retirement::NoAttemptObserved);
            assert_eq!(f.observe(), before);
            session.test_introduction_retire_before_dispatch(true);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| terminal.retire()))
                    .is_err()
            );
            assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
            assert_eq!(f.observe(), before);
            session.test_introduction_retire_before_dispatch(false);
            assert!(terminal.retire().is_err());
            assert_eq!(
                terminal.retirement(),
                Retirement::Unconfirmed,
                "NotSent cannot erase an earlier uncertain attempt"
            );
            terminal.retire().unwrap();
            assert_eq!(terminal.retirement(), Retirement::Retired);
            assert_eq!(terminal.test_bounds(), bounds);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_retirement_closed_transport_cannot_reconnect() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_terminal!(tls, |f, session, terminal| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = calls.clone();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != RETIRE {
                            return;
                        }
                        count.fetch_add(1, Ordering::SeqCst);
                        if lost {
                            StorageNodeServer::test_lose_retirement_reply();
                        } else {
                            rpc::set_storage_rpc_response_connection_reusable(
                                &mut frame.payload,
                                false,
                            )
                            .unwrap();
                        }
                    }));
                assert_eq!(terminal.retire().is_err(), lost);
                let expected = if lost {
                    Retirement::Unconfirmed
                } else {
                    Retirement::Retired
                };
                assert_eq!(terminal.retirement(), expected);
                assert!(complete_pending::observe(
                    &f.node(NodeId::new(4))
                        .get_pg(terminal.id().pg_id().get())
                        .unwrap()
                )
                .iter()
                .all(Vec::is_empty));
                let after = f.observe();
                let _replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(terminal.id().pg_id(), deadline())
                    .unwrap();
                assert!(terminal.retire().is_err());
                assert_eq!(terminal.retirement(), expected);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                drop(terminal);
                assert_eq!(f.observe(), after);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_retirement_rechecks_deadline_inside_delete_transaction() {
    for tls in [false, true] {
        with_application!(@consuming [(4, tls), (0, !tls), (2, tls)], |f, session, publication, owner, proof| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            // Arm on the primary's earlier apply response, on the SAME server
            // connection thread. The retirement hook runs after admission and
            // custody inspection, immediately before the transactional delete.
            primary.server.set_response_frame_test_hook(Arc::new(|kind, _| {
                if kind == APPLY { StorageNodeServer::test_expire_retirement_before_delete(); }
            }));
            publication.publish().unwrap();
            for member in [0, 4, 2, 3, 1] { apply_member!(f, publication, member).unwrap(); }
            let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            let before = f.observe();
            let bounds = terminal.test_bounds();
            assert!(std::time::Instant::now() < bounds.0);
            assert!(terminal.retire().is_err());
            assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
            // Acquiring the PG waits for server-side transaction teardown even
            // if the client times out just before the server's final check.
            assert_eq!(f.observe(), before, "expired delete preserves the slot, indexes and all durable custody");
            assert_eq!(terminal.test_bounds(), bounds);
            assert!(terminal.retire().is_err());
            assert_eq!(terminal.retirement(), Retirement::Unconfirmed);
        });
    }
}
