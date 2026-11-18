// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The owning sender, not a raw frame or reconstructed preparation, is retried.
use super::*;
use crate::node_runtime::server::StorageNodeServer;

const INSTALL: Kind = Kind::StagingDirectIntroductionInstall;

macro_rules! with_submission {
    ($tls:expr, |$f:ident, $session:ident, $submission:ident| $body:block) => {{
        let ($f, owner, proof, _dir) = fixture($tls);
        let primary = $f.remote(NodeId::new(4)).unwrap();
        let _running = primary.run();
        let admission = $f.handle.admit_current_route().unwrap();
        let route = admission.complete_fanout_route($f.context.id().pg_id(), $f.context.id().cluster_epoch()).unwrap();
        let $session = primary.client.open_complete_fanout_primary(&route, deadline()).unwrap();
        let before = $f.observe();
        let prepared = route.prepare_direct_introduction_remote(&primary.client, &$session,
            (&owner.subject.bucket, &owner.subject.key, &owner.subject.id.attempt, &proof), deadline()).unwrap();
        let bounds = prepared.test_bounds();
        let id = prepared.id();
        let mut $submission = prepared.into_submission();
        assert_eq!($submission.id(), id);
        assert_eq!($submission.test_bounds(), bounds);
        assert_eq!($submission.installation(), Installed::NoAcceptanceObserved);
        assert_eq!($f.observe(), before, "inert consuming transfer");
        let shadows = $f.shadows();
        $body
        assert_eq!($f.shadows(), shadows);
    }};
}

#[path = "payload_staging_direct_introduction_sender_publication_tests.rs"]
mod publication;

#[test]
fn staging_direct_introduction_sender_confirms_once_and_never_regresses() {
    for tls in [false, true] {
        with_submission!(tls, |f, session, submission| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == INSTALL {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let bounds = submission.test_bounds();
            let before = f.observe();
            submission.install().unwrap();
            assert_eq!(submission.installation(), Installed::Installed);
            let installed = f.observe();
            assert_ne!(installed[4], before[4]);
            assert_eq!(installed[..4], before[..4]);
            f.node(NodeId::new(4))
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .conn
                .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                .unwrap();
            let damaged = f.observe();
            submission.install().unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            crate::clock::with_time_override(
                crate::clock::current_time_millis() + 1_000_000,
                || {
                    assert!(submission.install().is_err());
                },
            );
            session.test_close_primary_section();
            assert!(submission.install().is_err());
            assert_eq!(submission.installation(), Installed::Installed);
            assert_eq!(submission.test_bounds(), bounds);
            assert_eq!(f.observe(), damaged);
            drop(submission);
            assert_eq!(f.observe(), damaged, "Drop neither cleans nor retries");
        });
    }
}

#[test]
fn staging_direct_introduction_sender_preflight_rejects_expiry_and_reopen_without_dispatch() {
    for tls in [false, true] {
        with_submission!(tls, |f, session, submission| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == INSTALL {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let before = f.observe();
            let bounds = submission.test_bounds();
            crate::clock::with_time_override(
                crate::clock::current_time_millis() + 1_000_000,
                || {
                    assert!(submission.install().is_err());
                },
            );
            session.test_close_primary_section();
            let _new = primary
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            let error = submission.install().unwrap_err();
            assert!(
                error.to_string().contains("exact open primary session"),
                "{error}"
            );
            assert_eq!(submission.installation(), Installed::NoAcceptanceObserved);
            assert_eq!(submission.test_bounds(), bounds);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_not_sent_and_unwind_preserve_prior_history() {
    for tls in [false, true] {
        with_submission!(tls, |f, session, submission| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == INSTALL {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let before = f.observe();
            let bounds = submission.test_bounds();
            session.test_introduction_install_before_dispatch(false);
            assert!(submission.install().is_err());
            assert_eq!(submission.installation(), Installed::NoAcceptanceObserved);
            session.test_introduction_install_before_dispatch(true);
            assert!(std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| submission.install())
            )
            .is_err());
            assert_eq!(submission.installation(), Installed::Unconfirmed);
            session.test_introduction_install_before_dispatch(false);
            assert!(submission.install().is_err());
            assert_eq!(submission.installation(), Installed::Unconfirmed);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
            submission.install().unwrap();
            assert_eq!(submission.installation(), Installed::Installed);
            assert_eq!(submission.test_bounds(), bounds);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_crossed_confirmations_retry_exact_request() {
    for tls in [false, true] {
        for fault in ["digest", "empty", "purpose", "remote-error"] {
            with_submission!(tls, |f, session, submission| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let seen = Arc::new(std::sync::Mutex::new(None));
                let capture = Arc::clone(&seen);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != INSTALL {
                            return;
                        }
                        let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        *capture.lock().unwrap() = Some(body.clone());
                        match fault {
                            "digest" => body[6] ^= 1,
                            "empty" => body.clear(),
                            "purpose" => body[0] ^= 1,
                            "remote-error" => (),
                            _ => unreachable!(),
                        }
                        frame.payload = if fault == "remote-error" {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected after installation".into(),
                            })
                            .unwrap()
                        } else {
                            rpc::encode_storage_rpc_success_response(&body)
                        };
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true)
                            .unwrap();
                    }));
                let bounds = submission.test_bounds();
                assert!(submission.install().is_err());
                assert_eq!(submission.installation(), Installed::Unconfirmed);
                let after = f.observe();
                session.test_introduction_install_before_dispatch(false);
                assert!(submission.install().is_err());
                assert_eq!(submission.installation(), Installed::Unconfirmed);
                let expected = seen.lock().unwrap().take().unwrap();
                primary.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == INSTALL {
                        assert_eq!(rpc::decode_storage_rpc_response_payload(&frame.payload).unwrap().unwrap(), expected,
                            "confirmation binds exact preparation, response, command and original cutoff");
                    }
                }));
                submission.install().unwrap();
                assert_eq!(submission.installation(), Installed::Installed);
                assert_eq!(submission.test_bounds(), bounds);
                assert_eq!(f.observe(), after);
                drop(submission);
                assert_eq!(f.observe(), after);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_lost_or_closed_reply_cannot_reconnect() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_submission!(tls, |f, session, submission| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == INSTALL {
                            if lost {
                                StorageNodeServer::test_lose_direct_introduction_reply();
                            } else {
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    false,
                                )
                                .unwrap();
                            }
                        }
                    }));
                let bounds = submission.test_bounds();
                assert_eq!(submission.install().is_err(), lost);
                let expected = if lost {
                    Installed::Unconfirmed
                } else {
                    Installed::Installed
                };
                assert_eq!(submission.installation(), expected);
                let after = f.observe();
                assert!(f
                    .node(NodeId::new(4))
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .inspect_complete_pending_command(4, &topology())
                    .unwrap()
                    .is_some());
                assert!(submission.install().is_err());
                session.test_close_primary_section();
                let _new = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                assert!(submission.install().is_err());
                assert_eq!(submission.installation(), expected);
                assert_eq!(submission.test_bounds(), bounds);
                drop(submission);
                assert_eq!(f.observe(), after);
            });
        }
    }
}
