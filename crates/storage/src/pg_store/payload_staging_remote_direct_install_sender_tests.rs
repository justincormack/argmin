// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real ciphertext/files/acknowledgements retained across owning install attempts.
use super::*;
use crate::node_runtime::pg_store::DirectTerminalInstallationObservation as Observed;

macro_rules! with_submission {
    ($tls:expr, $origin:expr, |$f:ident, $session:ident, $submission:ident, $owner:ident, $shards:ident| $body:block) => {
        with_writer(
            $tls,
            StagingTarget::DirectPut,
            false,
            $origin,
            |$f, mut writer, $owner, $shards| {
                let $session;
                let witnessed = witnessed!($f, writer, $owner, $shards, $session);
                let evidence = crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                    .committed_encryption_evidence(witnessed.encryption_subject());
                let prepared = witnessed
                    .prepare_terminal_remote(
                        &$f.remote(NodeId::new(4)).unwrap().client,
                        &$session,
                        &input(&evidence),
                        &evidence,
                    )
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let mut $submission = prepared.into_submission();
                $body
            },
        );
    };
}

#[path = "payload_staging_remote_direct_publication_tests.rs"]
mod publication;

#[test]
fn staging_remote_direct_install_sender_confirms_once_and_preserves_all_origins() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            with_submission!(tls, origin, |f, session, submission, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingDirectInstall {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let history = submission.test_binding_state();
                let before = f.observe();
                assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                submission
                    .install_remote(&primary.client, &session)
                    .unwrap();
                assert_eq!(submission.installation(), Observed::Installed);
                let installed = f.observe();
                assert_ne!(installed[4], before[4]);
                assert_eq!(installed[..4], before[..4]);
                assert_eq!(submission.test_binding_state(), history);
                require_payload(f, owner, shards, true);
                f.node(NodeId::new(4))
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .conn
                    .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                    .unwrap();
                let damaged = f.observe();
                submission
                    .install_remote(&primary.client, &session)
                    .unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert!(submission
                    .install_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                    .is_err());
                assert_eq!(submission.installation(), Observed::Installed);
                assert_eq!(f.observe(), damaged);
                let publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(publication.installation(), Observed::Installed);
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), damaged);
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_sender_rejects_foreign_reopened_expired_before_dispatch() {
    for tls in [false, true] {
        with_submission!(tls, "uncertain", |f, session, submission, owner, shards| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectInstall {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let before = f.observe();
            let history = submission.test_binding_state();
            assert!(submission
                .install_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                .is_err());
            crate::clock::with_time_override(
                crate::clock::current_time_millis() + 1_000_000,
                || {
                    assert!(submission
                        .install_remote(&primary.client, &session)
                        .is_err());
                },
            );
            session.test_close_primary_section();
            let replacement = primary
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            let error = submission
                .install_remote(&primary.client, &replacement)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("original remote primary session"),
                "{error}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
            assert_eq!(submission.test_binding_state(), history);
            assert_eq!(f.observe(), before);
            drop(submission);
            require_payload(f, owner, shards, true);
        });
    }
}

#[test]
fn staging_remote_direct_install_sender_uncertain_responses_keep_exact_request_and_history() {
    for tls in [false, true] {
        for fault in ["digest", "empty", "purpose", "remote-error"] {
            with_submission!(tls, "uncertain", |f, session, submission, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let reply = Arc::new(std::sync::Mutex::new(None));
                let observed = Arc::clone(&reply);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != Kind::StagingDirectInstall {
                            return;
                        }
                        let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        *observed.lock().unwrap() = Some(body.clone());
                        match fault {
                            "digest" => body[6] ^= 1,
                            "empty" => body.clear(),
                            "purpose" => body[0] ^= 1,
                            "remote-error" => {}
                            _ => unreachable!(),
                        }
                        frame.payload = if fault == "remote-error" {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected after committed installation".into(),
                            })
                            .unwrap()
                        } else {
                            rpc::encode_storage_rpc_success_response(&body)
                        };
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true)
                            .unwrap();
                    }));
                let history = submission.test_binding_state();
                assert!(submission
                    .install_remote(&primary.client, &session)
                    .is_err());
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                let after = f.observe();
                let failure = submission
                    .into_publication()
                    .err()
                    .expect("unconfirmed cannot publish");
                submission = failure.into_submission();
                assert!(submission
                    .install_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                    .is_err());
                session.test_direct_install_before_dispatch(false);
                assert!(submission
                    .install_remote(&primary.client, &session)
                    .is_err());
                assert_eq!(submission.installation(), Observed::Unconfirmed);
                let expected = reply.lock().unwrap().take().unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == Kind::StagingDirectInstall {
                            assert_eq!(
                                rpc::decode_storage_rpc_response_payload(&frame.payload)
                                    .unwrap()
                                    .unwrap(),
                                expected,
                                "exact command, preparation and cutoff on retry"
                            );
                        }
                    }));
                submission
                    .install_remote(&primary.client, &session)
                    .unwrap();
                assert_eq!(submission.installation(), Observed::Installed);
                assert_eq!(submission.test_binding_state(), history);
                assert_eq!(f.observe(), after);
                drop(submission);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), after);
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_sender_lost_and_closed_success_preserve_custody() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_submission!(tls, "installed", |f, session, submission, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == Kind::StagingDirectInstall {
                            if lost {
                                StorageNodeServer::test_lose_direct_snapshot_reply();
                            } else {
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    false,
                                )
                                .unwrap();
                            }
                        }
                    }));
                let history = submission.test_binding_state();
                let result = submission.install_remote(&primary.client, &session);
                let expected = if lost {
                    Observed::Unconfirmed
                } else {
                    Observed::Installed
                };
                assert_eq!(result.is_err(), lost);
                assert_eq!(submission.installation(), expected);
                let after = f.observe();
                assert!(submission
                    .install_remote(&primary.client, &session)
                    .is_err());
                session.test_close_primary_section();
                let replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                assert!(submission
                    .install_remote(&primary.client, &replacement)
                    .is_err());
                assert_eq!(submission.installation(), expected);
                assert_eq!(submission.test_binding_state(), history);
                assert_eq!(f.observe(), after);
                drop(submission);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), after);
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_sender_not_sent_and_unwind_cannot_reset_history() {
    for tls in [false, true] {
        with_submission!(tls, "uncertain", |f, session, submission, owner, shards| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectInstall {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let before = f.observe();
            let history = submission.test_binding_state();
            session.test_direct_install_before_dispatch(false);
            assert!(submission
                .install_remote(&primary.client, &session)
                .is_err());
            assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
            session.test_direct_install_before_dispatch(true);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || submission.install_remote(&primary.client, &session)
            ))
            .is_err());
            assert_eq!(submission.installation(), Observed::Unconfirmed);
            session.test_direct_install_before_dispatch(false);
            assert!(submission
                .install_remote(&primary.client, &session)
                .is_err());
            assert_eq!(submission.installation(), Observed::Unconfirmed);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
            assert_eq!(submission.test_binding_state(), history);
            require_payload(f, owner, shards, true);
            submission
                .install_remote(&primary.client, &session)
                .unwrap();
            assert_eq!(submission.installation(), Observed::Installed);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let after = f.observe();
            drop(submission);
            require_payload(f, owner, shards, true);
            assert_eq!(f.observe(), after);
        });
    }
}
