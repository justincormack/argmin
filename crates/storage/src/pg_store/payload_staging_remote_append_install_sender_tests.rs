// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual file/ack custody through the consuming preparation and owning install.
use super::*;
use crate::node_runtime::pg_store::StreamAppendInstallationObservation as Observed;

macro_rules! with_submission {
    ($tls:expr, $target:expr, $appended:expr, $origin:expr, |$f:ident, $session:ident, $submission:ident, $owner:ident, $shards:ident| $body:block) => {
        with_submission!(@with with_writer, $tls, $target, $appended, $origin, |$f, $session, $submission, $owner, $shards| $body);
    };
    (@with $writer:path, $tls:expr, $target:expr, $appended:expr, $origin:expr, |$f:ident, $session:ident, $submission:ident, $owner:ident, $shards:ident| $body:block) => {
        $writer(
            $tls,
            $target,
            $appended,
            $origin,
            |$f, mut writer, $owner, $shards| {
                write_all($f, &mut writer, $owner, $shards);
                writer
                    .persist(
                        &$f.remote(
                            $owner
                                .windows
                                .last()
                                .unwrap()
                                .participants
                                .acknowledgement_primary,
                        )
                        .unwrap()
                        .client,
                    )
                    .unwrap();
                let Writer::Stream(writer) = writer else {
                    unreachable!()
                };
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let primary = $f.remote(NodeId::new(4)).unwrap();
                let $session = ready.open_append_primary_session(&primary.client).unwrap();
                let prepared = ready
                    .prepare_append_remote(&primary.client, &$session)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let mut $submission = prepared.into_submission();
                $body
            },
        );
    };
}

#[path = "payload_staging_remote_append_application_tests.rs"]
mod application;
#[path = "payload_staging_mixed_append_application_tests.rs"]
mod mixed_application;
#[path = "payload_staging_remote_append_publication_tests.rs"]
mod publication;
#[path = "payload_staging_remote_append_terminal_tests.rs"]
mod terminal;

#[test]
fn staging_remote_append_install_sender_confirms_once_and_preserves_custody() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_submission!(
                    tls,
                    target.clone(),
                    appended,
                    "installed",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        let calls = Arc::new(AtomicUsize::new(0));
                        let count = Arc::clone(&calls);
                        primary
                            .server
                            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                                if kind == Kind::StagingStreamAppendInstall {
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
                        assert_eq!(submission.test_binding_state(), history);
                        let installed = f.observe();
                        assert_ne!(installed[4], before[4]);
                        assert_eq!(installed[..4], before[..4]);
                        require_payload(f, owner, shards, true);
                        let node = f.node(NodeId::new(4));
                        node.get_pg(f.context.id().pg_id().get())
                            .unwrap()
                            .conn
                            .execute_batch(
                                "DELETE FROM metadata_command_pending_staging_protection",
                            )
                            .unwrap();
                        let damaged = f.observe();
                        // Installed is historical evidence, not a current-slot claim.
                        submission
                            .install_remote(&primary.client, &session)
                            .unwrap();
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert_eq!(submission.installation(), Observed::Installed);
                        assert_eq!(f.observe(), damaged);
                        assert!(submission
                            .install_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                            .is_err());
                        assert_eq!(submission.installation(), Observed::Installed);
                        let publication = submission
                            .into_publication()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(publication.installation(), Observed::Installed);
                        assert_eq!(publication.test_binding_state(), history);
                        drop(publication);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), damaged);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_install_sender_rejects_wrong_client_reopen_and_expiry_before_dispatch() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(
                tls,
                target,
                false,
                "uncertain",
                |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::StagingStreamAppendInstall {
                                count.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let history = submission.test_binding_state();
                    let before = f.observe();
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
                        .open_metadata_command_critical_section_until(
                            f.context.id().pg_id(),
                            deadline(),
                        )
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
                    assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                    assert_eq!(submission.test_binding_state(), history);
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    assert_eq!(f.observe(), before);
                    drop(submission);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), before);
                }
            );
        }
    }
}

#[test]
fn staging_remote_append_install_sender_uncertain_replies_keep_history_and_exact_retry() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for fault in ["digest", "empty", "purpose", "remote-error"] {
                with_submission!(
                    tls,
                    target.clone(),
                    false,
                    "uncertain",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        let correlation = Arc::new(std::sync::Mutex::new(None));
                        let received = Arc::clone(&correlation);
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind != Kind::StagingStreamAppendInstall {
                                    return;
                                }
                                let mut body =
                                    rpc::decode_storage_rpc_response_payload(&frame.payload)
                                        .unwrap()
                                        .unwrap();
                                *received.lock().unwrap() = Some(body.clone());
                                match fault {
                                    "digest" => body[6] ^= 1,
                                    "empty" => body.clear(),
                                    "purpose" => body[0] ^= 1,
                                    "remote-error" => {}
                                    _ => unreachable!(),
                                }
                                frame.payload = if fault == "remote-error" {
                                    rpc::encode_storage_rpc_error_response(
                                        &rpc::StorageRpcErrorResponse {
                                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                            message: "injected response after committed insertion"
                                                .into(),
                                        },
                                    )
                                    .unwrap()
                                } else {
                                    rpc::encode_storage_rpc_success_response(&body)
                                };
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    true,
                                )
                                .unwrap();
                            },
                        ));
                        let history = submission.test_binding_state();
                        assert!(submission
                            .install_remote(&primary.client, &session)
                            .is_err());
                        assert_eq!(submission.installation(), Observed::Unconfirmed, "{fault}");
                        let after = f.observe();
                        assert_eq!(submission.test_binding_state(), history);
                        require_payload(f, owner, shards, true);
                        // Later local rejection cannot erase earlier ambiguity.
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
                        assert_eq!(submission.installation(), Observed::Unconfirmed);
                        let expected = correlation.lock().unwrap().take().unwrap();
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind == Kind::StagingStreamAppendInstall {
                                    assert_eq!(
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap(),
                                        expected,
                                        "retry must retain command, claims and cutoff"
                                    );
                                }
                            },
                        ));
                        submission
                            .install_remote(&primary.client, &session)
                            .unwrap();
                        assert_eq!(submission.installation(), Observed::Installed);
                        assert_eq!(submission.test_binding_state(), history);
                        assert_eq!(f.observe(), after);
                        drop(submission);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), after);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_install_sender_lost_and_closed_success_do_not_reset_custody() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for lost in [false, true] {
                with_submission!(
                    tls,
                    target.clone(),
                    false,
                    "installed",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind == Kind::StagingStreamAppendInstall {
                                    if lost {
                                        StorageNodeServer::test_lose_stream_append_reply();
                                    } else {
                                        rpc::set_storage_rpc_response_connection_reusable(
                                            &mut frame.payload,
                                            false,
                                        )
                                        .unwrap();
                                    }
                                }
                            },
                        ));
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
                        assert_eq!(submission.installation(), expected);
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
                            .unwrap_err()
                            .to_string()
                            .contains("original remote primary session"));
                        assert_eq!(submission.installation(), expected);
                        assert_eq!(submission.test_binding_state(), history);
                        assert_eq!(f.observe(), after);
                        drop(submission);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), after);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_install_sender_not_sent_and_unwind_preserve_monotonic_history() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(
                tls,
                target,
                false,
                "uncertain",
                |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::StagingStreamAppendInstall {
                                count.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let history = submission.test_binding_state();
                    let before = f.observe();
                    session.test_append_install_before_dispatch(false);
                    assert!(submission
                        .install_remote(&primary.client, &session)
                        .is_err());
                    assert_eq!(submission.installation(), Observed::NoAcceptanceObserved);
                    session.test_append_install_before_dispatch(true);
                    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        submission.install_remote(&primary.client, &session)
                    }));
                    assert!(unwind.is_err());
                    assert_eq!(submission.installation(), Observed::Unconfirmed);
                    session.test_append_install_before_dispatch(false);
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
                    assert_eq!(f.observe(), after);
                    require_payload(f, owner, shards, true);
                }
            );
        }
    }
}
