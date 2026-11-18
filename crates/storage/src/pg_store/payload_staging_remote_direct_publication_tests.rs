// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Publication-start retains the installed terminal command and physical owner.
use super::*;
use crate::node_runtime::pg_store::PublicationObservation as Published;

const PUBLISH: Kind = Kind::MetadataCommandPublicationStart;

macro_rules! with_publication {
    ($tls:expr, $origin:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $shards:ident| $body:block) => {
        with_submission!($tls, $origin, |$f,
                                         $session,
                                         submission,
                                         $owner,
                                         $shards| {
            submission
                .install_remote(&$f.remote(NodeId::new(4)).unwrap().client, &$session)
                .unwrap();
            let before = $f.observe();
            let history = submission.test_binding_state();
            let id = submission.id();
            let mut $publication = submission
                .into_publication()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!($publication.id(), id);
            assert_eq!($publication.installation(), Observed::Installed);
            assert_eq!($publication.test_binding_state(), history);
            assert_eq!($publication.publication(), Published::NoAttemptObserved);
            assert_eq!($f.observe(), before, "handoff is effect-free");
            $body
        });
    };
}

#[path = "payload_staging_remote_direct_application_tests.rs"]
mod application;
#[path = "payload_staging_mixed_direct_application_tests.rs"]
mod mixed_application;

#[test]
fn staging_remote_direct_publication_exact_replay_retains_all_origins() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            with_publication!(tls, origin, |f, session, publication, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let node = f.node(NodeId::new(4));
                let pg = publication.id().pg_id().get();
                let stable = without_marker(&node.get_pg(pg).unwrap());
                let before = f.observe();
                let history = publication.test_binding_state();
                let cutoff = publication.test_cutoff();
                assert!(cutoff.1 <= owner.windows.last().unwrap().cutoff_ms);
                let confirmation = Arc::new(std::sync::Mutex::new(None));
                let saved = Arc::clone(&confirmation);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == PUBLISH {
                            let body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap();
                            let mut expected = saved.lock().unwrap();
                            if let Some(expected) = expected.as_ref() {
                                assert_eq!(&body, expected, "exact complete command on retry");
                            } else {
                                *expected = Some(body);
                            }
                        }
                    }));
                publication
                    .publish_remote(&primary.client, &session)
                    .unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                assert!(started(&node.get_pg(pg).unwrap()));
                assert_eq!(without_marker(&node.get_pg(pg).unwrap()), stable);
                let after = f.observe();
                assert_eq!(before[..4], after[..4]);
                assert_ne!(before[4], after[4]);
                publication
                    .publish_remote(&primary.client, &session)
                    .unwrap();
                let retried = f.observe();
                // Trial apply/rollback increments total_changes, not durable state.
                for (actual, expected) in retried.iter().zip(&after) {
                    assert_eq!(actual.0, expected.0);
                }
                assert_eq!(retried[..4], after[..4]);
                assert!(retried[4].1 > after[4].1);
                assert_eq!(publication.test_cutoff(), cutoff);
                assert_eq!(publication.test_binding_state(), history);
                assert!(confirmation.lock().unwrap().is_some());
                let failure = publication
                    .into_terminal()
                    .err()
                    .expect("must retain original session");
                assert!(failure
                    .error()
                    .to_string()
                    .contains("remote direct terminal"));
                let publication = failure.into_publication();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                assert_eq!(publication.test_binding_state(), history);
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), retried, "Drop performs no cleanup");
            });
        }
    }
}

#[test]
fn staging_remote_direct_publication_rejects_reconnect_and_expiry_before_dispatch() {
    for tls in [false, true] {
        with_publication!(
            tls,
            "uncertain",
            |f, session, publication, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == PUBLISH {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let before = f.observe();
                let history = publication.test_binding_state();
                let cutoff = publication.test_cutoff();
                assert!(publication
                    .publish_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                    .is_err());
                crate::clock::with_time_override(cutoff.1, || {
                    assert!(publication
                        .publish_remote(&primary.client, &session)
                        .is_err());
                });
                session.test_close_primary_section();
                let replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                let error = publication
                    .publish_remote(&primary.client, &replacement)
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("original remote primary session"),
                    "{error}"
                );
                assert_eq!(publication.publication(), Published::NoAttemptObserved);
                assert_eq!(publication.installation(), Observed::Installed);
                assert_eq!(publication.test_binding_state(), history);
                assert_eq!(publication.test_cutoff(), cutoff);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), before);
            }
        );
    }
}

#[test]
fn staging_remote_direct_publication_uncertain_confirmation_cannot_reset_history() {
    for tls in [false, true] {
        for fault in ["digest", "empty", "purpose", "remote-error"] {
            with_publication!(
                tls,
                "installed",
                |f, session, publication, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let history = publication.test_binding_state();
                    let cutoff = publication.test_cutoff();
                    let confirmation = Arc::new(std::sync::Mutex::new(None));
                    let saved = Arc::clone(&confirmation);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind != PUBLISH {
                                return;
                            }
                            let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap();
                            *saved.lock().unwrap() = Some(body.clone());
                            match fault {
                                "digest" => {
                                    let last = body.len() - 1;
                                    body[last] ^= 1;
                                }
                                "empty" => body.clear(),
                                "purpose" => body[0] ^= 1,
                                "remote-error" => {}
                                _ => unreachable!(),
                            }
                            frame.payload = if fault == "remote-error" {
                                rpc::encode_storage_rpc_error_response(
                                    &rpc::StorageRpcErrorResponse {
                                        code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                        message: "injected after committed publication".into(),
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
                        }));
                    assert!(publication
                        .publish_remote(&primary.client, &session)
                        .is_err());
                    assert_eq!(publication.publication(), Published::Unconfirmed, "{fault}");
                    assert!(started(
                        &f.node(NodeId::new(4))
                            .get_pg(publication.id().pg_id().get())
                            .unwrap()
                    ));
                    let after = f.observe();
                    assert!(publication
                        .publish_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                        .is_err());
                    crate::clock::with_time_override(cutoff.1, || {
                        assert!(publication
                            .publish_remote(&primary.client, &session)
                            .is_err());
                    });
                    assert_eq!(publication.publication(), Published::Unconfirmed);
                    let expected = confirmation.lock().unwrap().take().unwrap();
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == PUBLISH {
                                assert_eq!(
                                    rpc::decode_storage_rpc_response_payload(&frame.payload)
                                        .unwrap()
                                        .unwrap(),
                                    expected
                                );
                            }
                        }));
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    assert_eq!(publication.publication(), Published::MarkerConfirmed);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == PUBLISH {
                                frame.payload = rpc::encode_storage_rpc_success_response(&[]);
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    true,
                                )
                                .unwrap();
                            }
                        }));
                    assert!(publication
                        .publish_remote(&primary.client, &session)
                        .is_err());
                    assert_eq!(publication.publication(), Published::MarkerConfirmed);
                    assert_eq!(publication.installation(), Observed::Installed);
                    assert_eq!(publication.test_binding_state(), history);
                    assert_eq!(publication.test_cutoff(), cutoff);
                    let retried = f.observe();
                    for (actual, expected) in retried.iter().zip(&after) {
                        assert_eq!(actual.0, expected.0);
                    }
                    assert_eq!(retried[..4], after[..4]);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), retried);
                }
            );
        }
    }
}

#[test]
fn staging_remote_direct_publication_lost_or_closed_reply_preserves_custody() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_publication!(
                tls,
                "installed",
                |f, session, publication, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let history = publication.test_binding_state();
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == PUBLISH {
                                if lost {
                                    StorageNodeServer::test_lose_publication_reply();
                                } else {
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        false,
                                    )
                                    .unwrap();
                                }
                            }
                        }));
                    let result = publication.publish_remote(&primary.client, &session);
                    assert_eq!(result.is_err(), lost);
                    let expected = if lost {
                        Published::Unconfirmed
                    } else {
                        Published::MarkerConfirmed
                    };
                    assert_eq!(publication.publication(), expected);
                    let after = f.observe();
                    assert!(started(
                        &f.node(NodeId::new(4))
                            .get_pg(publication.id().pg_id().get())
                            .unwrap()
                    ));
                    assert!(publication
                        .publish_remote(&primary.client, &session)
                        .is_err());
                    session.test_close_primary_section();
                    let replacement = primary
                        .client
                        .open_metadata_command_critical_section_until(
                            f.context.id().pg_id(),
                            deadline(),
                        )
                        .unwrap();
                    assert!(publication
                        .publish_remote(&primary.client, &replacement)
                        .unwrap_err()
                        .to_string()
                        .contains("original remote primary session"));
                    assert_eq!(publication.publication(), expected);
                    assert_eq!(publication.test_binding_state(), history);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), after);
                }
            );
        }
    }
}

#[test]
fn staging_remote_direct_publication_not_sent_and_unwind_preserve_history() {
    for tls in [false, true] {
        with_publication!(
            tls,
            "uncertain",
            |f, session, publication, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let history = publication.test_binding_state();
                let before = f.observe();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == PUBLISH {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                session.test_append_publish_before_dispatch(false);
                assert!(publication
                    .publish_remote(&primary.client, &session)
                    .unwrap_err()
                    .to_string()
                    .contains("not sent"));
                assert_eq!(publication.publication(), Published::NoAttemptObserved);
                session.test_append_publish_before_dispatch(true);
                assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || publication.publish_remote(&primary.client, &session)
                ))
                .is_err());
                assert_eq!(publication.publication(), Published::Unconfirmed);
                session.test_append_publish_before_dispatch(false);
                assert!(publication
                    .publish_remote(&primary.client, &session)
                    .is_err());
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(f.observe(), before);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                publication
                    .publish_remote(&primary.client, &session)
                    .unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(publication.test_binding_state(), history);
                drop(publication);
                require_payload(f, owner, shards, true);
            }
        );
    }
}
