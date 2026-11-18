// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Original-session publication of the installed introduction, not application.
use super::*;
use crate::node_runtime::pg_store::PublicationObservation as Published;

const PUBLISH: Kind = Kind::MetadataCommandPublicationStart;

#[path = "payload_staging_direct_introduction_sender_application_tests.rs"]
mod application;

macro_rules! with_publication {
    ($tls:expr, |$f:ident, $session:ident, $publication:ident| $body:block) => {
        with_submission!($tls, |$f, $session, submission| {
            submission.install().unwrap();
            let before = $f.observe();
            let bounds = submission.test_bounds();
            let id = submission.id();
            // Move clocks only around the inert handoff. Authenticated responses
            // independently enforce a one-second peer clock-skew bound.
            let mut $publication = crate::clock::with_time_override(
                crate::clock::current_time_millis().saturating_sub(1000),
                || {
                    submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()))
                },
            );
            assert_eq!($publication.id(), id);
            assert_eq!($publication.installation(), Installed::Installed);
            assert_eq!(
                $publication.test_installation(),
                InstallationObservation::Installed
            );
            assert_eq!($publication.test_bounds(), bounds);
            assert_eq!($publication.publication(), Published::NoAttemptObserved);
            assert_eq!($f.observe(), before, "handoff has no effects");
            $body
        });
    };
}

#[test]
fn staging_direct_introduction_sender_publication_requires_confirmed_custody_without_reset() {
    for tls in [false, true] {
        with_submission!(tls, |f, session, submission| {
            let bounds = submission.test_bounds();
            let before = f.observe();
            session.test_introduction_install_before_dispatch(false);
            assert!(submission.install().is_err());
            assert_eq!(submission.installation(), Installed::NoAcceptanceObserved);
            let failure = submission.into_publication().err().expect("not installed");
            assert!(failure
                .error()
                .to_string()
                .contains("confirmed installation"));
            let mut submission = failure.into_submission();
            assert_eq!(submission.installation(), Installed::NoAcceptanceObserved);
            session.test_introduction_install_before_dispatch(true);
            assert!(std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| submission.install())
            )
            .is_err());
            let failure = submission.into_publication().err().expect("unconfirmed");
            let mut submission = failure.into_submission();
            assert_eq!(submission.installation(), Installed::Unconfirmed);
            assert_eq!(submission.test_bounds(), bounds);
            assert_eq!(f.observe(), before);
            submission.install().unwrap();
            let installed = f.observe();
            // Transfer after expiry is inert and must not mint a fresh deadline.
            crate::clock::with_time_override(
                crate::clock::current_time_millis() + 1_000_000,
                || {
                    let mut publication = submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(publication.test_bounds(), bounds);
                    assert!(publication.publish().is_err());
                    assert_eq!(publication.publication(), Published::NoAttemptObserved);
                    assert_eq!(
                        publication.test_installation(),
                        InstallationObservation::Installed
                    );
                    drop(publication);
                },
            );
            assert_eq!(f.observe(), installed);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_publication_changes_only_primary_marker_and_replays_exactly()
{
    for tls in [false, true] {
        with_publication!(tls, |f, session, publication| {
            let node = f.node(NodeId::new(4));
            let pg = publication.id().pg_id().get();
            let stable = without_marker(&node.get_pg(pg).unwrap());
            let before = f.observe();
            let bounds = publication.test_bounds();
            assert!(!started(&node.get_pg(pg).unwrap()));
            publication.publish().unwrap();
            assert_eq!(publication.publication(), Published::MarkerConfirmed);
            assert!(started(&node.get_pg(pg).unwrap()));
            assert_eq!(without_marker(&node.get_pg(pg).unwrap()), stable);
            let after = f.observe();
            assert_eq!(before[..4], after[..4]);
            assert_ne!(before[4], after[4]);
            publication.publish().unwrap();
            assert_eq!(publication.test_bounds(), bounds);
            assert_eq!(
                publication.test_installation(),
                InstallationObservation::Installed
            );
            let retried = f.observe();
            for (actual, expected) in retried.iter().zip(&after) {
                assert_eq!(actual.0, expected.0);
            }
            assert_eq!(retried[..4], after[..4]);
            // Trial application is rolled back; only SQLite's counter advances.
            assert!(retried[4].1 > after[4].1);
            drop(publication);
            assert_eq!(f.observe(), retried);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_publication_bad_confirmations_preserve_installed_history() {
    for tls in [false, true] {
        for fault in ["digest", "empty", "purpose", "remote-error"] {
            with_publication!(tls, |f, session, publication| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                let seen = Arc::new(std::sync::Mutex::new(None));
                let capture = Arc::clone(&seen);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != PUBLISH {
                            return;
                        }
                        let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        *capture.lock().unwrap() = Some(body.clone());
                        match fault {
                            "digest" => {
                                let last = body.len() - 1;
                                body[last] ^= 1;
                            }
                            "empty" => body.clear(),
                            "purpose" => body[0] ^= 1,
                            "remote-error" => (),
                            _ => unreachable!(),
                        }
                        frame.payload = if fault == "remote-error" {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected after publication marker".into(),
                            })
                            .unwrap()
                        } else {
                            rpc::encode_storage_rpc_success_response(&body)
                        };
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true)
                            .unwrap();
                    }));
                let bounds = publication.test_bounds();
                assert!(publication.publish().is_err());
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(
                    publication.test_installation(),
                    InstallationObservation::Installed
                );
                assert!(started(
                    &f.node(NodeId::new(4))
                        .get_pg(publication.id().pg_id().get())
                        .unwrap()
                ));
                let after = f.observe();
                session.test_append_publish_before_dispatch(false);
                assert!(publication.publish().is_err());
                assert_eq!(publication.publication(), Published::Unconfirmed);
                assert_eq!(f.observe(), after);
                let expected = seen.lock().unwrap().take().unwrap();
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
                publication.publish().unwrap();
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                assert_eq!(publication.test_bounds(), bounds);
                session.test_append_publish_before_dispatch(false);
                assert!(publication.publish().is_err());
                assert_eq!(publication.publication(), Published::MarkerConfirmed);
                let retried = f.observe();
                for (actual, expected) in retried.iter().zip(&after) {
                    assert_eq!(actual.0, expected.0);
                }
                drop(publication);
                assert_eq!(f.observe(), retried);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_publication_loss_and_closed_success_cannot_reconnect() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_publication!(tls, |f, session, publication| {
                let primary = f.remote(NodeId::new(4)).unwrap();
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
                let bounds = publication.test_bounds();
                assert_eq!(publication.publish().is_err(), lost);
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
                session.test_close_primary_section();
                let _new = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        publication.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                assert!(publication.publish().is_err());
                assert_eq!(publication.publication(), expected);
                assert_eq!(
                    publication.test_installation(),
                    InstallationObservation::Installed
                );
                assert_eq!(publication.test_bounds(), bounds);
                drop(publication);
                assert_eq!(f.observe(), after);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_publication_preflight_not_sent_and_unwind_are_bounded() {
    for tls in [false, true] {
        with_publication!(tls, |f, session, publication| {
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
            let bounds = publication.test_bounds();
            crate::clock::with_time_override(
                crate::clock::current_time_millis() + 1_000_000,
                || {
                    assert!(publication.publish().is_err());
                },
            );
            assert_eq!(publication.publication(), Published::NoAttemptObserved);
            session.test_append_publish_before_dispatch(false);
            assert!(publication.publish().is_err());
            assert_eq!(publication.publication(), Published::NoAttemptObserved);
            session.test_append_publish_before_dispatch(true);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || publication.publish()
            ))
            .is_err());
            assert_eq!(publication.publication(), Published::Unconfirmed);
            session.test_append_publish_before_dispatch(false);
            assert!(publication.publish().is_err());
            assert_eq!(publication.publication(), Published::Unconfirmed);
            session.test_close_primary_section();
            let _new = primary
                .client
                .open_metadata_command_critical_section_until(publication.id().pg_id(), deadline())
                .unwrap();
            assert!(publication.publish().is_err());
            assert_eq!(publication.test_bounds(), bounds);
            assert_eq!(
                publication.test_installation(),
                InstallationObservation::Installed
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
        });
    }
}
