// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The original introduction must release its exact remote section before the
//! shared physical writer exists. No new command, reservation or cutoff.
use super::*;
use std::sync::mpsc::{sync_channel, RecvTimeoutError};
use std::time::Duration;

const RELEASE: Kind = Kind::MetadataCommandPgLockRelease;

#[test]
fn staging_direct_introduction_sender_files_preflight_rejects_expiry_and_committed_encryption() {
    for tls in [false, true] {
        for expired in [false, true] {
            with_terminal!(tls, |f, session, terminal| {
                terminal.retire().unwrap();
                let id = terminal.id();
                let bounds = terminal.test_bounds();
                let window = terminal
                    .into_confirmed_window()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
                let subject = crate::ObjectEncryptionSubject::standard_object(
                    1,
                    &BucketName::new("other").unwrap(),
                    &ObjectKey::new("other").unwrap(),
                    GenerationId::new(1).unwrap(),
                    &fixture.encryption,
                );
                let encryption = if expired {
                    fixture.encryption.clone()
                } else {
                    fixture.committed_encryption(&subject)
                };
                let before = f.observe();
                let primary = f.remote(NodeId::new(4)).unwrap();
                let release_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let observed = release_count.clone();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == RELEASE {
                            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }));
                let result = if expired {
                    crate::clock::with_time_override(bounds.1 + 1, || {
                        window.into_file_dispatch(&encryption)
                    })
                } else {
                    window.into_file_dispatch(&encryption)
                };
                let failure = result
                    .err()
                    .expect("invalid binding cannot release or mint physical authority");
                assert_eq!(failure.id(), id);
                if !expired {
                    assert!(failure
                        .error()
                        .to_string()
                        .contains("direct provisional encryption binding"));
                }
                assert_eq!(release_count.load(std::sync::atomic::Ordering::SeqCst), 0);
                primary.server.test_require_window_primary_held(id.pg_id());
                drop(failure);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_files_wait_for_confirmed_original_release() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            terminal.retire().unwrap();
            let id = terminal.id();
            let bounds = terminal.test_bounds();
            let window = terminal
                .into_confirmed_window()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let primary = f.remote(NodeId::new(4)).unwrap();
            let before = f.observe();
            let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
            let (entered_tx, entered) = sync_channel(1);
            let (resume_tx, resume) = sync_channel(1);
            let resume = std::sync::Mutex::new(resume);
            primary.server.test_before_window_release(Arc::new(move || {
                entered_tx.send(()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }));
            std::thread::scope(|scope| {
                let (done_tx, done) = sync_channel(1);
                let monitor = scope.spawn(move || {
                    entered.recv_timeout(Duration::from_secs(5)).unwrap();
                    primary.server.test_require_window_primary_held(id.pg_id());
                    assert!(matches!(
                        done.recv_timeout(Duration::from_millis(50)),
                        Err(RecvTimeoutError::Timeout)
                    ));
                    resume_tx.send(()).unwrap();
                    done.recv_timeout(Duration::from_secs(5)).unwrap();
                });
                let writer = window
                    .into_file_dispatch(&fixture.encryption)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                done_tx.send(()).unwrap();
                primary
                    .server
                    .test_require_window_primary_released(id.pg_id());
                assert_eq!(writer.id(), id);
                assert!(writer.test_binding_state().0 <= bounds.0);
                let history = writer.test_binding_state().1.unwrap();
                assert_eq!(history.command_id, id);
                assert_eq!(history.installation, InstallationObservation::Installed);
                for node in 0..5 {
                    assert_eq!(writer.test_application(NodeId::new(node)), Applied::Applied);
                }
                assert!(writer.test_cutoff() <= bounds.1);
                assert_eq!(f.observe(), before);
                assert!(session
                    .borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        deadline()
                    )
                    .is_err());
                drop(writer);
                assert_eq!(f.observe(), before, "Drop retains reservation and owner");
                monitor.join().unwrap();
            });
        });
    }
}

#[test]
fn staging_direct_introduction_sender_files_release_failure_keeps_custody() {
    for tls in [false, true] {
        for malformed in [false, true] {
            with_terminal!(tls, |f, session, terminal| {
                terminal.retire().unwrap();
                let id = terminal.id();
                let window = terminal
                    .into_confirmed_window()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == RELEASE {
                            frame.payload = if malformed {
                                rpc::encode_storage_rpc_success_response(
                                    b"invalid release acknowledgement",
                                )
                            } else {
                                rpc::encode_storage_rpc_error_response(
                                    &rpc::StorageRpcErrorResponse {
                                        code: rpc::StorageRpcErrorCode::MetadataCommandContention,
                                        message: "release not confirmed".into(),
                                    },
                                )
                                .unwrap()
                            };
                        }
                    }));
                let before = f.observe();
                let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
                let failure = window
                    .into_file_dispatch(&fixture.encryption)
                    .err()
                    .expect("unconfirmed release must not mint a writer");
                assert_eq!(failure.id(), id);
                let diagnostic = match failure.error() {
                    crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                        StoreError::StorageRpc { detail, .. },
                    ) => detail.as_str().to_owned(),
                    error => error.to_string(),
                };
                let expected = if malformed {
                    "nonempty release acknowledgement"
                } else {
                    "release not confirmed"
                };
                assert!(diagnostic.contains(expected), "{diagnostic}");
                primary
                    .server
                    .test_require_window_primary_released(id.pg_id());
                assert!(session
                    .borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        deadline()
                    )
                    .is_err());
                drop(failure);
                assert_eq!(
                    f.observe(),
                    before,
                    "failed release and Drop retain durable custody"
                );
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_files_reject_reconnected_primary() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            terminal.retire().unwrap();
            let id = terminal.id();
            let window = terminal
                .into_confirmed_window()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let primary = f.remote(NodeId::new(4)).unwrap();
            session.test_close_primary_section();
            let _replacement = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            let before = f.observe();
            let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
            let failure = window
                .into_file_dispatch(&fixture.encryption)
                .err()
                .expect("same-client replacement is not original authority");
            assert_eq!(failure.id(), id);
            drop(failure);
            assert_eq!(f.observe(), before);
        });
    }
}
