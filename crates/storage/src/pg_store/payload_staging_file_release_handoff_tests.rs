// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Frontend custody cannot become a file writer before its original primary
//! section acknowledges release. No remote physical effects are activated.
use super::*;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
use std::sync::{
    mpsc::{sync_channel, RecvTimeoutError},
    Mutex,
};

fn exercise(
    f: &Mixed,
    mode: &'static str,
    portable_cutoff: u64,
    bind: impl FnOnce() -> Result<(), String>,
) {
    let before = f.observe();
    let shadows = f.shadows();
    let primary = &f.remote(NodeId::new(4)).unwrap().server;
    let pg = f.context.id().pg_id();
    let (released_tx, released) = sync_channel(1);
    primary.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind != Kind::MetadataCommandPgLockRelease {
            return;
        }
        released_tx.send(()).unwrap();
        if mode == "error" {
            frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                code: rpc::StorageRpcErrorCode::MetadataCommandContention,
                message: "binding release rejected".into(),
            })
            .unwrap();
        } else if mode == "malformed" {
            frame.payload = rpc::encode_storage_rpc_success_response(b"extra release bytes");
        } else if mode == "post-release-expiry" {
            // Guard removal succeeded. Deliver an authentic success after the
            // original portable cutoff, but before the longer local IO bound.
            std::thread::sleep(Duration::from_millis(
                portable_cutoff.saturating_sub(crate::clock::current_time_millis()) + 1,
            ));
        }
    }));
    let result = if matches!(mode, "paused" | "timeout") {
        let (entered_tx, entered) = sync_channel(1);
        let (resume_tx, resume) = sync_channel(1);
        let resume = Mutex::new(resume);
        primary.test_before_window_release(Arc::new(move || {
            entered_tx.send(()).unwrap();
            resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }));
        std::thread::scope(|scope| {
            let (done_tx, done) = sync_channel::<bool>(1);
            let monitor = scope.spawn(move || {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
                primary.test_require_window_primary_held(pg);
                assert!(matches!(
                    done.recv_timeout(Duration::from_millis(50)),
                    Err(RecvTimeoutError::Timeout)
                ));
                if mode == "timeout" {
                    assert!(!done.recv_timeout(Duration::from_secs(5)).unwrap());
                    primary.test_require_window_primary_held(pg);
                }
                resume_tx.send(()).unwrap();
            });
            let result = bind();
            // Buffered: the success case need not wait for this notification.
            let _ = done_tx.send(result.is_ok());
            monitor.join().unwrap();
            result
        })
    } else {
        bind()
    };
    released.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(released.try_recv().is_err(), "no second release attempt");
    primary.test_require_window_primary_released(pg);
    match mode {
        "paused" => result.unwrap(),
        "error" => assert!(result.unwrap_err().contains("binding release rejected")),
        "malformed" => assert!(result
            .unwrap_err()
            .contains("nonempty release acknowledgement")),
        "timeout" => {
            assert!(result.is_err(), "expired release cannot mint a writer");
        }
        "post-release-expiry" => assert!(result
            .unwrap_err()
            .contains("confirmed staging window original cutoff")),
        _ => unreachable!(),
    }
    assert_eq!(
        f.observe(),
        before,
        "failure/Drop must retain custody and allocation"
    );
    assert_eq!(f.shadows(), shadows);
}

fn diagnostic(error: &E) -> String {
    match error {
        E::Store(StoreError::StorageRpc { detail, .. }) => detail.as_str().to_owned(),
        _ => error.to_string(),
    }
}

fn matrix(modes: &[&'static str]) {
    for tls in [false, true] {
        for (target, append) in candidate::cases() {
            for &mode in modes {
                let (_dir, f, proof) = allocation_remote::setup(tls, target.clone(), append);
                run_terminal(
                    &f,
                    proof.as_ref(),
                    "uncertain",
                    |f, primary, mut terminal| {
                        primary.retire(&mut terminal);
                        let echoed = Arc::new(Mutex::new(0));
                        let observed = Arc::clone(&echoed);
                        f.remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                                if kind == Kind::MetadataCommandReplicaState {
                                    let payload =
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap();
                                    assert_eq!(&payload[..4], b"WPOK");
                                    let route = rpc::complete_settled_prefix::decode_request(
                                        &payload
                                            [4..4 + rpc::complete_settled_prefix::REQUEST_BYTES],
                                    )
                                    .unwrap();
                                    *observed.lock().unwrap() =
                                        route.deadline.portable_wall_valid_until_ms;
                                }
                            }));
                        let cutoff = if mode == "post-release-expiry" {
                            Instant::now() + Duration::from_millis(1500)
                        } else if mode == "timeout" {
                            Instant::now() + Duration::from_secs(2)
                        } else {
                            deadline()
                        };
                        let confirmed = primary
                            .confirm(terminal, cutoff, |_| {})
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        let history = confirmed.installation_observation();
                        let portable_cutoff = *echoed.lock().unwrap();
                        assert_ne!(portable_cutoff, 0);
                        let Primary::Remote(client, session) = primary else {
                            unreachable!()
                        };
                        if target == StagingTarget::DirectPut {
                            exercise(f, mode, portable_cutoff, || {
                                match confirmed.into_direct_file_writer_remote(
                                    client,
                                    session,
                                    &crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                                        .encryption,
                                ) {
                                    Ok(writer) => {
                                        assert_eq!(writer.id(), f.context.id());
                                        assert_eq!(writer.test_binding_state().1, history);
                                        assert!(writer.test_binding_state().0 <= cutoff);
                                        Ok(())
                                    }
                                    Err(failure) => {
                                        assert_eq!(failure.id(), f.context.id());
                                        assert_eq!(failure.test_binding_state().1, history);
                                        assert!(failure.test_binding_state().0 <= cutoff);
                                        Err(diagnostic(failure.error()))
                                    }
                                }
                            });
                        } else {
                            let (_, request, expected) =
                                allocation_remote::inputs(f, target.clone());
                            let allocated = confirmed
                                .allocate_stream_remote(client, session, &request, &expected)
                                .unwrap_or_else(|e| panic!("{}", e.error()));
                            let original = allocated.test_binding_state();
                            exercise(f, mode, portable_cutoff, || {
                                match allocated.into_stream_file_writer_remote(client, session) {
                                    Ok(writer) => {
                                        assert_eq!(writer.test_binding_state(), original);
                                        Ok(())
                                    }
                                    Err(failure) => {
                                        assert_eq!(failure.test_binding_state(), original);
                                        Err(diagnostic(failure.error()))
                                    }
                                }
                            });
                        }
                        assert!(
                            session
                                .borrow_complete_primary(
                                    client,
                                    f.context.id().pg_id(),
                                    f.context.id().cluster_epoch(),
                                    deadline()
                                )
                                .is_err(),
                            "released/uncertain section must not be reusable"
                        );
                    },
                );
            }
        }
    }
}

#[test]
fn staging_file_handoff_waits_for_original_primary_release_before_writer() {
    matrix(&["paused"]);
}

#[test]
fn staging_file_handoff_release_timeout_keeps_original_custody_and_cutoff() {
    matrix(&["timeout"]);
}

#[test]
fn staging_file_handoff_bad_release_acknowledgements_keep_original_custody() {
    matrix(&["error", "malformed"]);
}

#[test]
fn staging_file_handoff_revalidates_original_cutoff_after_successful_release() {
    matrix(&["post-release-expiry"]);
}
