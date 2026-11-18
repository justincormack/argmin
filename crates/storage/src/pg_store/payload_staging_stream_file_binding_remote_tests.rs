// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming post-allocation binding, still not remote shard-write activation.
use super::*;
use crate::node_runtime::pg_store::AllocatedStreamWindow;
use crate::storage_rpc::{
    self as rpc, complete_stream_file_primary as wire, StorageRpcMessageKind as Kind,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn diagnostic(error: &E) -> String {
    match error {
        E::Store(StoreError::StorageRpc { detail, .. }) => detail.as_str().to_owned(),
        _ => error.to_string(),
    }
}

fn with_allocated(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    history: &str,
    short: bool,
    action: impl FnOnce(
        &Mixed,
        &UnixStorageNodeClient,
        &UnixStorageNodeMetadataCommandSession,
        AllocatedStreamWindow<'_>,
        Vec<u8>,
    ),
) {
    let (_dir, f, proof) = allocation_remote::setup(tls, target.clone(), appended);
    run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
        primary.retire(&mut terminal);
        let cutoff = if short {
            Instant::now() + Duration::from_secs(2)
        } else {
            deadline()
        };
        let confirmed = primary
            .confirm(terminal, cutoff, |_| {})
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let (_, request, expected) = allocation_remote::inputs(f, target);
        let Primary::Remote(client, session) = primary else {
            unreachable!()
        };
        let original_bytes = confirmed.test_stream_allocation_bytes(&request, &expected);
        let allocated = confirmed
            .allocate_stream_remote(client, session, &request, &expected)
            .unwrap_or_else(|e| panic!("{}", diagnostic(e.error())));
        // Compute the expected binding response from the ORIGINAL allocation
        // request, before the sender clock moves or the binder reconstructs it.
        let expected_reply = wire::encode_response(
            &wire::encode_request(&original_bytes, allocated.segment()).unwrap(),
        );
        action(f, client, session, allocated, expected_reply);
    });
}

fn success_matrix(tls: bool, reopen: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["installed", "inspected", "uncertain"] {
                with_allocated(
                    tls,
                    target.clone(),
                    appended,
                    history,
                    false,
                    |f, client, session, allocated, expected_reply| {
                        let original = allocated.test_binding_state();
                        let before = f.observe();
                        let shadows = f.shadows();
                        let calls = Arc::new(Mutex::new(Vec::new()));
                        let received = Arc::clone(&calls);
                        f.remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                                if kind == Kind::ObjectStreamSegmentAppendPrepare {
                                    received.lock().unwrap().push(
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap(),
                                    );
                                }
                            }));
                        if reopen {
                            session.test_close_primary_section();
                            let replacement = client
                                .open_metadata_command_critical_section_until(
                                    f.context.id().pg_id(),
                                    deadline(),
                                )
                                .unwrap();
                            // Establish that the old client/node/PG/epoch checks pass.
                            drop(
                                replacement
                                    .borrow_complete_primary(
                                        client,
                                        f.context.id().pg_id(),
                                        f.context.id().cluster_epoch(),
                                        deadline(),
                                    )
                                    .unwrap(),
                            );
                            let failure = allocated
                                .into_stream_file_writer_remote(client, &replacement)
                                .err()
                                .expect("reconnected section cannot mint a writer");
                            assert_eq!(failure.id(), f.context.id());
                            assert_eq!(failure.test_binding_state(), original);
                            assert!(matches!(failure.error(), E::Store(StoreError::RouteCapabilitySubjectMismatch { operation: "confirmed window requires its original remote primary session" })), "{}", diagnostic(failure.error()));
                            assert!(calls.lock().unwrap().is_empty(), "reject before transport");
                            assert_eq!(f.observe(), before);
                            drop(failure);
                        } else {
                            let writer = crate::clock::with_time_and_monotonic_override(
                                crate::clock::current_time_millis() + 100,
                                crate::clock::monotonic_time_millis(),
                                || allocated.into_stream_file_writer_remote(client, session),
                            )
                            .unwrap_or_else(|e| panic!("{}", diagnostic(e.error())));
                            assert_eq!(writer.id(), f.context.id());
                            assert_eq!(writer.test_binding_state(), original);
                            f.remote(NodeId::new(4))
                                .unwrap()
                                .server
                                .test_require_window_primary_released(f.context.id().pg_id());
                            assert!(session
                                .borrow_complete_primary(
                                    client,
                                    f.context.id().pg_id(),
                                    f.context.id().cluster_epoch(),
                                    deadline()
                                )
                                .is_err());
                            assert_eq!(*calls.lock().unwrap(), vec![expected_reply], "one exact inspection with original absolute cutoff; no allocation/retry");
                            assert_eq!(f.observe(), before);
                            drop(writer);
                        }
                        assert_eq!(
                            f.observe(),
                            before,
                            "Drop must preserve owner/cursor/custody"
                        );
                        assert_eq!(f.shadows(), shadows);
                    },
                );
            }
        }
    }
}

#[test]
fn staging_stream_file_binding_remote_unix_original_cutoff_and_custody() {
    success_matrix(false, false);
}
#[test]
fn staging_stream_file_binding_remote_tls_original_cutoff_and_custody() {
    success_matrix(true, false);
}
#[test]
fn staging_stream_file_binding_remote_unix_rejects_reopened_session() {
    success_matrix(false, true);
}
#[test]
fn staging_stream_file_binding_remote_tls_rejects_reopened_session() {
    success_matrix(true, true);
}

fn bad_replies(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for fault in ["purpose", "digest", "version", "error", "closed"] {
            with_allocated(
                tls,
                target.clone(),
                true,
                "uncertain",
                false,
                |f, client, session, allocated, _| {
                    let original = allocated.test_binding_state();
                    let before = f.observe();
                    let shadows = f.shadows();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let received = Arc::clone(&calls);
                    f.remote(NodeId::new(4)).unwrap().server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != Kind::ObjectStreamSegmentAppendPrepare { return; }
                    received.fetch_add(1, Ordering::SeqCst);
                    let envelope = rpc::decode_storage_rpc_response_payload_with_connection_disposition(&frame.payload).unwrap();
                    let mut payload = envelope.response.unwrap();
                    match fault {
                        "purpose" => payload[..4].copy_from_slice(b"SALR"),
                        "digest" => payload[6] ^= 1,
                        "version" => payload[4] ^= 1,
                        "error" | "closed" => {},
                        _ => unreachable!(),
                    }
                    frame.payload = if fault == "error" {
                        rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse { code: rpc::StorageRpcErrorCode::TransportTimeout, message: "lost stream binding reply".into() }).unwrap()
                    } else { rpc::encode_storage_rpc_success_response(&payload) };
                    rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, envelope.connection_reusable && fault != "closed").unwrap();
                }));
                    let failure = allocated
                        .into_stream_file_writer_remote(client, session)
                        .err()
                        .expect("bad reply must retain failed binding");
                    assert_eq!(failure.id(), f.context.id());
                    assert_eq!(failure.test_binding_state(), original);
                    let message = diagnostic(failure.error());
                    assert!(
                        message.contains(match fault {
                            "purpose" | "digest" | "version" =>
                                "stream file inspection request/response",
                            "error" => "lost stream binding reply",
                            "closed" => "lost its held section",
                            _ => unreachable!(),
                        }),
                        "{fault}: {message}"
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(f.observe(), before);
                    drop(failure);
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                },
            );
        }
    }
}

#[test]
fn staging_stream_file_binding_remote_unix_bad_replies_do_not_retry_or_drop_custody() {
    bad_replies(false);
}
#[test]
fn staging_stream_file_binding_remote_tls_bad_replies_do_not_retry_or_drop_custody() {
    bad_replies(true);
}

fn invalid_authority(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for fault in ["client", "foreign", "expired", "wait", "issuer", "cursor"] {
            with_allocated(
                tls,
                target.clone(),
                true,
                "inspected",
                fault == "wait",
                |f, client, session, allocated, _| {
                    let original = allocated.test_binding_state();
                    if matches!(fault, "issuer" | "cursor") {
                        let node = f.node(NodeId::new(4));
                        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                        if fault == "issuer" {
                            pg.begin_allocation_lifetime_replacement()
                                .unwrap()
                                .complete()
                                .unwrap();
                        } else {
                            pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = next_segment_vid - 1 WHERE session_id = ?1", [&allocated.segment().session_id]).unwrap();
                        }
                        // Isolate issuer/cursor validation, not cache invalidation.
                        pg.recover_complete_command_state(
                            PgStoreRecoveryContext::for_node(NodeId::new(4)),
                            &topology(),
                        )
                        .unwrap();
                    }
                    let before = f.observe();
                    let shadows = f.shadows();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let received = Arc::clone(&calls);
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::ObjectStreamSegmentAppendPrepare {
                                received.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let _clock = (fault == "expired").then(|| {
                        crate::clock::test_time_override_guard(
                            crate::clock::current_time_millis() + 100_000,
                        )
                    });
                    let held = (fault == "wait").then(|| {
                        session
                            .borrow_complete_primary(
                                client,
                                f.context.id().pg_id(),
                                f.context.id().cluster_epoch(),
                                deadline(),
                            )
                            .unwrap()
                    });
                    let foreign = (fault == "foreign").then(|| UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                    NodeId::new(4), f.context.id().cluster_epoch(), client.test_execution_endpoint().clone(),
                    crate::node_client::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT, None,
                ).with_pg_topology(Arc::new(topology())));
                    let selected = if fault == "client" {
                        &f.remote(NodeId::new(0)).unwrap().client
                    } else {
                        foreign.as_ref().unwrap_or(client)
                    };
                    let started = Instant::now();
                    let failure = allocated
                        .into_stream_file_writer_remote(selected, session)
                        .err()
                        .expect("invalid authority must fail binding");
                    assert_eq!(failure.id(), f.context.id());
                    assert_eq!(failure.test_binding_state(), original);
                    let message = diagnostic(failure.error());
                    match fault {
                        "client" | "foreign" => assert!(
                            matches!(
                                failure.error(),
                                E::Store(StoreError::RouteCapabilitySubjectMismatch { .. })
                            ),
                            "{message}"
                        ),
                        "issuer" => assert!(message.contains("stream file issuer"), "{message}"),
                        "cursor" => {
                            assert!(message.contains("stream file consumed cursor"), "{message}")
                        }
                        "expired" | "wait" => assert!(
                            message.contains("deadline")
                                || message.contains("expired")
                                || message.contains("cutoff"),
                            "{message}"
                        ),
                        _ => unreachable!(),
                    }
                    if fault == "wait" {
                        assert!(started.elapsed() < Duration::from_secs(3));
                        assert!(Instant::now() >= original.0);
                    }
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        usize::from(matches!(fault, "issuer" | "cursor"))
                    );
                    drop(held);
                    drop(failure);
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                },
            );
        }
    }
}

#[test]
fn staging_stream_file_binding_remote_unix_authority_and_deadline_rejections() {
    invalid_authority(false);
}
#[test]
fn staging_stream_file_binding_remote_tls_authority_and_deadline_rejections() {
    invalid_authority(true);
}
