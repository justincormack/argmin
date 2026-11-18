// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming remote binding, not remote shard IO. No observation can replace
//! the retained context/history or extend its original confirmation cutoff.
use super::*;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
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

#[test]
fn staging_direct_file_binding_remote_rejects_reopened_same_client_session() {
    for tls in [false, true] {
        for history in ["installed", "inspected", "uncertain"] {
            let (_dir, f, proof) = allocation_remote::setup(tls, StagingTarget::DirectPut, false);
            run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
                primary.retire(&mut terminal);
                let confirmed = primary
                    .confirm(terminal, deadline(), |_| {})
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let original = confirmed.installation_observation();
                let Primary::Remote(client, session) = primary else {
                    unreachable!()
                };
                let id = f.context.id();
                let before = f.observe();
                let shadows = f.shadows();
                // Invoke the same close/shutdown path as Drop, then actually
                // acquire a new section through the exact original client.
                session.test_close_primary_section();
                let replacement = client
                    .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                    .unwrap();
                // All prior node/PG/epoch/client/open-section checks succeed.
                drop(
                    replacement
                        .borrow_complete_primary(client, id.pg_id(), id.cluster_epoch(), deadline())
                        .unwrap(),
                );
                let calls = Arc::new(AtomicUsize::new(0));
                let received = Arc::clone(&calls);
                f.remote(NodeId::new(4))
                    .unwrap()
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::MetadataCommandReplicaState {
                            received.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let failure = confirmed
                    .into_direct_file_writer_remote(
                        client,
                        &replacement,
                        &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                    )
                    .err()
                    .expect("reopened section must not mint a file writer");
                assert_eq!(failure.id(), id);
                assert_eq!(failure.test_binding_state().1, original);
                assert!(
                    matches!(
                        failure.error(),
                        E::Store(StoreError::RouteCapabilitySubjectMismatch {
                            operation:
                                "confirmed window requires its original remote primary session",
                        })
                    ),
                    "{}",
                    diagnostic(failure.error())
                );
                assert_eq!(calls.load(Ordering::SeqCst), 0, "reject before transport");
                assert_eq!(f.observe(), before);
                assert_eq!(f.shadows(), shadows);
                drop(failure);
                assert_eq!(
                    f.observe(),
                    before,
                    "failed binding keeps durable custody on Drop"
                );
            });
        }
    }
}

#[test]
fn staging_direct_file_binding_remote_keeps_original_cutoff_history_and_state() {
    for tls in [false, true] {
        for history in ["installed", "inspected", "uncertain"] {
            let (_dir, f, proof) = allocation_remote::setup(tls, StagingTarget::DirectPut, false);
            run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
                primary.retire(&mut terminal);
                let peer = f.remote(NodeId::new(4)).unwrap();
                let echoes = Arc::new(Mutex::new(Vec::new()));
                let received = Arc::clone(&echoes);
                peer.server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == Kind::MetadataCommandReplicaState {
                            let payload = rpc::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap();
                            received
                                .lock()
                                .unwrap()
                                .push((payload[..4].to_vec(), payload[6..14].to_vec()));
                        }
                    }));
                // Force the inspection projection to differ from any projection
                // made earlier by its caller. Restore wall time before RPC/auth
                // IO and preserve monotonic time independently; the test must
                // not depend on a millisecond scheduling race.
                let mut projection_clock = None;
                let mut projected = false;
                let confirmed = crate::clock::with_time_and_monotonic_override(
                    crate::clock::current_time_millis(),
                    crate::clock::monotonic_time_millis(),
                    || {
                        primary.confirm(terminal, deadline(), |point| {
                            if point == "before portable cutoff" {
                                projected = true;
                                projection_clock = Some(crate::clock::test_time_override_guard(
                                    crate::clock::current_time_millis() - 100,
                                ));
                            } else if point == "before witness" {
                                drop(projection_clock.take());
                            }
                        })
                    },
                )
                .unwrap_or_else(|e| panic!("{}", e.error()));
                assert!(projected);
                assert!(projection_clock.is_none());
                let original = confirmed.installation_observation();
                let before = f.observe();
                let shadows = f.shadows();
                let Primary::Remote(client, session) = primary else {
                    unreachable!()
                };
                // Moving the sender wall clock forward must not recompute a
                // later portable cutoff from the same monotonic deadline.
                let writer = crate::clock::with_time_and_monotonic_override(
                    crate::clock::current_time_millis() + 100,
                    crate::clock::monotonic_time_millis(),
                    || {
                        confirmed.into_direct_file_writer_remote(
                            client,
                            session,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                    },
                )
                .unwrap_or_else(|e| panic!("{}", diagnostic(e.error())));
                assert_eq!(writer.id(), f.context.id());
                assert_eq!(writer.test_binding_state().1, original);
                peer.server
                    .test_require_window_primary_released(f.context.id().pg_id());
                assert!(session
                    .borrow_complete_primary(
                        client,
                        f.context.id().pg_id(),
                        f.context.id().cluster_epoch(),
                        deadline()
                    )
                    .is_err());
                let observed = echoes.lock().unwrap();
                assert_eq!(observed.len(), 2, "one confirmation, one binding; no retry");
                assert_eq!(observed[0].0, b"WPOK");
                assert_eq!(observed[1].0, b"DPOK");
                assert_eq!(
                    observed[0].1, observed[1].1,
                    "binding must reuse the captured absolute cutoff"
                );
                assert_eq!(f.observe(), before);
                assert_eq!(f.shadows(), shadows);
                drop(writer);
                assert_eq!(f.observe(), before, "Drop cannot release custody");
            });
        }
    }
}

#[test]
fn staging_direct_file_binding_remote_bad_replies_keep_custody_without_retry() {
    for tls in [false, true] {
        for fault in [
            "purpose",
            "descriptor",
            "witness",
            "prefix",
            "error",
            "closed",
        ] {
            let (_dir, f, proof) = allocation_remote::setup(tls, StagingTarget::DirectPut, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "uncertain",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let original = confirmed.installation_observation();
                    let before = f.observe();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let received = Arc::clone(&calls);
                    f.remote(NodeId::new(4)).unwrap().server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != Kind::MetadataCommandReplicaState { return; }
                    received.fetch_add(1, Ordering::SeqCst);
                    let envelope = rpc::decode_storage_rpc_response_payload_with_connection_disposition(&frame.payload).unwrap();
                    let mut payload = envelope.response.unwrap();
                    match fault {
                        "purpose" => payload[..4].copy_from_slice(b"WPOK"),
                        "descriptor" => payload[96] ^= 1,
                        "witness" => payload[129] ^= 1,
                        "prefix" => payload[70] ^= 1,
                        "error" | "closed" => {},
                        _ => unreachable!(),
                    }
                    frame.payload = if fault == "error" {
                        rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse { code: rpc::StorageRpcErrorCode::TransportTimeout, message: "lost binding response".into() }).unwrap()
                    } else { rpc::encode_storage_rpc_success_response(&payload) };
                    rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, envelope.connection_reusable && fault != "closed").unwrap();
                }));
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let failure = confirmed
                        .into_direct_file_writer_remote(
                            client,
                            session,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                        .err()
                        .expect("bad response cannot mint a writer");
                    assert_eq!(failure.id(), f.context.id());
                    assert_eq!(failure.test_binding_state().1, original);
                    if fault == "prefix" {
                        assert!(matches!(failure.error(), E::PrefixDisagreement));
                    } else {
                        let message = diagnostic(failure.error());
                        assert!(
                            message.contains(match fault {
                                "purpose" => "purpose",
                                "descriptor" | "witness" => "descriptor/witness",
                                "error" => "lost binding response",
                                "closed" => "lost its held section",
                                _ => unreachable!(),
                            }),
                            "{fault}: {message}"
                        );
                    }
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    drop(failure);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_direct_file_binding_remote_admission_and_wait_cannot_renew_custody() {
    for tls in [false, true] {
        for fault in [
            "client",
            "foreign",
            "expired",
            "session-wait",
            "issuer",
            "target",
        ] {
            let target = if fault == "target" {
                StagingTarget::StreamPut
            } else {
                StagingTarget::DirectPut
            };
            let (_dir, f, proof) = allocation_remote::setup(tls, target, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "inspected",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let cutoff = if fault == "session-wait" {
                        Instant::now() + Duration::from_secs(2)
                    } else {
                        deadline()
                    };
                    let confirmed = primary
                        .confirm(terminal, cutoff, |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let original = confirmed.installation_observation();
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    if fault == "issuer" {
                        let node = f.node(NodeId::new(4));
                        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                        pg.begin_allocation_lifetime_replacement()
                            .unwrap()
                            .complete()
                            .unwrap();
                        pg.recover_complete_command_state(
                            PgStoreRecoveryContext::for_node(NodeId::new(4)),
                            &topology(),
                        )
                        .unwrap();
                    }
                    let before = f.observe();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let observed = Arc::clone(&calls);
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::MetadataCommandReplicaState {
                                observed.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let _clock = (fault == "expired").then(|| {
                        crate::clock::test_time_override_guard(
                            crate::clock::current_time_millis() + 100_000,
                        )
                    });
                    let held = (fault == "session-wait").then(|| {
                        session
                            .borrow_complete_primary(
                                client,
                                f.context.id().pg_id(),
                                f.context.id().cluster_epoch(),
                                deadline(),
                            )
                            .unwrap()
                    });
                    let started = Instant::now();
                    let foreign = (fault == "foreign").then(|| UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                    NodeId::new(4), f.context.id().cluster_epoch(), client.test_execution_endpoint().clone(),
                    crate::node_client::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT, None,
                ).with_pg_topology(Arc::new(topology())));
                    let failure = confirmed
                        .into_direct_file_writer_remote(
                            if fault == "client" {
                                &f.remote(NodeId::new(0)).unwrap().client
                            } else if let Some(foreign) = &foreign {
                                foreign
                            } else {
                                client
                            },
                            session,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                        .err()
                        .expect("invalid authority cannot bind a file writer");
                    assert_eq!(failure.id(), f.context.id());
                    assert_eq!(failure.test_binding_state().1, original);
                    if fault == "session-wait" {
                        assert!(started.elapsed() < Duration::from_secs(3));
                        assert!(Instant::now() >= failure.test_binding_state().0);
                    }
                    match fault {
                        "client" | "foreign" => assert!(
                            matches!(
                                failure.error(),
                                E::Store(StoreError::RouteCapabilitySubjectMismatch { .. })
                            ),
                            "{}",
                            failure.error()
                        ),
                        "issuer" | "target" => assert!(
                            diagnostic(failure.error()).contains("target/issuer"),
                            "{}",
                            diagnostic(failure.error())
                        ),
                        "expired" | "session-wait" => {
                            let message = diagnostic(failure.error());
                            assert!(
                                message.contains("deadline")
                                    || message.contains("cutoff")
                                    || message.contains("expired"),
                                "{message}"
                            );
                        }
                        _ => unreachable!(),
                    }
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        usize::from(matches!(fault, "issuer" | "target"))
                    );
                    drop(held);
                    drop(failure);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}
