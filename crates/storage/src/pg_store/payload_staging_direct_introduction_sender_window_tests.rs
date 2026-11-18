// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh remote introductions must retain retirement custody through inspection.
use super::*;
const INSPECT: Kind = Kind::MetadataCommandReplicaState;

#[path = "payload_staging_direct_introduction_sender_file_tests.rs"]
mod files;

#[test]
fn staging_direct_introduction_sender_window_primary_loss_after_read_preserves_terminal_custody() {
    for tls in [false, true] {
        for during_reply in [false, true] {
            with_terminal!(tls, |f, session, terminal| {
                terminal.retire().unwrap();
                let primary = f.remote(NodeId::new(4)).unwrap();
                if during_reply {
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(|kind, frame| {
                            if kind == INSPECT {
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    false,
                                )
                                .unwrap();
                            }
                        }));
                }
                let before = f.observe();
                let bounds = terminal.test_bounds();
                let mut observed = false;
                let failure = terminal
                    .test_confirm_window(|point| {
                        if point == "after introduction window inspection" {
                            observed = true;
                            session.test_close_primary_section();
                        }
                    })
                    .err()
                    .expect("primary loss cannot mint a window holder");
                assert_eq!(
                    observed, !during_reply,
                    "exercise loss both during response and after a positive read"
                );
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                assert_eq!(terminal.test_bounds(), bounds);
                let _replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(terminal.id().pg_id(), deadline())
                    .unwrap();
                let failure = terminal
                    .into_confirmed_window()
                    .err()
                    .expect("no reconnect");
                drop(failure);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_window_requires_confirmed_retirement() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            let bounds = terminal.test_bounds();
            for expected in [Retirement::NoAttemptObserved, Retirement::Unconfirmed] {
                let before = f.observe();
                let failure = terminal
                    .test_confirm_window(|_| panic!("unconfirmed retirement reached inspection"))
                    .err()
                    .expect("retirement required");
                assert!(failure
                    .error()
                    .to_string()
                    .contains("confirmed slot retirement"));
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), expected);
                assert_eq!(terminal.test_bounds(), bounds);
                for node in 0..5 {
                    assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
                }
                assert_eq!(f.observe(), before);
                if expected == Retirement::NoAttemptObserved {
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(|kind, frame| {
                            if kind == RETIRE {
                                frame.payload = rpc::encode_storage_rpc_success_response(&[]);
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    true,
                                )
                                .unwrap();
                            }
                        }));
                    assert!(terminal.retire().is_err());
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(|_, _| {}));
                }
            }
            terminal.retire().unwrap();
            let before = f.observe();
            let window = terminal
                .into_confirmed_window()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            window.require_valid().unwrap();
            assert_eq!(window.test_bounds(), bounds);
            drop(window);
            assert_eq!(
                f.observe(),
                before,
                "inspection and Drop perform no durable or physical work"
            );
        });
    }
}

#[test]
fn staging_direct_introduction_sender_window_confirms_embedded_and_remote_witness_without_effects()
{
    for tls in [false, true] {
        for witness in [0, 3] {
            with_application!(@consuming [(4, tls), (witness, !tls), (2, tls)], |f, session, publication, owner, proof| {
                publication.publish().unwrap();
                for member in [0, 4, 2, 3, 1] { apply_member!(f, publication, member).unwrap(); }
                let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
                terminal.retire().unwrap();
                let before = f.observe();
                let bounds = terminal.test_bounds();
                let id = terminal.id();
                let mut points = Vec::new();
                let window = terminal.test_confirm_window(|point| points.push(point)).unwrap_or_else(|e| panic!("{}", e.error()));
                for point in ["before witness", "after witness", "after primary", "after introduction window inspection"] { assert!(points.contains(&point)); }
                assert_eq!(window.id(), id);
                assert_eq!(window.observation().witness, Some(NodeId::new(0)));
                assert_eq!(window.observation().prefix.applied_log_index, id.log_index().get());
                assert_eq!(window.test_bounds(), bounds);
                window.require_valid().unwrap();
                assert_eq!(f.observe(), before);
                session.test_close_primary_section();
                assert!(window.require_valid().is_err());
                drop(window);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_window_malformed_responses_retain_exact_retry_owner() {
    for tls in [false, true] {
        for fault in ["purpose", "descriptor", "witness", "empty", "error"] {
            with_terminal!(tls, |f, session, terminal| {
                terminal.retire().unwrap();
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != INSPECT {
                            return;
                        }
                        let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        match fault {
                            "purpose" => body[0] ^= 1,
                            "descriptor" => {
                                let i = body.len() - 37;
                                body[i] ^= 1;
                            }
                            "witness" => *body.last_mut().unwrap() ^= 1,
                            "empty" => body.clear(),
                            "error" => (),
                            _ => unreachable!(),
                        }
                        frame.payload = if fault == "error" {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected window inspection failure".into(),
                            })
                            .unwrap()
                        } else {
                            rpc::encode_storage_rpc_success_response(&body)
                        };
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true)
                            .unwrap();
                    }));
                let before = f.observe();
                let bounds = terminal.test_bounds();
                let failure = terminal.into_confirmed_window().err().expect(fault);
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                assert_eq!(terminal.test_bounds(), bounds);
                assert_eq!(terminal.publication(), Published::MarkerConfirmed);
                assert_eq!(f.observe(), before);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|_, _| {}));
                let window = terminal
                    .into_confirmed_window()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                window.require_valid().unwrap();
                assert_eq!(window.test_bounds(), bounds);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_window_rejects_damaged_primary_and_witness_without_effects() {
    for tls in [false, true] {
        for member in [0, 4] {
            with_terminal!(tls, |f, session, terminal| {
                terminal.retire().unwrap();
                let node = f.node(NodeId::new(member));
                {
                    let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                    let damage = if member == 4 {
                        "UPDATE payload_staging_windows SET cutoff_ms = cutoff_ms + 1"
                    } else {
                        "UPDATE metadata_command_log SET command_checksum = command_checksum + 1"
                    };
                    pg.conn.execute_batch(damage).unwrap();
                }
                let before = f.observe();
                let bounds = terminal.test_bounds();
                let failure = terminal
                    .into_confirmed_window()
                    .err()
                    .expect("damaged inspection");
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                assert_eq!(terminal.test_bounds(), bounds);
                drop(terminal);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_window_expiry_and_reconnection_never_renew_authority() {
    for tls in [false, true] {
        for point in [
            "before portable cutoff",
            "before witness",
            "after witness",
            "after primary",
            "after introduction window inspection",
        ] {
            with_application!(@until Instant::now() + std::time::Duration::from_secs(8), [(4, tls), (0, !tls), (2, tls)], |f, session, publication, owner, proof| {
                publication.publish().unwrap();
                for member in [0, 4, 2, 3, 1] { apply_member!(f, publication, member).unwrap(); }
                let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
                terminal.retire().unwrap();
                let before = f.observe();
                let bounds = terminal.test_bounds();
                let mut reached = false;
                let failure = terminal
                    .test_confirm_window(|at| {
                        if at == point {
                            reached = true;
                            assert!(Instant::now() < bounds.0, "must reach {point} before expiry");
                            while let Some(remaining) = bounds.0.checked_duration_since(Instant::now()) {
                                std::thread::sleep(remaining);
                            }
                        }
                    })
                    .err()
                    .expect("expired confirmation");
                assert!(reached, "{point}");
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                assert_eq!(terminal.test_bounds(), bounds);
                session.test_close_primary_section();
                let primary = f.remote(NodeId::new(4)).unwrap();
                let _replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(terminal.id().pg_id(), deadline())
                    .unwrap();
                let failure = terminal
                    .test_confirm_window(|_| panic!("reconnected session reached inspection"))
                    .err()
                    .expect("original session required");
                // Both the original deadline and session remain invalid; a
                // newly opened section must not renew either authority.
                assert!(failure.error().to_string().contains("deadline") || failure.error().to_string().contains("expired"));
                terminal = failure.into_terminal();
                assert_eq!(terminal.retirement(), Retirement::Retired);
                drop(terminal);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_window_keeps_primary_borrow_during_inspection() {
    for tls in [false, true] {
        with_terminal!(tls, |f, session, terminal| {
            terminal.retire().unwrap();
            let primary = f.remote(NodeId::new(4)).unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let id = terminal.id();
            let before = f.observe();
            std::thread::scope(|scope| {
                let session_ref = &session;
                let worker = scope.spawn(move || {
                    for _ in 0..2 {
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
                            .expect("primary borrow spans witness and primary inspection")
                            .to_string()
                            .contains("borrow complete primary session"));
                    }
                });
                let window = terminal
                    .test_confirm_window(|point| {
                        if matches!(point, "before witness" | "after primary") {
                            ready_tx.send(()).unwrap();
                            release_rx
                                .recv_timeout(std::time::Duration::from_secs(5))
                                .unwrap();
                        }
                    })
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                window.require_valid().unwrap();
                worker.join().unwrap();
            });
            assert_eq!(f.observe(), before);
        });
    }
}
