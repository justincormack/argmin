// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Sender contract tests. Real all-member application mints the holder; response
//! fixtures test acknowledgement validation, NOT remote deletion. The installed
//! candidate receiver now also exercises exact admitted retirement below.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteRetirementSendError as E, TerminalSlotRetirementObservation as O,
};
use crate::storage_rpc::{self as rpc, complete_retirement as wire};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn failure() -> StoreError {
    StoreError::OperationDeadlineExceeded {
        context: "injected retirement transport failure",
    }
}

fn confirmation(context: &ContextualCommand) -> Vec<u8> {
    wire::encode_confirmation(wire::Confirmation {
        node_id: NodeId::new(4),
        id: context.id(),
        command_sha256: context.encode_command().unwrap().command_sha256(),
    })
}

#[test]
fn staging_retirement_transport_preserves_history_and_binds_every_confirmation_field() {
    for case in [0, 1, 3, 4] {
        for history in ["inspected", "installed", "uncertain"] {
            terminal_retirement::with_terminal(case, history, |server, _, context, terminal, _| {
                let before = snapshot(
                    &server
                        .test_storage_node()
                        .get_pg(context.id().pg_id().get())
                        .unwrap(),
                );
                let encoded = context.encode_command().unwrap();
                let reply = confirmation(context);
                let mut frozen = None;
                let error = terminal
                    .test_retirement_attempt(deadline(), |attempt| {
                        let bytes = attempt.encode_request().unwrap();
                        let wire = wire::decode_request(&bytes).unwrap();
                        assert_eq!(wire.command.command, encoded.bytes());
                        assert_eq!(wire.command.checksum, encoded.checksum_crc64());
                        assert_eq!(wire.command.node_id, NodeId::new(4));
                        assert_eq!(wire.command.pg_id, context.id().pg_id());
                        frozen = Some((bytes, attempt.deadline()));
                        Err(attempt.not_sent(failure()))
                    })
                    .unwrap_err();
                assert!(matches!(error, E::NotSent(_)));
                assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                let (frozen, cutoff) = frozen.unwrap();
                let error = terminal
                    .test_retirement_attempt(deadline() + Duration::from_secs(600), |attempt| {
                        assert_eq!(attempt.encode_request().unwrap(), frozen);
                        assert_eq!(attempt.deadline(), cutoff);
                        Err(attempt.unconfirmed(failure()))
                    })
                    .unwrap_err();
                assert!(matches!(error, E::Unconfirmed(_)));
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                let error = terminal
                    .test_retirement_attempt(deadline(), |attempt| Err(attempt.not_sent(failure())))
                    .unwrap_err();
                assert!(matches!(error, E::NotSent(_)));
                assert_eq!(
                    terminal.slot_retirement_observation(),
                    O::Unconfirmed,
                    "later NotSent cannot erase earlier uncertainty"
                );
                let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _ = terminal.test_retirement_attempt(deadline(), |attempt| {
                        assert_eq!(attempt.encode_request().unwrap(), frozen);
                        panic!("interrupt borrowed retirement attempt");
                    });
                }));
                assert!(interrupted.is_err());
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                for offset in [0, 4, 6, 10, 18, 22, 30] {
                    let mut crossed = reply.clone();
                    crossed[offset] ^= 1;
                    let error = terminal
                        .test_retirement_attempt(deadline(), |attempt| attempt.confirm(&crossed))
                        .unwrap_err();
                    assert!(
                        matches!(
                            error,
                            E::Unconfirmed(StoreError::StorageRpc {
                                failure: rpc::StorageRpcErrorCode::PayloadDecode,
                                ..
                            })
                        ),
                        "{offset}: {error}"
                    );
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                }
                let publication = rpc::complete_publication::encode_confirmation(
                    rpc::complete_publication::Confirmation {
                        node_id: NodeId::new(4),
                        id: context.id(),
                        command_sha256: encoded.command_sha256(),
                    },
                );
                for wrong in [&[1_u8][..], publication.as_slice()] {
                    assert!(terminal
                        .test_retirement_attempt(deadline(), |attempt| attempt.confirm(wrong))
                        .is_err());
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                }
                // Explicit response-validation fixture, not a server-side effect.
                terminal
                    .test_retirement_attempt(deadline(), |attempt| attempt.confirm(&reply))
                    .unwrap();
                assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                terminal
                    .test_retirement_attempt(deadline(), |_| {
                        panic!("confirmed retirement dispatched again")
                    })
                    .unwrap();
                assert_eq!(
                    snapshot(
                        &server
                            .test_storage_node()
                            .get_pg(context.id().pg_id().get())
                            .unwrap()
                    ),
                    before,
                    "transport contract tests cannot remove custody"
                );
            });
        }
    }
}

#[test]
fn staging_retirement_transport_freezes_first_cutoff_and_rechecks_expiry_before_encoding() {
    terminal_retirement::with_terminal(3, "uncertain", |server, _, context, terminal, map| {
        let before = snapshot(
            &server
                .test_storage_node()
                .get_pg(context.id().pg_id().get())
                .unwrap(),
        );
        let now = crate::clock::current_time_millis();
        let clock = crate::clock::test_time_override_guard(now);
        let mut frozen = None;
        let _ = terminal.test_retirement_attempt(deadline(), |attempt| {
            frozen = Some((attempt.encode_request().unwrap(), attempt.deadline()));
            Err(attempt.unconfirmed(failure()))
        });
        let (frozen, cutoff) = frozen.unwrap();
        for wall in [now - 100, now + 100] {
            clock.set(wall);
            let _ = terminal.test_retirement_attempt(
                deadline() + Duration::from_secs(600),
                |attempt| {
                    assert_eq!(attempt.encode_request().unwrap(), frozen);
                    assert_eq!(attempt.deadline(), cutoff);
                    Err(attempt.not_sent(failure()))
                },
            );
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
        }
        let error = terminal
            .test_retirement_attempt(Instant::now(), |attempt| {
                let error = attempt.encode_request().unwrap_err();
                Err(attempt.not_sent(error))
            })
            .unwrap_err();
        assert!(matches!(
            error,
            E::NotSent(StoreError::OperationDeadlineExceeded { .. })
        ));
        assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
        clock.set(now);
        let error = terminal
            .test_retirement_attempt(deadline(), |attempt| {
                clock.set(now + 120_000);
                map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(now + 240_000).unwrap(),
                );
                map.require_route_map_valid_now().unwrap();
                let error = attempt.encode_request().unwrap_err();
                Err(attempt.not_sent(error))
            })
            .unwrap_err();
        assert!(matches!(error, E::NotSent(_)));
        assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
        assert!(terminal
            .test_retirement_attempt(deadline() + Duration::from_secs(600), |_| panic!(
                "renewed raw route must not renew retired admission"
            ))
            .is_err());
        assert_eq!(
            snapshot(
                &server
                    .test_storage_node()
                    .get_pg(context.id().pg_id().get())
                    .unwrap()
            ),
            before
        );
    });
}

#[test]
fn staging_retirement_transport_rejects_work_and_does_not_reset_local_retirement() {
    for case in [2, 5] {
        terminal_retirement::with_terminal(case, "inspected", |_, _, _, terminal, _| {
            let error = terminal
                .test_retirement_attempt(deadline(), |_| {
                    panic!("terminal work must prevent transport preparation")
                })
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("still requires terminal work integration"),
                "{error}"
            );
            assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
        });
    }
    terminal_retirement::with_terminal(1, "installed", |server, config, context, terminal, _| {
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                terminal
                    .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                    .unwrap();
                assert!(complete_pending::observe(pg).iter().all(Vec::is_empty));
            })
            .unwrap();
        terminal
            .test_retirement_attempt(deadline(), |_| {
                panic!("confirmed local retirement must not dispatch remotely")
            })
            .unwrap();
        assert_eq!(terminal.slot_retirement_observation(), O::Retired);
    });
}

#[test]
fn staging_retirement_transport_installed_receiver_retires_once_without_hidden_retry() {
    terminal_retirement::with_terminal_client(
        1,
        "uncertain",
        |server, config, context, terminal, _, client| {
            let before = introduction::state(
                &server
                    .test_storage_node()
                    .get_pg(context.id().pg_id().get())
                    .unwrap(),
            );
            let count = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&count);
            server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                if kind == rpc::StorageRpcMessageKind::MetadataCommandPendingSlotRemove {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            }));
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| server.accept_one().unwrap());
                let wrong_pg = config
                    .pg_ids
                    .iter()
                    .copied()
                    .find(|pg| *pg != context.id().pg_id().get())
                    .unwrap();
                let section = client
                    .open_metadata_command_critical_section_until(PgId::new(wrong_pg), deadline())
                    .unwrap();
                let error = terminal
                    .retire_remote(client, &section, deadline())
                    .unwrap_err();
                assert!(matches!(
                    error,
                    E::NotSent(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "complete retirement session subject"
                    })
                ));
                assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                assert_eq!(count.load(Ordering::SeqCst), 0);
                drop(section);
                worker.join().unwrap();
            });
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| server.accept_one().unwrap());
                let section = client
                    .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                    .unwrap();
                let (_, foreign) = prepared(config);
                let error = terminal
                    .retire_remote(&foreign, &section, deadline())
                    .unwrap_err();
                assert!(matches!(
                    error,
                    E::NotSent(StoreError::RouteCapabilitySubjectMismatch { .. })
                ));
                assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                assert_eq!(count.load(Ordering::SeqCst), 0);
                terminal
                    .retire_remote(client, &section, deadline())
                    .unwrap();
                assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    1,
                    "one explicit dispatch only"
                );
                terminal
                    .retire_remote(client, &section, Instant::now())
                    .unwrap();
                assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                assert_eq!(count.load(Ordering::SeqCst), 1);
                drop(section);
                worker.join().unwrap();
            });
            assert_eq!(
                introduction::state(
                    &server
                        .test_storage_node()
                        .get_pg(context.id().pg_id().get())
                        .unwrap()
                ),
                before
            );
        },
    );
}

#[test]
fn staging_retirement_transport_frozen_context_vector_is_the_complete_command() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_retirement_payload_v1_command_v14_context_v1.hex"
    ));
    let decoded = wire::decode_request(&request).unwrap();
    assert!(
        rpc::decode_metadata_command_pending_slot_request(
            &request,
            &crate::node_runtime::MetadataCommandDecodeAuthority::new(),
        )
        .is_err(),
        "no legacy fallback"
    );
    let encoded = context.encode_command().unwrap();
    assert_eq!(decoded.command.command, encoded.bytes());
    assert_eq!(decoded.command.checksum, encoded.checksum_crc64());
    assert_eq!(
        confirmation(&context),
        crate::tests::frozen_hex_fixture(include_str!(
            "testdata/complete_retirement_confirmation_v1_command_v14_context_v1.hex"
        ))
    );
}

#[test]
fn staging_retirement_transport_late_exact_confirmation_does_not_renew_authority() {
    terminal_retirement::with_terminal(0, "inspected", |_, _, context, terminal, map| {
        let now = crate::clock::current_time_millis();
        let clock = crate::clock::test_time_override_guard(now);
        let reply = confirmation(context);
        terminal
            .test_retirement_attempt(deadline(), |attempt| {
                let bytes = attempt.encode_request().unwrap();
                let cutoff = wire::decode_request(&bytes)
                    .unwrap()
                    .deadline
                    .portable_wall_valid_until_ms;
                clock.set(now + 120_000);
                assert!(crate::clock::current_time_millis() >= cutoff);
                map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(now + 240_000).unwrap(),
                );
                map.require_route_map_valid_now().unwrap();
                attempt.confirm(&reply)
            })
            .unwrap();
        assert_eq!(terminal.slot_retirement_observation(), O::Retired);
        // Wall-clock expiry need not expire the independently captured monotonic
        // route deadline. An already-confirmed operation is historical evidence:
        // asking again returns that observation without preparing or dispatching
        // another effect. The unconfirmed-attempt test checks expiry rejection.
        terminal
            .test_retirement_attempt(deadline() + Duration::from_secs(600), |_| {
                panic!("late confirmation must not create new authority")
            })
            .unwrap();
        assert_eq!(terminal.slot_retirement_observation(), O::Retired);
    });
}
