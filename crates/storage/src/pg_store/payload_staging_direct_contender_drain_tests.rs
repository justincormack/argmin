// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real encrypted PUT custody survives draining exactly one other command.
use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, PublicationObservation as Published,
    TerminalSlotRetirementObservation as Retired,
};

macro_rules! with_ready {
    (|$f:ident, $ready:ident, $fixture:ident, $admission:ident| $body:block) => {
        with_request_writer!(4096, |$f, writer, $fixture, $admission| {
            let prepared = writer
                .prepare_encryption(4096)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            $fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
            let evidence =
                $fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            $fixture.encryption = evidence.encryption().clone();
            let mut payload = prepared
                .bind_ciphertext($fixture.storage_bytes.clone(), evidence)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let owner = files::load_owner(&$f);
            payload
                .write_files(&vec![None; owner.windows[0].participants.files.len()])
                .unwrap();
            payload.persist_acknowledgements(None).unwrap();
            let $ready = payload
                .into_acknowledged()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            $body
        });
    };
}

fn install_next(f: &Setup) -> MetadataCommandId {
    let pg = f
        .map
        .node(NodeId::new(4))
        .unwrap()
        .test_node()
        .get_pg(f.context.id().pg_id().get())
        .unwrap();
    let command = complete_node_startup::next(&pg, f.context.id().cluster_epoch());
    let id = command.id();
    complete_pending::install(&mut command.into_submission(), &pg).unwrap();
    id
}

fn physical(f: &Setup) -> Vec<Vec<u8>> {
    let owner = files::load_owner(f);
    let window = &owner.windows[0];
    window
        .participants
        .files
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let key = crate::ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
            f.map
                .node(*node)
                .unwrap()
                .test_node()
                .read_shard_file(window.participants.data_pg.get(), &key)
                .unwrap()
        })
        .collect()
}

#[test]
fn staging_direct_contender_drain_one_effect_at_a_time_retains_payload_and_uncertain_confirmations()
{
    for loss in ["none", "marker", "apply", "apply-unwind", "retire", "next"] {
        with_ready!(|f, ready, fixture, admission| {
            let bounds = ready.test_terminal_bounds();
            let original_files = physical(&f);
            let primary = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    ready.id().pg_id(),
                    ready.id().cluster_epoch(),
                )
                .unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let id = install_next(&f);
            let captured = ready
                .inspect_contender_embedded(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let drain = captured
                .into_embedded_drain(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(drain.id(), id);
            assert_eq!(drain.test_attempts(), 1);
            let failure = drain
                .into_terminal(&section)
                .err()
                .expect("no premature handoff");
            let mut drain = failure.into_writer();
            if loss == "marker" {
                let mut reached = false;
                let error = drain
                    .test_advance_embedded_with_hook(&section, |point| {
                        if point == "after-commit" {
                            reached = true;
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "contender marker reply lost",
                            });
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached && error.to_string().contains("contender marker reply lost"));
                assert_eq!(drain.publication(), Published::Unconfirmed);
                for node in 0..5 {
                    assert_eq!(
                        drain.application(NodeId::new(node)),
                        Applied::NoAttemptObserved
                    );
                }
            }
            let marker = drain.advance_embedded(&section).unwrap();
            assert!(!marker.is_ready());
            assert_eq!(marker.applied_node(), None);
            for node in 0..5 {
                assert_eq!(
                    drain.application(NodeId::new(node)),
                    Applied::NoAttemptObserved
                );
            }
            if matches!(loss, "apply" | "apply-unwind") {
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drain.test_advance_embedded_with_hook(&section, |point| {
                        if point == "after-commit" {
                            reached = true;
                            assert_ne!(
                                loss, "apply-unwind",
                                "contender apply interrupted after commit"
                            );
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "contender witness reply lost",
                            });
                        }
                        Ok(())
                    })
                }));
                if loss == "apply-unwind" {
                    assert!(result.is_err());
                } else {
                    result.unwrap().unwrap_err();
                }
                assert!(reached);
                assert_eq!(drain.application(NodeId::new(0)), Applied::Unconfirmed);
                assert_eq!(
                    drain.application(NodeId::new(4)),
                    Applied::NoAttemptObserved
                );
                let applied = members(&f);
                assert_eq!(
                    drain.advance_embedded(&section).unwrap().applied_node(),
                    Some(NodeId::new(0))
                );
                assert_eq!(
                    members(&f),
                    applied,
                    "exact witness retry must not apply twice"
                );
            } else {
                assert_eq!(
                    drain.advance_embedded(&section).unwrap().applied_node(),
                    Some(NodeId::new(0))
                );
            }
            let failure = drain
                .into_terminal(&section)
                .err()
                .expect("partial confirmation retains drain");
            let mut drain = failure.into_writer();
            for (index, node) in [4, 1, 2, 3].into_iter().enumerate() {
                let attempts = drain.test_attempts();
                assert_eq!(
                    drain.advance_embedded(&section).unwrap().applied_node(),
                    Some(NodeId::new(node))
                );
                assert_eq!(drain.test_attempts(), attempts + 1);
                assert_eq!(
                    (0..5)
                        .filter(|id| drain.application(NodeId::new(*id)) == Applied::Applied)
                        .count(),
                    index + 2
                );
            }
            let attempts = drain.test_attempts();
            let applied = members(&f);
            for _ in 0..2 {
                assert!(drain.advance_embedded(&section).unwrap().is_ready());
            }
            assert_eq!(members(&f), applied);
            assert_eq!(drain.test_attempts(), attempts);
            let terminal = drain
                .into_terminal(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let failure = terminal
                .into_payload()
                .err()
                .expect("all-member apply is not slot retirement");
            let mut terminal = failure.into_writer();
            if loss == "retire" {
                let mut reached = false;
                terminal
                    .test_retire_embedded_with_hook(&section, |point| {
                        if point == "after-commit" {
                            reached = true;
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "contender retirement reply lost",
                            });
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached);
                assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                let failure = terminal
                    .into_payload()
                    .err()
                    .expect("unconfirmed retirement cannot release payload");
                terminal = failure.into_writer();
                let retired = members(&f);
                terminal.retire_embedded(&section).unwrap();
                assert_eq!(
                    members(&f),
                    retired,
                    "lost deletion reply retries observationally"
                );
            } else {
                terminal.retire_embedded(&section).unwrap();
            }
            assert_eq!(terminal.retirement(), Retired::Retired);
            let next = (loss == "next").then(|| install_next(&f));
            let retired = members(&f);
            let attempts = terminal.test_attempts();
            terminal.retire_embedded(&section).unwrap();
            assert_eq!(
                members(&f),
                retired,
                "confirmed retirement cannot touch a successor"
            );
            assert_eq!(terminal.test_attempts(), attempts);
            let ready = terminal
                .into_payload()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(ready.test_terminal_bounds(), bounds);
            assert_eq!(ready.test_attempts(), attempts);
            assert_eq!(physical(&f), original_files);
            if let Some(next) = next {
                let captured = ready
                    .inspect_contender_embedded(&section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(captured.contender_id(), Some(next));
                assert_eq!(captured.test_attempts(), attempts + 1);
            } else {
                let inspected = ready
                    .inspect_embedded(&section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(inspected.test_attempts(), attempts + 1);
            }
            assert_eq!(members(&f), retired);
            assert_eq!(physical(&f), original_files);
        });
    }
}

#[test]
fn staging_direct_contender_drain_preserves_custody_on_budget_or_deadline_exhaustion() {
    for phase in ["marker", "witness", "retirement"] {
        for expiry in [false, true] {
            with_ready!(|f, ready, fixture, admission| {
                let bounds = ready.test_terminal_bounds();
                let original_files = physical(&f);
                let primary = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        ready.id().pg_id(),
                        ready.id().cluster_epoch(),
                    )
                    .unwrap();
                let section = primary.lock_until(deadline()).unwrap();
                let id = install_next(&f);
                let captured = ready
                    .inspect_contender_embedded(&section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let mut drain = captured
                    .into_embedded_drain(&section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                if phase != "marker" {
                    drain.advance_embedded(&section).unwrap();
                }
                if phase == "retirement" {
                    for _ in 0..5 {
                        drain.advance_embedded(&section).unwrap();
                    }
                    let mut terminal = drain
                        .into_terminal(&section)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let attempts = terminal.test_attempts();
                    if !expiry {
                        terminal.test_limit_attempts(attempts);
                    }
                    let before = members(&f);
                    let error = if expiry {
                        crate::clock::with_time_override(bounds.1, || {
                            terminal.retire_embedded(&section)
                        })
                        .unwrap_err()
                    } else {
                        terminal.retire_embedded(&section).unwrap_err()
                    };
                    assert!(
                        error.to_string().contains(if expiry {
                            "original work cutoff"
                        } else {
                            "attempt budget"
                        }),
                        "{error}"
                    );
                    assert_eq!(terminal.retirement(), Retired::NoAttemptObserved);
                    assert_eq!(terminal.test_attempts(), attempts);
                    assert!(terminal.into_payload().is_err());
                    assert_eq!(members(&f), before);
                } else {
                    let attempts = drain.test_attempts();
                    if !expiry {
                        drain.test_limit_attempts(attempts);
                    }
                    let before = members(&f);
                    let error = if expiry {
                        crate::clock::with_time_override(bounds.1, || {
                            drain.advance_embedded(&section)
                        })
                        .unwrap_err()
                    } else {
                        drain.advance_embedded(&section).unwrap_err()
                    };
                    assert!(
                        error.to_string().contains(if expiry {
                            "original work cutoff"
                        } else {
                            "attempt budget"
                        }),
                        "{error}"
                    );
                    assert_eq!(drain.test_attempts(), attempts);
                    assert_eq!(drain.id(), id);
                    assert_eq!(
                        drain.application(NodeId::new(0)),
                        Applied::NoAttemptObserved
                    );
                    drop(drain);
                    assert_eq!(members(&f), before);
                }
                assert_eq!(physical(&f), original_files);
            });
        }
    }
}

#[test]
fn staging_direct_contender_drain_rejects_clear_and_nonquiescent_slots_before_effects() {
    for terminal_command in [false, true] {
        with_ready!(|f, ready, fixture, admission| {
            let primary = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    ready.id().pg_id(),
                    ready.id().cluster_epoch(),
                )
                .unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let ready = if terminal_command {
                let input = terminal_input(fixture.encryption.clone(), fixture.payload_crc64);
                let inspected = ready
                    .inspect_embedded(&section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let prepared = inspected
                    .prepare_terminal_embedded(&section, &input)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let command = prepared.test_command();
                let ready = prepared
                    .into_submission()
                    .into_reinspection()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let context =
                    ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap();
                let pg = f
                    .map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(ready.id().pg_id().get())
                    .unwrap();
                complete_pending::install(&mut context.into_submission(), &pg).unwrap();
                ready
            } else {
                ready
            };
            let captured = ready
                .inspect_contender_embedded(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let before = members(&f);
            let files = physical(&f);
            let expected = captured.contender_id();
            let failure = captured
                .into_embedded_drain(&section)
                .err()
                .expect("unsupported drain rejected");
            assert!(failure.error().to_string().contains(if terminal_command {
                "quiescent terminal work"
            } else {
                "requires a captured command"
            }));
            let captured = failure.into_writer();
            assert_eq!(captured.contender_id(), expected);
            drop(captured);
            assert_eq!(members(&f), before);
            assert_eq!(physical(&f), files);
        });
    }
}
