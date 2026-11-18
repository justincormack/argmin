// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real append ciphertext through all-member apply, exact slot retirement and
//! successor confirmation/allocation. Durable session/owner resources survive.
use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retired;

macro_rules! installed_publication {
    ($owner:expr, $writer:ident, $section:expr) => {{
        let prepared = prepared!($owner, $writer, $section);
        let mut submission = prepared.into_submission();
        submission.install_embedded($section).unwrap();
        submission
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[test]
fn staging_acknowledged_append_terminal_requires_exact_all_member_confirmation() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        with_writer_and_section(target, false, |f, owner, mut writer, section| {
            let mut publication = installed_publication!(owner, writer, section);
            let history = publication.test_binding_state();
            let cutoff = publication.test_cutoff();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg_id = publication.id().pg_id().get();
            for next in [None, Some(0), Some(4), Some(2), Some(3), Some(1)] {
                let before = {
                    let pg = node.get_pg(pg_id).unwrap();
                    (
                        introduction::state(&pg),
                        complete_pending::observe(&pg),
                        pg.conn.total_changes(),
                    )
                };
                let failure = match publication.into_terminal() {
                    Ok(_) => panic!("incomplete append reached terminal handoff"),
                    Err(failure) => failure,
                };
                assert!(failure.error().to_string().contains("confirmed"));
                publication = failure.into_publication();
                assert_eq!(publication.test_binding_state(), history);
                assert_eq!(publication.test_cutoff(), cutoff);
                {
                    let pg = node.get_pg(pg_id).unwrap();
                    assert_eq!(
                        (
                            introduction::state(&pg),
                            complete_pending::observe(&pg),
                            pg.conn.total_changes()
                        ),
                        before
                    );
                }
                match next {
                    None => publication.publish_embedded(section).unwrap(),
                    Some(member) => {
                        publication
                            .apply_embedded(section, NodeId::new(member))
                            .unwrap();
                    }
                }
            }
            let installation = publication.test_append_installation();
            let before = node.get_pg(pg_id).unwrap().conn.total_changes();
            let terminal = publication
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(terminal.installation_observation(), installation);
            for member in [0, 4, 2, 3, 1] {
                assert_eq!(
                    terminal.application_observation(NodeId::new(member)),
                    Applied::Applied
                );
            }
            assert_eq!(
                terminal.slot_retirement_observation(),
                Retired::NoAttemptObserved
            );
            assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), before);
            assert!(marker(&node.get_pg(pg_id).unwrap()));
            drop(terminal);
            assert_files_retained(f, owner);
        });
    }
}

#[test]
fn staging_acknowledged_append_terminal_rolls_over_without_releasing_session_resources() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            // Multiple window/file bindings conservatively shorten authority
            // for clock skew. Give the two-round traversal its budget up front;
            // no handoff is allowed to renew it.
            with_writer_and_section_duration(
                target.clone(),
                appended,
                Duration::from_secs(15),
                |f, initial, mut writer, section| {
                    let mut owner = initial.clone();
                    let mut payloads = Vec::new();
                    for round in 0..2 {
                        let mut publication = installed_publication!(&owner, writer, section);
                        let id = publication.id();
                        let cutoff = publication.test_cutoff();
                        let installation = publication.test_append_installation();
                        publication.publish_embedded(section).unwrap();
                        for member in [0, 4, 2, 3, 1] {
                            publication
                                .apply_embedded(section, NodeId::new(member))
                                .unwrap();
                        }
                        payloads.push(owner.clone());
                        let mut terminal = publication
                            .into_terminal()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert!(terminal.work().is_quiescent());
                        let failure = match terminal.into_confirmed_window_embedded(
                            section,
                            deadline(),
                            |_| {},
                        ) {
                            Ok(_) => panic!("unretired append minted a successor window"),
                            Err(failure) => failure,
                        };
                        assert!(failure
                            .error()
                            .to_string()
                            .contains("requires confirmed slot retirement"));
                        terminal = failure.into_terminal();
                        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                        let before = introduction::state(&node.get_pg(id.pg_id().get()).unwrap());
                        terminal.retire_embedded(section, deadline()).unwrap();
                        assert_eq!(terminal.slot_retirement_observation(), Retired::Retired);
                        assert_eq!(
                            introduction::state(&node.get_pg(id.pg_id().get()).unwrap()),
                            before
                        );
                        pending_install::assert_empty(&node.get_pg(id.pg_id().get()).unwrap());
                        let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
                        terminal.retire_embedded(section, deadline()).unwrap();
                        assert_eq!(
                            node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                            writes
                        );
                        let window = terminal
                            .into_confirmed_window_embedded(section, deadline(), |_| {})
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(window.id(), id);
                        assert_eq!(window.installation_observation(), installation);
                        assert_eq!(
                            node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                            writes
                        );
                        crate::clock::with_time_override(cutoff.1, || {
                            assert!(
                                window.require_valid().is_err(),
                                "successor must not renew inherited authority"
                            );
                        });
                        for payload in &payloads {
                            assert_files_retained(f, payload);
                        }
                        let (next, mut request, expected) = allocation::inputs(f, target.clone());
                        assert_eq!(next.subject, initial.subject);
                        assert_eq!(next.resources, initial.resources);
                        assert_eq!(next.windows.len(), initial.windows.len() + round + 1);
                        if round == 1 {
                            drop(window);
                            break;
                        }
                        let shards = files::shards(&next, b"data");
                        let tail = next.windows.last().unwrap();
                        let mut stored = shards[..usize::from(tail.geometry.ec.k)].concat();
                        stored.truncate(tail.geometry.codec.stored_size(4).unwrap() as usize);
                        request.segment_crc64 = checksum::crc64::checksum(&stored);
                        request.payload_crc64 = checksum::crc64::checksum(b"data");
                        let allocated = window
                            .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        writer = Writer::Stream(Box::new(
                            allocated
                                .into_stream_file_writer_embedded(section)
                                .unwrap_or_else(|e| panic!("{}", e.error())),
                        ));
                        owner = next;
                    }
                    for payload in &payloads {
                        assert_files_retained(f, payload);
                    }
                },
            );
        }
    }
}

#[test]
fn staging_acknowledged_append_terminal_lost_retirement_reply_blocks_successor_until_retry() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for (at, unwind) in [
            ("before-delete", false),
            ("after-commit", false),
            ("after-commit", true),
        ] {
            with_writer_and_section(target.clone(), false, |f, owner, mut writer, section| {
                let mut publication = installed_publication!(owner, writer, section);
                let id = publication.id();
                publication.publish_embedded(section).unwrap();
                for member in [0, 4, 2, 3, 1] {
                    publication
                        .apply_embedded(section, NodeId::new(member))
                        .unwrap();
                }
                let mut terminal = publication
                    .into_terminal()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let before = introduction::state(&node.get_pg(id.pg_id().get()).unwrap());
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    terminal.test_retire_embedded_with_hook(section, deadline(), |stage| {
                        if stage == at {
                            reached = true;
                            assert!(!unwind, "append retirement reply lost on unwind");
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "append retirement reply lost",
                            });
                        }
                        Ok(())
                    })
                }));
                assert!(reached);
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("append retirement reply lost"));
                }
                assert_eq!(terminal.slot_retirement_observation(), Retired::Unconfirmed);
                let slot = complete_pending::observe(&node.get_pg(id.pg_id().get()).unwrap());
                assert_eq!(slot.iter().all(Vec::is_empty), at == "after-commit");
                assert_eq!(
                    introduction::state(&node.get_pg(id.pg_id().get()).unwrap()),
                    before
                );
                let failure =
                    match terminal.into_confirmed_window_embedded(section, deadline(), |_| {}) {
                        Ok(_) => panic!("unconfirmed retirement minted successor"),
                        Err(failure) => failure,
                    };
                terminal = failure.into_terminal();
                assert_eq!(terminal.slot_retirement_observation(), Retired::Unconfirmed);
                crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 60_000,
                    || {
                        assert!(terminal.retire_embedded(section, deadline()).is_err());
                    },
                );
                assert_eq!(terminal.slot_retirement_observation(), Retired::Unconfirmed);
                terminal.retire_embedded(section, deadline()).unwrap();
                assert_eq!(terminal.slot_retirement_observation(), Retired::Retired);
                pending_install::assert_empty(&node.get_pg(id.pg_id().get()).unwrap());
                assert_eq!(
                    introduction::state(&node.get_pg(id.pg_id().get()).unwrap()),
                    before
                );
                // Failed/unwound retirement deliberately invalidates local
                // settled-prefix validation, even if the slot committed. An
                // exact retry confirms retirement, not canonical state. Keep
                // the terminal holder while explicit recovery revalidates it.
                let failure =
                    match terminal.into_confirmed_window_embedded(section, deadline(), |_| {}) {
                        Ok(_) => panic!("uncertain retirement silently reseeded prefix validation"),
                        Err(failure) => failure,
                    };
                assert!(failure
                    .error()
                    .to_string()
                    .contains("explicit local validation"));
                terminal = failure.into_terminal();
                assert_eq!(terminal.slot_retirement_observation(), Retired::Retired);
                node.get_pg(id.pg_id().get())
                    .unwrap()
                    .recover_complete_command_state(
                        PgStoreRecoveryContext::for_node(NodeId::new(4)),
                        &topology(),
                    )
                    .unwrap();
                let window = terminal
                    .into_confirmed_window_embedded(section, deadline(), |_| {})
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(window.id(), id);
                drop(window);
                assert_files_retained(f, owner);
            });
        }
    }
}

#[test]
fn staging_acknowledged_append_terminal_expiry_preserves_completed_publication() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        with_writer_and_section(target, false, |f, owner, mut writer, section| {
            let mut publication = installed_publication!(owner, writer, section);
            publication.publish_embedded(section).unwrap();
            for member in [0, 4, 2, 3, 1] {
                publication
                    .apply_embedded(section, NodeId::new(member))
                    .unwrap();
            }
            let cutoff = publication.test_cutoff();
            let history = publication.test_binding_state();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let before = node
                .get_pg(owner.subject.object_pg.get())
                .unwrap()
                .conn
                .total_changes();
            let failure =
                crate::clock::with_time_override(cutoff.1, || match publication.into_terminal() {
                    Ok(_) => panic!("expired append reached terminal handoff"),
                    Err(failure) => failure,
                });
            let publication = failure.into_publication();
            assert_eq!(publication.publication(), Published::MarkerConfirmed);
            assert_eq!(publication.test_binding_state(), history);
            assert_eq!(publication.test_cutoff(), cutoff);
            for member in [0, 4, 2, 3, 1] {
                assert_eq!(
                    publication.application(NodeId::new(member)),
                    Applied::Applied
                );
            }
            assert_eq!(
                node.get_pg(owner.subject.object_pg.get())
                    .unwrap()
                    .conn
                    .total_changes(),
                before
            );
            drop(publication);
            assert_files_retained(f, owner);
        });
    }
}
