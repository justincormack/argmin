// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real embedded issuer, complete all-member publication and consuming local
//! allocation. Metadata-only fixtures; no file or acknowledgement effects.
use super::*;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::ExpectedStreamAppendState;
use crate::{PrepareStreamUploadSegmentAppendReq, StoreError};

pub(super) fn setup_live(target: StagingTarget, appended: bool) -> Setup {
    setup_live_with_floor(target, appended, None)
}

pub(super) fn setup_live_with_floor(
    target: StagingTarget,
    appended: bool,
    floor: Option<u64>,
) -> Setup {
    let dir = test_util::tempdir();
    let (map, handle) = embedded::embedded(&dir, 1, PgState::Active, RouteMapValidity::Forever);
    let mut template = fresh_capture::template(target);
    let pg_id = template.subject.object_pg;
    for member in 0..5 {
        let node = map.node(NodeId::new(member)).unwrap().test_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_empty_state(member, config(1).cluster_epoch, digest)
            .unwrap();
        if let Some(floor) = floor {
            pg.conn.execute("INSERT INTO object_write_counters (bucket,key,next_write_sequence,max_committed_generation) VALUES (?1,?2,1,?3)", rusqlite::params![template.subject.bucket, template.subject.key, floor]).unwrap();
        }
        if member != 4 {
            introduction::seed_upload(&pg, &mut template);
        }
    }
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let context = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let command = fresh_capture::candidate(&pg, &mut template);
        ContextualCommand::from_described(candidate::capture(&pg, &template, command, false))
    };
    for member in 0..5 {
        let node = map.node(NodeId::new(member)).unwrap().test_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        complete_application::anchor(&pg, context.id());
        if appended {
            apply(&pg, &context);
        }
        pg.recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(member)),
            &topology(),
        )
        .unwrap();
    }
    let context = if appended {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let owner = load(&pg, &template);
        fresh_append_capture::prepare(&pg, &owner);
        let next = ContextualCommand::from_described(candidate::capture(
            &pg,
            &owner,
            append::command(&owner),
            true,
        ));
        // Raw predecessor-allocation fixture setup is validated explicitly, not
        // repaired by the operation under test.
        pg.recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(4)),
            &topology(),
        )
        .unwrap();
        next
    } else {
        context
    };
    Setup {
        _dir: dir,
        map,
        handle,
        context,
    }
}

fn with_confirmed(
    f: &Setup,
    history: &str,
    action: impl FnOnce(
        &crate::cluster::EmbeddedPublicationSection<'_>,
        crate::node_runtime::pg_store::ConfirmedStagingWindow<'_>,
    ),
) {
    with_confirmed_count(f, history, 1, |section, mut windows| {
        action(section, windows.pop().unwrap())
    });
}

pub(super) fn with_confirmed_count(
    f: &Setup,
    history: &str,
    count: usize,
    action: impl FnOnce(
        &crate::cluster::EmbeddedPublicationSection<'_>,
        Vec<crate::node_runtime::pg_store::ConfirmedStagingWindow<'_>>,
    ),
) {
    with_confirmed_duration(f, history, count, Duration::from_secs(10), action);
}

pub(super) fn with_confirmed_duration(
    f: &Setup,
    history: &str,
    count: usize,
    duration: Duration,
    action: impl FnOnce(
        &crate::cluster::EmbeddedPublicationSection<'_>,
        Vec<crate::node_runtime::pg_store::ConfirmedStagingWindow<'_>>,
    ),
) {
    with_confirmed_owned(f, history, count, duration, |section, windows| {
        action(&section, windows)
    });
}

pub(super) fn with_confirmed_owned(
    f: &Setup,
    history: &str,
    count: usize,
    duration: Duration,
    action: impl FnOnce(
        crate::cluster::EmbeddedPublicationSection<'_>,
        Vec<crate::node_runtime::pg_store::ConfirmedStagingWindow<'_>>,
    ),
) {
    let id = f.context.id();
    if matches!(
        f.context.terminal_slot_work(),
        TerminalSlotWork::UploadPartCreation
    ) {
        let resource = {
            // Subject-only fixture lookup; this does not manufacture release authority.
            let template = fresh_capture::template(multipart_target());
            proof(&template.subject, template.subject.creation_operation())
        };
        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
        terminal_release::seed_pg_reservations(
            &node
                .get_pg(topology().bucket_pg_for(&resource.bucket))
                .unwrap(),
            &resource,
            None,
        );
    }
    if history == "inspected" {
        install(
            &f.map
                .node(NodeId::new(4))
                .unwrap()
                .test_node()
                .get_pg(id.pg_id().get())
                .unwrap(),
            &f.context,
        );
    }
    let admission = f.handle.admit_current_route().unwrap();
    let sender = fanout_embedded::sender(&f.map, &f.context, history, deadline());
    if history == "uncertain" {
        f.map
            .node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap()
            .recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                &topology(),
            )
            .unwrap();
    }
    let mut fanout = sender
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    fanout.publish_embedded(&section, deadline()).unwrap();
    for member in [0, 4, 2, 3, 1] {
        fanout
            .apply_embedded(&section, NodeId::new(member), deadline())
            .unwrap();
    }
    let terminal = fanout
        .into_terminal()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let mut terminals = vec![terminal];
    for _ in 1..count {
        // Two independently inspected owners can finish the SAME existing slot.
        // Neither construction restores or re-installs an already retired slot.
        let mut second = fanout_embedded::sender(&f.map, &f.context, "inspected", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        second.publish_embedded(&section, deadline()).unwrap();
        for member in [0, 4, 2, 3, 1] {
            second
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        terminals.push(
            second
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error())),
        );
    }
    let mut windows = Vec::new();
    for mut terminal in terminals {
        terminal.release_reservation().unwrap();
        terminal.retire_embedded(&section, deadline()).unwrap();
        windows.push(
            terminal
                .into_confirmed_window_embedded(&section, Instant::now() + duration, |_| {})
                .unwrap_or_else(|e| panic!("{}", e.error())),
        );
    }
    action(section, windows);
}

pub(super) fn inputs(
    f: &Setup,
    target: StagingTarget,
) -> (
    StagingOwnerRecord,
    PrepareStreamUploadSegmentAppendReq,
    ExpectedStreamAppendState,
) {
    let template = fresh_capture::template(target);
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(&pg, &template);
    let expected = preparation::expected(&pg, &owner);
    let request = PrepareStreamUploadSegmentAppendReq {
        session_id: owner.subject.id.attempt.clone(),
        segment_index: owner.windows.last().unwrap().index,
        size: 4,
        segment_crc64: 0,
        payload_crc64: 0,
    };
    snapshot.finish().unwrap();
    (owner, request, expected)
}

fn load(pg: &PgStore, template: &StagingOwnerRecord) -> StagingOwnerRecord {
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(pg, template);
    snapshot.finish().unwrap();
    owner
}

fn cursor(f: &Setup, owner: &StagingOwnerRecord) -> GenerationId {
    f.map
        .node(NodeId::new(4))
        .unwrap()
        .test_node()
        .get_pg(f.context.id().pg_id().get())
        .unwrap()
        .get_stream_upload(&owner.subject.id.attempt)
        .unwrap()
        .next_segment_vid
}

#[test]
fn staging_stream_window_allocation_consumes_exact_slot_preserves_prefix() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["inspected", "installed", "uncertain"] {
                let f = setup_live(target.clone(), appended);
                with_confirmed(&f, history, |section, confirmed| {
                    let (owner, request, expected) = inputs(&f, target.clone());
                    let observe = |pg: &PgStore| {
                        (
                            pg.metadata_state_digest().unwrap(),
                            pg.metadata_command_replica_state().unwrap(),
                            complete_pending::observe(pg),
                            complete_maintenance::log_state(pg),
                        )
                    };
                    let before = observe(
                        &f.map
                            .node(NodeId::new(4))
                            .unwrap()
                            .test_node()
                            .get_pg(f.context.id().pg_id().get())
                            .unwrap(),
                    );
                    let allocated = confirmed
                        .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(allocated.id(), f.context.id());
                    assert_eq!(
                        allocated.segment().segment_vid,
                        owner.windows.last().unwrap().start
                    );
                    assert_eq!(cursor(&f, &owner), owner.windows.last().unwrap().end);
                    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert_eq!(load(&pg, &owner), owner);
                    assert_eq!(
                        observe(&pg),
                        before,
                        "local cursor is not canonical replicated state"
                    );
                    pg.inspect_complete_settled_prefix(
                        4,
                        f.context.id().cluster_epoch(),
                        &topology(),
                    )
                    .unwrap()
                    .finish()
                    .unwrap();
                    drop(pg);
                    let after = cursor(&f, &owner);
                    drop(allocated);
                    assert_eq!(cursor(&f, &owner), after);
                });
            }
        }
    }
}

#[test]
fn staging_stream_window_allocation_rechecks_cutoff_at_update_and_commit() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for stage in ["update", "before-commit"] {
            let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
            let f = setup_live(target.clone(), false);
            with_confirmed(&f, "installed", |section, confirmed| {
                let (owner, request, expected) = inputs(&f, target.clone());
                let now = crate::clock::current_time_millis();
                let control = clock.control();
                let result = super::super::super::super::preparation::with_before_effect(
                    move || {
                        if stage == "update" {
                            control.set(now + 15_000);
                        }
                    },
                    || {
                        confirmed.allocate_stream_embedded(section, &request, &expected, |point| {
                            if point == stage {
                                clock.set(now + 15_000);
                            }
                            Ok(())
                        })
                    },
                );
                let failure = result.err().expect("expired allocation must fail");
                assert!(
                    failure.error().to_string().contains("deadline")
                        || failure.error().to_string().contains("expired"),
                    "{}",
                    failure.error()
                );
                assert_eq!(cursor(&f, &owner), owner.windows[0].start);
            });
        }
    }
}

#[test]
fn staging_stream_window_allocation_rejects_second_independently_confirmed_holder() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            let f = setup_live(target.clone(), appended);
            with_confirmed_count(&f, "installed", 2, |section, mut windows| {
                let (owner, request, expected) = inputs(&f, target.clone());
                let first = windows
                    .pop()
                    .unwrap()
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let (_, request, expected) = inputs(&f, target.clone());
                let before = cursor(&f, &owner);
                let failure = windows
                    .pop()
                    .unwrap()
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .err()
                    .expect("second holder must not reuse the consumed slot");
                assert!(
                    failure.error().to_string().contains("unconsumed window"),
                    "{}",
                    failure.error()
                );
                assert_eq!(failure.id(), first.id());
                drop(failure);
                drop(first);
                assert_eq!(cursor(&f, &owner), before);
            });
        }
    }
}

#[test]
fn staging_stream_window_allocation_rechecks_pending_and_changed_prefix_after_confirmation() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for pending in [false, true] {
            let f = setup_live(target.clone(), false);
            with_confirmed(&f, "installed", |section, confirmed| {
                let (owner, request, expected) = inputs(&f, target.clone());
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let next = complete_node_startup::next(&pg, f.context.id().cluster_epoch());
                if pending {
                    install(&pg, &next);
                } else {
                    apply(&pg, &next);
                }
                let before = snapshot(&pg);
                let writes = pg.conn.total_changes();
                drop(pg);
                let failure = confirmed
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .err()
                    .expect("stale confirmation cannot allocate");
                let message = failure.error().to_string();
                assert!(
                    message.contains(if pending {
                        "pending command"
                    } else {
                        "prefixes differ"
                    }),
                    "{message}"
                );
                assert_eq!(cursor(&f, &owner), owner.windows[0].start);
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                assert_eq!(snapshot(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            });
        }
    }
}

#[test]
fn staging_stream_window_allocation_commit_failure_and_unwind_preserve_owner_without_grant() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for unwind in [false, true] {
            let f = setup_live(target.clone(), false);
            with_confirmed(&f, "installed", |section, confirmed| {
                let (owner, request, expected) = inputs(&f, target.clone());
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let before = snapshot(&pg);
                if !unwind {
                    pg.fail_next_metadata_txn_commit
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                drop(pg);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    confirmed.allocate_stream_embedded(section, &request, &expected, |point| {
                        assert!(
                            !(unwind && point == "before-commit"),
                            "injected allocation unwind"
                        );
                        Ok(())
                    })
                }));
                if unwind {
                    assert!(result.is_err());
                } else {
                    let failure = result
                        .unwrap_or_else(|_| panic!("unexpected unwind"))
                        .err()
                        .expect("COMMIT failure cannot grant");
                    assert!(failure
                        .error()
                        .to_string()
                        .contains("injected metadata transaction commit failure"));
                    assert_eq!(failure.id(), f.context.id());
                    drop(failure);
                }
                assert_eq!(cursor(&f, &owner), owner.windows[0].start);
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                assert!(pg.conn.is_autocommit());
                assert_eq!(snapshot(&pg), before);
            });
        }
    }
}

#[test]
fn staging_stream_window_allocation_lost_commit_reply_keeps_custody_and_consumed_cursor() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let f = setup_live(target.clone(), false);
        with_confirmed(&f, "uncertain", |section, confirmed| {
            let (owner, request, expected) = inputs(&f, target.clone());
            let failure = confirmed
                .allocate_stream_embedded(section, &request, &expected, |point| {
                    if point == "after-commit" {
                        Err(StoreError::OperationDeadlineExceeded {
                            context: "lost allocation reply",
                        })
                    } else {
                        Ok(())
                    }
                })
                .err()
                .expect("lost reply cannot return allocation");
            assert!(failure
                .error()
                .to_string()
                .contains("lost allocation reply"));
            assert_eq!(failure.id(), f.context.id());
            assert_eq!(cursor(&f, &owner), owner.windows[0].end);
            drop(failure);
            assert_eq!(cursor(&f, &owner), owner.windows[0].end);
        });
    }
}

#[test]
fn staging_stream_window_allocation_rejects_changed_issuer_and_captured_session() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for change in ["issuer", "expected", "request", "cold"] {
            let _clock =
                crate::clock::test_time_override_guard(crate::clock::current_time_millis());
            let f = setup_live(target.clone(), false);
            with_confirmed(&f, "installed", |section, confirmed| {
                let (owner, mut request, mut expected) = inputs(&f, target.clone());
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                if change == "issuer" {
                    node.get_pg(f.context.id().pg_id().get())
                        .unwrap()
                        .begin_allocation_lifetime_replacement()
                        .unwrap()
                        .complete()
                        .unwrap();
                    // Revalidate unchanged metadata under the NEW local lifetime.
                    // Rejection must compare the retained issuer, not merely
                    // observe an invalidated prefix-cache stamp.
                    node.get_pg(f.context.id().pg_id().get())
                        .unwrap()
                        .recover_complete_command_state(
                            PgStoreRecoveryContext::for_node(NodeId::new(4)),
                            &topology(),
                        )
                        .unwrap();
                }
                if change == "cold" {
                    node.get_pg(f.context.id().pg_id().get())
                        .unwrap()
                        .complete_prefix_validation
                        .invalidate();
                }
                if change == "request" {
                    request.segment_index += 1;
                }
                if change == "expected" {
                    match &mut expected {
                        ExpectedStreamAppendState::PutObject { session, .. }
                        | ExpectedStreamAppendState::UploadPart { session, .. } => {
                            session.created_at += 1
                        }
                    }
                }
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let writes = pg.conn.total_changes();
                drop(pg);
                let failure = confirmed
                    .allocate_stream_embedded(section, &request, &expected, |_| Ok(()))
                    .err()
                    .expect("crossed authority must fail");
                assert_eq!(failure.id(), f.context.id());
                let diagnostic = match change {
                    "issuer" => "allocation lifetime",
                    "expected" => "captured session authority",
                    "request" => "request subject",
                    "cold" => "explicit local validation",
                    _ => unreachable!(),
                };
                assert!(
                    failure.error().to_string().contains(diagnostic),
                    "{change}: {}",
                    failure.error()
                );
                assert_eq!(cursor(&f, &owner), owner.windows[0].start);
                assert_eq!(
                    node.get_pg(f.context.id().pg_id().get())
                        .unwrap()
                        .conn
                        .total_changes(),
                    writes
                );
            });
        }
    }
}
