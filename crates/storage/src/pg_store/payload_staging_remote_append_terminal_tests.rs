// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming remote append custody through exact retirement and successor capture.
use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, TerminalSlotRetirementObservation as Retired,
};
use crate::node_runtime::traits::PgMetadataStore;

const RETIRE: Kind = Kind::MetadataCommandPendingSlotRemove;

macro_rules! with_remote_terminal {
    ($writer:path, $tls:expr, $target:expr, $appended:expr, |$f:ident, $primary:ident, $session:ident, $terminal:ident, $owner:ident, $shards:ident| $body:block) => {
        with_submission!(@with $writer, $tls, $target, $appended, "installed", |$f, $session, submission, $owner, $shards| {
            let $primary = $f.remote(NodeId::new(4)).unwrap();
            submission.install_remote(&$primary.client, &$session).unwrap();
            let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish_remote(&$primary.client, &$session).unwrap();
            let before = $f.observe();
            let failure = publication.into_remote_terminal().err().expect("all members must confirm first");
            let mut publication = failure.into_publication();
            assert_eq!($f.observe(), before);
            for n in [0, 4, 1, 2, 3] {
                if let Some(peer) = $f.remote(NodeId::new(n)) {
                    publication.apply_remote(&$primary.client, &$session, &peer.client).unwrap();
                } else {
                    publication.apply_embedded_replica(&$primary.client, &$session, NodeId::new(n)).unwrap();
                }
            }
            let id = publication.id();
            let cutoff = publication.test_cutoff().1;
            let before = $f.observe();
            let mut $terminal = publication.into_remote_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!($terminal.id(), id);
            assert_eq!($terminal.test_cutoff(), cutoff);
            assert_eq!($terminal.retirement(), Retired::NoAttemptObserved);
            for n in 0..5 { assert_eq!($terminal.application(NodeId::new(n)), Applied::Applied); }
            assert_eq!($f.observe(), before, "handoff has no effects");
            $body
        });
    };
}

macro_rules! succeeds {
    ($writer:path, $tls:expr, $target:expr, $appended:expr) => {
        with_remote_terminal!(
            $writer,
            $tls,
            $target,
            $appended,
            |f, primary, session, terminal, owner, shards| {
                let before = f.observe();
                let failure = terminal
                    .into_confirmed_window(&primary.client, &session)
                    .err()
                    .expect("retirement required");
                assert!(failure
                    .error()
                    .to_string()
                    .contains("confirmed slot retirement"));
                terminal = failure.into_terminal();
                assert_eq!(f.observe(), before);
                terminal.retire(&primary.client, &session).unwrap();
                assert_eq!(terminal.retirement(), Retired::Retired);
                let retired = f.observe();
                assert_ne!(retired[4], before[4]);
                assert_eq!(retired[..4], before[..4]);
                terminal.retire(&primary.client, &session).unwrap();
                assert_eq!(f.observe(), retired);
                let confirmed = terminal
                    .into_confirmed_window(&primary.client, &session)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(f.observe(), retired, "confirmation only reads");
                require_payload(f, owner, shards, true);
                let (advanced, request, expected) =
                    allocation_remote::inputs(f, owner.subject.target.clone());
                assert_eq!(advanced.windows.len(), owner.windows.len() + 1);
                let allocated = confirmed
                    .allocate_stream_remote(&primary.client, &session, &request, &expected)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let writer = allocated
                    .into_stream_file_writer_remote(&primary.client, &session)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let before_drop = f.observe();
                drop(writer);
                assert_eq!(f.observe(), before_drop);
                require_payload(f, owner, shards, true);
                let node = f.node(NodeId::new(4));
                assert_eq!(
                    node.get_pg(owner.subject.object_pg.get())
                        .unwrap()
                        .list_stream_segments(&owner.subject.id.attempt)
                        .unwrap()
                        .len(),
                    usize::from($appended) + 1
                );
            }
        );
    };
}

#[test]
fn staging_remote_append_terminal_retires_and_confirms_successor_without_reset() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                succeeds!(with_writer, tls, target.clone(), appended);
            }
            succeeds!(
                super::mixed_application::with_mixed_writer,
                tls,
                target,
                false
            );
        }
    }
}

#[test]
fn staging_remote_append_terminal_rejects_reconnect_before_and_after_retirement() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for retired in [false, true] {
                with_remote_terminal!(
                    with_writer,
                    tls,
                    target.clone(),
                    false,
                    |f, primary, session, terminal, owner, shards| {
                        if retired {
                            terminal.retire(&primary.client, &session).unwrap();
                        }
                        let observation = terminal.retirement();
                        let before = f.observe();
                        assert!(terminal
                            .retire(&f.remote(NodeId::new(0)).unwrap().client, &session)
                            .is_err());
                        crate::clock::with_time_override(terminal.test_cutoff(), || {
                            assert!(terminal.retire(&primary.client, &session).is_err());
                        });
                        session.test_close_primary_section();
                        let replacement = primary
                            .client
                            .open_metadata_command_critical_section_until(
                                f.context.id().pg_id(),
                                deadline(),
                            )
                            .unwrap();
                        let error = terminal.retire(&primary.client, &replacement).unwrap_err();
                        assert!(
                            error
                                .to_string()
                                .contains("original remote primary session"),
                            "{error}"
                        );
                        assert_eq!(terminal.retirement(), observation);
                        let failure = terminal
                            .into_confirmed_window(&primary.client, &replacement)
                            .err()
                            .expect("confirmation cannot replace the session");
                        assert!(failure
                            .error()
                            .to_string()
                            .contains("original remote primary session"));
                        let terminal = failure.into_terminal();
                        assert_eq!(terminal.retirement(), observation);
                        drop(terminal);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), before);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_terminal_ambiguous_retirement_keeps_history_and_payload() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for close in [false, true] {
                with_remote_terminal!(
                    with_writer,
                    tls,
                    target.clone(),
                    false,
                    |f, primary, session, terminal, owner, shards| {
                        let count = Arc::new(AtomicUsize::new(0));
                        let calls = Arc::clone(&count);
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind == RETIRE && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                                    frame.payload = rpc::encode_storage_rpc_error_response(
                                        &rpc::StorageRpcErrorResponse {
                                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                            message: "injected after append retirement".into(),
                                        },
                                    )
                                    .unwrap();
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        !close,
                                    )
                                    .unwrap();
                                }
                            },
                        ));
                        assert!(terminal.retire(&primary.client, &session).is_err());
                        assert_eq!(count.load(Ordering::SeqCst), 1, "no hidden retry");
                        assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                        let after = f.observe();
                        let failure = terminal
                            .into_confirmed_window(&primary.client, &session)
                            .err()
                            .expect("uncertainty blocks successor");
                        terminal = failure.into_terminal();
                        assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                        if close {
                            session.test_close_primary_section();
                            let replacement = primary
                                .client
                                .open_metadata_command_critical_section_until(
                                    f.context.id().pg_id(),
                                    deadline(),
                                )
                                .unwrap();
                            assert!(terminal
                                .retire(&primary.client, &replacement)
                                .unwrap_err()
                                .to_string()
                                .contains("original remote primary session"));
                            assert_eq!(terminal.retirement(), Retired::Unconfirmed);
                        } else {
                            terminal.retire(&primary.client, &session).unwrap();
                            assert_eq!(terminal.retirement(), Retired::Retired);
                            assert_eq!(count.load(Ordering::SeqCst), 2);
                            terminal.retire(&primary.client, &session).unwrap();
                            assert_eq!(count.load(Ordering::SeqCst), 2);
                        }
                        assert_eq!(f.observe(), after, "exact absence retry is read-only");
                        drop(terminal);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), after, "Drop never retries or cleans payload");
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_terminal_failed_confirmation_retains_session_into_successor() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_remote_terminal!(
                with_writer,
                tls,
                target,
                false,
                |f, primary, session, terminal, owner, shards| {
                    terminal.retire(&primary.client, &session).unwrap();
                    let before = f.observe();
                    let count = Arc::new(AtomicUsize::new(0));
                    let calls = Arc::clone(&count);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == Kind::MetadataCommandReplicaState
                                && calls.fetch_add(1, Ordering::SeqCst) == 0
                            {
                                frame.payload = rpc::encode_storage_rpc_success_response(&[]);
                                rpc::set_storage_rpc_response_connection_reusable(
                                    &mut frame.payload,
                                    true,
                                )
                                .unwrap();
                            }
                        }));
                    let failure = terminal
                        .into_confirmed_window(&primary.client, &session)
                        .err()
                        .expect("malformed observation rejected");
                    assert_eq!(
                        count.load(Ordering::SeqCst),
                        1,
                        "fault reached primary inspection"
                    );
                    terminal = failure.into_terminal();
                    assert_eq!(terminal.retirement(), Retired::Retired);
                    for n in 0..5 {
                        assert_eq!(terminal.application(NodeId::new(n)), Applied::Applied);
                    }
                    assert_eq!(f.observe(), before);
                    let confirmed = terminal
                        .into_confirmed_window(&primary.client, &session)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(f.observe(), before);
                    session.test_close_primary_section();
                    let replacement = primary
                        .client
                        .open_metadata_command_critical_section_until(
                            f.context.id().pg_id(),
                            deadline(),
                        )
                        .unwrap();
                    let (_, request, expected) =
                        allocation_remote::inputs(f, owner.subject.target.clone());
                    let failure = confirmed
                        .allocate_stream_remote(&primary.client, &replacement, &request, &expected)
                        .err()
                        .expect("successor retains original section identity");
                    assert!(failure
                        .error()
                        .to_string()
                        .contains("original remote primary session"));
                    drop(failure);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                }
            );
        }
    }
}
