// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Source-bearing cleanup over genuine mixed endpoints. Metadata-only fixtures:
//! no live selection, terminal transfer, resource release or physical deletion.

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::{
    AbandonedPartInstallationObservation as I, ApplicationObservation as A, PreparedAbandonedPart,
    PublicationObservation as P,
};
use crate::storage_rpc::{
    self as wire, complete_application as application, StorageRpcMessageKind as Kind,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandApplyAndRecord;

fn fixture_mixed(
    dir: &test_util::TempDir,
    count: usize,
    endpoints: [(u32, bool); 2],
) -> (Mixed, ContextualCommand) {
    let (map, handle, source, cleanup) = abandonment_preparation::setup(dir, count);
    (
        Mixed::from_fixture(map, handle, source, 2, endpoints),
        cleanup,
    )
}

fn with_owner(
    endpoints: [(u32, bool); 2],
    count: usize,
    origin: &str,
    work: Option<Duration>,
    action: impl FnOnce(
        &Mixed,
        &ContextualCommand,
        &mut PreparedAbandonedPart<'_>,
        &EmbeddedPublicationSection<'_>,
    ),
) {
    with_owned(
        endpoints,
        count,
        origin,
        work,
        |mixed, cleanup, mut owner, section| {
            action(mixed, cleanup, &mut owner, section);
        },
    );
}

pub(super) fn with_owned(
    endpoints: [(u32, bool); 2],
    count: usize,
    origin: &str,
    work: Option<Duration>,
    action: impl FnOnce(
        &Mixed,
        &ContextualCommand,
        PreparedAbandonedPart<'_>,
        &EmbeddedPublicationSection<'_>,
    ),
) {
    let dir = test_util::tempdir();
    let (mixed, cleanup) = fixture_mixed(&dir, count, endpoints);
    let id = mixed.context.id();
    let admission = mixed.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let cutoff = work.map_or_else(deadline, |work| Instant::now() + work);
    let mut abandoned = unstarted_abandonment::sender(&mixed.map, &mixed.context, origin, cutoff)
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
    abandoned
        .record_primary_embedded(&section, deadline())
        .unwrap();
    for node in [2, 0, 3, 1].map(NodeId::new) {
        if let Some(peer) = mixed.remote(node) {
            abandoned
                .record_remote_replica_from_embedded_primary(&section, &peer.client, deadline())
                .unwrap();
        } else {
            abandoned
                .record_replica_embedded(&section, node, deadline())
                .unwrap();
        }
    }
    // End the setup connections before stopping their listeners. Keep the exact
    // client identity registered in the admitted cohort, but do not leave a
    // cached TLS connection to a deliberately stopped test worker.
    for peer in &mixed.remote {
        peer.client
            .test_execution_endpoint()
            .close_idle_requests_for_test();
    }
    drop(running);
    let mut owner = abandoned
        .into_abandoned()
        .unwrap_or_else(|e| panic!("{}", e.error()))
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    owner.install_cleanup(&section).unwrap();
    action(&mixed, &cleanup, owner, &section);
}

fn apply_member(
    mixed: &Mixed,
    owner: &mut PreparedAbandonedPart<'_>,
    section: &EmbeddedPublicationSection<'_>,
    id: u32,
) {
    if let Some(peer) = mixed.remote(NodeId::new(id)) {
        owner.apply_cleanup_remote(section, &peer.client).unwrap();
    } else {
        owner
            .apply_cleanup_embedded(section, NodeId::new(id))
            .unwrap();
    }
}

#[test]
fn staging_abandoned_part_remote_unix_witness_converges_all_origins() {
    matrix([(0, false), (2, true)]);
}
#[test]
fn staging_abandoned_part_remote_tls_witness_converges_all_origins() {
    matrix([(0, true), (2, false)]);
}
#[test]
fn staging_abandoned_part_remote_embedded_witness_converges_all_origins() {
    matrix([(2, false), (3, true)]);
}

fn matrix(endpoints: [(u32, bool); 2]) {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            with_owner(
                endpoints,
                count,
                origin,
                None,
                |mixed, cleanup, owner, section| {
                    let history = owner.installation_observation();
                    let shadows = mixed.shadows();
                    let before = mixed.observe();
                    for peer in &mixed.remote {
                        assert!(owner
                            .apply_cleanup_remote(section, &peer.client)
                            .unwrap_err()
                            .to_string()
                            .contains("marker is not confirmed"));
                        assert_eq!(owner.cleanup_application(peer.node), A::NoAttemptObserved);
                    }
                    assert_eq!(mixed.observe(), before);
                    owner.publish_cleanup(section).unwrap();
                    for peer in &mixed.remote {
                        assert!(
                            section.application_target(peer.node).is_err(),
                            "remote endpoint must not fall back to the embedded shadow"
                        );
                        if peer.node != NodeId::new(0) {
                            assert!(owner
                                .apply_cleanup_remote(section, &peer.client)
                                .unwrap_err()
                                .to_string()
                                .contains("primary confirmation"));
                        }
                    }
                    let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                    let mut expected = None;
                    for id in [0, 4, 2, 3, 1] {
                        let node = mixed.node(NodeId::new(id));
                        let before = snapshot(&node.get_pg(cleanup.id().pg_id().get()).unwrap());
                        apply_member(mixed, owner, section, id);
                        let pg = node.get_pg(cleanup.id().pg_id().get()).unwrap();
                        assert_ne!(snapshot(&pg), before);
                        let state = (
                            introduction::state(&pg),
                            complete_maintenance::log_state(&pg),
                            pg.metadata_command_replica_state().unwrap(),
                        );
                        assert_eq!(state.2.applied_log_index, cleanup.id().log_index().get());
                        if let Some(expected) = &expected {
                            assert_eq!(&state, expected);
                        } else {
                            expected = Some(state);
                        }
                        let after = (snapshot(&pg), pg.conn.total_changes());
                        drop(pg);
                        apply_member(mixed, owner, section, id);
                        let pg = node.get_pg(cleanup.id().pg_id().get()).unwrap();
                        assert_eq!(
                            (snapshot(&pg), pg.conn.total_changes()),
                            after,
                            "replay repeated cleanup transfer"
                        );
                        assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
                        assert_eq!(owner.installation_observation(), history);
                        owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
                    }
                    assert_eq!(owner.cleanup_installation(), I::Installed);
                    assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
                    assert_eq!(mixed.shadows(), shadows);
                    assert!(started(
                        &mixed
                            .node(NodeId::new(4))
                            .get_pg(cleanup.id().pg_id().get())
                            .unwrap()
                    ));
                },
            );
        }
    }
}

struct Wake<'a>(Option<&'a Remote>);
impl Drop for Wake<'_> {
    fn drop(&mut self) {
        if let Some(peer) = self.0 {
            if peer.tls {
                let _ = std::net::TcpStream::connect(peer.server.tcp_listener_addr_for_test());
            } else {
                let _ = std::os::unix::net::UnixStream::connect(peer.server.socket_path_for_test());
            }
        }
    }
}

#[test]
fn staging_abandoned_part_remote_lost_reply_blocks_order_until_exact_confirmation() {
    for tls in [false, true] {
        with_owner(
            [(0, tls), (2, !tls)],
            2,
            "uncertain",
            None,
            |mixed, cleanup, owner, section| {
                owner.publish_cleanup(section).unwrap();
                let history = owner.installation_observation();
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                let before = mixed.observe();
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        peer.server
                            .test_accept_one_complete_application_fault(false, true)
                            .unwrap()
                    });
                    let mut wake = Wake(Some(peer));
                    let error = owner
                        .apply_cleanup_remote(section, &peer.client)
                        .unwrap_err();
                    assert!(error.to_string().contains("unconfirmed"), "{error}");
                    wake.0 = None;
                    worker.join().unwrap();
                });
                assert_eq!(owner.cleanup_application(peer.node), A::Unconfirmed);
                let after = mixed.observe();
                assert_ne!(after[0], before[0]);
                for id in 1..5 {
                    assert_eq!(after[id], before[id]);
                }
                assert!(owner
                    .apply_cleanup_embedded(section, NodeId::new(4))
                    .unwrap_err()
                    .to_string()
                    .contains("witness confirmation"));
                assert!(owner
                    .apply_cleanup_remote(section, &mixed.remote(NodeId::new(2)).unwrap().client)
                    .unwrap_err()
                    .to_string()
                    .contains("primary confirmation"));
                assert_eq!(mixed.observe(), after);
                let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                owner.apply_cleanup_remote(section, &peer.client).unwrap();
                assert_eq!(owner.cleanup_application(peer.node), A::Applied);
                assert_eq!(
                    mixed.observe(),
                    after,
                    "lost-reply replay must not write again"
                );
                for id in [4, 2, 1, 3] {
                    apply_member(mixed, owner, section, id);
                }
                assert_eq!(owner.installation_observation(), history);
                owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
            },
        );
    }
}

#[test]
fn staging_abandoned_part_remote_authenticated_crossed_replies_never_confirm_or_retry_implicitly() {
    for tls in [false, true] {
        with_owner(
            [(0, tls), (2, !tls)],
            2,
            "uncertain",
            None,
            |mixed, cleanup, owner, section| {
                owner.publish_cleanup(section).unwrap();
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let seen = Arc::clone(&calls);
                peer.server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind != KIND {
                            return;
                        }
                        let attempt = seen.fetch_add(1, Ordering::SeqCst);
                        let response = wire::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap();
                        let mut response = application::decode_confirmation(&response).unwrap();
                        match attempt {
                            0 => response.node_id = NodeId::new(2),
                            1 => {
                                response.id = MetadataCommandId::new(
                                    response.id.cluster_epoch(),
                                    PgId::new(response.id.pg_id().get() + 1),
                                    response.id.log_index(),
                                )
                            }
                            2 => response.command_sha256[0] ^= 1,
                            3 => {
                                response.id = MetadataCommandId::new(
                                    response.id.cluster_epoch(),
                                    response.id.pg_id(),
                                    MetadataCommandLogIndex::new(1).unwrap(),
                                )
                            }
                            5 => {
                                frame.payload = wire::encode_storage_rpc_error_response(
                                    &wire::StorageRpcErrorResponse {
                                        code: wire::StorageRpcErrorCode::ResourceExhausted,
                                        message: "injected after confirmed cleanup".into(),
                                    },
                                )
                                .unwrap();
                                return;
                            }
                            _ => {}
                        }
                        frame.payload = wire::encode_storage_rpc_success_response(
                            &application::encode_confirmation(response),
                        );
                    }));
                let _running = peer.run();
                let mut after = None;
                for attempt in 0..6 {
                    let result = owner.apply_cleanup_remote(section, &peer.client);
                    if attempt == 4 {
                        result.unwrap();
                    } else {
                        let error = result.unwrap_err();
                        assert!(error.to_string().contains("unconfirmed"), "{error}");
                    }
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        attempt + 1,
                        "each call sends exactly one request"
                    );
                    assert_eq!(
                        owner.cleanup_application(peer.node),
                        if attempt < 4 {
                            A::Unconfirmed
                        } else {
                            A::Applied
                        }
                    );
                    if let Some(after) = &after {
                        assert_eq!(&mixed.observe(), after);
                    } else {
                        after = Some(mixed.observe());
                    }
                    owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
                }
            },
        );
    }
}

#[test]
fn staging_abandoned_part_remote_bad_authentication_retains_custody_and_replays_exactly() {
    for tls in [false, true] {
        with_owner(
            [(0, tls), (2, !tls)],
            2,
            "uncertain",
            None,
            |mixed, cleanup, owner, section| {
                owner.publish_cleanup(section).unwrap();
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                peer.server
                    .set_response_envelope_test_hook(Arc::new(|kind, bytes| {
                        if kind == KIND {
                            *bytes.last_mut().unwrap() ^= 1;
                        }
                    }));
                let _running = peer.run();
                owner
                    .apply_cleanup_remote(section, &peer.client)
                    .unwrap_err();
                assert_eq!(owner.cleanup_application(peer.node), A::Unconfirmed);
                let after = mixed.observe();
                peer.server
                    .set_response_envelope_test_hook(Arc::new(|_, _| {}));
                owner.apply_cleanup_remote(section, &peer.client).unwrap();
                assert_eq!(owner.cleanup_application(peer.node), A::Applied);
                assert_eq!(mixed.observe(), after);
                owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
            },
        );
    }
}

#[test]
fn staging_abandoned_part_remote_attempts_preserve_frozen_request_and_all_history_states() {
    with_owner(
        [(0, false), (2, true)],
        2,
        "uncertain",
        None,
        |mixed, cleanup, owner, section| {
            owner.publish_cleanup(section).unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let before = mixed.observe();
            let failure = || StoreError::OperationDeadlineExceeded {
                context: "injected cleanup send",
            };
            let mut frozen = None;
            let clock = crate::clock::test_time_override_guard(1_000);
            for (now, unsent) in [(1_000, true), (500, false), (200_000, true)] {
                clock.set(now);
                owner
                    .test_cleanup_remote_attempt(section, &peer.client, |attempt| {
                        let bytes = attempt.encode_request().unwrap();
                        let request = application::decode_request(&bytes).unwrap();
                        assert_eq!(
                            request.command.command,
                            cleanup.encode_command().unwrap().bytes()
                        );
                        assert_eq!(request.command.node_id, peer.node);
                        if let Some((bytes_before, deadline_before)) = &frozen {
                            assert_eq!(&bytes, bytes_before);
                            assert_eq!(attempt.deadline(), *deadline_before);
                        } else {
                            frozen = Some((bytes, attempt.deadline()));
                        }
                        if unsent {
                            Err(attempt.not_sent(failure()))
                        } else {
                            Err(attempt.unconfirmed(failure()))
                        }
                    })
                    .unwrap_err();
                assert_eq!(
                    owner.cleanup_application(peer.node),
                    if now == 1_000 {
                        A::NoAttemptObserved
                    } else {
                        A::Unconfirmed
                    }
                );
                assert_eq!(mixed.observe(), before);
            }
            drop(clock);
            for confirmed in [false, true] {
                let _running = peer.run();
                if confirmed {
                    owner.apply_cleanup_remote(section, &peer.client).unwrap();
                }
                let state = mixed.observe();
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    owner.test_cleanup_remote_attempt(section, &peer.client, |_| {
                        panic!("injected borrowed cleanup attempt unwind")
                    })
                }));
                assert!(panic.is_err());
                assert_eq!(
                    owner.cleanup_application(peer.node),
                    if confirmed {
                        A::Applied
                    } else {
                        A::Unconfirmed
                    }
                );
                assert_eq!(mixed.observe(), state);
                owner
                    .test_cleanup_remote_attempt(section, &peer.client, |attempt| {
                        Err(attempt.not_sent(failure()))
                    })
                    .unwrap_err();
                assert_eq!(
                    owner.cleanup_application(peer.node),
                    if confirmed {
                        A::Applied
                    } else {
                        A::Unconfirmed
                    }
                );
                owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
            }
        },
    );
}

#[test]
fn staging_abandoned_part_remote_foreign_endpoint_and_section_fail_before_transport() {
    let foreign_dir = test_util::tempdir();
    let (foreign, _) = fixture_mixed(&foreign_dir, 2, [(0, false), (2, true)]);
    let admission = foreign.handle.admit_current_route().unwrap();
    let id = foreign.context.id();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let foreign_section = route.lock_until(deadline()).unwrap();
    with_owner(
        [(0, false), (2, true)],
        2,
        "uncertain",
        None,
        |mixed, cleanup, owner, section| {
            owner.publish_cleanup(section).unwrap();
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let before = (mixed.observe(), foreign.observe());
            // Neither side has active servers here. Scope rejection must not wait on IO.
            for result in [
                owner.apply_cleanup_remote(section, &foreign.remote(peer.node).unwrap().client),
                owner.apply_cleanup_remote(&foreign_section, &peer.client),
            ] {
                assert!(result.unwrap_err().to_string().contains("subject"));
            }
            assert_eq!(owner.cleanup_application(peer.node), A::NoAttemptObserved);
            assert_eq!((mixed.observe(), foreign.observe()), before);
            owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
        },
    );
}

#[test]
fn staging_abandoned_part_remote_receiver_expiry_rejects_without_transfer() {
    for tls in [false, true] {
        with_owner(
            [(0, tls), (2, !tls)],
            2,
            "uncertain",
            None,
            |mixed, cleanup, owner, section| {
                owner.publish_cleanup(section).unwrap();
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                let before = mixed.observe();
                let mut frozen = None;
                owner
                    .test_cleanup_remote_attempt(section, &peer.client, |attempt| {
                        frozen = Some(attempt.encode_request().unwrap());
                        Err(attempt.not_sent(StoreError::OperationDeadlineExceeded {
                            context: "inspect frozen request before transport",
                        }))
                    })
                    .unwrap_err();
                let frozen = frozen.unwrap();
                let cutoff = application::decode_request(&frozen)
                    .unwrap()
                    .deadline
                    .portable_wall_valid_until_ms;
                let replies = Arc::new(AtomicUsize::new(0));
                let seen = Arc::clone(&replies);
                peer.server
                    .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                        if kind == KIND {
                            let error = wire::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap_err();
                            assert_eq!(error.code, wire::StorageRpcErrorCode::TransportTimeout);
                            seen.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                // Advance both wall clocks, not the captured sender Instant.
                // The first receiver binds 500 ms, shorter than its independent
                // one-second work budget, then expires after successful decode.
                // The second sees the SAME frozen wire cutoff already expired.
                for (now, expire_after_decode) in [(cutoff - 500, true), (cutoff + 1, false)] {
                    let _clock = crate::clock::test_time_override_guard(now);
                    owner
                        .test_cleanup_remote_attempt(section, &peer.client, |attempt| {
                            assert_eq!(attempt.encode_request().unwrap(), frozen);
                            Err(attempt.not_sent(StoreError::OperationDeadlineExceeded {
                                context: "inspect unchanged retry cutoff",
                            }))
                        })
                        .unwrap_err();
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| {
                            let _clock = crate::clock::test_time_override_guard(now);
                            peer.server
                                .test_accept_one_complete_application_fault(
                                    expire_after_decode,
                                    false,
                                )
                                .unwrap()
                        });
                        let mut wake = Wake(Some(peer));
                        owner
                            .apply_cleanup_remote(section, &peer.client)
                            .unwrap_err();
                        wake.0 = None;
                        peer.client
                            .test_execution_endpoint()
                            .close_idle_requests_for_test();
                        worker.join().unwrap();
                    });
                    assert_eq!(owner.cleanup_application(peer.node), A::Unconfirmed);
                    assert_eq!(mixed.observe(), before);
                }
                assert_eq!(replies.load(Ordering::SeqCst), 2);
                assert_eq!(owner.cleanup_application(peer.node), A::Unconfirmed);
                assert_eq!(mixed.observe(), before);
                owner.assert_contexts_for_test(&mixed.context, cleanup, &topology());
            },
        );
    }
}
