// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual owning publication -> retirement -> consuming window confirmation.
//! No allocation or physical effects are selected by these metadata fixtures.

use super::super::fanout_mixed::Mixed;
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};
use crate::node_runtime::pg_store::{
    CompleteTerminalHandoff, ConfirmedStagingWindow, StagingWindowHandoffFailure,
    TerminalSlotRetirementObservation as Retirement,
};

#[derive(Clone, Copy)]
enum Primary<'a> {
    Embedded(&'a EmbeddedPublicationSection<'a>),
    Remote(
        &'a UnixStorageNodeClient,
        &'a UnixStorageNodeMetadataCommandSession,
    ),
}

impl Primary<'_> {
    fn retire(self, terminal: &mut CompleteTerminalHandoff<'_>) {
        match self {
            Self::Embedded(section) => terminal.retire_embedded(section, deadline()),
            Self::Remote(client, session) => terminal.retire_remote(client, session, deadline()),
        }
        .unwrap();
    }

    fn confirm<'a>(
        self,
        terminal: CompleteTerminalHandoff<'a>,
        cutoff: Instant,
        hook: impl FnMut(&'static str),
    ) -> Result<ConfirmedStagingWindow<'a>, StagingWindowHandoffFailure<'a>> {
        match self {
            Self::Embedded(section) => {
                terminal.into_confirmed_window_embedded(section, cutoff, hook)
            }
            Self::Remote(client, session) => {
                terminal.into_confirmed_window_remote(client, session, cutoff, hook)
            }
        }
    }
}

fn with_terminal(
    transport: &str,
    remote_witness: bool,
    history: &str,
    target: StagingTarget,
    append: bool,
    action: impl FnOnce(&Mixed, Primary<'_>, CompleteTerminalHandoff<'_>),
) {
    let (
        Setup {
            _dir,
            map,
            handle,
            context,
        },
        proof,
    ) = unsettled_setup(target, append);
    let endpoints = match transport {
        "embedded" => [(if remote_witness { 0 } else { 2 }, false), (3, true)],
        "unix" | "tls" => [
            (4, transport == "tls"),
            (if remote_witness { 0 } else { 2 }, transport != "tls"),
        ],
        _ => unreachable!(),
    };
    let f = Mixed::from_settled_fixture(map, handle, context, endpoints);
    run_terminal(&f, proof.as_ref(), history, action);
}

fn run_terminal(
    f: &Mixed,
    proof: Option<&BucketWriteReservationProof>,
    history: &str,
    action: impl FnOnce(&Mixed, Primary<'_>, CompleteTerminalHandoff<'_>),
) {
    let id = f.context.id();
    if let Some(proof) = &proof {
        let node = f.node(NodeId::new(4));
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        terminal_release::seed_pg_reservations(&pg, proof, None);
    }
    if history == "inspected" {
        let node = f.node(NodeId::new(4));
        install(&node.get_pg(id.pg_id().get()).unwrap(), &f.context);
    }
    let admission = f.handle.admit_current_route().unwrap();
    let remote = f.remote(NodeId::new(4));
    let sender = if remote.is_some() {
        fanout_remote_primary::owner(f, history, deadline())
    } else {
        fanout_embedded::sender(&f.map, &f.context, history, deadline())
    };
    if history == "uncertain" {
        // The shared fixture injects a failed SQLite commit with raw DDL/SQL.
        // Explicit local recovery validates that setup before publication;
        // confirmation must never repair an invalidated prefix implicitly.
        f.node(NodeId::new(4))
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
    // Bucket release uses a separate connection while the object-PG primary
    // session remains held. A serial accept_one loop cannot serve that flow.
    let _servers: Vec<_> = f
        .remote
        .iter()
        .map(|remote| {
            application_sender::RunningReplica::start_concurrent(
                Arc::clone(&remote.server),
                remote.tls,
            )
        })
        .collect();
    let route = remote.is_none().then(|| {
        admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap()
    });
    let section = route
        .as_ref()
        .map(|route| route.lock_until(deadline()).unwrap());
    let session = remote.map(|remote| {
        fanout
            .open_primary_session(&remote.client, deadline())
            .unwrap_or_else(|error| match &error {
                crate::StoreError::StorageRpc { detail, .. } => {
                    panic!("open primary session: {error:?}; {}", detail.as_str())
                }
                _ => panic!("open primary session: {error:?}"),
            })
    });
    let primary = if let Some(section) = &section {
        fanout.publish_embedded(section, deadline()).unwrap();
        for member in [0, 4, 2, 3, 1] {
            f.apply(&mut fanout, section, member).unwrap();
        }
        Primary::Embedded(section)
    } else {
        let client = &remote.unwrap().client;
        let session = session.as_ref().unwrap();
        fanout.publish(client, session, deadline()).unwrap();
        for member in [0, 4, 2, 3, 1] {
            fanout_remote_primary::apply_member(f, &mut fanout, session, member).unwrap();
        }
        Primary::Remote(client, session)
    };
    let mut terminal = fanout
        .into_terminal()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    if proof.is_some() {
        terminal.release_reservation().unwrap();
    }
    action(f, primary, terminal);
}

#[path = "payload_staging_stream_window_remote_tests.rs"]
pub(super) mod allocation_remote;

#[path = "payload_staging_direct_file_primary_tests.rs"]
mod direct_file_primary;
#[path = "payload_staging_remote_direct_snapshot_tests.rs"]
mod direct_snapshot_receiver;

#[path = "payload_staging_stream_file_primary_tests.rs"]
mod stream_file_primary;
#[path = "payload_staging_stream_file_receiver_tests.rs"]
mod stream_file_receiver;

#[path = "payload_staging_ack_receiver_tests.rs"]
mod ack_receiver;
#[path = "payload_staging_ack_sender_tests.rs"]
mod ack_sender;

#[path = "payload_staging_stream_file_binding_remote_tests.rs"]
mod stream_file_binding_remote;

#[path = "payload_staging_direct_file_binding_remote_tests.rs"]
mod direct_file_binding_remote;

#[path = "payload_staging_direct_file_sender_tests.rs"]
mod direct_file_sender;

#[path = "payload_staging_stream_file_sender_tests.rs"]
mod stream_file_sender;

#[path = "payload_staging_file_release_handoff_tests.rs"]
mod file_release;

fn matrix(transport: &str, history: &str) {
    for remote_witness in [false, true] {
        for (target, append) in candidate::cases() {
            with_terminal(
                transport,
                remote_witness,
                history,
                target.clone(),
                append,
                |f, primary, terminal| {
                    let original = terminal.installation_observation();
                    let before = f.observe();
                    let failed = primary
                        .confirm(terminal, deadline(), |_| {
                            panic!("unretired handoff performed inspection")
                        })
                        .err()
                        .expect("unretired handoff must fail");
                    assert!(failed
                        .error()
                        .to_string()
                        .contains("confirmed slot retirement"));
                    let mut terminal = failed.into_terminal();
                    assert_eq!(terminal.installation_observation(), original);
                    assert_eq!(
                        terminal.slot_retirement_observation(),
                        Retirement::NoAttemptObserved
                    );
                    assert_eq!(f.observe(), before);
                    primary.retire(&mut terminal);
                    let settled = f.observe();
                    let shadows = f.shadows();
                    let mut reached = false;
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| reached = true)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert!(reached);
                    assert_eq!(confirmed.id(), f.context.id());
                    assert_eq!(confirmed.installation_observation(), original);
                    assert_eq!(confirmed.observation().witness, Some(NodeId::new(0)));
                    assert_eq!(
                        confirmed.observation().prefix.applied_log_index,
                        f.context.id().log_index().get()
                    );
                    confirmed.require_valid().unwrap();
                    assert_eq!(f.observe(), settled);
                    assert_eq!(f.shadows(), shadows);
                    drop(confirmed);
                    assert_eq!(f.observe(), settled, "Drop must not clean or retry");
                },
            );
        }
    }
}

macro_rules! handoff_matrix {
    ($name:ident, $transport:literal, $history:literal) => {
        #[test]
        fn $name() {
            matrix($transport, $history);
        }
    };
}

handoff_matrix!(
    staging_window_handoff_embedded_inspected,
    "embedded",
    "inspected"
);
handoff_matrix!(
    staging_window_handoff_embedded_installed,
    "embedded",
    "installed"
);
handoff_matrix!(
    staging_window_handoff_embedded_uncertain,
    "embedded",
    "uncertain"
);
handoff_matrix!(staging_window_handoff_unix_inspected, "unix", "inspected");
handoff_matrix!(staging_window_handoff_unix_installed, "unix", "installed");
handoff_matrix!(staging_window_handoff_unix_uncertain, "unix", "uncertain");
handoff_matrix!(staging_window_handoff_tls_inspected, "tls", "inspected");
handoff_matrix!(staging_window_handoff_tls_installed, "tls", "installed");
handoff_matrix!(staging_window_handoff_tls_uncertain, "tls", "uncertain");

#[test]
fn staging_window_handoff_failure_returns_retired_custody_and_reinspects() {
    for transport in ["embedded", "unix", "tls"] {
        with_terminal(
            transport,
            true,
            "uncertain",
            StagingTarget::StreamPut,
            false,
            |f, primary, mut terminal| {
                primary.retire(&mut terminal);
                let original = terminal.installation_observation();
                let before = f.observe();
                let failed = primary
                    .confirm(terminal, Instant::now(), |_| {
                        panic!("expired attempt must not inspect")
                    })
                    .err()
                    .expect("expired attempt must fail");
                assert!(failed.error().to_string().contains("deadline"));
                let terminal = failed.into_terminal();
                assert_eq!(terminal.installation_observation(), original);
                assert_eq!(terminal.slot_retirement_observation(), Retirement::Retired);
                assert_eq!(f.observe(), before);
                let before = f.observe();
                let mut inspected = false;
                let confirmed = primary
                    .confirm(terminal, deadline(), |_| inspected = true)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert!(inspected);
                confirmed.require_valid().unwrap();
                assert_eq!(confirmed.installation_observation(), original);
                drop(confirmed);
                assert_eq!(f.observe(), before);
            },
        );
    }
}

#[test]
fn staging_window_handoff_freezes_cutoff_and_checks_after_inspection() {
    for transport in ["embedded", "unix", "tls"] {
        for expire_in_read in [false, true] {
            with_terminal(
                transport,
                true,
                "installed",
                StagingTarget::StreamPut,
                false,
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let before = f.observe();
                    let original = terminal.installation_observation();
                    let clock =
                        crate::clock::test_time_override_guard(crate::clock::current_time_millis());
                    let now = crate::clock::current_time_millis();
                    let result = primary.confirm(
                        terminal,
                        Instant::now() + Duration::from_secs(10),
                        |stage| {
                            if expire_in_read && stage == "after witness" {
                                clock.set(now + 15_000);
                            }
                        },
                    );
                    if expire_in_read {
                        let failed = result
                            .err()
                            .expect("late positive result must not mint custody");
                        let terminal = failed.into_terminal();
                        assert_eq!(terminal.installation_observation(), original);
                        assert_eq!(terminal.slot_retirement_observation(), Retirement::Retired);
                    } else {
                        let confirmed = result.unwrap_or_else(|e| panic!("{}", e.error()));
                        confirmed.require_valid().unwrap();
                        // Cross only the captured request cutoff, not the original
                        // 60-second publication budget or five-minute stream window.
                        clock.set(now + 15_000);
                        assert!(confirmed.require_valid().is_err());
                        assert_eq!(confirmed.installation_observation(), original);
                    }
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_window_handoff_rejects_foreign_embedded_admission_before_inspection() {
    with_terminal(
        "embedded",
        false,
        "uncertain",
        StagingTarget::StreamPut,
        false,
        |f, primary, mut terminal| {
            primary.retire(&mut terminal);
            let original = terminal.installation_observation();
            let foreign = setup(StagingTarget::StreamPut, false);
            assert_eq!(foreign.context.id(), f.context.id());
            let admission = foreign.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    foreign.context.id().pg_id(),
                    foreign.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let before = f.observe();
            let failure = terminal
                .into_confirmed_window_embedded(&section, deadline(), |_| {
                    panic!("foreign admission reached inspection")
                })
                .err()
                .expect("crossed admission must fail");
            let terminal = failure.into_terminal();
            assert_eq!(terminal.installation_observation(), original);
            assert_eq!(terminal.slot_retirement_observation(), Retirement::Retired);
            drop(
                primary
                    .confirm(terminal, deadline(), |_| {})
                    .unwrap_or_else(|e| panic!("{}", e.error())),
            );
            assert_eq!(f.observe(), before);
        },
    );
}

#[test]
fn staging_window_handoff_lost_reply_keeps_terminal_and_requires_fresh_confirmation() {
    use crate::storage_rpc::StorageRpcMessageKind;
    use std::sync::atomic::{AtomicUsize, Ordering};
    for transport in ["unix", "tls"] {
        with_terminal(
            transport,
            true,
            "uncertain",
            StagingTarget::StreamPut,
            false,
            |f, primary, mut terminal| {
                primary.retire(&mut terminal);
                let original = terminal.installation_observation();
                let remote = f.remote(NodeId::new(4)).unwrap();
                let replies = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&replies);
                remote
                    .server
                    .set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
                        if kind == StorageRpcMessageKind::MetadataCommandReplicaState
                            && count.fetch_add(1, Ordering::SeqCst) == 0
                        {
                            *bytes.last_mut().unwrap() ^= 1;
                        }
                    }));
                let before = f.observe();
                let failure = primary
                    .confirm(terminal, deadline(), |_| {})
                    .err()
                    .expect("lost reply cannot confirm");
                assert_eq!(replies.load(Ordering::SeqCst), 1, "no automatic retry");
                let terminal = failure.into_terminal();
                assert_eq!(terminal.installation_observation(), original);
                assert_eq!(terminal.slot_retirement_observation(), Retirement::Retired);
                assert_eq!(f.observe(), before);
                let session = terminal
                    .open_primary_session(&remote.client, deadline())
                    .unwrap();
                let confirmed = terminal
                    .into_confirmed_window_remote(&remote.client, &session, deadline(), |_| {})
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(replies.load(Ordering::SeqCst), 2);
                confirmed.require_valid().unwrap();
                assert_eq!(confirmed.installation_observation(), original);
                assert_eq!(f.observe(), before);
            },
        );
    }
}
