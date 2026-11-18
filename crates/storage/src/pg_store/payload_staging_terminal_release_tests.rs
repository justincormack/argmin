// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual publication/application followed by closed terminal reservation release.
//! Metadata-only owner fixtures: no physical deletion. Creation additionally
//! exercises reservation release followed by exact pending-slot retirement.

use super::*;
use crate::metadata_command::MetadataCommandPayload;
use crate::node_runtime::pg_store::payload_staging::abandonment::source::PartFinalizationSource;
use crate::node_runtime::pg_store::CompleteTerminalHandoff;
use crate::node_runtime::pg_store::TerminalReservationObservation as Observation;
use crate::node_runtime::traits::{DurableBucketWriteReservationAcquire, PgMetadataStore};
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::StorageRpcMessageKind as Kind;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[path = "payload_staging_creation_retirement_tests.rs"]
mod creation_retirement;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    CreatePart,
    FinalizePart,
    AbortPut,
    RetainPut,
    RetainedFinalizer,
    RetainedFinalizerOnPublished,
}

impl Case {
    fn is_finalizer(self) -> bool {
        matches!(
            self,
            Self::RetainedFinalizer | Self::RetainedFinalizerOnPublished
        )
    }
    fn release(
        self,
        terminal: &mut CompleteTerminalHandoff<'_>,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        if self.is_finalizer() {
            terminal.release_retained_finalizer_reservation()
        } else {
            terminal.release_reservation()
        }
    }
    fn observation(self, terminal: &CompleteTerminalHandoff<'_>) -> Observation {
        if self.is_finalizer() {
            terminal.retained_finalizer_reservation_observation()
        } else {
            terminal.reservation_observation()
        }
    }
    fn interrupt(
        self,
        terminal: &mut CompleteTerminalHandoff<'_>,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        if self.is_finalizer() {
            terminal.test_interrupt_retained_finalizer_release()
        } else {
            terminal.test_interrupt_reservation_release()
        }
    }
}

struct Setup {
    primary: Arc<StorageNodeServer>,
    primary_client: Arc<crate::node_client::UnixStorageNodeClient>,
    config: StorageNodeProcessConfig,
    context: ContextualCommand,
    proof: BucketWriteReservationProof,
    crossed_source: Option<ContextualCommand>,
    winning_proof: Option<BucketWriteReservationProof>,
}

fn setup(tls: bool, case: Case, dir: &test_util::TempDir) -> Setup {
    let mut config = config(if matches!(case, Case::AbortPut | Case::FinalizePart) {
        2
    } else {
        0
    });
    let bucket_pg = topology().bucket_pg_for(&fixture(StagingTarget::DirectPut).subject.bucket);
    assert_ne!(
        bucket_pg,
        fixture(StagingTarget::DirectPut).subject.object_pg.get()
    );
    if case.is_finalizer() {
        config.cluster_epoch = ClusterEpoch::new(7).unwrap();
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
        }
    }
    if case == Case::AbortPut || case.is_finalizer() {
        config.historical_pg_routes = config.pg_routes.clone();
        for route in &mut config.historical_pg_routes {
            route.cluster_epoch =
                ClusterEpoch::new(if case.is_finalizer() { 6 } else { 5 }).unwrap();
            if route.pg_id == bucket_pg {
                route.primary_node_id = NodeId::new(2);
            }
        }
    }
    let (server, client, config) = if tls {
        config.data_dir = dir.path().join("node");
        config.socket_path = dir.path().join("unused.sock");
        let (server, client) =
            super::tls::tls_server_at(config.clone(), false, config.cluster_epoch);
        (server, client, config)
    } else {
        installed_config(dir, config, true)
    };
    let node = server.test_storage_node();
    let pg = node
        .get_pg(fixture(StagingTarget::DirectPut).subject.object_pg.get())
        .unwrap();
    let mut crossed_source = None;
    let mut winning_proof = None;
    let (context, proof) = {
        let _clock = crate::clock::test_time_override_guard(2_000);
        match case {
            Case::CreatePart | Case::RetainPut => {
                let target = if case == Case::CreatePart {
                    multipart_target()
                } else {
                    StagingTarget::StreamPut
                };
                let mut owner = fixture(target);
                owner.subject.id.introduction = MetadataCommandId::new(
                    owner.subject.originating_epoch,
                    owner.subject.object_pg,
                    MetadataCommandLogIndex::new(1).unwrap(),
                );
                let command = fresh_capture::candidate(&pg, &mut owner);
                let proof = command
                    .payload()
                    .primary_bucket_write_reservation_proof()
                    .unwrap()
                    .clone();
                (
                    ContextualCommand::from_described(candidate::capture(
                        &pg, &owner, command, false,
                    )),
                    proof,
                )
            }
            Case::FinalizePart => {
                let owner = part::initialize(&pg, 0);
                let command = part::command(&pg, &owner);
                let proof = command
                    .payload()
                    .primary_bucket_write_reservation_proof()
                    .unwrap()
                    .clone();
                (
                    ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap(),
                    proof,
                )
            }
            Case::AbortPut => {
                let owner = append::initialize(&pg, StagingTarget::StreamPut);
                let command = close::command(&pg, &owner);
                let MetadataCommandPayload::AbortStreamUpload(abort) = command.payload() else {
                    unreachable!()
                };
                let proof = abort
                    .stream_create_bucket_write_reservation
                    .clone()
                    .unwrap();
                (
                    ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap(),
                    proof,
                )
            }
            Case::RetainedFinalizer | Case::RetainedFinalizerOnPublished => {
                // Source epoch 6 is authenticated by a real complete tombstone;
                // cleanup runs in epoch 7 with its independent bucket route.
                let owner = part::initialize(&pg, 0);
                let source = part::command(&pg, &owner);
                let proof = source
                    .payload()
                    .primary_bucket_write_reservation_proof()
                    .unwrap()
                    .clone();
                let cleanup = crate::metadata_command::MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        config.cluster_epoch,
                        source.id().pg_id(),
                        MetadataCommandLogIndex::new(1).unwrap(),
                    ),
                    abandonment::cleanup(&pg, &owner, &source).payload().clone(),
                );
                let context = ContextualCommand::abandoned_part(
                    PartFinalizationSource::capture_for_test(&source)
                        .unwrap()
                        .bind_cleanup(cleanup.clone(), &topology())
                        .unwrap(),
                    &topology(),
                )
                .unwrap();
                let MetadataCommandPayload::CommitStreamPart(mut crossed) =
                    source.payload().clone()
                else {
                    unreachable!()
                };
                crossed
                    .bucket_write_reservation
                    .owner_token
                    .push_str("-crossed");
                let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
                    source.id(),
                    MetadataCommandPayload::CommitStreamPart(crossed),
                );
                // Identical outer abort; only the retained source differs.
                crossed_source = Some(
                    ContextualCommand::abandoned_part(
                        PartFinalizationSource::capture_for_test(&crossed)
                            .unwrap()
                            .bind_cleanup(cleanup, &topology())
                            .unwrap(),
                        &topology(),
                    )
                    .unwrap(),
                );
                abandonment::terminal(&pg, &source, true);
                pg.conn.execute_batch("COMMIT").unwrap();
                if case == Case::RetainedFinalizerOnPublished {
                    let MetadataCommandPayload::CommitStreamPart(mut winning) =
                        source.payload().clone()
                    else {
                        unreachable!()
                    };
                    winning
                        .bucket_write_reservation
                        .reservation_id
                        .push_str("-winner");
                    winning
                        .bucket_write_reservation
                        .owner_token
                        .push_str("-winner");
                    winning_proof = Some(winning.bucket_write_reservation.clone());
                    let winner = crate::metadata_command::MetadataCommandEnvelope::new(
                        MetadataCommandId::new(
                            source.id().cluster_epoch(),
                            source.id().pg_id(),
                            MetadataCommandLogIndex::new(source.id().log_index().get() + 1)
                                .unwrap(),
                        ),
                        MetadataCommandPayload::CommitStreamPart(winning),
                    );
                    let winner =
                        ContextualCommand::decode_field(winner, &[1, 0, 0], &topology()).unwrap();
                    complete_application::anchor(&pg, winner.id());
                    complete_log_range::apply(&pg, &winner);
                }
                (context, proof)
            }
        }
    };
    complete_application::anchor(&pg, context.id());
    install(&pg, &context);
    drop(pg);
    Setup {
        primary: Arc::new(server),
        primary_client: Arc::new(client),
        config,
        context,
        proof,
        crossed_source,
        winning_proof,
    }
}

fn seed_reservations(
    server: &StorageNodeServer,
    proof: &BucketWriteReservationProof,
    winning_proof: Option<&BucketWriteReservationProof>,
) {
    let node = server.test_storage_node();
    let pg = node
        .get_pg(topology().bucket_pg_for(&proof.bucket))
        .unwrap();
    seed_pg_reservations(&pg, proof, winning_proof);
}

pub(super) fn seed_pg_reservations(
    pg: &PgStore,
    proof: &BucketWriteReservationProof,
    winning_proof: Option<&BucketWriteReservationProof>,
) {
    pg.create_bucket(
        &proof.bucket,
        "owner",
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    // Owner-local metadata fixture: reproduce the existing bucket incarnation
    // captured by the command fixtures, then use the real reservation allocator.
    pg.conn
        .execute(
            "UPDATE buckets SET bucket_incarnation_generation = ?1,
        bucket_execution_generation = ?2 WHERE name = ?3",
            rusqlite::params![
                proof.bucket_incarnation_generation,
                proof.bucket_execution_generation,
                proof.bucket.as_str()
            ],
        )
        .unwrap();
    let mut canary = proof.clone();
    canary.reservation_id = "independent-canary".to_string();
    for proof in std::iter::once(proof)
        .chain(std::iter::once(&canary))
        .chain(winning_proof)
    {
        let record = pg
            .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                name: &proof.bucket,
                reservation_id: &proof.reservation_id,
                owner_token: &proof.owner_token,
                cluster_epoch: proof.cluster_epoch,
                operation_kind: &proof.operation_kind,
                created_at: proof.created_at,
                lease_deadline: proof.lease_deadline,
                target_context: proof.target_context.as_deref(),
            })
            .unwrap();
        assert!(proof.matches_record(&record));
    }
}

#[test]
fn staging_terminal_reservation_release_unix_preserves_custody_and_exact_retry() {
    matrix(false, &COMMAND_CASES);
}

#[test]
fn staging_terminal_reservation_release_tls_preserves_custody_and_exact_retry() {
    matrix(true, &COMMAND_CASES);
}

#[test]
fn staging_terminal_finalizer_release_unix_preserves_exact_source_and_retry_custody() {
    matrix(
        false,
        &[Case::RetainedFinalizer, Case::RetainedFinalizerOnPublished],
    );
}

#[test]
fn staging_terminal_finalizer_release_tls_preserves_exact_source_and_retry_custody() {
    matrix(
        true,
        &[Case::RetainedFinalizer, Case::RetainedFinalizerOnPublished],
    );
}

const COMMAND_CASES: [Case; 4] = [
    Case::CreatePart,
    Case::FinalizePart,
    Case::AbortPut,
    Case::RetainPut,
];

fn matrix(tls: bool, cases: &[Case]) {
    for history in ["inspected", "installed", "uncertain"] {
        for &case in cases {
            let primary_dir = test_util::tempdir();
            let Setup {
                primary,
                primary_client,
                config,
                context,
                proof,
                crossed_source,
                winning_proof,
            } = setup(tls, case, &primary_dir);
            let replica_dirs: Vec<_> = (0..4).map(|_| test_util::tempdir()).collect();
            let peers: Vec<_> = [2, 0, 3, 1]
                .into_iter()
                .zip(&replica_dirs)
                .map(|(id, dir)| {
                    let (server, client) = application_sender::replica_from_primary(
                        tls,
                        &primary,
                        &config,
                        context.id().pg_id(),
                        dir,
                        NodeId::new(id),
                    );
                    (Arc::new(server), Arc::new(client))
                })
                .collect();
            let reservation_node = if case == Case::AbortPut || case.is_finalizer() {
                &peers[0].0
            } else {
                &primary
            };
            seed_reservations(reservation_node, &proof, winning_proof.as_ref());
            let mut clients = vec![Arc::clone(&primary_client)];
            clients.extend(peers.iter().map(|(_, client)| Arc::clone(client)));
            let map_dir = test_util::tempdir();
            let now = crate::clock::current_time_millis();
            let monotonic = crate::clock::monotonic_time_millis();
            let mut frontend = config.clone();
            frontend.route_map_validity = RouteMapValidity::until_ms(now + 60_000).unwrap();
            let (map, handle) = fanout::map(&map_dir, &frontend, &clients);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
                .unwrap();
            let encoded = context.encode_command().unwrap();
            let sender = if history == "inspected" {
                pending(&context).into_publication_sender(config.node_id, deadline())
            } else {
                let copy = ContextualCommand::decode_command(
                    encoded.bytes(),
                    encoded.checksum_crc64(),
                    &topology(),
                )
                .unwrap();
                let mut submission = copy.into_submission();
                let node = primary.test_storage_node();
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                if history == "uncertain" {
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_slot")
                        .unwrap();
                    handoff::uncertain_install(&mut submission, &pg);
                    install(&pg, &context);
                } else {
                    complete_pending::install(&mut submission, &pg).unwrap();
                }
                submission.into_publication_sender(config.node_id, deadline())
            };
            let original = sender.installation_observation();
            let mut sender = sender
                .into_fanout(route)
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let running: Vec<_> = std::iter::once(Arc::clone(&primary))
                .chain(peers.iter().map(|(server, _)| Arc::clone(server)))
                .map(|server| application_sender::RunningReplica::start(server, tls))
                .collect();
            let session = primary_client
                .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                .unwrap();
            sender
                .publish(&primary_client, &session, deadline())
                .unwrap();
            sender.apply_replica(&peers[1].1, deadline()).unwrap();
            sender
                .apply_primary(&primary_client, &session, deadline())
                .unwrap();
            for i in [0, 2, 3] {
                sender.apply_replica(&peers[i].1, deadline()).unwrap();
            }
            drop(session); // Release RPC must not wait on the held object-PG session.
            let mut terminal = sender
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            if case.is_finalizer() {
                let node = primary.test_storage_node();
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                let snapshot = pg.command_build_snapshot().unwrap();
                let template = fixture(multipart_target());
                let retained =
                    sql::load(&pg.conn, context.id().pg_id(), &template.subject.id.attempt)
                        .unwrap()
                        .unwrap();
                let resource = |proof: &BucketWriteReservationProof| {
                    StagingBucketResource::capture(
                        proof,
                        PgId::new(topology().bucket_pg_for(&proof.bucket)),
                    )
                };
                if let Some(winner) = &winning_proof {
                    assert_eq!(retained.lifecycle, StagingLifecycle::Published);
                    assert_eq!(
                        retained.resources.finalization,
                        ResourceResponsibility::ReleaseDue(resource(winner))
                    );
                    assert_eq!(
                        retained.resources.abandoned_finalizations,
                        vec![resource(&proof)]
                    );
                } else {
                    assert_eq!(
                        retained.resources.finalization,
                        ResourceResponsibility::ReleaseDue(resource(&proof))
                    );
                    assert_eq!(
                        retained.lifecycle,
                        StagingLifecycle::Retiring(StagingRetirement::Unpublished)
                    );
                }
                snapshot.finish().unwrap();
            }
            let object_before = snapshot(
                &primary
                    .test_storage_node()
                    .get_pg(context.id().pg_id().get())
                    .unwrap(),
            );
            let observe = || {
                let node = reservation_node.test_storage_node();
                let pg = node
                    .get_pg(topology().bucket_pg_for(&proof.bucket))
                    .unwrap();
                (
                    pg.conn.total_changes(),
                    pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
                )
            };
            let before = observe();
            if case == Case::CreatePart {
                creation_retirement::require_release_gate(&mut terminal);
            }
            if case.is_finalizer() {
                terminal.release_reservation().unwrap();
                assert_eq!(terminal.reservation_observation(), Observation::NotRequired);
                assert_eq!(case.observation(&terminal), Observation::NoAttemptObserved);
            } else {
                let command_observation = terminal.reservation_observation();
                terminal.release_retained_finalizer_reservation().unwrap();
                assert_eq!(
                    terminal.retained_finalizer_reservation_observation(),
                    Observation::NotRequired
                );
                assert_eq!(terminal.reservation_observation(), command_observation);
            }
            assert_eq!(
                observe(),
                before,
                "the independent no-work branch cannot release this proof"
            );
            terminal.test_require_release_context(&context).unwrap();
            if let Some(crossed) = crossed_source {
                assert!(matches!(
                    terminal.test_require_release_context(&crossed),
                    Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "terminal reservation complete context"
                    })
                ));
                assert_eq!(observe(), before);
            }
            // Same command ID, valid complete carrier, different command bytes.
            // An ID-only authority check would accept this independently.
            let crossed = ContextualCommand::decode_field(
                complete_log_range::ordinary(context.id()),
                &[1, 0, 0],
                &topology(),
            )
            .unwrap();
            assert!(matches!(
                terminal.test_require_release_context(&crossed),
                Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "terminal reservation complete context"
                })
            ));
            assert_eq!(observe(), before);
            let replies = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&replies);
            reservation_node.set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
                if kind == Kind::ProofRelease && count.fetch_add(1, Ordering::SeqCst) == 0 {
                    *bytes.last_mut().unwrap() ^= 1; // Release committed, exact reply untrusted.
                }
            }));
            if case == Case::RetainPut {
                terminal.release_reservation().unwrap();
                assert_eq!(terminal.reservation_observation(), Observation::NotRequired);
                assert_eq!(
                    observe(),
                    before,
                    "live stream retains its exact creation proof"
                );
                assert_eq!(replies.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(case.observation(&terminal), Observation::NoAttemptObserved);
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        map.test_store_route_map_validity(
                            RouteMapValidity::until_ms(now + 240_000).unwrap(),
                        );
                        admission.require_valid_now().unwrap();
                        assert!(case.release(&mut terminal).is_err());
                        assert_eq!(case.observation(&terminal), Observation::NoAttemptObserved);
                    },
                );
                assert_eq!(observe(), before);
                assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || case.interrupt(&mut terminal)
                ))
                .is_err());
                assert_eq!(case.observation(&terminal), Observation::Unconfirmed);
                assert_eq!(observe(), before);
                assert_eq!(replies.load(Ordering::SeqCst), 0);
                let (result, first_cutoffs) =
                    crate::node_client::UnixStorageNodeClient::test_observe_proof_release_deadlines(
                        || case.release(&mut terminal),
                    );
                assert!(result.is_err());
                assert_eq!(first_cutoffs.len(), 1);
                assert_eq!(case.observation(&terminal), Observation::Unconfirmed);
                assert_eq!(replies.load(Ordering::SeqCst), 1, "no hidden retry");
                if case == Case::CreatePart {
                    creation_retirement::require_release_gate(&mut terminal);
                }
                let after = observe();
                assert_eq!(after.0, before.0 + 1);
                let expected_remaining: Vec<_> = before
                    .1
                    .iter()
                    .filter(|record| record.reservation_id != proof.reservation_id)
                    .cloned()
                    .collect();
                assert!(!expected_remaining.is_empty());
                assert_eq!(
                    after.1, expected_remaining,
                    "only the exact source proof may be released"
                );
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        assert!(case.release(&mut terminal).is_err());
                        assert_eq!(case.observation(&terminal), Observation::Unconfirmed);
                    },
                );
                assert_eq!(replies.load(Ordering::SeqCst), 1);
                // Move only the caller's wall clock, within RPC authentication's
                // freshness window. Independently inspect the encoded cutoff:
                // successful release alone would not detect its reprojection.
                let (result, retry_cutoffs) =
                    crate::node_client::UnixStorageNodeClient::test_observe_proof_release_deadlines(
                        || {
                            crate::clock::with_time_and_monotonic_override(
                                crate::clock::current_time_millis() - 100,
                                crate::clock::monotonic_time_millis(),
                                || case.release(&mut terminal),
                            )
                        },
                    );
                result.unwrap();
                assert_eq!(retry_cutoffs, first_cutoffs);
                assert_eq!(case.observation(&terminal), Observation::Released);
                assert_eq!(replies.load(Ordering::SeqCst), 2);
                assert_eq!(observe(), after, "absence replay does not delete again");
                case.release(&mut terminal).unwrap();
                assert_eq!(
                    replies.load(Ordering::SeqCst),
                    2,
                    "confirmed release is not dispatched twice"
                );
            }
            assert_eq!(terminal.installation_observation(), original);
            if case.is_finalizer() {
                terminal.release_reservation().unwrap();
                assert_eq!(terminal.reservation_observation(), Observation::NotRequired);
                assert_eq!(case.observation(&terminal), Observation::Released);
            } else {
                assert_eq!(
                    terminal.retained_finalizer_reservation_observation(),
                    Observation::NotRequired
                );
            }
            for id in [4, 2, 0, 3, 1] {
                assert_eq!(
                    terminal.application_observation(NodeId::new(id)),
                    crate::node_runtime::pg_store::ApplicationObservation::Applied
                );
            }
            let released = observe();
            assert_eq!(
                snapshot(
                    &primary
                        .test_storage_node()
                        .get_pg(context.id().pg_id().get())
                        .unwrap()
                ),
                object_before,
                "release must not retire pending slot, staging rows or terminal proof obligations"
            );
            if case == Case::CreatePart {
                creation_retirement::retire(
                    &primary,
                    &primary_client,
                    &context,
                    &mut terminal,
                    history == "installed",
                );
            }
            drop(terminal);
            assert_eq!(observe(), released, "retirement/Drop perform no release");
            drop(running);
        }
    }
}
