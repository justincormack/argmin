// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only historical command-proof release. No physical deletion or
//! owner retirement; every operation retains the all-member command holder.

use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandReservationDisposition as R,
};
use crate::node_runtime::pg_store::{CompleteTerminalHandoff, TerminalReservationObservation as O};
use crate::node_runtime::traits::PgMetadataStore;

#[path = "payload_staging_terminal_historical_command_release_fault_tests.rs"]
mod faults;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    DirectPut,
    OverwriteStandard,
    OverwriteMultipart,
    StreamPut,
    CreatePart,
    ReplacePart,
    AbortPut,
    CompleteMultipart,
    AbortMultipart,
    RetainPut,
    AbortPart,
}

const RELEASES: [Case; 7] = [
    Case::DirectPut,
    Case::StreamPut,
    Case::CreatePart,
    Case::ReplacePart,
    Case::AbortPut,
    Case::CompleteMultipart,
    Case::AbortMultipart,
];

fn context_for(
    pg: &PgStore,
    case: Case,
) -> (ContextualCommand, Option<BucketWriteReservationProof>) {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let mut described = None;
    let command = match case {
        Case::OverwriteStandard => displaced_retirement::setup(pg, 1, 2),
        Case::OverwriteMultipart => displaced_retirement::setup(pg, 2, 2),
        Case::CreatePart | Case::RetainPut => {
            let mut owner = fixture(if case == Case::CreatePart {
                multipart_target()
            } else {
                StagingTarget::StreamPut
            });
            let digest = pg.metadata_command_replica_state().unwrap().state_digest;
            pg.initialize_metadata_transfer_empty_state(4, owner.subject.originating_epoch, digest)
                .unwrap();
            owner.subject.id.introduction = MetadataCommandId::new(
                owner.subject.originating_epoch,
                owner.subject.object_pg,
                MetadataCommandLogIndex::new(1).unwrap(),
            );
            let command = fresh_capture::candidate(pg, &mut owner);
            described = Some(ContextualCommand::from_described(candidate::capture(
                pg,
                &owner,
                command.clone(),
                false,
            )));
            command
        }
        Case::DirectPut | Case::StreamPut => {
            let owner = object::initialize(
                pg,
                if case == Case::DirectPut {
                    StagingTarget::DirectPut
                } else {
                    StagingTarget::StreamPut
                },
                1,
            );
            object::command(pg, &owner, crate::VersionId::from_u64(42))
        }
        Case::ReplacePart => {
            let old = part::initialize(pg, 1);
            part::apply(pg, &part::command(pg, &old)).unwrap();
            let new = part::next_attempt(pg, &old, 1);
            part::command(pg, &new)
        }
        Case::AbortPut | Case::AbortPart => {
            let owner = if case == Case::AbortPut {
                object::initialize(pg, StagingTarget::StreamPut, 1)
            } else {
                part::initialize(pg, 1)
            };
            close::command(pg, &owner)
        }
        Case::CompleteMultipart | Case::AbortMultipart => {
            let owners = completion::setup(pg, 1);
            let command = if case == Case::CompleteMultipart {
                completion::command(pg, &owners[0], crate::VersionId::from_u64(42))
            } else {
                abort::command(pg, &owners[0])
            };
            // These component fixtures use deliberately sparse positions. At
            // their new epoch a short real anchored prefix is sufficient.
            MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                command.payload().clone(),
            )
        }
    };
    let proof = match command.payload().bucket_write_reservation_disposition() {
        R::ReleaseOnApply(proof)
        | R::ReleaseStreamCreationOnApply(proof)
        | R::RetainForStream(proof) => Some(proof.clone()),
        R::NoCommandProof => None,
    };
    let context = described.unwrap_or_else(|| {
        ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap()
    });
    complete_application::anchor(pg, context.id());
    (context, proof)
}

fn setup(case: Case, endpoints: &[(u32, bool)]) -> (Mixed, Option<BucketWriteReservationProof>) {
    let dir = test_util::tempdir();
    let mut proof = None;
    // Two original peers allow real Active publication at epoch 5, before the
    // fixtures' creation proofs. Epochs 6/7 remain Active until replacement.
    let fixture = Fixture::with_context_builder(&dir, 0, &[4, 2], None, true, |pg| {
        let (context, captured) = context_for(pg, case);
        proof = captured;
        context
    });
    (Mixed::from_fixture(dir, fixture, 0, endpoints, None), proof)
}

fn with_fanout(
    mixed: &Mixed,
    history: &str,
    cutoff: Instant,
    action: impl FnOnce(&crate::cluster::StorageClusterRouteAdmission, CompleteFanoutSender<'_>),
) {
    let f = &mixed.fixture;
    let admission = f.handle.admit_current_route().unwrap();
    let mut owner = mixed
        .owner(history, cutoff)
        .into_fanout(
            admission
                .complete_historical_fanout_route(&f.runtime_map, f.context.id().pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let original = owner.installation_observation();
    {
        let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
        let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
        let route = remote.is_none().then(|| {
            admission
                .embedded_historical_publication_route(&f.runtime_map, f.context.id().pg_id())
                .unwrap()
        });
        let section = route.as_ref().map(|r| r.lock_until(deadline()).unwrap());
        let session = remote.map(|p| owner.open_primary_session(&p.client, deadline()).unwrap());
        if let Some(section) = &section {
            owner.publish_embedded(section, deadline()).unwrap();
        } else {
            owner
                .publish(
                    &remote.unwrap().client,
                    session.as_ref().unwrap(),
                    deadline(),
                )
                .unwrap();
        }
        mixed
            .apply(&mut owner, section.as_ref(), session.as_ref(), 2)
            .unwrap();
        owner = owner
            .into_terminal()
            .err()
            .expect("the primary must also confirm")
            .into_fanout();
        assert_eq!(owner.installation_observation(), original);
        mixed
            .apply(&mut owner, section.as_ref(), session.as_ref(), 4)
            .unwrap();
    }
    assert!(owner.completion().is_ok());
    action(&admission, owner);
}

fn with_terminal(
    mixed: &Mixed,
    history: &str,
    cutoff: Instant,
    action: impl FnOnce(&crate::cluster::StorageClusterRouteAdmission, &mut CompleteTerminalHandoff<'_>),
) {
    with_fanout(mixed, history, cutoff, |admission, owner| {
        let original = owner.installation_observation();
        let before = mixed.observe();
        let mut terminal = owner
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(mixed.observe(), before);
        assert_eq!(
            terminal.retained_finalizer_reservation_observation(),
            O::NotRequired
        );
        action(admission, &mut terminal);
        assert_eq!(terminal.installation_observation(), original);
        for id in [2, 4] {
            assert_eq!(
                terminal.application_observation(NodeId::new(id)),
                Applied::Applied
            );
        }
        let before = mixed.observe();
        drop(terminal);
        assert_eq!(mixed.observe(), before, "Drop does not retry or retire");
    });
}

fn reservations(
    mixed: &Mixed,
    id: u32,
    proof: &BucketWriteReservationProof,
) -> (u64, Vec<crate::BucketWriteReservationRecord>) {
    let node = mixed.node(id);
    let pg = node
        .get_pg(topology().bucket_pg_for(&proof.bucket))
        .unwrap();
    (
        pg.conn.total_changes(),
        pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
    )
}

fn seed(mixed: &Mixed, proof: &BucketWriteReservationProof) {
    let pg_id = PgId::new(topology().bucket_pg_for(&proof.bucket));
    assert_ne!(pg_id, mixed.fixture.context.id().pg_id());
    assert!(proof.cluster_epoch < mixed.fixture.runtime_map.cluster_epoch());
    assert_eq!(
        mixed
            .fixture
            .map
            .metadata_pg_primary_node_for_retained_cleanup(proof.cluster_epoch, pg_id)
            .unwrap()
            .node_id(),
        NodeId::new(4)
    );
    assert_eq!(
        mixed.fixture.map.pg_route(pg_id).unwrap().primary_node_id(),
        NodeId::new(2)
    );
    for id in [4, 2] {
        let node = mixed.node(id);
        let pg = node.get_pg(pg_id.get()).unwrap();
        terminal_release::seed_pg_reservations(&pg, proof, None);
        pg.refresh_metadata_command_state_digest().unwrap();
    }
}

fn corrupt_first_success(remote: &Remote) -> Arc<AtomicUsize> {
    remote
        .server
        .set_response_frame_test_hook(Arc::new(|kind, frame| {
            if kind == Kind::ProofRelease {
                crate::storage_rpc::decode_storage_rpc_response_payload(&frame.payload)
                    .unwrap()
                    .expect("release succeeds before reply corruption");
            }
        }));
    let replies = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&replies);
    remote
        .server
        .set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
            if kind == Kind::ProofRelease && count.fetch_add(1, Ordering::SeqCst) == 0 {
                *bytes.last_mut().unwrap() ^= 1;
            }
        }));
    replies
}

fn matrix(transport: &str) {
    matrix_cases(transport, &RELEASES);
}

fn matrix_cases(transport: &str, cases: &[Case]) {
    let endpoints = match transport {
        "embedded" => vec![(2, false)],
        "unix" => vec![(4, false)],
        "tls" => vec![(4, true), (2, false)],
        _ => unreachable!(),
    };
    for &case in cases {
        for history in ["inspected", "installed", "uncertain"] {
            let (mixed, proof) = setup(case, &endpoints);
            let proof = proof.unwrap();
            with_terminal(&mixed, history, deadline(), |admission, terminal| {
                assert!(!terminal.work().is_quiescent());
                assert!(terminal.work().has_reservation_release());
                assert_eq!(terminal.reservation_observation(), O::NoAttemptObserved);
                seed(&mixed, &proof);
                let before = reservations(&mixed, 4, &proof);
                let current = reservations(&mixed, 2, &proof);
                let objects = mixed.observe();
                let materialized: Vec<_> = [2, 4]
                    .into_iter()
                    .map(|id| {
                        let node = mixed.node(id);
                        let pg = node
                            .get_pg(mixed.fixture.context.id().pg_id().get())
                            .unwrap();
                        introduction::state(&pg)
                    })
                    .collect();
                let remote = mixed.peers.iter().find(|p| p.id == NodeId::new(4));
                let replies = remote.map(corrupt_first_success);
                let _running = remote.map(Remote::run);
                terminal.release_retained_finalizer_reservation().unwrap();
                assert_eq!(reservations(&mixed, 4, &proof), before);
                assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || terminal.test_interrupt_reservation_release()
                ))
                .is_err());
                assert_eq!(terminal.reservation_observation(), O::Unconfirmed);
                assert_eq!(reservations(&mixed, 4, &proof), before);
                let (result, first_cutoffs) =
                    UnixStorageNodeClient::test_observe_proof_release_deadlines(|| {
                        terminal.release_reservation()
                    });
                if remote.is_some() {
                    assert!(result.is_err());
                    assert_eq!(terminal.reservation_observation(), O::Unconfirmed);
                    assert_eq!(replies.as_ref().unwrap().load(Ordering::SeqCst), 1);
                    assert_eq!(first_cutoffs.len(), 1);
                } else {
                    result.unwrap();
                }
                let after = reservations(&mixed, 4, &proof);
                assert_eq!(after.0, before.0 + 1, "{case:?}");
                assert_eq!(
                    after.1,
                    before
                        .1
                        .into_iter()
                        .filter(|r| r.reservation_id != proof.reservation_id)
                        .collect::<Vec<_>>()
                );
                assert_eq!(after.1.len(), 1);
                let (result, retry_cutoffs) =
                    UnixStorageNodeClient::test_observe_proof_release_deadlines(|| {
                        terminal.release_reservation()
                    });
                result.unwrap();
                if remote.is_some() {
                    assert_eq!(first_cutoffs, retry_cutoffs);
                }
                assert_eq!(terminal.reservation_observation(), O::Released);
                terminal.release_reservation().unwrap();
                assert_eq!(
                    reservations(&mixed, 4, &proof),
                    after,
                    "exact absence and confirmed retry do not write"
                );
                if let Some(replies) = replies {
                    assert_eq!(
                        replies.load(Ordering::SeqCst),
                        2,
                        "no redispatch after confirmation"
                    );
                }
                let retirement = if let Some(remote) = remote {
                    let session = terminal
                        .open_primary_session(&remote.client, deadline())
                        .unwrap();
                    terminal.retire_remote(&remote.client, &session, deadline())
                } else {
                    let route = admission
                        .embedded_historical_publication_route(
                            &mixed.fixture.runtime_map,
                            mixed.fixture.context.id().pg_id(),
                        )
                        .unwrap();
                    let section = route.lock_until(deadline()).unwrap();
                    terminal.retire_embedded(&section, deadline())
                };
                if matches!(
                    case,
                    Case::CreatePart
                        | Case::DirectPut
                        | Case::OverwriteStandard
                        | Case::OverwriteMultipart
                ) {
                    retirement.unwrap();
                    assert_eq!(
                        terminal.slot_retirement_observation(),
                        crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired
                    );
                    for (id, expected) in [2, 4].into_iter().zip(&materialized) {
                        let node = mixed.node(id);
                        let pg = node
                            .get_pg(mixed.fixture.context.id().pg_id().get())
                            .unwrap();
                        assert_eq!(&introduction::state(&pg), expected);
                        if id == 4 {
                            assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
                        }
                    }
                } else {
                    let error = retirement.unwrap_err();
                    assert!(
                        error.to_string().contains("terminal work integration"),
                        "{error}"
                    );
                    assert_eq!(mixed.observe(), objects, "release cannot remove owner, selected/displaced references, pending slot or indexes");
                }
                assert_eq!(
                    reservations(&mixed, 2, &proof),
                    current,
                    "current bucket primary must remain untouched"
                );
            });
        }
    }
}

#[test]
fn staging_historical_command_release_embedded_write_families_and_origins() {
    matrix("embedded");
}
#[test]
fn staging_historical_command_release_unix_write_families_and_origins() {
    matrix("unix");
}
#[test]
fn staging_historical_command_release_tls_write_families_and_origins() {
    matrix("tls");
}

#[test]
fn staging_displaced_retirement_embedded_complete_handoff() {
    matrix_cases(
        "embedded",
        &[Case::OverwriteStandard, Case::OverwriteMultipart],
    );
}

#[test]
fn staging_displaced_retirement_unix_complete_handoff() {
    matrix_cases("unix", &[Case::OverwriteStandard, Case::OverwriteMultipart]);
}

#[test]
fn staging_displaced_retirement_tls_complete_handoff() {
    matrix_cases("tls", &[Case::OverwriteStandard, Case::OverwriteMultipart]);
}

#[test]
fn staging_displaced_retirement_lost_reply_then_logged_reclaim_completion_converges() {
    use crate::metadata_command::{
        DeleteObjectPayloadReclaimCommand, MetadataCommandId, MetadataCommandLogIndex,
        MetadataCommandPayload, ObjectPayloadReclaimClaimProof, ObjectPayloadReclaimCommand,
    };
    for case in [Case::OverwriteStandard, Case::OverwriteMultipart] {
        let (mixed, proof) = setup(case, &[(2, false)]);
        let proof = proof.unwrap();
        with_terminal(&mixed, "installed", deadline(), |admission, terminal| {
            seed(&mixed, &proof);
            terminal.release_reservation().unwrap();
            let route = admission
                .embedded_historical_publication_route(
                    &mixed.fixture.runtime_map,
                    terminal.id().pg_id(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            assert!(terminal
                .test_retire_embedded_with_hook(&section, deadline(), |point| {
                    if point == "after-commit" {
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "lost overwrite slot removal reply",
                        });
                    }
                    Ok(())
                })
                .is_err());
            let node = mixed.node(4);
            let before = {
                let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
                let generation: u64 = pg.conn.query_row("SELECT COALESCE(upload_generation, payload_generation) FROM payload_staging_owners WHERE lifecycle = 3 LIMIT 1", [], |r| r.get(0)).unwrap();
                let generation = crate::GenerationId::new(generation).unwrap();
                let key =
                    crate::ObjectKey::try_from(proof.target_context.as_deref().unwrap()).unwrap();
                let payload = if case == Case::OverwriteStandard {
                    ObjectPayloadReclaimCommand::Segments(
                        pg.get_object_segments_reclaim(&proof.bucket, &key, generation)
                            .unwrap()
                            .unwrap(),
                    )
                } else {
                    ObjectPayloadReclaimCommand::Multipart(
                        pg.get_multipart_reclaim(&proof.bucket, &key, generation)
                            .unwrap()
                            .unwrap(),
                    )
                };
                let command = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        terminal.id().cluster_epoch(),
                        terminal.id().pg_id(),
                        MetadataCommandLogIndex::new(terminal.id().log_index().get() + 1).unwrap(),
                    ),
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                        DeleteObjectPayloadReclaimCommand::new(
                            proof.bucket.clone(),
                            key,
                            generation,
                            payload.clone(),
                            ObjectPayloadReclaimClaimProof {
                                bucket_incarnation_generation: proof.bucket_incarnation_generation,
                                reclaim_kind: payload.kind(),
                                claim_id: "later-reclaim".into(),
                                owner_token: "later-owner".into(),
                                cluster_epoch: terminal.id().cluster_epoch(),
                            },
                        ),
                    )),
                );
                let context =
                    ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap();
                pg.apply_complete_metadata_command_and_record(
                    4,
                    &context.encode_command().unwrap(),
                    &topology(),
                )
                .unwrap();
                (pg.conn.total_changes(), introduction::state(&pg))
            };
            terminal.retire_embedded(&section, deadline()).unwrap();
            assert_eq!(
                terminal.slot_retirement_observation(),
                crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired
            );
            let pg = node.get_pg(terminal.id().pg_id().get()).unwrap();
            assert_eq!((pg.conn.total_changes(), introduction::state(&pg)), before);
            assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
        });
    }
}

#[test]
fn staging_displaced_retirement_transaction_rejects_lost_old_custody_on_absence_retry() {
    use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retired;
    for case in [Case::OverwriteStandard, Case::OverwriteMultipart] {
        for absent in [false, true] {
            let (mixed, proof) = setup(case, &[(2, false)]);
            let proof = proof.unwrap();
            with_terminal(&mixed, "installed", deadline(), |admission, terminal| {
                seed(&mixed, &proof);
                terminal.release_reservation().unwrap();
                let route = admission
                    .embedded_historical_publication_route(
                        &mixed.fixture.runtime_map,
                        mixed.fixture.context.id().pg_id(),
                    )
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                if absent {
                    assert!(terminal
                        .test_retire_embedded_with_hook(&section, deadline(), |point| {
                            if point == "after-commit" {
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "overwrite retirement lost reply",
                                });
                            }
                            Ok(())
                        })
                        .is_err());
                }
                let node = mixed.node(4);
                let pg_id = mixed.fixture.context.id().pg_id().get();
                let before = {
                    let pg = node.get_pg(pg_id).unwrap();
                    // The incoming published owner remains intact; remove only
                    // the independently retained displaced owners.
                    let removed = pg
                        .conn
                        .execute("DELETE FROM payload_staging_owners WHERE lifecycle = 3", [])
                        .unwrap();
                    assert!(removed > 0);
                    (pg.conn.total_changes(), complete_pending::observe(&pg))
                };
                assert!(terminal
                    .test_retire_embedded_with_hook(&section, deadline(), |_| panic!(
                        "lost displaced custody reached DELETE"
                    ))
                    .is_err());
                assert_eq!(terminal.slot_retirement_observation(), Retired::Unconfirmed);
                let pg = node.get_pg(pg_id).unwrap();
                assert_eq!(
                    (pg.conn.total_changes(), complete_pending::observe(&pg)),
                    before
                );
                assert_eq!(before.1.iter().all(Vec::is_empty), absent);
            });
        }
    }
}

#[test]
fn staging_historical_command_release_does_not_release_live_put_or_admit_payload_only_abort() {
    for endpoints in [vec![], vec![(4, false)], vec![(4, true)]] {
        let (mixed, proof) = setup(Case::RetainPut, &endpoints);
        let proof = proof.unwrap();
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            assert_eq!(terminal.reservation_observation(), O::NotRequired);
            assert!(!terminal.work().has_reservation_release());
            seed(&mixed, &proof);
            let before = reservations(&mixed, 4, &proof);
            terminal.release_reservation().unwrap();
            assert_eq!(reservations(&mixed, 4, &proof), before);
        });
        let (mixed, proof) = setup(Case::AbortPart, &endpoints);
        assert!(proof.is_none());
        with_fanout(&mixed, "uncertain", deadline(), |_, owner| {
            let before = mixed.observe();
            let original = owner.installation_observation();
            let failure = owner
                .into_terminal()
                .err()
                .expect("physical-only cleanup stays gated");
            assert!(failure
                .error()
                .to_string()
                .contains("non-quiescent work integration"));
            let owner = failure.into_fanout();
            assert_eq!(owner.installation_observation(), original);
            assert!(owner.completion().is_ok());
            drop(owner);
            assert_eq!(mixed.observe(), before);
        });
    }
}

#[test]
fn staging_historical_command_release_restart_reconfirms_lost_delete_without_replaying_apply() {
    for (case, tls) in [(Case::AbortPut, false), (Case::ReplacePart, true)] {
        let (mixed, proof) = setup(case, &[(4, tls)]);
        let proof = proof.unwrap();
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            seed(&mixed, &proof);
            let peer = mixed.peer(4);
            let count = corrupt_first_success(peer);
            let _running = peer.run();
            assert!(terminal.release_reservation().is_err());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(terminal.reservation_observation(), O::Unconfirmed);
            assert_eq!(reservations(&mixed, 4, &proof).1.len(), 1);
        });
        let mixed = lifetime::reopen(mixed);
        with_terminal(&mixed, "inspected", deadline(), |_, terminal| {
            let before = reservations(&mixed, 4, &proof);
            let objects = mixed.observe();
            let peer = mixed.peer(4);
            let count = peer.count(Kind::ProofRelease);
            let _running = peer.run();
            terminal.release_reservation().unwrap();
            assert_eq!(terminal.reservation_observation(), O::Released);
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(reservations(&mixed, 4, &proof), before);
            assert_eq!(mixed.observe(), objects);
        });
    }
}
